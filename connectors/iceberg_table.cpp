#include "iceberg_table.h"
#include "iceberg_changes.h"
#include "iceberg_partition.h"

#include "ud/data.h"
#include "ud/memory.h"

#include "iceberg/catalog.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/data/data_writer.h"
#include "iceberg/data/equality_delete_writer.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/file_io.h"
#include "iceberg/iceberg_bundle_export.h"
#include "iceberg/location_provider.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/sort_order.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/table.h"
#include "iceberg/table_scan.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/type.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/row_delta.h"
#include "iceberg/update/update_schema.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <deque>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

/* These are foundation's deliberately small cross-platform file primitives.
 * The C adapter and this connector use the same path policy and large-file
 * offsets without exposing foundation internals through the public ABI. */
extern "C" {
ud_status ud_path_no_symlinks(ud_context *, const char *);
ud_status ud_open_regular(ud_context *, const char *, FILE **);
ud_status ud_remove_file(ud_context *, const char *);
ud_status ud_flush(ud_context *, FILE *);
ud_status ud_sync_parent(ud_context *, const char *);
FILE *ud_fopen(const char *, const char *);
int ud_seek(FILE *, uint64_t);
int64_t ud_tell(FILE *);
}

/* Apache Iceberg 0.4 keeps the REST JSON serde declarations in its source
 * tree, while exporting the symbols from iceberg_rest.  Keep the declarations
 * local so consumers only need the official installed headers and libraries. */
namespace iceberg::rest {
iceberg::Result<LoadTableResult> LoadTableResultFromJson(const nlohmann::json&);
iceberg::Result<nlohmann::json> ToJson(const CreateTableRequest&);
iceberg::Result<nlohmann::json> ToJson(const CommitTableRequest&);
iceberg::Result<CommitTableResponse> CommitTableResponseFromJson(const nlohmann::json&);
}  // namespace iceberg::rest

using iceberg::ErrorKind;
using iceberg::Result;
using iceberg::Status;

static ud_status map_error(const iceberg::Error& error) {
    switch (error.kind) {
    case ErrorKind::kAuthenticationFailed:
    case ErrorKind::kForbidden:
    case ErrorKind::kNotAuthorized:
        return UD_AUTH;
    case ErrorKind::kAlreadyExists:
    case ErrorKind::kCommitFailed:
    case ErrorKind::kRetryableValidationFailed:
        return UD_BUSY;
    case ErrorKind::kCommitStateUnknown:
        return UD_COMMIT_UNKNOWN;
    case ErrorKind::kInvalidArgument:
    case ErrorKind::kInvalid:
    case ErrorKind::kInvalidSchema:
    case ErrorKind::kBadRequest:
    case ErrorKind::kValidationFailed:
    case ErrorKind::kJsonParseError:
        return UD_SCHEMA;
    case ErrorKind::kNotSupported:
    case ErrorKind::kNotImplemented:
        return UD_UNSUPPORTED;
    case ErrorKind::kNoSuchTable:
    case ErrorKind::kNoSuchNamespace:
    case ErrorKind::kNotFound:
        return UD_IO;
    case ErrorKind::kServiceUnavailable:
    case ErrorKind::kInternalServerError:
        return UD_TRANSIENT;
    case ErrorKind::kIOError:
        return UD_IO;
    default:
        return UD_CORRUPT;
    }
}

static ud_status mapped_native_error(ud_context* context, const iceberg::Error& error) {
    const ud_status status = map_error(error);
    if (context && status == UD_UNSUPPORTED && !error.message.empty()) {
        const std::string message = "Iceberg native error: " + error.message;
        ud_context_report(context, status,
                          {message.data(), static_cast<uint32_t>(message.size())});
    }
    return status;
}

static void register_file_formats() {
    static std::once_flag once;
    std::call_once(once, [] {
        iceberg::avro::RegisterAll();
        iceberg::parquet::RegisterAll();
    });
}

template <typename T>
static ud_status result_status(const Result<T>& value) {
    return value.has_value() ? UD_OK : map_error(value.error());
}

static std::string copy_string(ud_string value) {
    return value.data && value.length ? std::string(value.data, value.length) : std::string();
}

static bool starts_with(std::string_view value, std::string_view prefix) {
    return value.size() >= prefix.size() && value.substr(0, prefix.size()) == prefix;
}

static std::string uri_decode(std::string_view value) {
    std::string output;
    output.reserve(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '%' && i + 2 < value.size()) {
            const auto digit = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                return -1;
            };
            const int high = digit(value[i + 1]);
            const int low = digit(value[i + 2]);
            if (high >= 0 && low >= 0) {
                output.push_back(static_cast<char>((high << 4) | low));
                i += 2;
                continue;
            }
        }
        output.push_back(value[i]);
    }
    return output;
}

static std::string trim_slash(std::string value) {
    while (value.size() > 1 && value.back() == '/') value.pop_back();
    return value;
}

static std::string url_encode(std::string_view value) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string output;
    output.reserve(value.size());
    for (unsigned char c : value) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~')
            output.push_back(static_cast<char>(c));
        else {
            output.push_back('%');
            output.push_back(hex[c >> 4]);
            output.push_back(hex[c & 15]);
        }
    }
    return output;
}

static std::string uri_path(std::string_view uri) {
    if (starts_with(uri, "file://")) {
        std::string path = uri_decode(uri.substr(7));
#ifdef _WIN32
        /* Standard Windows file URIs spell a drive path as file:///C:/...;
         * the local path must not retain the URI's extra leading slash. */
        if (path.size() >= 3 && path[0] == '/' &&
            ((path[1] >= 'a' && path[1] <= 'z') || (path[1] >= 'A' && path[1] <= 'Z')) &&
            path[2] == ':')
            path.erase(path.begin());
#endif
        return path;
    }
    if (starts_with(uri, "file:")) return uri_decode(uri.substr(5));
    return std::string(uri);
}

static bool has_uri_scheme(std::string_view value) {
    const size_t colon = value.find(':');
    if (colon == std::string_view::npos || colon == 0) return false;
    if (!((value[0] >= 'a' && value[0] <= 'z') ||
          (value[0] >= 'A' && value[0] <= 'Z'))) return false;
    for (size_t i = 1; i < colon; ++i) {
        const char c = value[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.'))
            return false;
    }
    return true;
}

/* Iceberg locations are URIs.  Keep the configured URI scheme, scope each table
 * below the warehouse, and make a local absolute path an explicit file URI so
 * REST consumers (including Trino) select their local filesystem implementation. */
static std::string table_location(std::string location,
                                  const iceberg::TableIdentifier& identifier) {
    const bool windows_path = location.size() >= 3 &&
                              ((location[0] >= 'a' && location[0] <= 'z') ||
                               (location[0] >= 'A' && location[0] <= 'Z')) &&
                              location[1] == ':' && (location[2] == '/' || location[2] == '\\');
    const bool local_path = !has_uri_scheme(location) &&
                            ((!location.empty() && (location.front() == '/' ||
                                                    location.front() == '\\')) || windows_path);
    location = trim_slash(std::move(location));
    if (local_path)
        for (char& c : location) if (c == '\\') c = '/';
    const auto append_segment = [&location](std::string_view segment) {
        if (location.empty() || location.back() != '/') location.push_back('/');
        location += url_encode(segment);
    };
    for (const auto& level : identifier.ns.levels) {
        append_segment(level);
    }
    append_segment(identifier.name);
    if (local_path) {
#ifdef _WIN32
        if (windows_path) return "file:///" + location;
#endif
        return "file://" + location;
    }
    return location;
}

static bool remote_uri(std::string_view uri) {
    return starts_with(uri, "http://") || starts_with(uri, "https://") ||
           starts_with(uri, "s3://") || starts_with(uri, "gs://") ||
           starts_with(uri, "abfs://") || starts_with(uri, "abfss://") ||
           starts_with(uri, "wasbs://");
}

static bool path_has_prefix(std::string_view path, std::string_view root) {
    if (root.empty() || path.size() < root.size() || path.substr(0, root.size()) != root)
        return false;
    if (path.size() == root.size()) return true;
    if (root.back() == '/') return true;
    return path[root.size()] == '/';
}

static std::string clean_idempotency_key(std::string_view prefix, std::string_view value) {
    std::string key(prefix);
    key.push_back('-');
    key.reserve(key.size() + value.size());
    for (unsigned char c : value) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == ':')
            key.push_back(static_cast<char>(c));
        else
            key.push_back('_');
    }
    return key;
}

static bool valid_hex_digest(std::string_view value) {
    if (value.size() != 64) return false;
    for (unsigned char c : value)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
              (c >= 'A' && c <= 'F')))
            return false;
    return true;
}

static constexpr uint64_t kHttpTimeoutMs = 300000;
static constexpr uint64_t kHttpMaximumBytes = 67108864;

class NativeFileIO;

#include "iceberg_input_stream.h"

#include "iceberg_output_stream.h"

#include "iceberg_input_file.h"

#include "iceberg_output_file.h"

#include "iceberg_file_io.h"

static int32_t field_id(const ud_field& field, int32_t fallback);

static int32_t nested_field_id(int32_t parent, uint32_t index) {
    const uint64_t base = static_cast<uint64_t>(parent > 0 ? parent : 1);
    const uint64_t value = base <= (static_cast<uint64_t>(INT32_MAX) - index - 1) / 1000
                               ? base * 1000 + index + 1
                               : static_cast<uint64_t>(INT32_MAX - index);
    return static_cast<int32_t>(value ? value : index + 1);
}

static std::shared_ptr<iceberg::Type> iceberg_type(const ud_field& field,
                                                   int32_t fallback_id = 1) {
    switch (field.type) {
    case UD_BOOL: return iceberg::boolean();
    case UD_INT64: return iceberg::int64();
    case UD_UINT64: return iceberg::int64();
    case UD_FLOAT64: return iceberg::float64();
    case UD_UTF8: return iceberg::string();
    case UD_BINARY: return iceberg::binary();
    case UD_DATE32: return iceberg::date();
    case UD_TIME64: return iceberg::time();
    case UD_TIMESTAMP:
        if (field.timezone_length) return std::shared_ptr<iceberg::Type>(iceberg::timestamp_tz());
        return std::shared_ptr<iceberg::Type>(iceberg::timestamp());
    case UD_DECIMAL128: return iceberg::decimal(field.precision, field.scale);
    case UD_STRUCT: {
        if (!field.children || !field.child_count) return {};
        std::vector<iceberg::SchemaField> children;
        children.reserve(field.child_count);
        for (uint32_t index = 0; index < field.child_count; ++index) {
            const ud_field& child = field.children[index];
            auto type = iceberg_type(child, nested_field_id(fallback_id, index));
            if (!type) return {};
            const int32_t id = field_id(child, nested_field_id(fallback_id, index));
            children.push_back(child.nullable
                                   ? iceberg::SchemaField::MakeOptional(
                                         id, std::string(child.name, child.name_length), type)
                                   : iceberg::SchemaField::MakeRequired(
                                         id, std::string(child.name, child.name_length), type));
        }
        return iceberg::struct_(std::move(children));
    }
    case UD_LIST: {
        if (!field.children || field.child_count != 1) return {};
        const ud_field& child = field.children[0];
        auto type = iceberg_type(child, nested_field_id(fallback_id, 0));
        if (!type) return {};
        const int32_t id = field_id(child, nested_field_id(fallback_id, 0));
        return iceberg::list(child.nullable
                                 ? iceberg::SchemaField::MakeOptional(id, "element", type)
                                 : iceberg::SchemaField::MakeRequired(id, "element", type));
    }
    case UD_MAP: {
        if (!field.children || field.child_count != 1 || field.children[0].type != UD_STRUCT ||
            field.children[0].child_count != 2 || !field.children[0].children)
            return {};
        const ud_field& entry = field.children[0];
        const ud_field& key = entry.children[0];
        const ud_field& value = entry.children[1];
        auto key_type = iceberg_type(key, nested_field_id(fallback_id, 0));
        auto value_type = iceberg_type(value, nested_field_id(fallback_id, 1));
        if (!key_type || !value_type) return {};
        const int32_t key_id = field_id(key, nested_field_id(fallback_id, 0));
        const int32_t value_id = field_id(value, nested_field_id(fallback_id, 1));
        return iceberg::map(iceberg::SchemaField::MakeRequired(key_id, "key", key_type),
                            value.nullable
                                ? iceberg::SchemaField::MakeOptional(value_id, "value", value_type)
                                : iceberg::SchemaField::MakeRequired(value_id, "value", value_type));
    }
    default: return {};
    }
}

static int32_t field_id(const ud_field& field, int32_t fallback) {
    uint64_t value = field.identity.low & UINT64_C(0x7fffffff);
    if (!value) value = static_cast<uint64_t>(fallback);
    return static_cast<int32_t>(value ? value : fallback);
}

static Result<std::shared_ptr<iceberg::Schema>> make_schema(const ud_batch* batch) {
    const uint32_t column_count = batch ? ud_batch_columns(batch) : 0;
    if (!batch || !column_count) return iceberg::InvalidArgument("Iceberg schema is empty");
    std::vector<iceberg::SchemaField> fields;
    fields.reserve(column_count);
    std::vector<int32_t> keys;
    for (uint32_t i = 0; i < column_count; ++i) {
        ud_field field{};
        if (ud_batch_field(batch, i, &field) != UD_OK) return iceberg::InvalidArgument("Invalid UniversalData field");
        auto type = iceberg_type(field);
        if (!type) return iceberg::NotSupported("UniversalData type is not supported by Iceberg");
        const int32_t id = field_id(field, static_cast<int32_t>(i + 1));
        fields.push_back(field.nullable ? iceberg::SchemaField::MakeOptional(id, std::string(field.name, field.name_length), type)
                                        : iceberg::SchemaField::MakeRequired(id, std::string(field.name, field.name_length), type));
        if (field.key_order) keys.push_back(id);
    }
    auto schema = iceberg::Schema::Make(std::move(fields), iceberg::Schema::kInitialSchemaId, std::move(keys));
    if (!schema.has_value()) return std::unexpected(schema.error());
    return std::shared_ptr<iceberg::Schema>(std::move(*schema));
}

static std::string namespace_endpoint(std::string base, const iceberg::TableIdentifier& id) {
    base = trim_slash(std::move(base));
    if (!base.ends_with("/v1")) base += "/v1";
    base += "/namespaces/";
    for (size_t i = 0; i < id.ns.levels.size(); ++i) {
        if (i) base += "%1F";
        base += url_encode(id.ns.levels[i]);
    }
    return base;
}

static std::string table_endpoint(std::string base, const iceberg::TableIdentifier& id) {
    return namespace_endpoint(std::move(base), id) + "/tables/" + url_encode(id.name);
}

static std::string tables_endpoint(std::string base, const iceberg::TableIdentifier& id) {
    return namespace_endpoint(std::move(base), id) + "/tables";
}

class NativeCatalog final : public iceberg::Catalog,
                            public std::enable_shared_from_this<NativeCatalog> {
 public:
    explicit NativeCatalog(const ud_iceberg_options& options)
        : options_(options), base_(copy_string(options.catalog_url)),
          io_(std::make_shared<NativeFileIO>(options)) {}
    std::string_view name() const override { return "UniversalData Native REST"; }

    void SetCommitToken(std::string identity, std::string digest) {
        std::lock_guard<std::mutex> lock(mutex_);
        commit_token_ = identity.empty() ? std::move(digest)
                                         : (digest.empty() ? std::move(identity)
                                                           : identity + ":" + digest);
    }

    void SetBeforeCommitStatus(ud_status status) {
        std::lock_guard<std::mutex> lock(mutex_);
        before_commit_status_ = status;
    }

    std::optional<ud_status> TakeBeforeCommitStatus() {
        std::lock_guard<std::mutex> lock(mutex_);
        auto status = before_commit_status_;
        before_commit_status_.reset();
        return status;
    }

    Result<std::shared_ptr<iceberg::Table>> LoadTable(const iceberg::TableIdentifier& identifier) override {
        auto response = Call("GET", table_endpoint(base_, identifier), {}, "application/json");
        if (!response.has_value()) return std::unexpected(response.error());
        if (response->first == 404) return iceberg::NoSuchTable(identifier.ToString());
        if (response->first == 401 || response->first == 403) return iceberg::NotAuthorized("Iceberg table load is not authorized");
        if (response->first == 429 || response->first >= 500) return iceberg::ServiceUnavailable("Iceberg catalog is unavailable");
        if (response->first < 200 || response->first >= 300) return iceberg::IOError("Iceberg table load failed");
        return FromLoad(identifier, response->second);
    }
    Result<std::shared_ptr<iceberg::Table>> CreateTable(
        const iceberg::TableIdentifier& identifier, const std::shared_ptr<iceberg::Schema>& schema,
        const std::shared_ptr<iceberg::PartitionSpec>& spec, const std::shared_ptr<iceberg::SortOrder>& order,
        const std::string& location, const std::unordered_map<std::string, std::string>& properties) override {
        iceberg::rest::CreateTableRequest request{.name = identifier.name,
            .location = location, .schema = schema, .partition_spec = spec,
            .write_order = order, .stage_create = false, .properties = properties};
        auto body = iceberg::rest::ToJson(request);
        if (!body.has_value()) return std::unexpected(body.error());
        /* The catalog may accept an arbitrary location, but this connector's
         * local FileIO must authorize the table root before metadata can be
         * published beneath it. */
        auto location_authorized = io_->AuthorizeLocation(location, UD_ACCESS_WRITE);
        if (!location_authorized.has_value())
            return std::unexpected(location_authorized.error());
        auto response = Call("POST", tables_endpoint(base_, identifier), body->dump(), "application/json");
        if (!response.has_value()) return std::unexpected(response.error());
        if (response->first == 409) return iceberg::AlreadyExists(identifier.ToString());
        if (response->first == 401 || response->first == 403) return iceberg::NotAuthorized("Iceberg table create is not authorized");
        if (response->first == 429 || response->first >= 500) return iceberg::ServiceUnavailable("Iceberg catalog is unavailable");
        if (response->first < 200 || response->first >= 300) return iceberg::IOError("Iceberg table create failed");
        return FromLoad(identifier, response->second);
    }
    Result<std::shared_ptr<iceberg::Table>> UpdateTable(
        const iceberg::TableIdentifier& identifier,
        const std::vector<std::unique_ptr<iceberg::TableRequirement>>& requirements,
        const std::vector<std::unique_ptr<iceberg::TableUpdate>>& updates) override {
        iceberg::rest::CommitTableRequest request;
        request.identifier = identifier;
        for (const auto& requirement : requirements)
            request.requirements.emplace_back(std::shared_ptr<iceberg::TableRequirement>(requirement->Clone().release()));
        for (const auto& update : updates)
            request.updates.emplace_back(std::shared_ptr<iceberg::TableUpdate>(update->Clone().release()));
        auto body = iceberg::rest::ToJson(request);
        if (!body.has_value()) return std::unexpected(body.error());
        const bool adds_snapshot = std::any_of(
            updates.begin(), updates.end(), [](const auto& update) {
                return update && update->kind() == iceberg::TableUpdate::Kind::kAddSnapshot;
            });
        if (adds_snapshot && options_.before_commit) {
            const ud_status status = options_.before_commit(options_.user);
            if (status != UD_OK) {
                SetBeforeCommitStatus(status);
                return iceberg::IOError("Iceberg commit preflight failed");
            }
        }
        auto response = Call("POST", table_endpoint(base_, identifier), body->dump(), "application/json", true);
        if (!response.has_value()) return std::unexpected(response.error());
        if (response->first == 409) return iceberg::CommitFailed("Iceberg table commit conflict");
        if (response->first == 401 || response->first == 403) return iceberg::NotAuthorized("Iceberg table commit is not authorized");
        if (response->first == 408 || response->first == 429 || response->first >= 500)
            return iceberg::ServiceUnavailable("Iceberg table commit failed transiently");
        if (response->first < 200 || response->first >= 300) return iceberg::IOError("Iceberg table commit failed");
        nlohmann::json json;
        try { json = nlohmann::json::parse(response->second); }
        catch (...) { return iceberg::JsonParseError("Invalid Iceberg commit response"); }
        auto committed = iceberg::rest::CommitTableResponseFromJson(json);
        if (!committed.has_value()) return std::unexpected(committed.error());
        return iceberg::Table::Make(identifier, committed->metadata,
                                    committed->metadata_location, io_, shared_from_this());
    }

    Result<bool> TableExists(const iceberg::TableIdentifier& identifier) const override {
        auto response = Call("HEAD", table_endpoint(base_, identifier), {}, "");
        if (!response.has_value()) return std::unexpected(response.error());
        if (response->first == 404) return false;
        return response->first >= 200 && response->first < 300;
    }

    Status CreateNamespace(const iceberg::Namespace&, const std::unordered_map<std::string, std::string>&) override {
        return iceberg::NotSupported("Native Iceberg namespace management is catalog-owned");
    }
    Result<std::vector<iceberg::Namespace>> ListNamespaces(const iceberg::Namespace&) const override { return iceberg::NotSupported("namespace listing"); }
    Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(const iceberg::Namespace&) const override { return iceberg::NotSupported("namespace properties"); }
    Status DropNamespace(const iceberg::Namespace&) override { return iceberg::NotSupported("namespace drop"); }
    Result<bool> NamespaceExists(const iceberg::Namespace&) const override { return iceberg::NotSupported("namespace exists"); }
    Status UpdateNamespaceProperties(const iceberg::Namespace&, const std::unordered_map<std::string, std::string>&, const std::unordered_set<std::string>&) override { return iceberg::NotSupported("namespace update"); }
    Result<std::vector<iceberg::TableIdentifier>> ListTables(const iceberg::Namespace&) const override { return iceberg::NotSupported("table listing"); }
    Result<std::shared_ptr<iceberg::Transaction>> StageCreateTable(const iceberg::TableIdentifier&, const std::shared_ptr<iceberg::Schema>&, const std::shared_ptr<iceberg::PartitionSpec>&, const std::shared_ptr<iceberg::SortOrder>&, const std::string&, const std::unordered_map<std::string, std::string>&) override { return iceberg::NotSupported("stage create"); }
    Status DropTable(const iceberg::TableIdentifier&, bool) override { return iceberg::NotSupported("table drop"); }
    Status RenameTable(const iceberg::TableIdentifier&, const iceberg::TableIdentifier&) override { return iceberg::NotSupported("table rename"); }
    Result<std::shared_ptr<iceberg::Table>> RegisterTable(const iceberg::TableIdentifier&, const std::string&) override { return iceberg::NotSupported("table register"); }

 private:
    Result<std::pair<int32_t, std::string>> Call(std::string_view method, const std::string& url,
                                                 std::string_view body, std::string_view content_type,
                                                 bool commit = false) const {
        if (!options_.request) return iceberg::NotAuthorized("Native REST request is missing");
        if (options_.check && options_.check(options_.user) != UD_OK)
            return iceberg::IOError("Iceberg operation cancelled");
        std::string idempotency;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            idempotency = commit_token_.empty() ? clean_idempotency_key("ud-catalog", url)
                                                : clean_idempotency_key("ud-commit", commit_token_);
        }
        ud_http_options request{sizeof(request), UD_ABI_VERSION,
                                {method.data(), static_cast<uint32_t>(method.size())},
                                {body.data(), static_cast<uint32_t>(body.size())},
                                {content_type.data(), static_cast<uint32_t>(content_type.size())},
                                {}, {}, {idempotency.data(), static_cast<uint32_t>(idempotency.size())},
                                kHttpTimeoutMs, kHttpMaximumBytes, 1, 0, 0, nullptr, nullptr};
        request.url = {url.data(), static_cast<uint32_t>(url.size())};
        ud_http_response response{};
        const ud_status status = options_.request(options_.user, &request, &response);
        if (status) {
            const bool attempted = response.attempted != 0;
            const int32_t response_code = response.status;
            const bool completed = response.completed != 0;
            if (completed && response_code > 0) {
                std::string payload = copy_string(response.body);
                ud_http_response_clear(&response);
                return std::make_pair(response_code, std::move(payload));
            }
            ud_http_response_clear(&response);
            if (commit && attempted)
                return iceberg::CommitStateUnknown("Iceberg commit outcome is unknown");
            if (response_code == 401 || response_code == 403) return iceberg::NotAuthorized("Iceberg REST authorization failed");
            return iceberg::IOError("Native REST request failed");
        }
        const int32_t code = response.status;
        std::string payload = copy_string(response.body);
        ud_http_response_clear(&response);
        return std::make_pair(code, std::move(payload));
    }
    Result<std::shared_ptr<iceberg::Table>> FromLoad(const iceberg::TableIdentifier& identifier,
                                                     const std::string& payload) {
        nlohmann::json json;
        try { json = nlohmann::json::parse(payload); }
        catch (...) { return iceberg::JsonParseError("Invalid Iceberg load response"); }
        auto loaded = iceberg::rest::LoadTableResultFromJson(json);
        if (!loaded.has_value()) return std::unexpected(loaded.error());
        return iceberg::Table::Make(identifier, loaded->metadata, loaded->metadata_location,
                                    io_, shared_from_this());
    }
    ud_iceberg_options options_;
    std::string base_;
    std::shared_ptr<NativeFileIO> io_;
    mutable std::mutex mutex_;
    std::string commit_token_;
    std::optional<ud_status> before_commit_status_;
};

struct FieldArena {
    std::deque<std::string> names;
    std::deque<std::vector<ud_field>> children;

    const char *Name(std::string_view value) {
        names.emplace_back(value);
        return names.back().c_str();
    }
};

static Result<ud_type> ud_type_for_iceberg(const std::shared_ptr<iceberg::Type>& type) {
    switch (type->type_id()) {
    case iceberg::TypeId::kBoolean: return UD_BOOL;
    case iceberg::TypeId::kInt:
    case iceberg::TypeId::kLong: return UD_INT64;
    case iceberg::TypeId::kFloat:
    case iceberg::TypeId::kDouble: return UD_FLOAT64;
    case iceberg::TypeId::kDecimal:
    case iceberg::TypeId::kDate:
    case iceberg::TypeId::kTime:
    case iceberg::TypeId::kTimestamp:
    case iceberg::TypeId::kTimestampTz:
    case iceberg::TypeId::kTimestampNs:
    case iceberg::TypeId::kTimestampTzNs:
    case iceberg::TypeId::kString: return type->type_id() == iceberg::TypeId::kDecimal ? UD_DECIMAL128 :
                                         type->type_id() == iceberg::TypeId::kDate ? UD_DATE32 :
                                         type->type_id() == iceberg::TypeId::kTime ? UD_TIME64 :
                                         type->type_id() == iceberg::TypeId::kString ? UD_UTF8 : UD_TIMESTAMP;
    case iceberg::TypeId::kBinary:
    case iceberg::TypeId::kFixed:
    case iceberg::TypeId::kUuid: return UD_BINARY;
    case iceberg::TypeId::kStruct: return UD_STRUCT;
    case iceberg::TypeId::kList: return UD_LIST;
    case iceberg::TypeId::kMap: return UD_MAP;
    default: return iceberg::NotSupported("Iceberg type cannot be represented by UniversalData");
    }
}

static Result<ud_field> make_ud_field(const iceberg::SchemaField& field,
                                      const std::unordered_map<int32_t, uint32_t>& key_order,
                                      FieldArena& arena) {
    ud_field output{};
    output.name = arena.Name(field.name());
    output.name_length = static_cast<uint32_t>(field.name().size());
    output.nullable = field.optional() ? 1 : 0;
    output.identity = {0, static_cast<uint64_t>(field.field_id())};
    if (auto key = key_order.find(field.field_id()); key != key_order.end())
        output.key_order = key->second;
    auto type = ud_type_for_iceberg(field.type());
    if (!type.has_value()) return std::unexpected(type.error());
    output.type = *type;
    if (*type == UD_DECIMAL128) {
        auto decimal = std::dynamic_pointer_cast<iceberg::DecimalType>(field.type());
        if (!decimal) return iceberg::InvalidSchema("Invalid Iceberg decimal type");
        output.precision = decimal->precision();
        output.scale = decimal->scale();
    } else if (*type == UD_TIMESTAMP) {
        auto timestamp = std::dynamic_pointer_cast<iceberg::TimestampBase>(field.type());
        if (timestamp && timestamp->is_zoned()) {
            static constexpr char utc[] = "UTC";
            output.timezone = utc;
            output.timezone_length = 3;
        }
    }
    if (field.type()->is_nested()) {
        auto nested = std::dynamic_pointer_cast<iceberg::NestedType>(field.type());
        if (!nested) return iceberg::InvalidSchema("Invalid nested Iceberg type");
        auto& children = arena.children.emplace_back();
        if (field.type()->type_id() == iceberg::TypeId::kMap) {
            /* UniversalData represents a map as one non-null entry struct
             * containing its key and value children, whereas Iceberg exposes
             * those as two direct MapType fields. */
            auto map = std::dynamic_pointer_cast<iceberg::MapType>(field.type());
            if (!map) return iceberg::InvalidSchema("Invalid Iceberg map type");
            auto key = make_ud_field(map->key(), key_order, arena);
            auto value = make_ud_field(map->value(), key_order, arena);
            if (!key.has_value()) return std::unexpected(key.error());
            if (!value.has_value()) return std::unexpected(value.error());
            auto& entry_children = arena.children.emplace_back();
            entry_children.push_back(*key);
            entry_children.push_back(*value);
            ud_field entry{};
            static constexpr char entry_name[] = "entries";
            entry.name = entry_name;
            entry.name_length = sizeof(entry_name) - 1;
            entry.type = UD_STRUCT;
            entry.children = entry_children.data();
            entry.child_count = static_cast<uint32_t>(entry_children.size());
            children.push_back(entry);
        } else {
            children.reserve(nested->fields().size());
            for (const auto& child : nested->fields()) {
                auto converted = make_ud_field(child, key_order, arena);
                if (!converted.has_value()) return std::unexpected(converted.error());
                children.push_back(*converted);
            }
        }
        output.children = children.data();
        output.child_count = static_cast<uint32_t>(children.size());
    }
    return output;
}

static Result<ud_batch *> make_ud_schema(ud_context* context,
                                          const std::shared_ptr<iceberg::Schema>& schema) {
    if (!context || !schema) return iceberg::InvalidArgument("Iceberg schema is missing");
    std::unordered_map<int32_t, uint32_t> key_order;
    uint32_t order = 1;
    for (int32_t id : schema->IdentifierFieldIds()) key_order.emplace(id, order++);
    FieldArena arena;
    std::vector<ud_field> fields;
    fields.reserve(schema->fields().size());
    for (const auto& field : schema->fields()) {
        auto converted = make_ud_field(field, key_order, arena);
        if (!converted.has_value()) return std::unexpected(converted.error());
        fields.push_back(*converted);
    }
    ud_batch* output = nullptr;
    const ud_status status = ud_batch_create(context, fields.data(), static_cast<uint32_t>(fields.size()), 0, &output);
    if (status) return std::unexpected(iceberg::Error{iceberg::ErrorKind::kInvalidSchema,
                                                       "Unable to create UniversalData Iceberg schema"});
    return output;
}

static int32_t field_id_from_ud(const ud_field& field) {
    return field.identity.low > INT32_MAX ? 0 : static_cast<int32_t>(field.identity.low);
}

static Result<std::vector<uint32_t>> map_input_columns(const ud_batch* input,
                                                       const ud_batch* target) {
    if (!input || !target) return iceberg::InvalidArgument("Iceberg batch schema is missing");
    std::vector<uint32_t> columns;
    columns.reserve(ud_batch_columns(target));
    for (uint32_t target_index = 0; target_index < ud_batch_columns(target); ++target_index) {
        ud_field expected{};
        if (ud_batch_field(target, target_index, &expected) != UD_OK)
            return iceberg::InvalidSchema("Invalid target Iceberg field");
        uint32_t found = UINT32_MAX;
        const int32_t expected_id = field_id_from_ud(expected);
        for (uint32_t source_index = 0; source_index < ud_batch_columns(input); ++source_index) {
            ud_field actual{};
            if (ud_batch_field(input, source_index, &actual) != UD_OK)
                return iceberg::InvalidSchema("Invalid input field");
            if (expected_id && expected_id == field_id_from_ud(actual)) {
                found = source_index;
                break;
            }
        }
        if (found == UINT32_MAX) {
            for (uint32_t source_index = 0; source_index < ud_batch_columns(input); ++source_index) {
                ud_field actual{};
                if (ud_batch_field(input, source_index, &actual) != UD_OK)
                    return iceberg::InvalidSchema("Invalid input field");
                if (actual.name_length == expected.name_length &&
                    !std::memcmp(actual.name, expected.name, expected.name_length)) {
                    found = source_index;
                    break;
                }
            }
        }
        if (found == UINT32_MAX) return iceberg::InvalidSchema("Iceberg input is missing a table column");
        columns.push_back(found);
    }
    return columns;
}

static Result<std::vector<int32_t>> configured_key_ids(const ud_batch* requested,
                                                       const ud_batch* table_schema) {
    if (!requested || !table_schema)
        return iceberg::InvalidArgument("Iceberg key schema is missing");
    struct Key {
        uint32_t order;
        int32_t id;
    };
    std::vector<Key> keys;
    for (uint32_t source_index = 0; source_index < ud_batch_columns(requested); ++source_index) {
        ud_field source{};
        if (ud_batch_field(requested, source_index, &source) != UD_OK)
            return iceberg::InvalidSchema("Invalid configured Iceberg key field");
        if (!source.key_order) continue;
        uint32_t target_index = UINT32_MAX;
        const int32_t source_id = field_id_from_ud(source);
        for (uint32_t index = 0; index < ud_batch_columns(table_schema); ++index) {
            ud_field target{};
            if (ud_batch_field(table_schema, index, &target) != UD_OK)
                return iceberg::InvalidSchema("Invalid table key field");
            if (source_id && source_id == field_id_from_ud(target)) {
                target_index = index;
                break;
            }
        }
        if (target_index == UINT32_MAX) {
            for (uint32_t index = 0; index < ud_batch_columns(table_schema); ++index) {
                ud_field target{};
                if (ud_batch_field(table_schema, index, &target) != UD_OK)
                    return iceberg::InvalidSchema("Invalid table key field");
                if (target.name_length == source.name_length &&
                    !std::memcmp(target.name, source.name, source.name_length)) {
                    target_index = index;
                    break;
                }
            }
        }
        if (target_index == UINT32_MAX) return iceberg::InvalidSchema("Configured key is absent from table");
        ud_field target{};
        if (ud_batch_field(table_schema, target_index, &target) != UD_OK)
            return iceberg::InvalidSchema("Invalid table key field");
        const int32_t id = field_id_from_ud(target);
        if (id <= 0) return iceberg::InvalidSchema("Configured key has no Iceberg field ID");
        keys.push_back({source.key_order, id});
    }
    std::sort(keys.begin(), keys.end(), [](const Key& left, const Key& right) {
        return left.order < right.order;
    });
    std::vector<int32_t> ids;
    ids.reserve(keys.size());
    for (const auto& key : keys) ids.push_back(key.id);
    return ids;
}

struct ud_iceberg_table {
    ud_iceberg_options options{};
    std::shared_ptr<NativeCatalog> catalog;
    std::shared_ptr<iceberg::Table> table;
    std::string uuid;
    ud_batch* table_schema_ud = nullptr;
    ud_batch* scan_schema_ud = nullptr;
    int64_t scan_snapshot_id = 0;
    bool scan_open = false;
    std::vector<std::shared_ptr<iceberg::FileScanTask>> scan_tasks;
    size_t scan_task_index = 0;
    uint32_t scan_batch_rows = 65536;
    std::unique_ptr<iceberg::FileScanTaskReader> scan_reader;
    ArrowArrayStream scan_stream{};
    ArrowSchema scan_arrow_schema{};
    bool scan_stream_open = false;
    ud_batch* pending_batch = nullptr;
    uint32_t pending_offset = 0;
    std::vector<int32_t> configured_key_ids;
    std::string commit_identity;
    std::string commit_digest;
};

static Result<ud_batch *> project_to_table(ud_iceberg_table* table, const ud_batch* input) {
    if (!table || !table->table_schema_ud || !input)
        return iceberg::InvalidArgument("Iceberg table batch is missing");
    auto columns = map_input_columns(input, table->table_schema_ud);
    if (!columns.has_value()) return std::unexpected(columns.error());
    std::vector<ud_field> fields(ud_batch_columns(table->table_schema_ud));
    for (uint32_t i = 0; i < fields.size(); ++i)
        if (ud_batch_field(table->table_schema_ud, i, &fields[i]) != UD_OK)
            return iceberg::InvalidSchema("Invalid table schema");
    ud_batch* output = nullptr;
    const ud_status status = ud_batch_project(table->options.memory, input, columns->data(),
                                              fields.data(), static_cast<uint32_t>(fields.size()), &output);
    if (status) return std::unexpected(iceberg::Error{iceberg::ErrorKind::kInvalidSchema,
                                                       "Iceberg input cannot be projected to table schema"});
    return output;
}

static ud_status refresh_table(ud_iceberg_table* table) {
    if (!table || !table->table) return UD_INVALID;
    auto status = table->table->Refresh();
    if (!status.has_value()) return map_error(status.error());
    ud_batch* schema = nullptr;
    auto current_schema = table->table->schema();
    if (!current_schema.has_value()) return map_error(current_schema.error());
    auto converted = make_ud_schema(table->options.memory, *current_schema);
    if (!converted.has_value()) return map_error(converted.error());
    schema = *converted;
    ud_batch_destroy(table->table_schema_ud);
    table->table_schema_ud = schema;
    table->uuid = table->table->uuid();
    return UD_OK;
}

static std::optional<ud_status> snapshot_receipt(const ud_iceberg_table* table,
                                                 std::string_view identity,
                                                 std::string_view digest) {
    if (identity.empty()) return std::nullopt;
    for (const auto& snapshot : table->table->snapshots()) {
        if (!snapshot) continue;
        const auto id = snapshot->summary.find("universaldata.commit.id");
        if (id == snapshot->summary.end() || id->second != identity) continue;
        const auto hash = snapshot->summary.find("universaldata.commit.digest");
        return hash != snapshot->summary.end() && hash->second == digest ? UD_OK : UD_COMMIT_UNKNOWN;
    }
    return std::nullopt;
}

static std::string operation_file_name(const ud_iceberg_table* table, std::string_view suffix,
                                       std::string_view group = {}) {
    std::string token = table->commit_identity;
    if (token.empty()) token = table->commit_digest;
    if (token.empty())
        token = std::to_string(static_cast<unsigned long long>(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    for (char& c : token)
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '-' || c == '_'))
            c = '_';
    std::string name = "ud-" + token;
    if (!group.empty()) name += "-" + std::string(group);
    return name + "-" + std::string(suffix) + ".parquet";
}

static Result<ud_batch *> select_rows(ud_iceberg_table* table, const ud_batch* source,
                                      const std::vector<uint8_t>& selected) {
    if (!source || selected.size() != ud_batch_rows(source))
        return iceberg::InvalidArgument("Iceberg row selection does not match the batch");
    std::vector<ud_field> fields(ud_batch_columns(source));
    for (uint32_t column = 0; column < fields.size(); ++column)
        if (ud_batch_field(source, column, &fields[column]) != UD_OK)
            return iceberg::InvalidSchema("Invalid Iceberg input field");
    ud_batch* output = nullptr;
    auto status = ud_batch_create(table->options.memory, fields.data(),
                                  static_cast<uint32_t>(fields.size()),
                                  static_cast<uint32_t>(std::count(selected.begin(), selected.end(), 1)),
                                  &output);
    if (status) return iceberg::InvalidSchema("Cannot create Iceberg row selection");
    std::vector<ud_value> values(fields.size());
    for (uint32_t row = 0; row < ud_batch_rows(source); ++row) {
        if (!selected[row]) continue;
        for (uint32_t column = 0; column < fields.size(); ++column) {
            status = ud_batch_get(source, row, column, &values[column]);
            if (status) break;
        }
        if (!status) status = ud_batch_append(output, values.data(), static_cast<uint32_t>(values.size()));
        if (!status) {
            ud_row_info info{};
            if (!ud_batch_row_info(source, row, &info))
                status = ud_batch_set_row_info(output, ud_batch_rows(output) - 1, &info);
        }
        if (status) break;
    }
    if (status) {
        ud_batch_destroy(output);
        return std::unexpected(iceberg::Error{iceberg::ErrorKind::kInvalidSchema,
                                               "Cannot copy selected Iceberg rows"});
    }
    status = ud_batch_seal(output);
    if (status) {
        ud_batch_destroy(output);
        return std::unexpected(iceberg::Error{iceberg::ErrorKind::kInvalidSchema,
                                               "Cannot seal selected Iceberg rows"});
    }
    return output;
}

static Result<ud_batch *> select_ordinals(ud_iceberg_table* table, const ud_batch* source,
                                          std::span<const uint32_t> ordinals) {
    if (!table || !source) return iceberg::InvalidArgument("Iceberg row selection is missing");
    std::vector<ud_field> fields(ud_batch_columns(source));
    for (uint32_t column = 0; column < fields.size(); ++column)
        if (ud_batch_field(source, column, &fields[column]) != UD_OK)
            return iceberg::InvalidSchema("Invalid Iceberg input field");
    ud_batch* output = nullptr;
    auto status = ud_batch_create(table->options.memory, fields.data(),
                                  static_cast<uint32_t>(fields.size()),
                                  static_cast<uint32_t>(ordinals.size()), &output);
    if (status) return iceberg::InvalidSchema("Cannot create Iceberg row selection");
    for (uint32_t row : ordinals) {
        status = ud_batch_append_from(output, source, row);
        if (!status) {
            ud_row_info info{};
            if (!ud_batch_row_info(source, row, &info))
                status = ud_batch_set_row_info(output, ud_batch_rows(output) - 1, &info);
        }
        if (status) break;
    }
    if (!status) status = ud_batch_seal(output);
    if (status) {
        ud_batch_destroy(output);
        return std::unexpected(iceberg::Error{iceberg::ErrorKind::kInvalidSchema,
                                               "Cannot copy selected Iceberg rows"});
    }
    return output;
}

static Result<std::vector<uint32_t>> key_columns(const ud_batch* source,
                                                 const ud_batch* table_schema,
                                                 const std::shared_ptr<iceberg::Schema>& schema,
                                                 const std::vector<int32_t>& preferred_key_ids,
                                                 std::vector<int32_t>& key_ids) {
    if (!source || !table_schema || !schema) return iceberg::InvalidArgument("Iceberg key schema is missing");
    key_ids = preferred_key_ids.empty() ? schema->IdentifierFieldIds() : preferred_key_ids;
    if (key_ids.empty()) return iceberg::InvalidSchema("Iceberg update requires identifier fields");
    std::vector<uint32_t> columns;
    std::vector<ud_field> target_fields;
    for (int32_t key_id : key_ids) {
        uint32_t target = UINT32_MAX;
        ud_field expected_field{};
        for (uint32_t column = 0; column < ud_batch_columns(table_schema); ++column) {
            ud_field field{};
            if (ud_batch_field(table_schema, column, &field) != UD_OK) return iceberg::InvalidSchema("Invalid table key field");
            if (field_id_from_ud(field) == key_id) { target = column; expected_field = field; break; }
        }
        if (target == UINT32_MAX) return iceberg::InvalidSchema("Iceberg identifier is absent from table schema");
        uint32_t source_column = UINT32_MAX;
        for (uint32_t column = 0; column < ud_batch_columns(source); ++column) {
            ud_field field{};
            if (ud_batch_field(source, column, &field) != UD_OK) return iceberg::InvalidSchema("Invalid input key field");
            if (field_id_from_ud(field) == key_id) { source_column = column; break; }
        }
        if (source_column == UINT32_MAX) {
            for (uint32_t column = 0; column < ud_batch_columns(source); ++column) {
                ud_field field{};
                if (ud_batch_field(source, column, &field) != UD_OK) return iceberg::InvalidSchema("Invalid input key field");
                if (field.name_length == expected_field.name_length &&
                    !std::memcmp(field.name, expected_field.name, expected_field.name_length)) {
                    source_column = column;
                    break;
                }
            }
        }
        if (source_column == UINT32_MAX) return iceberg::InvalidSchema("Iceberg input is missing an identifier");
        columns.push_back(source_column);
        ud_field target_field{};
        if (ud_batch_field(table_schema, target, &target_field) != UD_OK) return iceberg::InvalidSchema("Invalid table key field");
        target_fields.push_back(target_field);
    }
    return columns;
}

static Result<std::shared_ptr<iceberg::DataFile>> write_data_file(
    ud_iceberg_table* table, const ud_batch* rows, const std::shared_ptr<iceberg::Schema>& schema,
    const std::shared_ptr<iceberg::PartitionSpec>& spec,
    const iceberg::PartitionValues& partition, const std::string& name) {
    if (!rows || ud_batch_rows(rows) == 0) return std::unexpected(iceberg::InvalidArgument("Iceberg data file is empty").error());
    auto location = table->table->location_provider();
    if (!location.has_value()) return std::unexpected(location.error());
    auto path = (*location)->NewDataLocation(*spec, partition, name);
    if (!path.has_value()) return std::unexpected(path.error());
    iceberg::DataWriterOptions options{.path = *path, .schema = schema,
                                       .spec = spec, .partition = partition,
                                       .format = iceberg::FileFormatType::kParquet, .io = table->table->io(),
                                       .sort_order_id = std::nullopt, .properties = {}};
    auto writer = iceberg::DataWriter::Make(options);
    if (!writer.has_value()) return std::unexpected(writer.error());
    ArrowArray array{};
    ArrowSchema arrow_schema{};
    if (ud_batch_export_arrow(rows, &array, &arrow_schema) != UD_OK)
        return std::unexpected(iceberg::InvalidArrowData("Iceberg data batch cannot be exported").error());
    auto write_status = writer.value()->Write(&array);
    if (array.release) array.release(&array);
    if (!write_status.has_value()) {
        if (arrow_schema.release) arrow_schema.release(&arrow_schema);
        return std::unexpected(write_status.error());
    }
    if (arrow_schema.release) arrow_schema.release(&arrow_schema);
    auto closed = writer->get()->Close();
    if (!closed.has_value()) return std::unexpected(closed.error());
    auto metadata = writer->get()->Metadata();
    if (!metadata.has_value()) return std::unexpected(metadata.error());
    if (metadata->data_files.size() != 1) return std::unexpected(iceberg::Invalid("Iceberg writer produced multiple files").error());
    return metadata->data_files.front();
}

static Result<std::shared_ptr<iceberg::DataFile>> write_delete_file(
    ud_iceberg_table* table, const ud_batch* rows, const std::vector<int32_t>& key_ids,
    const std::shared_ptr<iceberg::Schema>& key_schema, const std::shared_ptr<iceberg::PartitionSpec>& spec,
    const iceberg::PartitionValues& partition, const std::string& name) {
    if (!rows || ud_batch_rows(rows) == 0) return std::unexpected(iceberg::InvalidArgument("Iceberg delete file is empty").error());
    auto location = table->table->location_provider();
    if (!location.has_value()) return std::unexpected(location.error());
    auto path = (*location)->NewDataLocation(*spec, partition, name);
    if (!path.has_value()) return std::unexpected(path.error());
    iceberg::EqualityDeleteWriterOptions options{.path = *path, .schema = key_schema,
        .spec = spec, .partition = partition, .format = iceberg::FileFormatType::kParquet,
        .io = table->table->io(), .equality_field_ids = key_ids, .sort_order_id = std::nullopt,
        .properties = {}};
    auto writer = iceberg::EqualityDeleteWriter::Make(options);
    if (!writer.has_value()) return std::unexpected(writer.error());
    ArrowArray array{};
    ArrowSchema arrow_schema{};
    if (ud_batch_export_arrow(rows, &array, &arrow_schema) != UD_OK)
        return std::unexpected(iceberg::InvalidArrowData("Iceberg delete batch cannot be exported").error());
    auto write_status = writer.value()->Write(&array);
    if (array.release) array.release(&array);
    if (!write_status.has_value()) {
        if (arrow_schema.release) arrow_schema.release(&arrow_schema);
        return std::unexpected(write_status.error());
    }
    if (arrow_schema.release) arrow_schema.release(&arrow_schema);
    auto closed = writer->get()->Close();
    if (!closed.has_value()) return std::unexpected(closed.error());
    auto metadata = writer->get()->Metadata();
    if (!metadata.has_value()) return std::unexpected(metadata.error());
    if (metadata->data_files.size() != 1) return std::unexpected(iceberg::Invalid("Iceberg writer produced multiple delete files").error());
    return metadata->data_files.front();
}

static ud_status evolve_missing_columns(ud_iceberg_table* table, const ud_batch* input,
                                        std::string_view identity, std::string_view digest) {
    if (!table || !input) return UD_INVALID;
    auto schema = table->table->schema();
    if (!schema.has_value()) return map_error(schema.error());
    auto update = table->table->NewUpdateSchema();
    if (!update.has_value()) return map_error(update.error());
    bool changed = false;
    for (uint32_t column = 0; column < ud_batch_columns(input); ++column) {
        ud_field field{};
        if (ud_batch_field(input, column, &field) != UD_OK || !field.name || !field.name_length)
            return UD_SCHEMA;
        const int32_t incoming_id = field_id(field, 0);
        if (incoming_id > 0) {
            auto by_id = (*schema)->FindFieldById(incoming_id);
            if (!by_id.has_value()) return map_error(by_id.error());
            if (by_id->has_value()) {
                /* A stable Iceberg ID is the authoritative identity.  A source
                 * alias (for example value -> label) aligns to that field and
                 * must not create a second physical column.  Explicit renames
                 * remain a separate schema-evolution operation. */
                continue;
            }
        }
        auto existing = (*schema)->FindFieldByName(std::string_view(field.name, field.name_length));
        if (!existing.has_value()) return map_error(existing.error());
        if (existing->has_value()) continue;
        auto type = iceberg_type(field);
        if (!type) return UD_UNSUPPORTED;
        update->get()->AddColumn(std::string(field.name, field.name_length), type);
        changed = true;
    }
    if (!changed) return UD_OK;
    table->catalog->SetCommitToken(std::string(identity) + "/schema", std::string(digest));
    auto status = update->get()->Commit();
    if (!status.has_value()) return map_error(status.error());
    return refresh_table(table);
}

static ud_status map_catalog_error(ud_iceberg_table* table, const iceberg::Error& error) {
    if (table && table->catalog) {
        if (auto status = table->catalog->TakeBeforeCommitStatus()) return *status;
    }
    return map_error(error);
}

static ud_status commit_append(ud_iceberg_table* table,
                               const std::vector<std::shared_ptr<iceberg::DataFile>>& data,
                               std::string_view identity, std::string_view digest) {
    auto append = table->table->NewFastAppend();
    if (!append.has_value()) return map_error(append.error());
    if (!identity.empty()) append->get()->Set("universaldata.commit.id", std::string(identity));
    if (!digest.empty()) append->get()->Set("universaldata.commit.digest", std::string(digest));
    for (const auto& file : data) append->get()->AppendFile(file);
    auto status = append->get()->Commit();
    if (!status.has_value()) return map_catalog_error(table, status.error());
    return refresh_table(table);
}

static ud_status commit_delta(ud_iceberg_table* table,
                              const std::vector<std::shared_ptr<iceberg::DataFile>>& data,
                              const std::vector<std::shared_ptr<iceberg::DataFile>>& deletes,
                              std::string_view identity, std::string_view digest,
                              std::optional<int64_t> starting_snapshot = std::nullopt,
                              const std::shared_ptr<iceberg::Expression>& conflict_filter = nullptr,
                              bool validate_delete_files = false) {
    auto delta = table->table->NewRowDelta();
    if (!delta.has_value()) return map_error(delta.error());
    /* Equality-delete locations were resolved against this snapshot. Keep
     * RowDelta's retry/rebase anchored to the same read and validate only
     * files that can contain the selected keys. */
    if (starting_snapshot) delta->get()->ValidateFromSnapshot(*starting_snapshot);
    if (conflict_filter) {
        delta->get()->ConflictDetectionFilter(conflict_filter);
        delta->get()->ValidateNoConflictingDataFiles();
        if (validate_delete_files) delta->get()->ValidateNoConflictingDeleteFiles();
    }
    if (!identity.empty()) delta->get()->Set("universaldata.commit.id", std::string(identity));
    if (!digest.empty()) delta->get()->Set("universaldata.commit.digest", std::string(digest));
    for (const auto& file : data) delta->get()->AddRows(file);
    for (const auto& file : deletes) delta->get()->AddDeletes(file);
    auto status = delta->get()->Commit();
    if (!status.has_value()) return map_catalog_error(table, status.error());
    return refresh_table(table);
}

struct ExistingRowLocation {
    std::shared_ptr<iceberg::PartitionSpec> spec;
    iceberg::PartitionValues partition;
};

static Result<std::unordered_map<std::string, std::shared_ptr<iceberg::PartitionSpec>>>
live_data_specs(ud_iceberg_table* table, int64_t snapshot_id) {
    std::unordered_map<std::string, std::shared_ptr<iceberg::PartitionSpec>> result;
    auto snapshot = table->table->SnapshotById(snapshot_id);
    if (!snapshot.has_value()) return std::unexpected(snapshot.error());
    auto schema = table->table->schema();
    if (!schema.has_value()) return std::unexpected(schema.error());
    iceberg::TableMetadataCache cache(table->table->metadata().get());
    auto specs = cache.GetPartitionSpecsById();
    if (!specs.has_value()) return std::unexpected(specs.error());
    iceberg::SnapshotReader snapshot_reader((*snapshot).get());
    auto manifests = snapshot_reader.DataManifests(table->table->io());
    if (!manifests.has_value()) return std::unexpected(manifests.error());
    for (const auto& manifest : *manifests) {
        auto reader = iceberg::ManifestReader::Make(manifest, table->table->io(), *schema,
                                                     specs->get());
        if (!reader.has_value()) return std::unexpected(reader.error());
        auto entries = reader->get()->LiveEntries();
        if (!entries.has_value()) return std::unexpected(entries.error());
        for (const auto& entry : *entries) {
            if (!entry.data_file || entry.data_file->content != iceberg::DataFile::Content::kData)
                continue;
            auto spec = specs->get().find(manifest.partition_spec_id);
            if (spec == specs->get().end()) return iceberg::InvalidSchema("Iceberg manifest spec is missing");
            result[entry.data_file->file_path] = spec->second;
        }
    }
    return result;
}

static bool same_key(const ud_batch* left, uint32_t left_row,
                     const std::vector<uint32_t>& left_columns,
                     const ud_batch* right, uint32_t right_row,
                     const std::vector<uint32_t>& right_columns) {
    for (size_t i = 0; i < left_columns.size(); ++i) {
        ud_value left_value{}, right_value{};
        if (ud_batch_get(left, left_row, left_columns[i], &left_value) != UD_OK ||
            ud_batch_get(right, right_row, right_columns[i], &right_value) != UD_OK ||
            left_value.is_null || right_value.is_null ||
            ud_value_compare(&left_value, &right_value) != 0)
            return false;
    }
    return true;
}

static Result<uint64_t> hash_key(const ud_batch* batch, uint32_t row,
                                 const std::vector<uint32_t>& columns) {
    if (!batch || row >= ud_batch_rows(batch))
        return iceberg::InvalidArgument("Iceberg key row is invalid");
    uint64_t hash = UD_VALUE_HASH_SEED;
    for (uint32_t column : columns) {
        ud_value value{};
        if (ud_batch_get(batch, row, column, &value) != UD_OK)
            return iceberg::InvalidArrowData("Iceberg key value cannot be read");
        hash = ud_value_hash(&value, hash);
    }
    return hash;
}

static Result<std::shared_ptr<iceberg::Expression>> key_conflict_filter(
    const ud_batch* source, const std::vector<int32_t>& key_ids,
    const std::vector<uint32_t>& source_columns,
    std::span<const uint8_t> selected, const std::shared_ptr<iceberg::Schema>& schema) {
    if (!source || !schema || selected.size() != ud_batch_rows(source))
        return iceberg::InvalidArgument("Iceberg conflict filter input is invalid");
    std::shared_ptr<iceberg::Expression> filter;
    for (size_t key = 0; key < key_ids.size(); ++key) {
        auto field = schema->FindFieldById(key_ids[key]);
        if (!field.has_value()) return std::unexpected(field.error());
        if (!field->has_value()) return iceberg::InvalidSchema("Iceberg conflict key is absent");
        const auto& schema_field = field->value().get();
        std::vector<iceberg::Literal> values;
        values.reserve(ud_batch_rows(source));
        for (uint32_t row = 0; row < ud_batch_rows(source); ++row) {
            if (!selected[row]) continue;
            ud_value value{};
            if (ud_batch_get(source, row, source_columns[key], &value) != UD_OK)
                return iceberg::InvalidArrowData("Iceberg conflict key cannot be read");
            auto literal = ud_iceberg_partition::literal(value, schema_field.type());
            if (!literal.has_value()) return std::unexpected(literal.error());
            values.push_back(std::move(*literal));
        }
        if (values.empty()) continue;
        auto predicate = iceberg::Expressions::In(std::string(schema_field.name()), std::move(values));
        std::shared_ptr<iceberg::Expression> field_filter = predicate;
        filter = filter ? iceberg::Expressions::And(std::move(filter), std::move(field_filter))
                        : std::move(field_filter);
    }
    return filter;
}

static Result<std::vector<std::vector<ExistingRowLocation>>> locate_existing_rows(
    ud_iceberg_table* table, const ud_batch* source, const std::vector<int32_t>& key_ids,
    const std::vector<uint32_t>& source_columns,
    std::span<const uint8_t> selected, int64_t snapshot_id,
    const std::shared_ptr<iceberg::Expression>& key_filter) {
    if (!source || selected.size() != ud_batch_rows(source))
        return iceberg::InvalidArgument("Iceberg old-row selection is invalid");
    std::vector<std::vector<ExistingRowLocation>> locations(ud_batch_rows(source));
    bool pending = false;
    for (uint8_t value : selected) pending = pending || value != 0;
    if (!pending) return locations;
    std::unordered_multimap<uint64_t, uint32_t> candidates;
    uint32_t selected_count = 0;
    for (uint8_t value : selected) selected_count += value != 0;
    candidates.reserve(selected_count);
    for (uint32_t row = 0; row < ud_batch_rows(source); ++row) {
        if (!selected[row]) continue;
        auto hash = hash_key(source, row, source_columns);
        if (!hash.has_value()) return std::unexpected(hash.error());
        candidates.emplace(*hash, row);
    }
    auto specs = live_data_specs(table, snapshot_id);
    if (!specs.has_value()) return std::unexpected(specs.error());
    auto builder = table->table->NewScan();
    if (!builder.has_value()) return std::unexpected(builder.error());
    builder->get()->UseSnapshot(snapshot_id);
    if (key_filter) builder->get()->Filter(key_filter);
    auto scan = builder->get()->Build();
    if (!scan.has_value()) return std::unexpected(scan.error());
    auto tasks = scan->get()->PlanFiles();
    if (!tasks.has_value()) return std::unexpected(tasks.error());
    auto schema = table->table->schema();
    if (!schema.has_value()) return std::unexpected(schema.error());
    auto key_projection = (*schema)->Project(
        std::unordered_set<int32_t>(key_ids.begin(), key_ids.end()));
    if (!key_projection.has_value()) return std::unexpected(key_projection.error());
    auto projected_schema = std::shared_ptr<iceberg::Schema>(std::move(*key_projection));
    const auto projected_fields = projected_schema->fields();
    std::vector<uint32_t> imported_columns, projected_columns;
    std::vector<ud_field> key_fields;
    for (int32_t id : key_ids) {
        auto field_by_id = std::ranges::find_if(projected_fields, [id](const auto& field) {
            return field.field_id() == id;
        });
        if (field_by_id == projected_fields.end()) return iceberg::InvalidSchema("Iceberg projected key is absent");
        imported_columns.push_back(static_cast<uint32_t>(field_by_id - projected_fields.begin()));
        projected_columns.push_back(static_cast<uint32_t>(projected_columns.size()));
        bool found = false;
        for (uint32_t column = 0; column < ud_batch_columns(table->table_schema_ud); ++column) {
            ud_field field{};
            if (ud_batch_field(table->table_schema_ud, column, &field) != UD_OK)
                return iceberg::InvalidSchema("Invalid Iceberg key field");
            if (field_id_from_ud(field) == id) {
                key_fields.push_back(field);
                found = true;
                break;
            }
        }
        if (!found) return iceberg::InvalidSchema("Iceberg scan key is absent");
    }
    iceberg::FileScanTaskReader::Options reader_options{.io = table->table->io(),
        .table_schema = *schema, .schemas = {}, .projected_schema = projected_schema,
        .name_mapping = nullptr, .properties = {}};
    auto reader = iceberg::FileScanTaskReader::Make(std::move(reader_options));
    if (!reader.has_value()) return std::unexpected(reader.error());
    for (const auto& task : *tasks) {
        if (!task || !task->data_file()) continue;
        auto spec = specs->find(task->data_file()->file_path);
        if (spec == specs->end()) return iceberg::InvalidSchema("Iceberg data-file spec is missing");
        auto stream = reader->get()->Open(*task);
        if (!stream.has_value()) return std::unexpected(stream.error());
        ArrowArrayStream arrow = std::move(*stream);
        ArrowSchema arrow_schema{};
        std::vector<uint8_t> matched_source_rows(ud_batch_rows(source), 0);
        if (!arrow.get_schema || arrow.get_schema(&arrow, &arrow_schema)) {
            if (arrow.release) arrow.release(&arrow);
            return iceberg::InvalidArrowData("Iceberg old-row schema cannot be read");
        }
        while (true) {
            ArrowArray array{};
            if (!arrow.get_next || arrow.get_next(&arrow, &array)) {
                if (array.release) array.release(&array);
                if (arrow_schema.release) arrow_schema.release(&arrow_schema);
                if (arrow.release) arrow.release(&arrow);
                return iceberg::InvalidArrowData("Iceberg old-row batch cannot be read");
            }
            if (!array.release) break;
            ud_batch* rows = nullptr;
            ud_status imported = ud_batch_import_arrow(table->options.memory, &array, &arrow_schema, &rows);
            array.release(&array);
            if (imported || !rows || ud_batch_seal(rows) != UD_OK) {
                ud_batch_destroy(rows);
                if (arrow_schema.release) arrow_schema.release(&arrow_schema);
                if (arrow.release) arrow.release(&arrow);
                return iceberg::InvalidArrowData("Iceberg old-row batch import failed");
            }
            ud_batch* projected = nullptr;
            if (ud_batch_project(table->options.memory, rows, imported_columns.data(),
                                 key_fields.data(), static_cast<uint32_t>(key_fields.size()),
                                 &projected) != UD_OK) {
                ud_batch_destroy(rows);
                if (arrow_schema.release) arrow_schema.release(&arrow_schema);
                if (arrow.release) arrow.release(&arrow);
                return iceberg::InvalidSchema("Iceberg old-row projection failed");
            }
            ud_batch_destroy(rows);
            rows = projected;
            for (uint32_t scan_row = 0; scan_row < ud_batch_rows(rows); ++scan_row) {
                auto hash = hash_key(rows, scan_row, projected_columns);
                if (!hash.has_value()) {
                    ud_batch_destroy(rows);
                    if (arrow_schema.release) arrow_schema.release(&arrow_schema);
                    if (arrow.release) arrow.release(&arrow);
                    return std::unexpected(hash.error());
                }
                const auto range = candidates.equal_range(*hash);
                for (auto candidate = range.first; candidate != range.second; ++candidate) {
                    const uint32_t source_row = candidate->second;
                    if (matched_source_rows[source_row] ||
                        !same_key(source, source_row, source_columns, rows, scan_row,
                                  projected_columns))
                        continue;
                    matched_source_rows[source_row] = 1;
                    locations[source_row].push_back(
                        ExistingRowLocation{spec->second, task->data_file()->partition});
                }
            }
            ud_batch_destroy(rows);
        }
        if (arrow_schema.release) arrow_schema.release(&arrow_schema);
        if (arrow.release) arrow.release(&arrow);
    }
    return locations;
}

struct DeletePlan {
    std::vector<ud_iceberg_partition::Group> groups;
    std::vector<uint32_t> source_keys;
    std::shared_ptr<iceberg::Schema> key_schema;
    std::vector<ud_field> key_fields;
    std::shared_ptr<iceberg::Expression> conflict_filter;
    bool validate_delete_files = false;
};

static Result<DeletePlan> plan_delete_files(
    ud_iceberg_table* table, const ud_batch* source, const std::vector<int32_t>& key_ids,
    std::vector<uint32_t> source_columns,
    std::span<const uint8_t> selected, const std::shared_ptr<iceberg::Schema>& table_schema,
    const std::shared_ptr<iceberg::PartitionSpec>& spec, bool validate_delete_files,
    std::optional<int64_t> snapshot_id) {
    DeletePlan plan;
    plan.source_keys = std::move(source_columns);
    auto conflict_filter = key_conflict_filter(source, key_ids, plan.source_keys, selected, table_schema);
    if (!conflict_filter.has_value()) return std::unexpected(conflict_filter.error());
    plan.conflict_filter = std::move(*conflict_filter);
    const auto metadata = table->table->metadata();
    const bool default_unpartitioned_only =
        spec->IsUnpartitioned() && metadata && metadata->partition_specs.size() == 1;
    std::vector<std::vector<ExistingRowLocation>> locations;
    if (snapshot_id && !default_unpartitioned_only) {
        auto found = locate_existing_rows(table, source, key_ids, plan.source_keys,
                                         selected, *snapshot_id, plan.conflict_filter);
        if (!found.has_value()) return std::unexpected(found.error());
        locations = std::move(*found);
    } else {
        locations.resize(ud_batch_rows(source));
    }
    for (int32_t id : key_ids) {
        for (uint32_t column = 0; column < ud_batch_columns(table->table_schema_ud); ++column) {
            ud_field field{};
            if (ud_batch_field(table->table_schema_ud, column, &field) != UD_OK) continue;
            if (field_id_from_ud(field) == id) { plan.key_fields.push_back(field); break; }
        }
    }
    auto key_schema = table_schema->Project(
        std::unordered_set<int32_t>(key_ids.begin(), key_ids.end()));
    if (!key_schema.has_value()) return std::unexpected(key_schema.error());
    plan.key_schema = std::shared_ptr<iceberg::Schema>(std::move(*key_schema));
    plan.validate_delete_files = validate_delete_files;
    std::unordered_map<iceberg::PartitionKey, size_t, iceberg::PartitionKeyHash,
                       iceberg::PartitionKeyEqual> indexes;
    indexes.reserve(ud_batch_rows(source));
    for (uint32_t row = 0; row < ud_batch_rows(source); ++row) {
        if (!selected[row]) continue;
        if (locations[row].empty()) {
            auto values = ud_iceberg_partition::values_for_row(source, row, table_schema, spec);
            if (!values.has_value()) return std::unexpected(values.error());
            locations[row].push_back(ExistingRowLocation{spec, std::move(*values)});
        }
        for (const auto& location : locations[row]) {
            iceberg::PartitionKey key{location.spec->spec_id(), location.partition};
            auto found = indexes.find(key);
            if (found == indexes.end()) {
                ud_iceberg_partition::Group group;
                group.spec = location.spec;
                group.partition = location.partition;
                group.rows.push_back(row);
                indexes.emplace(std::move(key), plan.groups.size());
                plan.groups.push_back(std::move(group));
            } else {
                plan.groups[found->second].rows.push_back(row);
            }
        }
    }
    return plan;
}

static ud_status write_files(ud_iceberg_table* table, const ud_batch* rows,
                             const ud_batch* before, const int32_t* operations,
                             uint32_t operation_count, std::string_view identity,
                             std::string_view digest, bool replay) {
    if (!table || !rows || operation_count != ud_batch_rows(rows)) return UD_INVALID;
    if (ud_batch_rows(rows) == 0) return UD_OK;
    auto spec = table->table->spec();
    if (!spec.has_value()) return map_error(spec.error());
    /* Stage filenames from the same operation token before any file is
     * created.  This makes upload-before-commit recovery address the exact
     * staged data/delete paths instead of a fresh clock-based name. */
    table->commit_identity.assign(identity);
    table->commit_digest.assign(digest);
    auto evolved = evolve_missing_columns(table, rows, identity, digest);
    if (evolved) return evolved;
    auto table_schema = table->table->schema();
    if (!table_schema.has_value()) return map_error(table_schema.error());
    std::optional<int64_t> starting_snapshot_id;
    const auto current_snapshot_id = table->table->metadata()->current_snapshot_id;
    if (current_snapshot_id != iceberg::kInvalidSnapshotId) starting_snapshot_id = current_snapshot_id;
    std::vector<uint8_t> insert_rows(ud_batch_rows(rows), 0), delete_rows(ud_batch_rows(rows), 0);
    bool needs_keys = false, validate_delete_files = false;
    for (uint32_t row = 0; row < operation_count; ++row) {
        switch (operations ? operations[row] : UD_CHANGE_INSERT) {
        case UD_CHANGE_INSERT: insert_rows[row] = 1; break;
        case UD_CHANGE_UPDATE:
            insert_rows[row] = 1; delete_rows[row] = 1; needs_keys = true;
            validate_delete_files = true; break;
        case UD_CHANGE_UPSERT:
            insert_rows[row] = 1; delete_rows[row] = 1; needs_keys = true;
            validate_delete_files = true; break;
        case UD_CHANGE_DELETE: delete_rows[row] = 1; needs_keys = true; break;
        case UD_CHANGE_TRUNCATE: return UD_UNSUPPORTED;
        default: return UD_SCHEMA;
        }
    }
    if (before && ud_batch_rows(before) != operation_count) return UD_SCHEMA;
    std::vector<int32_t> key_ids;
    std::vector<uint32_t> key_ordinals;
    if (operations) {
        auto keys = key_columns(rows, table->table_schema_ud, *table_schema,
                                table->configured_key_ids, key_ids);
        if (!keys.has_value()) return map_error(keys.error());
        key_ordinals = std::move(*keys);
        ud_status folded = ud_iceberg_select_current_rows(rows, before, key_ordinals,
                                                          insert_rows, delete_rows);
        if (folded) return folded;
    }
    auto projected = project_to_table(table, rows);
    if (!projected.has_value()) return map_error(projected.error());
    ud_batch* insert_batch = nullptr;
    auto selected = select_rows(table, *projected, insert_rows);
    if (!selected.has_value()) { ud_batch_destroy(*projected); return map_error(selected.error()); }
    insert_batch = *selected;
    ud_batch_destroy(*projected);
    std::vector<uint8_t> all_insert_rows(ud_batch_rows(insert_batch), 1);
    auto insert_groups = ud_iceberg_partition::group_rows(insert_batch, *table_schema, *spec,
                                                           all_insert_rows);
    if (!insert_groups.has_value()) {
        ud_batch_destroy(insert_batch);
        return mapped_native_error(table->options.memory, insert_groups.error());
    }

    const ud_batch* delete_source = before ? before : rows;
    DeletePlan delete_plan;
    if (needs_keys) {
        if (before) {
            std::vector<int32_t> before_ids;
            auto keys = key_columns(before, table->table_schema_ud, *table_schema, key_ids, before_ids);
            if (!keys.has_value()) { ud_batch_destroy(insert_batch); return map_error(keys.error()); }
            key_ordinals = std::move(*keys);
        }
        auto planned = plan_delete_files(table, delete_source, key_ids, std::move(key_ordinals), delete_rows,
                                         *table_schema, *spec, validate_delete_files, starting_snapshot_id);
        if (!planned.has_value()) {
            ud_batch_destroy(insert_batch);
            return mapped_native_error(table->options.memory, planned.error());
        }
        delete_plan = std::move(*planned);
    }

    auto location = table->table->location_provider();
    if (!location.has_value()) {
        ud_batch_destroy(insert_batch);
        return map_error(location.error());
    }
    const auto staged_path = [&](const ud_iceberg_partition::Group& group,
                                 std::string_view suffix) -> Result<std::string> {
        auto token = ud_iceberg_partition::group_token(group);
        if (!token.has_value()) return std::unexpected(token.error());
        const std::string group_name = group.spec->IsUnpartitioned() ? std::string() : *token;
        return (*location)->NewDataLocation(*group.spec, group.partition,
                                             operation_file_name(table, suffix, group_name));
    };
    if (replay) {
        const auto remove_staged = [&](const ud_iceberg_partition::Group& group,
                                       std::string_view suffix) -> ud_status {
            auto path = staged_path(group, suffix);
            if (!path.has_value()) return map_error(path.error());
            auto status = table->table->io()->DeleteFile(*path);
            return status.has_value() ? UD_OK : map_error(status.error());
        };
        for (const auto& group : *insert_groups) {
            auto status = remove_staged(group, "data");
            if (status) { ud_batch_destroy(insert_batch); return status; }
        }
        for (const auto& group : delete_plan.groups) {
            auto status = remove_staged(group, "delete");
            if (status) { ud_batch_destroy(insert_batch); return status; }
        }
    }

    std::vector<std::shared_ptr<iceberg::DataFile>> data_files, delete_files;
    for (const auto& group : *insert_groups) {
        ud_batch* grouped = nullptr;
        const bool reuse_insert_batch = group.spec->IsUnpartitioned() &&
            group.rows.size() == ud_batch_rows(insert_batch);
        if (reuse_insert_batch) {
            if (ud_batch_retain(insert_batch) != UD_OK) {
                ud_batch_destroy(insert_batch);
                return UD_RESOURCE;
            }
            grouped = insert_batch;
        } else {
            auto selected = select_ordinals(table, insert_batch, group.rows);
            if (!selected.has_value()) {
                ud_batch_destroy(insert_batch);
                return map_error(selected.error());
            }
            grouped = *selected;
        }
        auto token = ud_iceberg_partition::group_token(group);
        if (!token.has_value()) {
            ud_batch_destroy(grouped);
            ud_batch_destroy(insert_batch);
            return map_error(token.error());
        }
        const std::string group_name = group.spec->IsUnpartitioned() ? std::string() : *token;
        auto data = write_data_file(table, grouped, *table_schema, group.spec, group.partition,
                                    operation_file_name(table, "data", group_name));
        ud_batch_destroy(grouped);
        if (!data.has_value()) {
            ud_batch_destroy(insert_batch);
            return mapped_native_error(table->options.memory, data.error());
        }
        data_files.push_back(*data);
    }
    for (const auto& group : delete_plan.groups) {
        auto selected_source = select_ordinals(table, delete_source, group.rows);
        if (!selected_source.has_value()) {
            ud_batch_destroy(insert_batch);
            return map_error(selected_source.error());
        }
        ud_batch* key_batch = nullptr;
        auto key_status = ud_batch_project(table->options.memory, *selected_source,
                                           delete_plan.source_keys.data(), delete_plan.key_fields.data(),
                                           static_cast<uint32_t>(delete_plan.key_fields.size()), &key_batch);
        ud_batch_destroy(*selected_source);
        if (key_status) {
            ud_batch_destroy(insert_batch);
            return key_status;
        }
        auto token = ud_iceberg_partition::group_token(group);
        if (!token.has_value()) {
            ud_batch_destroy(key_batch);
            ud_batch_destroy(insert_batch);
            return map_error(token.error());
        }
        const std::string group_name = group.spec->IsUnpartitioned() ? std::string() : *token;
        auto deletes = write_delete_file(table, key_batch, key_ids, delete_plan.key_schema, group.spec,
                                          group.partition,
                                          operation_file_name(table, "delete", group_name));
        ud_batch_destroy(key_batch);
        if (!deletes.has_value()) {
            ud_batch_destroy(insert_batch);
            return mapped_native_error(table->options.memory, deletes.error());
        }
        delete_files.push_back(*deletes);
    }
    ud_batch_destroy(insert_batch);
    table->catalog->SetCommitToken(table->commit_identity, table->commit_digest);
    ud_status result = !data_files.empty() && !delete_files.empty()
                       ? commit_delta(table, data_files, delete_files, identity, digest,
                                      starting_snapshot_id, delete_plan.conflict_filter,
                                      delete_plan.validate_delete_files)
                       : !data_files.empty() ? commit_append(table, data_files, identity, digest)
                       : !delete_files.empty() ? commit_delta(table, {}, delete_files, identity, digest,
                                                              starting_snapshot_id,
                                                              delete_plan.conflict_filter,
                                                              delete_plan.validate_delete_files)
                                     : UD_OK;
    if (result == UD_OK) table->catalog->SetCommitToken({}, {});
    return result;
}

static void release_scan_task(ud_iceberg_table* table) {
    if (!table) return;
    if (table->scan_stream_open && table->scan_stream.release)
        table->scan_stream.release(&table->scan_stream);
    table->scan_stream = {};
    table->scan_stream_open = false;
    if (table->scan_arrow_schema.release)
        table->scan_arrow_schema.release(&table->scan_arrow_schema);
    table->scan_arrow_schema = {};
}

static ud_status close_scan(ud_iceberg_table* table) {
    if (!table) return UD_INVALID;
    release_scan_task(table);
    table->scan_reader.reset();
    table->scan_tasks.clear();
    table->scan_task_index = 0;
    table->scan_snapshot_id = 0;
    table->scan_batch_rows = 65536;
    ud_batch_destroy(table->pending_batch);
    table->pending_batch = nullptr;
    table->pending_offset = 0;
    ud_batch_destroy(table->scan_schema_ud);
    table->scan_schema_ud = nullptr;
    table->scan_open = false;
    return UD_OK;
}

static Result<ud_batch*> import_scan_batch(ud_iceberg_table* table, ArrowArray* array) {
    if (!table || !array || !table->scan_arrow_schema.release)
        return iceberg::InvalidArrowData("Iceberg scan array has no schema");
    ud_batch* imported = nullptr;
    ud_status status = ud_batch_import_arrow(table->options.memory, array,
                                             &table->scan_arrow_schema, &imported);
    if (status) return std::unexpected(iceberg::InvalidArrowData("Iceberg scan array import failed").error());
    status = ud_batch_seal(imported);
    if (status) {
        ud_batch_destroy(imported);
        return std::unexpected(iceberg::InvalidArrowData("Iceberg scan batch cannot be sealed").error());
    }
    std::vector<uint32_t> columns(ud_batch_columns(table->scan_schema_ud));
    std::vector<ud_field> fields(columns.size());
    if (columns.size() != ud_batch_columns(imported)) {
        ud_batch_destroy(imported);
        return iceberg::InvalidSchema("Iceberg scan schema changed during read");
    }
    for (uint32_t i = 0; i < columns.size(); ++i) {
        columns[i] = i;
        if (ud_batch_field(table->scan_schema_ud, i, &fields[i]) != UD_OK) {
            ud_batch_destroy(imported);
            return iceberg::InvalidSchema("Invalid Iceberg scan schema");
        }
    }
    ud_batch* projected = nullptr;
    const ud_status project_status = ud_batch_project(table->options.memory, imported, columns.data(),
                                                      fields.data(), static_cast<uint32_t>(fields.size()), &projected);
    ud_batch_destroy(imported);
    if (project_status) return std::unexpected(iceberg::InvalidSchema("Iceberg scan projection failed").error());
    return projected;
}

extern "C" ud_status ud_iceberg_table_open(const ud_iceberg_options* options,
                                              const ud_batch* schema_batch,
                                              ud_iceberg_table** out) {
    if (!options || !out || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION || !options->memory || !options->catalog_url.length ||
        !options->namespace_name.length || !options->table_name.length ||
        !options->storage_root.length || !options->request || !options->authorize_file)
        return UD_INVALID;
    *out = nullptr;
    auto table = std::unique_ptr<ud_iceberg_table>(new (std::nothrow) ud_iceberg_table());
    if (!table) return UD_RESOURCE;
    table->options = *options;
    table->catalog = std::make_shared<NativeCatalog>(*options);
    iceberg::TableIdentifier identifier{.ns = iceberg::Namespace{{copy_string(options->namespace_name)}},
                                        .name = copy_string(options->table_name)};
    auto loaded = table->catalog->LoadTable(identifier);
    if (!loaded.has_value() && loaded.error().kind == ErrorKind::kNoSuchTable) {
        if (!schema_batch) return UD_IO;
        auto schema = make_schema(schema_batch);
        if (!schema.has_value()) return map_error(schema.error());
        std::string location = copy_string(options->catalog_warehouse);
        if (location.empty()) location = copy_string(options->storage_root);
        location = table_location(std::move(location), identifier);
        auto created = table->catalog->CreateTable(identifier, *schema,
                                                    iceberg::PartitionSpec::Unpartitioned(),
                                                    iceberg::SortOrder::Unsorted(), location, {});
        if (!created.has_value()) {
            if (created.error().kind != ErrorKind::kAlreadyExists) return map_error(created.error());
            auto raced = table->catalog->LoadTable(identifier);
            if (!raced.has_value()) return map_error(raced.error());
            table->table = std::move(*raced);
        } else {
            table->table = std::move(*created);
        }
    } else if (!loaded.has_value()) {
        return map_error(loaded.error());
    } else {
        table->table = std::move(*loaded);
    }
    register_file_formats();
    auto schema = table->table->schema();
    if (!schema.has_value()) return map_error(schema.error());
    auto ud_schema = make_ud_schema(options->memory, *schema);
    if (!ud_schema.has_value()) return map_error(ud_schema.error());
    table->table_schema_ud = *ud_schema;
    if (schema_batch) {
        auto keys = configured_key_ids(schema_batch, table->table_schema_ud);
        if (!keys.has_value()) return map_error(keys.error());
        table->configured_key_ids = std::move(*keys);
    }
    table->uuid = table->table->uuid();
    *out = table.release();
    return UD_OK;
}

extern "C" ud_string ud_iceberg_table_uuid(const ud_iceberg_table* table) {
    return table ? ud_string{table->uuid.data(), static_cast<uint32_t>(table->uuid.size())} : ud_string{nullptr, 0};
}

extern "C" ud_status ud_iceberg_table_append(ud_iceberg_table* table, const ud_batch* batch,
                                                ud_string identity, ud_string digest,
                                                int replay, int committed) {
    if (!table || !batch || ud_batch_rows(batch) == 0) return UD_INVALID;
    const std::string identity_text = copy_string(identity);
    const std::string digest_text = copy_string(digest);
    if (!digest_text.empty() && !valid_hex_digest(digest_text)) return UD_INVALID;
    auto refreshed = refresh_table(table);
    if (refreshed) return refreshed;
    if (auto receipt = snapshot_receipt(table, identity_text, digest_text)) return *receipt;
    if (committed) return UD_COMMIT_UNKNOWN;
    return write_files(table, batch, nullptr, nullptr, ud_batch_rows(batch), identity_text,
                       digest_text, replay != 0);
}

extern "C" ud_status ud_iceberg_table_apply(ud_iceberg_table* table, const ud_batch* batch,
                                               const ud_batch* before, const int32_t* operations,
                                               uint32_t operation_count, ud_string identity,
                                               ud_string digest, int replay, int committed) {
    if (!table || !batch || !operations || operation_count != ud_batch_rows(batch)) return UD_INVALID;
    const std::string identity_text = copy_string(identity);
    const std::string digest_text = copy_string(digest);
    if (!digest_text.empty() && !valid_hex_digest(digest_text)) return UD_INVALID;
    auto refreshed = refresh_table(table);
    if (refreshed) return refreshed;
    if (auto receipt = snapshot_receipt(table, identity_text, digest_text)) return *receipt;
    if (committed) return UD_COMMIT_UNKNOWN;
    return write_files(table, batch, before, operations, operation_count, identity_text,
                       digest_text, replay != 0);
}

extern "C" ud_status ud_iceberg_table_read_open(ud_iceberg_table* table,
                                                   const ud_iceberg_read_options* options,
                                                   ud_batch** schema) {
    if (!table || !schema) return UD_INVALID;
    *schema = nullptr;
    close_scan(table);
    auto refreshed = refresh_table(table);
    if (refreshed) return refreshed;
    auto current_schema = table->table->schema();
    if (!current_schema.has_value()) return map_error(current_schema.error());
    auto scan_schema = make_ud_schema(table->options.memory, *current_schema);
    if (!scan_schema.has_value()) return map_error(scan_schema.error());
    table->scan_schema_ud = *scan_schema;
    ud_iceberg_read_options read_options{sizeof(read_options), UD_ABI_VERSION, 0, 65536};
    if (options) {
        if (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION) {
            close_scan(table);
            return UD_INVALID;
        }
        read_options.snapshot_id = options->snapshot_id;
        read_options.batch_rows = options->batch_rows ? options->batch_rows : 65536;
    }
    if (read_options.snapshot_id) {
        auto snapshot = table->table->SnapshotById(read_options.snapshot_id);
        if (!snapshot.has_value()) { close_scan(table); return map_error(snapshot.error()); }
        table->scan_snapshot_id = read_options.snapshot_id;
    } else {
        auto snapshot = table->table->current_snapshot();
        if (snapshot.has_value() && *snapshot) table->scan_snapshot_id = (*snapshot)->snapshot_id;
    }
    auto builder = table->table->NewScan();
    if (!builder.has_value()) { close_scan(table); return map_error(builder.error()); }
    if (table->scan_snapshot_id) builder->get()->UseSnapshot(table->scan_snapshot_id);
    auto scan = builder->get()->Build();
    if (!scan.has_value()) { close_scan(table); return map_error(scan.error()); }
    auto tasks = scan->get()->PlanFiles();
    if (!tasks.has_value()) { close_scan(table); return map_error(tasks.error()); }
    table->scan_tasks = std::move(*tasks);
    auto table_schema = table->table->schema();
    if (!table_schema.has_value()) { close_scan(table); return map_error(table_schema.error()); }
    iceberg::FileScanTaskReader::Options reader_options{.io = table->table->io(),
        .table_schema = *table_schema, .schemas = {}, .projected_schema = *table_schema,
        .name_mapping = nullptr, .properties = {}};
    auto reader = iceberg::FileScanTaskReader::Make(std::move(reader_options));
    if (!reader.has_value()) { close_scan(table); return map_error(reader.error()); }
    table->scan_reader = std::move(*reader);
    table->scan_batch_rows = read_options.batch_rows;
    table->scan_open = true;
    auto output_schema = make_ud_schema(table->options.memory, *table_schema);
    if (!output_schema.has_value()) { close_scan(table); return map_error(output_schema.error()); }
    *schema = *output_schema;
    return UD_OK;
}

extern "C" ud_status ud_iceberg_table_read_next(ud_iceberg_table* table, ud_batch** batch) {
    if (!table || !batch) return UD_INVALID;
    *batch = nullptr;
    if (!table->scan_open) return UD_END;
    const uint32_t batch_rows = table->scan_batch_rows;
    while (true) {
        if (table->pending_batch && table->pending_offset < ud_batch_rows(table->pending_batch)) {
            const uint32_t available = ud_batch_rows(table->pending_batch) - table->pending_offset;
            const uint32_t count = std::min(batch_rows, available);
            const ud_status status = ud_batch_slice(table->options.memory, table->pending_batch,
                                                    table->pending_offset, count, batch);
            if (status) return status;
            table->pending_offset += count;
            if (table->pending_offset == ud_batch_rows(table->pending_batch)) {
                ud_batch_destroy(table->pending_batch);
                table->pending_batch = nullptr;
                table->pending_offset = 0;
            }
            return UD_OK;
        }
        if (table->pending_batch) {
            ud_batch_destroy(table->pending_batch);
            table->pending_batch = nullptr;
            table->pending_offset = 0;
        }
        if (!table->scan_stream_open) {
            if (table->scan_task_index >= table->scan_tasks.size()) return UD_END;
            auto stream = table->scan_reader->Open(*table->scan_tasks[table->scan_task_index++]);
            if (!stream.has_value()) return map_error(stream.error());
            table->scan_stream = std::move(*stream);
            table->scan_stream_open = true;
            if (!table->scan_stream.get_schema || table->scan_stream.get_schema(&table->scan_stream, &table->scan_arrow_schema)) {
                release_scan_task(table);
                return UD_IO;
            }
        }
        ArrowArray array{};
        if (!table->scan_stream.get_next || table->scan_stream.get_next(&table->scan_stream, &array)) {
            release_scan_task(table);
            return UD_IO;
        }
        if (!array.release) {
            release_scan_task(table);
            continue;
        }
        auto imported = import_scan_batch(table, &array);
        array.release(&array);
        if (!imported.has_value()) return map_error(imported.error());
        if (ud_batch_rows(*imported) == 0) {
            ud_batch_destroy(*imported);
            continue;
        }
        table->pending_batch = *imported;
        table->pending_offset = 0;
    }
}

extern "C" ud_status ud_iceberg_table_snapshot_id(const ud_iceberg_table* table,
                                                     int64_t* snapshot_id) {
    if (!table || !snapshot_id) return UD_INVALID;
    *snapshot_id = table->scan_snapshot_id;
    return UD_OK;
}

extern "C" ud_status ud_iceberg_table_read_close(ud_iceberg_table* table) {
    return close_scan(table);
}
extern "C" void ud_iceberg_table_close(ud_iceberg_table* table) {
    if (!table) return;
    close_scan(table);
    ud_batch_destroy(table->table_schema_ud);
    delete table;
}
