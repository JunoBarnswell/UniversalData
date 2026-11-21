#include "s3_object.h"

#include "network.h"

#include <ctype.h>
#include <errno.h>
#ifdef _WIN32
#include <io.h>
#endif

enum { S3_TIMEOUT_MS = 300000, S3_RANGE_MAX = 8 * 1024 * 1024,
       S3_MULTIPART_PART = 8 * 1024 * 1024, S3_RESPONSE_MAX = 64 * 1024 * 1024 };

typedef struct s3_parts {
    ud_string bucket;
    ud_string key;
} s3_parts;

static int prefix(ud_string value, const char *text) {
    const size_t length = strlen(text);
    return value.length >= length && !memcmp(value.data, text, length);
}

static int control_or_space(ud_string value) {
    for (uint32_t i = 0; i < value.length; ++i)
        if ((unsigned char)value.data[i] < 32 || (unsigned char)value.data[i] == 127 ||
            value.data[i] == '\r' || value.data[i] == '\n')
            return 1;
    return 0;
}

static ud_status validate_key(ud_string key, int allow_empty) {
    if (!key.length)
        return allow_empty ? UD_OK : UD_SCHEMA;
    if (key.length > 32768 || control_or_space(key) || memchr(key.data, '\\', key.length) ||
        memchr(key.data, '?', key.length) || memchr(key.data, '#', key.length))
        return UD_SCHEMA;
    uint32_t begin = 0;
    for (uint32_t i = 0; i <= key.length; ++i) {
        if (i < key.length && key.data[i] != '/')
            continue;
        const uint32_t count = i - begin;
        if (!count || (count == 1 && key.data[begin] == '.') ||
            (count == 2 && key.data[begin] == '.' && key.data[begin + 1] == '.'))
            return UD_SCHEMA;
        begin = i + 1;
    }
    return UD_OK;
}

static ud_status split_s3(ud_string location, s3_parts *parts, int allow_empty) {
    if (!parts || !prefix(location, "s3://") || control_or_space(location))
        return UD_SCHEMA;
    const char *start = location.data + 5;
    const char *end = location.data + location.length;
    const char *slash = memchr(start, '/', (size_t)(end - start));
    const char *bucket_end = slash ? slash : end;
    if (bucket_end == start || (size_t)(bucket_end - start) > 1024 ||
        memchr(start, '?', (size_t)(bucket_end - start)) ||
        memchr(start, '#', (size_t)(bucket_end - start)))
        return UD_SCHEMA;
    parts->bucket = (ud_string){start, (uint32_t)(bucket_end - start)};
    parts->key = slash ? (ud_string){slash + 1, (uint32_t)(end - slash - 1)} : (ud_string){0};
    UD_TRY(validate_key(parts->key, allow_empty));
    return UD_OK;
}

static ud_status split_resource(ud_string resource, s3_parts *parts, ud_string *origin) {
    if (!parts || !origin || !prefix(resource, "https://") || control_or_space(resource))
        return UD_SCHEMA;
    const char *begin = resource.data + 8;
    const char *end = resource.data + resource.length;
    const char *slash = memchr(begin, '/', (size_t)(end - begin));
    const char *authority_end = slash ? slash : end;
    if (authority_end == begin || memchr(begin, '@', (size_t)(authority_end - begin)) ||
        memchr(begin, '?', (size_t)(authority_end - begin)) ||
        memchr(begin, '#', (size_t)(authority_end - begin)))
        return UD_SCHEMA;
    const char *path = slash ? slash + 1 : end;
    const char *bucket_end = memchr(path, '/', (size_t)(end - path));
    if (!bucket_end)
        bucket_end = end;
    if (bucket_end == path)
        return UD_SCHEMA;
    parts->bucket = (ud_string){path, (uint32_t)(bucket_end - path)};
    parts->key = bucket_end < end ? (ud_string){bucket_end + 1,
                                                (uint32_t)(end - bucket_end - 1)}
                                  : (ud_string){0};
    if (parts->key.length && parts->key.data[parts->key.length - 1] == '/')
        parts->key.length--;
    UD_TRY(validate_key(parts->key, 1));
    origin->data = resource.data;
    origin->length = (uint32_t)(authority_end - resource.data);
    return UD_OK;
}

static int same(ud_string a, ud_string b) {
    return a.length == b.length && !memcmp(a.data, b.data, a.length);
}

static ud_status resource_for(ud_connection_lease *lease, s3_parts *root, ud_string *origin) {
    ud_string resource;
    UD_TRY(ud_network_connection_resource(lease, &resource));
    return split_resource(resource, root, origin);
}

static ud_status ensure_inside(const s3_parts *root, const s3_parts *object) {
    if (!same(root->bucket, object->bucket))
        return UD_AUTH;
    if (!root->key.length)
        return UD_OK;
    if (!same(root->key, object->key) &&
        (object->key.length <= root->key.length ||
         object->key.data[root->key.length] != '/' ||
         memcmp(root->key.data, object->key.data, root->key.length)))
        return UD_AUTH;
    return UD_OK;
}

static ud_status object_url(ud_context *memory, ud_connection_lease *lease,
                            ud_string location, char **url) {
    *url = NULL;
    s3_parts root, object;
    ud_string origin;
    UD_TRY(resource_for(lease, &root, &origin));
    UD_TRY(split_s3(location, &object, 0));
    UD_TRY(ensure_inside(&root, &object));
    ud_bytes text = {.ctx = memory, .limit = 65536};
    UD_TRY(ud_bytes_put(&text, origin.data, origin.length));
    UD_TRY(ud_bytes_put(&text, "/", 1));
    UD_TRY(ud_bytes_put(&text, object.bucket.data, object.bucket.length));
    if (object.key.length) {
        UD_TRY(ud_bytes_put(&text, "/", 1));
        UD_TRY(ud_bytes_put(&text, object.key.data, object.key.length));
    }
    UD_TRY(ud_bytes_put(&text, "", 1));
    *url = (char *)text.data;
    return UD_OK;
}

ud_status ud_s3_object_root(ud_context *memory, ud_connection_lease *lease, ud_string *root) {
    if (!memory || !lease || !root)
        return UD_INVALID;
    *root = (ud_string){0};
    s3_parts parsed;
    ud_string origin;
    UD_TRY(resource_for(lease, &parsed, &origin));
    ud_bytes value = {.ctx = memory, .limit = 65536};
    ud_status status = ud_bytes_put(&value, "s3://", 5);
    if (!status)
        status = ud_bytes_put(&value, parsed.bucket.data, parsed.bucket.length);
    if (!status && parsed.key.length) {
        status = ud_bytes_put(&value, "/", 1);
        if (!status)
            status = ud_bytes_put(&value, parsed.key.data, parsed.key.length);
    }
    if (!status)
        status = ud_bytes_put(&value, "", 1);
    if (status) {
        ud_bytes_clear(&value);
        return status;
    }
    root->data = (const char *)value.data;
    root->length = (uint32_t)value.size - 1;
    return UD_OK;
}

ud_status ud_s3_object_authorize(const ud_component_context *context,
                                 ud_connection_lease *lease, ud_string location,
                                 int32_t action) {
    if (!context || !lease || !location.length)
        return UD_INVALID;
    char *url = NULL;
    ud_status status = object_url(context->memory, lease, location, &url);
    if (!status)
        status = ud_connection_lease_authorize(lease, action);
    if (!status)
        status = ud_connection_lease_authorize_uri(lease,
                                                   (ud_string){url, (uint32_t)strlen(url)});
    ud_free(url);
    return status;
}

static ud_status map_response(const ud_network_s3_result *response, ud_status transfer) {
    if (response->status == 401 || response->status == 403)
        return UD_AUTH;
    if (response->status == 404)
        return UD_IO;
    if (response->status == 409)
        return UD_BUSY;
    if (response->status == 412)
        return UD_FENCED;
    if (transfer == UD_COMMIT_UNKNOWN)
        return UD_COMMIT_UNKNOWN;
    if (response->status == 408 || response->status == 429 || response->status >= 500)
        return response->attempted && response->completed ? UD_TRANSIENT : UD_COMMIT_UNKNOWN;
    if (transfer)
        return transfer;
    return response->status < 200 || response->status >= 300 ? UD_IO : UD_OK;
}

static const char *find_bytes(ud_string haystack, const char *needle) {
    const size_t length = strlen(needle);
    if (!length || haystack.length < length)
        return NULL;
    const char *end = haystack.data + haystack.length - length;
    for (const char *at = haystack.data; at <= end; ++at)
        if (!memcmp(at, needle, length))
            return at;
    return NULL;
}

static ud_status request(const ud_component_context *context, ud_connection_lease *lease,
                         ud_string method, ud_string url, ud_string range, ud_string body,
                         ud_string if_none_match, ud_string metadata_digest, FILE *file,
                         uint64_t offset, uint64_t size, int32_t action,
                         ud_network_s3_result *response) {
    ud_network_s3_options options = {.struct_size = sizeof(options),
                                     .abi_version = UD_ABI_VERSION,
                                     .action = action,
                                     .method = method,
                                     .url = url,
                                     .range = range,
                                     .body = body,
                                     .if_none_match = if_none_match,
                                     .metadata_digest = metadata_digest,
                                     .file = file,
                                     .file_offset = offset,
                                     .file_size = size,
                                     .timeout = S3_TIMEOUT_MS,
                                     .maximum = size ? size : S3_RESPONSE_MAX};
    ud_status status = ud_network_s3_call(context, lease, &options, response);
    status = map_response(response, status);
    if (status && response->status != 404 && response->status != 412) {
        ud_error_detail detail = {.struct_size = sizeof(detail), .abi_version = UD_ABI_VERSION,
                                  .source = response->completed ? UD_ERROR_HTTP : UD_ERROR_TRANSPORT,
                                  .cause = status,
                                  .native_code = response->completed ? response->status : response->transport_code};
        char message[96];
        int length = snprintf(message, sizeof(message), "S3 %.*s failed (HTTP %d, transport %d)",
                              (int)method.length, method.data, response->status, response->transport_code);
        return ud_context_report_error(context->memory, status,
                                       (ud_string){message, (uint32_t)length}, &detail);
    }
    return status;
}

static int ascii_equal(const char *a, const char *b, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        unsigned char x = (unsigned char)a[i], y = (unsigned char)b[i];
        if (x >= 'A' && x <= 'Z') x = (unsigned char)(x + 'a' - 'A');
        if (y >= 'A' && y <= 'Z') y = (unsigned char)(y + 'a' - 'A');
        if (x != y) return 0;
    }
    return 1;
}

static ud_status header_value(ud_string headers, const char *name, char *out, size_t capacity) {
    const size_t wanted = strlen(name);
    if (!headers.data || !headers.length)
        return UD_END;
    const char *at = headers.data, *end = headers.data + headers.length;
    while (at < end) {
        const char *line_end = memchr(at, '\n', (size_t)(end - at));
        if (!line_end) line_end = end;
        const char *colon = memchr(at, ':', (size_t)(line_end - at));
        if (colon && (size_t)(colon - at) == wanted && ascii_equal(at, name, wanted)) {
            const char *value = colon + 1;
            while (value < line_end && (*value == ' ' || *value == '\t')) value++;
            const char *value_end = line_end;
            while (value_end > value && (value_end[-1] == '\r' || value_end[-1] == ' ' ||
                                         value_end[-1] == '\t')) value_end--;
            const size_t length = (size_t)(value_end - value);
            if (length + 1 > capacity) return UD_RESOURCE;
            memcpy(out, value, length);
            out[length] = 0;
            return UD_OK;
        }
        at = line_end < end ? line_end + 1 : end;
    }
    return UD_END;
}

static ud_status head_info(const ud_component_context *context, ud_connection_lease *lease,
                           ud_string location, uint64_t *size, char digest[65], int *exists) {
    char *url = NULL;
    ud_network_s3_result response = {0};
    ud_status status = object_url(context->memory, lease, location, &url);
    if (!status)
        status = request(context, lease, (ud_string){"HEAD", 4},
                         (ud_string){url, (uint32_t)strlen(url)}, (ud_string){0},
                         (ud_string){0}, (ud_string){0}, (ud_string){0}, NULL, 0, 0,
                         UD_ACCESS_READ, &response);
    if (exists) *exists = 0;
    if (response.status == 404) {
        status = UD_END;
    } else if (!status && response.status >= 200 && response.status < 300) {
        if (exists) *exists = 1;
        if (size) *size = response.content_length;
        if (size && !*size) {
            char value[64];
            if (!header_value(response.headers, "Content-Length", value, sizeof(value))) {
                char *end = NULL;
                errno = 0;
                unsigned long long parsed = strtoull(value, &end, 10);
                if (!errno && end && !*end) *size = (uint64_t)parsed;
            }
        }
        if (digest) {
            digest[0] = 0;
            char value[65];
            if (!header_value(response.headers, "x-amz-meta-universaldata-sha256", value,
                              sizeof(value)) && strlen(value) == 64)
                memcpy(digest, value, 65);
        }
        status = UD_OK;
    } else if (!status) {
        status = map_response(&response, UD_IO);
    }
    ud_network_s3_result_clear(&response);
    ud_free(url);
    return status;
}

ud_status ud_s3_object_head(const ud_component_context *context, ud_connection_lease *lease,
                            ud_string location, uint64_t *size, char digest[65]) {
    if (!context || !lease || !size)
        return UD_INVALID;
    *size = 0;
    if (digest) digest[0] = 0;
    int exists = 0;
    ud_status status = head_info(context, lease, location, size, digest, &exists);
    return status == UD_END ? UD_IO : status;
}

ud_status ud_s3_object_read(const ud_component_context *context, ud_connection_lease *lease,
                            ud_string location, uint64_t offset, uint8_t *buffer,
                            uint64_t length, uint64_t *received) {
    if (!context || !lease || !buffer || !received || !length || length > S3_RANGE_MAX)
        return UD_INVALID;
    *received = 0;
    if (offset > UINT64_MAX - length + 1)
        return UD_OVERFLOW;
    char *url = NULL;
    ud_network_s3_result response = {0};
    char range[96];
    const int n = snprintf(range, sizeof(range), "bytes=%llu-%llu",
                           (unsigned long long)offset,
                           (unsigned long long)(offset + length - 1));
    ud_status status = n > 0 && (size_t)n < sizeof(range) ? UD_OK : UD_OVERFLOW;
    if (!status)
        status = object_url(context->memory, lease, location, &url);
    if (!status)
        status = request(context, lease, (ud_string){"GET", 3},
                         (ud_string){url, (uint32_t)strlen(url)},
                         (ud_string){range, (uint32_t)n}, (ud_string){0}, (ud_string){0},
                         (ud_string){0}, NULL, 0, length, UD_ACCESS_READ, &response);
    if (!status && response.status == 206) {
        if (response.body.length > length) status = UD_RESOURCE;
        else {
            memcpy(buffer, response.body.data, response.body.length);
            *received = response.body.length;
        }
    } else if (!status) {
        status = map_response(&response, UD_IO);
    }
    ud_network_s3_result_clear(&response);
    ud_free(url);
    return status;
}

static void digest_hex(const uint8_t digest[32], char output[65]) {
    static const char digits[] = "0123456789abcdef";
    for (uint32_t i = 0; i < 32; ++i) {
        output[2 * i] = digits[digest[i] >> 4];
        output[2 * i + 1] = digits[digest[i] & 15];
    }
    output[64] = 0;
}

static ud_status stage_digest(FILE *stage, uint64_t *size, char digest[65]) {
    if (!stage || !size || !digest)
        return UD_INVALID;
    if (fflush(stage)) return UD_IO;
#ifdef _WIN32
    if (_fseeki64(stage, 0, SEEK_END)) return UD_IO;
#else
    if (fseeko(stage, 0, SEEK_END)) return UD_IO;
#endif
    int64_t end = ud_tell(stage);
    if (end < 0) return UD_IO;
    *size = (uint64_t)end;
    if (ud_seek(stage, 0)) return UD_IO;
    ud_sha256 hash;
    ud_sha256_init(&hash);
    uint8_t buffer[65536];
    for (;;) {
        const size_t count = fread(buffer, 1, sizeof(buffer), stage);
        if (count) ud_sha256_update(&hash, buffer, count);
        if (count < sizeof(buffer)) {
            if (ferror(stage)) return UD_IO;
            break;
        }
    }
    uint8_t value[32];
    ud_sha256_finish(&hash, value);
    digest_hex(value, digest);
    return ud_seek(stage, 0) ? UD_IO : UD_OK;
}

static ud_status query_url(ud_context *memory, const char *base, const char *query, char **out) {
    ud_bytes value = {.ctx = memory, .limit = 65536};
    UD_TRY(ud_bytes_put(&value, base, strlen(base)));
    UD_TRY(ud_bytes_put(&value, strchr(base, '?') ? "&" : "?", 1));
    UD_TRY(ud_bytes_put(&value, query, strlen(query)));
    UD_TRY(ud_bytes_put(&value, "", 1));
    *out = (char *)value.data;
    return UD_OK;
}

static ud_status query_component(ud_bytes *encoded, const char *value) {
    static const char hex[] = "0123456789ABCDEF";
    for (const unsigned char *at = (const unsigned char *)value; *at; ++at) {
        if ((*at >= 'A' && *at <= 'Z') || (*at >= 'a' && *at <= 'z') ||
            (*at >= '0' && *at <= '9') || *at == '-' || *at == '.' || *at == '_' ||
            *at == '~') {
            UD_TRY(ud_bytes_put(encoded, at, 1));
        } else {
            char escaped[] = {'%', hex[*at >> 4], hex[*at & 15]};
            UD_TRY(ud_bytes_put(encoded, escaped, sizeof(escaped)));
        }
    }
    return UD_OK;
}

static ud_status journal_state(const ud_component_context *context, ud_string location,
                               ud_string digest, const char *state, const char *upload_id) {
    if (!context || !context->activation)
        return UD_OK;
    ud_bytes payload = {.ctx = context->memory, .limit = 16384};
    ud_status status = ud_bytes_put(&payload, "{\"version\":1,\"state\":",
                                    sizeof("{\"version\":1,\"state\":") - 1);
    if (!status)
        status = ud_json_quote(&payload, state, (uint32_t)strlen(state));
    if (!status)
        status = ud_bytes_put(&payload, ",\"digest\":", sizeof(",\"digest\":") - 1);
    if (!status)
        status = ud_json_quote(&payload, digest.data, digest.length);
    if (!status)
        status = ud_bytes_put(&payload, ",\"uploadId\":", sizeof(",\"uploadId\":") - 1);
    if (!status)
        status = ud_json_quote(&payload, upload_id ? upload_id : "", upload_id ?
                               (uint32_t)strlen(upload_id) : 0);
    if (!status)
        status = ud_bytes_put(&payload, "}", 1);
    if (!status)
        status = ud_checkpoint_file_record(context->activation, UD_FILE_PREPARED, location,
                                           (ud_string){(const char *)payload.data,
                                                       (uint32_t)payload.size});
    ud_bytes_clear(&payload);
    return status;
}

static ud_status journal_upload(const ud_component_context *context, ud_string location,
                                ud_string digest, char *upload_id, size_t capacity,
                                int *pending) {
    if (pending)
        *pending = 0;
    if (!context || !context->activation)
        return UD_OK;
    char *saved = NULL;
    uint32_t saved_length = 0;
    ud_status status = ud_checkpoint_file_read(context->activation, UD_FILE_PREPARED, location,
                                               &saved, &saved_length);
    if (status == UD_END) {
        ud_free(saved);
        return UD_OK;
    }
    if (status) {
        ud_free(saved);
        return status;
    }
    ud_json *record = NULL;
    status = ud_json_parse(context->memory, saved, saved_length, &record, NULL);
    ud_free(saved);
    static const char *keys[] = {"version", "state", "digest", "uploadId"};
    uint64_t version = 0;
    ud_string state = {0}, recorded_digest = {0}, recorded_id = {0};
    if (!status)
        status = ud_json_keys(record, keys, sizeof(keys) / sizeof(*keys));
    if (!status)
        status = ud_json_uint(ud_member(record, "version"), &version);
    if (!status)
        status = ud_json_string(ud_member(record, "state"), &state);
    if (!status)
        status = ud_json_string(ud_member(record, "digest"), &recorded_digest);
    if (!status)
        status = ud_json_string(ud_member(record, "uploadId"), &recorded_id);
    if (!status && (version != 1 || recorded_id.length > 4096 ||
                    (!ud_named(state, "pending") &&
                                                  !ud_named(state, "complete") &&
                                                  !ud_named(state, "aborted"))))
        status = UD_CORRUPT;
    if (!status && ud_named(state, "pending") &&
        (recorded_digest.length != digest.length ||
         memcmp(recorded_digest.data, digest.data, digest.length)))
        status = UD_CORRUPT;
    if (!status && ud_named(state, "pending")) {
        if (!recorded_id.length || recorded_id.length + 1 > capacity)
            status = UD_CORRUPT;
        else {
            memcpy(upload_id, recorded_id.data, recorded_id.length);
            upload_id[recorded_id.length] = 0;
            if (pending)
                *pending = 1;
        }
    }
    ud_json_destroy(record);
    return status;
}

static ud_status abort_multipart(const ud_component_context *context,
                                 ud_connection_lease *lease, const char *url,
                                 const char *upload_id) {
    ud_bytes encoded_id = {.ctx = context->memory, .limit = 16384};
    ud_status status = query_component(&encoded_id, upload_id);
    char query[16384];
    const int query_length = !status
                                 ? snprintf(query, sizeof(query), "uploadId=%.*s",
                                            (int)encoded_id.size,
                                            (const char *)encoded_id.data)
                                 : -1;
    char *abort_url = NULL;
    if (query_length < 0 || (size_t)query_length >= sizeof(query))
        status = UD_RESOURCE;
    if (!status)
        status = query_url(context->memory, url, query, &abort_url);
    ud_network_s3_result response = {0};
    if (!status)
        status = request(context, lease, (ud_string){"DELETE", 6},
                         (ud_string){abort_url, (uint32_t)strlen(abort_url)},
                         (ud_string){0}, (ud_string){0}, (ud_string){0}, (ud_string){0},
                         NULL, 0, 0, UD_ACCESS_DELETE, &response);
    if (response.status == 404)
        status = UD_OK;
    ud_network_s3_result_clear(&response);
    ud_free(abort_url);
    ud_bytes_clear(&encoded_id);
    return status;
}

static ud_status upload_multipart(const ud_component_context *context, ud_connection_lease *lease,
                                  const char *url, FILE *stage, uint64_t size,
                                  ud_string location, ud_string digest, int overwrite) {
    char *init_url = NULL;
    UD_TRY(query_url(context->memory, url, "uploads", &init_url));
    ud_network_s3_result response = {0};
    ud_status status = request(context, lease, (ud_string){"POST", 4},
                               (ud_string){init_url, (uint32_t)strlen(init_url)}, (ud_string){0},
                               (ud_string){0}, (ud_string){0}, digest, NULL, 0, 0,
                               UD_ACCESS_WRITE, &response);
    ud_free(init_url);
    if (status || response.status < 200 || response.status >= 300) {
        const ud_status mapped = status ? status : map_response(&response, UD_IO);
        ud_network_s3_result_clear(&response);
        return mapped;
    }
    const ud_string response_body = response.body;
    const char *open = find_bytes(response_body, "<UploadId>");
    const char *close = open
                            ? find_bytes((ud_string){open,
                                                   (uint32_t)(response_body.data +
                                                              response_body.length - open)},
                                         "</UploadId>")
                            : NULL;
    if (!open || !close || close <= open + sizeof("<UploadId>") - 1 ||
        (size_t)(close - open - (sizeof("<UploadId>") - 1)) > 4096) {
        ud_network_s3_result_clear(&response);
        return UD_CORRUPT;
    }
    char upload_id[4097];
    const size_t upload_id_length = (size_t)(close - open - (sizeof("<UploadId>") - 1));
    memcpy(upload_id, open + sizeof("<UploadId>") - 1, upload_id_length);
    upload_id[upload_id_length] = 0;
    ud_network_s3_result_clear(&response);

    status = journal_state(context, location, digest, "pending", upload_id);
    if (status) {
        const ud_status aborted = abort_multipart(context, lease, url, upload_id);
        return aborted ? UD_COMMIT_UNKNOWN : status;
    }

    ud_bytes complete = {.ctx = context->memory, .limit = 64 * 1024 * 1024};
    status = ud_bytes_put(&complete, "<CompleteMultipartUpload>",
                          sizeof("<CompleteMultipartUpload>") - 1);
    uint64_t offset = 0;
    uint32_t part = 1;
    while (!status && offset < size) {
        const uint64_t count = size - offset > S3_MULTIPART_PART ? S3_MULTIPART_PART : size - offset;
        ud_bytes encoded_id = {.ctx = context->memory, .limit = 16384};
        status = query_component(&encoded_id, upload_id);
        char query[16384];
        const int query_length = !status
                                     ? snprintf(query, sizeof(query),
                                                "partNumber=%u&uploadId=%.*s", part,
                                                (int)encoded_id.size,
                                                (const char *)encoded_id.data)
                                     : -1;
        if (query_length < 0 || (size_t)query_length >= sizeof(query))
            status = UD_RESOURCE;
        char *part_url = NULL;
        if (!status)
            status = query_url(context->memory, url, query, &part_url);
        if (!status) {
            response = (ud_network_s3_result){0};
            status = request(context, lease, (ud_string){"PUT", 3},
                             (ud_string){part_url, (uint32_t)strlen(part_url)}, (ud_string){0},
                             (ud_string){0}, (ud_string){0}, (ud_string){0}, stage, offset, count,
                             UD_ACCESS_WRITE, &response);
        }
        ud_free(part_url);
        char etag[4096] = {0};
        if (!status && (response.status < 200 || response.status >= 300))
            status = map_response(&response, UD_IO);
        if (!status && header_value(response.headers, "ETag", etag, sizeof(etag)))
            status = UD_CORRUPT;
        if (!status) {
            char entry[4200];
            const int entry_length = snprintf(
                entry, sizeof(entry),
                "<Part><PartNumber>%u</PartNumber><ETag>%s</ETag></Part>", part, etag);
            if (entry_length < 0 || (size_t)entry_length >= sizeof(entry))
                status = UD_RESOURCE;
            else
                status = ud_bytes_put(&complete, entry, (size_t)entry_length);
        }
        ud_network_s3_result_clear(&response);
        ud_bytes_clear(&encoded_id);
        if (status) break;
        offset += count;
        if (part == UINT32_MAX) {
            status = UD_RESOURCE;
            break;
        }
        part++;
    }
    if (!status)
        status = ud_bytes_put(&complete, "</CompleteMultipartUpload>",
                              sizeof("</CompleteMultipartUpload>") - 1);
    if (!status) {
        ud_bytes encoded_id = {.ctx = context->memory, .limit = 16384};
        status = query_component(&encoded_id, upload_id);
        char query[16384];
        const int query_length = !status
                                     ? snprintf(query, sizeof(query), "uploadId=%.*s",
                                                (int)encoded_id.size,
                                                (const char *)encoded_id.data)
                                     : -1;
        char *complete_url = NULL;
        if (query_length < 0 || (size_t)query_length >= sizeof(query))
            status = UD_RESOURCE;
        if (!status)
            status = query_url(context->memory, url, query, &complete_url);
        if (!status) {
            response = (ud_network_s3_result){0};
            status = request(context, lease, (ud_string){"POST", 4},
                             (ud_string){complete_url, (uint32_t)strlen(complete_url)},
                             (ud_string){0}, (ud_string){(const char *)complete.data,
                                                           (uint32_t)complete.size},
                             overwrite ? (ud_string){0} : (ud_string){"*", 1},
                             (ud_string){0}, NULL, 0, 0, UD_ACCESS_WRITE, &response);
            if (!status && (response.status < 200 || response.status >= 300))
                status = map_response(&response, UD_IO);
            if (!status && (!response.body.length ||
                            !find_bytes(response.body, "CompleteMultipartUploadResult") ||
                            find_bytes(response.body, "<Error")))
                status = UD_CORRUPT;
            ud_network_s3_result_clear(&response);
        }
        ud_free(complete_url);
        ud_bytes_clear(&encoded_id);
    }
    if (!status) {
        uint64_t verified_size = 0;
        char verified_digest[65] = {0};
        int verified_exists = 0;
        status = head_info(context, lease, location, &verified_size, verified_digest,
                           &verified_exists);
        if (!status && (!verified_exists || verified_size != size ||
                        !verified_digest[0] || digest.length != 64 ||
                        memcmp(verified_digest, digest.data, 64)))
            status = UD_CORRUPT;
    }
    if (!status) {
        status = journal_state(context, location, digest, "complete", upload_id);
        if (status)
            status = UD_COMMIT_UNKNOWN;
    }
    ud_bytes_clear(&complete);
    if (status) {
        const ud_status aborted = abort_multipart(context, lease, url, upload_id);
        if (aborted && status != UD_COMMIT_UNKNOWN) status = UD_COMMIT_UNKNOWN;
    }
    return status;
}

ud_status ud_s3_object_upload(const ud_component_context *context, ud_connection_lease *lease,
                              ud_string location, FILE *stage, int overwrite,
                              char digest[65]) {
    if (!context || !lease || !stage || !digest)
        return UD_INVALID;
    uint64_t size = 0;
    UD_TRY(stage_digest(stage, &size, digest));
    int exists = 0;
    uint64_t existing_size = 0;
    char existing_digest[65] = {0};
    char *url = NULL;
    UD_TRY(object_url(context->memory, lease, location, &url));
    char prior_upload_id[4097] = {0};
    int pending_upload = 0;
    ud_status status = journal_upload(context, location, (ud_string){digest, 64},
                                      prior_upload_id, sizeof(prior_upload_id), &pending_upload);
    if (status)
        goto finish;
    if (pending_upload) {
        status = abort_multipart(context, lease, url, prior_upload_id);
        if (status)
            goto finish;
        (void)journal_state(context, location, (ud_string){digest, 64}, "aborted",
                            prior_upload_id);
    }
    status = head_info(context, lease, location, &existing_size, existing_digest, &exists);
    if (status != UD_OK && status != UD_END)
        goto finish;
    if (exists) {
        if (existing_size == size && existing_digest[0] && !strcmp(existing_digest, digest))
            goto finish;
        if (!overwrite) {
            status = UD_FENCED;
            goto finish;
        }
    }
    ud_status result;
    if (size <= S3_MULTIPART_PART) {
        ud_network_s3_result response = {0};
        result = request(context, lease, (ud_string){"PUT", 3},
                         (ud_string){url, (uint32_t)strlen(url)}, (ud_string){0},
                         (ud_string){0}, overwrite ? (ud_string){0} : (ud_string){"*", 1},
                         (ud_string){digest, 64}, stage, 0, size, UD_ACCESS_WRITE, &response);
        if (!result && (response.status < 200 || response.status >= 300))
            result = map_response(&response, UD_IO);
        if (result == UD_FENCED && !overwrite) {
            ud_network_s3_result_clear(&response);
            int replay_exists = 0;
            uint64_t replay_size = 0;
            char replay_digest[65] = {0};
            const ud_status replay = head_info(context, lease, location, &replay_size,
                                               replay_digest, &replay_exists);
            if (!replay && replay_exists && replay_size == size &&
                replay_digest[0] && !strcmp(replay_digest, digest))
                result = UD_OK;
        }
        ud_network_s3_result_clear(&response);
        if (!result) {
            uint64_t verified_size = 0;
            char verified_digest[65] = {0};
            int verified_exists = 0;
            result = head_info(context, lease, location, &verified_size, verified_digest,
                               &verified_exists);
            if (!result && (!verified_exists || verified_size != size ||
                            !verified_digest[0] || strcmp(verified_digest, digest)))
                result = UD_CORRUPT;
        }
    } else {
        result = upload_multipart(context, lease, url, stage, size, location,
                                  (ud_string){digest, 64}, overwrite);
    }
    status = result;
finish:
    if (status == UD_END)
        status = UD_OK;
    ud_free(url);
    return status;
}

ud_status ud_s3_object_delete(const ud_component_context *context, ud_connection_lease *lease,
                              ud_string location) {
    if (!context || !lease)
        return UD_INVALID;
    char *url = NULL;
    ud_network_s3_result response = {0};
    ud_status status = object_url(context->memory, lease, location, &url);
    if (!status)
        status = request(context, lease, (ud_string){"DELETE", 6},
                         (ud_string){url, (uint32_t)strlen(url)}, (ud_string){0},
                         (ud_string){0}, (ud_string){0}, (ud_string){0}, NULL, 0, 0,
                         UD_ACCESS_DELETE, &response);
    if (response.status == 404)
        status = UD_OK;
    ud_network_s3_result_clear(&response);
    ud_free(url);
    return status;
}
