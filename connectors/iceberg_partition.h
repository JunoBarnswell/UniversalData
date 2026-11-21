#ifndef UD_CONNECTOR_ICEBERG_PARTITION_H
#define UD_CONNECTOR_ICEBERG_PARTITION_H

#include "ud/data.h"

#include "iceberg/expression/literal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/partition_value_util.h"
#include "iceberg/util/int128.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ud_iceberg_partition {

using iceberg::PartitionSpec;
using iceberg::PartitionValues;
using iceberg::Result;

struct Group {
    std::shared_ptr<PartitionSpec> spec;
    PartitionValues partition;
    std::vector<uint32_t> rows;
};

inline int32_t field_id(const ud_field& field, int32_t fallback) {
    uint64_t value = field.identity.low & UINT64_C(0x7fffffff);
    if (!value) value = static_cast<uint64_t>(fallback);
    return static_cast<int32_t>(value ? value : fallback);
}

inline Result<iceberg::Literal> literal(const ud_value& value,
                                        const std::shared_ptr<iceberg::Type>& type) {
    if (!type || !type->is_primitive())
        return iceberg::InvalidSchema("Iceberg partition source is not primitive");
    const auto primitive = std::dynamic_pointer_cast<iceberg::PrimitiveType>(type);
    if (value.is_null) return iceberg::Literal::Null(primitive);
    switch (type->type_id()) {
    case iceberg::TypeId::kBoolean:
        if (value.type != UD_BOOL) return iceberg::InvalidSchema("Iceberg boolean partition value is invalid");
        return iceberg::Literal::Boolean(value.integer != 0);
    case iceberg::TypeId::kInt:
        if (value.type != UD_INT64 && value.type != UD_UINT64)
            return iceberg::InvalidSchema("Iceberg int partition value is invalid");
        if (value.type == UD_UINT64 && value.unsigned_integer > INT32_MAX)
            return iceberg::InvalidSchema("Iceberg int partition value overflows");
        if (value.type == UD_INT64 && (value.integer < INT32_MIN || value.integer > INT32_MAX))
            return iceberg::InvalidSchema("Iceberg int partition value overflows");
        return iceberg::Literal::Int(static_cast<int32_t>(value.type == UD_UINT64
                                                                ? value.unsigned_integer
                                                                : value.integer));
    case iceberg::TypeId::kLong:
        if (value.type == UD_UINT64) {
            if (value.unsigned_integer > static_cast<uint64_t>(INT64_MAX))
                return iceberg::InvalidSchema("Iceberg long partition value overflows");
            return iceberg::Literal::Long(static_cast<int64_t>(value.unsigned_integer));
        }
        if (value.type != UD_INT64) return iceberg::InvalidSchema("Iceberg long partition value is invalid");
        return iceberg::Literal::Long(value.integer);
    case iceberg::TypeId::kDate:
        if (value.type != UD_DATE32 || value.integer < INT32_MIN || value.integer > INT32_MAX)
            return iceberg::InvalidSchema("Iceberg date partition value is invalid");
        return iceberg::Literal::Date(static_cast<int32_t>(value.integer));
    case iceberg::TypeId::kTime:
        if (value.type != UD_TIME64) return iceberg::InvalidSchema("Iceberg time partition value is invalid");
        return iceberg::Literal::Time(value.integer);
    case iceberg::TypeId::kTimestamp:
        if (value.type != UD_TIMESTAMP) return iceberg::InvalidSchema("Iceberg timestamp partition value is invalid");
        return iceberg::Literal::Timestamp(value.integer);
    case iceberg::TypeId::kTimestampTz:
        if (value.type != UD_TIMESTAMP) return iceberg::InvalidSchema("Iceberg timestamp-tz partition value is invalid");
        return iceberg::Literal::TimestampTz(value.integer);
    case iceberg::TypeId::kTimestampNs:
        if (value.type != UD_TIMESTAMP || value.integer > INT64_MAX / 1000 ||
            value.integer < INT64_MIN / 1000)
            return iceberg::InvalidSchema("Iceberg timestamp-ns partition value is invalid");
        return iceberg::Literal::TimestampNs(value.integer * 1000);
    case iceberg::TypeId::kTimestampTzNs:
        if (value.type != UD_TIMESTAMP || value.integer > INT64_MAX / 1000 ||
            value.integer < INT64_MIN / 1000)
            return iceberg::InvalidSchema("Iceberg timestamp-tz-ns partition value is invalid");
        return iceberg::Literal::TimestampTzNs(value.integer * 1000);
    case iceberg::TypeId::kFloat:
        if (value.type != UD_FLOAT64) return iceberg::InvalidSchema("Iceberg float partition value is invalid");
        return iceberg::Literal::Float(static_cast<float>(value.real));
    case iceberg::TypeId::kDouble:
        if (value.type != UD_FLOAT64) return iceberg::InvalidSchema("Iceberg double partition value is invalid");
        return iceberg::Literal::Double(value.real);
    case iceberg::TypeId::kString:
        if (value.type != UD_UTF8 || (value.text_length && !value.text))
            return iceberg::InvalidSchema("Iceberg string partition value is invalid");
        return iceberg::Literal::String(std::string(value.text ? value.text : "", value.text_length));
    case iceberg::TypeId::kBinary:
    case iceberg::TypeId::kFixed: {
        if (value.type != UD_BINARY || (value.text_length && !value.text))
            return iceberg::InvalidSchema("Iceberg binary partition value is invalid");
        std::vector<uint8_t> bytes(value.text_length);
        if (!bytes.empty()) std::copy_n(reinterpret_cast<const uint8_t*>(value.text), bytes.size(), bytes.data());
        return type->type_id() == iceberg::TypeId::kFixed ? iceberg::Literal::Fixed(std::move(bytes))
                                                          : iceberg::Literal::Binary(std::move(bytes));
    }
    case iceberg::TypeId::kDecimal: {
        const auto decimal_type = std::dynamic_pointer_cast<iceberg::DecimalType>(type);
        if (value.type != UD_DECIMAL128 || !decimal_type ||
            value.precision != decimal_type->precision() || value.scale != decimal_type->scale())
            return iceberg::InvalidSchema("Iceberg decimal partition value is invalid");
        const uint128_t bits = (static_cast<uint128_t>(static_cast<uint64_t>(value.decimal.high)) << 64) |
                               static_cast<uint128_t>(value.decimal.low);
        const int128_t raw = static_cast<int128_t>(bits);
        return iceberg::Literal::Decimal(raw, value.precision, value.scale);
    }
    default:
        return iceberg::NotSupported("Iceberg partition transform source type is unsupported");
    }
}

inline Result<PartitionValues> values_for_row(const ud_batch* batch, uint32_t row,
                                              const std::shared_ptr<iceberg::Schema>& schema,
                                              const std::shared_ptr<PartitionSpec>& spec) {
    if (!batch || !schema || !spec) return iceberg::InvalidArgument("Iceberg partition input is missing");
    PartitionValues values;
    for (const auto& field : spec->fields()) {
        auto source = schema->FindFieldById(field.source_id());
        if (!source.has_value()) return std::unexpected(source.error());
        if (!source->has_value()) return iceberg::InvalidSchema("Iceberg partition source field is missing");
        uint32_t column = UINT32_MAX;
        for (uint32_t i = 0; i < ud_batch_columns(batch); ++i) {
            ud_field candidate{};
            if (ud_batch_field(batch, i, &candidate) != UD_OK)
                return iceberg::InvalidSchema("Invalid UniversalData partition field");
            if (field_id(candidate, static_cast<int32_t>(i + 1)) == field.source_id()) {
                column = i;
                break;
            }
        }
        if (column == UINT32_MAX) return iceberg::InvalidSchema("Iceberg partition source is absent from input");
        ud_value value{};
        if (ud_batch_get(batch, row, column, &value) != UD_OK)
            return iceberg::InvalidArrowData("Iceberg partition value cannot be read");
        const auto& source_field = source->value().get();
        auto source_literal = literal(value, source_field.type());
        if (!source_literal.has_value()) return std::unexpected(source_literal.error());
        auto bound = field.transform()->Bind(source_field.type());
        if (!bound.has_value()) return std::unexpected(bound.error());
        auto transformed = bound->get()->Transform(*source_literal);
        if (!transformed.has_value()) return std::unexpected(transformed.error());
        values.AddValue(std::move(*transformed));
    }
    return values;
}

inline Result<std::vector<Group>> group_rows(const ud_batch* batch,
                                             const std::shared_ptr<iceberg::Schema>& schema,
                                             const std::shared_ptr<PartitionSpec>& spec,
                                             std::span<const uint8_t> selected) {
    if (!batch || !schema || !spec || selected.size() != ud_batch_rows(batch))
        return iceberg::InvalidArgument("Iceberg partition selection is invalid");
    std::vector<Group> groups;
    std::unordered_map<iceberg::PartitionKey, size_t, iceberg::PartitionKeyHash,
                       iceberg::PartitionKeyEqual> indexes;
    indexes.reserve(ud_batch_rows(batch));
    for (uint32_t row = 0; row < ud_batch_rows(batch); ++row) {
        if (!selected[row]) continue;
        auto values = values_for_row(batch, row, schema, spec);
        if (!values.has_value()) return std::unexpected(values.error());
        iceberg::PartitionKey key{spec->spec_id(), *values};
        auto found = indexes.find(key);
        if (found == indexes.end()) {
            indexes.emplace(std::move(key), groups.size());
            groups.push_back(Group{spec, std::move(*values), {row}});
        } else {
            groups[found->second].rows.push_back(row);
        }
    }
    return groups;
}

inline Result<std::string> group_token(const Group& group) {
    if (!group.spec) return iceberg::InvalidArgument("Iceberg partition group has no spec");
    auto path = group.spec->PartitionPath(group.partition);
    if (!path.has_value()) return std::unexpected(path.error());
    std::string token = "s" + std::to_string(group.spec->spec_id()) + "-";
    if (path->empty()) token += "unpartitioned";
    else token += *path;
    for (char& c : token) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '-' || c == '_'))
            c = '_';
    }
    return token;
}

}  // namespace ud_iceberg_partition

#endif
