#include "../internal.h"
#include "ud/format.h"
#include <ctype.h>
static int word(ud_string text, const char *literal) {
    return text.length == strlen(literal) && !memcmp(text.data, literal, text.length);
}
static ud_status json_text(const ud_json *json, ud_string *out) {
    return ud_json_text(json, &out->data, &out->length);
}
ud_status ud_schema_fingerprint(const ud_batch *schema, uint8_t out[32]) {
    if (!schema || !out)
        return UD_INVALID;
    char *json = NULL;
    uint32_t length = 0;
    UD_TRY(ud_schema_to_json(schema, &json, &length));
    ud_hash(json, length, out);
    ud_free(json);
    return UD_OK;
}
static ud_status sql_type(ud_field *field, ud_string declaration, int32_t dialect) {
    if (!declaration.data || declaration.length > 255 ||
        memchr(declaration.data, 0, declaration.length))
        return UD_SCHEMA;
    char type[256];
    for (uint32_t j = 0; j < declaration.length; j++)
        type[j] = (char)tolower((unsigned char)declaration.data[j]);
    type[declaration.length] = 0;
    char *begin = type;
    while (*begin == ' ')
        begin++;
    size_t n = strlen(begin);
    while (n && begin[n - 1] == ' ')
        begin[--n] = 0;
    if (dialect == UD_SQL_CANONICAL) {
        const char *names[] = {"bool",   "int64",  "float64",   "utf8",  "decimal128",
                               "date32", "time64", "timestamp", "binary"};
        for (uint32_t i = 0; i < 9; i++)
            if (!strcmp(begin, names[i])) {
                field->type = (ud_type)i + 1;
                break;
            }
        if (field->type == UD_DECIMAL128)
            return UD_UNSUPPORTED;
    } else if (dialect == UD_SQL_SQLITE) {
        if (!strcmp(begin, "boolean") || !strcmp(begin, "bool"))
            field->type = UD_BOOL;
        else if (strstr(begin, "int"))
            field->type = UD_INT64;
        else if (strstr(begin, "char") || strstr(begin, "clob") || strstr(begin, "text"))
            field->type = UD_UTF8;
        else if (!*begin || strstr(begin, "blob"))
            field->type = UD_BINARY;
        else if (strstr(begin, "real") || strstr(begin, "floa") || strstr(begin, "doub"))
            field->type = UD_FLOAT64;
        else
            return UD_UNSUPPORTED; /* SQLite NUMERIC affinity cannot prove Decimal precision. */
    } else if (dialect == UD_SQL_POSTGRESQL) {
        if (!strcmp(begin, "boolean"))
            field->type = UD_BOOL;
        else if (!strcmp(begin, "smallint") || !strcmp(begin, "integer") ||
                 !strcmp(begin, "bigint"))
            field->type = UD_INT64;
        else if (!strcmp(begin, "double precision"))
            field->type = UD_FLOAT64;
        else if (!strcmp(begin, "text") || !strncmp(begin, "character varying", 17) ||
                 !strncmp(begin, "character(", 10))
            field->type = UD_UTF8;
        else if (!strncmp(begin, "numeric(", 8) || !strncmp(begin, "decimal(", 8)) {
            int precision = 0, scale = 0;
            char tail = 0;
            if (sscanf(strchr(begin, '('), "(%d,%d)%c", &precision, &scale, &tail) != 2 ||
                precision < 1 || precision > 38 || scale < -38 || scale > 38)
                return UD_UNSUPPORTED;
            field->type = UD_DECIMAL128;
            field->precision = precision;
            field->scale = scale;
        } else if (!strcmp(begin, "date"))
            field->type = UD_DATE32;
        else if (!strcmp(begin, "time without time zone"))
            field->type = UD_TIME64;
        else if (!strcmp(begin, "timestamp with time zone"))
            field->type = UD_TIMESTAMP;
        else if (!strcmp(begin, "bytea"))
            field->type = UD_BINARY;
    }
    if (!field->type)
        return UD_UNSUPPORTED;
    if (field->type == UD_TIMESTAMP) {
        field->timezone = "UTC";
        field->timezone_length = 3;
    }
    return UD_OK;
}
ud_status ud_schema_from_metadata(ud_context *ctx, const ud_batch *metadata, ud_string scope,
                                  int32_t dialect, ud_batch **out) {
    if (!ctx || !metadata || !out || metadata->columns < 4 || !metadata->rows || dialect < 0 ||
        dialect > 2)
        return UD_INVALID;
    *out = NULL;
    ud_field *fields = ud_alloc(ctx, (size_t)metadata->rows * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (uint32_t row = 0; !status && row < metadata->rows; row++) {
        ud_value name, type, nullable, key;
        status = ud_batch_get(metadata, row, 0, &name);
        if (!status)
            status = ud_batch_get(metadata, row, 1, &type);
        if (!status)
            status = ud_batch_get(metadata, row, 2, &nullable);
        if (!status)
            status = ud_batch_get(metadata, row, 3, &key);
        if (!status && (name.type != UD_UTF8 || type.type != UD_UTF8 || nullable.type != UD_BOOL ||
                        key.type != UD_INT64 || name.is_null || type.is_null || key.integer < 0 ||
                        key.integer > 65536))
            status = UD_SCHEMA;
        if (!status) {
            fields[row] = (ud_field){.name = name.text,
                                     .name_length = name.text_length,
                                     .nullable = (int32_t)nullable.integer,
                                     .key_order = (uint32_t)key.integer};
            status = sql_type(fields + row, (ud_string){type.text, type.text_length}, dialect);
        }
    }
    if (!status)
        status = ud_batch_create_named(ctx, fields, metadata->rows, 0, scope, out);
    ud_free(fields);
    return status;
}
static int locate(const ud_batch *schema, ud_string ref) {
    ud_id id;
    int identity = ud_id_parse(ref, &id) == UD_OK;
    for (uint32_t j = 0; j < schema->columns; j++) {
        const ud_field *field = &schema->column[j].field;
        if (identity
                ? !ud_id_compare(id, field->identity)
                : ref.length == field->name_length && !memcmp(ref.data, field->name, ref.length))
            return (int)j;
    }
    return -1;
}
static int by_id(const ud_batch *schema, ud_id id) {
    for (uint32_t j = 0; j < schema->columns; j++)
        if (!ud_id_compare(id, schema->column[j].field.identity))
            return (int)j;
    return -1;
}
static int lossless(const ud_field *actual, const ud_field *expected) {
    if (actual->type != expected->type || (actual->nullable && !expected->nullable) ||
        actual->key_order != expected->key_order || actual->child_count != expected->child_count)
        return 0;
    if (actual->type == UD_DECIMAL128 &&
        (expected->scale < actual->scale ||
         expected->precision - expected->scale < actual->precision - actual->scale))
        return 0;
    if (actual->type == UD_TIMESTAMP &&
        (actual->timezone_length != expected->timezone_length ||
         memcmp(actual->timezone, expected->timezone, actual->timezone_length)))
        return 0;
    for (uint32_t j = 0; j < actual->child_count; j++)
        if (!lossless(actual->children + j, expected->children + j) ||
            actual->children[j].name_length != expected->children[j].name_length ||
            memcmp(actual->children[j].name, expected->children[j].name,
                   actual->children[j].name_length))
            return 0;
    return 1;
}
static ud_status change(ud_bytes *json, int *first, const char *kind, const ud_field *field,
                        int expected, int actual) {
    char id[33], text[224];
    ud_id_format(field->identity, id);
    int n =
        snprintf(text, sizeof(text),
                 "%s{\"kind\":\"%s\",\"columnId\":\"%s\",\"expectedIndex\":%d,\"actualIndex\":%d}",
                 *first ? "" : ",", kind, id, expected, actual);
    *first = 0;
    return ud_bytes_put(json, text, (size_t)n);
}
static ud_status nested_changes(ud_bytes *out, int *first, const ud_field *expected,
                                const ud_field *actual, uint32_t depth) {
    if (depth > 32)
        return UD_SCHEMA;
    ud_status status = UD_OK;
    for (uint32_t j = 0; !status && j < expected->child_count; j++) {
        const ud_field *field = expected->children + j;
        int at = -1;
        for (uint32_t k = 0; k < actual->child_count; k++)
            if (!ud_id_compare(field->identity, actual->children[k].identity))
                at = (int)k;
        if (at < 0) {
            status = change(out, first, "Removed", field, (int)j, -1);
            continue;
        }
        const ud_field *now = actual->children + at;
        if (at != (int)j)
            status = change(out, first, "Reordered", field, (int)j, at);
        if (!status && field->nullable != now->nullable)
            status = change(out, first, "NullabilityChanged", field, (int)j, at);
        if (!status && !ud_field_matches(field, now, UD_FIELD_VALUE_TYPE))
            status = change(out, first, "TypeChanged", field, (int)j, at);
        if (!status)
            status = nested_changes(out, first, field, now, depth + 1);
    }
    for (uint32_t j = 0; !status && j < actual->child_count; j++) {
        int found = 0;
        for (uint32_t k = 0; k < expected->child_count; k++)
            found |= !ud_id_compare(actual->children[j].identity, expected->children[k].identity);
        if (!found)
            status = change(out, first, "Added", actual->children + j, -1, (int)j);
    }
    return status;
}
static ud_status binding(ud_context *ctx, const ud_batch *expected, const ud_batch *actual,
                         ud_string policy, uint32_t **mapping, ud_bytes *changes, int *compatible) {
    ud_json *options = NULL;
    UD_TRY(ud_json_parse(ctx, policy.data ? policy.data : "{\"policy\":\"strict\"}",
                         policy.data ? policy.length : 19, &options, NULL));
    static const char *keys[] = {"policy", "mapping"};
    ud_status status = ud_json_keys(options, keys, 2);
    ud_string mode = {0};
    if (!status)
        status = json_text(ud_json_find(options, "policy", 6), &mode);
    int strict = word(mode, "strict"), explicit_mapping = word(mode, "explicitMapping");
    if (!status && !strict && !explicit_mapping && !word(mode, "compatible"))
        status = UD_SCHEMA;
    const ud_json *map = ud_json_find(options, "mapping", 7);
    if (!status && explicit_mapping &&
        (ud_json_kind(map) != UD_JSON_ARRAY || ud_json_size(map) != expected->columns))
        status = UD_SCHEMA;
    uint32_t *indices =
        !status ? ud_alloc(ctx, (size_t)expected->columns * sizeof(*indices)) : NULL;
    uint8_t *used = !status ? ud_alloc(ctx, actual->columns + 1) : NULL;
    if (!status && (!indices || !used))
        status = UD_RESOURCE;
    *compatible = 1;
    int first = 1;
    for (uint32_t j = 0; !status && j < expected->columns; j++) {
        const ud_field *field = &expected->column[j].field;
        int at = by_id(actual, field->identity);
        if (explicit_mapping) {
            at = -1;
            for (uint32_t i = 0; !status && i < ud_json_size(map); i++) {
                const ud_json *entry = ud_json_at(map, i);
                static const char *entry_keys[] = {"expected", "actual"};
                status = ud_json_keys(entry, entry_keys, 2);
                ud_string a = {0}, b = {0};
                if (!status)
                    status = json_text(ud_json_find(entry, "expected", 8), &a);
                if (!status)
                    status = json_text(ud_json_find(entry, "actual", 6), &b);
                int match = !status ? locate(expected, a) : -1;
                if (!status && match < 0)
                    status = UD_SCHEMA;
                if (match == (int)j) {
                    if (at >= 0)
                        status = UD_SCHEMA;
                    at = locate(actual, b);
                }
            }
        }
        if (at < 0) {
            *compatible = 0;
            indices[j] = UINT32_MAX;
            status = change(changes, &first, "Removed", field, (int)j, -1);
            continue;
        }
        indices[j] = (uint32_t)at;
        used[at] = 1;
        const ud_field *now = &actual->column[at].field;
        if (at != (int)j) {
            status = change(changes, &first, "Reordered", field, (int)j, at);
            if (strict)
                *compatible = 0;
        }
        if (!status && (field->name_length != now->name_length ||
                        memcmp(field->name, now->name, field->name_length))) {
            status = change(changes, &first, "Renamed", field, (int)j, at);
            if (strict)
                *compatible = 0;
        }
        if (!status && field->nullable != now->nullable)
            status = change(changes, &first, "NullabilityChanged", field, (int)j, at);
        if (!status && field->key_order != now->key_order) {
            status = change(changes, &first, "KeyChanged", field, (int)j, at);
            if (!explicit_mapping)
                *compatible = 0;
        }
        if (!status && (field->precision != now->precision || field->scale != now->scale))
            status = change(changes, &first, "PrecisionChanged", field, (int)j, at);
        if (!status && !ud_field_matches(field, now, UD_FIELD_VALUE_TYPE))
            status = change(changes, &first, "TypeChanged", field, (int)j, at);
        if (!status && (field->child_count || now->child_count))
            status = nested_changes(changes, &first, field, now, 0);
        if (strict ? !ud_field_matches(field, now, UD_FIELD_PORT)
                   : !explicit_mapping && !lossless(now, field))
            *compatible = 0;
    }
    for (uint32_t j = 0; !status && j < actual->columns; j++)
        if (!used[j]) {
            status = change(changes, &first, "Added", &actual->column[j].field, -1, (int)j);
            if (strict)
                *compatible = 0;
        }
    ud_free(used);
    ud_json_destroy(options);
    if (status)
        ud_free(indices);
    else
        *mapping = indices;
    return status;
}
ud_status ud_schema_compare(ud_context *ctx, const ud_batch *expected, const ud_batch *actual,
                            ud_string policy, char **out, uint32_t *length) {
    if (!ctx || !expected || !actual || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    ud_bytes changes = {.ctx = ctx}, result = {.ctx = ctx};
    uint32_t *mapping = NULL;
    int compatible = 0;
    ud_status status = binding(ctx, expected, actual, policy, &mapping, &changes, &compatible);
    int requires_mapping = !compatible && expected->columns == actual->columns;
    for (uint32_t j = 0; requires_mapping && j < expected->columns; j++)
        if (!lossless(&actual->column[j].field, &expected->column[j].field))
            requires_mapping = 0;
    char head[192];
    int size = snprintf(head, sizeof(head),
                        "{\"version\":%u,\"result\":\"%s\",\"compatible\":%s,\"changes\":[",
                        UD_CONFIG_VERSION,
                        compatible         ? "Compatible"
                        : requires_mapping ? "RequiresExplicitMapping"
                                           : "Incompatible",
                        compatible ? "true" : "false");
    if (!status)
        status = ud_bytes_put(&result, head, (size_t)size);
    if (!status)
        status = ud_bytes_put(&result, changes.data, changes.size);
    if (!status)
        status = ud_bytes_put(&result, "],\"mapping\":[", 13);
    for (uint32_t j = 0; !status && j < expected->columns; j++) {
        char entry[48];
        size = snprintf(entry, sizeof(entry), "%s%u", j ? "," : "", mapping[j]);
        status = ud_bytes_put(&result, entry, (size_t)size);
    }
    if (!status)
        status = ud_bytes_put(&result, "]}", 2);
    if (!status && result.size > UINT32_MAX)
        status = UD_RESOURCE;
    if (!status) {
        *out = ud_strdup_n(ctx, (char *)result.data, result.size);
        if (!*out)
            status = UD_RESOURCE;
        else
            *length = (uint32_t)result.size;
    }
    ud_free(mapping);
    ud_bytes_clear(&changes);
    ud_bytes_clear(&result);
    return status;
}
ud_status ud_schema_map(ud_context *ctx, const ud_batch *actual, const ud_batch *expected,
                        ud_string policy, ud_batch **out) {
    if (!ctx || !actual || !expected || !out)
        return UD_INVALID;
    *out = NULL;
    ud_bytes changes = {.ctx = ctx};
    uint32_t *mapping = NULL;
    int compatible = 0;
    ud_status status = binding(ctx, expected, actual, policy, &mapping, &changes, &compatible);
    if (!status && !compatible)
        status = UD_SCHEMA;
    ud_field *fields = !status ? ud_alloc(ctx, (size_t)expected->columns * sizeof(*fields)) : NULL;
    ud_value *values = !status ? ud_alloc(ctx, (size_t)expected->columns * sizeof(*values)) : NULL;
    if (!status && (!fields || !values))
        status = UD_RESOURCE;
    for (uint32_t j = 0; !status && j < expected->columns; j++)
        fields[j] = expected->column[j].field;
    if (!status)
        status = ud_batch_create(ctx, fields, expected->columns, actual->rows, out);
    for (uint32_t row = 0; !status && row < actual->rows; row++) {
        for (uint32_t j = 0; !status && j < expected->columns; j++) {
            ud_value value;
            status = ud_batch_get(actual, row, mapping[j], &value);
            if (!status)
                status = ud_cast_value(ctx, &actual->column[mapping[j]].field, &value, fields + j,
                                       UD_ROUND_EXACT, values + j, 0);
        }
        if (!status)
            status = ud_batch_append(*out, values, expected->columns);
        if (!status)
            status = ud_batch_copy_row_info(*out, (*out)->rows - 1, actual, row);
        for (uint32_t j = 0; j < expected->columns; j++)
            ud_clear_value(values + j);
    }
    if (!status)
        status = ud_batch_seal(*out);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    ud_free(fields);
    ud_free(values);
    ud_free(mapping);
    ud_bytes_clear(&changes);
    return status;
}
ud_status ud_schema_compare_json(ud_context *ctx, ud_string expected, ud_string actual,
                                 ud_string policy, char **out, uint32_t *length) {
    if (!ctx || !out || !length)
        return UD_INVALID;
    ud_batch *left = NULL, *right = NULL;
    ud_status status = ud_schema_from_json(ctx, expected.data, expected.length,
                                           (ud_string){"schema-comparison", 17}, &left);
    if (!status)
        status = ud_schema_from_json(ctx, actual.data, actual.length,
                                     (ud_string){"schema-comparison", 17}, &right);
    if (!status)
        status = ud_schema_compare(ctx, left, right, policy, out, length);
    ud_batch_destroy(left);
    ud_batch_destroy(right);
    return status;
}
