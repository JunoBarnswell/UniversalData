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
static ud_status decimal_type(ud_field *field, const char *type, int default_scale) {
    const char *open = strchr(type, '(');
    int p = 0, scale = default_scale;
    char tail = 0;
    if (!open)
        return UD_UNSUPPORTED;
    int parsed = sscanf(open, "(%d,%d)%c", &p, &scale, &tail);
    if (parsed != 2 && sscanf(open, "(%d)%c", &p, &tail) != 1)
        return UD_SCHEMA;
    if (p < 1 || p > 38 || scale < -38 || scale > 38)
        return UD_UNSUPPORTED;
    field->type = UD_DECIMAL128;
    field->precision = p;
    field->scale = scale;
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
        ud_type_parse((ud_string){begin, (uint32_t)strlen(begin)}, &field->type);
        if (!strncmp(begin, "decimal128(", 11))
            UD_TRY(decimal_type(field, begin, 0));
        else if (field->type == UD_DECIMAL128)
            return UD_UNSUPPORTED;
    } else if (dialect == UD_SQL_SQLITE) {
        if (!strcmp(begin, "u64_text"))
            field->type = UD_UINT64;
        else if (!strcmp(begin, "duration"))
            field->type = UD_DURATION;
        else if (!strcmp(begin, "iv_text"))
            field->type = UD_INTERVAL;
        else if (!strcmp(begin, "timestamp_utc"))
            field->type = UD_TIMESTAMP;
        else if (!strcmp(begin, "date32"))
            field->type = UD_DATE32;
        else if (!strcmp(begin, "time64"))
            field->type = UD_TIME64;
        else if (!strcmp(begin, "boolean") || !strcmp(begin, "bool"))
            field->type = UD_BOOL;
        else if (strstr(begin, "int"))
            field->type = UD_INT64;
        else if (strstr(begin, "char") || strstr(begin, "clob") || strstr(begin, "text"))
            field->type = UD_UTF8;
        else if (!*begin || strstr(begin, "blob"))
            field->type = UD_BINARY;
        else if (strstr(begin, "real") || strstr(begin, "floa") || strstr(begin, "doub"))
            field->type = UD_FLOAT64;
        /* NUMERIC affinity cannot prove lossless Decimal; TIMESTAMP has no zone. */
    } else if (dialect == UD_SQL_POSTGRESQL) {
        if (!strcmp(begin, "boolean"))
            field->type = UD_BOOL;
        else if (!strcmp(begin, "smallint") || !strcmp(begin, "integer") ||
                 !strcmp(begin, "bigint"))
            field->type = UD_INT64;
        else if (!strcmp(begin, "real") || !strcmp(begin, "double precision"))
            field->type = UD_FLOAT64;
        else if (!strcmp(begin, "text") || !strncmp(begin, "character varying", 17) ||
                 !strncmp(begin, "character(", 10))
            field->type = UD_UTF8;
        else if (!strncmp(begin, "numeric(", 8) || !strncmp(begin, "decimal(", 8))
            UD_TRY(decimal_type(field, begin, 0));
        else if (!strcmp(begin, "date"))
            field->type = UD_DATE32;
        else if (!strncmp(begin, "time without time zone", 22))
            field->type = UD_TIME64;
        else if (!strcmp(begin, "timestamp with time zone") || !strncmp(begin, "timestamp_utc", 13))
            field->type = UD_TIMESTAMP;
        else if (!strcmp(begin, "bytea"))
            field->type = UD_BINARY;
        else if (!strncmp(begin, "interval", 8))
            field->type = UD_INTERVAL;
    } else if (dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB) {
        if (( !strncmp(begin,"tinyint",7)||!strncmp(begin,"smallint",8)||!strncmp(begin,"mediumint",9)||!strncmp(begin,"int",3)||!strncmp(begin,"bigint",6))&&strstr(begin,"unsigned")) {
            field->type = UD_UINT64;
        } else if (!strncmp(begin, "tinyint", 7) || !strncmp(begin, "smallint", 8) ||
                   !strncmp(begin, "mediumint", 9) || !strncmp(begin, "int", 3) ||
                   !strncmp(begin, "bigint", 6) || !strcmp(begin, "year"))
            field->type = UD_INT64;
        else if (!strcmp(begin, "boolean") || !strcmp(begin, "bool"))
            field->type = UD_BOOL;
        else if (!strncmp(begin, "decimal(", 8) || !strncmp(begin, "numeric(", 8))
            UD_TRY(decimal_type(field, begin, 0));
        else if (!strncmp(begin, "double", 6) || !strncmp(begin, "float", 5))
            field->type = UD_FLOAT64;
        else if (strstr(begin, "char") || strstr(begin, "text") || !strcmp(begin, "json"))
            field->type = UD_UTF8;
        else if (strstr(begin, "blob") || strstr(begin, "binary") || !strncmp(begin, "bit(", 4))
            field->type = UD_BINARY;
        else if (!strcmp(begin, "date"))
            field->type = UD_DATE32;
        else if (!strncmp(begin, "time", 4) && strncmp(begin, "timestamp", 9))
            field->type = UD_DURATION;
        else if (!strncmp(begin, "timestamp", 9) || !strncmp(begin, "datetime_utc", 12))
            field->type = UD_TIMESTAMP;
        /* DATETIME UTC is supplied only by an explicit session policy. */
    } else if (dialect == UD_SQL_SQLSERVER) {
        if (!strcmp(begin, "bit"))
            field->type = UD_BOOL;
        else if (!strcmp(begin, "tinyint") || !strcmp(begin, "smallint") || !strcmp(begin, "int") ||
                 !strcmp(begin, "bigint"))
            field->type = UD_INT64;
        else if (!strncmp(begin, "decimal(", 8) || !strncmp(begin, "numeric(", 8))
            UD_TRY(decimal_type(field, begin, 0));
        else if (!strcmp(begin, "money") || !strcmp(begin, "smallmoney")) {
            field->type = UD_DECIMAL128;
            field->precision = !strcmp(begin, "money") ? 19 : 10;
            field->scale = 4;
        } else if (!strncmp(begin, "float", 5) || !strcmp(begin, "real"))
            field->type = UD_FLOAT64;
        else if (strstr(begin, "char") || strstr(begin, "text") || !strcmp(begin, "xml"))
            field->type = UD_UTF8;
        else if (strstr(begin, "binary") || !strcmp(begin, "image") ||
                 !strcmp(begin, "timestamp") || !strcmp(begin, "rowversion"))
            field->type = UD_BINARY;
        else if (!strcmp(begin, "date"))
            field->type = UD_DATE32;
        else if (!strncmp(begin, "time(", 5) || !strcmp(begin, "time"))
            field->type = UD_TIME64;
        else if (!strncmp(begin, "datetimeoffset", 14) || !strncmp(begin, "datetime2_utc", 13) ||
                 !strcmp(begin, "datetime_utc"))
            field->type = UD_TIMESTAMP;
    } else if (dialect == UD_SQL_ORACLE) {
        if (!strncmp(begin, "number(", 7) || !strncmp(begin, "decimal(", 8) ||
            !strncmp(begin, "numeric(", 8))
            UD_TRY(decimal_type(field, begin, 0));
        else if (!strcmp(begin, "binary_double") || !strcmp(begin, "binary_float"))
            field->type = UD_FLOAT64;
        else if (strstr(begin, "char") || strstr(begin, "clob"))
            field->type = UD_UTF8;
        else if (!strncmp(begin, "raw", 3) || !strcmp(begin, "blob"))
            field->type = UD_BINARY;
        else if (!strcmp(begin, "date_utc") || !strncmp(begin, "timestamp_utc", 13) ||
                 strstr(begin, "with time zone") || strstr(begin, "with local time zone"))
            field->type = UD_TIMESTAMP;
        /* Bare NUMBER and DATE require an explicit precision / UTC policy. */
    } else if (dialect == UD_SQL_DB2) {
        if (!strcmp(begin, "boolean"))
            field->type = UD_BOOL;
        else if (!strcmp(begin, "smallint") || !strcmp(begin, "integer") ||
                 !strcmp(begin, "bigint"))
            field->type = UD_INT64;
        else if (!strncmp(begin, "decimal(", 8) || !strncmp(begin, "numeric(", 8))
            UD_TRY(decimal_type(field, begin, 0));
        else if (!strcmp(begin, "double") || !strcmp(begin, "real"))
            field->type = UD_FLOAT64;
        else if (strstr(begin, "char") || strstr(begin, "clob"))
            field->type = UD_UTF8;
        else if (strstr(begin, "blob") || strstr(begin, "binary"))
            field->type = UD_BINARY;
        else if (!strcmp(begin, "date"))
            field->type = UD_DATE32;
        else if (!strcmp(begin, "time"))
            field->type = UD_TIME64;
        else if (!strncmp(begin, "timestamp_utc", 13))
            field->type = UD_TIMESTAMP;
    } else if (dialect == UD_SQL_DUCKDB) {
        if (!strcmp(begin, "boolean"))
            field->type = UD_BOOL;
        else if (!strcmp(begin, "tinyint") || !strcmp(begin, "smallint") ||
                 !strcmp(begin, "integer") || !strcmp(begin, "bigint") ||
                 !strcmp(begin, "utinyint") || !strcmp(begin, "usmallint") ||
                 !strcmp(begin, "uinteger"))
            field->type = UD_INT64;
        else if (!strcmp(begin, "ubigint")) {
            field->type = UD_UINT64;
        } else if (!strncmp(begin, "decimal(", 8))
            UD_TRY(decimal_type(field, begin, 0));
        else if (!strcmp(begin, "double") || !strcmp(begin, "float"))
            field->type = UD_FLOAT64;
        else if (!strcmp(begin, "varchar"))
            field->type = UD_UTF8;
        else if (!strcmp(begin, "blob"))
            field->type = UD_BINARY;
        else if (!strcmp(begin, "date"))
            field->type = UD_DATE32;
        else if (!strcmp(begin, "time"))
            field->type = UD_TIME64;
        else if (!strcmp(begin, "timestamp with time zone") || !strcmp(begin, "timestamp_utc"))
            field->type = UD_TIMESTAMP;
    }
    if (!field->type)
        return UD_UNSUPPORTED;
    if (field->type == UD_TIMESTAMP) {
        field->timezone = "UTC";
        field->timezone_length = 3;
    }
    return UD_OK;
}
ud_status ud_field_from_sql(ud_string declaration, int32_t dialect, ud_field *out) {
    if (!out || dialect < UD_SQL_CANONICAL || dialect > UD_SQL_DUCKDB)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    return sql_type(out, declaration, dialect);
}
static ud_status sql_dictionary(ud_context *ctx, ud_string declaration, ud_field *field) {
    uint32_t at = 0;
    while (at < declaration.length && declaration.data[at] == ' ')
        at++;
    const char *text = declaration.data;
    uint32_t left = declaration.length - at;
    int kind = left >= 5 && tolower((unsigned char)text[at]) == 'e' &&
                       tolower((unsigned char)text[at + 1]) == 'n' &&
                       tolower((unsigned char)text[at + 2]) == 'u' &&
                       tolower((unsigned char)text[at + 3]) == 'm' && text[at + 4] == '('
                   ? UD_DICTIONARY_ENUM
               : left >= 4 && tolower((unsigned char)text[at]) == 's' &&
                       tolower((unsigned char)text[at + 1]) == 'e' &&
                       tolower((unsigned char)text[at + 2]) == 't' && text[at + 3] == '('
                   ? UD_DICTIONARY_SET
                   : UD_DICTIONARY_NONE;
    if (!kind)
        return UD_END;
    if (declaration.length > 16777216 || memchr(text, 0, declaration.length))
        return UD_RESOURCE;
    at += kind == UD_DICTIONARY_ENUM ? 5 : 4;
    ud_bytes json = {.ctx = ctx}, label = {.ctx = ctx};
    const char *prefix = kind == UD_DICTIONARY_ENUM ? "{\"kind\":\"enum\",\"values\":["
                                                    : "{\"kind\":\"set\",\"values\":[";
    ud_status status = ud_bytes_put(&json, prefix, strlen(prefix));
    uint32_t count = 0;
    while (!status) {
        if (at == declaration.length || text[at++] != '\'') {
            status = UD_SCHEMA;
            break;
        }
        label.size = 0;
        int closed = 0;
        while (at < declaration.length) {
            char ch = text[at++];
            if (ch == '\'') {
                if (at < declaration.length && text[at] == '\'')
                    at++;
                else {
                    closed = 1;
                    break;
                }
            } else if (ch == '\\') {
                if (at == declaration.length) {
                    status = UD_SCHEMA;
                    break;
                }
                ch = text[at++];
                ch = ch == 'n'   ? '\n'
                     : ch == 'r' ? '\r'
                     : ch == 't' ? '\t'
                     : ch == 'b' ? '\b'
                     : ch == '0' ? '\0'
                     : ch == 'Z' ? 26
                                 : ch;
            }
            status = ud_bytes_put(&label, &ch, 1);
            if (status)
                break;
        }
        if (!status && (!closed || ++count > (kind == UD_DICTIONARY_SET ? 64u : 65535u)))
            status = UD_SCHEMA;
        if (!status && count > 1)
            status = ud_bytes_put(&json, ",", 1);
        if (!status)
            status = ud_json_quote(&json, (const char *)label.data, (uint32_t)label.size);
        if (status)
            break;
        if (at == declaration.length) {
            status = UD_SCHEMA;
            break;
        }
        if (text[at] == ')') {
            at++;
            break;
        }
        if (text[at++] != ',') {
            status = UD_SCHEMA;
            break;
        }
    }
    while (at < declaration.length && text[at] == ' ')
        at++;
    if (!status && at != declaration.length)
        status = UD_SCHEMA;
    if (!status)
        status = ud_bytes_put(&json, "]}", 2);
    ud_json *dictionary = NULL;
    if (!status)
        status =
            ud_json_parse(ctx, (const char *)json.data, (uint32_t)json.size, &dictionary, NULL);
    if (!status) {
        field->type = UD_UINT64;
        status = ud_dictionary_parse(ctx, dictionary, field);
    }
    ud_json_destroy(dictionary);
    ud_bytes_clear(&json);
    ud_bytes_clear(&label);
    return status;
}
ud_status ud_schema_from_metadata(ud_context *ctx, const ud_batch *metadata, ud_string scope, ud_string physical_scope,
                                  int32_t dialect, ud_batch **out) {
    if (!ctx || !metadata || !out || metadata->columns < 4 || !metadata->rows || dialect < 0 ||
        dialect > UD_SQL_DUCKDB)
        return UD_INVALID;
    *out = NULL;
    ud_field *fields = ud_alloc(ctx, (size_t)metadata->rows * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    int physical_column=-1;
    for (uint32_t at=4;at<metadata->columns;at++) {
        const ud_field *field=&metadata->column[at].field;
        if (field->name_length==17 && !memcmp(field->name,"physical_identity",17)) {
            if (physical_column>=0 || field->type!=UD_UTF8) { ud_free(fields);return UD_SCHEMA; }
            physical_column=(int)at;
        }
    }
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
            status =
                dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB
                    ? sql_dictionary(ctx, (ud_string){type.text, type.text_length}, fields + row)
                    : UD_END;
            if (status == UD_END)
                status = sql_type(fields + row, (ud_string){type.text, type.text_length}, dialect);
        }
        if (!status && physical_column>=0) {
            ud_value physical={0};
            status=ud_batch_get(metadata,row,(uint32_t)physical_column,&physical);
            if (!status && !physical.is_null)
                status=ud_id_make(ctx,physical_scope,(ud_string){physical.text,physical.text_length},&fields[row].physical_identity);
        }
    }
    if (!status)
        status = ud_batch_create_named(ctx, fields, metadata->rows, 0, scope, out);
    for (uint32_t row = 0; row < metadata->rows; row++)
        ud_dictionary_clear(fields + row);
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
static int field_index(const ud_field *fields,uint32_t count,ud_id id) {
    for (uint32_t at=0;at<count;at++) if (!ud_id_compare(fields[at].identity,id)) return (int)at;
    return -1;
}
static unsigned scalar_width(uint32_t kind) {
    return kind >= UD_DT_I1 && kind <= UD_DT_I8     ? 8u << (kind - UD_DT_I1)
           : kind >= UD_DT_UI1 && kind <= UD_DT_UI8 ? 8u << (kind - UD_DT_UI1)
                                                    : 64;
}
static int scalar_lossless(const ud_field *a, const ud_field *b) {
    uint32_t x = a->scalar.kind, y = b->scalar.kind;
    if (!y)
        return 1;
    if (!x)
        return 0;
    if (a->scalar.locale_id != b->scalar.locale_id ||
        a->scalar.comparison_flags != b->scalar.comparison_flags)
        return 0;
    if (a->type == UD_INT64 || a->type == UD_UINT64)
        return scalar_width(x) <= scalar_width(y) &&
               ((x == UD_DT_FILETIME) == (y == UD_DT_FILETIME));
    if (a->type == UD_FLOAT64)
        return (x == y) || (x == UD_DT_R4 && y == UD_DT_R8);
    if (a->type == UD_UTF8)
        return a->scalar.code_page == b->scalar.code_page &&
               (!b->scalar.length || (a->scalar.length && a->scalar.length <= b->scalar.length));
    if (a->type == UD_BINARY)
        return (x == UD_DT_GUID) == (y == UD_DT_GUID) &&
               (!b->scalar.length || (a->scalar.length && a->scalar.length <= b->scalar.length));
    if (a->type == UD_DATETIME)
        return (x == UD_DT_DBTIMESTAMPOFFSET) == (y == UD_DT_DBTIMESTAMPOFFSET) &&
               ((x == UD_DT_DBTIME || x == UD_DT_DBTIME2) ==
                (y == UD_DT_DBTIME || y == UD_DT_DBTIME2)) &&
               (y != UD_DT_DBDATE || x == UD_DT_DBDATE) &&
               a->scalar.fractional_digits <= b->scalar.fractional_digits;
    return a->type == UD_DECIMAL128 || x == y;
}
static int lossless(const ud_field *actual, const ud_field *expected) {
    if (!scalar_lossless(actual, expected))
        return 0;
    int widening = expected->type == UD_DECIMAL128 && expected->scale >= 0 &&
                   ((actual->type == UD_UINT64 && expected->precision - expected->scale >= 20) ||
                    (actual->type == UD_INT64 && expected->precision - expected->scale >= 19));
    if ((!widening && actual->type != expected->type) ||
        (actual->nullable && !expected->nullable) || actual->key_order != expected->key_order ||
        ((expected->physical_identity.high || expected->physical_identity.low) &&
         ud_id_compare(actual->physical_identity,expected->physical_identity)))
        return 0;
    if (actual->dictionary_kind != expected->dictionary_kind ||
        actual->dictionary_count != expected->dictionary_count)
        return 0;
    for (uint32_t j = 0; j < actual->dictionary_count; j++)
        if (actual->dictionary[j].length != expected->dictionary[j].length ||
            memcmp(actual->dictionary[j].data, expected->dictionary[j].data,
                   actual->dictionary[j].length))
            return 0;
    if (actual->type == UD_DECIMAL128 &&
        (expected->scale < actual->scale ||
         expected->precision - expected->scale < actual->precision - actual->scale))
        return 0;
    if (actual->type == UD_TIMESTAMP &&
        (actual->timezone_length != expected->timezone_length ||
         memcmp(actual->timezone, expected->timezone, actual->timezone_length)))
        return 0;
    for (uint32_t j=0;j<expected->child_count;j++) {
        int at=expected->type==UD_STRUCT ? field_index(actual->children,actual->child_count,expected->children[j].identity) : (int)j;
        if (at<0 || (uint32_t)at>=actual->child_count || !lossless(actual->children+at,expected->children+j)) return 0;
    }
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
    int strict = word(mode, "strict"), evolution = word(mode, "evolutionMapping"),
        explicit_mapping = word(mode, "explicitMapping") || evolution;
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
            if (!explicit_mapping || evolution)
                *compatible = 0;
        }
        if (!status && (field->precision != now->precision || field->scale != now->scale))
            status = change(changes, &first, "PrecisionChanged", field, (int)j, at);
        if (!status && !ud_field_matches(field, now, UD_FIELD_VALUE_TYPE))
            status = change(changes, &first, "TypeChanged", field, (int)j, at);
        if (!status && (field->child_count || now->child_count))
            status = nested_changes(changes, &first, field, now, 0);
        if (strict ? !ud_field_matches(field, now, UD_FIELD_PORT)
                   : (!explicit_mapping || evolution) && !lossless(now, field))
            *compatible = 0;
    }
    for (uint32_t j = 0; !status && j < actual->columns; j++)
        if (!used[j]) {
            status = change(changes, &first, "Added", &actual->column[j].field, -1, (int)j);
            if (strict || actual->column[j].field.key_order)
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
static void binding_fields_clear(ud_field *fields,uint32_t count) {
    if (!fields) return;
    for (uint32_t at=0;at<count;at++) binding_fields_clear((ud_field *)fields[at].children,fields[at].child_count);
    ud_free(fields);
}
static ud_status bind_fields(ud_context *ctx,const ud_field *bound,uint32_t prior_count,
                              const ud_field *discovered,uint32_t count,ud_field **out,uint32_t depth) {
    *out=NULL;
    if (depth>32) return UD_SCHEMA;
    if (!count) return UD_OK;
    *out=ud_alloc(ctx,count*sizeof(**out));
    if (count && !*out) return UD_RESOURCE;
    for (uint32_t at=0;at<count;at++) {
        ud_field *field=*out+at;
        *field=discovered[at];field->children=NULL;field->child_count=0;
        const ud_field *before=NULL;
        for (uint32_t prior=0;prior<prior_count;prior++) {
            int known=bound[prior].physical_identity.high || bound[prior].physical_identity.low;
            int match=known ? !ud_id_compare(bound[prior].physical_identity,field->physical_identity) :
                (!ud_id_compare(bound[prior].identity,field->identity) ||
                 (bound[prior].name_length==field->name_length && !memcmp(bound[prior].name,field->name,field->name_length)));
            if (match) {
                if (before) return UD_SCHEMA;
                before=bound+prior;
            }
        }
        if (before) { field->identity=before->identity;field->lineage=before->lineage;field->lineage_count=before->lineage_count; }
        ud_field *children=NULL;
        ud_status status=bind_fields(ctx,before ? before->children : NULL,before ? before->child_count : 0,
            discovered[at].children,discovered[at].child_count,&children,depth+1);
        field->children=children;field->child_count=discovered[at].child_count;
        if (status) return status;
    }
    return UD_OK;
}
ud_status ud_schema_bind_identity(ud_context *ctx, const ud_batch *bound,
                                  const ud_batch *discovered, ud_batch **out) {
    if (!ctx || !bound || !discovered || !out || bound->ctx != ctx || discovered->ctx != ctx)
        return UD_INVALID;
    *out = NULL;
    ud_field *fields=NULL;
    ud_status status=bind_fields(ctx,bound->schema->fields,bound->columns,discovered->schema->fields,discovered->columns,&fields,0);
    if (!status) status=ud_batch_create(ctx,fields,discovered->columns,0,out);
    binding_fields_clear(fields,fields ? discovered->columns : 0);
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
typedef struct schema_projection {
    const ud_field *source,*target;
    uint32_t index,count;
    struct schema_projection *children;
} schema_projection;
static void projection_clear(schema_projection *plan) {
    for (uint32_t at=0;at<plan->count;at++) projection_clear(plan->children+at);
    ud_free(plan->children);
    plan->children=NULL;plan->count=0;
}
static ud_status projection_bind(ud_context *ctx,const ud_field *source,const ud_field *target,
                                  schema_projection *plan,uint32_t depth) {
    if (depth>32) return UD_SCHEMA;
    plan->source=source;plan->target=target;
    if (source->type!=target->type || target->type<UD_STRUCT) return UD_OK;
    plan->children=ud_alloc(ctx,target->child_count*sizeof(*plan->children));
    if (target->child_count && !plan->children) return UD_RESOURCE;
    plan->count=target->child_count;
    for (uint32_t at=0;at<target->child_count;at++) {
        int index=target->type==UD_STRUCT ? field_index(source->children,source->child_count,target->children[at].identity) : (int)at;
        if (index<0 || (uint32_t)index>=source->child_count) return UD_SCHEMA;
        plan->children[at].index=(uint32_t)index;
        UD_TRY(projection_bind(ctx,source->children+index,target->children+at,plan->children+at,depth+1));
    }
    return UD_OK;
}
static ud_status projection_apply(ud_context *ctx,const schema_projection *plan,const ud_value *value,ud_value *out) {
    if (!plan->count || value->is_null || value->is_absent)
        return ud_cast_value(ctx,plan->source,value,plan->target,UD_ROUND_EXACT,out,0);
    if (value->type!=plan->source->type || (value->child_count && !value->children) ||
        (value->type==UD_STRUCT && value->child_count!=plan->source->child_count)) return UD_SCHEMA;
    uint32_t count=value->type==UD_STRUCT ? plan->count : value->child_count;
    ud_value *children=ud_alloc(ctx,count*sizeof(*children));
    if (count && !children) return UD_RESOURCE;
    *out=(ud_value){.type=plan->target->type,.children=children,.child_count=count};
    for (uint32_t at=0;at<count;at++) {
        const schema_projection *child=plan->children+(value->type==UD_STRUCT ? at : 0);
        uint32_t index=value->type==UD_STRUCT ? child->index : at;
        UD_TRY(projection_apply(ctx,child,value->children+index,children+at));
    }
    return UD_OK;
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
    schema_projection *plans=!status ? ud_alloc(ctx,expected->columns*sizeof(*plans)) : NULL;
    if (!status && (!fields || !values || !plans))
        status = UD_RESOURCE;
    for (uint32_t j = 0; !status && j < expected->columns; j++) {
        fields[j] = expected->column[j].field;
        status=projection_bind(ctx,&actual->column[mapping[j]].field,fields+j,plans+j,0);
    }
    if (!status)
        status = ud_batch_create(ctx, fields, expected->columns, actual->rows, out);
    for (uint32_t row = 0; !status && row < actual->rows; row++) {
        for (uint32_t j = 0; !status && j < expected->columns; j++) {
            ud_value value;
            status = ud_batch_get(actual, row, mapping[j], &value);
            if (!status)
                status = projection_apply(ctx,plans+j,&value,values+j);
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
    for (uint32_t at=0;plans && at<expected->columns;at++) projection_clear(plans+at);
    ud_free(plans);
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
