#include "mysql_internal.h"
#include <ctype.h>
#include <stdatomic.h>
#include <stdio.h>
#include <math.h>
#include "../components/foundation/thread.h"
#define UD_MYSQL_CELL_LIMIT (16u * 1024u * 1024u)
ud_status ud_mysql_library_initialize(void) {
    return mysql_library_init(0, NULL, NULL) ? UD_RESOURCE : UD_OK;
}
void ud_mysql_library_shutdown(void) {
    mysql_thread_end();
    mysql_library_end();
}
static ud_status check(mysql_state *p) {
    if (p->cleanup)
        return UD_OK;
    if (atomic_load(&p->cancelled) || p->host.cancelled(p->host.context))
        return UD_CANCELLED;
    return p->check ? p->check(p->check_user) : UD_OK;
}
static ud_status classify_failure(mysql_state *p, unsigned int code, const char *state) {
    if (!p->cleanup && atomic_load(&p->cancelled))
        return UD_CANCELLED;
    if (code == 2002 || code == 2003 || code == 2005 || code == 2006 || code == 2013 ||
        code == 2055) {
        p->broken = 1;
        return UD_TRANSIENT;
    }
    if (code == 1045 || code == 2026 || (state && !strncmp(state, "28", 2)))
        return UD_AUTH;
    if (code == 1205)
        return UD_TIMEOUT;
    if (code == 1213 || (state && !strcmp(state, "40001")))
        return UD_TRANSIENT;
    if (state && !strncmp(state, "23", 2))
        return UD_QUALITY;
    if (code == 1406 || (state && !strcmp(state, "22001")))
        return UD_TRUNCATED;
    if (code == 1264 || (state && !strcmp(state, "22003")))
        return UD_OVERFLOW;
    if (state && !strncmp(state, "22", 2))
        return UD_PARSE;
    if (state && !strncmp(state, "42", 2))
        return UD_SCHEMA;
    return UD_IO;
}
static ud_status report_failure(mysql_state *p, unsigned int code, const char *state) {
    p->record_code = code;
    ud_status status = classify_failure(p, code, state);
    return plugin_db_error(&p->host, status, code, state, "SQL provider operation failed");
}
static ud_status failure(mysql_state *p, MYSQL_STMT *stmt) {
    return report_failure(p, stmt ? mysql_stmt_errno(stmt) : mysql_errno(p->db),
                          stmt ? mysql_stmt_sqlstate(stmt) : mysql_sqlstate(p->db));
}
static ud_status record_diagnostic(void *state, ud_string *out) {
    mysql_state *p = state;
    char code[32];
    int length = p->record_code ? snprintf(code, sizeof(code), "%u", p->record_code) : 0;
    *out = (ud_string){length ? plugin_string(&p->host, code, (uint32_t)length) : NULL,
                       (uint32_t)length};
    if (length && !out->data)
        return UD_RESOURCE;
    p->record_code = 0;
    return UD_OK;
}
static void bindings_free(bindings *b) {
    if (!b->state)
        return;
    for (uint32_t i = 0; i < b->count; i++)
        b->state->host.deallocate(b->owned ? b->owned[i] : NULL);
    b->state->host.deallocate(b->owned);
    b->state->host.deallocate(b->values);
    b->state->host.deallocate(b->bind);
    b->state->host.deallocate(b->length);
    b->state->host.deallocate(b->nulls);
    memset(b, 0, sizeof(*b));
}
static ud_status bindings_create(mysql_state *p, uint32_t n, bindings *b) {
    *b = (bindings){.state = p, .count = n};
    if (!n)
        return UD_OK;
    b->bind = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->bind));
    b->length = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->length));
    b->nulls = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->nulls));
    b->owned = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->owned));
    b->values = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->values));
    if (!b->bind || !b->length || !b->nulls || !b->owned || !b->values) {
        bindings_free(b);
        return UD_RESOURCE;
    }
    return UD_OK;
}
static ud_status bind_value(bindings *b, uint32_t at, ud_value v) {
    if (at >= b->count)
        return UD_SCHEMA;
    mysql_state *p = b->state;
    b->values[at] = v;
    b->nulls[at] = !!v.is_null;
    MYSQL_BIND *bind = b->bind + at;
    bind->is_null = b->nulls + at;
    bind->length = b->length + at;
    if (v.is_null) {
        bind->buffer_type = MYSQL_TYPE_NULL;
        return UD_OK;
    }
    if (v.type == UD_INT64 || v.type == UD_UINT64 || v.type == UD_BOOL) {
        if (v.type == UD_BOOL && v.integer != 0 && v.integer != 1)
            return UD_SCHEMA;
        bind->is_unsigned = v.type == UD_UINT64;
        bind->buffer_type = MYSQL_TYPE_LONGLONG;
        bind->buffer = &b->values[at].integer;
        bind->buffer_length = sizeof(int64_t);
        return UD_OK;
    }
    if (v.type == UD_FLOAT64) {
        if (!isfinite(v.real))
            return UD_UNSUPPORTED;
        bind->buffer_type = MYSQL_TYPE_DOUBLE;
        bind->buffer = &b->values[at].real;
        bind->buffer_length = sizeof(double);
        return UD_OK;
    }
    if (v.type == UD_UTF8 || v.type == UD_BINARY) {
        bind->buffer_type = v.type == UD_BINARY ? MYSQL_TYPE_BLOB : MYSQL_TYPE_STRING;
        bind->buffer = (void *)v.text;
        bind->buffer_length = b->length[at] = v.text_length;
        return UD_OK;
    }
    if (v.type == UD_DECIMAL128 || v.type == UD_DATE32 || v.type == UD_DURATION ||
        v.type == UD_TIME64 || v.type == UD_TIMESTAMP) {
        char value[160];
        uint32_t n = 0;
        ud_status status = p->host.format_value(&v, value, sizeof(value), &n);
        if (status)
            return status;
        if (!n || n > sizeof(value))
            return UD_RESOURCE;
        n--;
        if (v.type == UD_TIMESTAMP) {
            if (n < 20 || value[n - 1] != 'Z')
                return UD_SCHEMA;
            value[10] = ' ';
            value[--n] = 0;
        }
        b->owned[at] = plugin_string(&p->host, value, n);
        if (!b->owned[at])
            return UD_RESOURCE;
        bind->buffer_type = MYSQL_TYPE_STRING;
        bind->buffer = b->owned[at];
        bind->buffer_length = b->length[at] = n;
        return UD_OK;
    }
    return UD_UNSUPPORTED;
}
static ud_status bind_batch(bindings *b, uint32_t offset, const ud_batch *batch, uint32_t row,
                            const uint32_t *columns, uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        ud_value value;
        ud_status s = b->state->host.get(batch, row, columns ? columns[i] : i, &value);
        if (!s)
            s = bind_value(b, offset + i, value);
        if (s)
            return s;
    }
    return UD_OK;
}
static ud_status statement(mysql_state *p, ud_string sql, MYSQL_STMT **out) {
    *out = NULL;
    ud_status s = check(p);
    if (s)
        return s;
    if (!sql.data || !sql.length || memchr(sql.data, 0, sql.length))
        return UD_SCHEMA;
    MYSQL_STMT *stmt = mysql_stmt_init(p->db);
    if (!stmt)
        return UD_RESOURCE;
    if (mysql_stmt_prepare(stmt, sql.data, sql.length)) {
        s = failure(p, stmt);
        mysql_stmt_close(stmt);
        return s;
    }
    *out = stmt;
    return UD_OK;
}
static ud_status execute_bound(mysql_state *p, MYSQL_STMT *stmt, bindings *b, uint64_t *affected) {
    ud_status s = check(p);
    if (s)
        return s;
    if (mysql_stmt_param_count(stmt) != b->count)
        return UD_SCHEMA;
    if (b->count && mysql_stmt_bind_param(stmt, b->bind))
        return failure(p, stmt);
    if (mysql_stmt_execute(stmt))
        return failure(p, stmt);
    if (affected)
        *affected = (uint64_t)mysql_stmt_affected_rows(stmt);
    return check(p);
}
ud_status ud_mysql_check(mysql_state *p) {
    return check(p);
}
ud_status ud_mysql_failure(mysql_state *p, MYSQL_STMT *stmt) {
    return failure(p, stmt);
}
void ud_mysql_bindings_free(bindings *b) {
    bindings_free(b);
}
ud_status ud_mysql_bindings_create(mysql_state *p, uint32_t n, bindings *b) {
    return bindings_create(p, n, b);
}
ud_status ud_mysql_bind_value(bindings *b, uint32_t at, ud_value value) {
    return bind_value(b, at, value);
}
ud_status ud_mysql_statement(mysql_state *p, ud_string sql, MYSQL_STMT **out) {
    return statement(p, sql, out);
}
ud_status ud_mysql_execute_bound(mysql_state *p, MYSQL_STMT *stmt, bindings *b,
                                 uint64_t *affected) {
    return execute_bound(p, stmt, b, affected);
}
static ud_status command(mysql_state *p, const char *sql) {
    ud_status s = check(p);
    if (!s && mysql_real_query(p->db, sql, (unsigned long)strlen(sql)))
        s = failure(p, NULL);
    return s;
}
static ud_status session_policy(mysql_state *p) {
    if (mysql_set_character_set(p->db, "utf8mb4"))
        return failure(p, NULL);
    ud_status s = command(p, "SET SESSION time_zone='+00:00'");
    if (!s)
        s = command(
            p,
            "SET SESSION "
            "sql_mode='STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'");
    return s;
}
static ud_status read_close(void *state) {
    mysql_state *p = state;
    memset(&p->record_options, 0, sizeof(p->record_options));
    if (p->cursor)
        mysql_stmt_close(p->cursor);
    p->cursor = NULL;
    p->scalar = 0;
    p->host.batch_destroy(p->schema);
    p->schema = NULL;
    if (p->cursor_readonly) {
        p->cursor_readonly = 0;
        return command(p, "ROLLBACK");
    }
    return UD_OK;
}
static ud_status result_field(const MYSQL_FIELD *column, ud_field *field) {
    *field = (ud_field){.name = column->name, .name_length = column->name_length,
                        .nullable = (column->flags & NOT_NULL_FLAG) == 0, .type = UD_UTF8};
    switch (column->type) {
    case MYSQL_TYPE_TINY:
        if (column->length == 1) {
            field->type = UD_BOOL;
            break;
        }
        /* Other integer display widths have the same signed or unsigned domain. */
        /* fall through */
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_YEAR:
        field->type = column->flags & UNSIGNED_FLAG ? UD_UINT64 : UD_INT64;
        break;
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
        field->type = UD_FLOAT64;
        break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL: {
        unsigned long formatting = (column->decimals != 0) + !(column->flags & UNSIGNED_FLAG);
        if (column->length <= formatting || column->length - formatting > 38)
            return UD_UNSUPPORTED;
        field->type = UD_DECIMAL128;
        field->precision = (int32_t)(column->length - formatting);
        field->scale = (int32_t)column->decimals;
        break;
    }
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
        field->type = UD_DATE32;
        break;
    case MYSQL_TYPE_TIME:
        field->type = UD_TIME64;
        break;
    case MYSQL_TYPE_DATETIME:
        field->type = UD_DATETIME;
        break;
    case MYSQL_TYPE_TIMESTAMP:
        field->type = UD_TIMESTAMP;
        break;
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
        if (column->charsetnr == 63) /* The binary character set, including VARBINARY. */
            field->type = UD_BINARY;
        break;
    default:
        break;
    }
    return UD_OK;
}
static ud_status read_open(void *state, const ud_read_options *o, const ud_batch *schema) {
    mysql_state *p = state;
    if (p->cursor)
        return UD_BUSY;
    MYSQL_STMT *stmt = NULL;
    ud_status s = !p->transaction ? command(p, "START TRANSACTION READ ONLY") : UD_OK;
    if (!s && !p->transaction)
        p->cursor_readonly = 1;
    if (!s)
        s = statement(p, o->sql, &stmt);
    unsigned long cursor = CURSOR_TYPE_READ_ONLY, prefetch = o->batch_size;
    if (!s && (mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &cursor) ||
               mysql_stmt_attr_set(stmt, STMT_ATTR_PREFETCH_ROWS, &prefetch)))
        s = failure(p, stmt);
    if (!s && !mysql_stmt_field_count(stmt))
        s = UD_INVALID;
    bindings b = {0};
    uint32_t count = o->parameters ? p->host.columns(o->parameters) : 0;
    if (!s)
        s = bindings_create(p, count, &b);
    if (!s && count)
        s = bind_batch(&b, 0, o->parameters, o->parameter_row, NULL, count);
    if (!s)
        s = execute_bound(p, stmt, &b, NULL);
    bindings_free(&b);
    MYSQL_RES *meta = !s ? mysql_stmt_result_metadata(stmt) : NULL;
    uint32_t columns = meta ? mysql_num_fields(meta) : 0;
    if (!s && (!meta || (schema && columns != p->host.columns(schema))))
        s = UD_SCHEMA;
    MYSQL_FIELD *actual = meta ? mysql_fetch_fields(meta) : NULL;
    ud_field *fields = NULL;
    if (!s && schema)
        s = plugin_fields(&p->host, schema, &fields);
    if (!s && !schema) {
        fields = p->host.allocate(p->host.context, (size_t)columns * sizeof(*fields));
        if (!fields && columns)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < columns; i++)
            s = result_field(actual + i, fields + i);
    }
    if (!s && o->result_mode == UD_READ_SCALAR && columns != 1)
        s = UD_SCHEMA;
    for (uint32_t i = 0; !s && i < columns; i++) {
        if (fields[i].type >= UD_STRUCT || fields[i].name_length != actual[i].name_length ||
            memcmp(fields[i].name, actual[i].name, fields[i].name_length))
            s = UD_SCHEMA;
        if (!s && fields[i].dictionary_kind &&
            (actual[i].type != MYSQL_TYPE_LONGLONG || !(actual[i].flags & UNSIGNED_FLAG)))
            s = UD_INCOMPATIBLE;
        if (!s && fields[i].type == UD_TIMESTAMP && !p->utc &&
            actual[i].type == MYSQL_TYPE_DATETIME)
            s = UD_UNSUPPORTED;
    }
    if (!s)
        s = plugin_schema_outputs(&p->host, fields, columns, &p->schema, o->result_schema);
    p->host.deallocate(fields);
    if (meta)
        mysql_free_result(meta);
    if (s) {
        if (stmt)
            mysql_stmt_close(stmt);
        if (p->cursor_readonly) {
            p->cursor_readonly = 0;
            ud_status closed = command(p, "ROLLBACK");
            if (closed)
                return closed;
        }
        return s;
    }
    p->cursor = stmt;
    p->scalar = o->result_mode == UD_READ_SCALAR;
    p->batch_size = p->scalar ? 1 : o->batch_size;
    p->record_options = *o;
    return UD_OK;
}
static ud_status read_next(void *state, ud_batch **out) {
    mysql_state *p = state;
    *out = NULL;
    if (!p->cursor || !p->schema)
        return UD_END;
    uint32_t n = p->host.columns(p->schema);
    bindings b = {0};
    ud_status s = bindings_create(p, n, &b);
    ud_field *fields = NULL;
    if (!s)
        s = plugin_fields(&p->host, p->schema, &fields);
    for (uint32_t i = 0; !s && i < n; i++) {
        b.owned[i] = p->host.allocate(p->host.context, 257);
        if (!b.owned[i]) {
            s = UD_RESOURCE;
            break;
        }
        b.bind[i].buffer_type = fields[i].type == UD_BINARY ? MYSQL_TYPE_BLOB : MYSQL_TYPE_STRING;
        b.bind[i].buffer = b.owned[i];
        b.bind[i].buffer_length = 256;
        b.bind[i].length = b.length + i;
        b.bind[i].is_null = b.nulls + i;
    }
    if (!s && mysql_stmt_bind_result(p->cursor, b.bind))
        s = failure(p, p->cursor);
    if (!s)
        s = p->host.batch_create(p->host.context, fields, n, p->batch_size, out);
    int ended = 0;
    uint32_t fetched = 0, bad = UINT32_MAX;
    ud_value *raw = p->host.allocate(p->host.context, n * sizeof(*raw));
    if (!raw)
        s = UD_RESOURCE;
    while (!s && fetched < p->batch_size) {
        s = check(p);
        if (s)
            break;
        int rc = mysql_stmt_fetch(p->cursor);
        if (rc == MYSQL_NO_DATA) {
            ended = 1;
            break;
        }
        if (rc && rc != MYSQL_DATA_TRUNCATED) {
            s = failure(p, p->cursor);
            break;
        }
        fetched++;
        ud_status conversion = UD_OK;
        memset(raw, 0, n * sizeof(*raw));
        for (uint32_t i = 0; !s && i < n; i++) {
            ud_value *v = b.values + i;
            *v = (ud_value){.type = fields[i].type,
                            .precision = fields[i].precision,
                            .scale = fields[i].scale,
                            .is_null = !!b.nulls[i]};
            raw[i] = (ud_value){.type = UD_BINARY, .is_null = v->is_null};
            if (v->is_null)
                continue;
            if (b.length[i] > UD_MYSQL_CELL_LIMIT) {
                s = UD_RESOURCE;
                break;
            }
            if (b.length[i] > b.bind[i].buffer_length) {
                char *next = p->host.allocate(p->host.context, (size_t)b.length[i] + 1);
                if (!next) {
                    s = UD_RESOURCE;
                    break;
                }
                p->host.deallocate(b.owned[i]);
                b.owned[i] = next;
                b.bind[i].buffer = next;
                b.bind[i].buffer_length = b.length[i];
                if (mysql_stmt_fetch_column(p->cursor, b.bind + i, i, 0)) {
                    s = failure(p, p->cursor);
                    break;
                }
            }
            raw[i].text = b.owned[i];
            raw[i].text_length = (uint32_t)b.length[i];
            if (v->type == UD_BINARY) {
                v->text = b.owned[i];
                v->text_length = (uint32_t)b.length[i];
            } else if (v->type == UD_TIMESTAMP) {
                if (b.length[i] < 19 || b.length[i] > 40 || !memcmp(b.owned[i], "0000-00-00", 10)) {
                    s = UD_PARSE;
                }
                char time[48];
                if (!s) {
                    memcpy(time, b.owned[i], b.length[i]);
                    time[10] = 'T';
                    time[b.length[i]] = 'Z';
                    s = p->host.parse_field(p->host.context, fields + i, time,
                                            (uint32_t)b.length[i] + 1, UD_ROUND_EXACT, v);
                }
            } else if (v->type == UD_BOOL) {
                if (b.length[i] != 1 || (b.owned[i][0] != '0' && b.owned[i][0] != '1'))
                    s = UD_PARSE;
                else
                    v->integer = b.owned[i][0] - '0';
            } else
                s = p->host.parse_field(p->host.context, fields + i, b.owned[i],
                                        (uint32_t)b.length[i], UD_ROUND_EXACT, v);
            if (plugin_record_conversion(s)) {
                v->is_absent = 1;
                v->is_null = 1;
                if (!conversion) {
                    conversion = s;
                    bad = i;
                }
                s = UD_OK;
            }
        }
        if (!s)
            s = plugin_record_finish(&p->host, &p->record_options, *out, fields, b.values, raw, n,
                                     conversion, bad);
        /* Bind pointers are refreshed after a truncated column grows. */
        if (!s && mysql_stmt_bind_result(p->cursor, b.bind))
            s = failure(p, p->cursor);
    }
    p->host.deallocate(raw);
    p->host.deallocate(fields);
    bindings_free(&b);
    if (s || (ended && !p->host.rows(*out) && !fetched)) {
        p->host.batch_destroy(*out);
        *out = NULL;
        read_close(p);
        return s ? s : UD_END;
    }
    if (p->scalar && fetched)
        ended = 1;
    if (ended) {
        mysql_stmt_close(p->cursor);
        p->cursor = NULL;
    }
    return UD_OK;
}
static ud_status parameters(mysql_state *p, const ud_string *values, uint32_t count,
                            ud_batch **out) {
    ud_field f[8];
    ud_value v[8];
    if (count > 8)
        return UD_INVALID;
    memset(f, 0, sizeof(f));
    memset(v, 0, sizeof(v));
    char names[8][8];
    for (uint32_t i = 0; i < count; i++) {
        snprintf(names[i], sizeof(names[i]), "p%u", i);
        f[i] = (ud_field){
            .name = names[i], .name_length = (uint32_t)strlen(names[i]), .type = UD_UTF8};
        v[i] = (ud_value){.type = UD_UTF8, .text = values[i].data, .text_length = values[i].length};
    }
    ud_status s = p->host.batch_create(p->host.context, f, count, 1, out);
    if (!s)
        s = p->host.append(*out, v, count);
    return s;
}
static ud_status query(mysql_state *p, const char *sql, const ud_string *parameters_in,
                       uint32_t count, const ud_field *fields, uint32_t n, ud_batch **out) {
    if (p->cursor)
        return UD_BUSY;
    ud_batch *schema = NULL, *params = NULL;
    ud_status s = p->host.batch_create(p->host.context, fields, n, 0, &schema);
    if (!s && count)
        s = parameters(p, parameters_in, count, &params);
    ud_read_options o = {.struct_size = sizeof(o),
                         .abi_version = UD_ABI_VERSION,
                         .batch_size = 65536,
                         .sql = {sql, (uint32_t)strlen(sql)},
                         .parameters = params};
    if (!s)
        s = read_open(p, &o, schema);
    if (!s)
        s = read_next(p, out);
    if (s == UD_END) {
        s = p->host.batch_create(p->host.context, fields, n, 0, out);
    }
    read_close(p);
    p->host.batch_destroy(schema);
    p->host.batch_destroy(params);
    return s;
}
static ud_status table_parts(mysql_state *p, ud_string table, ud_string values[2]) {
    const char *dot = memchr(table.data, '.', table.length);
    values[0] = (ud_string){p->database, (uint32_t)strlen(p->database)};
    values[1] = table;
    if (dot) {
        values[0] = (ud_string){table.data, (uint32_t)(dot - table.data)};
        values[1] = (ud_string){dot + 1, table.length - (uint32_t)(dot - table.data) - 1};
    }
    return values[0].length && values[1].length ? UD_OK : UD_SCHEMA;
}
static ud_status prove_transactional_target(mysql_state *p, const ud_string args[2]) {
    ud_string identifier = {0};
    ud_status status = UD_OK;
    char *qualified = NULL, *locking = NULL;
    ud_batch *proof = NULL;
    /* Hold the provider's metadata lock until the Native row transaction ends.
     * Otherwise ALTER ENGINE/CREATE TRIGGER could invalidate an earlier proof. */
    if (!status) {
        size_t length = (size_t)args[0].length + args[1].length + 1;
        qualified = p->host.allocate(p->host.context, length + 1);
        if (!qualified)
            status = UD_RESOURCE;
        else {
            memcpy(qualified, args[0].data, args[0].length);
            qualified[args[0].length] = '.';
            memcpy(qualified + args[0].length + 1, args[1].data, args[1].length);
            status =
                p->host.table_identifier(p->host.context, UD_SQL_MYSQL,
                                         (ud_string){qualified, (uint32_t)length}, &identifier);
        }
    }
    if (!status) {
        locking = p->host.allocate(p->host.context, (size_t)identifier.length + 64);
        if (!locking)
            status = UD_RESOURCE;
        else {
            snprintf(locking, (size_t)identifier.length + 64,
                     "SELECT 1 AS ud_lock FROM %.*s LIMIT 0", (int)identifier.length,
                     identifier.data);
            ud_field field = {.name = "ud_lock", .name_length = 7, .type = UD_INT64};
            status = query(p, locking, NULL, 0, &field, 1, &proof);
        }
    }
    p->host.batch_destroy(proof);
    proof = NULL;
    ud_field fields[] = {{.name = "engine", .name_length = 6, .type = UD_UTF8},
                         {.name = "triggers", .name_length = 8, .type = UD_INT64},
                         {.name = "trigger_visibility", .name_length = 18, .type = UD_INT64}};
    /* TRIGGERS is privilege-filtered. Require a direct TRIGGER grant before
     * interpreting an empty result as absence. Schema grants store the provider's
     * database pattern, including escaped literal underscores and percent signs.
     * MySQL partial revokes make database names literal and can restrict a global
     * grant. In that mode only a direct schema or table grant proves visibility. */
    static const char proof_query[] =
        "SELECT t.ENGINE AS engine,(SELECT COUNT(*) FROM information_schema.TRIGGERS r "
        "WHERE r.TRIGGER_SCHEMA=t.TABLE_SCHEMA AND r.EVENT_OBJECT_TABLE=t.TABLE_NAME) AS triggers,"
        "(SELECT COUNT(*) FROM ("
        "SELECT GRANTEE,PRIVILEGE_TYPE,NULL AS scope_schema,NULL AS scope_table FROM "
        "information_schema.USER_PRIVILEGES "
        "UNION ALL SELECT GRANTEE,PRIVILEGE_TYPE,TABLE_SCHEMA,NULL FROM "
        "information_schema.SCHEMA_PRIVILEGES "
        "UNION ALL SELECT GRANTEE,PRIVILEGE_TYPE,TABLE_SCHEMA,TABLE_NAME FROM "
        "information_schema.TABLE_PRIVILEGES) ud_grants "
        "WHERE ud_grants.PRIVILEGE_TYPE='TRIGGER' AND CAST(ud_grants.GRANTEE AS BINARY)=CAST(CONCAT("
        "QUOTE(LEFT(CURRENT_USER(),LENGTH(CURRENT_USER())-LENGTH(SUBSTRING_INDEX(CURRENT_USER(),'@'"
        ",-1))-1)),"
        "'@',QUOTE(SUBSTRING_INDEX(CURRENT_USER(),'@',-1))) AS BINARY) "
        "AND CASE WHEN ud_grants.scope_schema IS NULL THEN %s "
        "WHEN ud_grants.scope_table IS NOT NULL THEN "
        "CAST(ud_grants.scope_schema AS BINARY)=CAST(t.TABLE_SCHEMA AS BINARY) AND "
        "CAST(ud_grants.scope_table AS BINARY)=CAST(t.TABLE_NAME AS BINARY) "
        "WHEN %s THEN CAST(t.TABLE_SCHEMA AS BINARY) LIKE "
        "CAST(ud_grants.scope_schema AS BINARY) ESCAPE 0x5c "
        "ELSE CAST(ud_grants.scope_schema AS BINARY)=CAST(t.TABLE_SCHEMA AS BINARY) END) AS "
        "trigger_visibility "
        "FROM information_schema.TABLES t WHERE t.TABLE_SCHEMA=? AND t.TABLE_NAME=?";
    if (!status) {
        const char *server = mysql_get_server_info(p->db);
        static const char without_partial_revokes[] = "@@GLOBAL.partial_revokes=0";
        const char *schema_patterns = server && !strstr(server, "MariaDB") &&
                                      mysql_get_server_version(p->db) >= 80016
                                          ? without_partial_revokes : "1";
        char sql[sizeof(proof_query) + 2 * sizeof(without_partial_revokes)];
        snprintf(sql, sizeof(sql), proof_query, schema_patterns, schema_patterns);
        status = query(p, sql, args, 2, fields, 3, &proof);
    }
    if (!status && p->host.rows(proof) != 1)
        status = UD_SCHEMA;
    ud_value engine = {0}, triggers = {0}, visible = {0};
    if (!status)
        status = p->host.get(proof, 0, 0, &engine);
    if (!status)
        status = p->host.get(proof, 0, 1, &triggers);
    if (!status)
        status = p->host.get(proof, 0, 2, &visible);
    if (!status && (visible.is_null || visible.integer <= 0))
        status = UD_AUTH;
    if (!status && (engine.is_null || engine.text_length != 6 || memcmp(engine.text, "InnoDB", 6) ||
                    triggers.is_null || triggers.integer))
        status = UD_UNSUPPORTED;
    p->host.batch_destroy(proof);
    p->host.deallocate((void *)identifier.data);
    p->host.deallocate(qualified);
    p->host.deallocate(locking);
    return status;
}
static ud_status record_validate(void *state, const ud_read_options *options) {
    mysql_state *p = state;
    ud_string schema = {0}, table = {0};
    ud_status status = p->host.record_target(p->host.context, options->sql, &schema, &table);
    if (!status) {
        ud_string args[] = {
            schema.length ? schema : (ud_string){p->database, (uint32_t)strlen(p->database)}, table};
        status = prove_transactional_target(p, args);
    }
    p->host.deallocate((void *)schema.data);
    p->host.deallocate((void *)table.data);
    return status;
}
static ud_status discover(void *state, ud_string table, ud_batch **out) {
    mysql_state *p = state;
    ud_string args[2];
    ud_status s = table_parts(p, table, args);
    ud_field f[] = {{.name = "name", .name_length = 4, .type = UD_UTF8},
                    {.name = "declared_type", .name_length = 13, .type = UD_UTF8},
                    {.name = "nullable", .name_length = 8, .type = UD_BOOL},
                    {.name = "key_order", .name_length = 9, .type = UD_INT64},
                    {.name = "default", .name_length = 7, .type = UD_UTF8, .nullable = 1},
                    {.name = "identity", .name_length = 8, .type = UD_BOOL}};
    const char *sql =
        p->utc
            ? "SELECT c.COLUMN_NAME AS "
              "name,IF(c.DATA_TYPE='datetime',CONCAT('datetime_utc(',c.DATETIME_PRECISION,')'),c."
              "COLUMN_TYPE) AS declared_type,(c.IS_NULLABLE='YES') AS "
              "nullable,COALESCE(k.ORDINAL_POSITION,0) AS key_order,c.COLUMN_DEFAULT AS "
              "`default`,(c.EXTRA LIKE '%auto_increment%') AS identity FROM "
              "information_schema.COLUMNS c LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON "
              "k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND "
              "k.COLUMN_NAME=c.COLUMN_NAME AND k.CONSTRAINT_NAME='PRIMARY' WHERE c.TABLE_SCHEMA=? "
              "AND c.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION"
            : "SELECT c.COLUMN_NAME AS name,c.COLUMN_TYPE AS declared_type,(c.IS_NULLABLE='YES') "
              "AS nullable,COALESCE(k.ORDINAL_POSITION,0) AS key_order,c.COLUMN_DEFAULT AS "
              "`default`,(c.EXTRA LIKE '%auto_increment%') AS identity FROM "
              "information_schema.COLUMNS c LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON "
              "k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND "
              "k.COLUMN_NAME=c.COLUMN_NAME AND k.CONSTRAINT_NAME='PRIMARY' WHERE c.TABLE_SCHEMA=? "
              "AND c.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION";
    return s ? s : query(p, sql, args, 2, f, 6, out);
}
static ud_status metadata(void *state, const ud_metadata_options *o, ud_batch **out) {
    mysql_state *p = state;
    if (o->kind == UD_METADATA_COLUMNS)
        return discover(p, o->parent, out);
    ud_field f[] = {{.name = "name", .name_length = 4, .type = UD_UTF8},
                    {.name = "parent", .name_length = 6, .type = UD_UTF8},
                    {.name = "definition", .name_length = 10, .type = UD_UTF8}};
    ud_string args[2];
    uint32_t count = 0;
    const char *sql = NULL;
    if (o->kind == UD_METADATA_CATALOGS || o->kind == UD_METADATA_SCHEMAS)
        sql = "SELECT SCHEMA_NAME AS name,'' AS parent,DEFAULT_CHARACTER_SET_NAME AS definition "
              "FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME";
    else if (o->kind == UD_METADATA_TABLES) {
        args[0] = o->parent;
        count = 1;
        sql = "SELECT TABLE_NAME AS name,TABLE_SCHEMA AS parent,COALESCE(ENGINE,TABLE_TYPE) AS "
              "definition FROM information_schema.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME";
    } else {
        ud_status s = table_parts(p, o->parent, args);
        if (s)
            return s;
        count = 2;
        if (o->kind == UD_METADATA_PRIMARY_KEYS)
            sql = "SELECT COLUMN_NAME AS name,TABLE_NAME AS parent,CAST(ORDINAL_POSITION AS CHAR) "
                  "AS definition FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND "
                  "TABLE_NAME=? AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION";
        else if (o->kind == UD_METADATA_FOREIGN_KEYS)
            sql = "SELECT COLUMN_NAME AS name,TABLE_NAME AS "
                  "parent,CONCAT(CONSTRAINT_NAME,':',ORDINAL_POSITION,':',REFERENCED_TABLE_SCHEMA,'"
                  ".',REFERENCED_TABLE_NAME,'.',REFERENCED_COLUMN_NAME) AS definition FROM "
                  "information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND "
                  "REFERENCED_TABLE_NAME IS NOT NULL ORDER BY CONSTRAINT_NAME,ORDINAL_POSITION";
        else if (o->kind == UD_METADATA_INDEXES)
            sql = "SELECT INDEX_NAME AS name,TABLE_NAME AS "
                  "parent,CONCAT(SEQ_IN_INDEX,':',COLUMN_NAME,':',NON_UNIQUE,':',INDEX_TYPE) AS "
                  "definition FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND "
                  "TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX";
        else
            return UD_UNSUPPORTED;
    }
    ud_batch *all = NULL;
    ud_status s = query(p, sql, args, count, f, 3, &all);
    uint64_t rows = !s ? p->host.rows(all) : 0, begin = o->offset < rows ? o->offset : rows,
             end = begin + o->limit;
    if (end > rows)
        end = rows;
    if (!s)
        s = p->host.batch_create(p->host.context, f, 3, (uint32_t)(end - begin), out);
    for (uint64_t row = begin; !s && row < end; row++) {
        ud_value v[3];
        for (uint32_t j = 0; !s && j < 3; j++)
            s = p->host.get(all, (uint32_t)row, j, v + j);
        if (!s)
            s = p->host.append(*out, v, 3);
    }
    p->host.batch_destroy(all);
    return s;
}
static ud_status begin(void *state, int32_t isolation) {
    mysql_state *p = state;
    if (p->transaction || p->cursor)
        return UD_BUSY;
    const char *sql = isolation == UD_ISOLATION_SERIALIZABLE
                          ? "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
                      : isolation == UD_ISOLATION_REPEATABLE_READ
                          ? "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                      : isolation == UD_ISOLATION_READ_COMMITTED
                          ? "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
                          : NULL;
    if (!sql)
        return UD_INVALID;
    ud_status s = command(p, sql);
    if (!s)
        s = command(p, "START TRANSACTION");
    if (!s)
        p->transaction = 1;
    return s;
}
static ud_status commit(void *state) {
    mysql_state *p = state;
    if (!p->transaction)
        return UD_INVALID;
    ud_status s = check(p);
    if (!s && mysql_commit(p->db)) {
        p->broken = 1;
        s = UD_COMMIT_UNKNOWN;
    }
    if (!s)
        p->transaction = 0;
    return s;
}
static ud_status rollback(void *state) {
    mysql_state *p = state;
    if (!p->transaction)
        return UD_INVALID;
    int rc = mysql_rollback(p->db);
    if (rc) {
        p->broken = 1;
        return failure(p, NULL);
    }
    p->transaction = 0;
    return UD_OK;
}
ud_status ud_mysql_begin(mysql_state *p, int32_t isolation) {
    return begin(p, isolation);
}
ud_status ud_mysql_commit(mysql_state *p) {
    return commit(p);
}
ud_status ud_mysql_rollback(mysql_state *p) {
    return rollback(p);
}
static ud_status test(void *state) {
    mysql_state *p = state;
    ud_status s = check(p);
    return s ? s : mysql_ping(p->db) ? failure(p, NULL) : UD_OK;
}
static void set_control(void *state, void *user, ud_status (*fn)(void *)) {
    mysql_state *p = state;
    p->check_user = user;
    p->check = fn;
    if (!fn)
        atomic_store(&p->cancelled, 0);
}
static ud_status savepoint(void *state, ud_string statement) {
    mysql_state *p = state;
    if (!p->transaction)
        return UD_UNSUPPORTED;
    char *sql = plugin_string(&p->host, statement.data, statement.length);
    if (!sql)
        return UD_RESOURCE;
    ud_status status = command(p, sql);
    p->host.deallocate(sql);
    return status;
}
static ud_status execute(void *state, const ud_read_options *o, uint64_t *processed) {
    mysql_state *p = state;
    if (p->cursor)
        return UD_BUSY;
    /* MySQL does not prepare every administrative/DDL statement (notably
     * CREATE TRIGGER). Unparameterized SQL uses its text command protocol;
     * typed row parameters always use the binary prepared protocol below. */
    if (!o->parameters) {
        if (processed)
            *processed = 0;
        ud_status status = check(p);
        if (!status && mysql_real_query(p->db, o->sql.data, o->sql.length))
            status = failure(p, NULL);
        uint64_t affected = 0;
        int result_set = 0;
        for (; !status;) {
            if (mysql_field_count(p->db)) {
                MYSQL_RES *result = mysql_store_result(p->db);
                if (!result)
                    status = failure(p, NULL);
                mysql_free_result(result);
                result_set = 1;
            } else {
                uint64_t rows = (uint64_t)mysql_affected_rows(p->db);
                if (rows > UINT64_MAX - affected)
                    status = UD_OVERFLOW;
                else
                    affected += rows;
            }
            if (status)
                break;
            int more = mysql_next_result(p->db);
            if (more < 0)
                break;
            if (more) {
                status = failure(p, NULL);
                break;
            }
        }
        if (!status && result_set)
            status = UD_SCHEMA;
        if (!status && processed)
            *processed = affected;
        return status ? status : check(p);
    }
    MYSQL_STMT *stmt = NULL;
    ud_status s = statement(p, o->sql, &stmt);
    uint32_t n = o->parameters ? p->host.columns(o->parameters) : 0,
             rows = o->parameters ? p->host.rows(o->parameters) : 1;
    uint64_t affected = 0;
    for (uint32_t row = 0; !s && row < rows; row++) {
        bindings b = {0};
        s = bindings_create(p, n, &b);
        if (!s && n)
            s = bind_batch(&b, 0, o->parameters, row, NULL, n);
        uint64_t a = 0;
        if (!s)
            s = execute_bound(p, stmt, &b, &a);
        bindings_free(&b);
        if (!s && mysql_stmt_field_count(stmt))
            s = UD_INVALID;
        if (!s) {
            if (a > UINT64_MAX - affected)
                s = UD_OVERFLOW;
            else
                affected += a;
        }
    }
    if (stmt)
        mysql_stmt_close(stmt);
    if (processed && !s)
        *processed = affected;
    return s;
}
static ud_status receipt(void *state, ud_string tenant, ud_string job, ud_string batch,
                         ud_commit_receipt *out) {
    mysql_state *p = state;
    memset(out, 0, sizeof(*out));
    ud_string args[] = {tenant, job, batch};
    ud_field f[] = {{.name = "digest", .name_length = 6, .type = UD_BINARY},
                    {.name = "source_position", .name_length = 15, .type = UD_UTF8}};
    ud_batch *rows = NULL;
    const char *sql = p->transaction ? "SELECT digest,source_position FROM ud_sdk_receipts_v11 "
                                       "WHERE tenant=? AND job=? AND batch_id=? FOR UPDATE"
                                     : "SELECT digest,source_position FROM ud_sdk_receipts_v11 "
                                       "WHERE tenant=? AND job=? AND batch_id=?";
    ud_status s = query(p, sql, args, 3, f, 2, &rows);
    if (!s && p->host.rows(rows) > 1)
        s = UD_CORRUPT;
    if (!s && p->host.rows(rows)) {
        ud_value d, pos;
        s = p->host.get(rows, 0, 0, &d);
        if (!s)
            s = p->host.get(rows, 0, 1, &pos);
        if (!s && (d.is_null || d.text_length != 32 || pos.is_null))
            s = UD_CORRUPT;
        if (!s) {
            out->source_position.data = plugin_string(&p->host, pos.text, pos.text_length);
            out->source_position.length = pos.text_length;
            if (!out->source_position.data)
                s = UD_RESOURCE;
            else {
                memcpy(out->digest, d.text, 32);
                out->committed = 1;
            }
        }
    }
    p->host.batch_destroy(rows);
    return s;
}
ud_status ud_mysql_receipt(mysql_state *p, ud_string tenant, ud_string job, ud_string batch,
                           ud_commit_receipt *out) {
    return receipt(p, tenant, job, batch, out);
}
static ud_status provision(void *state) {
    mysql_state *p = state;
    if (p->transaction || p->cursor)
        return UD_BUSY;
    ud_status status =
        command(p, "CREATE TABLE IF NOT EXISTS ud_sdk_receipts_v11(tenant VARBINARY(256) NOT "
                   "NULL,job VARBINARY(256) NOT NULL,batch_id VARBINARY(256) NOT NULL,digest "
                   "BINARY(32) NOT NULL,source_position LONGTEXT CHARACTER SET utf8mb4 NOT "
                   "NULL,PRIMARY KEY(tenant,job,batch_id)) ENGINE=InnoDB");
    if (!status)
        status = command(
            p, "CREATE TABLE IF NOT EXISTS ud_sdk_tx_receipts_v11("
               "branch_id BINARY(64) PRIMARY KEY,digest BINARY(32) NOT NULL) ENGINE=InnoDB");
    return status;
}
static ud_status xa_command(mysql_state *state, const char *operation,
                            const ud_transaction_branch *branch) {
    char statement[112];
    snprintf(statement, sizeof(statement), "XA %s '%.*s'", operation, 64, branch->id.data);
    return command(state, statement);
}
static ud_status transaction_recover(void *state, const ud_transaction_branch *branch,
                                     int32_t *result) {
    mysql_state *p = state;
    *result = UD_TX_UNKNOWN;
    if ((p->xa_state == UD_TX_ACTIVE || p->xa_state == -1) &&
        !memcmp(p->branch_id, branch->id.data, 64)) {
        *result = UD_TX_ACTIVE;
        return UD_OK;
    }
    int cleanup = p->cleanup;
    p->cleanup = 1;
    ud_status status = command(p, "XA RECOVER");
    MYSQL_RES *prepared = !status ? mysql_store_result(p->db) : NULL;
    if (!status && (!prepared || mysql_num_fields(prepared) != 4))
        status = UD_CORRUPT;
    MYSQL_ROW row;
    while (!status && (row = mysql_fetch_row(prepared))) {
        unsigned long *lengths = mysql_fetch_lengths(prepared);
        if (!lengths || !row[0] || !row[1] || !row[2] || !row[3]) {
            status = UD_CORRUPT;
            break;
        }
        if (!strcmp(row[0], "1") && !strcmp(row[1], "64") && !strcmp(row[2], "0") &&
            lengths[3] == 64 && !memcmp(row[3], branch->id.data, 64))
            *result = UD_TX_PREPARED;
    }
    if (prepared)
        mysql_free_result(prepared);
    if (!status && *result != UD_TX_PREPARED) {
        char statement[160];
        snprintf(statement, sizeof(statement),
                 "SELECT digest FROM ud_sdk_tx_receipts_v11 WHERE branch_id='%.*s'", 64,
                 branch->id.data);
        status = command(p, statement);
        MYSQL_RES *receipt_result = !status ? mysql_store_result(p->db) : NULL;
        if (!status && (!receipt_result || mysql_num_fields(receipt_result) != 1 ||
                        mysql_num_rows(receipt_result) > 1))
            status = UD_CORRUPT;
        if (!status && mysql_num_rows(receipt_result) == 1) {
            row = mysql_fetch_row(receipt_result);
            unsigned long *lengths = row ? mysql_fetch_lengths(receipt_result) : NULL;
            if (!row || !row[0] || !lengths || lengths[0] != 32)
                status = UD_CORRUPT;
            else if (memcmp(row[0], branch->digest, 32))
                status = UD_FENCED;
            else
                *result = UD_TX_COMMITTED;
        } else if (!status)
            *result = UD_TX_NOT_FOUND;
        if (receipt_result)
            mysql_free_result(receipt_result);
    }
    p->cleanup = cleanup;
    return status;
}
static ud_status transaction_begin(void *state, const ud_transaction_branch *branch) {
    mysql_state *p = state;
    if (p->transaction || p->cursor || p->branch_id[0])
        return UD_BUSY;
    const char *isolation = branch->isolation == UD_ISOLATION_SERIALIZABLE
                                ? "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
                            : branch->isolation == UD_ISOLATION_REPEATABLE_READ
                                ? "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                                : "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";
    ud_status status = command(p, isolation);
    if (!status)
        status = xa_command(p, "START", branch);
    if (!status) {
        p->transaction = 1;
        p->xa_state = UD_TX_ACTIVE;
        memcpy(p->branch_id, branch->id.data, 64);
        p->branch_id[64] = 0;
    }
    static const char alphabet[] = "0123456789abcdef";
    char digest[65];
    for (uint32_t index = 0; index < 32; index++) {
        digest[2 * index] = alphabet[branch->digest[index] >> 4];
        digest[2 * index + 1] = alphabet[branch->digest[index] & 15];
    }
    digest[64] = 0;
    char statement[320];
    snprintf(statement, sizeof(statement),
             "INSERT INTO ud_sdk_tx_receipts_v11(branch_id,digest) VALUES('%.*s',X'%s')", 64,
             branch->id.data, digest);
    if (!status)
        status = command(p, statement);
    if (status && p->xa_state == UD_TX_ACTIVE) {
        p->cleanup = 1;
        ud_status ended = xa_command(p, "END", branch);
        ud_status rolled_back = !ended ? xa_command(p, "ROLLBACK", branch) : ended;
        p->cleanup = 0;
        if (rolled_back)
            status = UD_COMMIT_UNKNOWN;
        else {
            p->transaction = p->xa_state = 0;
            memset(p->branch_id, 0, sizeof(p->branch_id));
        }
    }
    return status;
}
static ud_status transaction_prepare(void *state, const ud_transaction_branch *branch) {
    mysql_state *p = state;
    if (p->xa_state != UD_TX_ACTIVE || memcmp(p->branch_id, branch->id.data, 64))
        return UD_FENCED;
    ud_status status = xa_command(p, "END", branch);
    if (!status)
        p->xa_state = -1; /* Ended, not yet prepared; recover must not infer absence. */
    if (!status)
        status = xa_command(p, "PREPARE", branch);
    if (!status) {
        p->transaction = 0;
        p->xa_state = UD_TX_PREPARED;
    }
    return status;
}
static ud_status transaction_resolve(void *state, const ud_transaction_branch *branch,
                                     int32_t decision) {
    mysql_state *p = state;
    p->cleanup = 1;
    ud_status status = UD_OK;
    if (p->xa_state == UD_TX_ACTIVE) {
        if (decision != UD_TX_ROLLBACK || memcmp(p->branch_id, branch->id.data, 64))
            status = UD_FENCED;
        else
            status = xa_command(p, "END", branch);
    }
    if (!status)
        status = xa_command(p, decision == UD_TX_COMMIT ? "COMMIT" : "ROLLBACK", branch);
    if (!status) {
        p->transaction = p->xa_state = 0;
        memset(p->branch_id, 0, sizeof(p->branch_id));
    }
    p->cleanup = 0;
    return status;
}
static ud_status prove_innodb_target(mysql_state *p, const ud_string args[2]) {
    ud_field f = {.name = "engine", .name_length = 6, .type = UD_UTF8};
    ud_batch *engine = NULL;
    ud_status s = query(p,
                        "SELECT ENGINE AS engine FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND "
                        "TABLE_NAME=?",
                        args, 2, &f, 1, &engine);
    if (!s && p->host.rows(engine) != 1)
        s = UD_SCHEMA;
    ud_value e = {0};
    if (!s)
        s = p->host.get(engine, 0, 0, &e);
    if (!s && (e.is_null || e.text_length != 6 || memcmp(e.text, "InnoDB", 6)))
        s = UD_UNSUPPORTED;
    p->host.batch_destroy(engine);
    return s;
}
static ud_status prove_target(mysql_state *p, const ud_write_options *o, const ud_batch *batch) {
    ud_string args[2];
    ud_status s = table_parts(p, o->table, args);
    if (!s)
        s = p->transaction ? prove_transactional_target(p, args) : prove_innodb_target(p, args);
    ud_batch *meta = NULL;
    if (!s)
        s = discover(p, o->table, &meta);
    ud_batch *actual_schema = NULL;
    if (!s)
        s = p->host.schema_from_metadata(p->host.context, meta, o->table,
                                         p->host.resource_namespace, UD_MYSQL_DIALECT,
                                         &actual_schema);
    uint32_t columns = p->host.columns(batch);
    for (uint32_t i = 0; !s && i < columns; i++) {
        ud_field field;
        s = p->host.field(batch, i, &field);
        int found = 0;
        for (uint32_t row = 0; !s && row < p->host.columns(actual_schema); row++) {
            ud_field actual;
            s = p->host.field(actual_schema, row, &actual);
            if (!s && field.name_length == actual.name_length &&
                !memcmp(field.name, actual.name, field.name_length)) {
                found = 1;
                if (!p->host.field_matches(&actual, &field, UD_FIELD_VALUE_TYPE))
                    s = UD_SCHEMA;
                /* Boolean is an explicit value policy for integer 0/1, never a discovery inference.
                 */
                if (s == UD_SCHEMA && actual.type == UD_INT64 && field.type == UD_BOOL)
                    s = UD_OK;
                for (uint32_t k = 0; !s && k < o->key_count; k++)
                    if (o->keys[k] == i && actual.key_order != k + 1)
                        s = UD_SCHEMA;
                break;
            }
        }
        if (!s && !found)
            s = UD_SCHEMA;
    }
    p->host.batch_destroy(actual_schema);
    p->host.batch_destroy(meta);
    return s;
}
ud_status ud_mysql_prove_target(mysql_state *p, const ud_write_options *o, const ud_batch *batch) {
    return prove_target(p, o, batch);
}
static MYSQL *connect_session(mysql_state *p, ud_status *status) {
    *status = UD_RESOURCE;
    MYSQL *db = mysql_init(NULL);
    if (!db)
        return NULL;
    *status = UD_UNSUPPORTED;
    unsigned int local = 1, timeout = p->timeout;
    unsigned int protocol = MYSQL_PROTOCOL_TCP;
    mysql_options(db, MYSQL_OPT_PROTOCOL, &protocol);
#if defined(UD_MARIADB_CLIENT)
    ud_mysql_bool reconnect = 0;
#endif
    mysql_options(db, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    mysql_options(db, MYSQL_OPT_READ_TIMEOUT, &timeout);
    mysql_options(db, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
    mysql_options(db, MYSQL_OPT_LOCAL_INFILE, &local);
    mysql_set_local_infile_handler(db, ud_mysql_bulk_init, ud_mysql_bulk_read, ud_mysql_bulk_end,
                                   ud_mysql_bulk_error, p);
#if defined(UD_MARIADB_CLIENT)
    mysql_options(db, MYSQL_OPT_RECONNECT, &reconnect);
#endif
#if defined(UD_MYSQL_CLIENT)
    unsigned int mode = p->ssl_disabled ? SSL_MODE_DISABLED : SSL_MODE_VERIFY_IDENTITY;
    if (mysql_options(db, MYSQL_OPT_SSL_MODE, &mode)) {
        mysql_close(db);
        return NULL;
    }
#else
    ud_mysql_bool use_tls = !p->ssl_disabled;
    if (mysql_options(db, MYSQL_OPT_SSL_ENFORCE, &use_tls) ||
        mysql_options(db, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &use_tls)) {
        mysql_close(db);
        return NULL;
    }
#endif
    if (!p->ssl_disabled && mysql_ssl_set(db, p->key, p->cert, p->ca, NULL, NULL)) {
        mysql_close(db);
        return NULL;
    }
    if (!mysql_real_connect(db, p->host_name, p->user, p->password, p->database, p->port, NULL,
                            CLIENT_FOUND_ROWS)) {
        *status = report_failure(p, mysql_errno(db), mysql_sqlstate(db));
        mysql_close(db);
        return NULL;
    }
    if (!p->ssl_disabled && !mysql_get_ssl_cipher(db)) {
        *status = UD_AUTH;
        mysql_close(db);
        return NULL;
    }
    *status = UD_OK;
    return db;
}
static ud_status cdc_close(void *state);
static ud_status binlog_open(mysql_state *p) {
    mysql_cdc *c = p->cdc;
    if (mysql_query(c->stream, "SET @master_binlog_checksum=@@global.binlog_checksum"))
        return UD_IO;
#if defined(UD_MYSQL_CLIENT)
    memset(&c->rpl, 0, sizeof(c->rpl));
    c->rpl.file_name = c->file;
    c->rpl.file_name_length = strlen(c->file);
    c->rpl.start_position = c->position;
    c->rpl.server_id = c->rows;
    c->rpl.flags = 1;
    if (mysql_binlog_open(c->stream, &c->rpl)) {
        unsigned int code = mysql_errno(c->stream);
        char safe[96];
        snprintf(safe, sizeof(safe), "binlog transport failed (code %u)", code);
        p->host.report(p->host.context, UD_IO, safe);
        return code == 1236 ? UD_FENCED : UD_IO;
    }
#else
    if (c->rpl)
        mariadb_rpl_close(c->rpl);
    c->rpl = mariadb_rpl_init(c->stream);
    if (!c->rpl)
        return UD_RESOURCE;
    mariadb_rpl_optionsv(c->rpl, MARIADB_RPL_FILENAME, c->file, strlen(c->file));
    mariadb_rpl_optionsv(c->rpl, MARIADB_RPL_START, (unsigned long)c->position);
    mariadb_rpl_optionsv(c->rpl, MARIADB_RPL_SERVER_ID, c->rows);
    mariadb_rpl_optionsv(c->rpl, MARIADB_RPL_FLAGS,
                         MARIADB_RPL_BINLOG_DUMP_NON_BLOCK | MARIADB_RPL_BINLOG_SEND_ANNOTATE_ROWS);
    mariadb_rpl_optionsv(c->rpl, MARIADB_RPL_VERIFY_CHECKSUM, 1u);
    mariadb_rpl_optionsv(c->rpl, MARIADB_RPL_UNCOMPRESS, 1u);
    if (mariadb_rpl_open(c->rpl)) {
        unsigned int code = c->rpl->error_no;
        char safe[96];
        snprintf(safe, sizeof(safe), "binlog transport failed (code %u)", code);
        p->host.report(p->host.context, UD_IO, safe);
        return code == 1236 ? UD_FENCED : UD_IO;
    }
#endif
    c->opened = 1;
    return UD_OK;
}
static ud_status cdc_position(mysql_state *p, ud_string position) {
    ud_json *json = NULL;
    ud_status status =
        p->host.json_parse(p->host.context, position.data, position.length, &json, NULL);
    const char *file = NULL, *number = NULL, *vendor = NULL;
    uint32_t n = 0, k = 0, v = 0;
    if (!status)
        status = p->host.json_text(p->host.json_find(json, "file", 4), &file, &n);
    if (!status)
        status = p->host.json_text(p->host.json_find(json, "position", 8), &number, &k);
    if (!status)
        status = p->host.json_text(p->host.json_find(json, "vendor", 6), &vendor, &v);
#if defined(UD_MYSQL_CLIENT)
    if (!status && (v != 5 || memcmp(vendor, "mysql", 5)))
        status = UD_FENCED;
#else
    if (!status && (v != 7 || memcmp(vendor, "mariadb", 7)))
        status = UD_FENCED;
#endif
    const char *incarnation = NULL;
    uint32_t incarnation_length = 0;
    if (!status)
        status = p->host.json_text(p->host.json_find(json, "incarnation", 11), &incarnation,
                                   &incarnation_length);
    if (!status && (strlen(p->cdc->incarnation) != incarnation_length ||
                    memcmp(p->cdc->incarnation, incarnation, incarnation_length)))
        status = UD_FENCED;
    uint64_t value = 0;
    for (uint32_t i = 0; !status && i < k; i++) {
        if (number[i] < '0' || number[i] > '9' ||
            value > (UINT64_MAX - (unsigned)(number[i] - '0')) / 10)
            status = UD_CORRUPT;
        else
            value = value * 10 + (unsigned)(number[i] - '0');
    }
    for (uint32_t i = 0; !status && i < n; i++)
        if (!((file[i] >= 'a' && file[i] <= 'z') || (file[i] >= 'A' && file[i] <= 'Z') ||
              (file[i] >= '0' && file[i] <= '9') || file[i] == '.' || file[i] == '_' ||
              file[i] == '-'))
            status = UD_SCHEMA;
    if (!status && (!n || value < 4))
        status = UD_FENCED;
    if (!status) {
        p->cdc->file = plugin_string(&p->host, file, n);
        p->cdc->position = value;
        p->cdc->confirmed = value;
        if (!p->cdc->file)
            status = UD_RESOURCE;
    }
    p->host.json_destroy(json);
    return status;
}
static ud_status cdc_incarnation(mysql_state *p, const ud_cdc_options *o) {
#if defined(UD_MYSQL_CLIENT)
    const char *identity = "SELECT CONCAT(@@server_uuid,':',TABLE_ID) FROM "
                           "INFORMATION_SCHEMA.INNODB_TABLES WHERE NAME=CONCAT(DATABASE(),'/',?)";
#else
    const char *identity =
        "SELECT CONCAT(@@server_uid,':',TABLE_ID) FROM INFORMATION_SCHEMA.INNODB_SYS_TABLES WHERE "
        "NAME=CONCAT(DATABASE(),'/',?)";
#endif
    MYSQL_STMT *stmt = mysql_stmt_init(p->db);
    if (!stmt)
        return UD_RESOURCE;
    ud_status status = mysql_stmt_prepare(stmt, identity, (unsigned long)strlen(identity))
                           ? failure(p, stmt)
                           : UD_OK;
    MYSQL_BIND input = {0}, output = {0};
    unsigned long input_length = o->table.length, size = 0;
    input.buffer_type = MYSQL_TYPE_STRING;
    input.buffer = (void *)o->table.data;
    input.buffer_length = input_length;
    input.length = &input_length;
    char value[128] = {0};
    ud_mysql_bool null = 0, truncated = 0;
    output.buffer_type = MYSQL_TYPE_STRING;
    output.buffer = value;
    output.buffer_length = sizeof(value) - 1;
    output.length = &size;
    output.is_null = &null;
    output.error = &truncated;
    if (!status && (mysql_stmt_bind_param(stmt, &input) || mysql_stmt_execute(stmt) ||
                    mysql_stmt_bind_result(stmt, &output) || mysql_stmt_store_result(stmt)))
        status = failure(p, stmt);
    if (!status && (mysql_stmt_num_rows(stmt) != 1 || mysql_stmt_fetch(stmt) || null || truncated ||
                    !size || size >= sizeof(value)))
        status = UD_FENCED;
    for (uint32_t i = 0; !status && i < size; i++)
        if (!(value[i] >= 'a' && value[i] <= 'z') && !(value[i] >= 'A' && value[i] <= 'Z') &&
            !(value[i] >= '0' && value[i] <= '9') && !strchr("-:+/=", value[i]))
            status = UD_CORRUPT;
    if (!status) {
        p->cdc->incarnation = plugin_string(&p->host, value, (uint32_t)size);
        if (!p->cdc->incarnation)
            status = UD_RESOURCE;
    }
    mysql_stmt_close(stmt);
    return status;
}
static ud_status cdc_open(void *state, const ud_cdc_options *options) {
    mysql_state *p = state;
    if (p->cdc || p->cursor || p->transaction || !options->server_id)
        return UD_BUSY;
    mysql_cdc *c = p->host.allocate(p->host.context, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    p->cdc = c;
    c->rows = options->server_id;
    ud_status status;
    c->stream = connect_session(p, &status);
    c->table = plugin_string(&p->host, options->table.data, options->table.length);
    if (!c->table)
        status = UD_RESOURCE;
    ud_field *fields = NULL;
    if (!status)
        status = plugin_fields(&p->host, options->schema, &fields);
    if (!status)
        status = p->host.batch_create(p->host.context, fields, p->host.columns(options->schema), 0,
                                      &c->schema);
    p->host.deallocate(fields);
    if (!status)
        status = cdc_incarnation(p, options);
    if (!status)
        status = command(p, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    if (!status && mysql_query(p->db, "SELECT "
                                      "@@global.binlog_format,@@global.binlog_row_image,@@global."
                                      "log_bin,@@global.binlog_checksum"))
        status = failure(p, NULL);
    MYSQL_RES *probe = !status ? mysql_store_result(p->db) : NULL;
    MYSQL_ROW row = probe ? mysql_fetch_row(probe) : NULL;
    if (!status &&
        (!row || !row[0] || !row[1] || !row[2] || strcmp(row[0], "ROW") ||
         (strcmp(row[1], "FULL") && strcmp(row[1], "MINIMAL") && strcmp(row[1], "NOBLOB")) ||
         strcmp(row[2], "1")))
        status = UD_UNSUPPORTED;
    if (!status) {
        c->checksum = row[3] && !strcmp(row[3], "CRC32");
        if (!c->checksum && (!row[3] || strcmp(row[3], "NONE")))
            status = UD_UNSUPPORTED;
    }
    if (probe)
        mysql_free_result(probe);
    if (!status && options->mode == UD_CDC_SNAPSHOT_STREAM) {
        status = command(p, "FLUSH TABLES WITH READ LOCK");
        int locked = !status;
        if (!status)
            status = command(p, "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
#if defined(UD_MYSQL_CLIENT)
        const char *show = "SHOW BINARY LOG STATUS";
#else
        const char *show = "SHOW MASTER STATUS";
#endif
        if (!status && mysql_query(p->db, show))
            status = failure(p, NULL);
        MYSQL_RES *result = !status ? mysql_store_result(p->db) : NULL;
        MYSQL_ROW record = result ? mysql_fetch_row(result) : NULL;
        if (!status && (!record || !record[0] || !record[1]))
            status = UD_SCHEMA;
        if (!status) {
            c->file = plugin_string(&p->host, record[0], (uint32_t)strlen(record[0]));
            c->position = 0;
            for (const char *at = record[1]; *at; at++) {
                if (*at < '0' || *at > '9' || c->position > UINT64_MAX / 10) {
                    status = UD_CORRUPT;
                    break;
                }
                c->position = c->position * 10 + (unsigned)(*at - '0');
            }
            if (!c->file)
                status = UD_RESOURCE;
        }
        if (result)
            mysql_free_result(result);
        if (locked) {
            ud_status released = command(p, "UNLOCK TABLES");
            if (!status)
                status = released;
        }
        if (!status) {
            ud_string query = {0};
            status = p->host.compile_scan(p->host.context, UD_MYSQL_DIALECT, options->table,
                                          options->schema, &query);
            if (!status) {
                ud_read_options read = {.struct_size = sizeof(read),
                                        .abi_version = UD_ABI_VERSION,
                                        .batch_size = options->batch_rows,
                                        .sql = query};
                status = read_open(p, &read, options->schema);
            }
            p->host.deallocate((void *)query.data);
        }
        if (!status)
            c->snapshot = 1;
    } else if (!status)
        status = cdc_position(p, options->position);
    if (!status)
        status = ud_binlog_create(&p->host, options,
#if defined(UD_MYSQL_CLIENT)
                                  0,
#else
                                  1,
#endif
                                  (ud_string){c->incarnation, (uint32_t)strlen(c->incarnation)},
                                  c->checksum, &c->decoder);
    if (status) {
        cdc_close(p);
        return status;
    }
    return UD_OK;
}
static ud_status cdc_schema_event(mysql_state *p, ud_cdc_event *out) {
    mysql_cdc *c = p->cdc;
    ud_batch *metadata = NULL, *actual = NULL;
    ud_status status = discover(p, (ud_string){c->table, (uint32_t)strlen(c->table)}, &metadata);
    if (!status)
        status = p->host.schema_from_metadata(
            p->host.context, metadata,
            (ud_string){c->incarnation, (uint32_t)strlen(c->incarnation)},
            p->host.resource_namespace,
#if defined(UD_MYSQL_CLIENT)
            UD_SQL_MYSQL,
#else
            UD_SQL_MARIADB,
#endif
            &actual);
    int changed = !status && p->host.columns(actual) != p->host.columns(c->schema);
    for (uint32_t i = 0; !status && !changed && i < p->host.columns(actual); i++) {
        ud_field before, after;
        status = p->host.field(c->schema, i, &before);
        if (!status)
            status = p->host.field(actual, i, &after);
        if (!status &&
            (before.name_length != after.name_length ||
             memcmp(before.name, after.name, before.name_length) || before.type != after.type ||
             before.nullable != after.nullable || before.precision != after.precision ||
             before.scale != after.scale || before.key_order != after.key_order))
            changed = 1;
    }
    if (!status && !changed) {
        p->host.batch_destroy(actual);
        p->host.batch_destroy(metadata);
        ud_binlog_unchanged_schema(c->decoder, out);
        return UD_OK;
    }
    if (!status)
        status = p->host.schema_bind_identity(p->host.context, c->schema, actual, &out->after);
    p->host.batch_destroy(actual);
    p->host.batch_destroy(metadata);
    if (!status) {
        c->schema_changed = 1;
        p->host.deallocate(c->schema_position);
        c->schema_position = plugin_string(&p->host, out->position.data, out->position.length);
        if (!c->schema_position)
            status = UD_RESOURCE;
    }
    return status;
}
static ud_status cdc_rebind(void *state, const ud_batch *schema, uint64_t version) {
    mysql_state *p = state;
    mysql_cdc *c = p->cdc;
    if (!c)
        return UD_INVALID;
    ud_field *fields = NULL;
    ud_batch *owned = NULL;
    ud_status status = plugin_fields(&p->host, schema, &fields);
    if (!status)
        status = p->host.batch_create(p->host.context, fields, p->host.columns(schema), 0, &owned);
    p->host.deallocate(fields);
    ud_string position = {c->schema_position,
                          c->schema_position ? (uint32_t)strlen(c->schema_position) : 0};
    if (!status)
        status = ud_binlog_rebind(c->decoder, owned, version, position);
    if (!status) {
        p->host.batch_destroy(c->schema);
        c->schema = owned;
        owned = NULL;
        c->schema_changed = 0;
        p->host.deallocate(c->schema_position);
        c->schema_position = NULL;
    }
    p->host.batch_destroy(owned);
    return status;
}
static ud_status cdc_next(void *state, ud_cdc_event *out) {
    mysql_state *p = state;
    mysql_cdc *c = p->cdc;
    if (!c)
        return UD_INVALID;
    if (c->schema_changed)
        return UD_FENCED;
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);
    out->abi_version = UD_ABI_VERSION;
    out->schema_version = 1;
    ud_status status = check(p);
    if (status)
        return status;
    status = ud_binlog_drain(c->decoder, out);
    if (status != UD_BUSY)
        return status;
    if (c->snapshot) {
        if (!c->phase) {
            c->phase = 1;
            out->kind = UD_CDC_BEGIN;
            out->transaction_id = (ud_string){plugin_string(&p->host, "snapshot", 8), 8};
            return out->transaction_id.data ? UD_OK : UD_RESOURCE;
        }
        status = read_next(p, &out->after);
        if (!status) {
            uint32_t rows = p->host.rows(out->after);
            out->operations = p->host.allocate(p->host.context, rows * sizeof(*out->operations));
            if (!out->operations)
                return UD_RESOURCE;
            for (uint32_t i = 0; i < rows; i++)
                out->operations[i] = UD_CHANGE_INSERT;
            out->kind = UD_CDC_ROWS;
            out->position = (ud_string){plugin_string(&p->host, "snapshot", 8), 8};
            return UD_OK;
        }
        if (status != UD_END)
            return status;
        status = command(p, "COMMIT");
        if (status)
            return status;
        c->snapshot = 0;
        out->kind = UD_CDC_COMMIT;
        char position[512];
        int n = snprintf(position, sizeof(position),
                         "{\"file\":\"%s\",\"position\":%llu,\"gtid\":\"\",\"vendor\":\"%s\","
                         "\"incarnation\":\"%s\"}",
                         c->file, (unsigned long long)c->position,
#if defined(UD_MYSQL_CLIENT)
                         "mysql"
#else
                         "mariadb"
#endif
                         ,
                         c->incarnation);
        out->position = (ud_string){plugin_string(&p->host, position, (uint32_t)n), (uint32_t)n};
        return out->position.data ? UD_OK : UD_RESOURCE;
    }
    if (!c->opened) {
        status = binlog_open(p);
        if (status)
            return status;
    }
    const unsigned char *raw = NULL;
    size_t length = 0;
#if defined(UD_MYSQL_CLIENT)
    if (mysql_binlog_fetch(c->stream, &c->rpl)) {
        {
            unsigned int code = mysql_errno(c->stream);
            char safe[96];
            snprintf(safe, sizeof(safe), "binlog transport failed (code %u)", code);
            p->host.report(p->host.context, UD_IO, safe);
            return code == 1236 ? UD_FENCED : UD_IO;
        }
    }
    if (!c->rpl.size) {
        mysql_binlog_close(c->stream, &c->rpl);
        c->opened = 0;
        return UD_BUSY;
    }
    raw = c->rpl.buffer;
    length = c->rpl.size;
#else
    if (c->event)
        mariadb_free_rpl_event(c->event);
    c->event = mariadb_rpl_fetch(c->rpl, NULL);
    if (!c->event) {
        if (c->rpl->error_no) {
            unsigned int code = c->rpl->error_no;
            char safe[96];
            snprintf(safe, sizeof(safe), "binlog transport failed (code %u)", code);
            p->host.report(p->host.context, UD_IO, safe);
            return code == 1236 ? UD_FENCED : UD_IO;
        }
        c->opened = 0;
        return UD_BUSY;
    }
    raw = c->event->raw_data;
    length = c->event->raw_data_size;
#endif
    const uint8_t *event = NULL;
    size_t size = 0;
    status = ud_binlog_packet(raw, length, &event, &size);
    if (status)
        return status;
    uint32_t kind = event[4];
    uint64_t next = 0;
    for (uint32_t i = 0; i < 4; i++)
        next |= (uint64_t)event[13 + i] << (8 * i);
    status = ud_binlog_decode(c->decoder, raw, length,
                              (ud_string){c->file, (uint32_t)strlen(c->file)}, out);
    if (status == UD_OK && out->kind == UD_CDC_SCHEMA_CHANGE)
        status = cdc_schema_event(p, out);
    if (status == UD_OK && !out->kind) {
        char safe[96];
        snprintf(safe, sizeof(safe), "binlog decoder emitted no event for type %u", kind);
        p->host.report(p->host.context, UD_CORRUPT, safe);
        status = UD_CORRUPT;
    }
    if (status && status != UD_BUSY) {
        char safe[96];
        snprintf(safe, sizeof(safe), "binlog event %u failed decoding (%d)", kind, status);
        p->host.report(p->host.context, status, safe);
    }
    if (status == UD_OK || status == UD_BUSY) {
        if (next)
            c->position = next;
    }
    if (kind == 4 && (status == UD_OK || status == UD_BUSY)) {
        ud_string file = {0};
        uint64_t offset = 0;
        ud_status rotated = ud_binlog_rotation(c->decoder, raw, length, &file, &offset);
        if (rotated)
            return rotated;
        p->host.deallocate(c->file);
        c->file = (char *)file.data;
        c->position = offset;
    }
    return status;
}
static ud_status cdc_confirm(void *state, ud_string position) {
    mysql_state *p = state;
    mysql_cdc *c = p->cdc;
    if (!c)
        return UD_INVALID;
    ud_json *document = NULL;
    ud_status status =
        p->host.json_parse(p->host.context, position.data, position.length, &document, NULL);
    const char *value;
    uint32_t n;
    if (!status)
        status = p->host.json_text(p->host.json_find(document, "position", 8), &value, &n);
    uint64_t confirmed = 0;
    for (uint32_t i = 0; !status && i < n; i++) {
        if (value[i] < '0' || value[i] > '9' || confirmed > UINT64_MAX / 10)
            status = UD_CORRUPT;
        else
            confirmed = confirmed * 10 + (unsigned)(value[i] - '0');
    }
    if (!status)
        c->confirmed = confirmed;
    p->host.json_destroy(document);
    return status;
}
static ud_status cdc_close(void *state) {
    mysql_state *p = state;
    mysql_cdc *c = p->cdc;
    if (!c)
        return UD_OK;
    read_close(p);
#if defined(UD_MYSQL_CLIENT)
    if (c->opened)
        mysql_binlog_close(c->stream, &c->rpl);
#else
    if (c->event)
        mariadb_free_rpl_event(c->event);
    if (c->rpl)
        mariadb_rpl_close(c->rpl);
#endif
    if (c->stream)
        mysql_close(c->stream);
    p->host.deallocate(c->schema_position);
    ud_binlog_destroy(c->decoder);
    p->host.batch_destroy(c->schema);
    p->host.deallocate(c->file);
    p->host.deallocate(c->table);
    p->host.deallocate(c->gtid);
    p->host.deallocate(c->incarnation);
    p->host.deallocate(c);
    p->cdc = NULL;
    return UD_OK;
}
static void cancel(void *state) {
    mysql_state *p = state;
    atomic_store(&p->cancelled, 1);
    p->broken = 1;
    ud_mutex_lock(&p->cancellation);
    if (!p->cancel_db) {
        ud_status ignored;
        p->cancel_db = connect_session(p, &ignored);
    }
    if (p->cancel_db && p->db)
        mysql_kill(p->cancel_db, mysql_thread_id(p->db));
    if (p->cancel_db && p->cdc && p->cdc->stream)
        mysql_kill(p->cancel_db, mysql_thread_id(p->cdc->stream));
    ud_mutex_unlock(&p->cancellation);
}
static ud_status reset(void *state) {
    mysql_state *p = state;
    if (p->broken || p->cursor || p->transaction || p->branch_id[0])
        return UD_UNSUPPORTED;
    if (mysql_reset_connection(p->db))
        return failure(p, NULL);
    atomic_store(&p->cancelled, 0);
    return session_policy(p);
}
void ud_mysql_destroy(void *state) {
    mysql_state *p = state;
    if (!p)
        return;
    cdc_close(p);
    read_close(p);
    p->host.bulk_close(p->bulk);
    p->bulk = NULL;
    if (p->db)
        mysql_close(p->db);
    if (p->cancel_db)
        mysql_close(p->cancel_db);
    ud_mutex_destroy(&p->cancellation);
    char *strings[] = {p->host_name, p->user, p->password, p->database, p->ca, p->cert, p->key};
    for (uint32_t i = 0; i < 7; i++)
        p->host.deallocate(strings[i]);
    p->host.deallocate(p);
}
static ud_status config_string(mysql_state *p, const ud_json *j, const char *key, int required,
                               char **out) {
    const ud_json *v = p->host.json_find(j, key, (uint32_t)strlen(key));
    if (!v)
        return required ? UD_SCHEMA : UD_OK;
    const char *s;
    uint32_t n;
    if (p->host.json_kind(v) != UD_JSON_STRING)
        return UD_SCHEMA;
    ud_status status = p->host.json_text(v, &s, &n);
    if (!status) {
        *out = plugin_string(&p->host, s, n);
        if (!*out)
            status = UD_RESOURCE;
    }
    return status;
}
static ud_status config_option(const ud_host_api *h, const ud_json *value, const char *expected) {
    const char *text;
    uint32_t length;
    if (h->json_kind(value) != UD_JSON_STRING)
        return UD_SCHEMA;
    UD_TRY(h->json_text(value, &text, &length));
    if (length != strlen(expected))
        return UD_UNSUPPORTED;
    for (uint32_t i = 0; i < length; i++)
        if (tolower((unsigned char)text[i]) != expected[i])
            return UD_UNSUPPORTED;
    return UD_OK;
}
ud_status ud_mysql_initialize(const ud_host_api *h, const char *configuration, uint32_t length,
                              void **out) {
    *out = NULL;
    if (!h || h->struct_size != sizeof(*h) || h->abi_version != UD_PLUGIN_ABI)
        return UD_INCOMPATIBLE;
    mysql_state *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    p->port = 3306;
    p->timeout = 30;
    p->check = h->check_io;
    p->check_user = h->control_user;
    atomic_init(&p->cancelled, 0);
    ud_mutex_init(&p->cancellation);
    ud_json *j = NULL;
    ud_status s = h->json_parse(h->context, configuration, length, &j, NULL);
    static const char *keys[] = {"host", "port", "user", "password", "database", "ca",
                                 "cert", "key", "timestampTimezone", "timeoutSeconds",
                                 "charset", "sslMode"};
    if (!s)
        s = h->json_keys(j, keys, sizeof(keys) / sizeof(keys[0]));
    const ud_json *charset = !s ? h->json_find(j, "charset", 7) : NULL;
    if (charset)
        s = config_option(h, charset, "utf8mb4");
    const ud_json *ssl = !s ? h->json_find(j, "sslMode", 7) : NULL;
    if (ssl) {
        s = config_option(h, ssl, "none");
        p->ssl_disabled = !s;
    }
    const char *names[] = {"host", "user", "password", "database", "ca", "cert", "key"};
    char **targets[] = {&p->host_name, &p->user, &p->password, &p->database,
                        &p->ca,        &p->cert, &p->key};
    for (uint32_t i = 0; !s && i < 7; i++)
        s = config_string(p, j, names[i], i < 4 || (i == 4 && !p->ssl_disabled), targets[i]);
    if (!s && p->ssl_disabled && strcmp(p->host_name, "127.0.0.1") && strcmp(p->host_name, "::1"))
        s = UD_UNSUPPORTED;
    const char *numkeys[] = {"port", "timeoutSeconds"};
    for (uint32_t i = 0; !s && i < 2; i++) {
        const ud_json *v = h->json_find(j, numkeys[i], (uint32_t)strlen(numkeys[i]));
        if (v) {
            const char *t;
            uint32_t n;
            s = h->json_kind(v) != UD_JSON_NUMBER ? UD_SCHEMA : h->json_text(v, &t, &n);
            unsigned long val = 0;
            for (uint32_t k = 0; !s && k < n; k++) {
                if (t[k] < '0' || t[k] > '9' || val > 65535 / 10)
                    s = UD_SCHEMA;
                else
                    val = val * 10 + (unsigned)(t[k] - '0');
            }
            if (!s && (!val || val > 65535))
                s = UD_SCHEMA;
            if (!s) {
                if (i)
                    p->timeout = (uint32_t)val;
                else
                    p->port = (uint32_t)val;
            }
        }
    }
    const ud_json *zone = !s ? h->json_find(j, "timestampTimezone", 17) : NULL;
    if (!s && zone) {
        const char *t;
        uint32_t n;
        s = h->json_text(zone, &t, &n);
        if (!s && (n != 3 || memcmp(t, "UTC", 3)))
            s = UD_SCHEMA;
        if (!s)
            p->utc = 1;
    }
    if (!s) {
        p->db = connect_session(p, &s);
    }
    if (!s)
        s = session_policy(p);
    if (!s && mysql_query(p->db, "SHOW GLOBAL VARIABLES LIKE 'local_infile'"))
        s = failure(p, NULL);
    if (!s) {
        MYSQL_RES *result = mysql_store_result(p->db);
        MYSQL_ROW row = result ? mysql_fetch_row(result) : NULL;
        if (!result)
            s = failure(p, NULL);
        p->bulk_allowed = row && row[1] && (!strcmp(row[1], "ON") || !strcmp(row[1], "1"));
        mysql_free_result(result);
    }
    h->json_destroy(j);
    if (s) {
        ud_mysql_destroy(p);
        return s;
    }
    *out = p;
    return UD_OK;
}
const ud_connector_api ud_mysql_connector = {
    .struct_size = sizeof(ud_connector_api),
    .abi_version = UD_PLUGIN_ABI,
    .capabilities = UD_CONNECT_READONLY_SCAN | UD_CONNECT_TRANSACTIONAL_DML | UD_CONNECT_READ |
                    UD_CONNECT_RECORD_ERRORS | UD_CONNECT_APPEND | UD_CONNECT_UPSERT |
                    UD_CONNECT_DELETE | UD_CONNECT_RECEIPTS | UD_CONNECT_EXECUTE_SQL |
                    UD_CONNECT_TRANSACTION | UD_CONNECT_METADATA | UD_CONNECT_POOL |
                    UD_CONNECT_CHANGESET | UD_CONNECT_CDC | UD_CONNECT_NATIVE_CDC |
                    UD_CONNECT_PREPARED | UD_CONNECT_BATCH_WRITE | UD_CONNECT_PROVISION |
                    UD_CONNECT_NATIVE_BULK | UD_CONNECT_TRANSACTION_PARTICIPANT,
    .test = test,
    .discover = discover,
    .read_open = read_open,
    .read_next = read_next,
    .read_close = read_close,
    .write = ud_mysql_write_batch,
    .receipt = receipt,
    .cancel = cancel,
    .execute = execute,
    .begin = begin,
    .commit = commit,
    .rollback = rollback,
    .metadata = metadata,
    .set_control = set_control,
    .reset_session = reset,
    .session_capabilities = ud_mysql_session_capabilities,
    .record_diagnostic = record_diagnostic,
    .savepoint = savepoint,
    .record_validate = record_validate,
    .provision = provision,
    .parameter_style = UD_SQL_PARAMETER_QUESTION,
    .metadata_dialect = UD_MYSQL_DIALECT,
    .cdc_open = cdc_open,
    .cdc_next = cdc_next,
    .cdc_confirm = cdc_confirm,
    .cdc_close = cdc_close,
    .cdc_rebind = cdc_rebind,
    .transaction_begin = transaction_begin,
    .transaction_prepare = transaction_prepare,
    .transaction_resolve = transaction_resolve,
    .transaction_recover = transaction_recover};
