#include "common.h"
#include <duckdb.h>
#include <stdio.h>
typedef struct {
    ud_host_api host;
    duckdb_database database;
    duckdb_connection connection;
} duck_state;
static ud_status initialize(const ud_host_api *h, const char *config, uint32_t n, void **out) {
    *out = NULL;
    duck_state *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    char *path = plugin_string(h, config, n);
    if (!path) {
        h->deallocate(p);
        return UD_INVALID;
    }
    if (duckdb_open(n ? path : NULL, &p->database) == DuckDBError) {
        h->deallocate(path);
        h->deallocate(p);
        return h->report(h->context, UD_IO, "DuckDB open failed");
    }
    h->deallocate(path);
    if (duckdb_connect(p->database, &p->connection) == DuckDBError) {
        duckdb_close(&p->database);
        h->deallocate(p);
        return UD_IO;
    }
    *out = p;
    return UD_OK;
}
static ud_status command(duck_state *p, const char *sql) {
    duckdb_result result;
    memset(&result, 0, sizeof(result));
    duckdb_state state = duckdb_query(p->connection, sql, &result);
    ud_status s = state == DuckDBError
                      ? p->host.report(p->host.context, UD_IO, duckdb_result_error(&result))
                      : UD_OK;
    duckdb_destroy_result(&result);
    return s;
}
static char *quote(ud_host_api *h, const char *text, uint32_t n, char delimiter) {
    if (n && memchr(text, 0, n))
        return NULL;
    size_t cap = (size_t)n * 2 + 3;
    char *s = h->allocate(h->context, cap);
    if (!s)
        return NULL;
    size_t k = 0;
    s[k++] = delimiter;
    for (uint32_t i = 0; i < n; i++) {
        if (text[i] == delimiter)
            s[k++] = delimiter;
        s[k++] = text[i];
    }
    s[k++] = delimiter;
    s[k] = 0;
    return s;
}
static ud_status query(duck_state *p, const char *sql, ud_batch **out) {
    ud_host_api *h = &p->host;
    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(p->connection, sql, &result) == DuckDBError) {
        ud_status s = h->report(h->context, UD_IO, duckdb_result_error(&result));
        duckdb_destroy_result(&result);
        return s;
    }
    idx_t columns = duckdb_column_count(&result), rows = duckdb_row_count(&result);
    ud_status s = UD_OK;
    if (!columns) {
        duckdb_destroy_result(&result);
        return UD_OK;
    }
    if (columns > 65536 || rows > INT32_MAX)
        s = UD_RESOURCE;
    ud_field *fields = s ? NULL : h->allocate(h->context, (size_t)columns * sizeof(*fields));
    ud_value *values = s ? NULL : h->allocate(h->context, (size_t)columns * sizeof(*values));
    if (!s && (!fields || !values))
        s = UD_RESOURCE;
    for (idx_t i = 0; !s && i < columns; i++) {
        duckdb_type type = duckdb_column_type(&result, i);
        ud_type t = type == DUCKDB_TYPE_BOOLEAN ? UD_BOOL
                    : (type == DUCKDB_TYPE_TINYINT || type == DUCKDB_TYPE_SMALLINT ||
                       type == DUCKDB_TYPE_INTEGER || type == DUCKDB_TYPE_BIGINT)
                        ? UD_INT64
                    : (type == DUCKDB_TYPE_FLOAT || type == DUCKDB_TYPE_DOUBLE) ? UD_FLOAT64
                    : type == DUCKDB_TYPE_VARCHAR                               ? UD_UTF8
                                                                                : 0;
        const char *name = duckdb_column_name(&result, i);
        if (!t || !name) {
            s = UD_UNSUPPORTED;
            break;
        }
        fields[i] = (ud_field){name, (uint32_t)strlen(name), t, 1};
    }
    if (!s)
        s = h->batch_create(h->context, fields, (uint32_t)columns, (uint32_t)rows, out);
    for (idx_t r = 0; !s && r < rows; r++) {
        if (h->cancelled(h->context)) {
            s = UD_CANCELLED;
            break;
        }
        memset(values, 0, (size_t)columns * sizeof(*values));
        for (idx_t i = 0; i < columns; i++) {
            ud_value *v = values + i;
            v->type = fields[i].type;
            v->is_null = duckdb_value_is_null(&result, i, r);
            if (v->is_null)
                continue;
            if (v->type == UD_UTF8) {
                duckdb_string text = duckdb_value_string(&result, i, r);
                v->text = text.data;
                v->text_length = (uint32_t)text.size;
                if (text.size > UINT32_MAX || !text.data) {
                    s = UD_RESOURCE;
                    break;
                }
            } else if (v->type == UD_FLOAT64)
                v->real = duckdb_value_double(&result, i, r);
            else if (v->type == UD_BOOL)
                v->integer = duckdb_value_boolean(&result, i, r);
            else
                v->integer = duckdb_value_int64(&result, i, r);
        }
        if (!s)
            s = h->append(*out, values, (uint32_t)columns);
        for (idx_t i = 0; i < columns; i++)
            if (values[i].type == UD_UTF8)
                duckdb_free((void *)values[i].text);
    }
    h->deallocate(fields);
    h->deallocate(values);
    duckdb_destroy_result(&result);
    return s;
}
static ud_status parquet_write(duck_state *p, const ud_batch *b, const char *path, uint32_t n) {
    if (!b || !n)
        return UD_INVALID;
    ud_host_api *h = &p->host;
    for (uint32_t j=0;j<h->columns(b);j++) {
        ud_field f;h->field(b,j,&f);
        if(f.type>UD_UTF8) return UD_UNSUPPORTED;
    }
    ud_status s = command(p, "BEGIN TRANSACTION");
    if (s)
        return s;
    uint32_t cols = h->columns(b);
    char **names = h->allocate(h->context, cols * sizeof(char *));
    size_t size = 64;
    if (!names)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < cols; i++) {
        ud_field f;
        h->field(b, i, &f);
        names[i] = quote(h, f.name, f.name_length, '"');
        if (!names[i])
            s = UD_RESOURCE;
        else
            size += strlen(names[i]) + 20;
    }
    char *sql = !s ? h->allocate(h->context, size) : NULL;
    if (!s && !sql)
        s = UD_RESOURCE;
    if (!s) {
        strcpy(sql, "CREATE TEMP TABLE ud_parquet_export (");
        for (uint32_t i = 0; i < cols; i++) {
            ud_field f;
            h->field(b, i, &f);
            if (i)
                strcat(sql, ",");
            strcat(sql, names[i]);
            strcat(sql, f.type == UD_UTF8      ? " VARCHAR"
                        : f.type == UD_FLOAT64 ? " DOUBLE"
                        : f.type == UD_BOOL    ? " BOOLEAN"
                                               : " BIGINT");
        }
        strcat(sql, ")");
        s = command(p, sql);
    }
    h->deallocate(sql);
    if (names) {
        for (uint32_t i = 0; i < cols; i++)
            h->deallocate(names[i]);
        h->deallocate(names);
    }
    duckdb_appender appender = NULL;
    if (!s &&
        duckdb_appender_create(p->connection, NULL, "ud_parquet_export", &appender) == DuckDBError)
        s = UD_IO;
    for (uint32_t r = 0; !s && r < h->rows(b); r++) {
        if (h->cancelled(h->context)) {
            s = UD_CANCELLED;
            break;
        }
        if (duckdb_appender_begin_row(appender) == DuckDBError) {
            s = UD_IO;
            break;
        }
        for (uint32_t i = 0; i < cols; i++) {
            ud_value v;
            h->get(b, r, i, &v);
            duckdb_state rc = v.is_null ? duckdb_append_null(appender)
                              : v.type == UD_UTF8
                                  ? duckdb_append_varchar_length(appender, v.text, v.text_length)
                              : v.type == UD_FLOAT64 ? duckdb_append_double(appender, v.real)
                              : v.type == UD_BOOL    ? duckdb_append_bool(appender, v.integer != 0)
                                                     : duckdb_append_int64(appender, v.integer);
            if (rc == DuckDBError) {
                s = UD_IO;
                break;
            }
        }
        if (!s && duckdb_appender_end_row(appender) == DuckDBError)
            s = UD_IO;
    }
    if (appender) {
        if (!s && duckdb_appender_close(appender) == DuckDBError)
            s = UD_IO;
        duckdb_appender_destroy(&appender);
    }
    char *literal = quote(h, path, n, '\'');
    if (!literal && !s)
        s = UD_INVALID;
    sql = literal ? h->allocate(h->context, strlen(literal) + 100) : NULL;
    if (!sql && !s)
        s = UD_RESOURCE;
    if (!s) {
        snprintf(sql, strlen(literal) + 100, "COPY ud_parquet_export TO %s (FORMAT PARQUET)",
                 literal);
        s = command(p, sql);
    }
    h->deallocate(sql);
    h->deallocate(literal);
    if (!s)
        s = command(p, "DROP TABLE ud_parquet_export");
    if (!s)
        s = command(p, "COMMIT");
    if (s) {
        ud_status rollback = command(p, "ROLLBACK");
        if (rollback)
            return h->report(h->context, UD_IO,
                             "DuckDB rollback failed; connection requires disposal");
    }
    return s;
}
static ud_status execute(void *ptr, int32_t op, const ud_batch *b, const char *arg, uint32_t n,
                         ud_batch **out) {
    duck_state *p = ptr;
    ud_host_api *h = &p->host;
    *out = NULL;
    if (op == UD_OP_PARQUET_WRITE)
        return parquet_write(p, b, arg, n);
    if (op != UD_OP_SQL && op != UD_OP_PARQUET_READ)
        return UD_UNSUPPORTED;
    if (!n)
        return UD_INVALID;
    char *sql = NULL, *literal = NULL;
    if (op == UD_OP_SQL)
        sql = plugin_string(h, arg, n);
    else {
        literal = quote(h, arg, n, '\'');
        if (literal) {
            sql = h->allocate(h->context, strlen(literal) + 64);
            if (sql)
                snprintf(sql, strlen(literal) + 64, "SELECT * FROM read_parquet(%s)", literal);
        }
    }
    if (!sql) {
        h->deallocate(literal);
        return UD_INVALID;
    }
    ud_status s = query(p, sql, out);
    h->deallocate(sql);
    h->deallocate(literal);
    return s;
}
static void destroy(void *ptr) {
    duck_state *p = ptr;
    if (!p)
        return;
    duckdb_disconnect(&p->connection);
    duckdb_close(&p->database);
    p->host.deallocate(p);
}
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size < sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){sizeof(*out),
                           UD_PLUGIN_ABI,
                           "org.universaldata.backend.duckdb",
                           "1.0.0",
                           UD_PLUGIN_BACKEND | UD_PLUGIN_FORMAT,
                           UD_CAP_SQL | UD_CAP_PARQUET,
                           initialize,
                           execute,
                           destroy,
                           plugin_shutdown,
                           NULL};
    return UD_OK;
}
