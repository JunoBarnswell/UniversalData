#include "common.h"
#include <sqlite3.h>
typedef struct {
    ud_host_api host;
    sqlite3 *db;
    sqlite3_stmt *read;
} sqlite_state;
static ud_status failure(sqlite_state *p, int code) {
    return p->host.report(p->host.context,
                          code == SQLITE_BUSY || code == SQLITE_LOCKED ? UD_BUSY : UD_IO,
                          sqlite3_errmsg(p->db));
}
static ud_status initialize(const ud_host_api *h, const char *config, uint32_t n, void **out) {
    *out = NULL;
    sqlite_state *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    char *path = plugin_string(h, config, n);
    if (!path || !n) {
        h->deallocate(path);
        h->deallocate(p);
        return UD_INVALID;
    }
    int rc = sqlite3_open_v2(
        path, &p->db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
    h->deallocate(path);
    if (rc != SQLITE_OK) {
        ud_status s = failure(p, rc);
        sqlite3_close(p->db);
        h->deallocate(p);
        return s;
    }
    *out = p;
    return UD_OK;
}
static ud_status read_batch(sqlite_state *p, const ud_batch *schema, const char *sql, uint32_t n,
                            ud_batch **out) {
    ud_host_api *h = &p->host;
    if (!schema || (p->read && n) || (!p->read && !n) || n > INT_MAX)
        return UD_INVALID;
    if (!p->read) {
        const char *tail = NULL;
        int rc = sqlite3_prepare_v2(p->db, sql, (int)n, &p->read, &tail);
        if (rc != SQLITE_OK)
            return failure(p, rc);
        while (tail < sql + n && (*tail == ' ' || *tail == '\n' || *tail == '\r' || *tail == '\t'))
            tail++;
        if (tail != sql + n || !p->read || !sqlite3_stmt_readonly(p->read)) {
            sqlite3_finalize(p->read);
            p->read = NULL;
            return UD_INVALID;
        }
    }
    uint32_t columns = h->columns(schema);
    if (sqlite3_column_count(p->read) != (int)columns)
        return UD_SCHEMA;
    ud_field *fields = NULL;
    ud_status s = plugin_fields(h, schema, &fields);
    for (uint32_t i = 0; !s && i < columns; i++)
        if (strcmp(fields[i].name, sqlite3_column_name(p->read, (int)i)))
            s = UD_SCHEMA;
    if (!s)
        s = h->batch_create(h->context, fields, columns, 4096, out);
    ud_value *v = h->allocate(h->context, columns * sizeof(*v));
    if (!v && !s)
        s = UD_RESOURCE;
    while (!s && h->rows(*out) < 4096) {
        if (h->cancelled(h->context)) {
            s = UD_CANCELLED;
            break;
        }
        int rc = sqlite3_step(p->read);
        if (rc == SQLITE_DONE) {
            sqlite3_finalize(p->read);
            p->read = NULL;
            break;
        }
        if (rc != SQLITE_ROW) {
            s = failure(p, rc);
            break;
        }
        memset(v, 0, columns * sizeof(*v));
        for (uint32_t i = 0; i < columns; i++) {
            ud_value *x = v + i;
            int type = sqlite3_column_type(p->read, (int)i);
            x->type = fields[i].type;
            x->is_null = type == SQLITE_NULL;
            if (x->is_null)
                continue;
            if (x->type == UD_UTF8 && type == SQLITE_TEXT) {
                x->text = (const char *)sqlite3_column_text(p->read, (int)i);
                x->text_length = (uint32_t)sqlite3_column_bytes(p->read, (int)i);
            } else if ((x->type == UD_INT64 || x->type == UD_BOOL) && type == SQLITE_INTEGER)
                x->integer = sqlite3_column_int64(p->read, (int)i);
            else if (x->type == UD_FLOAT64 && type == SQLITE_FLOAT)
                x->real = sqlite3_column_double(p->read, (int)i);
            else {
                s = h->report(h->context, UD_SCHEMA,
                              "SQLite value type differs from explicit schema");
                break;
            }
        }
        if (!s)
            s = h->append(*out, v, columns);
    }
    h->deallocate(fields);
    h->deallocate(v);
    if (s && p->read) {
        sqlite3_finalize(p->read);
        p->read = NULL;
    }
    if (!s && !h->rows(*out)) {
        h->batch_destroy(*out);
        *out = NULL;
        return UD_END;
    }
    return s;
}
static ud_status write_batch(sqlite_state *p, const ud_batch *b, const char *sql, uint32_t n) {
    if (!b || !n || n > INT_MAX || p->read)
        return UD_INVALID;
    ud_host_api *h = &p->host;
    int rc = sqlite3_exec(p->db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
    if (rc != SQLITE_OK)
        return failure(p, rc);
    sqlite3_stmt *stmt = NULL;
    const char *tail = NULL;
    ud_status s = UD_OK;
    rc = sqlite3_prepare_v2(p->db, sql, (int)n, &stmt, &tail);
    if (rc != SQLITE_OK)
        s = failure(p, rc);
    if (!s) {
        while (tail < sql + n && (*tail == ' ' || *tail == '\n' || *tail == '\r' || *tail == '\t'))
            tail++;
        if (tail != sql + n || !stmt || sqlite3_stmt_readonly(stmt))
            s = UD_INVALID;
        else if (sqlite3_bind_parameter_count(stmt) != (int)h->columns(b) &&
                 !(sqlite3_bind_parameter_count(stmt) == 0 && h->rows(b) == 0))
            s = UD_INVALID;
    }
    if (!s && !h->rows(b) && sqlite3_bind_parameter_count(stmt) == 0 &&
        (rc = sqlite3_step(stmt)) != SQLITE_DONE)
        s = failure(p, rc);
    for (uint32_t r = 0; !s && r < h->rows(b); r++) {
        if (h->cancelled(h->context)) {
            s = UD_CANCELLED;
            break;
        }
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        for (uint32_t i = 0; i < h->columns(b); i++) {
            ud_value v;
            h->get(b, r, i, &v);
            int index = (int)i + 1;
            if (v.is_null)
                rc = sqlite3_bind_null(stmt, index);
            else if (v.type == UD_UTF8)
                rc = sqlite3_bind_text64(stmt, index, v.text, v.text_length, SQLITE_TRANSIENT,
                                         SQLITE_UTF8);
            else if (v.type == UD_FLOAT64)
                rc = sqlite3_bind_double(stmt, index, v.real);
            else
                rc = sqlite3_bind_int64(stmt, index, v.integer);
            if (rc != SQLITE_OK) {
                s = failure(p, rc);
                break;
            }
        }
        if (!s && (rc = sqlite3_step(stmt)) != SQLITE_DONE)
            s = failure(p, rc);
    }
    sqlite3_finalize(stmt);
    if (!s) {
        rc = sqlite3_exec(p->db, "COMMIT", NULL, NULL, NULL);
        if (rc != SQLITE_OK)
            s = failure(p, rc);
    }
    if (s) {
        rc = sqlite3_exec(p->db, "ROLLBACK", NULL, NULL, NULL);
        if (rc != SQLITE_OK)
            return h->report(h->context, UD_IO,
                             "SQLite rollback failed; transaction state requires inspection");
    }
    return s;
}
static ud_status execute(void *ptr, int32_t op, const ud_batch *b, const char *arg, uint32_t n,
                         ud_batch **out) {
    sqlite_state *p = ptr;
    *out = NULL;
    if (n && memchr(arg, 0, n))
        return UD_INVALID;
    if (op == UD_OP_READ)
        return read_batch(p, b, arg, n, out);
    if (op == UD_OP_WRITE)
        return write_batch(p, b, arg, n);
    return UD_UNSUPPORTED;
}
static void destroy(void *ptr) {
    sqlite_state *p = ptr;
    if (!p)
        return;
    sqlite3_finalize(p->read);
    sqlite3_close(p->db);
    p->host.deallocate(p);
}
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != 1 || !out || out->struct_size < sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){sizeof(*out),
                           1,
                           "org.universaldata.connector.sqlite",
                           "0.1.0",
                           UD_PLUGIN_SOURCE | UD_PLUGIN_SINK,
                           UD_CAP_READ | UD_CAP_WRITE | UD_CAP_TRANSACTION,
                           initialize,
                           execute,
                           destroy,
                           plugin_shutdown};
    return UD_OK;
}
