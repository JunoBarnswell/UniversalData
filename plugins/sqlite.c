#include "common.h"
#include <sqlite3.h>
#include <stdio.h>
#include <ctype.h>
#include <stdatomic.h>
#include <math.h>
typedef struct {
    ud_host_api host;
    sqlite3 *db;
    sqlite3_stmt *read;
    ud_batch *cursor_schema;
    uint32_t cursor_batch_size;
    ud_read_options record_options;
    int cursor_finished, cursor_scalar, cursor_pending;
    atomic_int cancelled;
    void *control_user;
    ud_status (*check)(void *);
    int transaction, cleaning, record_code;
    int read_only_scan;
    char branch_id[65];
    uint8_t branch_digest[32];
} sqlite_state;
static ud_status control(sqlite_state *p) {
    if (p->cleaning)
        return UD_OK;
    if (p->check) {
        ud_status status = p->check(p->control_user);
        if (status)
            return status;
    }
    return atomic_load(&p->cancelled) || p->host.cancelled(p->host.context) ? UD_CANCELLED : UD_OK;
}
static int progress(void *user) {
    return control(user) != UD_OK;
}
static int wait_for_writer(void *user, int attempt) {
    sqlite_state *p = user;
    if (attempt >= 6000 || control(p))
        return 0;
    sqlite3_sleep(5);
    return control(p) == UD_OK;
}
static void set_control(void *state, void *user, ud_status (*check)(void *)) {
    sqlite_state *p = state;
    p->control_user = user;
    p->check = check;
    atomic_store(&p->cancelled, 0);
    sqlite3_progress_handler(p->db, 1000, progress, p);
}
static ud_status failure(sqlite_state *p, int code) {
    p->record_code = code;
    ud_status interrupted = control(p);
    if (interrupted)
        return interrupted;
    return p->host.report(p->host.context,
                          code == SQLITE_BUSY || code == SQLITE_LOCKED ? UD_BUSY
                          : code == SQLITE_READONLY || code == SQLITE_AUTH || code == SQLITE_PERM
                              ? UD_AUTH
                          : code == SQLITE_INTERRUPT          ? UD_CANCELLED
                          : (code & 255) == SQLITE_CONSTRAINT ? UD_QUALITY
                          : code == SQLITE_TOOBIG             ? UD_TRUNCATED
                          : code == SQLITE_MISMATCH           ? UD_PARSE
                                                              : UD_IO,
                          "SQLite operation failed (diagnostic values redacted)");
}
static ud_status configure_database(sqlite_state *p) {
    int rc = sqlite3_exec(p->db, "PRAGMA foreign_keys=ON;PRAGMA main.journal_mode=WAL;"
                                 "PRAGMA main.synchronous=FULL", NULL, NULL, NULL);
    if (rc != SQLITE_OK)
        return failure(p, rc);
    sqlite3_stmt *statement = NULL;
    rc = sqlite3_prepare_v2(p->db,
        "SELECT (SELECT foreign_keys FROM pragma_foreign_keys)=1,"
        "(SELECT journal_mode FROM pragma_journal_mode)='wal',"
        "(SELECT synchronous FROM pragma_synchronous)=2", -1, &statement, NULL);
    if (rc != SQLITE_OK)
        return failure(p, rc);
    rc = sqlite3_step(statement);
    ud_status status = UD_OK;
    if (rc != SQLITE_ROW)
        status = failure(p, rc);
    else if (!sqlite3_column_int(statement, 0) || !sqlite3_column_int(statement, 1) ||
             !sqlite3_column_int(statement, 2) || sqlite3_compileoption_used("NO_SYNC"))
        status = p->host.report(p->host.context, UD_UNSUPPORTED,
                               "SQLite requires foreign key checks and WAL with FULL synchronization");
    sqlite3_finalize(statement);
    return status;
}
static ud_status initialize(const ud_host_api *h, const char *config, uint32_t n, void **out) {
    *out = NULL;
    sqlite_state *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    p->control_user = h->control_user;
    p->check = h->check_io;
    p->cursor_batch_size = 4096;
    atomic_init(&p->cancelled, 0);
    char *path = plugin_string(h, config, n);
    if (!path || !n) {
        h->deallocate(path);
        h->deallocate(p);
        return UD_INVALID;
    }
    int rc = sqlite3_open_v2(
        path, &p->db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
    h->deallocate(path);
    ud_status status = rc == SQLITE_OK ? UD_OK : failure(p, rc);
    if (!status) {
        sqlite3_busy_handler(p->db, wait_for_writer, p);
        status = configure_database(p);
    }
    if (status) {
        sqlite3_close(p->db);
        h->deallocate(p);
        return status;
    }
    *out = p;
    return UD_OK;
}
static int statement_authorize(void *, int, const char *, const char *, const char *, const char *);
static ud_status read_batch(sqlite_state *p, const ud_batch *schema, const char *sql, uint32_t n,
                            ud_batch **out) {
    ud_host_api *h = &p->host;
    if (!schema || (p->read && n) || (!p->read && !n) || n > INT_MAX)
        return UD_INVALID;
    if (!p->read) {
        const char *tail = NULL;
        sqlite3_set_authorizer(p->db, statement_authorize, p);
        int rc = sqlite3_prepare_v2(p->db, sql, (int)n, &p->read, &tail);
        sqlite3_set_authorizer(p->db, NULL, NULL);
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
        if (fields[i].type >= UD_STRUCT ||
            strcmp(fields[i].name, sqlite3_column_name(p->read, (int)i)))
            s = UD_SCHEMA;
    if (!s)
        s = h->batch_create(h->context, fields, columns, p->cursor_batch_size, out);
    ud_value *v = h->allocate(h->context, columns * sizeof(*v));
    ud_value *raw = h->allocate(h->context, columns * sizeof(*raw));
    uint32_t fetched = 0, bad = UINT32_MAX;
    if ((!v || !raw) && !s)
        s = UD_RESOURCE;
    while (!s && fetched < p->cursor_batch_size) {
        if (h->cancelled(h->context) || atomic_load(&p->cancelled)) {
            s = UD_CANCELLED;
            break;
        }
        int rc;
        if (p->cursor_pending) {
            p->cursor_pending = 0;
            rc = SQLITE_ROW;
        } else
            rc = sqlite3_step(p->read);
        if (rc == SQLITE_DONE) {
            sqlite3_finalize(p->read);
            p->read = NULL;
            break;
        }
        if (rc != SQLITE_ROW) {
            s = failure(p, rc);
            break;
        }
        fetched++;
        ud_status conversion = UD_OK;
        memset(v, 0, columns * sizeof(*v));
        for (uint32_t at = 0; at < columns; at++) {
            int type = sqlite3_column_type(p->read, (int)at);
            raw[at] = (ud_value){.type = UD_BINARY,
                                 .is_null = type == SQLITE_NULL,
                                 .text = type == SQLITE_BLOB
                                             ? (const char *)sqlite3_column_blob(p->read, (int)at)
                                             : (const char *)sqlite3_column_text(p->read, (int)at),
                                 .text_length = (uint32_t)sqlite3_column_bytes(p->read, (int)at)};
        }
        for (uint32_t i = 0; i < columns; i++) {
            ud_value *x = v + i;
            int type = sqlite3_column_type(p->read, (int)i);
            x->type = fields[i].type;
            x->precision = fields[i].precision;
            x->scale = fields[i].scale;
            x->is_null = type == SQLITE_NULL;
            if (x->is_null)
                continue;
            if (x->type == UD_UTF8 && type == SQLITE_TEXT) {
                x->text = (const char *)sqlite3_column_text(p->read, (int)i);
                x->text_length = (uint32_t)sqlite3_column_bytes(p->read, (int)i);
            } else if (x->type == UD_BINARY && type == SQLITE_BLOB) {
                x->text = (const char *)sqlite3_column_blob(p->read, (int)i);
                x->text_length = (uint32_t)sqlite3_column_bytes(p->read, (int)i);
            } else if ((x->type == UD_DECIMAL128 || x->type == UD_UINT64 ||
                        x->type == UD_INTERVAL) &&
                       (type == SQLITE_TEXT || type == SQLITE_INTEGER || type == SQLITE_FLOAT)) {
                s = h->parse_field(
                    h->context, fields + i, (const char *)sqlite3_column_text(p->read, (int)i),
                    (uint32_t)sqlite3_column_bytes(p->read, (int)i), UD_ROUND_EXACT, x);
            } else if ((x->type == UD_INT64 || x->type == UD_BOOL || x->type == UD_DATE32 ||
                        x->type == UD_TIME64 || x->type == UD_TIMESTAMP ||
                        x->type == UD_DURATION) &&
                       type == SQLITE_INTEGER)
                x->integer = sqlite3_column_int64(p->read, (int)i);
            else if (x->type == UD_FLOAT64 && type == SQLITE_FLOAT)
                x->real = sqlite3_column_double(p->read, (int)i);
            else {
                s = h->report(h->context, UD_PARSE,
                              "SQLite value type differs from explicit schema");
            }
            if (plugin_record_conversion(s)) {
                v[i].is_absent = 1;
                v[i].is_null = 1;
                if (!conversion) {
                    conversion = s;
                    bad = i;
                }
                s = UD_OK;
            }
            if (s)
                break;
        }
        if (!s)
            s = plugin_record_finish(h, &p->record_options, *out, fields, v, raw, columns,
                                     conversion, bad);
    }
    h->deallocate(fields);
    h->deallocate(v);
    h->deallocate(raw);
    if (s && p->read) {
        sqlite3_finalize(p->read);
        p->read = NULL;
    }
    if (!s && !h->rows(*out) && !fetched) {
        h->batch_destroy(*out);
        *out = NULL;
        return UD_END;
    }
    return s;
}
static void destroy(void *ptr) {
    sqlite_state *p = ptr;
    if (!p)
        return;
    sqlite3_finalize(p->read);
    p->host.batch_destroy(p->cursor_schema);
    sqlite3_close(p->db);
    p->host.deallocate(p);
}
static ud_status connector_test(void *state) {
    sqlite_state *p = state;
    int rc = sqlite3_exec(p->db, "SELECT 1", NULL, NULL, NULL);
    return rc == SQLITE_OK ? UD_OK : failure(p, rc);
}
static ud_status connector_discover(void *state, ud_string table, ud_batch **out) {
    sqlite_state *p = state;
    ud_host_api *h = &p->host;
    ud_field fields[] = {{.name = "name", .name_length = 4, .type = UD_UTF8},
                         {.name = "declared_type", .name_length = 13, .type = UD_UTF8},
                         {.name = "nullable", .name_length = 8, .type = UD_BOOL},
                         {.name = "key_order", .name_length = 9, .type = UD_INT64},
                         {.name = "readable", .name_length = 8, .type = UD_BOOL},
                         {.name = "appendable", .name_length = 10, .type = UD_BOOL},
                         {.name = "updatable", .name_length = 9, .type = UD_BOOL},
                         {.name = "deletable", .name_length = 9, .type = UD_BOOL}};
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(
        p->db,
        "SELECT name,type,(\"notnull\" OR (pk>0 AND (EXISTS(SELECT 1 FROM pragma_table_list WHERE "
        "name=?1 AND (wr OR strict)) OR (upper(type)='INTEGER' AND NOT EXISTS(SELECT 1 FROM "
        "pragma_index_list(?1) WHERE origin='pk'))))),pk FROM pragma_table_xinfo(?1) WHERE "
        "hidden=0 ORDER BY cid",
        -1, &stmt, NULL);
    if (rc != SQLITE_OK)
        return failure(p, rc);
    rc = sqlite3_bind_text64(stmt, 1, table.data, table.length, SQLITE_TRANSIENT, SQLITE_UTF8);
    ud_status s =
        rc == SQLITE_OK ? h->batch_create(h->context, fields, 8, 65536, out) : failure(p, rc);
    while (!s && (rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        ud_value values[8] = {{.type = UD_UTF8},
                              {.type = UD_UTF8},
                              {.type = UD_BOOL},
                              {.type = UD_INT64},
                              {.type = UD_BOOL, .integer = 1},
                              {.type = UD_BOOL},
                              {.type = UD_BOOL},
                              {.type = UD_BOOL}};
        for (int j = 0; j < 2; j++) {
            values[j].text = (const char *)sqlite3_column_text(stmt, j);
            values[j].text_length = (uint32_t)sqlite3_column_bytes(stmt, j);
        }
        values[2].integer = !sqlite3_column_int(stmt, 2);
        values[3].integer = sqlite3_column_int(stmt, 3);
        for (uint32_t j = 5; j < 8; j++)
            values[j].integer = sqlite3_db_readonly(p->db, "main") == 0;
        s = h->append(*out, values, 8);
    }
    if (!s && rc != SQLITE_DONE)
        s = failure(p, rc);
    sqlite3_finalize(stmt);
    if (!s && !h->rows(*out))
        s = UD_SCHEMA;
    return s;
}
static ud_status connector_read_close(void *state) {
    sqlite_state *p = state;
    memset(&p->record_options, 0, sizeof(p->record_options));
    int rc = sqlite3_finalize(p->read);
    p->read = NULL;
    p->host.batch_destroy(p->cursor_schema);
    p->cursor_schema = NULL;
    p->cursor_finished = 1;
    p->cursor_scalar = 0;
    p->cursor_pending = 0;
    if (p->read_only_scan) {
        int query_only = sqlite3_exec(p->db, "PRAGMA query_only=OFF", NULL, NULL, NULL);
        p->read_only_scan = 0;
        if (rc == SQLITE_OK)
            rc = query_only;
    }
    return rc == SQLITE_OK ? UD_OK : failure(p, rc);
}
static ud_status bind_connector_value(sqlite_state *, sqlite3_stmt *, int, const ud_value *,
                                      sqlite3_destructor_type);
static ud_status bind_parameters(sqlite_state *p, sqlite3_stmt *statement,
                                 const ud_batch *parameters, uint32_t row) {
    uint32_t count = parameters ? p->host.columns(parameters) : 0;
    if (sqlite3_bind_parameter_count(statement) != (int)count ||
        (parameters && row >= p->host.rows(parameters)))
        return UD_SCHEMA;
    for (uint32_t i = 0; i < count; i++) {
        ud_value value;
        ud_status s = p->host.get(parameters, row, i, &value);
        if (!s)
            s = bind_connector_value(p, statement, (int)i + 1, &value, SQLITE_TRANSIENT);
        if (s)
            return s;
    }
    return UD_OK;
}
static ud_type sqlite_result_type(const char *declared) {
    if (!declared || !*declared)
        return UD_UTF8;
    char type[96];
    size_t length = strlen(declared);
    if (length >= sizeof(type))
        length = sizeof(type) - 1;
    for (size_t i = 0; i < length; i++)
        type[i] = (char)toupper((unsigned char)declared[i]);
    type[length] = 0;
    if (strstr(type, "BOOL"))
        return UD_BOOL;
    if (strstr(type, "INT"))
        return UD_INT64;
    if (strstr(type, "CHAR") || strstr(type, "CLOB") || strstr(type, "TEXT"))
        return UD_UTF8;
    if (strstr(type, "BLOB"))
        return UD_BINARY;
    if (strstr(type, "REAL") || strstr(type, "FLOA") || strstr(type, "DOUB"))
        return UD_FLOAT64;
    if (strstr(type, "DEC") || strstr(type, "NUM") || strstr(type, "REAL"))
        return UD_DECIMAL128;
    if (strstr(type, "DATE"))
        return UD_DATE32;
    if (strstr(type, "TIME"))
        return UD_TIME64;
    return UD_UTF8;
}
static ud_type sqlite_runtime_type(int type) {
    return type == SQLITE_INTEGER ? UD_INT64
           : type == SQLITE_FLOAT  ? UD_FLOAT64
           : type == SQLITE_BLOB   ? UD_BINARY
                                    : UD_UTF8;
}
static ud_status sqlite_result_fields(sqlite_state *p, sqlite3_stmt *statement,
                                      const ud_batch *schema, ud_field **out,
                                      uint32_t *columns) {
    ud_host_api *h = &p->host;
    *out = NULL;
    *columns = (uint32_t)sqlite3_column_count(statement);
    if (schema) {
        if (h->columns(schema) != *columns)
            return UD_SCHEMA;
        return plugin_fields(h, schema, out);
    }
    ud_field *fields = h->allocate(h->context, *columns * sizeof(*fields));
    if (!fields && *columns)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < *columns; i++) {
        const char *name = sqlite3_column_name(statement, (int)i);
        const char *declared = sqlite3_column_decltype(statement, (int)i);
        if (!name || !*name) {
            h->deallocate(fields);
            return UD_SCHEMA;
        }
        fields[i] = (ud_field){.name = name,
                               .name_length = (uint32_t)strlen(name),
                               .type = declared ? sqlite_result_type(declared)
                                                 : p->cursor_pending
                                                       ? sqlite_runtime_type(sqlite3_column_type(
                                                             statement, (int)i))
                                                       : UD_UTF8,
                               .nullable = 1};
    }
    *out = fields;
    return UD_OK;
}
static ud_status connector_read_open(void *state, const ud_read_options *options,
                                     const ud_batch *schema) {
    sqlite_state *p = state;
    ud_host_api *h = &p->host;
    if (p->read || p->cursor_schema || options->sql.length > INT_MAX)
        return UD_INVALID;
    atomic_store(&p->cancelled, 0);
    int query_only = sqlite3_exec(p->db, "PRAGMA query_only=ON", NULL, NULL, NULL);
    if (query_only != SQLITE_OK)
        return failure(p, query_only);
    p->read_only_scan = 1;
    p->cursor_pending = 0;
    const char *tail = NULL;
    int rc =
        sqlite3_prepare_v2(p->db, options->sql.data, (int)options->sql.length, &p->read, &tail);
    if (rc != SQLITE_OK) {
        connector_read_close(p);
        return failure(p, rc);
    }
    while (tail < options->sql.data + options->sql.length &&
           (*tail == ' ' || *tail == '\n' || *tail == '\r' || *tail == '\t'))
        tail++;
    ud_status s = tail != options->sql.data + options->sql.length || !p->read ||
                          !sqlite3_stmt_readonly(p->read)
                      ? UD_INVALID
                      : UD_OK;
    if (!s)
        s = bind_parameters(p, p->read, options->parameters, options->parameter_row);
    if (!s && !schema) {
        int needs_peek = 0;
        int result_columns = sqlite3_column_count(p->read);
        for (int i = 0; i < result_columns; i++)
            if (!sqlite3_column_decltype(p->read, i)) {
                needs_peek = 1;
                break;
            }
        if (needs_peek) {
            rc = sqlite3_step(p->read);
            if (rc == SQLITE_ROW)
                p->cursor_pending = 1;
            else if (rc != SQLITE_DONE)
                s = failure(p, rc);
        }
    }
    uint32_t columns = 0;
    ud_field *fields = NULL;
    if (!s)
        s = sqlite_result_fields(p, p->read, schema, &fields, &columns);
    if (!s && options->result_mode == UD_READ_SCALAR && columns != 1)
        s = UD_SCHEMA;
    for (uint32_t j = 0; !s && j < columns; j++)
        if (fields[j].type >= UD_STRUCT ||
            strcmp(fields[j].name, sqlite3_column_name(p->read, (int)j)))
            s = UD_SCHEMA;
    if (!s)
        s = plugin_schema_outputs(h, fields, columns, &p->cursor_schema, options->result_schema);
    h->deallocate(fields);
    if (s) {
        connector_read_close(p);
        return s;
    }
    p->cursor_finished = 0;
    p->cursor_scalar = options->result_mode == UD_READ_SCALAR;
    p->cursor_batch_size = p->cursor_scalar ? 1 : options->batch_size;
    p->record_options = *options;
    return UD_OK;
}
static ud_status connector_read_next(void *state, ud_batch **out) {
    sqlite_state *p = state;
    *out = NULL;
    if (p->cursor_finished)
        return UD_END;
    if (atomic_load(&p->cancelled))
        return UD_CANCELLED;
    ud_status s = read_batch(p, p->cursor_schema, "", 0, out);
    if (!p->read)
        p->cursor_finished = 1;
    else if (!s && p->cursor_scalar && *out && p->host.rows(*out)) {
        sqlite3_finalize(p->read);
        p->read = NULL;
        p->cursor_finished = 1;
    }
    return s;
}
static void connector_cancel(void *state) {
    sqlite_state *p = state;
    atomic_store(&p->cancelled, 1);
    sqlite3_interrupt(p->db);
}
static ud_status connector_receipt(void *state, ud_string tenant, ud_string job, ud_string batch_id,
                                   ud_commit_receipt *out) {
    sqlite_state *p = state;
    memset(out, 0, sizeof(*out));
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(p->db,
                                "SELECT digest,source_position FROM ud_sdk_receipts_v11 WHERE "
                                "tenant=?1 AND job=?2 AND batch_id=?3",
                                -1, &stmt, NULL);
    if (rc != SQLITE_OK)
        return p->host.report(p->host.context, UD_COMMIT_UNKNOWN,
                              "receipt store is unavailable; commit outcome cannot be proved");
    ud_string keys[] = {tenant, job, batch_id};
    for (int j = 0; j < 3 && rc == SQLITE_OK; j++)
        rc = sqlite3_bind_text64(stmt, j + 1, keys[j].data, keys[j].length, SQLITE_TRANSIENT,
                                 SQLITE_UTF8);
    if (rc == SQLITE_OK)
        rc = sqlite3_step(stmt);
    ud_status s = UD_OK;
    if (rc == SQLITE_ROW) {
        if (sqlite3_column_type(stmt, 0) != SQLITE_BLOB || sqlite3_column_bytes(stmt, 0) != 32 ||
            sqlite3_column_type(stmt, 1) != SQLITE_TEXT)
            s = UD_CORRUPT;
        else {
            memcpy(out->digest, sqlite3_column_blob(stmt, 0), 32);
            const char *position = (const char *)sqlite3_column_text(stmt, 1);
            uint32_t length = (uint32_t)sqlite3_column_bytes(stmt, 1);
            char *owned = plugin_string(&p->host, position, length);
            if (!owned)
                s = UD_RESOURCE;
            else {
                out->source_position = (ud_string){owned, length};
                out->committed = 1;
            }
        }
    } else if (rc != SQLITE_DONE)
        s = UD_COMMIT_UNKNOWN;
    sqlite3_finalize(stmt);
    return s;
}
static ud_status bind_connector_value(sqlite_state *p, sqlite3_stmt *stmt, int index,
                                      const ud_value *v, sqlite3_destructor_type lifetime) {
    int rc;
    if (v->is_null)
        rc = sqlite3_bind_null(stmt, index);
    else if (v->type == UD_UTF8)
        rc = sqlite3_bind_text64(stmt, index, v->text, v->text_length, lifetime, SQLITE_UTF8);
    else if (v->type == UD_BINARY)
        rc = sqlite3_bind_blob64(stmt, index, v->text ? v->text : "", v->text_length, lifetime);
    else if (v->type == UD_DECIMAL128 || v->type == UD_UINT64 || v->type == UD_INTERVAL) {
        char text[160];
        uint32_t length;
        ud_status s = p->host.format_value(v, text, sizeof(text), &length);
        if (s)
            return s;
        rc = sqlite3_bind_text(stmt, index, text, (int)length - 1, SQLITE_TRANSIENT);
    } else if (v->type == UD_FLOAT64) {
        if (isnan(v->real))
            return p->host.report(
                p->host.context, UD_UNSUPPORTED,
                "SQLite cannot preserve Float64 NaN; refusing SQL NULL conversion");
        rc = sqlite3_bind_double(stmt, index, v->real);
    } else if (v->type <= UD_TIMESTAMP || v->type == UD_DURATION)
        rc = sqlite3_bind_int64(stmt, index, v->integer);
    else
        return UD_UNSUPPORTED;
    return rc == SQLITE_OK ? UD_OK : failure(p, rc);
}
static ud_status validate_target(sqlite_state *p, ud_string table, const ud_batch *batch) {
    ud_host_api *h = &p->host;
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(
        p->db, "SELECT name,type FROM pragma_table_xinfo(?1) WHERE hidden=0", -1, &stmt, NULL);
    if (rc != SQLITE_OK)
        return failure(p, rc);
    rc = sqlite3_bind_text64(stmt, 1, table.data, table.length, SQLITE_TRANSIENT, SQLITE_UTF8);
    uint32_t columns = h->columns(batch);
    uint8_t *seen = h->allocate(h->context, columns);
    ud_status s = !seen ? UD_RESOURCE : rc != SQLITE_OK ? failure(p, rc) : UD_OK;
    while (!s && (rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        const char *declared = (const char *)sqlite3_column_text(stmt, 1);
        int length = sqlite3_column_bytes(stmt, 1);
        char upper[128];
        if (!declared || length < 0 || length >= (int)sizeof(upper)) {
            s = UD_UNSUPPORTED;
            break;
        }
        for (int j = 0; j < length; j++)
            upper[j] = (char)toupper((unsigned char)declared[j]);
        upper[length] = 0;
        int affinity = strstr(upper, "INT")                                                      ? 1
                       : strstr(upper, "CHAR") || strstr(upper, "CLOB") || strstr(upper, "TEXT") ? 2
                       : !length || strstr(upper, "BLOB")                                        ? 3
                       : strstr(upper, "REAL") || strstr(upper, "FLOA") || strstr(upper, "DOUB")
                           ? 4
                           : 5;
        for (uint32_t j = 0; j < columns; j++) {
            ud_field f;
            h->field(batch, j, &f);
            if (strcmp(name, f.name))
                continue;
            seen[j] = 1;
            int expected = f.type == UD_UTF8 || f.type == UD_DECIMAL128 || f.type == UD_UINT64 ||
                                   f.type == UD_INTERVAL
                               ? 2
                           : f.type == UD_BINARY  ? 3
                           : f.type == UD_FLOAT64 ? 4
                                                  : 1;
            int temporal = (f.type == UD_TIMESTAMP && !strcmp(upper, "TIMESTAMP_UTC")) ||
                           (f.type == UD_DATE32 && !strcmp(upper, "DATE32")) ||
                           (f.type == UD_TIME64 && !strcmp(upper, "TIME64")) ||
                           (f.type == UD_DURATION && !strcmp(upper, "DURATION"));
            int boolean =
                f.type == UD_BOOL && (!strcmp(upper, "BOOLEAN") || !strcmp(upper, "BOOL"));
            if (expected != affinity && !temporal && !boolean)
                s = h->report(h->context, UD_SCHEMA,
                              "target SQLite affinity would coerce a typed value; use TEXT for "
                              "Decimal and INTEGER for UTC microseconds");
        }
    }
    if (!s && rc != SQLITE_DONE)
        s = failure(p, rc);
    for (uint32_t j = 0; !s && j < columns; j++)
        if (!seen[j])
            s = UD_SCHEMA;
    h->deallocate(seen);
    sqlite3_finalize(stmt);
    return s;
}
static ud_status write_rows(sqlite_state *p, const ud_write_options *options,
                            const ud_batch *batch, uint64_t *affected) {
    ud_host_api *h = &p->host;
    const int changeset = options->mode == UD_WRITE_CHANGESET;
    const uint32_t rows = h->rows(batch);
    ud_sql_statement plan = {0};
    sqlite3_stmt *stmt = NULL;
    ud_status status = UD_OK;
    for (uint32_t row = 0; !status && row < rows; row++) {
        status = control(p);
        if (status)
            break;
        if (!stmt || changeset) {
            sqlite3_finalize(stmt);
            stmt = NULL;
            h->statement_clear(&plan);
            status = h->compile_write(h->context, UD_SQL_SQLITE, UD_SQL_PARAMETER_QUESTION,
                                      options, batch, row, 1, &plan);
            if (!status) {
                int rc = sqlite3_prepare_v2(p->db, plan.sql.data, -1, &stmt, NULL);
                if (rc != SQLITE_OK)
                    status = failure(p, rc);
            }
        } else {
            int rc = sqlite3_reset(stmt);
            if (rc != SQLITE_OK)
                status = failure(p, rc);
        }
        /* Every parameter is rebound. Sealed values outlive this statement. */
        for (uint32_t i = 0; !status && i < plan.parameter_count; i++) {
            ud_sql_parameter ref = plan.parameters[i];
            const ud_batch *source = ref.source == UD_SQL_BEFORE ? options->before : batch;
            ud_value value;
            status = h->get(source, changeset ? ref.row : row, ref.column, &value);
            if (!status)
                status = bind_connector_value(p, stmt, (int)i + 1, &value, SQLITE_STATIC);
        }
        if (status)
            break;
        int rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            status = failure(p, rc);
            break;
        }
        uint64_t changed = (uint64_t)sqlite3_changes64(p->db);
        *affected += changed;
        if (changeset && options->operations[row] != UD_CHANGE_UPSERT &&
            options->operations[row] != UD_CHANGE_TRUNCATE && changed != 1)
            status = h->report(h->context, UD_FENCED,
                               "changeset preimage or current business-key conflict");
    }
    sqlite3_finalize(stmt);
    h->statement_clear(&plan);
    return status;
}

static ud_status connector_write(void *state, ud_string tenant, const ud_write_options *options,
                                 const ud_batch *batch, const uint8_t digest[32]) {
    sqlite_state *p = state;
    ud_host_api *h = &p->host;
    if (p->read || p->cursor_schema)
        return UD_BUSY;
    int receipted = options->delivery != UD_DELIVERY_RESPONSE;
    if (receipted && !digest)
        return UD_INVALID;
    atomic_store(&p->cancelled, 0);
    if (options->mode == UD_WRITE_CHANGESET && p->transaction)
        return UD_UNSUPPORTED;
    int own = !p->transaction;
    int rc = own ? sqlite3_exec(p->db, "BEGIN IMMEDIATE", NULL, NULL, NULL) : SQLITE_OK;
    if (rc != SQLITE_OK) {
        return rc == SQLITE_BUSY || rc == SQLITE_LOCKED ? UD_TRANSIENT : failure(p, rc);
    }
    ud_status s = validate_target(p, options->table, batch);
    ud_commit_receipt receipt = {0};
    if (!s && receipted)
        s = connector_receipt(p, tenant, options->job, options->batch_id, &receipt);
    int existing = receipt.committed;
    if (!s && existing &&
        (memcmp(receipt.digest, digest, 32) ||
         receipt.source_position.length != options->source_position.length ||
         (options->source_position.length &&
          memcmp(receipt.source_position.data, options->source_position.data,
                 options->source_position.length))))
        s = h->report(h->context, UD_CORRUPT,
                      "batch id was reused with a different payload / cursor / destination");
    h->deallocate((void *)receipt.source_position.data);
    uint64_t affected = 0;
    if (!s && !existing)
        s = write_rows(p, options, batch, &affected);
    sqlite3_stmt *stmt = NULL;
    if (!s && !existing && receipted) {
        rc = sqlite3_prepare_v2(
            p->db,
            "INSERT INTO ud_sdk_receipts_v11(tenant,job,batch_id,digest,source_position) "
            "VALUES(?1,?2,?3,?4,?5)",
            -1, &stmt, NULL);
        if (rc != SQLITE_OK)
            s = failure(p, rc);
        ud_string strings[] = {tenant, options->job, options->batch_id};
        for (int j = 0; !s && j < 3; j++) {
            rc = sqlite3_bind_text64(stmt, j + 1, strings[j].data, strings[j].length,
                                     SQLITE_TRANSIENT, SQLITE_UTF8);
            if (rc != SQLITE_OK)
                s = failure(p, rc);
        }
        if (!s) {
            rc = sqlite3_bind_blob(stmt, 4, digest, 32, SQLITE_TRANSIENT);
            if (rc != SQLITE_OK)
                s = failure(p, rc);
        }
        if (!s) {
            rc = sqlite3_bind_text64(
                stmt, 5, options->source_position.data ? options->source_position.data : "",
                options->source_position.length, SQLITE_TRANSIENT, SQLITE_UTF8);
            if (rc != SQLITE_OK)
                s = failure(p, rc);
        }
        if (!s && (rc = sqlite3_step(stmt)) != SQLITE_DONE)
            s = failure(p, rc);
    }
    sqlite3_finalize(stmt);
    if (!s && (h->cancelled(h->context) || atomic_load(&p->cancelled)))
        s = UD_CANCELLED;
    if (!s && own) {
        rc = sqlite3_exec(p->db, "COMMIT", NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_exec(p->db, "ROLLBACK", NULL, NULL, NULL);
            return h->report(
                h->context, UD_COMMIT_UNKNOWN,
                "commit returned an error; query the receipt before advancing source position");
        }
    } else if (s && own && sqlite3_exec(p->db, "ROLLBACK", NULL, NULL, NULL) != SQLITE_OK &&
               !sqlite3_get_autocommit(p->db))
        return h->report(h->context, UD_COMMIT_UNKNOWN,
                         "rollback failed; query the receipt before any replay");
    if (!s && options->affected_rows)
        *options->affected_rows = affected;
    return s;
}
static int statement_authorize(void *user, int action, const char *a, const char *b, const char *db,
                               const char *trigger) {
    (void)user;
    (void)a;
    (void)db;
    (void)trigger;
    /* Persistent data is allowed; lease-local session mutation cannot enter
     * the reusable pool. Driver metadata uses the separate typed contract. */
    return action == SQLITE_TRANSACTION || action == SQLITE_SAVEPOINT || action == SQLITE_ATTACH ||
                   action == SQLITE_DETACH || (action == SQLITE_PRAGMA && b) ||
                   action == SQLITE_CREATE_TEMP_TABLE || action == SQLITE_CREATE_TEMP_INDEX ||
                   action == SQLITE_CREATE_TEMP_TRIGGER || action == SQLITE_CREATE_TEMP_VIEW
               ? SQLITE_DENY
               : SQLITE_OK;
}
static ud_status savepoint(void *state, ud_string statement) {
    sqlite_state *p = state;
    if (!p->transaction)
        return UD_UNSUPPORTED;
    char *sql = plugin_string(&p->host, statement.data, statement.length);
    if (!sql)
        return UD_RESOURCE;
    int result = sqlite3_exec(p->db, sql, NULL, NULL, NULL);
    p->host.deallocate(sql);
    return result == SQLITE_OK ? UD_OK : failure(p, result);
}
static ud_status record_diagnostic(void *state, ud_string *out) {
    sqlite_state *p = state;
    *out = (ud_string){0};
    if (!p->record_code)
        return UD_OK;
    char code[32];
    int length = snprintf(code, sizeof(code), "SQLITE_%d", p->record_code);
    char *copy = plugin_string(&p->host, code, (uint32_t)length);
    if (!copy)
        return UD_RESOURCE;
    p->record_code = 0;
    *out = (ud_string){copy, (uint32_t)length};
    return UD_OK;
}
static ud_status connector_execute_command(void *state, const ud_read_options *options,
                                           uint64_t *processed) {
    if (processed)
        *processed = 0;
    ud_string statement = options->sql;
    sqlite_state *p = state;
    ud_host_api *h = &p->host;
    char *sql = plugin_string(&p->host, statement.data, statement.length);
    if (!sql)
        return UD_RESOURCE;
    int own = !p->transaction;
    ud_status status = control(p);
    int rc = SQLITE_OK;
    if (!status && own) {
        rc = sqlite3_exec(p->db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
        if (rc != SQLITE_OK)
            status = failure(p, rc);
    }
    if (!status) {
        const char *cursor = sql;
        uint32_t rows = options->parameters ? h->rows(options->parameters) : 1;
        sqlite3_set_authorizer(p->db, statement_authorize, p);
        while (!status && *cursor) {
            sqlite3_stmt *stmt = NULL;
            const char *tail = NULL;
            rc = sqlite3_prepare_v2(p->db, cursor, -1, &stmt, &tail);
            if (rc != SQLITE_OK)
                status = failure(p, rc);
            if (!stmt) {
                cursor = tail ? tail : cursor + strlen(cursor);
                continue;
            }
            for (uint32_t row = 0; !status && row < rows; row++) {
                sqlite3_reset(stmt);
                sqlite3_clear_bindings(stmt);
                status = bind_parameters(p, stmt, options->parameters, row);
                if (!status) {
                    rc = sqlite3_step(stmt);
                    if (rc != SQLITE_DONE)
                        status = rc == SQLITE_ROW ? UD_SCHEMA : failure(p, rc);
                    else if (processed)
                        *processed += (uint64_t)sqlite3_changes(p->db);
                }
                if (!status)
                    status = control(p);
            }
            sqlite3_finalize(stmt);
            cursor = tail ? tail : cursor + strlen(cursor);
        }
        sqlite3_set_authorizer(p->db, NULL, NULL);
    }
    p->host.deallocate(sql);
    if (own) {
        if (!status) {
            rc = sqlite3_exec(p->db, "COMMIT", NULL, NULL, NULL);
            if (rc != SQLITE_OK)
                status = UD_COMMIT_UNKNOWN;
        }
        if (status && !sqlite3_get_autocommit(p->db)) {
            p->cleaning = 1;
            rc = sqlite3_exec(p->db, "ROLLBACK", NULL, NULL, NULL);
            p->cleaning = 0;
            if (rc != SQLITE_OK)
                status = UD_COMMIT_UNKNOWN;
        }
    }
    return status;
}
static ud_status begin_transaction(void *state, int32_t isolation) {
    sqlite_state *p = state;
    if (p->transaction ||
        (isolation != UD_ISOLATION_SERIALIZABLE && isolation != UD_ISOLATION_READ_SNAPSHOT))
        return UD_UNSUPPORTED;
    ud_status checked = control(p);
    if (checked)
        return checked;
    const char *sql = isolation == UD_ISOLATION_READ_SNAPSHOT ? "BEGIN DEFERRED"
                                                               : "BEGIN IMMEDIATE";
    int rc = sqlite3_exec(p->db, sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK)
        return failure(p, rc);
    p->transaction = 1;
    return UD_OK;
}
static ud_status commit_transaction(void *state) {
    sqlite_state *p = state;
    if (!p->transaction)
        return UD_INVALID;
    ud_status checked = control(p);
    if (checked)
        return checked;
    int rc = sqlite3_exec(p->db, "COMMIT", NULL, NULL, NULL);
    if (rc != SQLITE_OK)
        return UD_COMMIT_UNKNOWN;
    p->transaction = 0;
    return UD_OK;
}
static ud_status rollback_transaction(void *state) {
    sqlite_state *p = state;
    if (!p->transaction)
        return UD_INVALID;
    p->cleaning = 1;
    int rc = sqlite3_exec(p->db, "ROLLBACK", NULL, NULL, NULL);
    p->cleaning = 0;
    if (rc != SQLITE_OK)
        return UD_COMMIT_UNKNOWN;
    p->transaction = 0;
    return UD_OK;
}
static ud_status ensure_transaction_receipts(sqlite_state *p) {
    if (!p || p->transaction || p->read || p->cursor_schema || !sqlite3_get_autocommit(p->db))
        return UD_BUSY;
    int rc = sqlite3_exec(
        p->db,
        "CREATE TABLE IF NOT EXISTS ud_sdk_tx_receipts_v11(branch_id TEXT COLLATE BINARY "
        "PRIMARY KEY CHECK(length(branch_id)=64),digest BLOB NOT NULL CHECK(length(digest)=32))",
        NULL, NULL, NULL);
    return rc == SQLITE_OK ? UD_OK : failure(p, rc);
}
static int branch_matches(const sqlite_state *p, const ud_transaction_branch *branch) {
    return p->branch_id[0] && !memcmp(p->branch_id, branch->id.data, 64) &&
           !memcmp(p->branch_digest, branch->digest, sizeof(p->branch_digest));
}
static ud_status local_receipt_state(sqlite_state *p, const ud_transaction_branch *branch,
                                     int32_t *result) {
    if (!p || !branch || !result)
        return UD_INVALID;
    *result = UD_TX_UNKNOWN;
    if (p->transaction) {
        if (!branch_matches(p, branch))
            return UD_FENCED;
        *result = UD_TX_ACTIVE;
        return UD_OK;
    }
    if (p->read || p->cursor_schema || !sqlite3_get_autocommit(p->db))
        return UD_BUSY;
    ud_status status = ensure_transaction_receipts(p);
    if (status)
        return status;
    int rc = sqlite3_exec(p->db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
    if (rc != SQLITE_OK)
        return failure(p, rc);
    sqlite3_stmt *stmt = NULL;
    status = UD_OK;
    rc = sqlite3_prepare_v2(
        p->db, "SELECT digest FROM ud_sdk_tx_receipts_v11 WHERE branch_id=?1", -1, &stmt, NULL);
    if (rc != SQLITE_OK)
        status = failure(p, rc);
    if (!status)
        rc = sqlite3_bind_text64(stmt, 1, branch->id.data, branch->id.length, SQLITE_TRANSIENT,
                                 SQLITE_UTF8);
    if (!status && rc != SQLITE_OK)
        status = failure(p, rc);
    if (!status)
        rc = sqlite3_step(stmt);
    if (!status && rc == SQLITE_ROW) {
        if (sqlite3_column_type(stmt, 0) != SQLITE_BLOB || sqlite3_column_bytes(stmt, 0) != 32)
            status = UD_CORRUPT;
        else if (memcmp(sqlite3_column_blob(stmt, 0), branch->digest, 32))
            status = UD_FENCED;
        else
            *result = UD_TX_COMMITTED;
    } else if (!status && rc == SQLITE_DONE)
        *result = UD_TX_NOT_FOUND;
    else if (!status)
        status = failure(p, rc);
    sqlite3_finalize(stmt);
    rc = sqlite3_exec(p->db, "ROLLBACK", NULL, NULL, NULL);
    if (!status && rc != SQLITE_OK)
        status = UD_COMMIT_UNKNOWN;
    return status;
}
static ud_status local_stage_receipt(sqlite_state *p, const ud_transaction_branch *branch) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(
        p->db, "SELECT digest FROM ud_sdk_tx_receipts_v11 WHERE branch_id=?1", -1, &stmt, NULL);
    ud_status status = rc == SQLITE_OK ? UD_OK : failure(p, rc);
    if (!status)
        rc = sqlite3_bind_text64(stmt, 1, branch->id.data, branch->id.length, SQLITE_TRANSIENT,
                                 SQLITE_UTF8);
    if (!status && rc != SQLITE_OK)
        status = failure(p, rc);
    if (!status)
        rc = sqlite3_step(stmt);
    if (!status && rc == SQLITE_ROW) {
        /* A proof already committed for this identity fences any replay,
         * even when its digest happens to match. */
        status = UD_FENCED;
    } else if (!status && rc != SQLITE_DONE)
        status = failure(p, rc);
    sqlite3_finalize(stmt);
    if (!status) {
        rc = sqlite3_prepare_v2(
            p->db,
            "INSERT INTO ud_sdk_tx_receipts_v11(branch_id,digest) VALUES(?1,?2)", -1, &stmt,
            NULL);
        if (rc != SQLITE_OK)
            status = failure(p, rc);
    }
    if (!status)
        rc = sqlite3_bind_text64(stmt, 1, branch->id.data, branch->id.length, SQLITE_TRANSIENT,
                                 SQLITE_UTF8);
    if (!status && rc != SQLITE_OK)
        status = failure(p, rc);
    if (!status)
        rc = sqlite3_bind_blob(stmt, 2, branch->digest, 32, SQLITE_TRANSIENT);
    if (!status && rc != SQLITE_OK)
        status = failure(p, rc);
    if (!status && (rc = sqlite3_step(stmt)) != SQLITE_DONE)
        status = rc == SQLITE_CONSTRAINT ? UD_FENCED : failure(p, rc);
    sqlite3_finalize(stmt);
    return status;
}
static ud_status transaction_recover(void *state, const ud_transaction_branch *branch,
                                     int32_t *result) {
    return local_receipt_state(state, branch, result);
}
static ud_status transaction_begin(void *state, const ud_transaction_branch *branch) {
    sqlite_state *p = state;
    if (p->transaction || p->branch_id[0] || p->read || p->cursor_schema)
        return UD_BUSY;
    ud_status status = ensure_transaction_receipts(p);
    if (!status)
        status = begin_transaction(p, branch->isolation);
    if (!status) {
        memcpy(p->branch_id, branch->id.data, 64);
        p->branch_id[64] = 0;
        memcpy(p->branch_digest, branch->digest, sizeof(p->branch_digest));
    }
    return status;
}
static ud_status transaction_prepare(void *state, const ud_transaction_branch *branch) {
    sqlite_state *p = state;
    if (!p->transaction || !branch_matches(p, branch))
        return UD_FENCED;
    /* The proof is part of the still-open SQLite transaction. It becomes
     * authoritative only if the provider COMMIT below succeeds. */
    return local_stage_receipt(p, branch);
}
static ud_status transaction_resolve(void *state, const ud_transaction_branch *branch,
                                     int32_t decision) {
    sqlite_state *p = state;
    if (!p->transaction || !branch_matches(p, branch))
        return UD_FENCED;
    ud_status status = decision == UD_TX_COMMIT ? commit_transaction(p) : rollback_transaction(p);
    if (!status) {
        memset(p->branch_id, 0, sizeof(p->branch_id));
        memset(p->branch_digest, 0, sizeof(p->branch_digest));
    }
    return status;
}
static ud_status metadata(void *state, const ud_metadata_options *o, ud_batch **out) {
    sqlite_state *p = state;
    ud_host_api *h = &p->host;
    if (o->offset > INT64_MAX)
        return UD_INVALID;
    const char *queries[] = {
        NULL,
        "SELECT name,'','catalog',seq,0,0,'',0 FROM pragma_database_list ORDER BY seq LIMIT ?2 "
        "OFFSET ?3",
        "SELECT name,'','schema',seq,0,0,'',0 FROM pragma_database_list ORDER BY seq LIMIT ?2 "
        "OFFSET ?3",
        "SELECT name,'main',type,0,0,0,'',0 FROM sqlite_schema WHERE type IN ('table','view') AND "
        "name NOT LIKE 'sqlite_%' ORDER BY name LIMIT ?2 OFFSET ?3",
        "SELECT name,?1,type,cid,NOT [notnull],pk,'',0 FROM pragma_table_info(?1) ORDER BY cid "
        "LIMIT ?2 OFFSET ?3",
        "SELECT name,?1,type,cid,0,pk,'',1 FROM pragma_table_info(?1) WHERE pk>0 ORDER BY pk LIMIT "
        "?2 OFFSET ?3",
        "SELECT [from],?1,'foreign_key',seq,1,id,[table] || '.' || [to],0 FROM "
        "pragma_foreign_key_list(?1) ORDER BY id,seq LIMIT ?2 OFFSET ?3",
        "SELECT name,?1,'index',seq,0,0,origin,[unique] FROM pragma_index_list(?1) ORDER BY seq "
        "LIMIT ?2 OFFSET ?3"};
    if (o->kind < UD_METADATA_CATALOGS || o->kind > UD_METADATA_INDEXES)
        return UD_INVALID;
    sqlite3_stmt *q = NULL;
    int rc = sqlite3_prepare_v2(p->db, queries[o->kind], -1, &q, NULL);
    if (rc != SQLITE_OK)
        return failure(p, rc);
    sqlite3_bind_text64(q, 1, o->parent.data ? o->parent.data : "", o->parent.length,
                        SQLITE_TRANSIENT, SQLITE_UTF8);
    sqlite3_bind_int64(q, 2, o->limit);
    sqlite3_bind_int64(q, 3, (sqlite3_int64)o->offset);
    ud_field fields[] = {
        {.name = "name", .name_length = 4, .type = UD_UTF8, .nullable = 1},
        {.name = "parent", .name_length = 6, .type = UD_UTF8, .nullable = 1},
        {.name = "declared_type", .name_length = 13, .type = UD_UTF8, .nullable = 1},
        {.name = "ordinal", .name_length = 7, .type = UD_INT64},
        {.name = "nullable", .name_length = 8, .type = UD_BOOL},
        {.name = "key_order", .name_length = 9, .type = UD_INT64},
        {.name = "reference", .name_length = 9, .type = UD_UTF8, .nullable = 1},
        {.name = "unique", .name_length = 6, .type = UD_BOOL}};
    ud_status status = h->batch_create(h->context, fields, 8, o->limit, out);
    while (!status && (rc = sqlite3_step(q)) == SQLITE_ROW) {
        status = control(p);
        ud_value v[8] = {{0}};
        for (uint32_t i = 0; i < 8; i++) {
            v[i].type = fields[i].type;
            v[i].is_null = fields[i].nullable && sqlite3_column_type(q, (int)i) == SQLITE_NULL;
            if (fields[i].type == UD_UTF8) {
                v[i].text = (const char *)sqlite3_column_text(q, (int)i);
                v[i].text_length = (uint32_t)sqlite3_column_bytes(q, (int)i);
            } else
                v[i].integer = sqlite3_column_int64(q, (int)i);
        }
        if (!status)
            status = h->append(*out, v, 8);
    }
    if (!status && rc != SQLITE_DONE)
        status = failure(p, rc);
    sqlite3_finalize(q);
    return status;
}
static ud_status reset_session(void *state) {
    sqlite_state *p = state;
    if (p->read || p->transaction || p->branch_id[0] || !sqlite3_get_autocommit(p->db) ||
        sqlite3_next_stmt(p->db, NULL))
        return UD_CORRUPT;
    p->cleaning = 1;
    ud_status status = connector_test(p);
    p->cleaning = 0;
    return status;
}
static ud_status provision(void *state) {
    sqlite_state *p = state;
    ud_status status = control(p);
    if (status)
        return status;
    int rc = sqlite3_exec(
        p->db,
        "CREATE TABLE IF NOT EXISTS ud_sdk_receipts_v11(tenant TEXT NOT NULL,job TEXT NOT "
        "NULL,batch_id TEXT NOT NULL,digest BLOB NOT NULL CHECK(length(digest)=32),source_position "
        "TEXT NOT NULL,PRIMARY KEY(tenant,job,batch_id))",
        NULL, NULL, NULL);
    if (rc == SQLITE_OK)
        rc = sqlite3_exec(
            p->db,
            "CREATE TABLE IF NOT EXISTS ud_sdk_tx_receipts_v11(branch_id TEXT COLLATE BINARY "
            "PRIMARY KEY CHECK(length(branch_id)=64),digest BLOB NOT NULL CHECK(length(digest)=32))",
            NULL, NULL, NULL);
    return rc == SQLITE_OK ? UD_OK : failure(p, rc);
}
static const ud_connector_api connector = {
    .struct_size = sizeof(connector),
    .abi_version = UD_PLUGIN_ABI,
    .capabilities = UD_CONNECT_READ_SNAPSHOT | UD_CONNECT_READONLY_SCAN | UD_CONNECT_READ |
                    UD_CONNECT_RECORD_ERRORS |
                    UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE |
                    UD_CONNECT_RECEIPTS | UD_CONNECT_EXECUTE_SQL | UD_CONNECT_TRANSACTION |
                    UD_CONNECT_METADATA | UD_CONNECT_POOL | UD_CONNECT_CHANGESET |
                    UD_CONNECT_PREPARED | UD_CONNECT_BATCH_WRITE | UD_CONNECT_PROVISION |
                    UD_CONNECT_TRANSACTIONAL_DML | UD_CONNECT_TRANSACTION_LOCAL,
    .test = connector_test,
    .discover = connector_discover,
    .read_open = connector_read_open,
    .read_next = connector_read_next,
    .read_close = connector_read_close,
    .write = connector_write,
    .receipt = connector_receipt,
    .cancel = connector_cancel,
    .execute = connector_execute_command,
    .begin = begin_transaction,
    .commit = commit_transaction,
    .rollback = rollback_transaction,
    .metadata = metadata,
    .set_control = set_control,
    .reset_session = reset_session,
    .provision = provision,
    .metadata_dialect = UD_SQL_SQLITE,
    .parameter_style = UD_SQL_PARAMETER_QUESTION,
    .savepoint = savepoint,
    .record_diagnostic = record_diagnostic,
    .transaction_begin = transaction_begin,
    .transaction_prepare = transaction_prepare,
    .transaction_resolve = transaction_resolve,
    .transaction_recover = transaction_recover};

UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){sizeof(*out),
                           UD_PLUGIN_ABI,
                           "org.universaldata.connector.sqlite",
                           UD_SDK_VERSION,
                           UD_PLUGIN_CONNECTOR,
                           UD_CAP_READ | UD_CAP_WRITE | UD_CAP_TRANSACTION | UD_CAP_THREAD_SAFE,
                           initialize,
                           destroy,
                           plugin_shutdown,
                           &connector,
                           NULL,
                           sizeof(ud_field),
                           sizeof(ud_value)};
    return UD_OK;
}
