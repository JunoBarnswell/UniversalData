#include "odbc_internal.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#define TRY(call)                                                                                  \
    do {                                                                                           \
        ud_status status = (call);                                                                 \
        if (status)                                                                                \
            return status;                                                                         \
    } while (0)
/* SQL Server CDC uses the provider's capture tables and transaction LSNs. The
 * peer session is owned by this same leased ODBC connection. Initial snapshot
 * holds a table writer fence while a marker is committed by the peer. Capture
 * is allowed to pass that marker after the snapshot releases the table lock;
 * later writers then have LSNs strictly beyond the snapshot boundary. */
struct ud_mssql_cdc {
    odbc_state *owner, *peer;
    ud_batch *schema, *log_schema, *chunk, *old, *fence_marker;
    char *table, *capture, *identity;
    uint8_t position[10], confirmed[10], transaction[10], upper[10], old_sequence[10];
    uint32_t batch_rows, row, phase;
    int snapshot, reading, transaction_open;
};
static ud_status scalar(odbc_state *p, const char *sql, const ud_batch *params, ud_type type,
                        ud_value *out) {
    ud_field field = {.name = "value", .name_length = 5, .type = type, .nullable = 1};
    ud_batch *schema = NULL, *batch = NULL;
    ud_status status = p->host.batch_create(p->host.context, &field, 1, 0, &schema);
    ud_read_options options = {.struct_size = sizeof(options),
                               .abi_version = UD_ABI_VERSION,
                               .batch_size = 2,
                               .sql = {sql, (uint32_t)strlen(sql)},
                               .parameters = params};
    if (!status)
        status = ud_odbc_read_open(p, &options, schema);
    if (!status)
        status = ud_odbc_read_next(p, &batch);
    if (!status && (p->host.rows(batch) != 1))
        status = UD_CORRUPT;
    if (!status)
        status = p->host.get(batch, 0, 0, out);
    if (!status && (type == UD_UTF8 || type == UD_BINARY) && !out->is_null) {
        char *owned = plugin_bytes(&p->host, out->text, out->text_length);
        out->text = owned;
        if (!owned)
            status = UD_RESOURCE;
    }
    ud_odbc_read_close(p);
    p->host.batch_destroy(batch);
    p->host.batch_destroy(schema);
    return status;
}
static ud_status parameter_batch(odbc_state *p, const uint8_t *first, uint32_t first_length,
                                 const uint8_t *second, uint32_t second_length, ud_batch **out) {
    ud_field fields[] = {{.name = "p1", .name_length = 2, .type = UD_BINARY},
                         {.name = "p2", .name_length = 2, .type = UD_BINARY}};
    ud_value values[] = {
        {.type = UD_BINARY, .text = (const char *)first, .text_length = first_length},
        {.type = UD_BINARY, .text = (const char *)second, .text_length = second_length}};
    uint32_t columns = second ? 2 : 1;
    ud_status status = p->host.batch_create(p->host.context, fields, columns, 1, out);
    if (!status)
        status = p->host.append(*out, values, columns);
    return status;
}
static char *quote(odbc_state *p, ud_string name) {
    if (!name.length || memchr(name.data, 0, name.length))
        return NULL;
    char *out = p->host.allocate(p->host.context, (size_t)name.length * 2 + 3);
    if (!out)
        return NULL;
    uint32_t n = 0;
    out[n++] = '[';
    for (uint32_t i = 0; i < name.length; i++) {
        if (name.data[i] == ']')
            out[n++] = ']';
        out[n++] = name.data[i];
    }
    out[n++] = ']';
    out[n] = 0;
    return out;
}
static char *table_name(odbc_state *p, ud_string name) {
    const char *dot = memchr(name.data, '.', name.length);
    if (!dot)
        return quote(p, name);
    if (memchr(dot + 1, '.', name.length - (uint32_t)(dot - name.data) - 1))
        return NULL;
    char *a = quote(p, (ud_string){name.data, (uint32_t)(dot - name.data)}),
         *b = quote(p, (ud_string){dot + 1, name.length - (uint32_t)(dot - name.data) - 1});
    char *out = a && b ? p->host.allocate(p->host.context, strlen(a) + strlen(b) + 2) : NULL;
    if (out)
        snprintf(out, strlen(a) + strlen(b) + 2, "%s.%s", a, b);
    p->host.deallocate(a);
    p->host.deallocate(b);
    return out;
}
static ud_status parse_lsn(ud_string text, uint8_t out[10]) {
    if (text.length != 20)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < 10; i++) {
        unsigned byte = 0;
        for (uint32_t j = 0; j < 2; j++) {
            char ch = text.data[i * 2 + j];
            int digit = ch >= '0' && ch <= '9'   ? ch - '0'
                        : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
                                                 : -1;
            if (digit < 0)
                return UD_SCHEMA;
            byte = byte * 16 + (unsigned)digit;
        }
        out[i] = (uint8_t)byte;
    }
    return UD_OK;
}
static ud_status parse_position(struct ud_mssql_cdc *c, ud_string text, uint8_t out[10]) {
    odbc_state *p = c->owner;
    ud_json *doc = NULL;
    ud_status status = p->host.json_parse(p->host.context, text.data, text.length, &doc, NULL);
    const char *keys[] = {"identity", "lsn"};
    if (!status)
        status = p->host.json_keys(doc, keys, 2);
    const char *value = NULL;
    uint32_t length = 0;
    if (!status)
        status = p->host.json_text(p->host.json_find(doc, "identity", 8), &value, &length);
    if (!status && (length != strlen(c->identity) || memcmp(value, c->identity, length)))
        status = UD_FENCED;
    if (!status)
        status = p->host.json_text(p->host.json_find(doc, "lsn", 3), &value, &length);
    if (!status)
        status = parse_lsn((ud_string){value, length}, out);
    p->host.json_destroy(doc);
    return status;
}
static ud_status event_position(struct ud_mssql_cdc *c, ud_cdc_event *out, const uint8_t lsn[10]) {
    char value[21];
    for (uint32_t i = 0; i < 10; i++)
        snprintf(value + i * 2, 3, "%02x", lsn[i]);
    size_t capacity = strlen(c->identity) + 80;
    char *text = c->owner->host.allocate(c->owner->host.context, capacity);
    if (!text)
        return UD_RESOURCE;
    int n = snprintf(text, capacity, "{\"identity\":\"%s\",\"lsn\":\"%s\"}", c->identity, value);
    if (n < 0 || (size_t)n >= capacity) {
        c->owner->host.deallocate(text);
        return UD_RESOURCE;
    }
    out->position = (ud_string){text, (uint32_t)n};
    return UD_OK;
}
static ud_status source_identity(struct ud_mssql_cdc *c) {
    char sql[640];
    snprintf(sql, sizeof(sql),
             "SELECT "
             "CONVERT(varchar(36),r.database_guid)+':'+CONVERT(varchar(36),r.recovery_fork_guid)+':"
             "'+CONVERT(varchar(20),t.object_id)+':'+CONVERT(varchar(33),t.create_date,126) AS "
             "[value] FROM sys.database_recovery_status r CROSS JOIN cdc.change_tables t WHERE "
             "r.database_id=DB_ID() AND t.capture_instance=N'%s'",
             c->capture);
    ud_value value = {0};
    ud_status status = scalar(c->owner, sql, NULL, UD_UTF8, &value);
    if (!status && (value.is_null || !value.text_length))
        status = UD_FENCED;
    if (!status) {
        c->identity = (char *)value.text;
        value.text = NULL;
    }
    c->owner->host.deallocate((void *)value.text);
    return status;
}
static ud_status source_fields(struct ud_mssql_cdc *c, char **out) {
    odbc_state *p = c->owner;
    size_t n = 1;
    uint32_t columns = p->host.columns(c->schema);
    for (uint32_t i = 0; i < columns; i++) {
        ud_field field;
        TRY(p->host.field(c->schema, i, &field));
        n += (size_t)field.name_length * 2 + 4;
    }
    char *text = p->host.allocate(p->host.context, n);
    if (!text)
        return UD_RESOURCE;
    size_t at = 0;
    for (uint32_t i = 0; i < columns; i++) {
        ud_field field;
        ud_status status = p->host.field(c->schema, i, &field);
        if (status) {
            p->host.deallocate(text);
            return status;
        }
        char *name = quote(p, (ud_string){field.name, field.name_length});
        if (!name) {
            p->host.deallocate(text);
            return UD_RESOURCE;
        }
        if (i)
            text[at++] = ',';
        memcpy(text + at, name, strlen(name));
        at += strlen(name);
        p->host.deallocate(name);
    }
    text[at] = 0;
    *out = text;
    return UD_OK;
}
ud_status ud_mssql_cdc_enabled(odbc_state *p, int *out) {
    *out = 0;
    ud_value value;
    ud_status status = scalar(p,
                              "SELECT CONVERT(bigint,is_cdc_enabled) AS [value] FROM sys.databases "
                              "WHERE database_id=DB_ID()",
                              NULL, UD_INT64, &value);
    if (!status)
        *out = !value.is_null && value.integer == 1;
    return status;
}
ud_status ud_mssql_cdc_provision(odbc_state *p) {
    return ud_odbc_command(p,
                           "IF OBJECT_ID(N'dbo.ud_sdk_cdc_fence',N'U') IS NULL CREATE TABLE "
                           "dbo.ud_sdk_cdc_fence(marker binary(16) NOT NULL PRIMARY KEY); "
                           "IF NOT EXISTS(SELECT 1 FROM cdc.change_tables WHERE "
                           "capture_instance=N'ud_sdk_cdc_fence') EXEC sys.sp_cdc_enable_table "
                           "@source_schema=N'dbo',@source_name=N'ud_sdk_cdc_fence',@capture_"
                           "instance=N'ud_sdk_cdc_fence',@role_name=NULL,@supports_net_changes=0");
}
static ud_status fence(struct ud_mssql_cdc *c) {
    odbc_state *p = c->owner;
    void *peer = NULL;
    TRY(ud_odbc_initialize(&p->host, p->configuration, p->configuration_length, &peer));
    ud_mutex_lock(&p->handles_lock);
    c->peer = peer;
    if (atomic_load(&p->cancelled))
        ud_odbc_cancel(c->peer);
    ud_mutex_unlock(&p->handles_lock);
    TRY(ud_odbc_begin(p, UD_ISOLATION_SERIALIZABLE));
    char *sql = p->host.allocate(p->host.context, strlen(c->table) + 160);
    if (!sql)
        return UD_RESOURCE;
    snprintf(sql, strlen(c->table) + 160,
             "SELECT COUNT_BIG(*) AS [value] FROM %s WITH(TABLOCKX,HOLDLOCK)", c->table);
    ud_value value;
    ud_status status = scalar(p, sql, NULL, UD_INT64, &value);
    p->host.deallocate(sql);
    if (status)
        return status;
    TRY(scalar(c->peer, "SELECT CONVERT(binary(16),NEWID()) AS [value]", NULL, UD_BINARY, &value));
    if (value.is_null || value.text_length != 16) {
        p->host.deallocate((void *)value.text);
        return UD_CORRUPT;
    }
    ud_batch *params = NULL;
    status = parameter_batch(p, (const uint8_t *)value.text, 16, NULL, 0, &params);
    p->host.deallocate((void *)value.text);
    ud_read_options insert = {
        .struct_size = sizeof(insert),
        .abi_version = UD_ABI_VERSION,
        .sql = {"INSERT INTO dbo.ud_sdk_cdc_fence(marker) VALUES(?)",
                sizeof("INSERT INTO dbo.ud_sdk_cdc_fence(marker) VALUES(?)") - 1},
        .parameters = params};
    uint64_t rows = 0;
    if (!status)
        status = ud_odbc_execute(c->peer, &insert, &rows);
    if (!status && rows != 1)
        status = UD_CORRUPT;
    if (!status) {
        c->fence_marker = params;
        params = NULL;
    }
    p->host.batch_destroy(params);
    return status;
}
static ud_status confirm_snapshot_fence(struct ud_mssql_cdc *c) {
    odbc_state *p = c->owner;
    ud_batch *params = c->fence_marker;
    ud_status status = params ? UD_OK : UD_CORRUPT;
    ud_value value = {0};
    const char *poll = "SELECT TOP(1) [__$start_lsn] AS [value] FROM cdc.ud_sdk_cdc_fence_CT WHERE "
                       "marker=? AND [__$operation]=2 ORDER BY [__$start_lsn] DESC";
    struct timespec started;
    clock_gettime(CLOCK_MONOTONIC, &started);
    while (!status) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec - started.tv_sec >= 30) {
            status = UD_TIMEOUT;
            break;
        }
        status = ud_odbc_control(p);
        if (status)
            break;
        status = scalar(c->peer, poll, params, UD_BINARY, &value);
        if (!status && !value.is_null) {
            if (value.text_length != 10)
                status = UD_CORRUPT;
            else
                memcpy(c->position, value.text, 10);
            p->host.deallocate((void *)value.text);
            break;
        }
        if (status != UD_END && status != UD_OK)
            break;
        status = UD_OK;
        struct timespec delay = {0, 10000000};
        nanosleep(&delay, NULL);
    }
    p->host.batch_destroy(params);
    c->fence_marker = NULL;
    if (status)
        return status;
    memcpy(c->confirmed, c->position, 10);
    return UD_OK;
}
static ud_status retained(struct ud_mssql_cdc *c, odbc_state *session) {
    ud_value value = {0};
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT sys.fn_cdc_get_min_lsn(N'%s') AS [value]", c->capture);
    ud_status status = scalar(session, sql, NULL, UD_BINARY, &value);
    if (!status &&
        (value.is_null || value.text_length != 10 || memcmp(c->position, value.text, 10) < 0))
        status = UD_FENCED;
    if (value.type == UD_BINARY && !value.is_null)
        session->host.deallocate((void *)value.text);
    return status;
}
static ud_status log_open(struct ud_mssql_cdc *c) {
    odbc_state *p = c->owner;
    TRY(retained(c, p));
    ud_value upper;
    TRY(scalar(p, "SELECT sys.fn_cdc_get_max_lsn() AS [value]", NULL, UD_BINARY, &upper));
    if (upper.is_null || upper.text_length != 10) {
        p->host.deallocate((void *)upper.text);
        return UD_CORRUPT;
    }
    memcpy(c->upper, upper.text, 10);
    p->host.deallocate((void *)upper.text);
    if (memcmp(c->upper, c->position, 10) <= 0)
        return UD_BUSY;
    ud_batch *parameters = NULL;
    ud_status status = parameter_batch(p, c->position, 10, c->upper, 10, &parameters);
    char *fields = NULL;
    if (!status)
        status = source_fields(c, &fields);
    size_t n = fields ? strlen(fields) + strlen(c->capture) + 320 : 0;
    char *sql = n ? p->host.allocate(p->host.context, n) : NULL;
    if (!status && !sql)
        status = UD_RESOURCE;
    if (!status)
        snprintf(
            sql, n,
            "SELECT [__$start_lsn],[__$seqval],CONVERT(bigint,[__$operation]) AS [__$operation],%s "
            "FROM cdc.fn_cdc_get_all_changes_%s(sys.fn_cdc_increment_lsn(?),?,'all update old') "
            "ORDER BY [__$start_lsn],[__$seqval],[__$operation]",
            fields, c->capture);
    ud_read_options read = {.struct_size = sizeof(read),
                            .abi_version = UD_ABI_VERSION,
                            .batch_size = c->batch_rows,
                            .sql = {sql, sql ? (uint32_t)strlen(sql) : 0},
                            .parameters = parameters};
    if (!status)
        status = ud_odbc_read_open(p, &read, c->log_schema);
    p->host.batch_destroy(parameters);
    p->host.deallocate(fields);
    p->host.deallocate(sql);
    if (!status)
        c->reading = 1;
    return status;
}
static ud_status log_row(struct ud_mssql_cdc *c) {
    odbc_state *p = c->owner;
    if (c->chunk && c->row < p->host.rows(c->chunk))
        return UD_OK;
    p->host.batch_destroy(c->chunk);
    c->chunk = NULL;
    c->row = 0;
    ud_status status = ud_odbc_read_next(p, &c->chunk);
    if (status == UD_END) {
        ud_odbc_read_close(p);
        c->reading = 0;
    }
    return status;
}
static ud_status copy_row(struct ud_mssql_cdc *c, ud_batch *out, const ud_batch *input,
                          uint32_t row, uint32_t offset) {
    uint32_t columns = c->owner->host.columns(out);
    ud_value *values = c->owner->host.allocate(c->owner->host.context, columns * sizeof(*values));
    if (!values)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < columns; i++)
        status = c->owner->host.get(input, row, offset + i, values + i);
    if (!status)
        status = c->owner->host.append(out, values, columns);
    c->owner->host.deallocate(values);
    return status;
}
ud_status ud_mssql_cdc_open(void *state, const ud_cdc_options *o) {
    odbc_state *p = state;
    if (p->dialect != DIALECT_MSSQL || !(p->capabilities & UD_CONNECT_NATIVE_CDC))
        return UD_UNSUPPORTED;
    if (p->cursor || p->transaction || p->cdc)
        return UD_BUSY;
    if (!o->schema || !o->slot.length || o->slot.length > 100)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < o->slot.length; i++) {
        unsigned char ch = o->slot.data[i];
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
              ch == '_'))
            return UD_SCHEMA;
    }
    struct ud_mssql_cdc *c = p->host.allocate(p->host.context, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    c->owner = p;
    ud_mutex_lock(&p->handles_lock);
    p->cdc = c;
    ud_mutex_unlock(&p->handles_lock);
    c->batch_rows = o->batch_rows;
    c->table = table_name(p, o->table);
    c->capture = plugin_string(&p->host, o->slot.data, o->slot.length);
    ud_field *fields = NULL;
    ud_status status =
        c->table && c->capture ? plugin_fields(&p->host, o->schema, &fields) : UD_RESOURCE;
    if (!status)
        status = p->host.batch_create(p->host.context, fields, p->host.columns(o->schema), 0,
                                      &c->schema);
    uint32_t columns = p->host.columns(o->schema);
    ud_field *log = p->host.allocate(p->host.context, (columns + 3) * sizeof(*log));
    if (!log && !status)
        status = UD_RESOURCE;
    if (!status) {
        log[0] = (ud_field){.name = "__$start_lsn", .name_length = 12, .type = UD_BINARY};
        log[1] = (ud_field){.name = "__$seqval", .name_length = 9, .type = UD_BINARY};
        log[2] = (ud_field){.name = "__$operation", .name_length = 12, .type = UD_INT64};
        memcpy(log + 3, fields, columns * sizeof(*fields));
        status = p->host.batch_create(p->host.context, log, columns + 3, 0, &c->log_schema);
    }
    p->host.deallocate(fields);
    p->host.deallocate(log);
    if (!status) {
        char probe[400];
        ud_field field = {.name = "p1", .name_length = 2, .type = UD_UTF8};
        ud_batch *parameter = NULL;
        ud_value value = {.type = UD_UTF8, .text = o->table.data, .text_length = o->table.length};
        status = p->host.batch_create(p->host.context, &field, 1, 1, &parameter);
        if (!status)
            status = p->host.append(parameter, &value, 1);
        snprintf(probe, sizeof(probe),
                 "SELECT CONVERT(bigint,COUNT(*)) AS [value] FROM cdc.change_tables WHERE "
                 "capture_instance=N'%s' AND source_object_id=OBJECT_ID(?)",
                 c->capture);
        if (!status)
            status = scalar(p, probe, parameter, UD_INT64, &value);
        if (!status && (value.is_null || value.integer != 1))
            status = UD_SCHEMA;
        p->host.batch_destroy(parameter);
    }
    if (!status)
        status = source_identity(c);
    if (!status && o->mode == UD_CDC_SNAPSHOT_STREAM) {
        status = fence(c);
        if (!status) {
            char *names = NULL;
            status = source_fields(c, &names);
            size_t n = names ? strlen(names) + strlen(c->table) + 40 : 0;
            char *sql = n ? p->host.allocate(p->host.context, n) : NULL;
            if (!status && !sql)
                status = UD_RESOURCE;
            if (!status) {
                snprintf(sql, n, "SELECT %s FROM %s", names, c->table);
                ud_read_options read = {.struct_size = sizeof(read),
                                        .abi_version = UD_ABI_VERSION,
                                        .batch_size = c->batch_rows,
                                        .sql = {sql, (uint32_t)strlen(sql)}};
                status = ud_odbc_read_open(p, &read, c->schema);
            }
            p->host.deallocate(names);
            p->host.deallocate(sql);
            if (!status)
                c->snapshot = 1;
        }
    } else if (!status) {
        status = parse_position(c, o->position, c->position);
        if (!status)
            memcpy(c->confirmed, c->position, 10);
    }
    if (!status && !c->snapshot)
        status = retained(c, p);
    if (status) {
        ud_mssql_cdc_close(p);
        return status;
    }
    return UD_OK;
}
ud_status ud_mssql_cdc_next(void *state, ud_cdc_event *out) {
    odbc_state *p = state;
    struct ud_mssql_cdc *c = p->cdc;
    if (!c)
        return UD_INVALID;
    TRY(ud_odbc_control(p));
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);
    out->abi_version = UD_ABI_VERSION;
    out->schema_version = 1;
    if (c->snapshot) {
        if (!c->phase) {
            c->phase = 1;
            out->kind = UD_CDC_BEGIN;
            out->transaction_id = (ud_string){plugin_string(&p->host, "snapshot", 8), 8};
            return out->transaction_id.data ? UD_OK : UD_RESOURCE;
        }
        ud_status status = ud_odbc_read_next(p, &out->after);
        if (status == UD_END) {
            ud_odbc_read_close(p);
            status = ud_odbc_commit(p);
            if (!status)
                status = confirm_snapshot_fence(c);
            if (!status)
                status = retained(c, c->peer);
            if (!status) {
                c->snapshot = 0;
                out->kind = UD_CDC_COMMIT;
                status = event_position(c, out, c->position);
            }
            return status;
        }
        if (status)
            return status;
        uint32_t rows = p->host.rows(out->after);
        out->operations = p->host.allocate(p->host.context, rows * sizeof(*out->operations));
        if (!out->operations)
            return UD_RESOURCE;
        for (uint32_t i = 0; i < rows; i++)
            out->operations[i] = UD_CHANGE_INSERT;
        out->kind = UD_CDC_ROWS;
        return event_position(c, out, c->position);
    }
    if (!c->reading && c->transaction_open) {
        if (c->old)
            return UD_CORRUPT;
        out->kind = UD_CDC_COMMIT;
        c->transaction_open = 0;
        memcpy(c->position, c->transaction, 10);
        return event_position(c, out, c->position);
    }
    if (!c->reading) {
        ud_status status = log_open(c);
        if (status)
            return status;
    }
    ud_status status = log_row(c);
    if (status == UD_END && c->transaction_open) {
        out->kind = UD_CDC_COMMIT;
        c->transaction_open = 0;
        memcpy(c->position, c->transaction, 10);
        return event_position(c, out, c->position);
    }
    if (status == UD_END) {
        memcpy(c->position, c->upper, 10);
        out->kind = UD_CDC_BEGIN;
        c->transaction_open = 1;
        memcpy(c->transaction, c->upper, 10);
        out->transaction_id = (ud_string){plugin_string(&p->host, "capture-fence", 13), 13};
        return out->transaction_id.data ? UD_OK : UD_RESOURCE;
    }
    if (status)
        return status;
    ud_value lsn;
    TRY(p->host.get(c->chunk, c->row, 0, &lsn));
    if (lsn.is_null || lsn.text_length != 10)
        return UD_CORRUPT;
    if (c->transaction_open && memcmp(c->transaction, lsn.text, 10)) {
        out->kind = UD_CDC_COMMIT;
        c->transaction_open = 0;
        memcpy(c->position, c->transaction, 10);
        return event_position(c, out, c->position);
    }
    if (!c->transaction_open) {
        memcpy(c->transaction, lsn.text, 10);
        c->transaction_open = 1;
        out->kind = UD_CDC_BEGIN;
        TRY(event_position(c, out, c->transaction));
        out->transaction_id = out->position;
        out->position = (ud_string){0};
        return UD_OK;
    }
    ud_field *fields = NULL;
    TRY(plugin_fields(&p->host, c->schema, &fields));
    uint32_t columns = p->host.columns(c->schema);
    status = p->host.batch_create(p->host.context, fields, columns, c->batch_rows, &out->after);
    if (!status)
        status =
            p->host.batch_create(p->host.context, fields, columns, c->batch_rows, &out->before);
    p->host.deallocate(fields);
    out->operations = p->host.allocate(p->host.context, c->batch_rows * sizeof(*out->operations));
    if (!out->operations && !status)
        status = UD_RESOURCE;
    while (!status && p->host.rows(out->after) < c->batch_rows) {
        status = log_row(c);
        if (status == UD_END) {
            status = UD_OK;
            break;
        }
        if (status)
            break;
        status = p->host.get(c->chunk, c->row, 0, &lsn);
        if (!status && (lsn.is_null || lsn.text_length != 10))
            status = UD_CORRUPT;
        if (status || memcmp(c->transaction, lsn.text, 10))
            break;
        ud_value action;
        status = p->host.get(c->chunk, c->row, 2, &action);
        if (status)
            break;
        if (action.integer == 3) {
            if (c->old) {
                status = UD_CORRUPT;
                break;
            }
            ud_field *old_fields = NULL;
            status = plugin_fields(&p->host, c->schema, &old_fields);
            if (!status)
                status = p->host.batch_create(p->host.context, old_fields, columns, 1, &c->old);
            p->host.deallocate(old_fields);
            ud_value sequence;
            if (!status)
                status = p->host.get(c->chunk, c->row, 1, &sequence);
            if (!status && (sequence.is_null || sequence.text_length != 10))
                status = UD_CORRUPT;
            if (!status)
                memcpy(c->old_sequence, sequence.text, 10);
            if (!status)
                status = copy_row(c, c->old, c->chunk, c->row, 3);
            c->row++;
            continue;
        }
        uint32_t row = p->host.rows(out->after);
        if (action.integer < 1 || action.integer > 4) {
            status = UD_CORRUPT;
            break;
        }
        if (action.integer == 4 && !c->old) {
            status = UD_CORRUPT;
            break;
        }
        if (c->old && action.integer != 4) {
            status = UD_CORRUPT;
            break;
        }
        if (c->old) {
            ud_value sequence;
            status = p->host.get(c->chunk, c->row, 1, &sequence);
            if (!status && (sequence.is_null || sequence.text_length != 10 ||
                            memcmp(sequence.text, c->old_sequence, 10)))
                status = UD_CORRUPT;
            if (status)
                break;
        }
        status = copy_row(c, out->after, c->chunk, c->row, 3);
        if (!status)
            status = copy_row(c, out->before, c->old ? c->old : c->chunk, c->old ? 0 : c->row,
                              c->old ? 0 : 3);
        if (!status)
            out->operations[row] = action.integer == 1   ? UD_CHANGE_DELETE
                                   : action.integer == 2 ? UD_CHANGE_INSERT
                                                         : UD_CHANGE_UPDATE;
        p->host.batch_destroy(c->old);
        c->old = NULL;
        c->row++;
    }
    if (!status && !p->host.rows(out->after))
        status = UD_BUSY;
    if (!status) {
        out->kind = UD_CDC_ROWS;
        status = event_position(c, out, c->transaction);
    }
    return status;
}
ud_status ud_mssql_cdc_confirm(void *state, ud_string position) {
    odbc_state *p = state;
    struct ud_mssql_cdc *c = p->cdc;
    if (!c)
        return UD_INVALID;
    uint8_t lsn[10];
    TRY(parse_position(c, position, lsn));
    if (memcmp(lsn, c->confirmed, 10) < 0 || memcmp(lsn, c->position, 10) > 0)
        return UD_FENCED;
    memcpy(c->confirmed, lsn, 10);
    return UD_OK;
}
void ud_mssql_cdc_cancel(odbc_state *p) {
    if (p->cdc && p->cdc->peer)
        ud_odbc_cancel(p->cdc->peer);
}
ud_status ud_mssql_cdc_close(void *state) {
    odbc_state *p = state;
    ud_mutex_lock(&p->handles_lock);
    struct ud_mssql_cdc *c = p->cdc;
    p->cdc = NULL;
    ud_mutex_unlock(&p->handles_lock);
    if (!c)
        return UD_OK;
    ud_odbc_read_close(p);
    if (p->transaction)
        ud_odbc_rollback(p);
    if (c->peer)
        ud_odbc_destroy(c->peer);
    p->host.batch_destroy(c->schema);
    p->host.batch_destroy(c->log_schema);
    p->host.batch_destroy(c->chunk);
    p->host.batch_destroy(c->old);
    p->host.batch_destroy(c->fence_marker);
    p->host.deallocate(c->table);
    p->host.deallocate(c->capture);
    p->host.deallocate(c->identity);
    p->host.deallocate(c);
    return UD_OK;
}
