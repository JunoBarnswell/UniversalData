#include "mysql_internal.h"
#include <stdio.h>
#include <string.h>

static ud_status receipt_insert(mysql_state *p, ud_string tenant, const ud_write_options *o,
                                const uint8_t digest[32]) {
    const char *sql = "INSERT INTO ud_sdk_receipts_v11(tenant,job,batch_id,digest,source_position) "
                      "VALUES(?,?,?,?,?)";
    MYSQL_STMT *stmt = NULL;
    ud_status status = ud_mysql_statement(p, (ud_string){sql, (uint32_t)strlen(sql)}, &stmt);
    bindings values = {0};
    if (!status)
        status = ud_mysql_bindings_create(p, 5, &values);
    ud_string text[] = {tenant, o->job, o->batch_id, o->source_position};
    for (uint32_t i = 0; !status && i < 4; i++) {
        uint32_t at = i < 3 ? i : 4;
        status = ud_mysql_bind_value(
            &values, at,
            (ud_value){.type = UD_UTF8, .text = text[i].data, .text_length = text[i].length});
    }
    if (!status)
        status = ud_mysql_bind_value(
            &values, 3,
            (ud_value){.type = UD_BINARY, .text = (const char *)digest, .text_length = 32});
    if (!status)
        status = ud_mysql_execute_bound(p, stmt, &values, NULL);
    ud_mysql_bindings_free(&values);
    if (stmt)
        mysql_stmt_close(stmt);
    return status;
}

static ud_status apply_statement(mysql_state *p, const ud_write_options *options,
                                 const ud_batch *batch, uint32_t first, uint32_t rows,
                                 uint64_t *affected_out) {
    ud_sql_statement plan = {0};
    ud_status status = p->host.compile_write(p->host.context, UD_MYSQL_DIALECT,
                                             UD_SQL_PARAMETER_QUESTION, options, batch, first,
                                             rows, &plan);
    MYSQL_STMT *stmt = NULL;
    bindings parameters = {0};
    if (!status && plan.sql.length)
        status = ud_mysql_statement(p, plan.sql, &stmt);
    if (!status)
        status = ud_mysql_bindings_create(p, plan.parameter_count, &parameters);
    for (uint32_t i = 0; !status && i < plan.parameter_count; i++) {
        const ud_sql_parameter *ref = plan.parameters + i;
        const ud_batch *source = ref->source == UD_SQL_BEFORE ? options->before : batch;
        ud_value value;
        status = source ? p->host.get(source, ref->row, ref->column, &value) : UD_SCHEMA;
        if (!status)
            status = ud_mysql_bind_value(&parameters, i, value);
    }
    uint64_t affected = 0;
    if (!status && stmt)
        status = ud_mysql_execute_bound(p, stmt, &parameters, &affected);
    if (!status && options->mode == UD_WRITE_CHANGESET &&
        options->operations[first] != UD_CHANGE_UPSERT &&
        options->operations[first] != UD_CHANGE_TRUNCATE && affected != 1)
        status = UD_FENCED;
    ud_mysql_bindings_free(&parameters);
    if (stmt)
        mysql_stmt_close(stmt);
    p->host.statement_clear(&plan);
    if (!status && affected_out)
        *affected_out = affected;
    return status;
}

/* LOCAL INFILE is driven only by the current sealed Native Batch. */
int ud_mysql_bulk_init(void **out, const char *filename, void *user) {
    mysql_state *p = user;
    *out = p;
    if (!p->bulk || p->bulk_requested || strcmp(filename, "ud-native-batch")) {
        p->bulk_status = UD_AUTH;
        return 1;
    }
    p->bulk_requested = 1;
    p->bulk_status = ud_mysql_check(p);
    return p->bulk_status ? 1 : 0;
}
int ud_mysql_bulk_read(void *user, char *buffer, unsigned int capacity) {
    mysql_state *p = user;
    uint32_t length = 0;
    p->bulk_status = ud_mysql_check(p);
    if (!p->bulk_status)
        p->bulk_status = p->host.bulk_read(p->bulk, buffer, capacity, &length);
    if (p->bulk_status == UD_END) {
        p->bulk_status = UD_OK;
        return 0;
    }
    return p->bulk_status ? -1 : (int)length;
}
void ud_mysql_bulk_end(void *user) {
    (void)user;
}
int ud_mysql_bulk_error(void *user, char *buffer, unsigned int capacity) {
    (void)user;
    if (capacity)
        snprintf(buffer, capacity, "%s", "Native controlled bulk input failed");
    return 1;
}

static ud_status native_load(mysql_state *p, const ud_write_options *o, const ud_batch *batch,
                             uint64_t *affected) {
    ud_string sql = {0};
    ud_status status = p->host.compile_bulk(p->host.context, UD_MYSQL_DIALECT, o, batch, &sql);
    p->bulk_requested = 0;
    p->bulk_status = UD_OK;
    if (!status && mysql_real_query(p->db, sql.data, sql.length))
        status = p->bulk_status ? p->bulk_status : ud_mysql_failure(p, NULL);
    uint64_t loaded = (uint64_t)mysql_affected_rows(p->db);
    if (!status && (!p->bulk_requested || mysql_warning_count(p->db) ||
                    loaded != p->host.rows(batch)))
        status = UD_SCHEMA;
    ud_string release = {0};
    ud_status cleaned =
        p->host.compile_bulk_release(p->host.context, UD_MYSQL_DIALECT, batch, &release);
    if (!cleaned && release.length && mysql_real_query(p->db, release.data, release.length))
        cleaned = ud_mysql_failure(p, NULL);
    p->host.deallocate((void *)release.data);
    if (!status)
        status = cleaned;
    p->host.deallocate((void *)sql.data);
    if (!status && affected)
        *affected = loaded;
    return status;
}

uint64_t ud_mysql_session_capabilities(void *state) {
    mysql_state *p = state;
    return p->bulk_allowed ? ud_mysql_connector.capabilities
                           : ud_mysql_connector.capabilities & ~(uint64_t)UD_CONNECT_NATIVE_BULK;
}

ud_status ud_mysql_write_batch(void *state, ud_string tenant, const ud_write_options *o,
                               const ud_batch *batch, const uint8_t digest[32]) {
    mysql_state *p = state;
    if (p->cursor)
        return UD_BUSY;
    int receipted = o->delivery != UD_DELIVERY_RESPONSE;
    if (receipted && !digest)
        return UD_INVALID;
    if (tenant.length > 256 || (receipted && (o->job.length > 256 || o->batch_id.length > 256)))
        return UD_RESOURCE;
    ud_status status = ud_mysql_check(p);
    if (!status && o->load_mode == UD_LOAD_NATIVE && !p->bulk_allowed)
        status = UD_UNSUPPORTED;
    if (!status && o->mode == UD_WRITE_APPEND && o->load_mode != UD_LOAD_PARAMETERS &&
        p->bulk_allowed) {
        status = p->host.bulk_open(p->host.context, batch, UD_MYSQL_DIALECT, &p->bulk);
        if (status == UD_UNSUPPORTED && o->load_mode == UD_LOAD_AUTO)
            status = UD_OK;
    }
    int own = !p->transaction;
    if (!status)
        status = ud_mysql_prove_target(p, o, batch);
    if (!status && own)
        status = ud_mysql_begin(p, UD_ISOLATION_SERIALIZABLE);
    ud_commit_receipt existing = {0};
    if (!status && receipted) {
        status = receipt_insert(p, tenant, o, digest);
        if (status == UD_QUALITY && p->record_code == 1062) {
            p->record_code = 0;
            status = ud_mysql_receipt(p, tenant, o->job, o->batch_id, &existing);
            if (!status && (!existing.committed || memcmp(existing.digest, digest, 32) ||
                            existing.source_position.length != o->source_position.length ||
                            (o->source_position.length &&
                             memcmp(existing.source_position.data, o->source_position.data,
                                    o->source_position.length))))
                status = UD_CORRUPT;
        }
    }
    uint64_t affected = 0;
    if (!status && !existing.committed) {
        uint32_t rows = p->host.rows(batch);
        uint32_t width = o->mode == UD_WRITE_DELETE ? o->key_count : p->host.columns(batch);
        uint32_t chunk = plugin_write_chunk_rows(&p->host, UD_MYSQL_DIALECT, o, batch, width, 1024);
        if (!chunk)
            status = UD_RESOURCE;
        if (!status && p->bulk)
            status = native_load(p, o, batch, &affected);
        for (uint32_t row = 0; !status && !p->bulk && row < rows;) {
            status = ud_mysql_check(p);
            if (status)
                break;
            uint64_t physical = 0;
            if (o->mode == UD_WRITE_CHANGESET) {
                status = apply_statement(p, o, batch, row++, 1, &physical);
            } else {
                uint32_t take = rows - row < chunk ? rows - row : chunk;
                status = apply_statement(p, o, batch, row, take, &physical);
                row += take;
            }
            if (!status)
                affected += physical;
        }
    }
    p->host.deallocate((void *)existing.source_position.data);
    p->host.bulk_close(p->bulk);
    p->bulk = NULL;
    if (own && p->transaction) {
        if (!status)
            status = ud_mysql_commit(p);
        else {
            ud_status clean = ud_mysql_rollback(p);
            if (clean)
                status = clean;
        }
    }
    if (!status && o->affected_rows)
        *o->affected_rows = affected;
    return status;
}
