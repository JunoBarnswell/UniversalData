#include "builtins_internal.h"
ud_status ud_builtin_diagnostics(void *ptr, char **out, uint32_t *length) {
    builtin *b = ptr;
    if (b->kind == XLSX_SOURCE)
        return ud_xlsx_reader_metadata(b->xlsx, out, length);
    ud_bytes json = {.ctx = b->context.memory};
    char *cursor = NULL;
    uint32_t cursor_length = 0;
    if (b->cursor)
        b->cursor->commit_unknown = b->context.activation->pipeline_result == UD_COMMIT_UNKNOWN;
    ud_status status =
        b->cursor ? ud_cursor_diagnostics(b->cursor, &cursor, &cursor_length) : UD_OK;
    char head[256];
    int n = snprintf(head, sizeof(head),
                     "{\"snapshotRows\":%llu,\"snapshotBatches\":%llu,\"snapshotReadMs\":%llu,"
                     "\"sourceHashMs\":%llu,\"materialized\":%s",
                     (unsigned long long)b->snapshot_rows, (unsigned long long)b->snapshot_count,
                     (unsigned long long)b->snapshot_read_ms, (unsigned long long)b->file_hash_ms,
                     b->materialized ? "true" : "false");
    if (!status)
        status = ud_bytes_put(&json, head, (size_t)n);
    if (!status && cursor) {
        status = ud_bytes_put(&json, ",\"cursor\":", 10);
        if (!status)
            status = ud_bytes_put(&json, cursor, cursor_length);
    }
    if (!status && b->schema_diff) {
        status = ud_bytes_put(&json, ",\"schema\":", 10);
        if (!status)
            status = ud_bytes_put(&json, b->schema_diff, strlen(b->schema_diff));
    }
    if (!status)
        status = ud_bytes_put(&json, "}", 1);
    if (!status) {
        *out = ud_strdup_n(b->context.memory, (char *)json.data, json.size);
        *length = (uint32_t)json.size;
        if (!*out)
            status = UD_RESOURCE;
    }
    ud_free(cursor);
    ud_bytes_clear(&json);
    return status;
}

void ud_builtin_dispose(void *ptr, ud_status result) {
    (void)result;
    builtin *b = ptr;
    if (!b)
        return;
    ud_xlsx_reader_close(b->xlsx);
#ifdef UD_WITH_ARROW_FILES
    if (b->columnar_reader)
        ud_columnar_reader_close(b->columnar_reader);
    if (b->columnar_writer)
        ud_columnar_writer_close(b->columnar_writer);
#endif
    ud_rest_reader_close(b->rest_reader);
    ud_batch_destroy(b->output_pending);
    ud_batch_destroy(b->source_errors);
    ud_expression_destroy(b->predicate);
    ud_free((void *)b->read_sql.data);
    if (b->xlsx_writer)
        ud_xlsx_writer_close(b->xlsx_writer);
    ud_csv_close(b->csv);
    ud_json_reader_close(b->reader);
    if (b->writer)
        (void)ud_json_writer_close(b->writer);
    if (b->csv_writer)
        (void)ud_csv_writer_close(b->csv_writer);
    if (b->temporary && b->temporary_owned && !b->published)
        (void)ud_remove_file(b->context.memory, b->temporary);
    if (b->stage_bytes)
        ud_buffer_release_external(b->context.execution->buffers, b->stage_bytes);
    ud_unlock(b->lock);
    (void)ud_connection_lease_release(b->lease);
    for (uint32_t i = 0; i < b->expression_count; i++)
        ud_expression_destroy(b->expressions[i]);
    ud_free(b->expressions);
    ud_batch_destroy(b->schema);
    ud_batch_destroy(b->errors);
    for (uint32_t i = 0; i < b->input_count; i++)
        ud_batch_destroy(b->inputs[i]);
    ud_free(b->inputs);
    (void)ud_operator_destroy(b->operator);
    ud_free(b->fields);
    ud_free(b->path);
    ud_free(b->temporary);
    ud_free(b->job);
    ud_free(b->keys);
    ud_free(b->write_indices);
    ud_rule_delivery_destroy(b->delivery);
    ud_cursor_destroy(b->cursor);
    ud_batch_destroy(b->actual_schema);
    ud_batch_destroy(b->write_schema);
    ud_batch_destroy(b->parameters);
    ud_operator_destroy(b->source_sort);
    for (uint64_t i = b->snapshot_at; b->snapshot && i < b->snapshot_count; i++)
        ud_buffer_release(b->snapshot[i]);
    ud_free(b->snapshot);
    ud_free(b->snapshot_ports);
    ud_free(b->schema_diff);
    ud_free((void *)b->schema_policy.data);
    ud_json_destroy(b->config);
    ud_free(b);
}

ud_status ud_builtin_checkpoint_state(void *ptr, ud_batch **out) {
    builtin *b = ptr;
    if (b->operator)
        return ud_operator_checkpoint(b->operator, out);
    if (b->kind != SQL_SINK && b->kind != REST_SINK && b->kind != BALANCED &&
        b->kind != SQL_COMMAND)
        return UD_UNSUPPORTED;
    ud_field field = {.name = "nextBatch", .name_length = 9, .type = UD_UINT64};
    ud_value value = {.type = UD_UINT64, .unsigned_integer = b->number};
    UD_TRY(ud_batch_create(b->context.memory, &field, 1, 1, out));
    ud_status status = ud_batch_append(*out, &value, 1);
    if (!status)
        status = ud_batch_seal(*out);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}

ud_status ud_builtin_restore_state(void *ptr, const ud_batch *state) {
    builtin *b = ptr;
    if (b->operator)
        return ud_operator_restore(&b->operator, state);
    if ((b->kind != SQL_SINK && b->kind != REST_SINK && b->kind != BALANCED &&
         b->kind != SQL_COMMAND) ||
        state->columns != 1 || state->rows != 1)
        return UD_CORRUPT;
    ud_value value;
    UD_TRY(ud_batch_get(state, 0, 0, &value));
    if (value.type != UD_UINT64 || value.is_null || value.is_absent)
        return UD_CORRUPT;
    b->number = value.unsigned_integer;
    return UD_OK;
}
