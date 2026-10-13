#include "builtins_internal.h"
ud_status ud_builtin_sink_schema(builtin *b) {
    ud_string connection = {0}, table = {0};
    UD_TRY(ud_json_string(ud_member(b->config, "connection"), &connection));
    UD_TRY(ud_json_string(ud_member(b->config, "table"), &table));
    char scope[33];
    UD_TRY(ud_connection_field_namespace(b->context.activation, connection, table, scope));
    ud_batch *actual = NULL;
    ud_status status = ud_connection_lease_schema(b->lease, table, (ud_string){scope, 32}, &actual);
    if (!status)
        status = ud_checkpoint_schema(b->context.activation, b->context.component_id, actual);
    char *policy = NULL;
    uint32_t length = 0;
    ud_json *options = NULL;
    if (!status && ud_member(b->config, "schemaPolicy"))
        status = ud_json_serialize(ud_member(b->config, "schemaPolicy"), &policy, &length);
    else if (!status) {
        const char *default_policy = "{\"policy\":\"compatible\"}";
        length = (uint32_t)strlen(default_policy);
        policy = ud_strdup_n(b->context.memory, default_policy, length);
        if (!policy)
            status = UD_RESOURCE;
    }
    b->schema_policy = (ud_string){policy, length};
    if (!status)
        status = ud_json_parse(b->context.memory, policy, length, &options, NULL);
    ud_field *target =
                 !status ? ud_alloc(b->context.memory, (size_t)b->columns * sizeof(*target)) : NULL,
             *input =
                 !status ? ud_alloc(b->context.memory, (size_t)b->columns * sizeof(*input)) : NULL;
    if (!status && (!target || !input))
        status = UD_RESOURCE;
    const ud_json *mapping = ud_member(options, "mapping");
    for (uint32_t i = 0; !status && i < b->columns; i++) {
        ud_string name = {b->fields[i].name, b->fields[i].name_length};
        for (uint32_t m = 0; !status && m < ud_json_size(mapping); m++) {
            ud_string from = {0}, to = {0};
            status = ud_json_string(ud_member(ud_json_at(mapping, m), "actual"), &from);
            if (!status)
                status = ud_json_string(ud_member(ud_json_at(mapping, m), "expected"), &to);
            if (!status && ud_same(from, name))
                name = to;
        }
        int at = -1;
        for (uint32_t j = 0; !status && j < actual->columns; j++) {
            const ud_field *f = &actual->column[j].field;
            char id[33];
            ud_id_format(f->identity, id);
            if ((name.length == f->name_length && !memcmp(name.data, f->name, name.length)) ||
                (name.length == 32 && !memcmp(name.data, id, 32)))
                at = (int)j;
        }
        if (!status && at < 0)
            status = UD_INCOMPATIBLE;
        if (!status) {
            target[i] = actual->column[at].field;
            input[i] = b->fields[i];
            input[i].identity = target[i].identity;
            input[i].lineage = NULL;
            input[i].lineage_count = 0;
            input[i].key_order = target[i].key_order;
        }
    }
    if (!status)
        status = ud_batch_create(b->context.memory, target, b->columns, 0, &b->actual_schema);
    if (!status)
        status = ud_batch_create(b->context.memory, input, b->columns, 0, &b->write_schema);
    uint32_t diff_length = 0;
    if (!status)
        status = ud_schema_compare(b->context.memory, b->actual_schema, b->write_schema,
                                   b->schema_policy, &b->schema_diff, &diff_length);
    ud_json *diff = NULL;
    if (!status)
        status = ud_json_parse(b->context.memory, b->schema_diff, diff_length, &diff, NULL);
    if (!status) {
        const char *p = NULL;
        uint32_t n = 0;
        status = ud_json_text(ud_member(diff, "compatible"), &p, &n);
        if (!status && (n != 4 || memcmp(p, "true", 4)))
            status = UD_INCOMPATIBLE;
    }
    if (b->schema_diff) {
        ud_status recorded = ud_schema_report(b->context.activation,
                                              (ud_string){b->schema_diff, diff_length}, status);
        if (!status)
            status = recorded;
    }
    ud_free(target);
    ud_free(input);
    ud_json_destroy(diff);
    ud_json_destroy(options);
    ud_batch_destroy(actual);
    return status;
}

ud_status ud_builtin_sink_batch(builtin *b, const ud_batch *input, ud_batch **out,
                                ud_batch **before) {
    *out = NULL;
    *before = NULL;
    ud_context *ctx = b->context.memory;
    ud_batch *selected = NULL, *old = NULL, *bound = NULL;
    ud_status status =
        ud_batch_create(ctx, b->schema->schema->fields, b->columns, input->rows, &selected);
    if (!status && b->before_column != UINT32_MAX)
        status = ud_batch_create(ctx, b->schema->schema->fields, b->columns, input->rows, &old);
    ud_value *values = ud_alloc(ctx, b->columns * sizeof(*values));
    uint32_t *indices = ud_alloc(ctx, b->columns * sizeof(*indices));
    if (!values || !indices)
        status = UD_RESOURCE;
    for (uint32_t col = 0; !status && col < b->columns; col++)
        indices[col] = col;
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        ud_row_info info;
        status = ud_batch_row_info(input, row, &info);
        if (!status && b->mode == UD_WRITE_CHANGESET && !info.operation)
            continue;
        if (!status && b->mode == UD_WRITE_CHANGESET && info.operation > UD_CHANGE_UPSERT)
            status = UD_SCHEMA;
        if (!status)
            status = ud_batch_append_projected(selected, input, row, b->write_indices);
        if (!status && old) {
            ud_value previous;
            status = ud_batch_get(input, row, b->before_column, &previous);
            if (!status && previous.is_null && info.operation != UD_CHANGE_INSERT &&
                info.operation != UD_CHANGE_UPSERT)
                status = UD_SCHEMA;
            for (uint32_t col = 0; !status && col < b->columns; col++) {
                if (previous.is_null)
                    status = ud_batch_get(input, row, b->write_indices[col], values + col);
                else
                    values[col] = previous.children[col];
            }
            if (!status)
                status = ud_batch_append(old, values, b->columns);
            if (!status)
                status = ud_batch_set_row_info(old, old->rows - 1, &info);
        }
    }
    if (!status)
        status = ud_batch_seal(selected);
    if (!status && old)
        status = ud_batch_seal(old);
    if (!status)
        status = ud_batch_project(ctx, selected, indices, b->write_schema->schema->fields,
                                  b->columns, &bound);
    if (!status)
        status = ud_schema_map(ctx, bound, b->actual_schema, b->schema_policy, out);
    ud_batch_destroy(bound);
    bound = NULL;
    if (!status && old)
        status = ud_batch_project(ctx, old, indices, b->write_schema->schema->fields, b->columns,
                                  &bound);
    if (!status && old)
        status = ud_schema_map(ctx, bound, b->actual_schema, b->schema_policy, before);
    ud_batch_destroy(bound);
    ud_batch_destroy(selected);
    ud_batch_destroy(old);
    ud_free(values);
    ud_free(indices);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
        ud_batch_destroy(*before);
        *before = NULL;
    }
    return status;
}

ud_status ud_pipeline_write(const ud_component_context *c, ud_connection_lease *lease,
                            const ud_write_options *options, const ud_batch *input) {
    ud_write_options o = *options;
    uint8_t digest[32];
    ud_status s = ud_connection_lease_digest(lease, &o, input, digest);
    size_t length = o.job.length + o.batch_id.length + 8;
    char *identity = ud_alloc(c->memory, length);
    if (!identity)
        return UD_RESOURCE;
    snprintf(identity, length, "%.*s/%.*s", (int)o.job.length, o.job.data, (int)o.batch_id.length,
             o.batch_id.data);
    ud_string key = {identity, (uint32_t)strlen(identity)};
    int replaying = ud_checkpoint_batch_seen(c->activation, key);
    if (!s)
        s = ud_checkpoint_batch(c->activation, key, digest, 0);
    if (!s && replaying) {
        ud_commit_receipt receipt = {0};
        s = ud_connection_lease_receipt(lease, o.job, o.batch_id, &receipt);
        if (!s && receipt.committed &&
            (memcmp(receipt.digest, digest, 32) ||
             !ud_same(receipt.source_position, o.source_position)))
            s = UD_CORRUPT;
        ud_free((void *)receipt.source_position.data);
    }
    struct ud_rule_delivery *delivery = NULL;
    if (!s)
        s = ud_rule_stage(c->activation, input, &delivery);
    if (!s)
        s = ud_connection_lease_write(lease, &o, input);
    if (!s) {
        if (!ud_transaction_rows(c->activation, input->rows, ud_batch_error_rows(input)))
            ud_target_rows(c->activation, input->rows, ud_batch_error_rows(input), 0);
        s = ud_transaction_active(c->activation) ? ud_transaction_delivery(c->activation, input)
                                                 : ud_rule_delivered(c->activation, delivery, 0);
        if (!s)
            s = ud_checkpoint_batch(c->activation, key, digest, 1);
        if (s)
            s = UD_COMMIT_UNKNOWN;
        else if (c->execution->journal) {
            ud_target_rows(c->activation, input->rows, ud_batch_error_rows(input), 1);
            s = ud_rule_delivered(c->activation, delivery, 1);
        }
    }
    if (s == UD_COMMIT_UNKNOWN)
        ud_rule_delivered(c->activation, delivery, 2);
    ud_rule_delivery_destroy(delivery);
    ud_free(identity);
    return s;
}

ud_status ud_builtin_sink_write(builtin *b, const ud_batch *input, const ud_batch *before) {
    if (!input->rows)
        return UD_OK;
    ud_string table = {0};
    UD_TRY(ud_json_string(ud_member(b->config, "table"), &table));
    char id[32], position[96];
    snprintf(id, sizeof(id), "%llu", (unsigned long long)b->number);
    ud_row_info last = {0};
    if (input->rows)
        UD_TRY(ud_batch_row_info(input, input->rows - 1, &last));
    if (last.id.high || last.id.low) {
        char row_id[33];
        ud_id_format(last.id, row_id);
        snprintf(position, sizeof(position), "row:%s/count:%u", row_id, input->rows);
    } else
        snprintf(position, sizeof(position), "batch:%llu", (unsigned long long)b->number);
    ud_write_options o = {sizeof(o),
                          UD_ABI_VERSION,
                          b->mode,
                          table,
                          {b->job, (uint32_t)strlen(b->job)},
                          {id, (uint32_t)strlen(id)},
                          {position, (uint32_t)strlen(position)},
                          b->keys,
                          b->key_count};
    o.current_column = UINT32_MAX;
    o.load_mode = b->load_mode;
    o.before = before;
    int32_t *operations = b->mode == UD_WRITE_CHANGESET
                              ? ud_alloc(b->context.memory, input->rows * sizeof(*operations))
                              : NULL;
    ud_status status = b->mode == UD_WRITE_CHANGESET && !operations ? UD_RESOURCE : UD_OK;
    for (uint32_t row = 0; !status && operations && row < input->rows; row++) {
        ud_row_info info;
        status = ud_batch_row_info(input, row, &info);
        operations[row] = (int32_t)info.operation;
    }
    o.operations = operations;
    if (!status)
        status = ud_pipeline_write(&b->context, b->lease, &o, input);
    ud_free(operations);
    if (!status)
        b->number++;
    return status;
}

ud_status ud_builtin_sink_finish(builtin *b) {
    ud_status s;
#ifdef UD_WITH_ARROW_FILES
    if (b->columnar_writer) {
        s = ud_columnar_writer_close(b->columnar_writer);
        b->columnar_writer = NULL;
    } else
#endif
        s = b->kind == CSV_SINK    ? ud_csv_writer_close(b->csv_writer)
            : b->kind == XLSX_SINK ? ud_xlsx_writer_close(b->xlsx_writer)
                                   : ud_json_writer_close(b->writer);
    b->csv_writer = NULL;
    b->writer = NULL;
    b->xlsx_writer = NULL;
    if (!s)
        s = ud_connection_lease_authorize(b->lease, UD_ACCESS_WRITE);
    if (!s && b->existing) {
        uint8_t expected[32], actual[32];
        s = ud_file_hash(b->context.memory, b->temporary, expected);
        if (!s)
            s = ud_file_hash(b->context.memory, b->path, actual);
        if (!s && memcmp(expected, actual, 32))
            s = UD_CORRUPT;
        if (!s)
            s = ud_remove_file(b->context.memory, b->temporary);
    } else if (!s)
        s = ud_publish(b->context.memory, b->temporary, b->path);
    if (!s) {
        b->published = 1;
        ud_target_rows(b->context.activation, b->written_rows, b->written_errors, 0);
        s = ud_rule_delivered(b->context.activation, b->delivery, 0);
        if (!s)
            s = ud_checkpoint_file_done(b->context.activation, b->path);
        if (s)
            s = UD_COMMIT_UNKNOWN;
        else if (b->context.execution->journal) {
            ud_target_rows(b->context.activation, b->written_rows, b->written_errors, 1);
            s = ud_rule_delivered(b->context.activation, b->delivery, 1);
        }
    }
    if (s == UD_COMMIT_UNKNOWN)
        ud_rule_delivered(b->context.activation, b->delivery, 2);
    return s;
}
