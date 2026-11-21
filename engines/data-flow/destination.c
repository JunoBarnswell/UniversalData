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
            input[i].physical_identity = target[i].physical_identity;
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

static ud_status map_sink_projection(builtin *b, const ud_batch *input, const uint32_t *indices,
                                     ud_batch **out) {
    ud_batch *bound = NULL;
    ud_status status = ud_batch_project(b->context.memory, input, indices,
                                        b->write_schema->schema->fields, b->columns, &bound);
    if (!status)
        status = ud_schema_map(b->context.memory, bound, b->actual_schema, b->schema_policy, out);
    ud_batch_destroy(bound);
    return status;
}

ud_status ud_builtin_sink_batch(builtin *b, const ud_batch *input, ud_batch **out,
                                ud_batch **before) {
    *out = NULL;
    *before = NULL;
    if (input->sealed && b->mode != UD_WRITE_CHANGESET && b->before_column == UINT32_MAX)
        return map_sink_projection(b, input, b->write_indices, out);
    ud_context *ctx = b->context.memory;
    ud_batch *selected = NULL, *old = NULL;
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
        if (!status && b->mode == UD_WRITE_CHANGESET && info.operation > UD_CHANGE_TRUNCATE)
            status = UD_SCHEMA;
        if (!status)
            status = ud_batch_append_projected(selected, input, row, b->write_indices);
        if (!status && old) {
            ud_value previous;
            status = ud_batch_get(input, row, b->before_column, &previous);
            if (!status && previous.is_null && info.operation != UD_CHANGE_INSERT &&
                info.operation != UD_CHANGE_UPSERT && info.operation != UD_CHANGE_TRUNCATE)
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
        status = map_sink_projection(b, selected, indices, out);
    if (!status && old)
        status = map_sink_projection(b, old, indices, before);
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
    int durable = c->execution->journal != NULL;
    ud_status s = UD_OK;
    char *identity = NULL;
    ud_string key = {0};
    ud_command_result prior = {.struct_size = sizeof(prior),.abi_version = UD_ABI_VERSION};
    if (durable) {
        UD_TRY(ud_connection_lease_digest(lease, &o, input, digest));
        size_t length = o.job.length + o.batch_id.length + 8;
        identity = ud_alloc(c->memory, length);
        if (!identity)
            return UD_RESOURCE;
        snprintf(identity, length, "%.*s/%.*s", (int)o.job.length, o.job.data, (int)o.batch_id.length,
                 o.batch_id.data);
        key = (ud_string){identity, (uint32_t)strlen(identity)};
        s = ud_checkpoint_effect_read(c->activation, key, digest, &prior);
    }
    int replaying = prior.state != UD_COMMAND_NOT_EXECUTED;
    int completed = prior.state == UD_COMMAND_APPLIED || prior.state == UD_COMMAND_COMMITTED;
    if (!s && prior.state == UD_COMMAND_REJECTED) s = UD_CORRUPT;
    ud_command_result_clear(&prior);
    int32_t guarantee = ud_connection_lease_delivery(lease);
    if (!s && durable)
        s = ud_checkpoint_effect_intent(c->activation, key, digest);
    if (!s && replaying && !completed && guarantee == UD_DELIVERY_RESPONSE)
        s = UD_COMMIT_UNKNOWN;
    /* Incomplete effects are resolved by the driver's atomic idempotent
     * write. A separate absent-key probe can acquire transaction gap locks. */
    if (!s && replaying && completed && guarantee == UD_DELIVERY_RECEIPT) {
        ud_commit_receipt receipt = {0};
        s = ud_connection_lease_receipt(lease, o.job, o.batch_id, &receipt);
        if (!s && receipt.committed &&
            (memcmp(receipt.digest, digest, 32) ||
             !ud_same(receipt.source_position, o.source_position)))
            s = UD_CORRUPT;
        if (!s && completed && !receipt.committed)
            s = UD_CORRUPT;
        ud_free((void *)receipt.source_position.data);
    }
    struct ud_rule_delivery *delivery = NULL;
    if (!s)
        s = ud_rule_stage(c->activation, input, &delivery);
    if (!s && !completed)
        s = ud_connection_lease_write(lease, &o, input);
    if (!s) {
        if (!ud_transaction_rows(c->activation, input->rows, ud_batch_error_rows(input)))
            ud_target_rows(c->activation, input->rows, ud_batch_error_rows(input), 0);
        s = ud_transaction_active(c->activation) ? ud_transaction_delivery(c->activation, input)
                                                 : ud_rule_delivered(c->activation, delivery, 0);
        if (!s && durable) {
            ud_command_result outcome = {.struct_size = sizeof(outcome),.abi_version = UD_ABI_VERSION,
                .state = ud_transaction_active(c->activation) ? UD_COMMAND_APPLIED : UD_COMMAND_COMMITTED,.affected_rows = input->rows};
            s = ud_checkpoint_effect_complete(c->activation,key,digest,&outcome);
        }
        if (s)
            s = UD_COMMIT_UNKNOWN;
        else if (durable && !ud_transaction_active(c->activation)) {
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
    if (b->kind == SQL_SINK)
        o.delivery = b->delivery_mode;
    const ud_json *truncate = ud_member(b->config, "allowTruncate");
    if (truncate) {
        const char *text;
        uint32_t length;
        if (ud_json_kind(truncate) != UD_JSON_BOOL)
            return UD_SCHEMA;
        UD_TRY(ud_json_text(truncate, &text, &length));
        o.allow_truncate = length == 4 && !memcmp(text, "true", 4);
    }
    o.before = before;
    int32_t *operations = b->mode == UD_WRITE_CHANGESET
                              ? ud_alloc(b->context.memory, input->rows * sizeof(*operations))
                              : NULL;
    ud_status status = b->mode == UD_WRITE_CHANGESET && !operations ? UD_RESOURCE : UD_OK;
    for (uint32_t row = 0; !status && operations && row < input->rows; row++) {
        ud_row_info info;
        status = ud_batch_row_info(input, row, &info);
        operations[row] = (int32_t)info.operation;
        if (!status && info.operation == UD_CHANGE_TRUNCATE) {
            if (!o.allow_truncate)
                status = UD_UNSUPPORTED;
            else
                status = ud_connection_lease_authorize(b->lease, UD_ACCESS_DELETE);
        }
    }
    o.operations = operations;
    if (!status)
        status = ud_pipeline_write(&b->context, b->lease, &o, input);
    ud_free(operations);
    if (!status)
        b->number++;
    return status;
}
static int record_failure(ud_status status) {
    return status == UD_PARSE || status == UD_OVERFLOW || status == UD_TRUNCATED ||
           status == UD_QUALITY;
}
static ud_status mapped_write(builtin *b, const ud_batch *input) {
    ud_batch *mapped = NULL, *before = NULL;
    ud_string previous = {0};
    ud_status status = ud_connection_lease_record_diagnostic(b->lease, &previous);
    ud_free((void *)previous.data);
    if (!status)
        status = ud_builtin_sink_batch(b, input, &mapped, &before);
    if (!status)
        status = ud_builtin_sink_write(b, mapped, before);
    ud_batch_destroy(mapped);
    ud_batch_destroy(before);
    return status;
}
ud_status ud_builtin_sink_records(builtin *b, const ud_batch *input) {
    int policy;
    UD_TRY(ud_error_policy(b->config, "errorPolicy", UD_ERROR_FAIL, &policy));
    if (policy == UD_ERROR_FAIL)
        return mapped_write(b, input);
    for (uint32_t row = 0; row < input->rows; row++) {
        ud_row_info info;
        UD_TRY(ud_batch_row_info(input, row, &info));
        if (info.operation == UD_CHANGE_TRUNCATE)
            return mapped_write(b, input); /* A table reset has no rejected-record boundary. */
    }
    if (!ud_transaction_active(b->context.activation))
        return UD_UNSUPPORTED;
    ud_string savepoint = {"ud_record_batch", 15};
    UD_TRY(ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_BEGIN));
    ud_status status = mapped_write(b, input);
    if (!status)
        return ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_RELEASE);
    if (!record_failure(status))
        return status;
    UD_TRY(ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_ROLLBACK));
    UD_TRY(ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_RELEASE));
    /* The failed batch intent remains fenced; row attempts use distinct identities. */
    if (b->number == UINT64_MAX)
        return UD_RESOURCE;
    b->number++;
    ud_batch *errors = NULL;
    status = ud_batch_create(b->context.memory, b->errors->schema->fields, b->errors->columns,
                             input->rows, &errors);
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        ud_batch *record = NULL;
        status = ud_batch_slice(b->context.memory, input, row, 1, &record);
        if (!status)
            status = ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_BEGIN);
        int load = b->load_mode;
        b->load_mode = UD_LOAD_PARAMETERS; /* Isolation retries use driver parameter binding. */
        ud_status written = !status ? mapped_write(b, record) : status;
        b->load_mode = load;
        ud_batch_destroy(record);
        if (status)
            break;
        if (!written) {
            status = ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_RELEASE);
            continue;
        }
        if (!record_failure(written)) {
            status = written;
            break;
        }
        ud_string provider = {0};
        status = ud_connection_lease_record_diagnostic(b->lease, &provider);
        if (!status)
            status = ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_ROLLBACK);
        if (!status)
            status = ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_RELEASE);
        if (!status && policy == UD_ERROR_REDIRECT)
            status = ud_error_append(&b->context.errors, errors,
                                     &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                                        .abi_version = UD_ABI_VERSION,
                                                        .input = input,
                                                        .row = row,
                                                        .rule = {"sql.destination", 15},
                                                        .rule_version = 1,
                                                        .code = written,
                                                        .policy = policy,
                                                        .provider_code = provider});
        ud_free((void *)provider.data);
        if (!status) {
            ud_task_rows(b->context.activation, 0, 0, 1);
            if (b->number == UINT64_MAX)
                status = UD_RESOURCE;
            else
                b->number++;
        }
    }
    if (!status && errors->rows) {
        status = ud_batch_seal(errors);
        if (!status)
            status = b->context.emit(b->context.emit_user, 0, errors);
    }
    ud_batch_destroy(errors);
    return status;
}

ud_status ud_builtin_sink_append_original(builtin *b) {
    ud_context *ctx = b->context.memory;
    UD_TRY(ud_file_generation_check_original(b->generation));
    ud_csv_reader *csv = NULL;
    ud_json_reader *json = NULL;
#ifdef UD_WITH_ARROW_FILES
    ud_columnar_reader *columnar = NULL;
#endif
    ud_status status = UD_OK;
    if (b->kind == CSV_SINK) {
        char *format = NULL;
        uint32_t length = 0;
        if (ud_member(b->config, "fileFormat"))
            status = ud_json_serialize(ud_member(b->config, "fileFormat"), &format, &length);
        ud_csv_options options = {.struct_size = sizeof(options),
                                  .abi_version = UD_ABI_VERSION,
                                  .batch_size = b->context.max_batch_rows,
                                  .max_record_bytes = 16777216,
                                  .format = {format, length}};
        if (!status)
            status =
                ud_csv_open(ctx, b->generation->original, b->fields, b->columns, &options, &csv);
        ud_free(format);
    } else if (b->kind == JSON_SINK) {
        ud_json_options options = {.struct_size = sizeof(options),
                                   .version = UD_CONFIG_VERSION,
                                   .batch_size = b->context.max_batch_rows,
                                   .max_record_bytes = 16777216};
        status = ud_json_reader_open(ctx, b->generation->original, b->fields, b->columns, &options,
                                     &json);
    }
#ifdef UD_WITH_ARROW_FILES
    else {
        ud_columnar_file_options options = {.struct_size = sizeof(options),
                                            .abi_version = UD_ABI_VERSION,
                                            .batch_rows = b->context.max_batch_rows,
                                            .format = b->kind == PARQUET_SINK ? UD_FILE_PARQUET
                                                      : b->kind == IPC_SINK   ? UD_FILE_ARROW_IPC
                                                                            : UD_FILE_ARROW_STREAM,
                                            .buffers = b->context.execution->buffers,
                                            .check = ud_builtin_operator_check,
                                            .user = b->context.activation};
        status =
            ud_columnar_reader_open(ctx, b->generation->original, b->schema, &options, &columnar);
    }
#else
    else
        status = UD_UNSUPPORTED;
#endif
    while (!status) {
        ud_batch *batch = NULL;
        status = ud_activation_check(b->context.activation);
        if (!status)
            status = ud_file_generation_check_original(b->generation);
        if (!status)
            status = csv    ? ud_csv_next(csv, &batch, NULL)
                     : json ? ud_json_reader_next(json, &batch, NULL)
#ifdef UD_WITH_ARROW_FILES
                            : ud_columnar_reader_next(columnar, &batch)
#else
                            : UD_UNSUPPORTED
#endif
                ;
        if (status == UD_END) {
            status = UD_OK;
            ud_batch_destroy(batch);
            break;
        }
        if (!status)
            status = csv    ? ud_csv_writer_append(b->csv_writer, batch)
                     : json ? ud_json_writer_append(b->writer, batch)
#ifdef UD_WITH_ARROW_FILES
                            : ud_columnar_writer_append(b->columnar_writer, batch)
#else
                            : UD_UNSUPPORTED
#endif
                ;
        ud_batch_destroy(batch);
    }
    ud_csv_close(csv);
    ud_json_reader_close(json);
#ifdef UD_WITH_ARROW_FILES
    ud_columnar_reader_close(columnar);
#endif
    return status;
}
ud_status ud_builtin_sink_finish(builtin *b) {
    ud_status s = UD_OK;
    if (!b->generation->prepared) {
#ifdef UD_WITH_ARROW_FILES
        if (b->columnar_writer) {
            s = ud_columnar_writer_close(b->columnar_writer);
            b->columnar_writer = NULL;
        } else
#endif
            s = b->kind == CSV_SINK    ? ud_csv_writer_close(b->csv_writer)
                : b->kind == XLSX_SINK ? ud_xlsx_writer_close(b->xlsx_writer)
                                       : ud_json_writer_close(b->writer);
    }
    b->csv_writer = NULL;
    b->writer = NULL;
    b->xlsx_writer = NULL;
    if (!s)
        s = ud_connection_lease_authorize(b->lease, UD_ACCESS_WRITE);
    if (!s)
        s = ud_file_generation_publish(b->generation);
    if (!s) {
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

ud_status ud_builtin_sink_projection(const ud_component_context *c, const ud_json *config,
                                     const ud_batch *input, int mode, ud_batch **out,
                                     uint32_t **indices, uint32_t *before) {
    if (!c || !config || !input || !out || !indices || !before)
        return UD_INVALID;
    *out = NULL;
    *indices = NULL;
    *before = UINT32_MAX;
    const ud_json *mapping = ud_member(config, "mapping");
    uint32_t count = mapping ? ud_json_size(mapping) : input->columns;
    if ((mapping && ud_json_kind(mapping) != UD_JSON_ARRAY) || !count || count > 65536)
        return UD_SCHEMA;
    if (mapping && mode == UD_WRITE_CHANGESET)
        return UD_UNSUPPORTED;
    ud_field *fields = ud_alloc(c->memory, count * sizeof(*fields));
    uint32_t *selected = ud_alloc(c->memory, count * sizeof(*selected));
    ud_status status = fields && selected ? UD_OK : UD_RESOURCE;
    ud_string before_name = {"before", 6};
    if (!status && ud_member(config, "beforeField"))
        status = ud_json_string(ud_member(config, "beforeField"), &before_name);
    uint32_t total = 0;char scope[33];
    if(!status&&mapping)status=ud_pipeline_field_namespace(c,scope);
    for (uint32_t j = 0; !status && j < count; j++) {
        uint32_t source = j;
        ud_string target = {0};
        if (mapping) {
            const ud_json *entry = ud_json_at(mapping, j);
            static const char *keys[] = {"source", "target"};
            status = ud_json_keys(entry, keys, 2);
            ud_string reference = {0};
            if (!status)
                status = ud_json_string(ud_member(entry, "source"), &reference);
            if (!status)
                status = ud_json_string(ud_member(entry, "target"), &target);
            ud_id id = {0};
            int by_id = !ud_id_parse(reference, &id), found = 0;
            for (uint32_t at = 0; !status && at < input->columns; at++) {
                const ud_field *field = input->schema->fields + at;
                if (by_id ? !ud_id_compare(id, field->identity)
                          : reference.length == field->name_length &&
                                !memcmp(reference.data, field->name, reference.length)) {
                    if (found) {
                        status = UD_SCHEMA;
                        break;
                    }
                    found = 1;
                    source = at;
                }
            }
            if (!status && !found)
                status = UD_SCHEMA;
        }
        if (status)
            break;
        ud_field field = input->schema->fields[source];
        if (mode == UD_WRITE_CHANGESET && field.name_length == before_name.length &&
            !memcmp(field.name, before_name.data, before_name.length)) {
            if (field.type != UD_STRUCT)
                status = UD_SCHEMA;
            else
                *before = source;
            continue;
        }
        selected[total] = source;
        if(mapping)status=ud_field_derive(c->memory,&field,(ud_string){scope,32},target,fields+total);
        else fields[total]=field;
        total++;
    }
    if (!status && *before != UINT32_MAX) {
        const ud_field *prior = input->schema->fields + *before;
        if (prior->child_count != total)
            status = UD_SCHEMA;
        for (uint32_t j = 0; !status && j < total; j++)
            if (!ud_field_matches(prior->children + j, fields + j, UD_FIELD_LAYOUT))
                status = UD_SCHEMA;
    }
    if (!status)
        status = ud_batch_create(c->memory, fields, total, 0, out);
    if(mapping)for(uint32_t j=0;j<count;j++)ud_field_clear(fields+j);
    ud_free(fields);
    if (status)
        ud_free(selected);
    else
        *indices = selected;
    return status;
}
