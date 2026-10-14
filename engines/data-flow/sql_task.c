#include "builtins_internal.h"
ud_status ud_builtin_sql_command_prepare(builtin *b) {
    ud_string statement;
    UD_TRY(ud_json_string(ud_member(b->config, "statement"), &statement));
    b->read_sql = (ud_string){ud_strdup_n(b->context.memory, statement.data, statement.length),
                              statement.length};
    if (!b->read_sql.data)
        return UD_RESOURCE;
    const ud_json *parameters = ud_member(b->config, "parameters");
    if (ud_json_kind(parameters) != UD_JSON_ARRAY || ud_json_size(parameters) > 65535)
        return UD_SCHEMA;
    b->key_count = ud_json_size(parameters);
    b->write_indices = ud_alloc(b->context.memory, (b->key_count + 1) * sizeof(*b->write_indices));
    if (!b->write_indices)
        return UD_RESOURCE;
    for (uint32_t at = 0; at < b->key_count; at++) {
        uint64_t column;
        UD_TRY(ud_json_uint(ud_json_at(parameters, at), &column));
        if (column >= b->columns)
            return UD_SCHEMA;
        b->write_indices[at] = (uint32_t)column;
        for (uint32_t previous = 0; previous < at; previous++)
            if (b->write_indices[previous] == column)
                return UD_SCHEMA;
    }
    return UD_OK;
}
static int command_record_failure(ud_status status) {
    return status == UD_QUALITY || status == UD_PARSE || status == UD_OVERFLOW ||
           status == UD_TRUNCATED;
}
static ud_status command_record(builtin *b, const ud_batch *parameters, uint32_t row,
                                int *executed) {
    *executed = 0;
    ud_batch *record = NULL;
    ud_status status =
        parameters ? ud_batch_slice(b->context.memory, parameters, row, 1, &record) : UD_OK;
    uint8_t fingerprint[32] = {0}, digest[32];
    if (!status && record)
        status = ud_batch_fingerprint(record, fingerprint);
    ud_bytes proof = {.ctx = b->context.memory};
    if (!status)
        status = ud_put_string(&proof, b->read_sql.data, b->read_sql.length);
    if (!status)
        status = ud_bytes_put(&proof, fingerprint, 32);
    if (!status)
        ud_hash(proof.data, proof.size, digest);
    ud_bytes_clear(&proof);
    ud_bytes identity = {.ctx = b->context.memory};
    if (!status)
        status = ud_put_string(&identity, b->context.execution->options.execution_id.data,
                               b->context.execution->options.execution_id.length);
    if (!status)
        status = ud_put_string(&identity, b->context.task_id.data, b->context.task_id.length);
    if (!status)
        status =
            ud_put_string(&identity, b->context.component_id.data, b->context.component_id.length);
    if (!status)
        status = ud_put_u64(&identity, b->number);
    char key[65];
    uint8_t key_hash[32];
    if (!status) {
        ud_hash(identity.data, identity.size, key_hash);
        for (uint32_t at = 0; at < 32; at++)
            snprintf(key + 2 * at, 3, "%02x", key_hash[at]);
    }
    ud_bytes_clear(&identity);
    int complete =
        !status && ud_checkpoint_batch_completed(b->context.activation, (ud_string){key, 64});
    if (!status && !complete &&
        ud_checkpoint_batch_seen(b->context.activation, (ud_string){key, 64}) &&
        !ud_transaction_active(b->context.activation))
        status = UD_COMMIT_UNKNOWN;
    if (!status)
        status = ud_checkpoint_batch(b->context.activation, (ud_string){key, 64}, digest, 0);
    if (!status && !complete) {
        ud_read_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .sql = b->read_sql,
                                   .parameters = record};
        status = ud_connection_lease_execute_command(b->lease, &options, NULL);
        if (status && !ud_transaction_active(b->context.activation) &&
            (status == UD_IO || status == UD_TRANSIENT || status == UD_TIMEOUT ||
             status == UD_CANCELLED))
            status = UD_COMMIT_UNKNOWN;
        if (!status)
            *executed = 1;
    }
    if (!status)
        status = ud_checkpoint_batch(b->context.activation, (ud_string){key, 64}, digest, 1);
    if (!status && !ud_transaction_rows(b->context.activation, 1, 0)) {
        ud_target_rows(b->context.activation, 1, 0, 0);
        if (b->context.execution->journal)
            ud_target_rows(b->context.activation, 1, 0, 1);
    }
    ud_batch_destroy(record);
    return status;
}
ud_status ud_builtin_sql_command(builtin *b, const ud_batch *input) {
    int policy;
    UD_TRY(ud_error_policy(b->config, "errorPolicy", UD_ERROR_FAIL, &policy));
    if (policy != UD_ERROR_FAIL && !ud_transaction_active(b->context.activation))
        return UD_UNSUPPORTED;
    ud_batch *parameters = NULL, *accepted = NULL, *errors = NULL;
    ud_field *fields =
        b->key_count ? ud_alloc(b->context.memory, b->key_count * sizeof(*fields)) : NULL;
    ud_status status = b->key_count && !fields ? UD_RESOURCE : UD_OK;
    for (uint32_t at = 0; !status && at < b->key_count; at++)
        fields[at] = input->column[b->write_indices[at]].field;
    if (!status && b->key_count)
        status = ud_batch_project(b->context.memory, input, b->write_indices, fields, b->key_count,
                                  &parameters);
    ud_free(fields);
    if (!status)
        status = ud_batch_create(b->context.memory, input->schema->fields, input->columns,
                                 input->rows, &accepted);
    if (!status)
        status = ud_batch_create(b->context.memory, b->errors->schema->fields, b->errors->columns,
                                 input->rows, &errors);
    ud_string savepoint = {"ud_row_command", 14};
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        if (b->number == UINT64_MAX) {
            status = UD_RESOURCE;
            break;
        }
        ud_string previous = {0};
        status = ud_connection_lease_record_diagnostic(b->lease, &previous);
        ud_free((void *)previous.data);
        if (!status && policy != UD_ERROR_FAIL)
            status = ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_BEGIN);
        int executed = 0;
        ud_status written = !status ? command_record(b, parameters, row, &executed) : status;
        if (status)
            break;
        if (!written) {
            if (policy != UD_ERROR_FAIL)
                status = ud_connection_lease_savepoint(b->lease, savepoint, UD_SAVEPOINT_RELEASE);
            if (!status)
                status = ud_batch_append_projected(accepted, input, row, NULL);
        } else if (policy == UD_ERROR_FAIL || !command_record_failure(written) || executed)
            status = executed ? UD_COMMIT_UNKNOWN : written;
        else {
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
                                                            .rule = {"sql.command", 11},
                                                            .rule_version = 1,
                                                            .code = written,
                                                            .policy = policy,
                                                            .provider_code = provider});
            ud_free((void *)provider.data);
            if (!status)
                ud_task_rows(b->context.activation, 0, 0, 1);
        }
        if (!status)
            b->number++;
    }
    if (!status)
        status = ud_batch_seal(accepted);
    if (!status)
        status = ud_batch_seal(errors);
    if (!status && accepted->rows)
        status = b->context.emit(b->context.emit_user, 0, accepted);
    if (!status && errors->rows)
        status = b->context.emit(b->context.emit_user, 1, errors);
    ud_batch_destroy(parameters);
    ud_batch_destroy(accepted);
    ud_batch_destroy(errors);
    return status;
}
ud_status ud_builtin_sql_task(void *user, const ud_component_context *c, ud_string text) {
    (void)user;
    ud_json *config = NULL;
    UD_TRY(ud_builtin_parse(c, text, &config));
    static const char *keys[] = {"connection", "statement", "parameters"};
    ud_status s = ud_json_keys(config, keys, 3);
    ud_string id = {0}, statement = {0};
    if (!s)
        s = ud_json_string(ud_member(config, "connection"), &id);
    if (!s)
        s = ud_json_string(ud_member(config, "statement"), &statement);
    ud_connection_lease *lease = NULL;
    if (!s)
        s = ud_activation_acquire_connection(c->activation, id, UD_CONNECT_EXECUTE_SQL, &lease);
    ud_batch *params = NULL;
    if (!s)
        s = ud_builtin_parameters(c, config, &params);
    if (!s)
        s = ud_connection_lease_execute_command(
            lease,
            &(ud_read_options){.struct_size = sizeof(ud_read_options),
                               .abi_version = UD_ABI_VERSION,
                               .sql = statement,
                               .parameters = params},
            NULL);
    ud_batch_destroy(params);
    ud_status clean = ud_connection_lease_release(lease);
    ud_json_destroy(config);
    return s ? s : clean;
}

ud_status ud_builtin_sql_task_configure(void *user, const ud_component_context *c, ud_string text,
                                        const ud_batch *const *inputs, ud_batch **outputs) {
    (void)user;
    (void)inputs;
    (void)outputs;
    ud_json *config = NULL;
    ud_status status = ud_builtin_parse(c, text, &config);
    static const char *keys[] = {"connection", "statement", "parameters"};
    if (!status)
        status = ud_json_keys(config, keys, 3);
    ud_string id = {0}, statement = {0};
    if (!status)
        status = ud_json_string(ud_member(config, "connection"), &id);
    /* A loop-variable string is typed at definition binding and materialized at
     * Activation binding. Nonempty SQL is checked by invoke before any lease. */
    if (!status && ud_json_kind(ud_member(config, "statement")) != UD_JSON_STRING)
        status = UD_SCHEMA;
    if (!status)
        status = ud_json_text(ud_member(config, "statement"), &statement.data, &statement.length);
    if (!status)
        status = ud_builtin_connection_defined(c, id);
    ud_batch *params = NULL;
    if (!status)
        status = ud_builtin_parameters(c, config, &params);
    ud_batch_destroy(params);
    ud_json_destroy(config);
    return status;
}
