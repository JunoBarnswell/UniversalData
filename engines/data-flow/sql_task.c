#include "builtins_internal.h"
ud_status ud_builtin_sql_command_prepare(builtin *b) {
    ud_string statement;
    UD_TRY(ud_json_string(ud_member(b->config,"statement"),&statement));
    UD_TRY(ud_sql_validate_record_command(statement));
    b->read_sql = (ud_string){ud_strdup_n(b->context.memory,statement.data,statement.length),statement.length};
    if (!b->read_sql.data) return UD_RESOURCE;
    const ud_json *parameters = ud_member(b->config,"parameters");
    if (ud_json_kind(parameters) != UD_JSON_ARRAY || ud_json_size(parameters) > 65535) return UD_SCHEMA;
    b->key_count = ud_json_size(parameters);
    b->write_indices = ud_alloc(b->context.memory,(b->key_count+1)*sizeof(*b->write_indices));
    if (!b->write_indices) return UD_RESOURCE;
    for (uint32_t at = 0; at < b->key_count; at++) {
        uint64_t column; UD_TRY(ud_json_uint(ud_json_at(parameters,at),&column));
        if (column >= b->columns) return UD_SCHEMA;
        b->write_indices[at] = (uint32_t)column;
    }
    return UD_OK;
}
static ud_status command_parameters(builtin *b,const ud_batch *input,ud_batch **out) {
    *out = NULL; if (!b->key_count) return UD_OK;
    ud_field *fields = ud_alloc(b->context.memory,b->key_count*sizeof(*fields));
    if (!fields) return UD_RESOURCE;
    char scope[33]; ud_id_format(b->context.errors.field_scope,scope);
    ud_status status = UD_OK;
    for (uint32_t at = 0; !status && at < b->key_count; at++) {
        char name[32]; int length = snprintf(name,sizeof(name),"parameter%u",at+1);
        status = ud_field_derive(b->context.memory,&input->column[b->write_indices[at]].field,
            (ud_string){scope,32},(ud_string){name,(uint32_t)length},fields+at);
    }
    if (!status) status = ud_batch_project(b->context.memory,input,b->write_indices,fields,b->key_count,out);
    for (uint32_t at = 0; at < b->key_count; at++) ud_field_clear(fields+at);
    ud_free(fields); return status;
}
static ud_status command_record(builtin *b,const ud_batch *parameters,uint32_t row,int policy,ud_command_result *outcome) {
    ud_batch *record = NULL;
    ud_status status = parameters ? ud_batch_slice(b->context.memory,parameters,row,1,&record) : UD_OK;
    uint8_t fingerprint[32] = {0},digest[32],key_hash[32];
    if (!status && record) status = ud_batch_fingerprint(record,fingerprint);
    ud_bytes proof = {.ctx = b->context.memory};
    if (!status) status = ud_put_string(&proof,b->read_sql.data,b->read_sql.length);
    if (!status) status = ud_bytes_put(&proof,fingerprint,32);
    if (!status) ud_hash(proof.data,proof.size,digest);
    ud_bytes_clear(&proof);
    ud_bytes identity = {.ctx = b->context.memory};
    if (!status) status = ud_put_string(&identity,b->context.execution->options.execution_id.data,b->context.execution->options.execution_id.length);
    if (!status) status = ud_put_string(&identity,b->context.task_id.data,b->context.task_id.length);
    if (!status) status = ud_put_string(&identity,b->context.component_id.data,b->context.component_id.length);
    if (!status) status = ud_put_u64(&identity,b->number);
    char key[65];
    if (!status) { ud_hash(identity.data,identity.size,key_hash); for (uint32_t at = 0; at < 32; at++) snprintf(key+2*at,3,"%02x",key_hash[at]); }
    ud_bytes_clear(&identity);
    if (!status) status = ud_checkpoint_effect_read(b->context.activation,(ud_string){key,64},digest,outcome);
    int retry=!status && outcome->state==UD_COMMAND_REJECTED && b->context.execution->options.resume &&
        !ud_transaction_active(b->context.activation) &&
        (policy==UD_ERROR_FAIL || !ud_record_error_recoverable(outcome->result));
    if (retry) {
        status=ud_checkpoint_effect_retry(b->context.activation,(ud_string){key,64},digest);
        if (!status) ud_command_result_clear(outcome);
    }
    int complete = outcome->state == UD_COMMAND_APPLIED || outcome->state == UD_COMMAND_COMMITTED || outcome->state == UD_COMMAND_REJECTED;
    if (!status && !complete && outcome->state == UD_COMMAND_PENDING && !ud_transaction_active(b->context.activation)) {
        outcome->state = UD_COMMAND_UNKNOWN; outcome->result = status = UD_COMMIT_UNKNOWN;
    }
    if (!status && !complete) {
        status = ud_checkpoint_effect_intent(b->context.activation,(ud_string){key,64},digest);
        if (!status) {
            ud_command_result_clear(outcome);
            ud_read_options options = {.struct_size = sizeof(options),.abi_version = UD_ABI_VERSION,.sql = b->read_sql,.parameters = record};
            ud_status written = ud_connection_lease_execute_record(b->lease,&options,outcome);
            outcome->result = written;
            if (outcome->state == UD_COMMAND_NOT_EXECUTED && written) outcome->state = UD_COMMAND_REJECTED;
            if (outcome->state != UD_COMMAND_UNKNOWN) {
                status = ud_checkpoint_effect_complete(b->context.activation,(ud_string){key,64},digest,outcome);
                if (status && (outcome->state == UD_COMMAND_APPLIED || outcome->state == UD_COMMAND_COMMITTED)) {
                    outcome->state = UD_COMMAND_UNKNOWN; outcome->result = status = UD_COMMIT_UNKNOWN;
                }
            }
            if (!status) status = written;
        }
    } else if (!status) status = outcome->result;
    if (!status && !ud_transaction_rows(b->context.activation,1,0)) {
        ud_target_rows(b->context.activation,1,0,0);
        if (b->context.execution->journal) ud_target_rows(b->context.activation,1,0,1);
    }
    ud_batch_destroy(record); return status;
}
ud_status ud_builtin_sql_command(builtin *b,const ud_batch *input) {
    int policy; UD_TRY(ud_error_policy(b->config,"errorPolicy",UD_ERROR_FAIL,&policy));
    ud_batch *parameters = NULL,*accepted = NULL,*errors = NULL;
    ud_status status = command_parameters(b,input,&parameters);
    if (!status) status = ud_batch_create(b->context.memory,input->schema->fields,input->columns,input->rows,&accepted);
    if (!status) status = ud_batch_create(b->context.memory,b->errors->schema->fields,b->errors->columns,input->rows,&errors);
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        if (b->number == UINT64_MAX) { status = UD_RESOURCE; break; }
        ud_command_result outcome = {.struct_size = sizeof(outcome),.abi_version = UD_ABI_VERSION};
        ud_status written = command_record(b,parameters,row,policy,&outcome);
        if (!written) status = ud_batch_append_projected(accepted,input,row,NULL);
        else if (policy == UD_ERROR_FAIL || outcome.state != UD_COMMAND_REJECTED || !ud_record_error_recoverable(written)) status = written;
        else {
            if (policy == UD_ERROR_REDIRECT) status = ud_error_append(&b->context.errors,errors,
                &(ud_record_error){.struct_size = sizeof(ud_record_error),.abi_version = UD_ABI_VERSION,
                    .input = input,.row = row,.rule = {"sql.command",11},.rule_version = 1,.code = written,.policy = policy,.provider_code = outcome.provider_code});
            if (!status) ud_task_rows(b->context.activation,0,0,1);
        }
        ud_command_result_clear(&outcome);
        if (!status) b->number++;
    }
    if (!status) status = ud_batch_seal(accepted);
    if (!status) status = ud_batch_seal(errors);
    if (!status && accepted->rows) status = b->context.emit(b->context.emit_user,0,accepted);
    if (!status && errors->rows) status = b->context.emit(b->context.emit_user,1,errors);
    ud_batch_destroy(parameters); ud_batch_destroy(accepted); ud_batch_destroy(errors); return status;
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
