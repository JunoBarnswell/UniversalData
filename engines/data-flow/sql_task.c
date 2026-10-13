#include "builtins_internal.h"
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
