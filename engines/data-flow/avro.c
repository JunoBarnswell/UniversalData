#include "builtins_internal.h"
#include "../package/file_generation.h"
#include "ud/avro.h"
typedef struct avro_component {
    ud_component_context context;
    ud_json *config;
    ud_batch *schema;
    ud_connection_lease *lease;
    ud_file_generation *generation;
    ud_avro_reader *reader;
    ud_avro_writer *writer;
    ud_record_options records;
    ud_avro_options options;
    int sink;
} avro_component;
static int flag(const ud_json *json) {
    const char *text;
    uint32_t length;
    return json && !ud_json_text(json, &text, &length) && length == 4 && !memcmp(text, "true", 4);
}
static ud_status parse(const ud_component_context *context, int sink, ud_string text,
                       const ud_batch *input, ud_json **config, ud_batch **schema) {
    UD_TRY(ud_builtin_parse(context, text, config));
    static const char *keys[] = {"connection", "schema",   "codec",       "maxBlockBytes",
                                 "batchSize",  "fileMode", "errorPolicy", "includeInput"};
    UD_TRY(ud_json_keys(*config, keys, 8));
    ud_string connection;
    UD_TRY(ud_json_string(ud_member(*config, "connection"), &connection));
    UD_TRY(ud_builtin_connection_defined(context, connection));
    if (sink) {
        if (ud_member(*config, "schema") || ud_member(*config, "errorPolicy") ||
            ud_member(*config, "includeInput"))
            return UD_SCHEMA;
        return ud_builtin_clone_schema(context->memory, input, schema);
    }
    int policy;
    UD_TRY(ud_error_policy(*config, "errorPolicy", UD_ERROR_FAIL, &policy));
    if (policy == UD_ERROR_REDIRECT && !(context->connected_outputs & 2))
        return UD_SCHEMA;
    if (ud_member(*config, "fileMode"))
        return UD_SCHEMA;
    return ud_builtin_schema_config(context, *config, schema);
}
static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    int sink = (int)(intptr_t)user;
    ud_json *config = NULL;
    ud_batch *schema = NULL;
    ud_status status = parse(context, sink, text, sink ? inputs[0] : NULL, &config, &schema);
    if (!status && !sink) {
        outputs[0] = schema;
        schema = NULL;
        status = ud_error_schema(&context->errors, NULL, flag(ud_member(config, "includeInput")),
                                 outputs + 1);
    }
    ud_batch_destroy(schema);
    ud_json_destroy(config);
    return status;
}
static ud_status check(void *user) {
    avro_component *component = user;
    return ud_connection_lease_authorize(component->lease,
                                     component->sink ? UD_ACCESS_WRITE : UD_ACCESS_READ);
}
static void dispose(void *instance, ud_status result) {
    (void)result;
    avro_component *component = instance;
    if (!component)
        return;
    ud_avro_reader_close(component->reader);
    (void)ud_avro_writer_close(component->writer);
    ud_file_generation_destroy(component->generation);
    (void)ud_connection_lease_release(component->lease);
    ud_batch_destroy(component->schema);
    ud_json_destroy(component->config);
    ud_free(component);
}
static ud_status copy_original(avro_component *component) {
    UD_TRY(ud_file_generation_check_original(component->generation));
    ud_avro_options options = component->options;
    options.records = NULL;
    ud_avro_reader *reader = NULL;
    UD_TRY(ud_avro_reader_open(component->context.memory, component->generation->original,
                               component->schema, &options, &reader));
    ud_status status = UD_OK;
    while (!status) {
        status = ud_file_generation_check_original(component->generation);
        ud_batch *batch = NULL, *errors = NULL;
        if (!status)
            status = ud_avro_reader_next(reader, &batch, &errors);
        if (status == UD_END) {
            status = UD_OK;
            ud_batch_destroy(batch);
            ud_batch_destroy(errors);
            break;
        }
        if (!status && batch)
            status = ud_avro_writer_append(component->writer, batch);
        if (!status && errors)
            status = UD_CORRUPT;
        ud_batch_destroy(batch);
        ud_batch_destroy(errors);
    }
    ud_avro_reader_close(reader);
    return status;
}
static ud_status prepare(void *user, const ud_component_context *context, ud_string text,
                         const ud_batch *const *inputs, const ud_batch *const *outputs,
                         void **out) {
    (void)outputs;
    *out = NULL;
    int sink = (int)(intptr_t)user;
    avro_component *component = ud_alloc(context->memory, sizeof(*component));
    if (!component)
        return UD_RESOURCE;
    component->context = *context;
    component->sink = sink;
    ud_status status =
        parse(context, sink, text, sink ? inputs[0] : NULL, &component->config, &component->schema);
    uint64_t batch = context->max_batch_rows, maximum = 16777216;
    if (!status && ud_member(component->config, "batchSize"))
        status = ud_json_uint(ud_member(component->config, "batchSize"), &batch);
    if (!status && ud_member(component->config, "maxBlockBytes"))
        status = ud_json_uint(ud_member(component->config, "maxBlockBytes"), &maximum);
    if (!status && (!batch || batch > context->max_batch_rows || !maximum || maximum > 67108864))
        status = UD_SCHEMA;
    component->options = (ud_avro_options){.struct_size = sizeof(ud_avro_options),
                                           .abi_version = UD_ABI_VERSION,
                                           .batch_rows = (uint32_t)batch,
                                           .max_block_bytes = (uint32_t)maximum,
                                           .check = check,
                                           .user = component};
    ud_string codec = {"null", 4};
    if (!status && ud_member(component->config, "codec"))
        status = ud_json_string(ud_member(component->config, "codec"), &codec);
    if (!status && !ud_named(codec, "null") && !ud_named(codec, "deflate"))
        status = UD_SCHEMA;
    component->options.codec = ud_named(codec, "deflate") ? UD_AVRO_DEFLATE : UD_AVRO_NULL;
    ud_string connection = {0}, path = {0};
    char *name = NULL;
    if (!status)
        status = ud_json_string(ud_member(component->config, "connection"), &connection);
    if (!status)
        status = ud_acquire_file_connection(context->activation, connection,
                                            sink ? UD_CONNECT_APPEND : UD_CONNECT_READ,
                                            &component->lease);
    if (!status)
        status = ud_connection_lease_path(component->lease, &path);
    if (!status) {
        name = ud_strdup_n(context->memory, path.data, path.length);
        if (!name)
            status = UD_RESOURCE;
    }
    if (!status && sink) {
        ud_string mode = {"create", 6};
        if (ud_member(component->config, "fileMode"))
            status = ud_json_string(ud_member(component->config, "fileMode"), &mode);
        int selected = ud_named(mode, "create")      ? UD_FILE_CREATE
                       : ud_named(mode, "overwrite") ? UD_FILE_OVERWRITE
                       : ud_named(mode, "append")    ? UD_FILE_APPEND
                                                     : -1;
        if (!status && selected < 0)
            status = UD_SCHEMA;
        if (!status)
            status = ud_file_generation_open(context->activation, component->lease,
                                             context->component_id, name, selected,
                                             &component->generation);
        if (!status && !component->generation->prepared)
            status =
                ud_avro_writer_open(context->memory, component->generation->temporary,
                                    component->schema, &component->options, &component->writer);
        if (!status && !component->generation->prepared && selected == UD_FILE_APPEND &&
            component->generation->base_exists)
            status = copy_original(component);
    } else if (!status) {
        status = ud_checkpoint_source(context->activation, name);
        component->records = (ud_record_options){
            .struct_size = sizeof(ud_record_options),
            .abi_version = UD_ABI_VERSION,
            .errors = context->errors,
            .include_input = flag(ud_member(component->config, "includeInput"))};
        if (!status)
            status = ud_error_policy(component->config, "errorPolicy", UD_ERROR_FAIL,
                                     &component->records.policy);
        uint8_t hash[32];
        char hex[65];
        if (!status)
            status = ud_file_hash(context->memory, name, hash);
        if (!status) {
            for (uint32_t at = 0; at < 32; at++) {
                hex[at * 2] = "0123456789abcdef"[hash[at] >> 4];
                hex[at * 2 + 1] = "0123456789abcdef"[hash[at] & 15];
            }
            hex[64] = 0;
            status = ud_id_make(context->memory, (ud_string){hex, 64}, path,
                                &component->records.source_scope);
        }
        component->options.records = &component->records;
        if (!status)
            status = ud_avro_reader_open(context->memory, name, component->schema,
                                         &component->options, &component->reader);
    }
    ud_free(name);
    if (status)
        dispose(component, status);
    else
        *out = component;
    return status ? ud_fail(context->memory, status, "Avro component preparation failed") : UD_OK;
}
static ud_status step(void *instance, const ud_component_event *event) {
    avro_component *component = instance;
    UD_TRY(check(component));
    if (component->sink) {
        if (!event)
            return UD_INVALID;
        if (event->kind != UD_COMPONENT_DATA)
            return event->kind == UD_COMPONENT_POLL ? UD_END : UD_OK;
        if (component->generation->prepared)
            return UD_OK;
        UD_TRY(ud_avro_writer_append(component->writer, event->batch));
        ud_task_rows(component->context.activation, 0, ud_batch_rows(event->batch), 0);
        return UD_OK;
    }
    ud_batch *batch = NULL, *errors = NULL;
    ud_status status = ud_avro_reader_next(component->reader, &batch, &errors);
    if (status && status != UD_END)
        (void)ud_fail(component->context.memory, status, "Avro record decoding failed");
    if (!status && batch)
        status = component->context.emit(component->context.emit_user, 0, batch);
    if (!status && errors)
        status = component->context.emit(component->context.emit_user, 1, errors);
    ud_batch_destroy(batch);
    ud_batch_destroy(errors);
    return status ? status : UD_MORE;
}
static ud_status finish(void *instance) {
    avro_component *component = instance;
    if (!component->sink)
        return UD_OK;
    ud_status status = ud_avro_writer_close(component->writer);
    component->writer = NULL;
    if (!status)
        status = ud_file_generation_publish(component->generation);
    if (!status)
        status =
            ud_checkpoint_file_done(component->context.activation, component->generation->target);
    return status;
}
ud_status ud_avro_register(ud_registry *registry) {
    static const char source_schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"connection\","
        "\"schema\"],\"properties\":{"
        "\"connection\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},\"schema\":{"
        "\"type\":\"object\"},"
        "\"batchSize\":{\"type\":\"integer\",\"minimum\":1},\"maxBlockBytes\":{\"type\":"
        "\"integer\",\"minimum\":1,\"maximum\":67108864},"
        "\"errorPolicy\":{\"enum\":[\"fail\",\"redirect\",\"ignore\"]},\"includeInput\":{\"type\":"
        "\"boolean\"},\"codec\":{\"enum\":[\"null\",\"deflate\"]}}}";
    static const char sink_schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"connection\"],"
        "\"properties\":{"
        "\"connection\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},\"fileMode\":{"
        "\"enum\":[\"create\",\"overwrite\",\"append\"]},"
        "\"batchSize\":{\"type\":\"integer\",\"minimum\":1},\"maxBlockBytes\":{\"type\":"
        "\"integer\",\"minimum\":1,\"maximum\":67108864},"
        "\"codec\":{\"enum\":[\"null\",\"deflate\"]}}}";
    for (int sink = 0; sink < 2; sink++) {
        const char *id = sink ? "file.avro.sink" : "file.avro.source",
                   *name = sink ? "Avro Destination" : "Avro Source",
                   *schema = sink ? sink_schema : source_schema;
        ud_component_descriptor descriptor = {
            .struct_size = sizeof(descriptor),
            .abi_version = UD_ABI_VERSION,
            .id = {id, (uint32_t)strlen(id)},
            .name = {name, (uint32_t)strlen(name)},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {schema, (uint32_t)strlen(schema)},
            .category = sink ? UD_COMPONENT_DESTINATION : UD_COMPONENT_SOURCE,
            .inputs = sink ? 1 : 0,
            .outputs = sink ? 0 : 2,
            .synchronous_input = -1,
            .error_output = sink ? -1 : 1,
            .optional_outputs = sink ? 0 : 2,
            .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE,
            .effects = sink ? UD_EFFECT_EXTERNAL_WRITE : UD_EFFECT_EXTERNAL_READ,
            .configure = configure,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .dispose = dispose,
            .user = (void *)(intptr_t)sink};
        UD_TRY(ud_registry_register(registry, &descriptor));
    }
    return UD_OK;
}
