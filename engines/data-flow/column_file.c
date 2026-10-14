#include "builtins_internal.h"
enum { COLUMN_IMPORT = 1, COLUMN_EXPORT = 2 };
typedef struct column_files {
    ud_component_context context;
    ud_json *config;
    ud_batch *schema;
    ud_field *fields;
    uint32_t columns, path_column, value_column, maximum;
    int kind, policy, include, mode;
    ud_string connection;
} column_files;
static int enabled(const ud_json *value) {
    const char *text;
    uint32_t size;
    return value && !ud_json_text(value, &text, &size) && size == 4;
}
static ud_status find_column(const ud_batch *schema, ud_string name, uint32_t *out) {
    for (uint32_t at = 0; at < schema->columns; at++)
        if (schema->column[at].field.name_length == name.length &&
            !memcmp(schema->column[at].field.name, name.data, name.length)) {
            *out = at;
            return UD_OK;
        }
    return UD_SCHEMA;
}
static ud_status read_config(const ud_component_context *context, int kind, ud_string text,
                             const ud_batch *input, ud_json **out, ud_batch **schema,
                             uint32_t *path_column, uint32_t *value_column) {
    UD_TRY(ud_json_parse(context->memory, text.data, text.length, out, NULL));
    static const char *keys[] = {"connection", "pathColumn", "valueColumn", "type",
                                 "maxBytes",   "fileMode",   "errorPolicy", "includeInput"};
    UD_TRY(ud_json_keys(*out, keys, 8));
    ud_string connection, path, value, type = {"binary", 6};
    UD_TRY(ud_json_string(ud_member(*out, "connection"), &connection));
    UD_TRY(ud_builtin_connection_defined(context, connection));
    UD_TRY(ud_json_string(ud_member(*out, "pathColumn"), &path));
    UD_TRY(ud_json_string(ud_member(*out, "valueColumn"), &value));
    UD_TRY(find_column(input, path, path_column));
    if (input->column[*path_column].field.type != UD_UTF8)
        return UD_SCHEMA;
    uint32_t count = input->columns + (kind == COLUMN_IMPORT);
    ud_field *fields = ud_alloc(context->memory, count * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    for (uint32_t at = 0; at < input->columns; at++)
        fields[at] = input->column[at].field;
    ud_status status = UD_OK;
    if (kind == COLUMN_IMPORT) {
        if (ud_member(*out, "type"))
            status = ud_json_string(ud_member(*out, "type"), &type);
        ud_type native_type = ud_named(type, "binary") ? UD_BINARY
                              : ud_named(type, "utf8") ? UD_UTF8
                                                       : 0;
        if (!status &&
            (!native_type || !value.length || count > 65536 || ud_member(*out, "fileMode")))
            status = UD_SCHEMA;
        *value_column = input->columns;
        fields[input->columns] =
            (ud_field){.name = value.data, .name_length = value.length, .type = native_type};
        char name_space[33];
        if (!status)
            status = ud_pipeline_field_namespace(context, name_space);
        if (!status)
            status = ud_id_make(context->memory, (ud_string){name_space, 32}, value,
                                &fields[input->columns].identity);
    } else {
        status = find_column(input, value, value_column);
        if (!status && input->column[*value_column].field.type != UD_BINARY &&
            input->column[*value_column].field.type != UD_UTF8)
            status = UD_SCHEMA;
        if (!status && ud_member(*out, "type"))
            status = UD_SCHEMA;
    }
    int policy;
    if (!status)
        status = ud_error_policy(*out, "errorPolicy", UD_ERROR_FAIL, &policy);
    if (!status && policy == UD_ERROR_REDIRECT && !(context->connected_outputs & 2))
        status = UD_SCHEMA;
    if (!status)
        status = ud_batch_create(context->memory, fields, count, 0, schema);
    ud_free(fields);
    return status;
}
static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    ud_json *config = NULL;
    uint32_t path, value;
    ud_status status =
        read_config(context, (int)(intptr_t)user, text, inputs[0], &config, outputs, &path, &value);
    if (!status)
        status = ud_error_schema(&context->errors, inputs[0],
                                 enabled(ud_member(config, "includeInput")), outputs + 1);
    ud_json_destroy(config);
    return status;
}
static void dispose(void *pointer, ud_status outcome) {
    (void)outcome;
    column_files *component = pointer;
    if (!component)
        return;
    ud_free(component->fields);
    ud_batch_destroy(component->schema);
    ud_json_destroy(component->config);
    ud_free(component);
}
static ud_status prepare(void *user, const ud_component_context *context, ud_string text,
                         const ud_batch *const *inputs, const ud_batch *const *outputs,
                         void **out) {
    (void)outputs;
    *out = NULL;
    column_files *component = ud_alloc(context->memory, sizeof(*component));
    if (!component)
        return UD_RESOURCE;
    component->context = *context;
    component->kind = (int)(intptr_t)user;
    component->maximum = 16777216;
    ud_status status =
        read_config(context, component->kind, text, inputs[0], &component->config,
                    &component->schema, &component->path_column, &component->value_column);
    if (!status)
        status = ud_json_string(ud_member(component->config, "connection"), &component->connection);
    uint64_t maximum = component->maximum;
    if (!status && ud_member(component->config, "maxBytes"))
        status = ud_json_uint(ud_member(component->config, "maxBytes"), &maximum);
    if (!status && (!maximum || maximum > 67108864))
        status = UD_SCHEMA;
    component->maximum = (uint32_t)maximum;
    ud_string mode = {"create", 6};
    if (!status && ud_member(component->config, "fileMode"))
        status = ud_json_string(ud_member(component->config, "fileMode"), &mode);
    component->mode = ud_named(mode, "create")      ? UD_FILE_CREATE
                      : ud_named(mode, "overwrite") ? UD_FILE_OVERWRITE
                                                    : -1;
    if (!status && component->mode < 0)
        status = UD_SCHEMA;
    if (!status)
        status =
            ud_error_policy(component->config, "errorPolicy", UD_ERROR_FAIL, &component->policy);
    component->include = enabled(ud_member(component->config, "includeInput"));
    component->columns = component->schema ? component->schema->columns : 0;
    component->fields = ud_alloc(context->memory, component->columns * sizeof(*component->fields));
    if (!status && !component->fields)
        status = UD_RESOURCE;
    for (uint32_t at = 0; !status && at < component->columns; at++)
        status = ud_batch_field(component->schema, at, component->fields + at);
    if (status)
        dispose(component, status);
    else
        *out = component;
    return status;
}
static ud_status import_value(column_files *component, ud_connection_lease *lease, const char *path,
                              ud_bytes *bytes, ud_value *value) {
    UD_TRY(ud_connection_lease_authorize(lease, UD_ACCESS_READ));
    UD_TRY(ud_path_no_symlinks(component->context.memory, path));
    int kind;
    UD_TRY(ud_path_kind(component->context.memory, path, &kind));
    if (kind != 1)
        return kind == 0 ? UD_QUALITY : UD_SCHEMA;
    UD_TRY(ud_checkpoint_source(component->context.activation, path));
    FILE *file = NULL;
    UD_TRY(ud_open_regular(component->context.memory, path, &file));
    ud_status status = UD_OK;
    uint8_t buffer[16384];
    while (!status) {
        status = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
        if (status)
            break;
        size_t length = fread(buffer, 1, sizeof(buffer), file);
        if (length > component->maximum - bytes->size) {
            status = UD_TRUNCATED;
            break;
        }
        status = ud_bytes_put(bytes, buffer, length);
        if (length < sizeof(buffer)) {
            if (ferror(file))
                status = UD_IO;
            break;
        }
    }
    if (fclose(file) && !status)
        status = UD_IO;
    if (!status && value->type == UD_UTF8 && !ud_utf8_valid((const char *)bytes->data, bytes->size))
        status = UD_PARSE;
    if (!status) {
        value->text = (const char *)bytes->data;
        value->text_length = (uint32_t)bytes->size;
    }
    return status;
}
static ud_status export_value(column_files *component, ud_connection_lease *lease, const char *path,
                              const ud_batch *input, uint32_t row, const ud_value *value) {
    if (value->is_null || value->is_absent)
        return UD_QUALITY;
    if (value->text_length > component->maximum)
        return UD_TRUNCATED;
    ud_row_info identity;
    UD_TRY(ud_batch_row_info(input, row, &identity));
    if (!identity.id.high && !identity.id.low)
        return UD_SCHEMA;
    char row_id[33];
    ud_id_format(identity.id, row_id);
    ud_bytes name = {.ctx = component->context.memory, .limit = 65536};
    ud_status status = ud_bytes_put(&name, component->context.component_id.data,
                                    component->context.component_id.length);
    if (!status)
        status = ud_bytes_put(&name, ":", 1);
    if (!status)
        status = ud_bytes_put(&name, row_id, 32);
    ud_file_generation *generation = NULL;
    if (!status)
        status = ud_file_generation_open(component->context.activation, lease,
                                         (ud_string){(const char *)name.data, (uint32_t)name.size},
                                         path, component->mode, &generation);
    if (!status && !generation->prepared)
        status = ud_write_file(component->context.memory, generation->temporary, value->text,
                               value->text_length);
    if (!status)
        status = ud_file_generation_publish(generation);
    if (!status)
        status = ud_checkpoint_file_done(component->context.activation, path);
    ud_file_generation_destroy(generation);
    ud_bytes_clear(&name);
    return status;
}
static ud_status step(void *pointer, const ud_component_event *event) {
    column_files *component = pointer;
    if (event->kind != UD_COMPONENT_DATA)
        return event->kind == UD_COMPONENT_POLL ? UD_END : UD_OK;
    ud_batch *accepted = NULL, *errors = NULL;
    ud_status status = ud_batch_create(component->context.memory, component->fields,
                                       component->columns, event->batch->rows, &accepted);
    if (!status)
        status =
            ud_error_batch(&component->context.errors, component->include,
                            ud_batch_rows(event->batch), &errors);
    ud_value *values =
        !status ? ud_alloc(component->context.memory, component->columns * sizeof(*values)) : NULL;
    if (!status && !values)
        status = UD_RESOURCE;
    for (uint32_t row = 0; !status && row < event->batch->rows; row++) {
        memset(values, 0, component->columns * sizeof(*values));
        for (uint32_t column = 0; !status && column < event->batch->columns; column++)
            status = ud_batch_get(event->batch, row, column, values + column);
        if (status)
            break;
        ud_status record =
            values[component->path_column].is_null || values[component->path_column].is_absent
                ? UD_QUALITY
                : UD_OK;
        ud_connection_lease *lease = NULL;
        char *path = NULL;
        if (!record)
            record = ud_activation_acquire_file_resource(
                component->context.activation, component->connection,
                (ud_string){values[component->path_column].text,
                            values[component->path_column].text_length},
                component->kind == COLUMN_IMPORT ? UD_CONNECT_READ : UD_CONNECT_APPEND, &lease);
        if (!record) {
            ud_string resource;
            record = ud_connection_lease_path(lease, &resource);
            if (!record)
                path = ud_strdup_n(component->context.memory, resource.data, resource.length);
            if (!record && !path)
                record = UD_RESOURCE;
        }
        ud_bytes bytes = {.ctx = component->context.memory, .limit = component->maximum};
        if (!record && component->kind == COLUMN_IMPORT) {
            values[component->value_column].type = component->fields[component->value_column].type;
            record = import_value(component, lease, path, &bytes, values + component->value_column);
        } else if (!record)
            record = export_value(component, lease, path, event->batch, row,
                                  values + component->value_column);
        if (!record) {
            status = ud_batch_append(accepted, values, component->columns);
            ud_row_info identity;
            if (!status)
                status = ud_batch_row_info(event->batch, row, &identity);
            if (!status)
                status = ud_batch_set_row_info(accepted, accepted->rows - 1, &identity);
        } else if (component->policy == UD_ERROR_FAIL || !ud_record_error_recoverable(record))
            status = record;
        else if (component->policy == UD_ERROR_REDIRECT)
            status = ud_error_append(
                &component->context.errors, errors,
                &(ud_record_error){
                    .struct_size = sizeof(ud_record_error),
                    .abi_version = UD_ABI_VERSION,
                    .input = event->batch,
                    .row = row,
                    .rule = {component->kind == COLUMN_IMPORT ? "column.import" : "column.export",
                             13},
                    .rule_version = 1,
                    .column = component->fields[component->value_column].identity,
                    .code = record,
                    .policy = component->policy});
        ud_bytes_clear(&bytes);
        ud_free(path);
        ud_status closed = ud_connection_lease_release(lease);
        if (!status)
            status = closed;
    }
    if (!status && accepted->rows) {
        status = ud_batch_seal(accepted);
        if (!status)
            status = component->context.emit(component->context.emit_user, 0, accepted);
    }
    if (!status && errors->rows) {
        status = ud_batch_seal(errors);
        if (!status)
            status = component->context.emit(component->context.emit_user, 1, errors);
    }
    ud_free(values);
    ud_batch_destroy(accepted);
    ud_batch_destroy(errors);
    return status;
}
static ud_status finish(void *instance) {
    (void)instance;
    return UD_OK;
}
ud_status ud_column_files_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,"
        "\"required\":[\"connection\",\"pathColumn\",\"valueColumn\"],\"properties\":{"
        "\"connection\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},"
        "\"pathColumn\":{\"type\":\"string\",\"x-role\":\"input-field\"},"
        "\"valueColumn\":{\"type\":\"string\"},\"type\":{\"enum\":[\"binary\",\"utf8\"]},"
        "\"maxBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864},"
        "\"fileMode\":{\"enum\":[\"create\",\"overwrite\"]},"
        "\"errorPolicy\":{\"enum\":[\"fail\",\"redirect\",\"ignore\"]},\"includeInput\":{\"type\":"
        "\"boolean\"}}}";
    const char *ids[] = {"importColumn", "exportColumn"};
    for (uint32_t at = 0; at < 2; at++) {
        ud_component_descriptor descriptor = {
            .struct_size = sizeof(descriptor),
            .abi_version = UD_ABI_VERSION,
            .id = {ids[at], (uint32_t)strlen(ids[at])},
            .name = {ids[at], (uint32_t)strlen(ids[at])},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {schema, sizeof(schema) - 1},
            .category = UD_COMPONENT_TRANSFORM,
            .inputs = 1,
            .outputs = 2,
            .synchronous_input = -1,
            .error_output = 1,
            .optional_outputs = 2,
            .user = (void *)(intptr_t)(at + 1),
            .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE,
            .effects =
                at ? UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_COMMIT_UNKNOWN : UD_EFFECT_EXTERNAL_READ,
            .configure = configure,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .dispose = dispose};
        UD_TRY(ud_registry_register(registry, &descriptor));
    }
    return UD_OK;
}
