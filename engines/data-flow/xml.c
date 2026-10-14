#include "builtins_internal.h"
#include "../../connectors/xml.h"
typedef struct xml_source {
    ud_component_context context;
    ud_json *config;
    ud_connection_lease *lease;
    ud_xml_stream *stream;
    ud_batch *schema;
    ud_field *fields;
    uint32_t columns, maximum;
    ud_id scope;
    int policy, include;
} xml_source;
static int enabled(const ud_json *value) {
    const char *text;
    uint32_t size;
    return value && !ud_json_text(value, &text, &size) && size == 4;
}
static ud_status read_config(const ud_component_context *context, ud_string text, ud_json **out,
                             ud_batch **schema) {
    UD_TRY(ud_json_parse(context->memory, text.data, text.length, out, NULL));
    static const char *keys[] = {"connection", "schema",         "recordPath",  "fields",
                                 "namespaces", "maxRecordBytes", "errorPolicy", "includeInput"};
    UD_TRY(ud_json_keys(*out, keys, 8));
    ud_string path;
    UD_TRY(ud_json_string(ud_member(*out, "recordPath"), &path));
    if (!path.length || path.length > 65536 || path.data[0] != '/' ||
        memchr(path.data, 0, path.length))
        return UD_SCHEMA;
    /* The streaming record selector is an absolute sequence of qualified
     * element names. Field selectors use actual XPath against each record. */
    for (uint32_t at = 0; at < path.length; at++)
        if (strchr("[]*()@ \t\r\n", path.data[at]) ||
            (path.data[at] == '/' && (at + 1 == path.length || path.data[at + 1] == '/')))
            return UD_SCHEMA;
    ud_string connection;
    UD_TRY(ud_json_string(ud_member(*out, "connection"), &connection));
    UD_TRY(ud_builtin_connection_defined(context, connection));
    UD_TRY(ud_builtin_schema_config(context, *out, schema));
    const ud_json *fields = ud_member(*out, "fields");
    if (ud_json_kind(fields) != UD_JSON_OBJECT || ud_json_size(fields) != ud_batch_columns(*schema))
        return UD_SCHEMA;
    for (uint32_t column = 0; column < ud_batch_columns(*schema); column++) {
        ud_field field;
        UD_TRY(ud_batch_field(*schema, column, &field));
        if (field.type >= UD_STRUCT || field.dictionary_kind)
            return UD_UNSUPPORTED;
        ud_string expression;
        UD_TRY(ud_json_string(ud_json_find(fields, field.name, field.name_length), &expression));
        if (!expression.length || expression.length > 16384 || expression.data[0] == '/' ||
            memchr(expression.data, 0, expression.length))
            return UD_SCHEMA;
    }
    int policy;
    UD_TRY(ud_error_policy(*out, "errorPolicy", UD_ERROR_FAIL, &policy));
    if (policy == UD_ERROR_REDIRECT && !(context->connected_outputs & UINT64_C(2)))
        return UD_SCHEMA;
    return UD_OK;
}
static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    (void)user;
    (void)inputs;
    ud_json *config = NULL;
    ud_status status = read_config(context, text, &config, outputs);
    if (!status)
        status = ud_error_schema(&context->errors, NULL, enabled(ud_member(config, "includeInput")),
                                 outputs + 1);
    ud_json_destroy(config);
    return status;
}
static void dispose(void *pointer, ud_status outcome) {
    (void)outcome;
    xml_source *source = pointer;
    if (!source)
        return;
    ud_xml_stream_close(source->stream);
    (void)ud_connection_lease_release(source->lease);
    ud_batch_destroy(source->schema);
    ud_json_destroy(source->config);
    ud_free(source->fields);
    ud_free(source);
}
static ud_status prepare(void *user, const ud_component_context *context, ud_string text,
                         const ud_batch *const *inputs, const ud_batch *const *outputs,
                         void **out) {
    (void)user;
    (void)inputs;
    (void)outputs;
    *out = NULL;
    xml_source *source = ud_alloc(context->memory, sizeof(*source));
    if (!source)
        return UD_RESOURCE;
    source->context = *context;
    source->maximum = 1048576;
    ud_status status = read_config(context, text, &source->config, &source->schema);
    uint64_t maximum = source->maximum;
    if (!status && ud_member(source->config, "maxRecordBytes"))
        status = ud_json_uint(ud_member(source->config, "maxRecordBytes"), &maximum);
    if (!status && (!maximum || maximum > 67108864))
        status = UD_SCHEMA;
    source->maximum = (uint32_t)maximum;
    ud_string connection, path, selector;
    if (!status)
        status = ud_json_string(ud_member(source->config, "connection"), &connection);
    if (!status)
        status = ud_acquire_file_connection(context->activation, connection, UD_CONNECT_READ,
                                            &source->lease);
    if (!status)
        status = ud_connection_lease_path(source->lease, &path);
    if (!status)
        status = ud_json_string(ud_member(source->config, "recordPath"), &selector);
    char *name = !status ? ud_strdup_n(context->memory, path.data, path.length) : NULL;
    if (!status && !name)
        status = UD_RESOURCE;
    if (!status)
        status = ud_checkpoint_source(context->activation, name);
    if (!status)
        status = ud_xml_stream_open(context, source->lease, name, selector, source->maximum,
                                    &source->stream);
    ud_bytes identity = {.ctx = context->memory};
    uint8_t file_hash[32], digest[32];
    if (!status)
        status = ud_file_hash(context->memory, name, file_hash);
    if (!status)
        status = ud_put_string(&identity, context->execution->options.tenant.data,
                               context->execution->options.tenant.length);
    if (!status)
        status = ud_put_string(&identity, path.data, path.length);
    if (!status)
        status = ud_put_string(&identity, selector.data, selector.length);
    if (!status)
        status = ud_bytes_put(&identity, file_hash, 32);
    if (!status) {
        ud_hash(identity.data, identity.size, digest);
        for (uint32_t at = 0; at < 8; at++) {
            source->scope.high = (source->scope.high << 8) | digest[at];
            source->scope.low = (source->scope.low << 8) | digest[at + 8];
        }
    }
    ud_bytes_clear(&identity);
    if (!status)
        status = ud_error_policy(source->config, "errorPolicy", UD_ERROR_FAIL, &source->policy);
    source->include = enabled(ud_member(source->config, "includeInput"));
    source->columns = ud_batch_columns(source->schema);
    source->fields = ud_alloc(context->memory, source->columns * sizeof(*source->fields));
    if (!status && !source->fields)
        status = UD_RESOURCE;
    for (uint32_t column = 0; !status && column < source->columns; column++)
        status = ud_batch_field(source->schema, column, source->fields + column);
    ud_free(name);
    if (status)
        dispose(source, status);
    else
        *out = source;
    return status;
}
static ud_status wrapper(xml_source *source, const ud_bytes *record, ud_bytes *bytes) {
    UD_TRY(ud_bytes_put(bytes, "<UDRecord", 9));
    ud_string declarations = ud_xml_stream_namespace_declarations(source->stream);
    UD_TRY(ud_bytes_put(bytes, declarations.data, declarations.length));
    UD_TRY(ud_bytes_put(bytes, ">", 1));
    UD_TRY(ud_bytes_put(bytes, record->data, record->size));
    return ud_bytes_put(bytes, "</UDRecord>", 11);
}
static ud_status convert(xml_source *source, const ud_bytes *record, ud_batch *accepted,
                         ud_batch *errors) {
    ud_context *memory = source->context.memory;
    ud_bytes bytes = {.ctx = memory, .limit = (size_t)source->maximum + 300000};
    ud_status status = wrapper(source, record, &bytes);
    xmlDocPtr document = NULL;
    xmlXPathContextPtr xpath = NULL;
    ud_memory_reservation *reservation = NULL;
    if (!status)
        status = ud_xml_document(&source->context, bytes.data, (uint32_t)bytes.size, &document,
                                 &reservation);
    if (!status) {
        xpath = xmlXPathNewContext(document);
        if (!xpath)
            status = UD_RESOURCE;
        else
            xpath->node = xmlFirstElementChild(xmlDocGetRootElement(document));
    }
    if (!status)
        status = ud_xml_namespaces(memory, xpath, ud_member(source->config, "namespaces"));
    ud_value *values = !status ? ud_alloc(memory, source->columns * sizeof(*values)) : NULL;
    xmlChar **contents = !status ? ud_alloc(memory, source->columns * sizeof(*contents)) : NULL;
    if (!status && (!values || !contents))
        status = UD_RESOURCE;
    ud_status conversion = UD_OK;
    uint32_t bad = UINT32_MAX;
    const ud_json *fields = ud_member(source->config, "fields");
    for (uint32_t column = 0; !status && !conversion && column < source->columns; column++) {
        ud_string expression;
        status = ud_json_string(
            ud_json_find(fields, source->fields[column].name, source->fields[column].name_length),
            &expression);
        xmlXPathObjectPtr result = NULL;
        if (!status)
            status = ud_xml_xpath(memory, xpath, expression, &result);
        values[column].type = source->fields[column].type;
        if (!status && result->type == XPATH_NODESET &&
            (!result->nodesetval || result->nodesetval->nodeNr == 0)) {
            values[column].is_null = 1;
            if (!source->fields[column].nullable)
                conversion = UD_QUALITY;
        } else if (!status && result->type == XPATH_NODESET && result->nodesetval->nodeNr != 1)
            conversion = UD_SCHEMA;
        else if (!status) {
            contents[column] = xmlXPathCastToString(result);
            if (!contents[column])
                status = UD_RESOURCE;
            else
                conversion = ud_value_parse_field(
                    memory, source->fields + column, (const char *)contents[column],
                    (uint32_t)xmlStrlen(contents[column]), UD_ROUND_EXACT, values + column);
        }
        xmlXPathFreeObject(result);
        if (conversion)
            bad = column;
    }
    ud_row_info identity;
    if (!status)
        status =
            ud_row_info_source(source->scope, ud_xml_stream_record(source->stream) - 1, &identity);
    if (!status && !conversion) {
        status = ud_batch_append(accepted, values, source->columns);
        if (!status)
            status = ud_batch_set_row_info(accepted, ud_batch_rows(accepted) - 1, &identity);
    } else if (!status &&
               (source->policy == UD_ERROR_FAIL || !ud_record_error_recoverable(conversion)))
        status = conversion;
    else if (!status && source->policy == UD_ERROR_REDIRECT)
        status = ud_error_append(&source->context.errors, errors,
                                 &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                                    .abi_version = UD_ABI_VERSION,
                                                    .source = identity,
                                                    .rule = {"xml.convert", 11},
                                                    .rule_version = 1,
                                                    .column = bad < source->columns
                                                                  ? source->fields[bad].identity
                                                                  : (ud_id){0},
                                                    .code = conversion,
                                                    .policy = source->policy,
                                                    .input_format = {"XML", 3},
                                                    .raw = record->data,
                                                    .raw_length = (uint32_t)record->size});
    if (contents)
        for (uint32_t column = 0; column < source->columns; column++)
            xmlFree(contents[column]);
    ud_free(contents);
    ud_free(values);
    xmlXPathFreeContext(xpath);
    xmlFreeDoc(document);
    ud_memory_release(reservation);
    ud_bytes_clear(&bytes);
    return status;
}
static ud_status step(void *pointer, const ud_component_event *event) {
    (void)event;
    xml_source *source = pointer;
    ud_batch *accepted = NULL, *errors = NULL;
    uint32_t capacity = source->context.recommended_batch_rows
                            ? source->context.recommended_batch_rows(source->context.emit_user)
                            : source->context.max_batch_rows;
    ud_status status = ud_batch_create(source->context.memory, source->fields, source->columns,
                                       capacity, &accepted);
    if (!status)
        status = ud_error_schema(&source->context.errors, NULL, source->include, &errors);
    ud_bytes record = {.ctx = source->context.memory, .limit = source->maximum};
    int end = 0;
    for (uint32_t row = 0; !status && row < capacity; row++) {
        status = ud_xml_stream_next(source->stream, &record);
        if (status == UD_END) {
            status = UD_OK;
            end = 1;
            break;
        }
        if (!status)
            status = convert(source, &record, accepted, errors);
    }
    if (!status && ud_batch_rows(accepted)) {
        status = ud_batch_seal(accepted);
        if (!status)
            status = source->context.emit(source->context.emit_user, 0, accepted);
    }
    if (!status && ud_batch_rows(errors)) {
        status = ud_batch_seal(errors);
        if (!status)
            status = source->context.emit(source->context.emit_user, 1, errors);
    }
    ud_batch_destroy(accepted);
    ud_batch_destroy(errors);
    ud_bytes_clear(&record);
    return status ? status : end ? UD_END : UD_MORE;
}
ud_status ud_xml_source_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,"
        "\"required\":[\"connection\",\"schema\",\"recordPath\",\"fields\"],\"properties\":{"
        "\"connection\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},"
        "\"schema\":{\"type\":\"object\"},\"recordPath\":{\"type\":\"string\"},"
        "\"fields\":{\"type\":\"object\"},\"namespaces\":{\"type\":\"object\"},"
        "\"maxRecordBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864},"
        "\"errorPolicy\":{\"enum\":[\"fail\",\"redirect\",\"ignore\"]},"
        "\"includeInput\":{\"type\":\"boolean\"}}}";
    ud_component_descriptor descriptor = {.struct_size = sizeof(descriptor),
                                          .abi_version = UD_ABI_VERSION,
                                          .id = {"file.xml.source", 15},
                                          .name = {"XML Source", 10},
                                          .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
                                          .configuration_schema = {schema, sizeof(schema) - 1},
                                          .category = UD_COMPONENT_SOURCE,
                                          .outputs = 2,
                                          .synchronous_input = -1,
                                          .error_output = 1,
                                          .optional_outputs = 2,
                                          .capabilities =
                                              UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE,
                                          .effects = UD_EFFECT_EXTERNAL_READ,
                                          .configure = configure,
                                          .prepare = prepare,
                                          .step = step,
                                          .dispose = dispose};
    return ud_registry_register(registry, &descriptor);
}
