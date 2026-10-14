#include "../package/internal.h"
#include "ud/connection_manager.h"
#include "ud/sql.h"
#include "ud/http.h"
#ifdef UD_WITH_XML
#include "../../connectors/xml.h"
#endif
enum { SERVER_BACKUP = 1, SERVER_CHECK, SERVER_REINDEX, AGENT_START, AGENT_STOP, SERVER_XMLA };
typedef struct server_config {
    ud_string connection, database, table, job, device, command, output, ca;
    uint64_t timeout, maximum;
} server_config;
static ud_status parse(const ud_component_context *context, int kind, ud_string text, ud_json **doc,
                       server_config *config) {
    *config = (server_config){.timeout = 60000, .maximum = 1048576};
    UD_TRY(ud_json_parse(context->memory, text.data, text.length, doc, NULL));
    static const char *keys[] = {
        "connection", "database",       "table",        "job",       "backupConnection",
        "command",    "resultVariable", "caConnection", "timeoutMs", "maxResponseBytes"};
    UD_TRY(ud_json_keys(*doc, keys, 10));
    UD_TRY(ud_json_string(ud_member(*doc, "connection"), &config->connection));
    const char *names[] = {"database", "table",          "job",         "backupConnection",
                           "command",  "resultVariable", "caConnection"};
    ud_string *values[] = {&config->database, &config->table,  &config->job, &config->device,
                           &config->command,  &config->output, &config->ca};
    for (uint32_t at = 0; at < 7; at++)
        if (ud_member(*doc, names[at]))
            UD_TRY(ud_json_string(ud_member(*doc, names[at]), values[at]));
    if (ud_member(*doc, "timeoutMs"))
        UD_TRY(ud_json_uint(ud_member(*doc, "timeoutMs"), &config->timeout));
    if (ud_member(*doc, "maxResponseBytes"))
        UD_TRY(ud_json_uint(ud_member(*doc, "maxResponseBytes"), &config->maximum));
    if (!config->timeout || config->timeout > 86400000 || !config->maximum ||
        config->maximum > 67108864)
        return UD_SCHEMA;
    const ud_json *definition;
    int owner;
    UD_TRY(ud_connection_definition(context->activation, config->connection, &definition, &owner));
    if (config->output.length) {
        ud_variable *variable;
        UD_TRY(ud_variable_writable(context->activation, config->output, &variable));
        if (variable->value.type != UD_UTF8)
            return UD_SCHEMA;
    }
    if (kind == SERVER_XMLA) {
        ud_string driver;
        UD_TRY(ud_json_string(ud_member(definition, "driver"), &driver));
        if (!ud_named(driver, "http") || !config->command.length ||
            config->command.length > 16777216 ||
            memchr(config->command.data, 0, config->command.length) || config->database.length ||
            config->table.length || config->job.length || config->device.length)
            return UD_SCHEMA;
#if !defined(UD_WITH_XML) || !defined(UD_WITH_HTTP)
        return UD_UNSUPPORTED;
#endif
    } else {
        if (config->command.length || config->ca.length || config->output.length)
            return UD_SCHEMA;
        if ((kind == SERVER_BACKUP || kind == SERVER_CHECK || kind == SERVER_REINDEX) &&
            (!config->database.length ||
             memchr(config->database.data, '.', config->database.length)))
            return UD_SCHEMA;
        if (kind == SERVER_REINDEX && !config->table.length)
            return UD_SCHEMA;
        if ((kind == AGENT_START || kind == AGENT_STOP) &&
            (!config->job.length || config->job.length > 128 || config->database.length ||
             config->table.length || config->device.length))
            return UD_SCHEMA;
        if (kind == SERVER_BACKUP) {
            if (!config->device.length)
                return UD_SCHEMA;
            UD_TRY(
                ud_connection_definition(context->activation, config->device, &definition, &owner));
            ud_string driver;
            UD_TRY(ud_json_string(ud_member(definition, "driver"), &driver));
            if (!ud_named(driver, "sqlserverFile"))
                return UD_UNSUPPORTED;
        } else if (config->device.length)
            return UD_SCHEMA;
        if (kind != SERVER_REINDEX && config->table.length)
            return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    (void)in;
    (void)out;
    server_config config;
    ud_json *doc = NULL;
    ud_status status = parse(context, (int)(intptr_t)user, text, &doc, &config);
    ud_json_destroy(doc);
    return status;
}
static ud_status parameters(const ud_component_context *context, ud_string value, ud_batch **out) {
    ud_field field = {.name = "argument", .name_length = 8, .type = UD_UTF8};
    UD_TRY(ud_batch_create(context->memory, &field, 1, 1, out));
    UD_TRY(ud_batch_append(
        *out, &(ud_value){.type = UD_UTF8, .text = value.data, .text_length = value.length}, 1));
    return ud_batch_seal(*out);
}
static ud_status maintenance(const ud_component_context *context, int kind,
                             const server_config *config) {
    if (ud_transaction_active(context->activation))
        return UD_UNSUPPORTED;
    ud_connection_lease *lease = NULL, *device = NULL;
    ud_batch *params = NULL;
    int attempted = 0;
    ud_status status = ud_activation_acquire_connection(context->activation, config->connection,
                                                        UD_CONNECT_EXECUTE_SQL, &lease);
    if (!status && ud_connection_lease_dialect(lease) != UD_SQL_SQLSERVER)
        status = UD_UNSUPPORTED;
    ud_string database = {0}, table = {0}, argument = {0};
    ud_bytes sql = {.ctx = context->memory, .limit = 65536};
    if (!status && config->database.length)
        status =
            ud_sql_table_identifier(context->memory, UD_SQL_SQLSERVER, config->database, &database);
    if (!status && config->table.length)
        status = ud_sql_table_identifier(context->memory, UD_SQL_SQLSERVER, config->table, &table);
    const char *prefix = kind == SERVER_BACKUP    ? "BACKUP DATABASE "
                         : kind == SERVER_CHECK   ? "DBCC CHECKDB ("
                         : kind == SERVER_REINDEX ? "USE "
                         : kind == AGENT_START    ? "EXEC msdb.dbo.sp_start_job @job_name=?"
                                                  : "EXEC msdb.dbo.sp_stop_job @job_name=?";
    if (!status)
        status = ud_bytes_put(&sql, prefix, strlen(prefix));
    if (!status && database.length)
        status = ud_bytes_put(&sql, database.data, database.length);
    if (!status && kind == SERVER_BACKUP) {
        status = ud_activation_acquire_grant(context->activation, config->device, &device);
        if (!status)
            status = ud_connection_lease_check(device, UD_ACCESS_WRITE);
        if (!status)
            status = ud_connection_lease_resource(device, &argument);
        if (!status && (!argument.length || memchr(argument.data, 0, argument.length)))
            status = UD_SCHEMA;
        if (!status)
            status = ud_bytes_put(&sql, " TO DISK=? WITH CHECKSUM, COPY_ONLY",
                                  sizeof(" TO DISK=? WITH CHECKSUM, COPY_ONLY") - 1);
    } else if (!status && kind == SERVER_CHECK)
        status = ud_bytes_put(&sql, ") WITH NO_INFOMSGS, ALL_ERRORMSGS",
                              sizeof(") WITH NO_INFOMSGS, ALL_ERRORMSGS") - 1);
    else if (!status && kind == SERVER_REINDEX) {
        status = ud_bytes_put(&sql, "; ALTER INDEX ALL ON ", sizeof("; ALTER INDEX ALL ON ") - 1);
        if (!status)
            status = ud_bytes_put(&sql, table.data, table.length);
        if (!status)
            status = ud_bytes_put(&sql, " REBUILD", 8);
    } else if (kind == AGENT_START || kind == AGENT_STOP)
        argument = config->job;
    if (!status && argument.length)
        status = parameters(context, argument, &params);
    if (!status) {
        attempted = 1;
        status = ud_connection_lease_execute_command(
            lease,
            &(ud_read_options){.struct_size = sizeof(ud_read_options),
                               .abi_version = UD_ABI_VERSION,
                               .sql = {(const char *)sql.data, (uint32_t)sql.size},
                               .parameters = params},
            NULL);
    }
    if (attempted && (status == UD_IO || status == UD_TRANSIENT || status == UD_TIMEOUT ||
                      status == UD_CANCELLED))
        status = UD_COMMIT_UNKNOWN;
    ud_free((void *)database.data);
    ud_free((void *)table.data);
    ud_bytes_clear(&sql);
    ud_batch_destroy(params);
    (void)ud_connection_lease_release(device);
    ud_status closed = ud_connection_lease_release(lease);
    return status ? status : closed;
}
#ifdef UD_WITH_XML
static ud_status xmla(const ud_component_context *context, const server_config *config) {
    if (ud_transaction_active(context->activation))
        return UD_UNSUPPORTED;
    xmlDocPtr request = NULL, response_doc = NULL;
    ud_memory_reservation *request_memory = NULL, *response_memory = NULL;
    ud_status status = ud_xml_document(context, (const uint8_t *)config->command.data,
                                       config->command.length, &request, &request_memory);
    xmlNodePtr root = request ? xmlDocGetRootElement(request) : NULL;
    if (!status && (!root || xmlStrcmp(root->name, BAD_CAST "Execute") || !root->ns ||
                    xmlStrcmp(root->ns->href, BAD_CAST "urn:schemas-microsoft-com:xml-analysis")))
        status = UD_SCHEMA;
    const char *before = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><soap:Envelope "
                         "xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body>";
    const char *after = "</soap:Body></soap:Envelope>";
    ud_bytes body = {.ctx = context->memory, .limit = 16778240};
    if (!status)
        status = ud_bytes_put(&body, before, strlen(before));
    /* Serialize only the checked document root, excluding XML declarations. */
    xmlBufferPtr buffer = !status ? xmlBufferCreate() : NULL;
    if (!status && !buffer)
        status = UD_RESOURCE;
    if (!status && xmlNodeDump(buffer, request, root, 0, 0) < 0)
        status = UD_IO;
    if (!status)
        status = ud_bytes_put(&body, xmlBufferContent(buffer), xmlBufferLength(buffer));
    if (!status)
        status = ud_bytes_put(&body, after, strlen(after));
    ud_connection_lease *lease = NULL;
    ud_http_response response = {0};
    if (!status)
        status = ud_acquire_http_connection(context->activation, config->connection, &lease);
    if (!status)
        status =
            ud_http_call(context, lease,
                         &(ud_http_options){.struct_size = sizeof(ud_http_options),
                                            .abi_version = UD_ABI_VERSION,
                                            .method = {"POST", 4},
                                            .content_type = {"text/xml; charset=utf-8", 22},
                                            .body = {(const char *)body.data, (uint32_t)body.size},
                                            .ca = config->ca,
                                            .timeout = config->timeout,
                                            .maximum = config->maximum,
                                            .attempts = 1},
                         &response);
    if (response.attempted &&
        (!response.completed || response.status >= 500 || response.status == 408))
        status = UD_COMMIT_UNKNOWN;
    if (!status && (response.status < 200 || response.status >= 300))
        status = UD_QUALITY;
    if (!status) {
        status = ud_xml_document(context, (const uint8_t *)response.body.data, response.body.length,
                                 &response_doc, &response_memory);
        if (status)
            status = UD_COMMIT_UNKNOWN;
    }
    xmlXPathContextPtr xpath = !status ? xmlXPathNewContext(response_doc) : NULL;
    xmlXPathObjectPtr fault = NULL;
    if (!status && !xpath)
        status = UD_RESOURCE;
    if (!status)
        status = ud_xml_xpath(
            context->memory, xpath,
            (ud_string){"//*[local-name()='Fault' or local-name()='Error']",
                        sizeof("//*[local-name()='Fault' or local-name()='Error']") - 1},
            &fault);
    if (!status && fault->nodesetval && fault->nodesetval->nodeNr)
        status = UD_QUALITY;
    if (!status && config->output.length)
        status = ud_activation_set_variable(context->activation, config->output,
                                            &(ud_value){.type = UD_UTF8,
                                                        .text = response.body.data,
                                                        .text_length = response.body.length},
                                            NULL);
    xmlXPathFreeObject(fault);
    xmlXPathFreeContext(xpath);
    xmlBufferFree(buffer);
    xmlFreeDoc(request);
    xmlFreeDoc(response_doc);
    ud_memory_release(request_memory);
    ud_memory_release(response_memory);
    ud_bytes_clear(&body);
    ud_http_response_clear(&response);
    (void)ud_connection_lease_release(lease);
    return status;
}
#endif
static ud_status invoke(void *user, const ud_component_context *context, ud_string text) {
    int kind = (int)(intptr_t)user;
    ud_json *doc = NULL;
    server_config config;
    ud_status status = parse(context, kind, text, &doc, &config);
    if (!status && kind != SERVER_XMLA)
        status = maintenance(context, kind, &config);
#ifdef UD_WITH_XML
    else if (!status)
        status = xmla(context, &config);
#endif
    ud_json_destroy(doc);
    return status;
}
ud_status ud_sqlserver_tasks_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"connection\"],"
        "\"properties\":{"
        "\"connection\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},\"database\":{"
        "\"type\":\"string\"},\"table\":{\"type\":\"string\"},"
        "\"job\":{\"type\":\"string\"},\"backupConnection\":{\"type\":\"string\",\"x-role\":"
        "\"resource-reference\"},\"command\":{\"type\":\"string\"},"
        "\"resultVariable\":{\"type\":\"string\"},\"caConnection\":{\"type\":\"string\"},"
        "\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000},"
        "\"maxResponseBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864}}}";
    const char *ids[] = {"sqlserver.backup",         "sqlserver.checkDatabase",
                         "sqlserver.rebuildIndexes", "sqlserver.agent.start",
                         "sqlserver.agent.stop",     "ssas.xmla"};
    for (uint32_t at = 0; at < 6; at++) {
        ud_component_descriptor descriptor = {
            .struct_size = sizeof(descriptor),
            .abi_version = UD_ABI_VERSION,
            .id = {ids[at], (uint32_t)strlen(ids[at])},
            .name = {ids[at], (uint32_t)strlen(ids[at])},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {schema, sizeof(schema) - 1},
            .category = UD_COMPONENT_TASK,
            .synchronous_input = -1,
            .error_output = -1,
            .capabilities = UD_COMPONENT_THREAD_SAFE,
            .effects = UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_COMMIT_UNKNOWN,
            .user = (void *)(intptr_t)(at + 1),
            .configure = configure,
            .invoke = invoke};
        UD_TRY(ud_registry_register(registry, &descriptor));
    }
    return UD_OK;
}
