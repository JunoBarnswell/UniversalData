#include "../package/file_generation.h"
#include "../../connectors/xml.h"
#include <libxml/xmlschemas.h>
#include <libxml/xmlIO.h>
#include <libxslt/transform.h>
#include <libxslt/xsltutils.h>
#include <libxslt/security.h>
#include <math.h>
enum { XML_XPATH = 1, XML_XSLT = 2, XML_XSD = 3 };
typedef struct xml_task_config {
    ud_string source, auxiliary, target, expression, variable;
    uint32_t maximum_input, maximum_output;
    int mode;
} xml_task_config;
static ud_status read_config(const ud_component_context *context, int kind, ud_string text,
                             ud_json **out, xml_task_config *config) {
    *config = (xml_task_config){.maximum_input = 16777216, .maximum_output = 16777216};
    UD_TRY(ud_json_parse(context->memory, text.data, text.length, out, NULL));
    static const char *keys[] = {"source",        "auxiliary",      "target",
                                 "expression",    "resultVariable", "namespaces",
                                 "maxInputBytes", "maxOutputBytes", "fileMode"};
    UD_TRY(ud_json_keys(*out, keys, 9));
    UD_TRY(ud_json_string(ud_member(*out, "source"), &config->source));
    if (kind == XML_XPATH)
        UD_TRY(ud_json_string(ud_member(*out, "expression"), &config->expression));
    else
        UD_TRY(ud_json_string(ud_member(*out, "auxiliary"), &config->auxiliary));
    if (ud_member(*out, "target"))
        UD_TRY(ud_json_string(ud_member(*out, "target"), &config->target));
    if (ud_member(*out, "resultVariable"))
        UD_TRY(ud_json_string(ud_member(*out, "resultVariable"), &config->variable));
    if (kind == XML_XSD && config->target.length)
        return UD_SCHEMA;
    if (kind != XML_XSD && !config->target.length && !config->variable.length)
        return UD_SCHEMA;
    if ((kind == XML_XPATH && ud_member(*out, "auxiliary")) ||
        (kind != XML_XPATH && (ud_member(*out, "expression") || ud_member(*out, "namespaces"))))
        return UD_SCHEMA;
    if (config->variable.length) {
        ud_variable *variable;
        UD_TRY(ud_variable_writable(context->activation, config->variable, &variable));
        if (variable->value.type != (kind == XML_XSD ? UD_BOOL : UD_UTF8))
            return UD_SCHEMA;
    }
    const char *sizes[] = {"maxInputBytes", "maxOutputBytes"};
    uint32_t *destinations[] = {&config->maximum_input, &config->maximum_output};
    for (uint32_t at = 0; at < 2; at++) {
        if (!ud_member(*out, sizes[at]))
            continue;
        uint64_t value;
        UD_TRY(ud_json_uint(ud_member(*out, sizes[at]), &value));
        if (!value || value > 67108864)
            return UD_SCHEMA;
        *destinations[at] = (uint32_t)value;
    }
    ud_string mode = {"create", 6};
    if (ud_member(*out, "fileMode"))
        UD_TRY(ud_json_string(ud_member(*out, "fileMode"), &mode));
    config->mode = ud_named(mode, "create")      ? UD_FILE_CREATE
                   : ud_named(mode, "overwrite") ? UD_FILE_OVERWRITE
                                                 : -1;
    if (config->mode < 0 || (ud_member(*out, "fileMode") && !config->target.length))
        return UD_SCHEMA;
    ud_string ids[] = {config->source, config->auxiliary, config->target};
    for (uint32_t at = 0; at < 3; at++) {
        if (!ids[at].length)
            continue;
        const ud_json *definition;
        int owner;
        UD_TRY(ud_connection_definition(context->activation, ids[at], &definition, &owner));
        ud_string driver;
        const ud_json *value = ud_member(definition, "driver");
        if (ud_json_kind(value) == UD_JSON_STRING) {
            UD_TRY(ud_json_string(value, &driver));
            if (!ud_named(driver, "file"))
                return UD_UNSUPPORTED;
        }
    }
    return UD_OK;
}
static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    (void)inputs;
    (void)outputs;
    ud_json *document = NULL;
    xml_task_config config;
    ud_status status = read_config(context, (int)(intptr_t)user, text, &document, &config);
    ud_json_destroy(document);
    return status;
}
static int bounded_write(void *pointer, const char *bytes, int count) {
    ud_bytes *output = pointer;
    if (count < 0 || ud_is_cancelled(output->ctx) || ud_bytes_put(output, bytes, (size_t)count))
        return -1;
    return count;
}
static int bounded_close(void *pointer) {
    (void)pointer;
    return 0;
}
static ud_status xpath_result(const ud_component_context *context, xmlDocPtr document,
                              const ud_json *config, ud_string expression, ud_bytes *output) {
    xmlXPathContextPtr xpath = xmlXPathNewContext(document);
    if (!xpath)
        return UD_RESOURCE;
    xpath->node = xmlDocGetRootElement(document);
    ud_status status = ud_xml_namespaces(context->memory, xpath, ud_member(config, "namespaces"));
    xmlXPathObjectPtr result = NULL;
    if (!status)
        status = ud_xml_xpath(context->memory, xpath, expression, &result);
    if (!status && result->type == XPATH_NODESET) {
        status = ud_bytes_put(output, "[", 1);
        int count = result->nodesetval ? result->nodesetval->nodeNr : 0;
        for (int at = 0; !status && at < count; at++) {
            status = ud_activation_check(context->activation);
            xmlNodePtr node = result->nodesetval->nodeTab[at];
            if (node->type == XML_NAMESPACE_DECL) {
                status = UD_UNSUPPORTED;
                break;
            }
            xmlBufferPtr buffer = xmlBufferCreate();
            if (!buffer) {
                status = UD_RESOURCE;
                break;
            }
            if (xmlNodeDump(buffer, document, node, 0, 0) < 0)
                status = UD_IO;
            if (!status && at)
                status = ud_bytes_put(output, ",", 1);
            if (!status)
                status = ud_json_quote(output, (const char *)xmlBufferContent(buffer),
                                       (uint32_t)xmlBufferLength(buffer));
            xmlBufferFree(buffer);
        }
        if (!status)
            status = ud_bytes_put(output, "]", 1);
    } else if (!status && result->type == XPATH_BOOLEAN)
        status = ud_bytes_put(output, result->boolval ? "true" : "false", result->boolval ? 4 : 5);
    else if (!status && result->type == XPATH_NUMBER) {
        char number[64];
        int count = snprintf(number, sizeof(number), "%.17g", result->floatval);
        if (!isfinite(result->floatval))
            status = UD_QUALITY;
        else
            status = ud_bytes_put(output, number, (size_t)count);
    } else if (!status && result->type == XPATH_STRING)
        status = ud_json_quote(output, (const char *)result->stringval,
                               (uint32_t)xmlStrlen(result->stringval));
    else if (!status)
        status = UD_UNSUPPORTED;
    xmlXPathFreeObject(result);
    xmlXPathFreeContext(xpath);
    return status;
}
static ud_status reject_external_modules(xmlNodePtr node) {
    for (; node; node = node->next) {
        if (node->type == XML_ELEMENT_NODE && node->ns && node->ns->href &&
            ((!xmlStrcmp(node->ns->href, BAD_CAST "http://www.w3.org/1999/XSL/Transform") &&
              (!xmlStrcmp(node->name, BAD_CAST "import") ||
               !xmlStrcmp(node->name, BAD_CAST "include"))) ||
             (!xmlStrcmp(node->ns->href, BAD_CAST "http://www.w3.org/2001/XMLSchema") &&
              (!xmlStrcmp(node->name, BAD_CAST "import") ||
               !xmlStrcmp(node->name, BAD_CAST "include") ||
               !xmlStrcmp(node->name, BAD_CAST "redefine")))))
            return UD_AUTH;
        UD_TRY(reject_external_modules(node->children));
    }
    return UD_OK;
}
static void quiet_xml_error(void *pointer, const char *format, ...) {
    (void)pointer;
    (void)format;
    /* XML libraries may include record text/URIs in messages. Native returns
     * the typed status; source row error retention is configured separately. */
}
static ud_status transform(const ud_component_context *context, xmlDocPtr input,
                           xmlDocPtr *stylesheet_document, ud_bytes *output) {
    UD_TRY(reject_external_modules(xmlDocGetRootElement(*stylesheet_document)));
    xsltStylesheetPtr stylesheet = xsltParseStylesheetDoc(*stylesheet_document);
    if (!stylesheet)
        return UD_PARSE;
    *stylesheet_document = NULL; /* Ownership transfers only on successful parsing. */
    xsltTransformContextPtr transform_context = xsltNewTransformContext(stylesheet, input);
    xsltSecurityPrefsPtr security = xsltNewSecurityPrefs();
    ud_status status = !transform_context || !security ? UD_RESOURCE : UD_OK;
    const xsltSecurityOption operations[] = {XSLT_SECPREF_READ_FILE, XSLT_SECPREF_WRITE_FILE,
                                             XSLT_SECPREF_CREATE_DIRECTORY,
                                             XSLT_SECPREF_READ_NETWORK, XSLT_SECPREF_WRITE_NETWORK};
    for (uint32_t at = 0; !status && at < 5; at++)
        if (xsltSetSecurityPrefs(security, operations[at], xsltSecurityForbid))
            status = UD_RESOURCE;
    if (!status && xsltSetCtxtSecurityPrefs(security, transform_context))
        status = UD_RESOURCE;
    if (!status) {
        xsltSetTransformErrorFunc(transform_context, NULL, quiet_xml_error);
        transform_context->maxTemplateDepth = 128;
        transform_context->maxTemplateVars = 4096;
        transform_context->opLimit = 1000000;
        status = ud_activation_check(context->activation);
    }
    xmlDocPtr result =
        !status ? xsltApplyStylesheetUser(stylesheet, input, NULL, NULL, NULL, transform_context)
                : NULL;
    if (!status && !result)
        status = UD_PARSE;
    if (!status)
        status = ud_activation_check(context->activation);
    if (!status) {
        xmlOutputBufferPtr buffer =
            xmlOutputBufferCreateIO(bounded_write, bounded_close, output, NULL);
        if (!buffer)
            status = UD_RESOURCE;
        else {
            if (xsltSaveResultTo(buffer, result, stylesheet) < 0)
                status = ud_is_cancelled(context->memory) ? UD_CANCELLED : UD_RESOURCE;
            if (xmlOutputBufferClose(buffer) < 0 && !status)
                status = UD_RESOURCE;
        }
    }
    xmlFreeDoc(result);
    xsltFreeTransformContext(transform_context);
    xsltFreeSecurityPrefs(security);
    xsltFreeStylesheet(stylesheet);
    return status;
}
static ud_status validate(xmlDocPtr input, xmlDocPtr schema_document, int *valid) {
    UD_TRY(reject_external_modules(xmlDocGetRootElement(schema_document)));
    xmlSchemaParserCtxtPtr parser = xmlSchemaNewDocParserCtxt(schema_document);
    if (!parser)
        return UD_RESOURCE;
    xmlSchemaSetParserErrors(parser, quiet_xml_error, quiet_xml_error, NULL);
    xmlSchemaPtr schema = xmlSchemaParse(parser);
    xmlSchemaFreeParserCtxt(parser);
    if (!schema)
        return UD_PARSE;
    xmlSchemaValidCtxtPtr validator = xmlSchemaNewValidCtxt(schema);
    ud_status status = validator ? UD_OK : UD_RESOURCE;
    if (!status) {
        xmlSchemaSetValidErrors(validator, quiet_xml_error, quiet_xml_error, NULL);
        int result = xmlSchemaValidateDoc(validator, input);
        *valid = result == 0;
        status = result < 0 ? UD_IO : result ? UD_QUALITY : UD_OK;
    }
    xmlSchemaFreeValidCtxt(validator);
    xmlSchemaFree(schema);
    return status;
}
static ud_status invoke(void *user, const ud_component_context *context, ud_string text) {
    int kind = (int)(intptr_t)user;
    ud_json *document = NULL;
    xml_task_config config;
    ud_status status = read_config(context, kind, text, &document, &config);
    ud_connection_lease *source = NULL, *auxiliary = NULL, *target = NULL;
    xmlDocPtr input = NULL, reference = NULL;
    ud_memory_reservation *input_reservation = NULL, *reference_reservation = NULL;
    ud_file_generation *generation = NULL;
    if (!status)
        status = ud_acquire_file_connection(context->activation, config.source, UD_CONNECT_READ,
                                            &source);
    if (!status && config.auxiliary.length)
        status = ud_acquire_file_connection(context->activation, config.auxiliary, UD_CONNECT_READ,
                                            &auxiliary);
    if (!status && config.target.length)
        status = ud_acquire_file_connection(context->activation, config.target, UD_CONNECT_APPEND,
                                            &target);
    if (!status && target) {
        ud_string path;
        status = ud_connection_lease_path(target, &path);
        char *name = !status ? ud_strdup_n(context->memory, path.data, path.length) : NULL;
        if (!status && !name)
            status = UD_RESOURCE;
        if (!status)
            status = ud_file_generation_open(context->activation, target, context->component_id,
                                             name, config.mode, &generation);
        ud_free(name);
    }
    if (!status)
        status =
            ud_xml_read_document(context, source, config.maximum_input, &input, &input_reservation);
    if (!status && auxiliary)
        status = ud_xml_read_document(context, auxiliary, config.maximum_input, &reference,
                                      &reference_reservation);
    ud_bytes output = {.ctx = context->memory, .limit = config.maximum_output};
    int valid = 0;
    if (!status && kind == XML_XPATH)
        status = xpath_result(context, input, document, config.expression, &output);
    else if (!status && kind == XML_XSLT)
        status = transform(context, input, &reference, &output);
    else if (!status)
        status = validate(input, reference, &valid);
    if (!status)
        status = ud_activation_check(context->activation);
    if ((!status || (kind == XML_XSD && status == UD_QUALITY)) && config.variable.length) {
        ud_value value = kind == XML_XSD ? (ud_value){.type = UD_BOOL, .integer = valid}
                                         : (ud_value){.type = UD_UTF8,
                                                      .text = (const char *)output.data,
                                                      .text_length = (uint32_t)output.size};
        ud_status saved =
            ud_activation_set_variable(context->activation, config.variable, &value, NULL);
        if (!status)
            status = saved;
    }
    if (!status && generation) {
        if (!generation->prepared)
            status = ud_write_file(context->memory, generation->temporary, output.data, output.size,
                                   UD_FILE_DURABLE);
        if (!status)
            status = ud_file_generation_publish(generation);
        if (!status)
            status = ud_checkpoint_file_done(context->activation, generation->target);
    }
    ud_bytes_clear(&output);
    xmlFreeDoc(input);
    xmlFreeDoc(reference);
    ud_memory_release(input_reservation);
    ud_memory_release(reference_reservation);
    ud_file_generation_destroy(generation);
    ud_status closed = ud_connection_lease_release(target);
    if (!status)
        status = closed;
    closed = ud_connection_lease_release(auxiliary);
    if (!status)
        status = closed;
    closed = ud_connection_lease_release(source);
    if (!status)
        status = closed;
    ud_json_destroy(document);
    return status;
}
ud_status ud_xml_tasks_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"source\"],"
        "\"properties\":{\"source\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},"
        "\"auxiliary\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},"
        "\"target\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},"
        "\"expression\":{\"type\":\"string\"},\"resultVariable\":{\"type\":\"string\"},"
        "\"namespaces\":{\"type\":\"object\"},\"maxInputBytes\":{\"type\":\"integer\","
        "\"minimum\":1,\"maximum\":67108864},\"maxOutputBytes\":{\"type\":\"integer\","
        "\"minimum\":1,\"maximum\":67108864},\"fileMode\":{\"enum\":[\"create\",\"overwrite\"]}}}";
    const char *ids[] = {"xml.xpath", "xml.xslt", "xml.xsd"};
    for (uint32_t at = 0; at < 3; at++) {
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
            .user = (void *)(intptr_t)(at + 1),
            .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE,
            .effects = UD_EFFECT_EXTERNAL_READ |
                       (at < 2 ? UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_COMMIT_UNKNOWN : 0),
            .configure = configure,
            .invoke = invoke};
        UD_TRY(ud_registry_register(registry, &descriptor));
    }
    return UD_OK;
}
