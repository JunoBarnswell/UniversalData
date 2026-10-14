#include "../package/internal.h"
#include "ud/connection_manager.h"
#include "ud/http.h"
typedef struct http_config {
    ud_string connection, method, body, content_type, ca, status_variable;
    ud_string body_variable, headers_variable;
    uint64_t timeout, maximum, attempts, delay, expected;
} http_config;
static int body_method(ud_string method) {
    return ud_named(method, "POST") || ud_named(method, "PUT") || ud_named(method, "PATCH");
}
static int clean_text(ud_string text) {
    for (uint32_t i = 0; i < text.length; i++)
        if ((unsigned char)text.data[i] < 32 || (unsigned char)text.data[i] == 127)
            return 0;
    return 1;
}
static ud_status read_config(const ud_component_context *c, ud_string text, ud_json **out,
                             http_config *p) {
    *out = NULL;
    *p = (http_config){
        .method = {"GET", 3}, .timeout = 60000, .maximum = 1048576, .attempts = 1, .delay = 100};
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, out, NULL));
    static const char *keys[] = {
        "connection",     "method",           "body",           "contentType",  "caConnection",
        "timeoutMs",      "maxResponseBytes", "maxAttempts",    "retryDelayMs", "expectedStatus",
        "statusVariable", "bodyVariable",     "headersVariable"};
    ud_status s = ud_json_keys(*out, keys, 13);
    if (!s)
        s = ud_json_string(ud_member(*out, "connection"), &p->connection);
    const char *names[] = {"method", "contentType", "caConnection", "statusVariable"};
    ud_string *strings[] = {&p->method, &p->content_type, &p->ca, &p->status_variable};
    for (int i = 0; !s && i < 4; i++)
        if (ud_member(*out, names[i]))
            s = ud_json_string(ud_member(*out, names[i]), strings[i]);
    int readonly = ud_named(p->method, "GET") || ud_named(p->method, "HEAD");
    if (!s && !readonly && !body_method(p->method) && !ud_named(p->method, "DELETE"))
        s = UD_UNSUPPORTED;
    const ud_json *body = ud_member(*out, "body");
    if (!s && body) {
        if (!body_method(p->method) || ud_json_kind(body) != UD_JSON_STRING)
            s = UD_SCHEMA;
        else
            s = ud_json_text(body, &p->body.data, &p->body.length);
        if (!s && p->body.length > 1048576)
            s = UD_RESOURCE;
    }
    if (!s && !clean_text(p->content_type))
        s = UD_SCHEMA;
    const char *numbers[] = {"timeoutMs", "maxResponseBytes", "maxAttempts", "retryDelayMs",
                             "expectedStatus"};
    uint64_t *values[] = {&p->timeout, &p->maximum, &p->attempts, &p->delay, &p->expected};
    for (int i = 0; !s && i < 5; i++)
        if (ud_member(*out, numbers[i]))
            s = ud_json_uint(ud_member(*out, numbers[i]), values[i]);
    if (!s && (!p->timeout || p->timeout > 86400000 || !p->maximum || p->maximum > 67108864 ||
               !p->attempts || p->attempts > 3 || p->delay > 60000 ||
               (p->expected && (p->expected < 100 || p->expected > 599)) ||
               (ud_member(*out, "expectedStatus") && !p->expected)))
        s = UD_SCHEMA;
    if (!s && !readonly && p->attempts != 1)
        s = UD_SCHEMA;
    if (!s && p->status_variable.length) {
        ud_variable *v = NULL;
        s = ud_variable_writable(c->activation, p->status_variable, &v);
        if (!s && v->value.type != UD_INT64)
            s = UD_SCHEMA;
    }
    const char *response_names[] = {"bodyVariable", "headersVariable"};
    ud_string *response_variables[] = {&p->body_variable, &p->headers_variable};
    for (uint32_t index = 0; !s && index < 2; index++) {
        if (!ud_member(*out, response_names[index]))
            continue;
        s = ud_json_string(ud_member(*out, response_names[index]), response_variables[index]);
        ud_variable *variable = NULL;
        if (!s)
            s = ud_variable_writable(c->activation, *response_variables[index], &variable);
        if (!s && variable->value.type != UD_UTF8 && (index || variable->value.type != UD_BINARY))
            s = UD_SCHEMA;
    }
    ud_string ids[] = {p->connection, p->ca};
    for (int i = 0; !s && i < 2; i++)
        if (ids[i].length) {
            const ud_json *def = NULL;
            int owner;
            s = ud_connection_definition(c->activation, ids[i], &def, &owner);
            const ud_json *driver = ud_member(def, "driver");
            ud_string value;
            if (!s && ud_json_kind(driver) == UD_JSON_STRING) {
                s = ud_json_string(driver, &value);
                if (!s && !ud_named(value, i ? "file" : "http"))
                    s = UD_UNSUPPORTED;
            }
        }
#ifndef UD_WITH_HTTP
    if (!s)
        s = UD_UNSUPPORTED;
#endif
    return s;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    (void)user;
    (void)in;
    (void)out;
    http_config p;
    ud_json *doc = NULL;
    ud_status s = read_config(c, text, &doc, &p);
    ud_json_destroy(doc);
    return s;
}
static ud_status invoke(void *user, const ud_component_context *c, ud_string text) {
    (void)user;
    ud_json *doc = NULL;
    http_config p;
    ud_status s = read_config(c, text, &doc, &p);
    ud_connection_lease *connection = NULL;
    if (!s)
        s = ud_acquire_http_connection(c->activation, p.connection, &connection);
    ud_http_options options = {.struct_size = sizeof(options),
                               .abi_version = UD_ABI_VERSION,
                               .method = p.method,
                               .body = p.body,
                               .content_type = p.content_type,
                               .ca = p.ca,
                               .timeout = p.timeout,
                               .maximum = p.maximum,
                               .attempts = p.attempts,
                               .delay = p.delay,
                               .expected = p.expected};
    ud_http_response response = {0};
    if (!s)
        s = ud_http_call(c, connection, &options, &response);
    if (response.status && p.status_variable.length && !ud_is_cancelled(c->memory)) {
        ud_value value = {.type = UD_INT64, .integer = response.status};
        ud_status saved =
            ud_activation_set_variable(c->activation, p.status_variable, &value, NULL);
        if (!s)
            s = saved;
    }
    if (s && !ud_named(p.method, "GET") && !ud_named(p.method, "HEAD") && response.attempted &&
        (!response.completed || response.status >= 500 || response.status == 408))
        s = UD_COMMIT_UNKNOWN;
    const ud_string response_variables[] = {p.body_variable, p.headers_variable};
    const ud_string response_values[] = {response.body, response.headers};
    for (uint32_t index = 0; response.completed && index < 2; index++) {
        if (!response_variables[index].length)
            continue;
        ud_variable *variable = NULL;
        ud_status saved = ud_variable_writable(c->activation, response_variables[index], &variable);
        ud_value value = {.type = variable ? variable->value.type : UD_UTF8,
                          .text = response_values[index].data,
                          .text_length = response_values[index].length};
        if (!saved)
            saved =
                ud_activation_set_variable(c->activation, response_variables[index], &value, NULL);
        if (!s)
            s = saved;
    }
    ud_http_response_clear(&response);
    ud_status closed = ud_connection_lease_release(connection);
    if (!s)
        s = closed;
    ud_json_destroy(doc);
    return s;
}
ud_status ud_http_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"connection\"],"
        "\"properties\":{\"connection\":{\"type\":\"string\"},\"method\":{\"enum\":[\"GET\","
        "\"HEAD\",\"POST\",\"PUT\",\"PATCH\",\"DELETE\"]},\"body\":{\"type\":\"string\"},"
        "\"contentType\":{\"type\":\"string\"},\"caConnection\":{\"type\":\"string\"},"
        "\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000},"
        "\"maxResponseBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864},"
        "\"maxAttempts\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":3},"
        "\"retryDelayMs\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":60000},"
        "\"expectedStatus\":{\"type\":\"integer\",\"minimum\":100,\"maximum\":599},"
        "\"statusVariable\":{\"type\":\"string\"},\"bodyVariable\":{\"type\":\"string\"},"
        "\"headersVariable\":{\"type\":\"string\"}}}";
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = {"http.request", 12},
                                 .name = {"HTTP Request", 12},
                                 .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
                                 .configuration_schema = {schema, sizeof(schema) - 1},
                                 .category = UD_COMPONENT_TASK,
                                 .synchronous_input = -1,
                                 .error_output = -1,
                                 .configure = configure,
                                 .invoke = invoke};
    return ud_registry_register(registry, &d);
}
