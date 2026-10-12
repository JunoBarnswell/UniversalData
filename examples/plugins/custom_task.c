#include "ud/plugin.h"
#include "ud/connection_manager.h"
#include "ud/json.h"
#include <stdio.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#endif
/* Only installed public SDK headers; no engine/private structures. */
static atomic_uint invoked, active, configured;
typedef struct task_config {
    ud_json *doc;
    ud_expression *expression;
    ud_string connection, variable;
    int result, marker;
    uint64_t hold;
} task_config;
static ud_status text(const ud_json *j, ud_string *out) {
    if (ud_json_kind(j) != UD_JSON_STRING)
        return UD_SCHEMA;
    ud_status s = ud_json_text(j, &out->data, &out->length);
    if (!s && (!out->length || memchr(out->data, 0, out->length)))
        s = UD_SCHEMA;
    return s;
}
static ud_status parse(const ud_component_context *c, ud_string config, task_config *p) {
    memset(p, 0, sizeof(*p));
    ud_status s = ud_json_parse(c->memory, config.data, config.length, &p->doc, NULL);
    if (s)
        return s;
    if (ud_json_kind(p->doc) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < ud_json_size(p->doc); i++) {
        const char *key;
        uint32_t n;
        ud_json_key(p->doc, i, &key, &n);
        const char *allowed[] = {"connection",   "variable", "expression",
                                 "returnStatus", "holdMs",   "appendMarker"};
        int found = 0;
        for (int k = 0; k < 6; k++)
            if (strlen(allowed[k]) == n && !memcmp(key, allowed[k], n))
                found = 1;
        if (!found)
            return UD_SCHEMA;
    }
    s = text(ud_json_find(p->doc, "connection", 10), &p->connection);
    if (!s)
        s = text(ud_json_find(p->doc, "variable", 8), &p->variable);
    ud_value v;
    if (!s)
        s = ud_execution_variable(c->execution, p->variable, &v);
    if (!s && v.type != UD_INT64)
        s = UD_SCHEMA;
    const ud_json *marker = ud_json_find(p->doc, "appendMarker", 12);
    if (!s && marker) {
        const char *value;
        uint32_t n;
        s = ud_json_kind(marker) == UD_JSON_BOOL ? ud_json_text(marker, &value, &n) : UD_SCHEMA;
        if (!s)
            p->marker = n == 4 && !memcmp(value, "true", 4);
    }
    const char *numbers[] = {"returnStatus", "holdMs"};
    for (int i = 0; !s && i < 2; i++) {
        const ud_json *j = ud_json_find(p->doc, numbers[i], (uint32_t)strlen(numbers[i]));
        if (!j)
            continue;
        const char *value;
        uint32_t n;
        s = ud_json_kind(j) == UD_JSON_NUMBER ? ud_json_text(j, &value, &n) : UD_SCHEMA;
        if (!s)
            s = ud_value_parse(c->memory, UD_INT64, value, n, &v);
        if (!s && (v.integer < 0 || v.integer > (i ? 5000 : 17)))
            s = UD_SCHEMA;
        if (!s) {
            if (i)
                p->hold = (uint64_t)v.integer;
            else
                p->result = (int)v.integer;
        }
    }
    char *json = NULL;
    uint32_t n;
    if (!s)
        s = ud_json_serialize(ud_json_find(p->doc, "expression", 10), &json, &n);
    ud_expression_options options = {sizeof(options), UD_ABI_VERSION, NULL, c->resolve,
                                     c->resolve_user};
    if (!s)
        s = ud_expression_create(c->memory, (ud_string){json, n}, &options, &p->expression);
    ud_free(json);
    if (!s && ud_expression_type(p->expression) != UD_INT64)
        s = UD_SCHEMA;
    return s;
}
static void clear(task_config *p) {
    ud_expression_destroy(p->expression);
    ud_json_destroy(p->doc);
}
static ud_status configure(void *user, const ud_component_context *c, ud_string config,
                           const ud_batch *const *in, ud_batch **out) {
    (void)user;
    (void)in;
    (void)out;
    atomic_fetch_add(&configured, 1);
    task_config p;
    ud_status s = parse(c, config, &p);
    clear(&p);
    return s;
}
static uint64_t now(void) {
    struct timespec t;
    timespec_get(&t, TIME_UTC);
    return (uint64_t)t.tv_sec * 1000 + (uint64_t)t.tv_nsec / 1000000;
}
static ud_status invoke(void *user, const ud_component_context *c, ud_string config) {
    (void)user;
    atomic_fetch_add(&invoked, 1);
    atomic_fetch_add(&active, 1);
    task_config p;
    ud_status s = parse(c, config, &p);
    ud_connection_lease *lease = NULL;
    if (!s)
        s = ud_execution_acquire_connection(c->execution, p.connection, 0, &lease);
    if (!s)
        s = ud_connection_lease_authorize(lease, UD_ACCESS_WRITE);
    uint64_t end = now() + p.hold;
    while (!s && now() < end) {
        s = ud_connection_lease_authorize(lease, UD_ACCESS_WRITE);
#ifdef _WIN32
        Sleep(1);
#else
        struct timespec tick = {0, 1000000};
        nanosleep(&tick, NULL);
#endif
    }
    ud_value value;
    if (!s)
        s = ud_expression_evaluate(p.expression, NULL, 0, &value);
    if (!s)
        s = ud_execution_set_variable(c->execution, p.variable, &value);
    if (!s && p.marker) {
        ud_string path;
        s = ud_connection_lease_path(lease, &path);
        char *file_name = !s ? ud_alloc(c->memory, path.length + 1) : NULL;
        if (!s && !file_name)
            s = UD_RESOURCE;
        if (!s) {
            memcpy(file_name, path.data, path.length);
            FILE *file = fopen(file_name, "ab");
            if (!file)
                s = UD_IO;
            else {
                if (fwrite("effect\n", 1, 7, file) != 7 || fflush(file))
                    s = UD_IO;
                if (fclose(file) && !s)
                    s = UD_IO;
            }
        }
        ud_free(file_name);
    }
    if (!s)
        s = p.result;
    ud_status closed = ud_connection_lease_release(lease);
    if (!s)
        s = closed;
    clear(&p);
    atomic_fetch_sub(&active, 1);
    return s;
}
static const char schema[] =
    "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"connection\",\"variable\","
    "\"expression\"],"
    "\"properties\":{\"connection\":{\"type\":\"string\"},\"variable\":{\"type\":\"string\"},"
    "\"expression\":{\"type\":\"object\"},\"returnStatus\":{\"type\":\"integer\",\"minimum\":0,"
    "\"maximum\":17},"
    "\"holdMs\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":5000},\"appendMarker\":{\"type\":"
    "\"boolean\"}}}";
static const ud_component_descriptor descriptors[] = {
    {.struct_size = sizeof(ud_component_descriptor),
     .abi_version = UD_ABI_VERSION,
     .id = {"sample.custom", 13},
     .name = {"Native Custom Task", 18},
     .version = {"1.0.0", 5},
     .configuration_schema = {schema, sizeof(schema) - 1},
     .category = UD_COMPONENT_TASK,
     .synchronous_input = -1,
     .error_output = -1,
     .configure = configure,
     .invoke = invoke},
    {.struct_size = sizeof(ud_component_descriptor),
#ifdef UD_BAD_COMPONENT
     .abi_version = 2,
#else
     .abi_version = UD_ABI_VERSION,
#endif
     .id = {"sample.second", 13},
     .name = {"Second Task", 11},
     .version = {"1.0.0", 5},
     .configuration_schema = {schema, sizeof(schema) - 1},
     .category = UD_COMPONENT_TASK,
     .synchronous_input = -1,
     .error_output = -1,
     .configure = configure,
     .invoke = invoke}};
static const ud_component_plugin_api components = {sizeof(components), UD_ABI_VERSION, 2,
                                                   descriptors};
typedef struct instance {
    ud_host_api host;
} instance;
static ud_status initialize(const ud_host_api *host, const char *config, uint32_t n, void **out) {
    (void)config;
    (void)n;
    instance *p = host->allocate(host->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *host;
    *out = p;
    return UD_OK;
}
static ud_status execute(void *state, int32_t operation, const ud_batch *input,
                         const char *argument, uint32_t n, ud_batch **out) {
    (void)input;
    (void)argument;
    (void)n;
    instance *p = state;
    *out = NULL;
    if (operation != UD_OP_SCALAR)
        return UD_UNSUPPORTED;
    ud_field fields[] = {{.name = "invoked", .name_length = 7, .type = UD_INT64},
                         {.name = "active", .name_length = 6, .type = UD_INT64},
                         {.name = "configured", .name_length = 10, .type = UD_INT64}};
    ud_status s = p->host.batch_create(p->host.context, fields, 3, 1, out);
    ud_value values[] = {{.type = UD_INT64, .integer = atomic_load(&invoked)},
                         {.type = UD_INT64, .integer = atomic_load(&active)},
                         {.type = UD_INT64, .integer = atomic_load(&configured)}};
    if (!s)
        s = p->host.append(*out, values, 3);
    return s;
}
static void destroy(void *state) {
    instance *p = state;
    if (p)
        p->host.deallocate(p);
}
static void shutdown(void) {}
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t abi, ud_plugin_api *out) {
    if (abi != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
#ifdef UD_COMPONENT_ONLY
    if (out->struct_size < sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){.struct_size = sizeof(*out),
                           .abi_version = UD_PLUGIN_ABI,
                           .id = "sample.component-only",
                           .version = "1.0.0",
                           .roles = UD_PLUGIN_COMPONENTS,
                           .components = &components};
    return UD_OK;
#else
    if (out->struct_size < sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){.struct_size = sizeof(*out),
                           .abi_version = UD_PLUGIN_ABI,
                           .id = "sample.custom",
                           .version = "1.0.0",
                           .roles = UD_PLUGIN_COMPONENTS | UD_PLUGIN_FUNCTION,
                           .capabilities = UD_CAP_SCALAR,
                           .initialize = initialize,
                           .execute = execute,
                           .destroy = destroy,
                           .shutdown = shutdown,
                           .components = &components};
    return UD_OK;
#endif
}
