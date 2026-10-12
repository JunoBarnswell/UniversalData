#include "check.h"
#include "ud/package.h"
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
typedef struct fixture {
    ud_execution *execution;
    ud_context *memory;
    int mode, child_calls, root_calls, child_terminal;
} fixture;
static ud_string str(const char *v) {
    return (ud_string){v, (uint32_t)strlen(v)};
}
static ud_status allow(void *u, ud_string t, ud_string r, int32_t a) {
    (void)u;
    (void)t;
    (void)r;
    (void)a;
    return UD_OK;
}
static int named(ud_string a, const char *b) {
    return a.length == strlen(b) && !memcmp(a.data, b, a.length);
}
static ud_status probe(void *user, const ud_component_context *c, ud_string config) {
    (void)config;
    fixture *f = user;
    if (c->execution != f->execution)
        return UD_CORRUPT;
    if (f->memory && f->memory != c->memory)
        return UD_CORRUPT;
    f->memory = c->memory;
    ud_value value;
    if (named(c->component_id, "unused"))
        return UD_CORRUPT;
    if (ud_activation_variable(c->activation, str("value"), &value) != UD_OK || value.integer != 7)
        return UD_CORRUPT;
    if (ud_activation_variable(c->activation, str("same"), &value) != UD_OK || value.integer != 999)
        return UD_CORRUPT;
    if (ud_activation_variable(c->activation, str("parentOnly"), &value) != UD_SCHEMA)
        return UD_CORRUPT;
    value = (ud_value){.type = UD_INT64, .integer = 100};
    if (ud_activation_set_variable(c->activation, str("value"), &value, NULL) != UD_AUTH)
        return UD_CORRUPT;
    f->child_calls++;
    return f->mode == 1 ? UD_IO : f->mode == 2 ? UD_COMMIT_UNKNOWN : UD_OK;
}
static ud_status root_probe(void *user, const ud_component_context *c, ud_string config) {
    (void)config;
    fixture *f = user;
    ud_value value;
    if (c->execution != f->execution || c->memory != f->memory)
        return UD_CORRUPT;
    if (ud_activation_variable(c->activation, str("same"), &value) != UD_OK || value.integer != 10)
        return UD_CORRUPT;
    if (ud_activation_variable(c->activation, str("value"), &value) != UD_SCHEMA)
        return UD_CORRUPT;
    f->root_calls++;
    return UD_OK;
}
static void observe(void *user, const ud_execution_event *event) {
    fixture *f = user;
    if (!f->execution)
        return;
    if (named(event->package_id, "child") && event->kind == UD_EXEC_EVENT_STATE &&
        ud_execution_terminal(event->state))
        f->child_terminal = event->state;
    if (f->mode == 3 && named(event->package_id, "child") && event->kind == UD_EXEC_EVENT_STATE &&
        event->state == UD_EXEC_RUNNING)
        ud_execution_cancel(f->execution);
    if (f->mode == 4 && event->kind == UD_EXEC_EVENT_AFTER_CHECKPOINT &&
        named(event->package_id, "child") && event->task_id.data &&
        strstr(event->task_id.data, "setChild"))
        _exit(91);
}
int main(int argc, char **argv) {
    CHECK(argc == 5);
    fixture f = {.mode = atoi(argv[2])};
    FILE *file = fopen(argv[1], "rb");
    CHECK(file && !fseek(file, 0, SEEK_END));
    long size = ftell(file);
    CHECK(size > 0 && !fseek(file, 0, SEEK_SET));
    char *text = malloc((size_t)size);
    CHECK(text && fread(text, 1, (size_t)size, file) == (size_t)size);
    fclose(file);
    ud_context *host = NULL;
    OK(ud_context_create(NULL, &host));
    ud_registry *registry = NULL;
    OK(ud_registry_create(host, &registry));
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = str("childProbe"),
                                 .name = str("childProbe"),
                                 .version = str("1"),
                                 .configuration_schema = str("{}"),
                                 .category = UD_COMPONENT_TASK,
                                 .synchronous_input = -1,
                                 .error_output = -1,
                                 .capabilities = UD_COMPONENT_REPLAY_SAFE,
                                 .user = &f,
                                 .invoke = probe};
    OK(ud_registry_register(registry, &d));
    d.id = d.name = str("rootProbe");
    d.invoke = root_probe;
    OK(ud_registry_register(registry, &d));
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .context = {.struct_size = sizeof(o.context),
                                          .abi_version = UD_ABI_VERSION,
                                          .memory_limit = 33554432},
                              .edge_capacity = 1,
                              .max_batch_rows = 3,
                              .max_depth = f.mode == 5 ? 1 : 32,
                              .tenant = str("tenant"),
                              .execution_id = str("parent-run"),
                              .registry = registry,
                              .authorize = allow,
                              .observe = observe,
                              .observe_user = &f,
                              .state_directory = str(argv[3]),
                              .resume = atoi(argv[4])};
    ud_execution *e = NULL;
    ud_status s = ud_execution_create_json((ud_string){text, (uint32_t)size}, &o, &e);
    free(text);
    if (f.mode == 5 || f.mode == 6 || f.mode == 8 || f.mode == 9) {
        CHECK(s == (f.mode == 5 ? UD_RESOURCE : f.mode==8?UD_PARSE:UD_SCHEMA) && !e);
    } else {
        OK(s);
        f.execution = e;
        ud_status result = ud_execution_execute(e);
        int expected = f.mode == 1   ? UD_IO
                       : f.mode == 2 ? UD_COMMIT_UNKNOWN
                       : f.mode == 3 ? UD_CANCELLED
                                     : UD_OK;
        CHECK(result == expected);
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        OK(ud_execution_get_metrics(e, &m));
        CHECK(m.result == expected && m.state == (f.mode == 1   ? UD_EXEC_FAILED
                                                  : f.mode == 2 ? UD_EXEC_RECOVERY_REQUIRED
                                                  : f.mode == 3 ? UD_EXEC_CANCELLED
                                                                : UD_EXEC_SUCCEEDED));
        ud_value value;
        OK(ud_execution_variable(e, str("same"), &value));
        CHECK(value.integer == 10);
        OK(ud_execution_variable(e, str("parentOnly"), &value));
        CHECK(value.integer == (f.mode == 1 ? 2 : 1));
        if (!expected) {
            CHECK(m.input_rows == 5 && m.output_rows == 5);
            if (!o.resume)
                CHECK(f.child_calls == (f.mode == 7 ? 2 : 1) && f.root_calls == 1 &&
                      f.child_terminal == UD_EXEC_SUCCEEDED);
        } else
            CHECK(!f.root_calls && f.child_terminal == m.state);
        OK(ud_execution_destroy(e));
    }
    OK(ud_registry_destroy(registry));
    CHECK(ud_memory_used(host) == 0);
    OK(ud_context_destroy(host));
    return 0;
}
