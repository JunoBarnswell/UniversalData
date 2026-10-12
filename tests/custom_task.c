#include "check.h"
#include "ud/plugin.h"
#include "ud/package.h"
#include <string.h>
#include <stdatomic.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
typedef struct fixture {
    ud_execution *execution;
    int mode;
    atomic_int entered;
} fixture;
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)tenant;
    (void)resource;
    return ((fixture *)user)->mode == 1 && action == UD_ACCESS_WRITE ? UD_AUTH : UD_OK;
}
static void observe(void *user, const ud_execution_event *event) {
    fixture *f = user;
    if(event->state == UD_EXEC_RUNNING && event->task_id.length) atomic_store(&f->entered,1);
    if (f->mode == 3 && f->execution && event->kind == UD_EXEC_EVENT_BEFORE_CHECKPOINT) {
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        if (!ud_execution_get_metrics(f->execution, &m) && m.completed_tasks == 1)
            _exit(91);
    }
}
int main(int argc, char **argv) {
    CHECK(argc == 9);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_registry *registry = NULL;
    OK(ud_registry_create(ctx, &registry));
    ud_plugin *plugin = NULL;
    OK(ud_plugin_load(ctx, argv[2], 0, &plugin));
    CHECK(ud_plugin_register_components(plugin, registry) == UD_INCOMPATIBLE);
    CHECK(ud_registry_count(registry) == 0);
    OK(ud_plugin_unload(plugin));
    OK(ud_plugin_load(ctx, argv[1], 0, &plugin));
    ud_plugin_api info = {.struct_size = sizeof(info), .abi_version = 2};
    CHECK(ud_plugin_info(plugin, &info) == UD_INCOMPATIBLE);
    info.abi_version = UD_PLUGIN_ABI;
    info.struct_size = sizeof(info) - sizeof(void *);
    CHECK(ud_plugin_info(plugin, &info) == UD_INCOMPATIBLE);
    info.struct_size = sizeof(info);
    OK(ud_plugin_info(plugin, &info));
    OK(ud_plugin_register_components(plugin, registry));
    CHECK(ud_registry_count(registry) == 2);
    CHECK(ud_plugin_register_components(plugin, registry) == UD_SCHEMA &&
          ud_registry_count(registry) == 2);
    CHECK(ud_plugin_unload(plugin) == UD_BUSY);
    ud_component_descriptor descriptor = {.struct_size = sizeof(descriptor),
                                          .abi_version = UD_ABI_VERSION};
    OK(ud_registry_find(registry, str("sample.custom"), &descriptor));
    CHECK(descriptor.category == UD_COMPONENT_TASK && descriptor.configure && descriptor.invoke);
    FILE *file = fopen(argv[3], "rb");
    CHECK(file && !fseek(file, 0, SEEK_END));
    long n = ftell(file);
    CHECK(n > 0);
    rewind(file);
    char *text = malloc((size_t)n);
    CHECK(text && fread(text, 1, (size_t)n, file) == (size_t)n);
    fclose(file);
    fixture f = {.mode = atoi(argv[4])};
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .context = {.struct_size = sizeof(o.context),
                                          .abi_version = UD_ABI_VERSION,
                                          .memory_limit = 33554432},
                              .edge_capacity = 1,
                              .max_batch_rows = 7,
                              .max_depth = 32,
                              .tenant = {"fixture", 7},
                              .execution_id = {"custom", 6},
                              .registry = registry,
                              .authorize = f.mode == 5 ? NULL : authorize,
                              .authorize_user = &f,
                              .observe = observe,
                              .observe_user = &f,
                              .timeout_ms = f.mode == 4 ? 100 : 0,
                              .state_directory = str(argv[7]),
                              .resume = atoi(argv[8])};
    ud_execution *e = NULL;
    ud_status s = ud_execution_create_json((ud_string){text, (uint32_t)n}, &o, &e);
    free(text);
    f.execution = e;
    if (!s) {
        CHECK(ud_registry_destroy(registry) == UD_BUSY);
        CHECK(ud_registry_register(registry, &descriptor) == UD_BUSY);
        CHECK(ud_plugin_register_components(plugin, registry) == UD_BUSY);
        CHECK(ud_plugin_unload(plugin) == UD_BUSY);
        if (f.mode == 2) {
            OK(ud_execution_start(e));
            int entered = 0;
            for (int i = 0; i < 500; i++) {
                if (atomic_load(&f.entered)) {
                    entered = 1;
                    break;
                }
                CHECK(ud_execution_wait(e, 10) == UD_BUSY);
            }
            CHECK(entered);
            ud_execution_cancel(e);
            do {
                s = ud_execution_wait(e, 100);
            } while (s == UD_BUSY);
        } else
            s = ud_execution_execute(e);
    }
    if (s != atoi(argv[5]))
        fprintf(stderr, "actual=%d expected=%s\n", s, argv[5]);
    CHECK(s == atoi(argv[5]));
    if (e) {
        ud_value value;
        OK(ud_execution_variable(e, str("n"), &value));
        CHECK(value.integer == strtoll(argv[6], NULL, 10));
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        OK(ud_execution_get_metrics(e, &m));
        CHECK(m.result == s && m.queued_batches == 0);
        OK(ud_execution_destroy(e));
    }
    CHECK(ud_plugin_unload(plugin) == UD_BUSY);
    OK(ud_registry_destroy(registry));
    OK(ud_plugin_unload(plugin));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
