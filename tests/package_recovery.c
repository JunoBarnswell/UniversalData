#include "check.h"
#include "ud/package.h"
#include <string.h>
#include <sys/stat.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
typedef struct fixture {
    ud_execution *execution;
    const char *database, *output, *effect;
    int mode, crashed;
} fixture;
static ud_status allow(void *u, ud_string tenant, ud_string resource, int32_t action) {
    (void)u;
    (void)tenant;
    (void)resource;
    (void)action;
    return UD_OK;
}
static ud_status resolve(void *u, ud_context *ctx, ud_string tenant, ud_string resource,
                         ud_string reference, ud_string *out) {
    (void)tenant;
    (void)resource;
    fixture *f = u;
    if (reference.length != 6 || memcmp(reference.data, "target", 6))
        return UD_AUTH;
    size_t n = strlen(f->database);
    char *copy = ud_alloc(ctx, n + 1);
    if (!copy)
        return UD_RESOURCE;
    memcpy(copy, f->database, n + 1);
    *out = (ud_string){copy, (uint32_t)n};
    return UD_OK;
}
static int exists(const char *path) {
    struct stat info;
    return !stat(path, &info);
}
static void observe(void *u, const ud_execution_event *event) {
    fixture *f = u;
    if (f->crashed || !f->mode || !f->execution)
        return;
    if (f->mode == 1 && event->kind == UD_EXEC_EVENT_AFTER_CHECKPOINT && event->task_id.data &&
        strstr(event->task_id.data, "increment")) {
        ud_value n;
        if (!ud_activation_variable(event->activation, (ud_string){"n", 1}, &n) && n.integer == 2)
            _exit(91);
    }
    if (f->mode == 2 && event->kind == UD_EXEC_EVENT_TARGET_COMMITTED &&
        event->component_id.length == 2 && !memcmp(event->component_id.data, "db", 2))
        _exit(91);
    if (f->mode == 3 && event->kind == UD_EXEC_EVENT_TARGET_COMMITTED && exists(f->output))
        _exit(91);
    if (f->mode == 4 && event->kind == UD_EXEC_EVENT_BEFORE_CHECKPOINT && event->task_id.data &&
        strstr(event->task_id.data, "effect") && exists(f->effect))
        _exit(91);
    if (f->mode == 5 && event->kind == UD_EXEC_EVENT_TARGET_COMMITTED) {
        f->crashed = 1;
        puts("paused");
        fflush(stdout);
        char signal[16];
        if (!fgets(signal, sizeof(signal), stdin))
            _exit(92);
    }
}
static ud_status effect(void *u, const ud_component_context *ctx, ud_string config) {
    (void)ctx;
    (void)config;
    fixture *f = u;
    FILE *file = fopen(f->effect, "ab");
    if (!file)
        return UD_IO;
    int ok = fwrite("effect\n", 1, 7, file) == 7;
    if (fclose(file))
        ok = 0;
    return ok ? UD_OK : UD_IO;
}
int main(int argc, char **argv) {
    CHECK(argc == 9);
    fixture f = {.database = argv[2], .output = argv[3], .effect = argv[4], .mode = atoi(argv[7])};
    FILE *file = fopen(argv[1], "rb");
    CHECK(file);
    CHECK(!fseek(file, 0, SEEK_END));
    long n = ftell(file);
    CHECK(n > 0 && !fseek(file, 0, SEEK_SET));
    char *document = malloc((size_t)n);
    CHECK(document);
    CHECK(fread(document, 1, (size_t)n, file) == (size_t)n);
    fclose(file);
    ud_context *host = NULL;
    OK(ud_context_create(NULL, &host));
    ud_registry *registry = NULL;
    OK(ud_registry_create(host, &registry));
    ud_component_descriptor task = {.struct_size = sizeof(task),
                                    .abi_version = UD_ABI_VERSION,
                                    .id = {"custom.effect", 13},
                                    .name = {"Test side effect", 16},
                                    .version = {"1", 1},
                                    .configuration_schema = {"{\"type\":\"object\"}", 17},
                                    .category = UD_COMPONENT_TASK,
                                    .synchronous_input = -1,
                                    .error_output = -1,
                                    .user = &f,
                                    .invoke = effect};
    OK(ud_registry_register(registry, &task));
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .edge_capacity = 1,
                              .max_batch_rows = 7,
                              .max_depth = 16,
                              .tenant = {"fixture", 7},
                              .execution_id = {"recovery-fixture", 16},
                              .authorize = allow,
                              .resolve = resolve,
                              .resolve_user = &f,
                              .observe = observe,
                              .observe_user = &f,
                              .state_directory = {argv[5], (uint32_t)strlen(argv[5])},
                              .resume = atoi(argv[6]),
                              .registry = registry};
    o.context = (ud_context_options){
        .struct_size = sizeof(o.context), .abi_version = UD_ABI_VERSION, .memory_limit = 16777216};
    ud_execution *e = NULL;
    ud_status s = ud_execution_create_json((ud_string){document, (uint32_t)n}, &o, &e);
    free(document);
    f.execution = e;
    if (!s)
        s = ud_execution_execute(e);
    if (s != atoi(argv[8]))
        fprintf(stderr, "actual=%d (%s), expected=%s\n", s, ud_status_name(s), argv[8]);
    CHECK(s == atoi(argv[8]));
    if (e) {
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        OK(ud_execution_get_metrics(e, &m));
        CHECK(m.result == s && m.queued_batches == 0);
        CHECK(m.state == (s == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                          : s                    ? UD_EXEC_FAILED
                                                 : UD_EXEC_SUCCEEDED));
        if (!s) {
            ud_value v;
            OK(ud_execution_variable(e, (ud_string){"n", 1}, &v));
            CHECK(v.integer == 4);
            CHECK(m.input_rows == 31 && m.output_rows == 62 && m.executed_tasks == 6);
        }
        OK(ud_execution_destroy(e));
    }
    OK(ud_registry_destroy(registry));
    CHECK(ud_memory_used(host) == 0);
    OK(ud_context_destroy(host));
    return 0;
}
