#include "check.h"
#include "ud/package.h"
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#include <sys/resource.h>
#endif
typedef struct fixture {
    ud_execution *execution;
    int kill_kind, kill_after, seen;
    const char *kill_component, *deny_resource;
} fixture;
static void process_usage(FILE *stream) {
#ifndef _WIN32
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage))
        return;
    fprintf(stream, ",\"maxRssKiB\":%ld,\"cpuSeconds\":%.6f", usage.ru_maxrss,
            (double)usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1000000.0 +
                usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1000000.0);
#else
    (void)stream;
#endif
}
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    fixture *f = user;
    (void)tenant;
    if (f->deny_resource && resource.length == strlen(f->deny_resource) &&
        !memcmp(resource.data, f->deny_resource, resource.length) && action == UD_ACCESS_READ &&
        f->seen)
        return UD_AUTH;
    return UD_OK;
}
static ud_status resolve(void *user, ud_context *ctx, ud_string tenant, ud_string resource,
                         ud_string reference, ud_string *out) {
    (void)user;
    (void)tenant;
    if (reference.length != 7 || memcmp(reference.data, "fixture", 7))
        return UD_AUTH;
    char *copy = ud_alloc(ctx, (size_t)resource.length + 1);
    if (!copy)
        return UD_RESOURCE;
    memcpy(copy, resource.data, resource.length);
    copy[resource.length] = 0;
    *out = (ud_string){copy, resource.length};
    return UD_OK;
}
static void observe(void *user, const ud_execution_event *event) {
    fixture *f = user;
    int matches = !f->kill_component || (event->component_id.length == strlen(f->kill_component) &&
                                         !memcmp(event->component_id.data, f->kill_component,
                                                 event->component_id.length));
    if (event->kind == f->kill_kind && matches && ++f->seen == f->kill_after) {
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        if (f->execution && !ud_execution_get_metrics(f->execution, &m)) {
            fprintf(stderr,
                    "{\"committed\":%llu,\"memoryUsed\":%llu,\"spillBytes\":%llu,"
                    "\"workerHighWater\":%u",
                    (unsigned long long)m.committed_rows, (unsigned long long)m.memory_used,
                    (unsigned long long)m.spill_bytes, m.worker_high_water);
            process_usage(stderr);
            fputs("}\n", stderr);
            fflush(stderr);
        }
        _exit(91);
    }
}
int main(int argc, char **argv) {
    CHECK(argc >= 5);
    fixture f = {.kill_kind = argc > 5 ? atoi(argv[5]) : 0,
                 .kill_after = argc > 6 ? atoi(argv[6]) : 1,
                 .kill_component = argc > 7 && *argv[7] ? argv[7] : NULL,
                 .deny_resource = argc > 9 && *argv[9] ? argv[9] : NULL};
    FILE *file = fopen(argv[1], "rb");
    CHECK(file && !fseek(file, 0, SEEK_END));
    long bytes = ftell(file);
    CHECK(bytes > 0);
    rewind(file);
    char *document = malloc((size_t)bytes);
    CHECK(document && fread(document, 1, (size_t)bytes, file) == (size_t)bytes);
    fclose(file);
    ud_execution_options options = {
        .struct_size = sizeof(options),
        .abi_version = UD_ABI_VERSION,
        .context = {.struct_size = sizeof(options.context),
                    .abi_version = UD_ABI_VERSION,
                    .memory_limit = argc > 8 ? strtoull(argv[8], NULL, 10) : 67108864},
        .edge_capacity = 1,
        .max_batch_rows = 256,
        .max_depth = 32,
        .max_concurrency = (uint32_t)atoi(argv[4]),
        .tenant = {"fixture", 7},
        .execution_id = {"next-phase", 10},
        .authorize = authorize,
        .resolve = resolve,
        .authorize_user = &f,
        .observe = observe,
        .observe_user = &f,
        .state_directory = str(argv[2]),
        .resume = atoi(argv[3]),
        .timeout_ms = 1800000};
    ud_execution *execution = NULL;
    ud_status result =
        ud_execution_create_json((ud_string){document, (uint32_t)bytes}, &options, &execution);
    free(document);
    f.execution = execution;
    if (!result)
        result = ud_execution_execute(execution);
    if (!execution) {
        printf("{\"result\":%d}\n", result);
        return 0;
    }
    ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
    OK(ud_execution_get_metrics(execution, &m));
    char *diagnostics = NULL;
    uint32_t length = 0;
    OK(ud_execution_diagnostics(execution, &diagnostics, &length));
    printf("{\"result\":%d,\"state\":%d,\"input\":%llu,\"rejected\":%llu,\"committed\":%llu,"
           "\"acknowledged\":%llu,\"errorRows\":%llu,\"committedErrors\":%llu,"
           "\"acknowledgedErrors\":%llu,\"diagnostics\":%.*s",
           result, m.state, (unsigned long long)m.input_rows, (unsigned long long)m.rejected_rows,
           (unsigned long long)m.committed_rows, (unsigned long long)m.acknowledged_rows,
           (unsigned long long)m.error_rows, (unsigned long long)m.committed_error_rows,
           (unsigned long long)m.acknowledged_error_rows, (int)length, diagnostics);
    ud_free(diagnostics);
    printf(",\"workerHighWater\":%u,\"treeHighWater\":%u,\"spillBytes\":%llu", m.worker_high_water,m.tree_high_water,(unsigned long long)m.spill_bytes);
    process_usage(stdout);
    printf("}\n");
    OK(ud_execution_destroy(execution));
    return 0;
}
