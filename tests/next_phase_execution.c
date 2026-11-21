#include "check.h"
#include "ud/package.h"
#include "ud/connection_manager.h"
#include "../engines/package/internal.h"
#include <stdatomic.h>
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#include <sys/resource.h>
#endif
typedef struct fixture {
    ud_execution *execution;
    int kill_kind, kill_after, seen, trace;
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
typedef struct lookup_epoch_node {
    ud_component_context context;
    atomic_int *updated;
    uint32_t phase;
} lookup_epoch_node;
static const ud_field lookup_epoch_fields[] = {
    {.name = "seq", .name_length = 3, .type = UD_INT64},
    {.name = "probe", .name_length = 5, .type = UD_UTF8}};
static ud_status lookup_epoch_source_schema(void *user, const ud_component_context *context,
                                            ud_string config, const ud_batch *const *input,
                                            ud_batch **out) {
    (void)user;
    (void)config;
    (void)input;
    return ud_batch_create(context->memory, lookup_epoch_fields, 2, 0, out);
}
static ud_status lookup_epoch_sink_schema(void *user, const ud_component_context *context,
                                          ud_string config, const ud_batch *const *input,
                                          ud_batch **out) {
    (void)user;
    (void)context;
    (void)config;
    (void)input;
    (void)out;
    return UD_OK;
}
static ud_status lookup_epoch_prepare(void *user, const ud_component_context *context,
                                      ud_string config, const ud_batch *const *in,
                                      const ud_batch *const *out, void **instance) {
    (void)config;
    (void)in;
    (void)out;
    lookup_epoch_node *node = ud_alloc(context->memory, sizeof(*node));
    if (!node)
        return UD_RESOURCE;
    node->context = *context;
    node->updated = user;
    *instance = node;
    return UD_OK;
}
static void lookup_epoch_dispose(void *instance, ud_status status) {
    (void)status;
    ud_free(instance);
}
static ud_status lookup_epoch_confirm(void *instance, uint64_t epoch, ud_string position) {
    (void)instance;
    return epoch < 3 && position.length == 1 ? UD_OK : UD_SCHEMA;
}
static ud_status lookup_epoch_source(void *instance, const ud_component_event *event) {
    (void)event;
    lookup_epoch_node *node = instance;
    ud_component_context *c = &node->context;
    if (node->phase == 5)
        return UD_END;
    if (node->phase == 1 && !atomic_load(node->updated))
        return UD_BUSY;
    if (node->phase == 2 || node->phase == 4) {
        ud_component_event barrier = {.struct_size = sizeof(barrier), .abi_version = UD_ABI_VERSION,
                                      .kind = UD_COMPONENT_CHECKPOINT_BARRIER,
                                      .epoch = node->phase / 2,
                                      .source_position = {node->phase == 2 ? "2" : "3", 1}};
        UD_TRY(c->emit_event(c->emit_user, 0, &barrier));
    } else {
        ud_value values[] = {{.type = UD_INT64, .integer = node->phase == 3 ? 2 : node->phase},
                             {.type = UD_UTF8, .text_length = 5,
                              .text = node->phase == 1 ? "ALICE" : "alice"}};
        ud_batch *batch = NULL;
        UD_TRY(ud_batch_create(c->memory, lookup_epoch_fields, 2, 1, &batch));
        ud_status status = ud_batch_append(batch, values, 2);
        if (!status)
            status = ud_batch_seal(batch);
        if (!status)
            status = c->emit(c->emit_user, 0, batch);
        ud_batch_destroy(batch);
        UD_TRY(status);
    }
    node->phase++;
    return UD_MORE;
}
static ud_status lookup_epoch_sink(void *instance, const ud_component_event *event) {
    if (event->kind == UD_COMPONENT_POLL)
        return UD_END;
    if (event->kind != UD_COMPONENT_DATA)
        return UD_OK;
    lookup_epoch_node *node = instance;
    if (ud_batch_rows(event->batch) != 1)
        return UD_CORRUPT;
    ud_value sequence, payload;
    UD_TRY(ud_batch_get(event->batch, 0, 0, &sequence));
    UD_TRY(ud_batch_get(event->batch, 0, 3, &payload));
    const char *expected = node->phase < 2 ? "original" : "changed";
    if (sequence.integer != node->phase || payload.is_null ||
        payload.text_length != strlen(expected) || memcmp(payload.text, expected, payload.text_length))
        return UD_CORRUPT;
    node->phase++;
    if (node->phase != 1)
        return UD_OK;
    ud_connection_lease *writer = NULL;
    UD_TRY(ud_activation_acquire_connection(node->context.activation, str("db"),
                                            UD_CONNECT_EXECUTE_SQL, &writer));
    ud_read_options update = {.struct_size = sizeof(update), .abi_version = UD_ABI_VERSION,
                              .sql = str("UPDATE reference SET payload='changed'")};
    uint64_t changed = 0;
    ud_status status = ud_connection_lease_execute_command(writer, &update, &changed);
    ud_status closed = ud_connection_lease_release(writer);
    if (!status)
        status = closed;
    if (!status && changed != 1)
        status = UD_CORRUPT;
    if (!status)
        atomic_store(node->updated, 1);
    return status;
}
static ud_status lookup_epoch_finish(void *instance) {
    lookup_epoch_node *node = instance;
    return node->phase == 3 || node->phase == 5 ? UD_OK : UD_CORRUPT;
}
static ud_status register_lookup_epoch(ud_registry *registry, atomic_int *updated) {
    ud_component_descriptor descriptor = {
        .struct_size = sizeof(descriptor), .abi_version = UD_ABI_VERSION,
        .id = {"fixture.lookup.source", 21}, .name = {"Lookup epoch source", 19},
        .version = {"1", 1}, .configuration_schema = {"{}", 2},
        .category = UD_COMPONENT_SOURCE, .outputs = 1, .synchronous_input = -1, .error_output = -1,
        .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_CONTINUOUS,
        .configure = lookup_epoch_source_schema, .prepare = lookup_epoch_prepare,
        .step = lookup_epoch_source, .finish = lookup_epoch_finish, .dispose = lookup_epoch_dispose,
        .confirm_epoch = lookup_epoch_confirm, .user = updated};
    UD_TRY(ud_registry_register(registry, &descriptor));
    descriptor.id = str("fixture.lookup.sink");
    descriptor.name = str("Lookup epoch oracle");
    descriptor.category = UD_COMPONENT_DESTINATION;
    descriptor.inputs = 1;
    descriptor.outputs = 0;
    descriptor.capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_STREAM_SAFE |
                              UD_COMPONENT_REPLAY_SAFE;
    descriptor.effects = UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_IDEMPOTENT;
    descriptor.configure = lookup_epoch_sink_schema;
    descriptor.step = lookup_epoch_sink;
    descriptor.confirm_epoch = NULL;
    return ud_registry_register(registry, &descriptor);
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
    if (reference.length > 5 && !memcmp(reference.data, "file:", 5)) {
        char *path = ud_alloc(ctx, (size_t)reference.length - 4);
        if (!path)
            return UD_RESOURCE;
        memcpy(path, reference.data + 5, reference.length - 5);
        FILE *f = fopen(path, "rb");
        ud_free(path);
        if (!f)
            return UD_IO;
        if (fseek(f, 0, SEEK_END)) {
            fclose(f);
            return UD_IO;
        }
        long n = ftell(f);
        if (n < 1 || n > 65536 || fseek(f, 0, SEEK_SET)) {
            fclose(f);
            return UD_SCHEMA;
        }
        char *text = ud_alloc(ctx, (size_t)n + 1);
        if (!text) {
            fclose(f);
            return UD_RESOURCE;
        }
        size_t got = fread(text, 1, (size_t)n, f);
        fclose(f);
        if (got != (size_t)n) {
            ud_free(text);
            return UD_IO;
        }
        *out = (ud_string){text, (uint32_t)n};
        return UD_OK;
    }
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
    if (f->trace && (event->kind == UD_EXEC_EVENT_STATE || event->kind == UD_EXEC_EVENT_TARGET_COMMITTED ||
                     (event->sequence % 128u) == 0)) {
        fprintf(stderr, "event %llu kind %d state %d status %d task %.*s component %.*s\n",
                (unsigned long long)event->sequence, event->kind, event->state, event->result,
                (int)event->task_id.length, event->task_id.data ? event->task_id.data : "",
                (int)event->component_id.length, event->component_id.data ? event->component_id.data : "");
        fflush(stderr);
    }
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
                 .trace = getenv("UD_TEST_RECOVERY_TRACE") != NULL,
                 .deny_resource = argc > 9 && *argv[9] ? argv[9] : NULL};
    FILE *file = fopen(argv[1], "rb");
    CHECK(file && !fseek(file, 0, SEEK_END));
    long bytes = ftell(file);
    CHECK(bytes > 0);
    rewind(file);
    char *document = malloc((size_t)bytes);
    CHECK(document && fread(document, 1, (size_t)bytes, file) == (size_t)bytes);
    fclose(file);
    ud_context *fixture_context = NULL;
    ud_registry *registry = NULL;
    atomic_int updated = 0;
    if (argc > 10 && !strcmp(argv[10], "lookup-epoch")) {
        OK(ud_context_create(NULL, &fixture_context));
        OK(ud_registry_create(fixture_context, &registry));
        OK(register_lookup_epoch(registry, &updated));
    }
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
        .registry = registry,
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
    if (result && execution)
        fprintf(stderr, "Native failure: %s\n", ud_error(execution->ctx));
    if (!execution) {
        printf("{\"result\":%d}\n", result);
        return 0;
    }
    ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
    OK(ud_execution_get_metrics(execution, &m));
    char *diagnostics = NULL;
    uint32_t length = 0;
    OK(ud_execution_diagnostics(execution, &diagnostics, &length));
    ud_string id;
    OK(ud_execution_id(execution, &id));
    printf("{\"executionId\":\"%.*s\",\"result\":%d,\"state\":%d,\"input\":%llu,\"rejected\":%llu,\"committed\":%llu,"
           "\"acknowledged\":%llu,\"errorRows\":%llu,\"committedErrors\":%llu,"
           "\"acknowledgedErrors\":%llu,\"diagnostics\":%.*s",
           (int)id.length, id.data, result, m.state, (unsigned long long)m.input_rows, (unsigned long long)m.rejected_rows,
           (unsigned long long)m.committed_rows, (unsigned long long)m.acknowledged_rows,
           (unsigned long long)m.error_rows, (unsigned long long)m.committed_error_rows,
           (unsigned long long)m.acknowledged_error_rows, (int)length, diagnostics);
    ud_free(diagnostics);
    printf(",\"workerHighWater\":%u,\"treeHighWater\":%u,\"spillBytes\":%llu", m.worker_high_water,
           m.tree_high_water, (unsigned long long)m.spill_bytes);
    process_usage(stdout);
    printf("}\n");
    OK(ud_execution_destroy(execution));
    if (registry) {
        OK(ud_registry_destroy(registry));
        OK(ud_context_destroy(fixture_context));
    }
    return 0;
}
