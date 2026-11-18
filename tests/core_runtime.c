#include "check.h"
#include "ud/package.h"
#include "ud/operator.h"
#include "../components/execution/state.h"
#include <stdatomic.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#endif

static ud_string text(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static void pause_ms(void) {
#ifdef _WIN32
    Sleep(1);
#else
    struct timespec delay = {0, 1000000};
    nanosleep(&delay, NULL);
#endif
}
static ud_status grant(void *user) {
    return *(ud_status *)user;
}
static int exchange_admission(const char *directory) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_field field = {.name = "x", .name_length = 1, .type = UD_INT64};
    ud_value value = {.type = UD_INT64, .integer = 42};
    ud_batch *batch = NULL;
    OK(ud_batch_create(ctx, &field, 1, 1, &batch));
    OK(ud_batch_append(batch, &value, 1));
    OK(ud_batch_seal(batch));
    ud_status permission = UD_OK;
    ud_buffer_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION,
        .resident_limit = 1, .working_limit = 1000000, .spill_limit = 10000000,
        .max_blocks = 4, .directory = text(directory), .check = grant, .user = &permission};
    ud_buffer_manager *manager = NULL;
    ud_buffer_block *frame = NULL, *fanout = NULL, *state = NULL;
    ud_buffer_metrics metrics;
    OK(ud_buffer_manager_create(ctx, &options, &manager));
    OK(ud_buffer_store_exchange(manager, batch, &frame));
    uint64_t bytes = ud_buffer_block_bytes(frame);
    CHECK(bytes > 1 && !ud_buffer_block_spill_bytes(frame));
    OK(ud_buffer_store_exchange(manager, batch, &fanout));
    OK(ud_buffer_manager_metrics(manager, &metrics));
    CHECK(metrics.working_bytes == bytes && !metrics.resident_bytes && !metrics.total_spill_bytes);
    OK(ud_buffer_store(manager, batch, &state));
    CHECK(ud_buffer_block_spill_bytes(state) > 0);
    OK(ud_buffer_release(state));
    OK(ud_buffer_release(fanout));
    OK(ud_buffer_release(frame));
    OK(ud_buffer_manager_metrics(manager, &metrics));
    CHECK(!metrics.working_bytes && !metrics.resident_bytes && !metrics.spill_bytes);
    OK(ud_buffer_manager_destroy(manager));

    options.resident_limit = bytes;
    OK(ud_buffer_manager_create(ctx, &options, &manager));
    OK(ud_buffer_store_exchange(manager, batch, &frame));
    OK(ud_buffer_store(manager, batch, &state));
    OK(ud_buffer_release(frame));
    OK(ud_buffer_manager_metrics(manager, &metrics));
    CHECK(!metrics.working_bytes && metrics.resident_bytes == bytes && !metrics.total_spill_bytes);
    OK(ud_buffer_release(state));
    OK(ud_buffer_manager_destroy(manager));
    ud_batch_destroy(batch);
    CHECK(!ud_memory_used(ctx));
    OK(ud_context_destroy(ctx));
    return 0;
}

static int buffers_and_operators(const char *directory) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_field field = {.name = "x", .name_length = 1, .type = UD_INT64};
    ud_batch *schema = NULL;
    OK(ud_batch_create(ctx, &field, 1, 0, &schema));
    ud_status permission = UD_OK;
    ud_buffer_options buffers = {.struct_size = sizeof(buffers),
                                 .abi_version = UD_ABI_VERSION,
                                 .max_blocks = 2048,
                                 .resident_limit = 1,
                                 .working_limit = 1000000,
                                 .spill_limit = 10000000,
                                 .directory = text(directory),
                                 .check = grant,
                                 .user = &permission};
    ud_buffer_manager *manager = NULL;
    OK(ud_buffer_manager_create(ctx, &buffers, &manager));
    uint32_t key = 0;
    ud_operator_options spec = {.struct_size = sizeof(spec),
                                .abi_version = UD_ABI_VERSION,
                                .kind = UD_OPERATOR_SORT,
                                .keys = &key,
                                .key_count = 1,
                                .max_batch_rows = 3,
                                .buffers = manager};
    const ud_batch *inputs[] = {schema};
    ud_operator *op = NULL;
    OK(ud_operator_create(ctx, &spec, inputs, &op));
    ud_batch *batch = NULL;
    CHECK(ud_operator_pull(op, &batch, NULL) == UD_BUSY && !batch);
    for (int i = 30; i >= 0; i--) {
        OK(ud_batch_create(ctx, &field, 1, 1, &batch));
        ud_value value = {.type = UD_INT64, .integer = i};
        OK(ud_batch_append(batch, &value, 1));
        CHECK(ud_operator_push(op, 0, batch) == UD_INVALID);
        OK(ud_batch_seal(batch));
        OK(ud_operator_push(op, 0, batch));
        ud_batch_destroy(batch);
        batch = NULL;
    }
    CHECK(ud_buffer_manager_destroy(manager) == UD_BUSY);
    OK(ud_operator_finish_input(op, 0));
    CHECK(ud_operator_finish_input(op, 0) == UD_INVALID);
    uint32_t rows = 0;
    ud_status status;
    while ((status = ud_operator_pull(op, &batch, NULL)) == UD_OK) {
        CHECK(ud_batch_rows(batch) > 0 && ud_batch_rows(batch) <= 3);
        for (uint32_t i = 0; i < ud_batch_rows(batch); i++) {
            ud_value value;
            OK(ud_batch_get(batch, i, 0, &value));
            CHECK(value.integer == rows++);
        }
        ud_batch_destroy(batch);
        batch = NULL;
    }
    CHECK(status == UD_END && !batch && rows == 31);
    OK(ud_operator_destroy(op));
    ud_buffer_metrics metrics;
    OK(ud_buffer_manager_metrics(manager, &metrics));
    CHECK(!metrics.blocks && !metrics.spill_bytes && metrics.total_spill_bytes > 0);
    OK(ud_buffer_manager_destroy(manager));

    /* Window and aggregate use the same bounded sort and buffer path. */
    buffers.resident_limit = 1000000;
    buffers.directory = (ud_string){0};
    OK(ud_buffer_manager_create(ctx, &buffers, &manager));
    spec.buffers = manager;
    spec.kind = UD_OPERATOR_WINDOW;
    spec.window_function = UD_WINDOW_RUNNING_SUM;
    spec.window_column = 0;
    spec.window_name = text("running");
    OK(ud_operator_create(ctx, &spec, inputs, &op));
    ud_batch *window_schema = NULL, *other_schema = NULL, *other_output = NULL;
    OK(ud_operator_schema(op, 0, &window_schema));
    ud_field original, other_field = field;
    OK(ud_batch_field(window_schema, 1, &original));
    CHECK(original.lineage_count == 1);
    OK(ud_id_make(ctx, text("other-source"), text("x"), &other_field.identity));
    OK(ud_batch_create(ctx, &other_field, 1, 0, &other_schema));
    const ud_batch *other_inputs[] = {other_schema};
    ud_operator *other_op = NULL;
    OK(ud_operator_create(ctx, &spec, other_inputs, &other_op));
    OK(ud_operator_schema(other_op, 0, &other_output));
    OK(ud_batch_field(other_output, 1, &other_field));
    CHECK(ud_id_compare(original.identity, other_field.identity));
    ud_batch_destroy(other_output);
    ud_batch_destroy(other_schema);
    ud_batch_destroy(window_schema);
    OK(ud_operator_destroy(other_op));
    OK(ud_batch_create(ctx, &field, 1, 3, &batch));
    for (int i = 3; i >= 1; i--) {
        ud_value value = {.type = UD_INT64, .integer = i};
        OK(ud_batch_append(batch, &value, 1));
    }
    OK(ud_batch_seal(batch));
    OK(ud_operator_push(op, 0, batch));
    ud_batch_destroy(batch);
    batch = NULL;
    OK(ud_operator_finish_input(op, 0));
    OK(ud_operator_pull(op, &batch, NULL));
    const int sums[] = {1, 3, 6};
    for (uint32_t i = 0; i < 3; i++) {
        ud_value value;
        OK(ud_batch_get(batch, i, 1, &value));
        CHECK(value.integer == sums[i]);
    }
    ud_batch_destroy(batch);
    batch = NULL;
    CHECK(ud_operator_pull(op, &batch, NULL) == UD_END);
    OK(ud_operator_destroy(op));
    ud_plan *plan = NULL;
    const char *definition = "{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"aggregates\":[{\"function\":\"count\",\"column\":"
                             "\"*\",\"name\":\"n\"}]}";
    OK(ud_plan_create_fields(ctx, UD_PLAN_VERSION, &field, 1, text(definition), NULL, &plan));
    spec.kind = UD_OPERATOR_AGGREGATE;
    spec.aggregation = plan;
    OK(ud_operator_create(ctx, &spec, inputs, &op));
    ud_plan_destroy(plan);
    OK(ud_operator_finish_input(op, 0));
    OK(ud_operator_pull(op, &batch, NULL));
    ud_value count;
    OK(ud_batch_get(batch, 0, 0, &count));
    CHECK(count.integer == 0);
    ud_batch_destroy(batch);
    batch = NULL;
    CHECK(ud_operator_pull(op, &batch, NULL) == UD_END);
    OK(ud_operator_destroy(op));
    OK(ud_buffer_manager_destroy(manager));

    /* Integrity, grant recheck, quota failure and cleanup after cancellation. */
    buffers.resident_limit = 1;
    buffers.directory = text(directory);
    buffers.max_blocks = 1;
    OK(ud_buffer_manager_create(ctx, &buffers, &manager));
    OK(ud_batch_create(ctx, &field, 1, 1, &batch));
    ud_value value = {.type = UD_INT64, .integer = 7};
    OK(ud_batch_append(batch, &value, 1));
    OK(ud_batch_seal(batch));
    ud_buffer_block *block = NULL, *extra = NULL;
    OK(ud_buffer_store(manager, batch, &block));
    uint8_t *public_wire = NULL;
    uint64_t public_bytes = 0;
    OK(ud_batch_serialize(batch, &public_wire, &public_bytes));
    CHECK(ud_buffer_block_spill_bytes(block) + 15 == public_bytes);
    ud_free(public_wire);
    CHECK(ud_buffer_store(manager, batch, &extra) == UD_RESOURCE && !extra);
    ud_buffer_view *read = NULL;
    permission = UD_AUTH;
    CHECK(ud_buffer_read(block, &read) == UD_AUTH && !read);
    permission = UD_OK;
    OK(ud_buffer_read(block, &read));
    CHECK(ud_buffer_release(block) == UD_BUSY);
    ud_value restored;
    OK(ud_batch_get(ud_buffer_view_batch(read), 0, 0, &restored));
    CHECK(restored.integer == 7);
    OK(ud_buffer_manager_metrics(manager, &metrics));
    uint64_t working = metrics.working_bytes;
    CHECK(working > 0 && metrics.views == 1 && metrics.working_high_water >= working);
    ud_buffer_view *other = NULL;
    OK(ud_buffer_read(block, &other));
    OK(ud_buffer_manager_metrics(manager, &metrics));
    CHECK(metrics.views == 2 && metrics.working_bytes > working);
    ud_buffer_view_release(other);
    ud_buffer_view_release(read);
    read = NULL;
    OK(ud_buffer_manager_metrics(manager, &metrics));
    CHECK(!metrics.views && !metrics.working_bytes);
    ud_memory_domain *state_domain = NULL;
    OK(ud_buffer_state_domain_create(manager, working - 1, &state_domain));
    CHECK(ud_buffer_read_in_domain(block, state_domain, &read) == UD_RESOURCE && !read);
    CHECK(ud_memory_domain_peak(state_domain) > 0 && !ud_memory_domain_used(state_domain));
    OK(ud_buffer_manager_metrics(manager, &metrics));
    CHECK(!metrics.views && !metrics.working_bytes && metrics.blocks == 1);
    OK(ud_memory_domain_destroy(state_domain));
    OK(ud_buffer_state_domain_create(manager, buffers.working_limit / 2, &state_domain));
    OK(ud_buffer_read_in_domain(block, state_domain, &read));
    OK(ud_batch_get(ud_buffer_view_batch(read), 0, 0, &restored));
    CHECK(restored.integer == 7);
    CHECK(ud_memory_domain_used(state_domain) == working);
    CHECK(ud_memory_domain_destroy(state_domain) == UD_BUSY);
    OK(ud_buffer_manager_metrics(manager, &metrics));
    CHECK(metrics.views == 1 && metrics.working_bytes == working);
    ud_buffer_view_release(read);
    read = NULL;
    CHECK(!ud_memory_domain_used(state_domain));
    OK(ud_memory_domain_destroy(state_domain));
    char path[4096];
    snprintf(path, sizeof(path), "%s/ud-buffer-%020llu.udw", directory, 1ULL);
    FILE *file = fopen(path, "r+b");
    CHECK(file && !fseek(file, -1, SEEK_END));
    int ch = fgetc(file);
    CHECK(ch != EOF && !fseek(file, -1, SEEK_END) && fputc(ch ^ 1, file) != EOF && !fclose(file));
    CHECK(ud_buffer_read(block, &read) == UD_CORRUPT && !read);
    OK(ud_buffer_release(block));
    OK(ud_buffer_manager_destroy(manager));
    buffers.working_limit = 1;
    OK(ud_buffer_manager_create(ctx, &buffers, &manager));
    block = NULL;
    CHECK(ud_buffer_store(manager, batch, &block) == UD_RESOURCE && !block);
    OK(ud_buffer_manager_metrics(manager, &metrics));
    CHECK(!metrics.working_bytes && !metrics.blocks);
    ud_cancel(ctx);
    OK(ud_buffer_manager_destroy(manager));
    ud_batch_destroy(batch);
    ud_batch_destroy(schema);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}

typedef struct fixture {
    atomic_int active, high, complete, running, arrived;
    uint64_t sequence;
} fixture;
static ud_status invoke(void *user, const ud_component_context *c, ud_string config) {
    fixture *f = user;
    (void)config;
    int active = atomic_fetch_add(&f->active, 1) + 1, high = atomic_load(&f->high);
    while (high < active && !atomic_compare_exchange_weak(&f->high, &high, active)) {
    }
    ud_status s = UD_OK;
    atomic_fetch_add(&f->arrived, 1);
    int deadline = strstr(c->task_id.data, "deadline") != NULL;
    /* Arrival barrier proves overlap without relying on a 5ms thread startup. */
    for (int i = 0; i < 5000 && atomic_load(&f->arrived) < 2; i++) {
        s = ud_activation_check(c->activation);
        if (s)
            break;
        pause_ms();
    }
    if (!s && atomic_load(&f->arrived) < 2)
        s = UD_RESOURCE;
    for (int i = 0; !s && i < (deadline ? 5000 : 50); i++) {
        s = ud_activation_check(c->activation);
        if (s)
            break;
        pause_ms();
    }
    if (!s && deadline)
        s = UD_RESOURCE;
    if (!s)
        atomic_fetch_add(&f->complete, 1);
    atomic_fetch_sub(&f->active, 1);
    return s;
}
static void observe(void *user, const ud_execution_event *event) {
    fixture *f = user;
    if (event->sequence <= f->sequence)
        abort();
    f->sequence = event->sequence;
    if (event->state == UD_EXEC_RUNNING && event->task_id.length)
        atomic_fetch_add(&f->running, 1);
}
static int activations(void) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_registry *registry = NULL;
    OK(ud_registry_create(ctx, &registry));
    fixture f = {0};
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = {"test.delay", 10},
                                 .name = {"Delay", 5},
                                 .version = {"1.0.0", 5},
                                 .configuration_schema = {"{\"type\":\"object\"}", 17},
                                 .category = UD_COMPONENT_TASK,
                                 .synchronous_input = -1,
                                 .error_output = -1,
                                 .capabilities = UD_COMPONENT_THREAD_SAFE,
                                 .user = &f,
                                 .invoke = invoke};
    OK(ud_registry_register(registry, &d));
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .context = {.struct_size = sizeof(o.context),
                                          .abi_version = UD_ABI_VERSION,
                                          .memory_limit = 16000000},
                              .edge_capacity = 1,
                              .max_batch_rows = 3,
                              .max_depth = 16,
                              .registry = registry,
                              .tenant = {"fixture", 7},
                              .max_concurrency = 3,
                              .observe = observe,
                              .observe_user = &f};
    const char *package =
        "{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"id\":\"concurrent\",\"controlFlow\":{\"tasks\":["
        "{\"id\":\"deadline\",\"kind\":\"test.delay\",\"timeoutMs\":1000,\"config\":{}},"
        "{\"id\":\"sibling\",\"kind\":\"test.delay\",\"config\":{}}],\"constraints\":[]},"
        "\"dataFlows\":[]}";
    ud_execution *execution = NULL;
    OK(ud_execution_create_json(text(package), &o, &execution));
    CHECK(ud_registry_destroy(registry) == UD_BUSY);
    CHECK(ud_execution_execute(execution) == UD_TIMEOUT);
    CHECK(atomic_load(&f.high) == 2 && atomic_load(&f.complete) == 1 && !atomic_load(&f.active));
    ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
    OK(ud_execution_get_metrics(execution, &m));
    CHECK(m.result == UD_TIMEOUT && m.executed_tasks == 1);
    OK(ud_execution_destroy(execution));
    /* Sequential sibling tasks may write their shared lexical parent. */
    o.max_concurrency = 1;
    f.sequence = 0;
    package =
        "{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"id\":\"sequential\",\"variables\":{\"n\":{\"type\":\"int64\","
        "\"default\":"
        "0}},"
        "\"eventHandlers\":{\"after\":{\"tasks\":[{\"id\":\"after\",\"kind\":\"set\",\"config\":{"
        "\"name\":\"n\",\"value\":{\"literal\":3,\"type\":\"int64\"}}}],\"constraints\":[]}},"
        "\"controlFlow\":{\"tasks\":[{\"id\":\"a\",\"kind\":\"set\",\"config\":{\"name\":\"n\","
        "\"value\":{\"literal\":1,\"type\":\"int64\"}}},"
        "{\"id\":\"b\",\"kind\":\"set\",\"config\":{\"name\":\"n\",\"value\":{\"literal\":2,"
        "\"type\":\"int64\"}}}],\"constraints\":[]},\"dataFlows\":[]}";
    OK(ud_execution_create_json(text(package), &o, &execution));
    OK(ud_execution_execute(execution));
    ud_value value;
    OK(ud_execution_variable(execution, text("n"), &value));
    CHECK(value.integer == 3);
    OK(ud_execution_destroy(execution));
    OK(ud_registry_destroy(registry));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
static int functions(void) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    const char *ast = "{\"function\":\"upper_ascii\",\"args\":[{\"function\":\"trim\",\"args\":[{"
                      "\"literal\":\"  a中文  \",\"type\":\"utf8\"}]}]}";
    ud_expression_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION};
    ud_expression *expression = NULL;
    OK(ud_expression_create(ctx, text(ast), &options, &expression));
    ud_value value;
    OK(ud_expression_evaluate(expression, NULL, 0, &value));
    CHECK(value.text_length == 7 && !memcmp(value.text, "A中文", 7));
    ud_expression_destroy(expression);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    CHECK(!exchange_admission(argv[1]));
    CHECK(!buffers_and_operators(argv[1]));
    CHECK(!activations());
    CHECK(!functions());
    puts("Shared spill/sort/window/aggregate, quota/integrity/cleanup, concurrent activation "
         "isolation and native functions passed");
    return 0;
}
