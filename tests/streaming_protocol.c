#include "check.h"
#include "ud/package.h"
#include <string.h>
#include <stdatomic.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#endif
#define TRY(call)                                                                                  \
    do {                                                                                           \
        ud_status s_ = (call);                                                                     \
        if (s_)                                                                                    \
            return s_;                                                                             \
    } while (0)
typedef struct fixture {
    atomic_int polls;
    atomic_uint received;
    atomic_int watermark;
    int source_watermark, idle_on_minimum;
    uint32_t expected_rows;
    atomic_int minimum_seen;
    atomic_int_fast64_t last_watermark;
    atomic_int *end_on_minimum;
} fixture;
typedef struct source {
    ud_component_context context;
    fixture *facts;
    uint32_t phase;
} source;
static const ud_field fields[] = {{.name = "time", .name_length = 4, .type = UD_INT64},
                                  {.name = "key", .name_length = 3, .type = UD_INT64},
                                  {.name = "value", .name_length = 5, .type = UD_INT64}};
static ud_status configure(void *u, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    (void)u;
    (void)text;
    (void)in;
    return ud_batch_create(c->memory, fields, 3, 0, out);
}
static ud_status describe(void *u, const ud_component_context *c, ud_string text,
                          const ud_batch *const *in, ud_component_plan *p) {
    (void)u;
    (void)c;
    (void)text;
    (void)in;
    p->boundedness = UD_SOURCE_CONTINUOUS;
    return UD_OK;
}
static ud_status prepare_source(void *u, const ud_component_context *c, ud_string text,
                                const ud_batch *const *in, const ud_batch *const *out,
                                void **instance) {
    (void)text;
    (void)in;
    (void)out;
    source *s = ud_alloc(c->memory, sizeof(*s));
    if (!s)
        return UD_RESOURCE;
    s->context = *c;
    s->facts = u;
    *instance = s;
    return UD_OK;
}
static ud_status finish(void *instance) {
    (void)instance;
    return UD_OK;
}
static void dispose_sink(void *instance, ud_status status) {
    (void)instance;
    (void)status;
}
static ud_status configure_sink(void *u, const ud_component_context *c, ud_string text,
                                const ud_batch *const *in, ud_batch **out) {
    (void)u;
    (void)c;
    (void)text;
    (void)in;
    (void)out;
    return UD_OK;
}
static ud_status source_step(void *instance, const ud_component_event *event) {
    (void)event;
    source *s = instance;
    if (ud_activation_stopping(s->context.activation))
        return UD_END;
    if (s->phase < 3) {
        ud_batch *b = NULL;
        TRY(ud_batch_create(s->context.memory, fields, 3, 1, &b));
        ud_value v[] = {{.type = UD_INT64, .integer = (int64_t)s->phase * 10},
                        {.type = UD_INT64, .integer = 1},
                        {.type = UD_INT64, .integer = 1}};
        ud_status status = ud_batch_append(b, v, 3);
        if (!status)
            status = ud_batch_seal(b);
        if (!status)
            status = s->context.emit(s->context.emit_user, 0, b);
        ud_batch_destroy(b);
        if (status)
            return status;
        s->phase++;
        return UD_MORE;
    }
    if (s->phase == 3) {
        ud_component_event wm = {.struct_size = sizeof(wm),
                                 .abi_version = UD_ABI_VERSION,
                                 .kind = UD_COMPONENT_WATERMARK,
                                 .watermark =
                                     s->facts->source_watermark ? s->facts->source_watermark : 40};
        TRY(s->context.emit_event(s->context.emit_user, 0, &wm));
        s->phase++;
        return UD_MORE;
    }
    if (ud_activation_stopping(s->context.activation))
        return UD_END;
    if (s->facts->end_on_minimum && atomic_load(s->facts->end_on_minimum)) {
        if (!s->facts->idle_on_minimum)
            return UD_END;
        if (s->phase == 4) {
            ud_component_event idle = {.struct_size = sizeof(idle),
                                       .abi_version = UD_ABI_VERSION,
                                       .kind = UD_COMPONENT_IDLE};
            TRY(s->context.emit_event(s->context.emit_user, 0, &idle));
            s->phase++;
            return UD_MORE;
        }
    }
    atomic_fetch_add(&s->facts->polls, 1);
    return UD_BUSY;
}
static void dispose_source(void *instance, ud_status status) {
    (void)status;
    ud_free(instance);
}
static ud_status prepare_sink(void *u, const ud_component_context *c, ud_string text,
                              const ud_batch *const *in, const ud_batch *const *out,
                              void **instance) {
    (void)c;
    (void)text;
    (void)in;
    (void)out;
    *instance = u;
    return UD_OK;
}
static ud_status sink_step(void *instance, const ud_component_event *event) {
    fixture *f = instance;
    if (event->kind == UD_COMPONENT_DATA) {
        if (f->watermark)
            return UD_CORRUPT;
        f->received += ud_batch_rows(event->batch);
    }
    if (event->kind == UD_COMPONENT_WATERMARK) {
        atomic_store(&f->last_watermark, event->watermark);
        if (event->watermark < 40 && event->watermark != 5)
            return UD_OK;
    }
    if (event->kind == UD_COMPONENT_WATERMARK && event->watermark == 5) {
        atomic_store(&f->minimum_seen, 1);
        return UD_OK;
    }
    if (event->kind == UD_COMPONENT_WATERMARK) {
        if (f->received != (f->expected_rows ? f->expected_rows : 3))
            return UD_CORRUPT;
        f->watermark = 1;
    }
    return event->kind == UD_COMPONENT_INPUT_END ? UD_END : UD_OK;
}
static uint64_t monotonic_ms(void) {
#ifdef _WIN32
    return GetTickCount64();
#else
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000 + (uint64_t)now.tv_nsec / 1000000;
#endif
}

static ud_status allow(void *u, ud_string tenant, ud_string resource, int32_t action) {
    (void)u;
    (void)tenant;
    (void)resource;
    (void)action;
    return UD_OK;
}
int main(void) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_registry *registry = NULL;
    OK(ud_registry_create(ctx, &registry));
    fixture f = {0};
    ud_component_descriptor source_descriptor = {.struct_size = sizeof(source_descriptor),
                                                 .abi_version = UD_ABI_VERSION,
                                                 .id = {"fixture.continuous", 18},
                                                 .name = {"Continuous fixture", 18},
                                                 .version = {"1", 1},
                                                 .configuration_schema = {"{}", 2},
                                                 .category = UD_COMPONENT_SOURCE,
                                                 .outputs = 1,
                                                 .synchronous_input = -1,
                                                 .error_output = -1,
                                                 .capabilities = UD_COMPONENT_THREAD_SAFE |
                                                                 UD_COMPONENT_CONTINUOUS |
                                                                 UD_COMPONENT_REPLAY_SAFE,
                                                 .configure = configure,
                                                 .prepare = prepare_source,
                                                 .step = source_step,
                                                 .dispose = dispose_source,
                                                 .finish = finish,
                                                 .describe_plan = describe,
                                                 .user = &f};
    OK(ud_registry_register(registry, &source_descriptor));
    ud_component_descriptor sink = {.struct_size = sizeof(sink),
                                    .abi_version = UD_ABI_VERSION,
                                    .id = {"fixture.observe", 15},
                                    .name = {"Observe", 7},
                                    .version = {"1", 1},
                                    .configuration_schema = {"{}", 2},
                                    .category = UD_COMPONENT_DESTINATION,
                                    .inputs = 1,
                                    .synchronous_input = -1,
                                    .error_output = -1,
                                    .capabilities = UD_COMPONENT_THREAD_SAFE |
                                                    UD_COMPONENT_STREAM_SAFE |
                                                    UD_COMPONENT_CONTROL_EVENTS,
                                    .configure = configure_sink,
                                    .finish = finish,
                                    .dispose = dispose_sink,
                                    .prepare = prepare_sink,
                                    .step = sink_step,
                                    .user = &f};
    OK(ud_registry_register(registry, &sink));
    const char *json =
        "{\"version\":12,\"id\":\"stream-contract\",\"connections\":[],\"variables\":{},"
        "\"parameters\":{},\"dataFlows\":[{\"id\":\"main\",\"components\":[{\"id\":\"source\","
        "\"component\":\"fixture.continuous\",\"config\":{}},{\"id\":\"window\",\"component\":"
        "\"eventWindow\",\"config\":{\"mode\":\"tumbling\",\"timeField\":\"time\",\"sizeMicros\":"
        "10,\"latePolicy\":\"fail\",\"plan\":{\"version\":12,\"group\":[\"key\"],\"aggregates\":[{"
        "\"function\":\"sum\",\"column\":\"value\",\"name\":\"total\"}]}}},{\"id\":\"sink\","
        "\"component\":\"fixture.observe\",\"config\":{}}],\"paths\":[{\"from\":\"source\","
        "\"output\":0,\"to\":\"window\",\"input\":0},{\"from\":\"window\",\"output\":0,\"to\":"
        "\"sink\",\"input\":0}]}],\"controlFlow\":{\"tasks\":[{\"id\":\"run\",\"kind\":"
        "\"dataFlow\",\"config\":{\"pipeline\":\"main\"}}],\"constraints\":[]}}";
    ud_execution *e = NULL;
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .registry = registry,
                              .authorize = allow,
                              .tenant = {"fixture", 7},
                              .max_batch_rows = 2,
                              .edge_capacity = 1,
                              .max_concurrency = 2,
                              .max_depth = 16,
                              .context = {.struct_size = sizeof(ud_context_options),
                                          .abi_version = UD_ABI_VERSION,
                                          .memory_limit = 16777216}};
    OK(ud_execution_create_json((ud_string){json, (uint32_t)strlen(json)}, &o, &e));
    OK(ud_execution_start(e));
    CHECK(ud_execution_wait(e, 350) == UD_BUSY);
    CHECK(atomic_load(&f.polls) > 0 && atomic_load(&f.polls) <= 12);
    char *progress = NULL;
    uint32_t length = 0;
    OK(ud_execution_progress(e, &progress, &length));
    CHECK(strstr(progress, "\"idle\":true") && strstr(progress, "\"paths\":") &&
          strstr(progress, "\"boundedness\":1"));
    ud_free(progress);
    ud_execution_stop(e);
    OK(ud_execution_wait(e, 5000));
    ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
    OK(ud_execution_get_metrics(e, &m));
    CHECK(m.state == UD_EXEC_STOPPED && m.queued_batches == 0 && f.received == 3 && f.watermark);
    OK(ud_execution_destroy(e));
    f.received = 0;
    f.watermark = 0;
    OK(ud_execution_create_json((ud_string){json, (uint32_t)strlen(json)}, &o, &e));
    const char debug[] = "{\"definitionBreakpoints\":[[\"run\"]]}";
    OK(ud_execution_debug_configure(e, (ud_string){debug, sizeof(debug) - 1}));
    OK(ud_execution_start(e));
    CHECK(ud_execution_wait(e, 100) == UD_BUSY);
    ud_execution_stop(e);
    OK(ud_execution_wait(e, 5000));
    OK(ud_execution_get_metrics(e, &m));
    CHECK(m.state == UD_EXEC_STOPPED && f.received == 0);
    OK(ud_execution_destroy(e));
    f.received = 0;
    f.watermark = 0;
    atomic_store(&f.last_watermark, INT64_MIN);
    const char *policy =
        "{\"version\":12,\"id\":\"stream-contract\",\"connections\":[],\"variables\":{},"
        "\"parameters\":{},\"dataFlows\":[{\"id\":\"main\",\"components\":[{\"id\":\"source\","
        "\"component\":\"fixture.continuous\",\"config\":{}},{\"id\":\"clock\",\"component\":"
        "\"eventTime\",\"config\":{\"field\":\"time\",\"lagMicros\":0}},{\"id\":\"window\","
        "\"component\":\"eventWindow\",\"config\":{\"mode\":\"tumbling\",\"timeField\":\"time\","
        "\"sizeMicros\":10,\"latePolicy\":\"fail\",\"plan\":{\"version\":12,\"group\":[\"key\"],"
        "\"aggregates\":[{\"function\":\"sum\",\"column\":\"value\",\"name\":\"total\"}]}}},{"
        "\"id\":\"sink\",\"component\":\"fixture.observe\",\"config\":{}}],\"paths\":[{\"from\":"
        "\"source\",\"output\":0,\"to\":\"clock\",\"input\":0},{\"from\":\"clock\",\"output\":0,"
        "\"to\":\"window\",\"input\":0},{\"from\":\"window\",\"output\":0,\"to\":\"sink\","
        "\"input\":0}]}],\"controlFlow\":{\"tasks\":[{\"id\":\"run\",\"kind\":\"dataFlow\","
        "\"config\":{\"pipeline\":\"main\"}}],\"constraints\":[]}}";
    OK(ud_execution_create_json((ud_string){policy, (uint32_t)strlen(policy)}, &o, &e));
    OK(ud_execution_start(e));
    CHECK(ud_execution_wait(e, 350) == UD_BUSY);
    CHECK(f.received == 2 && atomic_load(&f.last_watermark) == 20);
    ud_execution_stop(e);
    OK(ud_execution_wait(e, 5000));
    CHECK(f.received == 3);
    OK(ud_execution_destroy(e));
    // The other source has already advanced to 40 when the first emits END.
    // No subsequent input Watermark is available to unstick the minimum.
    fixture early = {.source_watermark = 5, .end_on_minimum = &f.minimum_seen};
    source_descriptor.id = (ud_string){"fixture.early", 13};
    source_descriptor.user = &early;
    OK(ud_registry_register(registry, &source_descriptor));
    for (int scenario = 0; scenario < 4; scenario++) {
        int join = scenario >= 2;
        early.idle_on_minimum = scenario % 2;
        f.received = 0;
        f.watermark = 0;
        f.expected_rows = join ? 7 : 6;
        atomic_store(&f.minimum_seen, 0);
        const char *format =
            "{\"version\":12,\"id\":\"multi-input-frontier\",\"connections\":[],\"controlFlow\":{"
            "\"tasks\":[{\"id\":\"run\",\"kind\":\"dataFlow\",\"config\":{\"pipeline\":\"main\"}}],"
            "\"constraints\":[]},\"dataFlows\":[{\"id\":\"main\",\"components\":[{\"id\":\"early\","
            "\"component\":\"fixture.early\",\"config\":{}},{\"id\":\"source\",\"component\":"
            "\"fixture."
            "continuous\",\"config\":{}},{\"id\":\"union\",\"component\":\"%s\",\"config\":%s"
            "},{"
            "\"id\":\"sink\",\"component\":\"fixture.observe\",\"config\":{}}],\"paths\":[{"
            "\"from\":"
            "\"early\",\"output\":0,\"to\":\"union\",\"input\":0},{\"from\":\"source\",\"output\":"
            "0,"
            "\"to\":\"union\",\"input\":1},{\"from\":\"union\",\"output\":0,\"to\":\"sink\","
            "\"input\":"
            "0}]}]}";
        char multi[1536];
        int size = snprintf(multi, sizeof(multi), format, join ? "eventJoin" : "unionAll",
                            join ? "{\"leftKeys\":[\"key\"],\"rightKeys\":[\"key\"],"
                                   "\"leftTimeField\":\"time\",\"rightTimeField\":\"time\","
                                   "\"withinMicros\":10}" : "{}");
        CHECK(size > 0 && (size_t)size < sizeof(multi));
        OK(ud_execution_create_json((ud_string){multi, (uint32_t)size}, &o, &e));
        OK(ud_execution_start(e));
        uint64_t deadline = monotonic_ms() + 5000;
        while (!atomic_load(&f.watermark) && monotonic_ms() < deadline)
            CHECK(ud_execution_wait(e, 25) == UD_BUSY);
        CHECK(atomic_load(&f.minimum_seen) && f.watermark && f.received == f.expected_rows);
        ud_execution_stop(e);
        OK(ud_execution_wait(e, 5000));
        OK(ud_execution_destroy(e));
    }
    OK(ud_registry_destroy(registry));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    puts("stream protocol: drain before Watermark, deferred idle source, shared progress and "
         "graceful stop passed");
    return 0;
}
