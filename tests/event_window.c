#include "check.h"
#include "ud/operator.h"
#include <string.h>
#include <inttypes.h>
#include <time.h>
#define UD_TRY(call)                                                                               \
    do {                                                                                           \
        ud_status s_ = (call);                                                                     \
        if (s_)                                                                                    \
            return s_;                                                                             \
    } while (0)
static ud_context *ctx;
static ud_buffer_manager *buffers;
static ud_field fields[] = {{.name = "time", .name_length = 4, .type = UD_INT64},
                            {.name = "key", .name_length = 3, .type = UD_INT64},
                            {.name = "value", .name_length = 5, .type = UD_INT64}};
static ud_status batch(const int64_t *times, uint32_t rows, ud_batch **out) {
    UD_TRY(ud_batch_create(ctx, fields, 3, rows, out));
    for (uint32_t i = 0; i < rows; i++) {
        ud_value v[] = {{.type = UD_INT64, .integer = times[i]},
                        {.type = UD_INT64, .integer = 1},
                        {.type = UD_INT64, .integer = 1}};
        ud_status s = ud_batch_append(*out, v, 3);
        if (s)
            return s;
    }
    return ud_batch_seal(*out);
}
static ud_status create(const char *mode, const char *late, uint64_t budget, ud_operator **out) {
    char json[1024];
    int n =
        snprintf(json, sizeof(json),
                 "{\"mode\":\"%s\",\"timeField\":\"time\",\"sizeMicros\":10,\"slideMicros\":5,"
                 "\"gapMicros\":5,\"allowedLatenessMicros\":2,\"stateBytes\":%llu,\"latePolicy\":"
                 "\"%s\",\"plan\":{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"group\":[\"key\"],\"aggregates\":[{"
                 "\"function\":\"sum\",\"column\":\"value\",\"name\":\"total\"}]}}",
                 mode, (unsigned long long)budget, late);
    ud_batch *schema = NULL;
    UD_TRY(ud_batch_create(ctx, fields, 3, 0, &schema));
    const ud_batch *inputs[] = {schema};
    ud_operator_options o = {.struct_size = sizeof(o),
                             .abi_version = UD_ABI_VERSION,
                             .kind = UD_OPERATOR_EVENT_WINDOW,
                             .max_batch_rows = 1024,
                             .configuration = {json, (uint32_t)n},
                             .buffers = buffers,
                             .memory_budget = budget};
    ud_status s = ud_operator_create(ctx, &o, inputs, out);
    ud_batch_destroy(schema);
    return s;
}
static int expect(ud_operator *o, int64_t start, int64_t end, int64_t sum) {
    ud_batch *b = NULL;
    uint32_t port = 99;
    OK(ud_operator_pull(o, &b, &port));
    CHECK(port == 0 && ud_batch_rows(b) == 1);
    ud_value v;
    OK(ud_batch_get(b, 0, 0, &v));
    CHECK(v.integer == start);
    OK(ud_batch_get(b, 0, 1, &v));
    CHECK(v.integer == end);
    OK(ud_batch_get(b, 0, 3, &v));
    CHECK(v.integer == sum);
    ud_batch_destroy(b);
    return 0;
}
static int push(ud_operator *o, const int64_t *times, uint32_t rows) {
    ud_batch *b = NULL;
    OK(batch(times, rows, &b));
    OK(ud_operator_push(o, 0, b));
    ud_batch_destroy(b);
    return 0;
}
static uint64_t wall_ns(void) {
    struct timespec now;
    timespec_get(&now, TIME_UTC);
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}
static int compare_ns(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}
int main(void) {
    OK(ud_context_create(&(ud_context_options){.struct_size = sizeof(ud_context_options),
                                               .abi_version = UD_ABI_VERSION,
                                               .memory_limit = 67108864},
                         &ctx));
    OK(ud_buffer_manager_create(ctx,
                                &(ud_buffer_options){.struct_size = sizeof(ud_buffer_options),
                                                     .abi_version = UD_ABI_VERSION,
                                                     .max_blocks = 8192,
                                                     .resident_limit = 1048576,
                                                     .working_limit = 16777216},
                                &buffers));
    ud_operator *o = NULL, *restored = NULL;
    ud_batch *state = NULL, *b = NULL;
    OK(create("tumbling", "redirect", 1048576, &o));
    CHECK(ud_buffer_manager_destroy(buffers) == UD_BUSY);
    CHECK(!push(o, (int64_t[]){-1, 0, 9, 10}, 4));
    ud_buffer_metrics working;
    OK(ud_buffer_manager_metrics(buffers, &working));
    CHECK(working.working_bytes > 0);
    OK(ud_operator_watermark(o, 11));
    CHECK(!expect(o, -10, 0, 1));
    CHECK(ud_operator_pull(o, &b, NULL) == UD_BUSY);
    OK(ud_operator_watermark(o, 12));
    CHECK(!expect(o, 0, 10, 2));
    CHECK(ud_operator_watermark(o, 11) == UD_FENCED);
    CHECK(!push(o, (int64_t[]){9}, 1));
    uint32_t port = 0;
    OK(ud_operator_pull(o, &b, &port));
    CHECK(port == 1 && ud_batch_rows(b) == 1);
    ud_batch_destroy(b);
    OK(ud_operator_checkpoint(o, &state));
    CHECK(ud_batch_rows(state) == 2);
    OK(create("tumbling", "redirect", 1048576, &restored));
    OK(ud_operator_restore(&restored, state));
    ud_batch_destroy(state);
    OK(ud_operator_watermark(restored, 22));
    CHECK(!expect(restored, 10, 20, 1));
    OK(ud_operator_destroy(restored));
    OK(ud_operator_destroy(o));
    OK(create("sliding", "drop", 1048576, &o));
    CHECK(!push(o, (int64_t[]){5, 9}, 2));
    OK(ud_operator_watermark(o, 12));
    CHECK(!expect(o, 0, 10, 2));
    OK(ud_operator_finish_input(o, 0));
    CHECK(!expect(o, 5, 15, 2));
    CHECK(ud_operator_pull(o, &b, NULL) == UD_END);
    OK(ud_operator_destroy(o));
    OK(create("session", "drop", 1048576, &o));
    CHECK(!push(o, (int64_t[]){0, 8, 4}, 3));
    OK(ud_operator_checkpoint(o, &state));
    OK(create("session", "drop", 1048576, &restored));
    OK(ud_operator_restore(&restored, state));
    ud_batch_destroy(state);
    OK(ud_operator_watermark(restored, 15));
    CHECK(!expect(restored, 0, 13, 3));
    OK(ud_operator_destroy(restored));
    OK(ud_operator_destroy(o));
    OK(create("tumbling", "fail", 1048576, &o));
    OK(ud_operator_watermark(o, 12));
    OK(batch((int64_t[]){9}, 1, &b));
    CHECK(ud_operator_push(o, 0, b) == UD_QUALITY);
    ud_batch_destroy(b);
    OK(ud_operator_destroy(o));
    // A restored compact state must emit the same order AND RowIds when
    // multiple keys close together, so receipt batches keep their identity.
    OK(create("tumbling", "drop", 1048576, &o));
    OK(ud_batch_create(ctx, fields, 3, 2, &b));
    for (int64_t key = 2; key >= 1; key--) {
        ud_value row[] = {{.type = UD_INT64, .integer = 1},
                          {.type = UD_INT64, .integer = key},
                          {.type = UD_INT64, .integer = key}};
        OK(ud_batch_append(b, row, 3));
    }
    OK(ud_batch_seal(b));
    OK(ud_operator_push(o, 0, b));
    ud_batch_destroy(b);
    OK(ud_operator_checkpoint(o, &state));
    OK(create("tumbling", "drop", 1048576, &restored));
    OK(ud_operator_restore(&restored, state));
    ud_batch_destroy(state);
    OK(ud_operator_watermark(o, 12));
    OK(ud_operator_watermark(restored, 12));
    for (int i = 0; i < 2; i++) {
        ud_batch *other = NULL;
        uint8_t left[32], right[32];
        OK(ud_operator_pull(o, &b, NULL));
        OK(ud_operator_pull(restored, &other, NULL));
        OK(ud_batch_fingerprint(b, left));
        OK(ud_batch_fingerprint(other, right));
        CHECK(!memcmp(left, right, 32));
        ud_batch_destroy(b);
        ud_batch_destroy(other);
    }
    OK(ud_operator_destroy(o));
    OK(ud_operator_destroy(restored));
    // Multiple keys close together; output remains ordered by typed key.
    OK(create("tumbling", "drop", 8388608, &o));
    OK(ud_batch_create(ctx, fields, 3, 1024, &b));
    for (int64_t key = 1023; key >= 0; key--) {
        ud_value row[] = {{.type = UD_INT64, .integer = 1},
                          {.type = UD_INT64, .integer = key},
                          {.type = UD_INT64, .integer = key}};
        OK(ud_batch_append(b, row, 3));
    }
    OK(ud_batch_seal(b));
    OK(ud_operator_push(o, 0, b));
    ud_batch_destroy(b);
    OK(ud_operator_watermark(o, 12));
    for (int64_t key = 0; key < 1024; key++) {
        CHECK(ud_operator_input_demand(o) == 0);
        CHECK(!expect(o, 0, 10, key));
    }
    CHECK(ud_operator_input_demand(o) == 1);
    OK(ud_operator_destroy(o));
    // One million actual input events. Only active window states survive checkpoints.
    OK(create("tumbling", "drop", 1048576, &o));
    uint64_t emitted = 0, latency[1000], started = wall_ns();
    for (uint32_t chunk = 0; chunk < 1000; chunk++) {
        uint64_t begin = wall_ns();
        int64_t times[1000];
        for (uint32_t i = 0; i < 1000; i++)
            times[i] = (int64_t)chunk * 1000 + i;
        CHECK(!push(o, times, 1000));
        OK(ud_operator_watermark(o, (int64_t)(chunk + 1) * 1000 + 2));
        for (;;) {
            ud_status s = ud_operator_pull(o, &b, NULL);
            if (s == UD_BUSY)
                break;
            OK(s);
            emitted++;
            ud_batch_destroy(b);
        }
        latency[chunk] = wall_ns() - begin;
    }
    uint64_t elapsed = wall_ns() - started;
    qsort(latency, 1000, sizeof(*latency), compare_ns);
    printf("{\"scenario\":\"1000-row batches / 10-microsecond keyed "
           "windows\",\"inputEvents\":1000000,\"emittedWindows\":100000,\"elapsedMs\":%.3f,"
           "\"eventsPerSecond\":%.3f,\"batchP95Ms\":%.3f,\"batchP99Ms\":%.3f}\n",
           (double)elapsed / 1e6, 1e15 / (double)elapsed, (double)latency[949] / 1e6,
           (double)latency[989] / 1e6);
    CHECK(emitted == 100000);
    OK(ud_operator_checkpoint(o, &state));
    CHECK(ud_batch_rows(state) == 1);
    ud_batch_destroy(state);
    OK(ud_operator_destroy(o));
    // Force working state to the existing Buffer Store and restore its typed snapshot.
    OK(create("tumbling", "drop", 16384, &o));
    int64_t times[20];
    for (uint32_t i = 0; i < 20; i++)
        times[i] = i * 10;
    CHECK(!push(o, times, 20));
    char *diagnostics = NULL;
    uint32_t length = 0;
    OK(ud_operator_diagnostics(o, &diagnostics, &length));
    CHECK(strstr(diagnostics, "\"spilledStates\":") && !strstr(diagnostics, "\"spilledStates\":0"));
    ud_free(diagnostics);
    OK(ud_operator_checkpoint(o, &state));
    OK(create("tumbling", "drop", 16384, &restored));
    OK(ud_operator_restore(&restored, state));
    ud_batch_destroy(state);
    OK(ud_operator_finish_input(restored, 0));
    for (uint32_t i = 0; i < 20; i++)
        CHECK(!expect(restored, i * 10, i * 10 + 10, 1));
    OK(ud_operator_destroy(restored));
    OK(ud_operator_destroy(o));
    ud_buffer_manager *shared = buffers, *small = NULL;
    OK(ud_buffer_manager_create(ctx,
                                &(ud_buffer_options){.struct_size = sizeof(ud_buffer_options),
                                                     .abi_version = UD_ABI_VERSION,
                                                     .max_blocks = 32,
                                                     .resident_limit = 1048576,
                                                     .working_limit = 4096},
                                &small));
    buffers = small;
    OK(create("tumbling", "drop", 16384, &o));
    void *held = NULL;
    OK(ud_buffer_working_allocate(small, 3500, &held));
    OK(batch((int64_t[]){0}, 1, &b));
    CHECK(ud_operator_push(o, 0, b) == UD_RESOURCE);
    ud_batch_destroy(b);
    ud_free(held);
    OK(ud_operator_destroy(o));
    OK(ud_buffer_manager_metrics(small, &working));
    CHECK(working.working_bytes == 0);
    OK(ud_buffer_manager_destroy(small));
    buffers = shared;
    OK(ud_buffer_manager_destroy(buffers));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    puts("event windows: boundaries, lateness, session merge, compact restore, spill and 1000000 "
         "events passed");
    return 0;
}
