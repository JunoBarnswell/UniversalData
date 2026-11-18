#include "check.h"
#include "ud/operator.h"
#include <string.h>

static ud_context *ctx;
static ud_batch *schema;
static const ud_field fields[] = {{.name = "time", .name_length = 4, .type = UD_INT64},
                                  {.name = "key", .name_length = 3, .type = UD_INT64}};
static int create(int kind, uint64_t bytes, ud_operator **op) {
    char config[256];
    int n = kind == UD_OPERATOR_EVENT_JOIN
                ? snprintf(config, sizeof(config),
                           "{\"leftKeys\":[\"key\"],\"rightKeys\":[\"key\"],\"leftTimeField\":"
                           "\"time\",\"rightTimeField\":\"time\",\"withinMicros\":5,\"stateBytes\":%llu}",
                           (unsigned long long)bytes)
                : snprintf(config, sizeof(config),
                           "{\"keys\":[\"key\"],\"timeField\":\"time\",\"ttlMicros\":5,"
                           "\"stateBytes\":%llu}", (unsigned long long)bytes);
    const ud_batch *schemas[] = {schema, schema};
    OK(ud_operator_create(ctx, &(ud_operator_options){.struct_size = sizeof(ud_operator_options),
                            .abi_version = UD_ABI_VERSION, .kind = kind, .max_batch_rows = 1,
                            .configuration = {config, (uint32_t)n}}, schemas, op));
    return 0;
}
static ud_status send(ud_operator *op, uint32_t port, int64_t key, const int64_t *times, uint32_t n) {
    ud_batch *input = NULL;
    OK(ud_batch_create(ctx, fields, 2, n, &input));
    for (uint32_t i = 0; i < n; i++) {
        ud_value values[] = {{.type = UD_INT64, .integer = times[i]},
                             {.type = UD_INT64, .integer = key}};
        OK(ud_batch_append(input, values, 2));
        ud_row_info row = {.id = {port + 1, (uint64_t)times[i] + 1000}};
        OK(ud_batch_set_row_info(input, i, &row));
    }
    OK(ud_batch_seal(input));
    ud_status status = ud_operator_push(op, port, input);
    ud_batch_destroy(input);
    return status;
}
static int receive(ud_operator *op, int64_t left, int64_t right, uint8_t *digest) {
    ud_batch *batch = NULL;
    OK(ud_operator_pull(op, &batch, NULL));
    CHECK(ud_batch_rows(batch) == 1);
    ud_row_info row;
    OK(ud_batch_row_info(batch, 0, &row));
    CHECK(row.id.high || row.id.low);
    ud_value value;
    OK(ud_batch_get(batch, 0, 0, &value));
    CHECK(value.integer == left);
    if (right != INT64_MIN) {
        CHECK(row.flags & UD_ROW_JOINED);
        OK(ud_batch_get(batch, 0, 2, &value));
        CHECK(value.integer == right);
    }
    if (digest)
        OK(ud_batch_fingerprint(batch, digest));
    ud_batch_destroy(batch);
    return 0;
}
static int dedup(void) {
    ud_operator *op = NULL, *restored = NULL;
    ud_batch *batch = NULL;
    CHECK(!create(UD_OPERATOR_EVENT_DEDUPLICATE, 1048576, &op));
    CHECK(!send(op, 0, 1, (int64_t[]){10, 11, 9}, 3));
    CHECK(ud_operator_input_demand(op) == 0);
    CHECK(ud_operator_checkpoint(op, &batch) == UD_BUSY);
    CHECK(!receive(op, 10, INT64_MIN, NULL));
    CHECK(ud_operator_pull(op, &batch, NULL) == UD_BUSY);
    CHECK(ud_operator_input_demand(op) == 1);
    OK(ud_operator_watermark(op, 14));
    OK(ud_operator_checkpoint(op, &batch));
    CHECK(ud_batch_rows(batch) == 2);
    CHECK(!create(UD_OPERATOR_EVENT_DEDUPLICATE, 1048576, &restored));
    OK(ud_operator_restore(&restored, batch));
    ud_batch_destroy(batch);
    CHECK(ud_operator_watermark(restored, 13) == UD_FENCED);
    CHECK(!send(restored, 0, 1, (int64_t[]){15}, 1));
    CHECK(ud_operator_pull(restored, &batch, NULL) == UD_BUSY);
    OK(ud_operator_watermark(restored, 15));
    CHECK(!send(restored, 0, 1, (int64_t[]){15, 16}, 2));
    CHECK(!receive(restored, 16, INT64_MIN, NULL));
    OK(ud_operator_finish_input(restored, 0));
    CHECK(ud_operator_pull(restored, &batch, NULL) == UD_END);
    OK(ud_operator_destroy(restored));
    OK(ud_operator_destroy(op));
    return 0;
}
static int join(void) {
    ud_operator *op = NULL, *restored = NULL;
    ud_batch *batch = NULL;
    CHECK(!create(UD_OPERATOR_EVENT_JOIN, 1048576, &op));
    CHECK(ud_operator_input_demand(op) == 3);
    CHECK(!send(op, 0, 3, (int64_t[]){10, 12, 20}, 3));
    CHECK(ud_operator_pull(op, &batch, NULL) == UD_BUSY);
    CHECK(!send(op, 1, 4, (int64_t[]){11}, 1));
    CHECK(ud_operator_pull(op, &batch, NULL) == UD_BUSY);
    OK(ud_operator_watermark(op, 9));
    OK(ud_operator_checkpoint(op, &batch));
    CHECK(ud_batch_rows(batch) == 5);
    uint8_t *wire = NULL;
    uint64_t bytes = 0;
    OK(ud_batch_serialize(batch, &wire, &bytes));
    ud_batch_destroy(batch);
    OK(ud_batch_deserialize(ctx, wire, bytes, &batch));
    ud_free(wire);
    CHECK(!create(UD_OPERATOR_EVENT_JOIN, 1048576, &restored));
    OK(ud_operator_restore(&restored, batch));
    ud_batch_destroy(batch);
    CHECK(!send(op, 1, 3, (int64_t[]){15}, 1));
    CHECK(!send(restored, 1, 3, (int64_t[]){15}, 1));
    CHECK(ud_operator_watermark(restored, 10) == UD_BUSY);
    for (int64_t time = 20; time; time = time == 20 ? 12 : time == 12 ? 10 : 0) {
        uint8_t x[32], y[32];
        CHECK(!receive(op, time, 15, x));
        CHECK(!receive(restored, time, 15, y));
        CHECK(!memcmp(x, y, 32));
    }
    CHECK(ud_operator_pull(op, &batch, NULL) == UD_BUSY);
    CHECK(ud_operator_pull(restored, &batch, NULL) == UD_BUSY);
    OK(ud_operator_watermark(restored, 15));
    OK(ud_operator_checkpoint(restored, &batch));
    CHECK(ud_batch_rows(batch) == 5);
    ud_batch_destroy(batch);
    OK(ud_operator_finish_input(restored, 1));
    CHECK(ud_operator_input_demand(restored) == 1);
    CHECK(!send(restored, 0, 3, (int64_t[]){15, 16}, 2));
    CHECK(!receive(restored, 16, 15, NULL));
    CHECK(ud_operator_pull(restored, &batch, NULL) == UD_BUSY);
    OK(ud_operator_finish_input(restored, 0));
    CHECK(ud_operator_pull(restored, &batch, NULL) == UD_END);
    OK(ud_operator_destroy(restored));
    OK(ud_operator_destroy(op));
    return 0;
}
static int limit(void) {
    ud_operator *op = NULL;
    CHECK(!create(UD_OPERATOR_EVENT_JOIN, 4096, &op));
    int exhausted = 0;
    for (int64_t i = 1; i < 64 && !exhausted; i++) {
        ud_batch *result = NULL;
        ud_status s = send(op, 0, 3, &i, 1);
        if (!s)
            s = ud_operator_pull(op, &result, NULL);
        CHECK(s == UD_BUSY || s == UD_RESOURCE);
        exhausted = s == UD_RESOURCE;
    }
    CHECK(exhausted);
    OK(ud_operator_destroy(op));
    return 0;
}
int main(void) {
    OK(ud_context_create(&(ud_context_options){.struct_size = sizeof(ud_context_options),
                                               .abi_version = UD_ABI_VERSION,
                                               .memory_limit = 67108864}, &ctx));
    OK(ud_batch_create(ctx, fields, 2, 0, &schema));
    CHECK(!dedup());
    CHECK(!join());
    CHECK(!limit());
    ud_batch_destroy(schema);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
