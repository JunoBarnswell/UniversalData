#include "check.h"
#include "ud/operator.h"
#include <inttypes.h>
#include <string.h>

static const ud_field fields[] = {{.name = "time", .name_length = 4, .type = UD_INT64},
                                  {.name = "key", .name_length = 3, .type = UD_INT64},
                                  {.name = "value", .name_length = 5, .type = UD_INT64}};
static const char configuration[] =
    "{\"timeField\":\"time\",\"sizeMicros\":64,\"allowedLatenessMicros\":4,"
    "\"maxWindows\":4096,\"stateBytes\":8388608,\"latePolicy\":\"drop\",\"plan\":{"
    "\"version\":12,\"group\":[\"key\"],\"aggregates\":[{\"function\":\"sum\","
    "\"column\":\"value\",\"name\":\"total\"}]}}";

static int read_checkpoint(ud_context *ctx, ud_operator **op, const char *path) {
    FILE *file = fopen(path, "rb");
    CHECK(file);
    CHECK(fseek(file, 0, SEEK_END) == 0);
    long length = ftell(file);
    CHECK(length > 0 && fseek(file, 0, SEEK_SET) == 0);
    uint8_t *wire = malloc((size_t)length);
    CHECK(wire);
    CHECK(fread(wire, 1, (size_t)length, file) == (size_t)length);
    CHECK(fclose(file) == 0);
    ud_batch *snapshot = NULL;
    OK(ud_batch_deserialize(ctx, wire, (uint64_t)length, &snapshot));
    free(wire);
    OK(ud_operator_restore(op, snapshot));
    ud_batch_destroy(snapshot);
    return 0;
}
static int save_checkpoint(ud_operator *op, const char *path, uint32_t *active) {
    ud_batch *snapshot = NULL;
    OK(ud_operator_checkpoint(op, &snapshot));
    *active = ud_batch_rows(snapshot) - 1;
    uint8_t *wire = NULL;
    uint64_t length = 0;
    OK(ud_batch_serialize(snapshot, &wire, &length));
    FILE *file = fopen(path, "wb");
    CHECK(file);
    CHECK(fwrite(wire, 1, (size_t)length, file) == length);
    CHECK(fclose(file) == 0);
    ud_free(wire);
    ud_batch_destroy(snapshot);
    return 0;
}
static int push_rows(ud_context *ctx, ud_operator *op, uint32_t count) {
    ud_batch *batch = NULL;
    OK(ud_batch_create(ctx, fields, 3, count, &batch));
    for (uint32_t i = 0; i < count; i++) {
        int64_t time, key, value;
        CHECK(scanf("%" SCNd64 " %" SCNd64 " %" SCNd64, &time, &key, &value) == 3);
        ud_value row[] = {{.type = UD_INT64, .integer = time},
                          {.type = UD_INT64, .integer = key},
                          {.type = UD_INT64, .integer = value}};
        OK(ud_batch_append(batch, row, 3));
    }
    OK(ud_batch_seal(batch));
    OK(ud_operator_push(op, 0, batch));
    ud_batch_destroy(batch);
    return 0;
}
static int drain(ud_operator *op, ud_status boundary, uint64_t *blocked) {
    for (;;) {
        if (!ud_operator_input_demand(op))
            (*blocked)++;
        ud_batch *batch = NULL;
        ud_status s = ud_operator_pull(op, &batch, NULL);
        if (s == boundary)
            return 0;
        OK(s);
        for (uint32_t r = 0; r < ud_batch_rows(batch); r++) {
            ud_value row[4];
            for (uint32_t c = 0; c < 4; c++)
                OK(ud_batch_get(batch, r, c, row + c));
            printf("window %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 "\n",
                   row[0].integer, row[1].integer, row[2].integer, row[3].integer);
        }
        ud_batch_destroy(batch);
        fflush(stdout);
        /* Acknowledgment from the external consumer controls every output pull. */
        char ack;
        CHECK(scanf(" %c", &ack) == 1 && ack == 'a');
    }
}
int main(int argc, char **argv) {
    CHECK(argc == 3);
    ud_context *ctx = NULL;
    OK(ud_context_create(&(ud_context_options){.struct_size = sizeof(ud_context_options),
                                               .abi_version = UD_ABI_VERSION,
                                               .memory_limit = 67108864}, &ctx));
    ud_batch *schema = NULL;
    OK(ud_batch_create(ctx, fields, 3, 0, &schema));
    const ud_batch *schemas[] = {schema};
    ud_operator *op = NULL;
    ud_operator_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION,
                                   .kind = UD_OPERATOR_EVENT_WINDOW, .max_batch_rows = 64,
                                   .configuration = {configuration, sizeof(configuration) - 1}};
    OK(ud_operator_create(ctx, &options, schemas, &op));
    ud_batch_destroy(schema);
    int restoring = !strcmp(argv[2], "restore");
    if (restoring)
        CHECK(!read_checkpoint(ctx, &op, argv[1]));
    printf("ready %d\n", restoring);
    fflush(stdout);
    char command;
    while (scanf(" %c", &command) == 1) {
        uint64_t blocked = 0;
        if (command == 'q') {
            OK(ud_operator_finish_input(op, 0));
            CHECK(!drain(op, UD_END, &blocked));
            puts("done");
            fflush(stdout);
            break;
        }
        CHECK(command == 'b');
        uint32_t count;
        int64_t watermark;
        CHECK(scanf("%u %" SCNd64, &count, &watermark) == 2 && count > 0 && count <= 64);
        CHECK(!push_rows(ctx, op, count));
        OK(ud_operator_watermark(op, watermark));
        CHECK(!drain(op, UD_BUSY, &blocked));
        uint32_t active = 0;
        CHECK(!save_checkpoint(op, argv[1], &active));
        char *diagnostics = NULL;
        uint32_t length;
        OK(ud_operator_diagnostics(op, &diagnostics, &length));
        printf("checkpoint %u %" PRIu64 " %s\n", active, blocked, diagnostics);
        fflush(stdout);
        ud_free(diagnostics);
    }
    OK(ud_operator_destroy(op));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
