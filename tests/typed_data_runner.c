#include "check.h"
#include "ud/format.h"
#include "../components/foundation/internal.h"
int main(int argc, char **argv) {
    CHECK(argc == 5);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_bytes schema = {.ctx = ctx}, data = {.ctx = ctx};
    OK(ud_read_file(ctx, argv[1], &schema));
    OK(ud_read_file(ctx, argv[2], &data));
    CHECK(schema.size <= UINT32_MAX && data.size <= UINT32_MAX);
    ud_batch *b = NULL;
    OK(ud_batch_from_json_schema(ctx, (char *)schema.data, (uint32_t)schema.size, (char *)data.data,
                                 (uint32_t)data.size, UD_ROUND_EXACT, &b));
    char *json = NULL;
    uint32_t length;
    OK(ud_batch_to_json(b, &json, &length));
    OK(ud_write_file(ctx, argv[3], json, length));
    ud_free(json);
    uint8_t *wire = NULL;
    uint64_t bytes;
    OK(ud_batch_serialize(b, &wire, &bytes));
    OK(ud_write_file(ctx, argv[4], wire, (size_t)bytes));
    ud_batch *restored = NULL;
    OK(ud_batch_deserialize(ctx, wire, bytes, &restored));
    CHECK(ud_batch_rows(restored) == ud_batch_rows(b));
    ud_batch_destroy(restored);
    ud_free(wire);
    ud_batch_destroy(b);
    ud_bytes_clear(&schema);
    ud_bytes_clear(&data);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
