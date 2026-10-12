#include "check.h"
#include "ud/format.h"
#include <limits.h>
static ud_status read_document(ud_context *ctx, const char *path, char **text, uint32_t *length) {
    FILE *file = fopen(path, "rb");
    if (!file)
        return UD_IO;
    ud_status status = UD_IO;
    if (!fseek(file, 0, SEEK_END)) {
        long bytes = ftell(file);
        if (bytes >= 0 && bytes <= 64 * 1024 * 1024 && !fseek(file, 0, SEEK_SET)) {
            *text = ud_alloc(ctx, (size_t)bytes + 1);
            if (*text) {
                status = fread(*text, 1, (size_t)bytes, file) == (size_t)bytes ? UD_OK : UD_IO;
                (*text)[bytes] = 0;
                *length = (uint32_t)bytes;
            } else {
                status = UD_RESOURCE;
            }
        }
    }
    if (fclose(file) && !status)
        status = UD_IO;
    return status;
}
static ud_status write_document(const char *path, const void *data, size_t bytes) {
    FILE *file = fopen(path, "wb");
    if (!file)
        return UD_IO;
    ud_status status = fwrite(data, 1, bytes, file) == bytes ? UD_OK : UD_IO;
    if (fclose(file) && !status)
        status = UD_IO;
    return status;
}
int main(int argc, char **argv) {
    CHECK(argc == 5);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    char *schema = NULL, *data = NULL;
    uint32_t schema_length = 0, data_length = 0;
    OK(read_document(ctx, argv[1], &schema, &schema_length));
    OK(read_document(ctx, argv[2], &data, &data_length));
    ud_batch *b = NULL;
    OK(ud_batch_from_json_schema(ctx, schema, schema_length, data, data_length, UD_ROUND_EXACT, &b));
    char *json = NULL;
    uint32_t length;
    OK(ud_batch_to_json(b, &json, &length));
    OK(write_document(argv[3], json, length));
    ud_free(json);
    uint8_t *wire = NULL;
    uint64_t bytes;
    OK(ud_batch_serialize(b, &wire, &bytes));
    CHECK(bytes <= SIZE_MAX);
    OK(write_document(argv[4], wire, (size_t)bytes));
    ud_batch *restored = NULL;
    OK(ud_batch_deserialize(ctx, wire, bytes, &restored));
    CHECK(ud_batch_rows(restored) == ud_batch_rows(b));
    ud_batch_destroy(restored);
    ud_free(wire);
    ud_batch_destroy(b);
    ud_free(schema);
    ud_free(data);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
