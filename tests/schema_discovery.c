#include "check.h"
#include "ud/csv.h"
#include "ud/format.h"
#include <string.h>
static int fixture(const char *path, const char *data) {
    FILE *file = fopen(path, "wb");
    if (!file)
        return 1;
    int failed = fwrite(data, 1, strlen(data), file) != strlen(data);
    return fclose(file) || failed;
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_csv_options csv = {
        .struct_size = sizeof(csv), .abi_version = UD_ABI_VERSION, .max_record_bytes = 65536};
    ud_batch *schema = NULL;
    CHECK(!fixture(argv[1], "id,amount,code,day\n18446744073709551615,1.125,001,2026-10-"
                            "13\n18446744073709551614,\\N,002,2026-10-14\n"));
    OK(ud_csv_discover(ctx, argv[1], (ud_string){"fixture", 7}, &csv, 256, &schema));
    ud_field field;
    OK(ud_batch_field(schema, 0, &field));
    CHECK(field.type == UD_UINT64);
    OK(ud_batch_field(schema, 1, &field));
    CHECK(field.type == UD_DECIMAL128 && field.scale == 3 && field.nullable);
    OK(ud_batch_field(schema, 2, &field));
    CHECK(field.type == UD_UTF8);
    OK(ud_batch_field(schema, 3, &field));
    CHECK(field.type == UD_DATE32);
    ud_batch_destroy(schema);
    CHECK(!fixture(argv[1],
                   "[{\"id\":18446744073709551615,\"text\":\"first\"},{\"id\":3,\"extra\":true}]"));
    ud_json_options json = {.struct_size = sizeof(json),
                            .version = UD_CONFIG_VERSION,
                            .batch_size = 4,
                            .max_record_bytes = 65536,
                            .framing = UD_JSON_FRAMING_ARRAY};
    OK(ud_json_discover(ctx, argv[1], (ud_string){"fixture", 7}, &json, 256, &schema));
    OK(ud_batch_field(schema, 0, &field));
    CHECK(field.type == UD_DECIMAL128);
    OK(ud_batch_field(schema, 1, &field));
    CHECK(field.type == UD_UTF8 && field.nullable);
    OK(ud_batch_field(schema, 2, &field));
    CHECK(field.type == UD_BOOL && field.nullable);
    ud_batch_destroy(schema);
    CHECK(!fixture(argv[1], "[{\"id\":1},{\"id\":\"text\"}]"));
    CHECK(ud_json_discover(ctx, argv[1], (ud_string){"fixture", 7}, &json, 256, &schema) ==
          UD_INCOMPATIBLE);
    CHECK(!schema);
    CHECK(!remove(argv[1]));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
