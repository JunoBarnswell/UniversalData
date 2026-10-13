#include "ud/format.h"
#include "check.h"
#include <string.h>
static ud_field scalar(const char *name, ud_type type, int nullable) {
    ud_field f = {0};
    f.name = name;
    f.name_length = (uint32_t)strlen(name);
    f.type = type;
    f.nullable = nullable;
    return f;
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_field children[2] = {scalar("name", UD_UTF8, 0), scalar("score", UD_INT64, 1)};
    ud_field item = scalar("item", UD_UTF8, 1);
    ud_field pair[2] = {scalar("key", UD_UTF8, 0), scalar("value", UD_INT64, 1)};
    ud_field entry = scalar("entries", UD_STRUCT, 0);
    entry.children = pair;
    entry.child_count = 2;
    ud_field f[8] = {scalar("id", UD_INT64, 0),       scalar("amount", UD_DECIMAL128, 0),
                     scalar("at", UD_TIMESTAMP, 0),   scalar("raw", UD_BINARY, 0),
                     scalar("profile", UD_STRUCT, 1), scalar("tags", UD_LIST, 0),
                     scalar("map", UD_MAP, 0),        scalar("missing", UD_UTF8, 1)};
    f[1].precision = 38;
    f[1].scale = 2;
    f[2].timezone = "Asia/Shanghai";
    f[2].timezone_length = 13;
    f[4].children = children;
    f[4].child_count = 2;
    f[5].children = &item;
    f[5].child_count = 1;
    f[6].children = &entry;
    f[6].child_count = 1;
    const char *row =
        "{\"id\":9223372036854775807,\"amount\":123456789012345678901234567890123456.78,"
        "\"at\":\"2024-02-29T08:00:00+08:00\",\"raw\":\"AP9B\",\"profile\":{\"name\":"
        "\"中文\\u0000\"},"
        "\"tags\":[\"one\",null],\"map\":[{\"key\":\"x\",\"value\":12}]}";
    char input[1024];
    snprintf(input, sizeof(input), "[%s]", row);
    ud_batch *batch = NULL;
    OK(ud_batch_from_json(ctx, f, 8, input, (uint32_t)strlen(input), UD_ROUND_EXACT, &batch));
    ud_value v;
    OK(ud_batch_get(batch, 0, 0, &v));
    CHECK(v.integer == INT64_MAX);
    OK(ud_batch_get(batch, 0, 3, &v));
    CHECK(v.text_length == 3 && !memcmp(v.text,
                                        "\0\xff"
                                        "A",
                                        3));
    OK(ud_batch_get(batch, 0, 4, &v));
    CHECK(v.children[1].is_null && v.children[0].text_length == 7);
    OK(ud_batch_get(batch, 0, 7, &v));
    CHECK(v.is_null);
    char *json = NULL;
    uint32_t length = 0;
    OK(ud_batch_to_json(batch, &json, &length));
    CHECK(strstr(json, "\"123456789012345678901234567890123456.78\"") &&
          strstr(json, "2024-02-29T00:00:00.000000Z"));
    ud_batch *copy = NULL;
    OK(ud_batch_from_json(ctx, f, 8, json, length, UD_ROUND_EXACT, &copy));
    char *schema_json = NULL;
    uint32_t schema_length = 0;
    OK(ud_schema_to_json(batch, &schema_json, &schema_length));
    ud_batch *bound = NULL;
    OK(ud_batch_from_json_schema(ctx, schema_json, schema_length, json, length, UD_ROUND_EXACT,
                                 &bound));
    CHECK(ud_batch_rows(bound) == 1 && ud_batch_columns(bound) == 8);
    ud_batch_destroy(bound);
    ud_free(schema_json);
    uint8_t *wire1 = NULL, *wire2 = NULL;
    uint64_t n1 = 0, n2 = 0;
    OK(ud_batch_serialize(batch, &wire1, &n1));
    OK(ud_batch_serialize(copy, &wire2, &n2));
    CHECK(n1 == n2 && !memcmp(wire1, wire2, (size_t)n1));
    ud_json_writer *writer = NULL;
    OK(ud_json_writer_open(ctx, argv[1], f, 8, 10000, &writer));
    OK(ud_json_writer_append(writer, batch));
    OK(ud_json_writer_append(writer, copy));
    OK(ud_json_writer_close(writer));
    ud_json_reader *nested_reader = NULL;
    ud_json_options nested_options = {sizeof(nested_options), UD_CONFIG_VERSION, 2, 10000,
                                      UD_ROUND_EXACT};
    OK(ud_json_reader_open(ctx, argv[1], f, 8, &nested_options, &nested_reader));
    ud_batch *two = NULL;
    OK(ud_json_reader_next(nested_reader, &two, NULL));
    CHECK(ud_batch_rows(two) == 2);
    ud_batch_destroy(two);
    ud_json_reader_close(nested_reader);
    OK(ud_json_writer_open(ctx, argv[1], f, 8, 10, &writer));
    CHECK(ud_json_writer_append(writer, batch) == UD_RESOURCE);
    CHECK(ud_json_writer_close(writer) == UD_RESOURCE);
    ud_free(wire1);
    ud_free(wire2);
    ud_free(json);
    ud_batch_destroy(copy);
    ud_batch_destroy(batch);
    const char *bad[] = {"[{\"raw\":\"AB==\"}]", "[{\"raw\":\"AAB=\"}]",  "[{\"raw\":\"AA=A\"}]",
                         "[{\"raw\":\"A===\"}]", "[{\"raw\":\"AAAA=\"}]", "[{\"other\":1}]"};
    for (uint32_t i = 0; i < sizeof(bad) / sizeof(*bad); i++) {
        CHECK(ud_batch_from_json(ctx, f + 3, 1, bad[i], (uint32_t)strlen(bad[i]), UD_ROUND_EXACT,
                                 &batch) != UD_OK);
        CHECK(!batch && !ud_memory_used(ctx));
    }
    ud_field id = scalar("id", UD_INT64, 0);
    FILE *file = fopen(argv[1], "wb");
    CHECK(file);
    CHECK(fputs("{\"id\":1}\r\n{\"id\":2}\n{\"id\":3}", file) >= 0);
    CHECK(!fclose(file));
    ud_json_options options = {sizeof(options), UD_CONFIG_VERSION, 1, 100, UD_ROUND_EXACT};
    ud_json_reader *reader = NULL;
    OK(ud_json_reader_open(ctx, argv[1], &id, 1, &options, &reader));
    OK(ud_json_reader_next(reader, &batch, NULL));
    ud_batch_destroy(batch);
    uint64_t offset = ud_json_reader_offset(reader);
    CHECK(ud_json_reader_record(reader) == 1);
    OK(ud_json_reader_next(reader, &batch, NULL));
    OK(ud_batch_get(batch, 0, 0, &v));
    CHECK(v.integer == 2);
    ud_batch_destroy(batch);
    OK(ud_json_reader_resume(reader, offset, 1));
    OK(ud_json_reader_next(reader, &batch, NULL));
    OK(ud_batch_get(batch, 0, 0, &v));
    CHECK(v.integer == 2);
    ud_batch_destroy(batch);
    OK(ud_json_reader_next(reader, &batch, NULL));
    OK(ud_batch_get(batch, 0, 0, &v));
    CHECK(v.integer == 3);
    ud_batch_destroy(batch);
    CHECK(ud_json_reader_next(reader, &batch, NULL) == UD_END && !batch);
    CHECK(ud_json_reader_resume(reader, 1, 1) == UD_INVALID);
    ud_json_reader_close(reader);
    CHECK(!remove(argv[1]));
    CHECK(!ud_memory_used(ctx));
    OK(ud_context_destroy(ctx));
    puts("nested JSON and bounded NDJSON fixtures passed");
    return 0;
}
