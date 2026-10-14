#include "check.h"
#include "ud/format.h"
#include "ud/sql.h"
#include <string.h>

static int roundtrip(ud_context *ctx, const ud_field *field, const ud_value *value) {
    ud_batch *batch = NULL, *copy = NULL;
    OK(ud_batch_create(ctx, field, 1, 1, &batch));
    OK(ud_batch_append(batch, value, 1));
    uint8_t *wire = NULL;
    uint64_t length;
    OK(ud_batch_serialize(batch, &wire, &length));
    OK(ud_batch_deserialize(ctx, wire, length, &copy));
    ud_value actual;
    OK(ud_batch_get(copy, 0, 0, &actual));
    CHECK(!ud_value_compare(value, &actual));
    CHECK(ud_value_hash(value, UD_VALUE_HASH_SEED) == ud_value_hash(&actual, UD_VALUE_HASH_SEED));
    ud_batch_destroy(copy);
    ud_free(wire);
    struct ArrowArray array;
    struct ArrowSchema schema;
    OK(ud_batch_export_arrow(batch, &array, &schema));
    OK(ud_batch_import_arrow(ctx, &array, &schema, &copy));
    ud_field restored;
    OK(ud_batch_field(copy, 0, &restored));
    ud_field bound;
    OK(ud_batch_field(batch, 0, &bound));
    CHECK(ud_field_matches(&bound, &restored, UD_FIELD_VALUE_TYPE));
    OK(ud_batch_get(copy, 0, 0, &actual));
    CHECK(!ud_value_compare(value, &actual));
    array.release(&array);
    schema.release(&schema);
    ud_batch_destroy(copy);
    ud_batch_destroy(batch);
    return 0;
}
int main(void) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_value value;
    OK(ud_value_parse(ctx, UD_UINT64, "18446744073709551615", 20, &value));
    CHECK(value.unsigned_integer == UINT64_MAX);
    ud_field unsigned_field = {.name = "u", .name_length = 1, .type = UD_UINT64};
    CHECK(!roundtrip(ctx, &unsigned_field, &value));
    CHECK(ud_value_parse(ctx, UD_UINT64, "18446744073709551616", 20, &value) == UD_OVERFLOW);
    CHECK(ud_value_parse(ctx, UD_UINT64, "-1", 2, &value) == UD_PARSE);
    CHECK(ud_value_parse(ctx, UD_UINT64, "-0", 2, &value) == UD_PARSE);
    ud_value maximum = {.type = UD_UINT64, .unsigned_integer = UINT64_MAX};
    ud_value one = {.type = UD_UINT64, .unsigned_integer = 1}, result;
    CHECK(ud_value_numeric(UD_NUMERIC_ADD, &maximum, &one, &unsigned_field, &result) ==
          UD_OVERFLOW);
    CHECK(ud_value_compare(&maximum, &one) > 0);
    ud_field duration = {.name = "d", .name_length = 1, .type = UD_DURATION};
    const char *negative = "-838:59:59.999999";
    OK(ud_value_parse_field(ctx, &duration, negative, strlen(negative), UD_ROUND_EXACT, &value));
    CHECK(value.integer == -INT64_C(3020399999999));
    CHECK(!roundtrip(ctx, &duration, &value));
    const char *minimum = "-PT9223372036854.775808S";
    OK(ud_value_parse(ctx, UD_DURATION, minimum, strlen(minimum), &value));
    CHECK(value.integer == INT64_MIN);
    CHECK(!roundtrip(ctx, &duration, &value));
    char formatted[160];
    uint32_t required;
    OK(ud_value_format(&value, formatted, sizeof(formatted), &required));
    OK(ud_value_parse(ctx, UD_DURATION, formatted, required - 1, &result));
    CHECK(result.integer == INT64_MIN);
    ud_field interval = {.name = "i", .name_length = 1, .type = UD_INTERVAL};
    value = (ud_value){.type = UD_INTERVAL, .interval = {INT32_MIN, INT32_MAX, INT64_MIN}};
    CHECK(!roundtrip(ctx, &interval, &value));
    OK(ud_value_format(&value, formatted, sizeof(formatted), &required));
    OK(ud_value_parse_field(ctx, &interval, formatted, required - 1, UD_ROUND_EXACT, &result));
    CHECK(!ud_value_compare(&value, &result));
    CHECK(ud_value_parse(ctx, UD_DURATION, "P1D", 3, &result) == UD_UNSUPPORTED);
    ud_string labels[] = {{"", 0}, {"1", 1}, {"a\0b", 3}};
    ud_field enumeration = {.name = "e",
                            .name_length = 1,
                            .type = UD_UINT64,
                            .dictionary_kind = UD_DICTIONARY_ENUM,
                            .dictionary = labels,
                            .dictionary_count = 3};
    for (uint64_t ordinal = 0; ordinal <= 3; ordinal++) {
        value = (ud_value){.type = UD_UINT64, .unsigned_integer = ordinal};
        CHECK(!roundtrip(ctx, &enumeration, &value));
    }
    ud_field set = enumeration;
    set.dictionary_kind = UD_DICTIONARY_SET;
    value = (ud_value){.type = UD_UINT64, .unsigned_integer = 7};
    CHECK(!roundtrip(ctx, &set, &value));
    ud_batch *batch = NULL;
    OK(ud_batch_create(ctx, &set, 1, 1, &batch));
    value.unsigned_integer = 8;
    CHECK(ud_batch_append(batch, &value, 1) == UD_QUALITY);
    ud_batch_destroy(batch);
    ud_field fields[] = {{.name = "id", .name_length = 2, .type = UD_INT64},
                         {.name = "value", .name_length = 5, .type = UD_UTF8}};
    OK(ud_batch_create(ctx, fields, 2, 1, &batch));
    ud_value partial[] = {{.type = UD_INT64, .integer = 7},
                          {.type = UD_UTF8, .is_null = 1, .is_absent = 1}};
    OK(ud_batch_append(batch, partial, 2));
    ud_id scope;
    OK(ud_id_make(ctx, (ud_string){"test", 4}, (ud_string){"source", 6}, &scope));
    ud_row_info info;
    OK(ud_row_info_source(scope, UINT64_MAX - 1, &info));
    OK(ud_batch_set_row_info(batch, 0, &info));
    OK(ud_batch_seal(batch));
    uint8_t *wire;
    uint64_t length;
    OK(ud_batch_serialize(batch, &wire, &length));
    ud_batch *copy = NULL;
    OK(ud_batch_deserialize(ctx, wire, length, &copy));
    CHECK(ud_batch_has_absent(copy));
    int32_t present;
    OK(ud_batch_cell_present(copy, 0, 1, &present));
    CHECK(!present);
    OK(ud_batch_row_info(copy, 0, &info));
    CHECK(info.has_source_position && info.source_position == UINT64_MAX - 1);
    char *json = NULL;
    uint32_t json_length;
    CHECK(ud_batch_to_json(copy, &json, &json_length) == UD_INCOMPATIBLE && !json);
    struct ArrowArray array;
    struct ArrowSchema schema;
    CHECK(ud_batch_export_arrow(copy, &array, &schema) == UD_INCOMPATIBLE && !array.release &&
          !schema.release);
    ud_free(wire);
    ud_batch_destroy(copy);
    ud_batch_destroy(batch);
    uint8_t unsigned_json[] = {10, 255, 255, 255, 255, 255, 255, 255, 255};
    OK(ud_mysql_json_decode(ctx, unsigned_json, sizeof(unsigned_json), &json, &json_length));
    CHECK(json_length == 20 && !memcmp(json, "18446744073709551615", 20));
    ud_free(json);
    uint8_t mixed_json[] = {2, 3,   0,   25,  0,   10,  13,  0,   5,   0, 128, 12, 21,
                            0, 255, 255, 255, 255, 255, 255, 255, 255, 3, 'a', 0,  'b'};
    OK(ud_mysql_json_decode(ctx, mixed_json, sizeof(mixed_json), &json, &json_length));
    CHECK(!strcmp(json, "[18446744073709551615,-32768,\"a\\u0000b\"]"));
    ud_free(json);
    mixed_json[6] = 255;
    CHECK(ud_mysql_json_decode(ctx, mixed_json, sizeof(mixed_json), &json, &json_length) ==
              UD_CORRUPT &&
          !json);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
