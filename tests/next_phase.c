#include "check.h"
#include "ud/cache.h"
#include "ud/cursor.h"
#include "ud/quality.h"
#include "ud/format.h"
#include <string.h>
#include <math.h>
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status allowed(void *user) {
    (void)user;
    return UD_OK;
}
static ud_status batch(ud_context *ctx, const char *schema, const char *rows, ud_batch **out) {
    return ud_batch_from_json_schema(ctx, schema, (uint32_t)strlen(schema), rows,
                                     (uint32_t)strlen(rows), UD_ROUND_EXACT, out);
}
static ud_status append_reference(ud_context *ctx, ud_cache *cache, const ud_batch *input) {
    for (uint32_t row = 0; row < ud_batch_rows(input); row++) {
        ud_batch *piece = NULL;
        ud_status status = ud_compute_slice(ctx, input, row, 1, &piece);
        if (!status)
            status = ud_batch_seal(piece);
        if (!status)
            status = ud_cache_append(cache, piece);
        ud_batch_destroy(piece);
        if (status)
            return status;
    }
    return UD_OK;
}
static int records_and_cache(ud_context *ctx, const char *directory) {
    const char *schema =
        "{\"version\":12,\"fields\":[{\"name\":\"k\",\"type\":\"int64\",\"nullable\":false},{"
        "\"name\":\"name\",\"type\":\"utf8\",\"nullable\":false},{\"name\":\"f\",\"type\":"
        "\"float64\",\"nullable\":false},{\"name\":\"d\",\"type\":\"decimal128\",\"precision\":38,"
        "\"scale\":2,\"nullable\":false},{\"name\":\"nested\",\"type\":\"struct\",\"nullable\":"
        "false,\"children\":[{\"name\":\"x\",\"type\":\"binary\",\"nullable\":true}]}]}";
    const char *rows =
        "[{\"k\":1,\"name\":\"汉\\u0000\",\"f\":-0.0,\"d\":\"123.45\",\"nested\":{\"x\":\"AP8=\"}},"
        "{\"k\":1,\"name\":\"汉\\u0000\",\"f\":0.0,\"d\":\"999.00\",\"nested\":{\"x\":null}},{"
        "\"k\":2,\"name\":\"😀\",\"f\":\"NaN\",\"d\":\"1.00\",\"nested\":{\"x\":\"AA==\"}}]";
    ud_batch *raw = NULL, *identified = NULL, *wire_batch = NULL;
    OK(batch(ctx, schema, rows, &raw));
    OK(ud_batch_seal(raw));
    ud_id scope;
    OK(ud_id_make(ctx, str("reference"), str("snapshot"), &scope));
    OK(ud_batch_identify(ctx, raw, scope, 100, &identified));
    ud_row_info a, b;
    OK(ud_batch_row_info(identified, 0, &a));
    OK(ud_batch_row_info(identified, 1, &b));
    CHECK(ud_id_compare(a.id, b.id));
    uint8_t *wire = NULL;
    uint64_t bytes = 0;
    OK(ud_batch_serialize(identified, &wire, &bytes));
    OK(ud_batch_deserialize(ctx, wire, bytes, &wire_batch));
    OK(ud_batch_row_info(wire_batch, 1, &a));
    CHECK(!ud_id_compare(a.id, b.id));
    ud_free(wire);
    ud_batch_destroy(wire_batch);
    ud_buffer_manager *buffers = NULL;
    ud_buffer_options budget = {.struct_size = sizeof(budget),
                                .abi_version = UD_ABI_VERSION,
                                .max_blocks = 1000,
                                .resident_limit = 1,
                                .working_limit = 1000000,
                                .spill_limit = 10000000,
                                .directory = str(directory),
                                .check = allowed};
    OK(ud_buffer_manager_create(ctx, &budget, &buffers));
    uint32_t keys[] = {0, 1};
    ud_cache_options options = {.struct_size = sizeof(options),
                                .abi_version = UD_ABI_VERSION,
                                .max_batch_rows = 1,
                                .keys = keys,
                                .key_count = 2,
                                .duplicates = UD_CACHE_ALL,
                                .error_policy = UD_ERROR_FAIL,
                                .buffers = buffers};
    ud_cache *cache = NULL;
    OK(ud_cache_create(ctx, raw, &options, &cache));
    OK(append_reference(ctx, cache, identified));
    OK(ud_cache_seal(cache));
    ud_cache_reader *one = NULL, *two = NULL;
    OK(ud_cache_reader_create(cache, raw, keys, 2, &one));
    OK(ud_cache_reader_create(cache, raw, keys, 2, &two));
    OK(ud_cache_probe(one, raw, 0));
    const ud_batch *match = NULL;
    uint32_t row = 0, count = 0;
    while (ud_cache_next(one, &match, &row) == UD_OK)
        count++;
    CHECK(count == 2);
    OK(ud_cache_probe(two, raw, 2));
    OK(ud_cache_next(two, &match, &row));
    ud_value value;
    OK(ud_batch_get(match, row, 2, &value));
    CHECK(isnan(value.real));
    CHECK(ud_cache_next(two, &match, &row) == UD_END);
    uint8_t fingerprint[32];
    OK(ud_cache_fingerprint(cache, fingerprint));
    ud_cache_reader_destroy(one);
    ud_cache_reader_destroy(two);
    OK(ud_cache_release(cache));
    options.duplicates = UD_CACHE_FIRST;
    OK(ud_cache_create(ctx, raw, &options, &cache));
    OK(append_reference(ctx, cache, identified));
    OK(ud_cache_seal(cache));
    OK(ud_cache_reader_create(cache, raw, keys, 2, &one));
    OK(ud_cache_probe(one, raw, 0));
    OK(ud_cache_next(one, &match, &row));
    OK(ud_batch_get(match, row, 3, &value));
    CHECK(value.decimal.low == 12345);
    CHECK(ud_cache_next(one, &match, &row) == UD_END);
    ud_cache_reader_destroy(one);
    OK(ud_cache_release(cache));
    options.duplicates = UD_CACHE_ERROR;
    options.error_policy = UD_ERROR_REDIRECT;
    OK(ud_cache_create(ctx, raw, &options, &cache));
    OK(append_reference(ctx, cache, identified));
    OK(ud_cache_seal(cache));
    uint64_t position = 0;
    ud_batch *duplicates = NULL;
    OK(ud_cache_duplicates(cache, &position, &duplicates));
    CHECK(ud_batch_rows(duplicates) == 1);
    OK(ud_batch_row_info(duplicates, 0, &a));
    CHECK(!ud_id_compare(a.id, b.id));
    ud_batch_destroy(duplicates);
    OK(ud_cache_release(cache));
    options.error_policy = UD_ERROR_FAIL;
    OK(ud_cache_create(ctx, raw, &options, &cache));
    OK(append_reference(ctx, cache, identified));
    CHECK(ud_cache_seal(cache) == UD_SCHEMA);
    OK(ud_cache_release(cache));
    ud_buffer_metrics metrics;
    OK(ud_buffer_manager_metrics(buffers, &metrics));
    CHECK(metrics.wire_encodes && metrics.wire_decodes && metrics.read_bytes &&
          metrics.write_bytes && metrics.spill_high_water);
    CHECK(!metrics.blocks && !metrics.views && !metrics.spill_bytes);
    OK(ud_buffer_manager_destroy(buffers));
    ud_batch_destroy(identified);
    ud_batch_destroy(raw);
    return 0;
}
static int cursor_and_schema(ud_context *ctx) {
    const char *schema =
        "{\"version\":12,\"fields\":[{\"name\":\"water\",\"type\":\"timestamp\",\"timezone\":"
        "\"UTC\",\"nullable\":false},{\"name\":\"id\",\"type\":\"int64\",\"nullable\":false}]}";
    ud_batch *input = NULL, *selected = NULL;
    OK(batch(ctx, schema,
             "[{\"water\":\"2026-10-12T00:00:00Z\",\"id\":1},{\"water\":\"2026-10-12T00:00:00Z\","
             "\"id\":2},{\"water\":\"2026-10-12T00:00:00Z\",\"id\":3}]",
             &input));
    const char *policy = "{\"mode\":\"composite\",\"keys\":[\"water\",\"id\"],\"unique\":true,"
                         "\"monotonic\":true,\"after\":[\"2026-10-12T00:00:00Z\",1]}";
    ud_cursor *cursor = NULL;
    OK(ud_cursor_create(ctx, input, str(policy), &cursor));
    OK(ud_cursor_filter(cursor, input, &selected));
    CHECK(ud_batch_rows(selected) == 2);
    char *diagnostics = NULL;
    uint32_t length = 0;
    OK(ud_cursor_diagnostics(cursor, &diagnostics, &length));
    CHECK(strstr(diagnostics, "\"lastConfirmed\":0") && !strstr(diagnostics, "2026-10"));
    ud_free(diagnostics);
    OK(ud_cursor_confirm(cursor));
    OK(ud_cursor_serialize(cursor, &diagnostics, &length));
    CHECK(strstr(diagnostics, "\"lastConfirmed\":2") && strstr(diagnostics, "\"keys\":["));
    ud_free(diagnostics);
    ud_batch_destroy(selected);
    CHECK(ud_cursor_filter(cursor, input, &selected) == UD_SCHEMA);
    ud_cursor_destroy(cursor);
    ud_batch_destroy(input);
    const char *expected =
        "{\"version\":12,\"fields\":[{\"name\":\"a\",\"type\":\"int64\",\"nullable\":false},{"
        "\"name\":\"b\",\"type\":\"utf8\",\"nullable\":true}]}";
    const char *actual = "{\"version\":12,\"fields\":[{\"name\":\"b\",\"type\":\"utf8\","
                         "\"nullable\":false},{\"name\":\"a\",\"type\":\"int64\",\"nullable\":"
                         "false},{\"name\":\"c\",\"type\":\"int64\",\"nullable\":true}]}";
    OK(ud_schema_compare_json(ctx, str(expected), str(actual), str("{\"policy\":\"strict\"}"),
                              &diagnostics, &length));
    CHECK(strstr(diagnostics, "\"compatible\":false"));
    ud_free(diagnostics);
    OK(ud_schema_compare_json(ctx, str(expected), str(actual), str("{\"policy\":\"compatible\"}"),
                              &diagnostics, &length));
    CHECK(strstr(diagnostics, "\"compatible\":true") && strstr(diagnostics, "Reordered") &&
          strstr(diagnostics, "Added"));
    ud_free(diagnostics);
    const char *target =
        "{\"version\":12,\"fields\":[{\"name\":\"number\",\"type\":\"int64\",\"nullable\":false}]}";
    const char *text =
        "{\"version\":12,\"fields\":[{\"name\":\"string\",\"type\":\"utf8\",\"nullable\":false}]}";
    const char *mapping = "{\"policy\":\"explicitMapping\",\"mapping\":[{\"expected\":\"number\","
                          "\"actual\":\"string\"}]}";
    OK(ud_schema_compare_json(ctx, str(target), str(text), str(mapping), &diagnostics, &length));
    CHECK(strstr(diagnostics, "\"compatible\":true"));
    ud_free(diagnostics);
    return 0;
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    CHECK(!records_and_cache(ctx, argv[1]));
    CHECK(!cursor_and_schema(ctx));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
