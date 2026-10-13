/* Manual live PostgreSQL gate: argv = installed plugin, fixture config JSON.
 * Config is resolved only within the explicit tenant/resource/reference scope.
 * Every SQL table is temporary, including the test's receipt store. */
#include "ud/plugin.h"
#include "ud/format.h"
#include "check.h"
#include <string.h>
#include <math.h>
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status grant(void *u, ud_string t, ud_string r, int32_t action) {
    (void)action;
    return *(int *)u && t.length == 7 && !memcmp(t.data, "fixture", 7) && r.length == 6 &&
                   !memcmp(r.data, "values", 6)
               ? UD_OK
               : UD_AUTH;
}
static ud_status resolve(void *u, ud_context *ctx, ud_string t, ud_string r, ud_string ref,
                         ud_string *out) {
    if (t.length != 7 || memcmp(t.data, "fixture", 7) || r.length != 6 ||
        memcmp(r.data, "values", 6) || ref.length != 6 || memcmp(ref.data, "source", 6))
        return UD_AUTH;
    const char *text = u;
    size_t n = strlen(text);
    char *owned = ud_alloc(ctx, n + 1);
    if (!owned)
        return UD_RESOURCE;
    memcpy(owned, text, n + 1);
    *out = (ud_string){owned, (uint32_t)n};
    return UD_OK;
}
int main(int argc, char **argv) {
    CHECK(argc == 3);
    FILE *config = fopen(argv[2], "rb");
    CHECK(config);
    char text[8192];
    size_t size = fread(text, 1, sizeof(text) - 1, config);
    CHECK(!ferror(config) && feof(config));
    fclose(config);
    text[size] = 0;
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_plugin *p = NULL;
    OK(ud_plugin_load(ctx, argv[1], 0, &p));
    int allowed = 1;
    ud_connection_options o = {.struct_size = sizeof(o),
                               .abi_version = UD_ABI_VERSION,
                               .tenant = {"fixture", 7},
                               .resource = {"values", 6},
                               .credential_reference = {"source", 6},
                               .authorize_user = &allowed,
                               .resolve = resolve,
                               .resolve_user = text};
    ud_connection *db = NULL;
    CHECK(ud_connection_open(p, &o, &db) == UD_AUTH && !db);
    o.authorize = grant;
    OK(ud_connection_open(p, &o, &db));
    OK(ud_connection_test(db));
    OK(ud_connection_execute_command(db, &(ud_read_options){.struct_size=sizeof(ud_read_options),.abi_version=UD_ABI_VERSION,.sql=str("CREATE TEMP TABLE ud_sdk_receipts_v2(tenant TEXT NOT NULL,job TEXT NOT NULL,batch_id "
            "TEXT NOT NULL,digest BYTEA NOT NULL CHECK(octet_length(digest)=32),source_position "
            "TEXT NOT NULL,PRIMARY KEY(tenant,job,batch_id))")}, NULL));
    OK(ud_connection_execute_command(db, &(ud_read_options){.struct_size=sizeof(ud_read_options),.abi_version=UD_ABI_VERSION,.sql=str("CREATE TEMP TABLE ud_value_semantics(id BIGINT PRIMARY KEY,b BOOLEAN NOT NULL,f "
                "DOUBLE PRECISION,s TEXT,d NUMERIC(38,2),date DATE,time TIME,at TIMESTAMPTZ,raw "
                "BYTEA)")}, NULL));
    const char *schema =
        "{\"version\":9,\"fields\":[{\"name\":\"id\",\"type\":\"int64\",\"nullable\":false},{"
        "\"name\":\"b\",\"type\":\"bool\",\"nullable\":false},{\"name\":\"f\",\"type\":\"float64\","
        "\"nullable\":true},{\"name\":\"s\",\"type\":\"utf8\",\"nullable\":true},{\"name\":\"d\","
        "\"type\":\"decimal128\",\"precision\":38,\"scale\":2,\"nullable\":true},{\"name\":"
        "\"date\",\"type\":\"date32\",\"nullable\":true},{\"name\":\"time\",\"type\":\"time64\","
        "\"nullable\":true},{\"name\":\"at\",\"type\":\"timestamp\",\"timezone\":\"Asia/"
        "Shanghai\",\"nullable\":true},{\"name\":\"raw\",\"type\":\"binary\",\"nullable\":true}]}";
    const char *rows =
        "[{\"id\":1,\"b\":true,\"f\":\"NaN\",\"s\":\"中文\",\"d\":"
        "\"123456789012345678901234567890123456.78\",\"date\":\"1969-12-31\",\"time\":\"23:59:59."
        "999999\",\"at\":\"1969-12-31T23:59:59.999999Z\",\"raw\":\"AP9B\"},{\"id\":2,\"b\":false,"
        "\"f\":\"Infinity\",\"s\":\"中文\",\"d\":\"-0.00\",\"date\":\"2000-02-29\",\"time\":\"00:"
        "00:00\",\"at\":\"2024-02-29T08:00:00+08:00\",\"raw\":\"\"}]";
    ud_batch *batch = NULL, *bad = NULL, *fields = NULL;
    OK(ud_batch_from_json_schema(ctx, schema, (uint32_t)strlen(schema), rows,
                                 (uint32_t)strlen(rows), UD_ROUND_EXACT, &batch));
    ud_field metadata[9];
    for (uint32_t j = 0; j < 9; j++)
        OK(ud_batch_field(batch, j, metadata + j));
    OK(ud_batch_create(ctx, metadata, 9, 2, &bad));
    ud_value values[9];
    for (uint32_t j = 0; j < 9; j++)
        OK(ud_batch_get(batch, 0, j, values + j));
    OK(ud_batch_append(bad, values, 9));
    for (uint32_t j = 0; j < 9; j++)
        OK(ud_batch_get(batch, 1, j, values + j));
    values[3].text = "a\0b";
    values[3].text_length = 3;
    OK(ud_batch_append(bad, values, 9));
    ud_write_options w = {.struct_size = sizeof(w),
                          .abi_version = UD_ABI_VERSION,
                          .mode = UD_WRITE_APPEND,
                          .table = {"ud_value_semantics", 18},
                          .job = {"semantics", 9},
                          .batch_id = {"0", 1},
                          .source_position = {"2", 1}};
    CHECK(ud_connection_write(db, &w, bad) == UD_PARSE);
    ud_commit_receipt receipt = {0};
    OK(ud_connection_receipt(db, w.job, w.batch_id, &receipt));
    CHECK(!receipt.committed);
    ud_free((void *)receipt.source_position.data);
    ud_batch_destroy(bad);
    OK(ud_connection_write(db, &w, batch));
    OK(ud_connection_write(db, &w, batch));
    OK(ud_connection_receipt(db, w.job, w.batch_id, &receipt));
    CHECK(receipt.committed);
    ud_free((void *)receipt.source_position.data);
    OK(ud_batch_create(ctx, metadata, 9, 0, &fields));
    ud_read_options read = {
        sizeof(read), UD_ABI_VERSION, 1,
        str("SELECT id,b,f,s,d,date,time,at,raw FROM ud_value_semantics ORDER BY id")};
    OK(ud_connection_read_open(db, &read, fields));
    ud_batch_destroy(fields);
    for (uint32_t row = 0; row < 2; row++) {
        ud_batch *actual = NULL;
        OK(ud_connection_read_next(db, &actual));
        CHECK(ud_batch_rows(actual) == 1);
        for (uint32_t col = 0; col < 9; col++) {
            ud_value a, b;
            OK(ud_batch_get(batch, row, col, &a));
            OK(ud_batch_get(actual, 0, col, &b));
            CHECK(!ud_value_compare(&a, &b) &&
                  ud_value_hash(&a, UD_VALUE_HASH_SEED) == ud_value_hash(&b, UD_VALUE_HASH_SEED));
        }
        ud_batch_destroy(actual);
    }
    fields = NULL;
    CHECK(ud_connection_read_next(db, &fields) == UD_END && !fields);
    OK(ud_connection_read_close(db));
    ud_batch_destroy(batch);
    for (int kind = UD_METADATA_CATALOGS; kind <= UD_METADATA_INDEXES; kind++) {
        ud_metadata_options metadata_options = {.struct_size = sizeof(metadata_options),
                                                .abi_version = UD_ABI_VERSION,
                                                .kind = kind,
                                                .parent = {"ud_value_semantics", 18},
                                                .limit = 1};
        ud_batch *page = NULL;
        OK(ud_connection_metadata(db, &metadata_options, &page));
        CHECK(ud_batch_rows(page) <= 1 && ud_batch_columns(page) == 8);
        ud_batch_destroy(page);
        metadata_options.offset = 1;
        OK(ud_connection_metadata(db, &metadata_options, &page));
        CHECK(ud_batch_rows(page) <= 1);
        ud_batch_destroy(page);
    }
    allowed = 0;
    CHECK(ud_connection_test(db) == UD_AUTH);
    allowed = 1;
    CHECK(ud_plugin_unload(p) == UD_BUSY);
    OK(ud_connection_close(db));
    OK(ud_plugin_unload(p));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    puts("PostgreSQL live typed connector: default deny, 9 scalar types/NaN/Infinity/exact "
         "Decimal/UTC/Binary, NUL write rollback + receipt absence, corrected idempotent retry, "
         "bounded cursor/END, Compare/Hash parity and clean release passed");
    return 0;
}
