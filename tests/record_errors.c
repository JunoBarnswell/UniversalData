#include "check.h"
#include "ud/csv.h"
#include "ud/format.h"
#include <string.h>
static int write_fixture(const char *path, const char *text) {
    FILE *file = fopen(path, "wb");
    CHECK(file);
    CHECK(fwrite(text, 1, strlen(text), file) == strlen(text));
    CHECK(!fclose(file));
    return 0;
}
static int records(ud_batch *good, ud_batch *errors, uint32_t good_rows, uint32_t bad_rows,
                   int raw) {
    CHECK(ud_batch_rows(good) == good_rows && ud_batch_rows(errors) == bad_rows);
    CHECK(ud_batch_columns(errors) == (raw ? 14u : 13u));
    for (uint32_t row = 0; row < bad_rows; row++) {
        ud_row_info info;
        OK(ud_batch_row_info(errors, row, &info));
        CHECK((info.flags & UD_ROW_ERROR) && info.has_source_position);
        ud_value value;
        OK(ud_batch_get(errors, row, 10, &value));
        CHECK(value.text_length);
        if (raw) {
            OK(ud_batch_get(errors, row, 13, &value));
            CHECK(value.text_length);
        }
    }
    return 0;
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_id scope;
    OK(ud_id_make(ctx, (ud_string){"test", 4}, (ud_string){"records", 7}, &scope));
    ud_record_options options = {.struct_size = sizeof(options),
                                 .abi_version = UD_ABI_VERSION,
                                 .errors = {.struct_size = sizeof(ud_error_context),
                                            .abi_version = UD_ABI_VERSION,
                                            .memory = ctx,
                                            .field_scope = scope,
                                            .component_id = {"source", 6}},
                                 .source_scope = scope,
                                 .policy = UD_ERROR_REDIRECT,
                                 .include_input = 1};
    ud_field fields[] = {{.name = "id", .name_length = 2, .type = UD_UINT64},
                         {.name = "text", .name_length = 4, .type = UD_UTF8}};
    CHECK(!write_fixture(
        argv[1], "id,text\n18446744073709551615,first\ninvalid,bad\n2,next\n3,too,many\n4,last\n"));
    ud_csv_options csv_options = {.struct_size = sizeof(csv_options),
                                  .abi_version = UD_ABI_VERSION,
                                  .batch_size = 16,
                                  .max_record_bytes = 1024,
                                  .records = &options};
    ud_csv_reader *csv = NULL;
    OK(ud_csv_open(ctx, argv[1], fields, 2, &csv_options, &csv));
    ud_batch *good = NULL, *errors = NULL;
    OK(ud_csv_next(csv, &good, &errors));
    CHECK(!records(good, errors, 3, 2, 1));
    ud_row_info info;
    OK(ud_batch_row_info(good, 1, &info));
    CHECK(info.source_position == 2);
    OK(ud_batch_row_info(errors, 0, &info));
    CHECK(info.source_position == 1);
    ud_batch_destroy(good);
    ud_batch_destroy(errors);
    CHECK(ud_csv_next(csv, &good, &errors) == UD_END && !good && !errors);
    ud_csv_close(csv);
    CHECK(!write_fixture(argv[1], "id,text\n1,\"unclosed\n2,next\n"));
    OK(ud_csv_open(ctx, argv[1], fields, 2, &csv_options, &csv));
    CHECK(ud_csv_next(csv, &good, &errors) == UD_PARSE && !good && !errors);
    CHECK(ud_csv_next(csv, &good, &errors) == UD_CORRUPT);
    ud_csv_close(csv);
    CHECK(!write_fixture(argv[1], "{\"id\":\"18446744073709551615\",\"text\":\"first\"}\n"
                                  "{invalid}\n{\"id\":2,\"text\":\"next\"}\n"
                                  "{\"id\":3,\"text\":null}\n{\"id\":4,\"text\":\"last\"}"));
    options.include_input = 0;
    ud_json_options json_options = {.struct_size = sizeof(json_options),
                                    .version = UD_CONFIG_VERSION,
                                    .batch_size = 16,
                                    .max_record_bytes = 1024,
                                    .records = &options};
    ud_json_reader *json = NULL;
    OK(ud_json_reader_open(ctx, argv[1], fields, 2, &json_options, &json));
    OK(ud_json_reader_next(json, &good, &errors));
    CHECK(!records(good, errors, 3, 2, 0));
    ud_value kind;
    OK(ud_batch_get(errors, 1, 10, &kind));
    CHECK(kind.text_length == 10 && !memcmp(kind.text, "constraint", 10));
    ud_batch_destroy(good);
    ud_batch_destroy(errors);
    ud_json_reader_close(json);
    CHECK(!write_fixture(argv[1], "[\n{\"id\":1,\"text\":\"a]b\"},\n"
                                  "{\"id\":\"bad\",\"text\":\"rejected\"},\n"
                                  "{\"id\":2,\"text\":\"last\"}\n]"));
    json_options.framing = UD_JSON_FRAMING_ARRAY;
    json_options.batch_size = 1;
    OK(ud_json_reader_open(ctx, argv[1], fields, 2, &json_options, &json));
    OK(ud_json_reader_next(json, &good, &errors));
    CHECK(good->rows == 1 && errors->rows == 0);
    uint64_t bookmark = ud_json_reader_offset(json);
    ud_batch_destroy(good);
    ud_batch_destroy(errors);
    OK(ud_json_reader_next(json, &good, &errors));
    CHECK(good->rows == 0 && errors->rows == 1);
    ud_batch_destroy(good);
    ud_batch_destroy(errors);
    OK(ud_json_reader_resume(json, bookmark, 1));
    OK(ud_json_reader_next(json, &good, &errors));
    CHECK(good->rows == 0 && errors->rows == 1);
    ud_batch_destroy(good);
    ud_batch_destroy(errors);
    OK(ud_json_reader_next(json, &good, &errors));
    CHECK(good->rows == 1 && errors->rows == 0);
    ud_batch_destroy(good);
    ud_batch_destroy(errors);
    CHECK(ud_json_reader_next(json, &good, &errors) == UD_END);
    ud_json_reader_close(json);
    CHECK(!write_fixture(argv[1], "[{\"id\":1,\"text\":\"unclosed}"));
    OK(ud_json_reader_open(ctx, argv[1], fields, 2, &json_options, &json));
    CHECK(ud_json_reader_next(json, &good, &errors) == UD_PARSE && !good && !errors);
    ud_json_reader_close(json);
    CHECK(!ud_record_error_recoverable(UD_COMMIT_UNKNOWN));
    CHECK(!ud_record_error_recoverable(UD_TRANSIENT));
    CHECK(!ud_record_error_recoverable(UD_CANCELLED));
    CHECK(ud_record_error_kind(UD_TRUNCATED) == UD_RECORD_TRUNCATION);
    CHECK(!remove(argv[1]));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
