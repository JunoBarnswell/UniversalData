#include "ud/csv.h"
#include "ud/format.h"
#include <stdio.h>
#include <string.h>
#define CHECK(x)                                                                                   \
    do {                                                                                           \
        if (!(x)) {                                                                                \
            fprintf(stderr, "line %d: %s\n", __LINE__, #x);                                        \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)
#define OK(x) CHECK((x) == UD_OK)
static ud_string str(const char *text) {
    return (ud_string){text, (uint32_t)strlen(text)};
}
int main(int argc, char **argv) {
    CHECK(argc == 4);
    ud_context *context = NULL;
    OK(ud_context_create(NULL, &context));
    ud_field fields[] = {{.name = "id", .name_length = 2, .type = UD_UINT64},
                         {.name = "name", .name_length = 4, .type = UD_UTF8, .nullable = 1}};
    ud_batch *batch = NULL;
    OK(ud_batch_create(context, fields, 2, 2, &batch));
    const char *name = "张；'三";
    ud_value row[] = {{.type = UD_UINT64, .unsigned_integer = UINT64_MAX},
                      {.type = UD_UTF8, .text = name, .text_length = (uint32_t)strlen(name)}};
    OK(ud_batch_append(batch, row, 2));
    row[0].unsigned_integer = 0;
    row[1].is_null = 1;
    OK(ud_batch_append(batch, row, 2));
    OK(ud_batch_seal(batch));
    ud_field bound[2];
    OK(ud_batch_field(batch, 0, bound));
    OK(ud_batch_field(batch, 1, bound + 1));
    ud_csv_options options = {
        .struct_size = sizeof(options),
        .abi_version = UD_ABI_VERSION,
        .batch_size = 2,
        .max_record_bytes = 4096,
        .format = str("{\"encoding\":\"UTF-16LE\",\"delimiter\":\"；\",\"qualifier\":\"'\","
                      "\"preamble\":[\"dataset\"],\"positions\":[1,0],\"nullMarker\":\"NULL\"}")};
    OK(ud_csv_write(context, argv[1], batch, &options));
    ud_csv_reader *reader = NULL;
    OK(ud_csv_open(context, argv[1], bound, 2, &options, &reader));
    ud_batch *rows = NULL;
    OK(ud_csv_next(reader, &rows, NULL));
    CHECK(ud_batch_rows(rows) == 2);
    ud_value value;
    OK(ud_batch_get(rows, 0, 0, &value));
    CHECK(value.unsigned_integer == UINT64_MAX);
    OK(ud_batch_get(rows, 0, 1, &value));
    CHECK(value.text_length == strlen(name) && !memcmp(value.text, name, value.text_length));
    OK(ud_batch_get(rows, 1, 1, &value));
    CHECK(value.is_null);
    ud_batch_destroy(rows);
    rows = NULL;
    uint64_t offset = ud_csv_offset(reader), number = ud_csv_record(reader);
    OK(ud_csv_resume(reader, offset, number));
    CHECK(ud_csv_next(reader, &rows, NULL) == UD_END);
    ud_csv_close(reader);
    ud_batch *discovered = NULL;
    OK(ud_csv_discover(context, argv[1], str("layout"), &options, 4, &discovered));
    ud_field field;
    OK(ud_batch_field(discovered, 0, &field));
    CHECK(field.type == UD_UINT64 && field.name_length == 2 && !memcmp(field.name, "id", 2));
    ud_batch_destroy(discovered);
    options.format = str("{\"encoding\":\"UTF-16BE\",\"widths\":[20,8],\"nullMarker\":\"NULL\","
                         "\"rowDelimiter\":\"CRLF\"}");
    OK(ud_csv_write(context, argv[2], batch, &options));
    OK(ud_csv_open(context, argv[2], bound, 2, &options, &reader));
    OK(ud_csv_next(reader, &rows, NULL));
    CHECK(ud_batch_rows(rows) == 2);
    OK(ud_batch_get(rows, 0, 1, &value));
    CHECK(value.text_length == strlen(name));
    ud_batch_destroy(rows);
    ud_csv_close(reader);
    ud_record_options records = {.struct_size = sizeof(records),
                                 .abi_version = UD_ABI_VERSION,
                                 .source_scope = {12, 13},
                                 .policy = UD_ERROR_REDIRECT,
                                 .include_input = 1,
                                 .errors = {.struct_size = sizeof(ud_error_context),
                                            .abi_version = UD_ABI_VERSION,
                                            .memory = context,
                                            .component_id = str("file")}};
    options.records = &records;
    options.format = str("{\"encoding\":\"UTF-16LE\",\"delimiter\":\";\"}");
    OK(ud_csv_open(context, argv[3], bound, 2, &options, &reader));
    ud_batch *errors = NULL;
    OK(ud_csv_next(reader, &rows, &errors));
    CHECK(ud_batch_rows(rows) == 1 && ud_batch_rows(errors) == 1);
    OK(ud_batch_get(errors, 0, 15, &value));
    CHECK(value.type == UD_BINARY && value.text_length == 20); // exact UTF-16 bytes of bad;value\n
    ud_batch_destroy(rows);
    ud_batch_destroy(errors);
    ud_csv_close(reader);
    ud_batch_destroy(batch);
    OK(ud_context_destroy(context));
    return 0;
}
