#include "check.h"
#include "ud/sql.h"
#include <string.h>
static int stream(ud_context *ctx, ud_batch *batch, int dialect, const char *expected,
                  size_t bytes) {
    ud_bulk_reader *r = NULL;
    OK(ud_sql_bulk_open(ctx, batch, dialect, &r));
    char actual[1024];
    uint32_t at = 0, n;
    /* Split every escape and binary hex pair at a network chunk boundary. */
    ud_status status;
    while ((status = ud_sql_bulk_read(r, actual + at, 1, &n)) == UD_OK) {
        CHECK(n == 1 && at < sizeof(actual) - 1);
        at += n;
    }
    CHECK(status == UD_END && at == bytes && !memcmp(actual, expected, bytes));
    ud_sql_bulk_close(r);
    return 0;
}
int main(void) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_field fields[] = {{.name = "flag", .name_length = 4, .type = UD_BOOL, .nullable = 1},
                         {.name = "label", .name_length = 5, .type = UD_UTF8, .nullable = 1},
                         {.name = "payload", .name_length = 7, .type = UD_BINARY, .nullable = 1},
                         {.name = "amount",
                          .name_length = 6,
                          .type = UD_DECIMAL128,
                          .nullable = 1,
                          .precision = 30,
                          .scale = 6}};
    ud_batch *batch = NULL;
    OK(ud_batch_create(ctx, fields, 4, 2, &batch));
    const char text[] = "\\N\t\n\r\b\f\v\032";
    const char binary[] = {0, 9, 92, 10, (char)255};
    ud_value values[] = {{.type = UD_BOOL, .integer = 1},
                         {.type = UD_UTF8, .text = text, .text_length = sizeof(text) - 1},
                         {.type = UD_BINARY, .text = binary, .text_length = sizeof(binary)},
                         {0}};
    OK(ud_value_parse_field(ctx, fields + 3, "1234567890.123456", 17, UD_ROUND_EXACT, values + 3));
    OK(ud_batch_append(batch, values, 4));
    for (int i = 0; i < 4; i++)
        values[i].is_null = 1;
    OK(ud_batch_append(batch, values, 4));
    OK(ud_batch_seal(batch));
    const char pg[] = "true\t\\\\N\\t\\n\\r\\b\\f\\v\\032\t\\\\x00095c0aff\t1234567890."
                      "123456\n\\N\t\\N\t\\N\t\\N\n";
    const char mysql[] =
        "1\t\\\\N\\t\\n\\r\\b\f\v\\Z\t\\0\\t\\\\\\n\xff\t1234567890.123456\n\\N\t\\N\t\\N\t\\N\n";
    CHECK(!stream(ctx, batch, UD_SQL_POSTGRESQL, pg, sizeof(pg) - 1));
    CHECK(!stream(ctx, batch, UD_SQL_MYSQL, mysql, sizeof(mysql) - 1));
    CHECK(!stream(ctx, batch, UD_SQL_MARIADB, mysql, sizeof(mysql) - 1));
    ud_bulk_reader *r = NULL;
    CHECK(ud_sql_bulk_open(ctx, batch, UD_SQL_SQLSERVER, &r) == UD_UNSUPPORTED && !r);
    OK(ud_sql_bulk_open(ctx, batch, UD_SQL_POSTGRESQL, &r));
    ud_batch_destroy(batch); /* Reader retains the actual sealed Batch. */
    char byte;
    uint32_t n = 0;
    OK(ud_sql_bulk_read(r, &byte, 1, &n));
    CHECK(n == 1 && byte == 't');
    ud_cancel(ctx);
    CHECK(ud_sql_bulk_read(r, &byte, 1, &n) == UD_CANCELLED && !n);
    ud_sql_bulk_close(r);
    OK(ud_context_destroy(ctx));
    return 0;
}
