/* Independent C consumer: actual vendor services, no database emulation. */
#include "check.h"
#include "ud/plugin.h"
#include "ud/sql.h"
#include "ud/format.h"
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
static ud_context *ctx;
#undef OK
#define OK(call)                                                                                   \
    do {                                                                                           \
        ud_status s_ = (call);                                                                     \
        if (s_) {                                                                                  \
            fprintf(stderr, "status=%d line=%d: %s\n", s_, __LINE__, ud_error(ctx));               \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)tenant;
    (void)resource;
    (void)action;
    return *(int *)user ? UD_OK : UD_AUTH;
}
static ud_status resolve(void *user, ud_context *c, ud_string tenant, ud_string resource,
                         ud_string ref, ud_string *out) {
    (void)tenant;
    (void)resource;
    (void)ref;
    FILE *f = fopen(user, "rb");
    if (!f)
        return UD_IO;
    if (fseek(f, 0, SEEK_END)) {
        fclose(f);
        return UD_IO;
    }
    long n = ftell(f);
    rewind(f);
    if (n < 1 || n > 65536) {
        fclose(f);
        return UD_SCHEMA;
    }
    char *p = ud_alloc(c, (size_t)n + 1);
    if (!p) {
        fclose(f);
        return UD_RESOURCE;
    }
    size_t got = fread(p, 1, (size_t)n, f);
    fclose(f);
    if (got != (size_t)n) {
        ud_free(p);
        return UD_IO;
    }
    *out = (ud_string){p, (uint32_t)n};
    return UD_OK;
}
static ud_status command(ud_connection *c, const char *sql) {
    ud_read_options o = {.struct_size = sizeof(o), .abi_version = UD_ABI_VERSION, .sql = str(sql)};
    return ud_connection_execute_command(c, &o, NULL);
}
static ud_status values(const ud_field *f, uint32_t i, ud_value *v) {
    memset(v, 0, 6 * sizeof(*v));
    v[0] = (ud_value){.type = f[0].type, .integer = i};
    if (f[0].type == UD_DECIMAL128) {
        char text[24];
        int n = snprintf(text, sizeof(text), "%u", i);
        ud_status s = ud_value_parse_field(ctx, f, text, (uint32_t)n, UD_ROUND_EXACT, v);
        if (s)
            return s;
    }
    v[1] = (ud_value){.type = UD_UTF8, .is_null = i % 11 == 0, .text = "汉字😀", .text_length = 10};
    ud_status s =
        ud_value_parse_field(ctx, f + 2, "9007199254740993.123456", 23, UD_ROUND_EXACT, v + 2);
    if (s)
        return s;
    v[2].is_null = i % 17 == 0;
    static const char binary[] = {0, (char)0xff, 1};
    v[3] = (ud_value){.type = UD_BINARY,
                      .is_null = i % 19 == 0,
                      .text = binary,
                      .text_length = i % 13 == 0 ? 0 : 3};
    v[4] =
        (ud_value){.type = UD_TIMESTAMP, .is_null = i % 23 == 0, .integer = 1700000000000000LL + i};
    v[5] = (ud_value){.type = f[5].type, .integer = i % 2};
    if (f[5].type == UD_DECIMAL128) {
        char digit = (char)('0' + i % 2);
        return ud_value_parse_field(ctx, f + 5, &digit, 1, UD_ROUND_EXACT, v + 5);
    }
    return UD_OK;
}
int main(int argc, char **argv) {
    CHECK(argc >= 3);
    int resume = argc > 3 && !strcmp(argv[3], "resume"),
        kill = argc > 3 && !strcmp(argv[3], "kill"),
        native = argc > 3 && !strcmp(argv[3], "native"), allowed = 1;
    OK(ud_context_create(NULL, &ctx));
    ud_plugin *p = NULL;
    OK(ud_plugin_load(ctx, argv[1], 0, &p));
    ud_connection_options co = {.struct_size = sizeof(co),
                                .abi_version = UD_ABI_VERSION,
                                .tenant = {"advanced", 8},
                                .resource = {"fixture", 7},
                                .credential_reference = {"fixture", 7},
                                .authorize_user = &allowed,
                                .authorize = authorize,
                                .resolve_user = argv[2],
                                .resolve = resolve};
    ud_connection *c = NULL;
    OK(ud_connection_open(p, &co, &c));
    OK(ud_connection_test(c));
    int dialect = ud_connection_dialect(c);
    const char *drop =
        dialect == UD_SQL_ORACLE
            ? "BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"ud_advanced_rows\" PURGE'; EXCEPTION WHEN "
              "OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
            : "DROP TABLE IF EXISTS ud_advanced_rows";
    const char *ddl =
        dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB
            ? "CREATE TABLE ud_advanced_rows(id BIGINT NOT NULL PRIMARY KEY,name VARCHAR(128) "
              "CHARACTER SET utf8mb4,amount DECIMAL(30,6),payload LONGBLOB,stamp TIMESTAMP(6) "
              "NULL,flag TINYINT NOT NULL)"
        : dialect == UD_SQL_SQLSERVER ? "CREATE TABLE ud_advanced_rows(id BIGINT NOT NULL PRIMARY "
                                        "KEY,name NVARCHAR(128),amount DECIMAL(30,6),payload "
                                        "VARBINARY(MAX),stamp DATETIMEOFFSET(6),flag BIT NOT NULL)"
        : dialect == UD_SQL_ORACLE
            ? "CREATE TABLE \"ud_advanced_rows\"(\"id\" NUMBER(19,0) NOT NULL PRIMARY KEY,\"name\" "
              "NVARCHAR2(128),\"amount\" NUMBER(30,6),\"payload\" BLOB,\"stamp\" TIMESTAMP(6) WITH "
              "TIME ZONE,\"flag\" NUMBER(1,0) NOT NULL)"
            : "CREATE TABLE ud_advanced_rows(id BIGINT NOT NULL PRIMARY KEY,name TEXT,amount "
              "NUMERIC(30,6),payload BYTEA,stamp TIMESTAMPTZ,flag BOOLEAN NOT NULL)";
    if (!resume) {
        OK(command(c, drop));
        OK(command(c, ddl));
    }
    OK(ud_connection_provision(c));
    if (!resume)
        OK(command(
            c, "DELETE FROM ud_sdk_receipts_v10 WHERE tenant='advanced' AND job='advanced-sql'"));
    ud_batch *metadata = NULL, *actual_schema = NULL;
    OK(ud_connection_discover(c, str("ud_advanced_rows"), &metadata));
    CHECK(ud_batch_rows(metadata) == 6);
    ud_batch_destroy(metadata);
    OK(ud_connection_schema(c, str("ud_advanced_rows"), str("advanced.sql"), &actual_schema));
    CHECK(ud_batch_columns(actual_schema) == 6);
    ud_metadata_options mo = {.struct_size = sizeof(mo),
                              .abi_version = UD_ABI_VERSION,
                              .kind = UD_METADATA_PRIMARY_KEYS,
                              .parent = {"ud_advanced_rows", 16},
                              .limit = 100};
    OK(ud_connection_metadata(c, &mo, &metadata));
    CHECK(ud_batch_rows(metadata) >= 1);
    ud_batch_destroy(metadata);
    ud_field fields[6];
    for (uint32_t j = 0; j < 6; j++)
        OK(ud_batch_field(actual_schema, j, fields + j));
    /* Driver metadata order must agree with the actual physical table declaration. */
    const char *names[] = {"id", "name", "amount", "payload", "stamp", "flag"};
    for (uint32_t j = 0; j < 6; j++)
        CHECK(!strcmp(fields[j].name, names[j]));
    ud_batch *b = NULL;
    OK(ud_batch_create(ctx, fields, 6, 1003, &b));
    for (uint32_t i = 1; i <= 1003; i++) {
        ud_value v[6];
        OK(values(fields, i, v));
        OK(ud_batch_append(b, v, 6));
    }
    OK(ud_batch_seal(b));
    uint32_t key = 0;
    ud_write_options w = {.struct_size = sizeof(w),
                          .abi_version = UD_ABI_VERSION,
                          .mode = UD_WRITE_UPSERT,
                          .table = {"ud_advanced_rows", 16},
                          .job = {"advanced-sql", 12},
                          .batch_id = {"1003", 4},
                          .source_position = {"1003", 4},
                          .keys = &key,
                          .key_count = 1,
                          .current_column = UINT32_MAX};
    if (native) {
        w.mode = UD_WRITE_APPEND;
        w.load_mode = UD_LOAD_NATIVE;
        CHECK(ud_connection_capabilities(c) & UD_CONNECT_NATIVE_BULK);
    }
    OK(ud_connection_write(c, &w, b));
    if (kill)
        _exit(91);
    OK(ud_connection_write(c, &w, b));
    ud_commit_receipt receipt = {0};
    OK(ud_connection_receipt(c, w.job, w.batch_id, &receipt));
    CHECK(receipt.committed && receipt.source_position.length == 4 &&
          !memcmp(receipt.source_position.data, "1003", 4));
    ud_free((void *)receipt.source_position.data);
    w.source_position = str("different");
    CHECK(ud_connection_write(c, &w, b) == UD_CORRUPT);
    w.source_position = str("1003");
    ud_batch *params = NULL;
    OK(ud_batch_create(ctx, fields, 1, 1, &params));
    ud_value lower[6];
    OK(values(fields, 1, lower));
    OK(ud_batch_append(params, lower, 1));
    OK(ud_batch_seal(params));
    char sql[512];
    const char *quote = dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB ? "`" : "\"";
    const char *marker = ud_connection_parameter_style(c) == UD_SQL_PARAMETER_NUMBERED
                             ? (dialect == UD_SQL_ORACLE ? ":1" : "$1")
                             : "?";
    snprintf(sql, sizeof(sql),
             "SELECT %sid%s,%sname%s,%samount%s,%spayload%s,%sstamp%s,%sflag%s FROM "
             "%sud_advanced_rows%s WHERE %sid%s >= %s ORDER BY %sid%s",
             quote, quote, quote, quote, quote, quote, quote, quote, quote, quote, quote, quote,
             quote, quote, quote, quote, marker, quote, quote);
    ud_read_options ro = {.struct_size = sizeof(ro),
                          .abi_version = UD_ABI_VERSION,
                          .batch_size = 127,
                          .sql = str(sql),
                          .parameters = params};
    OK(ud_connection_read_open(c, &ro, actual_schema));
    uint32_t count = 0;
    for (;;) {
        ud_batch *page = NULL;
        ud_status s = ud_connection_read_next(c, &page);
        if (s == UD_END)
            break;
        OK(s);
        CHECK(ud_batch_rows(page) <= 127);
        for (uint32_t r = 0; r < ud_batch_rows(page); r++) {
            ud_value expected[6];
            OK(values(fields, ++count, expected));
            for (uint32_t j = 0; j < 6; j++) {
                ud_value got;
                OK(ud_batch_get(page, r, j, &got));
                if (ud_value_compare(expected + j, &got)) {
                    fprintf(stderr, "typed mismatch row=%u column=%s\n", count, names[j]);
                    return 1;
                }
            }
        }
        ud_batch_destroy(page);
    }
    CHECK(count == 1003);
    OK(ud_connection_read_close(c));
    allowed = 0;
    CHECK(ud_connection_test(c) == UD_AUTH);
    OK(ud_connection_close(c));
    ud_batch_destroy(params);
    ud_batch_destroy(b);
    ud_batch_destroy(actual_schema);
    OK(ud_plugin_unload(p));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    printf("{\"nativeBulk\":%s,\"dialect\":%d,\"rows\":1003,\"tls\":\"verified\",\"prepared\":true,"
           "\"metadata\":true,\"nullUnicodeDecimalBinaryUTC\":true,\"receiptReplay\":true,"
           "\"revokedGrant\":true,\"resume\":%s}\n",
           native ? "true" : "false", dialect, resume ? "true" : "false");
    return 0;
}
