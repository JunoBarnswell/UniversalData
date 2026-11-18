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
static int ordered_changes(ud_context *ctx) {
    ud_field fields[] = {{.name="id",.name_length=2,.type=UD_INT64},
                         {.name="label",.name_length=5,.type=UD_UTF8,.nullable=1}};
    ud_batch *after = NULL, *before = NULL;
    OK(ud_batch_create(ctx, fields, 2, 3, &after));
    OK(ud_batch_create(ctx, fields, 2, 3, &before));
    for (int i = 0; i < 3; i++) {
        ud_value v[] = {{.type=UD_INT64,.integer=i==0 ? 1 : 2},
                        {.type=UD_UTF8,.text="v",.text_length=1}};
        OK(ud_batch_append(after, v, 2));
        if (i == 1) v[0].integer = 1;
        OK(ud_batch_append(before, v, 2));
    }
    OK(ud_batch_seal(after));
    OK(ud_batch_seal(before));
    uint32_t key = 0;
    int32_t actions[] = {UD_CHANGE_INSERT, UD_CHANGE_UPDATE, UD_CHANGE_DELETE};
    ud_write_options options = {.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,
        .mode=UD_WRITE_CHANGESET,.table={"target",6},.keys=&key,.key_count=1,
        .current_column=UINT32_MAX,.operations=actions,.before=before};
    ud_sql_statement sql = {0};
    OK(ud_sql_compile_write(ctx, UD_SQL_SQLSERVER, UD_SQL_PARAMETER_QUESTION,
                            &options, after, 0, 3, &sql));
    const char *ins = strstr(sql.sql.data,"INSERT"), *upd = strstr(sql.sql.data,"UPDATE"),
               *del = strstr(sql.sql.data,"DELETE");
    CHECK(ins && upd && del && ins < upd && upd < del);
    CHECK(strstr(ins,"@@ROWCOUNT") < upd && strstr(upd,"@@ROWCOUNT") < del && strstr(del,"@@ROWCOUNT"));
    int observed_before = 0;
    for (uint32_t i = 0; i < sql.parameter_count; i++) {
        CHECK(!i || sql.parameters[i-1].row <= sql.parameters[i].row);
        observed_before |= sql.parameters[i].row==1 && sql.parameters[i].source==UD_SQL_BEFORE;
    }
    CHECK(observed_before);
    ud_sql_statement_clear(&sql);
    ud_batch_destroy(after);
    ud_batch_destroy(before);
    return 0;
}
int main(void) {
    const char *valid_commands[] = {
        "INSERT INTO target(a,b) VALUES(?1,?2)",
        "UPDATE [target] SET [value]=:value WHERE id=:id; -- complete\n",
        "DELETE FROM target WHERE label='literal; COMMIT'",
        "WITH rows AS (SELECT 1 AS id) INSERT INTO target SELECT id FROM rows",
        "/* prepare */ MERGE INTO target USING source ON target.id=source.id WHEN MATCHED THEN UPDATE SET value=source.value"
    };
    const char *unsafe_commands[] = {
        "COMMIT", "CREATE TABLE target(a INT)", "UPDATE target SET a=1; COMMIT",
        "DELETE FROM target; INSERT INTO target VALUES(1)", "CALL external_side_effect()",
        "WITH t AS (SELECT 1) SELECT * FROM t", "INSERT INTO target VALUES($body$;COMMIT$body$)",
        "INSERT INTO target VALUES('back\\slash')", "UPDATE target SET a=1 /*! COMMIT */",
        "UPDATE target SET a=1 /* unterminated", "UPDATE target SET a=1 # comment", "UPDATE target SET a=(1"
    };
    for (uint32_t at=0; at<sizeof(valid_commands)/sizeof(*valid_commands); at++)
        OK(ud_sql_validate_record_command((ud_string){valid_commands[at],(uint32_t)strlen(valid_commands[at])}));
    for (uint32_t at=0; at<sizeof(unsafe_commands)/sizeof(*unsafe_commands); at++)
        CHECK(ud_sql_validate_record_command((ud_string){unsafe_commands[at],(uint32_t)strlen(unsafe_commands[at])}) != UD_OK);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    CHECK(!ordered_changes(ctx));
    const char *target_commands[]={"INSERT INTO `schema`.`ta``ble`(x) VALUES(?)", "UPDATE target SET x=ABS(?) WHERE id=?", "DELETE FROM target WHERE id=?"};
    for (uint32_t at=0; at<sizeof(target_commands)/sizeof(*target_commands); at++) {
        ud_string schema={0},table={0};
        OK(ud_sql_record_target(ctx,(ud_string){target_commands[at],(uint32_t)strlen(target_commands[at])},&schema,&table));
        const char *expected=at ? "target" : "ta`ble";
        CHECK(table.length==strlen(expected) && !memcmp(table.data,expected,table.length));
        CHECK(schema.length==(at ? 0u : 6u)); ud_free((void *)schema.data); ud_free((void *)table.data);
    }
    const char *unproved_targets[]={"UPDATE a,b SET a.x=1,b.x=2", "DELETE FROM a USING a,b WHERE a.id=b.id", "INSERT INTO a SELECT x FROM b", "INSERT INTO a VALUES(custom_function(?))", "UPDATE a SET x=other.ABS(?)", "WITH a AS (SELECT 1) INSERT INTO b VALUES(1)", "UPDATE `a.b` SET x=1", "UPDATE a SET x=1 /*M! COMMIT */"};
    for (uint32_t at=0; at<sizeof(unproved_targets)/sizeof(*unproved_targets); at++) {
        ud_string schema={0},table={0};
        CHECK(ud_sql_record_target(ctx,(ud_string){unproved_targets[at],(uint32_t)strlen(unproved_targets[at])},&schema,&table)!=UD_OK);
        CHECK(!schema.data && !table.data);
    }
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
