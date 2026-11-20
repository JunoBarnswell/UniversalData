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
static int record_commands(ud_connection *connection,int dialect,const ud_batch *schema) {
    ud_field fields[2]; OK(ud_batch_field(schema,0,fields)); OK(ud_batch_field(schema,5,fields+1));
    ud_batch *parameters=NULL; OK(ud_batch_create(ctx,fields,2,1,&parameters));
    ud_field all[6];
    for (uint32_t at=0; at<6; at++) OK(ud_batch_field(schema,at,all+at));
    ud_value row[6]; OK(values(all,2001,row));
    ud_value selected[]={row[0],row[5]}; OK(ud_batch_append(parameters,selected,2)); OK(ud_batch_seal(parameters));
    int style=ud_connection_parameter_style(connection);
    const char *first=style==UD_SQL_PARAMETER_NUMBERED ? "$1" : style==UD_SQL_PARAMETER_NAMED ? ":1" : "?";
    const char *second=style==UD_SQL_PARAMETER_NUMBERED ? "$2" : style==UD_SQL_PARAMETER_NAMED ? ":2" : "?";
    char sql[256];
    snprintf(sql,sizeof(sql),dialect==UD_SQL_ORACLE
        ? "INSERT INTO \"ud_advanced_rows\"(\"id\",\"flag\") VALUES(%s,%s)"
        : "INSERT INTO ud_advanced_rows(id,flag) VALUES(%s,%s)",first,second);
    ud_read_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,.sql=str(sql),.parameters=parameters};
    ud_command_result outcome={.struct_size=sizeof(outcome),.abi_version=UD_ABI_VERSION};
    OK(ud_connection_execute_record(connection,&options,&outcome));
    CHECK(outcome.state==UD_COMMAND_COMMITTED && outcome.affected_rows==1); ud_command_result_clear(&outcome);
    CHECK(ud_connection_execute_record(connection,&options,&outcome)==UD_QUALITY);
    CHECK(outcome.state==UD_COMMAND_REJECTED && outcome.provider_code.length); ud_command_result_clear(&outcome);
    OK(ud_connection_begin(connection,UD_ISOLATION_SERIALIZABLE));
    char statement[256];
    snprintf(statement,sizeof(statement),dialect==UD_SQL_ORACLE
        ? "UPDATE \"ud_advanced_rows\" SET \"id\"=2002 WHERE \"id\"=2001"
        : "UPDATE ud_advanced_rows SET id=2002 WHERE id=2001");
    ud_read_options update={.struct_size=sizeof(update),.abi_version=UD_ABI_VERSION,.sql=str(statement)};
    OK(ud_connection_execute_record(connection,&update,&outcome));
    CHECK(outcome.state==UD_COMMAND_APPLIED && outcome.affected_rows==1); ud_command_result_clear(&outcome);
    /* A rejected row must not roll back the sibling row's preceding update. */
    OK(ud_connection_execute_record(connection,&options,&outcome));
    CHECK(outcome.state==UD_COMMAND_APPLIED); ud_command_result_clear(&outcome);
    CHECK(ud_connection_execute_record(connection,&options,&outcome)==UD_QUALITY);
    CHECK(outcome.state==UD_COMMAND_REJECTED); ud_command_result_clear(&outcome);
    OK(ud_connection_rollback(connection));
    if (dialect==UD_SQL_MYSQL || dialect==UD_SQL_MARIADB) {
        OK(command(connection,"ALTER TABLE ud_advanced_rows ENGINE=MyISAM"));
        CHECK(ud_connection_execute_record(connection,&options,&outcome)==UD_UNSUPPORTED);
        CHECK(outcome.state==UD_COMMAND_REJECTED); ud_command_result_clear(&outcome);
        OK(command(connection,"ALTER TABLE ud_advanced_rows ENGINE=InnoDB"));
        OK(command(connection,"CREATE TRIGGER ud_record_trigger BEFORE INSERT ON ud_advanced_rows FOR EACH ROW SET NEW.flag=NEW.flag"));
        CHECK(ud_connection_execute_record(connection,&options,&outcome)==UD_UNSUPPORTED);
        CHECK(outcome.state==UD_COMMAND_REJECTED); ud_command_result_clear(&outcome);
        OK(command(connection,"DROP TRIGGER ud_record_trigger"));
    }
    snprintf(statement,sizeof(statement),dialect==UD_SQL_ORACLE ? "SELECT \"id\" FROM \"ud_advanced_rows\"" : "SELECT id FROM ud_advanced_rows");
    ud_batch *projection=NULL; OK(ud_batch_create(ctx,fields,1,0,&projection));
    ud_read_options read={.struct_size=sizeof(read),.abi_version=UD_ABI_VERSION,.batch_size=7,.sql=str(statement)};
    OK(ud_connection_read_open(connection,&read,projection)); ud_batch_destroy(projection);
    ud_batch *actual=NULL; OK(ud_connection_read_next(connection,&actual)); CHECK(ud_batch_rows(actual)==1);
    ud_value value; OK(ud_batch_get(actual,0,0,&value)); CHECK(ud_value_compare(&value,row)==0); ud_batch_destroy(actual);
    CHECK(ud_connection_read_next(connection,&actual)==UD_END); OK(ud_connection_read_close(connection));
    ud_batch_destroy(parameters);
    puts("row commands: committed/rejected/applied, savepoint isolation and outer rollback verified against real business rows");
    return 0;
}
static int changeset(ud_connection *c, uint32_t count) {
    CHECK(count && count <= 50000 && count % 5 == 0);
    OK(command(c, "DROP TABLE IF EXISTS ud_ordered_changes"));
    OK(command(c, "CREATE TABLE ud_ordered_changes(id BIGINT PRIMARY KEY, rev BIGINT NULL)"));
    ud_batch *schema = NULL, *after = NULL, *before = NULL;
    OK(ud_connection_schema(c, str("ud_ordered_changes"), str("changes"), &schema));
    ud_field fields[2];
    OK(ud_batch_field(schema, 0, fields));
    OK(ud_batch_field(schema, 1, fields + 1));
    OK(ud_batch_create(ctx, fields, 2, count, &after));
    OK(ud_batch_create(ctx, fields, 2, count, &before));
    int32_t *actions = calloc(count, sizeof(*actions));
    CHECK(actions);
    const int32_t sequence[] = {UD_CHANGE_INSERT, UD_CHANGE_UPDATE, UD_CHANGE_UPDATE,
                                UD_CHANGE_DELETE, UD_CHANGE_INSERT};
    for (uint32_t i = 0; i < count; i++) {
        uint32_t at = i % 5, base = 2 * (i / 5) + 1;
        actions[i] = sequence[at];
        ud_value old[] = {{.type=UD_INT64,.integer=base + (at>=2)},
                          {.type=UD_INT64,.integer=at==1 ? 7 : 8,.is_null=at==3}};
        ud_value now[] = {{.type=UD_INT64,.integer=base + (at!=0)},
                          {.type=UD_INT64,.integer=at==0 ? 7 : at==1 ? 8 : base,.is_null=at==2 || at==3}};
        if (at==2) now[0] = (ud_value){.type=UD_INT64,.is_null=1,.is_absent=1};
        OK(ud_batch_append(before, old, 2));
        OK(ud_batch_append(after, now, 2));
    }
    OK(ud_batch_seal(before));
    OK(ud_batch_seal(after));
    uint32_t key = 0;
    ud_write_options w = {.struct_size=sizeof(w),.abi_version=UD_ABI_VERSION,.mode=UD_WRITE_CHANGESET,
        .table=str("ud_ordered_changes"),.before=before,.operations=actions,.key_count=1,.keys=&key,
        .current_column=UINT32_MAX,.job=str("ordered"),
        .batch_id=str(count==5000 ? "changes-5k" : "changes-50k"),.source_position=str("done")};
    OK(ud_connection_provision(c));
    OK(command(c,"DELETE FROM ud_sdk_receipts_v11 WHERE tenant='advanced' AND job='ordered'"));
    OK(ud_connection_write(c,&w,after));
    OK(ud_connection_write(c,&w,after));
    ud_batch *failed_after = NULL, *failed_before = NULL;
    OK(ud_batch_create(ctx, fields, 2, 2, &failed_after));
    OK(ud_batch_create(ctx, fields, 2, 2, &failed_before));
    ud_value insert[] = {{.type=UD_INT64,.integer=-1},{.type=UD_INT64,.integer=10}};
    ud_value stale[] = {{.type=UD_INT64,.integer=2},{.type=UD_INT64,.integer=-999}};
    ud_value revision[] = {{.type=UD_INT64,.integer=2},{.type=UD_INT64,.integer=11}};
    OK(ud_batch_append(failed_after, insert, 2));
    OK(ud_batch_append(failed_after, revision, 2));
    OK(ud_batch_append(failed_before, insert, 2));
    OK(ud_batch_append(failed_before, stale, 2));
    OK(ud_batch_seal(failed_after));
    OK(ud_batch_seal(failed_before));
    const int32_t bad[] = {UD_CHANGE_INSERT,UD_CHANGE_UPDATE};
    w.before = failed_before;
    w.operations = bad;
    w.batch_id = str("ordered-stale-preimage");
    CHECK(ud_connection_write(c,&w,failed_after)==UD_FENCED);
    ud_batch_destroy(failed_before);
    ud_batch_destroy(failed_after);
    ud_read_options read = {.struct_size=sizeof(read),.abi_version=UD_ABI_VERSION,.batch_size=127,
                            .sql=str("SELECT id,rev FROM ud_ordered_changes ORDER BY id")};
    OK(ud_connection_read_open(c,&read,schema));
    uint32_t seen = 0;
    ud_batch *page = NULL;
    ud_status s;
    while ((s=ud_connection_read_next(c,&page))==UD_OK) {
        for (uint32_t i=0;i<ud_batch_rows(page);i++) {
            ud_value id,rev;
            OK(ud_batch_get(page,i,0,&id));
            OK(ud_batch_get(page,i,1,&rev));
            CHECK(id.integer==2*(int64_t)seen+2 && !rev.is_null && rev.integer==id.integer-1);
            seen++;
        }
        ud_batch_destroy(page);
    }
    CHECK(s==UD_END && seen==count/5);
    OK(ud_connection_read_close(c));
    ud_batch_destroy(schema); ud_batch_destroy(before); ud_batch_destroy(after); free(actions);
    printf("{\"changes\":%u,\"rowsVerified\":%u,\"receiptReplay\":true,\"preimageRollback\":true}\n",count,seen);
    return 0;
}
int main(int argc, char **argv) {
    CHECK(argc >= 3);
    int resume = argc > 3 && !strcmp(argv[3], "resume"),
        kill = argc > 3 && !strcmp(argv[3], "kill"),
        native = argc > 3 && !strcmp(argv[3], "native"),
        records = argc > 3 && !strcmp(argv[3], "row-command"), allowed = 1;
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
    if (argc == 5 && !strcmp(argv[3], "changeset")) {
        CHECK(!changeset(c, (uint32_t)strtoul(argv[4], NULL, 10)));
        OK(ud_connection_close(c));
        OK(ud_plugin_unload(p));
        OK(ud_context_destroy(ctx));
        return 0;
    }
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
              "TIME ZONE,\"flag\" NUMBER(1,0) NOT NULL) SEGMENT CREATION IMMEDIATE"
            : "CREATE TABLE ud_advanced_rows(id BIGINT NOT NULL PRIMARY KEY,name TEXT,amount "
              "NUMERIC(30,6),payload BYTEA,stamp TIMESTAMPTZ,flag BOOLEAN NOT NULL)";
    if (!resume) {
        OK(command(c, drop));
        OK(command(c, ddl));
    }
    OK(ud_connection_provision(c));
    if (!resume)
        OK(command(
            c, "DELETE FROM ud_sdk_receipts_v11 WHERE tenant='advanced' AND job='advanced-sql'"));
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
    if (records) {
        CHECK((ud_connection_capabilities(c)&UD_CONNECT_TRANSACTIONAL_DML)!=0);
        CHECK(!record_commands(c,dialect,actual_schema)); ud_batch_destroy(actual_schema);
        OK(ud_connection_close(c)); OK(ud_plugin_unload(p)); CHECK(!ud_memory_used(ctx)); OK(ud_context_destroy(ctx)); return 0;
    }
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
    int parameter_style = ud_connection_parameter_style(c);
    const char *marker = parameter_style == UD_SQL_PARAMETER_NAMED ? ":1"
                         : parameter_style == UD_SQL_PARAMETER_NUMBERED ? "$1"
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
