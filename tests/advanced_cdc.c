/* Actual provider snapshot/log stream, consumed entirely through the C ABI. */
#define main sql_fixture_main
#include "advanced_sql.c"
#undef main
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#endif
static void wait_for_log(void) {
#ifdef _WIN32
    Sleep(10);
#else
    struct timespec delay = {0, 10000000};
    nanosleep(&delay, NULL);
#endif
}
static ud_status collect(ud_connection *c, uint32_t expected_rows, int snapshot,
                         ud_string *position) {
    uint32_t rows = 0, begins = 0;
    int committed = 0;
    for (uint32_t turn = 0; turn < 1000; turn++) {
        ud_cdc_event event = {.struct_size = sizeof(event), .abi_version = UD_ABI_VERSION};
        ud_status s = ud_connection_cdc_next(c, &event);
        if (s == UD_BUSY) {
            wait_for_log();
            continue;
        }
        if (s)
            return s;
        if (event.kind == UD_CDC_BEGIN)
            begins++;
        if (event.kind == UD_CDC_ROWS) {
            uint32_t n = ud_batch_rows(event.after);
            for (uint32_t r = 0; r < n; r++) {
                if (!event.operations || event.operations[r] < UD_CHANGE_INSERT ||
                    event.operations[r] > UD_CHANGE_DELETE)
                    return UD_SCHEMA;
                if (!snapshot &&
                    (event.operations[r] == UD_CHANGE_UPDATE ||
                     event.operations[r] == UD_CHANGE_DELETE) &&
                    (!event.before || ud_batch_rows(event.before) != n))
                    return UD_SCHEMA;
            }
            rows += n;
        }
        if (event.kind == UD_CDC_COMMIT) {
            if (rows == expected_rows) {
                if (!begins || !event.position.length)
                    return UD_SCHEMA;
                char *p = ud_alloc(ctx, (size_t)event.position.length + 1);
                if (!p)
                    return UD_RESOURCE;
                memcpy(p, event.position.data, event.position.length);
                *position = (ud_string){p, event.position.length};
                s = ud_connection_cdc_confirm(c, event.position);
                committed = !s;
                ud_cdc_event_clear(&event);
                return committed ? UD_OK : s;
            }
        }
        ud_cdc_event_clear(&event);
    }
    return UD_TIMEOUT;
}
int main(int argc, char **argv) {
    CHECK(argc == 3);
    int allowed = 1;
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
    ud_connection *c = NULL, *writer = NULL;
    OK(ud_connection_open(p, &co, &c));
    OK(ud_connection_open(p, &co, &writer));
    int dialect = ud_connection_dialect(c);
    const char *drop =
        dialect == UD_SQL_ORACLE
            ? "BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"ud_advanced_cdc\" PURGE'; EXCEPTION WHEN "
              "OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
            : "DROP TABLE IF EXISTS ud_advanced_cdc";
    const char *ddl = dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB
                          ? "CREATE TABLE ud_advanced_cdc(id BIGINT NOT NULL PRIMARY KEY,name "
                            "VARCHAR(128) CHARACTER SET utf8mb4,rev BIGINT NOT NULL)"
                      : dialect == UD_SQL_SQLSERVER
                          ? "CREATE TABLE ud_advanced_cdc(id BIGINT NOT NULL PRIMARY KEY,name "
                            "NVARCHAR(128),rev BIGINT NOT NULL)"
                      : dialect == UD_SQL_ORACLE
                          ? "CREATE TABLE \"ud_advanced_cdc\"(\"id\" NUMBER(19,0) NOT NULL PRIMARY "
                            "KEY,\"name\" NVARCHAR2(128),\"rev\" NUMBER(19,0) NOT NULL)"
                          : "CREATE TABLE ud_advanced_cdc(id BIGINT NOT NULL PRIMARY KEY,name "
                            "TEXT,rev BIGINT NOT NULL)";
    if (dialect == UD_SQL_SQLSERVER)
        OK(command(writer, "IF EXISTS(SELECT 1 FROM cdc.change_tables WHERE "
                           "capture_instance=N'dbo_ud_advanced_cdc') EXEC sys.sp_cdc_disable_table "
                           "@source_schema=N'dbo',@source_name=N'ud_advanced_cdc',@capture_"
                           "instance=N'dbo_ud_advanced_cdc'"));
    OK(command(writer, drop));
    OK(command(writer, ddl));
    if (dialect == UD_SQL_SQLSERVER) {
        OK(command(writer, "EXEC sys.sp_cdc_enable_table "
                           "@source_schema=N'dbo',@source_name=N'ud_advanced_cdc',@role_name=NULL,@"
                           "supports_net_changes=1"));
        OK(ud_connection_provision(writer));
    }
    if (dialect == UD_SQL_POSTGRESQL) {
        OK(command(writer, "DO $$ BEGIN PERFORM pg_drop_replication_slot(slot_name) FROM "
                           "pg_replication_slots WHERE slot_name='ud_advanced_slot' AND NOT "
                           "active; END $$"));
        OK(command(writer, "ALTER TABLE ud_advanced_cdc REPLICA IDENTITY FULL"));
        OK(command(writer, "DROP PUBLICATION IF EXISTS ud_advanced_publication"));
        OK(command(writer, "CREATE PUBLICATION ud_advanced_publication FOR TABLE ud_advanced_cdc"));
    }
    if (dialect == UD_SQL_ORACLE)
        OK(command(writer,
                   "ALTER TABLE \"ud_advanced_cdc\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"));
    const char *initial = dialect == UD_SQL_ORACLE
                              ? "INSERT INTO \"ud_advanced_cdc\" VALUES(1,'one',1)"
                              : "INSERT INTO ud_advanced_cdc VALUES(1,'one',1)";
    OK(command(writer, initial));
    ud_batch *schema = NULL;
    OK(ud_connection_schema(c, str("ud_advanced_cdc"), str("advanced.cdc"), &schema));
    ud_cdc_options o = {.struct_size = sizeof(o),
                        .abi_version = UD_ABI_VERSION,
                        .batch_rows = 17,
                        .server_id = 64009,
                        .mode = UD_CDC_SNAPSHOT_STREAM,
                        .table = {"ud_advanced_cdc", 15},
                        .schema = schema,
                        .source_owner = {"advanced-cdc", 12},
                        .publication = {"ud_advanced_publication", 23}};
    if (dialect == UD_SQL_SQLSERVER) {
        o.slot = str("dbo_ud_advanced_cdc");
    }
    if (dialect == UD_SQL_POSTGRESQL)
        o.slot = str("ud_advanced_slot");
    fputs("opening CDC\n", stderr);
    OK(ud_connection_cdc_open(c, &o));
    ud_string snapshot = {0}, position = {0};
    fputs("snapshot\n", stderr);
    OK(collect(c, 1, 1, &snapshot));
    OK(ud_connection_begin(writer, UD_ISOLATION_READ_COMMITTED));
    OK(command(writer,
               dialect == UD_SQL_ORACLE
                   ? "UPDATE \"ud_advanced_cdc\" SET \"name\"='updated',\"rev\"=2 WHERE \"id\"=1"
                   : "UPDATE ud_advanced_cdc SET name='updated',rev=2 WHERE id=1"));
    OK(command(writer, dialect == UD_SQL_ORACLE
                           ? "INSERT INTO \"ud_advanced_cdc\" VALUES(2,'two',1)"
                           : "INSERT INTO ud_advanced_cdc VALUES(2,'two',1)"));
    OK(ud_connection_commit(writer));
    fputs("log changes\n", stderr);
    OK(collect(c, 2, 0, &position));
    CHECK(position.length && !(snapshot.length == position.length &&
                               !memcmp(snapshot.data, position.data, snapshot.length)));
    OK(ud_connection_cdc_close(c));
    o.mode = UD_CDC_STREAM;
    o.position = position;
    fputs("opening CDC\n", stderr);
    OK(ud_connection_cdc_open(c, &o));
    OK(command(writer, dialect == UD_SQL_ORACLE ? "DELETE FROM \"ud_advanced_cdc\" WHERE \"id\"=2"
                                                : "DELETE FROM ud_advanced_cdc WHERE id=2"));
    ud_string last = {0};
    OK(collect(c, 1, 0, &last));
    allowed = 0;
    ud_cdc_event event = {.struct_size = sizeof(event), .abi_version = UD_ABI_VERSION};
    CHECK(ud_connection_cdc_next(c, &event) == UD_AUTH);
    OK(ud_connection_cdc_close(c));
    allowed = 1;
    int schema_change = dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB;
    if (schema_change) {
        o.position = last;
        OK(ud_connection_cdc_open(c, &o));
        OK(command(writer, "ALTER TABLE ud_advanced_cdc ADD COLUMN added BIGINT NULL"));
        int changed = 0;
        for (uint32_t turn = 0; turn < 1000 && !changed; turn++) {
            ud_status s = ud_connection_cdc_next(c, &event);
            if (s == UD_BUSY) {
                wait_for_log();
                continue;
            }
            OK(s);
            if (event.kind == UD_CDC_SCHEMA_CHANGE) {
                CHECK(event.schema_version > 1 && ud_batch_columns(event.after) == 4);
                ud_field before, after;
                OK(ud_batch_field(schema, 0, &before));
                OK(ud_batch_field(event.after, 0, &after));
                CHECK(before.identity.high == after.identity.high &&
                      before.identity.low == after.identity.low);
                CHECK(event.position.length);
                changed = 1;
            }
            ud_cdc_event_clear(&event);
        }
        CHECK(changed);
        CHECK(ud_connection_cdc_next(c, &event) == UD_FENCED);
        OK(ud_connection_cdc_close(c));
    }
    if (dialect == UD_SQL_POSTGRESQL)
        OK(command(writer, "DO $$ BEGIN PERFORM pg_drop_replication_slot(slot_name) FROM "
                           "pg_replication_slots WHERE slot_name='ud_advanced_slot' AND NOT "
                           "active; END $$"));
    OK(ud_connection_close(writer));
    OK(ud_connection_close(c));
    ud_free((void *)snapshot.data);
    ud_free((void *)position.data);
    ud_free((void *)last.data);
    ud_batch_destroy(schema);
    OK(ud_plugin_unload(p));
    CHECK(!ud_memory_used(ctx));
    OK(ud_context_destroy(ctx));
    printf("{\"dialect\":%d,\"snapshotFence\":true,\"logInsertUpdateDelete\":true,\"beforeImages\":"
           "true,\"resume\":true,\"revoke\":true,\"schemaChangeFence\":%s}\n",
           dialect, schema_change ? "true" : "null");
    return 0;
}
