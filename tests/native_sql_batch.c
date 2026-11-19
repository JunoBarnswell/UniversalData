#include "ud/plugin.h"
#include "check.h"
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

static ud_string text(const char *value) {
    return (ud_string){value, (uint32_t)strlen(value)};
}

static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)resource;
    (void)action;
    return *(int *)user && tenant.length == 8 && !memcmp(tenant.data, "tenant-a", 8) ? UD_OK
                                                                                     : UD_AUTH;
}

static ud_status resolve(void *user, ud_context *ctx, ud_string tenant, ud_string resource,
                         ud_string reference, ud_string *out) {
    (void)resource;
    if (tenant.length != 8 || memcmp(tenant.data, "tenant-a", 8) || reference.length != 7 ||
        memcmp(reference.data, "fixture", 7))
        return UD_AUTH;
    const char *path = user;
    uint32_t length = (uint32_t)strlen(path);
    char *copy = ud_alloc(ctx, (size_t)length + 1);
    if (!copy)
        return UD_RESOURCE;
    memcpy(copy, path, length);
    *out = (ud_string){copy, length};
    return UD_OK;
}

static int prepare_fixture(const char *path) {
    sqlite3 *db = NULL;
    if (sqlite3_open(path, &db) != SQLITE_OK)
        return 0;
    const char *sql = "DROP TABLE IF EXISTS ud_native_batch;"
                      "DROP TABLE IF EXISTS ud_delete_audit;"
                      "CREATE TABLE ud_native_batch(id INTEGER PRIMARY KEY,value TEXT NOT NULL,"
                      "score INTEGER NOT NULL);"
                      "CREATE TABLE ud_delete_audit(sequence INTEGER PRIMARY KEY,id INTEGER NOT NULL);"
                      "CREATE TRIGGER record_delete AFTER DELETE ON ud_native_batch BEGIN "
                      "INSERT INTO ud_delete_audit(id) VALUES(old.id); END;";
    int result = sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK;
    sqlite3_close(db);
    return result;
}

static int verify_delete_order(const char *path) {
    sqlite3 *db = NULL;
    sqlite3_stmt *query = NULL;
    CHECK(sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK);
    CHECK(sqlite3_prepare_v2(db, "SELECT id FROM ud_delete_audit ORDER BY sequence", -1,
                             &query, NULL) == SQLITE_OK);
    CHECK(sqlite3_step(query) == SQLITE_ROW && sqlite3_column_int(query, 0) == 3);
    CHECK(sqlite3_step(query) == SQLITE_ROW && sqlite3_column_int(query, 0) == 2);
    CHECK(sqlite3_step(query) == SQLITE_DONE);
    CHECK(sqlite3_finalize(query) == SQLITE_OK);
    CHECK(sqlite3_close(db) == SQLITE_OK);
    return 0;
}

static int make_rows(ud_context *ctx, uint32_t count, int64_t first, const char *prefix,
                     ud_batch **out) {
    ud_field fields[] = {{.name = "id", .name_length = 2, .type = UD_INT64},
                         {.name = "value", .name_length = 5, .type = UD_UTF8},
                         {.name = "score", .name_length = 5, .type = UD_INT64}};
    OK(ud_batch_create(ctx, fields, 3, count, out));
    for (uint32_t i = 0; i < count; i++) {
        char value[64];
        snprintf(value, sizeof(value), "%s-%u", prefix, i);
        ud_value row[] = {{.type = UD_INT64, .integer = first + (int64_t)i},
                          {.type = UD_UTF8, .text = value, .text_length = (uint32_t)strlen(value)},
                          {.type = UD_INT64, .integer = (int64_t)i}};
        OK(ud_batch_append(*out, row, 3));
    }
    OK(ud_batch_seal(*out));
    return 0;
}

static int read_count(ud_connection *connection, ud_context *ctx, uint64_t expected) {
    ud_field fields[] = {{.name = "id", .name_length = 2, .type = UD_INT64},
                         {.name = "value", .name_length = 5, .type = UD_UTF8},
                         {.name = "score", .name_length = 5, .type = UD_INT64}};
    ud_batch *schema = NULL;
    OK(ud_batch_create(ctx, fields, 3, 0, &schema));
    const char sql[] = "SELECT id,value,score FROM ud_native_batch ORDER BY id";
    ud_read_options options = {.struct_size = sizeof(options),
                               .abi_version = UD_ABI_VERSION,
                               .batch_size = 2,
                               .sql = {sql, sizeof(sql) - 1}};
    OK(ud_connection_read_open(connection, &options, schema));
    uint64_t count = 0;
    for (;;) {
        ud_batch *page = NULL;
        ud_status status = ud_connection_read_next(connection, &page);
        if (status == UD_END)
            break;
        OK(status);
        count += ud_batch_rows(page);
        ud_batch_destroy(page);
    }
    OK(ud_connection_read_close(connection));
    ud_batch_destroy(schema);
    CHECK(count == expected);
    return 0;
}

static int discover_and_scalar(ud_connection *connection) {
    const char discover_sql[] = "SELECT id,value,score FROM ud_native_batch ORDER BY id";
    ud_batch *discovered = NULL;
    ud_read_options discover = {.struct_size = sizeof(discover),
                                .abi_version = UD_ABI_VERSION,
                                .batch_size = 2,
                                .sql = {discover_sql, sizeof(discover_sql) - 1},
                                .result_mode = UD_READ_DISCOVER,
                                .result_schema = &discovered};
    OK(ud_connection_read_open(connection, &discover, NULL));
    CHECK(discovered && ud_batch_columns(discovered) == 3 && ud_batch_rows(discovered) == 0);
    ud_batch *page = NULL;
    OK(ud_connection_read_next(connection, &page));
    CHECK(page && ud_batch_rows(page) == 2);
    ud_batch_destroy(page);
    OK(ud_connection_read_close(connection));
    ud_batch_destroy(discovered);

    const char scalar_sql[] = "SELECT score AS value FROM ud_native_batch ORDER BY id";
    ud_batch *scalar_schema = NULL;
    ud_read_options scalar = {.struct_size = sizeof(scalar),
                              .abi_version = UD_ABI_VERSION,
                              .batch_size = 64,
                              .sql = {scalar_sql, sizeof(scalar_sql) - 1},
                              .result_mode = UD_READ_SCALAR,
                              .result_schema = &scalar_schema};
    OK(ud_connection_read_open(connection, &scalar, NULL));
    CHECK(scalar_schema && ud_batch_columns(scalar_schema) == 1);
    page = NULL;
    OK(ud_connection_read_next(connection, &page));
    CHECK(page && ud_batch_rows(page) == 1);
    ud_batch_destroy(page);
    CHECK(ud_connection_read_next(connection, &page) == UD_END);
    OK(ud_connection_read_close(connection));
    ud_batch_destroy(scalar_schema);
    return 0;
}

static int revoke_during_cursor(ud_connection *connection, int *allowed) {
    ud_batch *schema = NULL;
    ud_read_options read = {.struct_size = sizeof(read), .abi_version = UD_ABI_VERSION,
                            .sql = text("SELECT id FROM ud_native_batch"),
                            .batch_size = 1, .result_mode = UD_READ_DISCOVER,
                            .result_schema = &schema};
    OK(ud_connection_read_open(connection, &read, NULL));
    ud_batch *batch = NULL;
    OK(ud_connection_read_next(connection, &batch));
    CHECK(ud_batch_rows(batch) == 1);
    ud_batch_destroy(batch);
    *allowed = 0;
    CHECK(ud_connection_read_next(connection, &batch) == UD_AUTH && !batch);
    OK(ud_connection_read_close(connection));
    ud_batch_destroy(schema);
    *allowed = 1;
    return 0;
}

static int read_after_releasing_parameter(ud_connection *connection, ud_context *ctx) {
    ud_field parameter = {.name = "value", .name_length = 5, .type = UD_UTF8};
    ud_field field = {.name = "id", .name_length = 2, .type = UD_INT64};
    ud_batch *parameters = NULL, *shape = NULL, *page = NULL;
    ud_value value = {.type = UD_UTF8, .text = "initial-0", .text_length = 9};
    OK(ud_batch_create(ctx, &parameter, 1, 1, &parameters));
    OK(ud_batch_append(parameters, &value, 1));
    OK(ud_batch_seal(parameters));
    OK(ud_batch_create(ctx, &field, 1, 0, &shape));
    ud_read_options read = {.struct_size = sizeof(read), .abi_version = UD_ABI_VERSION,
                            .sql = text("SELECT id FROM ud_native_batch WHERE value = ?"),
                            .parameters = parameters, .batch_size = 1};
    OK(ud_connection_read_open(connection, &read, shape));
    ud_batch_destroy(parameters);

    OK(ud_batch_create(ctx, &parameter, 1, 1, &parameters));
    value.text = "different";
    OK(ud_batch_append(parameters, &value, 1));
    OK(ud_batch_seal(parameters));
    OK(ud_connection_read_next(connection, &page));
    CHECK(ud_batch_rows(page) == 1);
    OK(ud_batch_get(page, 0, 0, &value));
    CHECK(value.type == UD_INT64 && value.integer == 1);
    ud_batch_destroy(page);
    OK(ud_connection_read_close(connection));
    ud_batch_destroy(parameters);
    ud_batch_destroy(shape);
    return 0;
}

int main(int argc, char **argv) {
    CHECK(argc == 3);
    CHECK(prepare_fixture(argv[2]));
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_plugin *plugin = NULL;
    OK(ud_plugin_load(ctx, argv[1], 0, &plugin));
    int allowed = 1;
    ud_connection_options options = {.struct_size = sizeof(options),
                                     .abi_version = UD_ABI_VERSION,
                                     .tenant = {"tenant-a", 8},
                                     .resource = {"test-db", 7},
                                     .credential_reference = {"fixture", 7},
                                     .authorize_user = &allowed,
                                     .authorize = authorize,
                                     .resolve_user = argv[2],
                                     .resolve = resolve};
    ud_connection *connection = NULL;
    OK(ud_connection_open(plugin, &options, &connection));
    OK(ud_connection_provision(connection));
    OK(ud_connection_test(connection));

    ud_batch *insert = NULL;
    CHECK(make_rows(ctx, 4, 1, "initial", &insert) == 0);
    uint64_t affected = 0;
    uint32_t chunk = 2;
    ud_write_options append = {.struct_size = sizeof(append),
                               .abi_version = UD_ABI_VERSION,
                               .mode = UD_WRITE_APPEND,
                               .table = {"ud_native_batch", 15},
                               .delivery = UD_DELIVERY_RESPONSE,
                               .chunk_rows = chunk,
                               .affected_rows = &affected};
    OK(ud_connection_write(connection, &append, insert));
    CHECK(affected == 4);
    ud_batch_destroy(insert);
    CHECK(read_count(connection, ctx, 4) == 0);
    CHECK(discover_and_scalar(connection) == 0);
    CHECK(read_after_releasing_parameter(connection, ctx) == 0);
    CHECK(revoke_during_cursor(connection, &allowed) == 0);
    CHECK(read_count(connection, ctx, 4) == 0);

    ud_batch *update = NULL;
    CHECK(make_rows(ctx, 2, 1, "updated", &update) == 0);
    uint32_t key = 0;
    affected = 0;
    ud_write_options change = {.struct_size = sizeof(change),
                               .abi_version = UD_ABI_VERSION,
                               .mode = UD_WRITE_UPDATE,
                               .table = {"ud_native_batch", 15},
                               .keys = &key,
                               .key_count = 1,
                               .delivery = UD_DELIVERY_RESPONSE,
                               .chunk_rows = 1,
                               .affected_rows = &affected};
    OK(ud_connection_write(connection, &change, update));
    CHECK(affected == 2);
    ud_batch_destroy(update);
    CHECK(read_count(connection, ctx, 4) == 0);

    ud_batch *delete_rows = NULL;
    ud_field id_field = {.name = "id", .name_length = 2, .type = UD_INT64};
    OK(ud_batch_create(ctx, &id_field, 1, 2, &delete_rows));
    ud_value id = {.type = UD_INT64, .integer = 3};
    OK(ud_batch_append(delete_rows, &id, 1));
    id.integer = 2;
    OK(ud_batch_append(delete_rows, &id, 1));
    OK(ud_batch_seal(delete_rows));
    affected = 0;
    ud_write_options remove = change;
    remove.mode = UD_WRITE_DELETE;
    remove.chunk_rows = 2;
    remove.affected_rows = &affected;
    OK(ud_connection_write(connection, &remove, delete_rows));
    CHECK(affected == 2);
    OK(ud_connection_write(connection, &remove, delete_rows));
    CHECK(affected == 0);
    ud_batch_destroy(delete_rows);
    CHECK(read_count(connection, ctx, 2) == 0);
    CHECK(verify_delete_order(argv[2]) == 0);

    OK(ud_connection_begin(connection, UD_ISOLATION_SERIALIZABLE));
    ud_batch *rolled = NULL;
    CHECK(make_rows(ctx, 1, 1, "rollback", &rolled) == 0);
    affected = 0;
    OK(ud_connection_write(connection, &change, rolled));
    CHECK(affected == 1);
    OK(ud_connection_rollback(connection));
    ud_batch_destroy(rolled);
    CHECK(read_count(connection, ctx, 2) == 0);

    ud_batch *empty = NULL;
    ud_field empty_field = {.name = "id", .name_length = 2, .type = UD_INT64};
    OK(ud_batch_create(ctx, &empty_field, 1, 0, &empty));
    OK(ud_batch_seal(empty));
    affected = 99;
    remove.affected_rows = &affected;
    OK(ud_connection_write(connection, &remove, empty));
    CHECK(affected == 0);
    ud_batch_destroy(empty);

    OK(ud_connection_close(connection));
    OK(ud_plugin_unload(plugin));
    CHECK(!ud_memory_used(ctx));
    OK(ud_context_destroy(ctx));
    return 0;
}
