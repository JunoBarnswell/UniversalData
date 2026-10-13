#include "ud/plugin.h"
#include "check.h"
#include <sqlite3.h>
#include <string.h>
#include <math.h>
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)resource;
    (void)action;
    return *(int *)user && tenant.length == 8 && !memcmp(tenant.data, "tenant-a", 8) ? UD_OK
                                                                                     : UD_AUTH;
}
static int exec_fixture(sqlite3 *db, const char *sql) {
    return sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK;
}
static ud_status resolve(void *user, ud_context *ctx, ud_string tenant, ud_string resource,
                         ud_string reference, ud_string *out) {
    const char **paths = user;
    if (tenant.length != 8 || memcmp(tenant.data, "tenant-a", 8) || reference.length != 7 ||
        memcmp(reference.data, "fixture", 7))
        return UD_AUTH;
    int index = resource.length == 6 && !memcmp(resource.data, "source", 6)   ? 0
                : resource.length == 6 && !memcmp(resource.data, "target", 6) ? 1
                                                                              : -1;
    if (index < 0)
        return UD_AUTH;
    uint32_t length = (uint32_t)strlen(paths[index]);
    char *copy = ud_alloc(ctx, (size_t)length + 1);
    if (!copy)
        return UD_RESOURCE;
    memcpy(copy, paths[index], length);
    *out = (ud_string){copy, length};
    return UD_OK;
}
static int transfer(ud_context *ctx, ud_connection *source, ud_connection *target, const char *sql,
                    const char *job) {
    ud_field fields[] = {{.name = "id", .name_length = 2, .type = UD_INT64},
                         {.name = "name", .name_length = 4, .type = UD_UTF8},
                         {.name = "rev", .name_length = 3, .type = UD_INT64}};
    ud_batch *schema = NULL;
    OK(ud_batch_create(ctx, fields, 3, 0, &schema));
    ud_read_options read = {sizeof(read), UD_ABI_VERSION, 257, {sql, (uint32_t)strlen(sql)}};
    OK(ud_connection_read_open(source, &read, schema));
    ud_batch_destroy(schema);
    uint32_t key = 0;
    uint64_t rows = 0;
    uint32_t number = 0;
    for (;;) {
        ud_batch *batch = NULL;
        ud_status s = ud_connection_read_next(source, &batch);
        if (s == UD_END)
            break;
        OK(s);
        char id[32], position[64];
        snprintf(id, sizeof(id), "%u", number++);
        ud_value last;
        OK(ud_batch_get(batch, ud_batch_rows(batch) - 1, 0, &last));
        snprintf(position, sizeof(position), "id=%lld", (long long)last.integer);
        ud_write_options write = {.struct_size = sizeof(write),
                                  .abi_version = UD_ABI_VERSION,
                                  .mode = UD_WRITE_UPSERT,
                                  .table = {"users", 5},
                                  .job = {job, (uint32_t)strlen(job)},
                                  .batch_id = {id, (uint32_t)strlen(id)},
                                  .source_position = {position, (uint32_t)strlen(position)},
                                  .keys = &key,
                                  .key_count = 1};
        OK(ud_connection_write(target, &write, batch));
        /* Repeating a committed batch must not change data / receipt / cursor. */
        OK(ud_connection_write(target, &write, batch));
        ud_commit_receipt receipt;
        OK(ud_connection_receipt(target, write.job, write.batch_id, &receipt));
        CHECK(receipt.committed);
        CHECK(receipt.source_position.length == strlen(position) &&
              !memcmp(receipt.source_position.data, position, strlen(position)));
        ud_free((void *)receipt.source_position.data);
        write.source_position = (ud_string){"different", 9};
        CHECK(ud_connection_write(target, &write, batch) == UD_CORRUPT);
        rows += ud_batch_rows(batch);
        ud_batch_destroy(batch);
    }
    OK(ud_connection_read_close(source));
    CHECK(rows == (!strcmp(job, "full") ? 10000u : 250u));
    return 0;
}
int main(int argc, char **argv) {
    CHECK(argc == 4);
    const char *source_path = argv[2], *target_path = argv[3];
    sqlite3 *source = NULL, *target = NULL;
    CHECK(sqlite3_open(source_path, &source) == SQLITE_OK &&
          sqlite3_open(target_path, &target) == SQLITE_OK);
    CHECK(exec_fixture(source, "CREATE TABLE users(id INTEGER PRIMARY KEY,name TEXT NOT NULL,rev "
                               "INTEGER NOT NULL); BEGIN"));
    sqlite3_stmt *insert = NULL;
    CHECK(sqlite3_prepare_v2(source, "INSERT INTO users VALUES(?1,?2,1)", -1, &insert, NULL) ==
          SQLITE_OK);
    for (int id = 0; id < 10000; id++) {
        char name[64];
        snprintf(name, sizeof(name), "中文-%d", id);
        sqlite3_reset(insert);
        sqlite3_bind_int(insert, 1, id);
        sqlite3_bind_text(insert, 2, name, -1, SQLITE_TRANSIENT);
        CHECK(sqlite3_step(insert) == SQLITE_DONE);
    }
    sqlite3_finalize(insert);
    CHECK(exec_fixture(source, "COMMIT"));
    CHECK(exec_fixture(
        target,
        "CREATE TABLE users(id INTEGER PRIMARY KEY,name TEXT NOT NULL,rev INTEGER NOT NULL)"));
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_plugin *plugin = NULL;
    OK(ud_plugin_load(ctx, argv[1], 0, &plugin));
    int allowed = 1;
    const char *paths[] = {source_path, target_path};
    ud_connection_options options = {.struct_size = sizeof(options),
                                     .abi_version = UD_ABI_VERSION,
                                     .tenant = {"tenant-a", 8},
                                     .resource = {"source", 6},
                                     .credential_reference = {"fixture", 7},
                                     .authorize_user = &allowed,
                                     .resolve_user = paths,
                                     .resolve = resolve};
    ud_connection *src = NULL, *dst = NULL;
    CHECK(ud_connection_open(plugin, &options, &src) == UD_AUTH && !src);
    options.authorize = authorize;
    OK(ud_connection_open(plugin, &options, &src));
    options.resource = (ud_string){"target", 6};
    OK(ud_connection_open(plugin, &options, &dst));
    OK(ud_connection_provision(dst));
    CHECK((ud_connection_capabilities(dst) & UD_CONNECT_UPSERT) != 0);
    CHECK((ud_connection_capabilities(dst) & UD_CONNECT_OVERWRITE) == 0);
    OK(ud_connection_test(src));
    ud_batch *discovery = NULL;
    OK(ud_connection_discover(src, (ud_string){"users", 5}, &discovery));
    CHECK(ud_batch_rows(discovery) == 3);
    ud_batch_destroy(discovery);
    CHECK(transfer(ctx, src, dst, "SELECT id,name,rev FROM users ORDER BY id", "full") == 0);
    CHECK(exec_fixture(source, "BEGIN; UPDATE users SET name='更新',rev=2 WHERE id<50; COMMIT"));
    CHECK(sqlite3_prepare_v2(source, "INSERT INTO users VALUES(?1,?2,2)", -1, &insert, NULL) ==
          SQLITE_OK);
    CHECK(exec_fixture(source, "BEGIN"));
    for (int id = 10000; id < 10200; id++) {
        char name[64];
        snprintf(name, sizeof(name), "新增-%d", id);
        sqlite3_reset(insert);
        sqlite3_bind_int(insert, 1, id);
        sqlite3_bind_text(insert, 2, name, -1, SQLITE_TRANSIENT);
        CHECK(sqlite3_step(insert) == SQLITE_DONE);
    }
    sqlite3_finalize(insert);
    CHECK(exec_fixture(source, "COMMIT"));
    CHECK(transfer(ctx, src, dst,
                   "SELECT id,name,rev FROM users WHERE rev>1 AND rev<=2 ORDER BY rev,id",
                   "incremental") == 0);
    sqlite3_stmt *left = NULL, *right = NULL;
    CHECK(sqlite3_prepare_v2(source, "SELECT id,name,rev FROM users ORDER BY id", -1, &left,
                             NULL) == SQLITE_OK);
    CHECK(sqlite3_prepare_v2(target, "SELECT id,name,rev FROM users ORDER BY id", -1, &right,
                             NULL) == SQLITE_OK);
    int count = 0;
    while (sqlite3_step(left) == SQLITE_ROW) {
        CHECK(sqlite3_step(right) == SQLITE_ROW);
        CHECK(sqlite3_column_int64(left, 0) == sqlite3_column_int64(right, 0));
        CHECK(sqlite3_column_int64(left, 2) == sqlite3_column_int64(right, 2));
        CHECK(sqlite3_column_bytes(left, 1) == sqlite3_column_bytes(right, 1));
        CHECK(!memcmp(sqlite3_column_text(left, 1), sqlite3_column_text(right, 1),
                      (size_t)sqlite3_column_bytes(left, 1)));
        count++;
    }
    CHECK(count == 10200 && sqlite3_step(right) == SQLITE_DONE);
    sqlite3_finalize(left);
    sqlite3_finalize(right);
    /* SQLite itself coerces NaN to NULL. The driver must reject it, roll back
     * all rows and the receipt, then accept a corrected retry of this batch. */
    CHECK(exec_fixture(target, "CREATE TABLE float_values(f REAL NOT NULL)"));
    ud_field float_field = {.name = "f", .name_length = 1, .type = UD_FLOAT64};
    ud_batch *floats = NULL;
    OK(ud_batch_create(ctx, &float_field, 1, 2, &floats));
    ud_value value = {.type = UD_FLOAT64, .real = 1.0};
    OK(ud_batch_append(floats, &value, 1));
    value.real = NAN;
    OK(ud_batch_append(floats, &value, 1));
    ud_write_options float_write = {.struct_size = sizeof(float_write),
                                    .abi_version = UD_ABI_VERSION,
                                    .mode = UD_WRITE_APPEND,
                                    .table = {"float_values", 12},
                                    .job = {"nan", 3},
                                    .batch_id = {"0", 1},
                                    .source_position = {"2", 1}};
    OK(ud_batch_seal(floats));
    CHECK(ud_connection_write(dst, &float_write, floats) == UD_UNSUPPORTED);
    ud_commit_receipt receipt;
    OK(ud_connection_receipt(dst, float_write.job, float_write.batch_id, &receipt));
    CHECK(!receipt.committed);
    ud_free((void *)receipt.source_position.data);
    ud_batch_destroy(floats);
    CHECK(sqlite3_prepare_v2(target, "SELECT count(*) FROM float_values", -1, &right, NULL) ==
          SQLITE_OK);
    CHECK(sqlite3_step(right) == SQLITE_ROW && sqlite3_column_int64(right, 0) == 0);
    sqlite3_finalize(right);
    OK(ud_batch_create(ctx, &float_field, 1, 2, &floats));
    value.real = INFINITY;
    OK(ud_batch_append(floats, &value, 1));
    value.real = -INFINITY;
    OK(ud_batch_append(floats, &value, 1));
    OK(ud_batch_seal(floats));
    OK(ud_connection_write(dst, &float_write, floats));
    ud_batch_destroy(floats);
    CHECK(sqlite3_prepare_v2(target, "SELECT f FROM float_values ORDER BY rowid", -1, &right,
                             NULL) == SQLITE_OK);
    CHECK(sqlite3_step(right) == SQLITE_ROW && sqlite3_column_type(right, 0) == SQLITE_FLOAT &&
          sqlite3_column_double(right, 0) == INFINITY);
    CHECK(sqlite3_step(right) == SQLITE_ROW && sqlite3_column_type(right, 0) == SQLITE_FLOAT &&
          sqlite3_column_double(right, 0) == -INFINITY);
    CHECK(sqlite3_step(right) == SQLITE_DONE);
    sqlite3_finalize(right);
    allowed = 0;
    CHECK(ud_connection_test(dst) == UD_AUTH);
    allowed = 1;
    CHECK(ud_plugin_unload(plugin) == UD_BUSY);
    OK(ud_connection_close(src));
    OK(ud_connection_close(dst));
    options.tenant = (ud_string){"tenant-b", 8};
    CHECK(ud_connection_open(plugin, &options, &dst) == UD_AUTH);
    OK(ud_plugin_unload(plugin));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    CHECK(sqlite3_close(source) == SQLITE_OK && sqlite3_close(target) == SQLITE_OK);
    return 0;
}
