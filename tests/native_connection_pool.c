#include "check.h"
#include "ud/package.h"
#include <string.h>

typedef struct scope {
    const char *database;
    int allowed;
} scope;

static ud_string text(const char *value) {
    return (ud_string){value, (uint32_t)strlen(value)};
}

static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)tenant;
    (void)resource;
    (void)action;
    return ((scope *)user)->allowed ? UD_OK : UD_AUTH;
}

static ud_status resolve(void *user, ud_context *ctx, ud_string tenant, ud_string resource,
                         ud_string reference, ud_string *out) {
    (void)tenant;
    (void)resource;
    (void)reference;
    ud_string path = text(((scope *)user)->database);
    char *copy = ud_alloc(ctx, (size_t)path.length + 1);
    if (!copy)
        return UD_RESOURCE;
    memcpy(copy, path.data, (size_t)path.length + 1);
    *out = (ud_string){copy, path.length};
    return UD_OK;
}

static int changes(ud_sql_session *session, int64_t expected) {
    ud_batch *schema = NULL, *batch = NULL;
    ud_value value;
    OK(ud_sql_session_read_open(session, text("SELECT total_changes() AS changes"), (ud_string){0},
                                (ud_string){0}, NULL, 8, UD_READ_DISCOVER, &schema));
    CHECK(ud_batch_columns(schema) == 1);
    OK(ud_sql_session_read_next(session, &batch));
    CHECK(ud_batch_rows(batch) == 1);
    OK(ud_batch_get(batch, 0, 0, &value));
    CHECK(value.type == UD_INT64 && !value.is_null && value.integer == expected);
    ud_batch_destroy(batch);
    ud_batch_destroy(schema);
    OK(ud_sql_session_read_close(session));
    return 0;
}

int main(int argc, char **argv) {
    CHECK(argc == 4);
    const char *document = argv[1];
    ud_context_options context = {.struct_size = sizeof(context), .abi_version = UD_ABI_VERSION};
    ud_connection_pool *pool = NULL;
    ud_execution *first = NULL, *second = NULL, *rotated = NULL;
    ud_sql_session *primary = NULL, *sibling = NULL;
    scope a = {argv[2], 1}, b = {argv[2], 0};
    ud_execution_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION,
        .context = context, .edge_capacity = 2, .max_batch_rows = 8, .max_depth = 8,
        .tenant = {"fixture", 7}, .authorize = authorize, .resolve = resolve,
        .authorize_user = &a, .resolve_user = &a};
    CHECK(ud_execution_create_json_in_pool(NULL, text(document), &options, &first) == UD_INVALID);
    OK(ud_connection_pool_create(2, 8 * 1024 * 1024, &pool));
    OK(ud_execution_create_json_in_pool(pool, text(document), &options, &first));
    CHECK(ud_connection_pool_destroy(pool) == UD_BUSY);
    OK(ud_execution_sql_session_open(first, text("db"), &primary));
    uint64_t affected;
    OK(ud_sql_session_execute(primary, text("CREATE TABLE entries(id INTEGER)"), NULL, &affected));
    OK(ud_sql_session_execute(primary, text("INSERT INTO entries VALUES(1),(2)"), NULL, &affected));
    CHECK(affected == 2 && !changes(primary, 2));
    OK(ud_sql_session_close(primary));

    options.authorize_user = options.resolve_user = &b;
    OK(ud_execution_create_json_in_pool(pool, text(document), &options, &second));
    CHECK(ud_execution_sql_session_open(second, text("db"), &sibling) == UD_AUTH && !sibling);
    b.allowed = 1;
    OK(ud_execution_sql_session_open(second, text("db"), &sibling));
    CHECK(!changes(sibling, 2));
    ud_execution_cancel(first);
    CHECK(!changes(sibling, 2));
    OK(ud_sql_session_close(sibling));
    OK(ud_execution_destroy(first));
    OK(ud_execution_destroy(second));

    b.database = argv[3];
    OK(ud_execution_create_json_in_pool(pool, text(document), &options, &rotated));
    OK(ud_execution_sql_session_open(rotated, text("db"), &sibling));
    CHECK(!changes(sibling, 0));
    OK(ud_sql_session_close(sibling));
    OK(ud_execution_destroy(rotated));
    OK(ud_connection_pool_destroy(pool));
    return 0;
}
