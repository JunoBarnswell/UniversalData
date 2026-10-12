/* Local PostgreSQL regression: interrupt a blocked FETCH, honor its deadline,
 * and retain bounded single-row paging for successful cursor reads. */
#include "ud/plugin.h"
#include "check.h"
#include "../components/foundation/thread.h"
#ifdef _WIN32
#include <process.h>
#endif
typedef struct fixture {
    const char *configuration;
    uint64_t deadline;
} fixture;
static ud_string text(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)user; (void)tenant; (void)resource; (void)action;
    return UD_OK;
}
static ud_status check(void *user) {
    fixture *f = user;
    return f->deadline && ud_now_ms() >= f->deadline ? UD_TIMEOUT : UD_OK;
}
static ud_status resolve(void *user, ud_context *ctx, ud_string tenant, ud_string resource,
                         ud_string reference, ud_string *out) {
    (void)tenant; (void)resource; (void)reference;
    fixture *f = user;
    char *copy = ud_strdup_n(ctx, f->configuration, strlen(f->configuration));
    if (!copy) return UD_RESOURCE;
    *out = text(copy);
    return UD_OK;
}
#ifdef _WIN32
static unsigned __stdcall cancel_read(void *connection) {
#else
static void *cancel_read(void *connection) {
#endif
    ud_sleep_ms(200);
    ud_connection_cancel(connection);
    return 0;
}
static int run(ud_plugin *plugin, ud_context *ctx, fixture *f, int mode) {
    ud_connection_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION,
        .tenant = {"fixture", 7}, .resource = {"cursor", 6}, .credential_reference = {"local", 5},
        .authorize = authorize, .authorize_user = f, .check = check,
        .resolve = resolve, .resolve_user = f};
    ud_connection *connection = NULL;
    OK(ud_connection_open(plugin, &options, &connection));
    ud_field field = {.name = "x", .name_length = 1, .type = UD_INT64};
    ud_batch *schema = NULL;
    OK(ud_batch_create(ctx, &field, 1, 0, &schema));
    ud_read_options read = {.struct_size = sizeof(read), .abi_version = UD_ABI_VERSION,
        .batch_size = 2, .sql = text(mode ? "SELECT 1::bigint AS x FROM pg_sleep(3)"
                                        : "SELECT generate_series(1, 5)::bigint AS x")};
    OK(ud_connection_read_open(connection, &read, schema));
    uint64_t started = ud_now_ms();
    if (mode == 2) f->deadline = started + 200;
#ifdef _WIN32
    HANDLE thread = NULL;
    if (mode == 1) {
        thread = (HANDLE)_beginthreadex(NULL, 0, cancel_read, connection, 0, NULL);
        CHECK(thread);
    }
#else
    pthread_t thread;
    if (mode == 1) CHECK(!pthread_create(&thread, NULL, cancel_read, connection));
#endif
    ud_batch *batch = NULL;
    ud_status status = ud_connection_read_next(connection, &batch);
    uint64_t elapsed = ud_now_ms() - started;
#ifdef _WIN32
    if (thread) { WaitForSingleObject(thread, INFINITE); CloseHandle(thread); }
#else
    if (mode == 1) CHECK(!pthread_join(thread, NULL));
#endif
    if (mode) {
        CHECK(status == (mode == 1 ? UD_CANCELLED : UD_TIMEOUT));
        CHECK(!batch && elapsed < 1500);
        printf("%s interrupted FETCH in %llu ms\n", mode == 1 ? "Cancel" : "Deadline",
               (unsigned long long)elapsed);
    } else {
        uint32_t rows = 0;
        while (status == UD_OK) {
            CHECK(ud_batch_rows(batch) > 0 && ud_batch_rows(batch) <= read.batch_size);
            for (uint32_t row = 0; row < ud_batch_rows(batch); row++) {
                ud_value value;
                OK(ud_batch_get(batch, row, 0, &value));
                CHECK(value.integer == (int64_t)++rows);
            }
            ud_batch_destroy(batch); batch = NULL;
            status = ud_connection_read_next(connection, &batch);
        }
        CHECK(status == UD_END && rows == 5 && !batch);
    }
    f->deadline = 0;
    ud_batch_destroy(schema);
    OK(ud_connection_close(connection));
    return 0;
}
int main(int argc, char **argv) {
    CHECK(argc == 3);
    fixture f = {.configuration = argv[2]};
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_plugin *plugin = NULL;
    OK(ud_plugin_load(ctx, argv[1], 0, &plugin));
    for (int mode = 0; mode < 3; mode++) CHECK(!run(plugin, ctx, &f, mode));
    OK(ud_plugin_unload(plugin));
    CHECK(!ud_memory_used(ctx));
    OK(ud_context_destroy(ctx));
    return 0;
}
