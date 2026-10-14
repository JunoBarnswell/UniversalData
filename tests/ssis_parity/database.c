/* All setup/readback uses the real driver through the public Native API. */
#include "../check.h"
#include "ud/plugin.h"
#include "ud/format.h"
#include <string.h>
static ud_context *ctx;
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static char *read_file(ud_context *c, const char *path, uint32_t *length) {
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    if (fseek(f, 0, SEEK_END)) {
        fclose(f);
        return NULL;
    }
    long n = ftell(f);
    if (n < 0 || n > 10000000 || fseek(f, 0, SEEK_SET)) {
        fclose(f);
        return NULL;
    }
    char *out = ud_alloc(c, (size_t)n + 1);
    if (!out) {
        fclose(f);
        return NULL;
    }
    if (fread(out, 1, (size_t)n, f) != (size_t)n) {
        fclose(f);
        ud_free(out);
        return NULL;
    }
    fclose(f);
    *length = (uint32_t)n;
    return out;
}
static ud_status grant(void *user, ud_string t, ud_string r, int32_t action) {
    (void)user;
    (void)t;
    (void)r;
    (void)action;
    return UD_OK;
}
static ud_status resolve(void *user, ud_context *c, ud_string t, ud_string r, ud_string ref,
                         ud_string *out) {
    (void)t;
    (void)r;
    (void)ref;
    uint32_t n = 0;
    char *data = read_file(c, user, &n);
    if (!data)
        return UD_IO;
    *out = (ud_string){data, n};
    return UD_OK;
}
#undef OK
#define OK(call)                                                                                   \
    do {                                                                                           \
        ud_status status = (call);                                                                 \
        if (status) {                                                                              \
            fprintf(stderr, "Native database acceptance status=%d line=%d\n", status, __LINE__);   \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)
int main(int argc, char **argv) {
    CHECK(argc == 5 || argc == 6);
    OK(ud_context_create(NULL, &ctx));
    ud_plugin *plugin = NULL;
    OK(ud_plugin_load(ctx, argv[1], 0, &plugin));
    ud_connection_options options = {.struct_size = sizeof(options),
                                     .abi_version = UD_ABI_VERSION,
                                     .tenant = {"ssis-parity", 11},
                                     .resource = {"isolated-ssis-fixture", 21},
                                     .credential_reference = {"acceptance", 10},
                                     .authorize = grant,
                                     .resolve = resolve,
                                     .resolve_user = argv[2]};
    ud_connection *connection = NULL;
    OK(ud_connection_open(plugin, &options, &connection));
    uint32_t n = 0;
    char *sql = read_file(ctx, argv[4], &n);
    CHECK(sql);
    ud_read_options command = {.struct_size = sizeof(command),
                               .abi_version = UD_ABI_VERSION,
                               .sql = {sql, n},
                               .batch_size = 1024};
    if (!strcmp(argv[3], "execute"))
        OK(ud_connection_execute_command(connection, &command, NULL));
    else {
        CHECK(argc == 6);
        uint32_t sn = 0;
        char *schema_json = read_file(ctx, argv[5], &sn);
        CHECK(schema_json);
        ud_batch *schema = NULL;
        OK(ud_schema_from_json(ctx, schema_json, sn, str("ssis-readback"), &schema));
        ud_free(schema_json);
        OK(ud_connection_read_open(connection, &command, schema));
        ud_batch_destroy(schema);
        for (;;) {
            ud_batch *batch = NULL;
            ud_status s = ud_connection_read_next(connection, &batch);
            if (s == UD_END)
                break;
            OK(s);
            char *text = NULL;
            uint32_t bytes;
            OK(ud_batch_to_json(batch, &text, &bytes));
            printf("%.*s\n", (int)bytes, text);
            ud_free(text);
            ud_batch_destroy(batch);
        }
        OK(ud_connection_read_close(connection));
    }
    ud_free(sql);
    OK(ud_connection_close(connection));
    ud_plugin_unload(plugin);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
