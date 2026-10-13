#include "check.h"
#include "ud/package.h"
#include "ud/format.h"
#include <string.h>
#define REQUIRE(condition)                                                                         \
    do {                                                                                           \
        if (!(condition)) {                                                                        \
            fprintf(stderr, "fixture failure at %s:%d\n", __FILE__, __LINE__);                     \
            exit(1);                                                                               \
        }                                                                                          \
    } while (0)
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static char *read_fixture(const char *path, uint32_t *length) {
    FILE *f = fopen(path, "rb");
    REQUIRE(f && !fseek(f, 0, SEEK_END));
    long size = ftell(f);
    REQUIRE(size > 0 && size <= 16777216);
    rewind(f);
    char *s = malloc((size_t)size + 1);
    REQUIRE(s && fread(s, 1, (size_t)size, f) == (size_t)size);
    s[size] = 0;
    fclose(f);
    *length = (uint32_t)size;
    return s;
}
static void write_fixture(const char *path, const void *data, size_t length) {
    FILE *f = fopen(path, "wb");
    REQUIRE(f && fwrite(data, 1, length, f) == length && !fclose(f));
}
static ud_status authorize(void *u, ud_string t, ud_string r, int32_t action) {
    (void)u;
    (void)t;
    (void)r;
    (void)action;
    return UD_OK;
}
static ud_status resolve(void *u, ud_context *ctx, ud_string t, ud_string r, ud_string reference,
                         ud_string *out) {
    (void)u;
    (void)t;
    if (reference.length != 7 || memcmp(reference.data, "fixture", 7))
        return UD_AUTH;
    char *s = ud_alloc(ctx, (size_t)r.length + 1);
    if (!s)
        return UD_RESOURCE;
    memcpy(s, r.data, r.length);
    s[r.length] = 0;
    *out = (ud_string){s, r.length};
    return UD_OK;
}
int main(int argc, char **argv) {
    CHECK(argc >= 3);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    char *json = NULL;
    uint32_t length = 0;
    if (!strcmp(argv[1], "--compare")) {
        CHECK(argc == 6);
        uint32_t n[3];
        char *s[3];
        for (int i = 0; i < 3; i++)
            s[i] = read_fixture(argv[i + 2], n + i);
        OK(ud_schema_compare_json(ctx, (ud_string){s[0], n[0]}, (ud_string){s[1], n[1]},
                                  (ud_string){s[2], n[2]}, &json, &length));
        write_fixture(argv[5], json, length);
        for (int i = 0; i < 3; i++)
            free(s[i]);
    } else if (!strcmp(argv[1], "--identify")) {
        CHECK(argc == 5);
        uint32_t n;
        char *wire = read_fixture(argv[2], &n);
        ud_batch *b = NULL, *identified = NULL;
        OK(ud_batch_deserialize(ctx, (uint8_t *)wire, n, &b));
        OK(ud_batch_seal(b));
        OK(ud_batch_identify(ctx, b, (ud_id){17, 23}, 100, &identified));
        uint8_t *body = NULL;
        uint64_t size = 0;
        OK(ud_batch_serialize(identified, &body, &size));
        write_fixture(argv[3], body, (size_t)size);
        OK(ud_batch_row_info_json(identified, 0, &json, &length));
        write_fixture(argv[4], json, length);
        ud_free(body);
        ud_batch_destroy(identified);
        ud_batch_destroy(b);
        free(wire);
    } else if (!strcmp(argv[1], "--instantiate")) {
        CHECK(argc == 4);
        uint32_t n;
        char *s = read_fixture(argv[2], &n);
        ud_package *package = NULL;
        OK(ud_package_create(ctx, (ud_string){s, n}, &package));
        OK(ud_package_serialize(package, &json, &length));
        write_fixture(argv[3], json, length);
        ud_package_destroy(package);
        free(s);
    } else if (!strcmp(argv[1], "--catalog") || !strcmp(argv[1], "--explain") ||
               !strcmp(argv[1], "--execute")) {
        CHECK(argc == 4);
        uint32_t n;
        char *s = read_fixture(argv[2], &n);
        ud_execution *e = NULL;
        ud_execution_options o = {.struct_size = sizeof(o),
                                  .abi_version = UD_ABI_VERSION,
                                  .context = {.struct_size = sizeof(ud_context_options),
                                              .abi_version = UD_ABI_VERSION,
                                              .memory_limit = 268435456},
                                  .edge_capacity = 2,
                                  .max_batch_rows = 4096,
                                  .max_depth = 32,
                                  .max_concurrency = 1,
                                  .tenant = {"fixture", 7},
                                  .authorize = authorize,
                                  .resolve = resolve};
        ud_status opened = ud_execution_create_json((ud_string){s, n}, &o, &e);
        if (opened) fprintf(stderr, "Native binding: %s\n", ud_error(ctx));
        OK(opened);
        if (!strcmp(argv[1], "--catalog"))
            OK(ud_execution_component_catalog(e, &json, &length));
        else if (!strcmp(argv[1], "--explain"))
            OK(ud_execution_explain(e, &json, &length));
        else {
            OK(ud_execution_execute(e));
            OK(ud_execution_diagnostics(e, &json, &length));
        }
        write_fixture(argv[3], json, length);
        ud_free(json);
        json = NULL;
        OK(ud_execution_destroy(e));
        free(s);
    } else if (!strcmp(argv[1], "--discover")) {
        CHECK(argc == 6);
        uint32_t n;
        char *s = read_fixture(argv[2], &n);
        ud_execution *e = NULL;
        ud_execution_options o = {.struct_size = sizeof(o),
                                  .abi_version = UD_ABI_VERSION,
                                  .context = {.struct_size = sizeof(ud_context_options),
                                              .abi_version = UD_ABI_VERSION,
                                              .memory_limit = 268435456},
                                  .edge_capacity = 2,
                                  .max_batch_rows = 4096,
                                  .max_depth = 32,
                                  .max_concurrency = 1,
                                  .tenant = {"fixture", 7},
                                  .authorize = authorize,
                                  .resolve = resolve};
        ud_status opened = ud_execution_create_json((ud_string){s, n}, &o, &e);
        if (opened) fprintf(stderr, "Native binding: %s\n", ud_error(ctx));
        OK(opened);
        OK(ud_execution_discover_schema(e, str(argv[3]), str(argv[4]), &json, &length));
        write_fixture(argv[5], json, length);
        ud_free(json);
        json = NULL;
        OK(ud_execution_destroy(e));
        free(s);
    } else
        CHECK(0);
    ud_free(json);
    CHECK(!ud_memory_used(ctx));
    OK(ud_context_destroy(ctx));
    return 0;
}
