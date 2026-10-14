#include "check.h"
#include "ud/package.h"
#include "ud/json.h"
#include <string.h>
static ud_string text(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)user;
    (void)resource;
    (void)action;
    return tenant.length == 7 && !memcmp(tenant.data, "fixture", 7) ? UD_OK : UD_AUTH;
}
static ud_status resolve(void *user, ud_context *ctx, ud_string tenant, ud_string resource,
                         ud_string reference, ud_string *out) {
    (void)tenant;
    (void)resource;
    if (reference.length != 7 || memcmp(reference.data, "fixture", 7))
        return UD_AUTH;
    const char *path = user;
    size_t n = strlen(path);
    char *copy = ud_alloc(ctx, n + 1);
    if (!copy)
        return UD_RESOURCE;
    memcpy(copy, path, n + 1);
    *out = (ud_string){copy, (uint32_t)n};
    return UD_OK;
}
int main(int argc, char **argv) {
    CHECK(argc == 5 || argc == 6);
    FILE *f = fopen(argv[1], "rb");
    CHECK(f && !fseek(f, 0, SEEK_END));
    long n = ftell(f);
    CHECK(n > 0);
    rewind(f);
    char *document = malloc((size_t)n);
    CHECK(document && fread(document, 1, (size_t)n, f) == (size_t)n);
    fclose(f);
    ud_execution_options options = {
        .struct_size = sizeof(options),
        .abi_version = UD_ABI_VERSION,
        .context = {.struct_size = sizeof(options.context), .abi_version = UD_ABI_VERSION},
        .edge_capacity = 1,
        .max_batch_rows = 3,
        .max_depth = 32,
        .max_concurrency = 4,
        .tenant = {"fixture", 7},
        .execution_id = {"four-engines", 12},
        .authorize = authorize,
        .resolve = resolve,
        .resolve_user = argv[2],
        .timeout_ms = 10000};
    if (argc == 6)
        options.state_directory = text(argv[5]);
    ud_execution *e = NULL;
    ud_status status = ud_execution_create_json((ud_string){document, (uint32_t)n}, &options, &e);
    free(document);
    if (!status)
        status = ud_execution_execute(e);
    if (status != atoi(argv[3]))
        fprintf(stderr, "actual=%d expected=%s\n", status, argv[3]);
    CHECK(status == atoi(argv[3]));
    if (e) {
        ud_value value;
        OK(ud_execution_variable(e, text("total"), &value));
        CHECK(value.integer == strtoll(argv[4], NULL, 10));
        char *diagnostics = NULL;
        uint32_t size = 0;
        OK(ud_execution_diagnostics(e, &diagnostics, &size));
        ud_context *ctx = NULL;
        OK(ud_context_create(NULL, &ctx));
        ud_json *json = NULL;
        OK(ud_json_parse(ctx, diagnostics, size, &json, NULL));
        ud_json_destroy(json);
        OK(ud_context_destroy(ctx));
        printf("%.*s\n", (int)size, diagnostics);
        ud_free(diagnostics);
        OK(ud_execution_destroy(e));
    }
    return 0;
}
