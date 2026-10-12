#include "check.h"
#include "ud/package.h"
#include <string.h>
static ud_status grant(void *u, ud_string tenant, ud_string resource, int32_t action) {
    (void)u;
    (void)resource;
    (void)action;
    return tenant.length == 7 && !memcmp(tenant.data, "fixture", 7) ? UD_OK : UD_AUTH;
}
static ud_status credential(void *u, ud_context *c, ud_string tenant, ud_string resource,
                            ud_string ref, ud_string *out) {
    (void)tenant;
    (void)resource;
    const char **paths = u;
    int which = ref.length == 6 && !memcmp(ref.data, "source", 6)   ? 0
                : ref.length == 6 && !memcmp(ref.data, "target", 6) ? 1
                                                                    : -1;
    if (which < 0)
        return UD_AUTH;
    size_t n = strlen(paths[which]);
    char *copy = ud_alloc(c, n + 1);
    if (!copy)
        return UD_RESOURCE;
    memcpy(copy, paths[which], n + 1);
    *out = (ud_string){copy, (uint32_t)n};
    return UD_OK;
}
int main(int argc, char **argv) {
    CHECK(argc == 4);
    FILE *f = fopen(argv[1], "rb");
    CHECK(f);
    CHECK(!fseek(f, 0, SEEK_END));
    long n = ftell(f);
    CHECK(n > 0 && !fseek(f, 0, SEEK_SET));
    char *text = malloc((size_t)n);
    CHECK(text);
    CHECK(fread(text, 1, (size_t)n, f) == (size_t)n);
    fclose(f);
    const char *paths[] = {argv[2], argv[3]};
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .edge_capacity = 1,
                              .max_batch_rows = 11,
                              .max_depth = 16,
                              .tenant = {"fixture", 7},
                              .execution_id = {"sql-fixture", 11},
                              .authorize = grant,
                              .resolve = credential,
                              .resolve_user = paths};
    o.context = (ud_context_options){
        .struct_size = sizeof(o.context), .abi_version = UD_ABI_VERSION, .memory_limit = 8388608};
    ud_execution *e = NULL;
    OK(ud_execution_create_json((ud_string){text, (uint32_t)n}, &o, &e));
    free(text);
    OK(ud_execution_execute(e));
    ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
    OK(ud_execution_get_metrics(e, &m));
    CHECK(m.state == UD_EXEC_SUCCEEDED && m.input_rows == 31 && m.output_rows == 31 &&
          m.completed_tasks == 4);
    OK(ud_execution_destroy(e));
    return 0;
}
