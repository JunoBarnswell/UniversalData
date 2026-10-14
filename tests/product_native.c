#include "check.h"
#include "ud/package.h"
#include "ud/format.h"
#include <string.h>
static ud_string str(const char *text) {
    return (ud_string){text, (uint32_t)strlen(text)};
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)user;
    (void)tenant;
    (void)resource;
    (void)action;
    return UD_OK;
}
static ud_status resolve(void *user, ud_context *memory, ud_string tenant, ud_string resource,
                         ud_string reference, ud_string *out) {
    (void)tenant;
    (void)resource;
    const char *path = user;
    if (!path || !*path)
        return UD_AUTH;
    FILE *file = fopen(path, "rb");
    if (!file)
        return UD_IO;
    char bytes[65536];
    size_t count = fread(bytes, 1, sizeof(bytes), file);
    int failed = ferror(file) || count == sizeof(bytes);
    fclose(file);
    if (failed)
        return UD_RESOURCE;
    ud_json *document = NULL;
    ud_status parsed = ud_json_parse(memory, bytes, (uint32_t)count, &document, NULL);
    if (parsed)
        return parsed;
    const ud_json *value = ud_json_find(document, reference.data, reference.length);
    char *text = NULL;
    uint32_t length = 0;
    ud_status status = value ? ud_json_serialize(value, &text, &length) : UD_AUTH;
    ud_json_destroy(document);
    if (!status)
        *out = (ud_string){text, length};
    return status;
}
int main(int argc, char **argv) {
    CHECK(argc >= 3 && argc <= 6);
    FILE *file = fopen(argv[1], "rb");
    CHECK(file && !fseek(file, 0, SEEK_END));
    long length = ftell(file);
    CHECK(length > 0 && length <= 16777216 && !fseek(file, 0, SEEK_SET));
    char *document = malloc((size_t)length);
    CHECK(document && fread(document, 1, (size_t)length, file) == (size_t)length);
    fclose(file);
    ud_execution_options options = {.struct_size = sizeof(options),
                                    .abi_version = UD_ABI_VERSION,
                                    .context = {.struct_size = sizeof(ud_context_options),
                                                .abi_version = UD_ABI_VERSION,
                                                .memory_limit = 268435456},
                                    .tenant = {"fixture", 7},
                                    .execution_id = {"product-native", 14},
                                    .authorize = authorize,
                                    .max_batch_rows = 7,
                                    .edge_capacity = 2,
                                    .max_concurrency = 2,
                                    .timeout_ms = 30000,
                                    .state_directory = argc > 3 ? str(argv[3]) : (ud_string){0},
                                    .resume = argc > 4 ? atoi(argv[4]) : 0,
                                    .resolve = resolve,
                                    .resolve_user = argc > 5 ? argv[5] : NULL};
    ud_execution *execution = NULL;
    ud_status status =
        ud_execution_create_json((ud_string){document, (uint32_t)length}, &options, &execution);
    free(document);
    if (!status)
        status = ud_execution_execute(execution);
    if (status != atoi(argv[2])) {
        fprintf(stderr, "expected=%s actual=%s\n", ud_status_name(atoi(argv[2])),
                ud_status_name(status));
        if (execution)
            (void)ud_execution_destroy(execution);
        return 1;
    }
    if (execution) {
        ud_execution_metrics metrics = {.struct_size = sizeof(metrics),
                                        .abi_version = UD_ABI_VERSION};
        OK(ud_execution_get_metrics(execution, &metrics));
        printf("{\"state\":%d,\"result\":%d,\"rows\":%llu,\"errors\":%llu}\n", metrics.state,
               metrics.result, (unsigned long long)metrics.output_rows,
               (unsigned long long)metrics.error_rows);
        OK(ud_execution_destroy(execution));
    }
    return 0;
}
