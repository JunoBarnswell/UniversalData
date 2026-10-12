#include "ud/package.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)tenant;
    if (action == UD_ACCESS_EXECUTE || action == UD_ACCESS_NETWORK)
        return UD_AUTH; /* File-root policy never opts into programs/network. */
    const char *root = user;
    size_t length = root ? strlen(root) : 0;
    return length && resource.length > length && !memcmp(resource.data, root, length) &&
                   (resource.data[length] == '/' || resource.data[length] == '\\')
               ? UD_OK
               : UD_AUTH;
}
int main(int argc, char **argv) {
    if (argc < 3 || argc > 7 || strcmp(argv[1], "execute")) {
        fprintf(stderr, "usage: ud_cli execute PACKAGE_JSON [PARAMETERS_JSON] [TENANT] "
                        "[EXECUTION_ID] [AUTHORIZED_ABSOLUTE_ROOT]\n");
        return 2;
    }
    FILE *file = fopen(argv[2], "rb");
    if (!file)
        return 2;
    if (fseek(file, 0, SEEK_END)) {
        fclose(file);
        return 2;
    }
    long n = ftell(file);
    if (n < 0 || n > 64 * 1024 * 1024 || fseek(file, 0, SEEK_SET)) {
        fclose(file);
        return 2;
    }
    char *text = malloc((size_t)n + 1);
    if (!text) {
        fclose(file);
        return 2;
    }
    if (fread(text, 1, (size_t)n, file) != (size_t)n) {
        free(text);
        fclose(file);
        return 2;
    }
    fclose(file);
    ud_execution_options o = {0};
    o.struct_size = sizeof(o);
    o.abi_version = UD_ABI_VERSION;
    o.context = (ud_context_options){
        .struct_size = sizeof(o.context), .abi_version = UD_ABI_VERSION, .memory_limit = 268435456};
    o.edge_capacity = 2;
    o.max_batch_rows = 4096;
    o.max_depth = 32;
    const char *parameters = argc > 3 ? argv[3] : "{}", *tenant = argc > 4 ? argv[4] : "cli",
               *id = argc > 5 ? argv[5] : "cli";
    o.parameters = (ud_string){parameters, (uint32_t)strlen(parameters)};
    o.tenant = (ud_string){tenant, (uint32_t)strlen(tenant)};
    o.execution_id = (ud_string){id, (uint32_t)strlen(id)};
    o.authorize = authorize;
    o.authorize_user = argc > 6 ? argv[6] : NULL;
    ud_execution *e = NULL;
    ud_status s = ud_execution_create_json((ud_string){text, (uint32_t)n}, &o, &e);
    free(text);
    if (!s)
        s = ud_execution_execute(e);
    if (e) {
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        ud_execution_get_metrics(e, &m);
        printf("{\"state\":%d,\"result\":%d,\"inputRows\":%llu,\"outputRows\":%llu,"
               "\"completedTasks\":%llu,\"spillBytes\":%llu}\n",
               m.state, m.result, (unsigned long long)m.input_rows,
               (unsigned long long)m.output_rows, (unsigned long long)m.completed_tasks,(unsigned long long)m.spill_bytes);
    }
    if (s)
        fprintf(stderr, "%s\n", ud_status_name(s));
    ud_status clean = ud_execution_destroy(e);
    return s || clean ? 1 : 0;
}
