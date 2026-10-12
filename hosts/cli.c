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
    if (argc < 3 || argc > 8 || (strcmp(argv[1], "execute") && strcmp(argv[1], "inspect"))) {
        fprintf(stderr, "usage: ud_cli execute|inspect PACKAGE_JSON [PARAMETERS_JSON] [TENANT] "
                        "[EXECUTION_ID] [AUTHORIZED_ABSOLUTE_ROOT] [MAX_WORKERS]\n");
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
    if (argc > 7) {
        char *end = NULL;
        unsigned long workers = strtoul(argv[7], &end, 10);
        if (!workers || workers > 64 || !end || *end) {
            free(text);
            return 2;
        }
        o.max_concurrency = (uint32_t)workers;
    }
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
    int inspect = !strcmp(argv[1], "inspect");
    if (!s && inspect) {
        char *plan = NULL;
        uint32_t size = 0;
        s = ud_execution_explain(e, &plan, &size);
        if (!s)
            printf("%.*s\n", (int)size, plan);
        ud_free(plan);
    } else if (!s)
        s = ud_execution_execute(e);
    if (e && !inspect) {
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        ud_execution_get_metrics(e, &m);
        printf("{\"state\":%d,\"result\":%d,\"inputRows\":%llu,\"outputRows\":%llu,"
               "\"executedTasks\":%llu,\"durableTasks\":%llu,\"recoveryTasks\":%llu,"
               "\"resumedTasks\":%llu,\"spillBytes\":%llu,\"pipelineTrees\":%llu,"
               "\"treeHighWater\":%llu,\"workerHighWater\":%llu,\"rejectedRows\":%llu,"
               "\"committedRows\":%llu,\"acknowledgedRows\":%llu,\"errorRows\":%llu,"
               "\"committedErrorRows\":%llu,\"acknowledgedErrorRows\":%llu}\n",
               m.state, m.result, (unsigned long long)m.input_rows,
               (unsigned long long)m.output_rows, (unsigned long long)m.executed_tasks,
               (unsigned long long)m.durable_tasks, (unsigned long long)m.recovery_tasks,
               (unsigned long long)m.resumed_tasks, (unsigned long long)m.spill_bytes,
               (unsigned long long)m.pipeline_trees, (unsigned long long)m.tree_high_water,
               (unsigned long long)m.worker_high_water, (unsigned long long)m.rejected_rows,
               (unsigned long long)m.committed_rows, (unsigned long long)m.acknowledged_rows,
               (unsigned long long)m.error_rows, (unsigned long long)m.committed_error_rows,
               (unsigned long long)m.acknowledged_error_rows);
    }
    if (s)
        fprintf(stderr, "%s\n", ud_status_name(s));
    ud_status clean = ud_execution_destroy(e);
    return s || clean ? 1 : 0;
}
