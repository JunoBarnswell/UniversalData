/* Independent installed C SDK consumer, not a second product host. */
#include <ud/package.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static ud_string text(const char *value) {
    return (ud_string){value, (uint32_t)strlen(value)};
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t access) {
    (void)tenant;
    (void)access;
    const char *root = user;
    size_t length = strlen(root);
    return resource.length >= length && !memcmp(resource.data, root, length) &&
                   (resource.length == length || resource.data[length] == '/' ||
                    resource.data[length] == '\\')
               ? UD_OK
               : UD_AUTH;
}
int main(int argc, char **argv) {
    if (argc != 6)
        return 2;
    FILE *input = fopen(argv[1], "rb");
    if (!input || fseek(input, 0, SEEK_END))
        return 1;
    long length = ftell(input);
    if (length < 1 || length > 16777216 || fseek(input, 0, SEEK_SET)) {
        fclose(input);
        return 1;
    }
    char *json = malloc((size_t)length);
    if (!json) {
        fclose(input);
        return 1;
    }
    size_t read = fread(json, 1, (size_t)length, input);
    fclose(input);
    if (read != (size_t)length) {
        free(json);
        return 1;
    }
    ud_execution_options options = {.struct_size = sizeof(options),
                                    .abi_version = UD_ABI_VERSION,
                                    .context = {.struct_size = sizeof(ud_context_options),
                                                .abi_version = UD_ABI_VERSION,
                                                .memory_limit = 268435456},
                                    .edge_capacity = 2,
                                    .max_batch_rows = 4096,
                                    .max_depth = 32,
                                    .max_concurrency = 4,
                                    .parameters = text(argv[2]),
                                    .tenant = text(argv[3]),
                                    .execution_id = text(argv[4]),
                                    .authorize = authorize,
                                    .authorize_user = argv[5]};
    ud_execution *execution = NULL;
    ud_status result =
        ud_execution_create_json((ud_string){json, (uint32_t)length}, &options, &execution);
    free(json);
    if (!result)
        result = ud_execution_execute(execution);
    ud_execution_metrics metrics = {.struct_size = sizeof(metrics), .abi_version = UD_ABI_VERSION};
    if (execution) {
        ud_status measured = ud_execution_get_metrics(execution, &metrics);
        if (measured && !result)
            result = measured;
    }
    printf("{\"state\":%d,\"result\":%d,\"inputRows\":%llu,\"outputRows\":%llu,"
           "\"executedTasks\":%llu,\"durableTasks\":%llu,\"recoveryTasks\":%llu,"
           "\"resumedTasks\":%llu,\"pipelineTrees\":%llu}\n",
           metrics.state, result, (unsigned long long)metrics.input_rows,
           (unsigned long long)metrics.output_rows, (unsigned long long)metrics.executed_tasks,
           (unsigned long long)metrics.durable_tasks, (unsigned long long)metrics.recovery_tasks,
           (unsigned long long)metrics.resumed_tasks, (unsigned long long)metrics.pipeline_trees);
    ud_status destroyed = ud_execution_destroy(execution);
    return result || destroyed ? 1 : 0;
}
