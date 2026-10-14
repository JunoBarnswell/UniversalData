#include "../engines/package/file_generation.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#define CHECK(x)                                                                                   \
    do {                                                                                           \
        if (!(x)) {                                                                                \
            fprintf(stderr, "line %d: %s\n", __LINE__, #x);                                        \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)
#define OK(x) CHECK((x) == UD_OK)
typedef struct fixture {
    int cut;
    ud_file_generation *generation;
} fixture;
static ud_string str(const char *text) {
    return (ud_string){text, (uint32_t)strlen(text)};
}
static ud_status grant(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)user;
    (void)tenant;
    (void)resource;
    (void)action;
    return UD_OK;
}
static void observe(void *user, const ud_execution_event *event) {
    fixture *state = user;
    if (state->cut != 1 || !state->generation || event->kind != UD_EXEC_EVENT_AFTER_CHECKPOINT)
        return;
    char *record = NULL;
    uint32_t length = 0;
    ud_status status =
        ud_checkpoint_file_read((ud_activation *)event->activation, UD_FILE_PREPARED,
                                (ud_string){state->generation->identity, 64}, &record, &length);
    ud_free(record);
    if (!status)
        _Exit(86);
}
static ud_status invoke(void *user, const ud_component_context *context, ud_string configuration) {
    (void)configuration;
    fixture *state = user;
    ud_connection_lease *lease = NULL;
    ud_file_generation *generation = NULL;
    UD_TRY(ud_activation_acquire_connection(context->activation, str("target"), UD_CONNECT_APPEND,
                                            &lease));
    ud_string resource;
    ud_status status = ud_connection_lease_path(lease, &resource);
    char *path = NULL;
    if (!status) {
        path = ud_strdup_n(context->memory, resource.data, resource.length);
        if (!path)
            status = UD_RESOURCE;
    }
    if (!status)
        status = ud_file_generation_open(context->activation, lease, str("generation"), path,
                                         UD_FILE_OVERWRITE, &generation);
    state->generation = generation;
    if (!status && !generation->prepared) {
        FILE *file = ud_fopen(generation->temporary, "wb");
        if (!file)
            status = UD_IO;
        else {
            if (fwrite("new\n", 1, 4, file) != 4)
                status = UD_IO;
            if (!status)
                status = ud_flush(context->memory, file);
            if (fclose(file) && !status)
                status = UD_IO;
        }
    }
    if (!status)
        status = ud_file_generation_publish(generation);
    if (!status && state->cut == 2)
        _Exit(87);
    if (!status)
        status = ud_checkpoint_file_done(context->activation, path);
    state->generation = NULL;
    ud_file_generation_destroy(generation);
    ud_free(path);
    ud_status released = ud_connection_lease_release(lease);
    return status ? status : released;
}
int main(int argc, char **argv) {
    CHECK(argc == 5);
    FILE *file = fopen(argv[1], "rb");
    CHECK(file);
    CHECK(!fseek(file, 0, SEEK_END));
    long size = ftell(file);
    CHECK(size > 0 && size < 65536);
    rewind(file);
    char *text = malloc((size_t)size + 1);
    CHECK(text);
    CHECK(fread(text, 1, (size_t)size, file) == (size_t)size);
    fclose(file);
    text[size] = 0;
    fixture state = {.cut = atoi(argv[4])};
    ud_context *context = NULL;
    OK(ud_context_create(NULL, &context));
    ud_registry *registry = NULL;
    OK(ud_registry_create(context, &registry));
    ud_component_descriptor descriptor = {.struct_size = sizeof(descriptor),
                                          .abi_version = UD_ABI_VERSION,
                                          .id = {"test.generation", 15},
                                          .name = {"generation", 10},
                                          .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
                                          .configuration_schema = {"{\"type\":\"object\"}", 17},
                                          .category = UD_COMPONENT_TASK,
                                          .synchronous_input = -1,
                                          .error_output = -1,
                                          .capabilities =
                                              UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_THREAD_SAFE,
                                          .user = &state,
                                          .invoke = invoke};
    OK(ud_registry_register(registry, &descriptor));
    ud_execution_options options = {.struct_size = sizeof(options),
                                    .abi_version = UD_ABI_VERSION,
                                    .context = {.struct_size = sizeof(ud_context_options),
                                                .abi_version = UD_ABI_VERSION},
                                    .edge_capacity = 2,
                                    .max_batch_rows = 7,
                                    .max_depth = 32,
                                    .parameters = {"{}", 2},
                                    .tenant = {"test", 4},
                                    .execution_id = {"file-generation", 15},
                                    .state_directory = str(argv[2]),
                                    .resume = atoi(argv[3]),
                                    .authorize = grant,
                                    .registry = registry,
                                    .observe = observe,
                                    .observe_user = &state};
    ud_execution *execution = NULL;
    OK(ud_execution_create_json((ud_string){text, (uint32_t)size}, &options, &execution));
    free(text);
    OK(ud_execution_execute(execution));
    OK(ud_execution_destroy(execution));
    OK(ud_registry_destroy(registry));
    OK(ud_context_destroy(context));
    return 0;
}
