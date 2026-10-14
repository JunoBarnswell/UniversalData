#include "check.h"
#include "ud/package.h"
#include <string.h>
typedef struct observed_identity {
    char id[256];
    uint32_t length;
    int mismatch;
} observed_identity;
static void observe(void *user, const ud_execution_event *event) {
    observed_identity *seen = user;
    if (!seen->length && event->execution_id.length <= 255) {
        seen->length = event->execution_id.length;
        memcpy(seen->id, event->execution_id.data, seen->length);
    } else if (seen->length != event->execution_id.length ||
               memcmp(seen->id, event->execution_id.data, seen->length))
        seen->mismatch = 1;
}
int main(void) {
    const char *document =
        "{\"version\":" UD_PROTOCOL_VERSION_TEXT
        ",\"id\":\"identity\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}";
    ud_string json = {document, (uint32_t)strlen(document)};
    observed_identity seen = {0};
    ud_execution_options options = {.struct_size = sizeof(options),
                                    .abi_version = UD_ABI_VERSION,
                                    .context = {.struct_size = sizeof(ud_context_options),
                                                .abi_version = UD_ABI_VERSION,
                                                .memory_limit = 16 * 1024 * 1024},
                                    .edge_capacity = 1,
                                    .max_batch_rows = 1,
                                    .max_depth = 2,
                                    .observe = observe,
                                    .observe_user = &seen};
    ud_execution *first = NULL, *second = NULL;
    OK(ud_execution_create_json(json, &options, &first));
    ud_string id = {0};
    OK(ud_execution_id(first, &id));
    CHECK(id.length == 36 && id.data[14] == '4' &&
          (id.data[19] == '8' || id.data[19] == '9' || id.data[19] == 'a' || id.data[19] == 'b'));
    CHECK(seen.length == id.length && !memcmp(seen.id, id.data, id.length) && !seen.mismatch);
    char original[37];
    memcpy(original, id.data, 36);
    original[36] = 0;
    options.observe = NULL;
    OK(ud_execution_create_json(json, &options, &second));
    OK(ud_execution_id(second, &id));
    CHECK(memcmp(original, id.data, 36));
    OK(ud_execution_destroy(second));
    OK(ud_execution_execute(first));
    OK(ud_execution_id(first, &id));
    CHECK(!memcmp(original, id.data, 36) && !seen.mismatch);
    OK(ud_execution_destroy(first));
    options.resume = 1;
    options.state_directory = (ud_string){"/not-opened", 11};
    CHECK(ud_execution_create_json(json, &options, &first) == UD_INVALID && !first);
    options.resume = 0;
    options.state_directory = (ud_string){0};
    options.execution_id = (ud_string){"host-id", 7};
    OK(ud_execution_create_json(json, &options, &first));
    OK(ud_execution_id(first, &id));
    CHECK(id.length == 7 && !memcmp(id.data, "host-id", 7));
    OK(ud_execution_destroy(first));
    options.execution_id = (ud_string){"bad\nidentity", 12};
    CHECK(ud_execution_create_json(json, &options, &first) == UD_INVALID && !first);
    options.execution_id = (ud_string){0};
    options.abi_version = UD_ABI_VERSION - 1;
    CHECK(ud_execution_create_json(json, &options, &first) == UD_INCOMPATIBLE && !first);
    return 0;
}
