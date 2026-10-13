#include "check.h"
#include "ud/catalog.h"
#include <string.h>
static ud_string str(const char *v) {
    return (ud_string){v, (uint32_t)strlen(v)};
}
static ud_status authorize(void *u, ud_string tenant, ud_string resource, int32_t action) {
    (void)tenant;
    (void)resource;
    (void)action;
    return *(int *)u ? UD_OK : UD_AUTH;
}
int main(int argc, char **argv) {
    CHECK(argc == 3);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    int permitted = 1;
    ud_catalog_options o = {
        sizeof(o), UD_ABI_VERSION, str(argv[1]), str("tenant-a"), authorize, &permitted, 1};
    ud_catalog *c = NULL;
    ud_catalog_key key = {sizeof(key), UD_ABI_VERSION, UD_CATALOG_PACKAGE, str("test"), 1};
    char *json = NULL;
    uint32_t length = 0;
    if (!strcmp(argv[2], "create")) {
        o.authorize = NULL;
        CHECK(ud_catalog_open(ctx, &o, &c) == UD_AUTH && !c);
        o.authorize = authorize;
        o.abi_version = 0;
        CHECK(ud_catalog_open(ctx, &o, &c) == UD_INCOMPATIBLE && !c);
        o.abi_version = UD_ABI_VERSION;
        OK(ud_catalog_open(ctx, &o, &c));
        CHECK(ud_catalog_get(c, &key, &json, &length) == UD_END && !json);
        const char *package = "{\"version\":9,\"id\":\"test\",\"controlFlow\":{\"tasks\":[],"
                              "\"constraints\":[]},\"dataFlows\":[]}";
        OK(ud_catalog_put(c, &key, str(package)));
        OK(ud_catalog_put(c, &key, str(package)));
        CHECK(ud_catalog_put(c, &key,
                             str("{\"version\":9,\"id\":\"test\",\"parameters\":{},\"controlFlow\":"
                                 "{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}")) ==
              UD_CORRUPT);
        OK(ud_catalog_get(c, &key, &json, &length));
        CHECK(length == strlen(package) && !strcmp(json, package));
        ud_free(json);
        json = NULL;
        key.revision = 2;
        OK(ud_catalog_put(c, &key, str(package)));
        key.id = str("other");
        CHECK(ud_catalog_put(c, &key, str(package)) == UD_SCHEMA);
        key.id = str("test");
        CHECK(ud_catalog_put(c, &key, str("[]")) == UD_SCHEMA);
        CHECK(ud_catalog_put(c, &key, str("{")) == UD_PARSE);
        OK(ud_catalog_list(c, UD_CATALOG_PACKAGE, &json, &length));
        CHECK(strstr(json, "\"revision\":1") && strstr(json, "\"revision\":2"));
        ud_free(json);
        json = NULL;
        ud_execution_event event = {.struct_size = sizeof(event),
                                    .abi_version = UD_ABI_VERSION,
                                    .package_id = str("test"),
                                    .execution_id = str("execution"),
                                    .task_id = str("task"),
                                    .state = UD_EXEC_SUCCEEDED,
                                    .result = UD_OK,
                                    .timestamp_ms = 42,
                                    .kind = UD_EXEC_EVENT_STATE};
        OK(ud_catalog_record_event(c, &event, 1));
        OK(ud_catalog_record_event(c, &event, 1));
        event.kind = 0;
        CHECK(ud_catalog_record_event(c, &event, 2) == UD_INVALID);
        ud_catalog_key event_key = {sizeof(event_key), UD_ABI_VERSION, UD_CATALOG_EVENT,
                                    str("execution"), 1};
        OK(ud_catalog_get(c, &event_key, &json, &length));
        CHECK(strstr(json, "\"timestampMs\":42") && !strstr(json, "config"));
        ud_free(json);
        json = NULL;
        permitted = 0;
        CHECK(ud_catalog_get(c, &key, &json, &length) == UD_AUTH && !json);
        CHECK(ud_catalog_put(c, &key, str(package)) == UD_AUTH);
        CHECK(ud_catalog_list(c, UD_CATALOG_PACKAGE, &json, &length) == UD_AUTH && !json);
        permitted = 1;
        ud_catalog_close(c);
        c = NULL;
        o.tenant = str("tenant-b");
        OK(ud_catalog_open(ctx, &o, &c));
        key.revision = 1;
        CHECK(ud_catalog_get(c, &key, &json, &length) == UD_END && !json);
        OK(ud_catalog_list(c, UD_CATALOG_PACKAGE, &json, &length));
        CHECK(!strcmp(json, "[]"));
        ud_free(json);
        json = NULL;
        OK(ud_catalog_put(c, &key, str(package)));
    } else {
        o.create_directory = 0;
        OK(ud_catalog_open(ctx, &o, &c));
        if (!strcmp(argv[2], "read")) {
            OK(ud_catalog_get(c, &key, &json, &length));
        } else if (!strcmp(argv[2], "corrupt")) {
            CHECK(ud_catalog_get(c, &key, &json, &length) == UD_CORRUPT && !json);
        } else if (!strcmp(argv[2], "renamed")) {
            CHECK(ud_catalog_list(c, UD_CATALOG_PACKAGE, &json, &length) == UD_CORRUPT && !json);
        } else if (!strcmp(argv[2], "staging")) {
            key.revision = 3;
            const char *package = "{\"version\":9,\"id\":\"test\",\"controlFlow\":{\"tasks\":[],"
                                  "\"constraints\":[]},\"dataFlows\":[]}";
            CHECK(ud_catalog_put(c, &key, str(package)) == UD_BUSY);
        } else {
            CHECK(0);
        }
    }
    ud_free(json);
    ud_catalog_close(c);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
