#include "check.h"
#include "ud/catalog.h"
#include "ud/json.h"
#include <string.h>
typedef struct fixture {
    ud_catalog *catalog;
    ud_execution *execution;
    uint64_t sequence;
    ud_status outcome, error;
} fixture;
static ud_string str(const char *v) {
    return (ud_string){v, (uint32_t)strlen(v)};
}
static ud_status allow(void *u, ud_string t, ud_string r, int32_t a) {
    (void)u;
    (void)t;
    (void)r;
    (void)a;
    return UD_OK;
}
static ud_status effect(void *user, const ud_component_context *c, ud_string config) {
    (void)c;
    (void)config;
    return ((fixture *)user)->outcome;
}
static void observe(void *user, const ud_execution_event *event) {
    fixture *f = user;
    if (!f->execution)
        return;
    ud_status s = ud_catalog_record_event(f->catalog, event, ++f->sequence);
    if (s)
        f->error = s;
    if (event->state == UD_EXEC_RUNNING) {
        s = ud_catalog_record_execution(f->catalog, f->execution, 9);
        if (s != UD_BUSY)
            f->error = UD_CORRUPT;
        if (f->outcome == UD_CANCELLED)
            ud_execution_cancel(f->execution);
    }
}
static int number(const ud_json *doc, const char *key, int expected) {
    const char *text;
    uint32_t n;
    char value[32];
    snprintf(value, sizeof(value), "%d", expected);
    const ud_json *member = ud_json_find(doc, key, (uint32_t)strlen(key));
    return !ud_json_text(member, &text, &n) && strlen(value) == n && !memcmp(text, value, n);
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    fixture f = {0};
    ud_catalog_options co = {sizeof(co), UD_ABI_VERSION, str(argv[1]), str("tenant"), allow, NULL,
                             1};
    OK(ud_catalog_open(ctx, &co, &f.catalog));
    ud_registry *registry = NULL;
    OK(ud_registry_create(ctx, &registry));
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = str("effect"),
                                 .name = str("effect"),
                                 .version = str("1"),
                                 .configuration_schema = str("{}"),
                                 .category = UD_COMPONENT_TASK,
                                 .synchronous_input = -1,
                                 .error_output = -1,
                                 .user = &f,
                                 .invoke = effect};
    OK(ud_registry_register(registry, &d));
    const char *package =
        "{\"version\":3,\"id\":\"history-package\",\"parameters\":{\"secret\":{\"type\":\"utf8\"}},"
        "\"project\":{\"id\":\"project\"},\"deployment\":{\"id\":\"deployment\",\"revision\":1,"
        "\"project\":{\"id\":\"project\",\"revision\":1},\"package\":{\"id\":\"history-package\","
        "\"revision\":2},\"environment\":{\"id\":\"environment\",\"revision\":3}},"
        "\"controlFlow\":{\"tasks\":[{\"id\":\"effect\",\"kind\":\"effect\",\"config\":{}}],"
        "\"constraints\":[]},\"dataFlows\":[]}";
    const int results[] = {UD_OK, UD_IO, UD_CANCELLED, UD_COMMIT_UNKNOWN},
              states[] = {UD_EXEC_SUCCEEDED, UD_EXEC_FAILED, UD_EXEC_CANCELLED,
                          UD_EXEC_RECOVERY_REQUIRED};
    for (int i = 0; i < 4; i++) {
        char id[64];
        snprintf(id, sizeof(id), "history-%d", i);
        f.outcome = results[i];
        f.error = 0;
        f.sequence = 0;
        ud_execution_options o = {
            .struct_size = sizeof(o),
            .abi_version = UD_ABI_VERSION,
            .context = {.struct_size = sizeof(o.context), .abi_version = UD_ABI_VERSION},
            .edge_capacity = 1,
            .max_batch_rows = 3,
            .max_depth = 32,
            .tenant = str("tenant"),
            .execution_id = str(id),
            .parameters = str("{\"secret\":\"FixtureSensitive\"}"),
            .registry = registry,
            .observe = observe,
            .observe_user = &f};
        ud_execution *e = NULL;
        OK(ud_execution_create_json(str(package), &o, &e));
        f.execution = e;
        OK(ud_catalog_record_execution(f.catalog, e, 1));
        CHECK(ud_execution_execute(e) == results[i]);
        CHECK(!f.error);
        OK(ud_catalog_record_execution(f.catalog, e, 2));
        OK(ud_catalog_record_execution(f.catalog, e, 2));
        ud_catalog_key key = {sizeof(key), UD_ABI_VERSION, UD_CATALOG_EXECUTION, str(id), 2};
        char *text = NULL;
        uint32_t n;
        OK(ud_catalog_get(f.catalog, &key, &text, &n));
        CHECK(!strstr(text, "FixtureSensitive") && !strstr(text, "\"secret\""));
        ud_json *doc = NULL;
        OK(ud_json_parse(ctx, text, n, &doc, NULL));
        CHECK(number(doc, "state", states[i]) && number(doc, "result", results[i]) &&
              number(doc, "completedTasks", i == 0 ? 1 : 0));
        CHECK(ud_json_find(doc, "parametersHash", 14));
        ud_json_destroy(doc);
        ud_free(text);
        ud_catalog *other = NULL;
        co.tenant = str("other");
        OK(ud_catalog_open(ctx, &co, &other));
        CHECK(ud_catalog_record_execution(other, e, 1) == UD_AUTH);
        ud_catalog_close(other);
        co.tenant = str("tenant");
        OK(ud_execution_destroy(e));
        f.execution = NULL;
    }
    ud_catalog_close(f.catalog);
    f.catalog = NULL;
    co.create_directory = 0;
    OK(ud_catalog_open(ctx, &co, &f.catalog));
    char *list = NULL;
    uint32_t n;
    OK(ud_catalog_list(f.catalog, UD_CATALOG_EXECUTION, &list, &n));
    ud_json *doc = NULL;
    OK(ud_json_parse(ctx, list, n, &doc, NULL));
    CHECK(ud_json_size(doc) == 8);
    ud_json_destroy(doc);
    ud_free(list);
    ud_catalog_close(f.catalog);
    OK(ud_registry_destroy(registry));
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
