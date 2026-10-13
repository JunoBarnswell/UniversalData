#include "check.h"
#include "ud/catalog.h"
#include "ud/project.h"
#include <string.h>
static ud_string str(const char *v) {
    return (ud_string){v, (uint32_t)strlen(v)};
}
static char *load(const char *root, const char *name) {
    char path[2048];
    snprintf(path, sizeof(path), "%s/%s", root, name);
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    if (fseek(f, 0, SEEK_END)) {
        fclose(f);
        return NULL;
    }
    long n = ftell(f);
    if (n < 0 || fseek(f, 0, SEEK_SET)) {
        fclose(f);
        return NULL;
    }
    char *text = calloc((size_t)n + 1, 1);
    if (!text || fread(text, 1, (size_t)n, f) != (size_t)n) {
        free(text);
        text = NULL;
    }
    fclose(f);
    return text;
}
static ud_status allow(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)tenant;
    (void)action;
    const char *root = user;
    size_t n = strlen(root);
    int granted = resource.length > n && !memcmp(resource.data, root, n) && resource.data[n] == '/';
    return granted ? UD_OK : UD_AUTH;
}
static int execute(ud_package *p, const char *params, const char *root, int outer) {
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .context = {.struct_size = sizeof(o.context),
                                          .abi_version = UD_ABI_VERSION,
                                          .memory_limit = 16777216},
                              .edge_capacity = 1,
                              .max_batch_rows = 3,
                              .max_depth = 32,
                              .parameters = str(params),
                              .tenant = str("tenant"),
                              .execution_id = str("project-run"),
                              .authorize = allow,
                              .authorize_user = (void *)root};
    static int run_number = 0;
    char state[2048];
    snprintf(state, sizeof(state), "%s/state-%d", root, ++run_number);
    o.state_directory = str(state);
    ud_execution *e = NULL;
    for (int replay = 0; replay < 2; replay++) {
        o.resume = replay;
        OK(ud_execution_create(p, &o, &e));
        OK(ud_execution_execute(e));
        const char *names[] = {"outerResult", "scopeResult", "containerResult",
                               "maskResult",  "countResult", "sameResult"};
        const int values[] = {outer, 444, 333, 222, 2, 11};
        for (int i = 0; i < 6; i++) {
            ud_value v;
            OK(ud_execution_variable(e, str(names[i]), &v));
            CHECK(v.type == UD_INT64 && v.integer == values[i]);
        }
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        OK(ud_execution_get_metrics(e, &m));
        CHECK(m.input_rows == 5 && m.output_rows == 5);
        OK(ud_execution_destroy(e));
    }
    return 0;
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    char *pj = load(argv[1], "project.json"), *pkg = load(argv[1], "package.json"),
         *env = load(argv[1], "environment.json");
    CHECK(pj && pkg && env);
    ud_project *project = NULL;
    ud_package *package = NULL, *bound = NULL;
    char *params = NULL;
    uint32_t n;
    OK(ud_project_create(ctx, str(pj), &project));
    OK(ud_package_create(ctx, str(pkg), &package));
    ud_project_binding binding = {sizeof(binding), UD_ABI_VERSION,        1,
                                  str(env),        str("{\"outer\":20}"), str("{}")};
    char direct_override[2048];
    snprintf(direct_override, sizeof(direct_override), "{\"output\":\"%s/direct.ndjson\"}",
             argv[1]);
    binding.package_parameters = str(direct_override);
    OK(ud_project_bind(project, package, &binding, &bound, &params, &n));
    CHECK(!execute(bound, params, argv[1], 20));
    ud_free(params);
    params = NULL;
    ud_package_destroy(bound);
    bound = NULL;
    binding.package_revision = 2;
    CHECK(ud_project_bind(project, package, &binding, &bound, &params, &n) == UD_INCOMPATIBLE &&
          !bound && !params);
    binding.package_revision = 1;
    binding.project_parameters = str("{\"typo\":1}");
    CHECK(ud_project_bind(project, package, &binding, &bound, &params, &n) == UD_SCHEMA);
    binding.project_parameters = str("{\"outer\":\"wrong-type\"}");
    CHECK(ud_project_bind(project, package, &binding, &bound, &params, &n) == UD_PARSE);
    binding.project_parameters = str("{}");
    binding.package_parameters = str("{\"shadow\":false}");
    CHECK(ud_project_bind(project, package, &binding, &bound, &params, &n) == UD_SCHEMA);
    binding.package_parameters = str("{}");
    binding.environment = str("{\"version\":9,\"id\":\"env\",\"projectId\":\"wrong\"}");
    CHECK(ud_project_bind(project, package, &binding, &bound, &params, &n) == UD_SCHEMA);
    char *invalid = load(argv[1], "readonly.json");
    CHECK(invalid);
    ud_execution_options options = {
        .struct_size = sizeof(options),
        .abi_version = UD_ABI_VERSION,
        .context = {.struct_size = sizeof(options.context), .abi_version = UD_ABI_VERSION},
        .edge_capacity = 1,
        .max_batch_rows = 3,
        .max_depth = 32};
    ud_execution *bad = NULL;
    CHECK(ud_execution_create_json(str(invalid), &options, &bad) == UD_AUTH && !bad);
    free(invalid);
    char directory[2048];
    snprintf(directory, sizeof(directory), "%s/catalog", argv[1]);
    ud_catalog_options co = {
        sizeof(co), UD_ABI_VERSION, str(directory), str("tenant"), allow, argv[1], 1};
    ud_catalog *catalog = NULL;
    OK(ud_catalog_open(ctx, &co, &catalog));
    const char *files[] = {"package.json",    "package-v2.json",   "project.json",
                           "project-v2.json", "environment.json",  "environment-v2.json",
                           "deployment.json", "deployment-v2.json"};
    const int kinds[] = {UD_CATALOG_PACKAGE,    UD_CATALOG_PACKAGE,     UD_CATALOG_PROJECT,
                         UD_CATALOG_PROJECT,    UD_CATALOG_ENVIRONMENT, UD_CATALOG_ENVIRONMENT,
                         UD_CATALOG_DEPLOYMENT, UD_CATALOG_DEPLOYMENT};
    const char *ids[] = {"p",           "p",           "project",    "project",
                         "environment", "environment", "deployment", "deployment"};
    for (int i = 0; i < 8; i++) {
        char *doc = load(argv[1], files[i]);
        CHECK(doc);
        ud_catalog_key key = {sizeof(key), UD_ABI_VERSION, kinds[i], str(ids[i]),
                              (uint64_t)(i % 2 + 1)};
        OK(ud_catalog_put(catalog, &key, str(doc)));
        free(doc);
    }
    ud_catalog_key deploy = {sizeof(deploy), UD_ABI_VERSION, UD_CATALOG_DEPLOYMENT, str("broken"),
                             1};
    CHECK(ud_catalog_put(catalog, &deploy,
                         str("{\"version\":9,\"id\":\"broken\",\"project\":{\"id\":\"absent\","
                             "\"revision\":1}}")) == UD_SCHEMA);
    ud_catalog_close(catalog);
    catalog = NULL;
    co.create_directory = 0;
    OK(ud_catalog_open(ctx, &co, &catalog));
    ud_catalog_binding cb = {sizeof(cb), UD_ABI_VERSION, str("deployment"), 1,
                             str("p"),   str("{}"),      str("{}")};
    OK(ud_catalog_bind_package(catalog, &cb, &bound, &params, &n));
    CHECK(!execute(bound, params, argv[1], 10));
    ud_package_destroy(bound);
    ud_free(params);
    bound = NULL;
    params = NULL;
    cb.deployment_revision = 2;
    OK(ud_catalog_bind_package(catalog, &cb, &bound, &params, &n));
    CHECK(!execute(bound, params, argv[1], 110));
    ud_package_destroy(bound);
    ud_free(params);
    bound = NULL;
    params = NULL;
    cb.deployment_revision = 1;
    char override[2048];
    snprintf(override, sizeof(override), "{\"output\":\"%s/rollback.ndjson\"}", argv[1]);
    cb.package_parameters = str(override);
    OK(ud_catalog_bind_package(catalog, &cb, &bound, &params, &n));
    CHECK(!execute(bound, params, argv[1], 10));
    ud_package_destroy(bound);
    ud_free(params);
    bound = NULL;
    params = NULL;
    cb.deployment_revision = 99;
    CHECK(ud_catalog_bind_package(catalog, &cb, &bound, &params, &n) == UD_END && !bound &&
          !params);
    cb.deployment_revision = 1;
    cb.package_id = str("absent");
    CHECK(ud_catalog_bind_package(catalog, &cb, &bound, &params, &n) == UD_SCHEMA);
    ud_catalog_close(catalog);
    ud_project_destroy(project);
    ud_package_destroy(package);
    free(pj);
    free(pkg);
    free(env);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
