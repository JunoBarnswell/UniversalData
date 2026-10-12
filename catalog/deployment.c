#include "internal.h"
#include "ud/project.h"
static ud_status get_document(ud_catalog *c, const ud_catalog_key *key, ud_json **out) {
    char *text = NULL;
    uint32_t n = 0;
    *out = NULL;
    ud_status s = ud_catalog_get(c, key, &text, &n);
    if (!s)
        s = ud_json_parse(c->ctx, text, n, out, NULL);
    ud_free(text);
    return s;
}
static ud_status key_from_ref(const ud_json *ref, int32_t kind, ud_catalog_key *key) {
    static const char *keys[] = {"id", "revision"};
    UD_TRY(ud_json_keys(ref, keys, 2));
    *key =
        (ud_catalog_key){.struct_size = sizeof(*key), .abi_version = UD_ABI_VERSION, .kind = kind};
    UD_TRY(ud_json_string(ud_member(ref, "id"), &key->id));
    UD_TRY(ud_json_uint(ud_member(ref, "revision"), &key->revision));
    return key->revision ? UD_OK : UD_SCHEMA;
}
static ud_status document_header(const ud_json *doc, const ud_catalog_key *key) {
    uint64_t v;
    ud_string id = {0};
    UD_TRY(ud_json_uint(ud_member(doc, "version"), &v));
    if (v != UD_CONFIG_VERSION)
        return UD_INCOMPATIBLE;
    UD_TRY(ud_json_string(ud_member(doc, "id"), &id));
    return ud_same(id, key->id) ? UD_OK : UD_SCHEMA;
}
static ud_status environment_shape(const ud_json *doc) {
    static const char *keys[] = {"version", "id", "projectId", "projectParameters",
                                 "packageParameters"};
    UD_TRY(ud_json_keys(doc, keys, 5));
    ud_string id = {0};
    UD_TRY(ud_json_string(ud_member(doc, "projectId"), &id));
    const char *objects[] = {"projectParameters", "packageParameters"};
    for (int i = 0; i < 2; i++) {
        const ud_json *v = ud_member(doc, objects[i]);
        if (v && ud_json_kind(v) != UD_JSON_OBJECT)
            return UD_SCHEMA;
    }
    const ud_json *values = ud_member(doc, "packageParameters");
    for (uint32_t i = 0; i < ud_json_size(values); i++)
        if (ud_json_kind(ud_json_at(values, i)) != UD_JSON_OBJECT)
            return UD_SCHEMA;
    return UD_OK;
}
static ud_status deployment_documents(ud_catalog *c, const ud_json *deployment, ud_json **project,
                                      ud_json **environment) {
    *project = NULL;
    *environment = NULL;
    static const char *keys[] = {"version", "id", "project", "environment"};
    UD_TRY(ud_json_keys(deployment, keys, 4));
    ud_catalog_key key;
    UD_TRY(key_from_ref(ud_member(deployment, "project"), UD_CATALOG_PROJECT, &key));
    ud_status s = get_document(c, &key, project);
    if (!s && ud_member(deployment, "environment")) {
        s = key_from_ref(ud_member(deployment, "environment"), UD_CATALOG_ENVIRONMENT, &key);
        if (!s)
            s = get_document(c, &key, environment);
    }
    return s == UD_END ? UD_SCHEMA : s;
}
static ud_status validate_references(ud_catalog *c, const ud_json *project, const ud_json *env) {
    const ud_json *refs = ud_member(project, "packages");
    ud_string project_id = {0}, environment_project = {0};
    UD_TRY(ud_json_string(ud_member(project, "id"), &project_id));
    if (env) {
        UD_TRY(environment_shape(env));
        UD_TRY(ud_json_string(ud_member(env, "projectId"), &environment_project));
        if (!ud_same(project_id, environment_project))
            return UD_SCHEMA;
        UD_TRY(ud_parameters_check(c->ctx, ud_member(project, "parameters"),
                                   ud_member(env, "projectParameters"), 0));
    }
    const ud_json *parameters = ud_member(env, "packageParameters");
    for (uint32_t i = 0; i < ud_json_size(parameters); i++) {
        const char *name;
        uint32_t n = 0;
        UD_TRY(ud_json_key(parameters, i, &name, &n));
        int found = 0;
        for (uint32_t j = 0; j < ud_json_size(refs); j++) {
            ud_string id = {0};
            UD_TRY(ud_json_string(ud_member(ud_json_at(refs, j), "id"), &id));
            if (ud_same(id, (ud_string){name, n}))
                found = 1;
        }
        if (!found)
            return UD_SCHEMA;
    }
    for (uint32_t i = 0; i < ud_json_size(refs); i++) {
        ud_catalog_key key;
        UD_TRY(key_from_ref(ud_json_at(refs, i), UD_CATALOG_PACKAGE, &key));
        ud_json *package = NULL;
        ud_status s = get_document(c, &key, &package);
        if (!s)
            s = ud_parameters_check(c->ctx, ud_member(package, "parameters"),
                                    ud_json_find(parameters, key.id.data, key.id.length), 0);
        if (!s && ud_member(package, "project"))
            s = UD_SCHEMA; /* Never bind twice. */
        ud_json_destroy(package);
        if (s)
            return s == UD_END ? UD_SCHEMA : s;
    }
    return UD_OK;
}
ud_status ud_catalog_definition_check(ud_catalog *c, const ud_catalog_key *key,
                                      const ud_json *doc) {
    if (key->kind != UD_CATALOG_PROJECT && key->kind != UD_CATALOG_ENVIRONMENT &&
        key->kind != UD_CATALOG_DEPLOYMENT)
        return UD_OK;
    UD_TRY(document_header(doc, key));
    if (key->kind == UD_CATALOG_ENVIRONMENT)
        return environment_shape(doc);
    if (key->kind == UD_CATALOG_PROJECT) {
        char *text = NULL;
        uint32_t n = 0;
        ud_project *project = NULL;
        ud_status s = ud_json_serialize(doc, &text, &n);
        if (!s)
            s = ud_project_create(c->ctx, (ud_string){text, n}, &project);
        ud_project_destroy(project);
        ud_free(text);
        return s;
    }
    ud_json *project = NULL, *environment = NULL;
    ud_status s = deployment_documents(c, doc, &project, &environment);
    if (!s)
        s = validate_references(c, project, environment);
    ud_json_destroy(project);
    ud_json_destroy(environment);
    return s;
}
static ud_status provenance(ud_catalog *c, const ud_catalog_binding *o, const ud_json *deployment,
                            uint64_t package_revision, ud_package **package) {
    char *text = NULL;
    uint32_t n = 0;
    ud_status s = ud_package_serialize(*package, &text, &n);
    ud_bytes b = {.ctx = c->ctx};
    if (!s)
        s = ud_bytes_put(&b, text, n - 1);
    if (!s)
        s = ud_bytes_put(&b, ",\"deployment\":{\"id\":", 20);
    if (!s)
        s = ud_json_quote(&b, o->deployment_id.data, o->deployment_id.length);
    char value[64];
    int count =
        snprintf(value, sizeof(value),
                 ",\"revision\":%llu,\"project\":", (unsigned long long)o->deployment_revision);
    if (!s)
        s = ud_bytes_put(&b, value, (size_t)count);
    const char *names[] = {"project", "environment"};
    for (int i = 0; !s && i < 2; i++) {
        const ud_json *ref = ud_member(deployment, names[i]);
        if (!ref)
            continue;
        if (i)
            s = ud_bytes_put(&b, ",\"environment\":", 15);
        char *ref_text = NULL;
        uint32_t rn = 0;
        if (!s)
            s = ud_json_serialize(ref, &ref_text, &rn);
        if (!s)
            s = ud_bytes_put(&b, ref_text, rn);
        ud_free(ref_text);
    }
    if (!s)
        s = ud_bytes_put(&b, ",\"package\":{\"id\":", 17);
    if (!s)
        s = ud_json_quote(&b, o->package_id.data, o->package_id.length);
    count = snprintf(value, sizeof(value), ",\"revision\":%llu}}}",
                     (unsigned long long)package_revision);
    if (!s)
        s = ud_bytes_put(&b, value, (size_t)count);
    if (!s && b.size > UINT32_MAX)
        s = UD_OVERFLOW;
    ud_package *with_provenance = NULL;
    if (!s)
        s = ud_package_create(c->ctx, (ud_string){(char *)b.data, (uint32_t)b.size},
                              &with_provenance);
    if (!s) {
        ud_package_destroy(*package);
        *package = with_provenance;
    }
    ud_free(text);
    ud_bytes_clear(&b);
    return s;
}
ud_status ud_catalog_bind_package(ud_catalog *c, const ud_catalog_binding *o, ud_package **out,
                                  char **parameters, uint32_t *length) {
    if (!out || !parameters || !length)
        return UD_INVALID;
    *out = NULL;
    *parameters = NULL;
    *length = 0;
    if (!c || !o)
        return UD_INVALID;
    if (o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!o->package_id.data || !o->package_id.length ||
        memchr(o->package_id.data, 0, o->package_id.length) ||
        !ud_utf8_valid(o->package_id.data, o->package_id.length))
        return UD_INVALID;
    ud_catalog_key key = {sizeof(key), UD_ABI_VERSION, UD_CATALOG_DEPLOYMENT, o->deployment_id,
                          o->deployment_revision};
    ud_json *deployment = NULL, *project_doc = NULL, *env_doc = NULL, *package_doc = NULL;
    ud_project *project = NULL;
    ud_package *package = NULL;
    char *project_text = NULL, *env_text = NULL, *package_text = NULL;
    uint32_t pn = 0, en = 0, kn = 0;
    ud_status s = get_document(c, &key, &deployment);
    if (!s)
        s = deployment_documents(c, deployment, &project_doc, &env_doc);
    if (!s)
        s = validate_references(c, project_doc, env_doc);
    if (!s) {
        const ud_json *refs = ud_member(project_doc, "packages");
        const ud_json *selected = NULL;
        for (uint32_t i = 0; i < ud_json_size(refs); i++) {
            ud_string id = {0};
            s = ud_json_string(ud_member(ud_json_at(refs, i), "id"), &id);
            if (s)
                break;
            if (ud_same(id, o->package_id))
                selected = ud_json_at(refs, i);
        }
        if (!s && !selected)
            s = UD_SCHEMA;
        if (!s)
            s = key_from_ref(selected, UD_CATALOG_PACKAGE, &key);
        if (!s)
            s = get_document(c, &key, &package_doc);
    }
    if (!s)
        s = ud_json_serialize(project_doc, &project_text, &pn);
    if (!s && env_doc)
        s = ud_json_serialize(env_doc, &env_text, &en);
    if (!s)
        s = ud_json_serialize(package_doc, &package_text, &kn);
    if (!s)
        s = ud_project_create(c->ctx, (ud_string){project_text, pn}, &project);
    if (!s)
        s = ud_package_create(c->ctx, (ud_string){package_text, kn}, &package);
    ud_project_binding binding = {sizeof(binding), UD_ABI_VERSION,        key.revision,
                                  {env_text, en},  o->project_parameters, o->package_parameters};
    if (!s)
        s = ud_project_bind(project, package, &binding, out, parameters, length);
    if (!s)
        s = provenance(c, o, deployment, key.revision, out);
    if (s) {
        ud_package_destroy(*out);
        *out = NULL;
        ud_free(*parameters);
        *parameters = NULL;
        *length = 0;
    }
    ud_project_destroy(project);
    ud_package_destroy(package);
    ud_json_destroy(deployment);
    ud_json_destroy(project_doc);
    ud_json_destroy(env_doc);
    ud_json_destroy(package_doc);
    ud_free(project_text);
    ud_free(env_text);
    ud_free(package_text);
    return s;
}