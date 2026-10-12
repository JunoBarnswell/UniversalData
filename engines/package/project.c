#include "internal.h"
#include "ud/project.h"
struct ud_project {
    ud_context *ctx;
    ud_json *document;
    ud_string id;
};
static ud_status json_put(ud_bytes *out, const ud_json *v) {
    char *json = NULL;
    uint32_t n=0;
    UD_TRY(ud_json_serialize(v, &json, &n));
    ud_status s = ud_bytes_put(out, json, n);
    ud_free(json);
    return s;
}
static ud_status read_object(ud_context *c, ud_string input, ud_json **out) {
    *out = NULL;
    if (!input.data && input.length)
        return UD_INVALID;
    ud_status s =
        ud_json_parse(c, input.data ? input.data : "{}", input.data ? input.length : 2, out, NULL);
    if (!s && ud_json_kind(*out) != UD_JSON_OBJECT)
        s = UD_SCHEMA;
    return s;
}
static ud_status version(const ud_json *v) {
    uint64_t n=0;
    UD_TRY(ud_json_uint(ud_member(v, "version"), &n));
    return n == UD_CONFIG_VERSION ? UD_OK : UD_INCOMPATIBLE;
}
static const ud_json *reference(const ud_project *p, ud_string id) {
    const ud_json *refs = ud_member(p->document, "packages");
    for (uint32_t i = 0; i < ud_json_size(refs); i++) {
        const ud_json *ref = ud_json_at(refs, i);
        ud_string name={0};
        if (!ud_json_string(ud_member(ref, "id"), &name) && ud_same(id, name))
            return ref;
    }
    return NULL;
}
ud_status ud_project_create(ud_context *c, ud_string input, ud_project **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!c || !input.data)
        return UD_INVALID;
    ud_project *p = ud_alloc(c, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->ctx = c;
    ud_status s = ud_json_parse(c, input.data, input.length, &p->document, NULL);
    static const char *keys[] = {"version",   "id",          "parameters",
                                 "variables", "connections", "packages"};
    if (!s)
        s = ud_json_keys(p->document, keys, 6);
    if (!s)
        s = version(p->document);
    if (!s)
        s = ud_json_string(ud_member(p->document, "id"), &p->id);
    if (!s)
        s = ud_parameters_check(p->ctx, ud_member(p->document, "parameters"), NULL, 0);
    if (!s)
        s = ud_parameters_check(p->ctx, ud_member(p->document, "variables"), NULL, 1);
    const ud_json *connections = ud_member(p->document, "connections");
    if (!s)
        s = ud_connection_definitions_check(connections);
    const ud_json *refs = ud_member(p->document, "packages");
    if (!s && ud_json_kind(refs) != UD_JSON_ARRAY)
        s = UD_SCHEMA;
    for (uint32_t i = 0; !s && i < ud_json_size(refs); i++) {
        const ud_json *ref = ud_json_at(refs, i);
        ud_string id={0};
        uint64_t revision=0;
        static const char *rk[] = {"id", "revision"};
        s = ud_json_keys(ref, rk, 2);
        if (!s)
            s = ud_json_string(ud_member(ref, "id"), &id);
        if (!s)
            s = ud_json_uint(ud_member(ref, "revision"), &revision);
        if (!s && (!revision || reference(p, id) != ref))
            s = UD_SCHEMA;
    }
    if (s) {
        ud_project_destroy(p);
        return s;
    }
    *out = p;
    return UD_OK;
}
ud_status ud_project_serialize(const ud_project *p, char **text, uint32_t *n) {
    return p ? ud_json_serialize(p->document, text, n) : UD_INVALID;
}
void ud_project_destroy(ud_project *p) {
    if (p) {
        ud_json_destroy(p->document);
        ud_free(p);
    }
}
/* Merge object values with explicit overrides without duplicating the parser. */
static ud_status merge(ud_context *ctx, const ud_json *first, const ud_json *second,
                       ud_json **out) {
    if ((first && ud_json_kind(first) != UD_JSON_OBJECT) ||
        (second && ud_json_kind(second) != UD_JSON_OBJECT))
        return UD_SCHEMA;
    ud_bytes b = {.ctx = ctx};
    ud_status s = ud_bytes_put(&b, "{", 1);
    int count = 0;
    for (int pass = 0; !s && pass < 2; pass++) {
        const ud_json *doc = pass ? second : first;
        for (uint32_t i = 0; !s && i < ud_json_size(doc); i++) {
            const char *key;
            uint32_t n=0;
            s = ud_json_key(doc, i, &key, &n);
            if (!s && !pass && ud_json_find(second, key, n))
                continue;
            if (!s && count++)
                s = ud_bytes_put(&b, ",", 1);
            if (!s)
                s = ud_json_quote(&b, key, n);
            if (!s)
                s = ud_bytes_put(&b, ":", 1);
            if (!s)
                s = json_put(&b, ud_json_at(doc, i));
        }
    }
    if (!s)
        s = ud_bytes_put(&b, "}", 1);
    if (!s && b.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s)
        s = ud_json_parse(ctx, (char *)b.data, (uint32_t)b.size, out, NULL);
    ud_bytes_clear(&b);
    return s;
}
static ud_status bound_definitions(ud_bytes *b, const ud_json *defs, const ud_json *values) {
    static const char *metadata[] = {"type", "nullable", "precision", "scale", "timezone", "children"};
    UD_TRY(ud_bytes_put(b, "{", 1));
    for (uint32_t i = 0; i < ud_json_size(defs); i++) {
        const char *key;
        uint32_t n=0;
        UD_TRY(ud_json_key(defs, i, &key, &n));
        const ud_json *def = ud_json_at(defs, i), *value = ud_json_find(values, key, n);
        if (!value) value = ud_member(def, "default");
        if (i) UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(ud_json_quote(b, key, n));
        UD_TRY(ud_bytes_put(b, ":{", 2));
        int field = 0;
        for (uint32_t j = 0; j < sizeof(metadata) / sizeof(metadata[0]); j++) {
            const ud_json *property = ud_member(def, metadata[j]);
            if (!property) continue;
            if (field++) UD_TRY(ud_bytes_put(b, ",", 1));
            UD_TRY(ud_json_quote(b, metadata[j], (uint32_t)strlen(metadata[j])));
            UD_TRY(ud_bytes_put(b, ":", 1));
            UD_TRY(json_put(b, property));
        }
        if (field++) UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(ud_bytes_put(b, "\"default\":", 10));
        UD_TRY(json_put(b, value));
        UD_TRY(ud_bytes_put(b, "}", 1));
    }
    return ud_bytes_put(b, "}", 1);
}
ud_status ud_project_bind(const ud_project *p, const ud_package *package,
                          const ud_project_binding *o, ud_package **out, char **parameters,
                          uint32_t *length) {
    if (!out || !parameters || !length)
        return UD_INVALID;
    *out = NULL;
    *parameters = NULL;
    *length = 0;
    if (!p || !package || !o)
        return UD_INVALID;
    if (o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!o->environment.data && o->environment.length)
        return UD_INVALID;
    if (ud_member(package->document, "project"))
        return UD_SCHEMA;
    const ud_json *ref = reference(p, package->id);
    uint64_t revision=0;
    if (!ref)
        return UD_SCHEMA;
    UD_TRY(ud_json_uint(ud_member(ref, "revision"), &revision));
    if (o->package_revision != revision)
        return UD_INCOMPATIBLE;
    ud_json *env = NULL, *explicit_project = NULL, *explicit_package = NULL, *project_values = NULL,
            *package_values = NULL;
    ud_status s = read_object(p->ctx, o->project_parameters, &explicit_project);
    if (!s)
        s = read_object(p->ctx, o->package_parameters, &explicit_package);
    if (!s && o->environment.data) {
        s = read_object(p->ctx, o->environment, &env);
        static const char *ek[] = {"version", "id", "projectId", "projectParameters",
                                   "packageParameters"};
        if (!s)
            s = ud_json_keys(env, ek, 5);
        if (!s)
            s = version(env);
        ud_string id={0};
        if (!s)
            s = ud_json_string(ud_member(env, "id"), &id);
        if (!s)
            s = ud_json_string(ud_member(env, "projectId"), &id);
        if (!s && !ud_same(id, p->id))
            s = UD_SCHEMA;
        const ud_json *values = ud_member(env, "packageParameters");
        if (!s && values && ud_json_kind(values) != UD_JSON_OBJECT)
            s = UD_SCHEMA;
        for (uint32_t i = 0; !s && i < ud_json_size(values); i++) {
            const char *key;
            uint32_t n=0;
            s = ud_json_key(values, i, &key, &n);
            if (!s && (!reference(p, (ud_string){key, n}) ||
                       ud_json_kind(ud_json_at(values, i)) != UD_JSON_OBJECT))
                s = UD_SCHEMA;
        }
    }
    if (!s)
        s = ud_parameters_check(p->ctx, ud_member(p->document, "parameters"),
                                ud_member(env, "projectParameters"), 0);
    if (!s)
        s = ud_parameters_check(
            p->ctx, ud_member(package->document, "parameters"),
            ud_json_find(ud_member(env, "packageParameters"), package->id.data, package->id.length),
            0);
    if (!s)
        s = merge(p->ctx, ud_member(env, "projectParameters"), explicit_project, &project_values);
    if (!s)
        s = merge(
            p->ctx,
            ud_json_find(ud_member(env, "packageParameters"), package->id.data, package->id.length),
            explicit_package, &package_values);
    if (!s)
        s = ud_parameters_check(p->ctx, ud_member(p->document, "parameters"), project_values, 1);
    if (!s)
        s = ud_parameters_check(p->ctx, ud_member(package->document, "parameters"), package_values, 1);
    ud_bytes b = {.ctx = p->ctx};
    if (!s)
        s = ud_bytes_put(&b, "{", 1);
    for (uint32_t i = 0; !s && i < ud_json_size(package->document); i++) {
        const char *key;
        uint32_t n=0;
        s = ud_json_key(package->document, i, &key, &n);
        if (!s && i)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = ud_json_quote(&b, key, n);
        if (!s)
            s = ud_bytes_put(&b, ":", 1);
        if (!s)
            s = json_put(&b, ud_json_at(package->document, i));
    }
    if (!s)
        s = ud_bytes_put(&b, ",\"project\":{\"id\":", 17);
    if (!s)
        s = ud_json_quote(&b, p->id.data, p->id.length);
    if (!s)
        s = ud_bytes_put(&b, ",\"parameters\":", 14);
    if (!s)
        s = bound_definitions(&b, ud_member(p->document, "parameters"), project_values);
    const char *fields[] = {"variables", "connections"};
    for (int i = 0; !s && i < 2; i++) {
        const ud_json *v = ud_member(p->document, fields[i]);
        if (!v)
            continue;
        s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = ud_json_quote(&b, fields[i], (uint32_t)strlen(fields[i]));
        if (!s)
            s = ud_bytes_put(&b, ":", 1);
        if (!s)
            s = json_put(&b, v);
    }
    if (!s)
        s = ud_bytes_put(&b, "}}", 2);
    if (!s && b.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s)
        s = ud_package_create(p->ctx, (ud_string){(char *)b.data, (uint32_t)b.size}, out);
    if (!s)
        s = ud_json_serialize(package_values, parameters, length);
    if (s) {
        ud_package_destroy(*out);
        *out = NULL;
    }
    ud_bytes_clear(&b);
    ud_json_destroy(env);
    ud_json_destroy(explicit_project);
    ud_json_destroy(explicit_package);
    ud_json_destroy(project_values);
    ud_json_destroy(package_values);
    return s;
}
