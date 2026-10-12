#include "internal.h"
const ud_json *ud_member(const ud_json *v, const char *s) {
    return ud_json_find(v, s, (uint32_t)strlen(s));
}
int ud_named(ud_string s, const char *v) {
    return s.length == strlen(v) && !memcmp(s.data, v, s.length);
}
int ud_same(ud_string a, ud_string b) {
    return a.length == b.length && (!a.length || !memcmp(a.data, b.data, a.length));
}
ud_status ud_json_string(const ud_json *v, ud_string *s) {
    if (ud_json_kind(v) != UD_JSON_STRING)
        return UD_SCHEMA;
    UD_TRY(ud_json_text(v, &s->data, &s->length));
    if (!s->length || s->length > 32767 || memchr(s->data, 0, s->length))
        return UD_SCHEMA;
    return UD_OK;
}
ud_status ud_json_uint(const ud_json *v, uint64_t *out) {
    if (ud_json_kind(v) != UD_JSON_NUMBER)
        return UD_SCHEMA;
    const char *text;
    uint32_t n;
    UD_TRY(ud_json_text(v, &text, &n));
    uint64_t value = 0;
    for (uint32_t i = 0; i < n; i++) {
        unsigned d = (unsigned char)text[i] - '0';
        if (d > 9 || value > (UINT64_MAX - d) / 10)
            return UD_OVERFLOW;
        value = value * 10 + d;
    }
    *out = value;
    return UD_OK;
}
ud_status ud_json_keys(const ud_json *v, const char *const *keys, uint32_t n) {
    if (ud_json_kind(v) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < ud_json_size(v); i++) {
        const char *key;
        uint32_t len;
        UD_TRY(ud_json_key(v, i, &key, &len));
        int found = 0;
        for (uint32_t j = 0; j < n; j++)
            if (len == strlen(keys[j]) && !memcmp(key, keys[j], len))
                found = 1;
        if (!found)
            return UD_SCHEMA;
    }
    return UD_OK;
}
ud_status ud_connection_definitions_check(const ud_json *defs) {
    if (defs && ud_json_kind(defs) != UD_JSON_ARRAY)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < ud_json_size(defs); i++) {
        const ud_json *def = ud_json_at(defs, i);
        static const char *keys[] = {"id", "driver", "resource", "credentialReference"};
        UD_TRY(ud_json_keys(def, keys, 4));
        ud_string id;
        UD_TRY(ud_json_string(ud_member(def, "id"), &id));
        for (uint32_t j = 0; j < i; j++) {
            ud_string other;
            UD_TRY(ud_json_string(ud_member(ud_json_at(defs, j), "id"), &other));
            if (ud_same(id, other))
                return UD_SCHEMA;
        }
        const char *fields[] = {"driver", "resource", "credentialReference"};
        for (int f = 0; f < 3; f++) {
            const ud_json *v = ud_member(def, fields[f]);
            if (f == 2 && !v)
                continue;
            if (ud_json_kind(v) == UD_JSON_STRING) {
                ud_string value;
                UD_TRY(ud_json_string(v, &value));
            } else {
                const ud_json *ref = ud_member(v, "$parameter"), *var = ud_member(v, "$variable");
                if (ud_json_kind(v) != UD_JSON_OBJECT || ud_json_size(v) != 1 || (!ref && !var))
                    return UD_SCHEMA;
                ud_string name;
                UD_TRY(ud_json_string(ref ? ref : var, &name));
            }
        }
    }
    return UD_OK;
}
ud_status ud_package_check(const ud_package *p) {
    static const char *keys[] = {"version",       "id",          "parameters", "variables",
                                 "connections",   "controlFlow", "dataFlows",  "policy",
                                 "eventHandlers", "project",     "deployment"};
    UD_TRY(ud_json_keys(p->document, keys, 11));
    uint64_t version;
    UD_TRY(ud_json_uint(ud_member(p->document, "version"), &version));
    if (version != UD_PACKAGE_VERSION)
        return UD_INCOMPATIBLE;
    const ud_json *project = ud_member(p->document, "project");
    if (project) {
        static const char *pk[] = {"id", "parameters", "variables", "connections"};
        UD_TRY(ud_json_keys(project, pk, 4));
        ud_string id;
        UD_TRY(ud_json_string(ud_member(project, "id"), &id));
        UD_TRY(ud_parameters_check(p->ctx, ud_member(project, "parameters"), NULL, 1));
        UD_TRY(ud_parameters_check(p->ctx, ud_member(project, "variables"), NULL, 1));
        const ud_json *connections = ud_member(project, "connections");
        UD_TRY(ud_connection_definitions_check(connections));
    }
    const ud_json *deployment = ud_member(p->document, "deployment");
    if (deployment) {
        static const char *dk[] = {"id", "revision", "project", "package", "environment"};
        UD_TRY(ud_json_keys(deployment, dk, 5));
        ud_string id;
        uint64_t revision;
        UD_TRY(ud_json_string(ud_member(deployment, "id"), &id));
        UD_TRY(ud_json_uint(ud_member(deployment, "revision"), &revision));
        if (!revision || !project)
            return UD_SCHEMA;
        const char *names[] = {"project", "package", "environment"};
        for (int i = 0; i < 3; i++) {
            const ud_json *ref = ud_member(deployment, names[i]);
            if (i == 2 && !ref)
                continue;
            static const char *rk[] = {"id", "revision"};
            UD_TRY(ud_json_keys(ref, rk, 2));
            UD_TRY(ud_json_string(ud_member(ref, "id"), &id));
            UD_TRY(ud_json_uint(ud_member(ref, "revision"), &revision));
            if (!revision)
                return UD_SCHEMA;
            if (i < 2) {
                ud_string expected;
                UD_TRY(ud_json_string(ud_member(i ? p->document : project, "id"), &expected));
                if (!ud_same(id, expected))
                    return UD_SCHEMA;
            }
        }
    }
    UD_TRY(ud_parameters_check(p->ctx, ud_member(p->document, "parameters"), NULL, 0));
    UD_TRY(ud_parameters_check(p->ctx, ud_member(p->document, "variables"), NULL, 1));
    if (ud_json_kind(ud_member(p->document, "controlFlow")) != UD_JSON_OBJECT ||
        ud_json_kind(ud_member(p->document, "dataFlows")) != UD_JSON_ARRAY)
        return UD_SCHEMA;
    const char *objects[] = {"parameters", "variables", "policy", "eventHandlers"};
    for (int i = 0; i < 4; i++) {
        const ud_json *v = ud_member(p->document, objects[i]);
        if (v && ud_json_kind(v) != UD_JSON_OBJECT)
            return UD_SCHEMA;
    }
    const ud_json *policy = ud_member(p->document, "policy");
    if (policy) {
        static const char *allowed[] = {"maxConcurrency"};
        UD_TRY(ud_json_keys(policy, allowed, 1));
        if (ud_member(policy, "maxConcurrency")) {
            uint64_t concurrency;
            UD_TRY(ud_json_uint(ud_member(policy, "maxConcurrency"), &concurrency));
            if (concurrency != 1)
                return UD_UNSUPPORTED;
        }
    }
    const ud_json *connections = ud_member(p->document, "connections");
    UD_TRY(ud_connection_definitions_check(connections));
    const ud_json *flows = ud_member(p->document, "dataFlows");
    for (uint32_t i = 0; i < ud_json_size(flows); i++) {
        ud_string id;
        UD_TRY(ud_json_string(ud_member(ud_json_at(flows, i), "id"), &id));
        for (uint32_t j = 0; j < i; j++) {
            ud_string other;
            UD_TRY(ud_json_string(ud_member(ud_json_at(flows, j), "id"), &other));
            if (ud_same(id, other))
                return UD_SCHEMA;
        }
    }
    return UD_OK;
}
ud_status ud_package_create(ud_context *c, ud_string json, ud_package **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!c || !json.data)
        return UD_INVALID;
    ud_package *p = ud_alloc(c, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->ctx = c;
    ud_status s = ud_json_parse(c, json.data, json.length, &p->document, NULL);
    if (!s)
        s = ud_package_check(p);
    if (!s)
        s = ud_json_string(ud_member(p->document, "id"), &p->id);
    if (!s) {
        char *text = NULL;
        uint32_t len;
        s = ud_json_serialize(p->document, &text, &len);
        if (!s)
            ud_hash(text, len, p->fingerprint);
        ud_free(text);
    }
    if (s) {
        ud_package_destroy(p);
        return ud_fail(c, s, "invalid Package definition");
    }
    *out = p;
    return UD_OK;
}
ud_status ud_package_serialize(const ud_package *p, char **out, uint32_t *length) {
    return p ? ud_json_serialize(p->document, out, length) : UD_INVALID;
}
void ud_package_destroy(ud_package *p) {
    if (p) {
        ud_json_destroy(p->document);
        ud_free(p);
    }
}
static ud_status json_put(ud_bytes *bytes, const ud_json *value) {
    char *text = NULL;
    uint32_t length = 0;
    UD_TRY(ud_json_serialize(value, &text, &length));
    ud_status s = ud_bytes_put(bytes, text, length);
    ud_free(text);
    return s;
}
ud_status ud_definition_box(ud_context *ctx, ud_string name_space, ud_string name,
                            const ud_json *def, const ud_json *provided, int required,
                            ud_batch **out) {
    if (!ctx || !out || !name.length || !name.data || memchr(name.data, 0, name.length))
        return UD_INVALID;
    *out = NULL;
    static const char *keys[] = {"type", "default", "nullable", "precision", "scale", "timezone", "children"};
    UD_TRY(ud_json_keys(def, keys, 7));
    if (!ud_member(def, "type")) return UD_SCHEMA;
    const ud_json *value = provided ? provided : ud_member(def, "default");
    if (!value && required) return UD_SCHEMA;
    ud_bytes schema = {.ctx = ctx};
    const char *prefix = "{\"version\":4,\"fields\":[{\"name\":";
    ud_status s = ud_bytes_put(&schema, prefix, strlen(prefix));
    if (!s) s = ud_json_quote(&schema, name.data, name.length);
    for (uint32_t i = 0; !s && i < 7; i++) {
        if (i == 1) continue;
        const ud_json *attribute = ud_member(def, keys[i]);
        if (!attribute && i != 2) continue;
        s = ud_bytes_put(&schema, ",", 1);
        if (!s) s = ud_json_quote(&schema, keys[i], (uint32_t)strlen(keys[i]));
        if (!s) s = ud_bytes_put(&schema, ":", 1);
        if (!s) s = attribute ? json_put(&schema, attribute) : ud_bytes_put(&schema, "true", 4);
    }
    if (!s) s = ud_bytes_put(&schema, "}]}", 3);
    if (!s && schema.size > UINT32_MAX) s = UD_OVERFLOW;
    ud_batch *definition = NULL;
    if (!s) s = ud_schema_from_json(ctx, (const char *)schema.data, (uint32_t)schema.size,
                                   name_space, &definition);
    ud_bytes_clear(&schema);
    if (!s && value) {
        ud_field field = {0};
        ud_value decoded = {0};
        s = ud_batch_field(definition, 0, &field);
        if (!s) s = ud_json_value_decode(ctx, &field, value, UD_ROUND_EXACT, &decoded);
        if (!s) s = ud_batch_create(ctx, &field, 1, 1, out);
        if (!s) s = ud_batch_append(*out, &decoded, 1);
        ud_clear_value(&decoded);
    } else if (!s) {
        *out = definition;
        definition = NULL;
    }
    if (!s) s = ud_batch_seal(*out);
    ud_batch_destroy(definition);
    if (s) { ud_batch_destroy(*out); *out = NULL; }
    return s;
}
ud_status ud_parameters_check(ud_context *ctx, const ud_json *defs, const ud_json *provided,
                              int required) {
    if ((defs && ud_json_kind(defs) != UD_JSON_OBJECT) ||
        (provided && ud_json_kind(provided) != UD_JSON_OBJECT)) return UD_SCHEMA;
    for (uint32_t i = 0; i < ud_json_size(provided); i++) {
        const char *key;
        uint32_t length;
        UD_TRY(ud_json_key(provided, i, &key, &length));
        if (!ud_json_find(defs, key, length)) return UD_SCHEMA;
    }
    for (uint32_t i = 0; i < ud_json_size(defs); i++) {
        const char *key;
        uint32_t length;
        UD_TRY(ud_json_key(defs, i, &key, &length));
        ud_batch *box = NULL;
        ud_status s = ud_definition_box(ctx, (ud_string){"definition-validation", 21},
                                        (ud_string){key, length}, ud_json_at(defs, i),
                                        ud_json_find(provided, key, length), required, &box);
        ud_batch_destroy(box);
        if (s) return s;
    }
    return UD_OK;
}
static void slots_free(ud_variable *slot) {
    while (slot) {
        ud_variable *next = slot->next;
        ud_batch_destroy(slot->box);
        ud_free(slot);
        slot = next;
    }
}
static ud_status slots_bind(ud_execution *e, ud_scope *scope, const ud_json *defs,
                           const ud_json *provided, int readonly, ud_variable **out) {
    UD_TRY(ud_parameters_check(e->ctx, defs, provided, 1));
    char identity[33];
    ud_column_id_format(scope->identity, identity);
    ud_variable *slots = NULL;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < ud_json_size(defs); i++) {
        const char *key;
        uint32_t length;
        s = ud_json_key(defs, i, &key, &length);
        ud_variable *slot = NULL;
        if (!s) { slot = ud_alloc(e->ctx, sizeof(*slot)); if (!slot) s = UD_RESOURCE; }
        if (!s) {
            slot->next = slots; slots = slot;
            slot->name = (ud_string){key, length}; slot->readonly = readonly;
            s = ud_definition_box(e->ctx, (ud_string){identity, 32}, slot->name,
                                   ud_json_at(defs, i), ud_json_find(provided, key, length),
                                   1, &slot->box);
        }
        if (!s) s = ud_batch_get(slot->box, 0, 0, &slot->value);
    }
    if (s) slots_free(slots); else *out = slots;
    return s;
}
ud_status ud_scope_enter(ud_execution *e, ud_string name, const ud_json *defs, ud_scope *scope) {
    scope->parent = e->scope;
    scope->values = scope->parameter_values = NULL;
    scope->parameters = scope->provided = NULL;
    e->scope = scope;
    ud_bytes root = {.ctx = e->ctx};
    ud_status s = UD_OK;
    char parent[33];
    if (scope->parent) {
        ud_column_id_format(scope->parent->identity, parent);
        s = ud_column_id_make(e->ctx, (ud_string){parent, 32}, name, &scope->identity);
    } else {
        s = ud_bytes_put(&root, "[", 1);
        if (!s) s = ud_json_quote(&root, e->options.tenant.data, e->options.tenant.length);
        if (!s) s = ud_bytes_put(&root, ",", 1);
        if (!s) s = ud_json_quote(&root, e->package->id.data, e->package->id.length);
        if (!s) s = ud_bytes_put(&root, "]", 1);
        if (!s && root.size > UINT32_MAX) s = UD_OVERFLOW;
        if (!s) s = ud_column_id_make(e->ctx, (ud_string){(const char *)root.data, (uint32_t)root.size},
                                      name, &scope->identity);
    }
    ud_bytes_clear(&root);
    if (!s) s = slots_bind(e, scope, defs, NULL, 0, &scope->values);
    return s;
}
ud_status ud_scope_bind_parameters(ud_execution *e, ud_scope *scope) {
    if (!e || !scope || scope->parameter_values) return UD_INVALID;
    return slots_bind(e, scope, scope->parameters, scope->provided, 1, &scope->parameter_values);
}
void ud_scope_leave(ud_execution *e, ud_scope *scope) {
    slots_free(scope->values);
    slots_free(scope->parameter_values);
    e->scope = scope->parent;
    scope->values = scope->parameter_values = NULL;
}
static ud_variable *find_slot(const ud_execution *e, ud_string name, int parameters_only) {
    for (ud_scope *scope = e->scope; scope; scope = scope->parent) {
        if (!parameters_only)
            for (ud_variable *v = scope->values; v; v = v->next)
                if (ud_same(v->name, name)) return v;
        for (ud_variable *v = scope->parameter_values; v; v = v->next)
            if (ud_same(v->name, name)) return v;
    }
    return NULL;
}
ud_status ud_execution_variable(const ud_execution *e, ud_string name, ud_value *out) {
    if (!e || !out) return UD_INVALID;
    ud_variable *slot = find_slot(e, name, 0);
    if (!slot) return UD_SCHEMA;
    *out = slot->value;
    return UD_OK;
}
ud_status ud_execution_variable_field(const ud_execution *e, ud_string name, ud_field *out) {
    if (!e || !out) return UD_INVALID;
    ud_variable *slot = find_slot(e, name, 0);
    return slot ? ud_batch_field(slot->box, 0, out) : UD_SCHEMA;
}
ud_status ud_variable_writable(ud_execution *e, ud_string name, ud_variable **out) {
    if (!e || !out) return UD_INVALID;
    *out = find_slot(e, name, 0);
    return !*out ? UD_SCHEMA : (*out)->readonly ? UD_AUTH : UD_OK;
}
ud_status ud_execution_set_variable(ud_execution *e, ud_string name, const ud_value *value,
                                    const ud_field *provenance) {
    if (!e || !value) return UD_INVALID;
    ud_variable *slot = NULL;
    UD_TRY(ud_variable_writable(e, name, &slot));
    ud_field declared = {0}, target = {0};
    UD_TRY(ud_batch_field(slot->box, 0, &declared));
    if (value->type != declared.type ||
        (provenance && !ud_field_matches(provenance, &declared, UD_FIELD_VALUE_TYPE))) return UD_SCHEMA;
    ud_status s = provenance ? ud_field_cast_metadata(e->ctx, provenance, &declared, &target)
                             : ud_field_copy(e->ctx, &declared, (ud_column_id){0}, &target);
    ud_batch *box = NULL;
    if (!s) s = ud_batch_create(e->ctx, &target, 1, 1, &box);
    ud_field_clear(&target);
    if (!s) s = ud_batch_append(box, value, 1);
    if (!s) s = ud_batch_seal(box);
    ud_value borrowed = {0};
    if (!s) s = ud_batch_get(box, 0, 0, &borrowed);
    if (!s) { ud_batch_destroy(slot->box); slot->box = box; slot->value = borrowed; }
    else ud_batch_destroy(box);
    return s;
}
ud_status ud_resolve_value(void *user, ud_string name, ud_value *out) {
    return ud_execution_variable(user, name, out);
}
ud_status ud_resolve_field(void *user, ud_string name, ud_field *out) {
    return ud_execution_variable_field(user, name, out);
}
ud_status ud_expression_json(ud_execution *e, const ud_json *value, const ud_batch *schema,
                             ud_expression **out) {
    char *json = NULL;
    uint32_t length;
    UD_TRY(ud_json_serialize(value, &json, &length));
    ud_expression_options options = {sizeof(options), UD_ABI_VERSION, schema, ud_resolve_value, e,
                                      ud_resolve_field};
    ud_status s = ud_expression_create(e->ctx, (ud_string){json, length}, &options, out);
    ud_free(json);
    return s;
}
ud_status ud_scalar_json(ud_bytes *bytes, const ud_field *field, const ud_value *value) {
    return ud_json_value_encode(bytes, field, value);
}
static ud_status bound_put(ud_execution *e, ud_bytes *b, const ud_json *v) {
    const ud_json *p = ud_member(v, "$parameter"), *var = ud_member(v, "$variable");
    if (p || var) {
        if (ud_json_size(v) != 1 || (p && var))
            return UD_SCHEMA;
        ud_string name;
        UD_TRY(ud_json_string(p ? p : var, &name));
        ud_variable *slot = find_slot(e, name, p != NULL);
        if (!slot) return UD_SCHEMA;
        ud_field field;
        UD_TRY(ud_batch_field(slot->box, 0, &field));
        return ud_scalar_json(b, &field, &slot->value);
    }
    int kind = ud_json_kind(v);
    if (kind != UD_JSON_OBJECT && kind != UD_JSON_ARRAY) {
        char *json;
        uint32_t length;
        UD_TRY(ud_json_serialize(v, &json, &length));
        ud_status s = ud_bytes_put(b, json, length);
        ud_free(json);
        return s;
    }
    UD_TRY(ud_bytes_put(b, kind == UD_JSON_OBJECT ? "{" : "[", 1));
    for (uint32_t i = 0; i < ud_json_size(v); i++) {
        if (i)
            UD_TRY(ud_bytes_put(b, ",", 1));
        if (kind == UD_JSON_OBJECT) {
            const char *key;
            uint32_t len;
            UD_TRY(ud_json_key(v, i, &key, &len));
            UD_TRY(ud_json_quote(b, key, len));
            UD_TRY(ud_bytes_put(b, ":", 1));
        }
        UD_TRY(bound_put(e, b, ud_json_at(v, i)));
    }
    return ud_bytes_put(b, kind == UD_JSON_OBJECT ? "}" : "]", 1);
}
ud_status ud_bound_config(ud_execution *e, const ud_json *v, ud_json **out) {
    ud_bytes b = {.ctx = e->ctx};
    ud_status s = bound_put(e, &b, v);
    if (!s && b.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s)
        s = ud_json_parse(e->ctx, (const char *)b.data, (uint32_t)b.size, out, NULL);
    ud_bytes_clear(&b);
    return s;
}
