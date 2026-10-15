#include "internal.h"

static ud_status append(ud_bytes *b, const ud_json *value) {
    char *text = NULL;
    uint32_t length = 0;
    ud_status s = ud_json_serialize(value, &text, &length);
    if (!s)
        s = ud_bytes_put(b, text, length);
    ud_free(text);
    return s;
}
static ud_status ports(ud_bytes *b, const ud_json *paths, const ud_json *bound) {
    if (!bound)
        return append(b, paths);
    if (ud_json_size(paths) != ud_json_size(bound))
        return UD_CORRUPT;
    UD_TRY(ud_bytes_put(b, "[", 1));
    for (uint32_t i = 0; i < ud_json_size(paths); i++) {
        const ud_json *path = ud_json_at(paths, i), *plan = ud_json_at(bound, i);
        ud_string from = {0}, to = {0}, a = {0}, z = {0};
        UD_TRY(ud_json_string(ud_member(path, "from"), &from));
        UD_TRY(ud_json_string(ud_member(path, "to"), &to));
        UD_TRY(ud_json_string(ud_member(plan, "from"), &a));
        UD_TRY(ud_json_string(ud_member(plan, "to"), &z));
        if (!ud_same(from, a) || !ud_same(to, z))
            return UD_CORRUPT;
        if (i)
            UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(ud_bytes_put(b, "{\"from\":", 8));
        UD_TRY(ud_json_quote(b, from.data, from.length));
        UD_TRY(ud_bytes_put(b, ",\"to\":", 6));
        UD_TRY(ud_json_quote(b, to.data, to.length));
        UD_TRY(ud_bytes_put(b, ",\"output\":", 10));
        UD_TRY(append(b, ud_member(plan, "output")));
        UD_TRY(ud_bytes_put(b, ",\"input\":", 9));
        UD_TRY(append(b, ud_member(plan, "input")));
        UD_TRY(ud_bytes_put(b, "}", 1));
    }
    return ud_bytes_put(b, "]", 1);
}
static ud_status flow(ud_bytes *b, const ud_json *definition, const ud_json *plans) {
    ud_string id = {0};
    UD_TRY(ud_json_string(ud_member(definition, "id"), &id));
    const ud_json *bound = NULL;
    for (uint32_t i = 0; i < ud_json_size(plans); i++) {
        const ud_json *plan = ud_json_at(plans, i);
        ud_string name = {0};
        UD_TRY(ud_json_string(ud_member(plan, "id"), &name));
        if (!ud_same(id, name))
            continue;
        const ud_json *next = ud_member(plan, "paths");
        if (bound) {
            /* Reused flows with scope-dependent physical port assignments may
             * not be silently fixed to one task's configuration. */
            char *a = NULL, *z = NULL;
            uint32_t an = 0, zn = 0;
            ud_status s = ud_json_serialize(bound, &a, &an);
            if (!s)
                s = ud_json_serialize(next, &z, &zn);
            if (!s && (an != zn || memcmp(a, z, an)))
                s = UD_UNSUPPORTED;
            ud_free(a);
            ud_free(z);
            if (s)
                return s;
        } else
            bound = next;
    }
    if (!bound)
        for (uint32_t i = 0; i < ud_json_size(ud_member(definition, "paths")); i++) {
            const ud_json *path = ud_json_at(ud_member(definition, "paths"), i);
            const char *keys[] = {"output", "input"};
            for (uint32_t k = 0; k < 2; k++) {
                const ud_json *reference = ud_member(path, keys[k]);
                ud_string text = {0};
                ud_id identity;
                if (ud_json_kind(reference) == UD_JSON_STRING &&
                    !ud_json_string(reference, &text) && !ud_id_parse(text, &identity))
                    return UD_UNSUPPORTED;
            }
        }
    UD_TRY(ud_bytes_put(b, "{", 1));
    for (uint32_t i = 0; i < ud_json_size(definition); i++) {
        const char *key = NULL;
        uint32_t n = 0;
        UD_TRY(ud_json_key(definition, i, &key, &n));
        if (i)
            UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(ud_json_quote(b, key, n));
        UD_TRY(ud_bytes_put(b, ":", 1));
        const ud_json *value = ud_json_find(definition, key, n);
        if (n == 5 && !memcmp(key, "paths", 5))
            UD_TRY(ports(b, value, bound));
        else
            UD_TRY(append(b, value));
    }
    return ud_bytes_put(b, "}", 1);
}
ud_status ud_execution_reidentify_package(ud_execution *e, ud_string id, char **out,
                                          uint32_t *length) {
    if (!e || !out || !length || !id.data || !id.length || id.length > 255)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (atomic_load(&e->state) != UD_EXEC_READY)
        return UD_BUSY;
    if (e->external || ud_member(e->package->document, "extensions"))
        return UD_UNSUPPORTED;
    char *plan = NULL;
    uint32_t size = 0;
    ud_json *report = NULL;
    ud_status s = ud_execution_explain(e, &plan, &size);
    if (!s)
        s = ud_json_parse(e->ctx, plan, size, &report, NULL);
    ud_bytes b = {.ctx = e->ctx, .limit = 67108864};
    if (!s)
        s = ud_bytes_put(&b, "{", 1);
    for (uint32_t i = 0; !s && i < ud_json_size(e->package->document); i++) {
        const char *key = NULL;
        uint32_t n = 0;
        s = ud_json_key(e->package->document, i, &key, &n);
        if (!s && i)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = ud_json_quote(&b, key, n);
        if (!s)
            s = ud_bytes_put(&b, ":", 1);
        const ud_json *value = !s ? ud_json_find(e->package->document, key, n) : NULL;
        if (!s && n == 2 && !memcmp(key, "id", 2))
            s = ud_json_quote(&b, id.data, id.length);
        else if (!s && n == 9 && !memcmp(key, "dataFlows", 9)) {
            s = ud_bytes_put(&b, "[", 1);
            for (uint32_t j = 0; !s && j < ud_json_size(value); j++) {
                if (j)
                    s = ud_bytes_put(&b, ",", 1);
                if (!s)
                    s = flow(&b, ud_json_at(value, j), ud_member(report, "pipelines"));
            }
            if (!s)
                s = ud_bytes_put(&b, "]", 1);
        } else if (!s)
            s = append(&b, value);
    }
    if (!s)
        s = ud_bytes_put(&b, "}", 1);
    ud_package *validated = NULL;
    if (!s)
        s = ud_package_create(e->ctx, (ud_string){(const char *)b.data, (uint32_t)b.size},
                              &validated);
    ud_package_destroy(validated);
    if (!s) {
        *out = ud_strdup_n(e->ctx, (const char *)b.data, (uint32_t)b.size);
        if (!*out)
            s = UD_RESOURCE;
        else
            *length = (uint32_t)b.size;
    }
    ud_bytes_clear(&b);
    ud_free(plan);
    ud_json_destroy(report);
    return s;
}
