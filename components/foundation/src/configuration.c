#include "../internal.h"
#include "ud/json.h"
#include <errno.h>
#include <math.h>

/* One declarative binder for Registry, Package and direct SDK consumers. */
static const ud_json *get(const ud_json *j, const char *key) {
    return ud_json_find(j, key, (uint32_t)strlen(key));
}
static int text(const ud_json *j, const char *literal) {
    const char *p = NULL;
    uint32_t n = 0;
    return j && !ud_json_text(j, &p, &n) && strlen(literal) == n && !memcmp(p, literal, n);
}
static ud_status number(const ud_json *j, long double *out) {
    const char *p;
    uint32_t n;
    if (ud_json_kind(j) != UD_JSON_NUMBER || ud_json_text(j, &p, &n) || !n || n >= 128)
        return UD_SCHEMA;
    char buffer[128], *end = NULL;
    memcpy(buffer, p, n);
    buffer[n] = 0;
    errno = 0;
    *out = strtold(buffer, &end);
    return errno || end != buffer + n || !isfinite(*out) ? UD_SCHEMA : UD_OK;
}
static int equal(const ud_json *a, const ud_json *b, uint32_t depth) {
    if (!a || !b || depth > 64 || ud_json_kind(a) != ud_json_kind(b))
        return 0;
    int k = ud_json_kind(a);
    if (k == UD_JSON_NULL)
        return 1;
    if (k == UD_JSON_NUMBER) {
        long double x, y;
        return !number(a, &x) && !number(b, &y) && x == y;
    }
    if (k == UD_JSON_ARRAY || k == UD_JSON_OBJECT) {
        if (ud_json_size(a) != ud_json_size(b))
            return 0;
        for (uint32_t i = 0; i < ud_json_size(a); i++) {
            const ud_json *other = ud_json_at(b, i);
            if (k == UD_JSON_OBJECT) {
                const char *key;
                uint32_t n;
                if (ud_json_key(a, i, &key, &n))
                    return 0;
                other = ud_json_find(b, key, n);
            }
            if (!equal(ud_json_at(a, i), other, depth + 1))
                return 0;
        }
        return 1;
    }
    const char *x, *y;
    uint32_t nx, ny;
    return !ud_json_text(a, &x, &nx) && !ud_json_text(b, &y, &ny) && nx == ny && !memcmp(x, y, nx);
}
static int type(const ud_json *v, const ud_json *t) {
    if (ud_json_kind(t) == UD_JSON_ARRAY) {
        for (uint32_t i = 0; i < ud_json_size(t); i++)
            if (type(v, ud_json_at(t, i)))
                return 1;
        return 0;
    }
    int k = ud_json_kind(v);
    if (text(t, "integer")) {
        long double n;
        return !number(v, &n) && truncl(n) == n;
    }
    return (text(t, "object") && k == UD_JSON_OBJECT) ||
           (text(t, "array") && k == UD_JSON_ARRAY) ||
           (text(t, "string") && k == UD_JSON_STRING) ||
           (text(t, "number") && k == UD_JSON_NUMBER) ||
           (text(t, "boolean") && k == UD_JSON_BOOL) || (text(t, "null") && k == UD_JSON_NULL);
}
static ud_status schema_keys(const ud_json *s) {
    static const char *known[] = {"type", "properties", "required", "additionalProperties",
        "items", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "minItems",
        "maxItems", "minLength", "maxLength", "minProperties", "maxProperties", "uniqueItems",
        "enum", "const", "anyOf", "oneOf", "allOf", "default", "title", "description",
        "$schema", "$id", "examples", "deprecated", "readOnly", "writeOnly"};
    if (ud_json_kind(s) == UD_JSON_BOOL)
        return UD_OK;
    if (ud_json_kind(s) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < ud_json_size(s); i++) {
        const char *key;
        uint32_t n;
        UD_TRY(ud_json_key(s, i, &key, &n));
        int valid = n > 2 && !memcmp(key, "x-", 2);
        for (uint32_t j = 0; j < sizeof(known) / sizeof(*known); j++)
            valid |= strlen(known[j]) == n && !memcmp(key, known[j], n);
        if (!valid)
            return UD_UNSUPPORTED;
    }
    return UD_OK;
}
static ud_status raw(ud_bytes *out, const ud_json *v) {
    char *p = NULL;
    uint32_t n = 0;
    UD_TRY(ud_json_serialize(v, &p, &n));
    ud_status s = ud_bytes_put(out, p, n);
    ud_free(p);
    return s;
}
static ud_status bind(ud_bytes *, const ud_json *, const ud_json *, uint32_t);
static ud_status choices(ud_bytes *out, const ud_json *s, const ud_json *v, uint32_t depth) {
    const char *names[] = {"anyOf", "oneOf", "allOf"};
    for (uint32_t k = 0; k < 3; k++) {
        const ud_json *list = get(s, names[k]);
        if (!list)
            continue;
        if (ud_json_kind(list) != UD_JSON_ARRAY || !ud_json_size(list))
            return UD_SCHEMA;
        uint32_t matched = 0;
        for (uint32_t i = 0; i < ud_json_size(list); i++) {
            ud_bytes tmp = {.ctx = out->ctx};
            ud_status status = bind(&tmp, ud_json_at(list, i), v, depth + 1);
            ud_bytes_clear(&tmp);
            if (status && status != UD_SCHEMA)
                return status;
            matched += !status;
        }
        if ((k == 0 && !matched) || (k == 1 && matched != 1) ||
            (k == 2 && matched != ud_json_size(list)))
            return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status bounds(const ud_json *s, const char *low, const char *high, long double n) {
    long double bound;
    const ud_json *v = get(s, low);
    if (v && (number(v, &bound) || n < bound))
        return UD_SCHEMA;
    v = get(s, high);
    return v && (number(v, &bound) || n > bound) ? UD_SCHEMA : UD_OK;
}
static ud_status bind(ud_bytes *out, const ud_json *s, const ud_json *v, uint32_t depth) {
    if (depth > 64 || !v)
        return UD_SCHEMA;
    UD_TRY(schema_keys(s));
    if (ud_json_kind(s) == UD_JSON_BOOL)
        return text(s, "true") ? raw(out, v) : UD_SCHEMA;
    const ud_json *t = get(s, "type");
    if (t && !type(v, t))
        return UD_SCHEMA;
    UD_TRY(choices(out, s, v, depth));
    const ud_json *constant = get(s, "const"), *enumeration = get(s, "enum");
    if (constant && !equal(v, constant, 0))
        return UD_SCHEMA;
    if (enumeration) {
        int found = 0;
        if (ud_json_kind(enumeration) != UD_JSON_ARRAY)
            return UD_SCHEMA;
        for (uint32_t i = 0; i < ud_json_size(enumeration); i++)
            found |= equal(v, ud_json_at(enumeration, i), 0);
        if (!found)
            return UD_SCHEMA;
    }
    int k = ud_json_kind(v);
    if (k == UD_JSON_NUMBER) {
        long double n, b;
        UD_TRY(number(v, &n));
        UD_TRY(bounds(s, "minimum", "maximum", n));
        const ud_json *x = get(s, "exclusiveMinimum"), *y = get(s, "exclusiveMaximum");
        if ((x && (number(x, &b) || n <= b)) || (y && (number(y, &b) || n >= b)))
            return UD_SCHEMA;
    }
    if (k == UD_JSON_STRING) {
        const char *p;
        uint32_t n, chars = 0;
        UD_TRY(ud_json_text(v, &p, &n));
        for (uint32_t i = 0; i < n; i++)
            chars += ((uint8_t)p[i] & 0xc0) != 0x80;
        UD_TRY(bounds(s, "minLength", "maxLength", chars));
    }
    if (k == UD_JSON_ARRAY) {
        UD_TRY(bounds(s, "minItems", "maxItems", ud_json_size(v)));
        const ud_json *item = get(s, "items");
        UD_TRY(ud_bytes_put(out, "[", 1));
        for (uint32_t i = 0; i < ud_json_size(v); i++) {
            if (text(get(s, "uniqueItems"), "true"))
                for (uint32_t j = 0; j < i; j++)
                    if (equal(ud_json_at(v, i), ud_json_at(v, j), 0))
                        return UD_SCHEMA;
            if (i)
                UD_TRY(ud_bytes_put(out, ",", 1));
            if (item)
                UD_TRY(bind(out, item, ud_json_at(v, i), depth + 1));
            else
                UD_TRY(raw(out, ud_json_at(v, i)));
        }
        return ud_bytes_put(out, "]", 1);
    }
    if (k != UD_JSON_OBJECT)
        return raw(out, v);
    UD_TRY(bounds(s, "minProperties", "maxProperties", ud_json_size(v)));
    const ud_json *props = get(s, "properties"), *required = get(s, "required");
    if ((props && ud_json_kind(props) != UD_JSON_OBJECT) ||
        (required && ud_json_kind(required) != UD_JSON_ARRAY))
        return UD_SCHEMA;
    for (uint32_t i = 0; i < ud_json_size(required); i++) {
        const char *key;
        uint32_t n;
        if (ud_json_kind(ud_json_at(required, i)) != UD_JSON_STRING)
            return UD_SCHEMA;
        UD_TRY(ud_json_text(ud_json_at(required, i), &key, &n));
        const ud_json *property = ud_json_find(props, key, n);
        if (!ud_json_find(v, key, n) && !get(property, "default"))
            return UD_SCHEMA;
    }
    UD_TRY(ud_bytes_put(out, "{", 1));
    uint32_t written = 0;
    for (uint32_t i = 0; i < ud_json_size(v); i++) {
        const char *key;
        uint32_t n;
        UD_TRY(ud_json_key(v, i, &key, &n));
        const ud_json *property = ud_json_find(props, key, n), *extra = get(s, "additionalProperties");
        if (!property && text(extra, "false"))
            return UD_SCHEMA;
        if (written++)
            UD_TRY(ud_bytes_put(out, ",", 1));
        UD_TRY(ud_json_quote(out, key, n));
        UD_TRY(ud_bytes_put(out, ":", 1));
        if (property || (extra && ud_json_kind(extra) == UD_JSON_OBJECT))
            UD_TRY(bind(out, property ? property : extra, ud_json_at(v, i), depth + 1));
        else
            UD_TRY(raw(out, ud_json_at(v, i)));
    }
    for (uint32_t i = 0; i < ud_json_size(props); i++) {
        const char *key;
        uint32_t n;
        UD_TRY(ud_json_key(props, i, &key, &n));
        const ud_json *property = ud_json_at(props, i), *value = get(property, "default");
        if (!value || ud_json_find(v, key, n))
            continue;
        if (written++)
            UD_TRY(ud_bytes_put(out, ",", 1));
        UD_TRY(ud_json_quote(out, key, n));
        UD_TRY(ud_bytes_put(out, ":", 1));
        UD_TRY(bind(out, property, value, depth + 1));
    }
    return ud_bytes_put(out, "}", 1);
}
ud_status ud_json_bind_schema(ud_context *ctx, const ud_json *schema, const ud_json *configuration,
                              ud_json **bound) {
    if (!ctx || !schema || !configuration || !bound)
        return UD_INVALID;
    *bound = NULL;
    ud_bytes out = {.ctx = ctx, .limit = 16 * 1024 * 1024};
    ud_status status = bind(&out, schema, configuration, 0);
    if (!status)
        status = ud_json_parse(ctx, (const char *)out.data, (uint32_t)out.size, bound, NULL);
    ud_bytes_clear(&out);
    return status;
}
