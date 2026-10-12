#include "../internal.h"
#include <ctype.h>

static const ud_json *member(const ud_json *v, const char *name) {
    return ud_json_find(v, name, (uint32_t)strlen(name));
}
static ud_status string(const ud_json *v, ud_string *out) {
    return ud_json_kind(v) == UD_JSON_STRING ? ud_json_text(v, &out->data, &out->length)
                                             : UD_SCHEMA;
}
static int named(ud_string name, const char *text) {
    return name.length == strlen(text) && !memcmp(name.data, text, name.length);
}
static int index_of(const ud_plan *p, ud_string name) {
    for (uint32_t i = 0; i < p->n; i++)
        if (p->fields[i].name_length == name.length &&
            !memcmp(p->fields[i].name, name.data, name.length))
            return (int)i;
    return -1;
}
static ud_status list(ud_plan *p, const ud_json *array, uint32_t **out, uint32_t *count) {
    if (!array)
        return UD_OK;
    if (ud_json_kind(array) != UD_JSON_ARRAY || !ud_json_size(array) || ud_json_size(array) > p->n)
        return UD_SCHEMA;
    *count = ud_json_size(array);
    *out = ud_alloc(p->ctx, *count * sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < *count; i++) {
        ud_string name;
        UD_TRY(string(ud_json_at(array, i), &name));
        int at = index_of(p, name);
        if (at < 0)
            return UD_SCHEMA;
        (*out)[i] = (uint32_t)at;
        for (uint32_t j = 0; j < i; j++)
            if ((*out)[j] == (uint32_t)at)
                return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status bind(ud_plan *p, const ud_expression_options *bindings) {
    static const char *keys[] = {"version", "filter", "projection", "group", "aggregates"};
    if (ud_json_kind(p->definition) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < ud_json_size(p->definition); i++) {
        const char *key;
        uint32_t length;
        int known = 0;
        UD_TRY(ud_json_key(p->definition, i, &key, &length));
        for (uint32_t j = 0; j < 5; j++)
            if (length == strlen(keys[j]) && !memcmp(key, keys[j], length))
                known = 1;
        if (!known)
            return UD_SCHEMA;
    }
    ud_string version;
    UD_TRY(ud_json_text(member(p->definition, "version"), &version.data, &version.length));
    if (!named(version, "4"))
        return UD_INCOMPATIBLE;
    UD_TRY(list(p, member(p->definition, "projection"), &p->projection, &p->project_n));
    UD_TRY(list(p, member(p->definition, "group"), &p->keys, &p->key_n));
    const ud_json *aggregates = member(p->definition, "aggregates");
    if (aggregates && (ud_json_kind(aggregates) != UD_JSON_ARRAY || !ud_json_size(aggregates) ||
                       ud_json_size(aggregates) > 65536 - p->key_n))
        return UD_SCHEMA;
    p->agg_n = ud_json_size(aggregates);
    if (p->key_n && !p->agg_n)
        return UD_SCHEMA;
    if (p->agg_n && p->project_n)
        return UD_SCHEMA;
    if (p->agg_n) {
        p->aggs = ud_alloc(p->ctx, p->agg_n * sizeof(*p->aggs));
        if (!p->aggs)
            return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < p->agg_n; i++) {
        const ud_json *def = ud_json_at(aggregates, i);
        ud_string function, column, name;
        if (ud_json_kind(def) != UD_JSON_OBJECT || ud_json_size(def) != 3)
            return UD_SCHEMA;
        UD_TRY(string(member(def, "function"), &function));
        UD_TRY(string(member(def, "column"), &column));
        UD_TRY(string(member(def, "name"), &name));
        if (!name.length || memchr(name.data, 0, name.length))
            return UD_SCHEMA;
        ud_aggregate_def *a = p->aggs + i;
        a->kind = named(function, "count") ? 0
                  : named(function, "sum") ? 1
                  : named(function, "min") ? 2
                  : named(function, "max") ? 3
                                           : -1;
        if (a->kind < 0)
            return UD_UNSUPPORTED;
        int at = index_of(p, column);
        if (named(column, "*") && a->kind == 0)
            a->column = UINT32_MAX;
        else if (at < 0)
            return UD_SCHEMA;
        else
            a->column = (uint32_t)at;
        if (a->kind == 1 && p->fields[a->column].type != UD_INT64 &&
            p->fields[a->column].type != UD_FLOAT64 && p->fields[a->column].type != UD_DECIMAL128)
            return UD_SCHEMA;
        a->name = ud_strdup_n(p->ctx, name.data, name.length);
        if (!a->name)
            return UD_RESOURCE;
    }
    const ud_json *filter = member(p->definition, "filter");
    if (filter) {
        char *json = NULL;
        uint32_t length = 0;
        UD_TRY(ud_json_serialize(filter, &json, &length));
        ud_expression_options options = {.struct_size = sizeof(options),
                                         .abi_version = UD_ABI_VERSION};
        if (bindings)
            options = *bindings;
        options.schema = p->schema_owner;
        ud_status s =
            ud_expression_create(p->ctx, (ud_string){json, length}, &options, &p->predicate);
        ud_free(json);
        if (s)
            return s;
        if (ud_expression_type(p->predicate) != UD_BOOL)
            return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status result_schema(ud_plan *p) {
    uint32_t count = p->agg_n ? p->key_n + p->agg_n : p->project_n ? p->project_n : p->n;
    ud_field *fields = ud_alloc(p->ctx, count * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    char fingerprint[65];
    for (uint32_t i = 0; i < 32; i++)
        snprintf(fingerprint + 2 * i, 3, "%02x", p->fingerprint[i]);
    for (uint32_t i = 0; !s && i < count; i++) {
        if (!p->agg_n || i < p->key_n) {
            uint32_t at = p->agg_n ? p->keys[i] : p->project_n ? p->projection[i] : i;
            s = ud_field_copy(p->ctx, p->fields + at, (ud_column_id){0}, fields + i);
        } else {
            ud_aggregate_def *a = p->aggs + i - p->key_n;
            ud_field field = a->kind ? p->fields[a->column] : (ud_field){.type = UD_INT64};
            field.nullable = a->kind != 0;
            if (!a->kind && a->column != UINT32_MAX) {
                field.lineage = p->fields[a->column].lineage;
                field.lineage_count = p->fields[a->column].lineage_count;
            }
            s = ud_field_derive(p->ctx, &field, (ud_string){fingerprint, 64},
                                (ud_string){a->name, (uint32_t)strlen(a->name)}, fields + i);
        }
    }
    if (!s)
        s = ud_batch_create(p->ctx, fields, count, 0, &p->output_schema);
    for (uint32_t i = 0; i < count; i++)
        ud_field_clear(fields + i);
    ud_free(fields);
    return s;
}
ud_status ud_plan_create_fields(ud_context *ctx, uint32_t version, const ud_field *fields,
                                uint32_t columns, ud_string definition,
                                const ud_expression_options *bindings, ud_plan **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (version != UD_PLAN_VERSION)
        return UD_INCOMPATIBLE;
    if (!ctx || !fields || !columns || !definition.data)
        return UD_INVALID;
    ud_plan *p = ud_alloc(ctx, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->ctx = ctx;
    atomic_init(&p->references, 1);
    ud_status s = ud_batch_create(ctx, fields, columns, 0, &p->schema_owner);
    if (!s) {
        p->n = columns;
        p->fields = ud_alloc(ctx, columns * sizeof(*p->fields));
        if (!p->fields)
            s = UD_RESOURCE;
        else
            for (uint32_t i = 0; i < columns; i++)
                ud_batch_field(p->schema_owner, i, p->fields + i);
    }
    if (!s)
        s = ud_json_parse(ctx, definition.data, definition.length, &p->definition, NULL);
    if (!s)
        s = bind(p, bindings);
    ud_bytes bytes = {.ctx = ctx};
    char *canonical = NULL;
    uint32_t length = 0;
    if (!s)
        s = ud_json_serialize(p->definition, &canonical, &length);
    if (!s)
        s = ud_encode_batch(&bytes, p->schema_owner);
    if (!s)
        s = ud_put_string(&bytes, canonical, length);
    if (!s)
        ud_hash(bytes.data, bytes.size, p->fingerprint);
    ud_free(canonical);
    ud_bytes_clear(&bytes);
    if (!s)
        s = result_schema(p);
    if (s) {
        ud_plan_destroy(p);
        return s;
    }
    *out = p;
    return UD_OK;
}
ud_status ud_plan_retain(ud_plan *p) {
    if (!p)
        return UD_INVALID;
    unsigned count = atomic_load(&p->references);
    do {
        if (!count || count == UINT32_MAX)
            return UD_RESOURCE;
    } while (!atomic_compare_exchange_weak(&p->references, &count, count + 1));
    return UD_OK;
}
void ud_plan_destroy(ud_plan *p) {
    if (!p || atomic_fetch_sub(&p->references, 1) != 1)
        return;
    ud_expression_destroy(p->predicate);
    for (uint32_t i = 0; p->aggs && i < p->agg_n; i++)
        ud_free(p->aggs[i].name);
    ud_free(p->fields);
    ud_free(p->projection);
    ud_free(p->keys);
    ud_free(p->aggs);
    ud_batch_destroy(p->output_schema);
    ud_batch_destroy(p->schema_owner);
    ud_json_destroy(p->definition);
    ud_free(p);
}
ud_status ud_plan_accept(const ud_plan *p, const ud_batch *batch, uint32_t row, int *accept) {
    if (!p || !batch || !accept)
        return UD_INVALID;
    if (ud_is_cancelled(p->ctx))
        return UD_CANCELLED;
    *accept = 1;
    if (p->predicate) {
        ud_value value;
        UD_TRY(ud_expression_evaluate(p->predicate, batch, row, &value));
        *accept = !value.is_null && value.integer;
    }
    return UD_OK;
}