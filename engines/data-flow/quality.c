#include "../package/internal.h"
#include "ud/quality.h"
typedef struct quality_component {
    ud_component_context context;
    ud_json *config;
    ud_quality *quality;
    ud_batch *schema, *errors, *pending_input;
    ud_quality_violation *violations;
    uint32_t violation_count, violation_at;
    int reference_ready, ended;
    ud_buffer_block **facts;
    uint32_t fact_count, fact_at, fact_capacity;
} quality_component;
static int column(const ud_batch *schema, ud_string name) {
    ud_id id = {0};
    int is_id = ud_id_parse(name, &id) == UD_OK;
    for (uint32_t i = 0; i < schema->columns; i++) {
        const ud_field *f = &schema->column[i].field;
        if ((is_id && !ud_id_compare(f->identity, id)) ||
            (name.length == f->name_length && !memcmp(name.data, f->name, name.length)))
            return (int)i;
    }
    return -1;
}
static ud_status indices(ud_context *ctx, const ud_json *list, const ud_batch *schema,
                         uint32_t **out, uint32_t *count) {
    *out = NULL;
    *count = ud_json_size(list);
    if (!schema || ud_json_kind(list) != UD_JSON_ARRAY || !*count || *count > schema->columns)
        return UD_SCHEMA;
    *out = ud_alloc(ctx, (size_t)*count * sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    for (uint32_t j = 0; j < *count; j++) {
        ud_string name;
        UD_TRY(ud_json_string(ud_json_at(list, j), &name));
        int at = column(schema, name);
        if (at < 0)
            return UD_SCHEMA;
        (*out)[j] = (uint32_t)at;
    }
    return UD_OK;
}
static ud_status check(void *user) {
    return ud_activation_check(user);
}
static ud_status bind(const ud_component_context *c, const ud_json *config,
                      const ud_batch *const *in, ud_quality **out) {
    static const char *keys[] = {"rules", "errorPolicy", "includeInput"};
    UD_TRY(ud_json_keys(config, keys, 3));
    const ud_json *rules = ud_member(config, "rules");
    uint32_t count = ud_json_size(rules);
    if (ud_json_kind(rules) != UD_JSON_ARRAY || !count || count > 1024)
        return UD_SCHEMA;
    ud_quality_rule *bound = ud_alloc(c->memory, (size_t)count * sizeof(*bound));
    if (!bound)
        return UD_RESOURCE;
    int policy = UD_ERROR_FAIL;
    ud_status status = ud_error_policy(config, "errorPolicy", UD_ERROR_FAIL, &policy);
    for (uint32_t i = 0; !status && i < count; i++) {
        const ud_json *rule = ud_json_at(rules, i);
        static const char *rk[] = {
            "id",      "version", "kind",    "columns",      "referenceColumns",
            "minimum", "maximum", "pattern", "expectedRows", "policy"};
        status = ud_json_keys(rule, rk, 10);
        ud_string id = {0}, kind = {0};
        uint64_t version = 1;
        if (!status)
            status = ud_json_string(ud_member(rule, "id"), &id);
        if (!status)
            status = ud_json_string(ud_member(rule, "kind"), &kind);
        if (!status && ud_member(rule, "version"))
            status = ud_json_uint(ud_member(rule, "version"), &version);
        if (!status && (!version || version > UINT32_MAX))
            status = UD_SCHEMA;
        ud_quality_rule *r = bound + i;
        *r = (ud_quality_rule){.struct_size = sizeof(*r),
                               .abi_version = UD_ABI_VERSION,
                               .rule_version = (uint32_t)version,
                               .policy = UD_ERROR_INHERIT,
                               .id = id.data,
                               .id_length = id.length,
                               .minimum = {.is_null = 1},
                               .maximum = {.is_null = 1}};
        if (!status)
            status = ud_error_policy(rule, "policy", UD_ERROR_INHERIT, &r->policy);
        r->kind = ud_named(kind, "notNull")      ? UD_RULE_NOT_NULL
                  : ud_named(kind, "unique")     ? UD_RULE_UNIQUE
                  : ud_named(kind, "primaryKey") ? UD_RULE_PRIMARY_KEY
                  : ud_named(kind, "foreignKey") ? UD_RULE_FOREIGN_KEY
                  : ud_named(kind, "range")      ? UD_RULE_RANGE
                  : ud_named(kind, "regex")      ? UD_RULE_REGEX
                  : ud_named(kind, "rowCount")   ? UD_RULE_ROW_COUNT
                                                 : 0;
        if (!r->kind)
            status = UD_UNSUPPORTED;
        if (!status && r->kind != UD_RULE_ROW_COUNT)
            status = indices(c->memory, ud_member(rule, "columns"), in[0], (uint32_t **)&r->columns,
                             &r->column_count);
        if (!status && r->kind == UD_RULE_FOREIGN_KEY) {
            uint32_t n = 0;
            r->reference = in[1];
            status = indices(c->memory, ud_member(rule, "referenceColumns"), in[1],
                             (uint32_t **)&r->reference_columns, &n);
            if (!status && n != r->column_count)
                status = UD_SCHEMA;
        }
        if (!status && r->kind == UD_RULE_RANGE) {
            if (r->column_count != 1)
                status = UD_SCHEMA;
            if (!status && ud_member(rule, "minimum"))
                status =
                    ud_json_value_decode(c->memory, &in[0]->column[r->columns[0]].field,
                                         ud_member(rule, "minimum"), UD_ROUND_EXACT, &r->minimum);
            if (!status && ud_member(rule, "maximum"))
                status =
                    ud_json_value_decode(c->memory, &in[0]->column[r->columns[0]].field,
                                         ud_member(rule, "maximum"), UD_ROUND_EXACT, &r->maximum);
        }
        if (!status && r->kind == UD_RULE_REGEX) {
            ud_string p;
            status = ud_json_string(ud_member(rule, "pattern"), &p);
            r->pattern = p.data;
            r->pattern_length = p.length;
        }
        if (!status && r->kind == UD_RULE_ROW_COUNT)
            status = ud_json_uint(ud_member(rule, "expectedRows"), &r->expected_rows);
    }
    ud_quality_options options = {.struct_size = sizeof(options),
                                  .abi_version = UD_ABI_VERSION,
                                  .max_batch_rows = c->max_batch_rows,
                                  .max_violations = c->max_batch_rows * count,
                                  .policy = policy,
                                  .buffers = c->execution->buffers,
                                  .check = check,
                                  .user = c->activation};
    if (!status)
        status = ud_quality_create(c->memory, in[0]->schema->fields, in[0]->columns, bound, count,
                                   &options, out);
    for (uint32_t i = 0; i < count; i++) {
        ud_free((void *)bound[i].columns);
        ud_free((void *)bound[i].reference_columns);
        ud_clear_value(&bound[i].minimum);
        ud_clear_value(&bound[i].maximum);
    }
    ud_free(bound);
    return status;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    (void)user;
    ud_json *config = NULL;
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, &config, NULL));
    ud_quality *quality = NULL;
    ud_status status = bind(c, config, in, &quality);
    int include = 0;
    if (!status && ud_member(config, "includeInput")) {
        const ud_json *value = ud_member(config, "includeInput");
        const char *p = NULL;
        uint32_t n = 0;
        if (ud_json_kind(value) != UD_JSON_BOOL)
            status = UD_SCHEMA;
        else {
            status = ud_json_text(value, &p, &n);
            include = n == 4 && !memcmp(p, "true", 4);
        }
    }
    if (!status)
        status = ud_quality_schema(quality, out);
    if (!status)
        status = ud_error_schema(&c->errors, in[0], include, out + 1);
    ud_quality_destroy(quality);
    ud_json_destroy(config);
    return status;
}
static void dispose(void *ptr, ud_status result) {
    (void)result;
    quality_component *q = ptr;
    if (!q)
        return;
    ud_quality_destroy(q->quality);
    ud_batch_destroy(q->schema);
    ud_batch_destroy(q->errors);
    ud_batch_destroy(q->pending_input);
    ud_free(q->violations);
    for (uint32_t i = q->fact_at; i < q->fact_count; i++)
        ud_buffer_release(q->facts[i]);
    ud_free(q->facts);
    ud_json_destroy(q->config);
    ud_free(q);
}
static ud_status prepare(void *user, const ud_component_context *c, ud_string text,
                         const ud_batch *const *in, const ud_batch *const *out, void **instance) {
    (void)user;
    *instance = NULL;
    quality_component *q = ud_alloc(c->memory, sizeof(*q));
    if (!q)
        return UD_RESOURCE;
    q->context = *c;
    ud_status status = ud_json_parse(c->memory, text.data, text.length, &q->config, NULL);
    if (!status)
        status = bind(c, q->config, in, &q->quality);
    if (!status)
        status = ud_batch_create(c->memory, out[0]->schema->fields, out[0]->columns, 0, &q->schema);
    if (!status)
        status = ud_batch_create(c->memory, out[1]->schema->fields, out[1]->columns, 0, &q->errors);
    q->reference_ready = !in[1];
    if (!status && q->reference_ready)
        status = ud_quality_reference_end(q->quality);
    if (status) {
        dispose(q, status);
        return status;
    }
    *instance = q;
    return UD_OK;
}
static ud_status classify(quality_component *q, const ud_batch *input) {
    ud_batch *accepted = NULL, *rejected = NULL;
    ud_status status = ud_quality_apply(q->quality, input, &accepted, &rejected, &q->violations,
                                        &q->violation_count);
    if (!status && rejected->rows)
        ud_task_rows(q->context.activation, 0, 0, rejected->rows);
    if (!status && accepted->rows)
        status = q->context.emit(q->context.emit_user, 0, accepted);
    if (!status && q->violation_count)
        status = ud_batch_retain((ud_batch *)input);
    if (!status && q->violation_count)
        q->pending_input = (ud_batch *)input;
    ud_batch_destroy(accepted);
    ud_batch_destroy(rejected);
    return status;
}
static ud_status drain(quality_component *q) {
    if (q->pending_input) {
        ud_batch *errors = NULL;
        UD_TRY(ud_batch_create(q->context.memory, q->errors->schema->fields, q->errors->columns,
                               q->context.max_batch_rows, &errors));
        ud_status status = UD_OK;
        while (!status && q->violation_at < q->violation_count &&
               errors->rows < q->context.max_batch_rows) {
            const ud_quality_violation *v = q->violations + q->violation_at++;
            if (v->policy != UD_ERROR_REDIRECT)
                continue;
            const ud_json *rule = ud_json_at(ud_member(q->config, "rules"), v->rule_index);
            ud_string id;
            uint64_t version = 1;
            status = ud_json_string(ud_member(rule, "id"), &id);
            if (!status && ud_member(rule, "version"))
                status = ud_json_uint(ud_member(rule, "version"), &version);
            if (!status)
                status = ud_error_append(&q->context.errors, errors, q->pending_input, v->input_row, id,
                                         (uint32_t)version, v->column_id, UD_QUALITY, v->policy);
        }
        if (!status && errors->rows)
            status = q->context.emit(q->context.emit_user, 1, errors);
        ud_batch_destroy(errors);
        if (q->violation_at == q->violation_count) {
            ud_batch_destroy(q->pending_input);
            q->pending_input = NULL;
            ud_free(q->violations);
            q->violations = NULL;
            q->violation_at = q->violation_count = 0;
        }
        if (status)
            return status;
        return UD_MORE;
    }
    if (q->reference_ready && q->fact_at < q->fact_count) {
        ud_buffer_block *block = q->facts[q->fact_at++];
        ud_buffer_view *view = NULL;
        ud_status status = ud_buffer_read(block, &view);
        if (!status)
            status = classify(q, ud_buffer_view_batch(view));
        ud_buffer_view_release(view);
        ud_status closed = ud_buffer_release(block);
        return status ? status : closed ? closed : UD_MORE;
    }
    if (q->ended == 3)
        return ud_quality_finish(q->quality) == UD_OK ? UD_END : UD_QUALITY;
    return UD_OK;
}
static ud_status step(void *ptr, const ud_component_event *event) {
    quality_component *q = ptr;
    UD_TRY(ud_activation_check(q->context.activation));
    if (event->kind == UD_COMPONENT_POLL)
        return drain(q);
    if (event->kind == UD_COMPONENT_INPUT_END) {
        if (event->input == 1) {
            UD_TRY(ud_quality_reference_end(q->quality));
            q->reference_ready = 1;
            q->ended |= 2;
            return q->fact_at < q->fact_count ? UD_MORE : UD_OK;
        }
        q->ended |= 1;
        if (q->reference_ready)
            q->ended |= 2;
        return UD_OK;
    }
    if (event->input == 1)
        return ud_quality_reference_push(q->quality, event->batch);
    if (q->reference_ready) {
        UD_TRY(classify(q, event->batch));
        return q->pending_input ? UD_MORE : UD_OK;
    }
    if (q->fact_count == q->fact_capacity) {
        uint32_t cap = q->fact_capacity ? q->fact_capacity * 2 : 32;
        if (cap < q->fact_capacity)
            return UD_RESOURCE;
        ud_buffer_block **items = ud_alloc(q->context.memory, (size_t)cap * sizeof(*items));
        if (!items)
            return UD_RESOURCE;
        if (q->fact_count)
            memcpy(items, q->facts, (size_t)q->fact_count * sizeof(*items));
        ud_free(q->facts);
        q->facts = items;
        q->fact_capacity = cap;
    }
    UD_TRY(ud_buffer_store(q->context.execution->buffers, event->batch, q->facts + q->fact_count));
    q->fact_count++;
    return UD_OK;
}
static ud_status finish(void *ptr) {
    return ud_quality_finish(((quality_component *)ptr)->quality);
}
static ud_status diagnostics(void *ptr, char **out, uint32_t *length) {
    quality_component *q = ptr;
    ud_bytes json = {.ctx = q->context.memory};
    ud_quality_metrics metrics;
    ud_quality_get_metrics(q->quality, &metrics);
    char text[256];
    int n = snprintf(text, sizeof(text),
                     "{\"processed\":%llu,\"accepted\":%llu,\"rejected\":%llu,\"violations\":%llu,"
                     "\"ignored\":%llu,\"rules\":[",
                     (unsigned long long)metrics.processed, (unsigned long long)metrics.accepted,
                     (unsigned long long)metrics.rejected, (unsigned long long)metrics.violations,
                     (unsigned long long)metrics.ignored);
    ud_status status = ud_bytes_put(&json, text, (size_t)n);
    uint32_t count = ud_json_size(ud_member(q->config, "rules"));
    for (uint32_t i = 0; !status && i < count; i++) {
        ud_quality_rule_metrics m;
        status = ud_quality_rule_statistics(q->quality, i, &m);
        const ud_json *rule = ud_json_at(ud_member(q->config, "rules"), i);
        ud_string id = {0};
        uint64_t version = 1, committed = 0, ack = 0, unknown = 0;
        ud_id token = {0};
        if (!status)
            status = ud_json_string(ud_member(rule, "id"), &id);
        if (!status && ud_member(rule, "version"))
            status = ud_json_uint(ud_member(rule, "version"), &version);
        if (!status)
            status = ud_rule_token(&q->context, id, (uint32_t)version, &token);
        if (!status)
            status = ud_rule_statistics(q->context.execution, token, &committed, &ack, &unknown);
        if (!status && i)
            status = ud_bytes_put(&json, ",", 1);
        if (!status)
            status = ud_bytes_put(&json, "{\"id\":", 6);
        if (!status)
            status = ud_json_quote(&json, id.data, id.length);
        char detail[640];
        n = snprintf(detail, sizeof(detail),
                     ",\"version\":%llu,\"evaluated\":%llu,\"failed\":%llu,\"rejected\":%llu,"
                     "\"ignored\":%llu,\"committed\":%llu,\"acknowledged\":%llu,\"unknown\":%llu,"
                     "\"deliveryUnit\":\"errorRecordsAcrossDefinition\",\"referenceLoaded\":%llu,"
                     "\"referenceHits\":%llu,\"referenceMisses\":%llu,\"referencePages\":%llu}",
                     (unsigned long long)version, (unsigned long long)m.evaluated,
                     (unsigned long long)m.failed, (unsigned long long)m.rejected,
                     (unsigned long long)m.ignored, (unsigned long long)committed,
                     (unsigned long long)ack, (unsigned long long)unknown,
                     (unsigned long long)m.reference_loaded, (unsigned long long)m.reference_hits,
                     (unsigned long long)m.reference_misses, (unsigned long long)m.reference_pages);
        if (!status)
            status = ud_bytes_put(&json, detail, (size_t)n);
    }
    if (!status)
        status = ud_bytes_put(&json, "]}", 2);
    if (!status) {
        *out = ud_strdup_n(q->context.memory, (const char *)json.data, json.size);
        *length = (uint32_t)json.size;
        if (!*out)
            status = UD_RESOURCE;
    }
    ud_bytes_clear(&json);
    return status;
}
ud_status ud_quality_component_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"properties\":{\"rules\":{\"type\":\"array\",\"items\":{\"type\":"
        "\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"version\":{\"type\":\"integer\","
        "\"minimum\":1},\"kind\":{\"enum\":[\"notNull\",\"unique\",\"primaryKey\",\"foreignKey\","
        "\"range\",\"regex\",\"rowCount\"]},\"columns\":{\"type\":\"array\",\"items\":{\"type\":"
        "\"string\"}},\"referenceColumns\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"
        "\"minimum\":{},\"maximum\":{},\"pattern\":{\"type\":\"string\"},\"expectedRows\":{"
        "\"type\":\"integer\",\"minimum\":0},\"policy\":{\"enum\":[\"fail\",\"redirect\","
        "\"ignore\"]}},\"required\":[\"id\",\"kind\"],\"additionalProperties\":false}},"
        "\"errorPolicy\":{\"enum\":[\"fail\",\"redirect\",\"ignore\"]},\"includeInput\":{\"type\":"
        "\"boolean\"}},\"required\":[\"rules\"],\"additionalProperties\":false}";
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = {"quality", 7},
                                 .name = {"Data Quality", 12},
                                 .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
                                 .configuration_schema = {schema, sizeof(schema) - 1},
                                 .category = UD_COMPONENT_TRANSFORM,
                                 .inputs = 2,
                                 .outputs = 2,
                                 .optional_inputs = 2,
                                 .optional_outputs = 2,
                                 .synchronous_input = -1,
                                 .error_output = 1,
                                 .capabilities = UD_COMPONENT_REPLAY_SAFE |
                                                 UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_STATEFUL |
                                                 UD_COMPONENT_PARTIALLY_BLOCKING,
                                 .configure = configure,
                                 .prepare = prepare,
                                 .step = step,
                                 .finish = finish,
                                 .dispose = dispose,
                                 .diagnostics = diagnostics};
    return ud_registry_register(registry, &d);
}
