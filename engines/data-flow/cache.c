#include "../package/internal.h"
#include "ud/compute.h"
typedef struct cache_component {
    ud_component_context context;
    ud_json *config;
    ud_string asset;
    ud_batch *schema, *reference, *errors;
    ud_cache *cache;
    ud_cache_reader *reader;
    uint32_t *mapping, *keys, key_count, row;
    const ud_batch *input;
    uint64_t duplicates_position;
    int build, policy, ended, sealed, probing, matched;
} cache_component;
static ud_status parse_indices(ud_context *ctx, const ud_json *array, uint32_t columns,
                               uint32_t **out, uint32_t *count) {
    *count = ud_json_size(array);
    if (ud_json_kind(array) != UD_JSON_ARRAY || !*count || *count > 65536)
        return UD_SCHEMA;
    *out = ud_alloc(ctx, (size_t)*count * sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < *count; i++) {
        uint64_t index = 0;
        UD_TRY(ud_json_uint(ud_json_at(array, i), &index));
        if (index >= columns)
            return UD_SCHEMA;
        (*out)[i] = (uint32_t)index;
    }
    return UD_OK;
}
static ud_status validate(const ud_component_context *c, const ud_json *config,
                          const ud_batch *input, int build, ud_batch **reference,
                          uint32_t **mapping, uint32_t **probe_keys, uint32_t *key_count,
                          int *policy) {
    static const char *build_keys[] = {"cache", "mapping"},
                      *lookup_keys[] = {"cache", "keys", "errorPolicy", "includeInput"};
    UD_TRY(ud_json_keys(config, build ? build_keys : lookup_keys, build ? 2 : 4));
    ud_string id;
    UD_TRY(ud_json_string(ud_member(config, "cache"), &id));
    UD_TRY(ud_asset_schema(c->activation, id, reference, NULL, policy));
    if (build) {
        uint32_t n = 0;
        UD_TRY(parse_indices(c->memory, ud_member(config, "mapping"), input->columns, mapping, &n));
        if (n != (*reference)->columns)
            return UD_SCHEMA;
        for (uint32_t i = 0; i < n; i++)
            if (!ud_field_matches(&input->column[(*mapping)[i]].field,
                                  &(*reference)->column[i].field, UD_FIELD_VALUE_TYPE) ||
                (input->column[(*mapping)[i]].field.nullable &&
                 !(*reference)->column[i].field.nullable))
                return UD_SCHEMA;
    } else {
        UD_TRY(ud_error_policy(config, "errorPolicy", UD_ERROR_IGNORE, policy));
        UD_TRY(parse_indices(c->memory, ud_member(config, "keys"), input->columns, probe_keys,
                             key_count));
        const uint32_t *keys = NULL;
        uint32_t n = 0;
        UD_TRY(ud_asset_keys(c->activation, id, &keys, &n));
        if (n != *key_count)
            return UD_SCHEMA;
        for (uint32_t i = 0; i < n; i++)
            if (!ud_field_matches(&input->column[(*probe_keys)[i]].field,
                                  &(*reference)->column[keys[i]].field, UD_FIELD_VALUE_TYPE))
                return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    int build = (int)(intptr_t)user;
    ud_json *config = NULL;
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, &config, NULL));
    ud_batch *reference = NULL;
    uint32_t *mapping = NULL, *keys = NULL, count = 0;
    int policy = 0;
    ud_status status =
        validate(c, config, in[0], build, &reference, &mapping, &keys, &count, &policy);
    int include = 0;
    const ud_json *flag = ud_member(config, "includeInput");
    if (!status && flag) {
        const char *p = NULL;
        uint32_t n = 0;
        if (ud_json_kind(flag) != UD_JSON_BOOL)
            status = UD_SCHEMA;
        else {
            status = ud_json_text(flag, &p, &n);
            include = n == 4 && !memcmp(p, "true", 4);
        }
    }
    if (!status && !build)
        status = ud_compute_join_schema(c->memory, in[0], reference, out);
    if (!status)
        status = ud_error_schema(c, build ? reference : in[0], include, out + (build ? 0 : 1));
    ud_free(mapping);
    ud_free(keys);
    ud_batch_destroy(reference);
    ud_json_destroy(config);
    return status;
}
static void dispose(void *ptr, ud_status result) {
    (void)result;
    cache_component *c = ptr;
    if (!c)
        return;
    ud_cache_reader_destroy(c->reader);
    ud_cache_release(c->cache);
    ud_batch_destroy(c->schema);
    ud_batch_destroy(c->reference);
    ud_batch_destroy(c->errors);
    ud_batch_destroy((ud_batch *)c->input);
    ud_free(c->mapping);
    ud_free(c->keys);
    ud_json_destroy(c->config);
    ud_free(c);
}
static ud_status prepare(void *user, const ud_component_context *context, ud_string text,
                         const ud_batch *const *in, const ud_batch *const *out, void **instance) {
    *instance = NULL;
    cache_component *c = ud_alloc(context->memory, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    c->context = *context;
    c->build = (int)(intptr_t)user;
    ud_status status = ud_json_parse(context->memory, text.data, text.length, &c->config, NULL);
    if (!status)
        status = ud_json_string(ud_member(c->config, "cache"), &c->asset);
    if (!status)
        status = validate(context, c->config, in[0], c->build, &c->reference, &c->mapping, &c->keys,
                          &c->key_count, &c->policy);
    if (!status)
        status = ud_batch_create(context->memory, out[c->build ? 0 : 1]->schema->fields,
                                 out[c->build ? 0 : 1]->columns, 0, &c->errors);
    if (!status && !c->build)
        status = ud_batch_create(context->memory, out[0]->schema->fields, out[0]->columns, 0,
                                 &c->schema);
    if (!status && c->build && ud_transaction_active(context->activation))
        status = UD_UNSUPPORTED;
    if (!status)
        status = c->build ? ud_asset_build(context->activation, c->asset, &c->cache)
                          : ud_asset_acquire(context->activation, c->asset, &c->cache);
    if (!status && !c->build)
        status = ud_cache_reader_create(c->cache, in[0], c->keys, c->key_count, &c->reader);
    if (status) {
        dispose(c, status);
        return status;
    }
    *instance = c;
    return UD_OK;
}
static ud_status build_drain(cache_component *c) {
    if (c->policy != UD_ERROR_REDIRECT)
        return UD_END;
    ud_batch *duplicates = NULL;
    ud_status status = ud_cache_duplicates(c->cache, &c->duplicates_position, &duplicates);
    if (status)
        return status;
    ud_batch *errors = NULL;
    status = ud_batch_create(c->context.memory, c->errors->schema->fields, c->errors->columns,
                             duplicates->rows, &errors);
    for (uint32_t row = 0; !status && row < duplicates->rows; row++)
        status = ud_error_append(&c->context, errors, duplicates, row,
                                 (ud_string){"cache.duplicate", 15}, 1, (ud_id){0}, UD_QUALITY,
                                 UD_ERROR_REDIRECT);
    if (!status && errors->rows) {
        ud_task_rows(c->context.activation, 0, 0, errors->rows);
        status = c->context.emit(c->context.emit_user, 0, errors);
    }
    ud_batch_destroy(errors);
    ud_batch_destroy(duplicates);
    return status ? status : UD_MORE;
}
static ud_status lookup_drain(cache_component *c) {
    if (!c->input)
        return c->ended ? UD_END : UD_OK;
    ud_batch *output = NULL, *errors = NULL;
    ud_status status = ud_batch_create(c->context.memory, c->schema->schema->fields,
                                       c->schema->columns, c->context.max_batch_rows, &output);
    if (!status)
        status = ud_batch_create(c->context.memory, c->errors->schema->fields, c->errors->columns,
                                 c->context.max_batch_rows, &errors);
    ud_value *values = ud_alloc(c->context.memory, (size_t)c->schema->columns * sizeof(*values));
    if (!status && !values)
        status = UD_RESOURCE;
    while (!status && c->row < c->input->rows && output->rows < c->context.max_batch_rows &&
           errors->rows < c->context.max_batch_rows) {
        if (!c->probing) {
            status = ud_cache_probe(c->reader, c->input, c->row);
            c->probing = !status;
            c->matched = 0;
        }
        const ud_batch *reference = NULL;
        uint32_t row = 0;
        if (!status)
            status = ud_cache_next(c->reader, &reference, &row);
        if (status == UD_END) {
            status = UD_OK;
            if (!c->matched) {
                if (c->policy == UD_ERROR_FAIL)
                    status = UD_QUALITY;
                else if (c->policy == UD_ERROR_REDIRECT) {
                    status = ud_error_append(&c->context, errors, c->input, c->row,
                                             (ud_string){"cache.miss", 10}, 1, (ud_id){0},
                                             UD_QUALITY, c->policy);
                    if (!status)
                        ud_task_rows(c->context.activation, 0, 0, 1);
                }
                /* Ignore drops a miss and records the native miss counter. */
            }
            c->row++;
            c->probing = 0;
            continue;
        }
        for (uint32_t i = 0; !status && i < c->input->columns; i++)
            status = ud_batch_get(c->input, c->row, i, values + i);
        for (uint32_t i = 0; !status && i < reference->columns; i++)
            status = ud_batch_get(reference, row, i, values + c->input->columns + i);
        if (!status)
            status = ud_batch_append(output, values, output->columns);
        if (!status)
            status =
                ud_batch_join_row_info(output, output->rows - 1, c->input, c->row, reference, row);
        c->matched = 1;
    }
    if (!status && output->rows)
        status = c->context.emit(c->context.emit_user, 0, output);
    if (!status && errors->rows)
        status = c->context.emit(c->context.emit_user, 1, errors);
    ud_free(values);
    ud_batch_destroy(output);
    ud_batch_destroy(errors);
    if (c->row == c->input->rows) {
        ud_batch_destroy((ud_batch *)c->input);
        c->input = NULL;
        c->row = 0;
    }
    return status ? status : UD_MORE;
}
static ud_status step(void *ptr, const ud_component_event *event) {
    cache_component *c = ptr;
    UD_TRY(ud_activation_check(c->context.activation));
    if (event->kind == UD_COMPONENT_POLL)
        return c->build ? build_drain(c) : lookup_drain(c);
    if (event->kind == UD_COMPONENT_INPUT_END) {
        c->ended = 1;
        if (!c->build)
            return UD_OK;
        UD_TRY(ud_cache_seal(c->cache));
        c->sealed = 1;
        UD_TRY(ud_asset_publish(c->context.activation, c->asset, c->cache));
        return c->policy == UD_ERROR_REDIRECT ? UD_MORE : UD_OK;
    }
    if (c->build) {
        ud_batch *projected = NULL;
        ud_status status =
            ud_batch_project(c->context.memory, event->batch, c->mapping,
                             c->reference->schema->fields, c->reference->columns, &projected);
        if (!status)
            status = ud_cache_append(c->cache, projected);
        ud_batch_destroy(projected);
        return status;
    }
    UD_TRY(ud_batch_retain((ud_batch *)event->batch));
    c->input = event->batch;
    return lookup_drain(c);
}
static ud_status finish(void *ptr) {
    cache_component *c = ptr;
    return c->build && !c->sealed ? UD_SCHEMA : UD_OK;
}
ud_status ud_cache_components_register(ud_registry *registry) {
    static const char *schemas[] = {
        "{\"type\":\"object\",\"properties\":{\"cache\":{\"type\":\"string\"},\"mapping\":{"
        "\"type\":\"array\",\"items\":{\"type\":\"integer\",\"minimum\":0}}},\"required\":["
        "\"cache\",\"mapping\"],\"additionalProperties\":false}",
        "{\"type\":\"object\",\"properties\":{\"cache\":{\"type\":\"string\"},\"keys\":{\"type\":"
        "\"array\",\"items\":{\"type\":\"integer\",\"minimum\":0}},\"errorPolicy\":{\"enum\":["
        "\"fail\",\"redirect\",\"ignore\"]},\"includeInput\":{\"type\":\"boolean\"}},\"required\":["
        "\"cache\",\"keys\"],\"additionalProperties\":false}"};
    for (uint32_t i = 0; i < 2; i++) {
        const char *name = i ? "cacheLookup" : "cacheBuild";
        ud_component_descriptor d = {
            .struct_size = sizeof(d),
            .abi_version = UD_ABI_VERSION,
            .id = {name, (uint32_t)strlen(name)},
            .name = {name, (uint32_t)strlen(name)},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {schemas[i], (uint32_t)strlen(schemas[i])},
            .category = i ? UD_COMPONENT_TRANSFORM : UD_COMPONENT_DESTINATION,
            .inputs = 1,
            .outputs = i ? 2 : 1,
            .synchronous_input = -1,
            .error_output = i ? 1 : 0,
            .optional_outputs = i ? 2 : 1,
            .capabilities = UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_THREAD_SAFE |
                            UD_COMPONENT_STATEFUL |
                            (i ? UD_COMPONENT_PARTIALLY_BLOCKING : UD_COMPONENT_BLOCKING),
            .user = (void *)(intptr_t)!i,
            .configure = configure,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .dispose = dispose};
        UD_TRY(ud_registry_register(registry, &d));
    }
    return UD_OK;
}
