#include "ud/connection_manager.h"
#include "ud/engine.h"
#include "../package/internal.h"
#include "ud/compute.h"
#include "ud/sql.h"
typedef struct cache_component {
    ud_component_context context;
    ud_json *config;
    ud_string asset;
    ud_batch *schema, *reference, *errors;
    ud_cache *cache;
    ud_cache_reader *reader;
    ud_operator *lookup;
    ud_connection_lease *remote;
    ud_string table;
    uint32_t mode;
    ud_id reference_scope;
    uint32_t *mapping, *keys, key_count, row;
    const ud_batch *input;
    uint64_t duplicates_position;
    char (*observations)[65];
    uint32_t observation_count, observation_capacity;
    int build, database, policy, ended, sealed;
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
    int build = (int)(intptr_t)user == 1;
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
        status =
            ud_error_schema(&c->errors, build ? reference : in[0], include, out + (build ? 0 : 1));
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
    ud_operator_destroy(c->lookup);
    ud_cache_reader_destroy(c->reader);
    if (c->remote)
        ud_connection_lease_release(c->remote);
    ud_cache_release(c->cache);
    ud_batch_destroy(c->schema);
    ud_batch_destroy(c->reference);
    ud_batch_destroy(c->errors);
    ud_batch_destroy((ud_batch *)c->input);
    ud_free(c->mapping);
    ud_free(c->keys);
    ud_free(c->observations);
    ud_json_destroy(c->config);
    ud_free(c);
}
static ud_status fill_misses(cache_component *, const ud_batch *);
static ud_status prepare(void *user, const ud_component_context *context, ud_string text,
                         const ud_batch *const *in, const ud_batch *const *out, void **instance) {
    *instance = NULL;
    cache_component *c = ud_alloc(context->memory, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    c->context = *context;
    c->build = (int)(intptr_t)user == 1;
    c->database = (int)(intptr_t)user == 2;
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
        status = ud_asset_mode(context->activation, c->asset, &c->mode, &c->table);
    if (!status && c->build && c->mode != UD_CACHE_FULL)
        status = UD_SCHEMA;
    if (!status && c->database && c->mode != UD_CACHE_PARTIAL)
        status = UD_SCHEMA;
    if (!status && !c->build && !c->database && c->mode != UD_CACHE_FULL)
        status = UD_SCHEMA;
    if (!status && c->database) {
        ud_string connection;
        status = ud_asset_source(context->activation, c->asset, &connection);
        if (!status)
            status = ud_activation_acquire_connection(
                context->activation, connection,
                UD_CONNECT_READ | UD_CONNECT_PREPARED | UD_CONNECT_METADATA |
                    (context->execution->preview_pipeline ? UD_CONNECT_READONLY_SCAN : 0),
                &c->remote);
        ud_batch *actual = NULL;
        if (!status)
            status = ud_connection_lease_discover(c->remote, c->table, &actual);
        const uint32_t *keys = NULL;
        uint32_t count = 0;
        if (!status)
            status = ud_asset_keys(context->activation, c->asset, &keys, &count);
        if (!status && actual->columns != c->reference->columns)
            status = UD_SCHEMA;
        for (uint32_t i = 0; !status && i < actual->columns; i++)
            if (!ud_field_matches(&actual->column[i].field, &c->reference->column[i].field,
                                  UD_FIELD_VALUE_TYPE) ||
                actual->column[i].field.name_length != c->reference->column[i].field.name_length ||
                memcmp(actual->column[i].field.name, c->reference->column[i].field.name,
                       actual->column[i].field.name_length))
                status = UD_SCHEMA;
        for (uint32_t k = 0; !status && k < count; k++)
            if (actual->column[keys[k]].field.key_order != k + 1)
                status = UD_SCHEMA;
        uint32_t primary = 0;
        for (uint32_t i = 0; actual && i < actual->columns; i++)
            primary += actual->column[i].field.key_order != 0;
        if (!status && primary != count)
            status = UD_SCHEMA;
        ud_batch_destroy(actual);
        char scope[33];
        if (!status)
            status =
                ud_connection_field_namespace(context->activation, connection, c->table, scope);
        if (!status)
            status = ud_id_parse((ud_string){scope, 32}, &c->reference_scope);
        if (!status)
            status = ud_asset_partial(context->activation, c->asset, c->remote, &c->cache);
    } else if (!status)
        status = c->build ? ud_asset_build(context->activation, c->asset, &c->cache)
                          : ud_asset_acquire(context->activation, c->asset, &c->cache);
    if (!status && !c->build) {
        if (c->database)
            status = ud_cache_reader_create(c->cache, in[0], c->keys, c->key_count, &c->reader);
        ud_operator_options options = {.struct_size = sizeof(options),
                                       .abi_version = UD_ABI_VERSION,
                                       .kind = UD_OPERATOR_CACHE_LOOKUP,
                                       .max_batch_rows = context->max_batch_rows,
                                       .buffers = context->execution->buffers,
                                       .reference = c->cache,
                                       .reference_schema = c->reference,
                                       .configuration = text};
        if (!status)
            status = ud_asset_keys(context->activation, c->asset, &options.reference_keys,
                                   &options.reference_key_count);
        if (!status)
            status = ud_component_operator_options(context, &options);
        options.record_state = 0;
        if (!status)
            status = ud_operator_create(context->memory, &options, in, &c->lookup);
    }
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
        status = ud_error_append(&c->context.errors, errors,
                                 &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                                    .abi_version = UD_ABI_VERSION,
                                                    .input = duplicates,
                                                    .row = row,
                                                    .rule = (ud_string){"cache.duplicate", 15},
                                                    .rule_version = 1,
                                                    .column = (ud_id){0},
                                                    .code = UD_QUALITY,
                                                    .policy = UD_ERROR_REDIRECT});
    if (!status && errors->rows) {
        ud_task_rows(c->context.activation, 0, 0, errors->rows);
        status = c->context.emit(c->context.emit_user, 0, errors);
    }
    ud_batch_destroy(errors);
    ud_batch_destroy(duplicates);
    return status ? status : UD_MORE;
}
static int keys_equal(const ud_batch *a, uint32_t ar, const uint32_t *ak, const ud_batch *b,
                      uint32_t br, const uint32_t *bk, uint32_t count) {
    for (uint32_t k = 0; k < count; k++) {
        ud_value left, right;
        if (ud_batch_get(a, ar, ak[k], &left) || ud_batch_get(b, br, bk[k], &right) ||
            ud_value_compare(&left, &right))
            return 0;
    }
    return 1;
}
static ud_status parameters(cache_component *c, const ud_sql_statement *statement,
                            const ud_batch *probe, ud_batch **out) {
    *out = NULL;
    uint32_t n = statement->parameter_count;
    ud_field *fields = ud_alloc(c->context.memory, n * sizeof(*fields));
    ud_value *values = ud_alloc(c->context.memory, n * sizeof(*values));
    ud_status status = fields && values ? UD_OK : UD_RESOURCE;
    for (uint32_t i = 0; !status && i < n; i++) {
        ud_sql_parameter p = statement->parameters[i];
        fields[i] = probe->column[p.column].field;
        char name[24];
        int length = snprintf(name, sizeof(name), "p%u", i + 1);
        fields[i].name = ud_strdup_n(c->context.memory, name, (size_t)length);
        fields[i].name_length = (uint32_t)length;
        fields[i].identity = (ud_id){0};
        fields[i].lineage = NULL;
        fields[i].lineage_count = 0;
        fields[i].key_order = 0;
        if (!fields[i].name)
            status = UD_RESOURCE;
        if (!status)
            status = ud_batch_get(probe, p.row, p.column, values + i);
    }
    if (!status)
        status = ud_batch_create(c->context.memory, fields, n, 1, out);
    if (!status)
        status = ud_batch_append(*out, values, n);
    if (!status)
        status = ud_batch_seal(*out);
    for (uint32_t i = 0; fields && i < n; i++)
        ud_free((void *)fields[i].name);
    ud_free(fields);
    ud_free(values);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
static ud_status fill_misses(cache_component *c, const ud_batch *input) {
    if (input->rows > ud_cache_entry_limit(c->cache))
        return UD_RESOURCE;
    const uint32_t *reference_keys = NULL;
    uint32_t key_count = 0;
    UD_TRY(ud_asset_keys(c->context.activation, c->asset, &reference_keys, &key_count));
    ud_field *fields = ud_alloc(c->context.memory, key_count * sizeof(*fields));
    uint32_t *keys = ud_alloc(c->context.memory, key_count * sizeof(*keys));
    ud_status status = fields && keys ? UD_OK : UD_RESOURCE;
    for (uint32_t k = 0; !status && k < key_count; k++) {
        fields[k] = input->column[c->keys[k]].field;
        keys[k] = k;
    }
    ud_batch *misses = NULL;
    if (!status)
        status = ud_batch_create(c->context.memory, fields, key_count, input->rows, &misses);
    uint32_t capacity = 1;
    while (capacity < input->rows * 2)
        capacity *= 2;
    uint32_t *buckets = NULL;
    if (!status)
        status = ud_buffer_working_allocate(c->context.execution->buffers,
                                            capacity * sizeof(*buckets), (void **)&buckets);
    if (!status)
        memset(buckets, 255, capacity * sizeof(*buckets));
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        int32_t cached = 0;
        status = ud_cache_partial_probe(c->reader, input, row, &cached);
        if (status || cached)
            continue;
        uint64_t hash = UD_VALUE_HASH_SEED;
        for (uint32_t k = 0; k < key_count; k++) {
            ud_value value;
            status = ud_batch_get(input, row, c->keys[k], &value);
            if (status)
                break;
            hash = ud_value_hash(&value, hash);
        }
        uint32_t slot = (uint32_t)(hash & (capacity - 1));
        while (!status && buckets[slot] != UINT32_MAX &&
               !keys_equal(input, row, c->keys, misses, buckets[slot], keys, key_count))
            slot = (slot + 1) & (capacity - 1);
        if (!status && buckets[slot] == UINT32_MAX) {
            buckets[slot] = misses->rows;
            status = ud_batch_append_projected(misses, input, row, c->keys);
        }
    }
    /* Readers release their probe pin before cache replacement. */
    ud_cache_reader_destroy(c->reader);
    c->reader = NULL;
    if (!status)
        status = ud_batch_seal(misses);
    for (uint32_t first = 0; !status && first < misses->rows;) {
        uint32_t count = misses->rows - first, limit = 1024 / key_count;
        if (count > limit)
            count = limit;
        ud_sql_statement query = {0};
        ud_batch *params = NULL;
        status = ud_sql_compile_lookup(c->context.memory, ud_connection_lease_dialect(c->remote),
                                       ud_connection_lease_parameter_style(c->remote), c->table,
                                       c->reference, reference_keys, misses, keys, key_count, first,
                                       count, &query);
        if (!status)
            status = parameters(c, &query, misses, &params);
        ud_read_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .batch_size = c->context.max_batch_rows,
                                   .sql = query.sql,
                                   .parameters = params};
        int opened = 0;
        if (!status) {
            status = ud_connection_lease_read_open(c->remote, &options, c->reference);
            opened = !status;
        }
        uint8_t *found = ud_alloc(c->context.memory, count);
        if (!found && !status)
            status = UD_RESOURCE;
        while (!status) {
            ud_batch *batch = NULL;
            status = ud_connection_lease_read_next(c->remote, &batch);
            if (status == UD_END) {
                status = UD_OK;
                break;
            }
            if (status) {
                ud_batch_destroy(batch);
                break;
            }
            ud_batch *identified = NULL;
            status = ud_batch_identify_keys(c->context.memory, batch, c->reference_scope,
                                            reference_keys, key_count, &identified);
            ud_batch_destroy(batch);
            for (uint32_t row = 0; !status && row < identified->rows; row++) {
                uint32_t at = 0;
                for (; at < count; at++)
                    if (keys_equal(misses, first + at, keys, identified, row, reference_keys,
                                   key_count))
                        break;
                if (at == count)
                    continue;
                if (found[at]) {
                    status = UD_QUALITY;
                    break;
                }
                found[at] = 1;
                ud_batch *value = NULL;
                status = ud_batch_create(c->context.memory, c->reference->schema->fields,
                                         c->reference->columns, 1, &value);
                if (!status)
                    status = ud_batch_append_projected(value, identified, row, NULL);
                if (!status)
                    status = ud_batch_seal(value);
                if (!status)
                    status = ud_cache_partial_put(c->cache, misses, first + at, keys, value);
                ud_batch_destroy(value);
            }
            ud_batch_destroy(identified);
        }
        if (opened) {
            ud_status closed = ud_connection_lease_read_close(c->remote);
            if (!status)
                status = closed;
        }
        for (uint32_t at = 0; !status && at < count; at++)
            if (!found[at])
                status = ud_cache_partial_put(c->cache, misses, first + at, keys, NULL);
        ud_free(found);
        ud_batch_destroy(params);
        ud_sql_statement_clear(&query);
        first += count;
    }
    ud_free(buckets);
    ud_free(fields);
    ud_free(keys);
    ud_batch_destroy(misses);
    if (!status)
        status = ud_cache_reader_create(c->cache, input, c->keys, c->key_count, &c->reader);
    return status;
}
static ud_status observe(cache_component *c, const ud_batch *input) {
    if (!c->context.execution->journal)
        return fill_misses(c, input);
    uint8_t digest[32];
    UD_TRY(ud_batch_fingerprint(input, digest));
    ud_bytes identity = {.ctx = c->context.memory};
    ud_status status =
        ud_put_string(&identity, c->context.component_id.data, c->context.component_id.length);
    if (!status)
        status = ud_bytes_put(&identity, digest, 32);
    if (!status)
        ud_hash(identity.data, identity.size, digest);
    ud_bytes_clear(&identity);
    if (status)
        return status;
    char key[65];
    for (uint32_t i = 0; i < 32; i++)
        snprintf(key + i * 2, 3, "%02x", digest[i]);
    ud_batch *state = NULL;
    status = ud_checkpoint_snapshot_read(c->context.activation, (ud_string){key, 64}, 0, &state);
    if (!status) {
        ud_cache_reader_destroy(c->reader);
        c->reader = NULL;
        status = ud_cache_restore(c->cache, state);
        if (!status)
            status = ud_cache_reader_create(c->cache, input, c->keys, c->key_count, &c->reader);
    } else if (status == UD_END) {
        status = fill_misses(c, input);
        if (!status)
            status = ud_cache_checkpoint(c->cache, &state);
        if (!status)
            status =
                ud_checkpoint_snapshot_write(c->context.activation, (ud_string){key, 64}, 0, state);
    }
    ud_batch_destroy(state);
    if (!status) {
        if (c->observation_count == c->observation_capacity) {
            uint32_t cap = c->observation_capacity ? c->observation_capacity * 2 : 16;
            char (*keys)[65] = ud_alloc(c->context.memory, (size_t)cap * 65);
            if (!keys)
                return UD_RESOURCE;
            if (c->observation_count)
                memcpy(keys, c->observations, (size_t)c->observation_count * 65);
            ud_free(c->observations);
            c->observations = keys;
            c->observation_capacity = cap;
        }
        memcpy(c->observations[c->observation_count++], key, 65);
    }
    return status;
}
static ud_status confirm_epoch(void *ptr, uint64_t epoch, ud_string position) {
    (void)epoch;
    (void)position;
    cache_component *c = ptr;
    for (uint32_t i = 0; i < c->observation_count; i++)
        UD_TRY(ud_checkpoint_snapshot_forget(c->context.activation,
                                             (ud_string){c->observations[i], 64}, 0, 1));
    c->observation_count = 0;
    return UD_OK;
}
static ud_status lookup_drain(cache_component *c) {
    ud_batch *batch = NULL;
    uint32_t port = 0;
    ud_status status = ud_operator_pull(c->lookup, &batch, &port);
    if (status == UD_BUSY && c->database && c->input && !ud_operator_input_demand(c->lookup)) {
        status = fill_misses(c, c->input);
        if (!status)
            status = ud_operator_pull(c->lookup, &batch, &port);
    }
    if (c->input && ud_operator_input_demand(c->lookup)) {
        ud_batch_destroy((ud_batch *)c->input);
        c->input = NULL;
    }
    if (status == UD_BUSY)
        return UD_OK;
    if (status)
        return status;
    if (port == 1)
        ud_task_rows(c->context.activation, 0, 0, batch->rows);
    if (c->context.connected_outputs & (UINT64_C(1) << port))
        status = c->context.emit(c->context.emit_user, port, batch);
    ud_batch_destroy(batch);
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
            return ud_operator_finish_input(c->lookup, 0);
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
    if (c->database) {
        UD_TRY(observe(c, event->batch));
        UD_TRY(ud_batch_retain((ud_batch *)event->batch));
        c->input = event->batch;
    }
    UD_TRY(ud_operator_push(c->lookup, 0, event->batch));
    return lookup_drain(c);
}
static ud_status finish(void *ptr) {
    cache_component *c = ptr;
    return c->build && !c->sealed ? UD_SCHEMA : UD_OK;
}
static ud_status checkpoint(void *ptr, ud_batch **out) {
    cache_component *c = ptr;
    if (c->input || (!c->build && !ud_operator_input_demand(c->lookup) && !c->ended))
        return UD_BUSY;
    ud_batch *state = NULL;
    ud_status status = ud_cache_checkpoint(c->cache, &state);
    ud_value value = {0};
    if (!status)
        status = ud_batch_get(state, 0, 0, &value);
    ud_field fields[] = {{.name = "state", .name_length = 5, .type = UD_BINARY},
                         {.name = "duplicatesPosition", .name_length = 18, .type = UD_INT64},
                         {.name = "ended", .name_length = 5, .type = UD_BOOL}};
    if (!status && c->duplicates_position > INT64_MAX)
        status = UD_OVERFLOW;
    ud_value values[] = {value,
                         {.type = UD_INT64, .integer = (int64_t)c->duplicates_position},
                         {.type = UD_BOOL, .integer = c->ended}};
    if (!status)
        status = ud_batch_create(c->context.memory, fields, 3, 1, out);
    if (!status)
        status = ud_batch_append(*out, values, 3);
    if (!status)
        status = ud_batch_seal(*out);
    ud_batch_destroy(state);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
static ud_status restore_state(void *ptr, const ud_batch *state) {
    cache_component *c = ptr;
    if (!state || state->rows != 1 || state->columns != 3)
        return UD_CORRUPT;
    ud_value payload, position, ended;
    UD_TRY(ud_batch_get(state, 0, 0, &payload));
    UD_TRY(ud_batch_get(state, 0, 1, &position));
    UD_TRY(ud_batch_get(state, 0, 2, &ended));
    if (payload.is_null || payload.type != UD_BINARY || position.is_null ||
        position.type != UD_INT64 || position.integer < 0 || ended.is_null || ended.type != UD_BOOL)
        return UD_CORRUPT;
    ud_field field = {.name = "nativeState", .name_length = 11, .type = UD_BINARY};
    ud_batch *cache = NULL;
    ud_status status = ud_batch_create(c->context.memory, &field, 1, 1, &cache);
    if (!status)
        status = ud_batch_append(cache, &payload, 1);
    if (!status)
        status = ud_batch_seal(cache);
    if (!status)
        status = ud_cache_restore(c->cache, cache);
    ud_batch_destroy(cache);
    if (!status) {
        c->duplicates_position = (uint64_t)position.integer;
        c->ended = (int)ended.integer;
        c->sealed = ud_cache_is_sealed(c->cache);
    }
    return status;
}
ud_status ud_cache_components_register(ud_registry *registry) {
    static const char *schemas[] = {
        "{\"type\":\"object\",\"properties\":{\"cache\":{\"type\":\"string\"},\"mapping\":{"
        "\"type\":\"array\",\"items\":{\"type\":\"integer\",\"minimum\":0}}},\"required\":["
        "\"cache\",\"mapping\"],\"additionalProperties\":false}"};
    for (uint32_t i = 0; i < 3; i++) {
        const char *name = i == 2 ? "dbLookup" : i ? "cacheLookup" : "cacheBuild";
        const char *label = i == 2 ? "Database Lookup" : i ? "Lookup Cache" : "Cache Build";
        ud_string declaration = {schemas[0], (uint32_t)strlen(schemas[0])};
        if (i)
            UD_TRY(ud_operator_configuration_schema(UD_OPERATOR_CACHE_LOOKUP, &declaration));
        ud_component_descriptor d = {
            .struct_size = sizeof(d),
            .abi_version = UD_ABI_VERSION,
            .id = {name, (uint32_t)strlen(name)},
            .name = {label, (uint32_t)strlen(label)},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = declaration,
            .category = i ? UD_COMPONENT_TRANSFORM : UD_COMPONENT_DESTINATION,
            .inputs = 1,
            .outputs = i ? 2 : 1,
            .synchronous_input = -1,
            .error_output = i ? 1 : 0,
            .optional_outputs = i ? 2 : 1,
            .capabilities = UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_THREAD_SAFE |
                            UD_COMPONENT_STATEFUL |
                            (i ? UD_COMPONENT_PARTIALLY_BLOCKING : UD_COMPONENT_BLOCKING),
            .user = (void *)(intptr_t)(i == 2 ? 2 : !i),
            .configure = configure,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .dispose = dispose,
            .state_version = 1,
            .checkpoint = checkpoint,
            .restore = restore_state,
            .confirm_epoch = i == 2 ? confirm_epoch : NULL,
            .effects = i == 2 ? UD_EFFECT_EXTERNAL_READ : 0};
        if (i == 2)
            d.capabilities |= UD_COMPONENT_PREVIEW_SAFE;
        UD_TRY(ud_registry_register(registry, &d));
    }
    return UD_OK;
}
