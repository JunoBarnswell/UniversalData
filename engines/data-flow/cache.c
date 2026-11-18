#include "ud/connection_manager.h"
#include "ud/engine.h"
#include "../package/internal.h"
#include "ud/compute.h"
#include "ud/sql.h"
#include "../../components/data/internal.h"
typedef struct cache_component {
    ud_component_context context;
    ud_json *config;
    ud_string asset;
    ud_batch *reference, *errors, *query_schema, *observation_schema;
    ud_cache *cache;
    ud_operator *lookup;
    ud_connection_lease *remote;
    ud_string table;
    uint32_t mode;
    uint64_t batch_index, confirmed_index, epoch;
    uint64_t sql_requests, sql_probes, replayed_batches;
    ud_id reference_scope;
    uint32_t *mapping, *keys, key_count;
    uint64_t duplicates_position;
    int build, database, policy, ended, sealed, epoch_scope, snapshot_open;
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
                      *lookup_keys[] = {"cache", "keys", "errorPolicy", "includeInput", "noMatch"};
    UD_TRY(ud_json_keys(config, build ? build_keys : lookup_keys, build ? 2 : 5));
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
    if (!status && !build)
        status = ud_batch_create(c->memory, in[0]->schema->fields, in[0]->columns, 0, out + 2);
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
    if (c->snapshot_open)
        ud_connection_lease_read_snapshot(c->remote, 0);
    if (c->remote)
        ud_connection_lease_release(c->remote);
    ud_cache_release(c->cache);
    ud_batch_destroy(c->query_schema);
    ud_batch_destroy(c->observation_schema);
    ud_batch_destroy(c->reference);
    ud_batch_destroy(c->errors);
    ud_free(c->mapping);
    ud_free(c->keys);
    ud_json_destroy(c->config);
    ud_free(c);
}
static ud_status database_schemas(cache_component *c) {
    uint32_t columns = c->reference->columns;
    ud_field *fields = ud_alloc(c->context.memory, (columns + 1) * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    memcpy(fields, c->reference->schema->fields, columns * sizeof(*fields));
    char ordinal[48];
    ud_sql_lookup_ordinal_name(c->reference, ordinal);
    fields[columns] = (ud_field){.name = ordinal, .name_length = (uint32_t)strlen(ordinal),
                                 .type = UD_INT64};
    ud_status status = ud_batch_create(c->context.memory, fields, columns + 1, 0,
                                      &c->query_schema);
    for (uint32_t i = 0; i < columns; i++) {
        fields[i].nullable = 1;
        fields[i].key_order = 0;
    }
    fields[columns].type = UD_BOOL;
    if (!status)
        status = ud_batch_create(c->context.memory, fields, columns + 1, 0,
                                 &c->observation_schema);
    ud_free(fields);
    return status;
}
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
    if (!status && c->build && ud_transaction_active(context->activation))
        status = UD_UNSUPPORTED;
    if (!status)
        status = ud_asset_mode(context->activation, c->asset, &c->mode, &c->table, &c->epoch_scope);
    if (!status && c->build && c->mode != UD_CACHE_FULL)
        status = UD_SCHEMA;
    if (!status && c->database && c->mode != UD_CACHE_PARTIAL && c->mode != UD_CACHE_NONE)
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
                    (context->execution->preview_pipeline ? UD_CONNECT_READONLY_SCAN : 0) |
                    (c->epoch_scope ? UD_CONNECT_READ_SNAPSHOT : 0),
                &c->remote);
        char scope[33];
        if (!status)
            status =
                ud_connection_field_namespace(context->activation, connection, c->table, scope);
        if (!status)
            status = ud_id_parse((ud_string){scope, 32}, &c->reference_scope);
        ud_batch *actual = NULL;
        if (!status)
            status = ud_connection_lease_schema(c->remote, c->table, (ud_string){scope, 32},
                                                &actual);
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
        if (!status)
            status = database_schemas(c);
        if (!status && c->mode == UD_CACHE_PARTIAL && !c->epoch_scope)
            status = ud_asset_partial(context->activation, c->asset, c->remote, &c->cache);
    } else if (!status)
        status = c->build ? ud_asset_build(context->activation, c->asset, &c->cache)
                          : ud_asset_acquire(context->activation, c->asset, &c->cache);
    if (!status && !c->build) {
        ud_operator_options options = {.struct_size = sizeof(options),
                                       .abi_version = UD_ABI_VERSION,
                                       .kind = UD_OPERATOR_CACHE_LOOKUP,
                                       .max_batch_rows = context->max_batch_rows,
                                       .buffers = context->execution->buffers,
                                       .reference = c->database ? NULL : c->cache,
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
        fields[i].physical_identity = (ud_id){0};
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
/* The SQL ordinal maps a row to the original probe, independent of collation. */
static ud_status query_matches(cache_component *c, const ud_batch *probes, const uint32_t *keys,
                               uint32_t first, uint32_t count, ud_batch *matches,
                               uint32_t *locations, uint32_t *projection) {
    const uint32_t *reference_keys;
    uint32_t key_count;
    UD_TRY(ud_asset_keys(c->context.activation, c->asset, &reference_keys, &key_count));
    ud_sql_statement query = {0};
    ud_status status = ud_sql_compile_lookup(
        c->context.memory, ud_connection_lease_dialect(c->remote),
        ud_connection_lease_parameter_style(c->remote), c->table, c->reference, reference_keys,
        probes, keys, key_count, first, count, &query);
    ud_batch *params = NULL;
    if (!status)
        status = parameters(c, &query, probes, &params);
    ud_read_options options = {.struct_size = sizeof(options),
                               .abi_version = UD_ABI_VERSION,
                               .batch_size = c->context.max_batch_rows,
                               .sql = query.sql,
                               .parameters = params};
    if (!status)
        status = ud_connection_lease_read_open(c->remote, &options, c->query_schema);
    if (!status) {
        c->sql_requests++;
        c->sql_probes += count;
        while (!status) {
            ud_batch *batch = NULL;
            status = ud_connection_lease_read_next(c->remote, &batch);
            if (status == UD_END) {
                status = UD_OK;
                break;
            }
            ud_batch *identified = NULL;
            if (!status)
                status = ud_batch_identify_keys(c->context.memory, batch, c->reference_scope,
                                                reference_keys, key_count, &identified);
            for (uint32_t row = 0; !status && row < identified->rows; row++) {
                ud_value ordinal;
                status = ud_batch_get(identified, row, c->reference->columns, &ordinal);
                if (!status && (ordinal.is_null || ordinal.type != UD_INT64 ||
                                ordinal.integer < 0 || (uint64_t)ordinal.integer >= count))
                    status = UD_SCHEMA;
                if (!status) {
                    uint32_t at = first + (uint32_t)ordinal.integer;
                    if (locations[at])
                        status = UD_QUALITY;
                    else {
                        status = ud_batch_append_projected(matches, identified, row, projection);
                        locations[at] = matches->rows;
                    }
                }
            }
            ud_batch_destroy(identified);
            ud_batch_destroy(batch);
        }
        ud_status closed = ud_connection_lease_read_close(c->remote);
        if (!status)
            status = closed;
    }
    ud_batch_destroy(params);
    ud_sql_statement_clear(&query);
    return status;
}

static ud_status partial_matches(cache_component *c, const ud_batch *input, ud_batch *matches,
                                  uint32_t *location, uint32_t *projection) {
    ud_context *ctx = c->context.memory;
    ud_field fields[16];
    uint32_t keys[16];
    for (uint32_t k = 0; k < c->key_count; k++) {
        fields[k] = input->column[c->keys[k]].field;
        keys[k] = k;
    }
    ud_batch *misses = NULL;
    ud_status status = ud_batch_create(ctx, fields, c->key_count, input->rows, &misses);
    uint32_t *uncached = ud_alloc(ctx, (size_t)input->rows * sizeof(*uncached));
    uint32_t *miss_location = ud_alloc(ctx, (size_t)input->rows * sizeof(*miss_location));
    uint32_t slots = 1;
    while (slots <= input->rows)
        slots <<= 1;
    uint32_t *buckets = ud_alloc(ctx, slots * sizeof(*buckets));
    ud_value *probe_values = ud_alloc(ctx, c->key_count * sizeof(*probe_values));
    if (!uncached || !miss_location || !buckets || !probe_values)
        status = UD_RESOURCE;
    ud_cache_reader *reader = NULL;
    if (!status)
        status = ud_cache_reader_create(c->cache, input, c->keys, c->key_count, &reader);
    /* Snapshot scope owns expiry at the epoch boundary. */
    ud_cache_reader_begin(reader, c->epoch_scope);
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        uncached[row] = UINT32_MAX;
        int32_t cached;
        status = ud_cache_partial_probe(reader, input, row, &cached);
        if (status)
            break;
        if (!cached) {
            uint64_t hash = UD_VALUE_HASH_SEED;
            for (uint32_t k = 0; !status && k < c->key_count; k++) {
                status = ud_batch_get(input, row, c->keys[k], probe_values + k);
                if (!status)
                    hash = ud_value_hash(probe_values + k, hash);
            }
            uint32_t slot = (uint32_t)hash & (slots - 1);
            while (!status && buckets[slot]) {
                uint32_t k = 0;
                for (; !status && k < c->key_count; k++) {
                    ud_value prior;
                    status = ud_batch_get(misses, buckets[slot] - 1, k, &prior);
                    if (!status && ud_value_compare(probe_values + k, &prior))
                        break;
                }
                if (k == c->key_count)
                    break;
                slot = (slot + 1) & (slots - 1);
            }
            if (!status && !buckets[slot]) {
                status = ud_batch_append(misses, probe_values, c->key_count);
                buckets[slot] = misses->rows;
            }
            uncached[row] = buckets[slot] - 1;
            continue;
        }
        const ud_batch *value = NULL;
        uint32_t at;
        status = ud_cache_next(reader, &value, &at);
        if (status == UD_END)
            status = UD_OK;
        else if (!status) {
            status = ud_batch_append_projected(matches, value, at, projection);
            location[row] = matches->rows;
        }
    }
    ud_cache_reader_destroy(reader);
    if (!status)
        status = ud_batch_seal(misses);
    for (uint32_t first = 0; !status && first < misses->rows;) {
        uint32_t count = misses->rows - first, limit = 1024 / c->key_count;
        if (count > limit)
            count = limit;
        status = query_matches(c, misses, keys, first, count, matches, miss_location, projection);
        first += count;
    }
    if (!status)
        status = ud_batch_seal(matches);
    for (uint32_t row = 0; !status && row < input->rows; row++)
        if (uncached[row] != UINT32_MAX)
            location[row] = miss_location[uncached[row]];
    for (uint32_t row = 0; !status && row < misses->rows; row++) {
        ud_batch *value = NULL;
        if (miss_location[row]) {
            status = ud_batch_create(ctx, c->reference->schema->fields,
                                     c->reference->columns, 1, &value);
            if (!status)
                status = ud_batch_append_projected(value, matches, miss_location[row] - 1,
                                                   projection);
            if (!status)
                status = ud_batch_seal(value);
        }
        if (!status)
            status = ud_cache_partial_put(c->cache, misses, row, keys, value);
        ud_batch_destroy(value);
    }
    ud_free(probe_values);
    ud_free(buckets);
    ud_free(miss_location);
    ud_free(uncached);
    ud_batch_destroy(misses);
    return status;
}

static ud_status database_observations(cache_component *c, const ud_batch *input, ud_batch **out) {
    *out = NULL;
    ud_context *ctx = c->context.memory;
    uint32_t columns = c->reference->columns;
    uint32_t *location = ud_alloc(ctx, (size_t)input->rows * sizeof(*location) + 1);
    uint32_t *projection = ud_alloc(ctx, columns * sizeof(*projection));
    ud_value *values = ud_alloc(ctx, (columns + 1) * sizeof(*values));
    ud_batch *matches = NULL;
    ud_status status = location && projection && values ? UD_OK : UD_RESOURCE;
    for (uint32_t col = 0; !status && col < columns; col++)
        projection[col] = col;
    if (!status)
        status = ud_batch_create(ctx, c->reference->schema->fields, columns, input->rows, &matches);
    if (!status && input->rows && c->mode == UD_CACHE_PARTIAL)
        status = partial_matches(c, input, matches, location, projection);
    if (!status && c->mode == UD_CACHE_NONE)
        for (uint32_t row = 0; !status && row < input->rows; row++)
            status = query_matches(c, input, c->keys, row, 1, matches, location, projection);
    if (!status)
        status = ud_batch_create(ctx, c->observation_schema->schema->fields, columns + 1,
                                 input->rows, out);
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        uint32_t at = location[row];
        for (uint32_t col = 0; !status && col < columns; col++) {
            if (at)
                status = ud_batch_get(matches, at - 1, col, values + col);
            else
                values[col] = (ud_value){.type = c->reference->column[col].field.type, .is_null = 1};
        }
        values[columns] = (ud_value){.type = UD_BOOL, .integer = at != 0};
        if (!status)
            status = ud_batch_append(*out, values, columns + 1);
        if (!status && at)
            status = ud_batch_copy_row_info(*out, row, matches, at - 1);
    }
    if (!status)
        status = ud_batch_seal(*out);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    ud_batch_destroy(matches);
    ud_free(location);
    ud_free(projection);
    ud_free(values);
    return status;
}

static ud_status observe(cache_component *c, const ud_batch *input, uint64_t epoch, ud_batch **out) {
    if (c->epoch_scope && c->epoch != epoch) {
        if (c->snapshot_open)
            return UD_FENCED;
        ud_cache_release(c->cache);
        c->cache = NULL;
        c->epoch = epoch;
    }
    ud_string key = c->context.component_id;
    ud_status status = ud_checkpoint_snapshot_read(c->context.activation, key, c->batch_index, out);
    if (status == UD_END) {
        if (c->epoch_scope && !c->snapshot_open) {
            UD_TRY(ud_connection_lease_read_snapshot(c->remote, 1));
            c->snapshot_open = 1;
        }
        if (c->mode == UD_CACHE_PARTIAL && !c->cache)
            UD_TRY(ud_asset_partial(c->context.activation, c->asset, c->remote, &c->cache));
        status = database_observations(c, input, out);
        if (!status)
            status = ud_checkpoint_snapshot_write(c->context.activation, key, c->batch_index, *out);
    } else if (!status) {
        c->replayed_batches++;
    }
    if (!status)
        c->batch_index++;
    return status;
}
static ud_status confirm_epoch(void *ptr, uint64_t epoch, ud_string position) {
    (void)epoch;
    (void)position;
    cache_component *c = ptr;
    if (c->snapshot_open) {
        UD_TRY(ud_connection_lease_read_snapshot(c->remote, 0));
        c->snapshot_open = 0;
    }
    if (c->context.execution->journal)
        while (c->confirmed_index < c->batch_index) {
            UD_TRY(ud_checkpoint_snapshot_forget(c->context.activation, c->context.component_id,
                                                 c->confirmed_index, 1));
            c->confirmed_index++;
        }
    return UD_OK;
}
static ud_status lookup_drain(cache_component *c) {
    ud_batch *batch = NULL;
    uint32_t port = 0;
    ud_status status = ud_operator_pull(c->lookup, &batch, &port);
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
    ud_batch *observations = NULL;
    ud_status status = c->database ? observe(c, event->batch, event->epoch, &observations) : UD_OK;
    if (!status)
        status = ud_operator_push(c->lookup, 0, event->batch);
    if (!status && c->database)
        status = ud_operator_lookup_observe(c->lookup, observations);
    ud_batch_destroy(observations);
    UD_TRY(status);
    return lookup_drain(c);
}
static ud_status finish(void *ptr) {
    cache_component *c = ptr;
    return c->build && !c->sealed ? UD_SCHEMA : UD_OK;
}
static ud_status diagnostics(void *ptr, char **out, uint32_t *length) {
    cache_component *c = ptr;
    char json[160];
    int n = snprintf(json, sizeof(json),
                     "{\"sqlRequests\":%llu,\"sqlProbes\":%llu,\"replayedBatches\":%llu}",
                     (unsigned long long)c->sql_requests, (unsigned long long)c->sql_probes,
                     (unsigned long long)c->replayed_batches);
    *out = ud_strdup_n(c->context.memory, json, (size_t)n);
    *length = (uint32_t)n;
    return *out ? UD_OK : UD_RESOURCE;
}
static ud_status checkpoint(void *ptr, ud_batch **out) {
    cache_component *c = ptr;
    if (!c->build && !ud_operator_input_demand(c->lookup) && !c->ended)
        return UD_BUSY;
    ud_batch *state = NULL;
    ud_status status = c->database ? UD_OK : ud_cache_checkpoint(c->cache, &state);
    ud_value value = {.type = UD_BINARY};
    if (!status && state)
        status = ud_batch_get(state, 0, 0, &value);
    ud_field fields[] = {{.name = "state", .name_length = 5, .type = UD_BINARY},
                         {.name = c->database ? "batchIndex" : "duplicatesPosition",
                          .name_length = c->database ? 10 : 18, .type = UD_INT64},
                         {.name = "ended", .name_length = 5, .type = UD_BOOL}};
    uint64_t position = c->database ? c->batch_index : c->duplicates_position;
    if (!status && position > INT64_MAX)
        status = UD_OVERFLOW;
    ud_value values[] = {value,
                         {.type = UD_INT64, .integer = (int64_t)position},
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
    if (c->database) {
        c->batch_index = (uint64_t)position.integer;
        c->confirmed_index = c->batch_index;
        c->ended = (int)ended.integer;
        return UD_OK;
    }
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
            .outputs = i ? 3 : 1,
            .synchronous_input = -1,
            .error_output = i ? 1 : 0,
            .optional_outputs = i ? 7 : 1,
            .capabilities = UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_THREAD_SAFE |
                            UD_COMPONENT_STATEFUL |
                            (i ? UD_COMPONENT_PARTIALLY_BLOCKING | UD_COMPONENT_STREAM_SAFE
                               : UD_COMPONENT_BLOCKING),
            .user = (void *)(intptr_t)(i == 2 ? 2 : !i),
            .configure = configure,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .dispose = dispose,
            .state_version = i == 2 ? 2 : 1,
            .checkpoint = checkpoint,
            .restore = restore_state,
            .confirm_epoch = i == 2 ? confirm_epoch : NULL,
            .diagnostics = i == 2 ? diagnostics : NULL,
            .effects = i == 2 ? UD_EFFECT_EXTERNAL_READ : 0};
        if (i == 2)
            d.capabilities |= UD_COMPONENT_PREVIEW_SAFE;
        UD_TRY(ud_registry_register(registry, &d));
    }
    return UD_OK;
}
