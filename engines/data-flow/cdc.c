#include "ud/connection_manager.h"
#include "ud/engine.h"
#include "../package/internal.h"
#include "ud/cdc.h"
#include "ud/format.h"
/* A single transactional Source adapter for actual vendor log callbacks.
 * Uncommitted rows stay in the existing bounded Buffer until COMMIT. */
typedef struct cdc_commit {
    uint32_t blocks;
    int64_t watermark;
    uint64_t version;
    ud_string schema_position;
} cdc_commit;

typedef struct cdc_source {
    ud_component_context context;
    ud_json *config;
    ud_connection_lease *lease;
    ud_batch *input, *actual, *pending;
    const ud_batch *output;
    ud_value *values, *before;
    char *schema_policy;
    uint32_t schema_policy_length;
    int schema_pending, direct_rows, append_only;
    ud_buffer_block **blocks;
    uint32_t count, capacity, at, output_at;
    ud_buffer_view *output_view;
    uint64_t epoch, transactions, row, schema_version, limit;
    uint64_t micro_batch_rows, micro_batch_ms, pending_rows, batch_started_ms;
    cdc_commit *cuts;
    uint32_t cut_count, cut_capacity, cut_at;
    ud_string position, transaction;
    int64_t watermark;
    int begun, committed, barrier, ended;

    char intent[65], policy[65];
} cdc_source;
static void bind_decoder(cdc_source *s) {
    s->direct_rows = s->actual && s->actual->columns == s->input->columns;
    for (uint32_t i = 0; s->direct_rows && i < s->input->columns; i++)
        s->direct_rows = ud_field_matches(&s->actual->column[i].field,
                                           &s->input->column[i].field, UD_FIELD_PORT);
}
static ud_status schema(const ud_component_context *c, const ud_json *config, ud_batch **input,
                        ud_batch **out) {
    char *json = NULL;
    uint32_t n;
    UD_TRY(ud_json_serialize(ud_member(config, "schema"), &json, &n));
    ud_string connection = {0}, table = {0};
    char scope[33];
    ud_status s = ud_json_string(ud_member(config, "connection"), &connection);
    if (!s)
        s = ud_json_string(ud_member(config, "table"), &table);
    if (!s)
        s = ud_connection_field_namespace(c->activation, connection, table, scope);
    if (!s)
        s = ud_schema_from_json(c->memory, json, n, (ud_string){scope, 32}, input);
    ud_free(json);
    ud_field *fields = !s ? ud_alloc(c->memory, ((*input)->columns + 1) * sizeof(*fields)) : NULL;
    if (!s && !fields)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < (*input)->columns; i++)
        fields[i] = (*input)->column[i].field;
    if (!s) {
        ud_field before = {.name = "before",
                           .name_length = 6,
                           .type = UD_STRUCT,
                           .nullable = 1,
                           .children = (*input)->schema->fields,
                           .child_count = (*input)->columns};
        s = ud_field_derive(c->memory, &before, (ud_string){scope, 32}, (ud_string){"before", 6},
                            fields + (*input)->columns);
        if (!s)
            s = ud_batch_create(c->memory, fields, (*input)->columns + 1, 0, out);
        ud_field_clear(fields + (*input)->columns);
    }
    ud_free(fields);
    return s;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    (void)in;
    ud_json *config = NULL;
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, &config, NULL));
    ud_batch *input = NULL;
    ud_status s = schema(c, config, &input, out);
    ud_batch_destroy(input);
    ud_json_destroy(config);
    return s;
}
static void dispose(void *instance, ud_status status) {
    (void)status;
    cdc_source *s = instance;
    if (!s)
        return;
    if (s->lease)
        ud_connection_lease_cdc_close(s->lease);
    ud_buffer_view_release(s->output_view);
    for (uint32_t i = s->at; i < s->count; i++)
        ud_buffer_release(s->blocks[i]);
    for (uint32_t i = 0; i < s->cut_count; i++)
        ud_free((void *)s->cuts[i].schema_position.data);
    ud_free(s->cuts);
    ud_free(s->blocks);
    ud_connection_lease_release(s->lease);
    ud_batch_destroy(s->input);
    ud_batch_destroy(s->actual);
    ud_batch_destroy(s->pending);
    ud_free(s->values);
    ud_free(s->before);
    ud_free(s->schema_policy);
    ud_free((void *)s->position.data);
    ud_free((void *)s->transaction.data);
    ud_json_destroy(s->config);
    ud_free(s);
}
static ud_status intent_key(cdc_source *s) {
    ud_bytes bytes = {.ctx = s->context.memory};
    ud_status status =
        ud_put_string(&bytes, s->context.pipeline_id.data, s->context.pipeline_id.length);
    if (!status)
        status =
            ud_put_string(&bytes, s->context.component_id.data, s->context.component_id.length);
    if (!status)
        status = ud_put_u64(&bytes, s->epoch + 1);
    uint8_t hash[32];
    if (!status) {
        ud_hash(bytes.data, bytes.size, hash);
        for (uint32_t i = 0; i < 32; i++)
            snprintf(s->intent + 2 * i, 3, "%02x", hash[i]);
    }
    ud_bytes_clear(&bytes);
    char *config = NULL;
    uint32_t n;
    if (!status)
        status = ud_json_serialize(s->config, &config, &n);
    if (!status) {
        ud_hash(config, n, hash);
        for (uint32_t i = 0; i < 32; i++)
            snprintf(s->policy + 2 * i, 3, "%02x", hash[i]);
    }
    ud_free(config);
    return status;
}
static ud_status save_intent(cdc_source *s) {
    if (!s->context.execution->journal)
        return UD_UNSUPPORTED;
    UD_TRY(intent_key(s));
    for (uint32_t i = 0; i < s->count; i++) {
        ud_buffer_view *view = NULL;
        UD_TRY(ud_buffer_read(s->blocks[i], &view));
        ud_status status = ud_checkpoint_snapshot_write(
            s->context.activation, (ud_string){s->intent, 64}, i, ud_buffer_view_batch(view));
        ud_buffer_view_release(view);
        if (status)
            return status;
    }
    ud_bytes payload = {.ctx = s->context.memory};
    char start[192];
    int n = snprintf(
        start, sizeof(start),
        "{\"policy\":\"%s\",\"batches\":%u,\"epoch\":%llu,\"schemaVersion\":%llu,\"position\":",
        s->policy, s->count, (unsigned long long)(s->epoch + 1),
        (unsigned long long)s->schema_version);
    ud_status status = ud_bytes_put(&payload, start, (size_t)n);
    if (!status)
        status = ud_json_quote(&payload, s->position.data, s->position.length);
    char *schema_text = NULL;
    uint32_t schema_length = 0;
    if (!status)
        status = ud_schema_to_json(s->actual ? s->actual : s->input, &schema_text, &schema_length);
    if (!status)
        status = ud_bytes_put(&payload, ",\"decoderSchema\":", 17);
    if (!status)
        status = ud_bytes_put(&payload, schema_text, schema_length);
    if (!status)
        status = ud_bytes_put(&payload, ",\"cuts\":[", 9);
    for (uint32_t i = 0; !status && i < s->cut_count; i++) {
        const cdc_commit *cut = s->cuts + i;
        n = snprintf(start, sizeof(start), "%s[%u,%lld,%llu,", i ? "," : "",
                     cut->blocks, (long long)cut->watermark, (unsigned long long)cut->version);
        status = ud_bytes_put(&payload, start, (size_t)n);
        if (!status)
            status = ud_json_quote(&payload, cut->schema_position.data ? cut->schema_position.data : "",
                                   cut->schema_position.length);
        if (!status)
            status = ud_bytes_put(&payload, "]", 1);
    }
    if (!status)
        status = ud_bytes_put(&payload, "]}", 2);
    ud_free(schema_text);
    if (!status)
        status =
            ud_checkpoint_snapshot_seal(s->context.activation, (ud_string){s->intent, 64},
                                        (ud_string){(char *)payload.data, (uint32_t)payload.size});
    ud_bytes_clear(&payload);
    return status;
}
static ud_status load_intent(cdc_source *s, ud_cdc_options *options) {
    UD_TRY(intent_key(s));
    char *text = NULL;
    uint32_t n;
    ud_status status = ud_checkpoint_snapshot_info(
        s->context.activation, (ud_string){s->intent, 64}, (ud_string){s->policy, 64}, &text, &n);
    if (status == UD_END)
        return UD_OK;
    if (status)
        return status;
    ud_json *entry = NULL;
    status = ud_json_parse(s->context.memory, text, n, &entry, NULL);
    ud_free(text);
    uint64_t count = 0, epoch = 0;
    if (!status)
        status = ud_json_uint(ud_member(entry, "batches"), &count);
    if (!status)
        status = ud_json_uint(ud_member(entry, "epoch"), &epoch);
    if (!status && epoch != s->epoch + 1)
        status = UD_FENCED;
    ud_string position = {0};
    if (!status)
        status = ud_json_string(ud_member(entry, "position"), &position);
    if (!status)
        status = ud_json_uint(ud_member(entry, "schemaVersion"), &s->schema_version);
    ud_buffer_options limits;
    if (!status)
        status = ud_buffer_manager_limits(s->context.execution->buffers, &limits);
    if (!status && (count > limits.max_blocks || count > UINT32_MAX))
        status = UD_RESOURCE;
    if (!status) {
        ud_free((void *)s->position.data);
        s->position = (ud_string){ud_strdup_n(s->context.memory, position.data, position.length),
                                  position.length};
        if (!s->position.data)
            status = UD_RESOURCE;
        s->blocks = ud_alloc(s->context.memory, ((size_t)count + 1) * sizeof(*s->blocks));
        if (!s->blocks)
            status = UD_RESOURCE;
        s->capacity = (uint32_t)count;
    }
    for (uint32_t i = 0; !status && i < count; i++) {
        ud_batch *b = NULL;
        status =
            ud_checkpoint_snapshot_read(s->context.activation, (ud_string){s->intent, 64}, i, &b);
        if (!status)
            status = ud_buffer_store(s->context.execution->buffers, b, s->blocks + i);
        ud_batch_destroy(b);
        if (!status)
            s->count++;
    }
    if (!status) {
        char *schema_text = NULL;
        uint32_t length = 0;
        status = ud_json_serialize(ud_member(entry, "decoderSchema"), &schema_text, &length);
        if (!status)
            status = ud_schema_from_json(s->context.memory, schema_text, length, (ud_string){0},
                                         &s->actual);
        ud_free(schema_text);
        const ud_json *cuts = ud_member(entry, "cuts");
        uint32_t length_cuts = cuts ? ud_json_size(cuts) : 1;
        if (!status && (!length_cuts || length_cuts > limits.max_blocks))
            status = UD_CORRUPT;
        if (!status) {
            s->cuts = ud_alloc(s->context.memory, length_cuts * sizeof(*s->cuts));
            if (!s->cuts)
                status = UD_RESOURCE;
        }
        for (uint32_t i = 0; !status && i < length_cuts; i++) {
            const ud_json *cut = ud_json_at(cuts, i);
            uint64_t blocks = count, watermark = 0, version = s->schema_version;
            ud_string change = {0};
            if (cuts) {
                status = ud_json_uint(ud_json_at(cut, 0), &blocks);
                if (!status) status = ud_json_uint(ud_json_at(cut, 1), &watermark);
                if (!status) status = ud_json_uint(ud_json_at(cut, 2), &version);
                const ud_json *schema_position = ud_json_at(cut, 3);
                if (!status && ud_json_kind(schema_position) != UD_JSON_STRING)
                    status = UD_CORRUPT;
                if (!status)
                    status = ud_json_text(schema_position, &change.data, &change.length);
            } else {
                const char *flag;
                uint32_t size;
                status = ud_json_text(ud_member(entry, "schemaPending"), &flag, &size);
                if (!status && size == 4 && !memcmp(flag, "true", 4))
                    change = position;
            }
            if (!status && (watermark > INT64_MAX || blocks > count ||
                    (i && blocks < s->cuts[i - 1].blocks) || (i + 1 == length_cuts && blocks != count)))
                status = UD_CORRUPT;
            if (!status) {
                cdc_commit *dest = s->cuts + s->cut_count++;
                *dest = (cdc_commit){(uint32_t)blocks, (int64_t)watermark, version};
                if (change.length) {
                    dest->schema_position = (ud_string){ud_strdup_n(s->context.memory, change.data,
                                                 change.length), change.length};
                    if (!dest->schema_position.data)
                        status = UD_RESOURCE;
                }
            }
        }
        s->cut_capacity = s->cut_count;
        if (!status)
            options->schema = s->actual;
    }
    if (!status) {
        options->position = s->position;
        options->mode = UD_CDC_STREAM;
        s->committed = 1;
        s->transactions = s->cut_count;
    }
    ud_json_destroy(entry);
    return status;
}
static ud_status prepare(void *user, const ud_component_context *c, ud_string text,
                         const ud_batch *const *in, const ud_batch *const *out, void **instance) {
    (void)in;
    *instance = NULL;
    if (!c->execution->journal)
        return UD_SCHEMA;
    cdc_source *s = ud_alloc(c->memory, sizeof(*s));
    if (!s)
        return UD_RESOURCE;
    s->context = *c;
    s->output = out[0];
    s->epoch = c->recovery_epoch;
    ud_status result = ud_json_parse(c->memory, text.data, text.length, &s->config, NULL);
    const ud_json *append_only = !result ? ud_member(s->config, "appendOnly") : NULL;
    if (append_only) {
        ud_string value;
        result = ud_json_text(append_only, &value.data, &value.length);
        if (!result)
            s->append_only = ud_named(value, "true");
    }
    ud_string connection = {0};
    if (!result)
        result = ud_json_string(ud_member(s->config, "connection"), &connection);
    if (!result)
        result = ud_batch_create(c->memory, s->output->schema->fields,
                                 s->output->columns - 1, 0, &s->input);
    if (!result)
        result = ud_activation_acquire_connection(
            c->activation, connection, UD_CONNECT_READ | UD_CONNECT_CDC | UD_CONNECT_NATIVE_CDC,
            &s->lease);
    int32_t expected = (int32_t)(intptr_t)user;
    if (!result && ud_connection_lease_dialect(s->lease) != expected)
        result = UD_INCOMPATIBLE;
    ud_cdc_options options = {.struct_size = sizeof(options),
                              .abi_version = UD_ABI_VERSION,
                              .batch_rows = c->max_batch_rows,
                              .mode = UD_CDC_SNAPSHOT_STREAM,
                              .schema = s->input};
    if (!result && ud_member(s->config, "microBatchRows"))
        result = ud_json_uint(ud_member(s->config, "microBatchRows"), &s->micro_batch_rows);
    if (!result && ud_member(s->config, "microBatchMs"))
        result = ud_json_uint(ud_member(s->config, "microBatchMs"), &s->micro_batch_ms);
    if (!result && ((!s->micro_batch_rows && ud_member(s->config, "microBatchRows")) ||
                    (!s->micro_batch_ms && ud_member(s->config, "microBatchMs"))))
        result = UD_SCHEMA;
    if (!result)
        result = ud_json_string(ud_member(s->config, "table"), &options.table);
    ud_string mode = {0};
    if (!result && ud_member(s->config, "mode"))
        result = ud_json_string(ud_member(s->config, "mode"), &mode);
    if (ud_named(mode, "stream"))
        options.mode = UD_CDC_STREAM;
    const char *names[] = {"position", "slot", "publication"};
    ud_string *values[] = {&options.position, &options.slot, &options.publication};
    for (uint32_t i = 0; !result && i < 3; i++)
        if (ud_member(s->config, names[i]))
            result = ud_json_string(ud_member(s->config, names[i]), values[i]);
    uint64_t server = 0;
    if (!result && ud_member(s->config, "serverId"))
        result = ud_json_uint(ud_member(s->config, "serverId"), &server);
    if (server > UINT32_MAX)
        result = UD_SCHEMA;
    options.server_id = (uint32_t)server;
    ud_bytes owner = {.ctx = c->memory};
    uint8_t owner_hash[32];
    char owner_text[65];
    if (!result)
        result = ud_put_string(&owner, c->execution->options.execution_id.data,
                               c->execution->options.execution_id.length);
    if (!result)
        result = ud_put_string(&owner, c->execution->options.tenant.data,
                               c->execution->options.tenant.length);
    ud_string resource = {0};
    if (!result)
        result = ud_connection_lease_resource(s->lease, &resource);
    if (!result)
        result = ud_put_string(&owner, resource.data, resource.length);
    if (!result)
        result = ud_put_string(&owner, options.table.data, options.table.length);
    if (!result) {
        ud_hash(owner.data, owner.size, owner_hash);
        for (uint32_t i = 0; i < 32; i++)
            snprintf(owner_text + 2 * i, 3, "%02x", owner_hash[i]);
        options.source_owner = (ud_string){owner_text, 64};
    }
    ud_bytes_clear(&owner);
    if (!result && ud_member(s->config, "maxTransactions"))
        result = ud_json_uint(ud_member(s->config, "maxTransactions"), &s->limit);
    if (c->recovery_position.length) {
        options.position = c->recovery_position;
        options.mode = UD_CDC_STREAM;
        s->position =
            (ud_string){ud_strdup_n(c->memory, options.position.data, options.position.length),
                        options.position.length};
        if (!s->position.data)
            result = UD_RESOURCE;
    }
    if (!result && !c->recovery_terminal)
        result = load_intent(s, &options);
    char namespace_text[33];
    if (!result)
        result =
            ud_connection_field_namespace(c->activation, connection, options.table, namespace_text);
    if (!result)
        options.field_namespace = (ud_string){namespace_text, 32};
    if (!result && ud_member(s->config, "schemaPolicy"))
        result = ud_json_serialize(ud_member(s->config, "schemaPolicy"), &s->schema_policy,
                                   &s->schema_policy_length);
    else if (!result) {
        const char *policy = "{\"policy\":\"strict\"}";
        s->schema_policy_length = (uint32_t)strlen(policy);
        s->schema_policy = ud_strdup_n(c->memory, policy, s->schema_policy_length);
        if (!s->schema_policy)
            result = UD_RESOURCE;
    }
    if (!result) {
        ud_json *policy = NULL;
        result = ud_json_parse(c->memory, s->schema_policy, s->schema_policy_length, &policy, NULL);
        ud_string mode = {0};
        if (!result)
            result = ud_json_string(ud_member(policy, "policy"), &mode);
        if (!result && ud_named(mode, "explicitMapping")) {
            char *mapping = NULL;
            uint32_t size = 0;
            result = ud_json_serialize(ud_member(policy, "mapping"), &mapping, &size);
            ud_bytes safe = {.ctx = c->memory};
            const char *prefix = "{\"policy\":\"evolutionMapping\",\"mapping\":";
            if (!result)
                result = ud_bytes_put(&safe, prefix, strlen(prefix));
            if (!result)
                result = ud_bytes_put(&safe, mapping, size);
            if (!result)
                result = ud_bytes_put(&safe, "}", 1);
            if (!result) {
                ud_free(s->schema_policy);
                s->schema_policy = (char *)safe.data;
                s->schema_policy_length = (uint32_t)safe.size;
                safe.data = NULL;
            }
            ud_free(mapping);
            ud_bytes_clear(&safe);
        }
        ud_json_destroy(policy);
    }
    options.schema_policy = (ud_string){s->schema_policy, s->schema_policy_length};
    if (!result && !c->recovery_terminal)
        result = ud_connection_lease_cdc_open(s->lease, &options);
    if (!result && !c->recovery_terminal) {
        ud_batch *actual = NULL;
        result = ud_connection_lease_cdc_schema(s->lease, &actual);
        if (!result) {
            ud_batch_destroy(s->actual);
            s->actual = actual;
            actual = NULL;
        }
        ud_batch_destroy(actual);
        if (!result && s->schema_version)
            result = ud_connection_lease_cdc_rebind(s->lease, s->actual, s->schema_version);
    }
    if (!result && !c->recovery_terminal) {
        s->values = ud_alloc(c->memory, s->output->columns * sizeof(*s->values));
        s->before = ud_alloc(c->memory, s->input->columns * sizeof(*s->before));
        if (!s->values || !s->before)
            result = UD_RESOURCE;
    }
    if (!result && c->recovery_terminal)
        s->ended = 1;
    if (result) {
        dispose(s, result);
        return result;
    }
    bind_decoder(s);
    *instance = s;
    return UD_OK;
}
static ud_status identity_prefix(cdc_source *s, ud_string position, ud_sha256 *prefix) {
    ud_bytes key = {.ctx = s->context.memory};
    ud_status status = ud_put_string(&key, s->context.execution->options.tenant.data,
                                     s->context.execution->options.tenant.length);
    ud_string resource = {0};
    if (!status)
        status = ud_connection_lease_resource(s->lease, &resource);
    if (!status)
        status = ud_put_string(&key, resource.data, resource.length);
    ud_string table = {0};
    if (!status)
        status = ud_json_string(ud_member(s->config, "table"), &table);
    if (!status)
        status = ud_put_string(&key, table.data, table.length);
    if (!status)
        status = ud_put_string(&key, position.data, position.length);
    if (!status)
        status = ud_put_string(&key, s->transaction.data, s->transaction.length);
    if (!status) {
        ud_sha256_init(prefix);
        ud_sha256_update(prefix, key.data, key.size);
    }
    ud_bytes_clear(&key);
    return status;
}
static ud_id row_identity(ud_sha256 prefix, uint64_t ordinal) {
    uint8_t encoded[8], hash[32];
    for (uint32_t i = 0; i < 8; i++)
        encoded[i] = (uint8_t)(ordinal >> (8 * i));
    ud_sha256_update(&prefix, encoded, sizeof(encoded));
    ud_sha256_finish(&prefix, hash);
    ud_id id = {0};
    for (uint32_t i = 0; i < 8; i++) {
        id.high = (id.high << 8) | hash[i];
        id.low = (id.low << 8) | hash[i + 8];
    }
    return id;
}
static ud_status flush_rows(cdc_source *s) {
    ud_batch *b = s->pending;
    if (!b)
        return UD_OK;
    ud_status status = ud_batch_seal(b);
    if (!status && s->count == s->capacity) {
        uint32_t capacity = s->capacity ? s->capacity * 2 : 16;
        ud_buffer_options limits;
        status = ud_buffer_manager_limits(s->context.execution->buffers, &limits);
        if (!status && capacity > limits.max_blocks)
            capacity = limits.max_blocks;
        if (!status && capacity <= s->capacity)
            status = UD_RESOURCE;
        ud_buffer_block **blocks =
            !status ? ud_alloc(s->context.memory, capacity * sizeof(*blocks)) : NULL;
        if (!status && !blocks)
            status = UD_RESOURCE;
        if (!status) {
            if (s->count)
                memcpy(blocks, s->blocks, s->count * sizeof(*blocks));
            ud_free(s->blocks);
            s->blocks = blocks;
            s->capacity = capacity;
        }
    }
    if (!status)
        status = ud_buffer_store(s->context.execution->buffers, b, s->blocks + s->count);
    if (!status)
        s->count++;
    ud_batch_destroy(s->pending);
    s->pending = NULL;
    return status;
}
static ud_status rows(cdc_source *s, const ud_cdc_event *event) {
    if (!s->begun || !event->after || !event->operations ||
        event->after->rows > s->context.max_batch_rows ||
        event->schema_version != s->schema_version || event->after->columns != s->input->columns)
        return UD_CORRUPT;
    if (event->before &&
        (event->before->rows != event->after->rows || event->before->columns != s->input->columns))
        return UD_CORRUPT;
    ud_value *values = s->values, *before = s->before;
    ud_sha256 prefix = {0};
    ud_status status = identity_prefix(s, event->position, &prefix);
    for (uint32_t row = 0; !status && row < event->after->rows; row++) {
        status = ud_activation_check(s->context.activation);
        int action = event->operations[row];
        if (!status && s->append_only && action != UD_CHANGE_INSERT)
            status = ud_fail(s->context.memory, UD_QUALITY,
                             "CDC append-only source received a change other than INSERT");
        if (!status && (action < UD_CHANGE_INSERT ||
                        (action > UD_CHANGE_DELETE && action != UD_CHANGE_TRUNCATE)))
            status = UD_CORRUPT;
        for (uint32_t col = 0; !status && col < s->input->columns; col++) {
            status = ud_batch_get(event->after, row, col, values + col);
            if (!status && event->before)
                status = ud_batch_get(event->before, row, col, before + col);
        }
        values[s->input->columns] =
            (ud_value){.type = UD_STRUCT,
                       .is_null = !event->before || action == UD_CHANGE_INSERT,
                       .children = before,
                       .child_count = event->before ? s->input->columns : 0};
        if (!status && !s->pending)
            status = ud_batch_create(s->context.memory, s->output->schema->fields, s->output->columns,
                                     s->context.max_batch_rows, &s->pending);
        if (!status)
            status = ud_batch_append(s->pending, values, s->output->columns);
        ud_row_info info = {.operation = (uint32_t)action};
        if (!status)
            info.id = row_identity(prefix, s->row++);
        if (!status)
            status = ud_batch_set_row_info(s->pending, s->pending->rows - 1, &info);
        if (!status && s->pending->rows == s->context.max_batch_rows)
            status = flush_rows(s);
    }
    return status;
}
static ud_status bound_rows(cdc_source *source, const ud_cdc_event *event) {
    if (source->direct_rows)
        return rows(source, event);
    ud_cdc_event bound = *event;
    bound.after = bound.before = NULL;
    ud_string policy = {source->schema_policy, source->schema_policy_length};
    ud_status status =
        ud_schema_map(source->context.memory, event->after, source->input, policy, &bound.after);
    if (!status && event->before)
        status = ud_schema_map(source->context.memory, event->before, source->input, policy,
                               &bound.before);
    if (!status)
        status = rows(source, &bound);
    ud_batch_destroy(bound.after);
    ud_batch_destroy(bound.before);
    return status;
}
static ud_status seal_epoch(cdc_source *s) {
    UD_TRY(save_intent(s));
    s->committed = 1;
    return UD_MORE;
}

static ud_status emit_epoch(cdc_source *s) {
    if (s->cut_at < s->cut_count) {
        cdc_commit *cut = s->cuts + s->cut_at;
        if (cut->schema_position.length) {
            ud_component_event change = {.struct_size = sizeof(change),
                                         .abi_version = UD_ABI_VERSION,
                                         .kind = UD_COMPONENT_SCHEMA_CHANGE,
                                         .batch = s->output,
                                         .epoch = s->epoch + 1,
                                         .schema_version = cut->version,
                                         .source_position = cut->schema_position};
            UD_TRY(s->context.emit_event(s->context.emit_user, 0, &change));
            ud_free((void *)cut->schema_position.data);
            cut->schema_position = (ud_string){0};
            return UD_MORE;
        }
        if (s->at < cut->blocks) {
            if (!s->output_view)
                UD_TRY(ud_buffer_read(s->blocks[s->at], &s->output_view));
            const ud_batch *batch = ud_buffer_view_batch(s->output_view);
            uint32_t rows = batch->rows - s->output_at,
                     limit = s->context.recommended_batch_rows
                                 ? s->context.recommended_batch_rows(s->context.emit_user)
                                 : s->context.max_batch_rows;
            if (!limit)
                limit = 1;
            if (rows > limit)
                rows = limit;
            ud_batch *slice = NULL;
            ud_status status = ud_batch_slice(s->context.memory, batch, s->output_at, rows, &slice);
            if (!status)
                status = s->context.emit(s->context.emit_user, 0, slice);
            ud_batch_destroy(slice);
            if (!status) {
                s->output_at += rows;
                if (s->output_at == batch->rows) {
                    ud_buffer_view_release(s->output_view);
                    s->output_view = NULL;
                    s->output_at = 0;
                    status = ud_buffer_release(s->blocks[s->at]);
                    s->blocks[s->at++] = NULL;
                }
            }
            return status ? status : UD_MORE;
        }
        if (cut->watermark > s->watermark) {
            ud_component_event watermark = {.struct_size = sizeof(watermark),
                                            .abi_version = UD_ABI_VERSION,
                                            .kind = UD_COMPONENT_WATERMARK,
                                            .watermark = cut->watermark,
                                            .schema_version = cut->version};
            UD_TRY(s->context.emit_event(s->context.emit_user, 0, &watermark));
            s->watermark = cut->watermark;
            return UD_MORE;
        }
        s->cut_at++;
        return UD_MORE;
    }
    if (!s->barrier) {
        ud_component_event barrier = {.struct_size = sizeof(barrier), .abi_version = UD_ABI_VERSION,
            .kind = UD_COMPONENT_CHECKPOINT_BARRIER, .epoch = s->epoch + 1,
            .schema_version = s->schema_version, .source_position = s->position};
        UD_TRY(s->context.emit_event(s->context.emit_user, 0, &barrier));
        s->epoch++;
        s->barrier = 1;
    }
    return UD_MORE;
}

static ud_status record_commit(cdc_source *s, const ud_cdc_event *event) {
    if (!s->begun || !event->position.length)
        return UD_CORRUPT;
    UD_TRY(flush_rows(s));
    ud_buffer_options limits;
    UD_TRY(ud_buffer_manager_limits(s->context.execution->buffers, &limits));
    if (s->cut_count == s->cut_capacity) {
        uint32_t cap = s->cut_capacity ? s->cut_capacity * 2 : 16;
        if (cap < s->cut_capacity || cap > limits.max_blocks)
            cap = limits.max_blocks;
        if (cap <= s->cut_count)
            return UD_RESOURCE;
        cdc_commit *cuts = ud_alloc(s->context.memory, cap * sizeof(*cuts));
        if (!cuts)
            return UD_RESOURCE;
        if (s->cut_count)
            memcpy(cuts, s->cuts, s->cut_count * sizeof(*cuts));
        ud_free(s->cuts);
        s->cuts = cuts;
        s->cut_capacity = cap;
    }
    ud_free((void *)s->position.data);
    s->position = (ud_string){ud_strdup_n(s->context.memory, event->position.data,
                                         event->position.length), event->position.length};
    if (!s->position.data)
        return UD_RESOURCE;
    if (!s->cut_count)
        s->batch_started_ms = ud_monotonic_ns() / 1000000;
    cdc_commit *cut = s->cuts + s->cut_count++;
    *cut = (cdc_commit){s->count, event->watermark > 0 ? event->watermark : 0, s->schema_version};
    if (s->schema_pending) {
        cut->schema_position = (ud_string){ud_strdup_n(s->context.memory, s->position.data,
                                          s->position.length), s->position.length};
        if (!cut->schema_position.data)
            return UD_RESOURCE;
        s->schema_pending = 0;
    }
    s->begun = 0;
    s->transactions++;
    s->pending_rows += s->row;
    if ((!s->micro_batch_rows && !s->micro_batch_ms) ||
        (s->micro_batch_rows && s->pending_rows >= s->micro_batch_rows) ||
        s->cut_count == limits.max_blocks || s->count >= limits.max_blocks)
        return seal_epoch(s);
    return UD_OK;
}

static ud_status step(void *instance, const ud_component_event *input) {
    (void)input;
    cdc_source *s = instance;
    UD_TRY(ud_connection_lease_authorize(s->lease, UD_ACCESS_READ));
    if (s->committed)
        return emit_epoch(s);
    if (!s->begun) {
        int stopping = s->ended || atomic_load(&s->context.execution->stop_requested) ||
                       (s->limit && s->transactions >= s->limit);
        if (s->cut_count && (stopping || (s->micro_batch_ms &&
                ud_monotonic_ns() / 1000000 - s->batch_started_ms >= s->micro_batch_ms)))
            return seal_epoch(s);
        if (stopping) {
            s->ended = 1;
            return UD_END;
        }
    }
    ud_cdc_event event = {0};
    ud_status status = ud_connection_lease_cdc_next(s->lease, &event);
    if ((status == UD_END || (status == UD_BUSY && !s->micro_batch_ms)) && !s->begun &&
        s->cut_count)
        return seal_epoch(s);
    if (status)
        return status == UD_END && s->begun ? UD_CORRUPT : status;
    if (event.kind == UD_CDC_BEGIN) {
        if (s->begun)
            status = UD_CORRUPT;
        else {
            s->begun = 1;
            s->row = 0;
            s->schema_version = event.schema_version;
            ud_free((void *)s->transaction.data);
            s->transaction = (ud_string){ud_strdup_n(s->context.memory, event.transaction_id.data,
                                                     event.transaction_id.length),
                                         event.transaction_id.length};
            if (!s->transaction.data)
                status = UD_RESOURCE;
        }
    } else if (event.kind == UD_CDC_ROWS)
        status = bound_rows(s, &event);
    else if (event.kind == UD_CDC_TRUNCATE) {
        if (!s->begun)
            status = UD_CORRUPT;
        else {
            ud_cdc_event reset = event;
            reset.operations = NULL;
            reset.after = NULL;
            reset.before = NULL;
            int32_t action = UD_CHANGE_TRUNCATE;
            ud_value *values = ud_alloc(s->context.memory, s->input->columns * sizeof(*values));
            status = values ? UD_OK : UD_RESOURCE;
            for (uint32_t col = 0; !status && col < s->input->columns; col++) {
                const ud_field *field = &s->input->column[col].field;
                values[col] = (ud_value){.type = field->type,
                                         .precision = field->precision,
                                         .scale = field->scale,
                                         .is_null = 1,
                                         .is_absent = 1};
            }
            if (!status)
                status = ud_batch_create(s->context.memory, s->input->schema->fields,
                                         s->input->columns, 1, &reset.after);
            if (!status)
                status = ud_batch_append(reset.after, values, s->input->columns);
            if (!status)
                status = ud_batch_seal(reset.after);
            reset.operations = &action;
            if (!status)
                status = rows(s, &reset);
            ud_batch_destroy(reset.after);
            ud_free(values);
        }
    } else if (event.kind == UD_CDC_COMMIT) {
        status = record_commit(s, &event);
    } else if (event.kind == UD_CDC_SCHEMA_CHANGE) {
        ud_batch *mapped = NULL;
        ud_status compatible =
            event.after
                ? ud_schema_map(s->context.memory, event.after, s->input,
                                (ud_string){s->schema_policy, s->schema_policy_length}, &mapped)
                : UD_INCOMPATIBLE;
        ud_batch_destroy(mapped);
        status = ud_checkpoint_schema_event(s->context.activation, s->context.component_id, &event,
                                            compatible);
        if (!status && event.after) {
            char *difference = NULL;
            uint32_t length = 0;
            status = ud_schema_compare(s->context.memory, s->input, event.after,
                                       (ud_string){s->schema_policy, s->schema_policy_length},
                                       &difference, &length);
            if (!status)
                status = ud_schema_report(s->context.activation, (ud_string){difference, length}, compatible);
            ud_free(difference);
        }
        if (!status && compatible)
            status = compatible == UD_SCHEMA || compatible == UD_INCOMPATIBLE
                ? ud_fail(s->context.memory, UD_INCOMPATIBLE,
                          "CDC schema no longer matches the published fields and keys. Input was not confirmed.")
                : compatible;
        if (!status) {
            status = ud_connection_lease_cdc_rebind(s->lease, event.after, event.schema_version);
            if (status) {
                ud_status recorded = ud_checkpoint_schema_event(
                    s->context.activation, s->context.component_id, &event, status);
                if (recorded)
                    status = recorded;
            }
        }
        if (!status) {
            ud_batch_destroy(s->actual);
            s->actual = NULL;
            status = ud_connection_lease_cdc_schema(s->lease, &s->actual);
            if (!status)
                bind_decoder(s);
        }
        if (!status) {
            s->schema_version = event.schema_version;
            s->schema_pending = 1;
        }
    } else if (event.kind != UD_CDC_HEARTBEAT)
        status = UD_CORRUPT;
    ud_cdc_event_clear(&event);
    return status ? status : UD_MORE;
}
static ud_status checkpoint(void *instance, ud_batch **out) {
    cdc_source *s = instance;
    if ((!s->committed && !s->ended) || s->at != s->count)
        return UD_BUSY;
    ud_field fields[] = {{.name = "position", .name_length = 8, .type = UD_UTF8},
                         {.name = "transactions", .name_length = 12, .type = UD_UINT64},
                         {.name = "schemaVersion", .name_length = 13, .type = UD_UINT64},
                         {.name = "ended", .name_length = 5, .type = UD_BOOL},
                         {.name = "decoderSchema", .name_length = 13, .type = UD_UTF8},
                         {.name = "watermark", .name_length = 9, .type = UD_INT64}};
    char *schema_text = NULL;
    uint32_t schema_length = 0;
    const ud_batch *actual = s->actual ? s->actual : s->input;
    UD_TRY(ud_schema_to_json(actual, &schema_text, &schema_length));
    ud_value values[] = {
        {.type = UD_UTF8, .text = s->position.data, .text_length = s->position.length},
        {.type = UD_UINT64, .unsigned_integer = s->transactions},
        {.type = UD_UINT64, .unsigned_integer = s->schema_version},
        {.type = UD_BOOL, .integer = s->ended},
        {.type = UD_UTF8, .text = schema_text, .text_length = schema_length},
        {.type = UD_INT64, .integer = s->watermark}};
    ud_status status = ud_batch_create(s->context.memory, fields, 6, 1, out);
    if (!status)
        status = ud_batch_append(*out, values, 6);
    if (!status)
        status = ud_batch_seal(*out);
    ud_free(schema_text);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
static ud_status restore(void *instance, const ud_batch *batch) {
    cdc_source *source = instance;
    if ((batch->columns != 5 && batch->columns != 6) || batch->rows != 1)
        return UD_CORRUPT;
    ud_value position, count, version, ended, schema;
    UD_TRY(ud_batch_get(batch, 0, 0, &position));
    UD_TRY(ud_batch_get(batch, 0, 1, &count));
    UD_TRY(ud_batch_get(batch, 0, 2, &version));
    UD_TRY(ud_batch_get(batch, 0, 3, &ended));
    UD_TRY(ud_batch_get(batch, 0, 4, &schema));
    if (position.is_null || count.is_null || version.is_null || ended.is_null || schema.is_null ||
        count.type != UD_UINT64 || version.type != UD_UINT64 ||
        !ud_same((ud_string){position.text, position.text_length},
                 source->context.recovery_position))
        return UD_FENCED;
    if (batch->columns == 6) {
        ud_value watermark;
        UD_TRY(ud_batch_get(batch, 0, 5, &watermark));
        if (watermark.type != UD_INT64 || watermark.is_null)
            return UD_CORRUPT;
        source->watermark = watermark.integer;
    }
    source->ended = (int)ended.integer;
    if (UINT64_MAX - count.unsigned_integer < source->cut_count)
        return UD_RESOURCE;
    source->transactions = count.unsigned_integer + source->cut_count;
    if (source->committed)
        return UD_OK; /* prepare loaded the later sealed decoder schema and log position */
    source->schema_version = version.unsigned_integer;
    ud_batch *actual = NULL, *mapped = NULL;
    ud_status status = ud_schema_from_json(source->context.memory, schema.text, schema.text_length,
                                           (ud_string){0}, &actual);
    if (!status)
        status = ud_schema_map(source->context.memory, actual, source->input,
                               (ud_string){source->schema_policy, source->schema_policy_length},
                               &mapped);
    ud_batch *current = NULL;
    if (!status && !source->ended)
        status = ud_connection_lease_cdc_schema(source->lease, &current);
    if (!status && current) {
        ud_batch_destroy(mapped);
        mapped = NULL;
        status = ud_schema_map(source->context.memory, current, actual,
                               (ud_string){source->schema_policy, source->schema_policy_length},
                               &mapped);
        if (!status)
            status = ud_connection_lease_cdc_rebind(source->lease, current, source->schema_version);
        if (!status) {
            ud_batch_destroy(actual);
            actual = current;
            current = NULL;
        }
    }
    if (!status) {
        ud_batch_destroy(source->actual);
        source->actual = actual;
        actual = NULL;
        bind_decoder(source);
    }
    ud_batch_destroy(current);
    ud_batch_destroy(actual);
    ud_batch_destroy(mapped);
    return status;
}
static ud_status confirm_epoch(void *instance, uint64_t epoch, ud_string position) {
    cdc_source *s = instance;
    if (epoch != s->epoch || !s->barrier || !ud_same(position, s->position))
        return UD_FENCED;
    UD_TRY(ud_connection_lease_cdc_confirm(s->lease, position));
    for (uint32_t i = 0; i < s->count; i++)
        UD_TRY(
            ud_checkpoint_snapshot_forget(s->context.activation, (ud_string){s->intent, 64}, i, 1));
    UD_TRY(ud_checkpoint_snapshot_forget(s->context.activation, (ud_string){s->intent, 64}, 0, 0));
    s->count = s->at = 0;
    s->cut_at = s->cut_count = 0;
    s->pending_rows = 0;
    s->begun = s->committed = s->barrier = 0;
    return ud_checkpoint_collect(s->context.activation);
}
static ud_status finish(void *instance) {
    cdc_source *s = instance;
    return s->begun ? UD_CORRUPT : UD_OK;
}
static ud_status describe(void *user, const ud_component_context *c, ud_string text,
                          const ud_batch *const *in, ud_component_plan *plan) {
    (void)user;(void)in;ud_json *config = NULL;
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, &config, NULL));
    uint64_t limit = 0; ud_status status = UD_OK;
    if (ud_member(config, "maxTransactions")) status = ud_json_uint(ud_member(config, "maxTransactions"), &limit);
    ud_string mode = {0};
    if (!status) status = ud_json_string(ud_member(config, "mode"), &mode);
    plan->boundedness = limit ? UD_SOURCE_BOUNDED : ud_named(mode, "snapshotAndStream") ? UD_SOURCE_SNAPSHOT_THEN_STREAM : UD_SOURCE_CONTINUOUS;
    ud_json_destroy(config);return status;
}
ud_status ud_cdc_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\",\"minLength\":1},"
        "\"table\":{\"type\":\"string\",\"minLength\":1},\"schema\":{\"type\":\"object\"},\"mode\":"
        "{\"type\":\"string\",\"enum\":[\"stream\",\"snapshotAndStream\"],\"default\":"
        "\"snapshotAndStream\"},\"position\":{\"type\":\"string\"},\"slot\":{\"type\":\"string\"},"
        "\"publication\":{\"type\":\"string\"},\"appendOnly\":{\"type\":\"boolean\",\"default\":false},"
        "\"serverId\":{\"type\":\"integer\",\"minimum\":1,"
        "\"maximum\":4294967295},\"maxTransactions\":{\"type\":\"integer\",\"minimum\":0,"
        "\"default\":0},\"microBatchRows\":{\"type\":\"integer\",\"minimum\":1},"
        "\"microBatchMs\":{\"type\":\"integer\",\"minimum\":1},\"schemaPolicy\":{\"type\":\"object\"}},\"required\":[\"connection\","
        "\"table\",\"schema\"],"
        "\"additionalProperties\":false}";
    const char *ids[] = {"mysql.cdc.source", "mariadb.cdc.source", "postgresql.cdc.source",
                         "sqlserver.cdc.source", "oracle.cdc.source"};
    const char *names[] = {"MySQL CDC Source", "MariaDB CDC Source", "PostgreSQL CDC Source",
                           "SQL Server CDC Source", "Oracle CDC Source"};
    const int32_t dialects[] = {UD_SQL_MYSQL, UD_SQL_MARIADB, UD_SQL_POSTGRESQL, UD_SQL_SQLSERVER,
                                UD_SQL_ORACLE};
    for (uint32_t i = 0; i < 5; i++) {
        ud_component_descriptor descriptor = {
            .struct_size = sizeof(descriptor),
            .abi_version = UD_ABI_VERSION,
            .id = {ids[i], (uint32_t)strlen(ids[i])},
            .name = {names[i], (uint32_t)strlen(names[i])},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {schema, sizeof(schema) - 1},
            .category = UD_COMPONENT_SOURCE,
            .outputs = 1,
            .synchronous_input = -1,
            .error_output = -1,
            .capabilities =
                UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_STATEFUL | UD_COMPONENT_THREAD_SAFE |
                UD_COMPONENT_CONTINUOUS | UD_COMPONENT_PREVIEW_SCHEMA,
            .effects = UD_EFFECT_EXTERNAL_READ,
            .state_version = 2,
            .user = (void *)(intptr_t)dialects[i],
            .configure = configure,
            .describe_plan = describe,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .dispose = dispose,
            .checkpoint = checkpoint,
            .restore = restore,
            .confirm_epoch = confirm_epoch};
        UD_TRY(ud_registry_register(registry, &descriptor));
    }
    return UD_OK;
}
