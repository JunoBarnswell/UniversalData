#include "ud/connection_manager.h"
#include "ud/engine.h"
#include "../package/internal.h"
#include "ud/cdc.h"
#include "ud/format.h"
/* A single transactional Source adapter for actual vendor log callbacks.
 * Uncommitted rows stay in the existing bounded Buffer until COMMIT. */
typedef struct cdc_source {
    ud_component_context context;
    ud_json *config;
    ud_connection_lease *lease;
    ud_batch *input, *output;
    ud_buffer_block **blocks;
    uint32_t count, capacity, at, output_at;
    ud_buffer_view *output_view;
    uint64_t epoch, transactions, row, schema_version, limit;
    ud_string position, transaction;
    int64_t watermark;
    int watermark_pending;
    int begun, committed, barrier, ended;
    char intent[65], policy[65];
} cdc_source;
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
    ud_free(s->blocks);
    ud_connection_lease_release(s->lease);
    ud_batch_destroy(s->input);
    ud_batch_destroy(s->output);
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
    if (!status)
        status = ud_bytes_put(&payload, "}", 1);
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
        options->position = s->position;
        options->mode = UD_CDC_STREAM;
        s->begun = s->committed = 1;
        s->transactions++;
    }
    ud_json_destroy(entry);
    return status;
}
static ud_status prepare(void *user, const ud_component_context *c, ud_string text,
                         const ud_batch *const *in, const ud_batch *const *out, void **instance) {
    (void)in;
    (void)out;
    *instance = NULL;
    if (!c->execution->journal)
        return UD_SCHEMA;
    cdc_source *s = ud_alloc(c->memory, sizeof(*s));
    if (!s)
        return UD_RESOURCE;
    s->context = *c;
    s->epoch = c->recovery_epoch;
    ud_status result = ud_json_parse(c->memory, text.data, text.length, &s->config, NULL);
    ud_string connection = {0};
    if (!result)
        result = ud_json_string(ud_member(s->config, "connection"), &connection);
    if (!result)
        result = schema(c, s->config, &s->input, &s->output);
    if (!result) {
        ud_batch_destroy(s->output);
        s->output = NULL;
        result = ud_batch_create(c->memory, out[0]->schema->fields, out[0]->columns, 0, &s->output);
    }
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
    if (!result && !c->recovery_terminal)
        result = ud_connection_lease_cdc_open(s->lease, &options);
    if (!result && c->recovery_terminal)
        s->ended = 1;
    if (result) {
        dispose(s, result);
        return result;
    }
    *instance = s;
    return UD_OK;
}
static ud_status row_identity(cdc_source *s, ud_string position, uint64_t ordinal,
                              ud_row_info *info) {
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
    if (!status)
        status = ud_put_u64(&key, ordinal);
    uint8_t hash[32];
    if (!status) {
        ud_hash(key.data, key.size, hash);
        info->id = (ud_id){0};
        for (uint32_t i = 0; i < 8; i++) {
            info->id.high = (info->id.high << 8) | hash[i];
            info->id.low = (info->id.low << 8) | hash[i + 8];
        }
    }
    ud_bytes_clear(&key);
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
    ud_batch *b = NULL;
    UD_TRY(ud_batch_create(s->context.memory, s->output->schema->fields, s->output->columns,
                           event->after->rows, &b));
    ud_value *values = ud_alloc(s->context.memory, b->columns * sizeof(*values)),
             *before = ud_alloc(s->context.memory, s->input->columns * sizeof(*before));
    ud_status status = values && before ? UD_OK : UD_RESOURCE;
    for (uint32_t row = 0; !status && row < event->after->rows; row++) {
        status = ud_activation_check(s->context.activation);
        int action = event->operations[row];
        if (!status && (action < UD_CHANGE_INSERT || action > UD_CHANGE_DELETE))
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
        if (!status)
            status = ud_batch_append(b, values, b->columns);
        ud_row_info info = {.operation = (uint32_t)action};
        if (!status)
            status = row_identity(s, event->position, s->row++, &info);
        if (!status)
            status = ud_batch_set_row_info(b, row, &info);
    }
    if (!status)
        status = ud_batch_seal(b);
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
    ud_batch_destroy(b);
    ud_free(values);
    ud_free(before);
    return status;
}
static ud_status step(void *instance, const ud_component_event *input) {
    (void)input;
    cdc_source *s = instance;
    UD_TRY(ud_connection_lease_authorize(s->lease, UD_ACCESS_READ));
    if (s->committed) {
        if (s->at < s->count) {
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
        if (s->watermark_pending) {
            ud_component_event watermark = {.struct_size = sizeof(watermark),
                                            .abi_version = UD_ABI_VERSION,
                                            .kind = UD_COMPONENT_WATERMARK,
                                            .watermark = s->watermark,
                                            .schema_version = s->schema_version};
            UD_TRY(s->context.emit_event(s->context.emit_user, 0, &watermark));
            s->watermark_pending = 0;
            return UD_MORE;
        }
        if (!s->barrier) {
            ud_component_event barrier = {.struct_size = sizeof(barrier),
                                          .abi_version = UD_ABI_VERSION,
                                          .kind = UD_COMPONENT_CHECKPOINT_BARRIER,
                                          .epoch = ++s->epoch,
                                          .schema_version = s->schema_version,
                                          .source_position = s->position};
            UD_TRY(s->context.emit_event(s->context.emit_user, 0, &barrier));
            s->barrier = 1;
            return UD_MORE;
        }
        return UD_MORE;
    }
    if (s->ended || (s->limit && s->transactions >= s->limit)) {
        s->ended = 1;
        return UD_END;
    }
    ud_cdc_event event = {0};
    ud_status status = ud_connection_lease_cdc_next(s->lease, &event);
    if (status == UD_BUSY) {
        UD_TRY(ud_wait(s->context.memory, 10));
        return UD_MORE;
    }
    if (status)
        return status;
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
        status = rows(s, &event);
    else if (event.kind == UD_CDC_COMMIT) {
        if (!s->begun || !event.position.length)
            status = UD_CORRUPT;
        else {
            ud_free((void *)s->position.data);
            s->position = (ud_string){
                ud_strdup_n(s->context.memory, event.position.data, event.position.length),
                event.position.length};
            if (!s->position.data)
                status = UD_RESOURCE;
            else {
                if (event.watermark > s->watermark) {
                    s->watermark = event.watermark;
                    s->watermark_pending = 1;
                }
                s->committed = 1;
                s->transactions++;
                status = save_intent(s);
            }
        }
    } else if (event.kind == UD_CDC_SCHEMA_CHANGE)
        status = UD_INCOMPATIBLE;
    else if (event.kind != UD_CDC_HEARTBEAT)
        status = UD_CORRUPT;
    ud_cdc_event_clear(&event);
    return status ? status : UD_MORE;
}
static ud_status checkpoint(void *instance, ud_batch **out) {
    cdc_source *s = instance;
    if ((!s->committed && !s->ended) || s->at != s->count)
        return UD_BUSY;
    ud_field fields[] = {{.name = "position", .name_length = 8, .type = UD_UTF8},
                         {.name = "transactions", .name_length = 12, .type = UD_INT64},
                         {.name = "schemaVersion", .name_length = 13, .type = UD_INT64},
                         {.name = "ended", .name_length = 5, .type = UD_BOOL}};
    ud_value values[] = {
        {.type = UD_UTF8, .text = s->position.data, .text_length = s->position.length},
        {.type = UD_INT64, .integer = (int64_t)s->transactions},
        {.type = UD_INT64, .integer = (int64_t)s->schema_version},
        {.type = UD_BOOL, .integer = s->ended}};
    UD_TRY(ud_batch_create(s->context.memory, fields, 4, 1, out));
    ud_status status = ud_batch_append(*out, values, 4);
    if (!status)
        status = ud_batch_seal(*out);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
static ud_status restore(void *instance, const ud_batch *b) {
    cdc_source *s = instance;
    if (b->columns != 4 || b->rows != 1)
        return UD_CORRUPT;
    ud_value p, n, v, ended;
    UD_TRY(ud_batch_get(b, 0, 0, &p));
    UD_TRY(ud_batch_get(b, 0, 1, &n));
    UD_TRY(ud_batch_get(b, 0, 2, &v));
    if (p.is_null || n.is_null || v.is_null || n.integer < 0 || v.integer < 0 ||
        !ud_same((ud_string){p.text, p.text_length}, s->context.recovery_position))
        return UD_FENCED;
    UD_TRY(ud_batch_get(b, 0, 3, &ended));
    if (ended.is_null || ended.type != UD_BOOL)
        return UD_CORRUPT;
    s->ended = (int)ended.integer;
    s->transactions = (uint64_t)n.integer + (s->committed ? 1 : 0);
    s->schema_version = (uint64_t)v.integer;
    return UD_OK;
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
    s->begun = s->committed = s->barrier = 0;
    return ud_checkpoint_collect(s->context.activation);
}
static ud_status finish(void *instance) {
    cdc_source *s = instance;
    return s->begun ? UD_CORRUPT : UD_OK;
}
ud_status ud_cdc_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\",\"minLength\":1},"
        "\"table\":{\"type\":\"string\",\"minLength\":1},\"schema\":{\"type\":\"object\"},\"mode\":"
        "{\"type\":\"string\",\"enum\":[\"stream\",\"snapshotAndStream\"],\"default\":"
        "\"snapshotAndStream\"},\"position\":{\"type\":\"string\"},\"slot\":{\"type\":\"string\"},"
        "\"publication\":{\"type\":\"string\"},\"serverId\":{\"type\":\"integer\",\"minimum\":1,"
        "\"maximum\":4294967295},\"maxTransactions\":{\"type\":\"integer\",\"minimum\":0,"
        "\"default\":0}},\"required\":[\"connection\",\"table\",\"schema\"],"
        "\"additionalProperties\":false}";
    const char *ids[] = {"mysql.cdc.source", "mariadb.cdc.source", "postgresql.cdc.source",
                         "sqlserver.cdc.source", "oracle.cdc.source"};
    const int32_t dialects[] = {UD_SQL_MYSQL, UD_SQL_MARIADB, UD_SQL_POSTGRESQL, UD_SQL_SQLSERVER,
                                UD_SQL_ORACLE};
    for (uint32_t i = 0; i < 5; i++) {
        ud_component_descriptor descriptor = {
            .struct_size = sizeof(descriptor),
            .abi_version = UD_ABI_VERSION,
            .id = {ids[i], (uint32_t)strlen(ids[i])},
            .name = {ids[i], (uint32_t)strlen(ids[i])},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {schema, sizeof(schema) - 1},
            .category = UD_COMPONENT_SOURCE,
            .outputs = 1,
            .synchronous_input = -1,
            .error_output = -1,
            .capabilities =
                UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_STATEFUL | UD_COMPONENT_THREAD_SAFE,
            .effects = UD_EFFECT_EXTERNAL_READ,
            .state_version = 1,
            .user = (void *)(intptr_t)dialects[i],
            .configure = configure,
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
