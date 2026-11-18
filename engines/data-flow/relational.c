#include "../package/internal.h"
#include "ud/connection_manager.h"
#include "ud/operator.h"
/* Thin component binding: all value, sort, grouping and classification semantics
 * are in Compute. Emission and receipts remain Pipeline/ConnectionManager owned. */
typedef struct relational_component {
    ud_component_context context;
    int kind, policy, error_port;
    ud_json *config;
    ud_operator *operator;
    ud_batch *errors, *dimension, *pending;
    uint32_t pending_row, ended, *keys, key_count, current;
    ud_string cache, table;
    ud_cache *reference;
    ud_connection_lease *lease;
    char *job;
    uint64_t committed, conflicts;
} relational_component;
static int outputs(int k) {
    if (k == UD_OPERATOR_EVENT_WINDOW)
        return 2;
    if (k == UD_OPERATOR_DIFF_COMPARE)
        return 5;
    if (k == UD_OPERATOR_ASOF_JOIN || k == UD_OPERATOR_TEMPORAL_JOIN)
        return 2;
    return k == UD_OPERATOR_PIVOT || k == UD_OPERATOR_ROW_SAMPLING ||
                   k == UD_OPERATOR_PERCENTAGE_SAMPLING
               ? 2
           : k == UD_OPERATOR_SCD ? 6
                                  : 1;
}
static ud_status bind_operator(const ud_component_context *c, int kind, ud_string text,
                               const ud_batch *const *in, ud_cache *reference, ud_operator **out) {
    ud_sort_contract order[2] = {{0}};
    for (uint32_t p = 0;
         p < (kind == UD_OPERATOR_MERGE || kind == UD_OPERATOR_MERGE_JOIN ||
                      kind == UD_OPERATOR_ASOF_JOIN || kind == UD_OPERATOR_TEMPORAL_JOIN ||
                      kind == UD_OPERATOR_DIFF_COMPARE || kind == UD_OPERATOR_EVENT_JOIN
                  ? 2u
                  : 1u);
         p++)
        if (c->input_plans)
            order[p] = c->input_plans[p].sort;
    ud_operator_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .kind = kind,
                                   .max_batch_rows = c->max_batch_rows,
                                   .buffers = c->execution->buffers,
                                   .check = (ud_status (*)(void *))ud_activation_check,
                                   .user = c->activation,
                                   .configuration = text,
                                   .input_order = order,
                                   .reference = reference,
                                   .executor = c->execution->executor,
                                   .partition_count = 1};
    UD_TRY(ud_component_operator_options(c, &options));
    return ud_operator_create(c->memory, &options, in, out);
}
static ud_status inputs_for_scd(const ud_component_context *c, const ud_json *config,
                                const ud_batch *input, const ud_batch **bound,
                                ud_batch **dimension) {
    ud_string id;
    UD_TRY(ud_json_string(ud_member(config, "cache"), &id));
    int duplicates = 0, policy = 0;
    UD_TRY(ud_asset_schema(c->activation, id, dimension, &duplicates, &policy));
    if (duplicates != UD_CACHE_ERROR || policy != UD_ERROR_FAIL)
        return UD_SCHEMA;
    bound[0] = input;
    bound[1] = *dimension;
    return UD_OK;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, &config, NULL));
    ud_batch *dimension = NULL;
    const ud_batch *bound[2] = {
        in[0], kind == UD_OPERATOR_MERGE || kind == UD_OPERATOR_MERGE_JOIN ||
                       kind == UD_OPERATOR_ASOF_JOIN || kind == UD_OPERATOR_TEMPORAL_JOIN ||
                       kind == UD_OPERATOR_DIFF_COMPARE || kind == UD_OPERATOR_EVENT_JOIN
                   ? in[1]
                   : NULL};
    ud_status s =
        kind == UD_OPERATOR_SCD ? inputs_for_scd(c, config, in[0], bound, &dimension) : UD_OK;
    ud_operator *o = NULL;
    if (!s)
        s = bind_operator(c, kind, text, bound, NULL, &o);
    for (uint32_t p = 0; !s && p < (uint32_t)outputs(kind); p++)
        if ((kind == UD_OPERATOR_PIVOT && p == 1) || (kind == UD_OPERATOR_SCD && p == 5)) {
            int include = 0;
            const ud_json *flag = ud_member(config, "includeInput");
            if (flag) {
                const char *v;
                uint32_t n;
                if (ud_json_kind(flag) != UD_JSON_BOOL)
                    s = UD_SCHEMA;
                else {
                    s = ud_json_text(flag, &v, &n);
                    include = n == 4;
                }
            }
            if (!s)
                s = ud_error_schema(&c->errors, in[0], include, out + p);
        } else
            s = ud_operator_schema(o, p, out + p);
    ud_operator_destroy(o);
    ud_batch_destroy(dimension);
    ud_json_destroy(config);
    return s;
}
static void dispose(void *ptr, ud_status outcome) {
    (void)outcome;
    relational_component *r = ptr;
    if (!r)
        return;
    ud_operator_destroy(r->operator);
    ud_cache_release(r->reference);
    ud_connection_lease_release(r->lease);
    ud_batch_destroy(r->dimension);
    ud_batch_destroy(r->pending);
    ud_batch_destroy(r->errors);
    ud_json_destroy(r->config);
    ud_free(r->keys);
    ud_free(r->job);
    ud_free(r);
}
static ud_status prepare(void *user, const ud_component_context *c, ud_string text,
                         const ud_batch *const *in, const ud_batch *const *out, void **instance) {
    *instance = NULL;
    relational_component *r = ud_alloc(c->memory, sizeof(*r));
    if (!r)
        return UD_RESOURCE;
    r->context = *c;
    r->kind = (int)(intptr_t)user;
    r->error_port = r->kind == UD_OPERATOR_PIVOT ? 1 : r->kind == UD_OPERATOR_SCD ? 5 : -1;
    ud_status s = ud_json_parse(c->memory, text.data, text.length, &r->config, NULL);
    if (!s)
        s = ud_error_policy(r->config, "errorPolicy", UD_ERROR_FAIL, &r->policy);
    const ud_batch *bound[2] = {
        in[0], r->kind == UD_OPERATOR_MERGE || r->kind == UD_OPERATOR_MERGE_JOIN ||
                       r->kind == UD_OPERATOR_ASOF_JOIN || r->kind == UD_OPERATOR_TEMPORAL_JOIN ||
                       r->kind == UD_OPERATOR_DIFF_COMPARE || r->kind == UD_OPERATOR_EVENT_JOIN
                   ? in[1]
                   : NULL};
    if (!s && r->kind == UD_OPERATOR_SCD) {
        s = inputs_for_scd(c, r->config, in[0], bound, &r->dimension);
        if (!s)
            s = ud_json_string(ud_member(r->config, "cache"), &r->cache);
        if (!s)
            s = ud_asset_acquire(c->activation, r->cache, &r->reference);
    }
    if (!s)
        s = bind_operator(c, r->kind, text, bound, r->reference, &r->operator);
    if (!s && r->error_port >= 0)
        s = ud_batch_create(c->memory, out[r->error_port]->schema->fields,
                            out[r->error_port]->columns, 0, &r->errors);
    if (s) {
        dispose(r, s);
        return s;
    }
    *instance = r;
    return UD_OK;
}
static ud_status pull(relational_component *r) {
    ud_batch *batch = NULL;
    uint32_t port = 0;
    ud_status s = ud_operator_pull(r->operator, &batch, &port);
    if (s == UD_BUSY)
        return UD_OK;
    if (s)
        return s;
    if (!(r->context.connected_outputs & (UINT64_C(1) << port))) {
        ud_batch_destroy(batch);
        return UD_MORE;
    }
    if ((int)port == r->error_port) {
        ud_batch *errors = NULL;
        s = ud_batch_create(r->context.memory, r->errors->schema->fields, r->errors->columns,
                            batch->rows, &errors);
        const char *name = r->kind == UD_OPERATOR_PIVOT ? "pivot.rejected" : "scd.fixedViolation";
        for (uint32_t row = 0; !s && row < batch->rows; row++)
            s = ud_error_append(
                &r->context.errors, errors,
                &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                   .abi_version = UD_ABI_VERSION,
                                   .input = batch,
                                   .row = row,
                                   .rule = (ud_string){name, (uint32_t)strlen(name)},
                                   .rule_version = 1,
                                   .column = (ud_id){0},
                                   .code = UD_QUALITY,
                                   .policy = r->policy});
        if (!s)
            s = ud_batch_seal(errors);
        if (!s)
            s = r->context.emit(r->context.emit_user, port, errors);
        ud_batch_destroy(errors);
    } else
        s = r->context.emit(r->context.emit_user, port, batch);
    ud_batch_destroy(batch);
    return s ? s : UD_MORE;
}
static ud_status step(void *ptr, const ud_component_event *event) {
    relational_component *r = ptr;
    UD_TRY(ud_activation_check(r->context.activation));
    if (event->kind == UD_COMPONENT_DATA)
        UD_TRY(ud_operator_push(r->operator, event->input, event->batch));
    else if (event->kind == UD_COMPONENT_INPUT_END)
        UD_TRY(ud_operator_finish_input(r->operator, event->input));
    if (event->kind == UD_COMPONENT_WATERMARK && r->kind >= UD_OPERATOR_EVENT_WINDOW)
        UD_TRY(ud_operator_watermark(r->operator, event->watermark));
    return pull(r);
}
static uint64_t demand(void *ptr) {
    return ud_operator_input_demand(((relational_component *)ptr)->operator);
}
static ud_status finish(void *ptr) {
    (void)ptr;
    return UD_OK;
}
static ud_status checkpoint(void *instance, ud_batch **out) {
    return ud_operator_checkpoint(((relational_component *)instance)->operator, out);
}
static ud_status restore_state(void *instance, const ud_batch *state) {
    return ud_operator_restore(&((relational_component *)instance)->operator, state);
}
static ud_status diagnostics(void *ptr, char **out, uint32_t *n) {
    return ud_operator_diagnostics(((relational_component *)ptr)->operator, out, n);
}
static ud_status describe(void *user, const ud_component_context *c, ud_string text,
                          const ud_batch *const *in, ud_component_plan *plan) {
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, &config, NULL));
    ud_batch *dimension = NULL;
    const ud_batch *bound[2] = {
        in[0], kind == UD_OPERATOR_MERGE || kind == UD_OPERATOR_MERGE_JOIN ||
                       kind == UD_OPERATOR_ASOF_JOIN || kind == UD_OPERATOR_TEMPORAL_JOIN ||
                       kind == UD_OPERATOR_DIFF_COMPARE || kind == UD_OPERATOR_EVENT_JOIN
                   ? in[1]
                   : NULL};
    ud_status s =
        kind == UD_OPERATOR_SCD ? inputs_for_scd(c, config, in[0], bound, &dimension) : UD_OK;
    ud_operator *o = NULL;
    if (!s)
        s = bind_operator(c, kind, text, bound, NULL, &o);
    if (!s)
        s = ud_component_operator_plan(o, plan);
    ud_operator_destroy(o);
    ud_batch_destroy(dimension);
    ud_json_destroy(config);
    return s;
}
static ud_status index(const ud_batch *b, const ud_json *j, uint32_t *out) {
    if (ud_json_kind(j) == UD_JSON_NUMBER) {
        uint64_t v;
        UD_TRY(ud_json_uint(j, &v));
        if (v >= b->columns)
            return UD_SCHEMA;
        *out = (uint32_t)v;
        return UD_OK;
    }
    ud_string n;
    UD_TRY(ud_json_string(j, &n));
    for (uint32_t i = 0; i < b->columns; i++) {
        char id[33];
        ud_id_format(b->column[i].field.identity, id);
        if ((n.length == b->column[i].field.name_length &&
             !memcmp(n.data, b->column[i].field.name, n.length)) ||
            (n.length == 32 && !memcmp(n.data, id, 32))) {
            *out = i;
            return UD_OK;
        }
    }
    return UD_SCHEMA;
}
static ud_status sink_bind(relational_component *r, const ud_batch *const *in) {
    if (!in[0] || in[0]->columns < 5)
        return UD_SCHEMA;
    uint32_t n = in[0]->columns - 4;
    for (uint32_t p = 1; p < 4; p++) {
        if (!in[p] || in[p]->columns != in[0]->columns)
            return UD_SCHEMA;
        for (uint32_t c = 0; c < in[0]->columns; c++)
            if (!ud_field_matches(&in[0]->column[c].field, &in[p]->column[c].field, UD_FIELD_PORT))
                return UD_SCHEMA;
    }
    const char *names[] = {"_before", "_eventId", "_effectiveTime", "_action"};
    for (uint32_t i = 0; i < 4; i++)
        if (in[0]->column[n + i].field.name_length != strlen(names[i]) ||
            memcmp(in[0]->column[n + i].field.name, names[i], strlen(names[i])))
            return UD_SCHEMA;
    UD_TRY(ud_batch_create(r->context.memory, in[0]->schema->fields, n, 0, &r->dimension));
    const ud_json *keys = ud_member(r->config, "businessKeys");
    r->key_count = ud_json_size(keys);
    if (ud_json_kind(keys) != UD_JSON_ARRAY || !r->key_count || r->key_count > 16)
        return UD_SCHEMA;
    r->keys = ud_alloc(r->context.memory, (r->key_count + 3) * sizeof(*r->keys));
    if (!r->keys)
        return UD_RESOURCE;
    for (uint32_t k = 0; k < r->key_count; k++)
        UD_TRY(index(r->dimension, ud_json_at(keys, k), r->keys + k));
    UD_TRY(index(r->dimension, ud_member(r->config, "current"), &r->current));
    if (r->dimension->column[r->current].field.type != UD_BOOL)
        return UD_SCHEMA;
    r->keys[r->key_count] = n + 2;
    r->keys[r->key_count + 1] = n + 1;
    r->keys[r->key_count + 2] = n + 3;
    UD_TRY(ud_json_string(ud_member(r->config, "table"), &r->table));
    ud_operator_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .kind = UD_OPERATOR_SORT,
                                   .max_batch_rows = r->context.max_batch_rows,
                                   .keys = r->keys,
                                   .key_count = r->key_count + 3,
                                   .buffers = r->context.execution->buffers,
                                   .check = (ud_status (*)(void *))ud_activation_check,
                                   .user = r->context.activation,
                                   .partition_count = 1};
    return ud_operator_create(r->context.memory, &options, in, &r->operator);
}
static ud_status sink_configure(void *user, const ud_component_context *c, ud_string text,
                                const ud_batch *const *in, ud_batch **out) {
    (void)user;
    (void)out;
    relational_component r = {.context = *c};
    ud_status s = ud_json_parse(c->memory, text.data, text.length, &r.config, NULL);
    if (!s)
        s = sink_bind(&r, in);
    ud_operator_destroy(r.operator);
    ud_batch_destroy(r.dimension);
    ud_free(r.keys);
    ud_json_destroy(r.config);
    return s;
}
static ud_status sink_prepare(void *user, const ud_component_context *c, ud_string text,
                              const ud_batch *const *in, const ud_batch *const *out,
                              void **instance) {
    (void)user;
    (void)out;
    *instance = NULL;
    relational_component *r = ud_alloc(c->memory, sizeof(*r));
    if (!r)
        return UD_RESOURCE;
    r->context = *c;
    ud_status s = ud_json_parse(c->memory, text.data, text.length, &r->config, NULL);
    if (!s && ud_transaction_active(c->activation))
        s = UD_UNSUPPORTED;
    if (!s)
        s = sink_bind(r, in);
    ud_string connection = {0};
    if (!s)
        s = ud_json_string(ud_member(r->config, "connection"), &connection);
    if (!s)
        s = ud_activation_acquire_connection(c->activation, connection,
                                             UD_CONNECT_CHANGESET | UD_CONNECT_RECEIPTS |
                                                 UD_CONNECT_TRANSACTION | UD_CONNECT_METADATA,
                                             &r->lease);
    if (!s && ud_member(r->config, "provisionReceipts")) {
        const char *flag = NULL;
        uint32_t n = 0;
        s = ud_json_text(ud_member(r->config, "provisionReceipts"), &flag, &n);
        if (!s && ud_json_kind(ud_member(r->config, "provisionReceipts")) != UD_JSON_BOOL)
            s = UD_SCHEMA;
        if (!s && n == 4)
            s = ud_connection_lease_provision(r->lease);
    }
    if (!s) {
        char scope[33];
        s = ud_connection_field_namespace(c->activation, connection, r->table, scope);
        ud_batch *actual = NULL;
        if (!s)
            s = ud_connection_lease_schema(r->lease, r->table, (ud_string){scope, 32}, &actual);
        for (uint32_t i = 0; !s && i < r->dimension->columns; i++) {
            const ud_field *f = &r->dimension->column[i].field;
            int found = 0;
            for (uint32_t j = 0; j < actual->columns; j++)
                if (f->name_length == actual->column[j].field.name_length &&
                    !memcmp(f->name, actual->column[j].field.name, f->name_length) &&
                    ud_field_matches(f, &actual->column[j].field, UD_FIELD_VALUE_TYPE))
                    found = 1;
            if (!found)
                s = UD_INCOMPATIBLE;
        }
        ud_batch_destroy(actual);
    }
    if (!s) {
        size_t n = c->execution->options.execution_id.length + c->task_id.length +
                   c->pipeline_id.length + c->component_id.length + 8;
        r->job = ud_alloc(c->memory, n);
        if (!r->job)
            s = UD_RESOURCE;
        else
            snprintf(r->job, n, "%.*s/%.*s/%.*s/%.*s",
                     (int)c->execution->options.execution_id.length,
                     c->execution->options.execution_id.data, (int)c->task_id.length,
                     c->task_id.data, (int)c->pipeline_id.length, c->pipeline_id.data,
                     (int)c->component_id.length, c->component_id.data);
    }
    if (s) {
        dispose(r, s);
        return s;
    }
    *instance = r;
    return UD_OK;
}
static ud_status sink_next(relational_component *r, const ud_batch **b, uint32_t *row) {
    if (r->pending && r->pending_row == r->pending->rows) {
        ud_batch_destroy(r->pending);
        r->pending = NULL;
    }
    if (!r->pending) {
        UD_TRY(ud_operator_pull(r->operator, &r->pending, NULL));
        r->pending_row = 0;
    }
    *b = r->pending;
    *row = r->pending_row;
    return UD_OK;
}
static ud_status sink_apply(relational_component *r) {
    const ud_batch *b;
    uint32_t row;
    UD_TRY(sink_next(r, &b, &row));
    uint32_t n = r->dimension->columns;
    ud_value id, time, action;
    UD_TRY(ud_batch_get(b, row, n + 1, &id));
    UD_TRY(ud_batch_get(b, row, n + 2, &time));
    UD_TRY(ud_batch_get(b, row, n + 3, &action));
    if (id.type != UD_UTF8 || id.text_length != 32 || action.integer < 1 || action.integer > 4 ||
        action.integer == 4)
        return UD_SCHEMA;
    char event[33];
    memcpy(event, id.text, 32);
    event[32] = 0;
    int pair = action.integer == 3;
    uint32_t rows = pair ? 2 : 1;
    ud_batch *after = NULL, *before = NULL;
    UD_TRY(ud_batch_create(r->context.memory, r->dimension->schema->fields, n, rows, &after));
    ud_field *f = ud_alloc(r->context.memory, n * sizeof(*f));
    ud_value *values = ud_alloc(r->context.memory, n * sizeof(*values));
    ud_status s = f && values ? UD_OK : UD_RESOURCE;
    for (uint32_t i = 0; !s && i < n; i++) {
        f[i] = r->dimension->column[i].field;
        f[i].nullable = 1;
    }
    if (!s)
        s = ud_batch_create(r->context.memory, f, n, rows, &before);
    ud_free(f);
    int32_t operations[2] = {action.integer == 1 ? UD_CHANGE_INSERT : UD_CHANGE_UPDATE,
                             UD_CHANGE_INSERT};
    for (uint32_t k = 0; !s && k < rows; k++) {
        if (k) {
            s = sink_next(r, &b, &row);
            ud_value next_id, next_time, next_action;
            if (!s)
                s = ud_batch_get(b, row, n + 1, &next_id);
            if (!s)
                s = ud_batch_get(b, row, n + 2, &next_time);
            if (!s)
                s = ud_batch_get(b, row, n + 3, &next_action);
            if (!s && (next_id.text_length != 32 || memcmp(next_id.text, event, 32) ||
                       next_time.integer != time.integer || next_action.integer != 4))
                s = UD_SCHEMA;
        }
        for (uint32_t i = 0; !s && i < n; i++)
            s = ud_batch_get(b, row, i, values + i);
        if (!s)
            s = ud_batch_append(after, values, n);
        if (!s)
            s = ud_batch_copy_row_info(after, k, b, row);
        ud_value pre;
        if (!s)
            s = ud_batch_get(b, row, n, &pre);
        if (!s && operations[k] == UD_CHANGE_UPDATE && (pre.is_null || pre.child_count != n))
            s = UD_SCHEMA;
        for (uint32_t i = 0; !s && i < n; i++)
            values[i] = pre.is_null
                            ? (ud_value){.type = r->dimension->column[i].field.type, .is_null = 1}
                            : pre.children[i];
        if (!s)
            s = ud_batch_append(before, values, n);
        if (!s)
            r->pending_row++;
    }
    if (!s)
        s = ud_batch_seal(after);
    if (!s)
        s = ud_batch_seal(before);
    ud_write_options options = {.struct_size = sizeof(options),
                                .abi_version = UD_ABI_VERSION,
                                .mode = UD_WRITE_CHANGESET,
                                .table = r->table,
                                .job = {r->job, (uint32_t)strlen(r->job)},
                                .batch_id = {event, 32},
                                .source_position = {event, 32},
                                .keys = r->keys,
                                .key_count = r->key_count,
                                .operations = operations,
                                .before = before,
                                .current_column = r->current};
    if (!s)
        s = ud_pipeline_write(&r->context, r->lease, &options, after);
    if (s == UD_FENCED)
        r->conflicts++;
    if (!s)
        r->committed += rows;
    ud_free(values);
    ud_batch_destroy(after);
    ud_batch_destroy(before);
    return s ? s : UD_MORE;
}
static ud_status sink_step(void *ptr, const ud_component_event *event) {
    relational_component *r = ptr;
    UD_TRY(ud_activation_check(r->context.activation));
    if (event->kind == UD_COMPONENT_DATA)
        return ud_operator_push(r->operator, 0, event->batch);
    if (event->kind == UD_COMPONENT_INPUT_END) {
        r->ended++;
        return r->ended == 4 ? ud_operator_finish_input(r->operator, 0) : UD_OK;
    }
    return sink_apply(r);
}
static ud_status sink_diagnostics(void *ptr, char **out, uint32_t *length) {
    relational_component *r = ptr;
    char b[160];
    int n =
        snprintf(b, sizeof(b),
                 "{\"committedChanges\":%llu,\"optimisticConflicts\":%llu,\"atomicSession\":true}",
                 (unsigned long long)r->committed, (unsigned long long)r->conflicts);
    *out = ud_strdup_n(r->context.memory, b, (size_t)n);
    *length = (uint32_t)n;
    return *out ? UD_OK : UD_RESOURCE;
}
typedef struct event_time {
    ud_component_context context;
    uint32_t field;
    int64_t lag, maximum, emitted;
    int pending;
} event_time;
static ud_status time_options(const ud_component_context *c, ud_string text, const ud_batch *input,
                              event_time *t) {
    ud_json *config = NULL;
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, &config, NULL));
    ud_status s = ud_compute_field_index(input, ud_member(config, "field"), &t->field);
    uint64_t lag = 0;
    if (!s)
        s = ud_json_uint(ud_member(config, "lagMicros"), &lag);
    if (!s && (lag > INT64_MAX || input->column[t->field].field.nullable ||
               (input->column[t->field].field.type != UD_TIMESTAMP &&
                input->column[t->field].field.type != UD_INT64)))
        s = UD_SCHEMA;
    t->lag = (int64_t)lag;
    t->context = *c;
    t->maximum = t->emitted = INT64_MIN;
    ud_json_destroy(config);
    return s;
}
static ud_status time_configure(void *u, const ud_component_context *c, ud_string text,
                                const ud_batch *const *in, ud_batch **out) {
    (void)u;
    event_time t = {0};
    UD_TRY(time_options(c, text, in[0], &t));
    return ud_batch_create(c->memory, in[0]->schema->fields, in[0]->columns, 0, out);
}
static ud_status time_prepare(void *u, const ud_component_context *c, ud_string text,
                              const ud_batch *const *in, const ud_batch *const *out,
                              void **instance) {
    (void)u;
    (void)out;
    *instance = NULL;
    event_time *t = ud_alloc(c->memory, sizeof(*t));
    if (!t)
        return UD_RESOURCE;
    ud_status s = time_options(c, text, in[0], t);
    if (s) {
        ud_free(t);
        return s;
    }
    *instance = t;
    return UD_OK;
}
static ud_status time_step(void *instance, const ud_component_event *event) {
    event_time *t = instance;
    UD_TRY(ud_activation_check(t->context.activation));
    if (event->kind == UD_COMPONENT_DATA) {
        for (uint32_t row = 0; row < event->batch->rows; row++) {
            ud_value value;
            UD_TRY(ud_batch_get(event->batch, row, t->field, &value));
            if (value.is_null || value.is_absent)
                return UD_SCHEMA;
            if (value.integer > t->maximum)
                t->maximum = value.integer;
        }
        UD_TRY(t->context.emit(t->context.emit_user, 0, (ud_batch *)event->batch));
        t->pending = 1;
        return UD_MORE;
    }
    if (event->kind == UD_COMPONENT_INPUT_END)
        return UD_END;
    if (event->kind == UD_COMPONENT_POLL && t->pending) {
        t->pending = 0;
        int64_t mark = t->maximum < INT64_MIN + t->lag ? INT64_MIN : t->maximum - t->lag;
        if (mark > t->emitted) {
            ud_component_event wm = {.struct_size = sizeof(wm),
                                     .abi_version = UD_ABI_VERSION,
                                     .kind = UD_COMPONENT_WATERMARK,
                                     .watermark = mark};
            UD_TRY(t->context.emit_event(t->context.emit_user, 0, &wm));
            t->emitted = mark;
            return UD_MORE;
        }
    }
    return UD_OK;
}
static void time_dispose(void *instance, ud_status outcome) {
    (void)outcome;
    ud_free(instance);
}
static ud_status time_checkpoint(void *instance, ud_batch **out) {
    event_time *t = instance;
    if (t->pending)
        return UD_BUSY;
    ud_field field = {.name = "maximum", .name_length = 7, .type = UD_INT64};
    ud_value value = {.type = UD_INT64, .integer = t->maximum};
    UD_TRY(ud_batch_create(t->context.memory, &field, 1, 1, out));
    ud_status s = ud_batch_append(*out, &value, 1);
    if (!s)
        s = ud_batch_seal(*out);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
static ud_status time_restore(void *instance, const ud_batch *state) {
    event_time *t = instance;
    ud_value value;
    if (state->rows != 1 || state->columns != 1)
        return UD_CORRUPT;
    UD_TRY(ud_batch_get(state, 0, 0, &value));
    if (value.type != UD_INT64 || value.is_null)
        return UD_CORRUPT;
    t->maximum = value.integer;
    t->emitted = t->maximum < INT64_MIN + t->lag ? INT64_MIN : t->maximum - t->lag;
    return UD_OK;
}
static ud_status register_event_time(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"properties\":{\"field\":{\"anyOf\":[{\"type\":\"integer\","
        "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
        "\"lagMicros\":{\"type\":\"integer\",\"minimum\":0,\"default\":0}},\"required\":[\"field\"]"
        ",\"additionalProperties\":false}";
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = {"eventTime", 9},
                                 .name = {"Event time / Watermark", 22},
                                 .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
                                 .configuration_schema = {schema, sizeof(schema) - 1},
                                 .category = UD_COMPONENT_TRANSFORM,
                                 .inputs = 1,
                                 .outputs = 1,
                                 .synchronous_input = 0,
                                 .error_output = -1,
                                 .capabilities =
                                     UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE |
                                     UD_COMPONENT_PREVIEW_SAFE | UD_COMPONENT_STATEFUL |
                                     UD_COMPONENT_STREAM_SAFE | UD_COMPONENT_CONTROL_EVENTS |
                                     UD_COMPONENT_WATERMARK_POLICY,
                                 .configure = time_configure,
                                 .prepare = time_prepare,
                                 .step = time_step,
                                 .finish = finish,
                                 .dispose = time_dispose,
                                 .state_version = 1,
                                 .checkpoint = time_checkpoint,
                                 .restore = time_restore};
    return ud_registry_register(registry, &d);
}

ud_status ud_relational_register(ud_registry *registry) {
    UD_TRY(register_event_time(registry));
    static const struct {
        const char *id;
        int kind;
        const char *name;
    } entries[] = {{"eventWindow", UD_OPERATOR_EVENT_WINDOW, NULL},
                   {"eventDeduplicate", UD_OPERATOR_EVENT_DEDUPLICATE, "Event-Time Deduplication"},
                   {"eventJoin", UD_OPERATOR_EVENT_JOIN, "Event-Time Inner Join"},
                   {"merge", UD_OPERATOR_MERGE, NULL},
                   {"mergeJoin", UD_OPERATOR_MERGE_JOIN, NULL},
                   {"pivot", UD_OPERATOR_PIVOT, NULL},
                   {"unpivot", UD_OPERATOR_UNPIVOT, NULL},
                   {"rowSampling", UD_OPERATOR_ROW_SAMPLING, NULL},
                   {"percentageSampling", UD_OPERATOR_PERCENTAGE_SAMPLING, NULL},
                   {"scd", UD_OPERATOR_SCD, NULL},
                   {"asOfJoin", UD_OPERATOR_ASOF_JOIN, NULL},
                   {"temporalJoin", UD_OPERATOR_TEMPORAL_JOIN, NULL},
                   {"diffCompare", UD_OPERATOR_DIFF_COMPARE, NULL}};
    for (uint32_t i = 0; i < sizeof(entries) / sizeof(*entries); i++) {
        int k = entries[i].kind;
        ud_string schema = {0};
        UD_TRY(ud_operator_configuration_schema(k, &schema));
        const char *name = entries[i].name ? entries[i].name : entries[i].id;
        ud_component_descriptor d = {
            .struct_size = sizeof(d),
            .abi_version = UD_ABI_VERSION,
            .id = {entries[i].id, (uint32_t)strlen(entries[i].id)},
            .name = {name, (uint32_t)strlen(name)},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = schema,
            .category = UD_COMPONENT_TRANSFORM,
            .inputs = k == UD_OPERATOR_MERGE || k == UD_OPERATOR_MERGE_JOIN ||
                              k == UD_OPERATOR_ASOF_JOIN || k == UD_OPERATOR_TEMPORAL_JOIN ||
                              k == UD_OPERATOR_DIFF_COMPARE || k == UD_OPERATOR_EVENT_JOIN
                          ? 2
                          : 1,
            .outputs = (uint32_t)outputs(k),
            .synchronous_input = -1,
            .error_output = k == UD_OPERATOR_DIFF_COMPARE                                  ? 4
                            : k == UD_OPERATOR_ASOF_JOIN || k == UD_OPERATOR_TEMPORAL_JOIN ? 1
                            : k == UD_OPERATOR_PIVOT                                       ? 1
                            : k == UD_OPERATOR_SCD                                         ? 5
                                                                                           : -1,
            .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE |
                            UD_COMPONENT_PREVIEW_SAFE | UD_COMPONENT_STATEFUL |
                            (k == UD_OPERATOR_ROW_SAMPLING ? UD_COMPONENT_BLOCKING
                                                           : UD_COMPONENT_PARTIALLY_BLOCKING),
            .optional_outputs = k == UD_OPERATOR_DIFF_COMPARE ? UINT64_C(16)
                                : k == UD_OPERATOR_ASOF_JOIN || k == UD_OPERATOR_TEMPORAL_JOIN
                                    ? UINT64_C(2)
                                : k == UD_OPERATOR_PIVOT ? UINT64_C(2)
                                : k == UD_OPERATOR_SCD   ? UINT64_C(33)
                                                         : 0,
            .user = (void *)(intptr_t)k,
            .configure = configure,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .dispose = dispose,
            .diagnostics = diagnostics,
            .describe_plan = describe,
            .input_demand = demand,
            .state_version = 1,
            .checkpoint = checkpoint,
            .restore = restore_state};
        if (k >= UD_OPERATOR_EVENT_WINDOW)
            d.capabilities |= UD_COMPONENT_CONTROL_EVENTS | UD_COMPONENT_STREAM_SAFE;
        if (k == UD_OPERATOR_EVENT_WINDOW)
            d.optional_outputs |= UINT64_C(2);
        UD_TRY(ud_registry_register(registry, &d));
    }
    ud_component_descriptor d = {
        .struct_size = sizeof(d),
        .abi_version = UD_ABI_VERSION,
        .id = {"scd.destination", 15},
        .name = {"Atomic SCD Destination", 22},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
        .configuration_schema =
            {"{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\","
             "\"minLength\":1,\"x-role\":\"resource-reference\"},\"table\":{\"type\":\"string\","
             "\"minLength\":1},\"businessKeys\":{\"type\":\"array\",\"items\":{\"anyOf\":[{"
             "\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-"
             "role\":\"field-selector\"},\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},"
             "\"current\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
             "\"minLength\":1}],\"x-role\":\"field-selector\"},\"provisionReceipts\":{\"type\":"
             "\"boolean\",\"default\":false}},\"required\":[\"connection\",\"table\","
             "\"businessKeys\",\"current\"],\"additionalProperties\":false}",
             588},
        .category = UD_COMPONENT_DESTINATION,
        .inputs = 4,
        .synchronous_input = -1,
        .error_output = -1,
        .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE |
                        UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING,
        .configure = sink_configure,
        .prepare = sink_prepare,
        .step = sink_step,
        .finish = finish,
        .dispose = dispose,
        .diagnostics = sink_diagnostics,
        .effects = UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_TRANSACTIONAL | UD_EFFECT_RECEIPTED |
                   UD_EFFECT_COMMIT_UNKNOWN};
    return ud_registry_register(registry, &d);
}

ud_status ud_component_operator_options(const ud_component_context *c, ud_operator_options *o) {
    o->record_state = c->execution->journal != NULL;
    ud_buffer_options limits = {0};
    UD_TRY(ud_execution_buffer_options(c->execution, &limits));
    uint32_t concurrency = ud_execution_concurrency(c->activation);
    if (!concurrency)
        return UD_SCHEMA;
    o->memory_budget = limits.working_limit / (2u * concurrency);
    o->spill_budget = limits.spill_limit;
    o->input_count = c->input_count;
    o->errors = c->errors;
    for (uint32_t i = 0; i < c->input_count && i < 64; i++)
        if (c->input_plans)
            o->estimated_rows[i] = c->input_plans[i].estimated_rows;
    return UD_OK;
}
ud_status ud_component_operator_plan(const ud_operator *o, ud_component_plan *out) {
    ud_physical_plan_info info = {.struct_size = sizeof(info), .abi_version = UD_ABI_VERSION};
    UD_TRY(ud_operator_physical_plan(o, &info));
    out->sort = info.sort;
    out->partitions = info.partitions;
    out->algorithm = info.algorithm;
    out->ordering = info.sort.count                            ? UD_ORDER_GLOBAL
                    : info.algorithm == UD_PHYSICAL_GRACE_HASH ? UD_ORDER_PARTITION
                                                               : UD_ORDER_UNORDERED;
    out->estimated_rows = info.estimated_rows;
    out->row_width = info.row_width;
    out->memory_budget = info.memory_budget;
    out->spill_budget = info.spill_budget;
    out->estimated_cost = info.estimated_cost;
    char *tree = NULL;
    uint32_t length = 0;
    UD_TRY(ud_operator_explain(o, &tree, &length));
    out->physical_tree = (ud_string){tree, length};
    return UD_OK;
}
