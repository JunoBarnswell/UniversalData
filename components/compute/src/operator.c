#include "ud/operator.h"
#include "../internal.h"

typedef struct block {
    ud_buffer_block *buffer;
    struct block *next;
} block;
typedef struct run {
    block *head, *tail;
    struct run *next;
} run;
typedef struct cursor {
    block *next;
    const ud_batch *batch;
    ud_buffer_view *view;
    uint32_t row;
} cursor;
struct ud_operator {
    ud_context *ctx;
    ud_operator_options options;
    ud_batch *inputs[2], *schema;
    uint32_t *keys, ports, ended, run_count;
    uint8_t port_ended[2];
    run *runs[2], *tail[2];
    cursor merge[8], left, right;
    uint32_t merge_count;
    int prepared, failed;
    block *left_block, *right_start;
    ud_operator *group_sort;
    ud_accumulator *group;
    ud_batch *pending;
    uint32_t pending_row;
    ud_value *group_keys;
    int group_active, input_done, saw_group;
    ud_value window_value;
    uint64_t window_rows;
};
static ud_status check(ud_operator *o) {
    if (o->failed)
        return UD_INVALID;
    if (ud_is_cancelled(o->ctx))
        return UD_CANCELLED;
    return o->options.check ? o->options.check(o->options.user) : UD_OK;
}
static ud_status copy_schema(ud_context *ctx, const ud_batch *input, uint32_t capacity,
                             ud_batch **out) {
    ud_field *fields = ud_alloc(ctx, input->columns * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < input->columns; i++)
        fields[i] = input->column[i].field;
    ud_status s = ud_batch_create(ctx, fields, input->columns, capacity, out);
    ud_free(fields);
    return s;
}
static ud_status compatible(const ud_batch *a, const ud_batch *b) {
    if (!a || !b || a->columns != b->columns)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < a->columns; i++)
        if (!ud_field_matches(&a->column[i].field, &b->column[i].field, UD_FIELD_PORT))
            return UD_SCHEMA;
    return UD_OK;
}
static ud_status row_copy(ud_batch *target, const ud_batch *source, uint32_t row,
                          ud_value *values) {
    for (uint32_t i = 0; i < source->columns; i++)
        UD_TRY(ud_batch_get(source, row, i, values + i));
    return ud_batch_append(target, values, source->columns);
}
static ud_status store(ud_operator *o, run *r, ud_batch *batch) {
    block *b = ud_alloc(o->ctx, sizeof(*b));
    if (!b)
        return UD_RESOURCE;
    ud_status s = ud_batch_seal(batch);
    if (!s)
        s = ud_buffer_store(o->options.buffers, batch, &b->buffer);
    if (s) {
        ud_free(b);
        return s;
    }
    if (r->tail)
        r->tail->next = b;
    else
        r->head = b;
    r->tail = b;
    return UD_OK;
}
static ud_status clear_run(run *r) {
    while (r->head) {
        block *b = r->head;
        UD_TRY(ud_buffer_release(b->buffer));
        r->head = b->next;
        ud_free(b);
    }
    ud_free(r);
    return UD_OK;
}
static ud_status advance(ud_operator *o, cursor *c) {
    while (!c->batch || c->row >= c->batch->rows) {
        ud_buffer_view_release(c->view);
        c->view = NULL;
        c->batch = NULL;
        c->row = 0;
        if (!c->next)
            return UD_END;
        block *b = c->next;
        c->next = b->next;
        UD_TRY(check(o));
        UD_TRY(ud_buffer_read(b->buffer, &c->view));
        c->batch = ud_buffer_view_batch(c->view);
        UD_TRY(compatible(o->inputs[0], c->batch));
    }
    return UD_OK;
}
static int compare(ud_operator *o, const cursor *a, const cursor *b) {
    for (uint32_t i = 0; i < o->options.key_count; i++) {
        ud_value x, y;
        ud_batch_get(a->batch, a->row, o->keys[i], &x);
        ud_batch_get(b->batch, b->row, o->keys[i], &y);
        int order = ud_value_compare(&x, &y);
        if (order)
            return o->options.descending ? -order : order;
    }
    return 0;
}
static ud_status merge_batch(ud_operator *o, cursor *streams, uint32_t count, ud_batch **out) {
    UD_TRY(copy_schema(o->ctx, o->schema, o->options.max_batch_rows, out));
    ud_value *values = ud_alloc(o->ctx, o->schema->columns * sizeof(*values));
    ud_status s = values ? UD_OK : UD_RESOURCE;
    while (!s && (*out)->rows < o->options.max_batch_rows) {
        s = check(o);
        if (s)
            break;
        int best = -1;
        for (uint32_t i = 0; !s && i < count; i++) {
            ud_status next = advance(o, streams + i);
            if (next == UD_END)
                continue;
            if (next) {
                s = next;
                break;
            }
            if (best < 0 || compare(o, streams + i, streams + best) < 0)
                best = (int)i;
        }
        if (s || best < 0)
            break;
        s = row_copy(*out, streams[best].batch, streams[best].row++, values);
    }
    ud_free(values);
    if (s || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_END;
    }
    return ud_batch_seal(*out);
}
static ud_status prepare_merge(ud_operator *o) {
    while (o->run_count > 8) {
        run *current = o->runs[0], *head = NULL, *tail = NULL;
        uint32_t groups = 0;
        ud_status s = UD_OK;
        while (current && !s) {
            cursor streams[8] = {{0}};
            uint32_t count = 0;
            while (current && count < 8) {
                streams[count++].next = current->head;
                current = current->next;
            }
            run *merged = ud_alloc(o->ctx, sizeof(*merged));
            if (!merged)
                s = UD_RESOURCE;
            else {
                if (tail)
                    tail->next = merged;
                else
                    head = merged;
                tail = merged;
                groups++;
            }
            while (!s) {
                ud_batch *batch = NULL;
                s = merge_batch(o, streams, count, &batch);
                if (s == UD_END) {
                    s = UD_OK;
                    break;
                }
                if (!s)
                    s = store(o, merged, batch);
                ud_batch_destroy(batch);
            }
            for (uint32_t i = 0; i < count; i++)
                ud_buffer_view_release(streams[i].view); streams[i].view = NULL;
        }
        if (s) {
            /* Retain every block for the common destructor, including failed
             * pass outputs. No file/ownership list disappears on failure. */
            if (tail)
                tail->next = o->runs[0];
            if (head)
                o->runs[0] = head;
            return s;
        }
        while (o->runs[0]) {
            run *r = o->runs[0], *next = r->next;
            s = clear_run(r);
            if (s) {
                tail->next = r;
                o->runs[0] = head;
                return s;
            }
            o->runs[0] = next;
        }
        o->runs[0] = head;
        o->tail[0] = tail;
        o->run_count = groups;
    }
    for (run *r = o->runs[0]; r; r = r->next)
        o->merge[o->merge_count++].next = r->head;
    return UD_OK;
}
ud_status ud_operator_create(ud_context *ctx, const ud_operator_options *options,
                             const ud_batch *const *inputs, ud_operator **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !options || !inputs || !inputs[0])
        return UD_INVALID;
    if (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (options->kind < UD_OPERATOR_SORT || options->kind > UD_OPERATOR_WINDOW ||
        !options->max_batch_rows || options->max_batch_rows > 65536 || options->descending < 0 ||
        options->descending > 1)
        return UD_INVALID;
    ud_operator *o = ud_alloc(ctx, sizeof(*o));
    if (!o)
        return UD_RESOURCE;
    o->ctx = ctx;
    o->options = *options;
    o->options.aggregation = NULL;
    o->ports =
        options->kind == UD_OPERATOR_INNER_JOIN || options->kind == UD_OPERATOR_LOOKUP ? 2 : 1;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < o->ports; i++) {
        if (!inputs[i] || inputs[i]->ctx != ctx)
            s = UD_INVALID;
        else
            s = copy_schema(ctx, inputs[i], 0, o->inputs + i);
    }
    if (!s && options->kind == UD_OPERATOR_AGGREGATE) {
        if (!options->aggregation || !options->aggregation->agg_n)
            s = UD_SCHEMA;
        if (!s)
            s = ud_plan_check_batch(options->aggregation, o->inputs[0]);
        if (!s)
            s = ud_plan_retain(options->aggregation);
        if (!s)
            o->options.aggregation = options->aggregation;
        if (!s)
            s = copy_schema(ctx, options->aggregation->output_schema, 0, &o->schema);
        ud_operator_options sort = *options;
        sort.kind = UD_OPERATOR_SORT;
        sort.aggregation = NULL;
        sort.keys = options->aggregation ? options->aggregation->keys : NULL;
        sort.key_count = options->aggregation ? options->aggregation->key_n : 0;
        if (!s)
            s = ud_operator_create(ctx, &sort, inputs, &o->group_sort);
        o->group_keys = !s ? ud_alloc(ctx, (sort.key_count + 1) * sizeof(*o->group_keys)) : NULL;
        if (!s && !o->group_keys)
            s = UD_RESOURCE;
    } else if (!s && options->kind == UD_OPERATOR_WINDOW) {
        if (options->partition_key_count > options->key_count || !options->window_name.length ||
            !options->window_name.data || options->window_function < UD_WINDOW_ROW_NUMBER ||
            options->window_function > UD_WINDOW_RUNNING_SUM ||
            (options->window_function == UD_WINDOW_RUNNING_SUM &&
             options->window_column >= inputs[0]->columns))
            s = UD_SCHEMA;
        ud_field spec = {.type = UD_INT64};
        if (!s && options->window_function == UD_WINDOW_RUNNING_SUM) {
            spec = inputs[0]->column[options->window_column].field;
            if (spec.type != UD_INT64 && spec.type != UD_FLOAT64 && spec.type != UD_DECIMAL128)
                s = UD_SCHEMA;
            spec.nullable = 1;
        }
        ud_field *fields = !s ? ud_alloc(ctx, (inputs[0]->columns + 1) * sizeof(*fields)) : NULL;
        if (!s && !fields)
            s = UD_RESOURCE;
        ud_field derived = {0};
        ud_bytes signature = {.ctx = ctx};
        ud_field *dependencies = NULL;
        ud_column_id *origins = NULL;
        uint32_t origin_count = 0, dependency_count = 0;
        if (!s &&
            (options->key_count > inputs[0]->columns || (options->key_count && !options->keys)))
            s = UD_SCHEMA;
        if (!s) {
            dependencies = ud_alloc(ctx, (options->key_count + 1) * sizeof(*dependencies));
            if (!dependencies)
                s = UD_RESOURCE;
        }
        for (uint32_t i = 0; !s && i < inputs[0]->columns; i++) {
            char id[33];
            ud_column_id_format(inputs[0]->column[i].field.identity, id);
            s = ud_bytes_put(&signature, id, 32);
        }
        const uint64_t semantics[] = {options->window_function, options->window_column,
                                      options->partition_key_count, options->descending,
                                      options->key_count};
        for (uint32_t i = 0; !s && i < 5; i++)
            s = ud_put_u64(&signature, semantics[i]);
        for (uint32_t i = 0; !s && i < options->key_count; i++) {
            if (options->keys[i] >= inputs[0]->columns)
                s = UD_SCHEMA;
            if (!s)
                s = ud_put_u64(&signature, options->keys[i]);
            if (!s)
                dependencies[dependency_count++] = inputs[0]->column[options->keys[i]].field;
        }
        if (!s && options->window_function == UD_WINDOW_RUNNING_SUM)
            dependencies[dependency_count++] = inputs[0]->column[options->window_column].field;
        if (!s)
            s = ud_lineage_merge(ctx, dependencies, dependency_count, &origins, &origin_count);
        if (!s) {
            uint8_t digest[32];
            char scope[65];
            static const char hex[] = "0123456789abcdef";
            ud_hash(signature.data, signature.size, digest);
            for (uint32_t i = 0; i < 32; i++) {
                scope[2 * i] = hex[digest[i] >> 4];
                scope[2 * i + 1] = hex[digest[i] & 15];
            }
            scope[64] = 0;
            spec.lineage = origins;
            spec.lineage_count = origin_count;
            s = ud_field_derive(ctx, &spec, (ud_string){scope, 64}, options->window_name, &derived);
        }
        ud_free(origins);
        ud_free(dependencies);
        ud_bytes_clear(&signature);
        if (!s) {
            for (uint32_t i = 0; i < inputs[0]->columns; i++)
                fields[i] = inputs[0]->column[i].field;
            fields[inputs[0]->columns] = derived;
            s = ud_batch_create(ctx, fields, inputs[0]->columns + 1, 0, &o->schema);
        }
        ud_field_clear(&derived);
        ud_free(fields);
        ud_operator_options sort = *options;
        sort.kind = UD_OPERATOR_SORT;
        if (!s)
            s = ud_operator_create(ctx, &sort, inputs, &o->group_sort);
        o->group_keys =
            !s ? ud_alloc(ctx, (options->partition_key_count + 1) * sizeof(*o->group_keys)) : NULL;
        if (!s && !o->group_keys)
            s = UD_RESOURCE;
        o->window_value = (ud_value){
            .type = spec.type, .is_null = 1, .precision = spec.precision, .scale = spec.scale};
    } else if (!s && o->ports == 2) {
        if (!inputs[1] || options->key_count != 1 || !options->keys ||
            options->keys[0] >= inputs[0]->columns || options->right_key >= inputs[1]->columns)
            s = UD_SCHEMA;
        if (!s &&
            !ud_field_matches(&inputs[0]->column[options->keys[0]].field,
                              &inputs[1]->column[options->right_key].field, UD_FIELD_VALUE_TYPE))
            s = UD_SCHEMA;
        if (!s)
            s = ud_compute_join_schema(ctx, inputs[0], inputs[1], &o->schema);
    } else if (!s)
        s = copy_schema(ctx, inputs[0], 0, &o->schema);
    if (!s && options->kind != UD_OPERATOR_AGGREGATE) {
        if (options->key_count > inputs[0]->columns || (options->key_count && !options->keys))
            s = UD_SCHEMA;
        o->keys = ud_alloc(ctx, (options->key_count + 1) * sizeof(*o->keys));
        if (!o->keys)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < options->key_count; i++) {
            if (options->keys[i] >= inputs[0]->columns)
                s = UD_SCHEMA;
            else
                o->keys[i] = options->keys[i];
        }
    }
    if (s) {
        (void)ud_operator_destroy(o);
        return s;
    }
    *out = o;
    return UD_OK;
}
ud_status ud_operator_schema(const ud_operator *o, ud_batch **out) {
    return o && out ? copy_schema(o->ctx, o->schema, 0, out) : UD_INVALID;
}
ud_status ud_operator_push(ud_operator *o, uint32_t port, const ud_batch *input) {
    if (!o || port >= o->ports || o->port_ended[port] || !input || !input->sealed ||
        !o->options.buffers || input->rows > o->options.max_batch_rows)
        return UD_INVALID;
    UD_TRY(check(o));
    UD_TRY(compatible(o->inputs[port], input));
    if (o->group_sort)
        return ud_operator_push(o->group_sort, 0, input);
    run *r = o->ports == 1 ? ud_alloc(o->ctx, sizeof(*r)) : o->runs[port];
    if (o->ports == 1 && !r)
        return UD_RESOURCE;
    if (!r) {
        r = ud_alloc(o->ctx, sizeof(*r));
        if (!r)
            return UD_RESOURCE;
        o->runs[port] = o->tail[port] = r;
    }
    ud_batch *sorted = NULL;
    ud_status s = UD_OK;
    if (o->ports == 1 && o->options.key_count) {
        const ud_batch *current = input;
        for (uint32_t i = o->options.key_count; !s && i; i--) {
            ud_batch *next = NULL;
            s = ud_compute_sort(o->ctx, current, o->keys[i - 1], o->options.descending, &next);
            ud_batch_destroy(sorted);
            sorted = next;
            current = sorted;
        }
    }
    if (!s)
        s = store(o, r, sorted ? sorted : (ud_batch *)input);
    ud_batch_destroy(sorted);
    if (o->ports == 1) {
        if (o->tail[0])
            o->tail[0]->next = r;
        else
            o->runs[0] = r;
        o->tail[0] = r;
        o->run_count++;
    }
    if (s)
        o->failed = 1;
    return s;
}
ud_status ud_operator_finish_input(ud_operator *o, uint32_t port) {
    if (!o || port >= o->ports || o->port_ended[port])
        return UD_INVALID;
    UD_TRY(check(o));
    o->port_ended[port] = 1;
    o->ended++;
    return o->group_sort ? ud_operator_finish_input(o->group_sort, 0) : UD_OK;
}
static ud_status join_pull(ud_operator *o, ud_batch **out) {
    UD_TRY(copy_schema(o->ctx, o->schema, o->options.max_batch_rows, out));
    ud_value *values = ud_alloc(o->ctx, o->schema->columns * sizeof(*values));
    ud_status s = values ? UD_OK : UD_RESOURCE;
    while (!s && (*out)->rows < o->options.max_batch_rows) {
        s = check(o);
        if (s)
            break;
        if (!o->left.batch || o->left.row >= o->left.batch->rows) {
            ud_buffer_view_release(o->left.view); o->left.view = NULL;
            o->left.batch = NULL;
            if (!o->left_block)
                break;
            block *b = o->left_block;
            o->left_block = b->next;
            s = ud_buffer_read(b->buffer, &o->left.view);
            o->left.batch = ud_buffer_view_batch(o->left.view);
            o->left.row = 0;
            if (!s)
                s = compatible(o->inputs[0], o->left.batch);
            o->right.next = o->right_start;
            o->right.row = 0;
            ud_buffer_view_release(o->right.view); o->right.view = NULL;
            o->right.batch = NULL;
        }
        if (s)
            break;
        ud_value left;
        s = ud_batch_get(o->left.batch, o->left.row, o->keys[0], &left);
        if (s)
            break;
        if (left.is_null) {
            o->left.row++;
            continue;
        }
        while (!s && (!o->right.batch || o->right.row >= o->right.batch->rows)) {
            ud_buffer_view_release(o->right.view); o->right.view = NULL;
            o->right.batch = NULL;
            o->right.row = 0;
            if (!o->right.next)
                break;
            block *b = o->right.next;
            o->right.next = b->next;
            s = ud_buffer_read(b->buffer, &o->right.view);
            o->right.batch = ud_buffer_view_batch(o->right.view);
            if (!s)
                s = compatible(o->inputs[1], o->right.batch);
        }
        if (s)
            break;
        if (!o->right.batch) {
            o->left.row++;
            o->right.next = o->right_start;
            continue;
        }
        ud_value right;
        uint32_t row = o->right.row++;
        s = ud_batch_get(o->right.batch, row, o->options.right_key, &right);
        if (s || right.is_null ||
            ud_value_hash(&left, UD_VALUE_HASH_SEED) != ud_value_hash(&right, UD_VALUE_HASH_SEED) ||
            ud_value_compare(&left, &right))
            continue;
        for (uint32_t i = 0; !s && i < o->left.batch->columns; i++)
            s = ud_batch_get(o->left.batch, o->left.row, i, values + i);
        for (uint32_t i = 0; !s && i < o->right.batch->columns; i++)
            s = ud_batch_get(o->right.batch, row, i, values + o->left.batch->columns + i);
        if (!s)
            s = ud_batch_append(*out, values, o->schema->columns);
        if (!s && o->options.kind == UD_OPERATOR_LOOKUP) {
            o->left.row++;
            o->right.next = o->right_start;
            ud_buffer_view_release(o->right.view); o->right.view = NULL;
            o->right.batch = NULL;
            o->right.row = 0;
        }
    }
    ud_free(values);
    if (s || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_END;
    }
    return ud_batch_seal(*out);
}
static ud_status aggregate_pull(ud_operator *o, ud_batch **out) {
    UD_TRY(copy_schema(o->ctx, o->schema, o->options.max_batch_rows, out));
    ud_plan *p = o->options.aggregation;
    ud_value *values = ud_alloc(o->ctx, o->schema->columns * sizeof(*values));
    ud_status s = values ? UD_OK : UD_RESOURCE;
    while (!s && (*out)->rows < o->options.max_batch_rows) {
        s = check(o);
        if (s)
            break;
        if (!o->pending || o->pending_row == o->pending->rows) {
            ud_batch_destroy(o->pending);
            o->pending = NULL;
            o->pending_row = 0;
            if (!o->input_done) {
                s = ud_operator_pull(o->group_sort, &o->pending);
                if (s == UD_END) {
                    s = UD_OK;
                    o->input_done = 1;
                }
            }
        }
        if (s)
            break;
        if (!o->input_done) {
            int accept = 0;
            s = ud_plan_accept(p, o->pending, o->pending_row, &accept);
            if (s)
                break;
            if (!accept) {
                o->pending_row++;
                continue;
            }
        }
        int boundary = o->input_done;
        if (!boundary && o->group_active)
            for (uint32_t i = 0; i < p->key_n; i++) {
                ud_value key;
                s = ud_batch_get(o->pending, o->pending_row, p->keys[i], &key);
                if (s || ud_value_compare(&key, o->group_keys + i)) {
                    boundary = 1;
                    break;
                }
            }
        if (s)
            break;
        if (boundary && o->group_active) {
            ud_batch *result = NULL;
            s = ud_accumulator_snapshot(o->group, &result);
            if (!s && result->rows != 1)
                s = UD_CORRUPT;
            if (!s)
                s = row_copy(*out, result, 0, values);
            ud_batch_destroy(result);
            ud_accumulator_destroy(o->group);
            o->group = NULL;
            for (uint32_t i = 0; i < p->key_n; i++)
                ud_clear_value(o->group_keys + i);
            o->group_active = 0;
            if (s || (*out)->rows == o->options.max_batch_rows)
                break;
        }
        if (o->input_done) {
            if (!p->key_n && !o->saw_group) {
                s = ud_accumulator_create(p, &o->group);
                o->group_active = !s;
                o->saw_group = 1;
                continue;
            }
            break;
        }
        if (!o->group_active) {
            s = ud_accumulator_create(p, &o->group);
            for (uint32_t i = 0; !s && i < p->key_n; i++) {
                ud_value key;
                s = ud_batch_get(o->pending, o->pending_row, p->keys[i], &key);
                if (!s)
                    s = ud_copy_value(o->ctx, &key, o->group_keys + i, 0);
            }
            o->group_active = !s;
            o->saw_group = 1;
        }
        if (!s)
            s = ud_accumulator_add_accepted(o->group, o->pending, o->pending_row++);
    }
    ud_free(values);
    if (s || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_END;
    }
    return ud_batch_seal(*out);
}
static ud_status window_pull(ud_operator *o, ud_batch **out) {
    UD_TRY(copy_schema(o->ctx, o->schema, o->options.max_batch_rows, out));
    ud_value *values = ud_alloc(o->ctx, o->schema->columns * sizeof(*values));
    ud_status s = values ? UD_OK : UD_RESOURCE;
    while (!s && (*out)->rows < o->options.max_batch_rows) {
        s = check(o);
        if (s)
            break;
        if (!o->pending || o->pending_row == o->pending->rows) {
            ud_batch_destroy(o->pending);
            o->pending = NULL;
            o->pending_row = 0;
            s = ud_operator_pull(o->group_sort, &o->pending);
            if (s == UD_END) {
                s = UD_OK;
                break;
            }
        }
        if (s)
            break;
        int boundary = !o->window_rows;
        for (uint32_t i = 0; !s && !boundary && i < o->options.partition_key_count; i++) {
            ud_value key;
            s = ud_batch_get(o->pending, o->pending_row, o->keys[i], &key);
            if (!s && ud_value_compare(&key, o->group_keys + i))
                boundary = 1;
        }
        if (boundary) {
            o->window_rows = 0;
            o->window_value.is_null = 1;
            for (uint32_t i = 0; !s && i < o->options.partition_key_count; i++) {
                ud_value key;
                s = ud_batch_get(o->pending, o->pending_row, o->keys[i], &key);
                ud_clear_value(o->group_keys + i);
                if (!s)
                    s = ud_copy_value(o->ctx, &key, o->group_keys + i, 0);
            }
        }
        if (s)
            break;
        if (o->window_rows == INT64_MAX) {
            s = UD_OVERFLOW;
            break;
        }
        o->window_rows++;
        for (uint32_t i = 0; !s && i < o->pending->columns; i++)
            s = ud_batch_get(o->pending, o->pending_row, i, values + i);
        if (!s && o->options.window_function == UD_WINDOW_RUNNING_SUM) {
            const ud_value *value = values + o->options.window_column;
            if (!value->is_null) {
                if (o->window_value.is_null)
                    o->window_value = *value;
                else
                    s = ud_value_numeric(UD_NUMERIC_ADD, &o->window_value, value,
                                         &o->schema->column[o->pending->columns].field,
                                         &o->window_value);
            }
            values[o->pending->columns] = o->window_value;
        } else
            values[o->pending->columns] =
                (ud_value){.type = UD_INT64, .integer = (int64_t)o->window_rows};
        if (!s)
            s = ud_batch_append(*out, values, o->schema->columns);
        o->pending_row++;
    }
    ud_free(values);
    if (s || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_END;
    }
    return ud_batch_seal(*out);
}
ud_status ud_operator_pull(ud_operator *o, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!o)
        return UD_INVALID;
    UD_TRY(check(o));
    if (o->ended != o->ports)
        return UD_BUSY;
    ud_status s = UD_OK;
    if (o->options.kind == UD_OPERATOR_WINDOW)
        s = window_pull(o, out);
    else if (o->group_sort)
        s = aggregate_pull(o, out);
    else if (o->ports == 2) {
        if (!o->prepared) {
            o->left_block = o->runs[0] ? o->runs[0]->head : NULL;
            o->right_start = o->runs[1] ? o->runs[1]->head : NULL;
            o->prepared = 1;
        }
        s = join_pull(o, out);
    } else {
        if (!o->prepared) {
            s = prepare_merge(o);
            o->prepared = !s;
        }
        if (!s)
            s = merge_batch(o, o->merge, o->merge_count, out);
    }
    if (s != UD_OK && s != UD_END)
        o->failed = 1;
    return s;
}
ud_status ud_operator_destroy(ud_operator *o) {
    if (!o)
        return UD_OK;
    UD_TRY(ud_operator_destroy(o->group_sort));
    o->group_sort = NULL;
    for (uint32_t i = 0; i < 8; i++) {
        ud_buffer_view_release(o->merge[i].view); o->merge[i].view = NULL;
        o->merge[i].batch = NULL;
    }
    ud_buffer_view_release(o->left.view); o->left.view = NULL;
    o->left.batch = NULL;
    ud_buffer_view_release(o->right.view); o->right.view = NULL;
    o->right.batch = NULL;
    for (uint32_t i = 0; i < 2; i++)
        while (o->runs[i]) {
            run *r = o->runs[i], *next = r->next;
            UD_TRY(clear_run(r));
            o->runs[i] = next;
        }
    ud_accumulator_destroy(o->group);
    if (o->options.aggregation && o->group_keys)
        for (uint32_t i = 0; i < o->options.aggregation->key_n; i++)
            ud_clear_value(o->group_keys + i);
    if (o->options.kind == UD_OPERATOR_WINDOW && o->group_keys)
        for (uint32_t i = 0; i < o->options.partition_key_count; i++)
            ud_clear_value(o->group_keys + i);
    ud_free(o->group_keys);
    ud_batch_destroy(o->pending);
    for (uint32_t i = 0; i < 2; i++)
        ud_batch_destroy(o->inputs[i]);
    ud_batch_destroy(o->schema);
    ud_plan_destroy(o->options.aggregation);
    ud_free(o->keys);
    ud_free(o);
    return UD_OK;
}
