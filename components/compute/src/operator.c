#include "ud/operator.h"
#include "relational.h"
#include "window.h"
#include "advanced.h"
#include "replay.h"
#include "../internal.h"
#include "../../execution/executor.h"

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
    ud_relational *relational;
    ud_window *window;
    ud_advanced *advanced;
    ud_replay *replay;
    ud_context *ctx;
    ud_operator_options options;
    ud_batch *inputs[2], *schema;
    uint32_t *keys, ports, ended, run_count;
    uint8_t port_ended[2];
    run *runs[2], *tail[2];
    cursor merge[32];
    uint32_t merge_count;
    int prepared, failed, raw_aggregation;
    ud_operator *group_sort;
    ud_accumulator *group;
    ud_batch *pending;
    uint32_t pending_row;
    ud_value *group_keys;
    int group_active, input_done, saw_group;

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
    (void)values;
    return ud_batch_append_from(target, source, row);
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
static int heap_less(ud_operator *o, cursor *streams, uint32_t a, uint32_t b) {
    int order = compare(o, streams + a, streams + b);
    return order < 0 || (!order && a < b);
}
static void heap_down(ud_operator *o, cursor *streams, uint32_t *heap, uint32_t size, uint32_t at) {
    for (;;) {
        uint32_t left = at * 2 + 1;
        if (left >= size)
            return;
        uint32_t best = left;
        if (left + 1 < size && heap_less(o, streams, heap[left + 1], heap[left]))
            best = left + 1;
        if (!heap_less(o, streams, heap[best], heap[at]))
            return;
        uint32_t swap = heap[at];
        heap[at] = heap[best];
        heap[best] = swap;
        at = best;
    }
}
static ud_status merge_batch(ud_operator *o, cursor *streams, uint32_t count, ud_batch **out) {
    UD_TRY(copy_schema(o->ctx, o->schema, o->options.max_batch_rows, out));
    uint32_t heap[32], size = 0;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < count; i++) {
        ud_status next = advance(o, streams + i);
        if (next == UD_END)
            continue;
        if (next) {
            s = next;
            break;
        }
        heap[size++] = i;
    }
    for (uint32_t i = size / 2; i; i--)
        heap_down(o, streams, heap, size, i - 1);
    while (!s && size && (*out)->rows < o->options.max_batch_rows) {
        s = check(o);
        if (s)
            break;
        uint32_t best = heap[0];
        s = ud_batch_append_from(*out, streams[best].batch, streams[best].row++);
        if (s)
            break;
        ud_status next = advance(o, streams + best);
        if (next == UD_END)
            heap[0] = heap[--size];
        else if (next) {
            s = next;
            break;
        }
        if (size)
            heap_down(o, streams, heap, size, 0);
    }
    if (s || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_END;
    }
    return ud_batch_seal(*out);
}
static ud_status prepare_merge(ud_operator *o) {
    while (o->run_count > 32) {
        run *current = o->runs[0], *head = NULL, *tail = NULL;
        uint32_t groups = 0;
        ud_status s = UD_OK;
        while (current && !s) {
            cursor streams[32] = {{0}};
            uint32_t count = 0;
            while (current && count < 32) {
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
            for (uint32_t i = 0; i < count; i++) {
                ud_buffer_view_release(streams[i].view);
                streams[i].view = NULL;
            }
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
    if (options->kind < UD_OPERATOR_SORT || options->kind > UD_OPERATOR_FUZZY_GROUPING ||
        !options->max_batch_rows || options->max_batch_rows > 65536 || options->descending < 0 ||
        options->descending > 1 || options->partition_count > 64 ||
        (options->partition_count > 1 && !options->executor))
        return UD_INVALID;
    ud_operator *o = ud_alloc(ctx, sizeof(*o));
    if (!o)
        return UD_RESOURCE;
    o->ctx = ctx;
    o->options = *options;
    ud_status captured=ud_replay_create(ctx,options,inputs,&o->replay);
    if(captured){ud_free(o);return captured;}
    ud_operator_options execution_options=*options;execution_options.record_state=0;options=&execution_options;o->options.record_state=0;

    if (!o->options.partition_count)
        o->options.partition_count = 1;
    if (options->kind >= UD_OPERATOR_DEDUPLICATE && options->kind!=UD_OPERATOR_ASOF_JOIN &&
        options->kind!=UD_OPERATOR_TEMPORAL_JOIN && options->kind!=UD_OPERATOR_DIFF_COMPARE) {
        ud_status status=ud_advanced_create(ctx,options,inputs,&o->advanced);
        if(status){ud_replay_destroy(o->replay);ud_free(o);return status;}*out=o;return UD_OK;
    }
    if (options->kind == UD_OPERATOR_WINDOW) {
        ud_status status = ud_window_create(ctx, options, inputs[0], &o->window);
        if (status) { ud_replay_destroy(o->replay); ud_free(o); return status; }
        *out = o; return UD_OK;
    }
    if (options->kind == UD_OPERATOR_JOIN || options->kind >= UD_OPERATOR_MERGE) {
        ud_status status = ud_relational_create(ctx, options, inputs, &o->relational);
        if (status) {
            ud_replay_destroy(o->replay); ud_free(o);
            return status;
        }
        *out = o;
        return UD_OK;
    }
    o->options.aggregation = NULL;
    o->ports = 1;
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
        for (uint32_t i = 0; !s && i < options->aggregation->agg_n; i++) {
            ud_aggregate_def *a = options->aggregation->aggs + i;
            if ((a->kind == 1 || a->kind == 4) && options->aggregation->fields[a->column].type == UD_FLOAT64)
                o->raw_aggregation = 1;
        }
        if (o->raw_aggregation)
            o->options.partition_count = 1;
        uint32_t *partial_keys = ud_alloc(
            ctx, (options->aggregation ? options->aggregation->key_n + 1 : 1) * sizeof(uint32_t));
        if (!partial_keys)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < options->aggregation->key_n; i++)
            partial_keys[i] = o->raw_aggregation ? options->aggregation->keys[i] : i;
        sort.keys = partial_keys;
        sort.key_count = options->aggregation ? options->aggregation->key_n : 0;
        sort.partition_count = 1;
        ud_batch *state_schema = NULL;
        if (!s)
            s = o->raw_aggregation
                    ? copy_schema(ctx, inputs[0], 0, &state_schema)
                    : ud_accumulator_state_schema(options->aggregation, &state_schema);
        const ud_batch *partial_inputs[] = {state_schema};
        if (!s)
            s = ud_operator_create(ctx, &sort, partial_inputs, &o->group_sort);
        ud_batch_destroy(state_schema);
        ud_free(partial_keys);
        o->group_keys = !s ? ud_alloc(ctx, (sort.key_count + 1) * sizeof(*o->group_keys)) : NULL;
        if (!s && !o->group_keys)
            s = UD_RESOURCE;
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
ud_status ud_operator_schema(const ud_operator *o, uint32_t output, ud_batch **out) {
    if (!o || !out)
        return UD_INVALID;
    if (o->advanced) return ud_advanced_schema(o->advanced,output,out);
    if (o->window) return output ? UD_INVALID : ud_window_schema(o->window, out);
    if (o->relational)
        return ud_relational_schema(o->relational, output, out);
    return output ? UD_INVALID : copy_schema(o->ctx, o->schema, 0, out);
}
typedef struct partition_work {
    ud_operator *operator;
    const ud_batch *input;
    ud_batch **results;
    uint32_t count, port;
} partition_work;
static ud_status partition_quantum(void *user, uint32_t index) {
    partition_work *work = user;
    ud_operator *o = work->operator;
    UD_TRY(check(o));
    ud_batch *piece = NULL;
    ud_status status = UD_OK;
    if (work->count == 1) {
        piece = (ud_batch *)work->input;
        UD_TRY(ud_batch_retain(piece));
    } else {
        uint32_t begin = (uint32_t)((uint64_t)work->input->rows * index / work->count);
        uint32_t end = (uint32_t)((uint64_t)work->input->rows * (index + 1) / work->count);
        status = copy_schema(o->ctx, work->input, end - begin, &piece);
        ud_value *values =
            !status ? ud_alloc(o->ctx, (size_t)piece->columns * sizeof(*values)) : NULL;
        if (!status && !values)
            status = UD_RESOURCE;
        for (uint32_t row = begin; !status && row < end; row++)
            status = row_copy(piece, work->input, row, values);
        ud_free(values);
        if (!status)
            status = ud_batch_seal(piece);
    }
    if (!status && o->options.kind == UD_OPERATOR_AGGREGATE) {
        ud_accumulator *partial = NULL;
        status = ud_accumulator_create(o->options.aggregation, &partial);
        if (!status)
            status = ud_accumulator_add(partial, piece);
        if (!status)
            status = ud_accumulator_partial_snapshot(partial, work->results + index);
        if (!status)
            status = ud_batch_seal(work->results[index]);
        ud_accumulator_destroy(partial);
    } else if (!status) {
        ud_batch *sorted = NULL;
        const ud_batch *current = piece;
        for (uint32_t i = o->options.key_count; !status && i; i--) {
            ud_batch *next = NULL;
            status = ud_compute_sort(o->ctx, current, o->keys[i - 1], o->options.descending, &next);
            ud_batch_destroy(sorted);
            sorted = next;
            current = sorted;
        }
        if (!status && sorted)
            work->results[index] = sorted;
        else {
            ud_batch_destroy(sorted);
            if (!status) {
                status = ud_batch_retain(piece);
                if (!status)
                    work->results[index] = piece;
            }
        }
    }
    ud_batch_destroy(piece);
    return status;
}
static ud_status parallel_push(ud_operator *o, uint32_t port, const ud_batch *input) {
    partition_work work = {
        .operator= o, .input = input, .count = o->options.partition_count, .port = port};
    if (work.count > input->rows && input->rows)
        work.count = input->rows;

    if (!input->rows)
        return UD_OK;
    work.results = ud_alloc(o->ctx, (size_t)work.count * sizeof(*work.results));
    if (!work.results)
        return UD_RESOURCE;
    ud_status status = UD_OK;

    ud_work_group *group = NULL;
    if (!status && o->options.kind == UD_OPERATOR_SORT && work.count > 1) {
        uint32_t grain = (input->rows + 4095) / 4096;
        if (grain < work.count)
            work.count = grain ? grain : 1;
    }
    if (!status && o->options.executor && work.count > 1) {
        status = ud_work_group_create(o->options.executor, work.count, work.count,
                                      partition_quantum, &work, &group);
        if (!status) {
            for (uint32_t i = 0; i < work.count; i++)
                ud_work_group_ready(group, i);
            status = ud_work_group_wait(group);
        }
        ud_status released = ud_work_group_destroy(group);
        if (!status)
            status = released;
    } else if (!status)
        status = partition_quantum(&work, 0);
    /* Publish in source partition order, never worker completion order. */
    for (uint32_t i = 0; !status && i < work.count; i++) {
        if (o->options.kind == UD_OPERATOR_AGGREGATE)
            status = ud_operator_push(o->group_sort, 0, work.results[i]);
        else {
            run *r = ud_alloc(o->ctx, sizeof(*r));
            if (!r) {
                status = UD_RESOURCE;
                break;
            }
            if (o->tail[0])
                o->tail[0]->next = r;
            else
                o->runs[0] = r;
            o->tail[0] = r;
            o->run_count++;
            status = store(o, r, work.results[i]);
        }
    }
    for (uint32_t i = 0; i < work.count; i++) {
        ud_batch_destroy(work.results[i]);
    }

    ud_free(work.results);
    return status;
}
static ud_status operator_push(ud_operator *o, uint32_t port, const ud_batch *input) {
    if(o && o->advanced)return ud_advanced_push(o->advanced,port,input);
    if (o && o->window) {
        if(port || !input || !input->sealed || input->rows > o->options.max_batch_rows) return UD_INVALID;
        return ud_window_push(o->window, input);
    }
    if (o && o->relational) {
        if (!input || !input->sealed || input->rows > o->options.max_batch_rows)
            return UD_INVALID;
        return ud_relational_push(o->relational, port, input);
    }
    if (!o || port >= o->ports || o->port_ended[port] || !input || !input->sealed ||
        !o->options.buffers || input->rows > o->options.max_batch_rows)
        return UD_INVALID;
    UD_TRY(check(o));
    UD_TRY(compatible(o->inputs[port], input));
    ud_status status = UD_OK;
    if (o->raw_aggregation)
        status = ud_operator_push(o->group_sort, 0, input);
    else
        status = parallel_push(o, port, input);
    if (status)
        o->failed = 1;
    return status;
}
static ud_status operator_end(ud_operator *o, uint32_t port) {
    if(o && o->advanced)return ud_advanced_end(o->advanced,port);
    if (o && o->window) return port ? UD_INVALID : ud_window_end(o->window);
    if (o && o->relational)
        return ud_relational_end(o->relational, port);
    if (!o || port >= o->ports || o->port_ended[port])
        return UD_INVALID;
    UD_TRY(check(o));
    o->port_ended[port] = 1;
    o->ended++;
    return o->group_sort ? ud_operator_finish_input(o->group_sort, 0) : UD_OK;
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
                s = ud_operator_pull(o->group_sort, &o->pending, NULL);
                if (s == UD_END) {
                    s = UD_OK;
                    o->input_done = 1;
                }
            }
        }
        if (s)
            break;
        if (!o->input_done && o->raw_aggregation) {
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
                s = ud_batch_get(o->pending, o->pending_row, o->raw_aggregation ? p->keys[i] : i,
                                 &key);
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
                s = ud_batch_get(o->pending, o->pending_row, o->raw_aggregation ? p->keys[i] : i,
                                 &key);
                if (!s)
                    s = ud_copy_value(o->ctx, &key, o->group_keys + i, 0);
            }
            o->group_active = !s;
            o->saw_group = 1;
        }
        if (!s)
            s = o->raw_aggregation
                    ? ud_accumulator_add_accepted(o->group, o->pending, o->pending_row++)
                    : ud_accumulator_merge_row(o->group, o->pending, o->pending_row++);
    }
    ud_free(values);
    if (s || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_END;
    }
    return ud_batch_seal(*out);
}

static ud_status operator_pull(ud_operator *o, ud_batch **out, uint32_t *output) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!o)
        return UD_INVALID;
    if (output)
        *output = 0;
    if(o->advanced)return ud_advanced_pull(o->advanced,out,output);
    if (o->window) return ud_window_pull(o->window, out);
    if (o->relational)
        return ud_relational_pull(o->relational, out, output);
    UD_TRY(check(o));
    if (o->ended != o->ports)
        return UD_BUSY;
    ud_status s = UD_OK;
    if (o->group_sort)
        s = aggregate_pull(o, out);
    else {
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
ud_status ud_operator_push(ud_operator*o,uint32_t port,const ud_batch*b){ud_status s=operator_push(o,port,b);if(!s)s=ud_replay_record(o->replay,1,port,b);return s;}
ud_status ud_operator_finish_input(ud_operator*o,uint32_t port){ud_status s=operator_end(o,port);if(!s)s=ud_replay_record(o->replay,2,port,NULL);return s;}
ud_status ud_operator_pull(ud_operator*o,ud_batch**b,uint32_t*port){uint32_t output=0;ud_status s=operator_pull(o,b,&output);if(!s)s=ud_replay_record(o->replay,3,output,*b);if(s&&s!=UD_BUSY&&s!=UD_END&&b){ud_batch_destroy(*b);*b=NULL;}if(port)*port=output;return s;}
ud_status ud_operator_checkpoint(ud_operator*o,ud_batch**out){if(!o||!out)return UD_INVALID;return ud_replay_snapshot(o->replay,out);}
ud_status ud_operator_restore(ud_operator**instance,const ud_batch*snapshot){if(!instance||!*instance)return UD_INVALID;ud_operator*old=*instance,*restored=NULL;ud_status s=ud_replay_restore(old->replay,snapshot,&restored);if(!s)s=ud_replay_rebase(old->replay,snapshot);if(s){ud_operator_destroy(restored);return s;}restored->replay=old->replay;old->replay=NULL;s=ud_operator_destroy(old);*instance=restored;return s;}
ud_status ud_operator_destroy(ud_operator *o) {
    if (!o)
        return UD_OK;
    if(o->advanced){ud_advanced_destroy(o->advanced);ud_replay_destroy(o->replay);ud_free(o);return UD_OK;}
    if (o->window) { ud_window_destroy(o->window); ud_replay_destroy(o->replay); ud_free(o); return UD_OK; }
    if (o->relational) {
        ud_relational_destroy(o->relational);
        ud_replay_destroy(o->replay); ud_free(o);
        return UD_OK;
    }
    UD_TRY(ud_operator_destroy(o->group_sort));
    o->group_sort = NULL;
    for (uint32_t i = 0; i < 32; i++) {
        ud_buffer_view_release(o->merge[i].view);
        o->merge[i].view = NULL;
        o->merge[i].batch = NULL;
    }
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
    ud_free(o->group_keys);
    ud_batch_destroy(o->pending);
    for (uint32_t i = 0; i < 2; i++)
        ud_batch_destroy(o->inputs[i]);
    ud_batch_destroy(o->schema);
    ud_plan_destroy(o->options.aggregation);
    ud_free(o->keys);
    ud_replay_destroy(o->replay);
    ud_free(o);
    return UD_OK;
}

uint32_t ud_operator_partition_count(const ud_operator *o) {
    return o ? o->options.partition_count : 0;
}

ud_status ud_operator_order(const ud_operator *o, ud_sort_contract *out) {
    if (!o || !out)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    if(o->advanced)return ud_advanced_order(o->advanced,out);
    if (o->window) return ud_window_order(o->window, out);
    if (o->relational)
        return ud_relational_order(o->relational, out);
    if (o->options.kind == UD_OPERATOR_SORT) {
        if (o->options.key_count > 16)
            return UD_RESOURCE;
        out->count = o->options.key_count;
        out->stable = 1;
        for (uint32_t k = 0; k < out->count; k++)
            out->keys[k] = (ud_sort_key){o->schema->column[o->keys[k]].field.identity,
                                         o->options.descending, !o->options.descending};
    }
    return UD_OK;
}
uint64_t ud_operator_input_demand(const ud_operator *o) {
    if (!o)
        return 0;
    if(o->advanced)return ud_advanced_demand(o->advanced);
    if (o->window) return ud_window_demand(o->window);
    if (o->relational)
        return ud_relational_demand(o->relational);
    uint64_t mask = 0;
    for (uint32_t p = 0; p < o->ports; p++)
        if (!o->port_ended[p])
            mask |= UINT64_C(1) << p;
    return mask;
}
ud_status ud_operator_diagnostics(const ud_operator *o, char **json, uint32_t *length) {
    if (!o || !json || !length)
        return UD_INVALID;
    if(o->advanced)return ud_advanced_diagnostics(o->advanced,json,length);
    if (o->relational)
        return ud_relational_diagnostics(o->relational, json, length);
    *json = ud_strdup_n(o->ctx, "{}", 2);
    *length = 2;
    return *json ? UD_OK : UD_RESOURCE;
}

ud_status ud_operator_physical_plan(const ud_operator *o, ud_physical_plan_info *out) {
    if (!o || !out) return UD_INVALID;
    if (out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION) return UD_INCOMPATIBLE;
    memset(out, 0, sizeof(*out));out->struct_size=sizeof(*out);out->abi_version=UD_ABI_VERSION;
    out->algorithm=o->advanced ? ud_advanced_algorithm(o->advanced) : o->relational ? ud_relational_algorithm(o->relational) : o->window || o->options.kind==UD_OPERATOR_SORT || o->group_sort ? UD_PHYSICAL_SORT : UD_PHYSICAL_STREAM;
    out->input_count=o->options.input_count ? o->options.input_count : o->options.kind==UD_OPERATOR_JOIN || o->options.kind==UD_OPERATOR_MERGE || o->options.kind==UD_OPERATOR_MERGE_JOIN ? 2 : 1;
    out->output_count=o->advanced ? ud_advanced_output_count(o->advanced) : o->relational ? ud_relational_output_count(o->relational) : o->options.kind==UD_OPERATOR_SCD?6:o->options.kind==UD_OPERATOR_PIVOT||o->options.kind==UD_OPERATOR_ROW_SAMPLING||o->options.kind==UD_OPERATOR_PERCENTAGE_SAMPLING?2:1;
    out->partitions=o->options.partition_count?o->options.partition_count:1;
    out->estimated_rows=o->options.estimated_rows[0];out->memory_budget=o->options.memory_budget;out->spill_budget=o->options.spill_budget;
    ud_batch *schema=NULL;ud_status status=ud_operator_schema(o,0,&schema);
    for(uint32_t i=0;!status&&i<schema->columns;i++) {ud_type t=schema->column[i].field.type;out->row_width+=t==UD_UTF8||t==UD_BINARY||t>=UD_STRUCT?32:t==UD_DECIMAL128?16:8;}
    ud_batch_destroy(schema);if(!status)status=ud_operator_order(o,&out->sort);
    if(out->estimated_rows && out->row_width <= UINT64_MAX/out->estimated_rows)out->estimated_cost=out->estimated_rows*out->row_width;
    const char *decision=out->algorithm==UD_PHYSICAL_MERGE?"bound sort contracts; canonical merge comparison":out->algorithm==UD_PHYSICAL_HASH?"right-side estimate fits declared hash budget":out->algorithm==UD_PHYSICAL_GRACE_HASH?"missing or large estimate; bounded hash partitions with merge fallback for skew":out->algorithm==UD_PHYSICAL_SORT?"external stable sort using shared Buffer runs":"bounded streaming kernel";
    out->decision=(ud_string){decision,(uint32_t)strlen(decision)};return status;
}
