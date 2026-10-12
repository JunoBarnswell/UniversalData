#include "ud/operator.h"
#include "ud/cache.h"
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
    ud_context *ctx;
    ud_operator_options options;
    ud_batch *inputs[2], *schema;
    uint32_t *keys, ports, ended, run_count;
    uint8_t port_ended[2];
    run *runs[2], *tail[2];
    cursor merge[8], left;
    uint32_t merge_count;
    int prepared, failed, raw_aggregation;
    block *left_block;
    ud_cache *join_cache;
    ud_cache_reader *join_reader;
    uint32_t *right_keys;
    int probing;
    ud_operator *group_sort;
    ud_accumulator *group;
    ud_batch *pending;
    uint32_t pending_row;
    ud_value *group_keys;
    int group_active, input_done, saw_group;
    ud_value window_value;
    uint64_t window_rows;
    ud_operator **partitions;
    uint32_t pull_partition;
    ud_batch **partition_outputs;
    uint8_t *partition_ended;
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
    if (options->kind < UD_OPERATOR_SORT || options->kind > UD_OPERATOR_WINDOW ||
        !options->max_batch_rows || options->max_batch_rows > 65536 || options->descending < 0 ||
        options->descending > 1 || options->partition_count > 64 ||
        (options->partition_count > 1 && !options->executor))
        return UD_INVALID;
    ud_operator *o = ud_alloc(ctx, sizeof(*o));
    if (!o)
        return UD_RESOURCE;
    o->ctx = ctx;
    o->options = *options;
    if (!o->options.partition_count)
        o->options.partition_count = 1;
    o->options.aggregation = NULL;
    o->ports = options->kind == UD_OPERATOR_INNER_JOIN ? 2 : 1;
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
            if (a->kind == 1 && options->aggregation->fields[a->column].type == UD_FLOAT64)
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
        ud_id *origins = NULL;
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
            ud_id_format(inputs[0]->column[i].field.identity, id);
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
        if (!inputs[1] || !options->key_count || !options->keys || !options->right_keys)
            s = UD_SCHEMA;
        o->right_keys =
            !s ? ud_alloc(ctx, (size_t)options->key_count * sizeof(*o->right_keys)) : NULL;
        if (!s && !o->right_keys)
            s = UD_RESOURCE;
        for (uint32_t j = 0; !s && j < options->key_count; j++) {
            if (options->keys[j] >= inputs[0]->columns ||
                options->right_keys[j] >= inputs[1]->columns)
                s = UD_SCHEMA;
            if (!s && !ud_field_matches(&inputs[0]->column[options->keys[j]].field,
                                        &inputs[1]->column[options->right_keys[j]].field,
                                        UD_FIELD_VALUE_TYPE))
                s = UD_SCHEMA;
            if (!s)
                o->right_keys[j] = options->right_keys[j];
        }
        if (!s)
            s = ud_compute_join_schema(ctx, inputs[0], inputs[1], &o->schema);
        ud_cache_options cache = {.struct_size = sizeof(cache),
                                  .abi_version = UD_ABI_VERSION,
                                  .max_batch_rows = options->max_batch_rows,
                                  .keys = o->right_keys,
                                  .key_count = options->key_count,
                                  .duplicates = UD_CACHE_ALL,
                                  .buffers = options->buffers,
                                  .executor = options->executor,
                                  .check = options->check,
                                  .user = options->user};
        if (!s && options->partition_count <= 1 && options->buffers)
            s = ud_cache_create(ctx, inputs[1], &cache, &o->join_cache);
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
    if (!s && o->ports == 2 && o->options.partition_count > 1) {
        o->partitions = ud_alloc(ctx, (size_t)o->options.partition_count * sizeof(*o->partitions));
        o->partition_outputs =
            ud_alloc(ctx, (size_t)o->options.partition_count * sizeof(*o->partition_outputs));
        o->partition_ended = ud_alloc(ctx, o->options.partition_count);
        if (!o->partitions || !o->partition_outputs || !o->partition_ended)
            s = UD_RESOURCE;
        ud_operator_options child = o->options;
        child.partition_count = 1;
        for (uint32_t i = 0; !s && i < o->options.partition_count; i++)
            s = ud_operator_create(ctx, &child, inputs, o->partitions + i);
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
typedef struct partition_work {
    ud_operator *operator;
    const ud_batch *input;
    ud_batch **pieces, **results;
    uint32_t count, port;
} partition_work;
static ud_status partition_quantum(void *user, uint32_t index) {
    partition_work *work = user;
    ud_operator *o = work->operator;
    UD_TRY(check(o));
    ud_batch *piece = NULL;
    ud_status status = UD_OK;
    if (work->pieces)
        piece = work->pieces[index];
    else if (work->count == 1) {
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
    if (!status && o->partitions)
        status = ud_operator_push(o->partitions[index], work->port, piece);
    else if (!status && o->options.kind == UD_OPERATOR_AGGREGATE) {
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
    if (!work->pieces)
        ud_batch_destroy(piece);
    return status;
}
static ud_status parallel_push(ud_operator *o, uint32_t port, const ud_batch *input) {
    partition_work work = {
        .operator= o, .input = input, .count = o->options.partition_count, .port = port};
    if (work.count > input->rows && input->rows)
        work.count = input->rows;
    if (o->partitions)
        work.count = o->options.partition_count;
    if (!input->rows)
        return UD_OK;
    work.results = ud_alloc(o->ctx, (size_t)work.count * sizeof(*work.results));
    if (!work.results)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    if (o->partitions) {
        ud_partition_options partition = {.struct_size = sizeof(partition),
                                          .abi_version = UD_ABI_VERSION,
                                          .strategy = UD_PARTITION_HASH,
                                          .count = work.count,
                                          .keys = port ? o->right_keys : o->keys,
                                          .key_count = o->options.key_count};
        status = ud_batch_partition(o->ctx, input, &partition, &work.pieces);
    }
    ud_work_group *group = NULL;
    if (!status && o->options.kind == UD_OPERATOR_SORT && work.count > 1 && !o->partitions) {
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
    for (uint32_t i = 0; !status && i < work.count && !o->partitions; i++) {
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
        if (work.pieces)
            ud_batch_destroy(work.pieces[i]);
    }
    ud_free(work.pieces);
    ud_free(work.results);
    return status;
}
ud_status ud_operator_push(ud_operator *o, uint32_t port, const ud_batch *input) {
    if (!o || port >= o->ports || o->port_ended[port] || !input || !input->sealed ||
        !o->options.buffers || input->rows > o->options.max_batch_rows)
        return UD_INVALID;
    UD_TRY(check(o));
    UD_TRY(compatible(o->inputs[port], input));
    ud_status status = UD_OK;
    if (o->options.kind == UD_OPERATOR_WINDOW || o->raw_aggregation)
        status = ud_operator_push(o->group_sort, 0, input);
    else if (o->options.kind == UD_OPERATOR_AGGREGATE || o->ports == 1 || o->partitions)
        status = parallel_push(o, port, input);
    else if (port == 1)
        status = ud_cache_append(o->join_cache, input);
    else {
        if (!o->runs[port]) {
            o->runs[port] = o->tail[port] = ud_alloc(o->ctx, sizeof(run));
            if (!o->runs[port])
                status = UD_RESOURCE;
        }
        if (!status)
            status = store(o, o->runs[port], (ud_batch *)input);
    }
    if (status)
        o->failed = 1;
    return status;
}
ud_status ud_operator_finish_input(ud_operator *o, uint32_t port) {
    if (!o || port >= o->ports || o->port_ended[port])
        return UD_INVALID;
    UD_TRY(check(o));
    o->port_ended[port] = 1;
    o->ended++;
    if (o->partitions)
        for (uint32_t i = 0; i < o->options.partition_count; i++)
            UD_TRY(ud_operator_finish_input(o->partitions[i], port));
    if (o->ports == 2 && !o->partitions && port == 1)
        return ud_cache_seal(o->join_cache);
    return o->group_sort ? ud_operator_finish_input(o->group_sort, 0) : UD_OK;
}
static ud_status join_pull(ud_operator *o, ud_batch **out) {
    UD_TRY(copy_schema(o->ctx, o->schema, o->options.max_batch_rows, out));
    ud_value *values = ud_alloc(o->ctx, (size_t)o->schema->columns * sizeof(*values));
    ud_status status = values ? UD_OK : UD_RESOURCE;
    while (!status && (*out)->rows < o->options.max_batch_rows) {
        status = check(o);
        if (status)
            break;
        if (!o->left.batch || o->left.row >= o->left.batch->rows) {
            ud_buffer_view_release(o->left.view);
            o->left.view = NULL;
            o->left.batch = NULL;
            o->probing = 0;
            if (!o->left_block)
                break;
            block *current = o->left_block;
            o->left_block = current->next;
            status = ud_buffer_read(current->buffer, &o->left.view);
            o->left.batch = ud_buffer_view_batch(o->left.view);
            o->left.row = 0;
            if (!status)
                status = compatible(o->inputs[0], o->left.batch);
        }
        if (!status && !o->probing) {
            status = ud_cache_probe(o->join_reader, o->left.batch, o->left.row);
            o->probing = !status;
        }
        const ud_batch *right = NULL;
        uint32_t row = 0;
        if (!status) {
            status = ud_cache_next(o->join_reader, &right, &row);
            if (status == UD_END) {
                status = UD_OK;
                o->left.row++;
                o->probing = 0;
                continue;
            }
        }
        for (uint32_t j = 0; !status && j < o->left.batch->columns; j++)
            status = ud_batch_get(o->left.batch, o->left.row, j, values + j);
        for (uint32_t j = 0; !status && j < right->columns; j++)
            status = ud_batch_get(right, row, j, values + o->left.batch->columns + j);
        if (!status)
            status = ud_batch_append(*out, values, o->schema->columns);
        if (!status)
            status = ud_batch_join_row_info(*out, (*out)->rows - 1, o->left.batch, o->left.row,
                                            right, row);
    }
    ud_free(values);
    if (status || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return status ? status : UD_END;
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
        if (!s)
            s = ud_batch_copy_row_info(*out, (*out)->rows - 1, o->pending, o->pending_row);
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
static ud_status join_partition_quantum(void *user, uint32_t index) {
    ud_operator *o = user;
    if (o->partition_outputs[index] || o->partition_ended[index])
        return UD_OK;
    ud_status status = ud_operator_pull(o->partitions[index], o->partition_outputs + index);
    if (status == UD_END) {
        o->partition_ended[index] = 1;
        return UD_OK;
    }
    return status;
}
static ud_status partition_join_pull(ud_operator *o, ud_batch **out) {
    ud_work_group *group = NULL;
    ud_status status =
        ud_work_group_create(o->options.executor, o->options.partition_count,
                             o->options.partition_count, join_partition_quantum, o, &group);
    if (!status) {
        for (uint32_t i = 0; i < o->options.partition_count; i++)
            ud_work_group_ready(group, i);
        status = ud_work_group_wait(group);
    }
    ud_status released = ud_work_group_destroy(group);
    if (!status)
        status = released;
    if (status)
        return status;
    for (uint32_t i = 0; i < o->options.partition_count; i++) {
        uint32_t index = (o->pull_partition + i) % o->options.partition_count;
        if (o->partition_outputs[index]) {
            *out = o->partition_outputs[index];
            o->partition_outputs[index] = NULL;
            o->pull_partition = (index + 1) % o->options.partition_count;
            return UD_OK;
        }
    }
    return UD_END;
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
    if (o->partitions) {
        s = partition_join_pull(o, out);
    } else if (o->options.kind == UD_OPERATOR_WINDOW)
        s = window_pull(o, out);
    else if (o->group_sort)
        s = aggregate_pull(o, out);
    else if (o->ports == 2) {
        if (!o->prepared) {
            o->left_block = o->runs[0] ? o->runs[0]->head : NULL;
            s = ud_cache_reader_create(o->join_cache, o->inputs[0], o->keys, o->options.key_count,
                                       &o->join_reader);
            o->prepared = !s;
        }
        if (!s)
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
    if (o->partitions) {
        for (uint32_t i = 0; i < o->options.partition_count; i++) {
            ud_batch_destroy(o->partition_outputs ? o->partition_outputs[i] : NULL);
            if (o->partition_outputs)
                o->partition_outputs[i] = NULL;
            UD_TRY(ud_operator_destroy(o->partitions[i]));
            o->partitions[i] = NULL;
        }
        ud_free(o->partitions);
        o->partitions = NULL;
    }
    ud_free(o->partition_outputs);
    o->partition_outputs = NULL;
    ud_free(o->partition_ended);
    o->partition_ended = NULL;
    ud_cache_reader_destroy(o->join_reader);
    o->join_reader = NULL;
    UD_TRY(ud_cache_release(o->join_cache));
    o->join_cache = NULL;
    UD_TRY(ud_operator_destroy(o->group_sort));
    o->group_sort = NULL;
    for (uint32_t i = 0; i < 8; i++) {
        ud_buffer_view_release(o->merge[i].view);
        o->merge[i].view = NULL;
        o->merge[i].batch = NULL;
    }
    ud_buffer_view_release(o->left.view);
    o->left.view = NULL;
    o->left.batch = NULL;
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
    ud_free(o->right_keys);
    ud_free(o);
    return UD_OK;
}

uint32_t ud_operator_partition_count(const ud_operator *o) {
    return o ? o->options.partition_count : 0;
}
