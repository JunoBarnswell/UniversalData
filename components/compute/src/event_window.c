#include "event_window.h"
#include "../../execution/state.h"
#include "stat.h"
#include "ud/connector.h"
/* Event-time lifecycle owned by the existing Compute operator. Accumulator
 * states and spilled snapshots use the existing typed statistics/Block Store. */
typedef struct event_group {
    int64_t start, end;
    uint64_t hash;
    ud_value *keys;
    ud_accumulator *aggregate;
    ud_buffer_block *spill;
    ud_batch *before;
    struct event_id *events;
    uint32_t event_count;
    uint64_t members, revision;
    int dirty;
    uint32_t heap_index;
    struct event_group *next, **link;
    struct event_group *hash_next, *dirty_next;
} event_group;
typedef struct event_id {
    ud_id value;
    struct event_id *next;
} event_id;
struct ud_event_window {
    ud_context *ctx;
    ud_operator_options options;
    ud_batch *input, *schema, *late;
    ud_plan *plan;
    ud_memory_domain *memory;
    char *configuration;
    uint8_t fingerprint[32];
    ud_id output_scope;
    event_group *groups, *dirty_head, *dirty_tail;
    event_group **buckets, **heap;
    uint32_t heap_capacity;
    uint32_t bucket_count;
    uint32_t time, count, maximum, late_at, before_field;
    int mode, changes, late_policy, ended, failed;
    int64_t size, slide, gap, lateness, watermark;
    uint64_t budget, late_rows, emitted, spills;
};
static const ud_json *member(const ud_json *j, const char *key) {
    return ud_json_find(j, key, (uint32_t)strlen(key));
}
static int named(const ud_json *j, const char *s) {
    const char *v;
    uint32_t n;
    return j && !ud_json_text(j, &v, &n) && n == strlen(s) && !memcmp(v, s, n);
}
static ud_status number(const ud_json *j, uint64_t fallback, uint64_t *out) {
    if (!j) {
        *out = fallback;
        return UD_OK;
    }
    const char *v;
    uint32_t n;
    UD_TRY(ud_json_text(j, &v, &n));
    if (ud_json_kind(j) != UD_JSON_NUMBER || !n)
        return UD_SCHEMA;
    *out = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (v[i] < '0' || v[i] > '9' || *out > (UINT64_MAX - (unsigned)(v[i] - '0')) / 10)
            return UD_SCHEMA;
        *out = *out * 10 + (unsigned)(v[i] - '0');
    }
    return UD_OK;
}
static ud_status check(const ud_event_window *w) {
    if (w->failed)
        return UD_FENCED;
    if (ud_is_cancelled(w->ctx))
        return UD_CANCELLED;
    return w->options.check ? w->options.check(w->options.user) : UD_OK;
}
static int closed(const ud_event_window *w, int64_t end) {
    return w->ended || (end <= INT64_MAX - w->lateness && end + w->lateness <= w->watermark);
}
static void destroy_group(ud_event_window *w, event_group *g) {
    for (uint32_t i = 0; g->keys && i < w->plan->key_n; i++)
        ud_clear_value(g->keys + i);
    ud_free(g->keys);
    ud_accumulator_destroy(g->aggregate);
    ud_buffer_release(g->spill);
    ud_batch_destroy(g->before);
    while (g->events) {
        event_id *next = g->events->next;
        ud_free(g->events);
        g->events = next;
    }
    ud_free(g);
}
static int has_id(const event_id *events, ud_id value) {
    for (; events; events = events->next)
        if (!ud_id_compare(events->value, value))
            return 1;
    return 0;
}
static int any_group_has_id(const ud_event_window *w, ud_id value) {
    if (!value.high && !value.low)
        return 0;
    for (event_group *g = w->groups; g; g = g->next)
        if (has_id(g->events, value))
            return 1;
    return 0;
}
static ud_status remember_id(ud_event_window *w, event_group *g, ud_id value) {
    if ((!value.high && !value.low) || has_id(g->events, value))
        return UD_OK;
    if (g->event_count == UINT32_MAX)
        return UD_OVERFLOW;
    ud_memory_scope scope;
    ud_memory_domain_enter(w->memory, &scope);
    event_id *entry = ud_alloc(w->ctx, sizeof(*entry));
    ud_memory_domain_leave(&scope);
    if (!entry)
        return UD_RESOURCE;
    entry->value = value;
    entry->next = g->events;
    g->events = entry;
    g->event_count++;
    return UD_OK;
}
static int same_key(ud_event_window *w, event_group *g, const ud_batch *b, uint32_t row,
                    uint64_t hash);
static uint32_t bucket_of(const ud_event_window *w, uint64_t hash, int64_t start) {
    if (w->mode != 3) {
        ud_value time = {.type = UD_INT64, .integer = start};
        hash = ud_value_hash(&time, hash);
    }
    return (uint32_t)(hash % w->bucket_count);
}
static event_group *find_group(ud_event_window *w, const ud_batch *b, uint32_t row,
                               uint64_t hash, int64_t start) {
    for (event_group *g = w->buckets[bucket_of(w, hash, start)]; g; g = g->hash_next)
        if (g->start == start && same_key(w, g, b, row, hash))
            return g;
    return NULL;
}
static void index_group(ud_event_window *w, event_group *g) {
    uint32_t bucket = bucket_of(w, g->hash, g->start);
    g->hash_next = w->buckets[bucket];
    w->buckets[bucket] = g;
}
static void unindex_group(ud_event_window *w, event_group *g) {
    event_group **at = &w->buckets[bucket_of(w, g->hash, g->start)];
    while (*at && *at != g)
        at = &(*at)->hash_next;
    if (*at == g)
        *at = g->hash_next;
    g->hash_next = NULL;
}
static ud_status load(ud_event_window *w, event_group *g) {
    if (g->aggregate)
        return UD_OK;
    ud_buffer_view *view = NULL;
    ud_status s = g->spill ? ud_buffer_read(g->spill, &view) : UD_OK;
    ud_memory_scope scope;
    ud_memory_domain_enter(w->memory, &scope);
    if (!s)
        s = ud_accumulator_create(w->plan, &g->aggregate);
    if (!s && view)
        s = ud_accumulator_merge(g->aggregate, ud_buffer_view_batch(view));
    ud_memory_domain_leave(&scope);
    ud_buffer_view_release(view);
    if (!s && g->spill) {
        s = ud_buffer_release(g->spill);
        g->spill = NULL;
    }
    return s;
}
static ud_status spill(ud_event_window *w, event_group *g) {
    if (!g->aggregate)
        return UD_OK;
    if (!w->options.buffers)
        return UD_RESOURCE;
    ud_batch *state = NULL;
    ud_status s = ud_accumulator_partial_snapshot(g->aggregate, &state);
    if (!s)
        s = ud_batch_seal(state);
    if (!s)
        s = ud_buffer_store(w->options.buffers, state, &g->spill);
    ud_batch_destroy(state);
    if (!s) {
        ud_accumulator_destroy(g->aggregate);
        g->aggregate = NULL;
        w->spills++;
    }
    return s;
}
static ud_status rebalance(ud_event_window *w) {
    if (ud_memory_domain_used(w->memory) < w->budget / 2)
        return UD_OK;
    for (event_group *g = w->groups; g && ud_memory_domain_used(w->memory) > w->budget / 4;
         g = g->next)
        UD_TRY(spill(w, g));
    return UD_OK;
}
static int same_key(ud_event_window *w, event_group *g, const ud_batch *b, uint32_t row,
                    uint64_t hash) {
    if (g->hash != hash)
        return 0;
    for (uint32_t i = 0; i < w->plan->key_n; i++) {
        ud_value v;
        const ud_field *field = &w->plan->output_schema->column[i].field;
        if (ud_batch_get(b, row, w->plan->keys[i], &v) ||
            ud_field_value_compare(field, g->keys + i, field, &v))
            return 0;
    }
    return 1;
}
static int precedes(const ud_event_window *w, const event_group *a, const event_group *b) {
    if (a->end != b->end)
        return a->end < b->end;
    if (a->start != b->start)
        return a->start < b->start;
    for (uint32_t i = 0; i < w->plan->key_n; i++) {
        const ud_field *field = &w->plan->output_schema->column[i].field;
        int order = ud_field_value_compare(field, a->keys + i, field, b->keys + i);
        if (order)
            return order < 0;
    }
    return 0;
}
static void heap_set(ud_event_window *w, uint32_t at, event_group *g) {
    w->heap[at] = g;
    g->heap_index = at;
}
static void heap_fix(ud_event_window *w, event_group *g) {
    uint32_t i = g->heap_index;
    while (i && precedes(w, g, w->heap[(i - 1) / 2])) {
        heap_set(w, i, w->heap[(i - 1) / 2]);
        i = (i - 1) / 2;
    }
    while (2 * i + 1 < w->count) {
        uint32_t child = 2 * i + 1;
        if (child + 1 < w->count && precedes(w, w->heap[child + 1], w->heap[child]))
            child++;
        if (!precedes(w, w->heap[child], g))
            break;
        heap_set(w, i, w->heap[child]);
        i = child;
    }
    heap_set(w, i, g);
}
static ud_status insert_group(ud_event_window *w, event_group *g) {
    if (w->count == w->heap_capacity) {
        uint32_t n = w->heap_capacity ? w->heap_capacity * 2 : 32;
        event_group **heap = ud_alloc(w->ctx, n * sizeof(*heap));
        if (!heap)
            return UD_RESOURCE;
        if (w->count)
            memcpy(heap, w->heap, w->count * sizeof(*heap));
        ud_free(w->heap);
        w->heap = heap;
        w->heap_capacity = n;
    }
    g->link = &w->groups;
    g->next = w->groups;
    if (g->next)
        g->next->link = &g->next;
    w->groups = g;
    index_group(w, g);
    heap_set(w, w->count++, g);
    heap_fix(w, g);
    return UD_OK;
}
static void remove_group(ud_event_window *w, event_group *g) {
    *g->link = g->next;
    if (g->next)
        g->next->link = g->link;
    unindex_group(w, g);
    if (g->heap_index != --w->count) {
        event_group *last = w->heap[w->count];
        heap_set(w, g->heap_index, last);
        heap_fix(w, last);
    }
    destroy_group(w, g);
}
static ud_status new_group(ud_event_window *w, const ud_batch *b, uint32_t row, uint64_t hash,
                           int64_t start, int64_t end, event_group **out) {
    if (w->count >= w->maximum)
        return UD_RESOURCE;
    event_group *g = ud_alloc(w->ctx, sizeof(*g));
    if (!g)
        return UD_RESOURCE;
    memset(g, 0, sizeof(*g));
    g->start = start;
    g->end = end;
    g->hash = hash;
    g->keys = ud_alloc(w->ctx, (w->plan->key_n + 1) * sizeof(*g->keys));
    ud_status s = g->keys ? UD_OK : UD_RESOURCE;
    for (uint32_t i = 0; !s && i < w->plan->key_n; i++) {
        ud_value v;
        s = ud_batch_get(b, row, w->plan->keys[i], &v);
        if (!s)
            s = ud_copy_value(w->ctx, &v, g->keys + i, 0);
    }
    if (!s)
        s = load(w, g);
    if (!s)
        s = insert_group(w, g);
    if (s) {
        destroy_group(w, g);
        return s;
    }
    *out = g;
    return UD_OK;
}
static ud_status bind_changes(ud_event_window *w, const ud_json *config) {
    w->before_field = UINT32_MAX;
    const ud_json *before = member(config, "beforeField");
    w->changes = named(member(config, "emit"), "changes");
    if (!w->changes)
        return before ? UD_SCHEMA : UD_OK;
    /* A changes stream needs one stable key and an exact inverse. Sessions can
     * merge or split under reordering, and min/max/distinct need row-level
     * state, so those shapes stay on the final-after-lateness contract. */
    if (w->mode == 3 || !w->plan->key_n)
        return UD_UNSUPPORTED;
    for (uint32_t i = 0; i < w->plan->agg_n; i++)
        if (!ud_stat_retractable(w->plan->aggs + i))
            return UD_UNSUPPORTED;
    for (uint32_t i = 0; i < w->plan->key_n; i++)
        if (w->input->schema->fields[w->plan->keys[i]].nullable)
            return UD_SCHEMA;

    if (before) {
        UD_TRY(ud_compute_field_index(w->input, before, &w->before_field));
    } else {
        for (uint32_t i = 0; i < w->input->columns; i++) {
            const ud_field *field = w->input->schema->fields + i;
            if (field->name_length == 6 && !memcmp(field->name, "before", 6)) {
                w->before_field = i;
                break;
            }
        }
    }
    if (w->before_field == UINT32_MAX)
        return UD_OK;
    const ud_field *field = w->input->schema->fields + w->before_field;
    if (field->type != UD_STRUCT || !field->nullable || field->child_count != w->input->columns - 1)
        return UD_SCHEMA;
    for (uint32_t i = 0, j = 0; i < w->input->columns; i++)
        if (i != w->before_field &&
            !ud_field_matches(w->input->schema->fields + i, field->children + j++, UD_FIELD_LAYOUT))
            return UD_SCHEMA;
    return UD_OK;
}
static ud_status bind_output(ud_event_window *w) {
    ud_context *ctx = w->ctx;
    uint32_t columns = w->plan->output_schema->columns, payload = columns + 2 + w->changes;
    ud_field *fields = ud_alloc(ctx, (payload + w->changes) * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < columns; i++)
        fields[i + 2] = w->plan->output_schema->column[i].field;
    const char *names[] = {"_window_start", "_window_end", "_revision"};
    ud_field integer = {.type = UD_INT64};
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < 2u + w->changes; i++) {
        ud_field *target = i < 2 ? fields + i : fields + payload - 1;
        status = ud_field_derive(ctx, &integer, w->options.configuration,
                                 (ud_string){names[i], (uint32_t)strlen(names[i])}, target);
    }
    if (!status && w->changes) {
        ud_field prior = {.type = UD_STRUCT, .nullable = 1, .children = fields,
                          .child_count = payload};
        status = ud_field_derive(ctx, &prior, w->options.configuration, (ud_string){"before", 6},
                                 fields + payload);
    }
    if (!status)
        status = ud_batch_create(ctx, fields, payload + w->changes, 0, &w->schema);
    if (w->changes) {
        ud_field_clear(fields + payload - 1);
        ud_field_clear(fields + payload);
    }
    ud_field_clear(fields + 1);
    ud_field_clear(fields);
    ud_free(fields);
    return status;
}
ud_status ud_event_window_create(ud_context *ctx, const ud_operator_options *o,
                                 const ud_batch *input, ud_event_window **out) {
    *out = NULL;
    if (!o->aggregation || !o->aggregation->agg_n)
        return UD_INVALID;
    ud_event_window *w = ud_alloc(ctx, sizeof(*w));
    if (!w)
        return UD_RESOURCE;
    w->ctx = ctx;
    w->options = *o;
    w->watermark = INT64_MIN;
    ud_json *config = NULL;
    ud_status s = ud_json_parse(ctx, o->configuration.data, o->configuration.length, &config, NULL);
    if (!s)
        s = ud_plan_retain(o->aggregation);
    if (!s)
        w->plan = o->aggregation;
    if (!s)
        s = ud_plan_check_batch(w->plan, input);
    if (!s)
        s = ud_batch_create(ctx, input->schema->fields, input->columns, 0, &w->input);
    if (!s)
        s = ud_compute_field_index(input, member(config, "timeField"), &w->time);
    if (!s && (input->column[w->time].field.nullable ||
               (input->column[w->time].field.type != UD_TIMESTAMP &&
                input->column[w->time].field.type != UD_INT64)))
        s = UD_SCHEMA;
    uint64_t size = 0, slide = 0, gap = 0, late = 0, maximum = 4096, budget = 16777216;
    if (!s)
        s = number(member(config, "sizeMicros"), 0, &size);
    if (!s)
        s = number(member(config, "slideMicros"), size, &slide);
    if (!s)
        s = number(member(config, "gapMicros"), 0, &gap);
    if (!s)
        s = number(member(config, "allowedLatenessMicros"), 0, &late);
    if (!s)
        s = number(member(config, "maxWindows"), 4096, &maximum);
    if (!s)
        s = number(member(config, "stateBytes"), o->memory_budget ? o->memory_budget : 16777216,
                   &budget);
    w->mode = named(member(config, "mode"), "session")   ? 3
              : named(member(config, "mode"), "sliding") ? 2
                                                         : 1;
    w->late_policy = named(member(config, "latePolicy"), "drop")       ? 1
                     : named(member(config, "latePolicy"), "redirect") ? 2
                                                                       : 0;
    if (w->mode == 1)
        slide = size;
    if (!s &&
        (!budget || budget < 4096 || !maximum || maximum > 1000000 || size > INT64_MAX ||
         slide > INT64_MAX || gap > INT64_MAX || late > INT64_MAX ||
         (o->memory_budget && budget > o->memory_budget) ||
         (w->mode == 3 ? !gap : !size || !slide || slide > size || (size - 1) / slide >= 4096)))
        s = UD_SCHEMA;
    w->size = (int64_t)size;
    w->slide = (int64_t)slide;
    w->gap = (int64_t)gap;
    w->lateness = (int64_t)late;
    w->maximum = (uint32_t)maximum;
    w->budget = budget;
    if (!s)
        s = bind_changes(w, config);
    w->bucket_count = 4096;
    w->buckets = ud_alloc(ctx, w->bucket_count * sizeof(*w->buckets));
    if (!w->buckets)
        s = UD_RESOURCE;
    if (!s)
        s = o->buffers ? ud_buffer_state_domain_create(o->buffers, budget, &w->memory)
                       : ud_memory_domain_create(ctx, budget, &w->memory);
    if (!s) {
        w->configuration = ud_strdup_n(ctx, o->configuration.data, o->configuration.length);
        if (!w->configuration)
            s = UD_RESOURCE;
        else {
            w->options.configuration = (ud_string){w->configuration, o->configuration.length};
            ud_hash(w->configuration, o->configuration.length, w->fingerprint);
        }
    }
    if (!s)
        s = bind_output(w);
    if (!s) {
        uint8_t digest[32];
        char hex[65];
        s = ud_schema_fingerprint(w->schema, digest);
        for (uint32_t i = 0; !s && i < 32; i++)
            snprintf(hex + i * 2, 3, "%02x", digest[i]);
        if (!s)
            s = ud_id_make(ctx, (ud_string){"native.eventWindow", 18}, (ud_string){hex, 64},
                           &w->output_scope);
    }
    ud_json_destroy(config);
    if (s) {
        ud_event_window_destroy(w);
        return s;
    }
    *out = w;
    return UD_OK;
}
ud_status ud_event_window_schema(const ud_event_window *w, uint32_t port, ud_batch **out) {
    if (port > 1)
        return UD_INVALID;
    const ud_batch *s = port ? w->input : w->schema;
    return ud_batch_create(w->ctx, s->schema->fields, s->columns, 0, out);
}
static ud_status queue_change(ud_event_window *w, event_group *g) {
    if (g->dirty)
        return UD_OK;
    if (g->members) {
        ud_memory_scope scope;
        ud_memory_domain_enter(w->memory, &scope);
        ud_status status = ud_accumulator_snapshot(g->aggregate, &g->before);
        ud_memory_domain_leave(&scope);
        if (status)
            return status;
    }
    g->dirty = 1;
    if (w->dirty_tail)
        w->dirty_tail->dirty_next = g;
    else
        w->dirty_head = g;
    w->dirty_tail = g;
    return UD_OK;
}
static ud_status update_group(ud_event_window *w, const ud_batch *batch, uint32_t row,
                              uint64_t hash, int64_t start, int64_t end, int direction,
                              ud_id source_id) {
    event_group *g = find_group(w, batch, row, hash, start);
    if (direction < 0 && (!g || !g->members))
        return UD_FENCED;
    if (!g)
        UD_TRY(new_group(w, batch, row, hash, start, end, &g));
    UD_TRY(load(w, g));
    if (w->changes) {
        if (direction > 0 && g->members == UINT64_MAX)
            return UD_OVERFLOW;
        UD_TRY(queue_change(w, g));
    }
    ud_memory_scope scope;
    ud_memory_domain_enter(w->memory, &scope);
    ud_status status = ud_accumulator_update_accepted(g->aggregate, batch, row, direction);
    ud_memory_domain_leave(&scope);
    if (!status && w->changes) {
        if (direction > 0)
            g->members++;
        else
            g->members--;
    }
    if (!status)
        status = remember_id(w, g, source_id);
    return status;
}
typedef struct event_input {
    const ud_batch *batch;
    uint32_t row;
    int64_t time, end;
    int accepted, late;
} event_input;
static ud_status bind_event(ud_event_window *w, const ud_batch *batch, uint32_t row,
                            event_input *input) {
    ud_value value;
    UD_TRY(ud_batch_get(batch, row, w->time, &value));
    if (value.is_null || value.is_absent)
        return UD_SCHEMA;
    *input = (event_input){.batch = batch, .row = row, .time = value.integer};
    UD_TRY(ud_plan_accept(w->plan, batch, row, &input->accepted));
    if (!input->accepted)
        return UD_OK;
    int64_t t = input->time;
    if (w->mode == 3) {
        if (t > INT64_MAX - w->gap)
            return UD_OVERFLOW;
        input->end = t + w->gap;
        input->late = w->watermark >= INT64_MIN + w->lateness && t < w->watermark - w->lateness;
    } else {
        int64_t quotient = t / w->slide;
        if (t % w->slide < 0)
            quotient--;
        if (quotient < INT64_MIN / w->slide)
            return UD_OVERFLOW;
        int64_t start = quotient * w->slide;
        if (start > INT64_MAX - w->size)
            return UD_OVERFLOW;
        input->end = start + w->size;
    }
    input->late |= closed(w, input->end);
    return UD_OK;
}
static ud_status add_session(ud_event_window *w, const event_input *input, uint64_t hash) {
    int64_t start = input->time, end = input->end;
    event_group *target = NULL;
    /* Adjacent half-open sessions remain separate. Recheck after each
     * extension because an out-of-order event can bridge multiple sessions. */
    for (;;) {
        int changed = 0;
        event_group **at = &w->buckets[bucket_of(w, hash, 0)];
        while (*at) {
            event_group *g = *at;
            if (g != target && same_key(w, g, input->batch, input->row, hash) &&
                g->start < end && g->end > start) {
                if (!target)
                    target = g;
                else {
                    UD_TRY(load(w, target));
                    UD_TRY(load(w, g));
                    ud_batch *partial = NULL;
                    UD_TRY(ud_accumulator_partial_snapshot(g->aggregate, &partial));
                    ud_memory_scope scope;
                    ud_memory_domain_enter(w->memory, &scope);
                    ud_status status = ud_accumulator_merge(target->aggregate, partial);
                    ud_memory_domain_leave(&scope);
                    ud_batch_destroy(partial);
                    if (status)
                        return status;
                    for (event_id *entry = g->events; !status && entry; entry = entry->next)
                        status = remember_id(w, target, entry->value);
                    if (status)
                        return status;
                }
                if (g->start < start)
                    start = g->start;
                if (g->end > end)
                    end = g->end;
                changed = 1;
                if (g != target) {
                    remove_group(w, g);
                    continue;
                }
            }
            at = &(*at)->hash_next;
        }
        if (!changed)
            break;
    }
    if (!target)
        UD_TRY(new_group(w, input->batch, input->row, hash, start, end, &target));
    target->start = start;
    target->end = end;
    heap_fix(w, target);
    UD_TRY(load(w, target));
    ud_memory_scope scope;
    ud_memory_domain_enter(w->memory, &scope);
    ud_status status = ud_accumulator_update_accepted(target->aggregate, input->batch, input->row, 1);
    ud_memory_domain_leave(&scope);
    if (!status) {
        ud_row_info info;
        status = ud_batch_row_info(input->batch, input->row, &info);
        if (!status)
            status = remember_id(w, target, info.id);
    }
    return status;
}
static ud_status update_event(ud_event_window *w, const event_input *input, int direction) {
    if (!input->accepted)
        return UD_OK;
    uint64_t hash = UD_VALUE_HASH_SEED;
    for (uint32_t key = 0; key < w->plan->key_n; key++) {
        ud_value value;
        UD_TRY(ud_batch_get(input->batch, input->row, w->plan->keys[key], &value));
        if (w->changes && (value.is_absent || value.is_null))
            return UD_INCOMPATIBLE;
        hash = ud_field_value_hash(&w->plan->output_schema->column[key].field, &value, hash);
    }
    if (w->mode == 3)
        return add_session(w, input, hash);
    int64_t start = input->end - w->size;
    for (uint32_t overlap = 0; overlap < 4096; overlap++) {
        int64_t end = start + w->size;
        if (input->time >= end)
            break;
        if (!closed(w, end)) {
            ud_row_info info;
            UD_TRY(ud_batch_row_info(input->batch, input->row, &info));
            UD_TRY(update_group(w, input->batch, input->row, hash, start, end, direction, info.id));
        }
        if (w->mode == 1 || start < INT64_MIN + w->slide)
            break;
        start -= w->slide;
    }
    return UD_OK;
}
static ud_status read_before(ud_event_window *w, const ud_batch *batch, uint32_t row, ud_batch **out) {
    if (w->before_field == UINT32_MAX)
        return UD_INCOMPATIBLE;
    ud_value before;
    UD_TRY(ud_batch_get(batch, row, w->before_field, &before));
    if (before.is_null || before.is_absent || before.child_count != w->input->columns - 1)
        return UD_INCOMPATIBLE;
    ud_context *ctx = w->ctx;
    ud_value *values = ud_alloc(ctx, w->input->columns * sizeof(*values));
    if (!values)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (uint32_t i = 0, j = 0; !status && i < w->input->columns; i++) {
        if (i == w->before_field)
            values[i] = (ud_value){.type = UD_STRUCT, .is_null = 1};
        else {
            values[i] = before.children[j++];
            if (values[i].is_absent)
                status = UD_INCOMPATIBLE;
        }
    }
    if (!status)
        status = ud_batch_create(ctx, w->input->schema->fields, w->input->columns, 1, out);
    if (!status)
        status = ud_batch_append(*out, values, w->input->columns);
    ud_free(values);
    return status;
}
static ud_status redirect_late(ud_event_window *w, const ud_batch *batch, uint32_t row) {
    w->late_rows++;
    if (!w->late_policy)
        return UD_QUALITY;
    if (w->late_policy != 2)
        return UD_OK;
    if (!w->late)
        UD_TRY(ud_batch_create(w->ctx, w->input->schema->fields, w->input->columns, batch->rows,
                               &w->late));
    return ud_batch_append_from(w->late, batch, row);
}
static ud_status consume_row(ud_event_window *w, const ud_batch *batch, uint32_t row) {
    ud_row_info info;
    UD_TRY(ud_batch_row_info(batch, row, &info));
    if (any_group_has_id(w, info.id))
        return UD_OK;
    uint32_t operation = info.operation ? info.operation : UD_CHANGE_INSERT;
    if (operation == UD_CHANGE_UPSERT || operation == UD_CHANGE_TRUNCATE ||
        (operation != UD_CHANGE_INSERT && !w->changes))
        return UD_UNSUPPORTED;
    if (operation > UD_CHANGE_TRUNCATE)
        return UD_CORRUPT;
    event_input prior = {0}, after = {0};
    ud_batch *before = NULL;
    ud_status status = UD_OK;
    if (operation != UD_CHANGE_INSERT) {
        status = read_before(w, batch, row, &before);
        if (!status)
            status = bind_event(w, before, 0, &prior);
    }
    if (!status && operation != UD_CHANGE_DELETE)
        status = bind_event(w, batch, row, &after);
    if (!status && (prior.late || after.late))
        status = redirect_late(w, batch, row);
    else if (!status) {
        status = update_event(w, &prior, -1);
        if (!status)
            status = update_event(w, &after, 1);
    }
    ud_batch_destroy(before);
    return status;
}
ud_status ud_event_window_push(ud_event_window *w, const ud_batch *batch) {
    UD_TRY(check(w));
    if (w->ended || w->late || w->dirty_head)
        return UD_BUSY;
    UD_TRY(ud_plan_check_batch(w->plan, batch));
    ud_status status = UD_OK;
    for (uint32_t row = 0; !status && row < batch->rows; row++) {
        status = check(w);
        if (!status)
            status = consume_row(w, batch, row);
        if (!status)
            status = rebalance(w);
    }
    if (!status && w->late)
        status = ud_batch_seal(w->late);
    if (status && w->changes)
        w->failed = 1;
    return status;
}
static void evict_changes(ud_event_window *w) {
    while (w->changes && w->count && closed(w, w->heap[0]->end))
        remove_group(w, w->heap[0]);
}
ud_status ud_event_window_watermark(ud_event_window *w, int64_t watermark) {
    UD_TRY(check(w));
    if (watermark < w->watermark)
        return UD_FENCED;
    if (w->changes && (w->dirty_head || w->late))
        return UD_BUSY;
    w->watermark = watermark;
    evict_changes(w);
    return UD_OK;
}
ud_status ud_event_window_end(ud_event_window *w) {
    UD_TRY(check(w));
    if (w->changes && (w->dirty_head || w->late))
        return UD_BUSY;
    w->ended = 1;
    evict_changes(w);
    return UD_OK;
}
uint64_t ud_event_window_demand(const ud_event_window *w) {
    if (w->ended || w->late || w->dirty_head)
        return 0;
    return !w->count || !closed(w, w->heap[0]->end);
}
static ud_status output_values(const event_group *g, const ud_batch *aggregate, ud_value *values) {
    values[0] = (ud_value){.type = UD_INT64, .integer = g->start};
    values[1] = (ud_value){.type = UD_INT64, .integer = g->end};
    for (uint32_t i = 0; i < aggregate->columns; i++)
        UD_TRY(ud_batch_get(aggregate, 0, i, values + 2 + i));
    return UD_OK;
}
static ud_status append_window(ud_event_window *w, event_group *g, ud_batch *batch,
                               ud_value *values, ud_value *prior) {
    if (w->changes && !g->members && !g->before)
        return UD_OK; /* The group was empty both before and after this input batch. */
    if (w->changes && g->revision == INT64_MAX)
        return UD_OVERFLOW;
    ud_batch *aggregate = NULL;
    if (!w->changes || g->members) {
        UD_TRY(load(w, g));
        UD_TRY(ud_accumulator_snapshot(g->aggregate, &aggregate));
    }
    const ud_batch *selected = aggregate ? aggregate : g->before;
    ud_status status = output_values(g, selected, values);
    ud_row_info info = selected->row_info[0];
    if (!status && w->changes) {
        uint32_t revision = batch->columns - 2;
        values[revision] = (ud_value){.type = UD_INT64, .integer = (int64_t)g->revision + 1};
        values[revision + 1] = (ud_value){.type = UD_STRUCT, .is_null = !g->before};
        if (g->before) {
            status = output_values(g, g->before, prior);
            prior[revision] = (ud_value){.type = UD_INT64, .integer = (int64_t)g->revision};
            values[revision + 1].children = prior;
            values[revision + 1].child_count = revision + 1;
        }
        info.operation = !g->members ? UD_CHANGE_DELETE
                         : g->before  ? UD_CHANGE_UPDATE
                                      : UD_CHANGE_INSERT;
    }
    if (!status)
        status = ud_batch_append(batch, values, batch->columns);
    if (!status)
        status = ud_batch_set_row_info(batch, batch->rows - 1, &info);
    if (!status && w->changes)
        g->revision++;
    ud_batch_destroy(aggregate);
    return status;
}
static event_group *emittable(const ud_event_window *w) {
    if (w->changes)
        return w->dirty_head;
    return w->count && closed(w, w->heap[0]->end) ? w->heap[0] : NULL;
}
static void published(ud_event_window *w, event_group *g) {
    if (!w->changes) {
        remove_group(w, g);
        return;
    }
    w->dirty_head = g->dirty_next;
    if (!w->dirty_head)
        w->dirty_tail = NULL;
    g->dirty_next = NULL;
    g->dirty = 0;
    ud_batch_destroy(g->before);
    g->before = NULL;
    if (!g->members && !g->revision)
        remove_group(w, g);
}
ud_status ud_event_window_pull(ud_event_window *w, ud_batch **out, uint32_t *port) {
    *out = NULL;
    UD_TRY(check(w));
    if (port)
        *port = 0;
    if (w->late) {
        uint32_t rows = w->late->rows - w->late_at;
        if (rows > w->options.max_batch_rows)
            rows = w->options.max_batch_rows;
        UD_TRY(ud_batch_slice(w->ctx, w->late, w->late_at, rows, out));
        w->late_at += rows;
        if (port)
            *port = 1;
        if (w->late_at == w->late->rows) {
            ud_batch_destroy(w->late);
            w->late = NULL;
            w->late_at = 0;
        }
        return UD_OK;
    }
    if (!emittable(w))
        return w->ended ? UD_END : UD_BUSY;
    uint32_t limit = w->changes ? w->options.max_batch_rows : 1;
    uint32_t capacity = limit < w->count ? limit : w->count;
    uint32_t columns = w->schema->columns;
    ud_batch *batch = NULL;
    UD_TRY(ud_batch_create(w->ctx, w->schema->schema->fields, columns, capacity, &batch));
    ud_value *values = ud_alloc(w->ctx, (size_t)columns * (1 + w->changes) * sizeof(*values));
    ud_status status = values ? UD_OK : UD_RESOURCE;
    event_group *g;
    while (!status && batch->rows < limit && (g = emittable(w))) {
        status = check(w);
        if (!status)
            status = append_window(w, g, batch, values, values + columns);
        if (!status)
            published(w, g);
    }
    if (!status && batch->rows)
        status = ud_batch_seal(batch);
    uint32_t *keys = NULL;
    if (!status && batch->rows) {
        keys = ud_alloc(w->ctx, (w->plan->key_n + 2) * sizeof(*keys));
        if (!keys)
            status = UD_RESOURCE;
    }
    if (!status && batch->rows) {
        for (uint32_t i = 0; i < w->plan->key_n + 2; i++)
            keys[i] = i;
        status = ud_batch_identify_keys(w->ctx, batch, w->output_scope, keys, w->plan->key_n + 2, out);
    }
    if (!status && !batch->rows)
        status = UD_BUSY;
    if (!status) {
        for (uint32_t row = 0; row < batch->rows; row++) {
            ud_row_info info = batch->row_info[row];
            info.id = (*out)->row_info[row].id;
            (*out)->row_info[row] = info;
        }
        w->emitted += batch->rows;
    }
    if (status && status != UD_BUSY && w->changes)
        w->failed = 1;
    ud_free(keys);
    ud_free(values);
    ud_batch_destroy(batch);
    return status;
}
static const ud_field snapshot_fields[] = {{.name = "start", .name_length = 5, .type = UD_INT64},
                                           {.name = "end", .name_length = 3, .type = UD_INT64},
                                           {.name = "members", .name_length = 7, .type = UD_UINT64},
                                           {.name = "revision", .name_length = 8, .type = UD_UINT64},
                                           {.name = "state", .name_length = 5, .type = UD_BINARY}};
static const uint8_t group_state_magic[] = {'U', 'D', 'E', 'W', '0', '0', '0', '1'};
static ud_status encode_group_state(ud_context *ctx, const ud_batch *partial,
                                     const event_group *g, uint8_t **out, uint64_t *bytes) {
    uint8_t *aggregate = NULL;
    uint64_t aggregate_bytes = 0;
    UD_TRY(ud_batch_serialize(partial, &aggregate, &aggregate_bytes));
    ud_bytes encoded = {.ctx = ctx};
    ud_status status = ud_bytes_put(&encoded, group_state_magic, sizeof(group_state_magic));
    if (!status)
        status = ud_put_u64(&encoded, aggregate_bytes);
    if (!status)
        status = ud_put_u64(&encoded, g->event_count);
    if (!status && aggregate_bytes > SIZE_MAX)
        status = UD_RESOURCE;
    if (!status)
        status = ud_bytes_put(&encoded, aggregate, (size_t)aggregate_bytes);
    for (event_id *entry = g->events; !status && entry; entry = entry->next) {
        status = ud_put_u64(&encoded, entry->value.high);
        if (!status)
            status = ud_put_u64(&encoded, entry->value.low);
    }
    ud_free(aggregate);
    if (!status && encoded.size > UINT32_MAX)
        status = UD_RESOURCE;
    if (status) {
        ud_bytes_clear(&encoded);
        return status;
    }
    *out = encoded.data;
    *bytes = encoded.size;
    encoded.data = NULL;
    ud_bytes_clear(&encoded);
    return UD_OK;
}
static void free_event_ids(event_id *events) {
    while (events) {
        event_id *next = events->next;
        ud_free(events);
        events = next;
    }
}
static ud_status decode_group_state(ud_event_window *w, const ud_value *payload, ud_batch **partial,
                                    event_id **events, uint32_t *event_count) {
    *partial = NULL;
    *events = NULL;
    *event_count = 0;
    if (payload->is_null || payload->is_absent || payload->text_length < sizeof(group_state_magic))
        return UD_CORRUPT;
    ud_bytes encoded = {.ctx = w->ctx,
                        .data = (uint8_t *)payload->text,
                        .size = payload->text_length,
                        .pos = sizeof(group_state_magic)};
    if (memcmp(encoded.data, group_state_magic, sizeof(group_state_magic)))
        return UD_CORRUPT;
    uint64_t aggregate_bytes = 0, count = 0;
    ud_status status = ud_get_u64(&encoded, &aggregate_bytes);
    if (!status)
        status = ud_get_u64(&encoded, &count);
    if (!status && (count > UINT32_MAX || aggregate_bytes > encoded.size - encoded.pos))
        status = UD_CORRUPT;
    if (!status)
        status = ud_batch_deserialize(w->ctx, encoded.data + encoded.pos, (size_t)aggregate_bytes,
                                      partial);
    if (!status)
        encoded.pos += (size_t)aggregate_bytes;
    for (uint64_t i = 0; !status && i < count; i++) {
        ud_memory_scope scope;
        ud_memory_domain_enter(w->memory, &scope);
        event_id *entry = ud_alloc(w->ctx, sizeof(*entry));
        ud_memory_domain_leave(&scope);
        uint64_t high = 0, low = 0;
        if (!entry)
            status = UD_RESOURCE;
        if (!status)
            status = ud_get_u64(&encoded, &high);
        if (!status)
            status = ud_get_u64(&encoded, &low);
        if (!status && (!high && !low))
            status = UD_CORRUPT;
        if (!status && has_id(*events, (ud_id){high, low}))
            status = UD_CORRUPT;
        if (status) {
            ud_free(entry);
            break;
        }
        entry->value = (ud_id){high, low};
        entry->next = *events;
        *events = entry;
        (*event_count)++;
    }
    if (!status && encoded.pos != encoded.size)
        status = UD_CORRUPT;
    if (status) {
        free_event_ids(*events);
        *events = NULL;
        *event_count = 0;
        ud_batch_destroy(*partial);
        *partial = NULL;
    }
    return status;
}
ud_status ud_event_window_checkpoint(ud_event_window *w, ud_batch **out) {
    *out = NULL;
    if (w->late || w->dirty_head || (!ud_event_window_demand(w) && !w->ended))
        return UD_BUSY;
    UD_TRY(check(w));
    UD_TRY(ud_batch_create(w->ctx, snapshot_fields, 5, w->count + 1, out));
    ud_bytes header = {.ctx = w->ctx};
    ud_status s = ud_bytes_put(&header, w->fingerprint, 32);
    if (!s)
        s = ud_put_u64(&header, (uint64_t)w->watermark);
    if (!s)
        s = ud_put_u64(&header, w->late_rows);
    if (!s)
        s = ud_put_u64(&header, w->ended);
    if (!s)
        s = ud_put_u64(&header, w->emitted);
    ud_value row[5] = {
        {.type = UD_INT64},
        {.type = UD_INT64},
        {.type = UD_UINT64},
        {.type = UD_UINT64},
        {.type = UD_BINARY, .text = (char *)header.data, .text_length = (uint32_t)header.size}};
    if (!s)
        s = ud_batch_append(*out, row, 5);
    ud_bytes_clear(&header);
    for (event_group *g = w->groups; !s && g; g = g->next) {
        ud_batch *partial = NULL;
        ud_buffer_view *view = NULL;
        s = g->spill ? ud_buffer_read(g->spill, &view)
                     : ud_accumulator_partial_snapshot(g->aggregate, &partial);
        uint8_t *wire = NULL;
        uint64_t bytes = 0;
        if (!s)
            s = encode_group_state(w->ctx, view ? ud_buffer_view_batch(view) : partial, g, &wire,
                                   &bytes);
        row[0].integer = g->start;
        row[1].integer = g->end;
        row[2].unsigned_integer = g->members;
        row[3].unsigned_integer = g->revision;
        row[4].text = (char *)wire;
        row[4].text_length = (uint32_t)bytes;
        if (!s)
            s = ud_batch_append(*out, row, 5);
        ud_free(wire);
        ud_batch_destroy(partial);
        ud_buffer_view_release(view);
    }
    if (!s)
        s = ud_batch_seal(*out);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_event_window_restore(ud_event_window **instance, const ud_batch *state) {
    ud_event_window *old = *instance, *w = NULL;
    if (!state || state->columns != 5 || !state->rows || state->rows - 1 > old->maximum)
        return UD_CORRUPT;
    for (uint32_t i = 0; i < 5; i++)
        if (state->column[i].field.type != snapshot_fields[i].type)
            return UD_CORRUPT;
    ud_value row[5];
    for (uint32_t i = 0; i < 5; i++)
        UD_TRY(ud_batch_get(state, 0, i, row + i));
    if (row[4].is_null || row[4].text_length != 64 || memcmp(row[4].text, old->fingerprint, 32))
        return UD_INCOMPATIBLE;
    ud_status s = ud_event_window_create(old->ctx, &old->options, old->input, &w);
    ud_bytes header = {.ctx = old->ctx, .data = (uint8_t *)row[4].text, .size = 64, .pos = 32};
    uint64_t mark = 0, ended = 0;
    if (!s)
        s = ud_get_u64(&header, &mark);
    if (!s)
        w->watermark = (int64_t)mark;
    if (!s)
        s = ud_get_u64(&header, &w->late_rows);
    if (!s)
        s = ud_get_u64(&header, &ended);
    if (!s)
        s = ud_get_u64(&header, &w->emitted);
    if (!s && header.pos != header.size)
        s = UD_CORRUPT;
    if (!s && ended > 1)
        s = UD_CORRUPT;
    if (!s)
        w->ended = (int)ended;
    for (uint32_t at = 1; !s && at < state->rows; at++) {
        for (uint32_t i = 0; !s && i < 5; i++) {
            s = ud_batch_get(state, at, i, row + i);
            if (!s && (row[i].is_null || row[i].is_absent))
                s = UD_CORRUPT;
        }
        if (!s && (row[0].integer >= row[1].integer || row[3].unsigned_integer > INT64_MAX ||
                   (w->changes ? !row[3].unsigned_integer || closed(w, row[1].integer)
                               : row[2].unsigned_integer || row[3].unsigned_integer)))
            s = UD_CORRUPT;
        ud_batch *partial = NULL;
        event_id *events = NULL;
        uint32_t event_count = 0;
        event_group *g = NULL;
        if (!s)
            s = decode_group_state(w, row + 4, &partial, &events, &event_count);
        if (!s && partial->rows != 1)
            s = UD_CORRUPT;
        if (!s) {
            g = ud_alloc(w->ctx, sizeof(*g));
            if (!g)
                s = UD_RESOURCE;
            else {
                memset(g, 0, sizeof(*g));
                g->start = row[0].integer;
                g->end = row[1].integer;
                g->members = row[2].unsigned_integer;
                g->revision = row[3].unsigned_integer;
                g->events = events;
                g->event_count = event_count;
                events = NULL;
                g->keys = ud_alloc(w->ctx, (w->plan->key_n + 1) * sizeof(*g->keys));
                if (!g->keys)
                    s = UD_RESOURCE;
            }
        }
        uint64_t hash = UD_VALUE_HASH_SEED;
        for (uint32_t k = 0; !s && k < w->plan->key_n; k++) {
            ud_value key;
            s = ud_batch_get(partial, 0, k, &key);
            if (!s) {
                hash = ud_field_value_hash(&w->plan->output_schema->column[k].field, &key, hash);
                s = ud_copy_value(w->ctx, &key, g->keys + k, 0);
            }
        }
        if (!s) {
            g->hash = hash;
            s = load(w, g);
        }
        if (!s) {
            ud_memory_scope scope;
            ud_memory_domain_enter(w->memory, &scope);
            s = ud_accumulator_merge(g->aggregate, partial);
            ud_memory_domain_leave(&scope);
        }
        ud_batch_destroy(partial);
        free_event_ids(events);
        if (!s)
            s = insert_group(w, g);
        if (!s)
            s = rebalance(w);
        else if (g)
            destroy_group(w, g);
    }
    if (s) {
        ud_event_window_destroy(w);
        return s;
    }
    *instance = w;
    ud_event_window_destroy(old);
    return UD_OK;
}
ud_status ud_event_window_diagnostics(const ud_event_window *w, char **out, uint32_t *length) {
    char text[384];
    int n = snprintf(
        text, sizeof(text),
        "{\"activeWindows\":%u,\"stateBytes\":%llu,\"stateHighWater\":%llu,\"spilledStates\":%llu,"
        "\"watermark\":%lld,\"lateRows\":%llu,\"emittedWindows\":%llu,\"timeUnit\":"
        "\"microseconds\",\"trigger\":\"%s\"}",
        w->count, (unsigned long long)ud_memory_domain_used(w->memory),
        (unsigned long long)ud_memory_domain_peak(w->memory), (unsigned long long)w->spills,
        (long long)w->watermark, (unsigned long long)w->late_rows, (unsigned long long)w->emitted,
        w->changes ? "changes" : "final-after-lateness");
    *out = ud_strdup_n(w->ctx, text, (size_t)n);
    *length = (uint32_t)n;
    return *out ? UD_OK : UD_RESOURCE;
}
void ud_event_window_destroy(ud_event_window *w) {
    if (!w)
        return;
    while (w->groups) {
        event_group *g = w->groups;
        w->groups = g->next;
        destroy_group(w, g);
    }
    ud_batch_destroy(w->input);
    ud_batch_destroy(w->schema);
    ud_batch_destroy(w->late);
    ud_plan_destroy(w->plan);
    ud_memory_domain_destroy(w->memory);
    ud_free(w->buckets);
    ud_free(w->heap);
    ud_free(w->configuration);
    ud_free(w);
}
