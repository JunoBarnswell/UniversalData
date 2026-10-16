#include "event_window.h"
#include "../../execution/state.h"
/* Event-time lifecycle owned by the existing Compute operator. Accumulator
 * states and spilled snapshots use the existing typed statistics/Block Store. */
typedef struct event_group {
    int64_t start, end;
    uint64_t hash;
    ud_value *keys;
    ud_accumulator *aggregate;
    ud_buffer_block *spill;
    struct event_group *next;
} event_group;
struct ud_event_window {
    ud_context *ctx;
    ud_operator_options options;
    ud_batch *input, *schema, *late;
    ud_plan *plan;
    ud_memory_domain *memory;
    char *configuration;
    uint8_t fingerprint[32];
    ud_id output_scope;
    event_group *groups;
    uint32_t time, count, maximum, late_at;
    int mode, late_policy, ended;
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
    ud_free(g);
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
        if (ud_batch_get(b, row, w->plan->keys[i], &v) || ud_value_compare(g->keys + i, &v))
            return 0;
    }
    return 1;
}
static ud_status new_group(ud_event_window *w, const ud_batch *b, uint32_t row, uint64_t hash,
                           int64_t start, int64_t end, event_group **out) {
    if (w->count >= w->maximum)
        return UD_RESOURCE;
    event_group *g = ud_alloc(w->ctx, sizeof(*g));
    if (!g)
        return UD_RESOURCE;
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
    if (s) {
        destroy_group(w, g);
        return s;
    }
    g->next = w->groups;
    w->groups = g;
    w->count++;
    *out = g;
    return UD_OK;
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
    uint32_t columns = !s ? w->plan->output_schema->columns : 0;
    ud_field *fields = !s ? ud_alloc(ctx, (columns + 2) * sizeof(*fields)) : NULL;
    if (!s && !fields)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < columns; i++)
        fields[i + 2] = w->plan->output_schema->column[i].field;
    const char *names[] = {"_window_start", "_window_end"};
    for (uint32_t i = 0; !s && i < 2; i++) {
        ud_field field = {.type = UD_INT64};
        s = ud_field_derive(ctx, &field, w->options.configuration,
                            (ud_string){names[i], (uint32_t)strlen(names[i])}, fields + i);
    }
    if (!s)
        s = ud_batch_create(ctx, fields, columns + 2, 0, &w->schema);
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
    if (fields) {
        ud_field_clear(fields);
        ud_field_clear(fields + 1);
    }
    ud_free(fields);
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
static ud_status add(ud_event_window *w, const ud_batch *b, uint32_t row, uint64_t hash,
                     int64_t start, int64_t end) {
    event_group *g = w->groups;
    for (; g; g = g->next)
        if (g->start == start && same_key(w, g, b, row, hash))
            break;
    if (!g)
        UD_TRY(new_group(w, b, row, hash, start, end, &g));
    UD_TRY(load(w, g));
    ud_memory_scope scope;
    ud_memory_domain_enter(w->memory, &scope);
    ud_status s = ud_accumulator_add_accepted(g->aggregate, b, row);
    ud_memory_domain_leave(&scope);
    return s;
}
ud_status ud_event_window_push(ud_event_window *w, const ud_batch *b) {
    UD_TRY(check(w));
    if (w->ended || w->late)
        return UD_BUSY;
    UD_TRY(ud_plan_check_batch(w->plan, b));
    for (uint32_t row = 0; row < b->rows; row++) {
        UD_TRY(check(w));
        ud_value time;
        UD_TRY(ud_batch_get(b, row, w->time, &time));
        if (time.is_null || time.is_absent)
            return UD_SCHEMA;
        int accept = 0;
        UD_TRY(ud_plan_accept(w->plan, b, row, &accept));
        if (!accept)
            continue;
        int64_t t = time.integer, expiry;
        if (w->mode == 3) {
            if (t > INT64_MAX - w->gap)
                return UD_OVERFLOW;
            expiry = t + w->gap;
        } else {
            int64_t quotient = t / w->slide;
            if (t % w->slide < 0)
                quotient--;
            if (quotient < INT64_MIN / w->slide)
                return UD_OVERFLOW;
            int64_t start = quotient * w->slide;
            if (start > INT64_MAX - w->size)
                return UD_OVERFLOW;
            expiry = start + w->size;
        }
        /* Session admission uses an event-time fence. Without retractions,
         * admitting a record behind this fence could reopen an emitted session. */
        int session_late = w->mode == 3 && w->watermark >= INT64_MIN + w->lateness &&
                           t < w->watermark - w->lateness;
        if (closed(w, expiry) || session_late) {
            w->late_rows++;
            if (!w->late_policy)
                return UD_QUALITY;
            if (w->late_policy == 2) {
                if (!w->late)
                    UD_TRY(ud_batch_create(w->ctx, w->input->schema->fields, w->input->columns,
                                           b->rows, &w->late));
                UD_TRY(ud_batch_append_from(w->late, b, row));
            }
            continue;
        }
        uint64_t hash = UD_VALUE_HASH_SEED;
        for (uint32_t key = 0; key < w->plan->key_n; key++) {
            ud_value v;
            UD_TRY(ud_batch_get(b, row, w->plan->keys[key], &v));
            hash = ud_value_hash(&v, hash);
        }
        if (w->mode == 3) {
            int64_t start = t, end = expiry;
            event_group *target = NULL;
            /* Transitive merging is necessary when an out-of-order event joins
             * two prior sessions. Publish only after end+lateness, never retract. */
            for (;;) {
                int changed = 0;
                event_group **at = &w->groups;
                while (*at) {
                    event_group *g = *at;
                    if (g != target && same_key(w, g, b, row, hash) && g->start < end &&
                        g->end > start) {
                        if (!target) {
                            target = g;
                            if (g->start < start)
                                start = g->start;
                            if (g->end > end)
                                end = g->end;
                            changed = 1;
                        } else {
                            UD_TRY(load(w, target));
                            UD_TRY(load(w, g));
                            ud_batch *partial = NULL;
                            UD_TRY(ud_accumulator_partial_snapshot(g->aggregate, &partial));
                            ud_memory_scope scope;
                            ud_memory_domain_enter(w->memory, &scope);
                            ud_status s = ud_accumulator_merge(target->aggregate, partial);
                            ud_memory_domain_leave(&scope);
                            ud_batch_destroy(partial);
                            if (s)
                                return s;
                            if (g->start < start)
                                start = g->start;
                            if (g->end > end)
                                end = g->end;
                            *at = g->next;
                            destroy_group(w, g);
                            w->count--;
                            changed = 1;
                            continue;
                        }
                    }
                    at = &(*at)->next;
                }
                if (!changed)
                    break;
            }
            if (!target)
                UD_TRY(new_group(w, b, row, hash, start, end, &target));
            target->start = start;
            target->end = end;
            UD_TRY(load(w, target));
            ud_memory_scope scope;
            ud_memory_domain_enter(w->memory, &scope);
            ud_status s = ud_accumulator_add_accepted(target->aggregate, b, row);
            ud_memory_domain_leave(&scope);
            if (s)
                return s;
        } else {
            int64_t start = expiry - w->size;
            for (uint32_t overlap = 0; overlap < 4096; overlap++) {
                int64_t end = start + w->size;
                if (t >= end)
                    break;
                if (!closed(w, end))
                    UD_TRY(add(w, b, row, hash, start, end));
                if (w->mode == 1 || start < INT64_MIN + w->slide)
                    break;
                start -= w->slide;
            }
        }
        UD_TRY(rebalance(w));
    }
    if (w->late)
        UD_TRY(ud_batch_seal(w->late));
    return UD_OK;
}
ud_status ud_event_window_watermark(ud_event_window *w, int64_t watermark) {
    UD_TRY(check(w));
    if (watermark < w->watermark)
        return UD_FENCED;
    w->watermark = watermark;
    return UD_OK;
}
ud_status ud_event_window_end(ud_event_window *w) {
    UD_TRY(check(w));
    w->ended = 1;
    return UD_OK;
}
uint64_t ud_event_window_demand(const ud_event_window *w) {
    if (w->ended || w->late)
        return 0;
    for (event_group *g = w->groups; g; g = g->next)
        if (closed(w, g->end))
            return 0;
    return 1;
}
static int precedes(const ud_event_window *w, const event_group *a, const event_group *b) {
    if (a->end != b->end)
        return a->end < b->end;
    if (a->start != b->start)
        return a->start < b->start;
    for (uint32_t i = 0; i < w->plan->key_n; i++) {
        int order = ud_value_compare(a->keys + i, b->keys + i);
        if (order)
            return order < 0;
    }
    return 0;
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
    event_group **best = NULL;
    for (event_group **at = &w->groups; *at; at = &(*at)->next)
        if (closed(w, (*at)->end) && (!best || precedes(w, *at, *best)))
            best = at;
    if (!best)
        return w->ended ? UD_END : UD_BUSY;
    event_group *g = *best;
    UD_TRY(load(w, g));
    ud_batch *result = NULL;
    UD_TRY(ud_accumulator_snapshot(g->aggregate, &result));
    ud_status s = ud_batch_create(w->ctx, w->schema->schema->fields, w->schema->columns, 1, out);
    ud_value *values = ud_alloc(w->ctx, w->schema->columns * sizeof(*values));
    if (!values && !s)
        s = UD_RESOURCE;
    if (!s) {
        values[0] = (ud_value){.type = UD_INT64, .integer = g->start};
        values[1] = (ud_value){.type = UD_INT64, .integer = g->end};
    }
    for (uint32_t i = 0; !s && i < result->columns; i++)
        s = ud_batch_get(result, 0, i, values + i + 2);
    if (!s)
        s = ud_batch_append(*out, values, w->schema->columns);
    if (!s)
        s = ud_batch_seal(*out);
    uint32_t *keys = NULL;
    if (!s) {
        keys = ud_alloc(w->ctx, (w->plan->key_n + 2) * sizeof(*keys));
        if (!keys)
            s = UD_RESOURCE;
    }
    for (uint32_t i = 0; !s && i < w->plan->key_n + 2; i++)
        keys[i] = i;
    ud_batch *identified = NULL;
    if (!s)
        s = ud_batch_identify_keys(w->ctx, *out, w->output_scope, keys, w->plan->key_n + 2,
                                   &identified);
    if (!s) {
        identified->row_info[0].flags = result->row_info[0].flags;
        identified->row_info[0].rule = result->row_info[0].rule;
        ud_batch_destroy(*out);
        *out = identified;
    }
    ud_free(keys);
    ud_batch_destroy(result);
    ud_free(values);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s;
    }
    *best = g->next;
    destroy_group(w, g);
    w->count--;
    w->emitted++;
    return UD_OK;
}
static const ud_field snapshot_fields[] = {{.name = "start", .name_length = 5, .type = UD_INT64},
                                           {.name = "end", .name_length = 3, .type = UD_INT64},
                                           {.name = "state", .name_length = 5, .type = UD_BINARY}};
ud_status ud_event_window_checkpoint(ud_event_window *w, ud_batch **out) {
    *out = NULL;
    if (w->late || (!ud_event_window_demand(w) && !w->ended))
        return UD_BUSY;
    UD_TRY(check(w));
    UD_TRY(ud_batch_create(w->ctx, snapshot_fields, 3, w->count + 1, out));
    ud_bytes header = {.ctx = w->ctx};
    ud_status s = ud_bytes_put(&header, w->fingerprint, 32);
    if (!s)
        s = ud_put_u64(&header, (uint64_t)w->watermark);
    if (!s)
        s = ud_put_u64(&header, w->late_rows);
    if (!s)
        s = ud_put_u64(&header, w->ended);
    ud_value row[3] = {
        {.type = UD_INT64},
        {.type = UD_INT64},
        {.type = UD_BINARY, .text = (char *)header.data, .text_length = (uint32_t)header.size}};
    if (!s)
        s = ud_batch_append(*out, row, 3);
    ud_bytes_clear(&header);
    for (event_group *g = w->groups; !s && g; g = g->next) {
        ud_batch *partial = NULL;
        ud_buffer_view *view = NULL;
        s = g->spill ? ud_buffer_read(g->spill, &view)
                     : ud_accumulator_partial_snapshot(g->aggregate, &partial);
        uint8_t *wire = NULL;
        uint64_t bytes = 0;
        if (!s)
            s = ud_batch_serialize(view ? ud_buffer_view_batch(view) : partial, &wire, &bytes);
        if (!s && bytes > UINT32_MAX)
            s = UD_RESOURCE;
        row[0].integer = g->start;
        row[1].integer = g->end;
        row[2].text = (char *)wire;
        row[2].text_length = (uint32_t)bytes;
        if (!s)
            s = ud_batch_append(*out, row, 3);
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
    if (!state || state->columns != 3 || !state->rows || state->rows - 1 > old->maximum)
        return UD_CORRUPT;
    for (uint32_t i = 0; i < 3; i++)
        if (state->column[i].field.type != snapshot_fields[i].type)
            return UD_CORRUPT;
    ud_value row[3];
    for (uint32_t i = 0; i < 3; i++)
        UD_TRY(ud_batch_get(state, 0, i, row + i));
    if (row[2].is_null || row[2].text_length != 56 || memcmp(row[2].text, old->fingerprint, 32))
        return UD_INCOMPATIBLE;
    ud_status s = ud_event_window_create(old->ctx, &old->options, old->input, &w);
    ud_bytes header = {.ctx = old->ctx, .data = (uint8_t *)row[2].text, .size = 56, .pos = 32};
    uint64_t mark = 0, ended = 0;
    if (!s)
        s = ud_get_u64(&header, &mark);
    if (!s)
        w->watermark = (int64_t)mark;
    if (!s)
        s = ud_get_u64(&header, &w->late_rows);
    if (!s)
        s = ud_get_u64(&header, &ended);
    if (!s && ended > 1)
        s = UD_CORRUPT;
    if (!s)
        w->ended = (int)ended;
    for (uint32_t at = 1; !s && at < state->rows; at++) {
        for (uint32_t i = 0; !s && i < 3; i++)
            s = ud_batch_get(state, at, i, row + i);
        if (!s && (row[0].is_null || row[1].is_null || row[2].is_null ||
                   row[0].integer >= row[1].integer))
            s = UD_CORRUPT;
        ud_batch *partial = NULL;
        if (!s)
            s = ud_batch_deserialize(w->ctx, (const uint8_t *)row[2].text, row[2].text_length,
                                     &partial);
        if (!s && partial->rows != 1)
            s = UD_CORRUPT;
        event_group *g = NULL;
        if (!s) {
            g = ud_alloc(w->ctx, sizeof(*g));
            if (!g)
                s = UD_RESOURCE;
            else {
                g->start = row[0].integer;
                g->end = row[1].integer;
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
                hash = ud_value_hash(&key, hash);
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
        if (!s) {
            g->next = w->groups;
            w->groups = g;
            w->count++;
            s = rebalance(w);
        } else if (g)
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
        "\"microseconds\",\"trigger\":\"final-after-lateness\"}",
        w->count, (unsigned long long)ud_memory_domain_used(w->memory),
        (unsigned long long)ud_memory_domain_peak(w->memory), (unsigned long long)w->spills,
        (long long)w->watermark, (unsigned long long)w->late_rows, (unsigned long long)w->emitted);
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
    ud_free(w->configuration);
    ud_free(w);
}
