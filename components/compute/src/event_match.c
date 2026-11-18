#include "event_match.h"
#include "../../execution/state.h"

enum { BUCKETS = 128 };
typedef struct event_entry {
    struct event_entry *next;
    ud_batch *record;
    uint64_t hash;
    int64_t time;
} event_entry;

struct ud_event_match {
    ud_context *ctx;
    ud_operator_options options;
    ud_memory_domain *memory;
    ud_batch *inputs[2], *schema, *input;
    event_entry *table[2][BUCKETS], *probe;
    char *configuration;
    uint8_t fingerprint[32], schema_digest[2][32];
    uint32_t ports, key_count, keys[2][16], clocks[2], ended, side, row, counts[2];
    int has_mark, probing;
    int64_t interval, mark, probe_time;
    uint64_t late, emitted, budget, probe_hash;
};

static const ud_json *member(const ud_json *json, const char *name) {
    return ud_json_find(json, name, (uint32_t)strlen(name));
}
static ud_status integer(const ud_json *j, uint64_t *value) {
    const char *text;
    uint32_t n;
    UD_TRY(ud_json_text(j, &text, &n));
    *value = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (text[i] < '0' || text[i] > '9' || *value > (UINT64_MAX - (text[i] - '0')) / 10)
            return UD_SCHEMA;
        *value = *value * 10 + (unsigned)(text[i] - '0');
    }
    return UD_OK;
}
static ud_status check(const ud_event_match *m) {
    if (ud_is_cancelled(m->ctx))
        return UD_CANCELLED;
    return m->options.check ? m->options.check(m->options.user) : UD_OK;
}
static ud_status bind_input(ud_event_match *m, const ud_json *config, const ud_batch *input,
                            uint32_t port) {
    const ud_json *keys = member(config, m->ports == 1 ? "keys" : port ? "rightKeys" : "leftKeys");
    uint32_t n = ud_json_size(keys);
    if (port && n != m->key_count)
        return UD_SCHEMA;
    m->key_count = n;
    UD_TRY(ud_compute_field_index(input, member(config, m->ports == 1 ? "timeField"
                                                                : port ? "rightTimeField"
                                                                       : "leftTimeField"),
                                 &m->clocks[port]));
    const ud_field *clock = &input->column[m->clocks[port]].field;
    if (clock->nullable || (clock->type != UD_TIMESTAMP && clock->type != UD_INT64))
        return UD_SCHEMA;
    for (uint32_t k = 0; k < n; k++) {
        uint32_t col;
        UD_TRY(ud_compute_field_index(input, ud_json_at(keys, k), &col));
        for (uint32_t prior = 0; prior < k; prior++)
            if (m->keys[port][prior] == col)
                return UD_SCHEMA;
        m->keys[port][k] = col;
        if (port) {
            const ud_field *left = &m->inputs[0]->column[m->keys[0][k]].field;
            const ud_field *right = &input->column[col].field;
            if (left->type != right->type || left->type >= UD_STRUCT ||
                (left->type == UD_UTF8 &&
                 (left->scalar.locale_id != right->scalar.locale_id ||
                  left->scalar.comparison_flags != right->scalar.comparison_flags)))
                return UD_SCHEMA;
        }
    }
    return ud_batch_create(m->ctx, input->schema->fields, input->columns, 0, &m->inputs[port]);
}

ud_status ud_event_match_create(ud_context *ctx, const ud_operator_options *options,
                                const ud_batch *const *inputs, ud_event_match **out) {
    *out = NULL;
    uint32_t ports = options->kind == UD_OPERATOR_EVENT_JOIN ? 2 : 1;
    if ((options->input_count && options->input_count != ports) ||
        (ports == 2 && !inputs[1]) || options->partition_count > 1)
        return UD_INVALID;
    ud_event_match *m = ud_alloc(ctx, sizeof(*m));
    if (!m)
        return UD_RESOURCE;
    m->ctx = ctx;
    m->options = *options;
    m->ports = ports;
    m->mark = INT64_MIN;
    ud_json *raw = NULL, *declaration = NULL, *config = NULL;
    ud_string schema;
    ud_status s = ud_operator_configuration_schema(options->kind, &schema);
    if (!s)
        s = ud_json_parse(ctx, schema.data, schema.length, &declaration, NULL);
    if (!s)
        s = ud_json_parse(ctx, options->configuration.data, options->configuration.length, &raw, NULL);
    if (!s)
        s = ud_json_bind_schema(ctx, declaration, raw, &config);
    uint32_t length = 0;
    if (!s)
        s = ud_json_serialize(config, &m->configuration, &length);
    m->options.configuration = (ud_string){m->configuration, length};
    for (uint32_t p = 0; !s && p < ports; p++)
        s = bind_input(m, config, inputs[p], p);
    uint64_t interval = 0;
    if (!s)
        s = integer(member(config, ports == 1 ? "ttlMicros" : "withinMicros"), &interval);
    if (!s)
        s = integer(member(config, "stateBytes"), &m->budget);
    m->interval = (int64_t)interval;
    if (!s && (interval > INT64_MAX || (options->memory_budget && m->budget > options->memory_budget)))
        s = UD_SCHEMA;
    if (!s)
        s = options->buffers ? ud_buffer_state_domain_create(options->buffers, m->budget, &m->memory)
                             : ud_memory_domain_create(ctx, m->budget, &m->memory);
    if (!s)
        s = ports == 2 ? ud_compute_join_schema(ctx, m->inputs[0], m->inputs[1], &m->schema)
                       : ud_batch_create(ctx, inputs[0]->schema->fields, inputs[0]->columns, 0, &m->schema);
    uint8_t fingerprints[3][32] = {{0}};
    if (!s)
        ud_hash(m->configuration, length, fingerprints[0]);
    for (uint32_t p = 0; !s && p < ports; p++) {
        s = ud_schema_fingerprint(m->inputs[p], m->schema_digest[p]);
        memcpy(fingerprints[p + 1], m->schema_digest[p], 32);
    }
    if (!s)
        ud_hash((const uint8_t *)fingerprints, sizeof(fingerprints), m->fingerprint);
    ud_json_destroy(config);
    ud_json_destroy(declaration);
    ud_json_destroy(raw);
    if (s) {
        ud_event_match_destroy(m);
        return s;
    }
    *out = m;
    return UD_OK;
}

ud_status ud_event_match_schema(const ud_event_match *m, uint32_t port, ud_batch **out) {
    if (port)
        return UD_INVALID;
    return ud_batch_create(m->ctx, m->schema->schema->fields, m->schema->columns, 0, out);
}
static ud_status key_hash(const ud_event_match *m, uint32_t port, const ud_batch *b, uint32_t row,
                          uint64_t *hash, int *unknown) {
    *hash = UD_VALUE_HASH_SEED;
    *unknown = 0;
    for (uint32_t k = 0; k < m->key_count; k++) {
        uint32_t column = m->keys[port][k];
        ud_value value;
        UD_TRY(ud_batch_get(b, row, column, &value));
        *hash = ud_field_value_hash(&b->column[column].field, &value, *hash);
        *unknown |= value.is_null || value.is_absent;
    }
    return UD_OK;
}
static int same_key(const ud_event_match *m, const event_entry *entry, uint32_t other,
                    const ud_batch *b, uint32_t row, uint64_t hash) {
    if (entry->hash != hash)
        return 0;
    for (uint32_t k = 0; k < m->key_count; k++) {
        uint32_t a = m->keys[m->side][k], c = m->keys[other][k];
        ud_value left, right;
        if (ud_batch_get(b, row, a, &left) || ud_batch_get(entry->record, 0, c, &right) ||
            ud_field_value_compare(&b->column[a].field, &left, &entry->record->column[c].field,
                                   &right))
            return 0;
    }
    return 1;
}
static ud_status insert(ud_event_match *m, uint32_t port, const ud_batch *b, uint32_t row,
                        uint64_t hash, int64_t time) {
    ud_memory_scope scope;
    ud_memory_domain_enter(m->memory, &scope);
    event_entry *entry = ud_alloc(m->ctx, sizeof(*entry));
    ud_status s = entry ? ud_batch_create(m->ctx, b->schema->fields, b->columns, 1, &entry->record)
                        : UD_RESOURCE;
    if (!s)
        s = ud_batch_append_from(entry->record, b, row);
    if (!s)
        s = ud_batch_seal(entry->record);
    ud_memory_domain_leave(&scope);
    if (s) {
        if (entry)
            ud_batch_destroy(entry->record);
        ud_free(entry);
        return s;
    }
    event_entry **bucket = &m->table[port][hash % BUCKETS];
    entry->hash = hash;
    entry->time = time;
    entry->next = *bucket;
    *bucket = entry;
    m->counts[port]++;
    return UD_OK;
}
static void remove_entry(event_entry **at) {
    event_entry *entry = *at;
    *at = entry->next;
    ud_batch_destroy(entry->record);
    ud_free(entry);
}
static void evict(ud_event_match *m) {
    for (uint32_t side = 0; side < m->ports; side++) {
        uint32_t unneeded = m->ports == 1 ? m->ended : m->ended & (1u << (1 - side));
        for (uint32_t i = 0; i < BUCKETS; i++) {
            event_entry **at = &m->table[side][i];
            while (*at) {
                int64_t time = (*at)->time;
                if (unneeded || (m->has_mark && time <= INT64_MAX - m->interval &&
                                 time + m->interval <= m->mark)) {
                    remove_entry(at);
                    m->counts[side]--;
                } else
                    at = &(*at)->next;
            }
        }
    }
}

uint64_t ud_event_match_demand(const ud_event_match *m) {
    return m->input ? 0 : ((1u << m->ports) - 1) & ~m->ended;
}
ud_status ud_event_match_push(ud_event_match *m, uint32_t port, const ud_batch *input) {
    UD_TRY(check(m));
    if (port >= m->ports || !input || !input->sealed || (m->ended & (1u << port)))
        return UD_INVALID;
    if (m->input)
        return UD_BUSY;
    uint8_t actual[32];
    UD_TRY(ud_schema_fingerprint(input, actual));
    if (memcmp(m->schema_digest[port], actual, 32))
        return UD_SCHEMA;
    if (!input->rows)
        return UD_OK;
    m->side = port;
    m->row = 0;
    /* Pending input is a borrowed buffer view copied within the state budget. */
    ud_memory_scope scope;
    ud_memory_domain_enter(m->memory, &scope);
    ud_status s = ud_batch_create(m->ctx, input->schema->fields, input->columns, input->rows,
                                 &m->input);
    for (uint32_t i = 0; !s && i < input->rows; i++)
        s = ud_batch_append_from(m->input, input, i);
    if (!s)
        s = ud_batch_seal(m->input);
    ud_memory_domain_leave(&scope);
    if (s) {
        ud_batch_destroy(m->input);
        m->input = NULL;
    }
    return s;
}
ud_status ud_event_match_end(ud_event_match *m, uint32_t port) {
    UD_TRY(check(m));
    if (port >= m->ports || (m->ended & (1u << port)))
        return UD_INVALID;
    if (m->input)
        return UD_BUSY;
    m->ended |= 1u << port;
    evict(m);
    return UD_OK;
}
ud_status ud_event_match_watermark(ud_event_match *m, int64_t mark) {
    UD_TRY(check(m));
    if (mark < m->mark)
        return UD_FENCED;
    if (m->input)
        return UD_BUSY;
    if (!m->has_mark || mark != m->mark) {
        m->has_mark = 1;
        m->mark = mark;
        evict(m);
    }
    return UD_OK;
}
static ud_status append_pair(ud_event_match *m, ud_batch *out, const event_entry *entry,
                             ud_value *values) {
    const ud_batch *left = m->side ? entry->record : m->input;
    const ud_batch *right = m->side ? m->input : entry->record;
    uint32_t lrow = m->side ? 0 : m->row, rrow = m->side ? m->row : 0;
    for (uint32_t i = 0; i < left->columns; i++)
        UD_TRY(ud_batch_get(left, lrow, i, values + i));
    for (uint32_t i = 0; i < right->columns; i++)
        UD_TRY(ud_batch_get(right, rrow, i, values + left->columns + i));
    UD_TRY(ud_batch_append(out, values, out->columns));
    return ud_batch_join_row_info(out, out->rows - 1, left, lrow, right, rrow);
}
static ud_status match_row(ud_event_match *m, ud_batch *out, ud_value *values) {
    uint32_t other = m->ports == 1 ? 0 : 1 - m->side;
    if (!m->probing) {
        ud_value clock;
        UD_TRY(ud_batch_get(m->input, m->row, m->clocks[m->side], &clock));
        if (clock.is_null || clock.is_absent)
            return UD_SCHEMA;
        if (m->has_mark && clock.integer <= m->mark) {
            m->late++;
            return UD_OK;
        }
        int unknown;
        UD_TRY(key_hash(m, m->side, m->input, m->row, &m->probe_hash, &unknown));
        if (m->ports == 2 && unknown)
            return UD_OK;
        m->probe_time = clock.integer;
        m->probing = 1;
        m->probe = m->table[other][m->probe_hash % BUCKETS];
    }
    while (m->probe) {
        UD_TRY(check(m));
        const event_entry *entry = m->probe;
        if (same_key(m, entry, other, m->input, m->row, m->probe_hash)) {
            if (m->ports == 1)
                return UD_OK;
            uint64_t distance = entry->time < m->probe_time
                                    ? (uint64_t)m->probe_time - (uint64_t)entry->time
                                    : (uint64_t)entry->time - (uint64_t)m->probe_time;
            if (distance <= (uint64_t)m->interval)
                UD_TRY(append_pair(m, out, entry, values));
        }
        m->probe = entry->next;
        if (out->rows == m->options.max_batch_rows && m->probe)
            return UD_MORE;
    }
    if (m->ports == 1)
        UD_TRY(ud_batch_append_from(out, m->input, m->row));
    /* Once the opposite side ends, only its already retained rows can match. */
    if (m->ports == 1 || !(m->ended & (1u << other)))
        UD_TRY(insert(m, m->side, m->input, m->row, m->probe_hash, m->probe_time));
    return UD_OK;
}

ud_status ud_event_match_pull(ud_event_match *m, ud_batch **out, uint32_t *port) {
    *out = NULL;
    if (port)
        *port = 0;
    UD_TRY(check(m));
    if (!m->input)
        return m->ended == (1u << m->ports) - 1 ? UD_END : UD_BUSY;
    uint32_t capacity = m->options.max_batch_rows < 32 ? m->options.max_batch_rows : 32;
    ud_status s = ud_batch_create(m->ctx, m->schema->schema->fields, m->schema->columns, capacity, out);
    ud_value *values = NULL;
    if (!s && m->ports == 2) {
        values = ud_alloc(m->ctx, m->schema->columns * sizeof(*values));
        if (!values)
            s = UD_RESOURCE;
    }
    while (!s && m->row < m->input->rows && (*out)->rows < m->options.max_batch_rows) {
        s = check(m);
        if (!s)
            s = match_row(m, *out, values);
        if (s == UD_MORE) {
            s = UD_OK;
            break;
        }
        if (!s) {
            m->row++;
            m->probing = 0;
            m->probe = NULL;
        }
    }
    ud_free(values);
    if (m->row == m->input->rows) {
        ud_batch_destroy(m->input);
        m->input = NULL;
    }
    if (!s && (*out)->rows)
        s = ud_batch_seal(*out);
    if (!s && !(*out)->rows)
        s = UD_BUSY;
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    } else
        m->emitted += (*out)->rows;
    return s;
}

static const ud_field snapshot_fields[] = {{.name = "input", .name_length = 5, .type = UD_INT64},
                                           {.name = "state", .name_length = 5, .type = UD_BINARY}};
ud_status ud_event_match_checkpoint(ud_event_match *m, ud_batch **out) {
    *out = NULL;
    UD_TRY(check(m));
    if (m->input)
        return UD_BUSY;
    UD_TRY(ud_batch_create(m->ctx, snapshot_fields, 2, m->counts[0] + m->counts[1] + 1, out));
    ud_bytes header = {.ctx = m->ctx};
    ud_status s = ud_bytes_put(&header, m->fingerprint, 32);
    if (!s)
        s = ud_put_u64(&header, (uint64_t)m->mark);
    if (!s)
        s = ud_put_u64(&header, m->ended | ((uint64_t)m->has_mark << 2));
    if (!s)
        s = ud_put_u64(&header, m->late);
    if (!s)
        s = ud_put_u64(&header, m->emitted);
    ud_value row[] = {{.type = UD_INT64, .integer = -1},
                      {.type = UD_BINARY, .text = (char *)header.data, .text_length = (uint32_t)header.size}};
    if (!s)
        s = ud_batch_append(*out, row, 2);
    ud_bytes_clear(&header);
    for (uint32_t p = 0; !s && p < m->ports; p++)
        for (uint32_t i = 0; !s && i < BUCKETS; i++)
            for (event_entry *entry = m->table[p][i]; !s && entry; entry = entry->next) {
                uint8_t *wire = NULL;
                uint64_t n = 0;
                s = ud_batch_serialize(entry->record, &wire, &n);
                if (!s && n > UINT32_MAX)
                    s = UD_RESOURCE;
                row[0].integer = p;
                row[1].text = (char *)wire;
                row[1].text_length = (uint32_t)n;
                if (!s)
                    s = ud_batch_append(*out, row, 2);
                ud_free(wire);
            }
    if (!s)
        s = ud_batch_seal(*out);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
static ud_status restore_row(ud_event_match *m, const ud_batch *state, uint32_t pos) {
    ud_value port, payload;
    UD_TRY(ud_batch_get(state, pos, 0, &port));
    UD_TRY(ud_batch_get(state, pos, 1, &payload));
    if (port.is_null || port.is_absent || port.integer < 0 || port.integer >= m->ports ||
        payload.is_null || payload.is_absent)
        return UD_CORRUPT;
    uint32_t side = (uint32_t)port.integer;
    ud_batch *row = NULL;
    ud_status s = ud_batch_deserialize(m->ctx, (const uint8_t *)payload.text, payload.text_length, &row);
    uint8_t actual[32];
    if (!s)
        s = ud_schema_fingerprint(row, actual);
    if (!s && (row->rows != 1 || memcmp(actual, m->schema_digest[side], 32) ||
               (m->ports == 2 && (m->ended & (1u << (1 - side))))))
        s = UD_CORRUPT;
    ud_value clock = {0};
    uint64_t hash = 0;
    int unknown = 0;
    if (!s)
        s = ud_batch_get(row, 0, m->clocks[side], &clock);
    if (!s)
        s = key_hash(m, side, row, 0, &hash, &unknown);
    if (!s && (clock.is_null || clock.is_absent || (m->ports == 2 && unknown) ||
               (m->has_mark && clock.integer <= INT64_MAX - m->interval &&
                clock.integer + m->interval <= m->mark)))
        s = UD_CORRUPT;
    if (!s)
        s = insert(m, side, row, 0, hash, clock.integer);
    ud_batch_destroy(row);
    return s;
}
ud_status ud_event_match_restore(ud_event_match **instance, const ud_batch *state) {
    ud_event_match *old = *instance, *m = NULL;
    if (!state || state->columns != 2 || !state->rows || state->column[0].field.type != UD_INT64 ||
        state->column[1].field.type != UD_BINARY)
        return UD_CORRUPT;
    ud_value id, value;
    UD_TRY(ud_batch_get(state, 0, 0, &id));
    UD_TRY(ud_batch_get(state, 0, 1, &value));
    if (id.is_null || id.is_absent || id.integer != -1 || value.is_null || value.is_absent ||
        value.text_length != 64)
        return UD_CORRUPT;
    if (memcmp(value.text, old->fingerprint, 32))
        return UD_INCOMPATIBLE;
    ud_bytes header = {.ctx = old->ctx, .data = (uint8_t *)value.text, .size = 64, .pos = 32};
    uint64_t mark, flags, late, emitted;
    UD_TRY(ud_get_u64(&header, &mark));
    UD_TRY(ud_get_u64(&header, &flags));
    UD_TRY(ud_get_u64(&header, &late));
    UD_TRY(ud_get_u64(&header, &emitted));
    uint32_t all = (1u << old->ports) - 1;
    if (flags & ~(uint64_t)(4u | all) || (!(flags & 4) && (int64_t)mark != INT64_MIN) ||
        ((flags & all) == all && state->rows != 1))
        return UD_CORRUPT;
    const ud_batch *schemas[] = {old->inputs[0], old->inputs[1]};
    UD_TRY(ud_event_match_create(old->ctx, &old->options, schemas, &m));
    m->mark = (int64_t)mark;
    m->has_mark = (flags & 4) != 0;
    m->ended = (uint32_t)flags & all;
    m->late = late;
    m->emitted = emitted;
    ud_status s = UD_OK;
    /* Reverse insertion reproduces bucket probe order and output fingerprints. */
    for (uint32_t i = state->rows; !s && i > 1;)
        s = restore_row(m, state, --i);
    if (s) {
        ud_event_match_destroy(m);
        return s;
    }
    *instance = m;
    ud_event_match_destroy(old);
    return UD_OK;
}
ud_status ud_event_match_diagnostics(const ud_event_match *m, char **out, uint32_t *length) {
    char json[384];
    int n = snprintf(json, sizeof(json),
                     "{\"leftRows\":%u,\"rightRows\":%u,\"stateBytes\":%llu,"
                     "\"stateHighWater\":%llu,\"stateLimit\":%llu,\"watermark\":%lld,"
                     "\"lateRows\":%llu,\"emittedRows\":%llu}",
                     m->counts[0], m->counts[1], (unsigned long long)ud_memory_domain_used(m->memory),
                     (unsigned long long)ud_memory_domain_peak(m->memory),
                     (unsigned long long)m->budget, (long long)m->mark,
                     (unsigned long long)m->late, (unsigned long long)m->emitted);
    *out = ud_strdup_n(m->ctx, json, (size_t)n);
    *length = (uint32_t)n;
    return *out ? UD_OK : UD_RESOURCE;
}
void ud_event_match_destroy(ud_event_match *m) {
    if (!m)
        return;
    for (uint32_t p = 0; p < m->ports; p++) {
        for (uint32_t i = 0; i < BUCKETS; i++)
            while (m->table[p][i])
                remove_entry(&m->table[p][i]);
        ud_batch_destroy(m->inputs[p]);
    }
    ud_batch_destroy(m->input);
    ud_batch_destroy(m->schema);
    ud_memory_domain_destroy(m->memory);
    ud_free(m->configuration);
    ud_free(m);
}
