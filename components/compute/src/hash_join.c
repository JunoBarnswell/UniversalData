#include "relational_internal.h"
#include "state.h"
#include "../../execution/state.h"
typedef struct hash_row {
    uint64_t hash;
    uint32_t page, row, next;
    uint8_t matched;
} hash_row;
struct ud_hash_join {
    ud_state *partitions[2][64];
    uint32_t count, partition, bucket_count, node_count, *buckets, candidate;
    hash_row *nodes;
    ud_state_reader left_reader, right_reader;
    ud_memory_domain *right_domain;
    ud_buffer_view **right_pages;
    uint32_t right_pages_retained;
    ud_operator *fallback;
    uint64_t left_position, right_position, left_hash, budget;
    int initialized, have_left, left_matched, left_null, phase;
};
static ud_status key_hash(ud_relational *r, uint32_t p, const ud_batch *b, uint32_t row,
                          uint64_t *out, int *null) {
    *out = UD_VALUE_HASH_SEED;
    *null = 0;
    for (uint32_t k = 0; k < r->key_count; k++) {
        ud_value v;
        UD_TRY(ud_batch_get(b, row, r->keys[p][k], &v));
        *null |= !!v.is_null;
        *out = ud_field_value_hash(&r->inputs[p]->column[r->keys[p][k]].field, &v, *out);
    }
    return UD_OK;
}
ud_status ud_hash_join_create(ud_relational *r) {
    r->hash_join = ud_alloc(r->ctx, sizeof(*r->hash_join));
    if (!r->hash_join)
        return UD_RESOURCE;
    ud_hash_join *h = r->hash_join;
    h->count = r->algorithm == UD_PHYSICAL_HASH ? 1 : 16;
    h->budget = r->options.memory_budget ? r->options.memory_budget : 16u * 1024u * 1024u;
    if (r->options.buffers) {
        ud_buffer_options limits = {0};
        UD_TRY(ud_buffer_manager_limits(r->options.buffers, &limits));
        if (h->budget > limits.working_limit / 2)
            h->budget = limits.working_limit / 2;
        if (limits.working_limit / 4) {
            ud_status status = ud_buffer_state_domain_create(r->options.buffers,
                                                            limits.working_limit / 4,
                                                            &h->right_domain);
            if (status && status != UD_RESOURCE)
                return status;
        }
    }
    return UD_OK;
}
ud_status ud_hash_join_push(ud_relational *r, uint32_t p, const ud_batch *b) {
    ud_hash_join *h = r->hash_join;
    if (p > 1 || r->in[p].ended || !b || b->ctx != r->ctx || !b->sealed || !r->options.buffers)
        return UD_INVALID;
    UD_TRY(ud_rel_control(r));
    if (!b->rows)
        return UD_OK;
    if (h->count == 1) {
        ud_state **state = &h->partitions[p][0];
        if (!*state)
            UD_TRY(ud_state_create(r->ctx, r->options.buffers, r->inputs[p],
                                   r->options.max_batch_rows, state));
        UD_TRY(ud_state_add_batch(*state, b));
    } else {
        const ud_batch *schema = r->inputs[p];
        if (b->columns != schema->columns)
            return UD_INVALID;
        for (uint32_t i = 0; i < b->columns; i++)
            if (!ud_field_matches(&b->column[i].field, &schema->column[i].field, UD_FIELD_PORT))
                return UD_SCHEMA;
        for (uint32_t row = 0; row < b->rows; row++) {
            if (ud_is_cancelled(r->ctx))
                return UD_CANCELLED;
            uint64_t hash;
            int null;
            UD_TRY(key_hash(r, p, b, row, &hash, &null));
            ud_state **state = &h->partitions[p][hash % h->count];
            if (!*state)
                UD_TRY(ud_state_create(r->ctx, r->options.buffers, schema,
                                       r->options.max_batch_rows, state));
            UD_TRY(ud_state_append_bound(*state, b, row));
        }
    }
    r->processed[p] += b->rows;
    return UD_OK;
}
ud_status ud_hash_join_end(ud_relational *r, uint32_t p) {
    ud_hash_join *h = r->hash_join;
    if (p > 1 || r->in[p].ended)
        return UD_INVALID;
    for (uint32_t i = 0; i < h->count; i++) {
        ud_state *state = h->partitions[p][i];
        if (state) {
            UD_TRY(ud_state_seal(state));
            r->hash_write_bytes[p] += state->written_bytes;
        }
    }
    r->in[p].ended = 1;
    return UD_OK;
}
static void right_pages_release(ud_hash_join *h) {
    for (uint32_t page = 0; page < h->right_pages_retained; page++)
        ud_buffer_view_release(h->right_pages[page]);
    ud_free(h->right_pages);
    h->right_pages = NULL;
    h->right_pages_retained = 0;
}
static void index_clear(ud_relational *r) {
    ud_hash_join *h = r->hash_join;
    r->hash_left_read_bytes += h->left_reader.read_bytes;
    ud_state_reader_clear(&h->left_reader);
    ud_state_reader_clear(&h->right_reader);
    right_pages_release(h);
    ud_free(h->nodes);
    h->nodes = NULL;
    h->buckets = NULL;
    ud_operator_destroy(h->fallback);
    h->fallback = NULL;
    h->node_count = h->bucket_count = 0;
    h->left_position = h->right_position = 0;
    h->initialized = h->have_left = h->left_matched = h->phase = 0;
}
static void partition_clear(ud_relational *r) {
    ud_hash_join *h = r->hash_join;
    index_clear(r);
    for (uint32_t p = 0; p < 2; p++) {
        ud_state_destroy(h->partitions[p][h->partition]);
        h->partitions[p][h->partition] = NULL;
    }
}
static ud_status merge_fallback(ud_relational *r) {
    ud_hash_join *h = r->hash_join;
    ud_bytes json = {.ctx = r->ctx};
    ud_status s = ud_bytes_put(&json, "{", 1);
    uint32_t written = 0;
    for (uint32_t i = 0; !s && i < ud_json_size(r->config); i++) {
        const char *key;
        uint32_t n;
        s = ud_json_key(r->config, i, &key, &n);
        if (s)
            break;
        if (n == 8 && !memcmp(key, "strategy", 8))
            continue;
        if (written++)
            s = ud_bytes_put(&json, ",", 1);
        if (!s)
            s = ud_json_quote(&json, key, n);
        if (!s)
            s = ud_bytes_put(&json, ":", 1);
        char *value = NULL;
        uint32_t length = 0;
        if (!s)
            s = ud_json_serialize(ud_json_at(r->config, i), &value, &length);
        if (!s)
            s = ud_bytes_put(&json, value, length);
        ud_free(value);
    }
    const char *tail = ",\"strategy\":\"merge\"}";
    if (!s)
        s = ud_bytes_put(&json, tail, strlen(tail));
    ud_operator_options o = r->options;
    o.configuration = (ud_string){(const char *)json.data, (uint32_t)json.size};
    o.input_order = NULL;
    o.partition_count = 1;
    const ud_batch *in[] = {r->inputs[0], r->inputs[1]};
    if (!s)
        s = ud_operator_create(r->ctx, &o, in, &h->fallback);
    ud_bytes_clear(&json);
    for (uint32_t p = 0; !s && p < 2; p++) {
        ud_state *state = h->partitions[p][h->partition];
        ud_state_reader reader = {0};
        for (uint32_t i = 0; state && !s && i < state->count; i++) {
            const ud_batch *b;
            s = ud_state_read_page(&reader, state, i, &b);
            if (!s)
                s = ud_operator_push(h->fallback, p, b);
        }
        ud_state_reader_clear(&reader);
        if (!s)
            s = ud_operator_finish_input(h->fallback, p);
    }
    if (!s)
        for (uint32_t p = 0; p < 2; p++) {
            ud_state_destroy(h->partitions[p][h->partition]);
            h->partitions[p][h->partition] = NULL;
        }
    if (!s)
        r->hash_fallbacks++;
    return s;
}
static ud_status read_right(ud_hash_join *h, const ud_state *right, uint32_t page,
                           const ud_batch **batch, uint64_t *read_bytes) {
    if (h->right_pages) {
        if (page == h->right_pages_retained) {
            ud_status status = ud_buffer_read_in_domain(right->pages[page].block, h->right_domain,
                                                        h->right_pages + page);
            if (status == UD_RESOURCE)
                right_pages_release(h);
            else if (status)
                return status;
            else {
                *read_bytes += ud_buffer_block_spill_bytes(right->pages[page].block);
                h->right_pages_retained++;
            }
        }
        if (h->right_pages) {
            *batch = ud_buffer_view_batch(h->right_pages[page]);
            return UD_OK;
        }
    }
    uint64_t previous = h->right_reader.read_bytes;
    UD_TRY(ud_state_read_page(&h->right_reader, right, page, batch));
    *read_bytes += h->right_reader.read_bytes - previous;
    return UD_OK;
}
static ud_status index_build(ud_relational *r) {
    ud_hash_join *h = r->hash_join;
    ud_state *right = h->partitions[1][h->partition];
    uint64_t n = right ? right->rows : 0;
    if (n > UINT32_MAX / 2)
        return r->algorithm == UD_PHYSICAL_GRACE_HASH ? merge_fallback(r) : UD_RESOURCE;
    uint64_t bucket_count = 16;
    while (bucket_count < n * 2 && bucket_count < UINT32_MAX / 2)
        bucket_count *= 2;
    uint64_t required = n * sizeof(hash_row) + bucket_count * sizeof(uint32_t);
    void *index = NULL;
    ud_status reservation = required > h->budget || required > SIZE_MAX
                                ? UD_RESOURCE
                                : ud_buffer_working_allocate(r->options.buffers, (size_t)required,
                                                              &index);
    if (reservation)
        return reservation == UD_RESOURCE && r->algorithm == UD_PHYSICAL_GRACE_HASH
                   ? merge_fallback(r)
                   : reservation;
    /* The row array owns the allocation. Its stride also aligns the bucket array. */
    h->nodes = index;
    h->buckets = (uint32_t *)(h->nodes + n);
    h->bucket_count = (uint32_t)bucket_count;
    h->node_count = (uint32_t)n;
    memset(h->buckets, 255, (size_t)bucket_count * sizeof(*h->buckets));
    if (h->right_domain && right && right->count > 1) {
        ud_memory_scope scope;
        ud_memory_domain_enter(h->right_domain, &scope);
        h->right_pages = ud_alloc(r->ctx, (size_t)right->count * sizeof(*h->right_pages));
        ud_memory_domain_leave(&scope);
    }
    uint32_t index_row = 0;
    for (uint32_t page = 0; right && page < right->count; page++) {
        const ud_batch *b;
        UD_TRY(read_right(h, right, page, &b, &r->hash_right_build_bytes));
        for (uint32_t row = 0; row < right->pages[page].rows; row++, index_row++) {
            UD_TRY(ud_rel_control(r));
            uint64_t hash;
            int null;
            UD_TRY(key_hash(r, 1, b, row, &hash, &null));
            uint32_t bucket = (uint32_t)(hash & (h->bucket_count - 1));
            h->nodes[index_row] = (hash_row){hash, page, row, h->buckets[bucket], 0};
            h->buckets[bucket] = index_row;
        }
    }
    return UD_OK;
}
ud_status ud_hash_join_pull(ud_relational *r, ud_batch **out) {
    *out = NULL;
    ud_hash_join *h = r->hash_join;
    if (!r->in[0].ended || !r->in[1].ended)
        return UD_BUSY;
    UD_TRY(ud_rel_clone(r->ctx, r->schema[0], r->options.max_batch_rows, out));
    ud_status s = UD_OK;
    while (!s && (*out)->rows < r->options.max_batch_rows && h->partition < h->count) {
        s = ud_rel_control(r);
        if (s)
            break;
        if (!h->initialized) {
            s = index_build(r);
            if (s)
                break;
            h->initialized = 1;
        }
        if (h->fallback) {
            if ((*out)->rows)
                break;
            ud_batch *next = NULL;
            s = ud_operator_pull(h->fallback, &next, NULL);
            if (s == UD_END) {
                s = UD_OK;
                partition_clear(r);
                h->partition++;
                continue;
            }
            if (s)
                break;
            /* Fallback output is already bounded; return it without another copy. */
            ud_batch_destroy(*out);
            *out = next;
            return UD_OK;
        }
        ud_state *left = h->partitions[0][h->partition], *right = h->partitions[1][h->partition];
        uint64_t left_rows = left ? left->rows : 0;
        if (h->left_position >= left_rows) {
            if (r->join_type == 2 || r->join_type == 3) {
                while (h->right_position < h->node_count &&
                       (*out)->rows < r->options.max_batch_rows) {
                    hash_row *node = h->nodes + h->right_position++;
                    if (node->matched)
                        continue;
                    const ud_batch *b;
                    s = read_right(h, right, node->page, &b, &r->hash_right_tail_bytes);
                    if (!s)
                        s = ud_rel_join_row(r, *out, NULL, 0, b, node->row);
                    if (s)
                        break;
                }
                if (s)
                    break;
                if (h->right_position < h->node_count)
                    continue;
            }
            partition_clear(r);
            h->partition++;
            continue;
        }
        const ud_batch *a;
        uint32_t ar;
        s = ud_state_row(&h->left_reader, left, h->left_position, &a, &ar);
        if (s == UD_RESOURCE && h->right_pages) {
            right_pages_release(h);
            s = ud_state_row(&h->left_reader, left, h->left_position, &a, &ar);
        }
        if (s)
            break;
        if (!h->have_left) {
            s = key_hash(r, 0, a, ar, &h->left_hash, &h->left_null);
            if (s)
                break;
            h->candidate = h->left_null
                               ? UINT32_MAX
                               : h->buckets[(uint32_t)(h->left_hash & (h->bucket_count - 1))];
            h->left_matched = 0;
            h->have_left = 1;
        }
        if (h->candidate != UINT32_MAX) {
            hash_row *node = h->nodes + h->candidate;
            h->candidate = node->next;
            if (node->hash != h->left_hash)
                continue;
            const ud_batch *b;
            s = read_right(h, right, node->page, &b, &r->hash_right_probe_bytes);
            if (s)
                break;
            int null = 0;
            if (ud_rel_compare_rows(r, a, ar, b, node->row, &null) || null)
                continue;
            node->matched = 1;
            h->left_matched = 1;
            if (r->join_type == 4) {
                s = ud_batch_append_from(*out, a, ar);
                h->candidate = UINT32_MAX;
            } else if (r->join_type == 5)
                h->candidate = UINT32_MAX;
            else
                s = ud_rel_join_row(r, *out, a, ar, b, node->row);
            continue;
        }
        if (!h->left_matched) {
            if (r->join_type == 1 || r->join_type == 2)
                s = ud_rel_join_row(r, *out, a, ar, NULL, 0);
            else if (r->join_type == 5)
                s = ud_batch_append_from(*out, a, ar);
        }
        h->have_left = 0;
        h->left_position++;
    }
    if (s || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_END;
    }
    return ud_batch_seal(*out);
}
void ud_hash_join_destroy(ud_relational *r) {
    ud_hash_join *h = r->hash_join;
    if (!h)
        return;
    index_clear(r);
    for (uint32_t p = 0; p < 2; p++)
        for (uint32_t i = 0; i < h->count; i++)
            ud_state_destroy(h->partitions[p][i]);
    if (h->right_domain)
        ud_memory_domain_destroy(h->right_domain);
    ud_free(h);
    r->hash_join = NULL;
}
