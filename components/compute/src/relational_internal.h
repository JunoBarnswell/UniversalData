#ifndef UD_RELATIONAL_STATE_H
#define UD_RELATIONAL_STATE_H
#include "relational.h"
#include "ud/connector.h"
#include "../internal.h"
#include "ud/cache.h"
#include <math.h>
typedef struct page {
    ud_buffer_block *block;
    struct page *next;
} page;
typedef struct stream {
    ud_buffer_block *block;
    ud_buffer_view *view;
    const ud_batch *batch;
    uint32_t row;
    int ended, have_last;
    ud_value last[16];
} stream;
typedef struct scan {
    page *at;
    ud_buffer_view *view;
    const ud_batch *batch;
    uint32_t row;
} scan;
typedef struct ud_hash_join ud_hash_join;
struct ud_relational {
    ud_context *ctx;
    ud_operator_options options;
    ud_json *config;
    ud_batch *inputs[2], *schema[6];
    uint32_t ports, outputs;
    stream in[2];
    uint32_t keys[2][16], key_count;
    ud_sort_contract order[2], output_order;
    uint64_t processed[2], emitted[6], rejected, ignored, groups, duplicates, sampled;
    int join_type, phase, group_closed[2], active, duplicate, policy, nulls;
    page *group[2], *tail[2];
    ud_batch *chunk[2];
    scan scans[2];
    ud_value group_key[16];
    uint32_t pivot_key, value_column, cell_count, *unpivot_columns, *retain, retain_count,
        unpivot_at;
    ud_value *pivot_values, *cells;
    uint8_t *seen;
    ud_plan *pivot_plan;
    ud_accumulator **acc;
    uint64_t seed, count, total, position;
    double percentage;
    ud_operator *rank_sort;
    ud_operator *input_sort[2];
    int sort_done[2];
    ud_batch *rank_schema, *pending;
    uint32_t pending_row;
    ud_cache *reference;
    ud_cache_reader *reader;
    uint32_t *mapping, *attributes;
    int32_t *attribute_kind;
    uint32_t attribute_count;
    uint32_t current, valid_from, valid_to, surrogate, version, effective;
    ud_batch *current_row;
    ud_id current_business;
    int scd_ready;
    uint32_t algorithm;
    ud_hash_join *hash_join;
    uint32_t left_time, right_time, right_end, tie_break;
    ud_batch *pending_error;
};
const ud_json *ud_rel_member(const ud_json *j, const char *key);
int ud_rel_named(const ud_json *j, const char *text);
ud_status ud_rel_number(const ud_json *j, uint64_t fallback, uint64_t *out);
ud_status ud_rel_text(const ud_json *j, ud_string *out);
ud_status ud_rel_clone(ud_context *ctx, const ud_batch *b, uint32_t capacity, ud_batch **out);
ud_status ud_rel_control(ud_relational *r);
ud_status ud_rel_selector(const ud_batch *b, const ud_json *j, uint32_t *out);
ud_status ud_rel_selectors(ud_relational *r, const ud_batch *b, const ud_json *j, uint32_t **out,
                           uint32_t *n);
ud_status ud_rel_keys(ud_relational *r, uint32_t port, const ud_json *j);
ud_status ud_rel_require_order(ud_relational *r, uint32_t port, int stable);
ud_status ud_rel_peek(ud_relational *r, uint32_t p, const ud_batch **b, uint32_t *row);
ud_status ud_rel_take(ud_relational *r, uint32_t p);
int ud_rel_compare_rows(ud_relational *r, const ud_batch *a, uint32_t ar, const ud_batch *b,
                        uint32_t br, int *null);
int ud_rel_same_group(ud_relational *r, uint32_t p, const ud_batch *b, uint32_t row);
ud_status ud_rel_set_group(ud_relational *r, const ud_batch *b, uint32_t row);
ud_status ud_rel_scan_next(scan *s);
ud_status ud_rel_flush_group(ud_relational *r, uint32_t p);
ud_status ud_rel_clear_group(ud_relational *r);
ud_status ud_rel_merge_pull(ud_relational *r, ud_batch **out);
ud_status ud_rel_derive(ud_relational *r, const ud_field *spec, const ud_field *deps, uint32_t n,
                        ud_string name, ud_field *out);
ud_status ud_rel_pivot_bind(ud_relational *r);
ud_status ud_rel_reject_row(ud_relational *r, const ud_batch *b, uint32_t row, ud_batch **out,
                            uint32_t *port);
ud_status ud_rel_pivot_pull(ud_relational *r, ud_batch **out, uint32_t *port);
ud_status ud_rel_unpivot_bind(ud_relational *r);
ud_status ud_rel_unpivot_pull(ud_relational *r, ud_batch **out);
ud_status ud_rel_sampling_bind(ud_relational *r);
ud_status ud_rel_sample_push(ud_relational *r, const ud_batch *b);
ud_status ud_rel_sample_pull(ud_relational *r, ud_batch **out, uint32_t *port);
ud_status ud_rel_scd_bind(ud_relational *r);
ud_status ud_rel_scd_pull(ud_relational *r, ud_batch **out, uint32_t *port);
ud_status ud_rel_join_row(ud_relational *, ud_batch *, const ud_batch *, uint32_t, const ud_batch *,
                          uint32_t);
ud_status ud_hash_join_create(ud_relational *);
ud_status ud_hash_join_push(ud_relational *, uint32_t, const ud_batch *);
ud_status ud_hash_join_end(ud_relational *, uint32_t);
ud_status ud_hash_join_pull(ud_relational *, ud_batch **);
void ud_hash_join_destroy(ud_relational *);
ud_status ud_rel_join_schema(ud_relational *);
ud_status ud_temporal_bind(ud_relational *);
ud_status ud_temporal_pull(ud_relational *, ud_batch **, uint32_t *);
ud_status ud_diff_bind(ud_relational *);
ud_status ud_diff_pull(ud_relational *, ud_batch **, uint32_t *);
#endif
