#ifndef UD_ADVANCED_STATE_H
#define UD_ADVANCED_STATE_H
#include "advanced.h"
#include "ud/connector.h"
#include "ud/expression.h"
typedef struct advanced_field {
    uint32_t root, path[32], depth, flags, group;
    ud_field owned;
    ud_string map_keys[32];
} advanced_field;
struct ud_advanced {
    ud_context *ctx;
    ud_operator_options options;
    ud_json *config;
    ud_batch *inputs[64], *schema[4], *pending, *held, *tokens, *sorted_schema;
    uint32_t ports, outputs, error_port, row, element, token_row, *keys, key_count, partition_keys;
    uint64_t ended, processed, emitted[4], rejected, position, group_rows, limit;
    ud_operator *sort, *window, *joins[63];
    uint32_t join_inputs[63], join_count, join_at, *join_projection;
    advanced_field *fields;
    uint32_t field_count, column;
    ud_regex *regex;
    ud_regex_span spans[65];
    ud_expression **conditions;
    int32_t *operations;
    uint32_t condition_count;
    ud_string namespace_name, replacement, path;
    ud_value *group_key;
    int active, mode, policy, done;
    ud_cache *reference;
    ud_cache_reader *cache_reader;
    ud_batch *lookup_observations;
    void *index;
    ud_expression_options expression_bindings;
    ud_value *owned_values;
    ud_row_info next_info;
    int override_info;
    char *tree_json;
    uint32_t tree_length;
    ud_batch *deferred;
    uint32_t deferred_port;
    int full_row;
    uint32_t window_column, private_columns;
    uint64_t window_width;
    ud_batch *fingerprint_row;
};
const ud_json *ud_advanced_member(const ud_json *, const char *);
int ud_advanced_named(const ud_json *, const char *);
ud_status ud_advanced_check(ud_advanced *);
ud_status ud_advanced_clone(ud_context *, const ud_batch *, uint32_t, ud_batch **);
ud_status ud_advanced_derive(ud_advanced *, const ud_field *, ud_string, ud_field *);
ud_status ud_advanced_select(ud_advanced *, const ud_json *, uint32_t **, uint32_t *);
ud_status ud_advanced_sorted(ud_advanced *, uint32_t *keys, uint32_t count);
ud_status ud_advanced_next(ud_advanced *);
ud_status ud_cache_lookup_bind(ud_advanced *);
ud_status ud_cache_lookup_row(ud_advanced *, ud_value *, uint32_t *, int *);
ud_status ud_nested_bind(ud_advanced *);
ud_status ud_nested_row(ud_advanced *, ud_value *, uint32_t *, int *);
ud_status ud_text_operator_bind(ud_advanced *);
ud_status ud_text_operator_row(ud_advanced *, ud_value *, uint32_t *, int *);
ud_status ud_ordered_bind(ud_advanced *);
ud_status ud_ordered_push(ud_advanced *, const ud_batch *);
ud_status ud_ordered_pull(ud_advanced *, ud_batch **, uint32_t *);
ud_status ud_matching_bind(ud_advanced *);
ud_status ud_matching_push(ud_advanced *, const ud_batch *);
ud_status ud_matching_row(ud_advanced *, ud_value *, uint32_t *, int *);
ud_status ud_matching_pull(ud_advanced *, ud_batch **, uint32_t *);
void ud_matching_clear(ud_advanced *);
ud_status ud_profile_operator_bind(ud_advanced *);
ud_status ud_profile_operator_push(ud_advanced *, const ud_batch *);
ud_status ud_profile_operator_pull(ud_advanced *, ud_batch **, uint32_t *);
void ud_profile_operator_clear(ud_advanced *);
ud_status ud_multiway_bind(ud_advanced *);
ud_status ud_multiway_push(ud_advanced *, uint32_t, const ud_batch *);
ud_status ud_multiway_end(ud_advanced *, uint32_t);
ud_status ud_multiway_pull(ud_advanced *, ud_batch **, uint32_t *);
#endif
