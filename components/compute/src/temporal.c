#include "relational_internal.h"
static ud_status sort_input(ud_relational *r, uint32_t p, uint32_t time) {
    uint32_t keys[16];
    if (r->key_count > 14)
        return UD_SCHEMA;
    memcpy(keys, r->keys[p], r->key_count * sizeof(*keys));
    keys[r->key_count] = time;
    ud_operator_options o = r->options;
    o.kind = UD_OPERATOR_SORT;
    o.configuration = (ud_string){0};
    o.keys = keys;
    o.key_count = r->key_count + 1;
    o.partition_count = 1;
    o.input_order = NULL;
    const ud_batch *input[] = {r->inputs[p]};
    UD_TRY(ud_operator_create(r->ctx, &o, input, &r->input_sort[p]));
    r->order[p] = (ud_sort_contract){.count = r->key_count + 1, .stable = 1};
    for (uint32_t i = 0; i < r->order[p].count; i++)
        r->order[p].keys[i] = (ud_sort_key){r->inputs[p]->column[keys[i]].field.identity, 0, 1};
    return UD_OK;
}
ud_status ud_temporal_bind(ud_relational *r) {
    r->ports = 2;
    r->algorithm = UD_PHYSICAL_MERGE;
    UD_TRY(ud_rel_keys(r, 0, ud_rel_member(r->config, "leftKeys")));
    UD_TRY(ud_rel_keys(r, 1, ud_rel_member(r->config, "rightKeys")));
    for (uint32_t k = 0; k < r->key_count; k++)
        if (!ud_field_matches(&r->inputs[0]->column[r->keys[0][k]].field,
                              &r->inputs[1]->column[r->keys[1][k]].field, UD_FIELD_VALUE_TYPE))
            return UD_SCHEMA;
    UD_TRY(ud_rel_selector(r->inputs[0], ud_rel_member(r->config, "leftTime"), &r->left_time));
    UD_TRY(ud_rel_selector(r->inputs[1], ud_rel_member(r->config, "rightTime"), &r->right_time));
    if (r->inputs[0]->column[r->left_time].field.type != UD_TIMESTAMP ||
        r->inputs[1]->column[r->right_time].field.type != UD_TIMESTAMP ||
        r->inputs[1]->column[r->right_time].field.nullable)
        return UD_SCHEMA;
    r->right_end = UINT32_MAX;
    r->tie_break = UINT32_MAX;
    if (r->options.kind == UD_OPERATOR_TEMPORAL_JOIN) {
        UD_TRY(ud_rel_selector(r->inputs[1], ud_rel_member(r->config, "rightEnd"), &r->right_end));
        if (r->inputs[1]->column[r->right_end].field.type != UD_TIMESTAMP)
            return UD_SCHEMA;
    } else if (ud_rel_member(r->config, "tieBreak"))
        UD_TRY(ud_rel_selector(r->inputs[1], ud_rel_member(r->config, "tieBreak"), &r->tie_break));
    UD_TRY(sort_input(r, 0, r->left_time));
    UD_TRY(sort_input(r, 1, r->right_time));
    r->join_type = ud_rel_named(ud_rel_member(r->config, "joinType"), "inner") ? 0 : 1;
    UD_TRY(ud_rel_join_schema(r));
    r->outputs = 2;
    UD_TRY(ud_error_schema(&r->options.errors, r->inputs[0],
                           ud_rel_named(ud_rel_member(r->config, "includeInput"), "true"),
                           &r->schema[1]));
    r->output_order = r->order[0];
    for (uint32_t k = 0; k < r->output_order.count; k++) {
        uint32_t column = k < r->key_count ? r->keys[0][k] : r->left_time;
        r->output_order.keys[k].field = r->schema[0]->column[column].field.identity;
    }
    return UD_OK;
}
static ud_status remember(ud_relational *r, const ud_batch *b, uint32_t row) {
    ud_value start;
    UD_TRY(ud_batch_get(b, row, r->right_time, &start));
    if (start.is_null)
        return UD_QUALITY;
    if (r->right_end != UINT32_MAX) {
        ud_value end;
        UD_TRY(ud_batch_get(b, row, r->right_end, &end));
        if (!end.is_null && end.integer <= start.integer)
            return UD_QUALITY;
    }
    if (r->current_row) {
        int equal = 1;
        for (uint32_t k = 0; k < r->key_count; k++) {
            ud_value old, now;
            UD_TRY(ud_batch_get(r->current_row, 0, r->keys[1][k], &old));
            UD_TRY(ud_batch_get(b, row, r->keys[1][k], &now));
            equal &= !ud_value_compare(&old, &now);
        }
        if (equal) {
            ud_value old_start;
            UD_TRY(ud_batch_get(r->current_row, 0, r->right_time, &old_start));
            if (r->right_end != UINT32_MAX) {
                ud_value old_end;
                UD_TRY(ud_batch_get(r->current_row, 0, r->right_end, &old_end));
                if (old_end.is_null || start.integer < old_end.integer)
                    return UD_QUALITY;
            } else if (old_start.integer == start.integer) {
                if (r->tie_break == UINT32_MAX)
                    return UD_QUALITY;
                ud_value old_tie, new_tie;
                UD_TRY(ud_batch_get(r->current_row, 0, r->tie_break, &old_tie));
                UD_TRY(ud_batch_get(b, row, r->tie_break, &new_tie));
                if (ud_value_compare(&new_tie, &old_tie) >= 0)
                    return UD_OK;
            }
        }
    }
    ud_batch *copy = NULL;
    UD_TRY(ud_rel_clone(r->ctx, r->inputs[1], 1, &copy));
    ud_status s = ud_batch_append_from(copy, b, row);
    if (!s)
        s = ud_batch_seal(copy);
    if (s) {
        ud_batch_destroy(copy);
        return s;
    }
    ud_batch_destroy(r->current_row);
    r->current_row = copy;
    return UD_OK;
}
static ud_status remember_row(ud_relational *r, const ud_batch *right, uint32_t row) {
    ud_status s = remember(r, right, row);
    if (s != UD_QUALITY || r->policy == UD_ERROR_FAIL)
        return s;
    if (r->policy == UD_ERROR_IGNORE) {
        r->ignored++;
        return UD_OK;
    }
    ud_batch *error = NULL;
    UD_TRY(ud_rel_clone(r->ctx, r->schema[1], 1, &error));
    s = ud_error_append(
        &r->options.errors, error,
        &(ud_record_error){.struct_size = sizeof(ud_record_error),
                           .abi_version = UD_ABI_VERSION,
                           .input = right,
                           .row = row,
                           .rule = (ud_string){"temporal.interval", 17},
                           .rule_version = 1,
                           .column = r->inputs[1]->column[r->right_time].field.identity,
                           .code = UD_QUALITY,
                           .policy = r->policy});
    if (!s)
        s = ud_batch_seal(error);
    if (s) {
        ud_batch_destroy(error);
        return s;
    }
    r->pending_error = error;
    r->rejected++;
    return UD_OK;
}
ud_status ud_temporal_pull(ud_relational *r, ud_batch **out, uint32_t *port) {
    *port = 0;
    if (r->pending_error) {
        *out = r->pending_error;
        r->pending_error = NULL;
        *port = 1;
        return UD_OK;
    }
    UD_TRY(ud_rel_clone(r->ctx, r->schema[0], r->options.max_batch_rows, out));
    ud_status status = UD_OK;
    while (!status && (*out)->rows < r->options.max_batch_rows) {
        status = ud_rel_control(r);
        if (status)
            break;
        const ud_batch *left = NULL, *right = NULL;
        uint32_t lr = 0, rr = 0;
        ud_status ls = ud_rel_peek(r, 0, &left, &lr), rs = ud_rel_peek(r, 1, &right, &rr);
        if (ls == UD_BUSY || rs == UD_BUSY) {
            status = UD_BUSY;
            break;
        }
        if ((ls && ls != UD_END) || (rs && rs != UD_END)) {
            status = ls && ls != UD_END ? ls : rs;
            break;
        }
        if (ls == UD_END) {
            if (rs == UD_END) {
                status = UD_END;
                break;
            }
            status = remember_row(r, right, rr);
            if (!status)
                status = ud_rel_take(r, 1);
            if (r->pending_error)
                break;
            continue;
        }
        ud_value time;
        status = ud_batch_get(left, lr, r->left_time, &time);
        if (status)
            break;
        int null = 0,
            order = rs == UD_END ? -1 : ud_rel_compare_rows(r, left, lr, right, rr, &null);
        if (rs == UD_OK && order > 0) {
            status = remember_row(r, right, rr);
            if (!status)
                status = ud_rel_take(r, 1);
            if (r->pending_error)
                break;
            continue;
        }
        if (rs == UD_OK && !order && !null && !time.is_null) {
            ud_value start;
            status = ud_batch_get(right, rr, r->right_time, &start);
            if (!status && start.integer <= time.integer) {
                status = remember_row(r, right, rr);
                if (!status)
                    status = ud_rel_take(r, 1);
                if (r->pending_error)
                    break;
                continue;
            }
            if (status)
                break;
        }
        int match = 0;
        if (r->current_row && !time.is_null) {
            int missing = 0;
            match = !ud_rel_compare_rows(r, left, lr, r->current_row, 0, &missing) && !missing;
            ud_value start;
            status = ud_batch_get(r->current_row, 0, r->right_time, &start);
            match &= start.integer <= time.integer;
            if (match && r->right_end != UINT32_MAX) {
                ud_value end;
                status = ud_batch_get(r->current_row, 0, r->right_end, &end);
                match = end.is_null || time.integer < end.integer;
            }
        }
        if (status)
            break;
        if (match || r->join_type == 1)
            status = ud_rel_join_row(r, *out, left, lr, match ? r->current_row : NULL, 0);
        if (!status)
            status = ud_rel_take(r, 0);
    }
    if (!status && !(*out)->rows && r->pending_error) {
        ud_batch_destroy(*out);
        *out = r->pending_error;
        r->pending_error = NULL;
        *port = 1;
        return UD_OK;
    }
    if ((status == UD_BUSY || status == UD_END) && (*out)->rows)
        status = UD_OK;
    if (status || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return status ? status : UD_BUSY;
    }
    return ud_batch_seal(*out);
}
