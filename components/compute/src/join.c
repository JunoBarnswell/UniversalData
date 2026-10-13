#include "relational_internal.h"
ud_status ud_rel_join_row(ud_relational *r, ud_batch *out, const ud_batch *a, uint32_t ar,
                          const ud_batch *b, uint32_t br) {
    ud_value *v = ud_alloc(r->ctx, (size_t)out->columns * sizeof(*v));
    if (!v)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < r->inputs[0]->columns; i++)
        if (a)
            s = ud_batch_get(a, ar, i, v + i);
        else
            v[i] = (ud_value){.type = r->inputs[0]->column[i].field.type, .is_null = 1};
    for (uint32_t i = 0; !s && i < r->inputs[1]->columns; i++)
        if (b)
            s = ud_batch_get(b, br, i, v + r->inputs[0]->columns + i);
        else
            v[r->inputs[0]->columns + i] =
                (ud_value){.type = r->inputs[1]->column[i].field.type, .is_null = 1};
    if (!s)
        s = ud_batch_append(out, v, out->columns);
    if (!s) {
        if (a && b)
            s = ud_batch_join_row_info(out, out->rows - 1, a, ar, b, br);
        else
            s = ud_batch_copy_row_info(out, out->rows - 1, a ? a : b, a ? ar : br);
    }
    ud_free(v);
    return s;
}
ud_status ud_rel_merge_pull(ud_relational *r, ud_batch **out) {
    UD_TRY(ud_rel_clone(r->ctx, r->schema[0], r->options.max_batch_rows, out));
    ud_status status = UD_OK;
    while (!status && (*out)->rows < r->options.max_batch_rows) {
        UD_TRY(ud_rel_control(r));
        if (r->phase == 1) {
            for (uint32_t p = 0; p < 2 && !status; p++)
                while (!r->group_closed[p]) {
                    const ud_batch *b = NULL;
                    uint32_t row = 0;
                    ud_status s = ud_rel_peek(r, p, &b, &row);
                    if (s == UD_BUSY) {
                        status = s;
                        break;
                    }
                    if (s == UD_END || (!s && !ud_rel_same_group(r, p, b, row))) {
                        r->group_closed[p] = 1;
                        status = ud_rel_flush_group(r, p);
                        break;
                    }
                    if (s) {
                        status = s;
                        break;
                    }
                    if (p == 1 && r->join_type >= 4) {
                        status = ud_rel_take(r, p);
                        continue;
                    }
                    if (!r->chunk[p])
                        status =
                            ud_rel_clone(r->ctx, r->inputs[p], r->options.max_batch_rows, r->chunk + p);
                    if (!status)
                        status = ud_batch_append_from(r->chunk[p], b, row);
                    if (!status)
                        status = ud_rel_take(r, p);
                    if (!status && r->chunk[p]->rows == r->options.max_batch_rows)
                        status = ud_rel_flush_group(r, p);
                }
            if (status)
                break;
            r->phase = 2;
            r->scans[0].at = r->group[0];
            r->scans[1].at = r->group[1];
            r->groups++;
        }
        if (r->phase == 2 && r->join_type == 5) {
            status = ud_rel_clear_group(r);
            continue;
        }
        if (r->phase == 2 && r->join_type == 4) {
            ud_status next = ud_rel_scan_next(r->scans);
            if (next == UD_END) {
                status = ud_rel_clear_group(r);
                continue;
            }
            if (next) {
                status = next;
                break;
            }
            status = ud_batch_append_from(*out, r->scans[0].batch, r->scans[0].row++);
            continue;
        }
        if (r->phase == 2) {
            ud_status a = ud_rel_scan_next(r->scans), b = ud_rel_scan_next(r->scans + 1);
            if (a == UD_END) {
                status = ud_rel_clear_group(r);
                continue;
            }
            if (a) {
                status = a;
                break;
            }
            if (b == UD_END) {
                r->scans[0].row++;
                ud_buffer_view_release(r->scans[1].view);
                memset(r->scans + 1, 0, sizeof(scan));
                r->scans[1].at = r->group[1];
                continue;
            }
            if (b) {
                status = b;
                break;
            }
            status = ud_rel_join_row(r, *out, r->scans[0].batch, r->scans[0].row, r->scans[1].batch,
                              r->scans[1].row++);
            continue;
        }
        const ud_batch *a = NULL, *b = NULL;
        uint32_t ar = 0, br = 0;
        ud_status sa = ud_rel_peek(r, 0, &a, &ar), sb = ud_rel_peek(r, 1, &b, &br);
        if (sa == UD_BUSY || sb == UD_BUSY) {
            status = UD_BUSY;
            break;
        }
        if ((sa && sa != UD_END) || (sb && sb != UD_END)) {
            status = sa && sa != UD_END ? sa : sb;
            break;
        }
        if (sa == UD_END && sb == UD_END) {
            status = UD_END;
            break;
        }
        int null = 0, c = sa == UD_END   ? 1
                          : sb == UD_END ? -1
                                         : ud_rel_compare_rows(r, a, ar, b, br, &null);
        if (r->options.kind == UD_OPERATOR_MERGE) {
            uint32_t p = c <= 0 ? 0 : 1;
            status = ud_batch_append_from(*out, p ? b : a, p ? br : ar);
            if (!status)
                status = ud_rel_take(r, p);
        } else if (!c && !null) {
            status = ud_rel_set_group(r, a, ar);
            if (!status)
                r->phase = 1;
        } else {
            uint32_t p = c <= 0 ? 0 : 1;
            if (!p && r->join_type == 5)
                status = ud_batch_append_from(*out, a, ar);
            else if ((!p && (r->join_type == 1 || r->join_type == 2)) ||
                     (p && (r->join_type == 2 || r->join_type == 3)))
                status = ud_rel_join_row(r, *out, p ? NULL : a, ar, p ? b : NULL, br);
            if (!status)
                status = ud_rel_take(r, p);
        }
    }
    if (status == UD_BUSY || status == UD_END) {
        if ((*out)->rows)
            status = UD_OK;
    }
    if (status || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return status ? status : UD_BUSY;
    }
    return ud_batch_seal(*out);
}
