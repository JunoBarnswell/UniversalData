#include "relational_internal.h"
ud_status ud_rel_sampling_bind(ud_relational *r) {
    UD_TRY(ud_rel_number(ud_rel_member(r->config, "seed"), 0, &r->seed));
    r->outputs = 2;
    UD_TRY(ud_rel_clone(r->ctx, r->inputs[0], 0, &r->schema[0]));
    UD_TRY(ud_rel_clone(r->ctx, r->inputs[0], 0, &r->schema[1]));
    if (r->options.kind == UD_OPERATOR_PERCENTAGE_SAMPLING) {
        const char *p;
        uint32_t n;
        UD_TRY(ud_json_text(ud_rel_member(r->config, "percentage"), &p, &n));
        ud_value v;
        UD_TRY(ud_value_parse(r->ctx, UD_FLOAT64, p, n, &v));
        if (!isfinite(v.real) || v.real < 0 || v.real > 100)
            return UD_SCHEMA;
        r->percentage = v.real;
        return UD_OK;
    }
    UD_TRY(ud_rel_number(ud_rel_member(r->config, "count"), UINT64_MAX, &r->count));
    if (r->count == UINT64_MAX)
        return UD_SCHEMA;
    const ud_json *shorter = ud_rel_member(r->config, "whenShort");
    if (shorter && !ud_rel_named(shorter, "all") && !ud_rel_named(shorter, "fail"))
        return UD_SCHEMA;
    r->nulls = ud_rel_named(shorter, "fail");
    ud_field *f = ud_alloc(r->ctx, (r->inputs[0]->columns + 2) * sizeof(*f));
    if (!f)
        return UD_RESOURCE;
    f[0] = (ud_field){.name = "_samplePriority", .name_length = 15, .type = UD_UTF8};
    f[1] = (ud_field){.name = "_sampleOrdinal", .name_length = 14, .type = UD_INT64};
    ud_status s = ud_id_make(r->ctx, (ud_string){"native.sampling", 15}, (ud_string){"priority", 8},
                             &f[0].identity);
    if (!s)
        s = ud_id_make(r->ctx, (ud_string){"native.sampling", 15}, (ud_string){"ordinal", 7},
                       &f[1].identity);
    for (uint32_t i = 0; i < r->inputs[0]->columns; i++)
        f[i + 2] = r->inputs[0]->column[i].field;
    if (!s)
        s = ud_batch_create(r->ctx, f, r->inputs[0]->columns + 2, 0, &r->rank_schema);
    ud_free(f);
    uint32_t k[] = {0, 1};
    ud_operator_options o = r->options;
    o.kind = UD_OPERATOR_SORT;
    o.configuration = (ud_string){0};
    o.key_count = 2;
    o.keys = k;
    o.partition_count = 1;
    o.configuration = (ud_string){0};
    o.input_order = NULL;
    o.reference = NULL;
    const ud_batch *in[] = {r->rank_schema};
    if (!s)
        s = ud_operator_create(r->ctx, &o, in, &r->rank_sort);
    return s;
}
static ud_status priority(ud_relational *r, const ud_batch *b, uint32_t row, char hex[65],
                          uint64_t *value) {
    ud_row_info info;
    UD_TRY(ud_batch_row_info(b, row, &info));
    if (!info.id.high && !info.id.low)
        return ud_fail(r->ctx, UD_SCHEMA, "sampling requires stable source row identities");
    ud_bytes bytes = {.ctx = r->ctx};
    UD_TRY(ud_put_u64(&bytes, r->seed));
    UD_TRY(ud_put_u64(&bytes, info.id.high));
    UD_TRY(ud_put_u64(&bytes, info.id.low));
    uint8_t h[32];
    ud_hash(bytes.data, bytes.size, h);
    ud_bytes_clear(&bytes);
    static const char digits[] = "0123456789abcdef";
    *value = 0;
    for (uint32_t i = 0; i < 32; i++) {
        hex[i * 2] = digits[h[i] >> 4];
        hex[i * 2 + 1] = digits[h[i] & 15];
        if (i < 8)
            *value = (*value << 8) | h[i];
    }
    hex[64] = 0;
    return UD_OK;
}
ud_status ud_rel_sample_push(ud_relational *r, const ud_batch *b) {
    ud_batch *rank = NULL;
    UD_TRY(ud_rel_clone(r->ctx, r->rank_schema, b->rows, &rank));
    ud_value *v = ud_alloc(r->ctx, rank->columns * sizeof(*v));
    ud_status s = v ? UD_OK : UD_RESOURCE;
    for (uint32_t row = 0; !s && row < b->rows; row++) {
        s = ud_rel_control(r);
        char hex[65];
        uint64_t h;
        if (!s)
            s = priority(r, b, row, hex, &h);
        v[0] = (ud_value){.type = UD_UTF8, .text = hex, .text_length = 64};
        if (r->total > INT64_MAX)
            s = UD_OVERFLOW;
        v[1] = (ud_value){.type = UD_INT64, .integer = (int64_t)r->total++};
        for (uint32_t i = 0; !s && i < b->columns; i++)
            s = ud_batch_get(b, row, i, v + i + 2);
        if (!s)
            s = ud_batch_append(rank, v, rank->columns);
        if (!s)
            s = ud_batch_copy_row_info(rank, row, b, row);
    }
    if (!s)
        s = ud_batch_seal(rank);
    if (!s)
        s = ud_operator_push(r->rank_sort, 0, rank);
    ud_free(v);
    ud_batch_destroy(rank);
    if (!s)
        r->processed[0] += b->rows;
    return s;
}
ud_status ud_rel_sample_pull(ud_relational *r, ud_batch **out, uint32_t *port) {
    if (r->options.kind == UD_OPERATOR_PERCENTAGE_SAMPLING) {
        const ud_batch *b;
        uint32_t row;
        UD_TRY(ud_rel_peek(r, 0, &b, &row));
        char hex[65];
        uint64_t value;
        UD_TRY(priority(r, b, row, hex, &value));
        long double threshold = (long double)r->percentage / 100.0L * 18446744073709551616.0L;
        *port = (long double)value < threshold ? 0 : 1;
        UD_TRY(ud_rel_clone(r->ctx, r->schema[*port], r->options.max_batch_rows, out));
        while ((*out)->rows < r->options.max_batch_rows) {
            ud_status s = ud_rel_peek(r, 0, &b, &row);
            if (s == UD_BUSY || s == UD_END)
                break;
            UD_TRY(s);
            UD_TRY(priority(r, b, row, hex, &value));
            if (((long double)value < threshold ? 0u : 1u) != *port)
                break;
            UD_TRY(ud_batch_append_from(*out, b, row));
            UD_TRY(ud_rel_take(r, 0));
        }
        if (!*port)
            r->sampled += (*out)->rows;
        return ud_batch_seal(*out);
    }
    if (!r->in[0].ended)
        return UD_BUSY;
    if (r->nulls && r->total < r->count)
        return UD_SCHEMA;
    if (!r->pending) {
        UD_TRY(ud_operator_pull(r->rank_sort, &r->pending, NULL));
        r->pending_row = 0;
    }
    *port = r->position < r->count ? 0 : 1;
    UD_TRY(ud_rel_clone(r->ctx, r->schema[*port], r->options.max_batch_rows, out));
    ud_value *v = ud_alloc(r->ctx, (*out)->columns * sizeof(*v));
    if (!v)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    while (!s && r->pending_row < r->pending->rows && (*out)->rows < r->options.max_batch_rows &&
           (r->position < r->count ? 0u : 1u) == *port) {
        for (uint32_t i = 0; !s && i < (*out)->columns; i++)
            s = ud_batch_get(r->pending, r->pending_row, i + 2, v + i);
        if (!s)
            s = ud_batch_append(*out, v, (*out)->columns);
        if (!s)
            s = ud_batch_copy_row_info(*out, (*out)->rows - 1, r->pending, r->pending_row);
        if (!s) {
            r->pending_row++;
            r->position++;
        }
    }
    ud_free(v);
    if (r->pending_row == r->pending->rows) {
        ud_batch_destroy(r->pending);
        r->pending = NULL;
    }
    if (!*port)
        r->sampled += (*out)->rows;
    return s ? s : ud_batch_seal(*out);
}
