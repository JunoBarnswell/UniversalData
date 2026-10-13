#include "advanced_internal.h"
#include "ud/profile.h"
ud_status ud_profile_operator_bind(ud_advanced *a) {
    ud_field fields[] = {{.name = "fieldId", .name_length = 7, .type = UD_UTF8},
                         {.name = "field", .name_length = 5, .type = UD_UTF8},
                         {.name = "profile", .name_length = 7, .type = UD_UTF8}};
    ud_field *owned = ud_alloc(a->ctx, 3 * sizeof(*owned));
    if (!owned)
        return UD_RESOURCE;
    ud_id *lineage = NULL;
    uint32_t count = 0;
    ud_status s = ud_lineage_merge(a->ctx, a->inputs[0]->schema->fields, a->inputs[0]->columns,
                                   &lineage, &count);
    for (uint32_t i = 0; !s && i < 3; i++) {
        fields[i].lineage = lineage;
        fields[i].lineage_count = count;
        s = ud_advanced_derive(a, fields + i, (ud_string){fields[i].name, fields[i].name_length},
                               owned + i);
    }
    if (!s)
        s = ud_batch_create(a->ctx, owned, 3, 0, a->schema);
    for (uint32_t i = 0; i < 3; i++)
        ud_field_clear(owned + i);
    ud_free(owned);
    ud_free(lineage);
    uint64_t maximum = INT64_MAX;
    const ud_json *limit = ud_advanced_member(a->config, "maxRows");
    if (limit) {
        const char *p;
        uint32_t n;
        s = ud_json_text(limit, &p, &n);
        maximum = 0;
        for (uint32_t i = 0; !s && i < n; i++) {
            if (p[i] < '0' || p[i] > '9' || maximum > (INT64_MAX - (uint32_t)(p[i] - '0')) / 10)
                s = UD_SCHEMA;
            else
                maximum = maximum * 10 + (uint32_t)(p[i] - '0');
        }
    }
    ud_profile_statistic stat = {
        .struct_size = sizeof(stat), .abi_version = UD_ABI_VERSION, .kind = UD_PROFILE_ROWS};
    ud_profile *profile = NULL;
    if (!s)
        s = ud_profile_create(a->ctx, a->inputs[0], &stat, 1, maximum, a->options.check,
                              a->options.user, &profile);
    a->index = profile;
    return s;
}
ud_status ud_profile_operator_push(ud_advanced *a, const ud_batch *b) {
    UD_TRY(ud_profile_add(a->index, b));
    a->processed += b->rows;
    return UD_OK;
}
ud_status ud_profile_operator_pull(ud_advanced *a, ud_batch **out, uint32_t *port) {
    *out = NULL;
    if (port)
        *port = 0;
    if (!a->ended)
        return UD_BUSY;
    if (a->position >= a->inputs[0]->columns)
        return UD_END;
    UD_TRY(ud_advanced_clone(a->ctx, a->schema[0], a->options.max_batch_rows, out));
    ud_status s = UD_OK;
    while (!s && a->position < a->inputs[0]->columns && (*out)->rows < a->options.max_batch_rows) {
        const ud_field *f = &a->inputs[0]->column[a->position].field;
        char id[33], *report = NULL;
        uint32_t n;
        ud_id_format(f->identity, id);
        s = ud_profile_report(a->index, (uint32_t)a->position, &report, &n);
        ud_value values[] = {{.type = UD_UTF8, .text = id, .text_length = 32},
                             {.type = UD_UTF8, .text = f->name, .text_length = f->name_length},
                             {.type = UD_UTF8, .text = report, .text_length = n}};
        if (!s)
            s = ud_batch_append(*out, values, 3);
        if (!s) {
            ud_row_info info = {.id = f->identity};
            s = ud_batch_set_row_info(*out, (*out)->rows - 1, &info);
        }
        ud_free(report);
        if (!s)
            a->position++;
    }
    if (!s)
        s = ud_batch_seal(*out);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
void ud_profile_operator_clear(ud_advanced *a) {
    ud_profile_destroy(a->index);
    a->index = NULL;
}
