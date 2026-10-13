#include "../package/internal.h"
struct ud_rule_delivery {
    ud_id token;
    uint64_t rows;
    struct ud_rule_delivery *next;
};
struct ud_rule_fact {
    ud_id token;
    uint64_t committed, acknowledged, unknown;
    struct ud_rule_fact *next;
};
static ud_status fact_add(ud_context *, struct ud_rule_fact **, ud_id, uint64_t, uint64_t,
                          uint64_t);
ud_status ud_rule_stage(ud_activation *a, const ud_batch *batch, struct ud_rule_delivery **head) {
    for (uint32_t row = 0; row < batch->rows; row++) {
        ud_row_info info;
        UD_TRY(ud_batch_row_info(batch, row, &info));
        if (!(info.flags & UD_ROW_ERROR) || (!info.rule.high && !info.rule.low))
            continue;
        struct ud_rule_delivery *item = *head;
        while (item && ud_id_compare(item->token, info.rule))
            item = item->next;
        if (!item) {
            item = ud_alloc(a->execution->ctx, sizeof(*item));
            if (!item)
                return UD_RESOURCE;
            item->token = info.rule;
            item->next = *head;
            *head = item;
        }
        ud_activation *owner = a->scope_authority ? a->scope_authority : a;
        ud_mutex_lock(&a->execution->diagnostics_lock);
        ud_status status =
            fact_add(a->execution->ctx, &a->execution->rule_facts, info.rule, 0, 0, 0);
        if (!status)
            status = fact_add(a->execution->ctx, &owner->rule_facts, info.rule, 0, 0, 0);
        ud_mutex_unlock(&a->execution->diagnostics_lock);
        if (status)
            return status;
        if (item->rows == UINT64_MAX)
            return UD_OVERFLOW;
        item->rows++;
    }
    return UD_OK;
}
void ud_rule_delivery_destroy(struct ud_rule_delivery *head) {
    while (head) {
        struct ud_rule_delivery *next = head->next;
        ud_free(head);
        head = next;
    }
}
static ud_status fact_add(ud_context *ctx, struct ud_rule_fact **head, ud_id token,
                          uint64_t committed, uint64_t acknowledged, uint64_t unknown) {
    struct ud_rule_fact *item = *head;
    while (item && ud_id_compare(item->token, token))
        item = item->next;
    if (!item) {
        item = ud_alloc(ctx, sizeof(*item));
        if (!item)
            return UD_RESOURCE;
        item->token = token;
        item->next = *head;
        *head = item;
    }
    if (committed > UINT64_MAX - item->committed ||
        acknowledged > UINT64_MAX - item->acknowledged || unknown > UINT64_MAX - item->unknown)
        return UD_OVERFLOW;
    item->committed += committed;
    item->acknowledged += acknowledged;
    item->unknown += unknown;
    return UD_OK;
}
ud_status ud_rule_delivered(ud_activation *a, const struct ud_rule_delivery *head, int kind) {
    ud_activation *owner = a->scope_authority ? a->scope_authority : a;
    ud_execution *e = a->execution;
    ud_status status = UD_OK;
    ud_mutex_lock(&e->diagnostics_lock);
    for (const struct ud_rule_delivery *item = head; !status && item; item = item->next) {
        uint64_t committed = kind == 0 ? item->rows : 0, ack = kind == 1 ? item->rows : 0,
                 unknown = kind == 2 ? item->rows : 0;
        status = fact_add(e->ctx, &e->rule_facts, item->token, committed, ack, unknown);
        if (!status)
            status = fact_add(e->ctx, &owner->rule_facts, item->token, committed, ack, unknown);
    }
    ud_mutex_unlock(&e->diagnostics_lock);
    return status;
}
void ud_rule_facts_destroy(struct ud_rule_fact **head) {
    while (*head) {
        struct ud_rule_fact *next = (*head)->next;
        ud_free(*head);
        *head = next;
    }
}
ud_status ud_rule_facts_json(const struct ud_rule_fact *head, ud_bytes *out) {
    UD_TRY(ud_bytes_put(out, "[", 1));
    int first = 1;
    for (; head; head = head->next) {
        char id[33], text[256];
        ud_id_format(head->token, id);
        int n = snprintf(
            text, sizeof(text),
            "%s{\"token\":\"%s\",\"committed\":%llu,\"acknowledged\":%llu,\"unknown\":%llu}",
            first ? "" : ",", id, (unsigned long long)head->committed,
            (unsigned long long)head->acknowledged, (unsigned long long)head->unknown);
        first = 0;
        UD_TRY(ud_bytes_put(out, text, (size_t)n));
    }
    return ud_bytes_put(out, "]", 1);
}
ud_status ud_rule_facts_restore(ud_execution *e, const ud_json *array, int subtract) {
    if (ud_json_kind(array) != UD_JSON_ARRAY)
        return UD_CORRUPT;
    for (uint32_t i = 0; i < ud_json_size(array); i++) {
        const ud_json *entry = ud_json_at(array, i);
        ud_string text;
        ud_id id = {0};
        uint64_t committed = 0, ack = 0, unknown = 0;
        UD_TRY(ud_json_string(ud_member(entry, "token"), &text));
        UD_TRY(ud_id_parse(text, &id));
        UD_TRY(ud_json_uint(ud_member(entry, "committed"), &committed));
        UD_TRY(ud_json_uint(ud_member(entry, "acknowledged"), &ack));
        UD_TRY(ud_json_uint(ud_member(entry, "unknown"), &unknown));
        if (!subtract)
            UD_TRY(fact_add(e->ctx, &e->rule_facts, id, committed, ack, unknown));
        else {
            struct ud_rule_fact *fact = e->rule_facts;
            while (fact && ud_id_compare(fact->token, id))
                fact = fact->next;
            if (!fact || fact->committed < committed || fact->acknowledged < ack ||
                fact->unknown < unknown)
                return UD_CORRUPT;
            fact->committed -= committed;
            fact->acknowledged -= ack;
            fact->unknown -= unknown;
        }
    }
    return UD_OK;
}
ud_status ud_rule_token(const ud_component_context *c, ud_string rule, uint32_t version,
                        ud_id *out) {
    return ud_error_rule_token(&c->errors, rule, version, out);
}
ud_status ud_rule_statistics(ud_execution *e, ud_id token, uint64_t *committed, uint64_t *ack,
                             uint64_t *unknown) {
    *committed = *ack = *unknown = 0;
    ud_mutex_lock(&e->diagnostics_lock);
    struct ud_rule_fact *fact = e->rule_facts;
    while (fact && ud_id_compare(fact->token, token))
        fact = fact->next;
    if (fact) {
        *committed = fact->committed;
        *ack = fact->acknowledged;
        *unknown = fact->unknown;
    }
    ud_mutex_unlock(&e->diagnostics_lock);
    return UD_OK;
}
