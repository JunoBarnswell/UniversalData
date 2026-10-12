#include "ud/quality.h"
#include "ud/regex.h"
#include "../internal.h"
typedef struct seen_key {
    ud_value *values;
    uint64_t hash;
    struct seen_key *next, *bucket_next;
} seen_key;
typedef struct quality_rule {
    ud_quality_rule config;
    ud_batch *reference;
    seen_key *seen;
    seen_key **buckets;
    uint64_t *reference_hashes;
    ud_regex *regex;
} quality_rule;
struct ud_quality {
    ud_context *ctx;
    ud_batch *schema;
    quality_rule *rules; uint32_t count;
    int32_t policy; int failed, finished;
    ud_quarantine_fn quarantine; void *user;
    ud_quality_metrics metrics;
};
static ud_status key_hash(const ud_batch *batch, uint32_t row, const uint32_t *columns,
                          uint32_t count, uint64_t *out) {
    *out = UD_VALUE_HASH_SEED;
    for (uint32_t j = 0; j < count; j++) {
        ud_value v;
        UD_TRY(ud_batch_get(batch, row, columns[j], &v));
        *out = ud_value_hash(&v, *out);
    }
    return UD_OK;
}
static ud_status copy_reference(ud_context *ctx,const ud_batch *reference,ud_batch **out) {
    uint32_t n=ud_batch_columns(reference);
    ud_field *fields=ud_alloc(ctx,n*sizeof(*fields));
    if(!fields) return UD_RESOURCE;
    for(uint32_t j=0;j<n;j++) ud_batch_field(reference,j,fields+j);
    ud_status s=ud_batch_cast(ctx,reference,fields,n,UD_ROUND_EXACT,out);ud_free(fields);return s;
}
void ud_quality_destroy(ud_quality *q) {
    if(!q) return;
    for(uint32_t j=0;j<q->count;j++) {
        quality_rule *r=q->rules+j;
        ud_free((void *)r->config.id);ud_free((void *)r->config.columns);
        ud_free((void *)r->config.reference_columns);ud_free((void *)r->config.pattern);
        ud_clear_value(&r->config.minimum);ud_clear_value(&r->config.maximum);
        ud_batch_destroy(r->reference);
        ud_regex_destroy(r->regex);
        ud_free(r->buckets);
        ud_free(r->reference_hashes);
        seen_key *key=r->seen;
        while(key) {
            seen_key *next=key->next;
            for(uint32_t k=0;k<r->config.column_count;k++) ud_clear_value(key->values+k);
            ud_free(key->values);ud_free(key);key=next;
        }
    }
    ud_free(q->rules);ud_batch_destroy(q->schema);ud_free(q);
}
ud_status ud_quality_create(ud_context *ctx,const ud_field *fields,uint32_t columns,
                             const ud_quality_rule *rules,uint32_t count,int32_t policy,
                             ud_quarantine_fn quarantine,void *user,ud_quality **out) {
    if(!out) return UD_INVALID;
    *out=NULL;
    if(!ctx || !rules || !count || count>65536 || policy<0 || policy>2 ||
        (policy==UD_QUALITY_QUARANTINE && !quarantine)) return UD_INVALID;
    ud_quality *q=ud_alloc(ctx,sizeof(*q));if(!q) return UD_RESOURCE;
    q->ctx=ctx;q->policy=policy;q->quarantine=quarantine;q->user=user;
    ud_status s=ud_batch_create(ctx,fields,columns,0,&q->schema);
    if(!s) { q->rules=ud_alloc(ctx,count*sizeof(*q->rules));if(!q->rules) s=UD_RESOURCE; }
    for(uint32_t j=0;!s && j<count;j++) {
        const ud_quality_rule *input=rules+j;quality_rule *r=q->rules+j;
        if(input->struct_size<sizeof(*input) || input->version!=UD_CONFIG_VERSION) { s=UD_INCOMPATIBLE;break; }
        if(!input->id || !input->id_length || memchr(input->id,0,input->id_length) ||
            !ud_utf8_valid(input->id,input->id_length) || input->kind<UD_RULE_NOT_NULL || input->kind>UD_RULE_ROW_COUNT ||
            (input->kind!=UD_RULE_ROW_COUNT && (!input->columns || !input->column_count || input->column_count>columns)) ||
            ((input->kind==UD_RULE_RANGE || input->kind==UD_RULE_REGEX) && input->column_count!=1)) { s=UD_SCHEMA;break; }
        for(uint32_t k=0;k<j;k++) if(input->id_length==rules[k].id_length && !memcmp(input->id,rules[k].id,input->id_length)) s=UD_SCHEMA;
        if(s) break;
        r->config=*input;r->config.id=NULL;r->config.columns=NULL;r->config.reference_columns=NULL;r->config.pattern=NULL;
        r->config.minimum=(ud_value){0};r->config.maximum=(ud_value){0};r->config.reference=NULL;
        q->count=j+1;
        r->config.id=ud_strdup_n(ctx,input->id,input->id_length);
        if(!r->config.id) { s=UD_RESOURCE;break; }
        if(input->kind!=UD_RULE_ROW_COUNT) {
            uint32_t *indices=ud_alloc(ctx,input->column_count*sizeof(*indices));r->config.columns=indices;
            if(!indices) { s=UD_RESOURCE;break; }
            for(uint32_t k=0;!s && k<input->column_count;k++) {
                indices[k]=input->columns[k];if(indices[k]>=columns) s=UD_SCHEMA;
                for(uint32_t earlier=0;earlier<k;earlier++) if(indices[earlier]==indices[k]) s=UD_SCHEMA;
            }
        } else { r->config.column_count=0; }
        if(s) break;
        if(input->kind==UD_RULE_RANGE) {
            s=ud_copy_value(ctx,&input->minimum,&r->config.minimum,0);
            if(!s) s=ud_copy_value(ctx,&input->maximum,&r->config.maximum,0);
            ud_field field=fields[input->columns[0]];field.nullable=1;
            ud_batch *check=NULL;if(!s) s=ud_batch_create(ctx,&field,1,2,&check);
            if(!s && !input->minimum.is_null) s=ud_batch_append(check,&input->minimum,1);
            if(!s && !input->maximum.is_null) s=ud_batch_append(check,&input->maximum,1);
            if(!s && !input->minimum.is_null && !input->maximum.is_null && ud_value_compare(&input->minimum,&input->maximum)>0) s=UD_SCHEMA;
            ud_batch_destroy(check);
        } else if(input->kind==UD_RULE_FOREIGN_KEY) {
            if(!input->reference || !input->reference_columns) { s=UD_SCHEMA;break; }
            uint32_t *indices=ud_alloc(ctx,input->column_count*sizeof(*indices));r->config.reference_columns=indices;
            if(!indices) { s=UD_RESOURCE;break; }
            for(uint32_t k=0;!s && k<input->column_count;k++) {
                indices[k]=input->reference_columns[k];
                ud_field reference,field=fields[input->columns[k]];
                s=ud_batch_field(input->reference,indices[k],&reference);
                if(!s) reference.nullable=field.nullable;
                if(!s && !ud_field_matches(&field,&reference,0)) s=UD_SCHEMA;
            }
            if(!s) s=copy_reference(ctx,input->reference,&r->reference);
            uint32_t rows = ud_batch_rows(r->reference);
            if (!s && rows) {
                r->reference_hashes = ud_alloc(ctx, (size_t)rows * sizeof(*r->reference_hashes));
                if (!r->reference_hashes)
                    s = UD_RESOURCE;
            }
            for (uint32_t row = 0; !s && row < rows; row++) {
                if (ud_is_cancelled(ctx)) {
                    s = UD_CANCELLED;
                    break;
                }
                s = key_hash(r->reference, row, indices, input->column_count,
                             r->reference_hashes + row);
            }
        } else if(input->kind==UD_RULE_REGEX) {
            if(fields[input->columns[0]].type!=UD_UTF8 || (!input->pattern && input->pattern_length) ||
                !ud_utf8_valid(input->pattern,input->pattern_length)) { s=UD_SCHEMA;break; }
            r->config.pattern=ud_strdup_n(ctx,input->pattern?input->pattern:"",input->pattern_length);
            if(!r->config.pattern) s=UD_RESOURCE;
            if(!s && !input->match) s=ud_regex_compile(ctx,r->config.pattern,r->config.pattern_length,&r->regex);
        }
    }
    if(s) { ud_quality_destroy(q);return s; }
    *out=q;return UD_OK;
}
static ud_status check_rule(ud_quality *q,quality_rule *r,const ud_batch *input,uint32_t row,int *valid) {
    const ud_quality_rule *rule=&r->config;*valid=1;
    if(rule->kind==UD_RULE_ROW_COUNT) return UD_OK;
    int null=0;
    for(uint32_t j=0;j<rule->column_count;j++) {
        ud_value v;UD_TRY(ud_batch_get(input,row,rule->columns[j],&v));null|=v.is_null;
    }
    if(null) {
        if(rule->kind==UD_RULE_NOT_NULL || rule->kind==UD_RULE_PRIMARY_KEY) *valid=0;
        return UD_OK;
    }
    if(rule->kind==UD_RULE_NOT_NULL) return UD_OK;
    if(rule->kind==UD_RULE_RANGE) {
        ud_value value;UD_TRY(ud_batch_get(input,row,rule->columns[0],&value));
        *valid=(rule->minimum.is_null || ud_value_compare(&value,&rule->minimum)>=0) &&
            (rule->maximum.is_null || ud_value_compare(&value,&rule->maximum)<=0);
    } else if(rule->kind==UD_RULE_REGEX) {
        ud_value value;UD_TRY(ud_batch_get(input,row,rule->columns[0],&value));int32_t matched=0;
        UD_TRY(rule->match?rule->match(rule->match_user,rule->pattern,rule->pattern_length,value.text,value.text_length,&matched):
            ud_regex_test(r->regex,value.text,value.text_length,&matched));
        if(matched!=0 && matched!=1) return UD_INVALID;
        *valid=matched;
    } else if(rule->kind==UD_RULE_FOREIGN_KEY) {
        *valid=0;
        uint64_t hash;
        UD_TRY(key_hash(input, row, rule->columns, rule->column_count, &hash));
        for(uint32_t ref=0;ref<ud_batch_rows(r->reference) && !*valid;ref++) {
            if (ud_is_cancelled(q->ctx))
                return UD_CANCELLED;
            if (hash != r->reference_hashes[ref])
                continue;
            int equal=1;
            for(uint32_t j=0;j<rule->column_count && equal;j++) {
                ud_value left,right;UD_TRY(ud_batch_get(input,row,rule->columns[j],&left));
                UD_TRY(ud_batch_get(r->reference,ref,rule->reference_columns[j],&right));
                equal=!right.is_null && !ud_value_compare(&left,&right);
            }
            *valid=equal;
        }
    } else {
        uint64_t hash;
        UD_TRY(key_hash(input, row, rule->columns, rule->column_count, &hash));
        for (seen_key *key = r->buckets ? r->buckets[hash & 63] : NULL; key;
             key = key->bucket_next) {
            if (ud_is_cancelled(q->ctx))
                return UD_CANCELLED;
            if (key->hash != hash)
                continue;
            uint32_t j=0;
            for(;j<rule->column_count;j++) {
                ud_value value;UD_TRY(ud_batch_get(input,row,rule->columns[j],&value));
                if(ud_value_compare(&value,key->values+j)) break;
            }
            if(j==rule->column_count) { *valid=0;break; }
        }
    }
    return UD_OK;
}
static ud_status record_keys(ud_quality *q,const ud_batch *input,uint32_t row) {
    for(uint32_t r=0;r<q->count;r++) {
        quality_rule *rule=q->rules+r;
        if(rule->config.kind!=UD_RULE_UNIQUE && rule->config.kind!=UD_RULE_PRIMARY_KEY) continue;
        int null=0;
        for(uint32_t j=0;j<rule->config.column_count;j++) { ud_value value;UD_TRY(ud_batch_get(input,row,rule->config.columns[j],&value));null|=value.is_null; }
        if(null) continue;
        if (!rule->buckets) {
            rule->buckets = ud_alloc(q->ctx, 64 * sizeof(*rule->buckets));
            if (!rule->buckets)
                return UD_RESOURCE;
        }
        seen_key *key=ud_alloc(q->ctx,sizeof(*key));if(!key) return UD_RESOURCE;
        key->values=ud_alloc(q->ctx,rule->config.column_count*sizeof(*key->values));
        if(!key->values) { ud_free(key);return UD_RESOURCE; }
        key->next=rule->seen;rule->seen=key;
        for(uint32_t j=0;j<rule->config.column_count;j++) {
            ud_value value;UD_TRY(ud_batch_get(input,row,rule->config.columns[j],&value));
            UD_TRY(ud_copy_value(q->ctx,&value,key->values+j,0));
        }
        UD_TRY(key_hash(input, row, rule->config.columns, rule->config.column_count, &key->hash));
        key->bucket_next = rule->buckets[key->hash & 63];
        rule->buckets[key->hash & 63] = key;
    }
    return UD_OK;
}
ud_status ud_quality_apply(ud_quality *q,const ud_batch *input,ud_batch **out) {
    if(!out) return UD_INVALID;
    *out=NULL;
    if(!q || q->failed || q->finished || !input || input->columns!=q->schema->columns) return UD_INVALID;
    for(uint32_t j=0;j<input->columns;j++) if(!ud_field_matches(&input->column[j].field,&q->schema->column[j].field,1)) return UD_SCHEMA;
    ud_field *fields=ud_alloc(q->ctx,input->columns*sizeof(*fields));
    ud_value *values=ud_alloc(q->ctx,input->columns*sizeof(*values));
    ud_quality_violation *violations=ud_alloc(q->ctx,(size_t)input->rows*sizeof(*violations)+1);
    ud_status s=!fields || !values || !violations ? UD_RESOURCE : UD_OK;
    ud_batch *bad=NULL;
    if(!s) {
        for(uint32_t j=0;j<input->columns;j++) fields[j]=q->schema->column[j].field;
        s=ud_batch_create(q->ctx,fields,input->columns,input->rows,out);
        if(!s && q->policy==UD_QUALITY_QUARANTINE) s=ud_batch_create(q->ctx,fields,input->columns,input->rows,&bad);
    }
    uint32_t rejected=0;
    for(uint32_t row=0;!s && row<input->rows;row++) {
        if(ud_is_cancelled(q->ctx)) { s=UD_CANCELLED;break; }
        uint32_t failed=q->count;
        for(uint32_t r=0;!s && r<q->count;r++) {
            int valid;s=check_rule(q,q->rules+r,input,row,&valid);
            if(!s && !valid) { failed=r;break; }
        }
        if(s) break;
        for(uint32_t j=0;j<input->columns;j++) ud_batch_get(input,row,j,values+j);
        if(failed==q->count) {
            s=ud_batch_append(*out,values,input->columns);
            if(!s) s=record_keys(q,input,row);
        } else if(q->policy==UD_QUALITY_ABORT) {
            s=ud_fail(q->ctx,UD_QUALITY,"quality rule %u failed at input row %u",failed,row);
        } else {
            violations[rejected]=(ud_quality_violation){row,rejected,failed};rejected++;
            if(bad) s=ud_batch_append(bad,values,input->columns);
        }
    }
    if(!s && rejected && q->policy==UD_QUALITY_QUARANTINE) s=q->quarantine(q->user,bad,violations,rejected);
    if(!s && (q->metrics.processed>UINT64_MAX-input->rows || q->metrics.accepted>UINT64_MAX-ud_batch_rows(*out) ||
        q->metrics.rejected>UINT64_MAX-rejected || q->metrics.quarantined>UINT64_MAX-rejected)) s=UD_OVERFLOW;
    if(!s) {
        q->metrics.processed+=input->rows;q->metrics.accepted+=ud_batch_rows(*out);q->metrics.rejected+=rejected;
        if(q->policy==UD_QUALITY_QUARANTINE) q->metrics.quarantined+=rejected;
    }
    ud_batch_destroy(bad);ud_free(fields);ud_free(values);ud_free(violations);
    if(s) { q->failed=1;ud_batch_destroy(*out);*out=NULL; }
    return s;
}
ud_status ud_quality_finish(ud_quality *q) {
    if(!q || q->failed) return UD_INVALID;
    if(q->finished) return UD_OK;
    for(uint32_t j=0;j<q->count;j++) if(q->rules[j].config.kind==UD_RULE_ROW_COUNT &&
        q->metrics.accepted!=q->rules[j].config.expected_rows) {
        q->failed=1;return ud_fail(q->ctx,UD_QUALITY,"accepted row count differs from reconciliation rule %u",j);
    }
    q->finished=1;return UD_OK;
}
void ud_quality_get_metrics(const ud_quality *q,ud_quality_metrics *out) {
    if(out) *out=q ? q->metrics : (ud_quality_metrics){0};
}
