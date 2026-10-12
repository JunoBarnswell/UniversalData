#include "ud/quality.h"
#include "check.h"
#include <string.h>
typedef struct { int fail, calls; uint32_t rows; } isolation_fixture;
static ud_status acknowledge(void *user,const ud_batch *bad,const ud_quality_violation *v,uint32_t count) {
    isolation_fixture *fixture=user;fixture->calls++;fixture->rows=ud_batch_rows(bad);
    if(count!=5 || fixture->rows!=5 || v[0].input_row!=1 || v[0].rejected_row!=0 || v[0].rule_index!=0) return UD_CORRUPT;
    return fixture->fail ? UD_IO : UD_OK;
}
int main(void) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));
    ud_field fields[]={{.name="id",.name_length=2,.type=UD_INT64,.nullable=1},
        {.name="score",.name_length=5,.type=UD_DECIMAL128,.nullable=1,.precision=38,.scale=2},
        {.name="name",.name_length=4,.type=UD_UTF8,.nullable=1}};
    ud_batch *input=NULL;OK(ud_batch_create(ctx,fields,3,7,&input));
    int64_t ids[]={1,1,2,3,4,0,5};const char *score[]={"10.00","10.00","101.00","10.00","50.00","10.00","10.00"};
    for(uint32_t r=0;r<7;r++) {
        ud_value values[3]={{.type=UD_INT64,.integer=ids[r]}, {0}, {.type=UD_UTF8,.text="中文",.text_length=6}};
        values[0].is_null=r==5;values[2].is_null=r==3;
        OK(ud_value_parse_field(ctx,fields+1,score[r],(uint32_t)strlen(score[r]),UD_ROUND_EXACT,values+1));
        OK(ud_batch_append(input,values,3));
    }
    ud_batch *reference=NULL;OK(ud_batch_create(ctx,fields,1,2,&reference));
    ud_value ref={.type=UD_INT64,.integer=1};OK(ud_batch_append(reference,&ref,1));ref.integer=4;OK(ud_batch_append(reference,&ref,1));
    uint32_t primary=0,amount=1,name=2;
    ud_quality_rule rules[]={
        {.struct_size=sizeof(ud_quality_rule),.version=UD_CONFIG_VERSION,.id="pk",.id_length=2,.kind=UD_RULE_PRIMARY_KEY,.columns=&primary,.column_count=1},
        {.struct_size=sizeof(ud_quality_rule),.version=UD_CONFIG_VERSION,.id="range",.id_length=5,.kind=UD_RULE_RANGE,.columns=&amount,.column_count=1},
        {.struct_size=sizeof(ud_quality_rule),.version=UD_CONFIG_VERSION,.id="name",.id_length=4,.kind=UD_RULE_NOT_NULL,.columns=&name,.column_count=1},
        {.struct_size=sizeof(ud_quality_rule),.version=UD_CONFIG_VERSION,.id="fk",.id_length=2,.kind=UD_RULE_FOREIGN_KEY,.columns=&primary,.column_count=1,.reference=reference,.reference_columns=&primary},
        {.struct_size=sizeof(ud_quality_rule),.version=UD_CONFIG_VERSION,.id="reconcile",.id_length=9,.kind=UD_RULE_ROW_COUNT,.expected_rows=2}};
    OK(ud_value_parse_field(ctx,fields+1,"0.00",4,UD_ROUND_EXACT,&rules[1].minimum));
    OK(ud_value_parse_field(ctx,fields+1,"100.00",6,UD_ROUND_EXACT,&rules[1].maximum));
    ud_quality *q=NULL;ud_batch *accepted=NULL;ud_quality_metrics metrics;
    isolation_fixture fixture={.fail=1};
    OK(ud_quality_create(ctx,fields,3,rules,5,UD_QUALITY_QUARANTINE,acknowledge,&fixture,&q));
    CHECK(ud_quality_apply(q,input,&accepted)==UD_IO && !accepted);
    ud_quality_get_metrics(q,&metrics);CHECK(!metrics.processed && !metrics.accepted && !metrics.quarantined);
    CHECK(ud_quality_apply(q,input,&accepted)==UD_INVALID);ud_quality_destroy(q);
    fixture.fail=0;
    OK(ud_quality_create(ctx,fields,3,rules,5,UD_QUALITY_QUARANTINE,acknowledge,&fixture,&q));
    ud_batch_destroy(reference);reference=NULL;
    OK(ud_quality_apply(q,input,&accepted));CHECK(ud_batch_rows(accepted)==2);
    ud_quality_get_metrics(q,&metrics);CHECK(metrics.processed==7 && metrics.accepted==2 && metrics.rejected==5 && metrics.quarantined==5);
    CHECK(fixture.calls==2 && fixture.rows==5);OK(ud_quality_finish(q));
    CHECK(ud_quality_apply(q,input,&reference)==UD_INVALID);
    ud_batch_destroy(accepted);ud_quality_destroy(q);
    /* This test exercises acknowledgment/error propagation only. Durable isolation
     * must separately pass the real database / process-termination acceptance gate. */
    ud_quality_rule regex={.struct_size=sizeof(regex),.version=UD_CONFIG_VERSION,.id="regex",.id_length=5,
        .kind=UD_RULE_REGEX,.columns=&name,.column_count=1,.pattern=".*",.pattern_length=2};
    OK(ud_quality_create(ctx,fields,3,&regex,1,UD_QUALITY_ABORT,NULL,NULL,&q));
    OK(ud_quality_apply(q,input,&accepted));CHECK(ud_batch_rows(accepted)==ud_batch_rows(input));
    ud_batch_destroy(accepted);ud_quality_destroy(q);
    ud_batch_destroy(input);CHECK(ud_memory_used(ctx)==0);OK(ud_context_destroy(ctx));return 0;
}
