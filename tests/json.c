#include "ud/json.h"
#include "check.h"
#include <string.h>
int main(void) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));ud_json *doc=NULL;
    const char *input="{\"中文\":[null,true,1.2300e+40,\"\\uD83D\\uDE00\\u0000\"],\"empty\":{}}";
    OK(ud_json_parse(ctx,input,(uint32_t)strlen(input),&doc,NULL));
    CHECK(ud_json_kind(doc)==UD_JSON_OBJECT && ud_json_size(doc)==2);
    const ud_json *a=ud_json_find(doc,"中文",6);CHECK(a && ud_json_size(a)==4);
    CHECK(ud_json_kind(ud_json_at(a,0))==UD_JSON_NULL);
    const char *text=NULL;uint32_t length=0;
    OK(ud_json_text(ud_json_at(a,2),&text,&length));CHECK(length==10 && !memcmp(text,"1.2300e+40",10));
    OK(ud_json_text(ud_json_at(a,3),&text,&length));CHECK(length==5 && !memcmp(text,"\xf0\x9f\x98\x80\0",5));
    char *encoded=NULL;uint32_t size=0;OK(ud_json_serialize(doc,&encoded,&size));
    ud_json *copy=NULL;OK(ud_json_parse(ctx,encoded,size,&copy,NULL));
    CHECK(ud_json_size(copy)==2);ud_json_destroy(copy);ud_free(encoded);ud_json_destroy(doc);
    CHECK(!ud_memory_used(ctx));
    const char *bad[]={"{\"a\":1,\"\\u0061\":2}","[1,]","{\"a\":}","+1","01","1.","1e","NaN",
        "\"\\uD800\"","\"\\uDC00\"","\"\\uD800\\u0000\"","true false","[","\"\xc0\x80\"","\"\n\"","{]"};
    for(uint32_t i=0;i<sizeof(bad)/sizeof(*bad);i++) {
        uint32_t at=UINT32_MAX;doc=NULL;
        CHECK(ud_json_parse(ctx,bad[i],(uint32_t)strlen(bad[i]),&doc,&at)==UD_PARSE);
        CHECK(!doc && at<=(uint32_t)strlen(bad[i]) && !ud_memory_used(ctx));
    }
    char deep[140];memset(deep,'[',65);deep[65]='0';memset(deep+66,']',65);
    CHECK(ud_json_parse(ctx,deep,131,&doc,NULL)==UD_RESOURCE && !doc && !ud_memory_used(ctx));
    ud_cancel(ctx);CHECK(ud_json_parse(ctx,"[]",2,&doc,NULL)==UD_CANCELLED);
    ud_reset_cancel(ctx);OK(ud_context_destroy(ctx));
    puts("strict JSON: exact numbers, unicode, duplicates, depth, ownership fixtures passed");return 0;
}
