#include "ud/regex.h"
#include "check.h"
#include <string.h>
static int check(ud_context *ctx,const char *pattern,const char *text,int expected) {
    ud_regex *r=NULL;OK(ud_regex_compile(ctx,pattern,(uint32_t)strlen(pattern),&r));
    int32_t matched=-1;OK(ud_regex_test(r,text,(uint32_t)strlen(text),&matched));
    CHECK(matched==expected);ud_regex_destroy(r);CHECK(!ud_memory_used(ctx));return 0;
}
int main(void) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));
    struct fixture { const char *pattern,*text;int expected; } fixtures[]={
        {"","",1},{"","abc",1},{"abc","xxabcxx",1},{"^abc$","xxabc",0},{"^abc$","abc",1},
        {"^(ab|cd)+$","abcdab",1},{"^(ab|cd)+$","abc",0},{"^a?b*$","",1},{"^a?b*$","abbb",1},
        {"^\\d+\\s\\w+$","123 中文",0},{"^\\d+\\s\\w+$","123 abc_1",1},{"^\\D+$","中文",1},
        {"^[^\\d]+$","abc",1},{"^[^\\d]+$","abc1",0},{"^[中-文]+$","中文",1},
        {"^..$","中文",1},{"^.$","中文",0},{"^.$","\n",0},{"^[\\n]$","\n",1},
        {"^[-a]+$","-aaa",1},{"^[a-]+$","--aa",1},{"(a|)","",1},
        {"^(a*)*$","aaaa",1},{"^(a|aa)*b$","aaaaaaaaaaaaaaaaaaaaac",0},
        {"^\\[a\\]$","[a]",1},{"^\\W+$","中文",1}
    };
    for(uint32_t i=0;i<sizeof(fixtures)/sizeof(*fixtures);i++) CHECK(!check(ctx,fixtures[i].pattern,fixtures[i].text,fixtures[i].expected));
    ud_regex *r=NULL;const char *bad[]={"(","[a","[z-a]","*a","[]","a)"};
    for(uint32_t i=0;i<sizeof(bad)/sizeof(*bad);i++) CHECK(ud_regex_compile(ctx,bad[i],(uint32_t)strlen(bad[i]),&r)==UD_PARSE && !r && !ud_memory_used(ctx));
    const char *unsupported[]={"a{2}","a*?","(a)\\1","(?=a)","\\bword","[\\D]"};
    for(uint32_t i=0;i<sizeof(unsupported)/sizeof(*unsupported);i++) CHECK(ud_regex_compile(ctx,unsupported[i],(uint32_t)strlen(unsupported[i]),&r)==UD_UNSUPPORTED && !r && !ud_memory_used(ctx));
    OK(ud_regex_compile(ctx,"^a+$",4,&r));int32_t matched=0;
    CHECK(ud_regex_test(r,"\xc0\x80",2,&matched)==UD_PARSE);
    char text[20001];memset(text,'a',20000);text[20000]='b';
    OK(ud_regex_test(r,text,sizeof(text),&matched));CHECK(!matched);
    ud_cancel(ctx);CHECK(ud_regex_test(r,"aaa",3,&matched)==UD_CANCELLED);
    ud_regex_destroy(r);ud_reset_cancel(ctx);CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));
    puts("UTF8 bounded NFA regex fixtures passed");return 0;
}
