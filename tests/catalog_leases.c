#include "check.h"
#include "ud/catalog.h"
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
static ud_string text(const char *s) { return (ud_string){s,(uint32_t)strlen(s)}; }
static ud_status allow(void *u,ud_string t,ud_string r,int32_t a) { (void)t;(void)r;(void)a;return *(int *)u ? UD_OK : UD_AUTH; }
int main(int argc,char **argv) {
    CHECK(argc==3);
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));int permitted=1;
    ud_catalog_options options={sizeof(options),UD_ABI_VERSION,text(argv[1]),text("a"),allow,&permitted,1};
    ud_catalog *first=NULL,*second=NULL;OK(ud_catalog_open(ctx,&options,&first));OK(ud_catalog_open(ctx,&options,&second));
    ud_catalog_lease_request request={sizeof(request),UD_ABI_VERSION,UD_CATALOG_AUDIT,text("run/one"),text("node-a"),text("workers"),300,2};
    uint64_t generation=0,until=0;
    ud_catalog_key key={sizeof(key),UD_ABI_VERSION,UD_CATALOG_AUDIT,text("run/one"),1};
    const char *queued = "{\"version\":12,\"id\":\"run/one\",\"state\":\"Queued\"}";
    if (!strcmp(argv[2],"crash")) {
        OK(ud_catalog_compare_exchange(first,&key,text(queued),0,NULL));
        OK(ud_catalog_lease_acquire(first,&request,&generation,&until));CHECK(generation==1 && until>0);
        ud_catalog_lease lease={sizeof(lease),UD_ABI_VERSION,key.kind,key.id,request.owner,generation};
        key.revision = 2;
        OK(ud_catalog_compare_exchange(
            first, &key, text("{\"version\":12,\"id\":\"run/one\",\"state\":\"Running\"}"), 1,
            &lease));
        fflush(NULL);_exit(86); /* No close/release: authority must expire durably. */
    }
    CHECK(!strcmp(argv[2],"recover"));
    int32_t active=1;OK(ud_catalog_lease_active(first,key.kind,key.id,&active));CHECK(!active);
    request.owner=text("node-b");request.duration_ms=30000;
    OK(ud_catalog_lease_acquire(second,&request,&generation,&until));CHECK(generation==2);
    ud_catalog_lease lease={sizeof(lease),UD_ABI_VERSION,key.kind,key.id,request.owner,generation},old=lease;
    old.owner=text("node-a");old.generation=1;
    CHECK(ud_catalog_lease_renew(first,&old,30000,&until)==UD_FENCED);
    CHECK(ud_catalog_lease_release(first,&old)==UD_FENCED);
    key.revision = 3;
    const char *recovery = "{\"version\":12,\"id\":\"run/one\",\"state\":\"RecoveryRequired\"}";
    CHECK(ud_catalog_compare_exchange(first,&key,text(recovery),2,&old)==UD_FENCED);
    CHECK(ud_catalog_put(first,&key,text(recovery))==UD_FENCED);
    OK(ud_catalog_compare_exchange(second,&key,text(recovery),2,&lease));
    CHECK(ud_catalog_compare_exchange(second,&key,text(recovery),2,&lease)==UD_FENCED);
    uint64_t ignored=0;
    CHECK(ud_catalog_lease_acquire(first,&request,&ignored,&until)==UD_BUSY);
    request.id=text("run/two");uint64_t token2=0;
    OK(ud_catalog_lease_acquire(first,&request,&token2,&until));
    ud_catalog_lease two={sizeof(two),UD_ABI_VERSION,key.kind,request.id,request.owner,token2};
    request.id=text("run/three");CHECK(ud_catalog_lease_acquire(second,&request,&ignored,&until)==UD_BUSY);
    request.capacity=3;CHECK(ud_catalog_lease_acquire(second,&request,&ignored,&until)==UD_FENCED);
    OK(ud_catalog_lease_release(first,&two));request.capacity=2;
    OK(ud_catalog_lease_acquire(second,&request,&ignored,&until));
    ud_catalog_lease three={sizeof(three),UD_ABI_VERSION,key.kind,request.id,request.owner,ignored};
    OK(ud_catalog_lease_release(second,&three));
    OK(ud_catalog_lease_renew(second,&lease,30000,&until));OK(ud_catalog_lease_release(second,&lease));
    char *json=NULL;uint32_t length=0;
    OK(ud_catalog_query(first,text("{\"kind\":7,\"latestOnly\":true}"),&json,&length));
    CHECK(strstr(json,"RecoveryRequired") && !strstr(json,"Queued") && !strstr(json,"Running"));ud_free(json);
    options.tenant=text("b");ud_catalog *other=NULL;OK(ud_catalog_open(ctx,&options,&other));
    request.id=text("run/one");request.owner=text("node-c");request.capacity=1;
    OK(ud_catalog_lease_acquire(other,&request,&ignored,&until));CHECK(ignored==1);
    permitted=0;CHECK(ud_catalog_lease_active(first,key.kind,key.id,&active)==UD_AUTH);
    CHECK(ud_catalog_lease_acquire(first,&request,&ignored,&until)==UD_AUTH);
    ud_catalog_close(other);ud_catalog_close(second);ud_catalog_close(first);
    CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));
    puts("Real Catalog process death, expiry, increasing fences, stale writes/renewal, pool ceilings, latest revisions and tenant isolation passed");return 0;
}
