#include "ud/query.h"
#include "ud/format.h"
#include "../components/foundation/internal.h"
#include <string.h>
static ud_string str(const char *s) { return (ud_string){s,(uint32_t)strlen(s)}; }
int main(int argc,char **argv) {
    if(argc!=9) return 2;
    ud_context_options config={sizeof(config),UD_ABI_VERSION,524288,NULL,NULL,NULL};ud_context *ctx=NULL;
    ud_status s=ud_context_create(&config,&ctx);if(s) return s;
    ud_external_options options={sizeof(options),UD_CONFIG_VERSION,16,4096,UINT64_C(536870912),str(argv[4])};
    if(!strcmp(argv[8],"quota")) options.spill_limit=64;
    if(!strcmp(argv[8],"cancel")) ud_cancel(ctx);
    ud_plan *plan=NULL;ud_batch *left=NULL,*right=NULL;ud_bytes schema[2]={{0}};
    if(!strcmp(argv[1],"aggregate")) {
        const char *definition=UD_SCHEMA_HEADER "k int64 nullable\nv decimal(38,2) nullable\n";
        const char *commands=UD_PLAN_HEADER "group k\ncount * cnt\nsum v total\n";
        s=ud_plan_create(ctx,definition,(uint32_t)strlen(definition),commands,(uint32_t)strlen(commands),&plan);
        if(!s) s=ud_query_aggregate_ndjson(ctx,str(argv[2]),str(argv[3]),plan,&options);
    } else {
        s=ud_read_file(ctx,argv[6],schema);if(!s) s=ud_read_file(ctx,argv[7],schema+1);
        if(!s) s=ud_schema_from_json(ctx,(const char *)schema[0].data,(uint32_t)schema[0].size,&left);
        if(!s) s=ud_schema_from_json(ctx,(const char *)schema[1].data,(uint32_t)schema[1].size,&right);
        if(!s) s=ud_query_join_ndjson(ctx,str(argv[2]),left,0,str(argv[5]),right,0,str(argv[3]),&options);
    }
    ud_plan_destroy(plan);ud_batch_destroy(left);ud_batch_destroy(right);ud_bytes_clear(schema);ud_bytes_clear(schema+1);
    if(ud_memory_used(ctx)) { fprintf(stderr,"external operator left live allocations\n");return UD_BUSY; }
    ud_status close=ud_context_destroy(ctx);return s?s:close;
}
