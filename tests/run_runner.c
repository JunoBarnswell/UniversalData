#include "ud/run.h"
#include "check.h"
#include <stdlib.h>
#include <string.h>
typedef struct permission { int deny,revoke;uint32_t grants; } permission;
static ud_status allow(void *user,ud_string tenant,ud_string resource,int32_t action) {
    permission *p=user;CHECK(tenant.length==7 && !memcmp(tenant.data,"tenant1",7));CHECK(resource.length && resource.data);
    CHECK(action==UD_RUN_ACCESS_READ || action==UD_RUN_ACCESS_WRITE);
    if(p->deny || (p->revoke && p->grants++>8)) return UD_AUTH;return UD_OK;
}
int main(int argc,char **argv) {
    CHECK(argc==5 || argc==6);ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));
    FILE *file=fopen(argv[1],"rb");CHECK(file);char *json=ud_alloc(ctx,1048576);CHECK(json);
    size_t n=fread(json,1,1048576,file);CHECK(n<1048576 && !ferror(file));CHECK(!fclose(file));
    ud_dag *dag=NULL;OK(ud_dag_create(ctx,json,(uint32_t)n,&dag));ud_free(json);
    permission permission_state={!strcmp(argv[2],"deny"),!strcmp(argv[2],"revoke"),0};
    ud_run_options options={0};options.struct_size=sizeof(options);options.abi_version=UD_ABI_VERSION;
    options.context=(ud_context_options){sizeof(ud_context_options),UD_ABI_VERSION,!strcmp(argv[2],"quota")?8192:524288,NULL,NULL,NULL};
    options.edge_capacity=2;options.max_batch_rows=32;options.tenant=(ud_string){"tenant1",7};options.authorize=allow;options.authorize_user=&permission_state;
    if(argc==6) options.parameter_values=(ud_string){argv[5],(uint32_t)strlen(argv[5])};
    ud_run *run=NULL;ud_status result=ud_run_create(dag,&options,&run);ud_dag_destroy(dag);CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));
    if(!result) {
        CHECK(ud_run_wait(run,0)==UD_BUSY);
        if(!strcmp(argv[2],"cancel")) ud_run_cancel(run);
        if(!strcmp(argv[2],"async") || !strcmp(argv[2],"cancel-async")) {
            OK(ud_run_start(run));CHECK(ud_run_start(run)==UD_BUSY);
            if(!strcmp(argv[2],"cancel-async")) ud_run_cancel(run);
            do { result=ud_run_wait(run,100); } while(result==UD_BUSY);
        } else result=ud_run_execute(run);
        CHECK(ud_run_execute(run)==UD_BUSY);ud_run_metrics metrics;ud_run_get_metrics(run,&metrics);
        CHECK(metrics.result==result && metrics.state==(result?UD_RUN_FAILED:UD_RUN_COMPLETE));
        CHECK(metrics.memory_used<=options.context.memory_limit);CHECK(metrics.queue_high_water<=2*6);
        if(!result) {
            CHECK(metrics.input_rows==strtoull(argv[4],NULL,10));CHECK(metrics.output_rows==metrics.input_rows*2);
            for(uint32_t j=0;j<6;j++) { int32_t state=-1;OK(ud_run_node_state(run,j,&state));CHECK(state==UD_NODE_COMPLETE); }
        }
        OK(ud_run_destroy(run));
    }
    CHECK(result==atoi(argv[3]));printf("DAG run result=%d; expectation matched\n",result);return 0;
}
