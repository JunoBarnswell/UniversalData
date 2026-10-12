#include "check.h"
#include "ud/package.h"
#include <string.h>
static ud_status access(void *user,ud_string tenant,ud_string resource,int32_t action) { (void)tenant;(void)resource;(void)action;return *(int *)user?UD_OK:UD_AUTH; }
int main(int argc,char **argv) {
    CHECK(argc==5);FILE *f=fopen(argv[1],"rb");CHECK(f);CHECK(!fseek(f,0,SEEK_END));long n=ftell(f);CHECK(n>0&&!fseek(f,0,SEEK_SET));char *text=malloc((size_t)n);CHECK(text);CHECK(fread(text,1,(size_t)n,f)==(size_t)n);fclose(f);
    int permit=atoi(argv[2]),expected_create=atoi(argv[3]),expected_run=atoi(argv[4]);
    ud_execution_options o={.struct_size=sizeof(o),.abi_version=UD_ABI_VERSION,.edge_capacity=1,.max_batch_rows=7,.max_depth=16,.tenant={"fixture",7},.execution_id={"failure-test",12},.authorize=access,.authorize_user=&permit};
    o.context=(ud_context_options){.struct_size=sizeof(o.context),.abi_version=UD_ABI_VERSION,.memory_limit=1048576};
    ud_execution *e=NULL;ud_status s=ud_execution_create_json((ud_string){text,(uint32_t)n},&o,&e);free(text);CHECK(s==expected_create);
    if(s) { CHECK(!e);return 0; }
    if(permit==2) ud_execution_cancel(e);
    s=ud_execution_execute(e);CHECK(s==expected_run);
    ud_execution_metrics m={.struct_size=sizeof(m),.abi_version=UD_ABI_VERSION};OK(ud_execution_get_metrics(e,&m));
    CHECK(m.result==s&&m.queued_batches==0);CHECK(m.state==(s==UD_CANCELLED?UD_EXEC_CANCELLED:s?UD_EXEC_FAILED:UD_EXEC_SUCCEEDED));
    OK(ud_execution_destroy(e));return 0;
}
