#include "ud/format.h"
#include "ud/dag.h"
#include "../components/foundation/internal.h"
#include "check.h"
int main(int argc,char **argv) {
    CHECK(argc==7);ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));ud_bytes files[4]={{0}};
    for(int j=0;j<4;j++) { OK(ud_read_file(ctx,argv[j+1],files+j));CHECK(files[j].size<=UINT32_MAX); }
    ud_batch *batch=NULL,*roundtrip=NULL;
    OK(ud_batch_from_json_schema(ctx,(char *)files[0].data,(uint32_t)files[0].size,(char *)files[1].data,(uint32_t)files[1].size,UD_ROUND_EXACT,&batch));
    char *json=NULL;uint32_t length=0;OK(ud_batch_to_json(batch,&json,&length));OK(ud_write_file(ctx,argv[5],json,length));ud_free(json);
    uint8_t *wire=NULL;uint64_t wire_length=0;OK(ud_batch_serialize(batch,&wire,&wire_length));
    OK(ud_write_file(ctx,argv[6],wire,(size_t)wire_length));OK(ud_batch_deserialize(ctx,wire,wire_length,&roundtrip));ud_free(wire);
    CHECK(ud_batch_rows(batch)==ud_batch_rows(roundtrip));ud_batch_destroy(roundtrip);
    ud_dag *dag=NULL,*bound=NULL;OK(ud_dag_create(ctx,(char *)files[2].data,(uint32_t)files[2].size,&dag));
    OK(ud_dag_bind(dag,(ud_string){(char *)files[3].data,(uint32_t)files[3].size},&bound));
    OK(ud_dag_serialize(bound,&json,&length));char *path=ud_alloc(ctx,strlen(argv[5])+5);CHECK(path);strcpy(path,argv[5]);strcat(path,".dag");
    OK(ud_write_file(ctx,path,json,length));ud_free(path);ud_free(json);ud_dag_destroy(bound);ud_dag_destroy(dag);ud_batch_destroy(batch);
    for(int j=0;j<4;j++) ud_bytes_clear(files+j);
    CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));puts("C typed data and DAG contract consumption fixtures passed");return 0;
}
