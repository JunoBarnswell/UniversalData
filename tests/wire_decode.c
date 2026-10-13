#include "check.h"
#include "ud/data.h"
#include "ud/format.h"
int main(int argc,char**argv){CHECK(argc==2);FILE*f=fopen(argv[1],"rb");CHECK(f&&!fseek(f,0,SEEK_END));long size=ftell(f);CHECK(size>0&&size<33554432);rewind(f);void*wire=malloc((size_t)size);CHECK(wire&&fread(wire,1,(size_t)size,f)==(size_t)size);fclose(f);ud_context*c=NULL;OK(ud_context_create(NULL,&c));ud_batch*b=NULL;OK(ud_batch_deserialize(c,wire,(uint64_t)size,&b));free(wire);char*rows=NULL,*info=NULL;uint32_t rn,in;OK(ud_batch_to_json(b,&rows,&rn));OK(ud_batch_row_info_json(b,0,&info,&in));printf("{\"rows\":%.*s,\"rowInfo\":%.*s}\n",(int)rn,rows,(int)in,info);ud_free(rows);ud_free(info);ud_batch_destroy(b);CHECK(!ud_memory_used(c));OK(ud_context_destroy(c));return 0;}
