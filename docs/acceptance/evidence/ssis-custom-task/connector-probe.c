#include "ud/plugin.h"
#include <stdio.h>
#include <string.h>
#define OK(call) do { ud_status s=(call);if(s){fprintf(stderr,"status=%d line=%d\n",s,__LINE__);return 1;} }while(0)
static ud_status grant(void *u,ud_string tenant,ud_string resource,int32_t action){(void)u;(void)resource;(void)action;return tenant.length==7 && !memcmp(tenant.data,"fixture",7)?UD_OK:UD_AUTH;}
static ud_status resolve(void *u,ud_context *ctx,ud_string t,ud_string r,ud_string ref,ud_string *out){(void)t;(void)r;if(ref.length!=6||memcmp(ref.data,"source",6))return UD_AUTH;const char *text=u;size_t n=strlen(text);char *p=ud_alloc(ctx,n+1);if(!p)return UD_RESOURCE;memcpy(p,text,n+1);*out=(ud_string){p,(uint32_t)n};return UD_OK;}
int main(int argc,char **argv){
 if(argc!=3)return 1;ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));ud_plugin *p=NULL;OK(ud_plugin_load(ctx,argv[1],0,&p));
 ud_plugin_api info={.struct_size=sizeof(info),.abi_version=UD_PLUGIN_ABI};OK(ud_plugin_info(p,&info));if(info.abi_version!=3||info.struct_size!=sizeof(info))return 1;
 if(!info.connector){
  ud_plugin_instance *instance=NULL;OK(ud_plugin_create(p,argv[2],(uint32_t)strlen(argv[2]),&instance));
  ud_field field={.name="value",.name_length=5,.type=UD_INT64,.nullable=1};ud_batch *schema=NULL,*batch=NULL;OK(ud_batch_create(ctx,&field,1,0,&schema));
  const char *sql="SELECT value FROM source";OK(ud_plugin_execute(instance,UD_OP_READ,schema,sql,(uint32_t)strlen(sql),&batch));ud_value v;OK(ud_batch_get(batch,0,0,&v));if(v.is_null||v.integer!=1||ud_batch_rows(batch)!=1)return 1;ud_batch_destroy(batch);batch=NULL;
  ud_batch_destroy(schema);OK(ud_plugin_instance_destroy(instance));OK(ud_plugin_unload(p));if(ud_memory_used(ctx))return 1;OK(ud_context_destroy(ctx));puts("Current ABI3 ODBC generic plugin: full table, actual SQLite driver connect/one typed bounded read and clean close passed (no typed Connection Manager table)");return 0;
 }
 ud_connection_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,.tenant={"fixture",7},.resource={"fixture-db",10},.credential_reference={"source",6},.resolve=resolve,.resolve_user=argv[2]};
 ud_connection *connection=NULL;if(ud_connection_open(p,&options,&connection)!=UD_AUTH||connection)return 1;
 options.authorize=grant;OK(ud_connection_open(p,&options,&connection));OK(ud_connection_test(connection));
 ud_field field={.name="value",.name_length=5,.type=UD_INT64,.nullable=1};ud_batch *schema=NULL;OK(ud_batch_create(ctx,&field,1,0,&schema));
 const char *sql="SELECT CAST(1 AS BIGINT) AS value";ud_read_options read={sizeof(read),UD_ABI_VERSION,2,{sql,(uint32_t)strlen(sql)}};
 OK(ud_connection_read_open(connection,&read,schema));ud_batch *batch=NULL;OK(ud_connection_read_next(connection,&batch));ud_value value;OK(ud_batch_get(batch,0,0,&value));if(value.integer!=1||value.is_null||ud_batch_rows(batch)!=1)return 1;ud_batch_destroy(batch);batch=NULL;
 if(ud_connection_read_next(connection,&batch)!=UD_END||batch)return 1;OK(ud_connection_read_close(connection));ud_batch_destroy(schema);
 OK(ud_connection_close(connection));OK(ud_plugin_unload(p));if(ud_memory_used(ctx)!=0)return 1;OK(ud_context_destroy(ctx));puts("Current ABI3 connector: full table, default deny, actual connection/test/bounded SELECT and clean close passed");return 0;
}
