#include "advanced_internal.h"
static int text_is(const ud_json*j,ud_string name){const char*p;uint32_t n;return j&&!ud_json_text(j,&p,&n)&&n==name.length&&!memcmp(p,name.data,n);}
static ud_status join_config(ud_advanced*a,const ud_json*entry,ud_string*out){ud_bytes bytes={.ctx=a->ctx};ud_status s=ud_bytes_put(&bytes,"{",1);uint32_t written=0;for(uint32_t i=0;!s&&i<ud_json_size(entry);i++){const char*key;uint32_t n;s=ud_json_key(entry,i,&key,&n);if(s)break;if(n==5&&!memcmp(key,"input",5))continue;if(written++)s=ud_bytes_put(&bytes,",",1);if(!s)s=ud_json_quote(&bytes,key,n);if(!s)s=ud_bytes_put(&bytes,":",1);char*value=NULL;uint32_t length=0;if(!s)s=ud_json_serialize(ud_json_at(entry,i),&value,&length);if(!s)s=ud_bytes_put(&bytes,value,length);ud_free(value);}if(!s)s=ud_bytes_put(&bytes,"}",1);if(!s)s=ud_bytes_put(&bytes,"",1);if(!s){*out=(ud_string){(char*)bytes.data,(uint32_t)bytes.size-1};bytes.data=NULL;}ud_bytes_clear(&bytes);return s;}
ud_status ud_multiway_bind(ud_advanced*a){const ud_json*names=ud_advanced_member(a->config,"inputs"),*joins=ud_advanced_member(a->config,"joins");if(a->ports<2||a->ports>64||ud_json_size(names)!=a->ports||ud_json_size(joins)!=a->ports-1)return UD_SCHEMA;
    int reorder=1;
    for(uint32_t i=0;i<ud_json_size(joins);i++) {
        const ud_json *type=ud_advanced_member(ud_json_at(joins,i),"joinType");
        if(type&&!ud_advanced_named(type,"inner"))reorder=0;
    }
    uint64_t used=1;ud_batch*left=NULL;UD_TRY(ud_advanced_clone(a->ctx,a->inputs[0],0,&left));ud_status status=UD_OK;
    for(uint32_t level=0;!status&&level<a->ports-1;level++){
      const ud_json*selected=NULL;uint32_t input=UINT32_MAX;uint64_t cost=UINT64_MAX;
      for(uint32_t i=0;i<ud_json_size(joins);i++){if(!reorder&&i!=level)continue;const ud_json*entry=ud_json_at(joins,i);ud_string name;const ud_json*named=ud_advanced_member(entry,"input");if(ud_json_text(named,&name.data,&name.length)){status=UD_SCHEMA;break;}
        uint32_t port=UINT32_MAX;for(uint32_t p=1;p<a->ports;p++)if(text_is(ud_json_at(names,p),name))port=p;if(port==UINT32_MAX){status=UD_SCHEMA;break;}if(used&(UINT64_C(1)<<port))continue;
        const ud_json*keys=ud_advanced_member(entry,"leftKeys");int ready=1;for(uint32_t k=0;k<ud_json_size(keys)&&ready;k++){uint32_t col;ready=!ud_compute_field_index(left,ud_json_at(keys,k),&col);}
        if(!ready)continue;uint64_t estimate=a->options.estimated_rows[port]?a->options.estimated_rows[port]:UINT64_MAX-1;
        if(!selected||estimate<cost||(estimate==cost&&port<input)){selected=entry;input=port;cost=estimate;}}
      if(status)break;if(!selected){status=UD_SCHEMA;break;}
      ud_string config={0};status=join_config(a,selected,&config);ud_operator_options o=a->options;o.kind=UD_OPERATOR_JOIN;o.configuration=config;o.input_count=2;o.input_order=NULL;o.partition_count=1;o.estimated_rows[1]=a->options.estimated_rows[input];const ud_batch*in[]={left,a->inputs[input]};
      if(!status)status=ud_operator_create(a->ctx,&o,in,&a->joins[level]);ud_free((void*)config.data);ud_batch*next=NULL;if(!status)status=ud_operator_schema(a->joins[level],0,&next);ud_batch_destroy(left);left=next;
      if(!status){a->join_inputs[level]=input;used|=UINT64_C(1)<<input;a->join_count++;}}
    if(!status)a->schema[0]=left;else ud_batch_destroy(left);return status;
}
ud_status ud_multiway_push(ud_advanced*a,uint32_t port,const ud_batch*b){uint32_t node=0,input=0;if(port){input=1;node=UINT32_MAX;for(uint32_t i=0;i<a->join_count;i++)if(a->join_inputs[i]==port)node=i;if(node==UINT32_MAX)return UD_SCHEMA;}return ud_operator_push(a->joins[node],input,b);}
ud_status ud_multiway_end(ud_advanced*a,uint32_t port){if(!port)return ud_operator_finish_input(a->joins[0],0);for(uint32_t i=0;i<a->join_count;i++)if(a->join_inputs[i]==port)return ud_operator_finish_input(a->joins[i],1);return UD_SCHEMA;}
ud_status ud_multiway_pull(ud_advanced*a,ud_batch**out,uint32_t*port){*out=NULL;if(port)*port=0;
    for(;;){UD_TRY(ud_advanced_check(a));if(a->join_at>=a->join_count)return UD_END;ud_batch*batch=NULL;ud_status s=ud_operator_pull(a->joins[a->join_at],&batch,NULL);
      if(s==UD_BUSY)return UD_BUSY;
      if(s==UD_END){uint32_t finished=a->join_at++;if(a->join_at<a->join_count)UD_TRY(ud_operator_finish_input(a->joins[a->join_at],0));ud_operator_destroy(a->joins[finished]);a->joins[finished]=NULL;continue;}
      if(s)return s;if(a->join_at+1==a->join_count){*out=batch;return UD_OK;}
      s=ud_operator_push(a->joins[a->join_at+1],0,batch);ud_batch_destroy(batch);if(s)return s;}
}
