#include "window.h"
#include <math.h>
struct ud_window {
    ud_context *ctx;
    ud_operator_options options;
    ud_operator *sort;
    ud_batch *input,*schema,*pending;
    uint32_t pending_row,*keys;
    ud_state *partition;
    ud_state_reader row_reader,value_reader;
    ud_value *partition_keys;
    uint64_t position,rank,dense_rank,frame_begin,frame_end;
    ud_sum_state sum;
    int ended,finished,frame_active;
};
static ud_status check(ud_window*w){if(ud_is_cancelled(w->ctx))return UD_CANCELLED;return w->options.check?w->options.check(w->options.user):UD_OK;}
static ud_status clone(ud_context*c,const ud_batch*b,uint32_t n,ud_batch**out){return ud_batch_create(c,b->schema->fields,b->columns,n,out);}
ud_status ud_window_create(ud_context*ctx,const ud_operator_options*o,const ud_batch*input,ud_window**out){
    *out=NULL;if(o->partition_key_count>o->key_count||o->key_count>16||!o->window_name.length||o->window_function<UD_WINDOW_ROW_NUMBER||o->window_function>UD_WINDOW_MOVING_AVERAGE)return UD_SCHEMA;
    int value_function=o->window_function==UD_WINDOW_RUNNING_SUM||o->window_function>=UD_WINDOW_LAG;
    if(value_function&&o->window_column>=input->columns)return UD_SCHEMA;
    if(o->frame_kind==UD_FRAME_RANGE && (o->key_count!=o->partition_key_count+1||o->descending))return UD_SCHEMA;
    ud_window*w=ud_alloc(ctx,sizeof(*w));if(!w)return UD_RESOURCE;w->ctx=ctx;w->options=*o;
    w->keys=ud_alloc(ctx,(o->key_count+1)*sizeof(*w->keys));w->partition_keys=ud_alloc(ctx,(o->partition_key_count+1)*sizeof(*w->partition_keys));
    ud_status s=w->keys&&w->partition_keys?clone(ctx,input,0,&w->input):UD_RESOURCE;
    for(uint32_t i=0;!s&&i<o->key_count;i++){if(o->keys[i]>=input->columns)s=UD_SCHEMA;else w->keys[i]=o->keys[i];}
    w->options.keys=w->keys;
    ud_field spec={.type=UD_INT64};
    if(!s&&value_function){spec=input->column[o->window_column].field;spec.nullable=1;}
    int numeric=o->window_function==UD_WINDOW_RUNNING_SUM||o->window_function==UD_WINDOW_MOVING_SUM||o->window_function==UD_WINDOW_MOVING_AVERAGE;
    if(!s&&numeric&&spec.type!=UD_INT64&&spec.type!=UD_FLOAT64&&spec.type!=UD_DECIMAL128)s=UD_SCHEMA;
    if(!s&&o->window_function==UD_WINDOW_MOVING_AVERAGE&&spec.type!=UD_FLOAT64){spec.type=UD_DECIMAL128;spec.precision=38;spec.scale=input->column[o->window_column].field.type==UD_INT64?6:input->column[o->window_column].field.scale+6;if(spec.scale>38)spec.scale=38;}
    if(!s&&o->frame_kind==UD_FRAME_RANGE){ud_type t=input->column[w->keys[o->partition_key_count]].field.type;if(t!=UD_INT64&&t!=UD_DATE32&&t!=UD_TIME64&&t!=UD_TIMESTAMP&&t!=UD_FLOAT64&&t!=UD_DECIMAL128)s=UD_SCHEMA;}
    ud_field*fields=!s?ud_alloc(ctx,(input->columns+1)*sizeof(*fields)):NULL;
    ud_id*lineage=NULL;uint32_t lineage_count=0;ud_field derived={0};ud_bytes signature={.ctx=ctx};
    if(!s&&!fields)s=UD_RESOURCE;
    for(uint32_t i=0;!s&&i<input->columns;i++){fields[i]=input->column[i].field;s=ud_put_u64(&signature,fields[i].identity.high);if(!s)s=ud_put_u64(&signature,fields[i].identity.low);}
    const uint64_t semantic[]={o->window_function,o->window_column,o->partition_key_count,o->window_offset,o->frame_kind,o->frame_preceding,o->frame_following,o->average_rounding};
    for(uint32_t i=0;!s&&i<sizeof(semantic)/sizeof(*semantic);i++)s=ud_put_u64(&signature,semantic[i]);
    if(!s)s=ud_lineage_merge(ctx,input->schema->fields,input->columns,&lineage,&lineage_count);
    if(!s){uint8_t hash[32];char scope[65];ud_hash(signature.data,signature.size,hash);for(uint32_t i=0;i<32;i++)snprintf(scope+2*i,3,"%02x",hash[i]);spec.lineage=lineage;spec.lineage_count=lineage_count;spec.key_order=0;s=ud_field_derive(ctx,&spec,(ud_string){scope,64},o->window_name,&derived);}
    if(!s){fields[input->columns]=derived;s=ud_batch_create(ctx,fields,input->columns+1,0,&w->schema);}
    ud_field_clear(&derived);ud_free(fields);ud_free(lineage);ud_bytes_clear(&signature);
    ud_operator_options sort=*o;sort.kind=UD_OPERATOR_SORT;sort.configuration=(ud_string){0};sort.keys=w->keys;sort.aggregation=NULL;sort.partition_count=1;
    const ud_batch*in[]={input};if(!s)s=ud_operator_create(ctx,&sort,in,&w->sort);
    if(s){ud_window_destroy(w);return s;}*out=w;return UD_OK;
}
static void partition_clear(ud_window*w){ud_state_reader_clear(&w->row_reader);ud_state_reader_clear(&w->value_reader);ud_state_destroy(w->partition);w->partition=NULL;if(w->partition_keys)for(uint32_t i=0;i<w->options.partition_key_count;i++)ud_clear_value(w->partition_keys+i);w->position=w->rank=w->dense_rank=0;w->frame_active=0;memset(&w->sum,0,sizeof(w->sum));}
static ud_status partition_gather(ud_window*w){
    partition_clear(w);UD_TRY(ud_state_create(w->ctx,w->options.buffers,w->input,w->options.max_batch_rows,&w->partition));
    for(;;){UD_TRY(check(w));if(!w->pending||w->pending_row==w->pending->rows){ud_batch_destroy(w->pending);w->pending=NULL;w->pending_row=0;ud_status s=ud_operator_pull(w->sort,&w->pending,NULL);if(s==UD_END){w->finished=1;break;}if(s)return s;}
      int same=1;for(uint32_t i=0;i<w->options.partition_key_count;i++){ud_value v;UD_TRY(ud_batch_get(w->pending,w->pending_row,w->keys[i],&v));if(!w->partition->rows)UD_TRY(ud_copy_value(w->ctx,&v,w->partition_keys+i,0));else same&=!ud_value_compare(&v,w->partition_keys+i);}
      if(!same)break;UD_TRY(ud_state_append(w->partition,w->pending,w->pending_row++));}
    UD_TRY(ud_state_seal(w->partition));return w->partition->rows?UD_OK:UD_END;
}
static ud_status value_at(ud_window*w,uint64_t at,uint32_t column,ud_value*out){const ud_batch*b;uint32_t row;UD_TRY(ud_state_row(&w->value_reader,w->partition,at,&b,&row));return ud_batch_get(b,row,column,out);}
static ud_status frame(ud_window*w,uint64_t*begin,uint64_t*end){
    uint64_t rows=w->partition->rows,pos=w->position;int f=w->options.window_function;
    if(!w->options.frame_kind){*begin=0;*end=f==UD_WINDOW_RUNNING_SUM?pos+1:rows;return UD_OK;}
    if(w->options.frame_kind==UD_FRAME_ROWS){*begin=pos>w->options.frame_preceding?pos-w->options.frame_preceding:0;*end=w->options.frame_following>=rows-pos-1?rows:pos+w->options.frame_following+1;return UD_OK;}
    uint32_t key=w->keys[w->options.partition_key_count];ud_value v;UD_TRY(value_at(w,pos,key,&v));
    ud_value minimum=v,maximum=v;int lower_unbounded=0,upper_unbounded=0;
    if(!v.is_null){
        if(v.type==UD_FLOAT64){minimum.real=v.real-(double)w->options.frame_preceding;maximum.real=v.real+(double)w->options.frame_following;}
        else if(v.type==UD_DECIMAL128){const ud_field*field=&w->input->column[key].field;uint64_t bounds[]={w->options.frame_preceding,w->options.frame_following};ud_value*outputs[]={&minimum,&maximum};
            for(uint32_t i=0;i<2;i++){char text[32];int n=snprintf(text,sizeof(text),"%llu",(unsigned long long)bounds[i]);ud_value delta;ud_status status=ud_value_parse_field(w->ctx,field,text,(uint32_t)n,UD_ROUND_EXACT,&delta);if(!status)status=ud_value_numeric(i?UD_NUMERIC_ADD:UD_NUMERIC_SUBTRACT,&v,&delta,field,outputs[i]);if(status==UD_OVERFLOW){if(i)upper_unbounded=1;else lower_unbounded=1;}else if(status)return status;}
        }else{uint64_t before=w->options.frame_preceding,after=w->options.frame_following;minimum.integer=INT64_MIN;maximum.integer=INT64_MAX;if(before<=INT64_MAX&&v.integer>=INT64_MIN+(int64_t)before)minimum.integer=v.integer-(int64_t)before;if(after<=INT64_MAX&&v.integer<=INT64_MAX-(int64_t)after)maximum.integer=v.integer+(int64_t)after;}
    }
    uint64_t low=0,high=rows;
    while(low<high){uint64_t mid=low+(high-low)/2;ud_value x;UD_TRY(value_at(w,mid,key,&x));int less=!v.is_null&&(x.is_null||(!lower_unbounded&&ud_value_compare(&x,&minimum)<0));if(less)low=mid+1;else high=mid;}*begin=low;
    low=*begin;high=rows;while(low<high){uint64_t mid=low+(high-low)/2;ud_value x;UD_TRY(value_at(w,mid,key,&x));int within=v.is_null?x.is_null:!x.is_null&&(upper_unbounded||ud_value_compare(&x,&maximum)<=0);if(within)low=mid+1;else high=mid;}*end=low;return UD_OK;
}
static ud_status frame_sum(ud_window*w,uint64_t begin,uint64_t end,ud_value*out){
    int floating=w->input->column[w->options.window_column].field.type==UD_FLOAT64;
    if(!w->frame_active||begin<w->frame_begin||end<w->frame_end||(floating&&!isfinite(w->sum.real))){memset(&w->sum,0,sizeof(w->sum));w->frame_begin=begin;w->frame_end=begin;w->frame_active=1;}
    while(w->frame_begin<begin&&w->frame_begin<w->frame_end){ud_value v;UD_TRY(value_at(w,w->frame_begin++,w->options.window_column,&v));UD_TRY(ud_sum_update(&w->sum,&v,-1));}
    if(w->frame_end<begin)w->frame_begin=w->frame_end=begin;
    while(w->frame_end<end){UD_TRY(check(w));ud_value v;UD_TRY(value_at(w,w->frame_end++,w->options.window_column,&v));UD_TRY(ud_sum_update(&w->sum,&v,1));}
    if(floating&&!isfinite(w->sum.real)){memset(&w->sum,0,sizeof(w->sum));for(uint64_t at=begin;at<end;at++){ud_value v;UD_TRY(value_at(w,at,w->options.window_column,&v));UD_TRY(ud_sum_update(&w->sum,&v,1));}}
    return ud_sum_result(&w->sum,&w->schema->column[w->input->columns].field,w->options.window_function==UD_WINDOW_MOVING_AVERAGE,w->options.average_rounding,out);
}
ud_status ud_window_pull(ud_window*w,ud_batch**out){
    *out=NULL;if(!w->ended)return UD_BUSY;UD_TRY(check(w));UD_TRY(clone(w->ctx,w->schema,w->options.max_batch_rows,out));
    ud_value*values=ud_alloc(w->ctx,w->schema->columns*sizeof(*values));ud_status s=values?UD_OK:UD_RESOURCE;
    while(!s&&(*out)->rows<w->options.max_batch_rows){s=check(w);if(s)break;
      if(!w->partition||w->position==w->partition->rows){if(w->finished)break;s=partition_gather(w);if(s==UD_END){s=UD_OK;break;}if(s)break;}
      const ud_batch*b=NULL;uint32_t row=0;s=ud_state_row(&w->row_reader,w->partition,w->position,&b,&row);
      for(uint32_t i=0;!s&&i<w->input->columns;i++)s=ud_batch_get(b,row,i,values+i);
      ud_value*result=values+w->input->columns;const ud_field*f=&w->schema->column[w->input->columns].field;*result=(ud_value){.type=f->type,.precision=f->precision,.scale=f->scale};int kind=w->options.window_function;
      if(!s&&(kind==UD_WINDOW_ROW_NUMBER||kind==UD_WINDOW_RANK||kind==UD_WINDOW_DENSE_RANK)){
        int peer=w->position>0;for(uint32_t k=w->options.partition_key_count;!s&&peer&&k<w->options.key_count;k++){ud_value prior;s=value_at(w,w->position-1,w->keys[k],&prior);if(!s)peer=!ud_value_compare(&prior,values+w->keys[k]);}
        if(!peer){w->rank=w->position+1;w->dense_rank++;}uint64_t n=kind==UD_WINDOW_ROW_NUMBER?w->position+1:kind==UD_WINDOW_RANK?w->rank:w->dense_rank;if(n>INT64_MAX)s=UD_OVERFLOW;else result->integer=(int64_t)n;
      }else if(!s&&(kind==UD_WINDOW_LAG||kind==UD_WINDOW_LEAD)){
        uint64_t offset=w->options.window_offset;int available=kind==UD_WINDOW_LAG?offset<=w->position:offset<w->partition->rows-w->position;
        if(!available)result->is_null=1;else s=value_at(w,kind==UD_WINDOW_LAG?w->position-offset:w->position+offset,w->options.window_column,result);
      }else if(!s){uint64_t begin=0,end=0;s=frame(w,&begin,&end);if(!s&&begin==end)result->is_null=1;
        else if(!s&&(kind==UD_WINDOW_FIRST||kind==UD_WINDOW_LAST))s=value_at(w,kind==UD_WINDOW_FIRST?begin:end-1,w->options.window_column,result);
        else if(!s)s=frame_sum(w,begin,end,result);}
      if(!s)s=ud_batch_append(*out,values,w->schema->columns);if(!s)s=ud_batch_copy_row_info(*out,(*out)->rows-1,b,row);if(!s)w->position++;
    }
    ud_free(values);if(s||!(*out)->rows){ud_batch_destroy(*out);*out=NULL;return s?s:UD_END;}return ud_batch_seal(*out);
}
ud_status ud_window_schema(const ud_window*w,ud_batch**out){return clone(w->ctx,w->schema,0,out);}
ud_status ud_window_order(const ud_window*w,ud_sort_contract*out){return ud_operator_order(w->sort,out);}
ud_status ud_window_push(ud_window*w,const ud_batch*b){return w->ended?UD_INVALID:ud_operator_push(w->sort,0,b);}
ud_status ud_window_end(ud_window*w){if(w->ended)return UD_INVALID;UD_TRY(ud_operator_finish_input(w->sort,0));w->ended=1;return UD_OK;}
uint64_t ud_window_demand(const ud_window*w){return w->ended?0:1;}
void ud_window_destroy(ud_window*w){if(!w)return;partition_clear(w);ud_batch_destroy(w->pending);ud_operator_destroy(w->sort);ud_batch_destroy(w->input);ud_batch_destroy(w->schema);ud_free(w->keys);ud_free(w->partition_keys);ud_free(w);}
