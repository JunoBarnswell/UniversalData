#include "advanced_internal.h"
#include <math.h>
/* One bounded candidate index for lookup and grouping. Unicode unigrams are
 * mandatory for completeness at a positive similarity threshold; ngrams rank
 * candidates. Exceeding the candidate budget fails, never silently truncates. */
typedef struct posting { uint32_t symbol,row,next; } posting;
typedef struct match_row { ud_id id; uint32_t parent; } match_row;
typedef struct match_choice { uint32_t row; double score; } match_choice;
typedef struct matching {
    uint32_t *buckets,*seen,*candidates;posting *postings;match_row *rows;
    uint32_t count,posting_count,maximum_rows,maximum_postings,maximum_candidates,stamp;
    uint32_t reference_column,method,topk,choices,choice;
    double threshold;match_choice best[64];
    ud_state *state;ud_state_reader reader,current_reader;
    uint64_t compared,candidate_total;
} matching;
static ud_status uint_config(ud_advanced*a,const char*name,uint32_t fallback,uint32_t*out){const ud_json*j=ud_advanced_member(a->config,name);if(!j){*out=fallback;return UD_OK;}const char*p;uint32_t n;UD_TRY(ud_json_text(j,&p,&n));uint64_t v=0;for(uint32_t i=0;i<n;i++){if(p[i]<'0'||p[i]>'9'||v>UINT32_MAX/10)return UD_SCHEMA;v=v*10+(unsigned)(p[i]-'0');}if(v>UINT32_MAX)return UD_SCHEMA;*out=(uint32_t)v;return UD_OK;}
static ud_status points(ud_advanced*a,const ud_value*v,uint32_t out[512],uint32_t*n){*n=0;if(v->is_null)return UD_OK;if(v->type!=UD_UTF8)return UD_SCHEMA;char*text=NULL;uint32_t length;UD_TRY(ud_text_transform(a->ctx,(ud_string){v->text,v->text_length},UD_TEXT_NFKC|UD_TEXT_FOLD|UD_TEXT_TRIM,&text,&length));ud_status s=UD_OK;
    for(uint32_t i=0;!s&&i<length;){if(*n==512){s=UD_RESOURCE;break;}uint32_t c=(unsigned char)text[i++],more=c<0x80?0:c<0xe0?1:c<0xf0?2:3;c&=more?(1u<<(6-more))-1:127;for(uint32_t k=0;k<more;k++)c=(c<<6)|((unsigned char)text[i++]&63);out[(*n)++]=c;}ud_free(text);return s;
}
static double similarity(const uint32_t*a,uint32_t n,const uint32_t*b,uint32_t m,uint32_t method){if(!n||!m)return n==m?1:0;
    if(method==1){uint32_t prior[513],next[513];for(uint32_t j=0;j<=m;j++)prior[j]=j;for(uint32_t i=0;i<n;i++){next[0]=i+1;for(uint32_t j=0;j<m;j++){uint32_t v=prior[j]+(a[i]!=b[j]);if(next[j]+1<v)v=next[j]+1;if(prior[j+1]+1<v)v=prior[j+1]+1;next[j+1]=v;}memcpy(prior,next,(m+1)*sizeof(*prior));}return 1.0-(double)prior[m]/(n>m?n:m);}
    uint8_t left[512]={0},right[512]={0};uint32_t window=(n>m?n:m)/2;window=window?window-1:0;uint32_t matches=0;
    for(uint32_t i=0;i<n;i++){uint32_t begin=i>window?i-window:0,end=i+window+1;if(end>m)end=m;for(uint32_t j=begin;j<end;j++)if(!right[j]&&a[i]==b[j]){left[i]=right[j]=1;matches++;break;}}
    if(!matches)return 0;uint32_t j=0,transpositions=0;for(uint32_t i=0;i<n;i++)if(left[i]){while(j<m&&!right[j])j++;transpositions+=a[i]!=b[j++];}
    double score=((double)matches/n+(double)matches/m+((double)matches-transpositions/2.0)/matches)/3;
    if(score>0.7){uint32_t prefix=0;while(prefix<4&&prefix<n&&prefix<m&&a[prefix]==b[prefix])prefix++;score+=prefix*0.1*(1-score);}return score;
}
static int id_compare(ud_id a,ud_id b){return a.high!=b.high?(a.high<b.high?-1:1):a.low!=b.low?(a.low<b.low?-1:1):0;}
static uint32_t root(matching*m,uint32_t row){uint32_t r=row;while(m->rows[r].parent!=r)r=m->rows[r].parent;while(row!=r){uint32_t next=m->rows[row].parent;m->rows[row].parent=r;row=next;}return r;}
static void unite(matching*m,uint32_t a,uint32_t b){a=root(m,a);b=root(m,b);if(a==b)return;if(id_compare(m->rows[a].id,m->rows[b].id)>0){uint32_t t=a;a=b;b=t;}m->rows[b].parent=a;}
static ud_status candidate_rows(ud_advanced*a,const uint32_t*p,uint32_t n,uint32_t*count){matching*m=a->index;*count=0;if(++m->stamp==0){memset(m->seen,0,m->maximum_rows*sizeof(*m->seen));m->stamp=1;}
    for(uint32_t i=0;i<n;i++)for(uint32_t at=m->buckets[p[i]&65535];at!=UINT32_MAX;at=m->postings[at].next){posting*entry=m->postings+at;if(entry->symbol!=p[i]||m->seen[entry->row]==m->stamp)continue;if(*count==m->maximum_candidates)return UD_RESOURCE;m->seen[entry->row]=m->stamp;m->candidates[(*count)++]=entry->row;}
    m->candidate_total+=*count;return UD_OK;
}
static ud_status insert_row(ud_advanced*a,const uint32_t*p,uint32_t n,ud_id id){matching*m=a->index;if(m->count==m->maximum_rows)return UD_RESOURCE;uint32_t row=m->count;
    for(uint32_t i=0;i<n;i++){int duplicate=0;for(uint32_t k=0;k<i;k++)duplicate|=p[k]==p[i];if(duplicate)continue;if(m->posting_count==m->maximum_postings)return UD_RESOURCE;uint32_t slot=p[i]&65535;m->postings[m->posting_count]=(posting){p[i],row,m->buckets[slot]};m->buckets[slot]=m->posting_count++;}
    m->rows[row]=(match_row){id,row};m->count++;return UD_OK;
}
static ud_status allocate_index(ud_advanced*a){matching*m=a->index;if(m->buckets)return UD_OK;if(!a->options.buffers)return UD_INVALID;
    uint64_t required=65536*sizeof(*m->buckets)+(uint64_t)m->maximum_rows*(sizeof(*m->rows)+sizeof(*m->seen))+(uint64_t)m->maximum_postings*sizeof(*m->postings)+(uint64_t)m->maximum_candidates*sizeof(*m->candidates);if(a->options.memory_budget&&required>a->options.memory_budget)return UD_RESOURCE;
    UD_TRY(ud_buffer_working_allocate(a->options.buffers,65536*sizeof(*m->buckets),(void**)&m->buckets));memset(m->buckets,255,65536*sizeof(*m->buckets));
    UD_TRY(ud_buffer_working_allocate(a->options.buffers,(size_t)m->maximum_rows*sizeof(*m->rows),(void**)&m->rows));
    UD_TRY(ud_buffer_working_allocate(a->options.buffers,(size_t)m->maximum_rows*sizeof(*m->seen),(void**)&m->seen));
    UD_TRY(ud_buffer_working_allocate(a->options.buffers,(size_t)m->maximum_postings*sizeof(*m->postings),(void**)&m->postings));
    return ud_buffer_working_allocate(a->options.buffers,(size_t)m->maximum_candidates*sizeof(*m->candidates),(void**)&m->candidates);
}
static ud_status reference_row(ud_advanced*a,uint32_t position,const ud_batch**b,uint32_t*row){matching*m=a->index;return a->options.kind==UD_OPERATOR_FUZZY_GROUPING?ud_state_row(&m->reader,m->state,position,b,row):ud_cache_read_at(a->cache_reader,position,b,row);}
static ud_status build_cache_index(ud_advanced*a){matching*m=a->index;UD_TRY(allocate_index(a));UD_TRY(ud_cache_scan_create(a->reference,&a->cache_reader));const ud_batch*b;uint32_t row;
    for(;;){ud_status s=ud_cache_next(a->cache_reader,&b,&row);if(s==UD_END)break;if(s)return s;UD_TRY(ud_advanced_check(a));ud_value v;UD_TRY(ud_batch_get(b,row,m->reference_column,&v));uint32_t p[512],n;UD_TRY(points(a,&v,p,&n));ud_row_info info;UD_TRY(ud_batch_row_info(b,row,&info));if(!info.id.high&&!info.id.low)return UD_SCHEMA;UD_TRY(insert_row(a,p,n,info.id));}
    return UD_OK;
}
ud_status ud_matching_bind(ud_advanced*a){matching*m=ud_alloc(a->ctx,sizeof(*m));if(!m)return UD_RESOURCE;a->index=m;m->threshold=0.8;m->method=ud_advanced_named(ud_advanced_member(a->config,"method"),"editDistance")?1:2;UD_TRY(uint_config(a,"maxRows",65536,&m->maximum_rows));uint32_t posting_limit=a->options.memory_budget?(uint32_t)(a->options.memory_budget/sizeof(posting)/2):262144;UD_TRY(uint_config(a,"maxIndexEntries",posting_limit,&m->maximum_postings));UD_TRY(uint_config(a,"maxCandidates",4096,&m->maximum_candidates));UD_TRY(uint_config(a,"topK",1,&m->topk));
    const ud_json*t=ud_advanced_member(a->config,"threshold");if(t){const char*p;uint32_t n;UD_TRY(ud_json_text(t,&p,&n));char*copy=ud_strdup_n(a->ctx,p,n);if(!copy)return UD_RESOURCE;m->threshold=strtod(copy,NULL);ud_free(copy);}if(m->threshold<=0||m->threshold>1||!m->topk||m->topk>64)return UD_SCHEMA;
    UD_TRY(ud_compute_field_index(a->inputs[0],ud_advanced_member(a->config,"column"),&a->column));if(a->inputs[0]->column[a->column].field.type!=UD_UTF8)return UD_SCHEMA;
    int grouping=a->options.kind==UD_OPERATOR_FUZZY_GROUPING;ud_batch*joined=NULL;ud_status s=UD_OK;
    if(!grouping){if(!a->inputs[1])return UD_SCHEMA;s=ud_compute_field_index(a->inputs[1],ud_advanced_member(a->config,"referenceColumn"),&m->reference_column);if(!s&&a->inputs[1]->column[m->reference_column].field.type!=UD_UTF8)s=UD_SCHEMA;if(!s)s=ud_compute_join_schema(a->ctx,a->inputs[0],a->inputs[1],&joined);a->outputs=3;a->error_port=2;if(!s)s=ud_advanced_clone(a->ctx,a->inputs[0],0,&a->schema[1]);}
    const ud_batch*base=grouping?a->inputs[0]:joined;uint32_t extra=grouping?3:1,n=base?base->columns:0;ud_field*fields=!s?ud_alloc(a->ctx,(n+extra)*sizeof(*fields)):NULL;if(!s&&!fields)s=UD_RESOURCE;for(uint32_t i=0;!s&&i<n;i++)fields[i]=base->column[i].field;
    const char*names[]={"matchScore","groupId","canonicalRowId"};for(uint32_t i=0;!s&&i<extra;i++){ud_field spec=a->inputs[0]->column[a->column].field;spec.type=i?UD_UTF8:UD_FLOAT64;spec.precision=spec.scale=0;spec.nullable=0;s=ud_advanced_derive(a,&spec,(ud_string){names[i],(uint32_t)strlen(names[i])},fields+n+i);}
    if(!s)s=ud_batch_create(a->ctx,fields,n+extra,0,&a->schema[0]);for(uint32_t i=n;fields&&i<n+extra;i++)ud_field_clear(fields+i);ud_free(fields);ud_batch_destroy(joined);
    if(!s&&!grouping&&a->options.reference){s=ud_cache_retain(a->options.reference);if(!s){a->reference=a->options.reference;s=build_cache_index(a);}}return s;
}
ud_status ud_matching_push(ud_advanced*a,const ud_batch*b){matching*m=a->index;UD_TRY(allocate_index(a));if(!m->state)UD_TRY(ud_state_create(a->ctx,a->options.buffers,a->inputs[0],a->options.max_batch_rows,&m->state));
    for(uint32_t row=0;row<b->rows;row++){UD_TRY(ud_advanced_check(a));ud_value v;UD_TRY(ud_batch_get(b,row,a->column,&v));uint32_t p[512],n,candidates;UD_TRY(points(a,&v,p,&n));UD_TRY(candidate_rows(a,p,n,&candidates));uint32_t current=m->count;
      ud_row_info info;UD_TRY(ud_batch_row_info(b,row,&info));if(!info.id.high&&!info.id.low)return UD_SCHEMA;UD_TRY(insert_row(a,p,n,info.id));
      for(uint32_t i=0;i<candidates;i++){const ud_batch*prior;uint32_t index;UD_TRY(ud_state_row(&m->reader,m->state,m->candidates[i],&prior,&index));ud_value candidate;UD_TRY(ud_batch_get(prior,index,a->column,&candidate));uint32_t q[512],count;UD_TRY(points(a,&candidate,q,&count));m->compared++;if(similarity(p,n,q,count,m->method)>=m->threshold)unite(m,current,m->candidates[i]);}
      UD_TRY(ud_state_append(m->state,b,row));a->processed++;}
    return UD_OK;
}
static ud_status select_matches(ud_advanced*a,const ud_value*v){matching*m=a->index;m->choice=m->choices=0;uint32_t p[512],n,candidates;UD_TRY(points(a,v,p,&n));if(!n)return UD_OK;UD_TRY(candidate_rows(a,p,n,&candidates));
    for(uint32_t i=0;i<candidates;i++){UD_TRY(ud_advanced_check(a));uint32_t position=m->candidates[i];const ud_batch*b;uint32_t row;UD_TRY(reference_row(a,position,&b,&row));ud_value value;UD_TRY(ud_batch_get(b,row,m->reference_column,&value));uint32_t q[512],count;UD_TRY(points(a,&value,q,&count));double score=similarity(p,n,q,count,m->method);m->compared++;if(score<m->threshold)continue;uint32_t at=0;while(at<m->choices&&(m->best[at].score>score||(m->best[at].score==score&&id_compare(m->rows[m->best[at].row].id,m->rows[position].id)<0)))at++;if(at>=m->topk)continue;if(m->choices<m->topk)m->choices++;for(uint32_t k=m->choices-1;k>at;k--)m->best[k]=m->best[k-1];m->best[at]=(match_choice){position,score};}
    return UD_OK;
}
ud_status ud_matching_row(ud_advanced*a,ud_value*values,uint32_t*port,int*advance){matching*m=a->index;if(!a->reference||!m->buckets)return UD_INVALID;for(uint32_t i=0;i<a->inputs[0]->columns;i++)UD_TRY(ud_batch_get(a->pending,a->row,i,values+i));if(!a->element)UD_TRY(select_matches(a,values+a->column));
    if(!m->choices){*port=1;*advance=1;return UD_OK;}match_choice choice=m->best[a->element];const ud_batch*b;uint32_t row;UD_TRY(reference_row(a,choice.row,&b,&row));uint32_t n=a->inputs[0]->columns;for(uint32_t i=0;i<b->columns;i++)UD_TRY(ud_batch_get(b,row,i,values+n+i));values[n+b->columns]=(ud_value){.type=UD_FLOAT64,.real=choice.score};*port=0;*advance=a->element+1==m->choices;
    ud_row_info left,right;UD_TRY(ud_batch_row_info(a->pending,a->row,&left));UD_TRY(ud_batch_row_info(b,row,&right));UD_TRY(ud_row_info_join(a->ctx,&left,&right,&a->next_info));a->override_info=1;return UD_OK;
}
ud_status ud_matching_pull(ud_advanced*a,ud_batch**out,uint32_t*port){matching*m=a->index;*out=NULL;if(port)*port=0;if(!a->ended)return UD_BUSY;if(a->position>=m->count)return UD_END;UD_TRY(ud_state_seal(m->state));UD_TRY(ud_advanced_clone(a->ctx,a->schema[0],a->options.max_batch_rows,out));ud_value*values=ud_alloc(a->ctx,a->schema[0]->columns*sizeof(*values));ud_status s=values?UD_OK:UD_RESOURCE;
    while(!s&&a->position<m->count&&(*out)->rows<a->options.max_batch_rows){uint32_t current=(uint32_t)a->position,canonical=root(m,current);const ud_batch*b;uint32_t row;s=ud_state_row(&m->current_reader,m->state,current,&b,&row);ud_value value;for(uint32_t i=0;!s&&i<b->columns;i++)s=ud_batch_get(b,row,i,values+i);uint32_t p[512],n,q[512],count;if(!s)s=points(a,values+a->column,p,&n);
      const ud_batch*master;uint32_t mr;if(!s)s=ud_state_row(&m->reader,m->state,canonical,&master,&mr);if(!s)s=ud_batch_get(master,mr,a->column,&value);if(!s)s=points(a,&value,q,&count);char id[33];ud_id_format(m->rows[canonical].id,id);uint32_t extra=a->inputs[0]->columns;values[extra]=(ud_value){.type=UD_FLOAT64,.real=similarity(p,n,q,count,m->method)};values[extra+1]=values[extra+2]=(ud_value){.type=UD_UTF8,.text=id,.text_length=32};if(!s)s=ud_batch_append(*out,values,(*out)->columns);if(!s){ud_row_info info; s=ud_batch_row_info(b,row,&info);if(!s)s=ud_batch_set_row_info(*out,(*out)->rows-1,&info);}if(!s)a->position++;}
    ud_free(values);if(!s)s=ud_batch_seal(*out);if(s){ud_batch_destroy(*out);*out=NULL;}return s;
}
void ud_matching_clear(ud_advanced*a){matching*m=a->index;if(!m)return;ud_state_reader_clear(&m->reader);ud_state_reader_clear(&m->current_reader);ud_state_destroy(m->state);ud_free(m->buckets);ud_free(m->postings);ud_free(m->rows);ud_free(m->seen);ud_free(m->candidates);ud_free(m);a->index=NULL;}
