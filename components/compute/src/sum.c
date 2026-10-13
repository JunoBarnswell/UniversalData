#include "state.h"
#include <math.h>
ud_status ud_sum_wide_add(uint64_t target[4],const uint64_t input[4]) {
    uint64_t sign_a=target[3]>>63,sign_b=input[3]>>63,carry=0;
    for(uint32_t i=0;i<4;i++){uint64_t first=target[i]+input[i],c1=first<target[i],second=first+carry,c2=second<first;target[i]=second;carry=c1|c2;}
    return sign_a==sign_b&&(target[3]>>63)!=sign_a?UD_OVERFLOW:UD_OK;
}
void ud_sum_wide_value(const ud_value*v,uint64_t words[4]) {
    if(v->type==UD_INT64){words[0]=(uint64_t)v->integer;words[1]=words[2]=words[3]=v->integer<0?UINT64_MAX:0;}
    else{words[0]=v->decimal.low;words[1]=(uint64_t)v->decimal.high;words[2]=words[3]=v->decimal.high<0?UINT64_MAX:0;}
}
ud_status ud_sum_wide_result(const ud_field*f,const uint64_t words[4],ud_value*out) {
    if(f->type==UD_INT64){uint64_t sign=words[0]>>63?UINT64_MAX:0;if(words[1]!=sign||words[2]!=sign||words[3]!=sign)return UD_OVERFLOW;out->integer=(int64_t)words[0];return UD_OK;}
    uint64_t sign=words[1]>>63?UINT64_MAX:0;if(words[2]!=sign||words[3]!=sign)return UD_OVERFLOW;
    return ud_decimal_rescale((ud_decimal128){words[0],(int64_t)words[1]},f->scale,f->precision,f->scale,UD_ROUND_EXACT,&out->decimal);
}
static void negate(uint64_t words[4]) {uint64_t carry=1;for(uint32_t i=0;i<4;i++){uint64_t v=~words[i];words[i]=v+carry;carry=carry&&words[i]==0;}}
ud_status ud_sum_update(ud_sum_state*s,const ud_value*v,int32_t direction) {
    if(!s||!v||(direction!=1&&direction!=-1))return UD_INVALID;if(v->is_null)return UD_OK;
    if((direction>0&&s->count==INT64_MAX)||(direction<0&&!s->count))return UD_OVERFLOW;
    if(!s->count)s->scale=v->type==UD_DECIMAL128?v->scale:0;
    if(v->type==UD_FLOAT64){double next=direction>0?s->real+v->real:s->real-v->real;if(isfinite(s->real)&&isfinite(v->real)&&!isfinite(next))return UD_OVERFLOW;s->real=next;}
    else if(v->type==UD_INT64||v->type==UD_DECIMAL128){uint64_t words[4];ud_sum_wide_value(v,words);if(direction<0)negate(words);UD_TRY(ud_sum_wide_add(s->words,words));}
    else return UD_SCHEMA;
    if(direction>0)s->count++;else s->count--;if(!s->count){memset(s->words,0,sizeof(s->words));s->real=0;}return UD_OK;
}
static ud_status times_ten(uint64_t words[4]) {
    uint64_t doubled[4];memcpy(doubled,words,sizeof(doubled));
    for(uint32_t k=0;k<3;k++){uint64_t carry=0;for(uint32_t i=0;i<4;i++){uint64_t next=doubled[i]>>63;doubled[i]=(doubled[i]<<1)|carry;carry=next;}if(carry)return UD_OVERFLOW;}
    uint64_t twice[4];memcpy(twice,words,sizeof(twice));uint64_t carry=0;for(uint32_t i=0;i<4;i++){uint64_t next=twice[i]>>63;twice[i]=(twice[i]<<1)|carry;carry=next;}if(carry)return UD_OVERFLOW;
    carry=0;for(uint32_t i=0;i<4;i++){uint64_t a=doubled[i]+twice[i],c=a<doubled[i],b=a+carry;carry=c|(b<a);words[i]=b;}return carry?UD_OVERFLOW:UD_OK;
}
ud_status ud_sum_result(const ud_sum_state*s,const ud_field*f,int average,int32_t rounding,ud_value*out) {
    if(!s||!f||!out)return UD_INVALID;*out=(ud_value){.type=f->type,.precision=f->precision,.scale=f->scale,.is_null=!s->count};if(!s->count)return UD_OK;
    if(f->type==UD_FLOAT64){out->real=average?s->real/(double)s->count:s->real;return UD_OK;}
    uint64_t words[4];memcpy(words,s->words,sizeof(words));if(!average)return ud_sum_wide_result(f,words,out);
    int negative=(int)(words[3]>>63);if(negative)negate(words);
    /* AVG promotes the coefficient by six decimal digits. The bound result
     * schema owns this scale change, never a conversion through double. */
    if(f->scale<s->scale || f->scale-s->scale>6)return UD_SCHEMA;
    for(int32_t i=s->scale;i<f->scale;i++)UD_TRY(times_ten(words));
    uint64_t quotient[4]={0},remainder=0;
    for(uint32_t bit=256;bit;bit--){uint32_t at=bit-1;remainder=(remainder<<1)|((words[at/64]>>(at%64))&1);if(remainder>=s->count){remainder-=s->count;quotient[at/64]|=UINT64_C(1)<<(at%64);}}
    if(remainder){if(rounding==UD_ROUND_EXACT)return UD_PARSE;
      int up=rounding==UD_ROUND_HALF_EVEN&&(remainder>s->count-remainder||(remainder==s->count-remainder&&(quotient[0]&1)));
      if(rounding!=UD_ROUND_HALF_EVEN)return UD_INVALID;
      if(up){uint64_t carry=1;for(uint32_t i=0;i<4&&carry;i++){uint64_t v=quotient[i];quotient[i]++;carry=quotient[i]<v;}}}
    if(negative)negate(quotient);return ud_sum_wide_result(f,quotient,out);
}
