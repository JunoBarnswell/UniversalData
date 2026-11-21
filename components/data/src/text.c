#include "ud/text.h"
#include "../internal.h"
#include <unicode/ustring.h>
#include <unicode/unorm2.h>
#include <unicode/uchar.h>
#include <unicode/utf16.h>
#include <unicode/utf8.h>
#include <unicode/ubrk.h>
static ud_status from_utf8(ud_context *ctx,ud_string input,UChar **out,int32_t *length) {
    if(input.length>INT32_MAX||(!input.data&&input.length))return UD_SCHEMA;
    UErrorCode error=U_ZERO_ERROR;*length=0;
    u_strFromUTF8(NULL,0,length,input.data,(int32_t)input.length,&error);
    if(error!=U_BUFFER_OVERFLOW_ERROR&&U_FAILURE(error))return UD_PARSE;
    *out=ud_alloc(ctx,((size_t)*length+1)*sizeof(UChar));if(!*out)return UD_RESOURCE;
    error=U_ZERO_ERROR;u_strFromUTF8(*out,*length+1,length,input.data,(int32_t)input.length,&error);
    if(U_FAILURE(error)){ud_free(*out);*out=NULL;return UD_PARSE;}return UD_OK;
}
static ud_status to_utf8(ud_context *ctx,const UChar *input,int32_t count,char **out,uint32_t *length) {
    UErrorCode error=U_ZERO_ERROR;int32_t size=0;u_strToUTF8(NULL,0,&size,input,count,&error);
    if(error!=U_BUFFER_OVERFLOW_ERROR&&U_FAILURE(error))return UD_PARSE;
    if(size<0||size==INT32_MAX)return UD_RESOURCE;*out=ud_alloc(ctx,(size_t)size+1);if(!*out)return UD_RESOURCE;
    error=U_ZERO_ERROR;u_strToUTF8(*out,size+1,&size,input,count,&error);
    if(U_FAILURE(error)){ud_free(*out);*out=NULL;return UD_PARSE;}*length=(uint32_t)size;return UD_OK;
}
static ud_status normalization(ud_context *ctx,UChar **data,int32_t *length,uint32_t flag) {
    UErrorCode error=U_ZERO_ERROR;
    const UNormalizer2 *normal=flag==UD_TEXT_NFC?unorm2_getNFCInstance(&error):unorm2_getNFKCInstance(&error);
    if(U_FAILURE(error))return UD_RESOURCE;
    int32_t size=unorm2_normalize(normal,*data,*length,NULL,0,&error);
    if(error!=U_BUFFER_OVERFLOW_ERROR&&U_FAILURE(error))return UD_PARSE;
    UChar *value=ud_alloc(ctx,((size_t)size+1)*sizeof(*value));if(!value)return UD_RESOURCE;
    error=U_ZERO_ERROR;size=unorm2_normalize(normal,*data,*length,value,size+1,&error);
    if(U_FAILURE(error)){ud_free(value);return UD_PARSE;}ud_free(*data);*data=value;*length=size;return UD_OK;
}
static ud_status casing(ud_context *ctx,UChar **data,int32_t *length,uint32_t flag) {
    UErrorCode error=U_ZERO_ERROR;int32_t size=flag==UD_TEXT_FOLD?u_strFoldCase(NULL,0,*data,*length,U_FOLD_CASE_DEFAULT,&error):flag==UD_TEXT_UPPER?u_strToUpper(NULL,0,*data,*length,"",&error):u_strToLower(NULL,0,*data,*length,"",&error);
    if(error!=U_BUFFER_OVERFLOW_ERROR&&U_FAILURE(error))return UD_PARSE;
    UChar *value=ud_alloc(ctx,((size_t)size+1)*sizeof(*value));if(!value)return UD_RESOURCE;
    error=U_ZERO_ERROR;if(flag==UD_TEXT_FOLD)size=u_strFoldCase(value,size+1,*data,*length,U_FOLD_CASE_DEFAULT,&error);else if(flag==UD_TEXT_UPPER)size=u_strToUpper(value,size+1,*data,*length,"",&error);else size=u_strToLower(value,size+1,*data,*length,"",&error);
    if(U_FAILURE(error)){ud_free(value);return UD_PARSE;}ud_free(*data);*data=value;*length=size;return UD_OK;
}
static ud_status trim_utf8(ud_context *ctx, ud_string input, char **out, uint32_t *length) {
    if (input.length > INT32_MAX || (!input.data && input.length))
        return UD_SCHEMA;
    const char *data = input.data ? input.data : "";
    if (!ud_utf8_valid(data, input.length))
        return UD_PARSE;
    int32_t begin = 0, end = (int32_t)input.length;
    while (begin < end) {
        int32_t next = begin;
        UChar32 codepoint;
        U8_NEXT_UNSAFE(data, next, codepoint);
        if (!u_isUWhiteSpace(codepoint))
            break;
        begin = next;
    }
    while (end > begin) {
        int32_t next = end;
        UChar32 codepoint;
        U8_PREV_UNSAFE(data, next, codepoint);
        if (!u_isUWhiteSpace(codepoint))
            break;
        end = next;
    }
    *out = ud_strdup_n(ctx, data + begin, (size_t)(end - begin));
    if (!*out)
        return UD_RESOURCE;
    *length = (uint32_t)(end - begin);
    return UD_OK;
}
ud_status ud_text_transform(ud_context *ctx,ud_string input,uint32_t flags,char **out,uint32_t *length) {
    if(!ctx||!out||!length||flags&~63u)return UD_INVALID;*out=NULL;*length=0;
    if (flags == UD_TEXT_TRIM)
        return trim_utf8(ctx, input, out, length);
    if((flags&UD_TEXT_NFC)&&(flags&UD_TEXT_NFKC))return UD_SCHEMA;
    uint32_t cases=flags&(UD_TEXT_FOLD|UD_TEXT_UPPER|UD_TEXT_LOWER);if(cases&&(cases&(cases-1)))return UD_SCHEMA;
    UChar *data=NULL;int32_t n=0;ud_status s=from_utf8(ctx,input,&data,&n);
    if(!s&&flags&(UD_TEXT_NFC|UD_TEXT_NFKC))s=normalization(ctx,&data,&n,flags&UD_TEXT_NFC?UD_TEXT_NFC:UD_TEXT_NFKC);
    if(!s&&cases)s=casing(ctx,&data,&n,cases);
    int32_t begin=0,end=n;
    if(!s&&flags&UD_TEXT_TRIM){while(begin<end){int32_t next=begin;UChar32 cp;U16_NEXT(data,next,end,cp);if(!u_isUWhiteSpace(cp))break;begin=next;}while(end>begin){int32_t next=end;UChar32 cp;U16_PREV(data,begin,next,cp);if(!u_isUWhiteSpace(cp))break;end=next;}}
    if(!s)s=to_utf8(ctx,data+begin,end-begin,out,length);ud_free(data);return s;
}
ud_status ud_text_terms(ud_context *ctx,ud_string input,uint32_t maximum,ud_batch **out) {
    if(!ctx||!maximum||maximum>65536||!out)return UD_INVALID;*out=NULL;
    UChar *data=NULL;int32_t n=0;UD_TRY(from_utf8(ctx,input,&data,&n));
    UErrorCode error=U_ZERO_ERROR;UBreakIterator *it=ubrk_open(UBRK_WORD,"",data,n,&error);ud_status s=U_FAILURE(error)?UD_PARSE:UD_OK;
    const ud_field fields[]={{.name="term",.name_length=4,.type=UD_UTF8},{.name="offset",.name_length=6,.type=UD_INT64}};
    if(!s)s=ud_batch_create(ctx,fields,2,maximum,out);
    int32_t begin=!s?ubrk_first(it):0; uint32_t byte_offset=0;
    for(int32_t end=!s?ubrk_next(it):UBRK_DONE;!s&&end!=UBRK_DONE;begin=end,end=ubrk_next(it)) {
        if(ud_is_cancelled(ctx)){s=UD_CANCELLED;break;}
        uint32_t offset=byte_offset; for(int32_t at=begin;at<end;){UChar32 cp;U16_NEXT(data,at,end,cp);byte_offset += cp<0x80?1:cp<0x800?2:cp<0x10000?3:4;}
        if(ubrk_getRuleStatus(it)<UBRK_WORD_NONE_LIMIT)continue;
        if((*out)->rows==maximum){s=UD_RESOURCE;break;}
        char *term=NULL;uint32_t length=0;s=to_utf8(ctx,data+begin,end-begin,&term,&length);
        if(!s){ud_value values[]={{.type=UD_UTF8,.text=term,.text_length=length},{.type=UD_INT64,.integer=offset}};s=ud_batch_append(*out,values,2);}ud_free(term);
    }
    if(it)ubrk_close(it);ud_free(data);if(!s)s=ud_batch_seal(*out);if(s){ud_batch_destroy(*out);*out=NULL;}return s;
}
