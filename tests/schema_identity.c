#include "check.h"
#include "ud/format.h"
#include <string.h>
static ud_string text(const char *s) { return (ud_string){s,(uint32_t)strlen(s)}; }
int main(void) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));
    ud_field old_children[]={
        {.name="amount",.name_length=6,.type=UD_DECIMAL128,.precision=10,.scale=2,.identity={1,2},.physical_identity={10,2}},
        {.name="label",.name_length=5,.type=UD_UTF8,.nullable=1,.identity={1,3},.physical_identity={10,3}}};
    ud_field actual_children[]={
        {.name="extra",.name_length=5,.type=UD_INT64,.identity={1,4},.physical_identity={10,4}},
        {.name="renamed",.name_length=7,.type=UD_UTF8,.nullable=1,.identity={9,3},.physical_identity={10,3}},
        {.name="amount",.name_length=6,.type=UD_DECIMAL128,.precision=8,.scale=1,.identity={9,2},.physical_identity={10,2}}};
    ud_field old={.name="record",.name_length=6,.type=UD_STRUCT,.nullable=1,.children=old_children,.child_count=2,.identity={1,1},.physical_identity={10,1}},
             actual={.name="renamed_record",.name_length=14,.type=UD_STRUCT,.nullable=1,.children=actual_children,.child_count=3,.identity={9,1},.physical_identity={10,1}};
    ud_batch *expected=NULL,*discovered=NULL,*bound=NULL,*input=NULL,*mapped=NULL;
    OK(ud_batch_create(ctx,&old,1,0,&expected));OK(ud_batch_create(ctx,&actual,1,0,&discovered));
    OK(ud_schema_bind_identity(ctx,expected,discovered,&bound));
    ud_field field={0};OK(ud_batch_field(bound,0,&field));
    CHECK(!ud_id_compare(field.identity,old.identity) && !ud_id_compare(field.physical_identity,old.physical_identity));
    CHECK(!ud_id_compare(field.children[1].identity,old_children[1].identity));
    OK(ud_batch_create(ctx,&field,1,2,&input));
    ud_value children[]={{.type=UD_INT64,.integer=999},{.type=UD_UTF8,.text="value",.text_length=5},
        {.type=UD_DECIMAL128,.precision=8,.scale=1,.decimal={123,0}}};
    ud_value row={.type=UD_STRUCT,.children=children,.child_count=3};OK(ud_batch_append(input,&row,1));
    ud_row_info identity={.id={99,101},.has_source_position=1,.source_position=800};OK(ud_batch_set_row_info(input,0,&identity));
    row=(ud_value){.type=UD_STRUCT,.is_null=1};OK(ud_batch_append(input,&row,1));OK(ud_batch_seal(input));
    OK(ud_schema_map(ctx,input,expected,text("{\"policy\":\"compatible\"}"),&mapped));
    ud_value value={0};OK(ud_batch_get(mapped,0,0,&value));CHECK(value.child_count==2 && value.children[0].decimal.low==1230 && value.children[0].scale==2);
    CHECK(value.children[1].text_length==5 && !memcmp(value.children[1].text,"value",5));
    OK(ud_batch_get(mapped,1,0,&value));CHECK(value.is_null);
    ud_row_info info={0};OK(ud_batch_row_info(mapped,0,&info));CHECK(!ud_id_compare(info.id,identity.id) && info.source_position==800);
    ud_batch *roundtrip=NULL;uint8_t *wire=NULL;uint64_t size=0;
    OK(ud_batch_serialize(mapped,&wire,&size));OK(ud_batch_deserialize(ctx,wire,size,&roundtrip));
    OK(ud_batch_field(roundtrip,0,&field));CHECK(ud_field_matches(&old,&field,UD_FIELD_PORT));ud_batch_destroy(roundtrip);roundtrip=NULL;
    uint64_t legacy_columns=1;memcpy(wire+8,&legacy_columns,8);
    CHECK(ud_batch_deserialize(ctx,wire,size,&roundtrip)==UD_INCOMPATIBLE && !roundtrip);ud_free(wire);
    struct ArrowArray array={0};struct ArrowSchema schema={0};OK(ud_batch_export_arrow(mapped,&array,&schema));
    OK(ud_batch_import_arrow(ctx,&array,&schema,&roundtrip));OK(ud_batch_field(roundtrip,0,&field));
    CHECK(!ud_id_compare(field.physical_identity,old.physical_identity) && !ud_id_compare(field.children[0].physical_identity,old_children[0].physical_identity));
    array.release(&array);schema.release(&schema);ud_batch_destroy(roundtrip);roundtrip=NULL;
    char *json=NULL;uint32_t length=0;OK(ud_schema_to_json(mapped,&json,&length));
    CHECK(strstr(json,"physicalIdentity"));OK(ud_schema_from_json(ctx,json,length,text("another-package"),&roundtrip));ud_free(json);
    OK(ud_batch_field(roundtrip,0,&field));CHECK(ud_field_matches(&old,&field,UD_FIELD_PORT));ud_batch_destroy(roundtrip);
    ud_batch_destroy(discovered);discovered=NULL;ud_batch_destroy(bound);bound=NULL;
    /* Same display name, different provider incarnation: never borrow the old logical ID. */
    actual=old;actual.identity=(ud_id){0};actual.physical_identity=(ud_id){10,999};
    OK(ud_batch_create(ctx,&actual,1,0,&discovered));OK(ud_schema_bind_identity(ctx,expected,discovered,&bound));
    OK(ud_batch_field(bound,0,&field));CHECK(ud_id_compare(field.identity,old.identity));
    roundtrip=NULL;CHECK(ud_schema_map(ctx,bound,expected,text("{\"policy\":\"compatible\"}"),&roundtrip)==UD_SCHEMA && !roundtrip);
    ud_batch_destroy(bound);ud_batch_destroy(discovered);ud_batch_destroy(mapped);ud_batch_destroy(input);ud_batch_destroy(expected);
    CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));
    puts("Physical/logical identity, nested rename/reorder/projection, exact decimal widening, nulls, provenance, Wire/Arrow/JSON and drop/add refusal passed");return 0;
}
