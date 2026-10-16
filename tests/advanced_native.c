#include "check.h"
#include "ud/operator.h"
#include "ud/quality.h"
#include "ud/cache.h"
#include "ud/format.h"
#include "ud/regex.h"
#include <string.h>
#include <math.h>
static ud_string text(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status input(ud_context *c, const char *schema, const char *rows, ud_batch **out) {
    ud_batch *raw = NULL;
    ud_status s = ud_batch_from_json_schema(c, schema, (uint32_t)strlen(schema), rows,
                                            (uint32_t)strlen(rows), UD_ROUND_EXACT, &raw);
    if (!s)
        s = ud_batch_seal(raw);
    ud_id scope;
    if (!s)
        s = ud_id_make(c, text("advanced.acceptance"), text(schema), &scope);
    if (!s)
        s = ud_batch_identify(c, raw, scope, 0, out);
    ud_batch_destroy(raw);
    return s;
}
static ud_status op(ud_context *c, ud_buffer_manager *m, int kind, const char *cfg,
                    const ud_batch *const *in, ud_operator **out) {
    ud_operator_options o = {.struct_size = sizeof(o),
                             .abi_version = UD_ABI_VERSION,
                             .kind = kind,
                             .max_batch_rows = 8,
                             .buffers = m,
                             .configuration = text(cfg),
                             .memory_budget = 1048576,
                             .spill_budget = 16777216,
                             .record_state = 1};
    return ud_operator_create(c, &o, in, out);
}
static int slice_roundtrip(ud_context *c) {
    const char *schema = "{\"version\":12,\"fields\":[{\"name\":\"k\",\"type\":\"int64\","
                         "\"nullable\":false},{\"name\":\"s\",\"type\":\"utf8\",\"nullable\":true},"
                         "{\"name\":\"xs\",\"type\":\"list\",\"children\":[{\"name\":\"item\","
                         "\"type\":\"int64\",\"nullable\":true}],\"nullable\":false}]}";
    ud_batch *b = NULL, *slice = NULL, *nested = NULL, *restored = NULL;
    OK(input(c, schema,
             "[{\"k\":1,\"s\":\"首\",\"xs\":[1]},{\"k\":2,\"s\":null,\"xs\":[2,null]},{\"k\":3,"
             "\"s\":\"汉😀\",\"xs\":[3,4]}]",
             &b));
    OK(ud_batch_slice(c, b, 1, 2, &slice));
    OK(ud_batch_slice(c, slice, 1, 1, &nested));
    ud_batch_destroy(slice);
    ud_batch_destroy(b);
    ud_value v;
    OK(ud_batch_get(nested, 0, 0, &v));
    CHECK(v.integer == 3);
    OK(ud_batch_get(nested, 0, 1, &v));
    CHECK(v.text_length == 7);
    OK(ud_batch_get(nested, 0, 2, &v));
    CHECK(v.child_count == 2 && v.children[1].integer == 4);
    ud_row_info expected, actual;
    OK(ud_batch_row_info(nested, 0, &expected));
    CHECK(expected.id.high || expected.id.low);
    uint8_t *wire = NULL;
    uint64_t size;
    OK(ud_batch_serialize(nested, &wire, &size));
    OK(ud_batch_deserialize(c, wire, size, &restored));
    ud_free(wire);
    OK(ud_batch_row_info(restored, 0, &actual));
    CHECK(!ud_id_compare(actual.id, expected.id));
    ud_batch_destroy(restored);
    struct ArrowSchema as = {0};
    struct ArrowArray aa = {0};
    OK(ud_batch_export_arrow(nested, &aa, &as));
    OK(ud_batch_import_arrow(c, &aa, &as, &restored));
    OK(ud_batch_get(restored, 0, 1, &v));
    CHECK(v.text_length == 7);
    OK(ud_batch_get(restored, 0, 2, &v));
    CHECK(v.child_count == 2 && v.children[0].integer == 3);
    aa.release(&aa);
    as.release(&as);
    ud_batch_destroy(restored);
    ud_batch_destroy(nested);
    return 0;
}
static int join_matrix(ud_context *c, ud_buffer_manager *m) {
    ud_batch *l = NULL, *r = NULL;
    OK(input(c,
             "{\"version\":12,\"fields\":[{\"name\":\"lk\",\"type\":\"int64\",\"nullable\":true}]}",
             "[{\"lk\":null},{\"lk\":1},{\"lk\":2}]", &l));
    OK(input(c,
             "{\"version\":12,\"fields\":[{\"name\":\"rk\",\"type\":\"int64\",\"nullable\":true}]}",
             "[{\"rk\":null},{\"rk\":2},{\"rk\":2},{\"rk\":3}]", &r));
    const char *types[] = {"inner", "left", "right", "full", "semi", "anti"};
    uint32_t expected[] = {2, 4, 4, 6, 1, 2};
    const char *strategies[] = {"hash", "graceHash", "merge"};
    ud_field lf, rf;
    OK(ud_batch_field(l, 0, &lf));
    OK(ud_batch_field(r, 0, &rf));
    ud_sort_contract orders[2] = {
        {.count = 1, .stable = 1, .keys = {{.field = lf.identity, .nulls_first = 1}}},
        {.count = 1, .stable = 1, .keys = {{.field = rf.identity, .nulls_first = 1}}}};
    for (uint32_t a = 0; a < 3; a++)
        for (uint32_t k = 0; k < 6; k++) {
            char cfg[180];
            snprintf(cfg, sizeof(cfg),
                     "{\"leftKeys\":[0],\"rightKeys\":[0],\"joinType\":\"%s\",\"strategy\":\"%s\"}",
                     types[k], strategies[a]);
            ud_operator_options o = {.struct_size = sizeof(o),
                                     .abi_version = UD_ABI_VERSION,
                                     .kind = UD_OPERATOR_JOIN,
                                     .max_batch_rows = 8,
                                     .buffers = m,
                                     .configuration = text(cfg),
                                     .input_order = orders,
                                     .memory_budget = 1048576,
                                     .record_state = 1};
            const ud_batch *in[] = {l, r};
            ud_operator *j = NULL;
            OK(ud_operator_create(c, &o, in, &j));
            OK(ud_operator_push(j, 1, r));
            OK(ud_operator_finish_input(j, 1));
            OK(ud_operator_push(j, 0, l));
            OK(ud_operator_finish_input(j, 0));
            uint32_t rows = 0;
            ud_batch *b = NULL;
            ud_status s;
            while ((s = ud_operator_pull(j, &b, NULL)) == UD_OK) {
                rows += ud_batch_rows(b);
                ud_row_info info;
                OK(ud_batch_row_info(b, 0, &info));
                CHECK(info.id.high || info.id.low);
                ud_batch_destroy(b);
                b = NULL;
            }
            CHECK(s == UD_END && rows == expected[k]);
            OK(ud_operator_destroy(j));
        }
    ud_batch_destroy(l);
    ud_batch_destroy(r);
    return 0;
}
static int windows(ud_context *c, ud_buffer_manager *m) {
    ud_batch *b = NULL;
    OK(input(
        c,
        "{\"version\":12,\"fields\":[{\"name\":\"g\",\"type\":\"int64\",\"nullable\":false},{"
        "\"name\":\"t\",\"type\":\"int64\",\"nullable\":false},{\"name\":\"x\",\"type\":\"int64\","
        "\"nullable\":true}]}",
        "[{\"g\":1,\"t\":1,\"x\":10},{\"g\":1,\"t\":2,\"x\":null},{\"g\":1,\"t\":2,\"x\":null},{"
        "\"g\":1,\"t\":4,\"x\":50}]",
        &b));
    const ud_batch *in[] = {b};
    const char *functions[] = {"rowNumber", "rank", "denseRank",  "lag",       "lead",
                               "first",     "last", "runningSum", "movingSum", "movingAverage"};
    const int64_t numbers[10][4] = {
        {1, 2, 3, 4},   {1, 2, 2, 4},   {1, 2, 2, 3},     {0, 10, 0, 0},    {0, 0, 50, 0},
        {10, 10, 0, 0}, {0, 0, 50, 50}, {10, 10, 10, 60}, {10, 10, 50, 50}, {10, 10, 50, 50}};
    const uint32_t nulls[] = {0, 0, 0, 13, 11, 12, 3, 0, 0, 0};
    for (uint32_t f = 0; f < 10; f++) {
        char cfg[512];
        snprintf(cfg, sizeof(cfg),
                 "{\"keys\":[\"g\",\"t\"],\"partitionKeys\":1,\"function\":\"%s\",\"column\":\"x\","
                 "\"name\":\"result\"%s}",
                 functions[f],
                 f >= 5 && f != 7 ? ",\"frame\":{\"type\":\"rows\",\"preceding\":1,\"following\":1}"
                                  : "");
        ud_operator *w = NULL;
        OK(op(c, m, UD_OPERATOR_WINDOW, cfg, in, &w));
        OK(ud_operator_push(w, 0, b));
        OK(ud_operator_finish_input(w, 0));
        ud_batch *out = NULL;
        OK(ud_operator_pull(w, &out, NULL));
        CHECK(ud_batch_rows(out) == 4);
        for (uint32_t row = 0; row < 4; row++) {
            ud_value v;
            OK(ud_batch_get(out, row, 3, &v));
            CHECK(v.is_null == !!(nulls[f] & (1u << row)));
            if (!v.is_null) {
                if (v.type == UD_DECIMAL128) {
                    ud_field fld;
                    OK(ud_batch_field(out, 3, &fld));
                    char raw[40];
                    snprintf(raw, sizeof(raw), "%lld", (long long)numbers[f][row]);
                    ud_value e;
                    OK(ud_value_parse_field(c, &fld, raw, (uint32_t)strlen(raw), UD_ROUND_EXACT,
                                            &e));
                    CHECK(!ud_value_compare(&v, &e));
                } else
                    CHECK(v.integer == numbers[f][row]);
            }
        }
        ud_batch_destroy(out);
        CHECK(ud_operator_pull(w, &out, NULL) == UD_END);
        OK(ud_operator_destroy(w));
    }
    ud_batch_destroy(b);
    return 0;
}
static int cache_state(ud_context *c, ud_buffer_manager *m) {
    ud_batch *reference = NULL, *probe = NULL, *state = NULL;
    OK(input(c,
             "{\"version\":12,\"fields\":[{\"name\":\"k\",\"type\":\"int64\",\"nullable\":false},{"
             "\"name\":\"name\",\"type\":\"utf8\",\"nullable\":false}]}",
             "[{\"k\":1,\"name\":\"汉😀\"}]", &reference));
    OK(input(c,
             "{\"version\":12,\"fields\":[{\"name\":\"k\",\"type\":\"int64\",\"nullable\":false}]}",
             "[{\"k\":1},{\"k\":2},{\"k\":3}]", &probe));
    uint32_t key = 0;
    ud_cache_options o = {.struct_size = sizeof(o),
                          .abi_version = UD_ABI_VERSION,
                          .max_batch_rows = 8,
                          .keys = &key,
                          .key_count = 1,
                          .buffers = m,
                          .mode = UD_CACHE_PARTIAL,
                          .max_entries = 2,
                          .ttl_ms = 60000};
    ud_cache *a = NULL, *b = NULL;
    OK(ud_cache_create(c, reference, &o, &a));
    OK(ud_cache_create(c, reference, &o, &b));
    OK(ud_cache_partial_put(a, probe, 0, &key, reference));
    OK(ud_cache_partial_put(a, probe, 1, &key, NULL));
    OK(ud_cache_checkpoint(a, &state));
    OK(ud_cache_restore(b, state));
    ud_batch_destroy(state);
    ud_cache_reader *reader = NULL;
    OK(ud_cache_reader_create(b, probe, &key, 1, &reader));
    int32_t cached;
    OK(ud_cache_partial_probe(reader, probe, 0, &cached));
    CHECK(cached);
    const ud_batch *match = NULL;
    uint32_t row;
    OK(ud_cache_next(reader, &match, &row));
    ud_value v;
    OK(ud_batch_get(match, row, 1, &v));
    CHECK(v.text_length == 7);
    CHECK(ud_cache_next(reader, &match, &row) == UD_END);
    OK(ud_cache_partial_probe(reader, probe, 1, &cached));
    CHECK(cached && ud_cache_next(reader, &match, &row) == UD_END);
    OK(ud_cache_partial_probe(reader, probe, 2, &cached));
    CHECK(!cached);
    ud_cache_reader_destroy(reader);
    OK(ud_cache_release(a));
    OK(ud_cache_release(b));
    ud_batch_destroy(reference);
    ud_batch_destroy(probe);
    return 0;
}
static int text_and_restore(ud_context *c, ud_buffer_manager *m) {
    ud_batch *b = NULL;
    OK(input(c,
             "{\"version\":12,\"fields\":[{\"name\":\"txt\",\"type\":\"utf8\",\"nullable\":true}]}",
             "[{\"txt\":\"  Ａé汉😀  \"},{\"txt\":null}]", &b));
    const ud_batch *in[] = {b};
    ud_operator *x = NULL;
    OK(op(c, m, UD_OPERATOR_STANDARDIZE,
          "{\"columns\":[{\"column\":\"txt\",\"trim\":true,\"normalization\":\"NFKC\",\"case\":"
          "\"lower\"}]}",
          in, &x));
    OK(ud_operator_push(x, 0, b));
    ud_batch *out = NULL;
    OK(ud_operator_pull(x, &out, NULL));
    ud_value v;
    OK(ud_batch_get(out, 0, 0, &v));
    CHECK(v.text_length == 10 && !memcmp(v.text, "aé汉😀", 10));
    OK(ud_batch_get(out, 1, 0, &v));
    CHECK(v.is_null);
    ud_batch_destroy(out);
    ud_batch *checkpoint = NULL;
    OK(ud_operator_checkpoint(x, &checkpoint));
    OK(ud_operator_restore(&x, checkpoint));
    ud_batch_destroy(checkpoint);
    OK(ud_operator_finish_input(x, 0));
    CHECK(ud_operator_pull(x, &out, NULL) == UD_END);
    OK(ud_operator_destroy(x));
    char *values = NULL;
    uint32_t length;
    OK(ud_pivot_discover_values(c, b, 0, text("p"), 8, &values, &length));
    CHECK(length > 20 && strstr(values, "value"));
    ud_free(values);
    ud_batch_destroy(b);
    return 0;
}
static int multiway_identity(ud_context *c, ud_buffer_manager *m) {
    ud_batch *left=NULL,*right=NULL;
    OK(input(c,
             "{\"version\":12,\"fields\":[{\"name\":\"k\",\"type\":\"int64\",\"nullable\":false},{"
             "\"name\":\"v\",\"type\":\"int64\",\"nullable\":false}]}",
             "[{\"k\":1,\"v\":0}]", &left));
    OK(input(c,
             "{\"version\":12,\"fields\":[{\"name\":\"k\",\"type\":\"int64\",\"nullable\":false},{"
             "\"name\":\"v\",\"type\":\"int64\",\"nullable\":false}]}",
             "[{\"k\":1,\"v\":1},{\"k\":1,\"v\":2}]", &right));
    const char *cfg="{\"inputs\":[\"a\",\"b\",\"c\"],\"joins\":[{\"input\":\"b\",\"leftKeys\":[\"k\"],\"rightKeys\":[\"k\"],\"rightAliases\":{\"k\":\"bk\",\"v\":\"bv\"}},{\"input\":\"c\",\"leftKeys\":[\"k\"],\"rightKeys\":[\"k\"],\"rightAliases\":{\"k\":\"ck\",\"v\":\"cv\"}}]}";
    const ud_batch *inputs[]={left,right,right};ud_id expected[4]={{0}},field_ids[6]={{0}};
    for(uint32_t reversed=0;reversed<2;reversed++) {
        ud_operator_options o={.struct_size=sizeof(o),.abi_version=UD_ABI_VERSION,.kind=UD_OPERATOR_MULTIWAY_JOIN,.max_batch_rows=8,.input_count=3,.configuration=text(cfg),.buffers=m,.memory_budget=1048576,.spill_budget=16777216};
        o.estimated_rows[1]=reversed?100:1;o.estimated_rows[2]=reversed?1:100;
        ud_operator *join=NULL;OK(ud_operator_create(c,&o,inputs,&join));ud_batch *schema=NULL;OK(ud_operator_schema(join,0,&schema));
        CHECK(ud_batch_columns(schema)==6);
        for(uint32_t j=0;j<6;j++){ud_field f;OK(ud_batch_field(schema,j,&f));if(reversed)CHECK(!ud_id_compare(field_ids[j],f.identity));else field_ids[j]=f.identity;}
        ud_batch_destroy(schema);char *explain=NULL;uint32_t size;
        OK(ud_operator_explain(join,&explain,&size));CHECK(strstr(explain,reversed?"\"rightInput\":2":"\"rightInput\":1"));ud_free(explain);
        for(uint32_t port=0;port<3;port++){OK(ud_operator_push(join,port,inputs[port]));OK(ud_operator_finish_input(join,port));}
        uint32_t count=0;for(;;){ud_batch*out=NULL;ud_status status=ud_operator_pull(join,&out,NULL);if(status==UD_END)break;OK(status);
            for(uint32_t row=0;row<ud_batch_rows(out);row++){ud_value b,cval;ud_row_info info;OK(ud_batch_get(out,row,3,&b));OK(ud_batch_get(out,row,5,&cval));OK(ud_batch_row_info(out,row,&info));uint32_t index=(uint32_t)((b.integer-1)*2+cval.integer-1);CHECK(index<4);
                if(reversed)CHECK(!ud_id_compare(expected[index],info.id));else expected[index]=info.id;count++;
            }
            uint8_t *wire=NULL;uint64_t bytes;ud_batch *restored=NULL;OK(ud_batch_serialize(out,&wire,&bytes));OK(ud_batch_deserialize(c,wire,bytes,&restored));ud_row_info x,y;OK(ud_batch_row_info(out,0,&x));OK(ud_batch_row_info(restored,0,&y));CHECK(!ud_id_compare(x.origins,y.origins)&&x.flags==y.flags);ud_free(wire);ud_batch_destroy(restored);ud_batch_destroy(out);
        }CHECK(count==4);OK(ud_operator_destroy(join));
    }
    for(uint32_t i=0;i<4;i++)for(uint32_t j=0;j<i;j++)CHECK(ud_id_compare(expected[i],expected[j]));
    ud_batch_destroy(left);ud_batch_destroy(right);return 0;
}
int main(void) {
    ud_context *c = NULL;
    OK(ud_context_create(&(ud_context_options){.struct_size = sizeof(ud_context_options),
                                               .abi_version = UD_ABI_VERSION,
                                               .memory_limit = 268435456},
                         &c));
    ud_buffer_manager *m = NULL;
    OK(ud_buffer_manager_create(c,
                                &(ud_buffer_options){.struct_size = sizeof(ud_buffer_options),
                                                     .abi_version = UD_ABI_VERSION,
                                                     .max_blocks = 4096,
                                                     .resident_limit = 67108864,
                                                     .working_limit = 67108864},
                                &m));
    CHECK(!slice_roundtrip(c));
    CHECK(!join_matrix(c, m));
    CHECK(!windows(c, m));
    CHECK(!cache_state(c, m));
    CHECK(!text_and_restore(c, m));
    CHECK(!multiway_identity(c, m));
    OK(ud_buffer_manager_destroy(m));
    CHECK(ud_memory_used(c) == 0);
    OK(ud_context_destroy(c));
    return 0;
}
