#include "ud/data.h"
#include "../components/data/internal.h"
#include "check.h"
#include <string.h>

static int decimal_cases(void) {
    ud_decimal128 x, y, z;
    const char *max = "99999999999999999999999999999999999999";
    OK(ud_decimal_parse(max, 38, 38, 0, UD_ROUND_EXACT, &x));
    OK(ud_decimal_parse("1", 1, 38, 0, UD_ROUND_EXACT, &y));
    CHECK(ud_decimal_add(x, y, 38, &z) == UD_OVERFLOW);
    CHECK(ud_decimal_parse("999.95", 6, 4, 1, UD_ROUND_HALF_EVEN, &z) == UD_OVERFLOW);
    OK(ud_decimal_parse("2.345", 5, 5, 2, UD_ROUND_HALF_EVEN, &x));
    OK(ud_decimal_parse("2.34", 4, 5, 2, UD_ROUND_EXACT, &y));
    CHECK(ud_decimal_compare(x, y) == 0);
    OK(ud_decimal_parse("2.355", 5, 5, 2, UD_ROUND_HALF_EVEN, &x));
    OK(ud_decimal_parse("2.36", 4, 5, 2, UD_ROUND_EXACT, &y));
    CHECK(ud_decimal_compare(x, y) == 0);
    OK(ud_decimal_parse("-0.001", 6, 5, 2, UD_ROUND_FLOOR, &x));
    OK(ud_decimal_parse("-0.01", 5, 5, 2, UD_ROUND_EXACT, &y));
    CHECK(ud_decimal_compare(x, y) == 0);
    OK(ud_decimal_parse("0.001", 5, 5, 2, UD_ROUND_CEILING, &x));
    CHECK(x.low == 1 && x.high == 0);
    CHECK(ud_decimal_parse("1.001", 5, 5, 2, UD_ROUND_EXACT, &x) == UD_PARSE);
    OK(ud_decimal_parse("1200", 4, 2, -2, UD_ROUND_EXACT, &x));
    CHECK(x.low == 12 && x.high == 0);
    OK(ud_decimal_rescale(x, -2, 6, 2, UD_ROUND_EXACT, &z));
    CHECK(z.low == 120000 && z.high == 0);
    OK(ud_decimal_parse("18446744073709551616", 20, 38, 0, UD_ROUND_EXACT, &x));
    CHECK(x.low == 0 && x.high == 1);
    OK(ud_decimal_parse("-18446744073709551616", 21, 38, 0, UD_ROUND_EXACT, &y));
    CHECK(y.low == 0 && y.high == -1);
    OK(ud_decimal_add(x, y, 38, &z));
    CHECK(z.low == 0 && z.high == 0);
    CHECK(ud_decimal_parse("NaN", 3, 38, 0, UD_ROUND_ZERO, &x) == UD_PARSE);
    CHECK(ud_decimal_parse("1e999999", 8, 38, 0, UD_ROUND_ZERO, &x) == UD_OVERFLOW);
    return 0;
}
int main(void) {
    CHECK(decimal_cases() == 0);
    ud_context *ctx = NULL; OK(ud_context_create(NULL, &ctx));
    ud_value parsed;
    OK(ud_value_parse(ctx, UD_INT64, "-9223372036854775808", 20, &parsed));CHECK(parsed.integer==INT64_MIN);
    CHECK(ud_value_parse(ctx, UD_INT64, "9223372036854775808", 19, &parsed)==UD_OVERFLOW);
    CHECK(ud_value_parse(ctx, UD_BOOL, "True", 4, &parsed)==UD_PARSE);
    CHECK(ud_value_parse(ctx, UD_UTF8, "\xc0\x80", 2, &parsed)==UD_PARSE);
    OK(ud_value_parse(ctx, UD_FLOAT64, "1.25e2", 6, &parsed));CHECK(parsed.real==125.0);
    OK(ud_value_parse(ctx, UD_FLOAT64, "5e-324", 6, &parsed));CHECK(parsed.real>0 && parsed.real<1e-308);
    CHECK(ud_value_parse(ctx,UD_FLOAT64,"1e-9999",7,&parsed)==UD_OVERFLOW);
    char long_number[1020];long_number[0]='1';memset(long_number+1,'0',1000);memcpy(long_number+1001,"e-1000",6);
    OK(ud_value_parse(ctx,UD_FLOAT64,long_number,1007,&parsed));CHECK(parsed.real==1.0);
    ud_field integer_field={.name="n",.name_length=1,.type=UD_INT64},float_field={.name="n",.name_length=1,.type=UD_FLOAT64};
    ud_value input_integer={.type=UD_INT64,.integer=INT64_MAX},converted={0};
    CHECK(ud_cast_value(ctx,&integer_field,&input_integer,&float_field,UD_ROUND_EXACT,&converted,0)==UD_PARSE);
    OK(ud_cast_value(ctx,&integer_field,&input_integer,&float_field,UD_ROUND_HALF_EVEN,&converted,0));
    CHECK(converted.real==9223372036854775808.0);
    CHECK(ud_cast_value(ctx,&float_field,&converted,&integer_field,UD_ROUND_ZERO,&parsed,0)==UD_OVERFLOW);
    input_integer.integer=INT64_MIN;OK(ud_cast_value(ctx,&integer_field,&input_integer,&float_field,UD_ROUND_EXACT,&converted,0));
    OK(ud_cast_value(ctx,&float_field,&converted,&integer_field,UD_ROUND_EXACT,&parsed,0));CHECK(parsed.integer==INT64_MIN);
    converted=(ud_value){.type=UD_FLOAT64,.real=-2.5};
    OK(ud_cast_value(ctx,&float_field,&converted,&integer_field,UD_ROUND_HALF_EVEN,&parsed,0));CHECK(parsed.integer==-2);
    OK(ud_cast_value(ctx,&float_field,&converted,&integer_field,UD_ROUND_HALF_AWAY,&parsed,0));CHECK(parsed.integer==-3);
    ud_field fields[] = {
        {.name="money", .name_length=5, .type=UD_DECIMAL128, .nullable=1, .precision=38, .scale=4},
        {.name="date", .name_length=4, .type=UD_DATE32},
        {.name="time", .name_length=4, .type=UD_TIME64},
        {.name="point", .name_length=5, .type=UD_TIMESTAMP, .timezone="Asia/Shanghai", .timezone_length=13},
        {.name="payload", .name_length=7, .type=UD_BINARY, .nullable=1},
        {.name="text", .name_length=4, .type=UD_UTF8, .nullable=1}
    };
    const char binary[] = {'\0', '\xff', '\xc0', '\x80'};
    const char *text[] = {"-123456789012345678901234567890.1234", "1969-12-31", "23:59:59.999999",
        "1970-01-01T08:00:00+08:00", binary, "中文"};
    uint32_t length[] = {35, 10, 15, 25, 4, 6};
    ud_value values[6];
    /* The decimal fixture's explicit length is calculated here to catch edits. */
    length[0] = (uint32_t)strlen(text[0]);
    for (uint32_t i = 0; i < 6; i++) OK(ud_value_parse_field(ctx, fields + i, text[i], length[i], UD_ROUND_EXACT, values + i));
    CHECK(values[1].integer == -1);
    CHECK(values[2].integer == INT64_C(86399999999));
    CHECK(values[3].integer == 0);
    CHECK(ud_value_parse_field(ctx, fields + 3, "1970-01-01T00:00:00", 19, UD_ROUND_EXACT, values + 3) == UD_PARSE);
    OK(ud_value_parse_field(ctx, fields + 3, "1969-12-31T23:59:59.999999Z", 27, UD_ROUND_EXACT, values + 3));
    CHECK(values[3].integer == -1);
    CHECK(ud_value_parse_field(ctx, fields + 1, "1900-02-29", 10, UD_ROUND_EXACT, values + 1) == UD_PARSE);
    OK(ud_value_parse_field(ctx, fields + 1, "2000-02-29", 10, UD_ROUND_EXACT, values + 1));
    ud_batch *batch = NULL; OK(ud_batch_create(ctx, fields, 6, 3, &batch));
    OK(ud_batch_append(batch, values, 6));
    ud_value borrowed[6];
    for (uint32_t i = 0; i < 6; i++) OK(ud_batch_get(batch, 0, i, borrowed + i));
    /* Same-batch aliased Binary / UTF8 values and quota-safe ownership. */
    OK(ud_batch_append(batch, borrowed, 6));
    values[0].is_null = 1; values[4].is_null = 1; values[5].is_null = 1;
    OK(ud_batch_append(batch, values, 6));
    struct ArrowArray array; struct ArrowSchema schema;
    OK(ud_batch_export_arrow(batch, &array, &schema));
    CHECK(strcmp(schema.children[0]->format, "d:38,4") == 0);
    CHECK(strcmp(schema.children[1]->format, "tdD") == 0);
    CHECK(strcmp(schema.children[2]->format, "ttu") == 0);
    CHECK(strcmp(schema.children[3]->format, "tsu:Asia/Shanghai") == 0);
    CHECK(strcmp(schema.children[4]->format, "z") == 0);
    CHECK(array.children[0]->n_buffers == 2 && array.children[4]->n_buffers == 3);
    /* Export survives producer destruction; sliced root retains both offsets. */
    ud_batch_destroy(batch); batch = NULL;
    array.offset = 1; array.length = 2;
    OK(ud_batch_import_arrow(ctx, &array, &schema, &batch));
    CHECK(ud_batch_rows(batch) == 2);
    ud_value value; OK(ud_batch_get(batch, 0, 4, &value));
    CHECK(value.text_length == sizeof(binary) && !memcmp(value.text, binary, sizeof(binary)));
    OK(ud_batch_get(batch, 1, 4, &value)); CHECK(value.is_null);
    array.release(&array); schema.release(&schema);
    ud_bytes wire = {.ctx=ctx}; OK(ud_encode_batch(&wire, batch));
    ud_batch *decoded = NULL; OK(ud_decode_batch(&wire, &decoded));
    CHECK(wire.pos == wire.size && ud_batch_rows(decoded) == 2);
    OK(ud_batch_get(decoded, 0, 0, &value));
    char formatted[160]; uint32_t required = 0;
    OK(ud_value_format(&value, formatted, sizeof(formatted), &required));
    CHECK(strcmp(formatted, text[0]) == 0);
    CHECK(ud_value_format(&value, NULL, 0, &required) == UD_RESOURCE);
    ud_field targets[6]; memcpy(targets,fields,sizeof(targets)); targets[0].scale=2;
    ud_batch *cast=NULL;
    CHECK(ud_batch_cast(ctx,decoded,targets,6,UD_ROUND_EXACT,&cast)==UD_PARSE && !cast);
    OK(ud_batch_cast(ctx,decoded,targets,6,UD_ROUND_HALF_EVEN,&cast));
    OK(ud_batch_get(cast,0,0,&value));
    OK(ud_value_format(&value,formatted,sizeof(formatted),&required));
    CHECK(!strcmp(formatted,"-123456789012345678901234567890.12"));
    ud_batch_destroy(cast);
    OK(ud_batch_get(decoded, 0, 3, &value));
    OK(ud_value_format(&value, formatted, sizeof(formatted), &required));
    CHECK(strcmp(formatted, "1969-12-31T23:59:59.999999Z") == 0);
    ud_batch_destroy(decoded); ud_batch_destroy(batch);
    wire.data[0] = 1; wire.pos = 0;
    CHECK(ud_decode_batch(&wire, &decoded) == UD_INCOMPATIBLE && !decoded);
    ud_bytes_clear(&wire);
    CHECK(ud_memory_used(ctx) == 0); OK(ud_context_destroy(ctx));
    return 0;
}
