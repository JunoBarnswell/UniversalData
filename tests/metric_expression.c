#include "check.h"
#include "ud/expression.h"
#include <stdio.h>
#include <string.h>

static ud_status expression(ud_context *context, const char *json, const ud_batch *schema,
                            ud_expression **out) {
    ud_expression_options options = {sizeof(options), UD_ABI_VERSION, schema, NULL, NULL};
    return ud_expression_create(context,
                                (ud_string){json, (uint32_t)strlen(json)}, &options, out);
}

int main(void) {
    ud_context *context = NULL;
    OK(ud_context_create(NULL, &context));
    ud_field fields[] = {
        {.name = "difference", .name_length = 10, .type = UD_DECIMAL128, .nullable = 1,
         .precision = 30, .scale = 2},
        {.name = "unique_orders", .name_length = 13, .type = UD_INT64},
        {.name = "lines", .name_length = 5, .type = UD_INT64},
        {.name = "zero", .name_length = 4, .type = UD_INT64},
    };
    ud_batch *schema = NULL;
    OK(ud_batch_create(context, fields, 4, 1, &schema));
    ud_value values[4] = {
        {.type = UD_DECIMAL128, .precision = 30, .scale = 2},
        {.type = UD_INT64, .integer = 1},
        {.type = UD_INT64, .integer = 2},
        {.type = UD_INT64, .integer = 0},
    };
    OK(ud_value_parse_field(context, fields, "150.00", 6, UD_ROUND_EXACT, values));
    OK(ud_batch_append(schema, values, 4));
    OK(ud_batch_seal(schema));

    const char *ratio =
        "{\"op\":\"conditional\",\"args\":[{\"op\":\"eq\",\"args\":["
        "{\"cast\":{\"name\":\"decimal128\",\"type\":\"decimal128\",\"nullable\":true,"
        "\"precision\":19,\"scale\":0},\"value\":{\"column\":\"unique_orders\"}},"
        "{\"literal\":\"0\",\"type\":\"decimal128\",\"precision\":19,\"scale\":0}]},"
        "{\"literal\":null,\"type\":\"decimal128\",\"precision\":38,\"scale\":6},"
        "{\"op\":\"div\",\"args\":["
        "{\"column\":\"difference\"},"
        "{\"cast\":{\"name\":\"decimal128\",\"type\":\"decimal128\",\"nullable\":true,"
        "\"precision\":19,\"scale\":0},\"value\":{\"column\":\"unique_orders\"}}]}]}";
    ud_expression *expression_value = NULL;
    ud_status ratio_status = expression(context, ratio, schema, &expression_value);
    if (ratio_status != UD_OK)
        fprintf(stderr, "ratio status=%d error=%s\n", ratio_status, ud_error(context));
    CHECK(ratio_status == UD_OK);
    ud_value result;
    OK(ud_expression_evaluate(expression_value, schema, 0, &result));
    CHECK(result.type == UD_DECIMAL128 && !result.is_null && result.precision == 38 &&
          result.scale == 6);
    ud_field expected_field = {.type = UD_DECIMAL128, .precision = 38, .scale = 6};
    ud_value expected;
    OK(ud_value_parse_field(context, &expected_field, "150", 3, UD_ROUND_EXACT, &expected));
    CHECK(ud_value_compare(&result, &expected) == 0);
    ud_expression_destroy(expression_value);

    const char *undefined =
        "{\"op\":\"conditional\",\"args\":[{\"op\":\"eq\",\"args\":["
        "{\"cast\":{\"name\":\"decimal128\",\"type\":\"decimal128\",\"nullable\":true,"
        "\"precision\":19,\"scale\":0},\"value\":{\"column\":\"zero\"}},"
        "{\"literal\":\"0\",\"type\":\"decimal128\",\"precision\":19,\"scale\":0}]},"
        "{\"literal\":null,\"type\":\"decimal128\",\"precision\":38,\"scale\":6},"
        "{\"op\":\"div\",\"args\":["
        "{\"cast\":{\"name\":\"decimal128\",\"type\":\"decimal128\",\"nullable\":true,"
        "\"precision\":19,\"scale\":0},\"value\":{\"column\":\"lines\"}},"
        "{\"cast\":{\"name\":\"decimal128\",\"type\":\"decimal128\",\"nullable\":true,"
        "\"precision\":19,\"scale\":0},\"value\":{\"column\":\"zero\"}}]}]}";
    ud_status undefined_status = expression(context, undefined, schema, &expression_value);
    if (undefined_status != UD_OK)
        fprintf(stderr, "undefined status=%d error=%s\n", undefined_status, ud_error(context));
    CHECK(undefined_status == UD_OK);
    OK(ud_expression_evaluate(expression_value, schema, 0, &result));
    CHECK(result.type == UD_DECIMAL128 && result.is_null);
    ud_expression_destroy(expression_value);
    ud_batch_destroy(schema);
    CHECK(ud_memory_used(context) == 0);
    OK(ud_context_destroy(context));
    puts("metric decimal derives: exact division and zero denominator passed");
    return 0;
}
