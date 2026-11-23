#include "check.h"
#include "ud/compute.h"
#include "ud/operator.h"
#include "ud/buffer.h"
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int append_row(ud_batch *batch, int64_t number) {
    char key[32];
    int length = snprintf(key, sizeof(key), "order-%lld", (long long)number);
    if (length <= 0 || (size_t)length >= sizeof(key))
        return 1;
    ud_value values[] = {
        {.type = UD_INT64, .integer = number},
        {.type = UD_UTF8, .text = key, .text_length = (uint32_t)length},
    };
    return ud_batch_append(batch, values, 2) == UD_OK ? 0 : 1;
}

int main(void) {
    const uint32_t rows = 180000;
    ud_context *context = NULL;
    OK(ud_context_create(NULL, &context));

    ud_field schema[] = {{.name = "order_id", .name_length = 8, .type = UD_INT64},
                         {.name = "order_key", .name_length = 9, .type = UD_UTF8}};
    const char *definition =
        "{\"version\":" UD_PROTOCOL_VERSION_TEXT
        ",\"group\":[],\"aggregates\":[{\"function\":\"count\","
        "\"column\":\"order_id\",\"name\":\"orders\",\"distinct\":true,"
        "\"spill\":true,\"maxDistinct\":120000},{\"function\":\"sum\","
        "\"column\":\"order_id\",\"name\":\"total\",\"distinct\":true,"
        "\"spill\":true,\"maxDistinct\":120000},{\"function\":\"count\","
        "\"column\":\"order_key\",\"name\":\"text_orders\",\"distinct\":true,"
        "\"spill\":true,\"maxDistinct\":120000}]}";
    ud_plan *plan = NULL;
    OK(ud_plan_create_fields(context, UD_PLAN_VERSION, schema, 2,
                              (ud_string){definition, (uint32_t)strlen(definition)}, NULL, &plan));
    const char *generic_over_budget =
        "{\"version\":" UD_PROTOCOL_VERSION_TEXT
        ",\"group\":[],\"aggregates\":[{\"function\":\"count\","
        "\"column\":\"order_id\",\"name\":\"orders\",\"distinct\":true,"
        "\"maxDistinct\":1000001}]}";
    ud_plan *rejected = NULL;
    CHECK(ud_plan_create_fields(context, UD_PLAN_VERSION, schema, 2,
                                (ud_string){generic_over_budget,
                                            (uint32_t)strlen(generic_over_budget)},
                                NULL, &rejected) == UD_SCHEMA);
    ud_plan_destroy(rejected);
    const char *spill_over_budget =
        "{\"version\":" UD_PROTOCOL_VERSION_TEXT
        ",\"group\":[],\"aggregates\":[{\"function\":\"count\","
        "\"column\":\"order_id\",\"name\":\"orders\",\"distinct\":true,"
        "\"spill\":true,\"maxDistinct\":30000001}]}";
    rejected = NULL;
    CHECK(ud_plan_create_fields(context, UD_PLAN_VERSION, schema, 2,
                                (ud_string){spill_over_budget,
                                            (uint32_t)strlen(spill_over_budget)},
                                NULL, &rejected) == UD_SCHEMA);
    ud_plan_destroy(rejected);
    const ud_field *fields = NULL;
    uint32_t field_count = 0;
    OK(ud_plan_schema(plan, &fields, &field_count));

    const uint32_t batch_rows = rows / 3;
    ud_batch *first = NULL, *second = NULL, *third = NULL;
    OK(ud_batch_create(context, fields, field_count, batch_rows, &first));
    OK(ud_batch_create(context, fields, field_count, batch_rows, &second));
    OK(ud_batch_create(context, fields, field_count, batch_rows, &third));
    for (uint32_t i = 0; i < batch_rows; i++) {
        CHECK(append_row(first, i) == 0);
        CHECK(append_row(second, i + 30000) == 0);
        CHECK(append_row(third, i + 60000) == 0);
    }
    OK(ud_batch_seal(first));
    OK(ud_batch_seal(second));
    OK(ud_batch_seal(third));

    ud_buffer_manager *buffers = NULL;
    OK(ud_buffer_manager_create(
        context,
        &(ud_buffer_options){.struct_size = sizeof(ud_buffer_options),
                             .abi_version = UD_ABI_VERSION,
                             .max_blocks = 128,
                             .resident_limit = 32 * 1024 * 1024,
                             .working_limit = 1024 * 1024,
                             .spill_limit = 128 * 1024 * 1024},
        &buffers));
    const ud_batch *inputs[] = {first};
    ud_operator *aggregate = NULL;
    OK(ud_operator_create(
        context,
        &(ud_operator_options){.struct_size = sizeof(ud_operator_options),
                               .abi_version = UD_ABI_VERSION,
                               .kind = UD_OPERATOR_AGGREGATE,
                               .max_batch_rows = 65536,
                               .aggregation = plan,
                               .buffers = buffers},
        inputs, &aggregate));
    OK(ud_operator_push(aggregate, 0, first));
    OK(ud_operator_push(aggregate, 0, second));
    OK(ud_operator_push(aggregate, 0, third));
    OK(ud_operator_finish_input(aggregate, 0));
    ud_batch *result = NULL;
    ud_status pull_status = ud_operator_pull(aggregate, &result, NULL);
    if (pull_status != UD_OK)
        fprintf(stderr, "aggregate pull status=%d error=%s\n", pull_status, ud_error(context));
    OK(pull_status);
    CHECK(result && ud_batch_rows(result) == 1 && ud_batch_columns(result) == 3);
    ud_value actual;
    OK(ud_batch_get(result, 0, 0, &actual));
    CHECK(actual.type == UD_INT64 && actual.integer == 120000);
    OK(ud_batch_get(result, 0, 1, &actual));
    CHECK(actual.type == UD_INT64 && actual.integer == INT64_C(7199940000));
    OK(ud_batch_get(result, 0, 2, &actual));
    CHECK(actual.type == UD_INT64 && actual.integer == 120000);
    ud_batch_destroy(result);
    CHECK(ud_operator_pull(aggregate, &result, NULL) == UD_END);
    ud_buffer_metrics metrics;
    OK(ud_buffer_manager_metrics(buffers, &metrics));
    /* The aggregate owns the temporary partition files and releases their
     * active charge when its final group is destroyed. Total/high-water
     * metrics remain as the durable evidence that the exact spill path ran. */
    CHECK(metrics.total_spill_bytes > 0 && metrics.spill_high_water > 0 &&
          metrics.spill_bytes == 0);
    OK(ud_operator_destroy(aggregate));
    OK(ud_buffer_manager_destroy(buffers));
    ud_batch_destroy(first);
    ud_batch_destroy(second);
    ud_batch_destroy(third);
    ud_plan_destroy(plan);
    CHECK(ud_memory_used(context) == 0);
    OK(ud_context_destroy(context));
    puts("metric exact distinct spill: 120000 values with cross-batch duplicates passed");
    return 0;
}
