#include "ud/quality.h"
#include "check.h"
#include <string.h>
int main(void) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_field fields[] = {{.name = "id", .name_length = 2, .type = UD_INT64, .nullable = 1},
                         {.name = "score",
                          .name_length = 5,
                          .type = UD_DECIMAL128,
                          .nullable = 1,
                          .precision = 38,
                          .scale = 2},
                         {.name = "name", .name_length = 4, .type = UD_UTF8, .nullable = 1}};
    ud_batch *input = NULL;
    OK(ud_batch_create(ctx, fields, 3, 7, &input));
    int64_t ids[] = {1, 1, 2, 3, 4, 0, 5};
    const char *score[] = {"10.00", "10.00", "101.00", "10.00", "50.00", "10.00", "10.00"};
    for (uint32_t r = 0; r < 7; r++) {
        ud_value values[3] = {{.type = UD_INT64, .integer = ids[r]},
                              {0},
                              {.type = UD_UTF8, .text = "中文", .text_length = 6}};
        values[0].is_null = r == 5;
        values[2].is_null = r == 3;
        OK(ud_value_parse_field(ctx, fields + 1, score[r], (uint32_t)strlen(score[r]),
                                UD_ROUND_EXACT, values + 1));
        OK(ud_batch_append(input, values, 3));
    }
    ud_batch *reference = NULL;
    OK(ud_batch_create(ctx, fields, 1, 2, &reference));
    ud_value ref = {.type = UD_INT64, .integer = 1};
    OK(ud_batch_append(reference, &ref, 1));
    ref.integer = 4;
    OK(ud_batch_append(reference, &ref, 1));
    uint32_t primary = 0, amount = 1, name = 2;
    ud_quality_rule rules[] = {{.struct_size = sizeof(ud_quality_rule),
                                .abi_version = UD_ABI_VERSION,
                                .rule_version = 1,
                                .policy = UD_ERROR_INHERIT,
                                .id = "pk",
                                .id_length = 2,
                                .kind = UD_RULE_PRIMARY_KEY,
                                .columns = &primary,
                                .column_count = 1},
                               {.struct_size = sizeof(ud_quality_rule),
                                .abi_version = UD_ABI_VERSION,
                                .rule_version = 1,
                                .policy = UD_ERROR_INHERIT,
                                .id = "range",
                                .id_length = 5,
                                .kind = UD_RULE_RANGE,
                                .columns = &amount,
                                .column_count = 1},
                               {.struct_size = sizeof(ud_quality_rule),
                                .abi_version = UD_ABI_VERSION,
                                .rule_version = 1,
                                .policy = UD_ERROR_INHERIT,
                                .id = "name",
                                .id_length = 4,
                                .kind = UD_RULE_NOT_NULL,
                                .columns = &name,
                                .column_count = 1},
                               {.struct_size = sizeof(ud_quality_rule),
                                .abi_version = UD_ABI_VERSION,
                                .rule_version = 1,
                                .policy = UD_ERROR_INHERIT,
                                .id = "fk",
                                .id_length = 2,
                                .kind = UD_RULE_FOREIGN_KEY,
                                .columns = &primary,
                                .column_count = 1,
                                .reference = reference,
                                .reference_columns = &primary},
                               {.struct_size = sizeof(ud_quality_rule),
                                .abi_version = UD_ABI_VERSION,
                                .rule_version = 1,
                                .policy = UD_ERROR_INHERIT,
                                .id = "reconcile",
                                .id_length = 9,
                                .kind = UD_RULE_ROW_COUNT,
                                .expected_rows = 2}};
    OK(ud_value_parse_field(ctx, fields + 1, "0.00", 4, UD_ROUND_EXACT, &rules[1].minimum));
    OK(ud_value_parse_field(ctx, fields + 1, "100.00", 6, UD_ROUND_EXACT, &rules[1].maximum));
    rules[4].policy = UD_ERROR_FAIL;
    ud_buffer_manager *buffers = NULL;
    ud_buffer_options bo = {.struct_size = sizeof(bo),
                            .abi_version = UD_ABI_VERSION,
                            .max_blocks = 100,
                            .resident_limit = 1000000,
                            .working_limit = 1000000};
    OK(ud_buffer_manager_create(ctx, &bo, &buffers));
    ud_quality_options qo = {.struct_size = sizeof(qo),
                             .abi_version = UD_ABI_VERSION,
                             .max_batch_rows = 32,
                             .max_violations = 100,
                             .policy = UD_ERROR_REDIRECT,
                             .buffers = buffers};
    ud_quality *q = NULL;
    ud_batch *accepted = NULL;
    ud_quality_metrics metrics;
    ud_batch *rejected = NULL;
    ud_quality_violation *violations = NULL;
    uint32_t count = 0;
    OK(ud_quality_create(ctx, fields, 3, rules, 5, &qo, &q));
    OK(ud_quality_reference_end(q));
    ud_batch_destroy(reference);
    reference = NULL;
    OK(ud_quality_apply(q, input, &accepted, &rejected, &violations, &count));
    CHECK(ud_batch_rows(accepted) == 2 && ud_batch_rows(rejected) == 5 && count == 7);
    CHECK(violations[0].input_row == 1 && violations[0].rejected_row == 0 &&
          violations[0].rule_index == 0);
    ud_quality_get_metrics(q, &metrics);
    CHECK(metrics.processed == 7 && metrics.accepted == 2 && metrics.rejected == 5);
    OK(ud_quality_finish(q));
    CHECK(ud_quality_apply(q, input, &reference, NULL, NULL, NULL) == UD_INVALID);
    ud_batch_destroy(accepted);
    ud_batch_destroy(rejected);
    ud_free(violations);
    ud_quality_destroy(q);
    /* Classification performs no target writes; Pipeline recovery/receipt gates
     * prove acknowledgment after both normal and error destinations commit. */
    ud_quality_rule regex = {.struct_size = sizeof(regex),
                             .abi_version = UD_ABI_VERSION,
                             .rule_version = 1,
                             .policy = UD_ERROR_INHERIT,
                             .id = "regex",
                             .id_length = 5,
                             .kind = UD_RULE_REGEX,
                             .columns = &name,
                             .column_count = 1,
                             .pattern = ".*",
                             .pattern_length = 2};
    qo.policy = UD_ERROR_FAIL;
    OK(ud_quality_create(ctx, fields, 3, &regex, 1, &qo, &q));
    OK(ud_quality_apply(q, input, &accepted, NULL, NULL, NULL));
    CHECK(ud_batch_rows(accepted) == ud_batch_rows(input));
    ud_batch_destroy(accepted);
    ud_quality_destroy(q);
    OK(ud_buffer_manager_destroy(buffers));
    ud_batch_destroy(input);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
