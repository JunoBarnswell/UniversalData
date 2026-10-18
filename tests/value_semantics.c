#include "ud/compute.h"
#include "ud/operator.h"
#include "ud/format.h"
#include "ud/expression.h"
#include "ud/quality.h"
#include "check.h"
#include <math.h>
#include <string.h>

static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static double nan_bits(uint64_t bits) {
    double value;
    memcpy(&value, &bits, sizeof(value));
    return value;
}
static int same_batch(const ud_batch *a, const ud_batch *b) {
    CHECK(ud_batch_rows(a) == ud_batch_rows(b) && ud_batch_columns(a) == ud_batch_columns(b));
    for (uint32_t row = 0; row < ud_batch_rows(a); row++) {
        uint64_t x = UD_VALUE_HASH_SEED, y = x;
        for (uint32_t col = 0; col < ud_batch_columns(a); col++) {
            ud_value left, right;
            OK(ud_batch_get(a, row, col, &left));
            OK(ud_batch_get(b, row, col, &right));
            CHECK(!ud_value_compare(&left, &right));
            x = ud_value_hash(&left, x);
            y = ud_value_hash(&right, y);
            CHECK(x == y);
        }
    }
    return 0;
}
static int roundtrips(ud_context *ctx) {
    ud_field children[] = {{.name = "n", .name_length = 1, .type = UD_FLOAT64, .nullable = 1},
                           {.name = "s", .name_length = 1, .type = UD_UTF8, .nullable = 1}};
    ud_field item = {.name = "item", .name_length = 4, .type = UD_FLOAT64, .nullable = 1};
    ud_field entries[] = {{.name = "key", .name_length = 3, .type = UD_UTF8},
                          {.name = "value", .name_length = 5, .type = UD_FLOAT64, .nullable = 1}};
    ud_field entry = {.name = "entry",
                      .name_length = 5,
                      .type = UD_STRUCT,
                      .children = entries,
                      .child_count = 2};
    ud_field fields[] = {{.name = "b", .name_length = 1, .type = UD_BOOL, .nullable = 1},
                         {.name = "i", .name_length = 1, .type = UD_INT64, .nullable = 1},
                         {.name = "f", .name_length = 1, .type = UD_FLOAT64, .nullable = 1},
                         {.name = "s", .name_length = 1, .type = UD_UTF8, .nullable = 1},
                         {.name = "d",
                          .name_length = 1,
                          .type = UD_DECIMAL128,
                          .nullable = 1,
                          .precision = 38,
                          .scale = 2},
                         {.name = "date", .name_length = 4, .type = UD_DATE32, .nullable = 1},
                         {.name = "time", .name_length = 4, .type = UD_TIME64, .nullable = 1},
                         {.name = "at",
                          .name_length = 2,
                          .type = UD_TIMESTAMP,
                          .nullable = 1,
                          .timezone = "UTC",
                          .timezone_length = 3},
                         {.name = "raw", .name_length = 3, .type = UD_BINARY, .nullable = 1},
                         {.name = "object",
                          .name_length = 6,
                          .type = UD_STRUCT,
                          .nullable = 1,
                          .children = children,
                          .child_count = 2},
                         {.name = "list",
                          .name_length = 4,
                          .type = UD_LIST,
                          .nullable = 1,
                          .children = &item,
                          .child_count = 1},
                         {.name = "map",
                          .name_length = 3,
                          .type = UD_MAP,
                          .nullable = 1,
                          .children = &entry,
                          .child_count = 1}};
    ud_value nested[] = {{.type = UD_FLOAT64, .real = -0.0},
                         {.type = UD_UTF8, .text = "中文\0", .text_length = 7}};
    ud_value items[] = {{.type = UD_FLOAT64, .real = INFINITY},
                        {.type = UD_FLOAT64, .is_null = 1},
                        {.type = UD_FLOAT64, .real = nan_bits(UINT64_C(0xfff8000000000042))}};
    ud_value pair[] = {{.type = UD_UTF8, .text = "a\0b", .text_length = 3},
                       {.type = UD_FLOAT64, .real = -INFINITY}};
    ud_value map = {.type = UD_STRUCT, .children = pair, .child_count = 2};
    ud_value values[] = {{.type = UD_BOOL, .integer = 1},
                         {.type = UD_INT64, .integer = INT64_MIN},
                         {.type = UD_FLOAT64, .real = nan_bits(UINT64_C(0x7ff8000000000001))},
                         {.type = UD_UTF8, .text = "中文\0x", .text_length = 8},
                         {0},
                         {.type = UD_DATE32, .integer = -1},
                         {.type = UD_TIME64, .integer = 86399999999LL},
                         {.type = UD_TIMESTAMP, .integer = -1},
                         {.type = UD_BINARY, .text = "\0\xffx", .text_length = 3},
                         {.type = UD_STRUCT, .children = nested, .child_count = 2},
                         {.type = UD_LIST, .children = items, .child_count = 3},
                         {.type = UD_MAP, .children = &map, .child_count = 1}};
    const char *decimal = "-123456789012345678901234567890123456.78";
    OK(ud_value_parse_field(ctx, fields + 4, decimal, (uint32_t)strlen(decimal), UD_ROUND_EXACT,
                            values + 4));
    ud_batch *batch = NULL, *copy = NULL;
    OK(ud_batch_create(ctx, fields, 12, 2, &batch));
    OK(ud_batch_append(batch, values, 12));
    for (uint32_t j = 0; j < 12; j++) {
        values[j].is_null = 1;
        values[j].integer = 123;
        values[j].real = NAN;
    }
    OK(ud_batch_append(batch, values, 12));
    uint8_t *wire = NULL;
    uint64_t length = 0;
    OK(ud_batch_serialize(batch, &wire, &length));
    OK(ud_batch_deserialize(ctx, wire, length, &copy));
    CHECK(!same_batch(batch, copy));
    ud_free(wire);
    ud_batch_destroy(copy);
    struct ArrowArray array = {0};
    struct ArrowSchema schema = {0};
    OK(ud_batch_export_arrow(batch, &array, &schema));
    OK(ud_batch_import_arrow(ctx, &array, &schema, &copy));
    CHECK(!same_batch(batch, copy));
    array.release(&array);
    schema.release(&schema);
    ud_batch_destroy(copy);
    char *json = NULL;
    uint32_t size = 0;
    OK(ud_batch_to_json(batch, &json, &size));
    CHECK(strstr(json, "\"NaN\"") && strstr(json, "\"Infinity\"") && strstr(json, "\"-Infinity\""));
    OK(ud_batch_from_json(ctx, fields, 12, json, size, UD_ROUND_EXACT, &copy));
    CHECK(!same_batch(batch, copy));
    ud_free(json);
    ud_batch_destroy(copy);
    ud_batch_destroy(batch);
    return 0;
}
static int float_operators(ud_context *ctx) {
    ud_buffer_manager *buffers = NULL;
    ud_buffer_options bo = {.struct_size = sizeof(bo),
                            .abi_version = UD_ABI_VERSION,
                            .max_blocks = 100,
                            .resident_limit = 1000000,
                            .working_limit = 1000000};
    OK(ud_buffer_manager_create(ctx, &bo, &buffers));
    ud_quality_options qo = {.struct_size = sizeof(qo),
                             .abi_version = UD_ABI_VERSION,
                             .max_batch_rows = 4096,
                             .max_violations = 4096,
                             .policy = UD_ERROR_REDIRECT,
                             .buffers = buffers};
    ud_field fields[] = {{.name = "key", .name_length = 3, .type = UD_FLOAT64, .nullable = 1},
                         {.name = "id", .name_length = 2, .type = UD_INT64}};
    double keys[] = {-0.0,
                     0.0,
                     nan_bits(UINT64_C(0x7ff8000000000001)),
                     nan_bits(UINT64_C(0xfff800000000ffff)),
                     -INFINITY,
                     INFINITY,
                     3.0,
                     0.0};
    ud_batch *input = NULL, *sorted = NULL, *joined = NULL, *right = NULL;
    OK(ud_batch_create(ctx, fields, 2, 8, &input));
    for (uint32_t j = 0; j < 8; j++) {
        ud_value v[] = {{.type = UD_FLOAT64, .real = keys[j], .is_null = j == 7},
                        {.type = UD_INT64, .integer = j}};
        OK(ud_batch_append(input, v, 2));
    }
    OK(ud_compute_sort(ctx, input, 0, 0, &sorted));
    int64_t order[] = {7, 4, 0, 1, 6, 5, 2, 3};
    for (uint32_t j = 0; j < 8; j++) {
        ud_value v;
        OK(ud_batch_get(sorted, j, 1, &v));
        CHECK(v.integer == order[j]);
    }
    ud_batch_destroy(sorted);
    fields[0].name = "rk";
    fields[0].name_length = 2;
    fields[1].name = "rid";
    fields[1].name_length = 3;
    OK(ud_batch_create(ctx, fields, 2, 5, &right));
    uint32_t select[] = {1, 3, 2, 7, 6};
    for (uint32_t j = 0; j < 5; j++) {
        ud_value v[] = {{.type = UD_FLOAT64, .real = keys[select[j]], .is_null = select[j] == 7},
                        {.type = UD_INT64, .integer = j}};
        OK(ud_batch_append(right, v, 2));
    }
    ud_buffer_manager *join_buffers = NULL;
    OK(ud_buffer_manager_create(ctx,
                                &(ud_buffer_options){.struct_size = sizeof(ud_buffer_options),
                                                     .abi_version = UD_ABI_VERSION,
                                                     .max_blocks = 100,
                                                     .resident_limit = 1048576,
                                                     .working_limit = 1048576},
                                &join_buffers));
    uint32_t join_key = 0;
    const ud_batch *join_inputs[] = {input, right};
    ud_operator *join_op = NULL;
    OK(ud_operator_create(ctx,
                          &(ud_operator_options){.struct_size = sizeof(ud_operator_options),
                                                 .abi_version = UD_ABI_VERSION,
                                                 .kind = UD_OPERATOR_JOIN,
                                                 .max_batch_rows = 8,
                                                 .keys = &join_key,
                                                 .right_keys = &join_key,
                                                 .key_count = 1,
                                                 .buffers = join_buffers},
                          join_inputs, &join_op));
    OK(ud_batch_seal(input));
    OK(ud_batch_seal(right));
    OK(ud_operator_push(join_op, 1, right));
    OK(ud_operator_finish_input(join_op, 1));
    OK(ud_operator_push(join_op, 0, input));
    OK(ud_operator_finish_input(join_op, 0));
    OK(ud_operator_pull(join_op, &joined, NULL));
    CHECK(ud_batch_rows(joined) == 7);
    int64_t pairs[][2] = {{0, 0}, {1, 0}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {6, 4}};
    uint32_t pair_seen = 0;
    for (uint32_t j = 0; j < 7; j++) {
        ud_value a, b;
        OK(ud_batch_get(joined, j, 1, &a));
        OK(ud_batch_get(joined, j, 3, &b));
        uint32_t at = 0;
        for (; at < 7; at++)
            if (a.integer == pairs[at][0] && b.integer == pairs[at][1])
                break;
        CHECK(at < 7 && !(pair_seen & (1u << at)));
        pair_seen |= 1u << at;
    }
    CHECK(pair_seen == 127);
    ud_batch_destroy(joined);
    CHECK(ud_operator_pull(join_op, &joined, NULL) == UD_END);
    OK(ud_operator_destroy(join_op));
    qo.buffers = join_buffers;
    const char *commands =
        "{\"version\":12,\"group\":[\"key\"],\"aggregates\":[{\"function\":\"count\",\"column\":\"*"
        "\",\"name\":\"cnt\"},{\"function\":\"min\",\"column\":\"key\",\"name\":\"lo\"},{"
        "\"function\":\"max\",\"column\":\"key\",\"name\":\"hi\"}]}";
    ud_field left_fields[2];
    OK(ud_batch_field(input, 0, left_fields));
    OK(ud_batch_field(input, 1, left_fields + 1));
    ud_plan *plan = NULL;
    OK(ud_plan_create_fields(ctx, UD_PLAN_VERSION, left_fields, 2,
                             (ud_string){commands, (uint32_t)strlen(commands)}, NULL, &plan));
    ud_batch *groups = NULL, *twice = NULL;
    OK(ud_compute_apply(plan, input, &groups));
    CHECK(ud_batch_rows(groups) == 6);
    ud_accumulator *acc = NULL;
    OK(ud_accumulator_create(plan, &acc));
    OK(ud_accumulator_restore(acc, groups));
    OK(ud_accumulator_add(acc, input));
    OK(ud_accumulator_snapshot(acc, &twice));
    CHECK(ud_batch_rows(twice) == 6);
    for (uint32_t j = 0; j < 6; j++) {
        ud_value key, count, doubled;
        OK(ud_batch_get(groups, j, 0, &key));
        OK(ud_batch_get(groups, j, 1, &count));
        OK(ud_batch_get(twice, j, 1, &doubled));
        CHECK(doubled.integer == count.integer * 2);
        CHECK(count.integer == (!key.is_null && (key.real == 0 || isnan(key.real)) ? 2 : 1));
    }
    ud_accumulator_destroy(acc);
    ud_batch_destroy(twice);
    ud_plan_destroy(plan);
    ud_field gf[4];
    for (uint32_t j = 0; j < 4; j++)
        OK(ud_batch_field(groups, j, gf + j));
    ud_batch *corrupt = NULL;
    OK(ud_batch_create(ctx, gf, 4, 2, &corrupt));
    ud_value duplicate[4];
    for (uint32_t j = 0; j < 4; j++)
        OK(ud_batch_get(groups, 1, j, duplicate + j));
    OK(ud_batch_append(corrupt, duplicate, 4));
    OK(ud_batch_append(corrupt, duplicate, 4));
    OK(ud_plan_create_fields(ctx, UD_PLAN_VERSION, left_fields, 2,
                             (ud_string){commands, (uint32_t)strlen(commands)}, NULL, &plan));
    OK(ud_accumulator_create(plan, &acc));
    CHECK(ud_accumulator_restore(acc, corrupt) == UD_CORRUPT);
    CHECK(ud_accumulator_add(acc, input) == UD_INVALID);
    ud_accumulator_destroy(acc);
    ud_plan_destroy(plan);
    ud_batch_destroy(corrupt);
    ud_batch_destroy(groups);
    uint32_t key = 0;
    ud_quality_rule rule = {.struct_size = sizeof(rule),
                            .abi_version = UD_ABI_VERSION,
                            .rule_version = 1,
                            .policy = UD_ERROR_INHERIT,
                            .id = "unique",
                            .id_length = 6,
                            .kind = UD_RULE_UNIQUE,
                            .columns = &key,
                            .column_count = 1};
    ud_quality *q = NULL;
    ud_batch *part = NULL, *accepted = NULL;
    OK(ud_quality_create(ctx, left_fields, 2, &rule, 1, &qo, &q));
    OK(ud_compute_slice(ctx, input, 0, 4, &part));
    OK(ud_quality_apply(q, part, &accepted, NULL, NULL, NULL));
    CHECK(ud_batch_rows(accepted) == 2);
    ud_batch_destroy(accepted);
    ud_batch_destroy(part);
    OK(ud_quality_apply(q, input, &accepted, NULL, NULL, NULL));
    CHECK(ud_batch_rows(accepted) == 4);
    ud_quality_metrics metrics;
    ud_quality_get_metrics(q, &metrics);
    CHECK(metrics.processed == 12 && metrics.accepted == 6 && metrics.rejected == 6);
    OK(ud_quality_finish(q));
    ud_batch_destroy(accepted);
    ud_quality_destroy(q);
    rule.kind = UD_RULE_FOREIGN_KEY;
    rule.reference = right;
    rule.reference_columns = &key;
    OK(ud_quality_create(ctx, left_fields, 2, &rule, 1, &qo, &q));
    OK(ud_quality_reference_end(q));
    ud_batch_destroy(right);
    OK(ud_quality_apply(q, input, &accepted, NULL, NULL, NULL));
    CHECK(ud_batch_rows(accepted) == 6);
    ud_batch_destroy(accepted);
    ud_quality_destroy(q);
    rule.kind = UD_RULE_RANGE;
    rule.minimum = (ud_value){.type = UD_FLOAT64, .real = -INFINITY};
    rule.maximum = (ud_value){.type = UD_FLOAT64, .real = INFINITY};
    OK(ud_quality_create(ctx, left_fields, 2, &rule, 1, &qo, &q));
    OK(ud_quality_apply(q, input, &accepted, NULL, NULL, NULL));
    CHECK(ud_batch_rows(accepted) == 6);
    ud_batch_destroy(accepted);
    ud_quality_destroy(q);
    ud_batch_destroy(input);
    OK(ud_buffer_manager_destroy(buffers));
    OK(ud_buffer_manager_destroy(join_buffers));
    return 0;
}
static int expressions(ud_context *ctx) {
    ud_field children[] = {{.name = "f", .name_length = 1, .type = UD_FLOAT64, .nullable = 1}};
    ud_field fields[] = {
        {.name = "a",
         .name_length = 1,
         .type = UD_DECIMAL128,
         .nullable = 1,
         .precision = 38,
         .scale = 2},
        {.name = "b",
         .name_length = 1,
         .type = UD_DECIMAL128,
         .nullable = 1,
         .precision = 38,
         .scale = 2},
        {.name = "x", .name_length = 1, .type = UD_STRUCT, .children = children, .child_count = 1},
        {.name = "y", .name_length = 1, .type = UD_STRUCT, .children = children, .child_count = 1}};
    ud_value nested = {.type = UD_FLOAT64, .real = NAN};
    ud_value v[4] = {{0},
                     {0},
                     {.type = UD_STRUCT, .children = &nested, .child_count = 1},
                     {.type = UD_STRUCT, .children = &nested, .child_count = 1}};
    OK(ud_value_parse_field(ctx, fields, "9007199254740993.25", 19, UD_ROUND_EXACT, v));
    OK(ud_value_parse_field(ctx, fields + 1, "9007199254740993.25", 19, UD_ROUND_EXACT, v + 1));
    ud_batch *batch = NULL, *vector = NULL;
    OK(ud_batch_create(ctx, fields, 4, 2, &batch));
    OK(ud_batch_append(batch, v, 4));
    v[0].is_null = 1;
    OK(ud_batch_append(batch, v, 4));
    OK(ud_batch_seal(batch));
    ud_expression_options o = {sizeof(o), UD_ABI_VERSION, batch, NULL, NULL};
    ud_expression *e = NULL;
    OK(ud_expression_create(
        ctx, str("{\"op\":\"eq\",\"args\":[{\"column\":\"a\"},{\"column\":\"b\"}]}"), &o, &e));
    OK(ud_expression_evaluate_batch(e, batch, str("same"), &vector));
    ud_value result;
    OK(ud_batch_get(vector, 0, 0, &result));
    CHECK(result.integer == 1 && !result.is_null);
    OK(ud_batch_get(vector, 1, 0, &result));
    CHECK(result.is_null);
    ud_batch_destroy(vector);
    ud_expression_destroy(e);
    OK(ud_expression_create(
        ctx, str("{\"op\":\"eq\",\"args\":[{\"column\":\"x\"},{\"column\":\"y\"}]}"), &o, &e));
    OK(ud_expression_evaluate(e, batch, 0, &result));
    CHECK(result.integer == 1);
    ud_expression_destroy(e);
    OK(ud_expression_create(ctx, str("{\"column\":\"a\"}"), &o, &e));
    OK(ud_expression_evaluate_batch(e, batch, str("copy"), &vector));
    ud_field metadata;
    OK(ud_batch_field(vector, 0, &metadata));
    CHECK(metadata.precision == 38 && metadata.scale == 2);
    ud_batch_destroy(vector);
    ud_expression_destroy(e);
    ud_batch_destroy(batch);
    fields[1].scale = 3;
    OK(ud_batch_create(ctx, fields, 4, 0, &batch));
    o.schema = batch;
    CHECK(ud_expression_create(
              ctx, str("{\"op\":\"eq\",\"args\":[{\"column\":\"a\"},{\"column\":\"b\"}]}"), &o,
              &e) == UD_OVERFLOW);
    ud_batch_destroy(batch);
    o.schema = NULL;
    OK(ud_expression_create(ctx,
                            str("{\"op\":\"eq\",\"args\":[{\"literal\":\"NaN\",\"type\":"
                                "\"float64\"},{\"literal\":\"NaN\",\"type\":\"float64\"}]}"),
                            &o, &e));
    OK(ud_expression_evaluate(e, NULL, 0, &result));
    CHECK(result.integer == 1);
    ud_expression_destroy(e);
    OK(ud_expression_create(ctx,
                            str("{\"op\":\"sub\",\"args\":[{\"literal\":\"Infinity\",\"type\":"
                                "\"float64\"},{\"literal\":\"Infinity\",\"type\":\"float64\"}]}"),
                            &o, &e));
    OK(ud_expression_evaluate(e, NULL, 0, &result));
    CHECK(isnan(result.real));
    ud_expression_destroy(e);
    CHECK(ud_expression_create(ctx,
                               str("{\"op\":\"mul\",\"args\":[{\"literal\":1e308,\"type\":"
                                   "\"float64\"},{\"literal\":2,\"type\":\"float64\"}]}"),
                               &o, &e) == UD_OVERFLOW);
    CHECK(ud_expression_create(ctx, str("{\"literal\":\"1.5\",\"type\":\"float64\"}"), &o, &e) ==
          UD_SCHEMA);
    return 0;
}
typedef struct allocation_fault {
    uint64_t calls, fail;
} allocation_fault;
static void *fault_allocate(void *user, size_t bytes) {
    allocation_fault *f = user;
    return ++f->calls == f->fail ? NULL : malloc(bytes);
}
static void fault_free(void *user, void *pointer) {
    (void)user;
    free(pointer);
}
static ud_status fixture_join(ud_context *ctx, ud_buffer_manager *buffers, const ud_batch *left,
                              const ud_batch *right, ud_batch **out) {
    uint32_t key = 0;
    const ud_batch *inputs[] = {left, right};
    ud_operator *j = NULL;
    ud_status status =
        ud_operator_create(ctx,
                           &(ud_operator_options){.struct_size = sizeof(ud_operator_options),
                                                  .abi_version = UD_ABI_VERSION,
                                                  .kind = UD_OPERATOR_JOIN,
                                                  .max_batch_rows = 8,
                                                  .buffers = buffers,
                                                  .keys = &key,
                                                  .right_keys = &key,
                                                  .key_count = 1},
                           inputs, &j);
    if (!status)
        status = ud_operator_push(j, 1, right);
    if (!status)
        status = ud_operator_finish_input(j, 1);
    if (!status)
        status = ud_operator_push(j, 0, left);
    if (!status)
        status = ud_operator_finish_input(j, 0);
    if (!status)
        status = ud_operator_pull(j, out, NULL);
    ud_status closed = ud_operator_destroy(j);
    return status ? status : closed;
}
static int allocation_failures(void) {
    ud_quality_options qo = {.struct_size = sizeof(qo),
                             .abi_version = UD_ABI_VERSION,
                             .max_batch_rows = 4096,
                             .max_violations = 4096,
                             .policy = UD_ERROR_REDIRECT};
    uint32_t failures[3] = {0}, successes[3] = {0};
    for (uint32_t operation = 0; operation < 3; operation++) {
        for (uint64_t offset = 1; offset <= 512; offset++) {
            allocation_fault fault = {0};
            ud_context_options o = {sizeof(o), UD_ABI_VERSION, 16 * 1024 * 1024,
                                    &fault,    fault_allocate, fault_free};
            ud_context *ctx = NULL;
            OK(ud_context_create(&o, &ctx));
            ud_field field = {.name = "key", .name_length = 3, .type = UD_FLOAT64, .nullable = 1};
            ud_batch *left = NULL, *right = NULL, *out = NULL;
            OK(ud_batch_create(ctx, &field, 1, 4, &left));
            ud_value v = {.type = UD_FLOAT64, .real = NAN};
            OK(ud_batch_append(left, &v, 1));
            v.real = -0.0;
            OK(ud_batch_append(left, &v, 1));
            v.real = 0;
            OK(ud_batch_append(left, &v, 1));
            v.is_null = 1;
            OK(ud_batch_append(left, &v, 1));
            field.name = "right";
            field.name_length = 5;
            OK(ud_batch_create(ctx, &field, 1, 1, &right));
            v.is_null = 0;
            v.real = NAN;
            OK(ud_batch_append(right, &v, 1));
            field.name = "key";
            field.name_length = 3;
            ud_plan *plan = NULL;
            ud_quality *q = NULL;
            ud_buffer_manager *buffers = NULL;
            OK(ud_buffer_manager_create(
                ctx,
                &(ud_buffer_options){.struct_size = sizeof(ud_buffer_options),
                                     .abi_version = UD_ABI_VERSION,
                                     .max_blocks = 100,
                                     .resident_limit = 1048576,
                                     .working_limit = 1048576},
                &buffers));
            qo.buffers = buffers;
            OK(ud_batch_seal(left));
            OK(ud_batch_seal(right));
            const char *commands = "{\"version\":12,\"group\":[\"key\"],\"aggregates\":[{"
                                   "\"function\":\"count\",\"column\":\"*\",\"name\":\"cnt\"}]}";
            OK(ud_plan_create_fields(ctx, UD_PLAN_VERSION, &field, 1,
                                     (ud_string){commands, (uint32_t)strlen(commands)}, NULL,
                                     &plan));
            uint32_t column = 0;
            ud_quality_rule rule = {.struct_size = sizeof(rule),
                                    .abi_version = UD_ABI_VERSION,
                                    .rule_version = 1,
                                    .policy = UD_ERROR_INHERIT,
                                    .id = "unique",
                                    .id_length = 6,
                                    .kind = UD_RULE_UNIQUE,
                                    .columns = &column,
                                    .column_count = 1};
            OK(ud_quality_create(ctx, &field, 1, &rule, 1, &qo, &q));
            fault.fail = fault.calls + offset;
            ud_status status = operation == 0   ? fixture_join(ctx, buffers, left, right, &out)
                               : operation == 1 ? ud_compute_apply(plan, left, &out)
                                                : ud_quality_apply(q, left, &out, NULL, NULL, NULL);
            CHECK(status == UD_OK || status == UD_RESOURCE);
            fault.fail = 0;
            if (status) {
                failures[operation]++;
                CHECK(!out);
                if (operation == 2) {
                    ud_quality_metrics metrics;
                    ud_quality_get_metrics(q, &metrics);
                    CHECK(!metrics.processed && !metrics.accepted && !metrics.rejected);
                    CHECK(ud_quality_apply(q, left, &out, NULL, NULL, NULL) == UD_INVALID && !out);
                }
            } else {
                successes[operation]++;
                CHECK(ud_batch_rows(out) == (operation ? 3u : 1u));
            }
            ud_batch_destroy(out);
            ud_batch_destroy(left);
            ud_batch_destroy(right);
            ud_quality_destroy(q);
            OK(ud_buffer_manager_destroy(buffers));
            ud_plan_destroy(plan);
            CHECK(ud_memory_used(ctx) == 0);
            OK(ud_context_destroy(ctx));
        }
        CHECK(failures[operation] && successes[operation]);
    }
    return 0;
}
int main(void) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    /* A wall clock has no implied UTC zone. Explicit offsets identifying the
     * same instant share a key; an unzoned wall value is a distinct domain. */
    ud_value times[] = {{.type = UD_DATETIME, .datetime = {.ticks = INT64_C(638960400000000000)}},
                        {.type = UD_DATETIME, .datetime = {.ticks = INT64_C(638960400000000000), .has_offset = 1}},
                        {.type = UD_DATETIME, .datetime = {.ticks = INT64_C(638960436000000000), .offset_minutes = 60, .has_offset = 1}}};
    CHECK(ud_value_compare(times, times + 1) != 0);
    CHECK(ud_value_hash(times, 123) != ud_value_hash(times + 1, 123));
    CHECK(!ud_value_compare(times + 1, times + 2));
    CHECK(ud_value_hash(times + 1, 123) == ud_value_hash(times + 2, 123));
    ud_field moment = {.name = "moment", .name_length = 6, .type = UD_DATETIME};
    ud_batch *moments = NULL, *time_groups = NULL;
    OK(ud_batch_create(ctx, &moment, 1, 3, &moments));
    for (uint32_t at = 0; at < 3; at++)
        OK(ud_batch_append(moments, times + at, 1));
    OK(ud_batch_seal(moments));
    const char *time_plan_json = "{\"version\":12,\"group\":[\"moment\"],\"aggregates\":[{\"function\":\"count\",\"column\":\"*\",\"name\":\"n\"}]}";
    ud_plan *time_plan = NULL;
    OK(ud_plan_create_fields(ctx, UD_PLAN_VERSION, &moment, 1,
                            str(time_plan_json), NULL, &time_plan));
    OK(ud_compute_apply(time_plan, moments, &time_groups));
    CHECK(ud_batch_rows(time_groups) == 2);
    ud_batch_destroy(time_groups);
    ud_plan_destroy(time_plan);
    ud_batch_destroy(moments);
    ud_value a = {.type = UD_FLOAT64, .real = 0}, b = {.type = UD_FLOAT64, .real = -0.0};
    CHECK(!ud_value_compare(&a, &b) && ud_value_hash(&a, 123) == ud_value_hash(&b, 123));
    for (uint64_t bits = 1; bits < 512; bits++) {
        a.real = nan_bits(UINT64_C(0x7ff0000000000000) | bits);
        b.real = nan_bits(UINT64_C(0xfff8000000000000) | (bits << 12));
        CHECK(!ud_value_compare(&a, &b) && ud_value_hash(&a, bits) == ud_value_hash(&b, bits));
    }
    CHECK(!roundtrips(ctx));
    CHECK(!float_operators(ctx));
    CHECK(!expressions(ctx));
    ud_field f = {.name = "f", .name_length = 1, .type = UD_FLOAT64};
    ud_batch *invalid = NULL;
    const char *bad[] = {"[{\"f\":\"1\"}]", "[{\"f\":\"nan\"}]", "[{\"f\":NaN}]",
                         "[{\"f\":\"+Infinity\"}]"};
    for (uint32_t j = 0; j < 4; j++) {
        CHECK(ud_batch_from_json(ctx, &f, 1, bad[j], (uint32_t)strlen(bad[j]), UD_ROUND_EXACT,
                                 &invalid) != UD_OK &&
              !invalid);
    }
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    CHECK(!allocation_failures());
    puts("value semantics: all types/NULL/NUL/Decimal/UTC/NaN wire+Arrow+JSON, stable Sort, hash "
         "Join, restored Group, cross-batch Unique/FK/Range and bound expressions passed");
    return 0;
}
