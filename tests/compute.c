#include "check.h"
#include "ud/query.h"
#include "ud/engine.h"
#include <string.h>
int main(void) {
    ud_context *c = NULL;
    OK(ud_context_create(NULL, &c));
    const char *schema = "key utf8 nullable\namount int64 nullable\n";
    const char *commands = "filter amount ge 0\nproject key amount\ngroup key\nsum amount "
                           "total\ncount * rows\nmin amount smallest\nmax amount largest\n";
    ud_plan *p = NULL;
    OK(ud_plan_create(c, schema, (uint32_t)strlen(schema), commands, (uint32_t)strlen(commands),
                      &p));
    const ud_field *f;
    uint32_t n;
    OK(ud_plan_schema(p, &f, &n));
    ud_batch *input = NULL, *result = NULL;
    OK(ud_batch_create(c, f, n, 4, &input));
    ud_value v[] = {{.type = UD_UTF8, .text = "a", .text_length = 1},
                    {.type = UD_INT64, .integer = 10}};
    OK(ud_batch_append(input, v, 2));
    v[1].integer = 5;
    OK(ud_batch_append(input, v, 2));
    v[1].integer = -1;
    OK(ud_batch_append(input, v, 2));
    v[1].is_null = 1;
    OK(ud_batch_append(input, v, 2));
    OK(ud_query_execute(p, input, &result));
    CHECK(ud_batch_rows(result) == 1);
    ud_value actual;
    OK(ud_batch_get(result, 0, 1, &actual));
    CHECK(actual.integer == 15);
    OK(ud_batch_get(result, 0, 2, &actual));
    CHECK(actual.integer == 2);
    OK(ud_batch_get(result, 0, 3, &actual));
    CHECK(actual.integer == 5);
    OK(ud_batch_get(result, 0, 4, &actual));
    CHECK(actual.integer == 10);
    ud_accumulator *acc = NULL;
    OK(ud_accumulator_create(p, &acc));
    OK(ud_accumulator_restore(acc, result));
    OK(ud_accumulator_add(acc, input));
    ud_batch *twice = NULL;
    OK(ud_accumulator_snapshot(acc, &twice));
    OK(ud_batch_get(twice, 0, 1, &actual));
    CHECK(actual.integer == 30);
    ud_batch_destroy(twice);
    ud_accumulator_destroy(acc);
    ud_batch_destroy(result);
    ud_batch *sorted = NULL, *page = NULL;
    OK(ud_query_sort(c, input, 1, 0, &sorted));
    OK(ud_batch_get(sorted, 0, 1, &actual));
    CHECK(actual.is_null);
    OK(ud_query_slice(c, sorted, 1, 2, &page));
    CHECK(ud_batch_rows(page) == 2);
    OK(ud_batch_get(page, 0, 1, &actual));
    CHECK(actual.integer == -1);
    ud_batch_destroy(page);
    ud_batch_destroy(sorted);
    ud_field right_fields[] = {{"other_key", 9, UD_UTF8, 1}, {"label", 5, UD_INT64, 0}};
    ud_batch *right = NULL, *joined = NULL;
    OK(ud_batch_create(c, right_fields, 2, 1, &right));
    v[1].is_null = 0;
    v[1].integer = 99;
    OK(ud_batch_append(right, v, 2));
    OK(ud_query_inner_join(c, input, 0, right, 0, &joined));
    CHECK(ud_batch_rows(joined) == 4 && ud_batch_columns(joined) == 4);
    ud_batch_destroy(joined);
    ud_batch_destroy(right);
    ud_queue *q = NULL;
    OK(ud_queue_create(c, 1, &q));
    OK(ud_queue_push(q, input));
    CHECK(ud_queue_push(q, input) == UD_BUSY);
    ud_batch *got = NULL;
    OK(ud_queue_pop(q, &got));
    CHECK(got == input);
    CHECK(ud_queue_pop(q, &got) == UD_END);
    ud_queue_destroy(q);
    CHECK(ud_truth_and(-1, 0) == 0 && ud_truth_and(-1, 1) == -1 && ud_truth_or(-1, 1) == 1 &&
          ud_truth_not(-1) == -1);
    ud_batch_destroy(input);
    ud_plan_destroy(p);
    CHECK(ud_memory_used(c) == 0);
    OK(ud_context_destroy(c));
    puts("compute/query: filters, aggregates, restore, sort, join, page, queue passed");
    return 0;
}
