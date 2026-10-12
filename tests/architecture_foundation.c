#include "check.h"
#include "ud/expression.h"
#include "ud/execution.h"
#include "ud/component.h"
#include <string.h>
#include <math.h>
static ud_status expression(ud_context *c, const char *json, const ud_batch *schema,
                            ud_expression **out) {
    ud_expression_options o = {sizeof(o), UD_ABI_VERSION, schema, NULL, NULL};
    return ud_expression_create(c, (ud_string){json, (uint32_t)strlen(json)}, &o, out);
}
static ud_status task(void *u,const ud_component_context *c,ud_string config) { (void)u;(void)c;(void)config;return UD_OK; }
int main(void) {
    ud_context *c = NULL;
    OK(ud_context_create(NULL, &c));
    ud_field f = {.name = "x", .name_length = 1, .type = UD_INT64, .nullable = 1};
    ud_batch *b = NULL;
    OK(ud_batch_create(c, &f, 1, 2, &b));
    ud_value v = {.type = UD_INT64, .integer = 7};
    OK(ud_batch_append(b, &v, 1));
    v.is_null = 1;
    OK(ud_batch_append(b, &v, 1));
    CHECK(ud_batch_retain(b) == UD_INVALID);
    OK(ud_batch_seal(b));
    OK(ud_batch_retain(b));
    CHECK(ud_batch_append(b, &v, 1) == UD_BUSY);
    ud_batch_destroy(b);
    CHECK(ud_batch_rows(b) == 2);
    ud_expression *e = NULL;
    OK(expression(
        c, "{\"op\":\"add\",\"args\":[{\"column\":\"x\"},{\"literal\":5,\"type\":\"int64\"}]}", b,
        &e));
    OK(ud_expression_evaluate(e, b, 0, &v));
    CHECK(v.integer == 12 && !v.is_null);
    OK(ud_expression_evaluate(e, b, 1, &v));
    CHECK(v.is_null);
    ud_expression_destroy(e);
    OK(expression(c,
                  "{\"op\":\"and\",\"args\":[{\"literal\":null,\"type\":\"bool\"},{\"literal\":"
                  "false,\"type\":\"bool\"}]}",
                  NULL, &e));
    OK(ud_expression_evaluate(e, NULL, 0, &v));
    CHECK(!v.is_null && !v.integer);
    ud_expression_destroy(e);
    CHECK(expression(c,
                     "{\"op\":\"add\",\"args\":[{\"literal\":9223372036854775807,\"type\":"
                     "\"int64\"},{\"literal\":1,\"type\":\"int64\"}]}",
                     NULL, &e) == UD_OVERFLOW &&
          !e);
    CHECK(expression(c, "{\"column\":\"missing\"}", b, &e) == UD_SCHEMA && !e);
    CHECK(expression(c, "{\"literal\":1,\"type\":\"bool\"}", NULL, &e) == UD_SCHEMA && !e);
    ud_value x = {.type = UD_FLOAT64, .real = NAN}, y = {.type = UD_FLOAT64, .real = 1};
    CHECK(ud_value_compare(&x, &y) > 0);
    CHECK(ud_value_compare(&x, &x) == 0);
    CHECK(ud_execution_transition_allowed(UD_EXEC_RUNNING, UD_EXEC_CANCELLED));
    CHECK(!ud_execution_transition_allowed(UD_EXEC_SUCCEEDED, UD_EXEC_RUNNING));
    CHECK(!ud_execution_transition_allowed(UD_EXEC_CREATED, UD_EXEC_SUCCEEDED));
    ud_registry *r = NULL;
    OK(ud_registry_create(c, &r));
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = {"driver", 6},
                                 .name = {"Driver", 6},
                                 .version = {"1", 1},
                                 .configuration_schema = {"{}", 2},
                                 .category = UD_COMPONENT_TASK,
                                 .synchronous_input = -1,
                                 .error_output = -1, .invoke=task};
    OK(ud_registry_register(r, &d));
    CHECK(ud_registry_register(r, &d) == UD_SCHEMA);
    CHECK(ud_registry_count(r) == 1);
    ud_component_descriptor got = {.struct_size = sizeof(got), .abi_version = UD_ABI_VERSION};
    OK(ud_registry_find(r, (ud_string){"driver", 6}, &got));
    CHECK(got.category == UD_COMPONENT_TASK);
    OK(ud_registry_destroy(r));
    ud_batch_destroy(b);
    CHECK(ud_memory_used(c) == 0);
    OK(ud_context_destroy(c));
    return 0;
}
