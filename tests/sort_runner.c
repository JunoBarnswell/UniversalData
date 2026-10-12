#include "ud/query.h"
#include <string.h>
#include <stdio.h>
int main(int argc, char **argv) {
    if (argc != 4)
        return 2;
    ud_context *ctx = NULL;
    ud_plan *plan = NULL;
    ud_status s = ud_context_create(NULL, &ctx);
    const char *schema = UD_SCHEMA_HEADER "team utf8 required\namount int64 nullable\n";
    if (!s)
        s = ud_plan_create(ctx, schema, (uint32_t)strlen(schema), UD_PLAN_HEADER, sizeof(UD_PLAN_HEADER) - 1, &plan);
    if (!s)
        s = ud_query_sort_csv(ctx, argv[1], argv[2], argv[3], plan, 1, 0, 2);
    if (s)
        fprintf(stderr, "%s\n", ud_error(ctx));
    ud_plan_destroy(plan);
    ud_status clean = ud_context_destroy(ctx);
    return s ? s : clean;
}
