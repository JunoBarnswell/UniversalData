#include <ud/data.h>
#include <stdio.h>
int main(void) {
    ud_context *ctx = NULL;
    ud_batch *b = NULL;
    ud_status s = ud_context_create(NULL, &ctx);
    ud_field f = {"value", 5, UD_INT64, 0};
    ud_value v = {.type = UD_INT64, .integer = 42};
    if (!s)
        s = ud_batch_create(ctx, &f, 1, 1, &b);
    if (!s)
        s = ud_batch_append(b, &v, 1);
    if (!s) {
        ud_value result;
        s = ud_batch_get(b, 0, 0, &result);
        if (!s && result.integer != 42)
            s = UD_CORRUPT;
    }
    if (s)
        fprintf(stderr, "%s\n", ud_error(ctx));
    ud_batch_destroy(b);
    ud_status clean = ud_context_destroy(ctx);
    return s || clean ? 1 : 0;
}
