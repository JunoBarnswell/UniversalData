#include "ud/compute.h"
#include <stdio.h>
#include <time.h>
int main(void) {
    ud_context *ctx = NULL;
    ud_status s = ud_context_create(NULL, &ctx);
    ud_field f = {"value", 5, UD_INT64, 0};
    ud_batch *b = NULL, *sorted = NULL;
    if (!s)
        s = ud_batch_create(ctx, &f, 1, 100000, &b);
    clock_t start = clock();
    for (int64_t i = 100000; !s && i > 0; i--) {
        ud_value v = {.type = UD_INT64, .integer = i};
        s = ud_batch_append(b, &v, 1);
    }
    if (!s)
        s = ud_compute_sort(ctx, b, 0, 0, &sorted);
    if (!s)
        printf("rows=100000 cpu_seconds=%.6f live_bytes=%llu\n",
               (double)(clock() - start) / CLOCKS_PER_SEC, (unsigned long long)ud_memory_used(ctx));
    ud_batch_destroy(sorted);
    ud_batch_destroy(b);
    ud_status clean = ud_context_destroy(ctx);
    return s || clean ? 1 : 0;
}
