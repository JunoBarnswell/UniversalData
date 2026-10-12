#include "check.h"
#include "../components/foundation/internal.h"
#include <string.h>
static void *allocate(void *ptr, size_t n) {
    size_t *count = ptr;
    (*count)++;
    return malloc(n);
}
static void deallocate(void *ptr, void *p) {
    size_t *count = ptr;
    (*count)--;
    free(p);
}
int main(void) {
    ud_context *c = NULL;
    size_t live = 0;
    ud_context_options o = {sizeof(o), UD_ABI_VERSION, 256, &live, allocate, deallocate};
    OK(ud_context_create(&o, &c));
    CHECK(live==1); /* Context handle also respects the creating allocator. */
    void *p = ud_alloc(c, 64);
    CHECK(p && live == 2);
    CHECK(ud_memory_used(c) >= 64);
    CHECK(ud_context_destroy(c) == UD_BUSY);
    CHECK(!ud_alloc(c, 1024));
    ud_free(p);
    CHECK(live == 1);
    ud_cancel(c);
    CHECK(ud_is_cancelled(c));
    ud_reset_cancel(c);
    CHECK(!ud_is_cancelled(c));
    uint8_t hash[32];
    ud_hash("abc", 3, hash);
    const uint8_t expected[32] = {0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40,
                                  0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17,
                                  0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad};
    CHECK(!memcmp(hash, expected, 32));
    OK(ud_context_destroy(c));
    CHECK(live==0);
    puts("foundation: quota, ownership, cancellation, SHA-256 passed");
    return 0;
}
