#ifndef UD_TEST_CHECK_H
#define UD_TEST_CHECK_H
#include <stdio.h>
#include <stdlib.h>
#define CHECK(expr)                                                                                \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expr);                             \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)
#define OK(expr)                                                                                   \
    do {                                                                                           \
        ud_status test_status_ = (expr);                                                           \
        if (test_status_ != UD_OK) {                                                               \
            fprintf(stderr, "status=%d at %s:%d: %s\n", test_status_, __FILE__, __LINE__, #expr);  \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)
#endif
