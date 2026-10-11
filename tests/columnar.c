#include "check.h"
#include "ud/data.h"
#include <string.h>
typedef struct {
    size_t calls, fail;
} allocator;
static void *allocate(void *user, size_t n) {
    allocator *a = user;
    if (++a->calls == a->fail)
        return NULL;
    return malloc(n);
}
static void deallocate(void *u, void *p) {
    (void)u;
    free(p);
}
int main(void) {
    ud_context *c = NULL;
    OK(ud_context_create(NULL, &c));
    ud_field fields[] = {{"id", 2, UD_INT64, 0}, {"text", 4, UD_UTF8, 1}, {"flag", 4, UD_BOOL, 0}};
    ud_batch *b = NULL;
    OK(ud_batch_create(c, fields, 3, 3, &b));
    ud_value values[] = {{.type = UD_INT64, .integer = 7},
                         {.type = UD_UTF8, .text = "中文\n\"", .text_length = 8},
                         {.type = UD_BOOL, .integer = 1}};
    OK(ud_batch_append(b, values, 3));
    values[1].is_null = 1;
    OK(ud_batch_append(b, values, 3));
    values[0].is_null = 1;
    CHECK(ud_batch_append(b, values, 3) == UD_SCHEMA);
    CHECK(ud_batch_rows(b) == 2);
    struct ArrowArray array;
    struct ArrowSchema schema;
    OK(ud_batch_export_arrow(b, &array, &schema));
    ud_batch_destroy(b);
    b = NULL;
    CHECK(array.n_children == 3 && array.children[1]->null_count == 1);
    OK(ud_batch_import_arrow(c, &array, &schema, &b));
    ud_value actual;
    OK(ud_batch_get(b, 0, 1, &actual));
    CHECK(actual.text_length == 8 && !memcmp(actual.text, "中文\n\"", 8));
    OK(ud_batch_get(b, 1, 1, &actual));
    CHECK(actual.is_null);
    array.release(&array);
    schema.release(&schema);
    CHECK(!array.release && !schema.release);
    struct ArrowArrayStream stream;
    OK(ud_batch_export_stream(b, &stream));
    ud_batch_destroy(b);
    CHECK(stream.get_schema(&stream, &schema) == 0);
    schema.release(&schema);
    CHECK(stream.get_next(&stream, &array) == 0 && array.release);
    array.release(&array);
    CHECK(stream.get_next(&stream, &array) == 0 && !array.release);
    stream.release(&stream);
    CHECK(ud_memory_used(c) == 0);
    OK(ud_context_destroy(c));
    /* Fail each allocation in Arrow export; all partial trees must release safely. */
    for (size_t fail = 1; fail <= 32; fail++) {
        allocator a = {0, 0};
        ud_context_options o = {sizeof(o), 1, 1024 * 1024, &a, allocate, deallocate};
        OK(ud_context_create(&o, &c));
        OK(ud_batch_create(c, fields, 3, 2, &b));
        values[0].is_null = 0;
        values[1].is_null = 0;
        OK(ud_batch_append(b, values, 3));
        a.calls = 0;
        a.fail = fail;
        ud_status s = ud_batch_export_arrow(b, &array, &schema);
        if (!s) {
            array.release(&array);
            schema.release(&schema);
        } else
            CHECK(s == UD_RESOURCE);
        ud_batch_destroy(b);
        CHECK(ud_memory_used(c) == 0);
        OK(ud_context_destroy(c));
    }
    puts("columnar: nulls, UTF-8, Arrow lifetimes, stream, allocation failures passed");
    return 0;
}
