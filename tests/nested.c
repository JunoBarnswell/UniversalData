#include "ud/compute.h"
#include "check.h"
#include <string.h>
int main(void) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_field person_fields[] = {{.name = "name", .name_length = 4, .type = UD_UTF8},
                                {.name = "age", .name_length = 3, .type = UD_INT64, .nullable = 1}};
    ud_field item = {.name = "item", .name_length = 4, .type = UD_INT64, .nullable = 1};
    ud_field entry_fields[] = {{.name = "key", .name_length = 3, .type = UD_UTF8},
                               {.name = "value", .name_length = 5, .type = UD_UTF8, .nullable = 1}};
    ud_field entry = {.name = "entries",
                      .name_length = 7,
                      .type = UD_STRUCT,
                      .children = entry_fields,
                      .child_count = 2};
    ud_field fields[] = {{.name = "person",
                          .name_length = 6,
                          .type = UD_STRUCT,
                          .nullable = 1,
                          .children = person_fields,
                          .child_count = 2},
                         {.name = "items",
                          .name_length = 5,
                          .type = UD_LIST,
                          .nullable = 1,
                          .children = &item,
                          .child_count = 1},
                         {.name = "attrs",
                          .name_length = 5,
                          .type = UD_MAP,
                          .nullable = 1,
                          .children = &entry,
                          .child_count = 1}};
    ud_batch *b = NULL;
    OK(ud_batch_create(ctx, fields, 3, 4, &b));
    ud_value person[] = {{.type = UD_UTF8, .text = "中文", .text_length = 6},
                         {.type = UD_INT64, .integer = 42}};
    ud_value items[] = {{.type = UD_INT64, .integer = 1},
                        {.type = UD_INT64, .is_null = 1},
                        {.type = UD_INT64, .integer = 3}};
    ud_value map_pair[] = {{.type = UD_UTF8, .text = "color", .text_length = 5},
                           {.type = UD_UTF8, .text = "blue", .text_length = 4}};
    ud_value pairs[] = {{.type = UD_STRUCT, .children = map_pair, .child_count = 2}};
    ud_value row[] = {{.type = UD_STRUCT, .children = person, .child_count = 2},
                      {.type = UD_LIST, .children = items, .child_count = 3},
                      {.type = UD_MAP, .children = pairs, .child_count = 1}};
    OK(ud_batch_append(b, row, 3));
    ud_value borrowed[3];
    for (uint32_t j = 0; j < 3; j++)
        OK(ud_batch_get(b, 0, j, borrowed + j));
    OK(ud_batch_append(b, borrowed, 3));
    person[0].is_null = 1;
    CHECK(ud_batch_append(b, row, 3) == UD_SCHEMA && ud_batch_rows(b) == 2);
    person[0].is_null = 0;
    /* Later nested validation failure must undo earlier nested append and preserve views. */
    items[2].integer = 4;
    map_pair[0].text = "bad\xc0\x80";
    map_pair[0].text_length = 5;
    CHECK(ud_batch_append(b, row, 3) == UD_PARSE && ud_batch_rows(b) == 2);
    map_pair[0].text = "color";
    map_pair[0].text_length = 5;
    OK(ud_batch_append(b, row, 3));
    for (uint32_t j = 0; j < 3; j++) {
        row[j].is_null = 1;
        row[j].children = NULL;
        row[j].child_count = 0;
    }
    OK(ud_batch_append(b, row, 3));
    struct ArrowArray a;
    struct ArrowSchema s;
    OK(ud_batch_export_arrow(b, &a, &s));
    CHECK(!strcmp(s.children[0]->format, "+s") && a.children[0]->n_buffers == 1);
    CHECK(!strcmp(s.children[1]->format, "+l") && a.children[1]->n_buffers == 2);
    CHECK(!strcmp(s.children[2]->format, "+m") && a.children[2]->n_children == 1);
    const int32_t *offsets = a.children[1]->buffers[1];
    CHECK(offsets[0] == 0 && offsets[1] == 3 && offsets[2] == 6 && offsets[3] == 9 &&
          offsets[4] == 9);
    CHECK(a.children[1]->children[0]->length == 9);
    ud_batch *imported = NULL;
    OK(ud_batch_import_arrow(ctx, &a, &s, &imported));
    a.release(&a);
    s.release(&s);
    ud_batch_destroy(b);
    b = NULL;
    ud_value value;
    OK(ud_batch_get(imported, 2, 1, &value));
    CHECK(value.child_count == 3 && value.children[2].integer == 4);
    OK(ud_batch_get(imported, 3, 0, &value));
    CHECK(value.is_null);
    uint8_t *wire = NULL;
    uint64_t size = 0;
    OK(ud_batch_serialize(imported, &wire, &size));
    OK(ud_batch_deserialize(ctx, wire, size, &b));
    ud_free(wire);
    ud_batch_destroy(imported);
    imported = NULL;
    OK(ud_batch_get(b, 0, 2, &value));
    CHECK(value.children[0].children[1].text_length == 4 &&
          !memcmp(value.children[0].children[1].text, "blue", 4));
    ud_field target_children[2];
    memcpy(target_children, person_fields, sizeof(target_children));
    target_children[1].type = UD_UTF8;
    ud_field targets[3];
    memcpy(targets, fields, sizeof(targets));
    targets[0].children = target_children;
    ud_batch *converted = NULL;
    OK(ud_batch_cast(ctx, b, targets, 3, UD_ROUND_EXACT, &converted));
    OK(ud_batch_get(converted, 0, 0, &value));
    CHECK(value.children[1].text_length == 2 && !memcmp(value.children[1].text, "42", 2));
    ud_batch_destroy(converted);
    /* Typed plan owns its schema and nested aggregate keys after producer destruction. */
    ud_plan *plan = NULL;
    const char *commands = "{\"version\":11,\"group\":[\"person\"],\"aggregates\":[{\"function\":"
                           "\"count\",\"column\":\"*\",\"name\":\"rows\"}]}";
    OK(ud_plan_create_fields(ctx, UD_PLAN_VERSION, fields, 3,
                             (ud_string){commands, (uint32_t)strlen(commands)}, NULL, &plan));
    ud_accumulator *acc = NULL;
    OK(ud_accumulator_create(plan, &acc));
    OK(ud_accumulator_add(acc, b));
    ud_batch_destroy(b);
    b = NULL;
    OK(ud_accumulator_snapshot(acc, &b));
    CHECK(ud_batch_rows(b) == 2);
    ud_batch_destroy(b);
    ud_accumulator_destroy(acc);
    ud_plan_destroy(plan);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
