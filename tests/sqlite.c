#include "check.h"
#include "ud/plugin.h"
#include <string.h>
int main(int argc, char **argv) {
    CHECK(argc == 2);
    ud_context *c = NULL;
    OK(ud_context_create(NULL, &c));
    ud_plugin *p = NULL;
    ud_plugin_instance *i = NULL;
    OK(ud_plugin_load(c, argv[1], UD_CAP_READ | UD_CAP_WRITE, &p));
    OK(ud_plugin_create(p, ":memory:", 8, &i));
    ud_field f = {"value", 5, UD_INT64, 0};
    ud_batch *empty = NULL, *rows = NULL, *out = NULL;
    OK(ud_batch_create(c, &f, 1, 0, &empty));
    OK(ud_batch_create(c, &f, 1, 2, &rows));
    ud_value v = {.type = UD_INT64, .integer = 42};
    OK(ud_batch_append(rows, &v, 1));
    OK(ud_batch_append(rows, &v, 1));
    const char *ddl = "CREATE TABLE data(value INTEGER UNIQUE)";
    OK(ud_plugin_execute(i, UD_OP_WRITE, empty, ddl, (uint32_t)strlen(ddl), &out));
    const char *insert = "INSERT INTO data(value) VALUES (?)";
    CHECK(ud_plugin_execute(i, UD_OP_WRITE, rows, insert, (uint32_t)strlen(insert), &out) == UD_IO);
    const char *select = "SELECT value FROM data";
    CHECK(ud_plugin_execute(i, UD_OP_READ, empty, select, (uint32_t)strlen(select), &out) ==
          UD_END);
    ud_batch_destroy(rows);
    OK(ud_batch_create(c, &f, 1, 1, &rows));
    OK(ud_batch_append(rows, &v, 1));
    OK(ud_plugin_execute(i, UD_OP_WRITE, rows, insert, (uint32_t)strlen(insert), &out));
    OK(ud_plugin_execute(i, UD_OP_READ, empty, select, (uint32_t)strlen(select), &out));
    CHECK(ud_batch_rows(out) == 1);
    ud_batch_destroy(out);
    ud_batch_destroy(rows);
    ud_batch_destroy(empty);
    OK(ud_plugin_instance_destroy(i));
    OK(ud_plugin_unload(p));
    CHECK(ud_memory_used(c) == 0);
    OK(ud_context_destroy(c));
    return 0;
}
