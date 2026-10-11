#include "check.h"
#include "ud/plugin.h"
#include <string.h>
int main(int argc, char **argv) {
    CHECK(argc == 3);
    remove(argv[2]);
    ud_context *c = NULL;
    OK(ud_context_create(NULL, &c));
    ud_plugin *p = NULL;
    ud_plugin_instance *i = NULL;
    ud_status loaded = ud_plugin_load(c, argv[1], UD_CAP_SQL | UD_CAP_PARQUET, &p);
    if (loaded)
        fprintf(stderr, "%s\n", ud_error(c));
    OK(loaded);
    OK(ud_plugin_create(p, "", 0, &i));
    ud_batch *out = NULL;
    const char *sql = "SELECT 42::BIGINT AS answer";
    OK(ud_plugin_execute(i, UD_OP_SQL, NULL, sql, (uint32_t)strlen(sql), &out));
    ud_value v;
    OK(ud_batch_get(out, 0, 0, &v));
    CHECK(v.integer == 42);
    ud_batch *in = out;
    out = NULL;
    OK(ud_plugin_execute(i, UD_OP_PARQUET_WRITE, in, argv[2], (uint32_t)strlen(argv[2]), &out));
    OK(ud_plugin_execute(i, UD_OP_PARQUET_READ, NULL, argv[2], (uint32_t)strlen(argv[2]), &out));
    OK(ud_batch_get(out, 0, 0, &v));
    CHECK(v.integer == 42);
    ud_batch_destroy(out);
    ud_batch_destroy(in);
    OK(ud_plugin_instance_destroy(i));
    OK(ud_plugin_unload(p));
    CHECK(ud_memory_used(c) == 0);
    OK(ud_context_destroy(c));
    remove(argv[2]);
    return 0;
}
