#include "check.h"
#include "ud/plugin.h"
int main(int argc, char **argv) {
    CHECK(argc == 2);
    ud_context *c = NULL;
    OK(ud_context_create(NULL, &c));
    ud_plugin *p = NULL;
    CHECK(ud_plugin_load(c, argv[1], UD_CAP_SQL, &p) == UD_UNSUPPORTED && !p);
    OK(ud_plugin_load(c, argv[1], UD_CAP_SCALAR, &p));
    ud_plugin_instance *i = NULL;
    OK(ud_plugin_create(p, "", 0, &i));
    CHECK(ud_plugin_unload(p) == UD_BUSY);
    ud_field f = {"value", 5, UD_INT64, 0};
    ud_batch *in = NULL, *out = NULL;
    OK(ud_batch_create(c, &f, 1, 1, &in));
    ud_value v = {.type = UD_INT64, .integer = -42};
    OK(ud_batch_append(in, &v, 1));
    OK(ud_plugin_execute(i, UD_OP_SCALAR, in, "", 0, &out));
    OK(ud_batch_get(out, 0, 0, &v));
    CHECK(v.integer == 42);
    ud_batch_destroy(out);
    out = NULL;
    CHECK(ud_plugin_execute(i, UD_OP_SQL, NULL, "", 0, &out) == UD_UNSUPPORTED);
    ud_batch_destroy(in);
    /* execute always resets its output slot; caller owns and must release earlier outputs. */
    OK(ud_plugin_instance_destroy(i));
    OK(ud_plugin_unload(p));
    CHECK(ud_memory_used(c) == 0);
    OK(ud_context_destroy(c));
    return 0;
}
