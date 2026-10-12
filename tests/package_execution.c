#include "check.h"
#include "ud/package.h"
#include <string.h>
static ud_status allow(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)tenant;
    (void)resource;
    (void)action;
    return *(int *)user ? UD_OK : UD_AUTH;
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    FILE *file = fopen(argv[1], "rb");
    CHECK(file);
    CHECK(!fseek(file, 0, SEEK_END));
    long length = ftell(file);
    CHECK(length > 0 && !fseek(file, 0, SEEK_SET));
    char *text = malloc((size_t)length);
    CHECK(text);
    CHECK(fread(text, 1, (size_t)length, file) == (size_t)length);
    fclose(file);
    ud_execution_options o = {0};
    o.struct_size = sizeof(o);
    o.abi_version = UD_ABI_VERSION;
    o.context = (ud_context_options){
        .struct_size = sizeof(o.context), .abi_version = UD_ABI_VERSION, .memory_limit = 67108864};
    o.edge_capacity = 1;
    o.max_batch_rows = 7;
    o.max_depth = 32;
    o.tenant = (ud_string){"fixture", 7};
    o.execution_id = (ud_string){"native", 6};
    int authorized = 1;
    o.authorize = allow;
    o.authorize_user = &authorized;
    ud_execution *e = NULL;
    OK(ud_execution_create_json((ud_string){text, (uint32_t)length}, &o, &e));
    free(text);
    OK(ud_execution_start(e));
    ud_status s;
    do {
        s = ud_execution_wait(e, 100);
    } while (s == UD_BUSY);
    OK(s);
    ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
    OK(ud_execution_get_metrics(e, &m));
    CHECK(m.state == UD_EXEC_SUCCEEDED && m.result == UD_OK && m.input_rows == 31 &&
          m.output_rows == 31 && m.executed_tasks == 6 && m.queue_high_water <= 5);
    ud_value iterations;
    OK(ud_execution_variable(e, (ud_string){"n", 1}, &iterations));
    CHECK(iterations.integer == 3);
    CHECK(ud_execution_execute(e) == UD_BUSY);
    OK(ud_execution_destroy(e));
    return 0;
}
