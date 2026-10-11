#include "ud/etl.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
static void crash(void *user, int32_t event, uint64_t generation) {
    if (event == *(int *)user && generation == 1)
        _Exit(73);
}
int main(int argc, char **argv) {
    if (argc < 3)
        return 2;
    int phase = argc > 3 ? atoi(argv[3]) : 0;
    const char *mode = argc > 4 ? argv[4] : "";
    ud_context *ctx = NULL;
    ud_context_options co = {sizeof(co), 1,    !strcmp(mode, "quota") ? 8192 : 64 * 1024 * 1024,
                             NULL,       NULL, NULL};
    ud_status s = ud_context_create(&co, &ctx);
    ud_job *j = NULL;
    const char *schema = "team utf8 required\namount int64 nullable\n";
    const char *plan = !strcmp(mode, "changed")
                           ? "filter amount ge 1\ngroup team\nsum amount total\ncount * rows\n"
                           : "filter amount ge 0\nproject team amount\ngroup team\nsum amount "
                             "total\ncount * rows\n";
    ud_job_options o = {0};
    o.struct_size = sizeof(o);
    o.abi_version = 1;
    o.input_directory = argv[1];
    o.state_directory = argv[2];
    o.schema = schema;
    o.schema_length = (uint32_t)strlen(schema);
    o.commands = plan;
    o.commands_length = (uint32_t)strlen(plan);
    o.batch_size = 1;
    o.checkpoint_rows = 1;
    o.observer = crash;
    o.observer_user = &phase;
    if (!s)
        s = ud_job_create(ctx, &o, &j);
    if (!s && !strcmp(mode, "hold")) {
        puts("READY");
        fflush(stdout);
        getchar();
    } else if (!s) {
        if (!strcmp(mode, "cancel"))
            ud_cancel(ctx);
        s = ud_job_poll(j);
    }
    if (s)
        fprintf(stderr, "%s: %s\n", ud_status_name(s), ud_error(ctx));
    ud_job_destroy(j);
    ud_status clean = ud_context_destroy(ctx);
    return s ? s : clean;
}
