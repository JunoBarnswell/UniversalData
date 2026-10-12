#include "check.h"
#include "ud/package.h"
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
typedef struct fixture {
    ud_execution *execution;
    int mode, reads;
} fixture;
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)tenant;
    (void)resource;
    fixture *f = user;
    if (f->mode == 1 && action == UD_ACCESS_READ)
        return UD_AUTH;
    if (f->mode == 2 && action == UD_ACCESS_WRITE && resource.length >= 13 &&
        !memcmp(resource.data + resource.length - 13, ".ud-task.part", 13)) {
        ud_execution_cancel(f->execution);
        return UD_OK;
    }
    if ((f->mode == 3 || f->mode == 7) && action == UD_ACCESS_READ && ++f->reads == 5) {
        if (f->mode == 3)
            return UD_AUTH;
        ud_execution_cancel(f->execution);
    }
    if (f->mode == 6 && action == UD_ACCESS_DELETE)
        return UD_AUTH;
    return UD_OK;
}
static void observe(void *user, const ud_execution_event *event) {
    fixture *f = user;
    if (f->mode == 4 && f->execution && event->kind == UD_EXEC_EVENT_BEFORE_CHECKPOINT &&
        event->task_id.length) {
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        if (!ud_execution_get_metrics(f->execution, &m) && m.executed_tasks == 1)
            _exit(91);
    }
}
int main(int argc, char **argv) {
    CHECK(argc == 7);
    fixture f = {.mode = atoi(argv[2])};
    FILE *file = fopen(argv[1], "rb");
    CHECK(file && !fseek(file, 0, SEEK_END));
    long n = ftell(file);
    CHECK(n > 0 && !fseek(file, 0, SEEK_SET));
    char *json = malloc((size_t)n);
    CHECK(json && fread(json, 1, (size_t)n, file) == (size_t)n);
    fclose(file);
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .context = {.struct_size = sizeof(o.context),
                                          .abi_version = UD_ABI_VERSION,
                                          .memory_limit = 16777216},
                              .edge_capacity = 1,
                              .max_batch_rows = 3,
                              .max_depth = 32,
                              .tenant = str("tenant"),
                              .execution_id = str("filesystem"),
                              .authorize = f.mode == 5 ? NULL : authorize,
                              .authorize_user = &f,
                              .observe = observe,
                              .observe_user = &f,
                              .state_directory = str(argv[3]),
                              .resume = atoi(argv[4])};
    ud_execution *e = NULL;
    ud_status s = ud_execution_create_json((ud_string){json, (uint32_t)n}, &o, &e);
    free(json);
    f.execution = e;
    if (!s)
        s = ud_execution_execute(e);
    if (s != atoi(argv[5]))
        fprintf(stderr, "actual %d expected %s\n", s, argv[5]);
    CHECK(s == atoi(argv[5]));
    if (e) {
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        OK(ud_execution_get_metrics(e, &m));
        CHECK(m.queued_batches == 0 && m.result == s);
        if (!s)
            CHECK(m.executed_tasks == (uint64_t)atoi(argv[6]));
        CHECK(m.state == (s == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                          : s == UD_CANCELLED    ? UD_EXEC_CANCELLED
                          : s                    ? UD_EXEC_FAILED
                                                 : UD_EXEC_SUCCEEDED));
        OK(ud_execution_destroy(e));
    }
    return 0;
}
