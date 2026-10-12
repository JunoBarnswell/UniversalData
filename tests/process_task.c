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
    int mode;
} fixture;
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status authorize(void *user, ud_string t, ud_string r, int32_t a) {
    (void)t;
    (void)r;
    return ((fixture *)user)->mode == 2 && a == UD_ACCESS_EXECUTE ? UD_AUTH : UD_OK;
}
static int exists(const char *p) {
    FILE *f = fopen(p, "rb");
    if (f) {
        fclose(f);
        return 1;
    }
    return 0;
}
static void observe(void *user, const ud_execution_event *event) {
    fixture *f = user;
    if (f->mode == 4 && f->execution && event->kind == UD_EXEC_EVENT_BEFORE_CHECKPOINT &&
        event->task_id.length) {
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        if (!ud_execution_get_metrics(f->execution, &m) && m.completed_tasks == 1)
            _exit(91);
    }
}
int main(int argc, char **argv) {
    CHECK(argc == 8);
    fixture f = {.mode = atoi(argv[2])};
    FILE *file = fopen(argv[1], "rb");
    CHECK(file && !fseek(file, 0, SEEK_END));
    long n = ftell(file);
    CHECK(n > 0 && !fseek(file, 0, SEEK_SET));
    char *text = malloc((size_t)n);
    CHECK(text && fread(text, 1, (size_t)n, file) == (size_t)n);
    fclose(file);
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .context = {.struct_size = sizeof(o.context),
                                          .abi_version = UD_ABI_VERSION,
                                          .memory_limit = 33554432},
                              .edge_capacity = 1,
                              .max_batch_rows = 3,
                              .max_depth = 32,
                              .tenant = str("tenant"),
                              .execution_id = str("process"),
                              .authorize = f.mode == 1 ? NULL : authorize,
                              .authorize_user = &f,
                              .observe = observe,
                              .observe_user = &f,
                              .state_directory = str(argv[6]),
                              .resume = atoi(argv[7]),
                              .timeout_ms = f.mode == 5 ? 1000 : 0};
    ud_execution *e = NULL;
    ud_status s = ud_execution_create_json((ud_string){text, (uint32_t)n}, &o, &e);
    free(text);
    f.execution = e;
    if (!s && f.mode == 3) {
        OK(ud_execution_start(e));
        int seen = 0;
        for (int i = 0; i < 300; i++) {
            if (exists(argv[5])) {
                seen = 1;
                break;
            }
            s = ud_execution_wait(e, 10);
            if (s != UD_BUSY)
                break;
        }
        CHECK(seen);
        ud_execution_cancel(e);
        do {
            s = ud_execution_wait(e, 100);
        } while (s == UD_BUSY);
    } else if (!s)
        s = ud_execution_execute(e);
    if (s != atoi(argv[3]))
        fprintf(stderr, "actual=%d expected=%s\n", s, argv[3]);
    CHECK(s == atoi(argv[3]));
    if (e) {
        ud_value exit;
        OK(ud_execution_variable(e, str("exit"), &exit));
        CHECK(exit.integer == strtoll(argv[4], NULL, 10));
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        OK(ud_execution_get_metrics(e, &m));
        CHECK(m.result == s && m.queued_batches == 0 &&
              m.state == (s == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                          : s == UD_CANCELLED    ? UD_EXEC_CANCELLED
                          : s                    ? UD_EXEC_FAILED
                                                 : UD_EXEC_SUCCEEDED));
        OK(ud_execution_destroy(e));
    }
    return 0;
}
