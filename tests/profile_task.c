#include "check.h"
#include "ud/package.h"
#include "ud/json.h"
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <process.h>
#include <windows.h>
#else
#include <unistd.h>
#endif
typedef struct fixture {
    ud_execution *execution;
    int mode, interrupted;
} fixture;
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)tenant;
    (void)resource;
    fixture *f = user;
    if (f->mode == 1)
        return UD_AUTH;
    if (f->mode == 7 && action == UD_ACCESS_READ && f->execution) {
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        if (!ud_execution_get_metrics(f->execution, &m) && m.input_rows)
            return UD_AUTH;
    }
    return UD_OK;
}
static ud_status credential(void *user, ud_context *ctx, ud_string tenant, ud_string resource,
                            ud_string reference, ud_string *out) {
    (void)user;
    (void)tenant;
    if (reference.length != 6 || memcmp(reference.data, "source", 6))
        return UD_AUTH;
    char *copy = ud_alloc(ctx, resource.length + 1);
    if (!copy)
        return UD_RESOURCE;
    memcpy(copy, resource.data, resource.length);
    *out = (ud_string){copy, resource.length};
    return UD_OK;
}
static void observe(void *user, const ud_execution_event *event) {
    fixture *f = user;
    if (!f->execution)
        return;
    ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
    if (ud_execution_get_metrics(f->execution, &m))
        return;
    if (f->mode == 3 && event->kind == UD_EXEC_EVENT_BEFORE_CHECKPOINT && m.executed_tasks == 1)
        _exit(91);
    if (!f->interrupted && event->pipeline_id.length && m.input_rows) {
        f->interrupted = 1;
        if (f->mode == 2)
            ud_execution_cancel(f->execution);
        if (f->mode == 4)
            _exit(91);
        if (f->mode == 5) {
#ifdef _WIN32
            Sleep(50);
#else
            struct timespec wait = {0, 50000000};
            nanosleep(&wait, NULL);
#endif
        }
    }
}
static char *read_file(const char *path, uint32_t *n) {
    FILE *file = fopen(path, "rb");
    if (!file)
        return NULL;
    fseek(file, 0, SEEK_END);
    long length = ftell(file);
    rewind(file);
    char *text = malloc((size_t)length + 1);
    if (!text || fread(text, 1, (size_t)length, file) != (size_t)length) {
        free(text);
        fclose(file);
        return NULL;
    }
    fclose(file);
    text[length] = 0;
    *n = (uint32_t)length;
    return text;
}
int main(int argc, char **argv) {
    CHECK(argc == 7);
    fixture f = {.mode = atoi(argv[2])};
    uint32_t n;
    char *text = read_file(argv[1], &n);
    CHECK(text);
    ud_execution_options o = {.struct_size = sizeof(o),
                              .abi_version = UD_ABI_VERSION,
                              .context = {.struct_size = sizeof(o.context),
                                          .abi_version = UD_ABI_VERSION,
                                          .memory_limit = f.mode == 6 ? 65536 : 33554432},
                              .edge_capacity = 1,
                              .max_batch_rows = 7,
                              .max_depth = 32,
                              .tenant = {"fixture", 7},
                              .execution_id = {"profile", 7},
                              .authorize = authorize,
                              .resolve = credential,
                              .authorize_user = &f,
                              .observe = observe,
                              .observe_user = &f,
                              .timeout_ms = f.mode == 5 ? 20 : 0,
                              .state_directory = str(argv[5]),
                              .resume = atoi(argv[6])};
    ud_execution *e = NULL;
    ud_status s = ud_execution_create_json((ud_string){text, n}, &o, &e);
    free(text);
    f.execution = e;
    if (!s)
        s = ud_execution_execute(e);
    if (s != atoi(argv[3]))
        fprintf(stderr, "actual=%d expected=%s\n", s, argv[3]);
    CHECK(s == atoi(argv[3]));
    if (e) {
        text = read_file(argv[4], &n);
        CHECK(text);
        ud_context *ctx = NULL;
        OK(ud_context_create(NULL, &ctx));
        ud_json *expected = NULL;
        OK(ud_json_parse(ctx, text, n, &expected, NULL));
        free(text);
        for (uint32_t i = 0; i < ud_json_size(expected); i++) {
            ud_string name;
            OK(ud_json_key(expected, i, &name.data, &name.length));
            ud_value actual;
            OK(ud_execution_variable(e, name, &actual));
            const ud_json *json = ud_json_at(expected, i);
            if (ud_json_kind(json) == UD_JSON_NULL) {
                CHECK(actual.is_null);
                continue;
            }
            const char *value;
            uint32_t length;
            OK(ud_json_text(json, &value, &length));
            ud_value wanted;
            OK(ud_value_parse(ctx, actual.type, value, length, &wanted));
            CHECK(!actual.is_null);
            if (actual.type == UD_UTF8)
                CHECK(actual.text_length == wanted.text_length &&
                      !memcmp(actual.text, wanted.text, wanted.text_length));
            else if (actual.type == UD_FLOAT64)
                CHECK(actual.real == wanted.real);
            else
                CHECK(actual.integer == wanted.integer);
            /* parsed UTF8 values borrow JSON text; no owned value allocation */
        }
        ud_json_destroy(expected);
        OK(ud_context_destroy(ctx));
        ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
        OK(ud_execution_get_metrics(e, &m));
        CHECK(m.result == s && m.queued_batches == 0 &&
              m.state == (s == UD_CANCELLED        ? UD_EXEC_CANCELLED
                          : s == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                          : s                      ? UD_EXEC_FAILED
                                                   : UD_EXEC_SUCCEEDED));
        OK(ud_execution_destroy(e));
    }
    return 0;
}
