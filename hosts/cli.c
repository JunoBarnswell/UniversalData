#include "ud/etl.h"
#include "ud/engine.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
/* OS signal handlers only touch sig_atomic_t; no allocator/atomic library calls. */
static volatile sig_atomic_t interrupted = 0;
static void stop(int sig) {
    (void)sig;
    interrupted = 1;
}
static char *read_text(const char *path, uint32_t *length) {
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    if (fseek(f, 0, SEEK_END)) {
        fclose(f);
        return NULL;
    }
    long n = ftell(f);
    if (n < 0 || n > UINT32_MAX || fseek(f, 0, SEEK_SET)) {
        fclose(f);
        return NULL;
    }
    char *s = calloc((size_t)n + 1, 1);
    if (!s) {
        fclose(f);
        return NULL;
    }
    if (fread(s, 1, (size_t)n, f) != (size_t)n || ferror(f)) {
        free(s);
        fclose(f);
        return NULL;
    }
    fclose(f);
    *length = (uint32_t)n;
    return s;
}
int main(int argc, char **argv) {
    if (argc != 6 || (strcmp(argv[1], "once") && strcmp(argv[1], "watch"))) {
        fprintf(stderr, "usage: ud_cli once|watch ABS_INPUT ABS_STATE schema.ud plan.ud\n");
        return 2;
    }
    uint32_t sn = 0, pn = 0;
    char *schema = read_text(argv[4], &sn), *plan = read_text(argv[5], &pn);
    if (!schema || !plan) {
        fprintf(stderr, "cannot read schema/plan\n");
        free(schema);
        free(plan);
        return 2;
    }
    ud_context *ctx = NULL;
    ud_status s = ud_context_create(NULL, &ctx);
    ud_job *job = NULL;
    ud_job_options o = {0};
    o.struct_size = sizeof(o);
    o.abi_version = UD_ABI_VERSION;
    o.input_directory = argv[2];
    o.state_directory = argv[3];
    o.schema = schema;
    o.schema_length = sn;
    o.commands = plan;
    o.commands_length = pn;
    if (!s)
        s = ud_job_create(ctx, &o, &job);
    free(schema);
    free(plan);
    signal(SIGINT, stop);
    signal(SIGTERM, stop);
    if (!s) {
        do {
            s = ud_job_poll(job);
            if (s || !strcmp(argv[1], "once"))
                break;
            s = ud_wait(ctx, 250);
            if (s)
                break;
            /* The synchronous CLI observes signals between complete polls. SDK
             * hosts can call ud_cancel from a separate thread during a poll. */
        } while (!interrupted);
    }
    if (s)
        fprintf(stderr, "%s: %s\n", ud_status_name(s), ud_error(ctx));
    else
        printf("generation=%llu\n", (unsigned long long)ud_job_generation(job));
    ud_job_destroy(job);
    ud_status clean = ud_context_destroy(ctx);
    return s || clean ? 1 : 0;
}
