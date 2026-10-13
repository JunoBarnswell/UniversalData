#include "ud/package.h"
#include "output.h"
#include <stdio.h>
#include <stdlib.h>
#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#endif
/* Current UD_WORKER_VERSION: LE u32 byte length + one Package JSON document. Each frame owns a
 * native Execution. No credentials/authorization are inferred from input. A
 * privileged embedding host must provide its policy through the SDK instead.
 * Reply is LE u32 length + JSON {version,executionId,state,result}. */
int main(void) {
#ifdef _WIN32
    _setmode(_fileno(stdin), _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
#endif
    for (;;) {
        unsigned char h[4];
        size_t got = fread(h, 1, 4, stdin);
        if (!got && feof(stdin))
            return 0;
        if (got != 4)
            return 2;
        uint32_t n =
            (uint32_t)h[0] | (uint32_t)h[1] << 8 | (uint32_t)h[2] << 16 | (uint32_t)h[3] << 24;
        if (n > 64 * 1024 * 1024)
            return 2;
        char *text = malloc((size_t)n + 1);
        if (!text)
            return 2;
        if (fread(text, 1, n, stdin) != n) {
            free(text);
            return 2;
        }
        ud_execution_options o = {.struct_size = sizeof(o),
                                  .abi_version = UD_ABI_VERSION,
                                  .edge_capacity = 2,
                                  .max_batch_rows = 4096,
                                  .max_depth = 32};
        o.context = (ud_context_options){.struct_size = sizeof(o.context),
                                         .abi_version = UD_ABI_VERSION,
                                         .memory_limit = 268435456};
        ud_execution *e = NULL;
        ud_status s = ud_execution_create_json((ud_string){text, n}, &o, &e);
        free(text);
        if (!s)
            s = ud_execution_execute(e);
        ud_execution_metrics m = {
            .struct_size = sizeof(m), .abi_version = UD_ABI_VERSION, .state = UD_EXEC_FAILED};
        if (e)
            ud_execution_get_metrics(e, &m);
        ud_string identity = {0};
        char encoded_id[513];
        if (e)
            ud_execution_id(e, &identity);
        host_identity_json(identity, encoded_id);
        ud_status clean = ud_execution_destroy(e);
        if (!s)
            s = clean;
        char reply[1024];
        int bytes = snprintf(reply, sizeof(reply),
                             "{\"version\":" UD_PROTOCOL_VERSION_TEXT
                             ",\"executionId\":\"%s\",\"state\":%d,\"result\":%d}",
                             encoded_id, m.state, s);
        uint32_t length = (uint32_t)bytes;
        for (int i = 0; i < 4; i++)
            h[i] = (unsigned char)(length >> (8 * i));
        if (fwrite(h, 1, 4, stdout) != 4 || fwrite(reply, 1, length, stdout) != length ||
            fflush(stdout))
            return 2;
    }
}
