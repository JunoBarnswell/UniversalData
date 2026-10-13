#ifndef UD_HOST_OUTPUT_H
#define UD_HOST_OUTPUT_H
#include "ud/package.h"
/* JSON encoding only; execution identity and its lifetime remain native. */
static void host_identity_json(ud_string id, char out[513]) {
    uint32_t at = 0;
    for (uint32_t i = 0; i < id.length; i++) {
        char ch = id.data[i];
        if (ch == '"' || ch == '\\')
            out[at++] = '\\';
        out[at++] = ch;
    }
    out[at] = 0;
}
#endif
