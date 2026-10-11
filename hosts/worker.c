#include "ud/query.h"
#include "ud/plugin.h"
#include "../components/data/internal.h"
#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#endif
static int read_frame(uint8_t **data, uint32_t *size) {
    uint8_t h[4];
    size_t n = fread(h, 1, 4, stdin);
    if (!n && feof(stdin))
        return 0;
    if (n != 4)
        return -1;
    *size =
        (uint32_t)h[0] | ((uint32_t)h[1] << 8) | ((uint32_t)h[2] << 16) | ((uint32_t)h[3] << 24);
    if (*size > 64 * 1024 * 1024)
        return -1;
    *data = malloc(*size ? *size : 1);
    if (!*data)
        return -1;
    if (fread(*data, 1, *size, stdin) != *size) {
        free(*data);
        *data = NULL;
        return -1;
    }
    return 1;
}
static ud_status process(ud_context *ctx, ud_bytes *b, ud_batch **out) {
    uint64_t version, operation;
    UD_TRY(ud_get_u64(b, &version));
    UD_TRY(ud_get_u64(b, &operation));
    if (version != 1)
        return UD_INCOMPATIBLE;
    ud_status s = UD_OK;
    ud_batch *input = NULL;
    if (operation == 1) {
        char *schema = NULL, *commands = NULL;
        ud_plan *plan = NULL;
        s = ud_get_string(b, &schema);
        if (!s)
            s = ud_get_string(b, &commands);
        if (!s)
            s = ud_plan_create(ctx, schema, (uint32_t)strlen(schema), commands,
                               (uint32_t)strlen(commands), &plan);
        if (!s)
            s = ud_decode_batch(b, &input);
        if (!s && b->pos != b->size)
            s = UD_CORRUPT;
        if (!s)
            s = ud_query_execute(plan, input, out);
        ud_plan_destroy(plan);
        ud_free(schema);
        ud_free(commands);
    } else if (operation == 2) {
        char *path = NULL, *config = NULL, *arg = NULL;
        uint64_t caps = 0, op = 0, has_input = 0;
        ud_plugin *plugin = NULL;
        ud_plugin_instance *instance = NULL;
        s = ud_get_string(b, &path);
        if (!s)
            s = ud_get_string(b, &config);
        if (!s)
            s = ud_get_u64(b, &caps);
        if (!s)
            s = ud_get_u64(b, &op);
        if (!s)
            s = ud_get_string(b, &arg);
        if (!s)
            s = ud_get_u64(b, &has_input);
        if (!s && has_input > 1)
            s = UD_CORRUPT;
        if (!s && has_input)
            s = ud_decode_batch(b, &input);
        if (!s && b->pos != b->size)
            s = UD_CORRUPT;
        if (!s)
            s = ud_plugin_load(ctx, path, caps, &plugin);
        if (!s)
            s = ud_plugin_create(plugin, config, (uint32_t)strlen(config), &instance);
        if (!s)
            s = ud_plugin_execute(instance, (int32_t)op, input, arg, (uint32_t)strlen(arg), out);
        ud_plugin_instance_destroy(instance);
        ud_plugin_unload(plugin);
        ud_free(path);
        ud_free(config);
        ud_free(arg);
    } else
        s = UD_UNSUPPORTED;
    ud_batch_destroy(input);
    return s;
}
int main(void) {
#ifdef _WIN32
    _setmode(_fileno(stdin), _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
#endif
    for (;;) {
        uint8_t *raw = NULL;
        uint32_t length = 0;
        int frame = read_frame(&raw, &length);
        if (frame <= 0)
            return frame == 0 ? 0 : 2;
        ud_context *ctx = NULL;
        ud_status s = ud_context_create(NULL, &ctx);
        if (s) {
            free(raw);
            return 3;
        }
        ud_bytes request = {.ctx = ctx, .data = raw, .size = length, .capacity = length},
                 reply = {.ctx = ctx};
        ud_batch *out = NULL;
        s = process(ctx, &request, &out);
        if (s && out) {
            ud_batch_destroy(out);
            out = NULL;
        }
        ud_status encode = ud_put_u64(&reply, (uint64_t)s);
        const char *error = s ? ud_error(ctx) : "";
        if (!encode)
            encode = ud_put_string(&reply, error, strlen(error));
        if (!encode)
            encode = ud_put_u64(&reply, out ? 1 : 0);
        if (!encode && out)
            encode = ud_encode_batch(&reply, out);
        if (reply.size > 64 * 1024 * 1024)
            encode = UD_RESOURCE;
        uint32_t n = (uint32_t)reply.size;
        uint8_t h[4] = {(uint8_t)n, (uint8_t)(n >> 8), (uint8_t)(n >> 16), (uint8_t)(n >> 24)};
        if (!encode && (fwrite(h, 1, 4, stdout) != 4 ||
                        fwrite(reply.data, 1, reply.size, stdout) != reply.size || fflush(stdout)))
            encode = UD_IO;
        ud_batch_destroy(out);
        ud_bytes_clear(&reply);
        free(raw);
        ud_status clean = ud_context_destroy(ctx);
        if (encode || clean)
            return 4;
    }
}
