#include "common.h"
typedef struct {
    ud_host_api host;
    char *path;
    ud_csv_reader *reader;
} csv_state;
static ud_status initialize(const ud_host_api *h, const char *s, uint32_t n, void **out) {
    *out = NULL;
    if (!n)
        return UD_INVALID;
    csv_state *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    p->path = plugin_string(h, s, n);
    if (!p->path) {
        h->deallocate(p);
        return UD_INVALID;
    }
    *out = p;
    return UD_OK;
}
static ud_status execute(void *ptr, int32_t op, const ud_batch *b, const char *arg, uint32_t n,
                         ud_batch **out) {
    (void)arg;
    if (n || !b)
        return UD_INVALID;
    csv_state *p = ptr;
    ud_host_api *h = &p->host;
    *out = NULL;
    if (op == UD_OP_WRITE)
        return h->csv_write(h->context, p->path, b, "\\N");
    if (op != UD_OP_READ)
        return UD_UNSUPPORTED;
    if (!p->reader) {
        ud_field *f = NULL;
        ud_status s = plugin_fields(h, b, &f);
        if (!s)
            s = h->csv_open(h->context, p->path, f, h->columns(b), NULL, &p->reader);
        h->deallocate(f);
        if (s)
            return s;
    }
    return h->csv_next(p->reader, out);
}
static void destroy(void *ptr) {
    csv_state *p = ptr;
    if (!p)
        return;
    p->host.csv_close(p->reader);
    p->host.deallocate(p->path);
    p->host.deallocate(p);
}
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){sizeof(*out),
                           UD_PLUGIN_ABI,
                           "org.universaldata.format.csv",
                           "1.0.0",
                           UD_PLUGIN_FORMAT | UD_PLUGIN_SOURCE | UD_PLUGIN_SINK,
                           UD_CAP_READ | UD_CAP_WRITE,
                           initialize,
                           execute,
                           destroy,
                           plugin_shutdown,
                           NULL, NULL};
    return UD_OK;
}
