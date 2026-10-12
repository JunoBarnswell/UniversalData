#include "common.h"
#include <math.h>
static ud_status initialize(const ud_host_api *h, const char *s, uint32_t n, void **out) {
    (void)s;
    *out = NULL;
    if (n)
        return UD_INVALID;
    ud_host_api *copy = h->allocate(h->context, sizeof(*copy));
    if (!copy)
        return UD_RESOURCE;
    *copy = *h;
    *out = copy;
    return UD_OK;
}
static ud_status execute(void *ptr, int32_t op, const ud_batch *b, const char *arg, uint32_t n,
                         ud_batch **out) {
    (void)arg;
    ud_host_api *h = ptr;
    *out = NULL;
    if (op != UD_OP_SCALAR || n || !b || h->columns(b) != 1)
        return UD_INVALID;
    ud_field f;
    h->field(b, 0, &f);
    if (f.type != UD_INT64 && f.type != UD_FLOAT64)
        return UD_UNSUPPORTED;
    ud_status s = h->batch_create(h->context, &f, 1, h->rows(b), out);
    for (uint32_t r = 0; !s && r < h->rows(b); r++) {
        if (h->cancelled(h->context)) {
            s = UD_CANCELLED;
            break;
        }
        ud_value v;
        h->get(b, r, 0, &v);
        if (!v.is_null) {
            if (v.type == UD_INT64) {
                if (v.integer == INT64_MIN) {
                    s = h->report(h->context, UD_OVERFLOW, "ABS int64 overflow");
                    break;
                }
                if (v.integer < 0)
                    v.integer = -v.integer;
            } else
                v.real = fabs(v.real);
        }
        s = h->append(*out, &v, 1);
    }
    return s;
}
static void destroy(void *p) {
    if (p) {
        ud_host_api *h = p;
        h->deallocate(p);
    }
}
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){sizeof(*out),
                           UD_PLUGIN_ABI,
                           "org.universaldata.function.abs",
                           "1.0.0",
                           UD_PLUGIN_FUNCTION,
                           UD_CAP_SCALAR,
                           initialize,
                           execute,
                           destroy,
                           plugin_shutdown,
                           NULL, NULL};
    return UD_OK;
}
