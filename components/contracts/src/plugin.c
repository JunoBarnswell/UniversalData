#include "ud/plugin.h"
#include "../../foundation/internal.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif
struct ud_plugin {
    ud_context *ctx;
    void *library;
    ud_plugin_api api;
    ud_host_api host;
    uint32_t live, active;
};
struct ud_plugin_instance {
    ud_plugin *plugin;
    void *state;
    int busy;
};
static ud_status report(ud_context *ctx, ud_status s, const char *message) {
    return ud_fail(ctx, s, "%s", message ? message : "plugin failure");
}
static void close_library(void *lib) {
#ifdef _WIN32
    if (lib)
        FreeLibrary((HMODULE)lib);
#else
    if (lib)
        dlclose(lib);
#endif
}
ud_status ud_plugin_load(ud_context *ctx, const char *path, uint64_t caps, ud_plugin **out) {
    if (!path || !out)
        return UD_INVALID;
    *out = NULL;
#ifdef _WIN32
    if (strlen(path) < 3 || !((path[1] == ':' && (path[2] == '/' || path[2] == '\\')) ||
                              (path[0] == '\\' && path[1] == '\\')))
        return UD_INVALID;
#else
    if (path[0] != '/')
        return UD_INVALID;
#endif
    ud_plugin *p = ud_alloc(ctx, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->ctx = ctx;
    ud_plugin_entry_fn entry = NULL;
    unsigned long loader_error = 0;
#ifdef _WIN32
    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, NULL, 0);
    wchar_t *wide = len ? ud_alloc(ctx, (size_t)len * sizeof(wchar_t)) : NULL;
    if (wide) {
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, wide, len);
        /* LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR requires a native absolute path;
         * CMake and external consumers commonly supply forward slashes. */
        for (int i = 0; i < len; i++)
            if (wide[i] == L'/')
                wide[i] = L'\\';
        p->library = LoadLibraryExW(
            wide, NULL, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS);
        ud_free(wide);
    }
    if (p->library) {
        FARPROC symbol = GetProcAddress((HMODULE)p->library, "ud_plugin_get_api");
        memcpy(&entry, &symbol, sizeof(entry));
    }
    if (!entry)
        loader_error = GetLastError();
#else
    p->library = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (p->library) {
        void *symbol = dlsym(p->library, "ud_plugin_get_api");
        memcpy(&entry, &symbol, sizeof(entry));
    }
#endif
    if (!entry) {
        close_library(p->library);
        ud_free(p);
        return ud_fail(ctx, UD_IO, "plugin library or entry point missing (OS error %lu): %s",
                       loader_error, path);
    }
    p->api.struct_size = sizeof(p->api);
    p->api.abi_version = UD_PLUGIN_ABI;
    ud_status s = entry(UD_PLUGIN_ABI, &p->api);
    if (!s && (p->api.struct_size < sizeof(p->api) || p->api.abi_version != UD_PLUGIN_ABI ||
               !p->api.id || !p->api.version || !p->api.initialize || !p->api.execute ||
               !p->api.destroy || !p->api.shutdown))
        s = UD_INCOMPATIBLE;
    if (!s && (p->api.capabilities & caps) != caps)
        s = UD_UNSUPPORTED;
    if (s) {
        close_library(p->library);
        ud_free(p);
        return ud_fail(ctx, s, "plugin ABI/capabilities incompatible: %s", path);
    }
    p->host = (ud_host_api){sizeof(ud_host_api),
                            1,
                            ctx,
                            ud_alloc,
                            ud_free,
                            ud_batch_rows,
                            ud_batch_columns,
                            ud_batch_field,
                            ud_batch_get,
                            ud_batch_create,
                            ud_batch_append,
                            ud_batch_destroy,
                            ud_csv_open,
                            ud_csv_next,
                            ud_csv_close,
                            ud_csv_write,
                            ud_is_cancelled,
                            report};
    *out = p;
    return UD_OK;
}
ud_status ud_plugin_info(const ud_plugin *p, ud_plugin_api *out) {
    if (!p || !out || out->struct_size < sizeof(*out))
        return UD_INVALID;
    *out = p->api;
    return UD_OK;
}
ud_status ud_plugin_create(ud_plugin *p, const char *config, uint32_t n, ud_plugin_instance **out) {
    if (!p || !out || (!config && n) || p->active)
        return UD_INVALID;
    *out = NULL;
    ud_plugin_instance *instance = ud_alloc(p->ctx, sizeof(*instance));
    if (!instance)
        return UD_RESOURCE;
    p->active++;
    ud_status s = p->api.initialize(&p->host, config ? config : "", n, &instance->state);
    p->active--;
    if (s) {
        if (instance->state)
            p->api.destroy(instance->state);
        ud_free(instance);
        return s;
    }
    instance->plugin = p;
    p->live++;
    *out = instance;
    return UD_OK;
}
static uint64_t operation_cap(int32_t op) {
    switch (op) {
    case UD_OP_READ:
        return UD_CAP_READ;
    case UD_OP_WRITE:
        return UD_CAP_WRITE;
    case UD_OP_SCALAR:
        return UD_CAP_SCALAR;
    case UD_OP_SQL:
        return UD_CAP_SQL;
    case UD_OP_PARQUET_READ:
    case UD_OP_PARQUET_WRITE:
        return UD_CAP_PARQUET;
    default:
        return 0;
    }
}
ud_status ud_plugin_execute(ud_plugin_instance *i, int32_t op, const ud_batch *in, const char *arg,
                            uint32_t n, ud_batch **out) {
    if (!i || !out || (!arg && n))
        return UD_INVALID;
    *out = NULL;
    ud_plugin *p = i->plugin;
    if (i->busy || p->active)
        return UD_BUSY;
    uint64_t cap = operation_cap(op);
    if (!cap || !(p->api.capabilities & cap))
        return ud_fail(p->ctx, UD_UNSUPPORTED, "plugin does not support operation %d", op);
    if (ud_is_cancelled(p->ctx))
        return UD_CANCELLED;
    i->busy = 1;
    p->active++;
    ud_status s = p->api.execute(i->state, op, in, arg ? arg : "", n, out);
    p->active--;
    i->busy = 0;
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_plugin_instance_destroy(ud_plugin_instance *i) {
    if (!i)
        return UD_OK;
    if (i->busy || i->plugin->active)
        return UD_BUSY;
    ud_plugin *p = i->plugin;
    p->active++;
    p->api.destroy(i->state);
    p->active--;
    p->live--;
    ud_free(i);
    return UD_OK;
}
ud_status ud_plugin_unload(ud_plugin *p) {
    if (!p)
        return UD_OK;
    if (p->live || p->active)
        return ud_fail(p->ctx, UD_BUSY, "plugin has live instances or callbacks");
    p->active++;
    p->api.shutdown();
    close_library(p->library);
    ud_free(p);
    return UD_OK;
}
