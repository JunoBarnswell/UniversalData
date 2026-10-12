#ifndef UD_PLUGIN_H
#define UD_PLUGIN_H
#include "csv.h"
#include "connector.h"
#include "component.h"
#ifdef __cplusplus
extern "C" {
#endif
#define UD_PLUGIN_ABI 3u
enum {
    UD_PLUGIN_SOURCE = 1,
    UD_PLUGIN_SINK = 2,
    UD_PLUGIN_TRANSFORM = 4,
    UD_PLUGIN_FUNCTION = 8,
    UD_PLUGIN_BACKEND = 16,
    UD_PLUGIN_FORMAT = 32,
    UD_PLUGIN_COMPONENTS = 64
};
enum {
    UD_CAP_READ = 1,
    UD_CAP_WRITE = 2,
    UD_CAP_SCALAR = 4,
    UD_CAP_SQL = 8,
    UD_CAP_PARQUET = 16,
    UD_CAP_TRANSACTION = 32
};
enum {
    UD_OP_READ = 1,
    UD_OP_WRITE = 2,
    UD_OP_SCALAR = 3,
    UD_OP_SQL = 4,
    UD_OP_PARQUET_READ = 5,
    UD_OP_PARQUET_WRITE = 6
};
typedef struct ud_host_api {
    uint32_t struct_size, abi_version;
    ud_context *context;
    void *(*allocate)(ud_context *, size_t);
    void (*deallocate)(void *);
    uint32_t (*rows)(const ud_batch *), (*columns)(const ud_batch *);
    ud_status (*field)(const ud_batch *, uint32_t, ud_field *);
    ud_status (*get)(const ud_batch *, uint32_t, uint32_t, ud_value *);
    ud_status (*batch_create)(ud_context *, const ud_field *, uint32_t, uint32_t, ud_batch **);
    ud_status (*append)(ud_batch *, const ud_value *, uint32_t);
    void (*batch_destroy)(ud_batch *);
    ud_status (*csv_open)(ud_context *, const char *, const ud_field *, uint32_t,
                          const ud_csv_options *, ud_csv_reader **);
    ud_status (*csv_next)(ud_csv_reader *, ud_batch **);
    void (*csv_close)(ud_csv_reader *);
    ud_status (*csv_write)(ud_context *, const char *, const ud_batch *, const char *);
    int32_t (*cancelled)(const ud_context *);
    ud_status (*report)(ud_context *, ud_status, const char *);
    ud_status (*parse_field)(ud_context *, const ud_field *, const char *, uint32_t, int32_t,
                             ud_value *);
    ud_status (*format_value)(const ud_value *, char *, uint32_t, uint32_t *);
} ud_host_api;
typedef struct ud_plugin_api {
    uint32_t struct_size, abi_version;
    const char *id, *version;
    uint64_t roles, capabilities;
    /* Generic factories remain mandatory for generic/mixed roles. A module
     * whose sole role is COMPONENTS may omit all three and optional shutdown. */
    ud_status (*initialize)(const ud_host_api *, const char *configuration, uint32_t length,
                            void **instance);
    ud_status (*execute)(void *instance, int32_t operation, const ud_batch *input,
                         const char *argument, uint32_t argument_length, ud_batch **output);
    void (*destroy)(void *instance);
    void (*shutdown)(void);
    const ud_connector_api *connector; /* Optional; ABI 3 table, no network dependency in core. */
    /* Optional component table in the current complete ABI3 layout. Table and callback user data
     * belong to the library; registration holds a module reference until registry destruction. */
    const struct ud_component_plugin_api *components;
} ud_plugin_api;
typedef struct ud_component_plugin_api {
    uint32_t struct_size, abi_version, count;
    const ud_component_descriptor *descriptors;
} ud_component_plugin_api;
typedef ud_status (*ud_plugin_entry_fn)(uint32_t requested_abi, ud_plugin_api *);
#if defined(_WIN32)
#define UD_PLUGIN_EXPORT __declspec(dllexport)
#else
#define UD_PLUGIN_EXPORT __attribute__((visibility("default")))
#endif
/* Plugins export ud_plugin_get_api. The host owns the caller-supplied API table. */
typedef struct ud_plugin ud_plugin;
typedef struct ud_plugin_instance ud_plugin_instance;
UD_API ud_status ud_plugin_load(ud_context *, const char *absolute_library_path,
                                uint64_t required_capabilities, ud_plugin **);
UD_API ud_status ud_plugin_info(const ud_plugin *, ud_plugin_api *);
/* Register the complete exported component table atomically. Plugins are trusted
 * native code; the host authorizes loading, and callbacks use common SDK grants
 * for resources. Missing table is Unsupported, not a no-op success. */
UD_API ud_status ud_plugin_register_components(ud_plugin *, ud_registry *);
UD_API ud_status ud_plugin_create(ud_plugin *, const char *, uint32_t, ud_plugin_instance **);
UD_API ud_status ud_plugin_execute(ud_plugin_instance *, int32_t, const ud_batch *, const char *,
                                   uint32_t, ud_batch **);
UD_API ud_status ud_plugin_instance_destroy(ud_plugin_instance *);
UD_API ud_status ud_plugin_unload(ud_plugin *);
#ifdef __cplusplus
}
#endif
#endif
