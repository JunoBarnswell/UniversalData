#ifndef UD_PLUGIN_H
#define UD_PLUGIN_H
#include "data.h"
#include "json.h"
#include "sql.h"
#include "connector.h"
#include "component.h"
#ifdef __cplusplus
extern "C" {
#endif
enum { UD_PLUGIN_CONNECTOR = 1, UD_PLUGIN_COMPONENTS = 2 };
enum {
    UD_CAP_READ = 1,
    UD_CAP_WRITE = 2,
    UD_CAP_SQL = 8,
    UD_CAP_TRANSACTION = 32,
    UD_CAP_THREAD_SAFE = 64,
    UD_CAP_THREAD_AFFINE = 128
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
    int32_t (*cancelled)(const ud_context *);
    ud_status (*report)(ud_context *, ud_status, const char *);
    ud_status (*parse_field)(ud_context *, const ud_field *, const char *, uint32_t, int32_t,
                             ud_value *);
    ud_status (*format_value)(const ud_value *, char *, uint32_t, uint32_t *);
    void *control_user;
    ud_status (*check_io)(void *);
    ud_status (*json_parse)(ud_context *, const char *, uint32_t, ud_json **, uint32_t *);
    void (*json_destroy)(ud_json *);
    const ud_json *(*json_find)(const ud_json *, const char *, uint32_t);
    ud_status (*json_text)(const ud_json *, const char **, uint32_t *);
    int32_t (*json_kind)(const ud_json *);
    uint32_t (*json_size)(const ud_json *);
    const ud_json *(*json_at)(const ud_json *, uint32_t);
    ud_status (*json_keys)(const ud_json *, const char *const *, uint32_t);
    ud_status (*json_bind_schema)(ud_context *, const ud_json *, const ud_json *, ud_json **);
    ud_status (*field_from_sql)(ud_string, int32_t, ud_field *);
    uint32_t (*parameter_limit)(int32_t);
    ud_status (*compile_write)(ud_context *, int32_t, int32_t, const ud_write_options *,
                               const ud_batch *, uint32_t, uint32_t, ud_sql_statement *);
    void (*statement_clear)(ud_sql_statement *);
    int32_t (*compare_value)(const ud_value *, const ud_value *);
    ud_status (*compile_preimage)(ud_context *, int32_t, int32_t, const ud_write_options *,
                                  const ud_batch *, uint32_t, ud_sql_statement *);
    ud_status (*schema_from_metadata)(ud_context *, const ud_batch *, ud_string, int32_t,
                                      ud_batch **);
    ud_status (*bulk_open)(ud_context *, const ud_batch *, int32_t, ud_bulk_reader **);
    ud_status (*bulk_read)(ud_bulk_reader *, void *, uint32_t, uint32_t *);
    void (*bulk_close)(ud_bulk_reader *);
    ud_status (*compile_bulk)(ud_context *, int32_t, const ud_write_options *, const ud_batch *,
                              ud_string *);
    ud_status (*table_identifier)(ud_context *, int32_t, ud_string, ud_string *);
    ud_status (*mysql_json_decode)(ud_context *, const uint8_t *, uint32_t, char **, uint32_t *);
    int32_t (*field_matches)(const ud_field *, const ud_field *, int32_t);
    ud_status (*compile_scan)(ud_context *, int32_t, ud_string, const ud_batch *, ud_string *);
    ud_status (*compile_bulk_release)(ud_context *, int32_t, const ud_batch *, ud_string *);
    ud_status (*compile_stage)(ud_context *, int32_t, const ud_write_options *, const ud_batch *,
                               ud_string, int32_t, ud_string *);
    ud_status (*random_bytes)(ud_context *, void *, uint32_t);
    ud_status (*schema_compare)(ud_context *, const ud_batch *, const ud_batch *, ud_string,
                                char **, uint32_t *);
    ud_status (*schema_map)(ud_context *, const ud_batch *, const ud_batch *, ud_string,
                            ud_batch **);
    ud_status (*schema_bind_identity)(ud_context *, const ud_batch *, const ud_batch *,
                                      ud_batch **);
} ud_host_api;
typedef struct ud_plugin_api {
    uint32_t struct_size, abi_version;
    const char *id, *version;
    uint64_t roles, capabilities;
    /* Connection factories are used only by Connection Manager. Component-only
     * modules omit them; functions are registered as component descriptors. */
    ud_status (*initialize)(const ud_host_api *, const char *configuration, uint32_t length,
                            void **instance);
    void (*destroy)(void *instance);
    void (*shutdown)(void);
    const ud_connector_api *connector; /* Optional current complete table. */
    /* Optional component table in the current complete ABI layout. Table and callback user data
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
UD_API ud_status ud_plugin_load(ud_context *, const char *absolute_library_path,
                                uint64_t required_capabilities, ud_plugin **);
UD_API ud_status ud_plugin_info(const ud_plugin *, ud_plugin_api *);
/* Register the complete exported component table atomically. Plugins are trusted
 * native code; the host authorizes loading, and callbacks use common SDK grants
 * for resources. Missing table is Unsupported, not a no-op success. */
UD_API ud_status ud_plugin_register_components(ud_plugin *, ud_registry *);
UD_API ud_status ud_plugin_unload(ud_plugin *);
#ifdef __cplusplus
}
#endif
#endif
