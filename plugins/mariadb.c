#include "mysql_family.h"
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t requested, ud_plugin_api *out) {
    if (requested != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    ud_status status = ud_mysql_library_initialize();
    if (status)
        return status;
    *out = (ud_plugin_api){.struct_size = sizeof(*out),
                           .abi_version = UD_PLUGIN_ABI,
                           .field_size=sizeof(ud_field),.value_size=sizeof(ud_value),
                           .id = UD_MYSQL_ID,
                           .version = UD_SDK_VERSION,
                           .roles = UD_PLUGIN_CONNECTOR,
                           .capabilities = UD_CAP_READ | UD_CAP_WRITE | UD_CAP_SQL |
                                           UD_CAP_TRANSACTION | UD_CAP_THREAD_SAFE,
                           .initialize = ud_mysql_initialize,
                           .destroy = ud_mysql_destroy,
                           .shutdown = ud_mysql_library_shutdown,
                           .connector = &ud_mysql_connector};
    return UD_OK;
}
