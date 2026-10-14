#ifndef UD_NETWORK_INTERNAL_H
#define UD_NETWORK_INTERNAL_H
#include "../engines/package/internal.h"
#include "ud/connection_manager.h"
enum {
    UD_NETWORK_FTP = 1,
    UD_NETWORK_SFTP,
    UD_NETWORK_SMTP,
    UD_NETWORK_AZURE_BLOB,
    UD_NETWORK_ADLS,
    UD_NETWORK_S3
};
typedef struct ud_network_options {
    int protocol, upload, overwrite, require_tls;
    const char *path;
    ud_connection_lease *file;
    ud_string content_type, ca, sender, message;
    const ud_json *recipients;
    uint64_t timeout, maximum;
} ud_network_options;
typedef struct ud_network_result {
    int attempted, completed;
    int32_t status;
    uint64_t bytes;
} ud_network_result;
/* The existing curl transport owns HTTP, FTP/SFTP, SMTP and object transfer;
 * native tasks own intent, staging and publication. No shell or host transfer. */
ud_status ud_network_call(const ud_component_context *, ud_connection_lease *,
                          const ud_network_options *, ud_network_result *);
#endif
