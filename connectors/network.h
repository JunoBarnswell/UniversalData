#ifndef UD_NETWORK_INTERNAL_H
#define UD_NETWORK_INTERNAL_H
#include "../engines/package/internal.h"
#include "ud/connection_manager.h"
#include <stdio.h>
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

/* Internal object requests share the same curl/SigV4 transfer as network
 * tasks.  The object adapter owns URI/root policy; this primitive only owns
 * bounded transfer mechanics and credential setup. */
typedef struct ud_network_s3_options {
    uint32_t struct_size, abi_version;
    int32_t action;
    ud_string method, url, range, body, content_type, idempotency_key,
        if_none_match, metadata_digest, ca;
    FILE *file;
    ud_connection_lease *file_lease;
    uint64_t file_offset, file_size, timeout, maximum;
} ud_network_s3_options;
typedef struct ud_network_s3_result {
    int32_t status, transport_code, attempted, completed;
    uint64_t bytes, content_length;
    ud_string body, headers;
} ud_network_s3_result;
ud_status ud_network_s3_call(const ud_component_context *, ud_connection_lease *,
                             const ud_network_s3_options *, ud_network_s3_result *);
void ud_network_s3_result_clear(ud_network_s3_result *);
/* The existing curl transport owns HTTP, FTP/SFTP, SMTP and object transfer;
 * native tasks own intent, staging and publication. No shell or host transfer. */
ud_status ud_network_call(const ud_component_context *, ud_connection_lease *,
                          const ud_network_options *, ud_network_result *);
#endif
