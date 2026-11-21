#ifndef UD_CONNECTOR_S3_OBJECT_H
#define UD_CONNECTOR_S3_OBJECT_H

#include "ud/component.h"
#include "ud/connection_manager.h"

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* S3 object locations are canonical s3:// URIs.  The connection lease remains
 * the authority for the HTTPS path-style endpoint, credential scope and tenant
 * grant; callers never provide an alternate endpoint. */
ud_status ud_s3_object_root(ud_context *, ud_connection_lease *, ud_string *root);
ud_status ud_s3_object_authorize(const ud_component_context *, ud_connection_lease *,
                                 ud_string location, int32_t action);
ud_status ud_s3_object_head(const ud_component_context *, ud_connection_lease *,
                            ud_string location, uint64_t *size, char digest[65]);
ud_status ud_s3_object_read(const ud_component_context *, ud_connection_lease *,
                            ud_string location, uint64_t offset, uint8_t *buffer,
                            uint64_t length, uint64_t *received);
ud_status ud_s3_object_upload(const ud_component_context *, ud_connection_lease *,
                              ud_string location, FILE *stage, int overwrite,
                              char digest[65]);
ud_status ud_s3_object_delete(const ud_component_context *, ud_connection_lease *,
                              ud_string location);

#ifdef __cplusplus
}
#endif
#endif
