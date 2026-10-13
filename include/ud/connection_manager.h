#ifndef UD_CONNECTION_MANAGER_H
#define UD_CONNECTION_MANAGER_H
#include "package.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_connection_lease ud_connection_lease;
/* Uses Package connection definitions, current lexical bindings, authorization
 * and host credential resolver. File/process/HTTP endpoints and SQL drivers share this
 * logical model. Each lease owns a compatible physical connection/cursor. */
UD_API ud_status ud_activation_acquire_connection(ud_activation *, ud_string id,
                                                  uint64_t capabilities, ud_connection_lease **);
/* A retained logical grant uses the same binding/authorization owner without
 * opening a second physical session; used by execution snapshot assets. */
UD_API ud_status ud_activation_acquire_grant(ud_activation *, ud_string, ud_connection_lease **);
UD_API ud_status ud_connection_lease_authorize(ud_connection_lease *, int32_t action);
UD_API ud_status ud_connection_lease_authorize_uri(ud_connection_lease *,ud_string);
UD_API ud_status ud_connection_lease_resource(const ud_connection_lease *, ud_string *);
UD_API ud_status ud_connection_lease_snapshot_grant(ud_connection_lease *, ud_activation *,
                                                    ud_connection_lease **);
UD_API ud_status ud_connection_lease_path(const ud_connection_lease *, ud_string *);
UD_API ud_status ud_connection_lease_cdc_open(ud_connection_lease *,const ud_cdc_options *);
UD_API ud_status ud_connection_lease_cdc_next(ud_connection_lease *,ud_cdc_event *);
UD_API ud_status ud_connection_lease_cdc_confirm(ud_connection_lease *,ud_string);
UD_API ud_status ud_connection_lease_cdc_close(ud_connection_lease *);
UD_API ud_status ud_connection_lease_read_open(ud_connection_lease *, const ud_read_options *,
                                               const ud_batch *);
UD_API ud_status ud_connection_lease_read_next(ud_connection_lease *, ud_batch **);
UD_API ud_status ud_connection_lease_read_close(ud_connection_lease *);
UD_API ud_status ud_connection_lease_digest(ud_connection_lease *, const ud_write_options *,
                                            const ud_batch *, uint8_t out[32]);
UD_API ud_status ud_connection_lease_write(ud_connection_lease *, const ud_write_options *,
                                           const ud_batch *);
UD_API ud_status ud_connection_lease_receipt(ud_connection_lease *, ud_string job,
                                             ud_string batch_id, ud_commit_receipt *);
UD_API ud_status ud_connection_lease_execute_command(ud_connection_lease *, const ud_read_options *,
                                                     uint64_t *);
UD_API ud_status ud_connection_lease_provision(ud_connection_lease *);
UD_API ud_status ud_connection_lease_metadata(ud_connection_lease *, const ud_metadata_options *,
                                              ud_batch **);
UD_API ud_status ud_connection_lease_schema(ud_connection_lease *, ud_string table,
                                            ud_string name_space, ud_batch **);
UD_API ud_status ud_connection_lease_discover(ud_connection_lease *, ud_string table, ud_batch **);
UD_API ud_status ud_connection_lease_release(ud_connection_lease *);
#ifdef __cplusplus
}
#endif
#endif
