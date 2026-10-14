#ifndef UD_REST_INTERNAL_H
#define UD_REST_INTERNAL_H
#include "ud/http.h"
typedef struct ud_rest_reader ud_rest_reader;
typedef struct ud_rest_target ud_rest_target;
ud_status ud_rest_reader_open(const ud_component_context *, ud_connection_lease *, const ud_json *,
                              const ud_batch *, ud_rest_reader **);
ud_status ud_rest_reader_next(ud_rest_reader *, ud_batch **);
void ud_rest_reader_close(ud_rest_reader *);
ud_status ud_rest_target_open(const ud_component_context *, ud_connection_lease *, const ud_json *,
                              ud_rest_target **);
ud_status ud_rest_target_write(ud_rest_target *, const ud_write_options *, const ud_batch *,
                               const uint8_t[32]);
ud_status ud_rest_target_receipt(ud_rest_target *, ud_string, ud_string, ud_commit_receipt *);
ud_status ud_rest_delivery(const ud_json *, int32_t *);
int32_t ud_rest_target_delivery(const ud_rest_target *);
void ud_rest_target_close(ud_rest_target *);
ud_status ud_connection_lease_bind_rest(ud_connection_lease *, const ud_component_context *,
                                        const ud_json *);
#endif
