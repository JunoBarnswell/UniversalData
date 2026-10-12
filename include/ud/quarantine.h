#ifndef UD_QUARANTINE_H
#define UD_QUARANTINE_H
#include "quality.h"
#include "connector.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_quarantine_store ud_quarantine_store;
enum { UD_ACCESS_QUARANTINE_READ=6, UD_ACCESS_QUARANTINE_WRITE=7 };
typedef struct ud_quarantine_options {
    uint32_t struct_size, version;
    ud_string directory, tenant, job;
    uint8_t plan_fingerprint[32];
    void *authorize_user;
    ud_authorize_fn authorize;
} ud_quarantine_options;
/* Local durable isolation, not a multi-machine coordination store. Scope is
 * copied; absent authorization is denied. Each batch write is protected by an
 * OS file lock and atomic no-replace publication plus file/directory sync. */
UD_API ud_status ud_quarantine_open(ud_context *, const ud_quarantine_options *, ud_quarantine_store **);
/* Set before quality_apply. Same batch ID must retain identical rows,
 * violations, plan and source position; replay mismatch returns CORRUPT. */
UD_API ud_status ud_quarantine_set_batch(ud_quarantine_store *, ud_string batch_id, ud_string source_position);
/* Directly usable as ud_quality_create's durable callback. */
UD_API ud_status ud_quarantine_write(void *store, const ud_batch *, const ud_quality_violation *, uint32_t);
/* Owned outputs. batch uses ud_batch_destroy, violations and position use ud_free.
 * Unknown batch returns END. Corrupt / incompatible state never becomes END. */
UD_API ud_status ud_quarantine_read(ud_quarantine_store *, ud_string batch_id, ud_batch **,
                                    ud_quality_violation **, uint32_t *, ud_string *source_position);
UD_API void ud_quarantine_close(ud_quarantine_store *);
#ifdef __cplusplus
}
#endif
#endif
