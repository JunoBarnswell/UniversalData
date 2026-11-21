#ifndef UD_PACKAGE_INCREMENTAL_H
#define UD_PACKAGE_INCREMENTAL_H

#include "internal.h"

typedef struct ud_incremental_plan {
    ud_string task, pipeline, component;
    const ud_json *initial;
    char fingerprint[65];
} ud_incremental_plan;

ud_status ud_incremental_describe(ud_context *, const ud_json *package, const ud_json *parameters,
                                  ud_incremental_plan *);
ud_status ud_incremental_binding_json(ud_bytes *, const ud_incremental_plan *, const ud_json *);
int ud_incremental_source(const ud_activation *, ud_string component);
int ud_incremental_task(const ud_activation *);
ud_status ud_incremental_cursor_policy(const ud_activation *, ud_string component,
                                       const ud_json *, char **, uint32_t *);
ud_status ud_incremental_capture(ud_execution *);
ud_status ud_checkpoint_cursor_read(ud_activation *, ud_string, char **, uint32_t *);

#endif
