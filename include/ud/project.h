#ifndef UD_PROJECT_H
#define UD_PROJECT_H
#include "package.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_project ud_project;
typedef struct ud_project_binding {
    uint32_t struct_size, abi_version;
    uint64_t package_revision;
    ud_string environment;                            /* Optional version-3 Environment document. */
    ud_string project_parameters, package_parameters; /* Explicit overrides. */
} ud_project_binding;
/* Project contains immutable Package revision references, shared connection
 * definitions and typed outer-scope parameters/variables. No executor here. */
UD_API ud_status ud_project_create(ud_context *, ud_string, ud_project **);
UD_API ud_status ud_project_serialize(const ud_project *, char **, uint32_t *);
UD_API void ud_project_destroy(ud_project *);
/* Binds exact reference + Environment + explicit overrides into one native
 * Package. Parameters precedence: explicit > Environment > definition default.
 * Owned outputs use the Project context; text released with ud_free. Bound
 * Package includes its immutable outer project scope; caller passes returned
 * parameter JSON to the existing Execution API. Input Package is unchanged. */
UD_API ud_status ud_project_bind(const ud_project *, const ud_package *, const ud_project_binding *,
                                 ud_package **, char **package_parameters, uint32_t *length);
#ifdef __cplusplus
}
#endif
#endif
