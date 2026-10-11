#ifndef UD_ETL_H
#define UD_ETL_H
#include "compute.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_job ud_job;
enum { UD_STATE_WRITTEN = 1, UD_OUTPUT_WRITTEN = 2, UD_BEFORE_COMMIT = 3, UD_AFTER_COMMIT = 4 };
typedef void (*ud_checkpoint_observer)(void *user, int32_t event, uint64_t generation);
typedef struct ud_job_options {
    uint32_t struct_size, abi_version;
    const char *input_directory, *state_directory;
    const char *schema;
    uint32_t schema_length;
    const char *commands;
    uint32_t commands_length;
    const char *null_marker;
    uint32_t null_marker_length;
    uint32_t batch_size, checkpoint_rows, checkpoint_ms, poll_ms;
    ud_checkpoint_observer observer;
    void *observer_user;
} ud_job_options;
/* Owns a plan copy and exclusive state lock. Input and state paths must be absolute. */
UD_API ud_status ud_job_create(ud_context *, const ud_job_options *, ud_job **);
/* Poll processes the currently discoverable files and commits at the end.
 * run watches until cancellation; any other error poisons this job. */
UD_API ud_status ud_job_poll(ud_job *);
UD_API ud_status ud_job_run(ud_job *);
UD_API uint64_t ud_job_generation(const ud_job *);
UD_API void ud_job_destroy(ud_job *);
/* Caller frees returned absolute snapshot path using ud_free. Checks both hashes. */
UD_API ud_status ud_snapshot_latest(ud_context *, const char *state_directory, char **csv_path,
                                    uint64_t *generation);
#ifdef __cplusplus
}
#endif
#endif
