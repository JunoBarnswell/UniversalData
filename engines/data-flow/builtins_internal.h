#ifndef UD_BUILTINS_INTERNAL_H
#define UD_BUILTINS_INTERNAL_H
#include "../package/internal.h"
#include "ud/connection_manager.h"
#include "ud/compute.h"
#include "ud/operator.h"
#include "ud/format.h"
#include "ud/csv.h"
#include "ud/xlsx.h"
#include "../../connectors/rest.h"
#ifdef UD_WITH_ARROW_FILES
#include "ud/columnar_file.h"
#endif
#include "../../components/data/cursor_internal.h"
#include <errno.h>
#include <sys/stat.h>
enum {
    CSV_SOURCE = 1,
    JSON_SOURCE,
    SQL_SOURCE,
    CSV_SINK,
    JSON_SINK,
    SQL_SINK,
    PASS,
    CAST,
    DERIVE,
    SPLIT,
    MULTICAST,
    UNION_ALL,
    SORT,
    JOIN,
    AGGREGATE,
    WINDOW,
    XLSX_SOURCE,
    XLSX_SINK,
    REST_SOURCE,
    REST_SINK,
    PARQUET_SOURCE,
    PARQUET_SINK,
    IPC_SOURCE,
    IPC_SINK,
    IPC_STREAM_SOURCE,
    IPC_STREAM_SINK
};

static inline int file_source(int k) {
    return k == CSV_SOURCE || k == JSON_SOURCE || k == XLSX_SOURCE || k == PARQUET_SOURCE ||
           k == IPC_SOURCE || k == IPC_STREAM_SOURCE;
}
static inline int source_kind(int k) {
    return k <= SQL_SOURCE || file_source(k) || k == REST_SOURCE;
}
static inline int file_sink(int k) {
    return k == CSV_SINK || k == JSON_SINK || k == XLSX_SINK || k == PARQUET_SINK ||
           k == IPC_SINK || k == IPC_STREAM_SINK;
}
static inline int sink_kind(int k) {
    return file_sink(k) || k == SQL_SINK || k == REST_SINK;
}
typedef struct builtin {
    int kind;
    ud_component_context context;
    ud_json *config;
    ud_batch *schema, **inputs, *errors;
    uint32_t input_count;
    ud_operator *operator;
    ud_batch *output_pending;
    ud_batch *source_errors;
    uint32_t output_at;
    uint32_t output_port;
    ud_record_options records;
    ud_expression *predicate;
    ud_string read_sql;
    int predicate_pushed;
    ud_rest_reader *rest_reader;
    ud_field *fields;
    uint32_t columns;
    ud_expression **expressions;
    uint32_t expression_count;
    ud_connection_lease *lease;
    ud_csv_reader *csv;
    ud_csv_writer *csv_writer;
    ud_json_reader *reader;
    ud_json_writer *writer;
    ud_xlsx_reader *xlsx;
    ud_xlsx_writer *xlsx_writer;
#ifdef UD_WITH_ARROW_FILES
    ud_columnar_reader *columnar_reader;
    ud_columnar_writer *columnar_writer;
#endif
    uint64_t stage_bytes;
    int32_t xlsx_rejected;
    char *path, *temporary, *job;
    void *lock;
    int published, existing, temporary_owned;
    uint64_t number, source_position, source_begin, source_end, written_rows, written_errors;
    uint32_t source_partitions, source_partition;
    uint32_t *keys, key_count, *write_indices, before_column;
    int mode, load_mode;
    struct ud_rule_delivery *delivery;
    ud_cursor *cursor;
    ud_batch *actual_schema, *write_schema, *parameters;
    ud_operator *source_sort;
    ud_buffer_block **snapshot;
    uint32_t *snapshot_ports;
    uint32_t snapshot_capacity;
    uint64_t snapshot_count, snapshot_at, snapshot_rows, snapshot_error_rows, source_identified,
        snapshot_read_ms;
    int materialized;
    uint8_t file_fingerprint[32];
    uint64_t file_hash_ms;
    ud_sha256 snapshot_hash;
    uint8_t snapshot_fingerprint[32];
    ud_id source_scope;
    char source_policy[65];
    char *schema_diff;
    ud_string schema_policy;
} builtin;

ud_status ud_builtin_bind_expr(const ud_component_context *c, const ud_json *value,
                               const ud_batch *schema, ud_expression **out);
ud_status ud_builtin_blocking(builtin *b);
uint64_t ud_builtin_builtin_demand(void *ptr);
ud_status ud_builtin_builtin_ports(void *user, const ud_component_context *c, ud_string text,
                                   ud_component_ports *ports);
ud_status ud_builtin_cast_rows(builtin *b, const ud_batch *input);
ud_status ud_builtin_checkpoint_state(void *ptr, ud_batch **out);
ud_status ud_builtin_clone_schema(ud_context *ctx, const ud_batch *in, ud_batch **out);
ud_status ud_builtin_configure(void *user, const ud_component_context *c, ud_string text,
                               const ud_batch *const *in, ud_batch **out);
ud_status ud_builtin_confirm(void *ptr);
ud_status ud_builtin_connection_defined(const ud_component_context *ctx, ud_string id);
ud_status ud_builtin_create_operator(const ud_component_context *c, int kind, const ud_json *config,
                                     const ud_batch *const *inputs, ud_operator **out);
ud_status ud_builtin_describe_operator(void *user, const ud_component_context *c, ud_string text,
                                       const ud_batch *const *inputs, ud_component_plan *out);
ud_status ud_builtin_describe_source(void *user, const ud_component_context *c, ud_string text,
                                     const ud_batch *const *in, ud_component_plan *plan);
ud_status ud_builtin_diagnostics(void *ptr, char **out, uint32_t *length);
void ud_builtin_dispose(void *ptr, ud_status result);
ud_status ud_builtin_operator_check(void *user);
ud_status ud_builtin_parameters(const ud_component_context *c, const ud_json *config,
                                ud_batch **out);
ud_status ud_builtin_parse(const ud_component_context *c, ud_string config, ud_json **out);
ud_status ud_builtin_restore_state(void *ptr, const ud_batch *state);
ud_status ud_builtin_schema_config(const ud_component_context *c, const ud_json *config,
                                   ud_batch **out);
ud_status ud_builtin_sink_batch(builtin *b, const ud_batch *input, ud_batch **out,
                                ud_batch **before);
ud_status ud_builtin_sink_schema(builtin *b);
ud_status ud_builtin_sink_write(builtin *b, const ud_batch *input, const ud_batch *before);
ud_status ud_builtin_snapshot_store(builtin *b, ud_batch *batch, uint32_t port);
ud_status ud_builtin_source_identity(builtin *b, ud_batch *input, ud_batch **out);
ud_status ud_builtin_source_next(builtin *b, ud_batch **out);
ud_status ud_builtin_source_partition(builtin *b, ud_batch *input, ud_batch **out);
ud_status ud_builtin_source_prepare(builtin *b, uint32_t batch_rows);
ud_status ud_builtin_source_raw(builtin *b, ud_batch **out);
ud_status ud_builtin_source_sealed(builtin *b, const uint8_t digest[32]);
ud_status ud_builtin_sql_task(void *user, const ud_component_context *c, ud_string text);
ud_status ud_builtin_sql_task_configure(void *user, const ud_component_context *c, ud_string text,
                                        const ud_batch *const *inputs, ud_batch **outputs);
ud_status ud_builtin_transformed(builtin *b, const ud_batch *in);
ud_status ud_pipeline_write(const ud_component_context *c, ud_connection_lease *lease,
                            const ud_write_options *options, const ud_batch *input);
ud_status ud_builtin_source_finish(builtin *);
ud_status ud_builtin_sink_finish(builtin *);
#endif
