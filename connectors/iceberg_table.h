#ifndef UD_ICEBERG_TABLE_H
#define UD_ICEBERG_TABLE_H

#include "ud/http.h"
#include "ud/buffer.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ud_iceberg_table ud_iceberg_table;

typedef struct ud_iceberg_read_options {
    uint32_t struct_size, abi_version;
    int64_t snapshot_id; /* 0 means the table's current snapshot. */
    uint32_t batch_rows;
} ud_iceberg_read_options;

typedef struct ud_iceberg_options {
    uint32_t struct_size, abi_version;
    ud_context *memory;
    const ud_component_context *context;
    ud_buffer_manager *buffers;
    ud_connection_lease *storage_lease;
    void *user;
    ud_string catalog_url, catalog_warehouse, storage_root, namespace_name, table_name;
    ud_status (*request)(void *, const ud_http_options *, ud_http_response *);
    ud_status (*authorize_file)(void *, ud_string location, int32_t action);
    ud_status (*check)(void *);
    ud_status (*before_commit)(void *);
} ud_iceberg_options;

ud_status ud_iceberg_table_open(const ud_iceberg_options *, const ud_batch *, ud_iceberg_table **);
ud_string ud_iceberg_table_uuid(const ud_iceberg_table *);
ud_status ud_iceberg_table_append(ud_iceberg_table *, const ud_batch *, ud_string identity,
                                  ud_string digest, int replay, int committed);
ud_status ud_iceberg_table_apply(ud_iceberg_table *, const ud_batch *, const ud_batch *,
                                 const int32_t *, uint32_t, ud_string identity,
                                 ud_string digest, int replay, int committed);
ud_status ud_iceberg_table_read_open(ud_iceberg_table *, const ud_iceberg_read_options *,
                                     ud_batch **schema);
ud_status ud_iceberg_table_snapshot_id(const ud_iceberg_table *, int64_t *snapshot_id);
ud_status ud_iceberg_table_read_next(ud_iceberg_table *, ud_batch **batch);
ud_status ud_iceberg_table_read_close(ud_iceberg_table *);
void ud_iceberg_table_close(ud_iceberg_table *);

#ifdef __cplusplus
}
#endif
#endif
