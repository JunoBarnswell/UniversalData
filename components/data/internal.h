#ifndef UD_COLUMNAR_INTERNAL_H
#define UD_COLUMNAR_INTERNAL_H
#include "ud/data.h"
#include "ud/json.h"
#include "ud/error.h"
#include "../foundation/internal.h"
#include <stdatomic.h>
typedef struct ud_column {
    ud_field field;
    uint8_t *validity;
    uint8_t *absence; /* Optional bitmap, all cells present when NULL. */
    void *values;
    int32_t *offsets;
    char *strings;
    size_t string_bytes, string_capacity;
    ud_batch *nested;
    ud_value **views;
} ud_column;
typedef struct ud_schema_owner {
    atomic_uint references;
    ud_context *ctx;
    ud_field *fields;
    uint32_t columns;
} ud_schema_owner;
struct ud_batch {
    atomic_uint references;
    int sealed;
    ud_context *ctx;
    uint32_t columns, rows, capacity, row_offset, owner_first;
    ud_column *column;
    ud_schema_owner *schema;
    ud_row_info *row_info;
    ud_value *append_values;     /* Mutable batch-local workspace; released on seal. */
    struct ud_batch *data_owner; /* Sealed projection retains original buffers. */
};
typedef ud_status (*ud_storage_visit)(void *, const void *allocation, uint64_t bytes);
ud_status ud_batch_visit_storage(const ud_batch *, ud_storage_visit, void *);
ud_status ud_schema_owner_create(ud_context *, const ud_field *, uint32_t, ud_string,
                                 ud_schema_owner **);
ud_status ud_schema_owner_retain(ud_schema_owner *);
void ud_schema_owner_release(ud_schema_owner *);
ud_status ud_field_copy(ud_context *, const ud_field *, ud_id parent, ud_field *);
void ud_field_clear(ud_field *);
ud_status ud_dictionary_validate(const ud_field *, const ud_value *);
ud_status ud_dictionary_parse(ud_context *, const ud_json *, ud_field *);
ud_status ud_dictionary_json(ud_bytes *, const ud_field *);
void ud_dictionary_clear(ud_field *);
ud_status ud_record_options_validate(ud_context *, const ud_record_options *);
ud_status ud_infer_scalar(ud_context *, ud_field *, uint8_t *negative_integer,
                         const char *, uint32_t, int json_kind);
ud_status ud_infer_schema(ud_context *, ud_field *, uint32_t, ud_string, ud_batch **);
ud_status ud_field_derive(ud_context *, const ud_field *, ud_string name_space,
                          ud_string output_name, ud_field *);
ud_status ud_field_cast_metadata(ud_context *, const ud_field *, const ud_field *, ud_field *);
ud_status ud_lineage_merge(ud_context *, const ud_field *, uint32_t, ud_id **, uint32_t *);
ud_status ud_batch_create_named(ud_context *, const ud_field *, uint32_t, uint32_t, ud_string,
                                ud_batch **);
ud_status ud_json_value_decode(ud_context *, const ud_field *, const ud_json *, int32_t,
                               ud_value *);
ud_status ud_json_value_encode(ud_bytes *, const ud_field *, const ud_value *);
ud_status ud_batch_copy_row_info(ud_batch *, uint32_t, const ud_batch *, uint32_t);
ud_status ud_batch_join_row_info(ud_batch *, uint32_t, const ud_batch *, uint32_t, const ud_batch *,
                                 uint32_t);
ud_status ud_batch_clone(const ud_batch *, ud_batch **);
ud_status ud_encode_batch(ud_bytes *, const ud_batch *);
ud_status ud_decode_batch(ud_bytes *, ud_batch **);
/* Temporary Buffer Manager pages use a private Wire layout. */
ud_status ud_batch_serialize_spill(const ud_batch *, uint8_t **, uint64_t *);
ud_status ud_batch_deserialize_spill(ud_context *, const uint8_t *, uint64_t, ud_batch **);
ud_status ud_copy_value(ud_context *, const ud_value *, ud_value *, uint32_t depth);
void ud_clear_value(ud_value *);
ud_status ud_cast_value_base(ud_context *, const ud_field *, const ud_value *, const ud_field *,
                             int32_t rounding, ud_value *, uint32_t depth);
ud_status ud_cast_value(ud_context *, const ud_field *, const ud_value *, const ud_field *, int32_t,
                        ud_value *, uint32_t);
ud_status ud_parse_field_base(ud_context *, const ud_field *, const char *, uint32_t, int32_t,
                              ud_value *);
ud_status ud_scalar_field_validate(const ud_field *);
uint32_t ud_scalar_text_prefix(const ud_field *, const char *, uint32_t);
ud_status ud_scalar_text_bytes(ud_context *,const ud_field *,ud_string,uint32_t *);
ud_status ud_scalar_text(ud_context *, const ud_field *, ud_string, int truncate, char **,
                         uint32_t *);
ud_status ud_value_canonical(const ud_value *, ud_status (*)(void *, const void *, size_t), void *,
                             uint32_t);
ud_status ud_batch_hash_rows(const ud_batch *, ud_sha256 *);
ud_status ud_batch_hash_tagged_rows(const ud_batch *, ud_sha256 *, uint8_t port);
void ud_sql_lookup_ordinal_name(const ud_batch *, char name[48]);

/* Native SQL session compilation; parameters are an owned, sealed one-row batch. */
ud_status ud_sql_validate_session_command(ud_string, int);
ud_status ud_sql_bind_command(ud_context *, int32_t, int32_t, ud_string, const ud_batch *,
                              ud_string *, ud_batch **);
ud_status ud_sql_compile_table_query(ud_context *, int32_t, int32_t, ud_string,
                                     const ud_batch *, const ud_batch *, ud_string *, ud_batch **);

#endif
