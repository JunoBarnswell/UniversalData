#ifndef UD_COLUMNAR_INTERNAL_H
#define UD_COLUMNAR_INTERNAL_H
#include "ud/data.h"
#include "ud/json.h"
#include "../foundation/internal.h"
#include <stdatomic.h>
typedef struct ud_column {
    ud_field field;
    uint8_t *validity;
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
    uint32_t columns, rows, capacity;
    ud_column *column;
    ud_schema_owner *schema;
    struct ud_batch *data_owner; /* Sealed projection retains original buffers. */
};
ud_status ud_schema_owner_create(ud_context *, const ud_field *, uint32_t, ud_string,
                                 ud_schema_owner **);
ud_status ud_schema_owner_retain(ud_schema_owner *);
void ud_schema_owner_release(ud_schema_owner *);
ud_status ud_field_copy(ud_context *, const ud_field *, ud_column_id parent, ud_field *);
void ud_field_clear(ud_field *);
ud_status ud_field_derive(ud_context *, const ud_field *, ud_string name_space,
                          ud_string output_name, ud_field *);
ud_status ud_field_cast_metadata(ud_context *, const ud_field *, const ud_field *, ud_field *);
ud_status ud_lineage_merge(ud_context *, const ud_field *, uint32_t, ud_column_id **, uint32_t *);
ud_status ud_batch_create_named(ud_context *, const ud_field *, uint32_t, uint32_t, ud_string,
                                ud_batch **);
ud_status ud_json_value_decode(ud_context *, const ud_field *, const ud_json *, int32_t,
                               ud_value *);
ud_status ud_json_value_encode(ud_bytes *, const ud_field *, const ud_value *);
ud_status ud_batch_clone(const ud_batch *, ud_batch **);
ud_status ud_encode_batch(ud_bytes *, const ud_batch *);
ud_status ud_decode_batch(ud_bytes *, ud_batch **);
ud_status ud_copy_value(ud_context *, const ud_value *, ud_value *, uint32_t depth);
void ud_clear_value(ud_value *);
ud_status ud_cast_value(ud_context *, const ud_field *, const ud_value *, const ud_field *,
                        int32_t rounding, ud_value *, uint32_t depth);
#endif
