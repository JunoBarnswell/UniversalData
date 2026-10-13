#include "../internal.h"

/* Only allocation ownership is visited. No row/value codec or content hashing. */
static ud_status allocation(const void *pointer, ud_storage_visit visit, void *user) {
    return pointer ? visit(user, pointer, ud_allocation_bytes(pointer)) : UD_OK;
}
static ud_status fields(const ud_field *metadata, uint32_t count, ud_storage_visit visit,
                        void *user) {
    UD_TRY(allocation(metadata, visit, user));
    for (uint32_t i = 0; i < count; i++) {
        const ud_field *field = metadata + i;
        UD_TRY(allocation(field->name, visit, user));
        UD_TRY(allocation(field->timezone, visit, user));
        UD_TRY(allocation(field->lineage, visit, user));
        UD_TRY(allocation(field->dictionary, visit, user));
        for (uint32_t j = 0; j < field->dictionary_count; j++)
            UD_TRY(allocation(field->dictionary[j].data, visit, user));
        if (field->child_count)
            UD_TRY(fields(field->children, field->child_count, visit, user));
    }
    return UD_OK;
}
ud_status ud_batch_visit_storage(const ud_batch *batch, ud_storage_visit visit, void *user) {
    if (!batch || !batch->sealed || !visit)
        return UD_INVALID;
    UD_TRY(allocation(batch, visit, user));
    UD_TRY(allocation(batch->column, visit, user));
    UD_TRY(allocation(batch->row_info, visit, user));
    UD_TRY(allocation(batch->schema, visit, user));
    UD_TRY(fields(batch->schema->fields, batch->schema->columns, visit, user));
    if (batch->data_owner)
        return ud_batch_visit_storage(batch->data_owner, visit, user);
    for (uint32_t i = 0; i < batch->columns; i++) {
        const ud_column *column = batch->column + i;
        UD_TRY(allocation(column->validity, visit, user));
        UD_TRY(allocation(column->absence, visit, user));
        UD_TRY(allocation(column->values, visit, user));
        UD_TRY(allocation(column->offsets, visit, user));
        UD_TRY(allocation(column->strings, visit, user));
        UD_TRY(allocation(column->views, visit, user));
        if (column->views)
            for (uint32_t row = 0; row < batch->capacity; row++)
                UD_TRY(allocation(column->views[row], visit, user));
        if (column->nested)
            UD_TRY(ud_batch_visit_storage(column->nested, visit, user));
    }
    return UD_OK;
}
