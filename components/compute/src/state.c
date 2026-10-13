#include "state.h"
static ud_status schema_copy(ud_context *ctx, const ud_batch *source, uint32_t capacity,
                             ud_batch **out) {
    return ud_batch_create(ctx, source->schema->fields, source->columns, capacity, out);
}
ud_status ud_state_create(ud_context *ctx, ud_buffer_manager *buffers, const ud_batch *schema,
                          uint32_t batch_size, ud_state **out) {
    if (!ctx || !buffers || !schema || !batch_size || !out)
        return UD_INVALID;
    *out = ud_alloc(ctx, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->ctx = ctx;
    (*out)->buffers = buffers;
    (*out)->batch_size = batch_size;
    ud_status s = schema_copy(ctx, schema, 0, &(*out)->schema);
    if (s) {
        ud_state_destroy(*out);
        *out = NULL;
    }
    return s;
}
static ud_status flush(ud_state *state) {
    if (!state->chunk || !state->chunk->rows)
        return UD_OK;
    if (state->count == state->capacity) {
        uint32_t capacity = state->capacity ? state->capacity * 2 : 16;
        if (capacity < state->capacity)
            return UD_RESOURCE;
        ud_state_page *pages = ud_alloc(state->ctx, (size_t)capacity * sizeof(*pages));
        if (!pages)
            return UD_RESOURCE;
        if (state->count)
            memcpy(pages, state->pages, (size_t)state->count * sizeof(*pages));
        ud_free(state->pages);
        state->pages = pages;
        state->capacity = capacity;
    }
    UD_TRY(ud_batch_seal(state->chunk));
    ud_buffer_block *block = NULL;
    UD_TRY(ud_buffer_store(state->buffers, state->chunk, &block));
    state->pages[state->count++] =
        (ud_state_page){block, state->rows - state->chunk->rows, state->chunk->rows};
    ud_batch_destroy(state->chunk);
    state->chunk = NULL;
    return UD_OK;
}
ud_status ud_state_append(ud_state *s, const ud_batch *b, uint32_t row) {
    if (!s || !b || s->sealed || b->columns != s->schema->columns || row >= b->rows)
        return UD_INVALID;
    for (uint32_t i = 0; i < b->columns; i++)
        if (!ud_field_matches(&b->column[i].field, &s->schema->column[i].field, UD_FIELD_PORT))
            return UD_SCHEMA;
    if (s->rows == UINT64_MAX)
        return UD_OVERFLOW;
    if (!s->chunk)
        UD_TRY(schema_copy(s->ctx, s->schema, s->batch_size, &s->chunk));
    UD_TRY(ud_batch_append_from(s->chunk, b, row));
    s->rows++;
    return s->chunk->rows == s->batch_size ? flush(s) : UD_OK;
}
ud_status ud_state_seal(ud_state *s) {
    if (!s)
        return UD_INVALID;
    if (s->sealed)
        return UD_OK;
    UD_TRY(flush(s));
    s->sealed = 1;
    return UD_OK;
}
void ud_state_reader_clear(ud_state_reader *r) {
    if (!r)
        return;
    ud_buffer_view_release(r->view);
    memset(r, 0, sizeof(*r));
}
ud_status ud_state_row(ud_state_reader *r, const ud_state *s, uint64_t position,
                       const ud_batch **batch, uint32_t *row) {
    if (!r || !s || !batch || !row)
        return UD_INVALID;
    if (position >= s->rows)
        return UD_END;
    if (s->chunk && position >= s->rows - s->chunk->rows) {
        ud_state_reader_clear(r);
        *batch = s->chunk;
        *row = (uint32_t)(position - (s->rows - s->chunk->rows));
        return UD_OK;
    }
    uint32_t low = 0, high = s->count;
    while (low < high) {
        uint32_t mid = low + (high - low) / 2;
        if (s->pages[mid].first + s->pages[mid].rows <= position)
            low = mid + 1;
        else
            high = mid;
    }
    if (low == s->count)
        return UD_CORRUPT;
    if (r->state != s || !r->view || r->page != low) {
        ud_state_reader_clear(r);
        UD_TRY(ud_buffer_read(s->pages[low].block, &r->view));
        r->state = s;
        r->page = low;
    }
    *batch = ud_buffer_view_batch(r->view);
    *row = (uint32_t)(position - s->pages[low].first);
    return UD_OK;
}
void ud_state_destroy(ud_state *s) {
    if (!s)
        return;
    for (uint32_t i = 0; i < s->count; i++)
        ud_buffer_release(s->pages[i].block);
    ud_batch_destroy(s->schema);
    ud_batch_destroy(s->chunk);
    ud_free(s->pages);
    ud_free(s);
}

ud_status ud_state_add_batch(ud_state *s, const ud_batch *batch) {
    if (!s || !batch || !batch->sealed || s->sealed || batch->columns != s->schema->columns)
        return UD_INVALID;
    if (!batch->rows)
        return UD_OK;
    for (uint32_t i = 0; i < batch->columns; i++)
        if (!ud_field_matches(&batch->column[i].field, &s->schema->column[i].field, UD_FIELD_PORT))
            return UD_SCHEMA;
    UD_TRY(flush(s));
    if (s->count == s->capacity) {
        uint32_t cap = s->capacity ? s->capacity * 2 : 16;
        if (cap < s->capacity)
            return UD_RESOURCE;
        ud_state_page *pages = ud_alloc(s->ctx, (size_t)cap * sizeof(*pages));
        if (!pages)
            return UD_RESOURCE;
        if (s->count)
            memcpy(pages, s->pages, (size_t)s->count * sizeof(*pages));
        ud_free(s->pages);
        s->pages = pages;
        s->capacity = cap;
    }
    if (batch->rows > UINT64_MAX - s->rows)
        return UD_OVERFLOW;
    ud_buffer_block *block = NULL;
    UD_TRY(ud_buffer_store(s->buffers, batch, &block));
    s->pages[s->count++] = (ud_state_page){block, s->rows, batch->rows};
    s->rows += batch->rows;
    return UD_OK;
}
