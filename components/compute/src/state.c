#include "state.h"
#include "../../execution/state.h"
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
    ud_buffer_options limits = {0};
    ud_status budget = ud_buffer_manager_limits(buffers, &limits);
    if (budget) {
        ud_free(*out);
        *out = NULL;
        return budget;
    }
    /* A page must fit concurrently with its encoded bytes and other readers.
     * Event rows may contain complete variable-size batches, so a row count is
     * never a sufficient memory bound. */
    (*out)->page_limit = limits.working_limit / 8;
    if (!(*out)->page_limit)
        (*out)->page_limit = 1;
    uint64_t width = sizeof(ud_row_info) + (uint64_t)schema->columns * 32;
    uint64_t capacity = (*out)->page_limit / width;
    if (!capacity)
        capacity = 1;
    (*out)->batch_size = capacity < batch_size ? (uint32_t)capacity : batch_size;
    ud_status s = schema_copy(ctx, schema, 0, &(*out)->schema);
    if (s) {
        ud_state_destroy(*out);
        *out = NULL;
    }
    return s;
}
static ud_status retain_page(ud_state *state, const ud_batch *batch, uint64_t first) {
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
    ud_buffer_block *block = NULL;
    UD_TRY(ud_buffer_store(state->buffers, batch, &block));
    state->pages[state->count++] = (ud_state_page){block, first, batch->rows};
    state->written_bytes += ud_buffer_block_spill_bytes(block);
    return UD_OK;
}
static ud_status flush(ud_state *state) {
    if (!state->chunk || !state->chunk->rows)
        return UD_OK;
    UD_TRY(ud_batch_seal(state->chunk));
    UD_TRY(retain_page(state, state->chunk, state->rows - state->chunk->rows));
    ud_batch_destroy(state->chunk);
    state->chunk = NULL;
    state->chunk_bytes = 0;
    return UD_OK;
}
static uint64_t payload_bytes(const ud_value *value) {
    uint64_t n = value->type == UD_UTF8 || value->type == UD_BINARY ? value->text_length : 0;
    for (uint32_t j = 0; j < value->child_count; j++) {
        uint64_t child = payload_bytes(value->children + j);
        if (child > UINT64_MAX - sizeof(ud_value) - n)
            return UINT64_MAX;
        n += sizeof(ud_value) + child;
    }
    return n;
}
static ud_status row_bytes(const ud_batch *batch, uint32_t row, uint64_t *out) {
    uint64_t n = sizeof(ud_row_info) + (uint64_t)batch->columns * 32;
    for (uint32_t j = 0; j < batch->columns; j++) {
        ud_value value;
        UD_TRY(ud_batch_get(batch, row, j, &value));
        uint64_t size = payload_bytes(&value);
        if (size > UINT64_MAX - n)
            return UD_OVERFLOW;
        n += size;
    }
    *out = n;
    return UD_OK;
}
ud_status ud_state_append_bound(ud_state *s, const ud_batch *b, uint32_t row) {
    if (s->rows == UINT64_MAX)
        return UD_OVERFLOW;
    uint64_t bytes = 0;
    UD_TRY(row_bytes(b, row, &bytes));
    if (s->chunk && s->chunk->rows &&
        (bytes > s->page_limit || s->chunk_bytes > s->page_limit - bytes))
        UD_TRY(flush(s));
    if (!s->chunk)
        UD_TRY(schema_copy(s->ctx, s->schema, s->batch_size, &s->chunk));
    UD_TRY(ud_batch_append_from(s->chunk, b, row));
    s->rows++;
    if (bytes > UINT64_MAX - s->chunk_bytes)
        return UD_OVERFLOW;
    s->chunk_bytes += bytes;
    return s->chunk->rows == s->batch_size || s->chunk_bytes >= s->page_limit ? flush(s) : UD_OK;
}
ud_status ud_state_append(ud_state *s, const ud_batch *b, uint32_t row) {
    if (!s || !b || s->sealed || b->columns != s->schema->columns || row >= b->rows)
        return UD_INVALID;
    for (uint32_t i = 0; i < b->columns; i++)
        if (!ud_field_matches(&b->column[i].field, &s->schema->column[i].field, UD_FIELD_PORT))
            return UD_SCHEMA;
    return ud_state_append_bound(s, b, row);
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
ud_status ud_state_read_page(ud_state_reader *r, const ud_state *s, uint32_t page,
                             const ud_batch **batch) {
    if (r->state != s || !r->view || r->page != page) {
        ud_buffer_view_release(r->view);
        r->view = NULL;
        UD_TRY(ud_buffer_read(s->pages[page].block, &r->view));
        r->read_bytes += ud_buffer_block_spill_bytes(s->pages[page].block);
        r->state = s;
        r->page = page;
    }
    *batch = ud_buffer_view_batch(r->view);
    return UD_OK;
}
ud_status ud_state_row(ud_state_reader *r, const ud_state *s, uint64_t position,
                       const ud_batch **batch, uint32_t *row) {
    if (!r || !s || !batch || !row)
        return UD_INVALID;
    if (position >= s->rows)
        return UD_END;
    if (s->chunk && position >= s->rows - s->chunk->rows) {
        ud_buffer_view_release(r->view);
        r->view = NULL;
        *batch = s->chunk;
        *row = (uint32_t)(position - (s->rows - s->chunk->rows));
        return UD_OK;
    }
    uint32_t low = 0, high = s->count;
    if (r->state == s && r->view) {
        const ud_state_page *current = s->pages + r->page;
        if (position >= current->first && position - current->first < current->rows) {
            *batch = ud_buffer_view_batch(r->view);
            *row = (uint32_t)(position - current->first);
            return UD_OK;
        }
        if (position >= current->first)
            low = r->page + 1;
        else
            high = r->page;
    }
    while (low < high) {
        uint32_t mid = low + (high - low) / 2;
        if (s->pages[mid].first + s->pages[mid].rows <= position)
            low = mid + 1;
        else
            high = mid;
    }
    if (low == s->count)
        return UD_CORRUPT;
    UD_TRY(ud_state_read_page(r, s, low, batch));
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
    /* Partial partition fragments pack into the existing bounded chunk. A
     * sealed full chunk still shares its columns directly with Buffer. */
    uint64_t bytes = 0;
    for (uint32_t row = 0; row < batch->rows && bytes <= s->page_limit; row++) {
        if (ud_is_cancelled(s->ctx))
            return UD_CANCELLED;
        uint64_t n = 0;
        UD_TRY(row_bytes(batch, row, &n));
        if (n > UINT64_MAX - bytes)
            return UD_OVERFLOW;
        bytes += n;
    }
    if (s->chunk || batch->rows < s->batch_size || bytes > s->page_limit) {
        for (uint32_t row = 0; row < batch->rows; row++) {
            if (ud_is_cancelled(s->ctx))
                return UD_CANCELLED;
            UD_TRY(ud_state_append_bound(s, batch, row));
        }
        return UD_OK;
    }
    if (batch->rows > UINT64_MAX - s->rows)
        return UD_OVERFLOW;
    UD_TRY(retain_page(s, batch, s->rows));
    s->rows += batch->rows;
    return UD_OK;
}
