#include "ud/cache.h"
#include "../internal.h"
#include <stdatomic.h>
typedef struct cache_page {
    ud_buffer_block *block;
    uint64_t first;
    uint32_t rows;
    ud_value *maximum;
    uint8_t *duplicate;
} cache_page;
struct ud_cache {
    ud_context *ctx;
    ud_cache_options options;
    ud_batch *schema;
    uint32_t *keys;
    ud_operator *sort;
    cache_page *pages;
    uint32_t count, capacity;
    uint64_t rows, duplicates;
    atomic_uint references;
    atomic_uint_fast64_t hits, misses;
    int sealed, failed;
    ud_sha256 version;
    uint8_t fingerprint[32];
};
struct ud_cache_reader {
    ud_cache *cache;
    uint32_t *keys, count, page;
    ud_buffer_view *view;
    uint64_t next, end;
};
static ud_status check(const ud_cache *cache) {
    if (ud_is_cancelled(cache->ctx))
        return UD_CANCELLED;
    return cache->options.check ? cache->options.check(cache->options.user) : UD_OK;
}
static ud_status clone_schema(ud_context *ctx, const ud_batch *schema, uint32_t capacity,
                              ud_batch **out) {
    ud_field *fields = ud_alloc(ctx, (size_t)schema->columns * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    for (uint32_t j = 0; j < schema->columns; j++)
        fields[j] = schema->column[j].field;
    ud_status status = ud_batch_create(ctx, fields, schema->columns, capacity, out);
    ud_free(fields);
    return status;
}
ud_status ud_cache_create(ud_context *ctx, const ud_batch *schema, const ud_cache_options *options,
                          ud_cache **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !schema || !options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!options->buffers || !options->keys || !options->key_count ||
        options->key_count > schema->columns || !options->max_batch_rows ||
        options->duplicates < 0 || options->duplicates > 2 || options->error_policy < 0 ||
        options->error_policy > 2)
        return UD_INVALID;
    ud_cache *cache = ud_alloc(ctx, sizeof(*cache));
    if (!cache)
        return UD_RESOURCE;
    cache->ctx = ctx;
    cache->options = *options;
    atomic_init(&cache->references, 1);
    atomic_init(&cache->hits, 0);
    atomic_init(&cache->misses, 0);
    ud_sha256_init(&cache->version);
    cache->keys = ud_alloc(ctx, (size_t)options->key_count * sizeof(*cache->keys));
    ud_status status = cache->keys ? UD_OK : UD_RESOURCE;
    for (uint32_t j = 0; !status && j < options->key_count; j++) {
        cache->keys[j] = options->keys[j];
        if (cache->keys[j] >= schema->columns)
            status = UD_SCHEMA;
        for (uint32_t k = 0; k < j; k++)
            if (cache->keys[k] == cache->keys[j])
                status = UD_SCHEMA;
    }
    if (!status)
        status = clone_schema(ctx, schema, 0, &cache->schema);
    if (!status) {
        uint8_t fingerprint[32];
        status = ud_schema_fingerprint(schema, fingerprint);
        if (!status)
            ud_sha256_update(&cache->version, fingerprint, 32);
    }
    for (uint32_t i = 0; !status && i < options->key_count + 2; i++) {
        uint64_t value = i < options->key_count    ? cache->keys[i]
                         : i == options->key_count ? (uint64_t)options->duplicates
                                                   : (uint64_t)options->error_policy;
        uint8_t bytes[8];
        for (uint32_t j = 0; j < 8; j++)
            bytes[j] = (uint8_t)(value >> (8 * j));
        ud_sha256_update(&cache->version, bytes, 8);
    }
    ud_operator_options sort = {.struct_size = sizeof(sort),
                                .abi_version = UD_ABI_VERSION,
                                .kind = UD_OPERATOR_SORT,
                                .max_batch_rows = options->max_batch_rows,
                                .keys = cache->keys,
                                .key_count = options->key_count,
                                .buffers = options->buffers,
                                .check = options->check,
                                .user = options->user,
                                .partition_count = 1,
                                .executor = options->executor};
    const ud_batch *inputs[] = {schema};
    if (!status)
        status = ud_operator_create(ctx, &sort, inputs, &cache->sort);
    if (status)
        ud_cache_release(cache);
    else
        *out = cache;
    return status;
}
ud_status ud_cache_append(ud_cache *cache, const ud_batch *batch) {
    if (!cache || !batch || cache->sealed || cache->failed)
        return UD_INVALID;
    UD_TRY(check(cache));
    ud_status status = ud_operator_push(cache->sort, 0, batch);
    if (!status) {
        if (batch->rows > UINT64_MAX - cache->rows)
            status = UD_OVERFLOW;
        else
            cache->rows += batch->rows;
    }
    if (status)
        cache->failed = 1;
    return status;
}
static ud_status copy_keys(ud_cache *cache, const ud_batch *batch, uint32_t row, ud_value **out) {
    *out = ud_alloc(cache->ctx, (size_t)cache->options.key_count * sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (uint32_t j = 0; !status && j < cache->options.key_count; j++) {
        ud_value value;
        status = ud_batch_get(batch, row, cache->keys[j], &value);
        if (!status)
            status = ud_copy_value(cache->ctx, &value, *out + j, 0);
    }
    return status;
}
static void clear_keys(ud_cache *cache, ud_value *keys) {
    if (keys)
        for (uint32_t j = 0; j < cache->options.key_count; j++)
            ud_clear_value(keys + j);
    ud_free(keys);
}
static int key_compare(ud_cache *cache, const ud_batch *input, uint32_t row,
                       const uint32_t *columns, const ud_value *values) {
    for (uint32_t j = 0; j < cache->options.key_count; j++) {
        ud_value value;
        ud_batch_get(input, row, columns[j], &value);
        int cmp = ud_value_compare(&value, values + j);
        if (cmp)
            return cmp;
    }
    return 0;
}
static int row_compare(ud_cache *cache, const ud_batch *input, uint32_t row,
                       const uint32_t *columns, const ud_batch *reference, uint32_t ref) {
    for (uint32_t j = 0; j < cache->options.key_count; j++) {
        ud_value a, b;
        ud_batch_get(input, row, columns[j], &a);
        ud_batch_get(reference, ref, cache->keys[j], &b);
        int cmp = ud_value_compare(&a, &b);
        if (cmp)
            return cmp;
    }
    return 0;
}
ud_status ud_cache_seal(ud_cache *cache) {
    if (!cache || cache->failed)
        return UD_INVALID;
    if (cache->sealed)
        return UD_OK;
    UD_TRY(check(cache));
    ud_status status = ud_operator_finish_input(cache->sort, 0);
    ud_batch *batch = NULL;
    uint64_t position = 0;
    ud_value *previous = NULL;
    while (!status && (status = ud_operator_pull(cache->sort, &batch, NULL)) == UD_OK) {
        if (cache->count == cache->capacity) {
            uint32_t capacity = cache->capacity ? cache->capacity * 2 : 16;
            if (capacity < cache->capacity || (uint64_t)capacity > SIZE_MAX / sizeof(*cache->pages))
                status = UD_RESOURCE;
            cache_page *pages =
                !status ? ud_alloc(cache->ctx, (size_t)capacity * sizeof(*pages)) : NULL;
            if (!status && !pages)
                status = UD_RESOURCE;
            if (!status) {
                if (cache->count)
                    memcpy(pages, cache->pages, (size_t)cache->count * sizeof(*pages));
                ud_free(cache->pages);
                cache->pages = pages;
                cache->capacity = capacity;
            }
        }
        cache_page *page = !status ? cache->pages + cache->count++ : NULL;
        if (!status) {
            page->first = position;
            page->rows = batch->rows;
            page->duplicate = ud_alloc(cache->ctx, batch->rows + 1);
            if (!page->duplicate)
                status = UD_RESOURCE;
        }
        if (!status)
            status = copy_keys(cache, batch, batch->rows - 1, &page->maximum);
        for (uint32_t row = 0; !status && row < batch->rows; row++) {
            status = check(cache);
            int null = 0;
            for (uint32_t j = 0; j < cache->options.key_count; j++) {
                ud_value value;
                ud_batch_get(batch, row, cache->keys[j], &value);
                null |= value.is_null;
            }
            if (!status && previous && !null &&
                !key_compare(cache, batch, row, cache->keys, previous)) {
                cache->duplicates++;
                page->duplicate[row] = 1;
                if (cache->options.duplicates == UD_CACHE_ERROR &&
                    cache->options.error_policy == UD_ERROR_FAIL)
                    status = UD_SCHEMA;
            }
            clear_keys(cache, previous);
            previous = NULL;
            if (!status)
                status = copy_keys(cache, batch, row, &previous);
        }
        if (!status)
            status = ud_batch_hash_rows(batch, &cache->version);
        if (!status)
            status = ud_buffer_store(cache->options.buffers, batch, &page->block);
        position += batch->rows;
        ud_batch_destroy(batch);
        batch = NULL;
    }
    clear_keys(cache, previous);
    ud_batch_destroy(batch);
    if (status == UD_END)
        status = UD_OK;
    if (!status && position != cache->rows)
        status = UD_CORRUPT;
    if (!status) {
        status = ud_operator_destroy(cache->sort);
        if (!status)
            cache->sort = NULL;
    }
    if (!status) {
        ud_sha256_finish(&cache->version, cache->fingerprint);
        cache->sealed = 1;
    } else
        cache->failed = 1;
    return status;
}
ud_status ud_cache_duplicates(ud_cache *cache, uint64_t *position, ud_batch **out) {
    if (!cache || !position || !out || !cache->sealed)
        return UD_INVALID;
    *out = NULL;
    UD_TRY(check(cache));
    ud_status status = clone_schema(cache->ctx, cache->schema, cache->options.max_batch_rows, out);
    for (uint32_t page = 0;
         !status && page < cache->count && (*out)->rows < cache->options.max_batch_rows; page++) {
        cache_page *p = cache->pages + page;
        if (*position >= p->first + p->rows)
            continue;
        ud_buffer_view *view = NULL;
        status = ud_buffer_read(p->block, &view);
        const ud_batch *batch = ud_buffer_view_batch(view);
        for (uint32_t row = (uint32_t)(*position > p->first ? *position - p->first : 0);
             !status && row < p->rows && (*out)->rows < cache->options.max_batch_rows; row++) {
            if (p->duplicate[row])
                status = ud_batch_append_from(*out, batch, row);
            *position = p->first + row + 1;
        }
        ud_buffer_view_release(view);
    }
    if (!status && !(*out)->rows)
        status = UD_END;
    if (!status)
        status = ud_batch_seal(*out);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
ud_status ud_cache_retain(ud_cache *cache) {
    if (!cache || !cache->sealed)
        return UD_INVALID;
    unsigned count = atomic_load(&cache->references);
    do {
        if (!count || count == UINT_MAX)
            return UD_RESOURCE;
    } while (!atomic_compare_exchange_weak(&cache->references, &count, count + 1));
    return UD_OK;
}
ud_status ud_cache_release(ud_cache *cache) {
    if (!cache)
        return UD_OK;
    if (atomic_fetch_sub(&cache->references, 1) != 1)
        return UD_OK;
    ud_status status = ud_operator_destroy(cache->sort);
    cache->sort = NULL;
    for (uint32_t i = 0; i < cache->count; i++) {
        ud_status closed = ud_buffer_release(cache->pages[i].block);
        if (!status)
            status = closed;
        ud_free(cache->pages[i].duplicate);
        clear_keys(cache, cache->pages[i].maximum);
    }
    ud_batch_destroy(cache->schema);
    ud_free(cache->pages);
    ud_free(cache->keys);
    ud_free(cache);
    return status;
}
ud_status ud_cache_schema(const ud_cache *cache, ud_batch **out) {
    return cache && out ? clone_schema(cache->ctx, cache->schema, 0, out) : UD_INVALID;
}
ud_status ud_cache_fingerprint(const ud_cache *cache, uint8_t out[32]) {
    if (!cache || !out || !cache->sealed)
        return UD_INVALID;
    memcpy(out, cache->fingerprint, 32);
    return UD_OK;
}
void ud_cache_get_metrics(const ud_cache *cache, ud_cache_metrics *out) {
    if (out)
        *out =
            cache ? (ud_cache_metrics){cache->rows, atomic_load(&cache->hits),
                                       atomic_load(&cache->misses), cache->duplicates, cache->count}
                  : (ud_cache_metrics){0};
}
static ud_status view(ud_cache_reader *reader, uint32_t page) {
    if (reader->view && reader->page == page)
        return UD_OK;
    ud_buffer_view_release(reader->view);
    reader->view = NULL;
    reader->page = page;
    return ud_buffer_read(reader->cache->pages[page].block, &reader->view);
}
ud_status ud_cache_reader_create(ud_cache *cache, const ud_batch *schema, const uint32_t *keys,
                                 uint32_t count, ud_cache_reader **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!cache || !schema || !keys || !cache->sealed || count != cache->options.key_count)
        return UD_INVALID;
    for (uint32_t j = 0; j < count; j++)
        if (keys[j] >= schema->columns ||
            !ud_field_matches(&schema->column[keys[j]].field,
                              &cache->schema->column[cache->keys[j]].field, UD_FIELD_VALUE_TYPE))
            return UD_SCHEMA;
    ud_cache_reader *reader = ud_alloc(cache->ctx, sizeof(*reader));
    if (!reader)
        return UD_RESOURCE;
    reader->keys = ud_alloc(cache->ctx, (size_t)count * sizeof(*reader->keys));
    ud_status status = reader->keys ? UD_OK : UD_RESOURCE;
    if (!status)
        status = ud_cache_retain(cache);
    if (!status) {
        reader->cache = cache;
        reader->count = count;
        reader->page = UINT32_MAX;
        memcpy(reader->keys, keys, (size_t)count * sizeof(*keys));
        *out = reader;
    } else {
        ud_free(reader->keys);
        ud_free(reader);
    }
    return status;
}
static ud_status boundary(ud_cache_reader *reader, const ud_batch *input, uint32_t row, int upper,
                          uint64_t *out) {
    ud_cache *cache = reader->cache;
    uint32_t low = 0, high = cache->count;
    while (low < high) {
        uint32_t mid = low + (high - low) / 2;
        int cmp = key_compare(cache, input, row, reader->keys, cache->pages[mid].maximum);
        if (cmp > 0 || (upper && !cmp))
            low = mid + 1;
        else
            high = mid;
    }
    if (low == cache->count) {
        *out = cache->rows;
        return UD_OK;
    }
    UD_TRY(view(reader, low));
    const ud_batch *reference = ud_buffer_view_batch(reader->view);
    uint32_t begin = 0, end = reference->rows;
    while (begin < end) {
        uint32_t mid = begin + (end - begin) / 2;
        int cmp = row_compare(cache, input, row, reader->keys, reference, mid);
        if (cmp > 0 || (upper && !cmp))
            begin = mid + 1;
        else
            end = mid;
    }
    *out = cache->pages[low].first + begin;
    return UD_OK;
}
ud_status ud_cache_probe(ud_cache_reader *reader, const ud_batch *input, uint32_t row) {
    if (!reader || !input || row >= input->rows)
        return UD_INVALID;
    UD_TRY(check(reader->cache));
    reader->next = reader->end = 0;
    for (uint32_t j = 0; j < reader->count; j++) {
        if (reader->keys[j] >= input->columns ||
            !ud_field_matches(&input->column[reader->keys[j]].field,
                              &reader->cache->schema->column[reader->cache->keys[j]].field,
                              UD_FIELD_VALUE_TYPE))
            return UD_SCHEMA;
        ud_value value;
        UD_TRY(ud_batch_get(input, row, reader->keys[j], &value));
        if (value.is_null) {
            atomic_fetch_add(&reader->cache->misses, 1);
            return UD_OK;
        }
    }
    UD_TRY(boundary(reader, input, row, 0, &reader->next));
    UD_TRY(boundary(reader, input, row, 1, &reader->end));
    if (reader->end > reader->next) {
        atomic_fetch_add(&reader->cache->hits, 1);
        if (reader->cache->options.duplicates != UD_CACHE_ALL)
            reader->end = reader->next + 1;
    } else
        atomic_fetch_add(&reader->cache->misses, 1);
    return UD_OK;
}
ud_status ud_cache_next(ud_cache_reader *reader, const ud_batch **out, uint32_t *row) {
    if (!reader || !out || !row)
        return UD_INVALID;
    *out = NULL;
    if (reader->next >= reader->end)
        return UD_END;
    UD_TRY(check(reader->cache));
    uint32_t low = 0, high = reader->cache->count;
    while (low + 1 < high) {
        uint32_t mid = low + (high - low) / 2;
        if (reader->cache->pages[mid].first <= reader->next)
            low = mid;
        else
            high = mid;
    }
    UD_TRY(view(reader, low));
    *out = ud_buffer_view_batch(reader->view);
    *row = (uint32_t)(reader->next - reader->cache->pages[low].first);
    reader->next++;
    return UD_OK;
}
void ud_cache_reader_destroy(ud_cache_reader *reader) {
    if (reader) {
        ud_buffer_view_release(reader->view);
        ud_cache_release(reader->cache);
        ud_free(reader->keys);
        ud_free(reader);
    }
}

ud_status ud_cache_scan_create(ud_cache *cache, ud_cache_reader **out) {
    if (!cache || !out || !cache->sealed || cache->failed) return UD_INVALID;
    *out = NULL; UD_TRY(check(cache));
    ud_cache_reader *reader = ud_alloc(cache->ctx, sizeof(*reader)); if (!reader) return UD_RESOURCE;
    ud_status status = ud_cache_retain(cache); if (status) { ud_free(reader); return status; }
    reader->cache = cache; reader->end = cache->rows; *out = reader; return UD_OK;
}
ud_status ud_cache_read_at(ud_cache_reader *reader, uint64_t position, const ud_batch **batch, uint32_t *row) {
    if (!reader || position >= reader->cache->rows) return UD_INVALID;
    reader->next = position; reader->end = position + 1; return ud_cache_next(reader, batch, row);
}
