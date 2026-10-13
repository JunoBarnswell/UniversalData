#include "ud/cache.h"
#include "../internal.h"
#include <stdatomic.h>
#include "snapshot.h"
#include "../../foundation/thread.h"
typedef struct cache_page {
    ud_buffer_block *block;
    uint64_t first;
    uint32_t rows;
    ud_value *maximum;
    uint8_t *duplicate;
} cache_page;
typedef struct partial_entry {
    ud_value *keys;
    ud_buffer_block *block;
    uint64_t hash, expires, used;
    uint32_t rows, next;
    int occupied;
} partial_entry;
typedef struct partial_index {
    ud_mutex mutex;
    partial_entry *entries;
    uint32_t *buckets;
    uint32_t count, maximum, bucket_count;
    uint64_t clock;
} partial_index;
static ud_status partial_create(ud_cache *);
static void partial_destroy(ud_cache *);
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
    partial_index *partial;
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
        options->mode > UD_CACHE_PARTIAL || options->duplicates < 0 || options->duplicates > 2 ||
        options->error_policy < 0 || options->error_policy > 2)
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
                                .executor = options->executor,
                                .record_state = options->record_state};
    const ud_batch *inputs[] = {schema};
    if (!status && options->mode == UD_CACHE_PARTIAL)
        status = partial_create(cache);
    else if (!status)
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
    partial_destroy(cache);
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
    if (reader->cache->partial) {
        int32_t cached;
        ud_status status = ud_cache_partial_probe(reader, input, row, &cached);
        return status ? status : cached ? UD_OK : UD_BUSY;
    }
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
    if (reader->next >= reader->end) {
        ud_buffer_view_release(reader->view);
        reader->view = NULL;
        return UD_END;
    }
    UD_TRY(check(reader->cache));
    if (reader->cache->partial) {
        if (!reader->view)
            return UD_CORRUPT;
        *out = ud_buffer_view_batch(reader->view);
        *row = (uint32_t)reader->next++;
        return UD_OK;
    }
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
    if (!cache || !out || !cache->sealed || cache->failed)
        return UD_INVALID;
    if (cache->partial)
        return UD_UNSUPPORTED;
    *out = NULL;
    UD_TRY(check(cache));
    ud_cache_reader *reader = ud_alloc(cache->ctx, sizeof(*reader));
    if (!reader)
        return UD_RESOURCE;
    ud_status status = ud_cache_retain(cache);
    if (status) {
        ud_free(reader);
        return status;
    }
    reader->cache = cache;
    reader->end = cache->rows;
    *out = reader;
    return UD_OK;
}
ud_status ud_cache_read_at(ud_cache_reader *reader, uint64_t position, const ud_batch **batch,
                           uint32_t *row) {
    if (!reader || position >= reader->cache->rows)
        return UD_INVALID;
    reader->next = position;
    reader->end = position + 1;
    return ud_cache_next(reader, batch, row);
}

static ud_status partial_create(ud_cache *c) {
    if (!c->options.max_entries || c->options.max_entries > 65536 || !c->options.ttl_ms)
        return UD_SCHEMA;
    partial_index *p = ud_alloc(c->ctx, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    c->partial = p;
    p->maximum = c->options.max_entries;
    p->bucket_count = 1;
    while (p->bucket_count < p->maximum * 2)
        p->bucket_count *= 2;
    ud_mutex_init(&p->mutex);
    ud_status status = ud_buffer_working_allocate(
        c->options.buffers, (size_t)p->maximum * sizeof(*p->entries), (void **)&p->entries);
    if (!status)
        status = ud_buffer_working_allocate(c->options.buffers,
                                            (size_t)p->bucket_count * sizeof(*p->buckets),
                                            (void **)&p->buckets);
    if (!status) {
        memset(p->buckets, 255, p->bucket_count * sizeof(*p->buckets));
        c->sealed = 1;
        uint8_t schema[32];
        status = ud_schema_fingerprint(c->schema, schema);
        if (!status) {
            ud_sha256 version = c->version;
            uint64_t configuration[] = {c->options.mode, c->options.max_entries, c->options.ttl_ms};
            for (uint32_t i = 0; i < 3; i++) {
                uint8_t value[8];
                for (uint32_t k = 0; k < 8; k++)
                    value[k] = (uint8_t)(configuration[i] >> (8 * k));
                ud_sha256_update(&version, value, 8);
            }
            ud_sha256_finish(&version, c->fingerprint);
        }
    }
    return status;
}
static void partial_destroy(ud_cache *c) {
    partial_index *p = c->partial;
    if (!p)
        return;
    for (uint32_t i = 0; p->entries && i < p->maximum; i++) {
        partial_entry *e = p->entries + i;
        if (e->block)
            ud_buffer_release(e->block);
        if (e->keys) {
            for (uint32_t k = 0; k < c->options.key_count; k++)
                ud_clear_value(e->keys + k);
            ud_free(e->keys);
        }
    }
    ud_free(p->entries);
    ud_free(p->buckets);
    ud_mutex_destroy(&p->mutex);
    ud_free(p);
    c->partial = NULL;
}
static uint64_t key_hash(const ud_cache *c, const ud_batch *b, uint32_t row, const uint32_t *keys) {
    uint64_t hash = UD_VALUE_HASH_SEED;
    for (uint32_t k = 0; k < c->options.key_count; k++) {
        ud_value value;
        ud_batch_get(b, row, keys[k], &value);
        hash = ud_value_hash(&value, hash);
    }
    return hash;
}
static int key_equal(const ud_cache *c, const partial_entry *e, const ud_batch *b, uint32_t row,
                     const uint32_t *keys) {
    for (uint32_t k = 0; k < c->options.key_count; k++) {
        ud_value value;
        ud_batch_get(b, row, keys[k], &value);
        if (ud_value_compare(e->keys + k, &value))
            return 0;
    }
    return 1;
}
ud_status ud_cache_partial_put(ud_cache *c, const ud_batch *key_batch, uint32_t row,
                               const uint32_t *keys, const ud_batch *values) {
    if (!c || !c->partial || !key_batch || !keys || row >= key_batch->rows)
        return UD_INVALID;
    UD_TRY(check(c));
    for (uint32_t k = 0; k < c->options.key_count; k++)
        if (keys[k] >= key_batch->columns ||
            !ud_field_matches(&key_batch->column[keys[k]].field,
                              &c->schema->column[c->keys[k]].field, UD_FIELD_VALUE_TYPE))
            return UD_SCHEMA;
    if (values) {
        if(values->ctx!=c->ctx||values->rows>c->options.max_batch_rows)return UD_SCHEMA;
        if (!values->sealed || values->columns != c->schema->columns)
            return UD_SCHEMA;
        for (uint32_t k = 0; k < values->columns; k++)
            if (!ud_field_matches(&values->column[k].field, &c->schema->column[k].field,
                                  UD_FIELD_PORT))
                return UD_SCHEMA;
    }
    for(uint32_t vr=0;values&&vr<values->rows;vr++)for(uint32_t k=0;k<c->options.key_count;k++){ud_value probe,result;UD_TRY(ud_batch_get(key_batch,row,keys[k],&probe));UD_TRY(ud_batch_get(values,vr,c->keys[k],&result));if(probe.is_null||result.is_null||ud_value_compare(&probe,&result))return UD_SCHEMA;}
    ud_value *owned = ud_alloc(c->ctx, c->options.key_count * sizeof(*owned));
    if (!owned)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (uint32_t k = 0; !status && k < c->options.key_count; k++) {
        ud_value value;
        status = ud_batch_get(key_batch, row, keys[k], &value);
        if (!status && value.is_null)
            status = UD_SCHEMA;
        if (!status)
            status = ud_copy_value(c->ctx, &value, owned + k, 0);
    }
    ud_buffer_block *block = NULL;
    if (!status && values && values->rows)
        status = ud_buffer_store(c->options.buffers, values, &block);
    partial_index *p = c->partial;
    uint64_t hash = key_hash(c, key_batch, row, keys);
    ud_mutex_lock(&p->mutex);
    uint32_t at = UINT32_MAX;
    uint32_t slot = (uint32_t)(hash & (p->bucket_count - 1));
    for (uint32_t i = p->buckets[slot]; i != UINT32_MAX; i = p->entries[i].next)
        if (p->entries[i].hash == hash && key_equal(c, p->entries + i, key_batch, row, keys)) {
            at = i;
            break;
        }
    if (at == UINT32_MAX)
        for (uint32_t i = 0; i < p->maximum; i++) {
            partial_entry *e = p->entries + i;
            if (!e->occupied) {
                at = i;
                break;
            }
            if (at == UINT32_MAX || e->used < p->entries[at].used)
                at = i;
        }
    partial_entry *e = at == UINT32_MAX ? NULL : p->entries + at;
    if (!status && !e)
        status = UD_RESOURCE;
    if (!status && e->block) {
        status = ud_buffer_release(e->block);
        if (status == UD_BUSY)
            status = UD_RESOURCE;
        else
            e->block = NULL;
    }
    if (!status) {
        if (e->occupied) {
            uint32_t prior_slot = (uint32_t)(e->hash & (p->bucket_count - 1));
            uint32_t *link = p->buckets + prior_slot;
            while (*link != UINT32_MAX && *link != at)
                link = &p->entries[*link].next;
            if (*link == at)
                *link = e->next;
        }
        if (e->keys) {
            for (uint32_t k = 0; k < c->options.key_count; k++)
                ud_clear_value(e->keys + k);
            ud_free(e->keys);
        }
        c->rows=c->rows-e->rows+(values?values->rows:0);
        if (!e->occupied)
            p->count++;
        uint64_t now = ud_now_ms();
        *e = (partial_entry){
            .keys = owned,
            .block = block,
            .hash = hash,
            .expires = c->options.ttl_ms > UINT64_MAX - now ? UINT64_MAX : now + c->options.ttl_ms,
            .used = ++p->clock,
            .rows = values ? values->rows : 0,
            .occupied = 1,
            .next = p->buckets[slot]};
        p->buckets[slot] = at;
        owned = NULL;
        block = NULL;
    }
    ud_mutex_unlock(&p->mutex);
    ud_buffer_release(block);
    if (owned) {
        for (uint32_t k = 0; k < c->options.key_count; k++)
            ud_clear_value(owned + k);
        ud_free(owned);
    }
    return status;
}
ud_status ud_cache_partial_probe(ud_cache_reader *r, const ud_batch *b, uint32_t row,
                                 int32_t *cached) {
    if (!r || !r->cache->partial || !b || !cached || row >= b->rows)
        return UD_INVALID;
    UD_TRY(check(r->cache));
    *cached = 0;
    ud_buffer_view_release(r->view);
    r->view = NULL;
    r->next = r->end = 0;
    ud_cache *c = r->cache;
    partial_index *p = c->partial;
    for (uint32_t k = 0; k < r->count; k++) {
        if(r->keys[k]>=b->columns||!ud_field_matches(&b->column[r->keys[k]].field,&c->schema->column[c->keys[k]].field,UD_FIELD_VALUE_TYPE))return UD_SCHEMA;
        ud_value value;
        UD_TRY(ud_batch_get(b, row, r->keys[k], &value));
        if (value.is_null) {
            *cached = 1;
            return UD_OK;
        }
    }
    uint64_t hash = key_hash(c, b, row, r->keys);
    ud_status status = UD_OK;
    ud_mutex_lock(&p->mutex);
    uint32_t slot = (uint32_t)(hash & (p->bucket_count - 1));
    for (uint32_t i = p->buckets[slot]; i != UINT32_MAX; i = p->entries[i].next) {
        partial_entry *e = p->entries + i;
        if (!e->occupied || e->expires < ud_now_ms() || e->hash != hash ||
            !key_equal(c, e, b, row, r->keys))
            continue;
        *cached = 1;
        e->used = ++p->clock;
        if (e->block)
            status = ud_buffer_read(e->block, &r->view);
        if (!status)
            r->end = e->rows;
        break;
    }
    ud_mutex_unlock(&p->mutex);
    if (!status) {
        if (*cached)
            atomic_fetch_add(&c->hits, 1);
        else
            atomic_fetch_add(&c->misses, 1);
    }
    return status;
}

static ud_status cache_contract(ud_cache *c, uint8_t digest[32]) {
    ud_bytes bytes = {.ctx = c->ctx};
    uint8_t schema[32];
    ud_status status = ud_schema_fingerprint(c->schema, schema);
    if (!status)
        status = ud_bytes_put(&bytes, schema, 32);
    uint64_t values[] = {c->options.mode,        c->options.duplicates, c->options.error_policy,
                         c->options.max_entries, c->options.ttl_ms,     c->options.key_count};
    for (uint32_t i = 0; !status && i < 6; i++)
        status = ud_put_u64(&bytes, values[i]);
    for (uint32_t i = 0; !status && i < c->options.key_count; i++)
        status = ud_put_u64(&bytes, c->keys[i]);
    if (!status)
        ud_hash(bytes.data, bytes.size, digest);
    ud_bytes_clear(&bytes);
    return status;
}
ud_status ud_cache_checkpoint(ud_cache *c, ud_batch **out) {
    if (!c || !out || c->failed)
        return UD_INVALID;
    *out = NULL;
    UD_TRY(check(c));
    ud_bytes bytes = {.ctx = c->ctx};
    uint8_t contract[32];
    ud_status status = cache_contract(c, contract);
    if (!status)
        status = ud_bytes_put(&bytes, "UDCACHE9", 8);
    if (!status)
        status = ud_bytes_put(&bytes, contract, 32);
    if (!status)
        status = ud_put_u64(&bytes, c->sealed);
    if (!status)
        status = ud_put_u64(&bytes, c->rows);
    if (!status && c->partial) {
        partial_index *p = c->partial;
        ud_mutex_lock(&p->mutex);
        uint64_t now = ud_now_ms(), count = 0;
        for (uint32_t i = 0; i < p->maximum; i++)
            if (p->entries[i].occupied && p->entries[i].expires >= now)
                count++;
        status = ud_put_u64(&bytes, count);
        ud_field *fields = ud_alloc(c->ctx, c->options.key_count * sizeof(*fields));
        if (!fields && !status)
            status = UD_RESOURCE;
        for (uint32_t k = 0; fields && k < c->options.key_count; k++)
            fields[k] = c->schema->column[c->keys[k]].field;
        for (uint32_t i = 0; !status && i < p->maximum; i++) {
            partial_entry *entry = p->entries + i;
            if (!entry->occupied || entry->expires < now)
                continue;
            ud_batch *key = NULL;
            status = ud_batch_create(c->ctx, fields, c->options.key_count, 1, &key);
            if (!status)
                status = ud_batch_append(key, entry->keys, c->options.key_count);
            if (!status)
                status = ud_batch_seal(key);
            if (!status)
                status = ud_encode_batch(&bytes, key);
            ud_batch_destroy(key);
            if (!status)
                status = ud_put_u64(&bytes, entry->expires - now);
            if (!status)
                status = ud_put_u64(&bytes, entry->block != NULL);
            if (!status && entry->block) {
                ud_buffer_view *view = NULL;
                status = ud_buffer_read(entry->block, &view);
                if (!status)
                    status = ud_encode_batch(&bytes, ud_buffer_view_batch(view));
                ud_buffer_view_release(view);
            }
        }
        ud_free(fields);
        ud_mutex_unlock(&p->mutex);
    } else if (!status && !c->sealed) {
        ud_batch *state = NULL;
        status = ud_operator_checkpoint(c->sort, &state);
        if (!status)
            status = ud_encode_batch(&bytes, state);
        ud_batch_destroy(state);
    } else if (!status) {
        status = ud_bytes_put(&bytes, c->fingerprint, 32);
        if (!status)
            status = ud_put_u64(&bytes, c->count);
        for (uint32_t i = 0; !status && i < c->count; i++) {
            ud_buffer_view *view = NULL;
            status = ud_buffer_read(c->pages[i].block, &view);
            if (!status)
                status = ud_encode_batch(&bytes, ud_buffer_view_batch(view));
            ud_buffer_view_release(view);
        }
    }
    if (!status)
        status = ud_snapshot_batch(c->ctx, &bytes, out);
    ud_bytes_clear(&bytes);
    return status;
}
ud_status ud_cache_restore(ud_cache *c, const ud_batch *state) {
    if (!c || !state)
        return UD_INVALID;
    UD_TRY(check(c));
    if (c->partial) {
        for (uint32_t i = 0; i < c->partial->maximum; i++)
            if (ud_buffer_block_views(c->partial->entries[i].block))
                return UD_BUSY;
    } else
        for (uint32_t i = 0; i < c->count; i++)
            if (ud_buffer_block_views(c->pages[i].block))
                return UD_BUSY;
    ud_bytes bytes;
    UD_TRY(ud_snapshot_bytes(c->ctx, state, &bytes));
    uint8_t magic[8], contract[32], expected[32];
    UD_TRY(ud_bytes_get(&bytes, magic, 8));
    UD_TRY(ud_bytes_get(&bytes, contract, 32));
    UD_TRY(cache_contract(c, expected));
    if (memcmp(magic, "UDCACHE9", 8) || memcmp(contract, expected, 32))
        return UD_FENCED;
    uint64_t sealed, rows;
    UD_TRY(ud_get_u64(&bytes, &sealed));
    UD_TRY(ud_get_u64(&bytes, &rows));
    if (sealed > 1)
        return UD_CORRUPT;
    ud_cache *fresh = NULL;
    ud_status status = ud_cache_create(c->ctx, c->schema, &c->options, &fresh);
    if (!status && c->partial) {
        uint64_t count = 0;
        status = ud_get_u64(&bytes, &count);
        if (!status && count > c->options.max_entries)
            status = UD_CORRUPT;
        uint32_t *keys = ud_alloc(c->ctx, c->options.key_count * sizeof(*keys));
        if (!keys && !status)
            status = UD_RESOURCE;
        for (uint32_t k = 0; keys && k < c->options.key_count; k++)
            keys[k] = k;
        for (uint64_t i = 0; !status && i < count; i++) {
            ud_batch *key = NULL, *value = NULL;
            uint64_t ttl = 0, present = 0;
            status = ud_decode_batch(&bytes, &key);
            if (!status)
                status = ud_get_u64(&bytes, &ttl);
            if (!status)
                status = ud_get_u64(&bytes, &present);
            if (!status && (present > 1 || ttl > c->options.ttl_ms || key->rows != 1 ||
                            key->columns != c->options.key_count))
                status = UD_CORRUPT;
            if (!status && present)
                status = ud_decode_batch(&bytes, &value);
            if (!status)
                status = ud_cache_partial_put(fresh, key, 0, keys, value);
            if (!status) {
                uint64_t hash = key_hash(fresh, key, 0, keys);
                partial_index *p = fresh->partial;
                for (uint32_t at = p->buckets[hash & (p->bucket_count - 1)]; at != UINT32_MAX;
                     at = p->entries[at].next)
                    if (p->entries[at].hash == hash &&
                        key_equal(fresh, p->entries + at, key, 0, keys)) {
                        p->entries[at].expires = ud_now_ms() + ttl;
                        break;
                    }
            }
            ud_batch_destroy(key);
            ud_batch_destroy(value);
        }
        ud_free(keys);
    } else if (!status && !sealed) {
        ud_batch *operator_state = NULL;
        status = ud_decode_batch(&bytes, &operator_state);
        if (!status)
            status = ud_operator_restore(&fresh->sort, operator_state);
        if (!status)
            fresh->rows = rows;
        ud_batch_destroy(operator_state);
    } else if (!status) {
        uint8_t fingerprint[32];
        uint64_t pages = 0;
        status = ud_bytes_get(&bytes, fingerprint, 32);
        if (!status)
            status = ud_get_u64(&bytes, &pages);
        if (!status && pages > rows)
            status = UD_CORRUPT;
        for (uint64_t i = 0; !status && i < pages; i++) {
            ud_batch *batch = NULL;
            status = ud_decode_batch(&bytes, &batch);
            if (!status)
                status = ud_cache_append(fresh, batch);
            ud_batch_destroy(batch);
        }
        if (!status)
            status = ud_cache_seal(fresh);
        if (!status && (fresh->rows != rows || memcmp(fresh->fingerprint, fingerprint, 32)))
            status = UD_CORRUPT;
    }
    if (!status && bytes.pos != bytes.size)
        status = UD_CORRUPT;
    if (!status) { /* Controller exclusion is required by the C ABI. Cache readers
                    always resolve pages through this stable asset object. */
        ud_operator *sort = c->sort;
        c->sort = fresh->sort;
        fresh->sort = sort;
        cache_page *pages = c->pages;
        c->pages = fresh->pages;
        fresh->pages = pages;
        uint32_t count = c->count, capacity = c->capacity;
        c->count = fresh->count;
        c->capacity = fresh->capacity;
        fresh->count = count;
        fresh->capacity = capacity;
        partial_index *partial = c->partial;
        c->partial = fresh->partial;
        fresh->partial = partial;
        c->rows = fresh->rows;
        c->duplicates = fresh->duplicates;
        c->sealed = fresh->sealed;
        c->failed = 0;
        c->version = fresh->version;
        memcpy(c->fingerprint, fresh->fingerprint, 32);
    }
    ud_cache_release(fresh);
    return status;
}

uint32_t ud_cache_entry_limit(const ud_cache *c) {
    return c && c->partial ? c->options.max_entries : UINT32_MAX;
}
int32_t ud_cache_is_sealed(const ud_cache *c) {
    return c && c->sealed;
}
