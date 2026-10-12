#include "ud/query.h"
#include "ud/csv.h"
#include "../../../components/compute/internal.h"
typedef struct {
    ud_csv_reader *reader;
    ud_batch *row;
} cursor;
static ud_status run_path(ud_context *c, const char *dir, uint64_t id, char **out) {
    char name[64];
    snprintf(name, sizeof(name), "ud-sort-%020llu.csv", (unsigned long long)id);
    return ud_join_path(c, dir, name, out);
}
static ud_status advance(cursor *c) {
    ud_batch_destroy(c->row);
    c->row = NULL;
    ud_status s = ud_csv_next(c->reader, &c->row);
    return s == UD_END ? UD_OK : s;
}
static ud_status merge(ud_context *c, char **inputs, uint32_t count, const char *out,
                       const ud_field *fields, uint32_t columns, uint32_t key, int desc) {
    cursor streams[8] = {{0}};
    ud_csv_writer *writer = NULL;
    ud_csv_options options = {sizeof(options), UD_ABI_VERSION, 1, "\\N", 2};
    ud_status s = ud_csv_writer_open(c, out, fields, columns, "\\N", &writer);
    for (uint32_t i = 0; !s && i < count; i++) {
        s = ud_csv_open(c, inputs[i], fields, columns, &options, &streams[i].reader);
        if (!s)
            s = advance(streams + i);
    }
    while (!s) {
        if (ud_is_cancelled(c)) {
            s = UD_CANCELLED;
            break;
        }
        int best = -1;
        ud_value value;
        for (uint32_t i = 0; i < count; i++)
            if (streams[i].row) {
                ud_value v;
                ud_batch_get(streams[i].row, 0, key, &v);
                int cmp = best < 0 ? -1 : ud_value_compare(&v, &value);
                if (desc)
                    cmp = -cmp;
                if (best < 0 || cmp < 0) {
                    best = (int)i;
                    value = v;
                }
            }
        if (best < 0)
            break;
        s = ud_csv_writer_append(writer, streams[best].row);
        if (!s)
            s = advance(streams + best);
    }
    for (uint32_t i = 0; i < count; i++) {
        ud_batch_destroy(streams[i].row);
        ud_csv_close(streams[i].reader);
    }
    ud_status close = ud_csv_writer_close(writer);
    return s ? s : close;
}
ud_status ud_query_sort_csv(ud_context *c, const char *input, const char *output, const char *dir,
                            const ud_plan *p, uint32_t key, int32_t desc, uint32_t run_rows) {
    if (!input || !output || !dir || !p || p->agg_n || key >= p->n || (desc != 0 && desc != 1))
        return UD_INVALID;
    if (!run_rows)
        run_rows = 4096;
    char *lock_path = NULL;
    void *lock = NULL;
    UD_TRY(ud_join_path(c, dir, "ud-sort.lock", &lock_path));
    ud_status s = ud_lock(c, lock_path, &lock);
    ud_free(lock_path);
    if (s)
        return s;
    /* Sorting is a schema-preserving operation. Apply projection separately. */
    if (p->project_n) {
        ud_unlock(lock);
        return ud_fail(c, UD_INVALID, "external sort requires a schema-preserving plan");
    }
    uint64_t dev, id;
    if (!ud_file_identity(c, output, &dev, &id)) {
        ud_unlock(lock);
        return ud_fail(c, UD_INVALID, "external sort destination must not exist");
    }
    ud_csv_options options = {sizeof(options), UD_ABI_VERSION, run_rows, "\\N", 2};
    ud_csv_reader *reader = NULL;
    s = ud_csv_open(c, input, p->fields, p->n, &options, &reader);
    char **runs = NULL;
    uint32_t count = 0;
    uint64_t next = 1;
    char **owned = NULL;
    uint32_t owned_n = 0;
    while (!s) {
        ud_batch *batch = NULL, *filtered = NULL, *sorted = NULL;
        s = ud_csv_next(reader, &batch);
        if (s == UD_END) {
            s = UD_OK;
            break;
        }
        if (s)
            break;
        s = ud_compute_apply(p, batch, &filtered);
        if (!s)
            s = ud_query_sort(c, filtered, key, desc, &sorted);
        char *path = NULL;
        if (!s)
            s = run_path(c, dir, next++, &path);
        if (!s) { /* Never overwrite a leftover spill from an earlier failed invocation. */
            FILE *exists = ud_fopen(path, "rb");
            if (exists) {
                fclose(exists);
                s = ud_fail(c, UD_BUSY, "spill exists; use a fresh temporary directory: %s", path);
            }
        }
        if (!s) {
            char **v = ud_alloc(c, (size_t)(owned_n + 1) * sizeof(char *));
            if (!v)
                s = UD_RESOURCE;
            else {
                if (owned_n)
                    memcpy(v, owned, owned_n * sizeof(char *));
                v[owned_n++] = path;
                ud_free(owned);
                owned = v;
                path = NULL;
            }
        }
        if (!s)
            s = ud_csv_write(c, owned[owned_n - 1], sorted, "\\N");
        if (!s) {
            char **v = ud_alloc(c, (size_t)(count + 1) * sizeof(char *));
            if (!v)
                s = UD_RESOURCE;
            else {
                if (count)
                    memcpy(v, runs, count * sizeof(char *));
                v[count++] = owned[owned_n - 1];
                ud_free(runs);
                runs = v;
            }
        }
        ud_free(path);
        ud_batch_destroy(batch);
        ud_batch_destroy(filtered);
        ud_batch_destroy(sorted);
    }
    ud_csv_close(reader);
    while (!s && count > 1) {
        uint32_t groups = (count + 7) / 8;
        char **merged = ud_alloc(c, groups * sizeof(char *));
        if (!merged) {
            s = UD_RESOURCE;
            break;
        }
        for (uint32_t start = 0; !s && start < count; start += 8) {
            char *path = NULL;
            s = run_path(c, dir, next++, &path);
            if (s)
                break;
            FILE *exists = ud_fopen(path, "rb");
            if (exists) {
                fclose(exists);
                ud_free(path);
                s = UD_BUSY;
                break;
            }
            char **v = ud_alloc(c, (size_t)(owned_n + 1) * sizeof(char *));
            if (!v) {
                ud_free(path);
                s = UD_RESOURCE;
                break;
            }
            if (owned_n)
                memcpy(v, owned, owned_n * sizeof(char *));
            v[owned_n++] = path;
            ud_free(owned);
            owned = v;
            merged[start / 8] = path;
            uint32_t n = count - start < 8 ? count - start : 8;
            s = merge(c, runs + start, n, path, p->fields, p->n, key, desc);
        }
        ud_free(runs);
        runs = merged;
        count = groups;
    }
    if (!s && !count) {
        ud_batch *empty = NULL;
        s = ud_batch_create(c, p->fields, p->n, 0, &empty);
        char *path = NULL;
        if (!s)
            s = run_path(c, dir, next++, &path);
        if (!s) {
            FILE *exists = ud_fopen(path, "rb");
            if (exists) {
                fclose(exists);
                s = UD_BUSY;
            }
        }
        if (!s)
            s = ud_csv_write(c, path, empty, "\\N");
        if (!s)
            s = ud_publish(c, path, output);
        if (path && !s)
            ud_remove_file(c, path);
        ud_free(path);
        ud_batch_destroy(empty);
    } else if (!s)
        s = ud_publish(c, runs[0], output);
    /* Exact files owned by this invocation only. Never recurse or delete leftovers. */
    for (uint32_t i = 0; i < owned_n; i++) {
        ud_status clean = ud_remove_file(c, owned[i]);
        if (!s && clean)
            s = clean;
        ud_free(owned[i]);
    }
    ud_free(owned);
    ud_free(runs);
    ud_unlock(lock);
    return s;
}
