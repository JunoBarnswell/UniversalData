#include "ud/etl.h"
#include "ud/csv.h"
#include "ud/engine.h"
#include "../../../components/compute/internal.h"
#include "../../../components/data/internal.h"
typedef struct {
    char *name;
    uint64_t device, file, offset, record;
    uint8_t digest[32];
    int complete;
} input_entry;
struct ud_job {
    ud_context *ctx;
    ud_plan *plan;
    ud_accumulator *acc;
    ud_queue *queue;
    char *input, *state, *null_marker;
    uint32_t null_length;
    uint32_t batch_size, checkpoint_rows, checkpoint_ms, poll_ms;
    input_entry *entries;
    uint32_t count;
    uint64_t generation, uncommitted, last_checkpoint;
    void *lock;
    int failed, dirty;
    ud_checkpoint_observer observer;
    void *observer_user;
};
static int absolute_path(const char *s) {
#ifdef _WIN32
    return s && strlen(s) > 2 &&
           ((s[1] == ':' && (s[2] == '/' || s[2] == '\\')) || (s[0] == '\\' && s[1] == '\\'));
#else
    return s && s[0] == '/';
#endif
}
static ud_status generation_path(ud_context *c, const char *dir, uint64_t gen, const char *suffix,
                                 char **out) {
    char name[96];
    snprintf(name, sizeof(name), "generation-%020llu.%s", (unsigned long long)gen, suffix);
    return ud_join_path(c, dir, name, out);
}
static ud_status latest_number(ud_context *c, const char *dir, uint64_t *out) {
    char **list = NULL;
    uint32_t n = 0;
    *out = 0;
    UD_TRY(ud_list(c, dir, ".commit", &list, &n));
    ud_status s = UD_OK;
    for (uint32_t i = 0; i < n; i++) {
        const char *name = list[i];
        size_t len = strlen(name);
        if (len != 38 || memcmp(name, "generation-", 11) || strcmp(name + 31, ".commit")) {
            s = UD_CORRUPT;
            break;
        }
        uint64_t gen = 0;
        for (unsigned k = 11; k < 31; k++) {
            unsigned d = (unsigned char)name[k] - '0';
            if (d > 9 || gen > (UINT64_MAX - d) / 10) {
                s = UD_CORRUPT;
                break;
            }
            gen = gen * 10 + d;
        }
        if (s || gen != *out + 1) {
            s = UD_CORRUPT;
            break;
        }
        *out = gen;
    }
    ud_list_free(list, n);
    return s ? ud_fail(c, s, "non-contiguous or malformed checkpoint generations in %s", dir)
             : UD_OK;
}
static ud_status validate_commit(ud_context *c, const char *dir, uint64_t gen, char **state,
                                 char **csv) {
    *state = NULL;
    *csv = NULL;
    char *commit = NULL;
    ud_bytes bytes = {.ctx = c};
    ud_status s = generation_path(c, dir, gen, "commit", &commit);
    if (!s)
        s = ud_read_file(c, commit, &bytes);
    uint8_t hashes[64];
    uint64_t version = 0, number = 0;
    if (!s)
        s = ud_get_u64(&bytes, &version);
    if (!s)
        s = ud_get_u64(&bytes, &number);
    if (!s)
        s = ud_bytes_get(&bytes, hashes, 64);
    if (!s && (version != 1 || number != gen || bytes.pos != bytes.size))
        s = UD_CORRUPT;
    if (!s)
        s = generation_path(c, dir, gen, "state", state);
    if (!s)
        s = generation_path(c, dir, gen, "csv", csv);
    uint8_t actual[32];
    if (!s)
        s = ud_file_hash(c, *state, actual);
    if (!s && memcmp(actual, hashes, 32))
        s = UD_CORRUPT;
    if (!s)
        s = ud_file_hash(c, *csv, actual);
    if (!s && memcmp(actual, hashes + 32, 32))
        s = UD_CORRUPT;
    ud_free(commit);
    ud_bytes_clear(&bytes);
    if (s) {
        ud_free(*state);
        ud_free(*csv);
        *state = *csv = NULL;
        return ud_fail(c, s, "generation %llu is invalid; no rollback performed",
                       (unsigned long long)gen);
    }
    return UD_OK;
}
ud_status ud_snapshot_latest(ud_context *c, const char *dir, char **csv, uint64_t *gen) {
    if (!dir || !csv || !gen)
        return UD_INVALID;
    *csv = NULL;
    UD_TRY(latest_number(c, dir, gen));
    if (!*gen)
        return UD_END;
    char *state = NULL;
    ud_status s = validate_commit(c, dir, *gen, &state, csv);
    ud_free(state);
    return s;
}
static ud_status validate_inputs(ud_job *j) {
    for (uint32_t i = 0; i < j->count; i++) {
        input_entry *e = j->entries + i;
        char *path = NULL;
        UD_TRY(ud_join_path(j->ctx, j->input, e->name, &path));
        uint64_t dev, id;
        uint8_t digest[32];
        ud_status s = ud_file_identity(j->ctx, path, &dev, &id);
        if (!s)
            s = ud_file_hash(j->ctx, path, digest);
        if (!s && (dev != e->device || id != e->file || memcmp(digest, e->digest, 32)))
            s = ud_fail(j->ctx, UD_CORRUPT, "accepted input changed: %s", path);
        ud_free(path);
        if (s)
            return s;
    }
    return UD_OK;
}
static ud_status restore(ud_job *j) {
    UD_TRY(latest_number(j->ctx, j->state, &j->generation));
    if (!j->generation)
        return UD_OK;
    char *state = NULL, *csv = NULL;
    UD_TRY(validate_commit(j->ctx, j->state, j->generation, &state, &csv));
    ud_free(csv);
    ud_bytes b = {.ctx = j->ctx};
    ud_status s = ud_read_file(j->ctx, state, &b);
    ud_free(state);
    uint64_t version = 0, generation = 0, count = 0;
    uint8_t fingerprint[32];
    char *input = NULL, *marker = NULL;
    if (!s)
        s = ud_get_u64(&b, &version);
    if (!s)
        s = ud_get_u64(&b, &generation);
    if (!s)
        s = ud_bytes_get(&b, fingerprint, 32);
    if (!s)
        s = ud_get_string(&b, &input);
    if (!s)
        s = ud_get_string(&b, &marker);
    if (!s && (version != 1 || generation != j->generation ||
               memcmp(fingerprint, j->plan->fingerprint, 32) || strcmp(input, j->input) ||
               strcmp(marker, j->null_marker)))
        s = UD_INCOMPATIBLE;
    ud_free(input);
    ud_free(marker);
    if (!s)
        s = ud_get_u64(&b, &count);
    if (!s && (count > UINT32_MAX || count > SIZE_MAX / sizeof(input_entry)))
        s = UD_CORRUPT;
    if (!s && count) {
        j->entries = ud_alloc(j->ctx, (size_t)count * sizeof(input_entry));
        if (!j->entries)
            s = UD_RESOURCE;
        else
            j->count = (uint32_t)count;
    }
    for (uint32_t i = 0; !s && i < j->count; i++) {
        input_entry *e = j->entries + i;
        uint64_t complete = 0;
        s = ud_get_string(&b, &e->name);
        if (!s && (!*e->name || strchr(e->name, '/') || strchr(e->name, '\\') ||
                   !strcmp(e->name, ".") || !strcmp(e->name, "..")))
            s = UD_CORRUPT;
        if (!s)
            s = ud_get_u64(&b, &e->device);
        if (!s)
            s = ud_get_u64(&b, &e->file);
        if (!s)
            s = ud_bytes_get(&b, e->digest, 32);
        if (!s)
            s = ud_get_u64(&b, &e->offset);
        if (!s)
            s = ud_get_u64(&b, &e->record);
        if (!s)
            s = ud_get_u64(&b, &complete);
        if (!s && complete > 1)
            s = UD_CORRUPT;
        if (!s)
            e->complete = (int)complete;
        for (uint32_t k = 0; !s && k < i; k++)
            if (!strcmp(e->name, j->entries[k].name))
                s = UD_CORRUPT;
    }
    ud_batch *snapshot = NULL;
    if (!s)
        s = ud_decode_batch(&b, &snapshot);
    if (!s && b.pos != b.size)
        s = UD_CORRUPT;
    if (!s)
        s = ud_accumulator_restore(j->acc, snapshot);
    ud_batch_destroy(snapshot);
    ud_bytes_clear(&b);
    if (s)
        return ud_fail(
            j->ctx, s,
            "checkpoint cannot be restored; plan/schema/input must match generation %llu",
            (unsigned long long)j->generation);
    return validate_inputs(j);
}
static ud_status checkpoint(ud_job *j) {
    if (!j->dirty)
        return UD_OK;
    if (ud_is_cancelled(j->ctx))
        return UD_CANCELLED;
    UD_TRY(validate_inputs(j));
    if (j->generation == UINT64_MAX)
        return UD_OVERFLOW;
    uint64_t gen = j->generation + 1;
    ud_bytes b = {.ctx = j->ctx}, commit = {.ctx = j->ctx};
    ud_batch *snapshot = NULL;
    char *state = NULL, *csv = NULL, *tmp = NULL, *marker = NULL;
    ud_status s = ud_accumulator_snapshot(j->acc, &snapshot);
    if (!s)
        s = ud_put_u64(&b, 1);
    if (!s)
        s = ud_put_u64(&b, gen);
    if (!s)
        s = ud_bytes_put(&b, j->plan->fingerprint, 32);
    if (!s)
        s = ud_put_string(&b, j->input, strlen(j->input));
    if (!s)
        s = ud_put_string(&b, j->null_marker, j->null_length);
    if (!s)
        s = ud_put_u64(&b, j->count);
    for (uint32_t i = 0; !s && i < j->count; i++) {
        input_entry *e = j->entries + i;
        s = ud_put_string(&b, e->name, strlen(e->name));
        if (!s)
            s = ud_put_u64(&b, e->device);
        if (!s)
            s = ud_put_u64(&b, e->file);
        if (!s)
            s = ud_bytes_put(&b, e->digest, 32);
        if (!s)
            s = ud_put_u64(&b, e->offset);
        if (!s)
            s = ud_put_u64(&b, e->record);
        if (!s)
            s = ud_put_u64(&b, (uint64_t)e->complete);
    }
    if (!s)
        s = ud_encode_batch(&b, snapshot);
    if (!s)
        s = generation_path(j->ctx, j->state, gen, "state", &state);
    if (!s)
        s = generation_path(j->ctx, j->state, gen, "csv", &csv);
    if (!s)
        s = generation_path(j->ctx, j->state, gen, "commit.tmp", &tmp);
    if (!s)
        s = generation_path(j->ctx, j->state, gen, "commit", &marker);
    if (!s)
        s = ud_write_file(j->ctx, state, b.data, b.size);
    if (!s && j->observer)
        j->observer(j->observer_user, UD_STATE_WRITTEN, gen);
    if (!s)
        s = ud_csv_write(j->ctx, csv, snapshot, j->null_marker);
    if (!s && j->observer)
        j->observer(j->observer_user, UD_OUTPUT_WRITTEN, gen);
    uint8_t hashes[64];
    if (!s)
        ud_hash(b.data, b.size, hashes);
    if (!s)
        s = ud_file_hash(j->ctx, csv, hashes + 32);
    if (!s)
        s = ud_put_u64(&commit, 1);
    if (!s)
        s = ud_put_u64(&commit, gen);
    if (!s)
        s = ud_bytes_put(&commit, hashes, 64);
    if (!s)
        s = ud_write_file(j->ctx, tmp, commit.data, commit.size);
    if (!s)
        s = validate_inputs(j);
    if (!s && ud_is_cancelled(j->ctx))
        s = UD_CANCELLED;
    if (!s && j->observer)
        j->observer(j->observer_user, UD_BEFORE_COMMIT, gen);
    if (!s)
        s = ud_publish(j->ctx, tmp, marker);
    if (!s) {
        j->generation = gen;
        j->dirty = 0;
        j->uncommitted = 0;
        j->last_checkpoint = ud_now_ms();
        if (j->observer)
            j->observer(j->observer_user, UD_AFTER_COMMIT, gen);
    }
    ud_batch_destroy(snapshot);
    ud_bytes_clear(&b);
    ud_bytes_clear(&commit);
    ud_free(state);
    ud_free(csv);
    ud_free(tmp);
    ud_free(marker);
    return s;
}
ud_status ud_job_create(ud_context *ctx, const ud_job_options *o, ud_job **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !o || o->struct_size < sizeof(*o) || o->abi_version != 1 ||
        !absolute_path(o->input_directory) || !absolute_path(o->state_directory) ||
        !strcmp(o->input_directory, o->state_directory))
        return ud_fail(ctx, UD_INVALID,
                       "job requires ABI 1 and distinct absolute input/state paths");
    ud_job *j = ud_alloc(ctx, sizeof(*j));
    if (!j)
        return UD_RESOURCE;
    j->ctx = ctx;
    j->input = ud_strdup_n(ctx, o->input_directory, strlen(o->input_directory));
    j->state = ud_strdup_n(ctx, o->state_directory, strlen(o->state_directory));
    const char *null = o->null_marker ? o->null_marker : "\\N";
    j->null_length = o->null_marker ? o->null_marker_length : 2;
    if (!j->null_length || memchr(null, 0, j->null_length) || memchr(null, ',', j->null_length) ||
        memchr(null, '\n', j->null_length) || memchr(null, '\r', j->null_length) ||
        memchr(null, '"', j->null_length) || !ud_utf8_valid(null, j->null_length)) {
        ud_job_destroy(j);
        return UD_INVALID;
    }
    j->null_marker = ud_strdup_n(ctx, null, j->null_length);
    j->batch_size = o->batch_size ? o->batch_size : 4096;
    j->checkpoint_rows = o->checkpoint_rows ? o->checkpoint_rows : 10000;
    j->checkpoint_ms = o->checkpoint_ms ? o->checkpoint_ms : 5000;
    j->poll_ms = o->poll_ms ? o->poll_ms : 250;
    j->observer = o->observer;
    j->observer_user = o->observer_user;
    ud_status s = j->input && j->state && j->null_marker ? UD_OK : UD_RESOURCE;
    if (!s)
        s = ud_plan_create(ctx, o->schema, o->schema_length, o->commands, o->commands_length,
                           &j->plan);
    if (!s && !j->plan->agg_n)
        s = ud_fail(ctx, UD_INVALID, "recoverable CSV job requires at least one aggregate");
    if (!s)
        s = ud_accumulator_create(j->plan, &j->acc);
    if (!s)
        s = ud_queue_create(ctx, 1, &j->queue);
    if (!s)
        s = ud_mkdir(ctx, j->state);
    if (!s)
        s = ud_distinct_directories(ctx, j->input, j->state);
    char *lock = NULL;
    if (!s)
        s = ud_join_path(ctx, j->state, "writer.lock", &lock);
    if (!s)
        s = ud_lock(ctx, lock, &j->lock);
    ud_free(lock);
    if (!s)
        s = restore(j);
    if (s) {
        ud_job_destroy(j);
        return s;
    }
    j->last_checkpoint = ud_now_ms();
    *out = j;
    return UD_OK;
}
void ud_job_destroy(ud_job *j) {
    if (!j)
        return;
    ud_queue_destroy(j->queue);
    ud_accumulator_destroy(j->acc);
    ud_plan_destroy(j->plan);
    for (uint32_t i = 0; i < j->count; i++)
        ud_free(j->entries[i].name);
    ud_free(j->entries);
    ud_free(j->input);
    ud_free(j->state);
    ud_free(j->null_marker);
    ud_unlock(j->lock);
    ud_free(j);
}
static ud_status add_input(ud_job *j, const char *name, uint32_t *index) {
    if (j->count == UINT32_MAX || (size_t)(j->count + 1) > SIZE_MAX / sizeof(input_entry))
        return UD_RESOURCE;
    input_entry *entries = ud_alloc(j->ctx, (size_t)(j->count + 1) * sizeof(*entries));
    if (!entries)
        return UD_RESOURCE;
    if (j->count)
        memcpy(entries, j->entries, j->count * sizeof(*entries));
    input_entry *e = entries + j->count;
    e->name = ud_strdup_n(j->ctx, name, strlen(name));
    char *path = NULL;
    ud_status s = e->name ? ud_join_path(j->ctx, j->input, name, &path) : UD_RESOURCE;
    if (!s)
        s = ud_file_identity(j->ctx, path, &e->device, &e->file);
    if (!s)
        s = ud_file_hash(j->ctx, path, e->digest);
    ud_free(path);
    if (s) {
        ud_free(e->name);
        ud_free(entries);
        return s;
    }
    ud_free(j->entries);
    j->entries = entries;
    *index = j->count++;
    j->dirty = 1;
    return UD_OK;
}
static ud_status process_input(ud_job *j, uint32_t index) {
    input_entry *e = j->entries + index;
    if (e->complete)
        return UD_OK;
    char *path = NULL;
    UD_TRY(ud_join_path(j->ctx, j->input, e->name, &path));
    ud_csv_options o = {sizeof(o), 1, j->batch_size, j->null_marker, j->null_length};
    ud_csv_reader *reader = NULL;
    ud_status s = ud_csv_open(j->ctx, path, j->plan->fields, j->plan->n, &o, &reader);
    ud_free(path);
    if (!s && e->offset)
        s = ud_csv_resume(reader, e->offset, e->record);
    while (!s) {
        ud_batch *batch = NULL;
        s = ud_csv_next(reader, &batch);
        if (s == UD_END) {
            s = UD_OK;
            e->complete = 1;
            e->offset = ud_csv_offset(reader);
            e->record = ud_csv_record(reader);
            j->dirty = 1;
            break;
        }
        if (s)
            break;
        uint32_t rows = ud_batch_rows(batch);
        s = ud_queue_push(j->queue, batch);
        if (s) {
            ud_batch_destroy(batch);
            break;
        }
        batch = NULL;
        s = ud_queue_pop(j->queue, &batch);
        if (!s)
            s = ud_accumulator_add(j->acc, batch);
        ud_batch_destroy(batch);
        if (s)
            break;
        e->offset = ud_csv_offset(reader);
        e->record = ud_csv_record(reader);
        if (e->offset == UINT64_MAX) {
            s = UD_IO;
            break;
        }
        j->uncommitted += rows;
        j->dirty = 1;
        if (j->uncommitted >= j->checkpoint_rows ||
            ud_now_ms() - j->last_checkpoint >= j->checkpoint_ms)
            s = checkpoint(j);
    }
    ud_csv_close(reader);
    return s;
}
ud_status ud_job_poll(ud_job *j) {
    if (!j || j->failed)
        return UD_INVALID;
    if (ud_is_cancelled(j->ctx))
        return UD_CANCELLED;
    ud_status s = validate_inputs(j);
    char **files = NULL;
    uint32_t count = 0;
    if (!s)
        s = ud_list(j->ctx, j->input, ".csv", &files, &count);
    /* Resume accepted files before newly discovered files, preserving aggregate order. */
    for (uint32_t i = 0; !s && i < j->count; i++)
        s = process_input(j, i);
    for (uint32_t i = 0; !s && i < count; i++) {
        int found = 0;
        for (uint32_t k = 0; k < j->count; k++)
            if (!strcmp(files[i], j->entries[k].name)) {
                found = 1;
                break;
            }
        if (found)
            continue;
        uint32_t index;
        s = add_input(j, files[i], &index);
        if (!s)
            s = process_input(j, index);
    }
    ud_list_free(files, count);
    if (!s)
        s = checkpoint(j);
    if (s)
        j->failed = 1;
    return s;
}
ud_status ud_job_run(ud_job *j) {
    if (!j)
        return UD_INVALID;
    while (!ud_is_cancelled(j->ctx)) {
        UD_TRY(ud_job_poll(j));
        UD_TRY(ud_wait(j->ctx, j->poll_ms));
    }
    return UD_CANCELLED;
}
uint64_t ud_job_generation(const ud_job *j) {
    return j ? j->generation : 0;
}
