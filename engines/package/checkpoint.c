#include "internal.h"
#include <errno.h>
#include <sys/stat.h>
enum {
    RECORD_HEADER = 1,
    RECORD_TASK_BEGIN,
    RECORD_TASK_DONE,
    RECORD_SOURCE,
    RECORD_STAGE,
    RECORD_FILE_DONE,
    RECORD_BATCH_INTENT,
    RECORD_BATCH_DONE
};
typedef struct checkpoint_entry {
    uint64_t kind;
    ud_string key;
    ud_json *payload;
    struct checkpoint_entry *next;
} checkpoint_entry;
typedef struct checkpoint_journal {
    ud_execution *execution;
    char *directory;
    void *lock;
    uint64_t sequence;
    int failed;
    uint8_t previous[32];
    checkpoint_entry *entries;
} checkpoint_journal;
static checkpoint_entry *find(checkpoint_journal *j, uint64_t kind, ud_string key) {
    for (checkpoint_entry *p = j->entries; p; p = p->next)
        if (p->kind == kind && ud_same(p->key, key))
            return p;
    return NULL;
}
static ud_status grant(ud_execution *e, const char *path, int32_t action) {
    if (!e->options.authorize || !e->options.tenant.length)
        return UD_AUTH;
    ud_string resource = {path, (uint32_t)strlen(path)};
    return e->options.authorize(e->options.authorize_user, e->options.tenant, resource, action) ==
                   UD_OK
               ? UD_OK
               : UD_AUTH;
}
static void hex(const uint8_t hash[32], char out[65]) {
    static const char digits[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) {
        out[i * 2] = digits[hash[i] >> 4];
        out[i * 2 + 1] = digits[hash[i] & 15];
    }
    out[64] = 0;
}
static ud_status cache(checkpoint_journal *j, uint64_t kind, ud_string key, ud_string payload) {
    ud_json *document = NULL;
    UD_TRY(ud_json_parse(j->execution->ctx, payload.data, payload.length, &document, NULL));
    checkpoint_entry *p = find(j, kind, key);
    if (!p) {
        p = ud_alloc(j->execution->ctx, sizeof(*p));
        if (!p) {
            ud_json_destroy(document);
            return UD_RESOURCE;
        }
        p->key.data = ud_strdup_n(j->execution->ctx, key.data, key.length);
        p->key.length = key.length;
        p->kind = kind;
        if (!p->key.data) {
            ud_json_destroy(document);
            ud_free(p);
            return UD_RESOURCE;
        }
        p->next = j->entries;
        j->entries = p;
    }
    ud_json_destroy(p->payload);
    p->payload = document;
    return UD_OK;
}
static ud_status append(checkpoint_journal *j, ud_activation *activation, uint64_t kind, ud_string key, ud_string payload) {
    ud_execution *e = j->execution;
    if (j->failed)
        return UD_COMMIT_UNKNOWN;
    UD_TRY(grant(e, j->directory, UD_ACCESS_WRITE));
    if (j->sequence >= 1000000)
        return UD_RESOURCE;
    ud_bytes bytes = {.ctx = e->ctx};
    ud_status s = ud_put_u64(&bytes, UD_CHECKPOINT_VERSION);
    if (!s)
        s = ud_put_u64(&bytes, j->sequence + 1);
    if (!s)
        s = ud_bytes_put(&bytes, j->previous, 32);
    if (!s)
        s = ud_put_u64(&bytes, kind);
    if (!s)
        s = ud_put_string(&bytes, key.data, key.length);
    if (!s)
        s = ud_put_string(&bytes, payload.data, payload.length);
    uint8_t digest[32];
    if (!s) {
        ud_hash(bytes.data, bytes.size, digest);
        s = ud_bytes_put(&bytes, digest, 32);
    }
    char name[64], *path = NULL, *temporary = NULL;
    snprintf(name, sizeof(name), "record-%020llu.udj", (unsigned long long)(j->sequence + 1));
    if (!s)
        s = ud_join_path(e->ctx, j->directory, name, &path);
    if (!s) {
        size_t n = strlen(path);
        temporary = ud_alloc(e->ctx, n + 6);
        if (!temporary)
            s = UD_RESOURCE;
        else
            snprintf(temporary, n + 6, "%s.part", path);
    }
    if (!s) {
        ud_activation_notify_kind(activation, UD_EXEC_EVENT_BEFORE_CHECKPOINT, UD_EXEC_RUNNING, UD_OK);
        s = ud_write_file(e->ctx, temporary, bytes.data, bytes.size);
    }
    if (!s)
        s = ud_publish(e->ctx, temporary, path);
    if (!s) {
        j->sequence++;
        memcpy(j->previous, digest, 32);
        s = cache(j, kind, key, payload);
    }
    if (!s)
        ud_activation_notify_kind(activation, UD_EXEC_EVENT_AFTER_CHECKPOINT, UD_EXEC_RUNNING, UD_OK);
    if (temporary)
        (void)ud_remove_file(e->ctx, temporary);
    ud_free(path);
    ud_free(temporary);
    ud_bytes_clear(&bytes);
    if (s)
        j->failed = 1;
    return s;
}
static ud_status snapshot(ud_activation *e, ud_bytes *bytes) {
    UD_TRY(ud_bytes_put(bytes, "{\"scopes\":[", 11));
    uint32_t depth = 0;
    for (ud_scope *scope = e->scope; scope && (!e->parallel || scope->owner == e); scope = scope->parent, depth++) {
        if (depth)
            UD_TRY(ud_bytes_put(bytes, ",", 1));
        UD_TRY(ud_bytes_put(bytes, "[", 1));
        uint32_t count = 0;
        for (ud_variable *v = scope->values; v; v = v->next, count++) {
            if (count)
                UD_TRY(ud_bytes_put(bytes, ",", 1));
            UD_TRY(ud_bytes_put(bytes, "{\"name\":", 8));
            UD_TRY(ud_json_quote(bytes, v->name.data, v->name.length));
            char type[48];
            int n = snprintf(type, sizeof(type), ",\"type\":%d,\"schema\":", v->value.type);
            UD_TRY(ud_bytes_put(bytes, type, (size_t)n));
            char *schema = NULL;
            uint32_t schema_length = 0;
            UD_TRY(ud_schema_to_json(v->box, &schema, &schema_length));
            UD_TRY(ud_bytes_put(bytes, schema, schema_length));
            ud_free(schema);
            UD_TRY(ud_bytes_put(bytes, ",\"value\":", 9));
            ud_field field;
            UD_TRY(ud_batch_field(v->box, 0, &field));
            UD_TRY(ud_json_value_encode(bytes, &field, &v->value));
            UD_TRY(ud_bytes_put(bytes, "}", 1));
        }
        UD_TRY(ud_bytes_put(bytes, "]", 1));
    }
    char metrics[512];
    int n = snprintf(
        metrics, sizeof(metrics),
        "],\"invocation\":%llu,\"input\":%llu,\"output\":%llu,\"rejected\":%llu,\"tasks\":%llu}",
        (unsigned long long)e->invocation, (unsigned long long)atomic_load(&e->execution->input_rows),
        (unsigned long long)atomic_load(&e->execution->output_rows),
        (unsigned long long)atomic_load(&e->execution->rejected_rows),
        (unsigned long long)atomic_load(&e->execution->completed_tasks));
    return ud_bytes_put(bytes, metrics, (size_t)n);
}
static ud_status restore(ud_activation *e, const ud_json *payload) {
    const ud_json *scopes = ud_member(payload, "scopes");
    if (ud_json_kind(scopes) != UD_JSON_ARRAY)
        return UD_CORRUPT;
    ud_scope *scope = e->scope;
    for (uint32_t i = 0; i < ud_json_size(scopes); i++) {
        if (!scope)
            return UD_CORRUPT;
        const ud_json *values = ud_json_at(scopes, i);
        if (ud_json_kind(values) != UD_JSON_ARRAY)
            return UD_CORRUPT;
        uint32_t count = 0;
        for (ud_variable *v = scope->values; v; v = v->next)
            count++;
        if (count != ud_json_size(values))
            return UD_CORRUPT;
        for (uint32_t k = 0; k < ud_json_size(values); k++) {
            const ud_json *record = ud_json_at(values, k);
            ud_string name;
            uint64_t type;
            UD_TRY(ud_json_string(ud_member(record, "name"), &name));
            UD_TRY(ud_json_uint(ud_member(record, "type"), &type));
            ud_variable *v = scope->values;
            while (v && !ud_same(v->name, name))
                v = v->next;
            if (!v || type != (uint64_t)v->value.type)
                return UD_CORRUPT;
            ud_field expected, restored;
            UD_TRY(ud_batch_field(v->box, 0, &expected));
            char *schema_json = NULL;
            uint32_t schema_length = 0;
            UD_TRY(ud_json_serialize(ud_member(record, "schema"), &schema_json, &schema_length));
            ud_batch *schema = NULL;
            ud_status status = ud_schema_from_json(e->execution->ctx, schema_json, schema_length,
                                                   (ud_string){0}, &schema);
            ud_free(schema_json);
            if (status) return UD_CORRUPT;
            status = ud_batch_field(schema, 0, &restored);
            int same = !status && ud_field_matches(&expected, &restored, UD_FIELD_PORT);
            ud_batch_destroy(schema);
            if (!same) return UD_CORRUPT;
            ud_value decoded = {0};
            status = ud_json_value_decode(e->execution->ctx, &expected, ud_member(record, "value"),
                                          UD_ROUND_EXACT, &decoded);
            if (!status) status = ud_variable_assign(e->execution->ctx, v, &decoded, &expected);
            ud_clear_value(&decoded);
            if (status) return status;
        }
        scope = scope->parent;
    }
    if (scope && (!e->parallel || scope->owner == e))
        return UD_CORRUPT;
    uint64_t value;
    UD_TRY(ud_json_uint(ud_member(payload, "invocation"), &value));
    e->invocation = value;
    UD_TRY(ud_json_uint(ud_member(payload, "input"), &value));
    if (!e->parallel || value > atomic_load(&e->execution->input_rows)) atomic_store(&e->execution->input_rows, value);
    UD_TRY(ud_json_uint(ud_member(payload, "output"), &value));
    if (!e->parallel || value > atomic_load(&e->execution->output_rows)) atomic_store(&e->execution->output_rows, value);
    UD_TRY(ud_json_uint(ud_member(payload, "rejected"), &value));
    if (!e->parallel || value > atomic_load(&e->execution->rejected_rows)) atomic_store(&e->execution->rejected_rows, value);
    UD_TRY(ud_json_uint(ud_member(payload, "tasks"), &value));
    if (!e->parallel || value > atomic_load(&e->execution->completed_tasks)) atomic_store(&e->execution->completed_tasks, value);
    return UD_OK;
}
static ud_status file_record(checkpoint_journal *j, ud_activation *activation, uint64_t kind, const char *path) {
    ud_execution *e = j->execution;
    UD_TRY(grant(e, path, UD_ACCESS_READ));
    uint8_t digest[32];
    UD_TRY(ud_file_hash(e->ctx, path, digest));
    char encoded[65];
    hex(digest, encoded);
    ud_string key = {path, (uint32_t)strlen(path)};
    checkpoint_entry *p = find(j, kind, key);
    if (p) {
        ud_string expected;
        UD_TRY(ud_json_string(p->payload, &expected));
        return ud_named(expected, encoded) ? UD_OK : UD_CORRUPT;
    }
    char payload[68];
    snprintf(payload, sizeof(payload), "\"%s\"", encoded);
    return append(j, activation, kind, key, (ud_string){payload, 66});
}
ud_status ud_checkpoint_open(ud_execution *e) {
    if (!e->options.state_directory.length)
        return e->options.resume ? UD_INVALID : UD_OK;
    if (!e->options.execution_id.length || !e->options.state_directory.data ||
        memchr(e->options.state_directory.data, 0, e->options.state_directory.length))
        return UD_INVALID;
#ifdef _WIN32
    if (e->options.state_directory.length < 3 || e->options.state_directory.data[1] != ':')
        return UD_INVALID;
#else
    if (e->options.state_directory.data[0] != '/')
        return UD_INVALID;
#endif
    checkpoint_journal *j = ud_alloc(e->ctx, sizeof(*j));
    if (!j)
        return UD_RESOURCE;
    j->execution = e;
    e->journal = j;
    j->directory =
        ud_strdup_n(e->ctx, e->options.state_directory.data, e->options.state_directory.length);
    if (!j->directory)
        return UD_RESOURCE;
    UD_TRY(grant(e, j->directory, UD_ACCESS_OPEN));
    UD_TRY(grant(e, j->directory, UD_ACCESS_WRITE));
    UD_TRY(ud_mkdir(e->ctx, j->directory));
    char *lock = NULL;
    ud_status s = ud_join_path(e->ctx, j->directory, "writer.lock", &lock);
    if (!s)
        s = ud_lock(e->ctx, lock, &j->lock);
    ud_free(lock);
    if (s)
        return s;
    char **names = NULL;
    uint32_t count = 0;
    UD_TRY(ud_list(e->ctx, j->directory, ".udj", &names, &count));
    if ((count && !e->options.resume) || (!count && e->options.resume)) {
        ud_list_free(names, count);
        return count ? UD_BUSY : UD_CORRUPT;
    }
    for (uint32_t i = 0; !s && i < count; i++) {
        char expected[64];
        snprintf(expected, sizeof(expected), "record-%020llu.udj", (unsigned long long)i + 1);
        if (strcmp(names[i], expected)) {
            s = UD_CORRUPT;
            break;
        }
        char *path = NULL;
        s = ud_join_path(e->ctx, j->directory, names[i], &path);
        ud_bytes bytes = {.ctx = e->ctx};
        if (!s)
            s = ud_read_file(e->ctx, path, &bytes);
        ud_free(path);
        uint8_t digest[32];
        if (!s && bytes.size < 88)
            s = UD_CORRUPT;
        if (!s) {
            ud_hash(bytes.data, bytes.size - 32, digest);
            if (memcmp(digest, bytes.data + bytes.size - 32, 32))
                s = UD_CORRUPT;
        }
        if (!s)
            bytes.size -= 32;
        uint64_t version = 0, sequence = 0, kind = 0;
        uint8_t previous[32];
        char *key = NULL, *payload = NULL;
        if (!s)
            s = ud_get_u64(&bytes, &version);
        if (!s)
            s = ud_get_u64(&bytes, &sequence);
        if (!s)
            s = ud_bytes_get(&bytes, previous, 32);
        if (!s && (version != UD_CHECKPOINT_VERSION || sequence != i + 1 ||
                   memcmp(previous, j->previous, 32)))
            s = UD_CORRUPT;
        if (!s)
            s = ud_get_u64(&bytes, &kind);
        if (!s)
            s = ud_get_string(&bytes, &key);
        if (!s)
            s = ud_get_string(&bytes, &payload);
        if (!s && (bytes.pos != bytes.size || kind < RECORD_HEADER || kind > RECORD_BATCH_DONE ||
                   (i == 0 && kind != RECORD_HEADER) || (i > 0 && kind == RECORD_HEADER)))
            s = UD_CORRUPT;
        if (!s)
            s = cache(j, kind, (ud_string){key, (uint32_t)strlen(key)},
                      (ud_string){payload, (uint32_t)strlen(payload)});
        if (!s) {
            j->sequence = sequence;
            memcpy(j->previous, digest, 32);
        }
        ud_free(key);
        ud_free(payload);
        ud_bytes_clear(&bytes);
    }
    ud_list_free(names, count);
    if (s)
        return s;
    ud_bytes manifest = {.ctx = e->ctx};
    s = ud_bytes_put(&manifest, e->package->fingerprint, 32);
    char *parameters = NULL;
    uint32_t length = 0;
    if (!s)
        s = ud_json_serialize(e->parameters, &parameters, &length);
    if (!s)
        s = ud_put_string(&manifest, parameters, length);
    ud_free(parameters);
    if (!s)
        s = ud_put_string(&manifest, e->options.tenant.data, e->options.tenant.length);
    if (!s)
        s = ud_put_string(&manifest, e->options.execution_id.data, e->options.execution_id.length);
    if (!s)
        s = ud_put_u64(&manifest, e->options.max_batch_rows);
    if (!s)
        s = ud_put_u64(&manifest, e->options.edge_capacity);
    if (!s) s = ud_put_u64(&manifest, e->options.max_concurrency);
    uint8_t digest[32];
    char encoded[65], payload[68];
    if (!s) {
        ud_hash(manifest.data, manifest.size, digest);
        hex(digest, encoded);
        snprintf(payload, sizeof(payload), "\"%s\"", encoded);
    }
    ud_bytes_clear(&manifest);
    if (s)
        return s;
    if (count) {
        checkpoint_entry *header = find(j, RECORD_HEADER, e->options.execution_id);
        ud_string hash;
        if (!header || ud_json_string(header->payload, &hash) || !ud_named(hash, encoded))
            return UD_INCOMPATIBLE;
        for (checkpoint_entry *p = j->entries; p; p = p->next)
            if (p->kind == RECORD_SOURCE || p->kind == RECORD_FILE_DONE) {
                char *path = ud_strdup_n(e->ctx, p->key.data, p->key.length);
                if (!path)
                    return UD_RESOURCE;
                s = file_record(j, &e->root, p->kind, path);
                ud_free(path);
                if (s)
                    return s;
            }
    } else
        s = append(j, &e->root, RECORD_HEADER, e->options.execution_id, (ud_string){payload, 66});
    return s;
}
static ud_status ud_checkpoint_task_begin_record(ud_activation *e, ud_string kind, int *completed) {
    *completed = 0;
    checkpoint_journal *j = e->execution->journal;
    if (!j)
        return UD_OK;
    checkpoint_entry *done = find(j, RECORD_TASK_DONE, e->task_id);
    if (done) {
        UD_TRY(restore(e, done->payload));
        *completed = 1;
        return UD_OK;
    }
    checkpoint_entry *begin = find(j, RECORD_TASK_BEGIN, e->task_id);
    if (begin) {
        int replay = ud_named(kind, "sequence") || ud_named(kind, "for") ||
                     ud_named(kind, "foreach") || ud_named(kind, "dataFlow") ||
                     ud_named(kind, "set") || ud_named(kind, "executePackage");
        if (!replay) {
            ud_component_descriptor d = {.struct_size = sizeof(d), .abi_version = UD_ABI_VERSION};
            UD_TRY(ud_component_lookup(e, kind, &d));
            replay = (d.capabilities & UD_COMPONENT_REPLAY_SAFE) != 0;
        }
        return replay ? UD_OK : UD_COMMIT_UNKNOWN;
    }
    ud_bytes value = {.ctx = e->execution->ctx};
    ud_status s = snapshot(e, &value);
    if (!s)
        s = append(j, e, RECORD_TASK_BEGIN, e->task_id,
                   (ud_string){(char *)value.data, (uint32_t)value.size});
    ud_bytes_clear(&value);
    return s;
}
static ud_status ud_checkpoint_task_done_record(ud_activation *e) {
    checkpoint_journal *j = e->execution->journal;
    if (!j)
        return UD_OK;
    ud_bytes value = {.ctx = e->execution->ctx};
    ud_status s = snapshot(e, &value);
    if (!s)
        s = append(j, e, RECORD_TASK_DONE, e->task_id,
                   (ud_string){(char *)value.data, (uint32_t)value.size});
    ud_bytes_clear(&value);
    return s ? UD_COMMIT_UNKNOWN : UD_OK;
}
static ud_status ud_checkpoint_source_record(ud_activation *e, const char *path) {
    return e->execution->journal ? file_record(e->execution->journal, e, RECORD_SOURCE, path) : UD_OK;
}
static ud_status ud_checkpoint_stage_record(ud_activation *e, const char *target, const char *temporary,
                              int *replaying) {
    *replaying = 0;
    checkpoint_journal *j = e->execution->journal;
    if (!j)
        return UD_OK;
    ud_string key = {target, (uint32_t)strlen(target)};
    checkpoint_entry *entry = find(j, RECORD_STAGE, key);
    if (entry) {
        ud_string owned;
        UD_TRY(ud_json_string(entry->payload, &owned));
        if (!ud_named(owned, temporary))
            return UD_CORRUPT;
        *replaying = 1;
        return UD_OK;
    }
    struct stat info;
    if (!stat(target, &info) || !stat(temporary, &info))
        return UD_BUSY;
    if (errno != ENOENT)
        return UD_IO;
    ud_bytes payload = {.ctx = e->execution->ctx};
    ud_status s = ud_json_quote(&payload, temporary, (uint32_t)strlen(temporary));
    if (!s)
        s = append(j, e, RECORD_STAGE, key, (ud_string){(char *)payload.data, (uint32_t)payload.size});
    ud_bytes_clear(&payload);
    return s;
}
static ud_status ud_checkpoint_file_done_record(ud_activation *e, const char *path) {
    return e->execution->journal ? file_record(e->execution->journal, e, RECORD_FILE_DONE, path) : UD_OK;
}
static int ud_checkpoint_batch_seen_record(ud_activation *e, ud_string identity) {
    checkpoint_journal *j = e->execution->journal;
    return j && find(j, RECORD_BATCH_INTENT, identity) != NULL;
}
static ud_status ud_checkpoint_batch_record(ud_activation *e, ud_string identity, const uint8_t digest[32],
                              int committed) {
    checkpoint_journal *j = e->execution->journal;
    if (!j)
        return UD_OK;
    uint64_t kind = committed ? RECORD_BATCH_DONE : RECORD_BATCH_INTENT;
    char encoded[65], payload[68];
    hex(digest, encoded);
    snprintf(payload, sizeof(payload), "\"%s\"", encoded);
    checkpoint_entry *entry = find(j, kind, identity);
    if (entry) {
        ud_string hash;
        UD_TRY(ud_json_string(entry->payload, &hash));
        return ud_named(hash, encoded) ? UD_OK : UD_CORRUPT;
    }
    return append(j, e, kind, identity, (ud_string){payload, 66});
}
void ud_checkpoint_close(ud_execution *e) {
    checkpoint_journal *j = e->journal;
    if (!j)
        return;
    checkpoint_entry *p = j->entries;
    while (p) {
        checkpoint_entry *next = p->next;
        ud_json_destroy(p->payload);
        ud_free((void *)p->key.data);
        ud_free(p);
        p = next;
    }
    ud_unlock(j->lock);
    ud_free(j->directory);
    ud_free(j);
    e->journal = NULL;
}

ud_status ud_checkpoint_task_begin(ud_activation *e, ud_string kind, int *completed) {
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_task_begin_record(e, kind, completed);
    ud_mutex_unlock(&e->execution->journal_lock);
    return result;
}

ud_status ud_checkpoint_task_done(ud_activation *e) {
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_task_done_record(e);
    ud_mutex_unlock(&e->execution->journal_lock);
    return result;
}

ud_status ud_checkpoint_source(ud_activation *e, const char *path) {
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_source_record(e, path);
    ud_mutex_unlock(&e->execution->journal_lock);
    return result;
}

ud_status ud_checkpoint_stage(ud_activation *e, const char *target, const char *temporary, int *replaying) {
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_stage_record(e, target, temporary, replaying);
    ud_mutex_unlock(&e->execution->journal_lock);
    return result;
}

ud_status ud_checkpoint_file_done(ud_activation *e, const char *path) {
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_file_done_record(e, path);
    ud_mutex_unlock(&e->execution->journal_lock);
    return result;
}

int ud_checkpoint_batch_seen(ud_activation *e, ud_string identity) {
    ud_mutex_lock(&e->execution->journal_lock);
    int result = ud_checkpoint_batch_seen_record(e, identity);
    ud_mutex_unlock(&e->execution->journal_lock);
    return result;
}

ud_status ud_checkpoint_batch(ud_activation *e, ud_string identity, const uint8_t digest[32], int committed) {
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_batch_record(e, identity, digest, committed);
    ud_mutex_unlock(&e->execution->journal_lock);
    return result;
}
