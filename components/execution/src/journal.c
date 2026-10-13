#include "../journal.h"
#include "../../foundation/internal.h"

#define SEGMENT_BYTES UINT64_C(1048576)
#define SEGMENT_FRAMES UINT64_C(2048)
#define COMMIT_MARKER UINT64_C(0x35444e45544d4355)
#define FRAME_LIMIT UINT64_C(16777216)
typedef struct journal_entry {
    uint64_t kind, hash, owner_hash;
    ud_string key, owner;
    ud_json *payload;
    struct journal_entry *bucket_next, *owner_next, *next, *previous;
} journal_entry;
struct ud_journal {
    ud_context *ctx;
    char *directory, *segment;
    void *writer;
    uint64_t sequence, segment_start, segment_bytes, segment_frames, snapshot_sequence;
    uint8_t previous[32];
    uint8_t acknowledged_hash[32];
    uint64_t acknowledged;
    int acknowledged_seen;
    int failed;
    journal_entry *entries, **index, **owners;
    uint32_t capacity, count;
    ud_journal_metrics metrics;
};
static uint64_t hash_key(uint64_t kind, ud_string key) {
    uint64_t hash = UINT64_C(14695981039346656037) ^ kind;
    for (uint32_t i = 0; i < key.length; i++)
        hash = (hash ^ (uint8_t)key.data[i]) * UINT64_C(1099511628211);
    return hash;
}
static int equal(ud_string a, ud_string b) {
    return a.length == b.length && (!a.length || !memcmp(a.data, b.data, a.length));
}
static journal_entry *lookup(ud_journal *journal, uint64_t kind, ud_string key) {
    journal->metrics.lookups++;
    uint64_t hash = hash_key(kind, key);
    journal_entry *entry =
        journal->capacity ? journal->index[hash & (journal->capacity - 1)] : NULL;
    for (; entry; entry = entry->bucket_next) {
        journal->metrics.probes++;
        if (entry->hash == hash && entry->kind == kind && equal(entry->key, key))
            return entry;
    }
    return NULL;
}
const ud_json *ud_journal_find(ud_journal *journal, uint64_t kind, ud_string key) {
    journal_entry *entry = journal ? lookup(journal, kind, key) : NULL;
    return entry ? entry->payload : NULL;
}
static void destroy_entry(journal_entry *entry) {
    if (!entry)
        return;
    ud_json_destroy(entry->payload);
    ud_free((void *)entry->key.data);
    ud_free((void *)entry->owner.data);
    ud_free(entry);
}
static void remove_entry(ud_journal *journal, journal_entry *entry) {
    journal_entry **link = journal->index + (entry->hash & (journal->capacity - 1));
    while (*link != entry)
        link = &(*link)->bucket_next;
    *link = entry->bucket_next;
    if (entry->owner.length) {
        link = journal->owners + (entry->owner_hash & (journal->capacity - 1));
        while (*link != entry)
            link = &(*link)->owner_next;
        *link = entry->owner_next;
    }
    if (entry->previous)
        entry->previous->next = entry->next;
    else
        journal->entries = entry->next;
    if (entry->next)
        entry->next->previous = entry->previous;
    journal->count--;
    destroy_entry(entry);
}
static void retire_owner(ud_journal *journal, ud_string owner) {
    if (!journal->capacity)
        return;
    uint64_t hash = hash_key(0, owner);
    journal_entry *entry = journal->owners[hash & (journal->capacity - 1)];
    while (entry) {
        journal_entry *next = entry->owner_next;
        if (entry->owner_hash == hash && equal(entry->owner, owner))
            remove_entry(journal, entry);
        entry = next;
    }
}
static ud_status reserve_index(ud_journal *journal, uint32_t added) {
    if (added > UINT32_MAX - journal->count)
        return UD_RESOURCE;
    uint32_t capacity = journal->capacity ? journal->capacity : 64;
    while (journal->count + added > capacity / 2) {
        if (capacity > UINT32_MAX / 2)
            return UD_RESOURCE;
        capacity *= 2;
    }
    if (capacity == journal->capacity)
        return UD_OK;
    journal_entry **index = ud_alloc(journal->ctx, (size_t)capacity * sizeof(*index));
    journal_entry **owners = ud_alloc(journal->ctx, (size_t)capacity * sizeof(*owners));
    if (!index || !owners) {
        ud_free(index);
        ud_free(owners);
        return UD_RESOURCE;
    }
    for (journal_entry *entry = journal->entries; entry; entry = entry->next) {
        uint32_t slot = (uint32_t)(entry->hash & (capacity - 1));
        entry->bucket_next = index[slot];
        index[slot] = entry;
        if (entry->owner.length) {
            slot = (uint32_t)(entry->owner_hash & (capacity - 1));
            entry->owner_next = owners[slot];
            owners[slot] = entry;
        }
    }
    ud_free(journal->index);
    ud_free(journal->owners);
    journal->index = index;
    journal->owners = owners;
    journal->capacity = capacity;
    return UD_OK;
}
static ud_status prepare_entry(ud_journal *journal, const ud_journal_record *record,
                               journal_entry **out) {
    *out = NULL;
    if (!record->kind)
        return record->owner.length && record->owner.data &&
                       !memchr(record->owner.data, 0, record->owner.length) &&
                       !record->key.length && !record->payload.length
                   ? UD_OK
                   : UD_INVALID;
    if (!record->key.length || !record->key.data || !record->payload.length ||
        !record->payload.data || (record->owner.length && !record->owner.data) ||
        memchr(record->key.data, 0, record->key.length) ||
        (record->owner.length && memchr(record->owner.data, 0, record->owner.length)))
        return UD_INVALID;
    journal_entry *entry = ud_alloc(journal->ctx, sizeof(*entry));
    if (!entry)
        return UD_RESOURCE;
    entry->kind = record->kind;
    entry->hash = hash_key(record->kind, record->key);
    entry->owner_hash = hash_key(0, record->owner);
    entry->key = (ud_string){ud_strdup_n(journal->ctx, record->key.data, record->key.length),
                             record->key.length};
    if (record->owner.length)
        entry->owner =
            (ud_string){ud_strdup_n(journal->ctx, record->owner.data, record->owner.length),
                        record->owner.length};
    ud_status status = !entry->key.data || (record->owner.length && !entry->owner.data)
                           ? UD_RESOURCE
                           : ud_json_parse(journal->ctx, record->payload.data,
                                           record->payload.length, &entry->payload, NULL);
    if (!status && record->kind == UINT64_MAX) {
        const char *text = NULL;
        uint32_t n = 0;
        uint64_t value = 0;
        status = ud_json_text(entry->payload, &text, &n);
        if (!status && (ud_json_kind(entry->payload) != UD_JSON_NUMBER || !n || n > 20 ||
                        record->owner.length))
            status = UD_CORRUPT;
        for (uint32_t i = 0; !status && i < n; i++) {
            if (text[i] < '0' || text[i] > '9' ||
                value > (UINT64_MAX - (unsigned)(text[i] - '0')) / 10)
                status = UD_CORRUPT;
            else
                value = value * 10 + (unsigned)(text[i] - '0');
        }
        if (!status && (!value || value == UINT64_MAX))
            status = UD_CORRUPT;
    }
    if (status)
        destroy_entry(entry);
    else
        *out = entry;
    return status;
}
static void apply(ud_journal *journal, const ud_journal_record *records, journal_entry **entries,
                  uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        journal_entry *entry = entries[i];
        /* A durable record transfers its prepared entry to apply exactly once,
         * regardless of insertion, replacement or targeted retirement. */
        entries[i] = NULL;
        if (!entry) {
            retire_owner(journal, records[i].owner);
            continue;
        }
        if (entry->kind == UINT64_MAX) {
            const char *text = NULL;
            uint32_t n = 0;
            uint64_t kind = 0;
            ud_json_text(entry->payload, &text, &n);
            for (uint32_t j = 0; j < n; j++)
                kind = kind * 10 + (uint64_t)(text[j] - '0');
            journal_entry *old = lookup(journal, kind, entry->key);
            if (old)
                remove_entry(journal, old);
            destroy_entry(entry);
            continue;
        }
        journal_entry *old = lookup(journal, entry->kind, entry->key);
        if (old)
            remove_entry(journal, old);
        uint32_t slot = (uint32_t)(entry->hash & (journal->capacity - 1));
        entry->bucket_next = journal->index[slot];
        journal->index[slot] = entry;
        if (entry->owner.length) {
            slot = (uint32_t)(entry->owner_hash & (journal->capacity - 1));
            entry->owner_next = journal->owners[slot];
            journal->owners[slot] = entry;
        }
        entry->next = journal->entries;
        if (entry->next)
            entry->next->previous = entry;
        journal->entries = entry;
        journal->count++;
    }
}
static ud_status encode_record(ud_bytes *bytes, const ud_journal_record *record) {
    UD_TRY(ud_put_u64(bytes, record->kind));
    UD_TRY(ud_put_string(bytes, record->key.data, record->key.length));
    UD_TRY(ud_put_string(bytes, record->owner.data, record->owner.length));
    return ud_put_string(bytes, record->payload.data, record->payload.length);
}
static ud_status named_path(ud_journal *journal, const char *prefix, uint64_t sequence,
                            const char *suffix, char **out) {
    char name[80];
    snprintf(name, sizeof(name), "%s-%020llu.%s", prefix, (unsigned long long)sequence, suffix);
    return ud_join_path(journal->ctx, journal->directory, name, out);
}
static ud_status new_segment(ud_journal *journal) {
    if (journal->segment)
        return UD_OK;
    char *path = NULL;
    UD_TRY(named_path(journal, "segment", journal->sequence + 1, "udg", &path));
    ud_status status = ud_path_no_symlinks(journal->ctx, path);
    if (!status)
        status = ud_reserve_file(journal->ctx, path);
    if (!status)
        status = ud_sync_parent(journal->ctx, path);
    if (status)
        ud_free(path);
    else {
        journal->segment = path;
        journal->segment_start = journal->sequence + 1;
        journal->metrics.segment_files++;
    }
    return status;
}
static ud_status publish_bytes(ud_journal *journal, const char *target, const ud_bytes *bytes,
                               int replace) {
    size_t length = strlen(target);
    char *temporary = ud_alloc(journal->ctx, length + 6);
    if (!temporary)
        return UD_RESOURCE;
    snprintf(temporary, length + 6, "%s.part", target);
    /* Fixed temporary belongs to the fenced writer. It never constitutes a
     * committed generation and recovery does not promote it. */
    ud_status status = ud_path_no_symlinks(journal->ctx, temporary);
    if (!status)
        status = ud_write_file(journal->ctx, temporary, bytes->data, bytes->size);
    if (!status)
        status = replace ? ud_replace_file(journal->ctx, temporary, target)
                         : ud_publish(journal->ctx, temporary, target);
    ud_free(temporary);
    return status;
}
static ud_status blob_path(ud_journal *journal, ud_string digest, char **out) {
    if (digest.length != 64 || !digest.data)
        return UD_CORRUPT;
    for (uint32_t i = 0; i < digest.length; i++)
        if (!((digest.data[i] >= '0' && digest.data[i] <= '9') ||
              (digest.data[i] >= 'a' && digest.data[i] <= 'f')))
            return UD_CORRUPT;
    char name[80];
    snprintf(name, sizeof(name), "blob-%.*s.udb", 64, digest.data);
    return ud_join_path(journal->ctx, journal->directory, name, out);
}
ud_status ud_journal_read_blob(ud_journal *journal, ud_string digest, uint64_t length,
                               ud_bytes *out) {
    if (!journal || !out || !length || length > FRAME_LIMIT)
        return UD_CORRUPT;
    char *path = NULL;
    UD_TRY(blob_path(journal, digest, &path));
    ud_status status = ud_path_no_symlinks(journal->ctx, path);
    out->limit = (size_t)length;
    if (!status)
        status = ud_read_file(journal->ctx, path, out);
    ud_free(path);
    uint8_t hash[32];
    char encoded[65];
    static const char hex[] = "0123456789abcdef";
    if (!status && out->size != length)
        status = UD_CORRUPT;
    if (!status) {
        ud_hash(out->data, out->size, hash);
        for (uint32_t i = 0; i < 32; i++) {
            encoded[i * 2] = hex[hash[i] >> 4];
            encoded[i * 2 + 1] = hex[hash[i] & 15];
        }
        if (memcmp(encoded, digest.data, 64))
            status = UD_CORRUPT;
    }
    return status == UD_IO ? UD_CORRUPT : status;
}
ud_status ud_journal_write_blob(ud_journal *journal, const void *data, uint64_t length,
                                char digest[65]) {
    if (!journal || !data || !digest || !length || length > FRAME_LIMIT)
        return UD_RESOURCE;
    if (journal->failed)
        return UD_COMMIT_UNKNOWN;
    uint8_t hash[32];
    ud_hash(data, (size_t)length, hash);
    static const char hex[] = "0123456789abcdef";
    for (uint32_t i = 0; i < 32; i++) {
        digest[i * 2] = hex[hash[i] >> 4];
        digest[i * 2 + 1] = hex[hash[i] & 15];
    }
    digest[64] = 0;
    char *path = NULL;
    UD_TRY(blob_path(journal, (ud_string){digest, 64}, &path));
    ud_status status = ud_path_no_symlinks(journal->ctx, path);
    int kind = 0;
    if (!status)
        status = ud_path_kind(journal->ctx, path, &kind);
    if (!status && kind) {
        ud_bytes existing = {.ctx = journal->ctx};
        status = kind == 1
                     ? ud_journal_read_blob(journal, (ud_string){digest, 64}, length, &existing)
                     : UD_CORRUPT;
        ud_bytes_clear(&existing);
    } else if (!status) {
        ud_bytes bytes = {.data = (uint8_t *)data, .size = (size_t)length};
        status = publish_bytes(journal, path, &bytes, 0);
    }
    ud_free(path);
    return status;
}
static ud_status publish_tail(ud_journal *journal, uint64_t sequence, const uint8_t anchor[32]) {
    ud_bytes bytes = {.ctx = journal->ctx};
    ud_status status = ud_put_u64(&bytes, UD_CHECKPOINT_VERSION);
    if (!status)
        status = ud_put_u64(&bytes, sequence);
    if (!status)
        status = ud_bytes_put(&bytes, anchor, 32);
    uint8_t hash[32];
    if (!status) {
        ud_hash(bytes.data, bytes.size, hash);
        status = ud_bytes_put(&bytes, hash, 32);
    }
    char *path = NULL;
    if (!status)
        status = ud_join_path(journal->ctx, journal->directory, "journal.tail", &path);
    if (!status)
        status = publish_bytes(journal, path, &bytes, 1);
    ud_free(path);
    ud_bytes_clear(&bytes);
    return status;
}
static ud_status read_tail(ud_journal *journal, uint64_t *sequence, uint8_t anchor[32]) {
    char *path = NULL;
    UD_TRY(ud_join_path(journal->ctx, journal->directory, "journal.tail", &path));
    ud_bytes bytes = {.ctx = journal->ctx, .limit = 80};
    ud_status status = ud_path_no_symlinks(journal->ctx, path);
    if (!status)
        status = ud_read_file(journal->ctx, path, &bytes);
    ud_free(path);
    uint8_t hash[32];
    if (!status && bytes.size != 80)
        status = UD_CORRUPT;
    if (!status) {
        ud_hash(bytes.data, 48, hash);
        if (memcmp(hash, bytes.data + 48, 32))
            status = UD_CORRUPT;
    }
    uint64_t version = 0;
    if (!status)
        status = ud_get_u64(&bytes, &version);
    if (!status && version != UD_CHECKPOINT_VERSION)
        status = UD_INCOMPATIBLE;
    if (!status)
        status = ud_get_u64(&bytes, sequence);
    if (!status)
        status = ud_bytes_get(&bytes, anchor, 32);
    ud_bytes_clear(&bytes);
    return status == UD_IO ? UD_CORRUPT : status;
}
static ud_status compact(ud_journal *journal) {
    ud_bytes snapshot = {.ctx = journal->ctx};
    ud_status status = ud_put_u64(&snapshot, UD_CHECKPOINT_VERSION);
    if (!status)
        status = ud_put_u64(&snapshot, journal->sequence);
    if (!status)
        status = ud_bytes_put(&snapshot, journal->previous, 32);
    if (!status)
        status = ud_put_u64(&snapshot, journal->count);
    for (journal_entry *entry = journal->entries; !status && entry; entry = entry->next) {
        char *payload = NULL;
        uint32_t length = 0;
        status = ud_json_serialize(entry->payload, &payload, &length);
        if (!status) {
            ud_journal_record record = {entry->kind, entry->key, entry->owner, {payload, length}};
            status = encode_record(&snapshot, &record);
        }
        ud_free(payload);
    }
    uint8_t digest[32];
    if (!status) {
        ud_hash(snapshot.data, snapshot.size, digest);
        status = ud_bytes_put(&snapshot, digest, 32);
    }
    char *path = NULL, *head_path = NULL;
    if (!status)
        status = named_path(journal, "snapshot", journal->sequence, "uds", &path);
    if (!status)
        status = publish_bytes(journal, path, &snapshot, 0);
    ud_bytes_clear(&snapshot);
    ud_bytes head = {.ctx = journal->ctx};
    if (!status)
        status = ud_put_u64(&head, UD_CHECKPOINT_VERSION);
    if (!status)
        status = ud_put_u64(&head, journal->sequence);
    if (!status)
        status = ud_bytes_put(&head, journal->previous, 32);
    if (!status)
        status = ud_bytes_put(&head, digest, 32);
    if (!status) {
        ud_hash(head.data, head.size, digest);
        status = ud_bytes_put(&head, digest, 32);
    }
    if (!status)
        status = ud_join_path(journal->ctx, journal->directory, "journal.head", &head_path);
    if (!status)
        status = publish_bytes(journal, head_path, &head, 1);
    ud_bytes_clear(&head);
    ud_free(path);
    ud_free(head_path);
    if (status)
        return status;
    /* Head is now durable. Obsolete segments cannot affect recovery. Retain the
     * immediately preceding snapshot for auditing, never as fallback. */
    uint64_t previous_snapshot = journal->snapshot_sequence;
    journal->snapshot_sequence = journal->sequence;
    journal->metrics.snapshots++;
    ud_free(journal->segment);
    journal->segment = NULL;
    journal->segment_bytes = journal->segment_frames = 0;
    char **names = NULL;
    uint32_t count = 0;
    UD_TRY(ud_list(journal->ctx, journal->directory, ".udg", &names, &count));
    for (uint32_t i = 0; !status && i < count; i++) {
        path = NULL;
        status = ud_join_path(journal->ctx, journal->directory, names[i], &path);
        if (!status)
            status = ud_remove_file(journal->ctx, path);
        ud_free(path);
        if (!status)
            journal->metrics.segment_files--;
    }
    ud_list_free(names, count);
    if (status)
        return status;
    UD_TRY(ud_list(journal->ctx, journal->directory, ".uds", &names, &count));
    for (uint32_t i = 0; !status && i < count; i++) {
        unsigned long long generation = 0;
        char extra = 0;
        if (sscanf(names[i], "snapshot-%20llu.uds%c", &generation, &extra) != 1) {
            status = UD_CORRUPT;
            break;
        }
        if (generation == journal->snapshot_sequence || generation == previous_snapshot)
            continue;
        path = NULL;
        status = ud_join_path(journal->ctx, journal->directory, names[i], &path);
        if (!status)
            status = ud_remove_file(journal->ctx, path);
        ud_free(path);
    }
    ud_list_free(names, count);
    return status;
}
ud_status ud_journal_append(ud_journal *journal, const ud_journal_record *records, uint32_t count) {
    if (!journal || !records || !count || count > 1024)
        return UD_INVALID;
    if (journal->failed)
        return UD_COMMIT_UNKNOWN;
    if (journal->sequence == UINT64_MAX)
        return UD_RESOURCE;
    journal_entry **prepared = ud_alloc(journal->ctx, (size_t)count * sizeof(*prepared));
    if (!prepared)
        return UD_RESOURCE;
    ud_status status = reserve_index(journal, count);
    ud_bytes bytes = {.ctx = journal->ctx, .limit = FRAME_LIMIT};
    for (uint32_t i = 0; !status && i < count; i++) {
        for (uint32_t k = 0; k < i; k++)
            if (records[i].kind && records[k].kind == records[i].kind &&
                equal(records[k].key, records[i].key))
                status = UD_INVALID;
        if (!status)
            status = prepare_entry(journal, records + i, prepared + i);
    }
    if (!status)
        status = ud_put_u64(&bytes, 0); /* Patched canonical frame length below. */
    if (!status)
        status = ud_put_u64(&bytes, UD_CHECKPOINT_VERSION);
    if (!status)
        status = ud_put_u64(&bytes, journal->sequence + 1);
    if (!status)
        status = ud_bytes_put(&bytes, journal->previous, 32);
    if (!status)
        status = ud_put_u64(&bytes, count);
    for (uint32_t i = 0; !status && i < count; i++)
        status = encode_record(&bytes, records + i);
    uint8_t digest[32];
    if (!status) {
        ud_hash(bytes.data + 8, bytes.size - 8, digest);
        status = ud_bytes_put(&bytes, digest, 32);
    }
    if (!status)
        status = ud_put_u64(&bytes, COMMIT_MARKER);
    if (!status) {
        uint64_t length = bytes.size - 8;
        for (unsigned i = 0; i < 8; i++)
            bytes.data[i] = (uint8_t)(length >> (i * 8));
        status = new_segment(journal);
        if (!status)
            status = ud_append_file(journal->ctx, journal->segment, bytes.data, bytes.size);
        if (!status)
            status = publish_tail(journal, journal->sequence + 1, digest);
        if (status)
            journal->failed = 1;
    }
    if (!status) {
        journal->sequence++;
        journal->metrics.frames++;
        memcpy(journal->previous, digest, 32);
        journal->segment_bytes += bytes.size;
        journal->segment_frames++;
        apply(journal, records, prepared, count);
        if (journal->segment_bytes >= SEGMENT_BYTES || journal->segment_frames >= SEGMENT_FRAMES) {
            status = compact(journal);
            if (status)
                journal->failed = 1;
        }
    }
    for (uint32_t i = 0; i < count; i++)
        destroy_entry(prepared[i]);
    ud_free(prepared);
    ud_bytes_clear(&bytes);
    return journal->failed ? UD_COMMIT_UNKNOWN : status;
}
static ud_status decode_records(ud_journal *journal, ud_bytes *bytes, uint64_t count) {
    if (count > UINT32_MAX || count > bytes->size / 32)
        return UD_CORRUPT;
    UD_TRY(reserve_index(journal, (uint32_t)count));
    for (uint32_t i = 0; i < (uint32_t)count; i++) {
        uint64_t kind = 0;
        char *key = NULL, *owner = NULL, *payload = NULL;
        ud_status status = ud_get_u64(bytes, &kind);
        if (!status)
            status = ud_get_string(bytes, &key);
        if (!status)
            status = ud_get_string(bytes, &owner);
        if (!status)
            status = ud_get_string(bytes, &payload);
        journal_entry *entry = NULL;
        ud_journal_record record = {kind,
                                    {key, key ? (uint32_t)strlen(key) : 0},
                                    {owner, owner ? (uint32_t)strlen(owner) : 0},
                                    {payload, payload ? (uint32_t)strlen(payload) : 0}};
        if (!status)
            status = prepare_entry(journal, &record, &entry);
        if (!status)
            apply(journal, &record, &entry, 1);
        destroy_entry(entry);
        ud_free(key);
        ud_free(owner);
        ud_free(payload);
        if (status)
            return status == UD_RESOURCE ? status : UD_CORRUPT;
    }
    return UD_OK;
}
static ud_status read_head(ud_journal *journal, int *present) {
    char *path = NULL;
    UD_TRY(ud_join_path(journal->ctx, journal->directory, "journal.head", &path));
    int kind = 0;
    ud_status status = ud_path_no_symlinks(journal->ctx, path);
    if (!status)
        status = ud_path_kind(journal->ctx, path, &kind);
    ud_bytes bytes = {.ctx = journal->ctx, .limit = 112};
    if (!status && kind) {
        *present = 1;
        status = kind == 1 ? ud_read_file(journal->ctx, path, &bytes) : UD_CORRUPT;
    }
    ud_free(path);
    if (status || !kind) {
        ud_bytes_clear(&bytes);
        return status;
    }
    uint8_t digest[32], snapshot_hash[32];
    if (bytes.size != 112)
        status = UD_CORRUPT;
    if (!status) {
        ud_hash(bytes.data, 80, digest);
        if (memcmp(digest, bytes.data + 80, 32))
            status = UD_CORRUPT;
    }
    uint64_t version = 0;
    if (!status)
        status = ud_get_u64(&bytes, &version);
    if (!status && version != UD_CHECKPOINT_VERSION)
        status = UD_INCOMPATIBLE;
    if (!status)
        status = ud_get_u64(&bytes, &journal->sequence);
    if (!status)
        status = ud_bytes_get(&bytes, journal->previous, 32);
    if (!status)
        status = ud_bytes_get(&bytes, snapshot_hash, 32);
    ud_bytes_clear(&bytes);
    if (status)
        return status;
    journal->snapshot_sequence = journal->sequence;
    path = NULL;
    bytes.limit = 0;
    UD_TRY(named_path(journal, "snapshot", journal->sequence, "uds", &path));
    status = ud_path_no_symlinks(journal->ctx, path);
    if (!status)
        status = ud_read_file(journal->ctx, path, &bytes);
    ud_free(path);
    uint64_t sequence = 0, count = 0;
    uint8_t anchor[32];
    if (!status && bytes.size < 88)
        status = UD_CORRUPT;
    if (!status) {
        ud_hash(bytes.data, bytes.size - 32, digest);
        if (memcmp(digest, snapshot_hash, 32) || memcmp(digest, bytes.data + bytes.size - 32, 32))
            status = UD_CORRUPT;
        bytes.size -= 32;
    }
    if (!status)
        status = ud_get_u64(&bytes, &version);
    if (!status)
        status = ud_get_u64(&bytes, &sequence);
    if (!status)
        status = ud_bytes_get(&bytes, anchor, 32);
    if (!status && (version != UD_CHECKPOINT_VERSION || sequence != journal->sequence ||
                    memcmp(anchor, journal->previous, 32)))
        status = UD_CORRUPT;
    if (!status)
        status = ud_get_u64(&bytes, &count);
    if (!status)
        status = decode_records(journal, &bytes, count);
    if (!status && bytes.pos != bytes.size)
        status = UD_CORRUPT;
    ud_bytes_clear(&bytes);
    return status;
}
static ud_status replay_segment(ud_journal *journal, const char *name, uint64_t first) {
    if (first != journal->sequence + 1)
        return UD_CORRUPT;
    char *path = NULL;
    UD_TRY(ud_join_path(journal->ctx, journal->directory, name, &path));
    ud_status status = ud_path_no_symlinks(journal->ctx, path);
    ud_bytes bytes = {.ctx = journal->ctx, .limit = SEGMENT_BYTES + FRAME_LIMIT};
    if (!status)
        status = ud_read_file(journal->ctx, path, &bytes);
    if (!status && !bytes.size)
        status = UD_CORRUPT;
    uint64_t frames = 0;
    while (!status && bytes.pos < bytes.size) {
        uint64_t length = 0;
        status = ud_get_u64(&bytes, &length);
        if (!status && (length < 96 || length > FRAME_LIMIT || length > bytes.size - bytes.pos))
            status = UD_CORRUPT;
        if (status)
            break;
        ud_bytes frame = {
            .ctx = journal->ctx, .data = bytes.data + bytes.pos, .size = (size_t)length - 40};
        uint8_t digest[32], previous[32];
        ud_hash(frame.data, frame.size, digest);
        if (memcmp(digest, frame.data + frame.size, 32)) {
            status = UD_CORRUPT;
            break;
        }
        ud_bytes marker = {.data = frame.data + frame.size + 32, .size = 8};
        uint64_t committed = 0, version = 0, sequence = 0, count = 0;
        status = ud_get_u64(&marker, &committed);
        if (!status && committed != COMMIT_MARKER)
            status = UD_CORRUPT;
        if (!status)
            status = ud_get_u64(&frame, &version);
        if (!status && version != UD_CHECKPOINT_VERSION)
            status = UD_INCOMPATIBLE;
        if (!status)
            status = ud_get_u64(&frame, &sequence);
        if (!status)
            status = ud_bytes_get(&frame, previous, 32);
        if (!status &&
            (sequence != journal->sequence + 1 || memcmp(previous, journal->previous, 32)))
            status = UD_CORRUPT;
        if (!status)
            status = ud_get_u64(&frame, &count);
        if (!status)
            status = count && count <= 1024 ? decode_records(journal, &frame, count) : UD_CORRUPT;
        if (!status && frame.pos != frame.size)
            status = UD_CORRUPT;
        if (!status) {
            if (sequence == journal->acknowledged) {
                if (memcmp(digest, journal->acknowledged_hash, 32)) {
                    status = UD_CORRUPT;
                    break;
                }
                journal->acknowledged_seen = 1;
            }
            journal->sequence = sequence;
            memcpy(journal->previous, digest, 32);
            journal->metrics.frames++;
            frames++;
            bytes.pos += (size_t)length;
        }
    }
    if (!status) {
        ud_free(journal->segment);
        journal->segment = path;
        path = NULL;
        journal->segment_start = first;
        journal->segment_bytes = bytes.size;
        journal->segment_frames = frames;
    }
    ud_free(path);
    ud_bytes_clear(&bytes);
    return status;
}
ud_status ud_journal_open(ud_context *ctx, ud_string directory, int resume, ud_journal **out) {
    if (!ctx || !out || !directory.data || !directory.length ||
        memchr(directory.data, 0, directory.length))
        return UD_INVALID;
    *out = NULL;
    ud_journal *journal = ud_alloc(ctx, sizeof(*journal));
    if (!journal)
        return UD_RESOURCE;
    journal->ctx = ctx;
    journal->directory = ud_strdup_n(ctx, directory.data, directory.length);
    ud_status status = journal->directory ? ud_mkdir(ctx, journal->directory) : UD_RESOURCE;
    if (!status)
        status = ud_path_no_symlinks(ctx, journal->directory);
    char *path = NULL;
    if (!status)
        status = ud_join_path(ctx, journal->directory, "writer.lock", &path);
    if (!status)
        status = ud_lock(ctx, path, &journal->writer);
    ud_free(path);
    char **names = NULL;
    uint32_t count = 0;
    if (!status)
        status = ud_list(ctx, journal->directory, ".udj", &names, &count);
    if (!status && count)
        status = UD_INCOMPATIBLE;
    ud_list_free(names, count);
    names = NULL;
    count = 0;
    int head = 0;
    if (!status)
        status = read_head(journal, &head);
    if (!status && resume) {
        status = read_tail(journal, &journal->acknowledged, journal->acknowledged_hash);
        if (!status && journal->acknowledged < journal->sequence)
            status = UD_CORRUPT;
        if (!status && journal->acknowledged == journal->sequence) {
            if (memcmp(journal->acknowledged_hash, journal->previous, 32))
                status = UD_CORRUPT;
            else
                journal->acknowledged_seen = 1;
        }
    }
    if (!status)
        status = ud_list(ctx, journal->directory, ".udg", &names, &count);
    if (!status && ((!resume && (head || count)) || (resume && !head && !count)))
        status = resume ? UD_CORRUPT : UD_BUSY;
    journal->metrics.segment_files = count;
    for (uint32_t i = 0; !status && i < count; i++) {
        unsigned long long first = 0;
        char extra = 0, expected[80];
        if (sscanf(names[i], "segment-%20llu.udg%c", &first, &extra) != 1 || !first) {
            status = UD_CORRUPT;
            break;
        }
        snprintf(expected, sizeof(expected), "segment-%020llu.udg", first);
        if (strcmp(names[i], expected)) {
            status = UD_CORRUPT;
            break;
        }
        if (head && first <= journal->snapshot_sequence)
            continue; /* Previously committed reclamation interrupted. */
        status = replay_segment(journal, names[i], first);
    }
    ud_list_free(names, count);
    if (!status && resume && !journal->acknowledged_seen)
        status = UD_CORRUPT;
    if (status)
        ud_journal_close(journal);
    else
        *out = journal;
    return status;
}
ud_status ud_journal_visit(ud_journal *journal,
                           ud_status (*visitor)(void *, uint64_t, ud_string, const ud_json *),
                           void *user) {
    if (!journal || !visitor)
        return UD_INVALID;
    for (journal_entry *entry = journal->entries; entry; entry = entry->next)
        UD_TRY(visitor(user, entry->kind, entry->key, entry->payload));
    return UD_OK;
}
void ud_journal_get_metrics(const ud_journal *journal, ud_journal_metrics *out) {
    if (!journal || !out)
        return;
    *out = journal->metrics;
    out->entries = journal->count;
}
void ud_journal_close(ud_journal *journal) {
    if (!journal)
        return;
    journal_entry *entry = journal->entries;
    while (entry) {
        journal_entry *next = entry->next;
        destroy_entry(entry);
        entry = next;
    }
    ud_unlock(journal->writer);
    ud_free(journal->directory);
    ud_free(journal->segment);
    ud_free(journal->index);
    ud_free(journal->owners);
    ud_free(journal);
}

ud_status ud_journal_forget(ud_journal *j, uint64_t kind, ud_string key) {
    if (!j || !kind || kind == UINT64_MAX)
        return UD_INVALID;
    if (!ud_journal_find(j, kind, key))
        return UD_OK;
    char payload[24];
    int n = snprintf(payload, sizeof(payload), "%llu", (unsigned long long)kind);
    ud_journal_record record = {UINT64_MAX, key, {0}, {payload, (uint32_t)n}};
    return ud_journal_append(j, &record, 1);
}
static int blob_compare(const void *a, const void *b) {
    return memcmp(a, b, 64);
}
ud_status ud_journal_collect_blobs(ud_journal *j) {
    if (!j)
        return UD_INVALID;
    if (j->failed)
        return UD_COMMIT_UNKNOWN;
    char(*live)[65] = ud_alloc(j->ctx, (size_t)(j->count ? j->count : 1) * 65);
    if (!live)
        return UD_RESOURCE;
    uint32_t used = 0;
    ud_status status = UD_OK;
    for (journal_entry *e = j->entries; !status && e; e = e->next) {
        const ud_json *blob = ud_json_find(e->payload, "blob", 4);
        if (!blob)
            continue;
        const char *text;
        uint32_t n;
        status = ud_json_text(blob, &text, &n);
        if (!status && (ud_json_kind(blob) != UD_JSON_STRING || n != 64))
            status = UD_CORRUPT;
        if (!status) {
            memcpy(live[used], text, 64);
            live[used++][64] = 0;
        }
    }
    if (!status)
        qsort(live, used, 65, blob_compare);
    char **names = NULL;
    uint32_t count = 0;
    if (!status)
        status = ud_list(j->ctx, j->directory, ".udb", &names, &count);
    for (uint32_t i = 0; !status && i < count; i++) {
        if (strlen(names[i]) != 73 || memcmp(names[i], "blob-", 5)) {
            status = UD_CORRUPT;
            break;
        }
        char digest[65];
        memcpy(digest, names[i] + 5, 64);
        digest[64] = 0;
        if (bsearch(digest, live, used, 65, blob_compare))
            continue;
        char *path = NULL;
        status = ud_join_path(j->ctx, j->directory, names[i], &path);
        if (!status)
            status = ud_remove_file(j->ctx, path);
        ud_free(path);
    }
    ud_list_free(names, count);
    ud_free(live);
    return status;
}
