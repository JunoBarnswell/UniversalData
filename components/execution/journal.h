#ifndef UD_JOURNAL_INTERNAL_H
#define UD_JOURNAL_INTERNAL_H
#include "ud/json.h"
typedef struct ud_journal ud_journal;
typedef struct ud_bytes ud_bytes;
typedef struct ud_journal_record {
    uint64_t kind; /* Zero: retire all ephemeral records belonging to owner. */
    ud_string key, owner, payload;
} ud_journal_record;
typedef struct ud_journal_metrics {
    uint64_t frames, lookups, probes, snapshots;
    uint32_t entries, segment_files;
} ud_journal_metrics;
/* Single writer. Package Runtime serializes all accesses using its journal lock.
 * Durability errors poison append; no invalid tail is ever silently discarded. */
ud_status ud_journal_open(ud_context *, ud_string directory, int resume, ud_journal **);
const ud_json *ud_journal_find(ud_journal *, uint64_t kind, ud_string key);
ud_status ud_journal_append(ud_journal *, const ud_journal_record *, uint32_t count);
/* Immutable, bounded binary payloads owned by the same fenced journal. Records
 * store their digest and length; compaction never copies their bodies. */
ud_status ud_journal_write_blob(ud_journal *, const void *, uint64_t, char digest[65]);
ud_status ud_journal_read_blob(ud_journal *, ud_string digest, uint64_t, ud_bytes *);
ud_status ud_journal_forget(ud_journal *, uint64_t kind, ud_string key);
ud_status ud_journal_collect_blobs(ud_journal *);
ud_status ud_journal_visit(ud_journal *,
                           ud_status (*)(void *, uint64_t, ud_string, const ud_json *), void *);
/* Pending transaction fact envelopes use {order,records:[kind,key,owner,payload]}.
 * Publish preserves their recorded order; discard never publishes their effects. */
ud_status ud_journal_publish_owner(ud_journal *, uint64_t pending_kind, ud_string owner);
ud_status ud_journal_retire_owner(ud_journal *, ud_string owner);
void ud_journal_get_metrics(const ud_journal *, ud_journal_metrics *);
void ud_journal_close(ud_journal *);
#endif
