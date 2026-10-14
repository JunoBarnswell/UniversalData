#include "internal.h"
#include "../../components/execution/journal.h"
#include <errno.h>
#include <sys/stat.h>
enum {
    RECORD_HEADER = 1,
    RECORD_TASK_BEGIN,
    RECORD_TASK_DONE,
    RECORD_SOURCE,
    RECORD_RESERVED_5,
    RECORD_FILE_DONE,
    RECORD_BATCH_INTENT,
    RECORD_BATCH_DONE,
    RECORD_ENUMERATOR,
    RECORD_SNAPSHOT_BATCH,
    RECORD_SNAPSHOT_DONE,
    RECORD_CURSOR,
    RECORD_SCHEMA,
    RECORD_EPOCH,
    RECORD_TRANSACTION_MANIFEST,
    RECORD_TRANSACTION_DECISION,
    RECORD_TRANSACTION_COMPLETE,
    RECORD_TRANSACTION_FACT,
    RECORD_SCHEMA_EVENT,
    RECORD_FILE_BASE,
    RECORD_FILE_PREPARED,
    RECORD_PACKAGE_DONE
};
typedef struct checkpoint_journal {
    ud_execution *execution;
    ud_journal *store;
    char *directory;
} checkpoint_journal;
static const ud_json *find(checkpoint_journal *j, uint64_t kind, ud_string key) {
    return ud_journal_find(j->store, kind, key);
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
static ud_status append(checkpoint_journal *j, ud_activation *e, uint64_t kind, ud_string key,
                        ud_string payload) {
    ud_string owner = kind == RECORD_TASK_BEGIN || kind == RECORD_BATCH_INTENT ||
                              kind == RECORD_BATCH_DONE || kind == RECORD_SNAPSHOT_BATCH ||
                              kind == RECORD_SNAPSHOT_DONE
                          ? e->task_id
                          : (ud_string){0};
    ud_journal_record records[2] = {{kind, key, owner, payload}, {0, {0}, e->task_id, {0}}};
    int tentative = kind == RECORD_TASK_BEGIN || kind == RECORD_TASK_DONE ||
                    kind == RECORD_BATCH_DONE || kind == RECORD_ENUMERATOR ||
                    kind == RECORD_CURSOR || kind == RECORD_EPOCH || kind == RECORD_PACKAGE_DONE;
    ud_string transaction = {0};
    uint64_t order = 0;
    ud_status positioned =
        tentative ? ud_transaction_fact_position(e, &transaction, &order) : UD_END;
    if (positioned != UD_END && positioned)
        return positioned;
    if (!positioned) {
        ud_bytes envelope = {.ctx = e->execution->ctx};
        char head[80];
        int length = snprintf(head, sizeof(head), "{\"order\":%llu,\"records\":[",
                              (unsigned long long)order);
        ud_status status = ud_bytes_put(&envelope, head, (size_t)length);
        uint32_t count = kind == RECORD_TASK_DONE ? 2 : 1;
        for (uint32_t index = 0; !status && index < count; index++) {
            if (index)
                status = ud_bytes_put(&envelope, ",", 1);
            length = snprintf(head, sizeof(head),
                              "{\"kind\":%llu,\"key\":", (unsigned long long)records[index].kind);
            if (!status)
                status = ud_bytes_put(&envelope, head, (size_t)length);
            if (!status)
                status =
                    ud_json_quote(&envelope, records[index].key.data, records[index].key.length);
            if (!status)
                status = ud_bytes_put(&envelope, ",\"owner\":", 9);
            if (!status)
                status = ud_json_quote(&envelope, records[index].owner.data,
                                       records[index].owner.length);
            if (!status)
                status = ud_bytes_put(&envelope, ",\"payload\":", 11);
            if (!status)
                status = records[index].kind ? ud_bytes_put(&envelope, records[index].payload.data,
                                                            records[index].payload.length)
                                             : ud_bytes_put(&envelope, "null", 4);
            if (!status)
                status = ud_bytes_put(&envelope, "}", 1);
        }
        if (!status)
            status = ud_bytes_put(&envelope, "]}", 2);
        char identity[96];
        length = snprintf(identity, sizeof(identity), "%.*s/%020llu", (int)transaction.length,
                          transaction.data, (unsigned long long)order);
        if (!status && envelope.size > UINT32_MAX)
            status = UD_RESOURCE;
        ud_journal_record pending = {
            .kind = RECORD_TRANSACTION_FACT,
            .key = {identity, (uint32_t)length},
            .owner = transaction,
            .payload = {(const char *)envelope.data, (uint32_t)envelope.size}};
        if (!status)
            status = ud_journal_append(j->store, &pending, 1);
        ud_bytes_clear(&envelope);
        return status;
    }
    return ud_journal_append(j->store, records, kind == RECORD_TASK_DONE ? 2 : 1);
}
static uint64_t transaction_kind(int32_t phase) {
    return phase == UD_TX_JOURNAL_MANIFEST   ? RECORD_TRANSACTION_MANIFEST
           : phase == UD_TX_JOURNAL_DECISION ? RECORD_TRANSACTION_DECISION
           : phase == UD_TX_JOURNAL_COMPLETE ? RECORD_TRANSACTION_COMPLETE
                                             : 0;
}
ud_status ud_checkpoint_transaction_read(ud_activation *activation, int32_t phase,
                                         ud_string identity, char **out, uint32_t *length) {
    if (!activation || !out || !length || !transaction_kind(phase))
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    checkpoint_journal *journal = activation->execution->journal;
    if (!journal)
        return UD_END;
    UD_TRY(grant(activation->execution, journal->directory, UD_ACCESS_CHECKPOINT_READ));
    ud_mutex_lock(&activation->execution->journal_lock);
    const ud_json *value = find(journal, transaction_kind(phase), identity);
    ud_status status = value ? ud_json_serialize(value, out, length) : UD_END;
    ud_mutex_unlock(&activation->execution->journal_lock);
    return status;
}
ud_status ud_checkpoint_transaction_record(ud_activation *activation, int32_t phase,
                                           ud_string identity, ud_string payload) {
    if (!activation || !transaction_kind(phase))
        return UD_INVALID;
    checkpoint_journal *journal = activation->execution->journal;
    if (!journal)
        return UD_UNSUPPORTED;
    UD_TRY(grant(activation->execution, journal->directory, UD_ACCESS_CHECKPOINT_WRITE));
    ud_journal_record record = {
        .kind = transaction_kind(phase), .key = identity, .payload = payload};
    ud_mutex_lock(&activation->execution->journal_lock);
    ud_status status = ud_journal_append(journal->store, &record, 1);
    ud_mutex_unlock(&activation->execution->journal_lock);
    return status;
}
ud_status ud_checkpoint_transaction_facts(ud_activation *activation, ud_string identity,
                                          int commit) {
    if (!activation)
        return UD_INVALID;
    checkpoint_journal *journal = activation->execution->journal;
    if (!journal)
        return UD_UNSUPPORTED;
    UD_TRY(grant(activation->execution, journal->directory, UD_ACCESS_CHECKPOINT_WRITE));
    ud_mutex_lock(&activation->execution->journal_lock);
    ud_status status =
        commit ? ud_journal_publish_owner(journal->store, RECORD_TRANSACTION_FACT, identity)
               : ud_journal_retire_owner(journal->store, identity);
    ud_mutex_unlock(&activation->execution->journal_lock);
    return status;
}
static ud_status snapshot(ud_activation *e, ud_bytes *bytes) {
    UD_TRY(ud_bytes_put(bytes, "{\"scopes\":[", 11));
    uint32_t depth = 0;
    for (ud_scope *scope = e->scope; scope && (!e->parallel || scope->owner == e);
         scope = scope->parent, depth++) {
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
    /* The success snapshot is tentative until the coordinator publishes it.
     * Account for the owner's pending effects without mutating live counters
     * before commit. Children never include the enclosing owner's effects. */
    uint64_t pending = 0, pending_errors = 0;
    ud_transaction_pending(e, &pending, &pending_errors);
    uint64_t committed = atomic_load(&e->committed_rows),
             acknowledged = atomic_load(&e->acknowledged_rows),
             committed_errors = atomic_load(&e->committed_error_rows),
             acknowledged_errors = atomic_load(&e->acknowledged_error_rows);
    if (pending > UINT64_MAX - committed || pending > UINT64_MAX - acknowledged ||
        pending_errors > UINT64_MAX - committed_errors ||
        pending_errors > UINT64_MAX - acknowledged_errors)
        return UD_OVERFLOW;
    char metrics[512];
    int n =
        snprintf(metrics, sizeof(metrics),
                 "],\"invocation\":%llu,\"input\":%llu,\"output\":%llu,\"rejected\":%llu,"
                 "\"committed\":%llu,\"acknowledged\":%llu,\"errorRows\":%llu,\"committedErrors\":%"
                 "llu,\"acknowledgedErrors\":%llu}",
                 (unsigned long long)e->invocation, (unsigned long long)atomic_load(&e->input_rows),
                 (unsigned long long)atomic_load(&e->output_rows),
                 (unsigned long long)atomic_load(&e->rejected_rows),
                 (unsigned long long)(committed + pending),
                 (unsigned long long)(acknowledged + pending),
                 (unsigned long long)atomic_load(&e->error_rows),
                 (unsigned long long)(committed_errors + pending_errors),
                 (unsigned long long)(acknowledged_errors + pending_errors));
    UD_TRY(ud_bytes_put(bytes, metrics, (size_t)n - 1));
    UD_TRY(ud_bytes_put(bytes, ",\"rules\":", 9));
    UD_TRY(ud_rule_facts_json(e->rule_facts, bytes));
    return ud_bytes_put(bytes, "}", 1);
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
            if (status)
                return UD_CORRUPT;
            status = ud_batch_field(schema, 0, &restored);
            int same = !status && ud_field_matches(&expected, &restored, UD_FIELD_PORT);
            ud_batch_destroy(schema);
            if (!same)
                return UD_CORRUPT;
            ud_value decoded = {0};
            status = ud_json_value_decode(e->execution->ctx, &expected, ud_member(record, "value"),
                                          UD_ROUND_EXACT, &decoded);
            if (!status)
                status = ud_variable_assign(e->execution->ctx, v, &decoded, &expected);
            ud_clear_value(&decoded);
            if (status)
                return status;
        }
        scope = scope->parent;
    }
    if (scope && (!e->parallel || scope->owner == e))
        return UD_CORRUPT;
    uint64_t value;
    UD_TRY(ud_json_uint(ud_member(payload, "invocation"), &value));
    e->invocation = value;
    return UD_OK;
}
static ud_status file_record(checkpoint_journal *j, ud_activation *a, uint64_t kind,
                             const char *path) {
    uint8_t digest[32];
    UD_TRY(ud_file_hash(j->execution->ctx, path, digest));
    char encoded[65], payload[68];
    hex(digest, encoded);
    ud_string key = {path, (uint32_t)strlen(path)};
    const ud_json *entry = find(j, kind, key);
    if (entry) {
        ud_string expected;
        UD_TRY(ud_json_string(entry, &expected));
        return ud_named(expected, encoded) ? UD_OK : UD_CORRUPT;
    }
    snprintf(payload, sizeof(payload), "\"%s\"", encoded);
    return append(j, a, kind, key, (ud_string){payload, 66});
}
static ud_status restore_fact(void *user, uint64_t kind, ud_string key, const ud_json *payload) {
    checkpoint_journal *j = user;
    ud_execution *e = j->execution;
    if (kind < RECORD_HEADER || kind > RECORD_PACKAGE_DONE)
        return UD_CORRUPT;
    if (kind == RECORD_SOURCE || kind == RECORD_FILE_DONE) {
        char *path = ud_strdup_n(e->ctx, key.data, key.length);
        if (!path)
            return UD_RESOURCE;
        ud_status s = grant(e, path, UD_ACCESS_READ);
        if (!s)
            s = file_record(j, &e->root, kind, path);
        ud_free(path);
        return s;
    }
    if (kind == RECORD_TASK_BEGIN)
        atomic_fetch_add(&e->recovery_tasks, 1);
    if (kind == RECORD_TASK_DONE || kind == RECORD_PACKAGE_DONE) {
        uint64_t input = 0, output = 0, rejected = 0;
        UD_TRY(ud_json_uint(ud_member(payload, "input"), &input));
        UD_TRY(ud_json_uint(ud_member(payload, "output"), &output));
        UD_TRY(ud_json_uint(ud_member(payload, "rejected"), &rejected));
        atomic_fetch_add(&e->input_rows, input);
        atomic_fetch_add(&e->output_rows, output);
        atomic_fetch_add(&e->rejected_rows, rejected);
        uint64_t committed = 0, acknowledged = 0;
        UD_TRY(ud_json_uint(ud_member(payload, "committed"), &committed));
        UD_TRY(ud_json_uint(ud_member(payload, "acknowledged"), &acknowledged));
        atomic_fetch_add(&e->committed_rows, committed);
        atomic_fetch_add(&e->acknowledged_rows, acknowledged);
        uint64_t errors = 0, committed_errors = 0, acknowledged_errors = 0;
        UD_TRY(ud_json_uint(ud_member(payload, "errorRows"), &errors));
        UD_TRY(ud_json_uint(ud_member(payload, "committedErrors"), &committed_errors));
        UD_TRY(ud_json_uint(ud_member(payload, "acknowledgedErrors"), &acknowledged_errors));
        atomic_fetch_add(&e->error_rows, errors);
        atomic_fetch_add(&e->committed_error_rows, committed_errors);
        atomic_fetch_add(&e->acknowledged_error_rows, acknowledged_errors);
        UD_TRY(ud_rule_facts_restore(e, ud_member(payload, "rules"), 0));
        if (kind == RECORD_TASK_DONE) {
            atomic_fetch_add(&e->executed_tasks, 1);
            atomic_fetch_add(&e->durable_tasks, 1);
        }
    }
    return UD_OK;
}
ud_status ud_checkpoint_open(ud_execution *e) {
    if (!e->options.state_directory.length)
        return e->options.resume ? UD_INVALID : UD_OK;
    ud_string directory = e->options.state_directory;
    if (!e->options.execution_id.length || !directory.data ||
        memchr(directory.data, 0, directory.length))
        return UD_INVALID;
#ifdef _WIN32
    if (directory.length < 3 || directory.data[1] != ':')
        return UD_INVALID;
#else
    if (directory.data[0] != '/')
        return UD_INVALID;
#endif
    checkpoint_journal *j = ud_alloc(e->ctx, sizeof(*j));
    if (!j)
        return UD_RESOURCE;
    e->journal = j;
    j->execution = e;
    j->directory = ud_strdup_n(e->ctx, directory.data, directory.length);
    if (!j->directory)
        return UD_RESOURCE;
    UD_TRY(grant(e, j->directory, UD_ACCESS_CHECKPOINT_OPEN));
    UD_TRY(grant(e, j->directory, UD_ACCESS_CHECKPOINT_WRITE));
    if (e->options.resume)
        UD_TRY(grant(e, j->directory, UD_ACCESS_CHECKPOINT_READ));
    UD_TRY(ud_journal_open(e->ctx, directory, e->options.resume, &j->store));
    ud_bytes identity = {.ctx = e->ctx};
    ud_status s = ud_bytes_put(&identity, e->package->fingerprint, 32);
    char *parameters = NULL;
    uint32_t length = 0;
    if (!s)
        s = ud_json_serialize(e->parameters, &parameters, &length);
    if (!s)
        s = ud_put_string(&identity, parameters, length);
    ud_free(parameters);
    if (!s)
        s = ud_put_string(&identity, e->options.tenant.data, e->options.tenant.length);
    if (!s)
        s = ud_put_string(&identity, e->options.execution_id.data, e->options.execution_id.length);
    if (!s)
        s = ud_put_u64(&identity, e->options.max_batch_rows);
    if (!s)
        s = ud_put_u64(&identity, e->options.edge_capacity);
    if (!s)
        s = ud_put_u64(&identity, e->options.max_concurrency);
    uint8_t digest[32];
    char encoded[65], payload[160]; int payload_length = 0;
    if (!s) {
        ud_hash(identity.data, identity.size, digest);
        hex(digest, encoded);
        payload_length = snprintf(payload,sizeof(payload),"{\"version\":%u,\"revision\":%u,\"identity\":\"%s\"}",
                                  UD_CHECKPOINT_VERSION,UD_CHECKPOINT_FORMAT_REVISION,encoded);
        if (payload_length < 0 || (size_t)payload_length >= sizeof(payload)) s = UD_OVERFLOW;
    }
    ud_bytes_clear(&identity);
    if (s)
        return s;
    if (e->options.resume) {
        const ud_json *header = find(j, RECORD_HEADER, e->options.execution_id);
        ud_string hash; uint64_t version = 0,revision = 0;
        if (!header || ud_json_kind(header) != UD_JSON_OBJECT ||
            ud_json_uint(ud_member(header,"version"),&version) || version != UD_CHECKPOINT_VERSION ||
            ud_json_uint(ud_member(header,"revision"),&revision) || revision != UD_CHECKPOINT_FORMAT_REVISION ||
            ud_json_string(ud_member(header,"identity"), &hash) || !ud_named(hash, encoded))
            return UD_INCOMPATIBLE;
        return ud_journal_visit(j->store, restore_fact, j);
    }
    return append(j, &e->root, RECORD_HEADER, e->options.execution_id, (ud_string){payload,(uint32_t)payload_length});
}
static ud_status ud_checkpoint_task_begin_record(ud_activation *e, ud_string kind, int *completed) {
    *completed = 0;
    checkpoint_journal *j = e->execution->journal;
    if (!j)
        return UD_OK;
    const ud_json *done = find(j, RECORD_TASK_DONE, e->task_id);
    if (done) {
        /* Completed tasks never repeat external reads. Execution cache assets
         * restore their frozen Native state lazily from this same journal. */
        UD_TRY(restore(e, done));
        e->task_facts |= UD_TASK_EXECUTED | UD_TASK_DURABLE;
        *completed = 1;
        return UD_OK;
    }
    if (find(j, RECORD_TASK_BEGIN, e->task_id)) {
        e->task_facts |= UD_TASK_UNCERTAIN; /* Already counted during recovery. */
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
    return e->execution->journal ? file_record(e->execution->journal, e, RECORD_SOURCE, path)
                                 : UD_OK;
}
static ud_status ud_checkpoint_file_done_record(ud_activation *e, const char *path) {
    return e->execution->journal ? file_record(e->execution->journal, e, RECORD_FILE_DONE, path)
                                 : UD_OK;
}
static ud_status effect_parse(checkpoint_journal *journal,const ud_json *entry,const uint8_t digest[32],ud_command_result *out) {
    ud_string recorded = {0},provider = {0}; char expected[65]; hex(digest,expected);
    uint64_t state = 0,result = 0,rows = 0;
    static const char *keys[] = {"digest","state","result","rows","providerCode"};
    UD_TRY(ud_json_keys(entry,keys,5));
    UD_TRY(ud_json_string(ud_member(entry,"digest"),&recorded));
    if (!ud_named(recorded,expected)) return UD_CORRUPT;
    UD_TRY(ud_json_uint(ud_member(entry,"state"),&state));
    UD_TRY(ud_json_uint(ud_member(entry,"result"),&result));
    UD_TRY(ud_json_uint(ud_member(entry,"rows"),&rows));
    if (ud_json_kind(ud_member(entry,"providerCode")) != UD_JSON_STRING) return UD_CORRUPT;
    UD_TRY(ud_json_text(ud_member(entry,"providerCode"),&provider.data,&provider.length));
    if (provider.length > 128 || (provider.length && memchr(provider.data,0,provider.length))) return UD_CORRUPT;
    if (state < UD_COMMAND_PENDING || state > UD_COMMAND_REJECTED || result > UD_TRUNCATED || result == UD_END || result == UD_MORE ||
        (state == UD_COMMAND_REJECTED && !result) ||
        ((state == UD_COMMAND_APPLIED || state == UD_COMMAND_COMMITTED || state == UD_COMMAND_PENDING) && result)) return UD_CORRUPT;
    out->state = (int32_t)state; out->result = (ud_status)result; out->affected_rows = rows;
    if (provider.length) {
        out->provider_code = (ud_string){ud_strdup_n(journal->execution->ctx,provider.data,provider.length),provider.length};
        if (!out->provider_code.data) return UD_RESOURCE;
    }
    return UD_OK;
}
static ud_status effect_record(ud_activation *activation,ud_string identity,const uint8_t digest[32],const ud_command_result *result) {
    checkpoint_journal *journal = activation->execution->journal;
    if (!journal) return UD_OK;
    uint64_t kind = result->state == UD_COMMAND_PENDING ? RECORD_BATCH_INTENT : RECORD_BATCH_DONE;
    const ud_json *existing = find(journal,kind,identity);
    if (existing) {
        ud_command_result prior = {.struct_size = sizeof(prior),.abi_version = UD_ABI_VERSION};
        ud_status status = effect_parse(journal,existing,digest,&prior);
        if (!status && (prior.state != result->state || prior.result != result->result || prior.affected_rows != result->affected_rows ||
            !ud_same(prior.provider_code,result->provider_code))) status = UD_CORRUPT;
        ud_command_result_clear(&prior); return status;
    }
    char encoded[65],head[192]; hex(digest,encoded);
    int length = snprintf(head,sizeof(head),"{\"digest\":\"%s\",\"state\":%d,\"result\":%d,\"rows\":%llu,\"providerCode\":",
        encoded,result->state,result->result,(unsigned long long)result->affected_rows);
    ud_bytes bytes = {.ctx = activation->execution->ctx};
    ud_status status = ud_bytes_put(&bytes,head,(size_t)length);
    if (!status) status = ud_json_quote(&bytes,result->provider_code.data ? result->provider_code.data : "",result->provider_code.length);
    if (!status) status = ud_bytes_put(&bytes,"}",1);
    if (!status) status = append(journal,activation,kind,identity,(ud_string){(char *)bytes.data,(uint32_t)bytes.size});
    ud_bytes_clear(&bytes); return status;
}
void ud_checkpoint_close(ud_execution *e) {
    checkpoint_journal *j = e->journal;
    if (!j)
        return;
    ud_journal_close(j->store);
    ud_free(j->directory);
    ud_free(j);
    e->journal = NULL;
}
static ud_status checkpoint_before(ud_activation *a, const char *path) {
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_OK;
    UD_TRY(grant(a->execution, j->directory, UD_ACCESS_CHECKPOINT_WRITE));
    if (path)
        UD_TRY(grant(a->execution, path, UD_ACCESS_READ));
    ud_activation_notify_kind(a, UD_EXEC_EVENT_BEFORE_CHECKPOINT, UD_EXEC_RUNNING, UD_OK);
    return grant(a->execution, j->directory, UD_ACCESS_CHECKPOINT_WRITE);
}
static void checkpoint_after(ud_activation *a, ud_status status) {
    if (!status && a->execution->journal)
        ud_activation_notify_kind(a, UD_EXEC_EVENT_AFTER_CHECKPOINT, UD_EXEC_RUNNING, UD_OK);
}
ud_status ud_checkpoint_task_begin(ud_activation *e, ud_string kind, int *completed) {
    UD_TRY(checkpoint_before(e, NULL));
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_task_begin_record(e, kind, completed);
    ud_mutex_unlock(&e->execution->journal_lock);
    checkpoint_after(e, result);
    return result;
}

ud_status ud_checkpoint_task_done(ud_activation *e) {
    ud_status grant_status = checkpoint_before(e, NULL);
    if (grant_status)
        return e->execution->journal ? UD_COMMIT_UNKNOWN : grant_status;
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_task_done_record(e);
    ud_mutex_unlock(&e->execution->journal_lock);
    checkpoint_after(e, result);
    return result;
}
ud_status ud_checkpoint_package_resume(ud_activation *activation, int *completed) {
    *completed = 0;
    checkpoint_journal *journal = activation->execution->journal;
    if (!journal || !activation->execution->options.resume)
        return UD_OK;
    char identity[65];
    UD_TRY(ud_activation_scope_identity(activation, identity));
    UD_TRY(grant(activation->execution, journal->directory, UD_ACCESS_CHECKPOINT_READ));
    ud_mutex_lock(&activation->execution->journal_lock);
    const ud_json *done = find(journal, RECORD_PACKAGE_DONE, (ud_string){identity,64});
    ud_status status = done ? restore(activation, done) : UD_OK;
    *completed = done != NULL;
    ud_mutex_unlock(&activation->execution->journal_lock);
    return status;
}
ud_status ud_checkpoint_package_done(ud_activation *activation) {
    checkpoint_journal *journal = activation->execution->journal;
    if (!journal)
        return UD_UNSUPPORTED;
    char identity[65];
    UD_TRY(ud_activation_scope_identity(activation, identity));
    UD_TRY(checkpoint_before(activation, NULL));
    ud_mutex_lock(&activation->execution->journal_lock);
    ud_bytes payload = {.ctx = activation->execution->ctx};
    ud_status status = snapshot(activation, &payload);
    if (!status)
        status = append(journal, activation, RECORD_PACKAGE_DONE, (ud_string){identity,64},
                        (ud_string){(const char *)payload.data, (uint32_t)payload.size});
    ud_bytes_clear(&payload);
    ud_mutex_unlock(&activation->execution->journal_lock);
    checkpoint_after(activation, status);
    return status;
}

ud_status ud_checkpoint_source(ud_activation *e, const char *path) {
    UD_TRY(checkpoint_before(e, path));
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_source_record(e, path);
    ud_mutex_unlock(&e->execution->journal_lock);
    checkpoint_after(e, result);
    return result;
}

ud_status ud_checkpoint_file_read(ud_activation *activation, int32_t phase, ud_string key,
                                  char **out, uint32_t *length) {
    if (!activation || !out || !length || phase < UD_FILE_BASE || phase > UD_FILE_PREPARED)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    checkpoint_journal *journal = activation->execution->journal;
    if (!journal)
        return UD_END;
    ud_mutex_lock(&activation->execution->journal_lock);
    const ud_json *record =
        find(journal, phase == UD_FILE_BASE ? RECORD_FILE_BASE : RECORD_FILE_PREPARED, key);
    ud_status status = record ? ud_json_serialize(record, out, length) : UD_END;
    ud_mutex_unlock(&activation->execution->journal_lock);
    return status;
}
ud_status ud_checkpoint_file_record(ud_activation *activation, int32_t phase, ud_string key,
                                    ud_string value) {
    if (!activation || phase < UD_FILE_BASE || phase > UD_FILE_PREPARED)
        return UD_INVALID;
    checkpoint_journal *journal = activation->execution->journal;
    if (!journal)
        return UD_OK;
    UD_TRY(checkpoint_before(activation, NULL));
    ud_mutex_lock(&activation->execution->journal_lock);
    ud_status status =
        append(journal, activation, phase == UD_FILE_BASE ? RECORD_FILE_BASE : RECORD_FILE_PREPARED,
               key, value);
    ud_mutex_unlock(&activation->execution->journal_lock);
    checkpoint_after(activation, status);
    return status;
}
ud_status ud_checkpoint_file_done(ud_activation *e, const char *path) {
    UD_TRY(checkpoint_before(e, path));
    ud_mutex_lock(&e->execution->journal_lock);
    ud_status result = ud_checkpoint_file_done_record(e, path);
    ud_mutex_unlock(&e->execution->journal_lock);
    checkpoint_after(e, result);
    return result;
}

ud_status ud_checkpoint_effect_read(ud_activation *activation,ud_string identity,const uint8_t digest[32],ud_command_result *out) {
    if (!activation || !out || out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION || out->provider_code.data) return UD_INVALID;
    *out = (ud_command_result){.struct_size = sizeof(*out),.abi_version = UD_ABI_VERSION};
    checkpoint_journal *journal = activation->execution->journal;
    if (!journal) return UD_OK;
    ud_mutex_lock(&activation->execution->journal_lock);
    const ud_json *entry = find(journal,RECORD_BATCH_DONE,identity);
    if (!entry) entry = find(journal,RECORD_BATCH_INTENT,identity);
    ud_status status = entry ? effect_parse(journal,entry,digest,out) : UD_OK;
    ud_mutex_unlock(&activation->execution->journal_lock);
    if (status) ud_command_result_clear(out);
    return status;
}
ud_status ud_checkpoint_effect_intent(ud_activation *activation,ud_string identity,const uint8_t digest[32]) {
    UD_TRY(checkpoint_before(activation,NULL));
    ud_command_result result = {.struct_size = sizeof(result),.abi_version = UD_ABI_VERSION,.state = UD_COMMAND_PENDING};
    ud_mutex_lock(&activation->execution->journal_lock);
    ud_status status = effect_record(activation,identity,digest,&result);
    ud_mutex_unlock(&activation->execution->journal_lock);
    checkpoint_after(activation,status); return status;
}
ud_status ud_checkpoint_effect_retry(ud_activation *activation,ud_string identity,const uint8_t digest[32]) {
    if (!activation || !activation->execution->options.resume || ud_transaction_active(activation)) return UD_INVALID;
    checkpoint_journal *journal=activation->execution->journal;
    if (!journal) return UD_INVALID;
    UD_TRY(checkpoint_before(activation,NULL));
    ud_mutex_lock(&activation->execution->journal_lock);
    ud_command_result prior={.struct_size=sizeof(prior),.abi_version=UD_ABI_VERSION};
    const ud_json *entry=find(journal,RECORD_BATCH_DONE,identity);
    ud_status status=entry ? effect_parse(journal,entry,digest,&prior) : UD_CORRUPT;
    if (!status && prior.state!=UD_COMMAND_REJECTED) status=UD_INVALID;
    /* Retire only proven rollback. The original durable intent remains: a kill
     * after retirement and before a new result is conservatively UNKNOWN. */
    if (!status) status=ud_journal_forget(journal->store,RECORD_BATCH_DONE,identity);
    ud_command_result_clear(&prior);
    ud_mutex_unlock(&activation->execution->journal_lock);
    checkpoint_after(activation,status); return status;
}
ud_status ud_checkpoint_effect_complete(ud_activation *activation,ud_string identity,const uint8_t digest[32],const ud_command_result *result) {
    if (!result || result->struct_size != sizeof(*result) || result->abi_version != UD_ABI_VERSION ||
        result->state < UD_COMMAND_APPLIED || result->state > UD_COMMAND_REJECTED) return UD_INVALID;
    UD_TRY(checkpoint_before(activation,NULL));
    ud_mutex_lock(&activation->execution->journal_lock);
    ud_status status = effect_record(activation,identity,digest,result);
    ud_mutex_unlock(&activation->execution->journal_lock);
    checkpoint_after(activation,status); return status;
}
ud_status ud_checkpoint_enumerator(ud_activation *e, const uint8_t digest[32]) {
    checkpoint_journal *j = e->execution->journal;
    if (!j)
        return UD_OK;
    char hash[65], payload[68];
    hex(digest, hash);
    snprintf(payload, sizeof(payload), "\"%s\"", hash);
    UD_TRY(checkpoint_before(e, NULL));
    ud_mutex_lock(&e->execution->journal_lock);
    const ud_json *existing = find(j, RECORD_ENUMERATOR, e->task_id);
    ud_status status = UD_OK;
    if (existing) {
        ud_string expected;
        status = ud_json_string(existing, &expected);
        if (!status && !ud_named(expected, hash))
            status = UD_CORRUPT;
    } else
        status = append(j, e, RECORD_ENUMERATOR, e->task_id, (ud_string){payload, 66});
    ud_mutex_unlock(&e->execution->journal_lock);
    checkpoint_after(e, status);
    return status;
}

static ud_status snapshot_key(ud_activation *a, ud_string component, uint64_t batch, int indexed,
                              ud_bytes *out) {
    UD_TRY(ud_put_string(out, a->task_id.data, a->task_id.length));
    UD_TRY(ud_put_string(out, a->pipeline_id.data, a->pipeline_id.length));
    UD_TRY(ud_put_string(out, component.data, component.length));
    if (indexed)
        UD_TRY(ud_put_u64(out, batch));
    uint8_t digest[32];
    ud_hash(out->data, out->size, digest);
    out->size = 0;
    char key[65];
    hex(digest, key);
    return ud_bytes_put(out, key, 64);
}
ud_status ud_checkpoint_snapshot_read(ud_activation *a, ud_string component, uint64_t index,
                                      ud_batch **out) {
    if (!a || !out)
        return UD_INVALID;
    *out = NULL;
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_END;
    UD_TRY(checkpoint_before(a, NULL));
    ud_bytes key = {.ctx = a->execution->ctx};
    ud_status status = snapshot_key(a, component, index, 1, &key);
    ud_bytes wire = {.ctx = a->execution->ctx};
    ud_mutex_lock(&a->execution->journal_lock);
    const ud_json *entry =
        !status ? find(j, RECORD_SNAPSHOT_BATCH, (ud_string){(char *)key.data, (uint32_t)key.size})
                : NULL;
    if (!status && !entry)
        status = UD_END;
    ud_string digest = {0};
    uint64_t bytes = 0;
    if (!status)
        status = ud_json_string(ud_member(entry, "blob"), &digest);
    if (!status)
        status = ud_json_uint(ud_member(entry, "bytes"), &bytes);
    if (!status)
        status = ud_journal_read_blob(j->store, digest, bytes, &wire);
    ud_mutex_unlock(&a->execution->journal_lock);
    if (!status)
        status = ud_batch_deserialize(a->execution->ctx, wire.data, wire.size, out);
    if (!status)
        status = ud_batch_seal(*out);
    ud_bytes_clear(&wire);
    ud_bytes_clear(&key);
    return status;
}
ud_status ud_checkpoint_snapshot_write(ud_activation *a, ud_string component, uint64_t index,
                                       const ud_batch *batch) {
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_OK;
    UD_TRY(checkpoint_before(a, NULL));
    ud_batch *existing = NULL;
    ud_status status = ud_checkpoint_snapshot_read(a, component, index, &existing);
    if (!status) {
        uint8_t old[32], now[32];
        status = ud_batch_fingerprint(existing, old);
        if (!status)
            status = ud_batch_fingerprint(batch, now);
        if (!status && memcmp(old, now, 32))
            status = UD_INCOMPATIBLE;
        ud_batch_destroy(existing);
        return status;
    }
    if (status != UD_END)
        return status;
    uint8_t *wire = NULL;
    uint64_t bytes = 0;
    status = ud_batch_serialize(batch, &wire, &bytes);
    ud_bytes key = {.ctx = a->execution->ctx};
    if (!status)
        status = snapshot_key(a, component, index, 1, &key);
    if (!status) {
        char digest[65], payload[128];
        ud_mutex_lock(&a->execution->journal_lock);
        status = ud_journal_write_blob(j->store, wire, bytes, digest);
        int n = 0;
        if (!status)
            n = snprintf(payload, sizeof(payload), "{\"blob\":\"%s\",\"bytes\":%llu}", digest,
                         (unsigned long long)bytes);
        if (!status)
            status = append(j, a, RECORD_SNAPSHOT_BATCH,
                            (ud_string){(char *)key.data, (uint32_t)key.size},
                            (ud_string){payload, (uint32_t)n});
        ud_mutex_unlock(&a->execution->journal_lock);
    }
    ud_free(wire);
    ud_bytes_clear(&key);
    checkpoint_after(a, status);
    return status;
}
ud_status ud_checkpoint_snapshot_info(ud_activation *a, ud_string component, ud_string expected,
                                      char **out, uint32_t *length) {
    if (!a || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_END;
    ud_bytes key = {.ctx = a->execution->ctx};
    ud_status status = snapshot_key(a, component, 0, 0, &key);
    ud_mutex_lock(&a->execution->journal_lock);
    const ud_json *entry =
        !status ? find(j, RECORD_SNAPSHOT_DONE, (ud_string){(char *)key.data, (uint32_t)key.size})
                : NULL;
    if (!status && !entry)
        status = UD_END;
    if (!status && expected.length) {
        ud_string policy = {0};
        status = ud_json_string(ud_member(entry, "policy"), &policy);
        if (!status && !ud_same(policy, expected))
            status = UD_INCOMPATIBLE;
    }
    if (!status)
        status = ud_json_serialize(entry, out, length);
    ud_mutex_unlock(&a->execution->journal_lock);
    ud_bytes_clear(&key);
    return status;
}
ud_status ud_checkpoint_snapshot_seal(ud_activation *a, ud_string component, ud_string payload) {
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_OK;
    UD_TRY(checkpoint_before(a, NULL));
    ud_bytes key = {.ctx = a->execution->ctx};
    ud_status status = snapshot_key(a, component, 0, 0, &key);
    if (!status) {
        ud_mutex_lock(&a->execution->journal_lock);
        status = append(j, a, RECORD_SNAPSHOT_DONE,
                        (ud_string){(char *)key.data, (uint32_t)key.size}, payload);
        ud_mutex_unlock(&a->execution->journal_lock);
    }
    ud_bytes_clear(&key);
    checkpoint_after(a, status);
    return status;
}
ud_status ud_checkpoint_cursor(ud_activation *a, ud_string component, ud_string payload) {
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_OK;
    UD_TRY(checkpoint_before(a, NULL));
    ud_bytes key = {.ctx = a->execution->ctx};
    ud_status status = snapshot_key(a, component, 0, 0, &key);
    if (!status) {
        ud_mutex_lock(&a->execution->journal_lock);
        status =
            append(j, a, RECORD_CURSOR, (ud_string){(char *)key.data, (uint32_t)key.size}, payload);
        ud_mutex_unlock(&a->execution->journal_lock);
    }
    ud_bytes_clear(&key);
    checkpoint_after(a, status);
    return status;
}
ud_status ud_checkpoint_schema_event(ud_activation *a, ud_string component,
                                     const ud_cdc_event *event, ud_status decision) {
    checkpoint_journal *journal = a->execution->journal;
    if (!journal)
        return UD_OK;
    ud_bytes key = {.ctx = a->execution->ctx},
             record = {.ctx = a->execution->ctx, .limit = 16u * 1024u * 1024u};
    ud_status status = snapshot_key(a, component, event->schema_version, 0, &key);
    char header[160];
    int length =
        snprintf(header, sizeof(header),
                 "{\"version\":%u,\"schemaVersion\":%llu,\"result\":%d,\"sourcePosition\":",
                 UD_CONFIG_VERSION, (unsigned long long)event->schema_version, decision);
    if (!status)
        status = ud_bytes_put(&record, header, (size_t)length);
    if (!status)
        status = ud_json_quote(&record, event->position.data, event->position.length);
    if (!status)
        status = ud_bytes_put(&record, ",\"schema\":", 10);
    char *schema = NULL;
    uint32_t size = 0;
    if (!status && event->after)
        status = ud_schema_to_json(event->after, &schema, &size);
    if (!status)
        status = ud_bytes_put(&record, schema ? schema : "null", schema ? size : 4);
    if (!status)
        status = ud_bytes_put(&record, "}", 1);
    ud_mutex_lock(&a->execution->journal_lock);
    if (!status)
        status = append(journal, a, RECORD_SCHEMA_EVENT,
                        (ud_string){(char *)key.data, (uint32_t)key.size},
                        (ud_string){(char *)record.data, (uint32_t)record.size});
    ud_mutex_unlock(&a->execution->journal_lock);
    ud_free(schema);
    ud_bytes_clear(&key);
    ud_bytes_clear(&record);
    return status;
}
ud_status ud_checkpoint_schema(ud_activation *a, ud_string component, const ud_batch *schema) {
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_OK;
    uint8_t digest[32];
    UD_TRY(ud_schema_fingerprint(schema, digest));
    char fingerprint[65], payload[68];
    hex(digest, fingerprint);
    snprintf(payload, sizeof(payload), "\"%s\"", fingerprint);
    UD_TRY(checkpoint_before(a, NULL));
    ud_bytes key = {.ctx = a->execution->ctx};
    ud_status status = snapshot_key(a, component, 0, 0, &key);
    ud_mutex_lock(&a->execution->journal_lock);
    const ud_json *entry =
        !status ? find(j, RECORD_SCHEMA, (ud_string){(char *)key.data, (uint32_t)key.size}) : NULL;
    if (!status && entry) {
        ud_string expected;
        status = ud_json_string(entry, &expected);
        if (!status && !ud_named(expected, fingerprint))
            status = UD_INCOMPATIBLE;
    } else if (!status)
        status = append(j, a, RECORD_SCHEMA, (ud_string){(char *)key.data, (uint32_t)key.size},
                        (ud_string){payload, 66});
    ud_mutex_unlock(&a->execution->journal_lock);
    ud_bytes_clear(&key);
    checkpoint_after(a, status);
    return status;
}

ud_status ud_checkpoint_epoch_info(ud_activation *a, ud_string pipeline, char **out,
                                   uint32_t *length) {
    if (!a || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_END;
    ud_bytes key = {.ctx = a->execution->ctx};
    ud_status s = snapshot_key(a, pipeline, 0, 0, &key);
    ud_mutex_lock(&a->execution->journal_lock);
    const ud_json *entry =
        !s ? find(j, RECORD_EPOCH, (ud_string){(char *)key.data, (uint32_t)key.size}) : NULL;
    if (!s)
        s = entry ? ud_json_serialize(entry, out, length) : UD_END;
    ud_mutex_unlock(&a->execution->journal_lock);
    ud_bytes_clear(&key);
    return s;
}
ud_status ud_checkpoint_epoch_commit(ud_activation *a, ud_string pipeline, ud_string payload) {
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_UNSUPPORTED;
    UD_TRY(checkpoint_before(a, NULL));
    ud_bytes key = {.ctx = a->execution->ctx};
    ud_status s = snapshot_key(a, pipeline, 0, 0, &key);
    ud_mutex_lock(&a->execution->journal_lock);
    if (!s)
        s = append(j, a, RECORD_EPOCH, (ud_string){(char *)key.data, (uint32_t)key.size}, payload);
    ud_mutex_unlock(&a->execution->journal_lock);
    ud_bytes_clear(&key);
    checkpoint_after(a, s);
    return s;
}

ud_status ud_checkpoint_snapshot_forget(ud_activation *a, ud_string component, uint64_t index,
                                        int indexed) {
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_OK;
    UD_TRY(checkpoint_before(a, NULL));
    ud_bytes key = {.ctx = a->execution->ctx};
    ud_status status = snapshot_key(a, component, index, indexed, &key);
    ud_mutex_lock(&a->execution->journal_lock);
    if (!status)
        status = ud_journal_forget(j->store, indexed ? RECORD_SNAPSHOT_BATCH : RECORD_SNAPSHOT_DONE,
                                   (ud_string){(char *)key.data, (uint32_t)key.size});
    ud_mutex_unlock(&a->execution->journal_lock);
    ud_bytes_clear(&key);
    checkpoint_after(a, status);
    return status;
}
ud_status ud_checkpoint_collect(ud_activation *a) {
    checkpoint_journal *j = a->execution->journal;
    if (!j)
        return UD_OK;
    UD_TRY(checkpoint_before(a, NULL));
    ud_mutex_lock(&a->execution->journal_lock);
    ud_status status = ud_journal_collect_blobs(j->store);
    ud_mutex_unlock(&a->execution->journal_lock);
    checkpoint_after(a, status);
    return status;
}
