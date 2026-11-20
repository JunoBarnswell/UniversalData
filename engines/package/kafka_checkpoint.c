#include "internal.h"
#include "kafka_checkpoint.h"
#include "../../components/execution/state.h"

enum {
    STATE_CHUNK_BYTES = 512 * 1024,
    STATE_MANIFEST_BYTES = 768 * 1024,
    STATE_MAX_CHUNKS = 8192,
    STATE_MAX_COMPONENTS = 2048,
    STATE_TRANSACTION_ID_BYTES = 78,
    STATE_KEY_BYTES = STATE_TRANSACTION_ID_BYTES + 9,
    STATE_KEY_MANIFEST = 0,
    STATE_KEY_CHUNK = 1,
};

static const char manifest_magic[] = "UDKafkaCheckpoint\3";
static const char epoch_header[] = "ud-state-epoch";

typedef struct checkpoint_proof {
    uint32_t chunks;
    uint64_t bytes, epoch;
    uint8_t digest[32];
} checkpoint_proof;

typedef struct checkpoint_chunk {
    ud_buffer_block *block;
    uint32_t bytes;
    uint8_t digest[32];
} checkpoint_chunk;

typedef struct checkpoint_component {
    checkpoint_proof committed, staged;
    checkpoint_chunk *loaded;
    uint32_t capacity;
} checkpoint_component;

typedef struct checkpoint_scan {
    ud_kafka_checkpoint *state;
    ud_string envelope;
    int64_t manifest_offset;
    int saw_chunk;
} checkpoint_scan;

struct ud_kafka_checkpoint {
    ud_activation *activation;
    ud_context *memory;
    ud_buffer_manager *buffers;
    ud_memory_domain *wire_domain;
    ud_connection_lease *lease;
    ud_message_target target;
    char transaction_id[STATE_TRANSACTION_ID_BYTES + 1];
    char checkpoint_id[STATE_TRANSACTION_ID_BYTES];
    ud_mutex mutex;
    int32_t partition;
    int64_t manifest_offset;
    int active, commit_unknown;
    ud_status failed;
    uint64_t committed_epoch, staged_epoch, wire_limit;
    uint32_t count, loaded_slots, staged_chunks;
    int fingerprint_matches;
    ud_string manifest;
    checkpoint_component *components;
};

static void pack_uint(char *out, uint64_t value, uint32_t bytes) {
    for (uint32_t i = 0; i < bytes; i++)
        out[i] = (char)(uint8_t)(value >> (8 * i));
}

static uint64_t unpack_uint(const char *data, uint32_t bytes) {
    uint64_t value = 0;
    for (uint32_t i = 0; i < bytes; i++)
        value |= (uint64_t)(uint8_t)data[i] << (8 * i);
    return value;
}

static ud_status transaction_identity(ud_kafka_checkpoint *state, ud_string pipeline,
                                       ud_string group) {
    ud_activation *a = state->activation;
    ud_string project = {0};
    const ud_json *definition = ud_member(a->package->document, "project");
    if (definition)
        UD_TRY(ud_json_string(ud_member(definition, "id"), &project));
    const ud_string values[] = {a->execution->options.tenant, project, a->package->id, pipeline, group};
    ud_sha256 hash;
    ud_sha256_init(&hash);
    for (uint32_t i = 0; i < sizeof(values) / sizeof(*values); i++) {
        char size[4];
        pack_uint(size, values[i].length, sizeof(size));
        ud_sha256_update(&hash, size, sizeof(size));
        if (values[i].length)
            ud_sha256_update(&hash, values[i].data, values[i].length);
    }
    char partition[4];
    pack_uint(partition, (uint32_t)state->partition, sizeof(partition));
    ud_sha256_update(&hash, partition, sizeof(partition));
    uint8_t digest[32];
    ud_sha256_finish(&hash, digest);
    static const char prefix[] = "ud-checkpoint-";
    static const char hex[] = "0123456789abcdef";
    _Static_assert(sizeof(prefix) - 1 + 64 == STATE_TRANSACTION_ID_BYTES, "transaction ID size");
    memcpy(state->transaction_id, prefix, sizeof(prefix) - 1);
    for (uint32_t i = 0; i < sizeof(digest); i++) {
        state->transaction_id[sizeof(prefix) - 1 + 2 * i] = hex[digest[i] >> 4];
        state->transaction_id[sizeof(prefix) + 2 * i] = hex[digest[i] & 15];
    }
    state->transaction_id[STATE_TRANSACTION_ID_BYTES] = 0;
    memcpy(state->checkpoint_id, state->transaction_id, STATE_TRANSACTION_ID_BYTES);
    ud_string run = a->execution->assignment_count ? a->execution->partition_run : a->execution->options.worker_run_id;
    if (run.length) {
        ud_sha256_init(&hash);
        ud_sha256_update(&hash, digest, sizeof(digest));
        ud_sha256_update(&hash, run.data, run.length);
        ud_sha256_finish(&hash, digest);
        for (uint32_t i = 0; i < sizeof(digest); i++) {
            state->checkpoint_id[sizeof(prefix) - 1 + 2 * i] = hex[digest[i] >> 4];
            state->checkpoint_id[sizeof(prefix) + 2 * i] = hex[digest[i] & 15];
        }
    }
    return UD_OK;
}

static void release_loaded(ud_kafka_checkpoint *state) {
    for (uint32_t i = 0; state->components && i < state->count; i++) {
        checkpoint_component *component = state->components + i;
        for (uint32_t j = 0; j < component->capacity; j++)
            if (component->loaded[j].block)
                (void)ud_buffer_release(component->loaded[j].block);
        ud_free(component->loaded);
        component->loaded = NULL;
        component->capacity = 0;
    }
    state->loaded_slots = 0;
}

static ud_status store_chunk(ud_kafka_checkpoint *state, uint32_t component, uint32_t ordinal,
                             ud_string payload) {
    if (!payload.length || payload.length > STATE_CHUNK_BYTES ||
        component >= state->count || ordinal >= STATE_MAX_CHUNKS)
        return UD_CORRUPT;
    checkpoint_component *owner = state->components + component;
    if (ordinal >= owner->capacity) {
        uint32_t next = owner->capacity ? owner->capacity * 2 : 1;
        if (next < ordinal + 1)
            next = ordinal + 1;
        if (next > STATE_MAX_CHUNKS - state->loaded_slots + owner->capacity)
            next = ordinal + 1;
        if (next > STATE_MAX_CHUNKS - state->loaded_slots + owner->capacity)
            return UD_RESOURCE;
        checkpoint_chunk *expanded = ud_alloc(state->memory, (size_t)next * sizeof(*expanded));
        if (!expanded)
            return UD_RESOURCE;
        if (owner->capacity)
            memcpy(expanded, owner->loaded, (size_t)owner->capacity * sizeof(*expanded));
        ud_free(owner->loaded);
        state->loaded_slots += next - owner->capacity;
        owner->loaded = expanded;
        owner->capacity = next;
    }
    checkpoint_chunk *slot = owner->loaded + ordinal;
    if (slot->block)
        return UD_CORRUPT;
    const ud_field field = {.name = "wire", .name_length = 4, .type = UD_BINARY};
    const ud_value value = {.type = UD_BINARY, .text = payload.data, .text_length = payload.length};
    ud_batch *batch = NULL;
    ud_status status = ud_batch_create(state->memory, &field, 1, 1, &batch);
    if (!status)
        status = ud_batch_append(batch, &value, 1);
    if (!status)
        status = ud_batch_seal(batch);
    if (!status)
        status = ud_buffer_store(state->buffers, batch, &slot->block);
    ud_batch_destroy(batch);
    if (!status) {
        slot->bytes = payload.length;
        ud_hash(payload.data, payload.length, slot->digest);
    }
    return status;
}

static ud_status manifest_read(ud_kafka_checkpoint *state, ud_string payload) {
    if (payload.length > STATE_MANIFEST_BYTES)
        return UD_CORRUPT;
    ud_bytes wire = {.data = (uint8_t *)payload.data, .size = payload.length, .ctx = state->memory};
    char magic[sizeof(manifest_magic) - 1];
    UD_TRY(ud_bytes_get(&wire, magic, sizeof(magic)));
    if (memcmp(magic, manifest_magic, sizeof(magic)))
        return UD_INCOMPATIBLE;
    uint64_t protocol, revision, epoch, count;
    uint8_t fingerprint[32];
    UD_TRY(ud_get_u64(&wire, &protocol));
    UD_TRY(ud_get_u64(&wire, &revision));
    UD_TRY(ud_get_u64(&wire, &epoch));
    UD_TRY(ud_bytes_get(&wire, fingerprint, sizeof(fingerprint)));
    UD_TRY(ud_get_u64(&wire, &count));
    if (protocol != UD_PACKAGE_VERSION || revision != UD_CHECKPOINT_FORMAT_REVISION ||
        count != state->count)
        return UD_INCOMPATIBLE;
    if (!epoch || epoch <= state->committed_epoch)
        return UD_CORRUPT;
    uint64_t chunks_total = 0;
    for (uint32_t i = 0; i < state->count; i++) {
        checkpoint_proof *proof = &state->components[i].committed;
        uint64_t chunks;
        UD_TRY(ud_get_u64(&wire, &chunks));
        UD_TRY(ud_get_u64(&wire, &proof->bytes));
        UD_TRY(ud_get_u64(&wire, &proof->epoch));
        UD_TRY(ud_bytes_get(&wire, proof->digest, sizeof(proof->digest)));
        if (!proof->epoch || proof->epoch > epoch || !chunks || chunks > STATE_MAX_CHUNKS - chunks_total || !proof->bytes ||
            proof->bytes > chunks * STATE_CHUNK_BYTES ||
            proof->bytes <= (chunks - 1) * STATE_CHUNK_BYTES)
            return UD_CORRUPT;
        proof->chunks = (uint32_t)chunks;
        chunks_total += chunks;
    }
    uint64_t length = 0;
    UD_TRY(ud_get_u64(&wire, &length));
    if (!length || length != wire.size - wire.pos)
        return UD_CORRUPT;
    char *document = ud_strdup_n(state->memory, payload.data + wire.pos, (size_t)length);
    if (!document)
        return UD_RESOURCE;
    ud_free((void *)state->manifest.data);
    state->manifest = (ud_string){document, (uint32_t)length};
    state->committed_epoch = epoch;
    state->fingerprint_matches =
        !memcmp(fingerprint, state->activation->package->fingerprint, sizeof(fingerprint));
    return UD_OK;
}

static const char *checkpoint_key(const ud_kafka_checkpoint *state, ud_string key) {
    return key.length > STATE_TRANSACTION_ID_BYTES && key.data &&
                   !memcmp(key.data, state->checkpoint_id, STATE_TRANSACTION_ID_BYTES)
               ? key.data + STATE_TRANSACTION_ID_BYTES
               : NULL;
}

static ud_status visit_manifest(void *user, const ud_message *message) {
    checkpoint_scan *scan = user;
    const char *suffix = checkpoint_key(scan->state, message->key);
    if (!suffix)
        return UD_OK;
    if (suffix[0] == STATE_KEY_CHUNK) {
        if (message->payload.data)
            scan->saw_chunk = 1;
        return UD_OK;
    }
    if (suffix[0] != STATE_KEY_MANIFEST ||
        message->key.length != STATE_TRANSACTION_ID_BYTES + 1 || !message->payload.data ||
        message->payload.length > STATE_MANIFEST_BYTES || message->offset < 0)
        return UD_CORRUPT;
    char *owned = ud_strdup_n(scan->state->memory, message->payload.data, message->payload.length);
    if (!owned)
        return UD_RESOURCE;
    ud_free((void *)scan->envelope.data);
    scan->envelope = (ud_string){owned, message->payload.length};
    scan->manifest_offset = message->offset;
    return UD_OK;
}

static ud_status visit_state(void *user, const ud_message *message) {
    ud_kafka_checkpoint *state = user;
    const char *suffix = checkpoint_key(state, message->key);
    if (!suffix)
        return UD_OK;
    if (message->offset > state->manifest_offset)
        return UD_FENCED;
    if (suffix[0] == STATE_KEY_MANIFEST)
        return UD_OK;
    if (suffix[0] != STATE_KEY_CHUNK || message->key.length != STATE_KEY_BYTES)
        return UD_CORRUPT;
    /* Tombstones retire prior suffixes. The epoch in a live record decides its membership. */
    if (!message->payload.data)
        return UD_OK;
    if (message->header_count != 1 ||
        !ud_named(message->headers[0].name, epoch_header) || !message->headers[0].value.data ||
        message->headers[0].value.length != 8)
        return UD_CORRUPT;
    uint32_t component = (uint32_t)unpack_uint(suffix + 1, 4);
    uint32_t ordinal = (uint32_t)unpack_uint(suffix + 5, 4);
    uint64_t epoch = unpack_uint(message->headers[0].value.data, 8);
    if (component >= state->count)
        return UD_CORRUPT;
    uint64_t expected = state->components[component].committed.epoch;
    if (epoch < expected)
        return UD_OK;
    if (epoch > expected)
        return UD_FENCED;
    return store_chunk(state, component, ordinal, message->payload);
}

static ud_status validate_scan(ud_kafka_checkpoint *state) {
    if (!state->committed_epoch)
        return UD_OK;
    if (!state->fingerprint_matches)
        return UD_INCOMPATIBLE;
    for (uint32_t i = 0; i < state->count; i++) {
        checkpoint_component *component = state->components + i;
        checkpoint_proof *proof = &component->committed;
        if (proof->chunks > component->capacity)
            return UD_CORRUPT;
        ud_sha256 hash;
        ud_sha256_init(&hash);
        uint64_t size = 0;
        for (uint32_t j = 0; j < component->capacity; j++) {
            const checkpoint_chunk *chunk = component->loaded + j;
            if (!!chunk->block != (j < proof->chunks))
                return UD_CORRUPT;
            if (!chunk->block)
                continue;
            uint64_t remaining = proof->bytes - size;
            uint32_t length = remaining < STATE_CHUNK_BYTES ? (uint32_t)remaining : STATE_CHUNK_BYTES;
            if (chunk->bytes != length)
                return UD_CORRUPT;
            size += chunk->bytes;
            ud_sha256_update(&hash, chunk->digest, sizeof(chunk->digest));
        }
        uint8_t digest[32];
        ud_sha256_finish(&hash, digest);
        if (size != proof->bytes || memcmp(digest, proof->digest, sizeof(digest)))
            return UD_CORRUPT;
    }
    return UD_OK;
}

static ud_status validate_manifest_json(ud_kafka_checkpoint *state, ud_string manifest,
                                        uint64_t epoch) {
    if (!manifest.data || !manifest.length || manifest.length > STATE_MANIFEST_BYTES)
        return UD_INVALID;
    ud_json *document = NULL;
    ud_status status = ud_json_parse(state->memory, manifest.data, manifest.length, &document, NULL);
    if (!status) {
        uint64_t declared;
        status = ud_json_uint(ud_member(document, "epoch"), &declared);
        if (!status && epoch != declared)
            status = UD_CORRUPT;
    }
    ud_json_destroy(document);
    return status;
}

static ud_status load_committed(ud_kafka_checkpoint *state) {
    checkpoint_scan scan = {.state = state};
    ud_status status = ud_connection_lease_message_visit_committed(
        state->lease, state->target.name, state->partition, &scan, visit_manifest);
    if (!status && !scan.envelope.data)
        status = scan.saw_chunk ? UD_CORRUPT : UD_END;
    if (!status)
        status = manifest_read(state, scan.envelope);
    state->manifest_offset = scan.manifest_offset;
    ud_free((void *)scan.envelope.data);
    if (status)
        return status == UD_END ? UD_OK : status;
    if (!state->fingerprint_matches)
        return UD_INCOMPATIBLE;
    UD_TRY(validate_manifest_json(state, state->manifest, state->committed_epoch));
    /* Only the latest epoch consumes recovery storage. Compaction can leave prior snapshots
     * physically readable and they can exceed the state size of the current epoch. */
    UD_TRY(ud_connection_lease_message_visit_committed(
        state->lease, state->target.name, state->partition, state, visit_state));
    return validate_scan(state);
}

static int text(ud_string value) {
    return value.data && value.length && !memchr(value.data, 0, value.length);
}

static ud_status begin_transaction(ud_kafka_checkpoint *state) {
    if (state->failed)
        return state->failed;
    if (state->active)
        return UD_OK;
    ud_string id = {state->transaction_id, STATE_TRANSACTION_ID_BYTES};
    ud_status status = ud_connection_lease_message_begin_epoch(state->lease, id, (ud_string){0});
    if (!status)
        state->active = 1;
    else
        state->failed = status;
    return status;
}

ud_status ud_kafka_checkpoint_open(ud_activation *activation, ud_string connection, ud_string topic,
                                     int32_t partition, ud_string pipeline, ud_string group,
                                     uint32_t component_count, ud_kafka_checkpoint **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!activation || !text(connection) || !text(topic) || !text(pipeline) || !text(group) ||
        partition < 0 || !component_count || component_count > STATE_MAX_COMPONENTS ||
        !activation->execution->buffers)
        return UD_INVALID;
    ud_context *memory = activation->execution->ctx;
    ud_kafka_checkpoint *state = ud_alloc(memory, sizeof(*state));
    if (!state)
        return UD_RESOURCE;
    state->activation = activation;
    state->memory = memory;
    state->buffers = activation->execution->buffers;
    state->partition = partition;
    state->count = component_count;
    state->target = (ud_message_target){.kind = UD_MESSAGE_TOPIC,
                                      .name = {ud_strdup_n(memory, topic.data, topic.length),
                                               topic.length}};
    ud_mutex_init(&state->mutex);
    state->components = ud_alloc(memory, (size_t)component_count * sizeof(*state->components));
    ud_status status = state->components && state->target.name.data ? UD_OK : UD_RESOURCE;
    ud_buffer_options limits;
    if (!status)
        status = ud_buffer_manager_limits(state->buffers, &limits);
    if (!status) {
        state->wire_limit = limits.working_limit;
        uint64_t protocol_limit = (uint64_t)STATE_MAX_CHUNKS * STATE_CHUNK_BYTES;
        if (state->wire_limit > protocol_limit)
            state->wire_limit = protocol_limit;
        if (state->wire_limit > SIZE_MAX)
            state->wire_limit = SIZE_MAX;
        status = ud_buffer_state_domain_create(state->buffers, state->wire_limit, &state->wire_domain);
    }
    if (!status)
        status = transaction_identity(state, pipeline, group);
    if (!status)
        status = ud_activation_acquire_connection(activation, connection,
                                                    UD_CONNECT_MESSAGE_PUBLISH |
                                                        UD_CONNECT_MESSAGE_TRANSACTION,
                                                    &state->lease);
    /* Producer initialization fences the previous owner before obtaining the read-committed view. */
    if (!status)
        status = begin_transaction(state);
    if (!status)
        status = load_committed(state);
    if (status) {
        state->failed = status;
        ud_kafka_checkpoint_close(state);
        return status;
    }
    *out = state;
    return UD_OK;
}

ud_status ud_kafka_checkpoint_manifest(ud_kafka_checkpoint *state, ud_string *out) {
    if (!state || !out)
        return UD_INVALID;
    ud_mutex_lock(&state->mutex);
    *out = state->manifest;
    ud_status status = state->committed_epoch ? UD_OK : UD_END;
    ud_mutex_unlock(&state->mutex);
    return status;
}

ud_status ud_kafka_checkpoint_restore(ud_kafka_checkpoint *state, uint32_t index, uint64_t epoch,
                                       ud_batch **out) {
    if (!state || !out || index >= state->count || !epoch)
        return UD_INVALID;
    *out = NULL;
    ud_mutex_lock(&state->mutex);
    ud_status status = UD_OK;
    checkpoint_component *component = state->components + index;
    const checkpoint_proof *proof = &component->committed;
    if (epoch != state->committed_epoch)
        status = UD_END;
    else if (proof->bytes > SIZE_MAX || proof->bytes > state->wire_limit)
        status = UD_RESOURCE;
    void *data = NULL;
    if (!status)
        status = ud_buffer_working_allocate(state->buffers, (size_t)proof->bytes, &data);
    size_t cursor = 0;
    for (uint32_t i = 0; !status && i < proof->chunks; i++) {
        ud_buffer_view *view = NULL;
        status = ud_buffer_read(component->loaded[i].block, &view);
        if (!status) {
            ud_value value;
            status = ud_batch_get(ud_buffer_view_batch(view), 0, 0, &value);
            if (!status && (value.is_null || value.text_length != component->loaded[i].bytes))
                status = UD_CORRUPT;
            if (!status) {
                memcpy((uint8_t *)data + cursor, value.text, value.text_length);
                cursor += value.text_length;
            }
        }
        ud_buffer_view_release(view);
    }
    if (!status)
        status = ud_batch_deserialize(state->memory, data, cursor, out);
    ud_free(data);
    ud_mutex_unlock(&state->mutex);
    return status;
}

ud_status ud_kafka_checkpoint_begin(ud_kafka_checkpoint *state) {
    if (!state)
        return UD_INVALID;
    ud_mutex_lock(&state->mutex);
    ud_status status = begin_transaction(state);
    ud_mutex_unlock(&state->mutex);
    return status;
}

ud_connection_lease *ud_kafka_checkpoint_producer(ud_kafka_checkpoint *state) {
    return state ? state->lease : NULL;
}

static ud_status publish_record(ud_kafka_checkpoint *state, uint32_t component, uint32_t ordinal,
                                 uint64_t epoch, ud_string payload) {
    char key[STATE_KEY_BYTES], epoch_bytes[8];
    memcpy(key, state->checkpoint_id, STATE_TRANSACTION_ID_BYTES);
    uint32_t key_length = STATE_TRANSACTION_ID_BYTES + 1;
    if (component == UINT32_MAX) {
        key[STATE_TRANSACTION_ID_BYTES] = STATE_KEY_MANIFEST;
    } else {
        key[STATE_TRANSACTION_ID_BYTES] = STATE_KEY_CHUNK;
        pack_uint(key + STATE_TRANSACTION_ID_BYTES + 1, component, 4);
        pack_uint(key + STATE_TRANSACTION_ID_BYTES + 5, ordinal, 4);
        key_length = STATE_KEY_BYTES;
    }
    pack_uint(epoch_bytes, epoch, sizeof(epoch_bytes));
    ud_message_header header = {{epoch_header, sizeof(epoch_header) - 1},
                                 {epoch_bytes, sizeof(epoch_bytes)}};
    int with_epoch = component != UINT32_MAX && payload.data != NULL;
    ud_message message = {.payload = payload, .key = {key, key_length},
                          .headers = with_epoch ? &header : NULL, .header_count = (uint32_t)with_epoch,
                          .partition = state->partition, .offset = -1, .timestamp_ms = -1};
    ud_message_result result = {0};
    ud_status status = ud_connection_lease_message_publish(state->lease, &state->target, &message,
                                                            1, &result);
    if (!status && result.state != UD_MESSAGE_ACCEPTED)
        status = result.result ? result.result : UD_IO;
    return status;
}

static ud_status stage_wire(ud_kafka_checkpoint *state, uint32_t index, uint64_t epoch,
                             const ud_batch *batch) {
    ud_memory_scope scope;
    ud_memory_domain_enter(state->wire_domain, &scope);
    ud_bytes wire = {.ctx = state->memory, .limit = (size_t)state->wire_limit};
    ud_status status = ud_encode_batch(&wire, batch);
    ud_memory_domain_leave(&scope);
    uint64_t chunks = wire.size ? ((uint64_t)wire.size - 1) / STATE_CHUNK_BYTES + 1 : 0;
    if (!status && (!chunks || chunks > STATE_MAX_CHUNKS - state->staged_chunks))
        status = UD_RESOURCE;
    checkpoint_proof proof = {.bytes = wire.size, .epoch = epoch, .chunks = (uint32_t)chunks};
    ud_sha256 hash;
    ud_sha256_init(&hash);
    uint64_t cursor = 0;
    for (uint32_t i = 0; !status && i < proof.chunks; i++) {
        uint64_t remaining = proof.bytes - cursor;
        uint32_t length = remaining < STATE_CHUNK_BYTES ? (uint32_t)remaining : STATE_CHUNK_BYTES;
        ud_string bytes = {(const char *)wire.data + cursor, length};
        status = publish_record(state, index, i, epoch, bytes);
        if (!status) {
            uint8_t digest[32];
            ud_hash(bytes.data, bytes.length, digest);
            ud_sha256_update(&hash, digest, sizeof(digest));
            cursor += length;
        }
    }
    ud_bytes_clear(&wire);
    if (!status) {
        ud_sha256_finish(&hash, proof.digest);
        state->components[index].staged = proof;
        state->staged_chunks += proof.chunks;
    }
    return status;
}

ud_status ud_kafka_checkpoint_stage(ud_kafka_checkpoint *state, uint32_t index, uint64_t epoch,
                                     const ud_batch *batch) {
    if (!state || index >= state->count || !batch || !batch->sealed || !epoch)
        return UD_INVALID;
    ud_mutex_lock(&state->mutex);
    ud_status status = state->failed;
    if (!status && (epoch <= state->committed_epoch ||
                    (state->staged_epoch && state->staged_epoch != epoch) ||
                    state->components[index].staged.chunks))
        status = UD_INVALID;
    if (!status)
        status = begin_transaction(state);
    if (!status) {
        state->staged_epoch = epoch;
        status = stage_wire(state, index, epoch, batch);
        if (status)
            state->failed = status;
    }
    ud_mutex_unlock(&state->mutex);
    return status;
}

ud_status ud_kafka_checkpoint_reuse(ud_kafka_checkpoint *state, uint32_t index, uint64_t epoch) {
    ud_mutex_lock(&state->mutex);
    checkpoint_component *component = state->components + index;
    ud_status status = state->failed;
    if (!status && (epoch <= state->committed_epoch || !component->committed.chunks ||
                    (state->staged_epoch && state->staged_epoch != epoch) || component->staged.chunks ||
                    component->committed.chunks > STATE_MAX_CHUNKS - state->staged_chunks))
        status = UD_INVALID;
    if (!status)
        status = begin_transaction(state);
    if (!status) {
        state->staged_epoch = epoch;
        component->staged = component->committed;
        state->staged_chunks += component->staged.chunks;
    }
    ud_mutex_unlock(&state->mutex);
    return status;
}

static ud_status manifest_write(ud_kafka_checkpoint *state, uint64_t epoch, ud_string manifest,
                                 ud_bytes *wire) {
    UD_TRY(validate_manifest_json(state, manifest, epoch));
    UD_TRY(ud_bytes_put(wire, manifest_magic, sizeof(manifest_magic) - 1));
    UD_TRY(ud_put_u64(wire, UD_PACKAGE_VERSION));
    UD_TRY(ud_put_u64(wire, UD_CHECKPOINT_FORMAT_REVISION));
    UD_TRY(ud_put_u64(wire, epoch));
    const uint8_t *fingerprint = state->activation->package->fingerprint;
    UD_TRY(ud_bytes_put(wire, fingerprint, sizeof(state->activation->package->fingerprint)));
    UD_TRY(ud_put_u64(wire, state->count));
    for (uint32_t i = 0; i < state->count; i++) {
        const checkpoint_proof *proof = &state->components[i].staged;
        if (!proof->chunks)
            return UD_INVALID;
        UD_TRY(ud_put_u64(wire, proof->chunks));
        UD_TRY(ud_put_u64(wire, proof->bytes));
        UD_TRY(ud_put_u64(wire, proof->epoch));
        UD_TRY(ud_bytes_put(wire, proof->digest, sizeof(proof->digest)));
    }
    return ud_put_string(wire, manifest.data, manifest.length);
}

static ud_status retire_suffixes(ud_kafka_checkpoint *state, uint64_t epoch) {
    for (uint32_t i = 0; i < state->count; i++) {
        const checkpoint_component *component = state->components + i;
        for (uint32_t j = component->staged.chunks; j < component->committed.chunks; j++)
            UD_TRY(publish_record(state, i, j, epoch, (ud_string){0}));
    }
    return UD_OK;
}

ud_status ud_kafka_checkpoint_commit(ud_kafka_checkpoint *state, uint64_t epoch,
                                      ud_string manifest, const ud_message_epoch *offsets) {
    if (!state || !epoch || !offsets)
        return UD_INVALID;
    ud_mutex_lock(&state->mutex);
    ud_status status = state->failed;
    if (!status && (!state->active || state->staged_epoch != epoch))
        status = UD_INVALID;
    ud_bytes wire = {.ctx = state->memory, .limit = STATE_MANIFEST_BYTES};
    if (!status)
        status = manifest_write(state, epoch, manifest, &wire);
    char *next_manifest = NULL;
    if (!status) {
        next_manifest = ud_strdup_n(state->memory, manifest.data, manifest.length);
        if (!next_manifest)
            status = UD_RESOURCE;
    }
    if (!status)
        status = retire_suffixes(state, epoch);
    if (!status)
        status = publish_record(state, UINT32_MAX, 0, epoch,
                                 (ud_string){(const char *)wire.data, (uint32_t)wire.size});
    if (!status) {
        status = ud_connection_lease_message_commit_epoch(state->lease, offsets);
        if (status == UD_COMMIT_UNKNOWN || status == UD_FENCED)
            state->commit_unknown = 1;
    }
    ud_bytes_clear(&wire);
    if (status) {
        state->failed = status;
        ud_free(next_manifest);
    } else {
        state->active = 0;
        state->committed_epoch = epoch;
        state->staged_epoch = 0;
        state->staged_chunks = 0;
        ud_free((void *)state->manifest.data);
        state->manifest = (ud_string){next_manifest, manifest.length};
        release_loaded(state);
        for (uint32_t i = 0; i < state->count; i++) {
            state->components[i].committed = state->components[i].staged;
            state->components[i].staged = (checkpoint_proof){0};
        }
    }
    ud_mutex_unlock(&state->mutex);
    return status;
}

void ud_kafka_checkpoint_close(ud_kafka_checkpoint *state) {
    if (!state)
        return;
    if (state->active && !state->commit_unknown && state->failed != UD_FENCED)
        (void)ud_connection_lease_message_abort_epoch(state->lease);
    (void)ud_connection_lease_release(state->lease);
    release_loaded(state);
    if (state->wire_domain)
        (void)ud_memory_domain_destroy(state->wire_domain);
    ud_mutex_destroy(&state->mutex);
    ud_free((void *)state->target.name.data);
    ud_free((void *)state->manifest.data);
    ud_free(state->components);
    ud_free(state);
}
