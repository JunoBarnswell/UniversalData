#include "file_generation.h"
#include <errno.h>
static void hash_text(const uint8_t hash[32], char text[65]) {
    for (uint32_t at = 0; at < 32; at++)
        snprintf(text + at * 2, 3, "%02x", hash[at]);
}
static ud_status parse_hash(ud_string text, uint8_t hash[32]) {
    if (text.length != 64)
        return UD_CORRUPT;
    for (uint32_t at = 0; at < 32; at++) {
        unsigned value = 0;
        for (uint32_t n = 0; n < 2; n++) {
            char byte = text.data[at * 2 + n];
            int digit = byte >= '0' && byte <= '9'   ? byte - '0'
                        : byte >= 'a' && byte <= 'f' ? byte - 'a' + 10
                                                     : -1;
            if (digit < 0)
                return UD_CORRUPT;
            value = value * 16 + (unsigned)digit;
        }
        hash[at] = (uint8_t)value;
    }
    return UD_OK;
}
static ud_status file_exists(ud_context *ctx, const char *path, int *exists) {
    UD_TRY(ud_path_no_symlinks(ctx, path));
    int kind;
    UD_TRY(ud_path_kind(ctx, path, &kind));
    *exists = kind == 1;
    return kind == 0 || kind == 1 ? UD_OK : UD_INVALID;
}
static ud_status grant(ud_file_generation *generation, const char *path, int action) {
    ud_execution *execution = generation->activation->execution;
    return execution->options.authorize &&
                   execution->options.authorize(
                       execution->options.authorize_user, execution->options.tenant,
                       (ud_string){path, (uint32_t)strlen(path)}, action) == UD_OK
               ? UD_OK
               : UD_AUTH;
}
static ud_status capture_original(ud_file_generation *generation) {
    ud_context *ctx = generation->activation->execution->ctx;
    int exists;
    UD_TRY(file_exists(ctx, generation->original, &exists));
    if (exists) {
        uint8_t hash[32];
        UD_TRY(ud_file_hash(ctx, generation->original, hash));
        return memcmp(hash, generation->base_hash, 32) ? UD_FENCED : UD_OK;
    }
    UD_TRY(grant(generation, generation->original, UD_ACCESS_WRITE));
    size_t path_size = strlen(generation->original) + 8;
    char *partial = ud_alloc(ctx, path_size);
    if (!partial)
        return UD_RESOURCE;
    snprintf(partial, path_size, "%s.part", generation->original);
    ud_status status = grant(generation, partial, UD_ACCESS_WRITE);
    if (!status)
        status = ud_remove_file(ctx, partial);
    FILE *source = NULL, *target = NULL;
    if (!status)
        status = ud_open_regular(ctx, generation->target, &source);
    if (!status) {
        target = ud_fopen(partial, "wbx");
        if (!target)
            status = UD_IO;
    }
    uint8_t *buffer = !status ? ud_alloc(ctx, 65536) : NULL;
    if (!status && !buffer)
        status = UD_RESOURCE;
    while (!status) {
        status = ud_activation_check(generation->activation);
        if (!status)
            status = ud_connection_lease_authorize(generation->lease, UD_ACCESS_READ);
        if (status)
            break;
        size_t size = fread(buffer, 1, 65536, source);
        if (size && fwrite(buffer, 1, size, target) != size)
            status = UD_IO;
        if (size < 65536) {
            if (ferror(source))
                status = UD_IO;
            break;
        }
    }
    if (!status)
        status = ud_flush(ctx, target);
    if (source && fclose(source) && !status)
        status = UD_IO;
    if (target && fclose(target) && !status)
        status = UD_IO;
    ud_free(buffer);
    uint8_t hash[32];
    if (!status)
        status = ud_file_hash(ctx, partial, hash);
    if (!status && memcmp(hash, generation->base_hash, 32))
        status = UD_FENCED;
    if (!status)
        status = ud_publish(ctx, partial, generation->original);
    ud_free(partial);
    return status;
}
static ud_status record(ud_file_generation *generation, int phase) {
    ud_bytes json = {.ctx = generation->activation->execution->ctx};
    char base[65], output[65], prefix[160];
    hash_text(generation->base_hash, base);
    hash_text(generation->output_hash, output);
    int size = snprintf(
        prefix, sizeof(prefix),
        "{\"version\":%u,\"mode\":%d,\"baseExists\":%s,\"baseHash\":\"%s\",\"target\":",
        UD_CHECKPOINT_VERSION, generation->mode, generation->base_exists ? "true" : "false", base);
    ud_status status = ud_bytes_put(&json, prefix, (size_t)size);
    if (!status)
        status = ud_json_quote(&json, generation->target, (uint32_t)strlen(generation->target));
    if (!status)
        status = ud_bytes_put(&json, ",\"temporary\":", 13);
    if (!status)
        status =
            ud_json_quote(&json, generation->temporary, (uint32_t)strlen(generation->temporary));
    if (!status)
        status = ud_bytes_put(&json, ",\"original\":", 12);
    if (!status)
        status = ud_json_quote(&json, generation->original, (uint32_t)strlen(generation->original));
    if (!status && phase == UD_FILE_PREPARED) {
        size = snprintf(prefix, sizeof(prefix), ",\"outputHash\":\"%s\"", output);
        status = ud_bytes_put(&json, prefix, (size_t)size);
    }
    if (!status)
        status = ud_bytes_put(&json, "}", 1);
    if (!status)
        status = ud_checkpoint_file_record(generation->activation, phase,
                                           (ud_string){generation->identity, 64},
                                           (ud_string){(char *)json.data, (uint32_t)json.size});
    ud_bytes_clear(&json);
    return status;
}
static ud_status restore(ud_file_generation *generation, int phase, ud_string text) {
    ud_json *json = NULL;
    ud_context *ctx = generation->activation->execution->ctx;
    UD_TRY(ud_json_parse(ctx, text.data, text.length, &json, NULL));
    static const char *keys[] = {"version", "mode",      "baseExists", "baseHash",
                                 "target",  "temporary", "original",   "outputHash"};
    ud_status status = ud_json_keys(json, keys, phase == UD_FILE_BASE ? 7 : 8);
    uint64_t version, mode;
    ud_string value;
    if (!status)
        status = ud_json_uint(ud_member(json, "version"), &version);
    if (!status && version != UD_CHECKPOINT_VERSION)
        status = UD_INCOMPATIBLE;
    if (!status)
        status = ud_json_uint(ud_member(json, "mode"), &mode);
    if (!status && mode != (uint64_t)generation->mode)
        status = UD_FENCED;
    const char *names[] = {"target", "temporary", "original"};
    const char *expected[] = {generation->target, generation->temporary, generation->original};
    for (uint32_t at = 0; !status && at < 3; at++) {
        status = ud_json_string(ud_member(json, names[at]), &value);
        if (!status && !ud_named(value, expected[at]))
            status = UD_CORRUPT;
    }
    if (!status)
        status = ud_json_string(ud_member(json, "baseHash"), &value);
    if (!status)
        status = parse_hash(value, generation->base_hash);
    const char *boolean;
    uint32_t length;
    if (!status)
        status = ud_json_text(ud_member(json, "baseExists"), &boolean, &length);
    if (!status)
        generation->base_exists = length == 4 && !memcmp(boolean, "true", 4);
    if (!status && phase == UD_FILE_PREPARED) {
        status = ud_json_string(ud_member(json, "outputHash"), &value);
        if (!status)
            status = parse_hash(value, generation->output_hash);
    }
    ud_json_destroy(json);
    return status;
}
ud_status ud_file_generation_open(ud_activation *activation, ud_connection_lease *lease,
                                  ud_string component, const char *target, int mode,
                                  ud_file_generation **out) {
    if (!activation || !lease || !target || !out || mode < 0 || mode > 2)
        return UD_INVALID;
    *out = NULL;
    ud_context *ctx = activation->execution->ctx;
    ud_file_generation *generation = ud_alloc(ctx, sizeof(*generation));
    if (!generation)
        return UD_RESOURCE;
    generation->activation = activation;
    generation->lease = lease;
    generation->mode = mode;
    ud_bytes identity = {.ctx = ctx};
    ud_string parts[] = {activation->execution->options.execution_id,
                         activation->task_id,
                         activation->pipeline_id,
                         component,
                         {target, (uint32_t)strlen(target)}};
    ud_status status = UD_OK;
    for (uint32_t at = 0; !status && at < 5; at++)
        status = ud_put_string(&identity, parts[at].data, parts[at].length);
    if (!status)
        status = ud_put_u64(&identity, activation->invocation);
    uint8_t digest[32];
    if (!status) {
        ud_hash(identity.data, identity.size, digest);
        hash_text(digest, generation->identity);
    }
    ud_bytes_clear(&identity);
    size_t length = strlen(target) + 96;
    generation->target = ud_strdup_n(ctx, target, strlen(target));
    generation->temporary = ud_alloc(ctx, length);
    generation->original = ud_alloc(ctx, length);
    char *lock = ud_alloc(ctx, length);
    if (!status &&
        (!generation->target || !generation->temporary || !generation->original || !lock))
        status = UD_RESOURCE;
    if (!status) {
        snprintf(generation->temporary, length, "%s.ud-generation-%.32s", target,
                 generation->identity);
        snprintf(generation->original, length, "%s.ud-original-%.32s", target,
                 generation->identity);
        snprintf(lock, length, "%s.ud-package.lock", target);
        status = ud_lock(ctx, lock, &generation->lock);
    }
    ud_free(lock);
    if (!status)
        status = grant(generation, generation->temporary, UD_ACCESS_WRITE);
    char *saved = NULL;
    uint32_t saved_length = 0;
    int replaying = 0;
    if (!status) {
        status = ud_checkpoint_file_read(
            activation, UD_FILE_BASE, (ud_string){generation->identity, 64}, &saved, &saved_length);
        if (status == UD_END) {
            status = file_exists(ctx, target, &generation->base_exists);
            if (!status && generation->base_exists && mode == UD_FILE_CREATE)
                status = UD_BUSY;
            if (!status && generation->base_exists)
                status = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
            if (!status && generation->base_exists)
                status = ud_file_hash(ctx, target, generation->base_hash);
            if (!status)
                status = record(generation, UD_FILE_BASE);
        } else if (!status) {
            status = restore(generation, UD_FILE_BASE, (ud_string){saved, saved_length});
            if (!status)
                replaying = 1;
        }
    }
    ud_free(saved);
    saved = NULL;
    if (!status) {
        status =
            ud_checkpoint_file_read(activation, UD_FILE_PREPARED,
                                    (ud_string){generation->identity, 64}, &saved, &saved_length);
        if (status == UD_END)
            status = UD_OK;
        else if (!status) {
            status = restore(generation, UD_FILE_PREPARED, (ud_string){saved, saved_length});
            if (!status)
                generation->prepared = 1;
        }
    }
    ud_free(saved);
    if (!status && !generation->prepared) {
        if (generation->base_exists)
            status = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
        if (!status && generation->base_exists && mode != UD_FILE_CREATE)
            status = ud_connection_lease_authorize(lease, UD_ACCESS_DELETE);
        int exists = 0;
        if (!status)
            status = file_exists(ctx, target, &exists);
        uint8_t hash[32];
        if (!status && exists != generation->base_exists)
            status = UD_FENCED;
        if (!status && exists)
            status = ud_file_hash(ctx, target, hash);
        if (!status && exists && memcmp(hash, generation->base_hash, 32))
            status = UD_FENCED;
        if (!status && generation->base_exists)
            status = capture_original(generation);
        int staged = 0;
        if (!status)
            status = file_exists(ctx, generation->temporary, &staged);
        if (!status && staged && !replaying)
            status = UD_BUSY;
        if (!status && staged)
            status = ud_remove_file(ctx, generation->temporary);
        if (!status) {
            status = ud_reserve_file(ctx, generation->temporary);
            if (!status)
                generation->owns_stage = 1;
        }
    }
    if (status) {
        ud_file_generation_destroy(generation);
        return status;
    }
    *out = generation;
    return UD_OK;
}
ud_status ud_file_generation_check_original(ud_file_generation *generation) {
    UD_TRY(ud_connection_lease_authorize(generation->lease, UD_ACCESS_READ));
    UD_TRY(grant(generation, generation->original, UD_ACCESS_READ));
    return UD_OK;
}
ud_status ud_file_generation_publish(ud_file_generation *generation) {
    ud_context *ctx = generation->activation->execution->ctx;
    UD_TRY(ud_connection_lease_authorize(generation->lease, UD_ACCESS_WRITE));
    if (!generation->prepared) {
        UD_TRY(ud_file_hash(ctx, generation->temporary, generation->output_hash));
        UD_TRY(record(generation, UD_FILE_PREPARED));
        generation->prepared = 1;
    }
    int exists;
    UD_TRY(file_exists(ctx, generation->target, &exists));
    uint8_t hash[32];
    if (exists) {
        UD_TRY(ud_file_hash(ctx, generation->target, hash));
        if (!memcmp(hash, generation->output_hash, 32)) {
            return UD_OK;
        }
    }
    if (exists != generation->base_exists || (exists && memcmp(hash, generation->base_hash, 32)))
        return UD_FENCED;
    UD_TRY(ud_file_hash(ctx, generation->temporary, hash));
    if (memcmp(hash, generation->output_hash, 32))
        return UD_CORRUPT;
    if (exists) {
        UD_TRY(ud_connection_lease_authorize(generation->lease, UD_ACCESS_DELETE));
        UD_TRY(ud_file_hash(ctx, generation->original, hash));
        if (memcmp(hash, generation->base_hash, 32))
            return UD_FENCED;
    }
    ud_status status = exists ? ud_replace_file(ctx, generation->temporary, generation->target)
                              : ud_publish(ctx, generation->temporary, generation->target);
    if (status)
        return UD_COMMIT_UNKNOWN;
    return UD_OK;
}
void ud_file_generation_destroy(ud_file_generation *generation) {
    if (!generation)
        return;
    ud_context *ctx = generation->activation->execution->ctx;
    if (!generation->prepared && generation->owns_stage && generation->temporary)
        (void)ud_remove_file(ctx, generation->temporary);
    ud_unlock(generation->lock);
    ud_free(generation->target);
    ud_free(generation->temporary);
    ud_free(generation->original);
    ud_free(generation);
}
