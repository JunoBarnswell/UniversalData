#include "ud/catalog.h"
#include "ud/json.h"
#include <errno.h>
#include "internal.h"
static int kind_valid(int32_t kind) {
    return kind >= UD_CATALOG_PROJECT && kind <= UD_CATALOG_AUDIT;
}
static int string_valid(ud_string s, int required) {
    return (!required || s.length) && (!s.length || (s.data && !memchr(s.data, 0, s.length) &&
                                                     ud_utf8_valid(s.data, s.length)));
}
static ud_status grant(ud_catalog *c, int action) {
    return c->options.authorize &&
                   c->options.authorize(c->options.authorize_user, c->options.tenant,
                                        c->options.directory, action) == UD_OK
               ? UD_OK
               : UD_AUTH;
}
static ud_status key_check(const ud_catalog_key *key) {
    if (!key)
        return UD_INVALID;
    if (key->struct_size != sizeof(*key) || key->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    return kind_valid(key->kind) && string_valid(key->id, 1) && key->id.length <= 255 &&
                   key->revision
               ? UD_OK
               : UD_INVALID;
}
static ud_status path(ud_catalog *c, const ud_catalog_key *key, char **out) {
    ud_bytes input = {.ctx = c->ctx};
    ud_status s = ud_put_string(&input, c->tenant, strlen(c->tenant));
    if (!s)
        s = ud_put_string(&input, key->id.data, key->id.length);
    uint8_t digest[32];
    char encoded[65], name[128];
    static const char hex[] = "0123456789abcdef";
    if (!s) {
        ud_hash(input.data, input.size, digest);
        for (int i = 0; i < 32; i++) {
            encoded[i * 2] = hex[digest[i] >> 4];
            encoded[i * 2 + 1] = hex[digest[i] & 15];
        }
        encoded[64] = 0;
        snprintf(name, sizeof(name), "catalog-%d-%s-%020llu.udc", key->kind, encoded,
                 (unsigned long long)key->revision);
        s = ud_join_path(c->ctx, c->directory, name, out);
    }
    ud_bytes_clear(&input);
    return s;
}
static ud_status read_envelope(ud_catalog *c, const char *file, const ud_catalog_key *expected,
                               int32_t *kind, char **id, uint64_t *revision, char **payload,
                               uint32_t *length) {
    *id = NULL;
    *payload = NULL;
    ud_bytes bytes = {.ctx = c->ctx};
    FILE *probe = ud_fopen(file, "rb");
    if (!probe)
        return errno == ENOENT ? UD_END : UD_IO;
    if (fclose(probe))
        return UD_IO;
    ud_status s = ud_read_file(c->ctx, file, &bytes);
    uint64_t version = 0, type = 0;
    char *tenant = NULL;
    uint8_t digest[32];
    if (!s && bytes.size < 72)
        s = UD_CORRUPT;
    if (!s) {
        ud_hash(bytes.data, bytes.size - 32, digest);
        if (memcmp(digest, bytes.data + bytes.size - 32, 32))
            s = UD_CORRUPT;
        else
            bytes.size -= 32;
    }
    if (!s)
        s = ud_get_u64(&bytes, &version);
    if (!s && version != UD_CONFIG_VERSION)
        s = UD_INCOMPATIBLE;
    if (!s)
        s = ud_get_u64(&bytes, &type);
    if (!s && (type > INT32_MAX || !kind_valid((int32_t)type)))
        s = UD_CORRUPT;
    if (!s)
        s = ud_get_string(&bytes, &tenant);
    if (!s && strcmp(tenant, c->tenant))
        s = UD_AUTH;
    if (!s)
        s = ud_get_string(&bytes, id);
    if (!s)
        s = ud_get_u64(&bytes, revision);
    if (!s)
        s = ud_get_string(&bytes, payload);
    if (!s && (bytes.pos != bytes.size || !*revision || !**id || strlen(*id) > 255 ||
               !ud_utf8_valid(*id, strlen(*id))))
        s = UD_CORRUPT;
    if (!s && expected &&
        (type != (uint64_t)expected->kind || *revision != expected->revision ||
         strlen(*id) != expected->id.length || memcmp(*id, expected->id.data, expected->id.length)))
        s = UD_CORRUPT;
    if (!s) {
        ud_catalog_key actual = {
            sizeof(actual), UD_ABI_VERSION, (int32_t)type, {*id, (uint32_t)strlen(*id)}, *revision};
        char *canonical_path = NULL;
        s = path(c, &actual, &canonical_path);
        if (!s && strcmp(file, canonical_path))
            s = UD_CORRUPT;
        ud_free(canonical_path);
    }
    if (!s) {
        ud_json *doc = NULL;
        *length = (uint32_t)strlen(*payload);
        s = ud_json_parse(c->ctx, *payload, *length, &doc, NULL);
        if (!s && ud_json_kind(doc) != UD_JSON_OBJECT)
            s = UD_CORRUPT;
        ud_json_destroy(doc);
        if (s == UD_PARSE || s == UD_SCHEMA)
            s = UD_CORRUPT;
    }
    if (!s)
        *kind = (int32_t)type;
    ud_free(tenant);
    ud_bytes_clear(&bytes);
    if (s) {
        ud_free(*id);
        ud_free(*payload);
        *id = NULL;
        *payload = NULL;
    }
    return s;
}
static ud_status normalize(ud_catalog *c, const ud_catalog_key *key, ud_string input, char **json,
                           uint32_t *length) {
    ud_json *document = NULL;
    UD_TRY(ud_json_parse(c->ctx, input.data, input.length, &document, NULL));
    ud_status s = ud_json_kind(document) == UD_JSON_OBJECT ? UD_OK : UD_SCHEMA;
    if (!s && key->kind == UD_CATALOG_PACKAGE) {
        ud_package *p = NULL;
        s = ud_package_create(c->ctx, input, &p);
        ud_package_destroy(p);
        const ud_json *id = ud_json_find(document, "id", 2);
        const char *text = NULL;
        uint32_t n = 0;
        if (!s)
            s = ud_json_text(id, &text, &n);
        if (!s && (n != key->id.length || memcmp(text, key->id.data, n)))
            s = UD_SCHEMA;
    }
    if (!s)
        s = ud_catalog_definition_check(c, key, document);
    if (!s)
        s = ud_json_serialize(document, json, length);
    ud_json_destroy(document);
    return s;
}
ud_status ud_catalog_open(ud_context *ctx, const ud_catalog_options *o, ud_catalog **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !o)
        return UD_INVALID;
    if (o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!string_valid(o->directory, 1) || !string_valid(o->tenant, 1) ||
        o->directory.length > 32767 || o->tenant.length > 255 || o->create_directory < 0 ||
        o->create_directory > 1)
        return UD_INVALID;
#ifdef _WIN32
    if (o->directory.length < 3 || o->directory.data[1] != ':')
        return UD_INVALID;
#else
    if (o->directory.data[0] != '/')
        return UD_INVALID;
#endif
    ud_catalog *c = ud_alloc(ctx, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    c->ctx = ctx;
    c->options = *o;
    c->directory = ud_strdup_n(ctx, o->directory.data, o->directory.length);
    c->tenant = ud_strdup_n(ctx, o->tenant.data, o->tenant.length);
    if (!c->directory || !c->tenant) {
        ud_catalog_close(c);
        return UD_RESOURCE;
    }
    c->options.directory = (ud_string){c->directory, o->directory.length};
    c->options.tenant = (ud_string){c->tenant, o->tenant.length};
    ud_status s = grant(c, UD_ACCESS_OPEN);
    if (!s && o->create_directory) {
        s = grant(c, UD_ACCESS_WRITE);
        if (!s)
            s = ud_mkdir(ctx, c->directory);
    }
    if (s) {
        ud_catalog_close(c);
        return s;
    }
    *out = c;
    return UD_OK;
}
ud_status ud_catalog_get(ud_catalog *c, const ud_catalog_key *key, char **json, uint32_t *length) {
    if (!c || !json || !length)
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(key_check(key));
    UD_TRY(grant(c, UD_ACCESS_READ));
    char *file = NULL;
    UD_TRY(path(c, key, &file));
    char *id = NULL;
    uint64_t revision = 0;
    int32_t kind = 0;
    ud_status s = read_envelope(c, file, key, &kind, &id, &revision, json, length);
    ud_free(file);
    ud_free(id);
    return s;
}
ud_status ud_catalog_put(ud_catalog *c, const ud_catalog_key *key, ud_string input) {
    if (!c)
        return UD_INVALID;
    UD_TRY(key_check(key));
    UD_TRY(grant(c, UD_ACCESS_WRITE));
    char *json = NULL, *file = NULL, *lockfile = NULL, *temporary = NULL;
    uint32_t n = 0;
    void *lock = NULL;
    int temporary_owned = 0;
    ud_status s = normalize(c, key, input, &json, &n);
    if (!s)
        s = path(c, key, &file);
    if (!s)
        s = ud_join_path(c->ctx, c->directory, "catalog.lock", &lockfile);
    if (!s)
        s = ud_lock(c->ctx, lockfile, &lock);
    ud_free(lockfile);
    char *id = NULL, *previous = NULL;
    uint32_t previous_length = 0;
    uint64_t revision = 0;
    int32_t kind = 0;
    if (!s) {
        ud_status found =
            read_envelope(c, file, key, &kind, &id, &revision, &previous, &previous_length);
        if (found == UD_OK) {
            s = previous_length == n && !memcmp(previous, json, n) ? UD_OK : UD_CORRUPT;
            goto done;
        }
        if (found != UD_END) {
            s = found;
            goto done;
        }
    }
    ud_bytes bytes = {.ctx = c->ctx};
    if (!s)
        s = ud_put_u64(&bytes, UD_CONFIG_VERSION);
    if (!s)
        s = ud_put_u64(&bytes, (uint64_t)key->kind);
    if (!s)
        s = ud_put_string(&bytes, c->tenant, strlen(c->tenant));
    if (!s)
        s = ud_put_string(&bytes, key->id.data, key->id.length);
    if (!s)
        s = ud_put_u64(&bytes, key->revision);
    if (!s)
        s = ud_put_string(&bytes, json, n);
    uint8_t digest[32];
    if (!s) {
        ud_hash(bytes.data, bytes.size, digest);
        s = ud_bytes_put(&bytes, digest, 32);
    }
    if (!s) {
        size_t size = strlen(file) + 6;
        temporary = ud_alloc(c->ctx, size);
        if (!temporary)
            s = UD_RESOURCE;
        else
            snprintf(temporary, size, "%s.part", file);
    }
    if (!s) {
        s = ud_reserve_file(c->ctx, temporary);
        temporary_owned = s == UD_OK;
    }
    if (!s)
        s = ud_write_file(c->ctx, temporary, bytes.data, bytes.size);
    if (!s)
        s = ud_publish(c->ctx, temporary, file);
    ud_bytes_clear(&bytes);
done:
    if (temporary_owned)
        (void)ud_remove_file(c->ctx, temporary);
    ud_free(previous);
    ud_free(id);
    ud_unlock(lock);
    ud_free(json);
    ud_free(file);
    ud_free(temporary);
    return s;
}
ud_status ud_catalog_list(ud_catalog *c, int32_t type, char **json, uint32_t *length) {
    if (!c || !json || !length || !kind_valid(type))
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(grant(c, UD_ACCESS_READ));
    char **names = NULL;
    uint32_t count = 0;
    UD_TRY(ud_list(c->ctx, c->directory, ".udc", &names, &count));
    ud_bytes array = {.ctx = c->ctx};
    ud_status s = ud_bytes_put(&array, "[", 1);
    uint32_t emitted = 0;
    for (uint32_t i = 0; !s && i < count; i++) {
        char *file = NULL, *id = NULL, *payload = NULL;
        uint32_t size;
        uint64_t revision = 0;
        int32_t kind = 0;
        s = ud_join_path(c->ctx, c->directory, names[i], &file);
        if (!s)
            s = read_envelope(c, file, NULL, &kind, &id, &revision, &payload, &size);
        ud_free(file);
        if (s == UD_AUTH) {
            s = UD_OK;
            continue;
        } /* Other tenant namespace is not returned. */
        if (!s && kind == type) {
            if (emitted++)
                s = ud_bytes_put(&array, ",", 1);
            if (!s)
                s = ud_bytes_put(&array, "{\"id\":", 6);
            if (!s)
                s = ud_json_quote(&array, id, (uint32_t)strlen(id));
            char value[64];
            int n =
                snprintf(value, sizeof(value), ",\"revision\":%llu}", (unsigned long long)revision);
            if (!s)
                s = ud_bytes_put(&array, value, (size_t)n);
        }
        ud_free(id);
        ud_free(payload);
    }
    if (!s)
        s = ud_bytes_put(&array, "]", 1);
    if (!s)
        s = ud_bytes_put(&array, "", 1);
    if (!s && array.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s) {
        *json = (char *)array.data;
        *length = (uint32_t)array.size - 1;
        array.data = NULL;
    }
    ud_bytes_clear(&array);
    ud_list_free(names, count);
    return s;
}
ud_status ud_catalog_record_event(ud_catalog *c, const ud_execution_event *event,
                                  uint64_t sequence) {
    if (!c || !event || !sequence)
        return UD_INVALID;
    if (event->struct_size != sizeof(*event) || event->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!string_valid(event->execution_id, 1) || !string_valid(event->package_id, 1))
        return UD_INVALID;
    if (event->state < UD_EXEC_CREATED || event->state > UD_EXEC_RECOVERY_REQUIRED ||
        event->kind < UD_EXEC_EVENT_STATE || event->kind > UD_EXEC_EVENT_TASK_UNCERTAIN)
        return UD_INVALID;
    ud_bytes json = {.ctx = c->ctx};
    ud_status s = ud_bytes_put(&json, "{\"version\":6", 12);
    const char *keys[] = {"packageId", "executionId", "taskId", "pipelineId", "componentId"};
    ud_string values[] = {event->package_id, event->execution_id, event->task_id,
                          event->pipeline_id, event->component_id};
    for (int i = 0; !s && i < 5; i++) {
        if (!string_valid(values[i], 0)) {
            s = UD_INVALID;
            break;
        }
        s = ud_bytes_put(&json, ",", 1);
        if (!s)
            s = ud_json_quote(&json, keys[i], (uint32_t)strlen(keys[i]));
        if (!s)
            s = ud_bytes_put(&json, ":", 1);
        if (!s)
            s = ud_json_quote(&json, values[i].data ? values[i].data : "", values[i].length);
    }
    char numbers[160];
    int n = snprintf(numbers, sizeof(numbers),
                     ",\"state\":%d,\"result\":%d,\"timestampMs\":%llu,\"kind\":%d}", event->state,
                     event->result, (unsigned long long)event->timestamp_ms, event->kind);
    if (!s)
        s = ud_bytes_put(&json, numbers, (size_t)n);
    ud_catalog_key key = {sizeof(key), UD_ABI_VERSION, UD_CATALOG_EVENT, event->execution_id,
                          sequence};
    if (!s && json.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s)
        s = ud_catalog_put(c, &key, (ud_string){(char *)json.data, (uint32_t)json.size});
    ud_bytes_clear(&json);
    return s;
}
void ud_catalog_close(ud_catalog *c) {
    if (c) {
        ud_free(c->directory);
        ud_free(c->tenant);
        ud_free(c);
    }
}
