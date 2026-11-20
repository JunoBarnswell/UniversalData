#include "../engines/package/internal.h"
#include "schema_registry.h"
#include "ud/http.h"

enum {
    SCHEMA_MAX_RESPONSE_BYTES = 16 * 1024 * 1024,
    SCHEMA_CACHE_BYTES = 32 * 1024 * 1024,
    SCHEMA_CACHE_ENTRIES = 64
};

typedef struct registry_entry {
    uint32_t id;
    ud_string schema;
} registry_entry;

struct ud_schema_registry {
    ud_component_context context;
    ud_connection_lease *lease;
    ud_string url, subject;
    uint32_t count, cached_bytes;
    registry_entry entries[SCHEMA_CACHE_ENTRIES];
};

static int registry_text(ud_string text) {
    return text.data && text.length && !memchr(text.data, 0, text.length);
}

static ud_status append_subject(ud_bytes *url, ud_string subject) {
    static const char hex[] = "0123456789ABCDEF";
    for (uint32_t i = 0; i < subject.length; i++) {
        unsigned char ch = (unsigned char)subject.data[i];
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
            (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '~' ||
            (ch == '.' && !ud_named(subject, ".") && !ud_named(subject, ".."))) {
            UD_TRY(ud_bytes_put(url, &ch, 1));
        } else {
            char escaped[] = {'%', hex[ch >> 4], hex[ch & 15]};
            UD_TRY(ud_bytes_put(url, escaped, sizeof(escaped)));
        }
    }
    return UD_OK;
}

static ud_status registry_url(ud_schema_registry *registry, uint32_t number, int by_id,
                              ud_bytes *url) {
    ud_string base = registry->url;
    while (base.length && base.data[base.length - 1] == '/')
        base.length--;
    UD_TRY(ud_bytes_put(url, base.data, base.length));
    if (by_id) {
        UD_TRY(ud_bytes_put(url, "/schemas/ids/", sizeof("/schemas/ids/") - 1));
    } else {
        UD_TRY(ud_bytes_put(url, "/subjects/", sizeof("/subjects/") - 1));
        UD_TRY(append_subject(url, registry->subject));
        UD_TRY(ud_bytes_put(url, "/versions/", sizeof("/versions/") - 1));
    }
    char suffix[48];
    int length = snprintf(suffix, sizeof(suffix), "%u?format=resolved", (unsigned)number);
    if (length < 0 || (size_t)length >= sizeof(suffix))
        return UD_OVERFLOW;
    UD_TRY(ud_bytes_put(url, suffix, (size_t)length));
    if (by_id) {
        UD_TRY(ud_bytes_put(url, "&subject=", sizeof("&subject=") - 1));
        UD_TRY(append_subject(url, registry->subject));
    }
    return UD_OK;
}

static ud_status registry_response(ud_schema_registry *registry, uint32_t number, int by_id,
                                   ud_json **out) {
    ud_bytes url = {.ctx = registry->context.memory, .limit = 65536};
    ud_status status = registry_url(registry, number, by_id, &url);
    ud_http_response response = {0};
    if (!status) {
        ud_http_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .method = {"GET", 3},
                                   .url = {(const char *)url.data, (uint32_t)url.size},
                                   .timeout = 10000,
                                   .maximum = SCHEMA_MAX_RESPONSE_BYTES,
                                   .attempts = 1,
                                   .expected = 200};
        status = ud_http_call(&registry->context, registry->lease, &options, &response);
    }
    ud_bytes_clear(&url);
    if (!status || (status == UD_HTTP_STATUS && response.completed && response.status == 404)) {
        ud_json *document = NULL;
        ud_status parsed = ud_json_parse(registry->context.memory, response.body.data,
                                         response.body.length, &document, NULL);
        if (!status) {
            status = parsed == UD_PARSE ? UD_IO : parsed;
            if (!status) {
                *out = document;
                document = NULL;
            }
        } else if (!parsed) {
            uint64_t error = 0;
            if (!ud_json_uint(ud_member(document, "error_code"), &error)) {
                if (by_id && error == 40403)
                    status = UD_QUALITY;
                else if (!by_id && (error == 40401 || error == 40402))
                    status = UD_SCHEMA;
            }
        }
        ud_json_destroy(document);
    } else if (status == UD_HTTP_STATUS && response.completed &&
               (response.status == 429 || response.status == 500 || response.status == 502 ||
                response.status == 503 || response.status == 504)) {
        status = UD_TRANSIENT;
    }
    ud_http_response_clear(&response);
    return status;
}

static ud_status fetch_schema(ud_schema_registry *registry, uint32_t number, int by_id,
                              registry_entry *entry) {
    ud_json *document = NULL;
    ud_status status = registry_response(registry, number, by_id, &document);
    uint64_t id = number;
    if (!status) {
        const ud_json *schema_type = ud_member(document, "schemaType");
        if (schema_type) {
            ud_string type = {0};
            if (ud_json_string(schema_type, &type))
                status = UD_IO;
            else if (!ud_named(type, "AVRO"))
                status = by_id ? UD_QUALITY : UD_UNSUPPORTED;
        }
    }
    if (!status && !by_id) {
        ud_string subject = {0};
        uint64_t version = 0;
        if (ud_json_string(ud_member(document, "subject"), &subject) ||
            subject.length != registry->subject.length ||
            memcmp(subject.data, registry->subject.data, subject.length) ||
            ud_json_uint(ud_member(document, "version"), &version) || version != number)
            status = UD_IO;
    }
    if (!status && (!by_id || ud_member(document, "id")) &&
        (ud_json_uint(ud_member(document, "id"), &id) || !id || id > INT32_MAX ||
         (by_id && id != number)))
        status = UD_IO;
    if (!status) {
        ud_string schema = {0};
        if (ud_json_string(ud_member(document, "schema"), &schema) || !registry_text(schema))
            status = UD_IO;
        else if (schema.length > SCHEMA_CACHE_BYTES - registry->cached_bytes)
            status = UD_RESOURCE;
        else {
            char *owned = ud_strdup_n(registry->context.memory, schema.data, schema.length);
            if (!owned)
                status = UD_RESOURCE;
            else {
                *entry = (registry_entry){(uint32_t)id, {owned, schema.length}};
                registry->cached_bytes += schema.length;
            }
        }
    }
    ud_json_destroy(document);
    return status;
}

ud_status ud_schema_registry_open(const ud_component_context *context, ud_string connection,
                                   ud_string subject, uint32_t version, ud_schema_registry **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!context || !registry_text(connection) || !registry_text(subject) || !version ||
        version > INT32_MAX)
        return UD_INVALID;
    ud_schema_registry *registry = ud_alloc(context->memory, sizeof(*registry));
    if (!registry)
        return UD_RESOURCE;
    registry->context = *context;
    char *owned_subject = ud_strdup_n(context->memory, subject.data, subject.length);
    ud_status status = owned_subject ? UD_OK : UD_RESOURCE;
    registry->subject = (ud_string){owned_subject, subject.length};
    if (!status)
        status = ud_acquire_http_connection(context->activation, connection, &registry->lease);
    if (!status)
        status = ud_http_connection_resource(registry->lease, &registry->url);
    if (!status && (!registry_text(registry->url) ||
                    memchr(registry->url.data, '?', registry->url.length) ||
                    memchr(registry->url.data, '#', registry->url.length)))
        status = UD_SCHEMA;
    if (!status)
        status = fetch_schema(registry, version, 0, registry->entries);
    if (status) {
        ud_schema_registry_close(registry);
        return status;
    }
    registry->count = 1;
    *out = registry;
    return UD_OK;
}

uint32_t ud_schema_registry_id(const ud_schema_registry *registry) {
    return registry->entries[0].id;
}

ud_string ud_schema_registry_schema(const ud_schema_registry *registry) {
    return registry->entries[0].schema;
}

ud_status ud_schema_registry_find(ud_schema_registry *registry, uint32_t writer_id,
                                  ud_string *schema) {
    *schema = (ud_string){0};
    if (!writer_id || writer_id > INT32_MAX)
        return UD_QUALITY;
    for (uint32_t i = 0; i < registry->count; i++) {
        if (registry->entries[i].id == writer_id) {
            UD_TRY(ud_connection_lease_authorize(registry->lease, UD_ACCESS_NETWORK));
            *schema = registry->entries[i].schema;
            return UD_OK;
        }
    }
    if (registry->count == SCHEMA_CACHE_ENTRIES)
        return UD_RESOURCE;
    registry_entry *entry = registry->entries + registry->count;
    UD_TRY(fetch_schema(registry, writer_id, 1, entry));
    registry->count++;
    *schema = entry->schema;
    return UD_OK;
}

void ud_schema_registry_close(ud_schema_registry *registry) {
    if (!registry)
        return;
    for (uint32_t i = 0; i < registry->count; i++)
        ud_free((void *)registry->entries[i].schema.data);
    (void)ud_connection_lease_release(registry->lease);
    ud_free((void *)registry->subject.data);
    ud_free(registry);
}
