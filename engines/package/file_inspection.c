#include "internal.h"
#include "ud/connection_manager.h"
#include "ud/csv.h"
#include "ud/xlsx.h"
#include "ud/plugin.h"

ud_status ud_execution_test_message_connection(ud_execution *e, ud_string connection,
                                               uint64_t *capabilities, uint64_t *elapsed_ms) {
    if (!e || !connection.data || !connection.length || !capabilities || !elapsed_ms)
        return UD_INVALID;
    *capabilities = *elapsed_ms = 0;
    if (atomic_load(&e->state) != UD_EXEC_READY)
        return UD_BUSY;
    uint64_t started = ud_now_ms();
    ud_connection_lease *lease = NULL;
    uint64_t needed = UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_PUBLISH;
    ud_status status = ud_activation_acquire_connection(&e->root, connection, needed, &lease);
    if (!status) {
        *capabilities = ud_connection_lease_capabilities(lease);
        status = ud_connection_lease_release(lease);
    }
    *elapsed_ms = ud_now_ms() - started;
    return status;
}

ud_status ud_execution_connector_catalog(ud_execution *e, ud_string paths, char **out,
                                         uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (atomic_load(&e->state) != UD_EXEC_READY)
        return UD_BUSY;
    ud_json *request = NULL;
    ud_status s = ud_json_parse(e->ctx, paths.data, paths.length, &request, NULL);
    if (!s && (ud_json_kind(request) != UD_JSON_ARRAY || ud_json_size(request) > 32))
        s = UD_INVALID;
    ud_bytes b = {.ctx = e->ctx, .limit = 262144};
    if (!s)
        s = ud_bytes_put(&b, "[", 1);
    for (uint32_t i = 0; !s && i < ud_json_size(request); i++) {
        ud_string path = {0};
        s = ud_json_string(ud_json_at(request, i), &path);
        if (s)
            break;
        ud_status status = e->options.authorize
                               ? e->options.authorize(e->options.authorize_user, e->options.tenant,
                                                      path, UD_ACCESS_OPEN)
                               : UD_AUTH;
        ud_plugin *plugin = NULL;
        char *name = NULL;
        if (!status) {
            name = ud_strdup_n(e->ctx, path.data, path.length);
            if (!name)
                status = UD_RESOURCE;
        }
        if (!status)
            status = ud_plugin_load(e->ctx, name, 0, &plugin);
        ud_plugin_api info = {.struct_size = sizeof(info), .abi_version = UD_PLUGIN_ABI};
        if (!status)
            status = ud_plugin_info(plugin, &info);
        if (!status && !info.connector)
            status = UD_UNSUPPORTED;
        if (i)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = ud_bytes_put(&b, "{\"path\":", 8);
        if (!s)
            s = ud_json_quote(&b, path.data, path.length);
        if (!s)
            s = ud_bytes_put(&b, ",\"id\":", 6);
        if (!s)
            s = ud_json_quote(&b, status ? "" : info.id, status ? 0 : (uint32_t)strlen(info.id));
        if (!s)
            s = ud_bytes_put(&b, ",\"version\":", 11);
        if (!s)
            s = ud_json_quote(&b, status ? "" : info.version,
                              status ? 0 : (uint32_t)strlen(info.version));
        char flags[144];
        int n = snprintf(flags, sizeof(flags), ",\"result\":%d,\"capabilities\":%llu}", status,
                         (unsigned long long)(status ? 0 : info.connector->capabilities));
        if (!s)
            s = ud_bytes_put(&b, flags, (uint32_t)n);
        if (plugin) {
            ud_status closed = ud_plugin_unload(plugin);
            if (!s)
                s = closed;
        }
        ud_free(name);
    }
    if (!s)
        s = ud_bytes_put(&b, "]", 1);
    if (!s) {
        *out = ud_strdup_n(e->ctx, (const char *)b.data, (uint32_t)b.size);
        if (!*out)
            s = UD_RESOURCE;
        else
            *length = (uint32_t)b.size;
    }
    ud_bytes_clear(&b);
    ud_json_destroy(request);
    return s;
}

static ud_status csv_options(ud_execution *e, const char *path, const ud_json *requested,
                             ud_bytes *options, ud_bytes *metadata) {
    FILE *file = NULL;
    UD_TRY(ud_open_regular(e->ctx, path, &file));
    unsigned char sample[65536];
    size_t size = fread(sample, 1, sizeof(sample), file);
    int failed = ferror(file);
    fclose(file);
    if (failed)
        return UD_IO;
    const char *encoding = "UTF-8";
    uint32_t begin = 0, stride = 1;
    int be = 0, bom = 0;
    if (size >= 2 && sample[0] == 255 && sample[1] == 254) {
        encoding = "UTF-16LE";
        stride = 2;
        begin = 2;
        bom = 1;
    } else if (size >= 2 && sample[0] == 254 && sample[1] == 255) {
        encoding = "UTF-16BE";
        stride = 2;
        begin = 2;
        be = 1;
        bom = 1;
    } else if (size >= 3 && sample[0] == 239 && sample[1] == 187 && sample[2] == 191) {
        begin = 3;
        bom = 1;
    }
    const char separators[] = {',', '\t', ';', '|'};
    uint32_t counts[4][32] = {{0}}, row = 0;
    int quoted = 0;
    for (size_t at = begin; at + stride <= size && row < 32; at += stride) {
        unsigned c = sample[at + (be ? 1 : 0)];
        if (stride == 2 && sample[at + (be ? 0 : 1)])
            continue;
        if (c == '"')
            quoted = !quoted;
        if (!quoted && c == '\n')
            row++;
        for (uint32_t d = 0; d < 4; d++)
            if (!quoted && c == (unsigned)separators[d])
                counts[d][row]++;
    }
    uint32_t best = 0, score = 0, winners = 0;
    for (uint32_t d = 0; d < 4; d++) {
        uint32_t n = 0;
        if (counts[d][0])
            for (uint32_t r = 0; r < (row ? row : 1); r++)
                if (counts[d][r] == counts[d][0])
                    n++;
        if (n > score) {
            score = n;
            best = d;
            winners = 1;
        } else if (n && n == score)
            winners++;
    }
    const char *prefix = "{\"encoding\":";
    UD_TRY(ud_bytes_put(options, prefix, strlen(prefix)));
    const ud_json *enc = ud_member(requested, "encoding"),
                  *delimiter = ud_member(requested, "delimiter");
    char *text = NULL;
    uint32_t n = 0;
    ud_status s = UD_OK;
    if (enc)
        s = ud_json_serialize(enc, &text, &n);
    else {
        s = ud_json_quote(options, encoding, (uint32_t)strlen(encoding));
    }
    if (!s && enc)
        s = ud_bytes_put(options, text, n);
    ud_free(text);
    text = NULL;
    if (!s)
        s = ud_bytes_put(options, ",\"delimiter\":", sizeof(",\"delimiter\":") - 1);
    if (!s && delimiter)
        s = ud_json_serialize(delimiter, &text, &n);
    if (!s && delimiter)
        s = ud_bytes_put(options, text, n);
    else if (!s)
        s = ud_json_quote(options, separators + best, 1);
    ud_free(text);
    for (uint32_t i = 0; !s && i < ud_json_size(requested); i++) {
        const char *key = NULL;
        uint32_t len = 0;
        s = ud_json_key(requested, i, &key, &len);
        if (!s && ((len == 8 && !memcmp(key, "encoding", 8)) ||
                   (len == 9 && !memcmp(key, "delimiter", 9))))
            continue;
        if (!s)
            s = ud_bytes_put(options, ",", sizeof(",") - 1);
        if (!s)
            s = ud_json_quote(options, key, len);
        if (!s)
            s = ud_bytes_put(options, ":", sizeof(":") - 1);
        text = NULL;
        if (!s)
            s = ud_json_serialize(ud_json_find(requested, key, len), &text, &n);
        if (!s)
            s = ud_bytes_put(options, text, n);
        ud_free(text);
    }
    if (!s)
        s = ud_bytes_put(options, "}", sizeof("}") - 1);
    char flags[220];
    n = (uint32_t)snprintf(flags, sizeof(flags),
                           "{\"format\":\"csv\",\"sampleRows\":256,\"bom\":%s,"
                           "\"delimiterAmbiguous\":%s,\"headerConfirmed\":false,\"options\":",
                           bom ? "true" : "false",
                           !delimiter && (!score || winners != 1) ? "true" : "false");
    if (!s)
        s = ud_bytes_put(metadata, flags, n);
    if (!s)
        s = ud_bytes_put(metadata, options->data, options->size);
    if (!s)
        s = ud_bytes_put(metadata, "}", sizeof("}") - 1);
    return s;
}
ud_status ud_execution_inspect_file(ud_execution *e, ud_string connection, ud_string config,
                                    char **out, uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (atomic_load(&e->state) != UD_EXEC_READY)
        return UD_BUSY;
    ud_json *request = NULL;
    ud_connection_lease *lease = NULL;
    ud_batch *schema = NULL;
    char *path = NULL, *options_text = NULL;
    uint32_t options_length = 0;
    ud_status s = ud_json_parse(e->ctx, config.data, config.length, &request, NULL);
    ud_string format = {0}, resource = {0};
    if (!s)
        s = ud_json_string(ud_member(request, "format"), &format);
    if (!s)
        s = ud_acquire_file_connection(&e->root, connection, UD_CONNECT_READ, &lease);
    if (!s)
        s = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
    if (!s)
        s = ud_connection_lease_path(lease, &resource);
    if (!s) {
        path = ud_strdup_n(e->ctx, resource.data, resource.length);
        if (!path)
            s = UD_RESOURCE;
    }
    char scope[33];
    if (!s)
        s = ud_connection_field_namespace(&e->root, connection, format, scope);
    ud_bytes options = {.ctx = e->ctx}, metadata = {.ctx = e->ctx},
             json = {.ctx = e->ctx, .limit = 2097152};
    if (!s && ud_named(format, "csv")) {
        s = csv_options(e, path, ud_member(request, "options"), &options, &metadata);
        ud_csv_options csv = {.struct_size = sizeof(csv),
                              .abi_version = UD_ABI_VERSION,
                              .batch_size = 256,
                              .max_record_bytes = 1048576,
                              .format = {(char *)options.data, (uint32_t)options.size}};
        if (!s)
            s = ud_csv_discover(e->ctx, path, (ud_string){scope, 32}, &csv, 256, &schema);
    } else if (!s && ud_named(format, "xlsx")) {
        const ud_json *opts = ud_member(request, "options");
        if (opts)
            s = ud_json_serialize(opts, &options_text, &options_length);
        char *meta = NULL;
        uint32_t n = 0;
        if (!s)
            s = ud_xlsx_discover(e->ctx, path, (ud_string){scope, 32},
                                 (ud_string){options_text, options_length}, &schema, &meta, &n);
        if (!s)
            s = ud_bytes_put(&metadata, meta, n);
        ud_free(meta);
    } else if (!s)
        s = UD_UNSUPPORTED;
    char *schema_text = NULL;
    uint32_t n = 0;
    if (!s)
        s = ud_schema_to_json(schema, &schema_text, &n);
    if (!s)
        s = ud_bytes_put(&json, "{\"schema\":", sizeof("{\"schema\":") - 1);
    if (!s)
        s = ud_bytes_put(&json, schema_text, n);
    if (!s)
        s = ud_bytes_put(&json, ",\"metadata\":", sizeof(",\"metadata\":") - 1);
    if (!s)
        s = ud_bytes_put(&json, metadata.data, metadata.size);
    if (!s)
        s = ud_bytes_put(&json, "}", sizeof("}") - 1);
    if (!s) {
        *out = ud_strdup_n(e->ctx, (char *)json.data, json.size);
        if (!*out)
            s = UD_RESOURCE;
        else
            *length = (uint32_t)json.size;
    }
    ud_status closed = ud_connection_lease_release(lease);
    if (!s)
        s = closed;
    ud_free(schema_text);
    ud_free(options_text);
    ud_free(path);
    ud_batch_destroy(schema);
    ud_json_destroy(request);
    ud_bytes_clear(&options);
    ud_bytes_clear(&metadata);
    ud_bytes_clear(&json);
    if (s) {
        ud_free(*out);
        *out = NULL;
        *length = 0;
    }
    return s;
}

ud_status ud_execution_metadata(ud_execution *e, ud_string connection, int32_t kind,
                                ud_string parent, uint64_t offset, char **out, uint32_t *length) {
    if (!e || !out || !length || kind < UD_METADATA_CATALOGS || kind > UD_METADATA_INDEXES)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (atomic_load(&e->state) != UD_EXEC_READY)
        return UD_BUSY;
    ud_connection_lease *lease = NULL;
    ud_batch *batch = NULL;
    ud_status s = ud_activation_acquire_connection(&e->root, connection,
                                                   UD_CONNECT_READ | UD_CONNECT_METADATA, &lease);
    ud_metadata_options options = {sizeof(options), UD_ABI_VERSION, kind, parent, offset, 100};
    if (!s)
        s = ud_connection_lease_metadata(lease, &options, &batch);
    if (!s)
        s = ud_batch_to_json(batch, out, length);
    ud_batch_destroy(batch);
    ud_status closed = ud_connection_lease_release(lease);
    if (!s)
        s = closed;
    if (s) {
        ud_free(*out);
        *out = NULL;
        *length = 0;
    }
    return s;
}
