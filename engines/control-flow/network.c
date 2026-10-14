#include "../package/file_generation.h"
#include "../../connectors/network.h"
#include <time.h>
typedef struct network_config {
    ud_string connection, file, ca, sender, subject, body, status_variable, bytes_variable;
    uint64_t timeout, maximum;
    int mode, overwrite, require_tls;
} network_config;
static int flag(const ud_json *value, int fallback) {
    const char *text;
    uint32_t length;
    return value ? !ud_json_text(value, &text, &length) && length == 4 : fallback;
}
static ud_status email(ud_string value) {
    if (!value.length || value.length > 320 || !memchr(value.data, '@', value.length))
        return UD_SCHEMA;
    for (uint32_t at = 0; at < value.length; at++)
        if ((uint8_t)value.data[at] <= 32 || (uint8_t)value.data[at] >= 127 ||
            strchr("<>(),;:\\\"", value.data[at]))
            return UD_SCHEMA;
    return UD_OK;
}
static const char *driver_name(int protocol) {
    static const char *names[] = {"", "ftp", "sftp", "smtp", "azureBlob", "adlsGen2", "s3"};
    return names[protocol];
}
static ud_status read_config(const ud_component_context *context, int kind, ud_string text,
                             ud_json **out, network_config *config) {
    int protocol = kind >> 1, sending = kind & 1;
    *config = (network_config){.timeout = 60000, .maximum = UINT64_C(1073741824), .require_tls = 1};
    UD_TRY(ud_json_parse(context->memory, text.data, text.length, out, NULL));
    static const char *keys[] = {"connection",     "file",         "caConnection",  "timeoutMs",
                                 "maxBytes",       "fileMode",     "replaceRemote", "requireTls",
                                 "from",           "to",           "subject",       "body",
                                 "statusVariable", "bytesVariable"};
    UD_TRY(ud_json_keys(*out, keys, 14));
    UD_TRY(ud_json_string(ud_member(*out, "connection"), &config->connection));
    if (ud_member(*out, "caConnection"))
        UD_TRY(ud_json_string(ud_member(*out, "caConnection"), &config->ca));
    if (ud_member(*out, "timeoutMs"))
        UD_TRY(ud_json_uint(ud_member(*out, "timeoutMs"), &config->timeout));
    if (ud_member(*out, "maxBytes"))
        UD_TRY(ud_json_uint(ud_member(*out, "maxBytes"), &config->maximum));
    if (!config->timeout || config->timeout > 86400000 || !config->maximum ||
        config->maximum > INT64_MAX)
        return UD_SCHEMA;
    config->overwrite = flag(ud_member(*out, "replaceRemote"), 0);
    config->require_tls = flag(ud_member(*out, "requireTls"), 1);
    if (protocol == UD_NETWORK_SMTP) {
        UD_TRY(ud_json_string(ud_member(*out, "from"), &config->sender));
        UD_TRY(email(config->sender));
        UD_TRY(ud_json_string(ud_member(*out, "subject"), &config->subject));
        UD_TRY(ud_json_string(ud_member(*out, "body"), &config->body));
        if (config->subject.length > 998 || config->body.length > 16777216 ||
            memchr(config->subject.data, 0, config->subject.length) ||
            memchr(config->subject.data, '\r', config->subject.length) ||
            memchr(config->subject.data, '\n', config->subject.length))
            return UD_SCHEMA;
        const ud_json *recipients = ud_member(*out, "to");
        if (ud_json_kind(recipients) != UD_JSON_ARRAY || !ud_json_size(recipients) ||
            ud_json_size(recipients) > 100)
            return UD_SCHEMA;
        for (uint32_t at = 0; at < ud_json_size(recipients); at++) {
            ud_string recipient;
            UD_TRY(ud_json_string(ud_json_at(recipients, at), &recipient));
            UD_TRY(email(recipient));
        }
        if (ud_member(*out, "file") || ud_member(*out, "fileMode") ||
            ud_member(*out, "replaceRemote"))
            return UD_SCHEMA;
    } else {
        UD_TRY(ud_json_string(ud_member(*out, "file"), &config->file));
        if (ud_member(*out, "from") || ud_member(*out, "to") || ud_member(*out, "subject") ||
            ud_member(*out, "body"))
            return UD_SCHEMA;
        ud_string mode = {"create", 6};
        if (ud_member(*out, "fileMode"))
            UD_TRY(ud_json_string(ud_member(*out, "fileMode"), &mode));
        config->mode = ud_named(mode, "create")      ? UD_FILE_CREATE
                       : ud_named(mode, "overwrite") ? UD_FILE_OVERWRITE
                                                     : -1;
        if (config->mode < 0 || (sending && ud_member(*out, "fileMode")) ||
            (!sending && ud_member(*out, "replaceRemote")))
            return UD_SCHEMA;
        /* FTP/SFTP have no portable exclusive-create verb. Explicit replacement
         * is required before an upload rather than claiming an atomic check. */
        if (sending && protocol <= UD_NETWORK_SFTP && !config->overwrite)
            return UD_UNSUPPORTED;
    }
    ud_string ids[] = {config->connection, config->file, config->ca};
    for (uint32_t at = 0; at < 3; at++) {
        if (!ids[at].length)
            continue;
        const ud_json *definition;
        int owner;
        UD_TRY(ud_connection_definition(context->activation, ids[at], &definition, &owner));
        const ud_json *value = ud_member(definition, "driver");
        if (ud_json_kind(value) == UD_JSON_STRING) {
            ud_string driver;
            UD_TRY(ud_json_string(value, &driver));
            if (!ud_named(driver, at ? "file" : driver_name(protocol)))
                return UD_UNSUPPORTED;
        }
    }
    const char *variable_names[] = {"statusVariable", "bytesVariable"};
    ud_string *variables[] = {&config->status_variable, &config->bytes_variable};
    for (uint32_t at = 0; at < 2; at++) {
        if (!ud_member(*out, variable_names[at]))
            continue;
        UD_TRY(ud_json_string(ud_member(*out, variable_names[at]), variables[at]));
        ud_variable *variable;
        UD_TRY(ud_variable_writable(context->activation, *variables[at], &variable));
        if (variable->value.type != (at ? UD_UINT64 : UD_INT64))
            return UD_SCHEMA;
    }
#ifndef UD_WITH_NETWORK
    return UD_UNSUPPORTED;
#else
    return UD_OK;
#endif
}
static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    (void)inputs;
    (void)outputs;
    ud_json *document = NULL;
    network_config config;
    ud_status status = read_config(context, (int)(intptr_t)user, text, &document, &config);
    ud_json_destroy(document);
    return status;
}
static ud_status base64(ud_context *memory, ud_string value, ud_bytes *output, int wrap) {
    (void)memory;
    return ud_base64_encode(output, (const uint8_t *)value.data, value.length, wrap ? 76 : 0);
}
static ud_status message(const ud_component_context *context, const network_config *config,
                         const ud_json *document, ud_bytes *output) {
    UD_TRY(ud_bytes_put(output, "From: ", 6));
    UD_TRY(ud_bytes_put(output, config->sender.data, config->sender.length));
    UD_TRY(ud_bytes_put(output, "\r\nTo: ", 6));
    const ud_json *recipients = ud_member(document, "to");
    for (uint32_t at = 0; at < ud_json_size(recipients); at++) {
        ud_string recipient;
        UD_TRY(ud_json_string(ud_json_at(recipients, at), &recipient));
        if (at)
            UD_TRY(ud_bytes_put(output, ", ", 2));
        UD_TRY(ud_bytes_put(output, recipient.data, recipient.length));
    }
    UD_TRY(ud_bytes_put(output, "\r\nSubject: ", 11));
    for (uint32_t at = 0; at < config->subject.length;) {
        uint32_t count = config->subject.length - at;
        if (count > 45) {
            count = 45;
            while (count && ((uint8_t)config->subject.data[at + count] & 0xc0) == 0x80)
                count--;
        }
        if (at)
            UD_TRY(ud_bytes_put(output, "\r\n ", 3));
        UD_TRY(ud_bytes_put(output, "=?UTF-8?B?", 10));
        UD_TRY(base64(context->memory, (ud_string){config->subject.data + at, count}, output, 0));
        UD_TRY(ud_bytes_put(output, "?=", 2));
        at += count;
    }
    UD_TRY(ud_bytes_put(output, "\r\nDate: ", 8));
    time_t now = time(NULL);
    struct tm calendar;
#ifdef _WIN32
    if (gmtime_s(&calendar, &now))
        return UD_IO;
#else
    if (!gmtime_r(&now, &calendar))
        return UD_IO;
#endif
    /* RFC 5322 month/day names are fixed English, independent of process locale. */
    const char *days[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    char date[96];
    int size =
        snprintf(date, sizeof(date), "%s, %02d %s %04d %02d:%02d:%02d +0000\r\n",
                 days[calendar.tm_wday], calendar.tm_mday, months[calendar.tm_mon],
                 calendar.tm_year + 1900, calendar.tm_hour, calendar.tm_min, calendar.tm_sec);
    UD_TRY(ud_bytes_put(output, date, (size_t)size));
    static const char mime[] = "MIME-Version: 1.0\r\nContent-Type: text/plain; "
                               "charset=UTF-8\r\nContent-Transfer-Encoding: base64\r\n\r\n";
    UD_TRY(ud_bytes_put(output, mime, sizeof(mime) - 1));
    return base64(context->memory, config->body, output, 1);
}
static ud_status invoke(void *user, const ud_component_context *context, ud_string text) {
    int kind = (int)(intptr_t)user;
    network_config config;
    ud_json *document = NULL;
    ud_status status = read_config(context, kind, text, &document, &config);
    ud_connection_lease *connection = NULL, *file = NULL;
    ud_file_generation *generation = NULL;
    char *path = NULL;
    int sending = kind & 1, protocol = kind >> 1;
    if (!status && sending && ud_transaction_active(context->activation))
        status = UD_UNSUPPORTED;
    if (!status)
        status = ud_activation_acquire_grant(context->activation, config.connection, &connection);
    if (!status && config.file.length)
        status = ud_acquire_file_connection(context->activation, config.file,
                                            sending ? UD_CONNECT_READ : UD_CONNECT_APPEND, &file);
    if (!status && file) {
        ud_string resource;
        status = ud_connection_lease_path(file, &resource);
        if (!status)
            path = ud_strdup_n(context->memory, resource.data, resource.length);
        if (!status && !path)
            status = UD_RESOURCE;
        if (!status && !sending)
            status = ud_file_generation_open(context->activation, file, context->component_id, path,
                                             config.mode, &generation);
        if (!status && sending)
            status = ud_checkpoint_source(context->activation, path);
    }
    ud_bytes mail = {.ctx = context->memory, .limit = 33554432};
    if (!status && protocol == UD_NETWORK_SMTP)
        status = message(context, &config, document, &mail);
    ud_network_options options = {.protocol = protocol,
                                  .upload = sending,
                                  .overwrite = config.overwrite,
                                  .require_tls = config.require_tls,
                                  .path = generation ? generation->temporary : path,
                                  .file = file,
                                  .content_type = {"application/octet-stream", 24},
                                  .ca = config.ca,
                                  .sender = config.sender,
                                  .message = {(const char *)mail.data, (uint32_t)mail.size},
                                  .recipients = ud_member(document, "to"),
                                  .timeout = config.timeout,
                                  .maximum = config.maximum};
    ud_network_result result = {0};
    if (!status && (!generation || !generation->prepared))
        status = ud_network_call(context, connection, &options, &result);
    if (!status && generation) {
        status = ud_file_generation_publish(generation);
        if (!status)
            status = ud_checkpoint_file_done(context->activation, generation->target);
    }
    if (!status && config.status_variable.length) {
        ud_value value = {.type = UD_INT64, .integer = result.status};
        status =
            ud_activation_set_variable(context->activation, config.status_variable, &value, NULL);
    }
    if (!status && config.bytes_variable.length) {
        ud_value value = {.type = UD_UINT64, .unsigned_integer = result.bytes};
        status =
            ud_activation_set_variable(context->activation, config.bytes_variable, &value, NULL);
    }
    ud_bytes_clear(&mail);
    ud_free(path);
    ud_file_generation_destroy(generation);
    ud_status closed = ud_connection_lease_release(file);
    if (!status)
        status = closed;
    closed = ud_connection_lease_release(connection);
    if (!status)
        status = closed;
    ud_json_destroy(document);
    return status;
}
ud_status ud_network_tasks_register(ud_registry *registry) {
#ifndef UD_WITH_NETWORK
    (void)registry;
    return UD_OK;
#else
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"connection\"],"
        "\"properties\":{"
        "\"connection\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},"
        "\"file\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},\"caConnection\":{"
        "\"type\":\"string\"},"
        "\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000},"
        "\"maxBytes\":{\"type\":\"integer\",\"minimum\":1},\"fileMode\":{\"enum\":[\"create\","
        "\"overwrite\"]},"
        "\"replaceRemote\":{\"type\":\"boolean\"},\"requireTls\":{\"type\":\"boolean\"},"
        "\"from\":{\"type\":\"string\"},\"to\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}"
        ","
        "\"subject\":{\"type\":\"string\"},\"body\":{\"type\":\"string\"},"
        "\"statusVariable\":{\"type\":\"string\"},\"bytesVariable\":{\"type\":\"string\"}}}";
    const struct {
        const char *id;
        int protocol, upload;
    } entries[] = {{"ftp.download", UD_NETWORK_FTP, 0},
                   {"ftp.upload", UD_NETWORK_FTP, 1},
                   {"sftp.download", UD_NETWORK_SFTP, 0},
                   {"sftp.upload", UD_NETWORK_SFTP, 1},
                   {"smtp.send", UD_NETWORK_SMTP, 1},
                   {"azureBlob.download", UD_NETWORK_AZURE_BLOB, 0},
                   {"azureBlob.upload", UD_NETWORK_AZURE_BLOB, 1},
                   {"adlsGen2.download", UD_NETWORK_ADLS, 0},
                   {"adlsGen2.upload", UD_NETWORK_ADLS, 1},
                   {"s3.download", UD_NETWORK_S3, 0},
                   {"s3.upload", UD_NETWORK_S3, 1}};
    for (uint32_t at = 0; at < sizeof(entries) / sizeof(*entries); at++) {
        ud_component_descriptor descriptor = {
            .struct_size = sizeof(descriptor),
            .abi_version = UD_ABI_VERSION,
            .id = {entries[at].id, (uint32_t)strlen(entries[at].id)},
            .name = {entries[at].id, (uint32_t)strlen(entries[at].id)},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {schema, sizeof(schema) - 1},
            .category = UD_COMPONENT_TASK,
            .synchronous_input = -1,
            .error_output = -1,
            .user = (void *)(intptr_t)((entries[at].protocol << 1) | entries[at].upload),
            .capabilities =
                UD_COMPONENT_THREAD_SAFE | (entries[at].upload ? 0 : UD_COMPONENT_REPLAY_SAFE),
            .effects =
                UD_EFFECT_EXTERNAL_READ | UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_COMMIT_UNKNOWN,
            .configure = configure,
            .invoke = invoke};
        UD_TRY(ud_registry_register(registry, &descriptor));
    }
    return UD_OK;
#endif
}
