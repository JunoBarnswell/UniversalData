#include "../engines/package/internal.h"
#include "http_internal.h"
#include "network.h"
#ifdef UD_WITH_HTTP
#include <curl/curl.h>
#endif
static int body_method(ud_string method) {
    return ud_named(method, "POST") || ud_named(method, "PUT") || ud_named(method, "PATCH");
}
static int clean_text(ud_string text) {
    for (uint32_t i = 0; i < text.length; i++)
        if ((unsigned char)text.data[i] < 32 || (unsigned char)text.data[i] == 127)
            return 0;
    return 1;
}
#ifdef UD_WITH_HTTP
static int initialized;
static void initialize(void) {
    initialized = curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK;
}
#ifdef _WIN32
static INIT_ONCE once = INIT_ONCE_STATIC_INIT;
static BOOL CALLBACK initialize_once(PINIT_ONCE o, PVOID p, PVOID *context) {
    (void)o;
    (void)p;
    (void)context;
    initialize();
    return TRUE;
}
static void ensure_initialized(void) {
    InitOnceExecuteOnce(&once, initialize_once, NULL, NULL);
}
#else
static pthread_once_t once = PTHREAD_ONCE_INIT;
static void ensure_initialized(void) {
    pthread_once(&once, initialize);
}
#endif
typedef struct transfer {
    ud_activation *execution;
    ud_connection_lease *connection;
    uint64_t deadline, maximum, received, headers;
    ud_status status;
    ud_bytes body, header_bytes;
    uint64_t retry_after;
    const ud_json *response_headers;
    const ud_json *request_header_definitions;
    const struct curl_slist *request_headers;
    uint32_t credential_header_count;
    int32_t transport_code;
    FILE *file;
    ud_connection_lease *file_lease;
    const uint8_t *upload_bytes;
    uint64_t upload_size, uploaded;
    int network_action, file_action;
    ud_connection_lease *trust_lease, *key_lease;
} transfer;
static ud_status check(transfer *t) {
    ud_status s = ud_connection_lease_authorize(t->connection, UD_ACCESS_NETWORK);
    if (!s && t->network_action)
        s = ud_connection_lease_authorize(t->connection, t->network_action);
    if (!s && t->file_lease)
        s = ud_connection_lease_authorize(t->file_lease, t->file_action);
    if (!s && t->trust_lease)
        s = ud_connection_lease_authorize(t->trust_lease, UD_ACCESS_READ);
    if (!s && t->key_lease)
        s = ud_connection_lease_authorize(t->key_lease, UD_ACCESS_READ);
    if (!s && ud_now_ms() >= t->deadline)
        s = UD_TIMEOUT;
    if (s)
        t->status = s;
    return s;
}
static size_t receive(char *ptr, size_t size, size_t count, void *user) {
    transfer *t = user;
    if (size && count > SIZE_MAX / size) {
        t->status = UD_RESOURCE;
        return 0;
    }
    size_t n = size * count;
    if (check(t))
        return 0;
    if (n > t->maximum - t->received) {
        t->status = UD_RESOURCE;
        return 0;
    }
    t->status = t->file && t->file_action == UD_ACCESS_WRITE
                    ? fwrite(ptr, 1, n, t->file) == n ? UD_OK : UD_IO
                    : ud_bytes_put(&t->body, ptr, n);
    if (t->status)
        return 0;
    t->received += n;
    return n;
}
static size_t upload(char *bytes, size_t size, size_t count, void *pointer) {
    transfer *t = pointer;
    if (size && count > SIZE_MAX / size) {
        t->status = UD_RESOURCE;
        return CURL_READFUNC_ABORT;
    }
    if (check(t))
        return CURL_READFUNC_ABORT;
    size_t capacity = size * count;
    if ((uint64_t)capacity > t->upload_size - t->uploaded)
        capacity = (size_t)(t->upload_size - t->uploaded);
    size_t received = 0;
    if (t->file && t->file_action == UD_ACCESS_READ) {
        received = fread(bytes, 1, capacity, t->file);
        if (received != capacity && ferror(t->file)) {
            t->status = UD_IO;
            return CURL_READFUNC_ABORT;
        }
        if (!received && t->uploaded < t->upload_size) {
            t->status = UD_FENCED;
            return CURL_READFUNC_ABORT;
        }
    } else {
        received = capacity;
        if (received)
            memcpy(bytes, t->upload_bytes + t->uploaded, received);
    }
    t->uploaded += received;
    return received;
}

static int selected_header(const ud_json *selection, const char *line, size_t length) {
    const char *end = memchr(line, ':', length);
    if (!end)
        return 0;
    size_t name_length = (size_t)(end - line);
    if (name_length == 10 && curl_strnequal(line, "Set-Cookie", 10))
        return 0;
    if (!selection)
        return (name_length == 12 && curl_strnequal(line, "Content-Type", 12)) ||
               (name_length == 12 && curl_strnequal(line, "X-Request-Id", 12));
    for (uint32_t i = 0; i < ud_json_size(selection); ++i) {
        ud_string name;
        if (!ud_json_string(ud_json_at(selection, i), &name) && name.length == name_length &&
            curl_strnequal(name.data, line, name_length))
            return 1;
    }
    return 0;
}

static int contains_value(const char *text, size_t length, const char *value) {
    size_t size = strlen(value);
    if (!size || size > length)
        return 0;
    for (size_t at = 0; at <= length - size; ++at)
        if (!memcmp(text + at, value, size))
            return 1;
    return 0;
}

static int reflects_credentials(const transfer *t, const char *line, size_t length) {
    const char *separator = memchr(line, ':', length);
    const char *text = separator + 1;
    length -= (size_t)(text - line);
    uint32_t index = 0;
    for (const struct curl_slist *header = t->request_headers; header; header = header->next, ++index) {
        const char *colon = strchr(header->data, ':');
        if (!colon)
            continue;
        size_t name_length = (size_t)(colon - header->data);
        int authorization = name_length == 13 && curl_strnequal(header->data, "Authorization", 13);
        const ud_json *definition = index < t->credential_header_count ? NULL :
            ud_json_at(t->request_header_definitions, index - t->credential_header_count);
        if (!authorization && index >= t->credential_header_count &&
            !ud_member(definition, "credentialReference") && !ud_member(definition, "variable"))
            continue;
        const char *value = colon + 1;
        while (*value == ' ' || *value == '\t')
            ++value;
        if (contains_value(text, length, value))
            return 1;
        const char *token = authorization ? strchr(value, ' ') : NULL;
        if (token && contains_value(text, length, token + 1))
            return 1;
    }
    return 0;
}

static size_t header(char *ptr, size_t size, size_t count, void *user) {
    transfer *t = user;
    if (size && count > SIZE_MAX / size) {
        t->status = UD_RESOURCE;
        return 0;
    }
    size_t n = size * count;
    if (check(t))
        return 0;
    if (n > 65536 - t->headers) {
        t->status = UD_RESOURCE;
        return 0;
    }
    if (n >= 5 && !memcmp(ptr, "HTTP/", 5))
        t->header_bytes.size = 0;
    if (selected_header(t->response_headers, ptr, n) && !reflects_credentials(t, ptr, n)) {
        t->status = ud_bytes_put(&t->header_bytes, ptr, n);
        if (t->status)
            return 0;
    }
    if (n > 12 && curl_strnequal(ptr, "Retry-After:", 12)) {
        uint64_t seconds = 0;
        uint32_t i = 12;
        while (i < n && (ptr[i] == ' ' || ptr[i] == '\t'))
            i++;
        while (i < n && ptr[i] >= '0' && ptr[i] <= '9' && seconds < 3600)
            seconds = seconds * 10 + (unsigned)(ptr[i++] - '0');
        if (seconds <= 3600)
            t->retry_after = seconds * 1000;
    }
    t->headers += n;
    return n;
}
static int progress(void *user, curl_off_t a, curl_off_t b, curl_off_t c, curl_off_t d) {
    (void)a;
    (void)b;
    (void)c;
    (void)d;
    return check(user) != UD_OK;
}
static ud_status validate_url(const char *text) {
    ud_string value = {text, (uint32_t)strlen(text)};
    if (!clean_text(value) || strchr(text, ' '))
        return UD_SCHEMA;
    CURLU *url = curl_url();
    if (!url)
        return UD_RESOURCE;
    ud_status s = curl_url_set(url, CURLUPART_URL, text, 0) ? UD_SCHEMA : UD_OK;
    char *scheme = NULL, *host = NULL;
    if (!s && curl_url_get(url, CURLUPART_SCHEME, &scheme, 0))
        s = UD_SCHEMA;
    if (!s && strcmp(scheme, "https") && strcmp(scheme, "http"))
        s = UD_UNSUPPORTED;
    if (!s && (curl_url_get(url, CURLUPART_HOST, &host, 0) || !*host))
        s = UD_SCHEMA;
    CURLUPart forbidden[] = {CURLUPART_USER, CURLUPART_PASSWORD, CURLUPART_FRAGMENT};
    for (int i = 0; !s && i < 3; i++) {
        char *part = NULL;
        if (!curl_url_get(url, forbidden[i], &part, 0))
            s = UD_SCHEMA;
        curl_free(part);
    }
    curl_free(scheme);
    curl_free(host);
    curl_url_cleanup(url);
    return s;
}
static int named_ci(ud_string s, const char *text) {
    if (s.length != strlen(text))
        return 0;
    for (uint32_t i = 0; i < s.length; i++) {
        unsigned char c = (unsigned char)s.data[i];
        if (c >= 'A' && c <= 'Z')
            c += 'a' - 'A';
        if (c != (unsigned char)text[i])
            return 0;
    }
    return 1;
}
static ud_status add_header(struct curl_slist **list, const char *value) {
    struct curl_slist *next = curl_slist_append(*list, value);
    if (!next)
        return UD_RESOURCE;
    *list = next;
    return UD_OK;
}
static int header_name(ud_string name) {
    if (!name.length || name.length > 128)
        return 0;
    for (uint32_t i = 0; i < name.length; ++i) {
        unsigned char c = (unsigned char)name.data[i];
        if (!c || !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                    (c >= '0' && c <= '9') || strchr("!#$%&'*+-.^_`|~", c)))
            return 0;
    }
    return 1;
}
static int request_header(ud_string name) {
    static const char *reserved[] = {"host", "content-length", "transfer-encoding", "connection",
                                    "expect", "proxy-authorization", "content-type", "idempotency-key"};
    if (!header_name(name))
        return 0;
    for (uint32_t i = 0; i < sizeof(reserved) / sizeof(*reserved); ++i)
        if (named_ci(name, reserved[i]))
            return 0;
    return 1;
}
ud_status ud_http_validate_headers(const ud_json *headers, const ud_json *response) {
    if ((headers && (ud_json_kind(headers) != UD_JSON_ARRAY || ud_json_size(headers) > 64)) ||
        (response && (ud_json_kind(response) != UD_JSON_ARRAY || ud_json_size(response) > 64)))
        return UD_SCHEMA;
    for (uint32_t i = 0; i < ud_json_size(headers); ++i) {
        const ud_json *entry = ud_json_at(headers, i);
        static const char *keys[] = {"name", "value", "variable", "credentialReference"};
        UD_TRY(ud_json_keys(entry, keys, 4));
        ud_string name, value;
        UD_TRY(ud_json_string(ud_member(entry, "name"), &name));
        const ud_json *literal = ud_member(entry, "value");
        const ud_json *variable = ud_member(entry, "variable");
        const ud_json *credential = ud_member(entry, "credentialReference");
        if (!request_header(name) || !!literal + !!variable + !!credential != 1 ||
            (named_ci(name, "authorization") && !credential))
            return UD_SCHEMA;
        UD_TRY(ud_json_string(literal ? literal : variable ? variable : credential, &value));
        if (value.length > 8192 || !clean_text(value))
            return UD_SCHEMA;
    }
    for (uint32_t i = 0; i < ud_json_size(response); ++i) {
        ud_string name;
        UD_TRY(ud_json_string(ud_json_at(response, i), &name));
        if (!header_name(name) || named_ci(name, "set-cookie") || named_ci(name, "authorization") ||
            named_ci(name, "proxy-authorization") || named_ci(name, "cookie"))
            return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status header_value(ud_context *memory, struct curl_slist **list, ud_string name,
                              ud_string value) {
    if (!request_header(name) || value.length > 16384 || !clean_text(value))
        return UD_SCHEMA;
    for (const struct curl_slist *entry = *list; entry; entry = entry->next) {
        const char *separator = strchr(entry->data, ':');
        if (separator && (size_t)(separator - entry->data) == name.length &&
            curl_strnequal(entry->data, name.data, name.length))
            return UD_SCHEMA;
    }
    ud_bytes line = {.ctx = memory, .limit = 32768};
    ud_status status = ud_bytes_put(&line, name.data, name.length);
    if (!status)
        status = ud_bytes_put(&line, ": ", 2);
    if (!status)
        status = ud_bytes_put(&line, value.data, value.length);
    if (!status)
        status = ud_bytes_put(&line, "", 1);
    if (!status)
        status = add_header(list, (char *)line.data);
    ud_string secret = {(const char *)line.data, (uint32_t)line.size};
    ud_http_clear_secret(&secret);
    return status;
}
static ud_status credentials(ud_context *memory, ud_string text, struct curl_slist **list,
                             uint32_t *count) {
    if (!text.length)
        return UD_OK;
    ud_json *document = NULL;
    UD_TRY(ud_json_parse(memory, text.data, text.length, &document, NULL));
    static const char *keys[] = {"headers"};
    ud_status status = ud_json_keys(document, keys, 1);
    const ud_json *headers = ud_member(document, "headers");
    if (!status && (ud_json_kind(headers) != UD_JSON_OBJECT || ud_json_size(headers) > 64))
        status = UD_SCHEMA;
    if (!status)
        *count = ud_json_size(headers);
    for (uint32_t i = 0; !status && i < *count; ++i) {
        ud_string name, value;
        status = ud_json_key(headers, i, &name.data, &name.length);
        if (!status)
            status = ud_json_string(ud_json_at(headers, i), &value);
        if (!status)
            status = header_value(memory, list, name, value);
    }
    ud_json_destroy(document);
    return status;
}
static ud_status request_headers(const ud_component_context *context, ud_connection_lease *lease,
                                 const ud_http_options *request, ud_string resource,
                                 struct curl_slist **list) {
    for (uint32_t i = 0; i < ud_json_size(request->headers); ++i) {
        const ud_json *entry = ud_json_at(request->headers, i);
        ud_string name, value = {0}, secret = {0};
        UD_TRY(ud_json_string(ud_member(entry, "name"), &name));
        const ud_json *literal = ud_member(entry, "value");
        const ud_json *reference = ud_member(entry, "credentialReference");
        char buffer[256];
        if (literal)
            UD_TRY(ud_json_string(literal, &value));
        else if (reference) {
            ud_string id;
            UD_TRY(ud_json_string(reference, &id));
            UD_TRY(ud_http_resolve_reference(lease, resource, id, &secret));
            value = secret;
        } else {
            ud_string variable;
            ud_value data;
            UD_TRY(ud_json_string(ud_member(entry, "variable"), &variable));
            UD_TRY(ud_activation_variable(context->activation, variable, &data));
            if (data.is_null || data.type == UD_BINARY)
                return UD_SCHEMA;
            if (data.type == UD_UTF8)
                value = (ud_string){data.text, data.text_length};
            else {
                uint32_t length = 0;
                UD_TRY(ud_value_format(&data, buffer, sizeof(buffer), &length));
                value = (ud_string){buffer, length};
            }
        }
        ud_status status = header_value(context->memory, list, name, value);
        ud_http_clear_secret(&secret);
        if (status)
            return status;
    }
    return UD_OK;
}
static void clear_headers(struct curl_slist *headers) {
    for (struct curl_slist *entry = headers; entry; entry = entry->next) {
        volatile char *value = entry->data;
        while (*value)
            *value++ = 0;
    }
    curl_slist_free_all(headers);
}
static ud_status load_ca(ud_activation *e, ud_string id, ud_bytes *bytes) {
    if (!id.length)
        return UD_OK;
    ud_connection_lease *lease = NULL;
    ud_status s = ud_acquire_file_connection(e, id, UD_CONNECT_READ, &lease);
    ud_string path = {0};
    if (!s)
        s = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
    if (!s)
        s = ud_connection_lease_path(lease, &path);
    char *filename = !s ? ud_strdup_n(e->execution->ctx, path.data, path.length) : NULL;
    if (!s && !filename)
        s = UD_RESOURCE;
    if (!s)
        s = ud_path_no_symlinks(e->execution->ctx, filename);
    FILE *file = NULL;
    if (!s)
        s = ud_open_regular(e->execution->ctx, filename, &file);
    uint8_t buffer[4096];
    while (!s && file) {
        s = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
        if (s)
            break;
        size_t n = fread(buffer, 1, sizeof(buffer), file);
        if (n)
            s = ud_bytes_put(bytes, buffer, n);
        if (n < sizeof(buffer)) {
            if (ferror(file))
                s = UD_IO;
            break;
        }
    }
    if (file)
        fclose(file);
    if (!s && !bytes->size)
        s = UD_SCHEMA;
    ud_free(filename);
    ud_status closed = ud_connection_lease_release(lease);
    return s ? s : closed;
}
static ud_status perform(CURL *easy, transfer *t, long *status) {
    CURLM *multi = curl_multi_init();
    if (!multi)
        return UD_RESOURCE;
    ud_status s = curl_multi_add_handle(multi, easy) == CURLM_OK ? UD_OK : UD_IO;
    int added = !s, running = 1;
    CURLcode result = CURLE_FAILED_INIT;
    int received = 0;
    while (!s && running) {
        s = check(t);
        if (!s && curl_multi_perform(multi, &running) != CURLM_OK)
            s = UD_IO;
        if (!s && running && curl_multi_poll(multi, NULL, 0, 50, NULL) != CURLM_OK)
            s = UD_IO;
    }
    CURLMsg *message;
    int remaining;
    while ((message = curl_multi_info_read(multi, &remaining)))
        if (message->msg == CURLMSG_DONE && message->easy_handle == easy) {
            received = 1;
            result = message->data.result;
        }
    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, status);
    if (added)
        curl_multi_remove_handle(multi, easy);
    curl_multi_cleanup(multi);
    t->transport_code = (int32_t)result;
    if (t->status)
        s = t->status;
    if (!s && (!received || result != CURLE_OK))
        s = result == CURLE_OPERATION_TIMEDOUT ? UD_TIMEOUT : UD_IO;
    return s;
}
static ud_status configure_transport(CURL *easy, transfer *t, const char *url,
                                     const char *protocols, uint64_t timeout) {
    ud_status status = UD_OK;
#define SET(option, value)                                                                         \
    do {                                                                                           \
        if (!status && curl_easy_setopt(easy, option, value) != CURLE_OK)                          \
            status = UD_IO;                                                                        \
    } while (0)
    SET(CURLOPT_URL, url);
    SET(CURLOPT_PROTOCOLS_STR, protocols);
    SET(CURLOPT_REDIR_PROTOCOLS_STR, protocols);
    SET(CURLOPT_DISALLOW_USERNAME_IN_URL, 1L);
    SET(CURLOPT_FOLLOWLOCATION, 0L);
    SET(CURLOPT_PROXY, "");
    SET(CURLOPT_NETRC, CURL_NETRC_IGNORED);
    SET(CURLOPT_SSL_VERIFYPEER, 1L);
    SET(CURLOPT_SSL_VERIFYHOST, 2L);
    SET(CURLOPT_NOSIGNAL, 1L);
    SET(CURLOPT_CONNECTTIMEOUT_MS, (long)timeout);
    SET(CURLOPT_TIMEOUT_MS, (long)timeout);
    SET(CURLOPT_WRITEFUNCTION, receive);
    SET(CURLOPT_WRITEDATA, t);
    SET(CURLOPT_HEADERFUNCTION, header);
    SET(CURLOPT_HEADERDATA, t);
    SET(CURLOPT_XFERINFOFUNCTION, progress);
    SET(CURLOPT_XFERINFODATA, t);
    SET(CURLOPT_NOPROGRESS, 0L);
#undef SET
    return status;
}
static int retry_status(long code) {
    return code == 408 || code == 429 || code == 500 || code == 502 || code == 503 || code == 504;
}
static ud_status request(const ud_component_context *c, const ud_http_options *p,
                         ud_connection_lease *connection, const char *url, ud_bytes *ca,
                         struct curl_slist *headers, uint32_t credential_count, long *status,
                         ud_http_response *out) {
    CURL *easy = curl_easy_init();
    if (!easy)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    uint64_t now = ud_now_ms();
    transfer t = {.execution = c->activation,
                  .connection = connection,
                  .maximum = p->maximum,
                  .response_headers = p->response_headers,
                  .request_header_definitions = p->headers,
                  .request_headers = headers,
                  .credential_header_count = credential_count,
                  .deadline = p->timeout > UINT64_MAX - now ? UINT64_MAX : now + p->timeout};
    t.body = (ud_bytes){.ctx = c->memory, .limit = (size_t)p->maximum};
    t.header_bytes = (ud_bytes){.ctx = c->memory, .limit = 65536};
    struct curl_blob trust = {ca->data, ca->size, CURL_BLOB_NOCOPY};
    char *method = ud_strdup_n(c->memory, p->method.data, p->method.length);
    if (!method)
        s = UD_RESOURCE;
#define SET(option, value)                                                                         \
    do {                                                                                           \
        if (!s && curl_easy_setopt(easy, option, value) != CURLE_OK)                               \
            s = UD_IO;                                                                             \
    } while (0)
    if (!s)
        s = configure_transport(easy, &t, url, "http,https", p->timeout);
    SET(CURLOPT_HTTPHEADER, headers);
    if (ca->size)
        SET(CURLOPT_CAINFO_BLOB, &trust);
    if (ud_named(p->method, "HEAD"))
        SET(CURLOPT_NOBODY, 1L);
    if (body_method(p->method)) {
        SET(CURLOPT_POSTFIELDS, p->body.data ? p->body.data : "");
        SET(CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)p->body.length);
    }
    SET(CURLOPT_CUSTOMREQUEST, method);
#undef SET
    int completed = 0;
    for (uint64_t i = 0; !s && i < p->attempts; i++) {
        t.received = t.headers = 0;
        t.body.size = t.header_bytes.size = 0;
        t.retry_after = 0;
        out->attempted = 1;
        s = perform(easy, &t, status);
        int transport_failed = s != UD_OK;
        completed = !transport_failed;
        if (s) {
            if ((!ud_named(p->method, "GET") && !ud_named(p->method, "HEAD") &&
                 !p->idempotency_key.length) ||
                i + 1 == p->attempts || s == UD_AUTH || s == UD_CANCELLED || s == UD_RESOURCE)
                break;
            s = UD_OK;
            t.status = UD_OK;
        }
        int success = p->expected ? *status == (long)p->expected : *status >= 200 && *status < 300;
        if (success && !transport_failed)
            break;
        if ((!transport_failed && !retry_status(*status)) || i + 1 == p->attempts) {
            s = UD_HTTP_STATUS;
            break;
        }
        uint64_t pause = p->delay;
        for (uint64_t attempt = 0; attempt < i && pause < 60000; attempt++)
            pause *= 2;
        if (pause > 60000)
            pause = 60000;
        if (t.retry_after > pause)
            pause = t.retry_after;
        uint64_t delay_end = ud_now_ms() + pause;
        do {
            s = check(&t);
            if (s || ud_now_ms() >= delay_end)
                break;
            ud_sleep_ms(10);
        } while (1);
    }
    curl_easy_cleanup(easy);
    ud_free(method);
    out->status = (int32_t)*status;
    out->transport_code = t.transport_code;
    out->completed = completed;
    out->retry_after_ms = t.retry_after;
    if (completed) {
        out->body = (ud_string){(const char *)t.body.data, (uint32_t)t.body.size};
        t.body.data = NULL;
        out->headers =
            (ud_string){(const char *)t.header_bytes.data, (uint32_t)t.header_bytes.size};
        t.header_bytes.data = NULL;
    }
    ud_bytes_clear(&t.body);
    ud_bytes_clear(&t.header_bytes);
    return s;
}
static ud_status http_outcome(ud_context *memory, ud_status status, const ud_http_response *response) {
    if (status == UD_HTTP_STATUS && (response->status == 401 || response->status == 403))
        status = UD_AUTH;
    if (status && response->attempted) {
        ud_error_detail detail = {.struct_size = sizeof(detail), .abi_version = UD_ABI_VERSION,
                                  .source = response->completed ? UD_ERROR_HTTP : UD_ERROR_TRANSPORT,
                                  .cause = status,
                                  .native_code = response->completed ? response->status : response->transport_code};
        const char *message = response->completed ? "HTTP status rejected" : "HTTP transfer failed";
        return ud_context_report_error(memory, status, (ud_string){message, (uint32_t)strlen(message)},
                                        &detail);
    }
    return status;
}
ud_status ud_http_token_exchange(const ud_component_context *context, ud_connection_lease *lease,
                                 const ud_http_options *options, ud_string endpoint, ud_string body,
                                 ud_string authorization, ud_http_response *response) {
    char *url = ud_strdup_n(context->memory, endpoint.data, endpoint.length);
    if (!url)
        return UD_RESOURCE;
    ud_status status = validate_url(url);
    if (!status)
        status = ud_connection_lease_authorize_uri(lease, endpoint);
    struct curl_slist *headers = NULL;
    ud_bytes ca = {.ctx = context->memory, .limit = 1048576};
    if (!status)
        status = load_ca(context->activation, options->ca, &ca);
    if (!status)
        status = add_header(&headers, "Content-Type: application/x-www-form-urlencoded");
    if (!status && authorization.length) {
        char *line = ud_strdup_n(context->memory, authorization.data, authorization.length);
        status = line ? add_header(&headers, line) : UD_RESOURCE;
        ud_string secret = {line, line ? authorization.length : 0};
        ud_http_clear_secret(&secret);
    }
    ud_http_options request_options = {.method = {"POST", 4}, .body = body,
                                       .timeout = options->timeout, .maximum = 65536,
                                       .attempts = 1};
    long code = 0;
    if (!status)
        status = request(context, &request_options, lease, url, &ca, headers, 0, &code, response);
    clear_headers(headers);
    ud_bytes_clear(&ca);
    ud_free(url);
    return http_outcome(context->memory, status, response);
}
#else
ud_status ud_http_validate_headers(const ud_json *headers, const ud_json *response) {
    return headers || response ? UD_UNSUPPORTED : UD_OK;
}
#endif

void ud_http_response_clear(ud_http_response *response) {
    if (!response)
        return;
    ud_free((void *)response->body.data);
    ud_free((void *)response->headers.data);
    memset(response, 0, sizeof(*response));
}
ud_status ud_http_resolve_url(ud_context *memory, ud_string base, ud_string requested, ud_string *out) {
    *out = (ud_string){0};
    ud_string selected = requested.length ? requested : base;
    if (selected.length && selected.data[0] == '/') {
#ifndef UD_WITH_HTTP
        return UD_UNSUPPORTED;
#else
        if (selected.length > 1 && selected.data[1] == '/')
            return UD_SCHEMA;
        CURLU *url = curl_url();
        char *origin = ud_strdup_n(memory, base.data, base.length);
        char *relative = ud_strdup_n(memory, selected.data, selected.length);
        char *resolved = NULL;
        ud_status status = url && origin && relative ? UD_OK : UD_RESOURCE;
        if (!status && (curl_url_set(url, CURLUPART_URL, origin, 0) ||
                        curl_url_set(url, CURLUPART_URL, relative, 0) ||
                        curl_url_get(url, CURLUPART_URL, &resolved, 0)))
            status = UD_SCHEMA;
        if (!status) {
            out->length = (uint32_t)strlen(resolved);
            out->data = ud_strdup_n(memory, resolved, out->length);
            if (!out->data)
                status = UD_RESOURCE;
        }
        curl_free(resolved);
        if (url)
            curl_url_cleanup(url);
        ud_free(relative);
        ud_free(origin);
        return status;
#endif
    }
    out->data = ud_strdup_n(memory, selected.data, selected.length);
    out->length = selected.length;
    return out->data ? UD_OK : UD_RESOURCE;
}
ud_status ud_http_call(const ud_component_context *c, ud_connection_lease *connection,
                       const ud_http_options *p, ud_http_response *out) {
    if (!c || !connection || !p || !out || p->struct_size != sizeof(*p) ||
        p->abi_version != UD_ABI_VERSION)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
#ifndef UD_WITH_HTTP
    return UD_UNSUPPORTED;
#else
    if (!p->timeout || !p->maximum || p->maximum > 67108864 || !p->attempts || p->attempts > 8 ||
        p->delay > 60000 || !clean_text(p->method) || !clean_text(p->content_type) ||
        !clean_text(p->idempotency_key))
        return UD_SCHEMA;
    if (p->attempts > 1 && !ud_named(p->method, "GET") && !ud_named(p->method, "HEAD") &&
        !ud_named(p->method, "PUT") && !ud_named(p->method, "DELETE") && !p->idempotency_key.length)
        return UD_UNSUPPORTED;
    UD_TRY(ud_http_validate_headers(p->headers, p->response_headers));
    ud_string resource = {0}, secret = {0};
    ud_bytes ca = {.ctx = c->memory, .limit = 1048576};
    struct curl_slist *headers = NULL;
    uint32_t credential_count = 0;
    char *url = NULL;
    ud_status status = ud_connection_lease_authorize(connection, UD_ACCESS_NETWORK);
    if (!status)
        status = ud_http_connection_resource(connection, &resource);
    ud_string selected = {0};
    if (!status)
        status = ud_http_resolve_url(c->memory, resource, p->url, &selected);
    url = (char *)selected.data;
    if (!status)
        status = validate_url(url);
    if (!status)
        status =
            ud_connection_lease_authorize_uri(connection, (ud_string){url, (uint32_t)strlen(url)});
    if (!status && p->url.length) {
        char *base = ud_strdup_n(c->memory, resource.data, resource.length);
        if (!base)
            status = UD_RESOURCE;
        else {
            CURLU *a = curl_url(), *b = curl_url();
            if (!a || !b)
                status = UD_RESOURCE;
            else {
                if (curl_url_set(a, CURLUPART_URL, base, 0) ||
                    curl_url_set(b, CURLUPART_URL, url, 0))
                    status = UD_SCHEMA;
                const CURLUPart parts[] = {CURLUPART_SCHEME, CURLUPART_HOST, CURLUPART_PORT};
                for (uint32_t i = 0; !status && i < 3; i++) {
                    char *x = NULL, *y = NULL;
                    curl_url_get(a, parts[i], &x, CURLU_DEFAULT_PORT);
                    curl_url_get(b, parts[i], &y, CURLU_DEFAULT_PORT);
                    if (!x || !y || strcmp(x, y))
                        status = UD_AUTH;
                    curl_free(x);
                    curl_free(y);
                }
            }
            if (a)
                curl_url_cleanup(a);
            if (b)
                curl_url_cleanup(b);
            ud_free(base);
        }
    }
    if (!status) {
        ensure_initialized();
        const curl_version_info_data *version = curl_version_info(CURLVERSION_NOW);
        if (!initialized || !version || version->version_num < 0x075500 ||
            !(version->features & CURL_VERSION_SSL) ||
            !(version->features & CURL_VERSION_ASYNCHDNS))
            status = UD_UNSUPPORTED;
    }
    if (!status)
        status = ud_http_connection_credentials(connection, &secret);
    if (!status)
        status = credentials(c->memory, secret, &headers, &credential_count);
    ud_http_clear_secret(&secret);
    if (!status)
        status = request_headers(c, connection, p, resource, &headers);
    ud_http_oauth_cache *cache;
    const ud_json *oauth = ud_http_connection_oauth(connection, &cache);
    if (!status && oauth) {
        ud_string token;
        status = ud_http_oauth_bearer(c, connection, p, oauth, cache, &token);
        if (!status) {
            ud_bytes authorization = {.ctx = c->memory, .limit = 16384};
            status = ud_bytes_put(&authorization, "Bearer ", 7);
            if (!status)
                status = ud_bytes_put(&authorization, token.data, token.length);
            if (!status)
                status = header_value(c->memory, &headers, (ud_string){"Authorization", 13},
                                      (ud_string){(const char *)authorization.data,
                                                  (uint32_t)authorization.size});
            ud_string value = {(const char *)authorization.data, (uint32_t)authorization.size};
            ud_http_clear_secret(&value);
        }
    }
    if (!status)
        status = add_header(&headers, "Expect:");
    const ud_string values[] = {p->content_type, p->idempotency_key};
    const char *names[] = {"Content-Type: ", "Idempotency-Key: "};
    for (uint32_t i = 0; !status && i < 2; i++)
        if (values[i].length) {
            ud_bytes line = {.ctx = c->memory, .limit = 65536};
            status = ud_bytes_put(&line, names[i], strlen(names[i]));
            if (!status)
                status = ud_bytes_put(&line, values[i].data, values[i].length);
            if (!status)
                status = ud_bytes_put(&line, "", 1);
            if (!status)
                status = add_header(&headers, (char *)line.data);
            ud_bytes_clear(&line);
        }
    if (!status)
        status = load_ca(c->activation, p->ca, &ca);
    long code = 0;
    if (!status)
        status = request(c, p, connection, url, &ca, headers, credential_count, &code, out);
    clear_headers(headers);
    ud_bytes_clear(&ca);
    ud_free(url);
    return http_outcome(c->memory, status, out);
#endif
}

#ifdef UD_WITH_HTTP
static ud_status network_url(const char *text, int protocol) {
    if (!clean_text((ud_string){text, (uint32_t)strlen(text)}) || strchr(text, ' '))
        return UD_SCHEMA;
    CURLU *url = curl_url();
    if (!url)
        return UD_RESOURCE;
    ud_status status = curl_url_set(url, CURLUPART_URL, text, 0) ? UD_SCHEMA : UD_OK;
    char *scheme = NULL, *host = NULL;
    if (!status && curl_url_get(url, CURLUPART_SCHEME, &scheme, 0))
        status = UD_SCHEMA;
    if (!status && curl_url_get(url, CURLUPART_HOST, &host, 0))
        status = UD_SCHEMA;
    if (!status && (!host || !*host))
        status = UD_SCHEMA;
    if (!status &&
        !(protocol == UD_NETWORK_FTP    ? !strcmp(scheme, "ftp") || !strcmp(scheme, "ftps")
          : protocol == UD_NETWORK_SFTP ? !strcmp(scheme, "sftp")
          : protocol == UD_NETWORK_SMTP ? !strcmp(scheme, "smtp") || !strcmp(scheme, "smtps")
                                        : !strcmp(scheme, "http") || !strcmp(scheme, "https")))
        status = UD_UNSUPPORTED;
    CURLUPart forbidden[] = {CURLUPART_USER, CURLUPART_PASSWORD, CURLUPART_FRAGMENT};
    for (uint32_t at = 0; !status && at < 3; at++) {
        char *part = NULL;
        if (!curl_url_get(url, forbidden[at], &part, 0))
            status = UD_SCHEMA;
        curl_free(part);
    }
    curl_free(scheme);
    curl_free(host);
    curl_url_cleanup(url);
    return status;
}
static ud_status credential_file(const ud_component_context *context, const ud_json *config,
                                 const char *key, ud_connection_lease **lease, char **path) {
    const ud_json *value = ud_member(config, key);
    if (!value)
        return UD_OK;
    ud_string connection, resource;
    UD_TRY(ud_json_string(value, &connection));
    UD_TRY(ud_acquire_file_connection(context->activation, connection, UD_CONNECT_READ, lease));
    UD_TRY(ud_connection_lease_authorize(*lease, UD_ACCESS_READ));
    UD_TRY(ud_connection_lease_path(*lease, &resource));
    *path = ud_strdup_n(context->memory, resource.data, resource.length);
    if (!*path)
        return UD_RESOURCE;
    return ud_path_no_symlinks(context->memory, *path);
}
static ud_status network_auth(const ud_component_context *context, ud_connection_lease *connection,
                              const ud_network_options *options, CURL *easy,
                              transfer *transfer_state, struct curl_slist **headers,
                              ud_json **document, char **trust_path, char **key_path,
                              char *strings[5]) {
    ud_string secret = {0};
    ud_status status = ud_http_connection_credentials(connection, &secret);
    if (!status && secret.length)
        status = ud_json_parse(context->memory, secret.data, secret.length, document, NULL);
    ud_free((void *)secret.data);
    static const char *keys[] = {"headers",
                                 "username",
                                 "password",
                                 "awsSigV4",
                                 "bearerToken",
                                 "knownHostsConnection",
                                 "privateKeyConnection",
                                 "keyPassphrase"};
    if (!status && *document)
        status = ud_json_keys(*document, keys, 8);
    const char *names[] = {"username", "password", "awsSigV4", "bearerToken", "keyPassphrase"};
    for (uint32_t at = 0; !status && at < 5; at++) {
        const ud_json *value = ud_member(*document, names[at]);
        if (!value)
            continue;
        ud_string text;
        status = ud_json_string(value, &text);
        if (!status && (!text.length || text.length > 16384 || !clean_text(text) ||
                        memchr(text.data, 0, text.length)))
            status = UD_SCHEMA;
        if (!status) {
            strings[at] = ud_strdup_n(context->memory, text.data, text.length);
            if (!strings[at])
                status = UD_RESOURCE;
        }
    }
    const ud_json *header_values = ud_member(*document, "headers");
    if (!status && header_values) {
        ud_bytes json = {.ctx = context->memory, .limit = 65536};
        char *value = NULL;
        uint32_t length = 0;
        status = ud_json_serialize(header_values, &value, &length);
        if (!status)
            status = ud_bytes_put(&json, "{\"headers\":", 11);
        if (!status)
            status = ud_bytes_put(&json, value, length);
        if (!status)
            status = ud_bytes_put(&json, "}", 1);
        if (!status)
            status = credentials(context->memory,
                                 (ud_string){(char *)json.data, (uint32_t)json.size}, headers,
                                 &transfer_state->credential_header_count);
        ud_free(value);
        ud_bytes_clear(&json);
    }
    transfer_state->request_headers = *headers;
    if (!status)
        status = credential_file(context, *document, "knownHostsConnection",
                                 &transfer_state->trust_lease, trust_path);
    if (!status)
        status = credential_file(context, *document, "privateKeyConnection",
                                 &transfer_state->key_lease, key_path);
    if (!status && options->protocol == UD_NETWORK_SFTP && !*trust_path)
        status = UD_SCHEMA;
    if (!status && options->protocol != UD_NETWORK_SFTP && (*trust_path || *key_path || strings[4]))
        status = UD_SCHEMA;
    if (!status && strings[2] && options->protocol != UD_NETWORK_S3)
        status = UD_SCHEMA;
    if (!status && strings[2] && (!strings[0] || !strings[1]))
        status = UD_SCHEMA;
    if (!status && strings[3] && (strings[1] || strings[2]))
        status = UD_SCHEMA;
#define SET(option, value)                                                                         \
    do {                                                                                           \
        if (!status && curl_easy_setopt(easy, option, value) != CURLE_OK)                          \
            status = UD_IO;                                                                        \
    } while (0)
    if (strings[0])
        SET(CURLOPT_USERNAME, strings[0]);
    if (strings[1])
        SET(CURLOPT_PASSWORD, strings[1]);
    if (strings[2])
        SET(CURLOPT_AWS_SIGV4, strings[2]);
    if (strings[3]) {
        SET(CURLOPT_HTTPAUTH, (long)CURLAUTH_BEARER);
        SET(CURLOPT_XOAUTH2_BEARER, strings[3]);
    }
    if (*trust_path)
        SET(CURLOPT_SSH_KNOWNHOSTS, *trust_path);
    if (*key_path)
        SET(CURLOPT_SSH_PRIVATE_KEYFILE, *key_path);
    if (strings[4])
        SET(CURLOPT_KEYPASSWD, strings[4]);
#undef SET
    return status;
}
static ud_status url_query(ud_context *memory, const char *base, const char *query, char **out) {
    CURLU *url = curl_url();
    if (!url)
        return UD_RESOURCE;
    char *existing = NULL, *text = NULL;
    ud_status status = curl_url_set(url, CURLUPART_URL, base, 0) ? UD_SCHEMA : UD_OK;
    if (!status && !curl_url_get(url, CURLUPART_QUERY, &existing, 0) && *existing)
        status = UD_SCHEMA; /* Credentials use references; operation query is Native-owned. */
    if (!status && curl_url_set(url, CURLUPART_QUERY, query, 0))
        status = UD_SCHEMA;
    if (!status && curl_url_get(url, CURLUPART_URL, &text, 0))
        status = UD_SCHEMA;
    if (!status) {
        *out = ud_strdup_n(memory, text, strlen(text));
        if (!*out)
            status = UD_RESOURCE;
    }
    curl_free(existing);
    curl_free(text);
    curl_url_cleanup(url);
    return status;
}
static ud_status network_perform(CURL *easy, transfer *transfer_state, const char *url,
                                 ud_network_result *result, int http) {
    UD_TRY(ud_connection_lease_authorize_uri(transfer_state->connection,
                                             (ud_string){url, (uint32_t)strlen(url)}));
    if (curl_easy_setopt(easy, CURLOPT_URL, url) != CURLE_OK)
        return UD_IO;
    result->attempted = 1;
    long code = 0;
    ud_status status = perform(easy, transfer_state, &code);
    result->status = (int32_t)code;
    result->completed = !status;
    result->bytes = transfer_state->file_action == UD_ACCESS_READ ? transfer_state->uploaded
                                                                  : transfer_state->received;
    if (!status && http && (code < 200 || code >= 300))
        status = code == 401 || code == 403 ? UD_AUTH : code == 412 ? UD_FENCED : UD_IO;
    if (status && transfer_state->network_action == UD_ACCESS_WRITE &&
        (!result->completed || code >= 500 || code == 408))
        status = UD_COMMIT_UNKNOWN;
    return status;
}
#endif
ud_status ud_network_call(const ud_component_context *context, ud_connection_lease *connection,
                          const ud_network_options *options, ud_network_result *result) {
    if (!context || !connection || !options || !result)
        return UD_INVALID;
    *result = (ud_network_result){0};
#ifndef UD_WITH_HTTP
    return UD_UNSUPPORTED;
#else
    if (!options->timeout || options->timeout > 86400000 || !options->maximum ||
        options->maximum > INT64_MAX || options->protocol < UD_NETWORK_FTP ||
        options->protocol > UD_NETWORK_S3 || !clean_text(options->content_type))
        return UD_SCHEMA;
    if (options->upload && ud_transaction_active(context->activation))
        return UD_UNSUPPORTED; /* This resource cannot prepare a transaction. */
    ensure_initialized();
    if (!initialized)
        return UD_UNSUPPORTED;
    ud_string resource;
    ud_status status = ud_network_connection_resource(connection, &resource);
    char *url = !status ? ud_strdup_n(context->memory, resource.data, resource.length) : NULL;
    if (!status && !url)
        status = UD_RESOURCE;
    if (!status)
        status = network_url(url, options->protocol);
    if (!status) {
        const curl_version_info_data *version = curl_version_info(CURLVERSION_NOW);
        CURLU *parsed = curl_url();
        char *scheme = NULL;
        int supported = 0;
        if (!version || !parsed || curl_url_set(parsed, CURLUPART_URL, url, 0) ||
            curl_url_get(parsed, CURLUPART_SCHEME, &scheme, 0))
            status = UD_UNSUPPORTED;
        for (uint32_t at = 0; !status && version->protocols[at]; at++)
            if (!strcmp(version->protocols[at], scheme))
                supported = 1;
        if (!status && !supported)
            status = UD_UNSUPPORTED;
        curl_free(scheme);
        if (parsed)
            curl_url_cleanup(parsed);
    }
    const char *protocols = options->protocol == UD_NETWORK_FTP    ? "ftp,ftps"
                            : options->protocol == UD_NETWORK_SFTP ? "sftp"
                            : options->protocol == UD_NETWORK_SMTP ? "smtp,smtps"
                                                                   : "http,https";
    CURL *easy = !status ? curl_easy_init() : NULL;
    if (!status && !easy)
        status = UD_RESOURCE;
    transfer state = {.execution = context->activation,
                      .connection = connection,
                      .maximum = options->upload ? 1048576 : options->maximum,
                      .deadline = ud_now_ms() + options->timeout,
                      .network_action = options->upload ? UD_ACCESS_WRITE : UD_ACCESS_READ,
                      .file_lease = options->file,
                      .file_action = options->upload ? UD_ACCESS_READ : UD_ACCESS_WRITE,
                      .body = {.ctx = context->memory, .limit = 1048576},
                      .header_bytes = {.ctx = context->memory, .limit = 65536}};
    if (!status)
        status = check(&state);
    if (!status && options->path) {
        if (options->upload) {
            status = ud_open_regular(context->memory, options->path, &state.file);
            if (!status && ud_seek(state.file, 0))
                status = UD_IO;
            if (!status) {
#ifdef _WIN32
                if (_fseeki64(state.file, 0, SEEK_END))
                    status = UD_IO;
#else
                if (fseeko(state.file, 0, SEEK_END))
                    status = UD_IO;
#endif
                int64_t length = !status ? ud_tell(state.file) : -1;
                if (length < 0 || (uint64_t)length > options->maximum)
                    status = UD_RESOURCE;
                else
                    state.upload_size = (uint64_t)length;
                if (!status && ud_seek(state.file, 0))
                    status = UD_IO;
            }
        } else {
            /* The task passes its already reserved Native generation path. */
            status = ud_path_no_symlinks(context->memory, options->path);
            if (!status)
                state.file = ud_fopen(options->path, "wb");
            if (!state.file)
                status = status ? status : UD_IO;
        }
    } else if (!status && options->protocol == UD_NETWORK_SMTP) {
        state.upload_bytes = (const uint8_t *)options->message.data;
        state.upload_size = options->message.length;
    } else if (!status)
        status = UD_SCHEMA;
    if (!status)
        status = configure_transport(easy, &state, url, protocols, options->timeout);
    ud_json *auth = NULL;
    struct curl_slist *headers = NULL, *recipients = NULL;
    char *trust_path = NULL, *key_path = NULL, *strings[5] = {0}, *sender = NULL;
    ud_bytes ca = {.ctx = context->memory, .limit = 1048576};
    if (!status)
        status = network_auth(context, connection, options, easy, &state, &headers, &auth,
                              &trust_path, &key_path, strings);
    if (!status)
        status = load_ca(context->activation, options->ca, &ca);
    struct curl_blob trust = {ca.data, ca.size, CURL_BLOB_NOCOPY};
#define SET(option, value)                                                                         \
    do {                                                                                           \
        if (!status && curl_easy_setopt(easy, option, value) != CURLE_OK)                          \
            status = UD_IO;                                                                        \
    } while (0)
    if (ca.size)
        SET(CURLOPT_CAINFO_BLOB, &trust);
    if (options->require_tls &&
        (options->protocol == UD_NETWORK_FTP || options->protocol == UD_NETWORK_SMTP))
        SET(CURLOPT_USE_SSL, (long)CURLUSESSL_ALL);
    if (options->upload) {
        SET(CURLOPT_UPLOAD, 1L);
        SET(CURLOPT_READFUNCTION, upload);
        SET(CURLOPT_READDATA, &state);
        SET(CURLOPT_INFILESIZE_LARGE, (curl_off_t)state.upload_size);
    }
    if (options->protocol == UD_NETWORK_SMTP) {
        sender = ud_strdup_n(context->memory, options->sender.data, options->sender.length);
        if (!sender)
            status = UD_RESOURCE;
        SET(CURLOPT_MAIL_FROM, sender);
        for (uint32_t at = 0; !status && at < ud_json_size(options->recipients); at++) {
            ud_string recipient;
            status = ud_json_string(ud_json_at(options->recipients, at), &recipient);
            char *text =
                !status ? ud_strdup_n(context->memory, recipient.data, recipient.length) : NULL;
            if (!status && !text)
                status = UD_RESOURCE;
            if (!status)
                status = add_header(&recipients, text);
            ud_free(text);
        }
        SET(CURLOPT_MAIL_RCPT, recipients);
    }
    int http = options->protocol >= UD_NETWORK_AZURE_BLOB;
    if (!status && http)
        status = add_header(&headers, "Expect:");
    if (!status && http && options->upload && options->protocol != UD_NETWORK_ADLS &&
        !options->overwrite)
        status = add_header(&headers, "If-None-Match: *");
    if (!status &&
        (options->protocol == UD_NETWORK_AZURE_BLOB || options->protocol == UD_NETWORK_ADLS))
        status = add_header(&headers, "x-ms-version: 2023-11-03");
    if (!status && options->protocol == UD_NETWORK_AZURE_BLOB && options->upload)
        status = add_header(&headers, "x-ms-blob-type: BlockBlob");
    if (!status && http && options->content_type.length) {
        ud_bytes header_text = {.ctx = context->memory, .limit = 65536};
        status = ud_bytes_put(&header_text, "Content-Type: ", 14);
        if (!status)
            status = ud_bytes_put(&header_text, options->content_type.data,
                                  options->content_type.length);
        if (!status)
            status = ud_bytes_put(&header_text, "", 1);
        if (!status)
            status = add_header(&headers, (char *)header_text.data);
        ud_bytes_clear(&header_text);
    }
    SET(CURLOPT_HTTPHEADER, headers);
    char *operation_url = NULL;
    int adls_created = 0;
    if (!status && options->protocol == UD_NETWORK_ADLS && options->upload) {
        status = url_query(context->memory, url, "resource=file", &operation_url);
        SET(CURLOPT_INFILESIZE_LARGE, (curl_off_t)0);
        SET(CURLOPT_CUSTOMREQUEST, "PUT");
        if (!status && !options->overwrite) {
            status = add_header(&headers, "If-None-Match: *");
            SET(CURLOPT_HTTPHEADER, headers);
        }
        if (!status) {
            status = network_perform(easy, &state, operation_url, result, 1);
            adls_created = !status;
        }
        ud_free(operation_url);
        operation_url = NULL;
        /* Rebuild header ownership to remove create-only conditional semantics. */
        struct curl_slist *last = headers;
        if (!options->overwrite && !status) {
            while (last && last->next && last->next->next)
                last = last->next;
            if (last && last->next) {
                curl_slist_free_all(last->next);
                last->next = NULL;
            }
        }
        SET(CURLOPT_HTTPHEADER, headers);
        if (!status)
            status = url_query(context->memory, url, "action=append&position=0", &operation_url);
        SET(CURLOPT_CUSTOMREQUEST, "PATCH");
        SET(CURLOPT_INFILESIZE_LARGE, (curl_off_t)state.upload_size);
        if (!status)
            status = network_perform(easy, &state, operation_url, result, 1);
        ud_free(operation_url);
        operation_url = NULL;
        char query[96];
        snprintf(query, sizeof(query), "action=flush&position=%llu",
                 (unsigned long long)state.upload_size);
        if (!status)
            status = url_query(context->memory, url, query, &operation_url);
        SET(CURLOPT_INFILESIZE_LARGE, (curl_off_t)0);
        if (!status)
            status = network_perform(easy, &state, operation_url, result, 1);
    } else if (!status)
        status = network_perform(easy, &state, url, result, http);
    if (status && adls_created)
        status = UD_COMMIT_UNKNOWN;
#undef SET
    if (!status && !options->upload)
        status = ud_flush(context->memory, state.file);
    if (state.file && fclose(state.file) && !status)
        status = UD_IO;
    if (easy)
        curl_easy_cleanup(easy);
    curl_slist_free_all(headers);
    curl_slist_free_all(recipients);
    ud_bytes_clear(&state.body);
    ud_bytes_clear(&state.header_bytes);
    ud_bytes_clear(&ca);
    (void)ud_connection_lease_release(state.trust_lease);
    (void)ud_connection_lease_release(state.key_lease);
    ud_json_destroy(auth);
    for (uint32_t at = 0; at < 5; at++)
        ud_free(strings[at]);
    ud_free(trust_path);
    ud_free(key_path);
    ud_free(sender);
    ud_free(operation_url);
    ud_free(url);
    return status;
#endif
}
