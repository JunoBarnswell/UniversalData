#include "../engines/package/internal.h"
#include "ud/http.h"
#ifdef UD_WITH_HTTP
#include <curl/curl.h>
#include <strings.h>
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
} transfer;
static ud_status check(transfer *t) {
    ud_status s = ud_connection_lease_authorize(t->connection, UD_ACCESS_NETWORK);
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
    t->status = ud_bytes_put(&t->body, ptr, n);
    if (t->status)
        return 0;
    t->received += n;
    return n;
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
    t->status = ud_bytes_put(&t->header_bytes, ptr, n);
    if (t->status)
        return 0;
    if (n > 12 && !strncasecmp(ptr, "Retry-After:", 12)) {
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
static ud_status credentials(ud_context *ctx, ud_string text, struct curl_slist **list) {
    if (!text.length)
        return UD_OK;
    ud_json *doc = NULL;
    UD_TRY(ud_json_parse(ctx, text.data, text.length, &doc, NULL));
    static const char *keys[] = {"headers"};
    ud_status s = ud_json_keys(doc, keys, 1);
    const ud_json *headers = ud_member(doc, "headers");
    if (!s && (ud_json_kind(headers) != UD_JSON_OBJECT || ud_json_size(headers) > 64))
        s = UD_SCHEMA;
    for (uint32_t i = 0; !s && i < ud_json_size(headers); i++) {
        ud_string name, value;
        s = ud_json_key(headers, i, &name.data, &name.length);
        if (!s)
            s = ud_json_string(ud_json_at(headers, i), &value);
        if (!s && (!name.length || !clean_text(value)))
            s = UD_SCHEMA;
        for (uint32_t j = 0; !s && j < name.length; j++) {
            unsigned char c = (unsigned char)name.data[j];
            if (!c || !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                        (c >= '0' && c <= '9') || strchr("!#$%&'*+-.^_`|~", c)))
                s = UD_SCHEMA;
        }
        static const char *forbidden[] = {"host",        "content-length", "transfer-encoding",
                                          "connection",  "expect",         "proxy-authorization",
                                          "content-type"};
        for (int j = 0; !s && j < 7; j++)
            if (named_ci(name, forbidden[j]))
                s = UD_SCHEMA;
        ud_bytes line = {.ctx = ctx, .limit = 32768};
        if (!s)
            s = ud_bytes_put(&line, name.data, name.length);
        if (!s)
            s = ud_bytes_put(&line, ": ", 2);
        if (!s)
            s = ud_bytes_put(&line, value.data, value.length);
        if (!s)
            s = ud_bytes_put(&line, "", 1);
        if (!s)
            s = add_header(list, (char *)line.data);
        ud_bytes_clear(&line);
    }
    ud_json_destroy(doc);
    return s;
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
    if (t->status)
        s = t->status;
    if (!s && (!received || result != CURLE_OK))
        s = result == CURLE_OPERATION_TIMEDOUT ? UD_TIMEOUT : UD_IO;
    return s;
}
static int retry_status(long code) {
    return code == 408 || code == 429 || code == 500 || code == 502 || code == 503 || code == 504;
}
static ud_status request(const ud_component_context *c, const ud_http_options *p,
                         ud_connection_lease *connection, const char *url, ud_bytes *ca,
                         struct curl_slist *headers, long *status, ud_http_response *out) {
    CURL *easy = curl_easy_init();
    if (!easy)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    uint64_t now = ud_now_ms();
    transfer t = {.execution = c->activation,
                  .connection = connection,
                  .maximum = p->maximum,
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
    SET(CURLOPT_URL, url);
    SET(CURLOPT_PROTOCOLS_STR, "http,https");
    SET(CURLOPT_REDIR_PROTOCOLS_STR, "http,https");
    SET(CURLOPT_DISALLOW_USERNAME_IN_URL, 1L);
    SET(CURLOPT_FOLLOWLOCATION, 0L);
    SET(CURLOPT_PROXY, "");
    SET(CURLOPT_NETRC, CURL_NETRC_IGNORED);
    SET(CURLOPT_SSL_VERIFYPEER, 1L);
    SET(CURLOPT_SSL_VERIFYHOST, 2L);
    SET(CURLOPT_NOSIGNAL, 1L);
    SET(CURLOPT_CONNECTTIMEOUT_MS, (long)p->timeout);
    SET(CURLOPT_TIMEOUT_MS, (long)p->timeout);
    SET(CURLOPT_WRITEFUNCTION, receive);
    SET(CURLOPT_WRITEDATA, &t);
    SET(CURLOPT_HEADERFUNCTION, header);
    SET(CURLOPT_HEADERDATA, &t);
    SET(CURLOPT_XFERINFOFUNCTION, progress);
    SET(CURLOPT_XFERINFODATA, &t);
    SET(CURLOPT_NOPROGRESS, 0L);
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
            s = UD_IO;
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
#endif

void ud_http_response_clear(ud_http_response *response) {
    if (!response)
        return;
    ud_free((void *)response->body.data);
    ud_free((void *)response->headers.data);
    memset(response, 0, sizeof(*response));
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
    ud_string resource = {0}, secret = {0};
    ud_bytes ca = {.ctx = c->memory, .limit = 1048576};
    struct curl_slist *headers = NULL;
    char *url = NULL;
    ud_status status = ud_connection_lease_authorize(connection, UD_ACCESS_NETWORK);
    if (!status)
        status = ud_http_connection_resource(connection, &resource);
    if (!status) {
        ud_string selected = p->url.length ? p->url : resource;
        url = ud_strdup_n(c->memory, selected.data, selected.length);
        if (!url)
            status = UD_RESOURCE;
    }
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
        status = credentials(c->memory, secret, &headers);
    ud_free((void *)secret.data);
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
        status = request(c, p, connection, url, &ca, headers, &code, out);
    if (status && code == 401)
        status = UD_AUTH;
    else if (status && code == 403)
        status = UD_AUTH;
    curl_slist_free_all(headers);
    ud_bytes_clear(&ca);
    ud_free(url);
    return status;
#endif
}
