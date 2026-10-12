#include "../package/internal.h"
#include "ud/connection_manager.h"
#ifdef UD_WITH_HTTP
#include <curl/curl.h>
#endif
typedef struct http_config {
    ud_string connection, method, body, content_type, ca, status_variable;
    uint64_t timeout, maximum, attempts, delay, expected;
} http_config;
static int body_method(ud_string method) {
    return ud_named(method, "POST") || ud_named(method, "PUT") || ud_named(method, "PATCH");
}
static int clean_text(ud_string text) {
    for (uint32_t i = 0; i < text.length; i++)
        if ((unsigned char)text.data[i] < 32 || (unsigned char)text.data[i] == 127)
            return 0;
    return 1;
}
static ud_status read_config(const ud_component_context *c, ud_string text, ud_json **out,
                             http_config *p) {
    *out = NULL;
    *p = (http_config){
        .method = {"GET", 3}, .timeout = 60000, .maximum = 1048576, .attempts = 1, .delay = 100};
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, out, NULL));
    static const char *keys[] = {"connection",       "method",        "body",
                                 "contentType",      "caConnection",  "timeoutMs",
                                 "maxResponseBytes", "maxAttempts",   "retryDelayMs",
                                 "expectedStatus",   "statusVariable"};
    ud_status s = ud_json_keys(*out, keys, 11);
    if (!s)
        s = ud_json_string(ud_member(*out, "connection"), &p->connection);
    const char *names[] = {"method", "contentType", "caConnection", "statusVariable"};
    ud_string *strings[] = {&p->method, &p->content_type, &p->ca, &p->status_variable};
    for (int i = 0; !s && i < 4; i++)
        if (ud_member(*out, names[i]))
            s = ud_json_string(ud_member(*out, names[i]), strings[i]);
    int readonly = ud_named(p->method, "GET") || ud_named(p->method, "HEAD");
    if (!s && !readonly && !body_method(p->method) && !ud_named(p->method, "DELETE"))
        s = UD_UNSUPPORTED;
    const ud_json *body = ud_member(*out, "body");
    if (!s && body) {
        if (!body_method(p->method) || ud_json_kind(body) != UD_JSON_STRING)
            s = UD_SCHEMA;
        else
            s = ud_json_text(body, &p->body.data, &p->body.length);
        if (!s && p->body.length > 1048576)
            s = UD_RESOURCE;
    }
    if (!s && !clean_text(p->content_type))
        s = UD_SCHEMA;
    const char *numbers[] = {"timeoutMs", "maxResponseBytes", "maxAttempts", "retryDelayMs",
                             "expectedStatus"};
    uint64_t *values[] = {&p->timeout, &p->maximum, &p->attempts, &p->delay, &p->expected};
    for (int i = 0; !s && i < 5; i++)
        if (ud_member(*out, numbers[i]))
            s = ud_json_uint(ud_member(*out, numbers[i]), values[i]);
    if (!s && (!p->timeout || p->timeout > 86400000 || !p->maximum || p->maximum > 67108864 ||
               !p->attempts || p->attempts > 3 || p->delay > 60000 ||
               (p->expected && (p->expected < 100 || p->expected > 599)) ||
               (ud_member(*out, "expectedStatus") && !p->expected)))
        s = UD_SCHEMA;
    if (!s && !readonly && p->attempts != 1)
        s = UD_SCHEMA;
    if (!s && p->status_variable.length) {
        ud_variable *v = NULL;
        s = ud_variable_writable(c->execution, p->status_variable, &v);
        if (!s && v->value.type != UD_INT64)
            s = UD_SCHEMA;
    }
    ud_string ids[] = {p->connection, p->ca};
    for (int i = 0; !s && i < 2; i++)
        if (ids[i].length) {
            const ud_json *def = NULL;
            int owner;
            s = ud_connection_definition(c->execution, ids[i], &def, &owner);
            const ud_json *driver = ud_member(def, "driver");
            ud_string value;
            if (!s && ud_json_kind(driver) == UD_JSON_STRING) {
                s = ud_json_string(driver, &value);
                if (!s && !ud_named(value, i ? "file" : "http"))
                    s = UD_UNSUPPORTED;
            }
        }
#ifndef UD_WITH_HTTP
    if (!s)
        s = UD_UNSUPPORTED;
#endif
    return s;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    (void)user;
    (void)in;
    (void)out;
    http_config p;
    ud_json *doc = NULL;
    ud_status s = read_config(c, text, &doc, &p);
    ud_json_destroy(doc);
    return s;
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
    ud_execution *execution;
    ud_connection_lease *connection;
    uint64_t deadline, maximum, received, headers;
    ud_status status;
} transfer;
static ud_status check(transfer *t) {
    ud_status s = ud_connection_lease_authorize(t->connection, UD_ACCESS_NETWORK);
    if (!s && ud_now_ms() >= t->deadline)
        s = UD_TRANSIENT;
    if (s)
        t->status = s;
    return s;
}
static size_t receive(char *ptr, size_t size, size_t count, void *user) {
    (void)ptr;
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
    t->received += n;
    return n;
}
static size_t header(char *ptr, size_t size, size_t count, void *user) {
    (void)ptr;
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
static ud_status load_ca(ud_execution *e, ud_string id, ud_bytes *bytes) {
    if (!id.length)
        return UD_OK;
    ud_connection_lease *lease = NULL;
    ud_status s = ud_acquire_file_connection(e, id, &lease);
    ud_string path = {0};
    if (!s)
        s = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
    if (!s)
        s = ud_connection_lease_path(lease, &path);
    char *filename = !s ? ud_strdup_n(e->ctx, path.data, path.length) : NULL;
    if (!s && !filename)
        s = UD_RESOURCE;
    if (!s)
        s = ud_path_no_symlinks(e->ctx, filename);
    FILE *file = NULL;
    if (!s)
        s = ud_open_regular(e->ctx, filename, &file);
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
        s = result == CURLE_OPERATION_TIMEDOUT ? UD_TRANSIENT : UD_IO;
    return s;
}
static int retry_status(long code) {
    return code == 408 || code == 429 || code == 500 || code == 502 || code == 503 || code == 504;
}
static ud_status request(const ud_component_context *c, const http_config *p,
                         ud_connection_lease *connection, const char *url, ud_bytes *ca,
                         struct curl_slist *headers, long *status) {
    CURL *easy = curl_easy_init();
    if (!easy)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    uint64_t now = ud_now_ms();
    transfer t = {.execution = c->execution,
                  .connection = connection,
                  .maximum = p->maximum,
                  .deadline = p->timeout > UINT64_MAX - now ? UINT64_MAX : now + p->timeout};
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
    for (uint64_t i = 0; !s && i < p->attempts; i++) {
        t.received = t.headers = 0;
        s = perform(easy, &t, status);
        if (s)
            break; /* Transport failure is never silently retried. */
        int success = p->expected ? *status == (long)p->expected : *status >= 200 && *status < 300;
        if (success)
            break;
        if (!retry_status(*status) || i + 1 == p->attempts) {
            s = UD_IO;
            break;
        }
        uint64_t delay_end = ud_now_ms() + p->delay;
        do {
            s = check(&t);
            if (s || ud_now_ms() >= delay_end)
                break;
            ud_sleep_ms(10);
        } while (1);
    }
    curl_easy_cleanup(easy);
    ud_free(method);
    return s;
}
#endif
static ud_status invoke(void *user, const ud_component_context *c, ud_string text) {
    (void)user;
    ud_json *doc = NULL;
    http_config p;
    ud_status s = read_config(c, text, &doc, &p);
#ifdef UD_WITH_HTTP
    ud_connection_lease *connection = NULL;
    ud_string resource = {0}, secret = {0};
    ud_bytes ca = {.ctx = c->memory, .limit = 1048576};
    struct curl_slist *headers = NULL;
    char *url = NULL;
    if (!s)
        s = ud_acquire_http_connection(c->execution, p.connection, &connection);
    if (!s)
        s = ud_connection_lease_authorize(connection, UD_ACCESS_NETWORK);
    if (!s)
        s = ud_http_connection_resource(connection, &resource);
    if (!s) {
        url = ud_strdup_n(c->memory, resource.data, resource.length);
        if (!url)
            s = UD_RESOURCE;
    }
    if (!s)
        s = validate_url(url);
    if (!s) {
        ensure_initialized();
        const curl_version_info_data *version = curl_version_info(CURLVERSION_NOW);
        if (!initialized || !version || version->version_num < 0x075500 ||
            !(version->features & CURL_VERSION_SSL) ||
            !(version->features & CURL_VERSION_ASYNCHDNS))
            s = UD_UNSUPPORTED;
    }
    if (!s)
        s = ud_http_connection_credentials(connection, &secret);
    if (!s)
        s = credentials(c->memory, secret, &headers);
    ud_free((void *)secret.data);
    if (!s)
        s = add_header(&headers, "Expect:");
    ud_bytes content = {.ctx = c->memory, .limit = 65536};
    if (!s && p.content_type.length) {
        s = ud_bytes_put(&content, "Content-Type: ", 14);
        if (!s)
            s = ud_bytes_put(&content, p.content_type.data, p.content_type.length);
        if (!s)
            s = ud_bytes_put(&content, "", 1);
        if (!s)
            s = add_header(&headers, (char *)content.data);
    }
    ud_bytes_clear(&content);
    if (!s)
        s = load_ca(c->execution, p.ca, &ca);
    long status = 0;
    if (!s)
        s = request(c, &p, connection, url, &ca, headers, &status);
    if (status && p.status_variable.length && !ud_is_cancelled(c->memory)) {
        ud_value value = {.type = UD_INT64, .integer = status};
        ud_status saved = ud_execution_set_variable(c->execution, p.status_variable, &value);
        if (!s)
            s = saved;
    }
    curl_slist_free_all(headers);
    ud_bytes_clear(&ca);
    ud_free(url);
    ud_status closed = ud_connection_lease_release(connection);
    if (!s)
        s = closed;
#endif
    ud_json_destroy(doc);
    return s;
}
ud_status ud_http_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"connection\"],"
        "\"properties\":{\"connection\":{\"type\":\"string\"},\"method\":{\"enum\":[\"GET\","
        "\"HEAD\",\"POST\",\"PUT\",\"PATCH\",\"DELETE\"]},\"body\":{\"type\":\"string\"},"
        "\"contentType\":{\"type\":\"string\"},\"caConnection\":{\"type\":\"string\"},"
        "\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000},"
        "\"maxResponseBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864},"
        "\"maxAttempts\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":3},"
        "\"retryDelayMs\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":60000},"
        "\"expectedStatus\":{\"type\":\"integer\",\"minimum\":100,\"maximum\":599},"
        "\"statusVariable\":{\"type\":\"string\"}}}";
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = {"http.request", 12},
                                 .name = {"HTTP Request", 12},
                                 .version = {"1.0.0", 5},
                                 .configuration_schema = {schema, sizeof(schema) - 1},
                                 .category = UD_COMPONENT_TASK,
                                 .synchronous_input = -1,
                                 .error_output = -1,
                                 .configure = configure,
                                 .invoke = invoke};
    return ud_registry_register(registry, &d);
}
