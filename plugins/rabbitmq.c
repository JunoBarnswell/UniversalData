#include "common.h"
#include "ud/message.h"
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/framing.h>
#include <rabbitmq-c/ssl_socket.h>
#include <rabbitmq-c/tcp_socket.h>
#include <stdatomic.h>
#include <stdio.h>
#include <time.h>
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <windows.h>
#else
#include <sys/time.h>
#endif

enum {
    RABBIT_CHANNEL = 1,
    RABBIT_FRAME_SIZE = 131072,
    RABBIT_HEARTBEAT = 15,
    RABBIT_IO_TIMEOUT_MS = 5000,
    RABBIT_CONTROL_INTERVAL_MS = 50,
    RABBIT_BASIC_PUBLISH_METHOD_ID = 40,
    RABBIT_CONTENT_HEADER_OVERHEAD = 22,
    RABBIT_CONSUMER = 1,
    RABBIT_PUBLISHER = 2
};

static const char key_header[] = "x-ud-key";
static const char null_header[] = "x-ud-null-payload";
static const char timestamp_header[] = "x-ud-timestamp-ms";
static atomic_uint_fast64_t next_handle = 1;

typedef struct rabbit_credentials {
    char *host, *virtual_host, *username, *password, *ca_file;
    unsigned port;
    int tls;
} rabbit_credentials;

typedef struct rabbit_delivery {
    uint64_t handle, tag, bytes;
    int selected;
} rabbit_delivery;

typedef struct rabbit_state {
    ud_host_api host;
    amqp_connection_state_t connection;
    atomic_int cancelled;
    void *control_user;
    ud_status (*check)(void *);
    int purpose;
    uint64_t published;
    ud_string queue;
    rabbit_delivery *unacked;
    uint32_t prefetch, outstanding, max_message;
    uint64_t max_in_flight, in_flight;
    ud_message incoming;
    uint64_t incoming_tag, incoming_bytes;
    uint32_t body_size, body_read;
    int header_ready, null_payload;
} rabbit_state;

static ud_status control(rabbit_state *p) {
    if (atomic_load(&p->cancelled) || p->host.cancelled(p->host.context))
        return UD_CANCELLED;
    return p->check ? p->check(p->control_user) : UD_OK;
}

static void cancel(void *state) {
    rabbit_state *p = state;
    atomic_store(&p->cancelled, 1);
}

static void set_control(void *state, void *user, ud_status (*check)(void *)) {
    rabbit_state *p = state;
    p->control_user = user;
    p->check = check;
}

static void release_message(const ud_host_api *host, ud_message *message) {
    host->deallocate((void *)message->payload.data);
    host->deallocate((void *)message->key.data);
    host->deallocate((void *)message->id.data);
    host->deallocate((void *)message->address.data);
    for (uint32_t i = 0; i < message->header_count; i++) {
        host->deallocate((void *)message->headers[i].name.data);
        host->deallocate((void *)message->headers[i].value.data);
    }
    host->deallocate(message->headers);
    *message = (ud_message){0};
}

static void retire(rabbit_state *p) {
    if (p->connection) {
        /* Dropping the channel requeues outstanding deliveries, including a partially read body. */
        amqp_destroy_connection(p->connection);
        p->connection = NULL;
    }
    release_message(&p->host, &p->incoming);
    p->host.deallocate((void *)p->queue.data);
    p->queue = (ud_string){0};
    p->host.deallocate(p->unacked);
    p->unacked = NULL;
    p->outstanding = 0;
    p->in_flight = 0;
}

static void destroy(void *state) {
    rabbit_state *p = state;
    retire(p);
    p->host.deallocate(p);
}

static ud_status library_status(int result) {
    switch (result) {
    case AMQP_STATUS_OK:
        return UD_OK;
    case AMQP_STATUS_TIMEOUT:
        return UD_TIMEOUT;
    case AMQP_STATUS_NO_MEMORY:
        return UD_RESOURCE;
    case AMQP_STATUS_TABLE_TOO_BIG:
        return UD_OVERFLOW;
    case AMQP_STATUS_BAD_AMQP_DATA:
    case AMQP_STATUS_UNKNOWN_CLASS:
    case AMQP_STATUS_UNKNOWN_METHOD:
        return UD_CORRUPT;
    case AMQP_STATUS_SSL_PEER_VERIFY_FAILED:
    case AMQP_STATUS_SSL_HOSTNAME_VERIFY_FAILED:
        return UD_AUTH;
    case AMQP_STATUS_BROKER_UNSUPPORTED_SASL_METHOD:
    case AMQP_STATUS_SSL_UNIMPLEMENTED:
        return UD_UNSUPPORTED;
    default:
        return UD_IO;
    }
}

static uint16_t close_code(amqp_method_t method) {
    if (method.id == AMQP_CHANNEL_CLOSE_METHOD)
        return ((amqp_channel_close_t *)method.decoded)->reply_code;
    if (method.id == AMQP_CONNECTION_CLOSE_METHOD)
        return ((amqp_connection_close_t *)method.decoded)->reply_code;
    return 0;
}

static ud_status broker_status(uint16_t code) {
    switch (code) {
    case AMQP_ACCESS_REFUSED:
    case AMQP_NOT_ALLOWED:
        return UD_AUTH;
    case AMQP_NOT_FOUND:
        return UD_INVALID;
    case AMQP_RESOURCE_LOCKED:
        return UD_TRANSIENT;
    case AMQP_PRECONDITION_FAILED:
        return UD_SCHEMA;
    default:
        return UD_IO;
    }
}

static ud_status rpc_status(amqp_rpc_reply_t reply) {
    switch (reply.reply_type) {
    case AMQP_RESPONSE_NORMAL:
        return UD_OK;
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        return library_status(reply.library_error);
    case AMQP_RESPONSE_SERVER_EXCEPTION:
        return broker_status(close_code(reply.reply));
    default:
        return UD_IO;
    }
}

static ud_status option_text(const ud_host_api *h, const ud_json *object, const char *name,
                             const char *fallback, uint32_t maximum, char **out) {
    const ud_json *value = h->json_find(object, name, (uint32_t)strlen(name));
    const char *text = fallback;
    uint32_t length = text ? (uint32_t)strlen(text) : 0;
    if (value && (h->json_kind(value) != UD_JSON_STRING ||
                  h->json_text(value, &text, &length) || !length || length > maximum ||
                  memchr(text, 0, length)))
        return UD_SCHEMA;
    if (!text)
        return UD_OK;
    *out = plugin_string(h, text, length);
    return *out ? UD_OK : UD_RESOURCE;
}

static ud_status read_credentials(const ud_host_api *h, const ud_json *object,
                                  rabbit_credentials *c) {
    static const char *const keys[] = {
        "host", "port", "virtualHost", "username", "password", "tls", "caFile",
    };
    ud_status status = h->json_keys(object, keys, sizeof(keys) / sizeof(*keys));
    const ud_json *tls = h->json_find(object, "tls", 3);
    if (!status && tls) {
        const char *value;
        uint32_t length;
        if (h->json_kind(tls) != UD_JSON_BOOL || h->json_text(tls, &value, &length))
            return UD_SCHEMA;
        c->tls = length == 4;
    }
    c->port = c->tls ? 5671 : 5672;
    const ud_json *port = h->json_find(object, "port", 4);
    if (!status && port) {
        const char *value;
        uint32_t length;
        if (h->json_kind(port) != UD_JSON_NUMBER || h->json_text(port, &value, &length) ||
            !length || length > 5)
            return UD_SCHEMA;
        c->port = 0;
        for (uint32_t i = 0; i < length; i++) {
            if (value[i] < '0' || value[i] > '9')
                return UD_SCHEMA;
            c->port = c->port * 10 + (unsigned)(value[i] - '0');
        }
        if (!c->port || c->port > UINT16_MAX)
            return UD_SCHEMA;
    }
    if (!status)
        status = option_text(h, object, "host", NULL, 253, &c->host);
    if (!status)
        status = option_text(h, object, "virtualHost", "/", 255, &c->virtual_host);
    if (!status)
        status = option_text(h, object, "username", NULL, 255, &c->username);
    if (!status)
        status = option_text(h, object, "password", NULL, 4096, &c->password);
    if (!status)
        status = option_text(h, object, "caFile", NULL, 4096, &c->ca_file);
    if (!status && (!c->host || !c->username || !c->password || (!!c->ca_file != c->tls)))
        status = UD_SCHEMA;
    return status;
}

static ud_status authorize_endpoint(rabbit_state *p, const rabbit_credentials *c) {
    char endpoint[272];
    int ipv6 = strchr(c->host, ':') != NULL;
    for (const char *ch = c->host; *ch; ch++) {
        int digit = *ch >= '0' && *ch <= '9';
        int letter = (*ch >= 'A' && *ch <= 'Z') || (*ch >= 'a' && *ch <= 'z');
        int hex = (*ch >= 'A' && *ch <= 'F') || (*ch >= 'a' && *ch <= 'f');
        if (!(digit || *ch == '.' || (ipv6 ? (hex || *ch == ':') : (letter || *ch == '-'))))
            return UD_SCHEMA;
    }
    int length = snprintf(endpoint, sizeof(endpoint), ipv6 ? "%s://[%s]:%u" : "%s://%s:%u",
                          c->tls ? "amqps" : "amqp", c->host, c->port);
    if (length < 0 || (size_t)length >= sizeof(endpoint))
        return UD_SCHEMA;
    if (!p->host.authorize_endpoint)
        return UD_AUTH;
    return p->host.authorize_endpoint(p->host.control_user,
                                      (ud_string){endpoint, (uint32_t)length});
}

static ud_status connect_broker(rabbit_state *p, const rabbit_credentials *c) {
    const struct timeval timeout = {RABBIT_IO_TIMEOUT_MS / 1000, 0};
    ud_status status = control(p);
    if (!status)
        status = authorize_endpoint(p, c);
    if (status)
        return status;
    p->connection = amqp_new_connection();
    if (!p->connection)
        return UD_RESOURCE;
    amqp_socket_t *socket =
        c->tls ? amqp_ssl_socket_new(p->connection) : amqp_tcp_socket_new(p->connection);
    if (!socket)
        return UD_RESOURCE;
    if (c->tls) {
        amqp_ssl_socket_set_verify_peer(socket, 1);
        amqp_ssl_socket_set_verify_hostname(socket, 1);
        status = library_status(amqp_ssl_socket_set_cacert(socket, c->ca_file));
        if (status)
            return status;
    }
    status = library_status(amqp_socket_open_noblock(socket, c->host, (int)c->port, &timeout));
    if (!status)
        status = control(p);
    if (!status)
        status = library_status(amqp_set_handshake_timeout(p->connection, &timeout));
    if (!status)
        status = library_status(amqp_set_rpc_timeout(p->connection, &timeout));
    if (!status)
        status = rpc_status(amqp_login(p->connection, c->virtual_host, 1, RABBIT_FRAME_SIZE,
                                       RABBIT_HEARTBEAT, AMQP_SASL_METHOD_PLAIN, c->username,
                                       c->password));
    if (!status)
        status = control(p);
    if (!status) {
        amqp_channel_open(p->connection, RABBIT_CHANNEL);
        status = rpc_status(amqp_get_rpc_reply(p->connection));
    }
    return status ? status : control(p);
}

static ud_status initialize(const ud_host_api *h, const char *configuration, uint32_t length,
                            void **instance) {
    rabbit_state *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    memset(p, 0, sizeof(*p));
    p->host = *h;
    p->check = h->check_io;
    p->control_user = h->control_user;
    atomic_init(&p->cancelled, 0);
    rabbit_credentials credentials = {0};
    ud_json *configuration_json = NULL;
    ud_status status = h->json_parse(h->context, configuration, length, &configuration_json, NULL);
    if (!status)
        status = read_credentials(h, configuration_json, &credentials);
    if (!status)
        status = connect_broker(p, &credentials);
    h->json_destroy(configuration_json);
    h->deallocate(credentials.host);
    h->deallocate(credentials.virtual_host);
    h->deallocate(credentials.username);
    h->deallocate(credentials.password);
    h->deallocate(credentials.ca_file);
    if (status) {
        destroy(p);
        return h->report(h->context, status, "RabbitMQ connection initialization failed");
    }
    *instance = p;
    return UD_OK;
}

static ud_status test(void *state) {
    rabbit_state *p = state;
    return p->connection ? control(p) : UD_IO;
}

static int short_text(ud_string text, int required) {
    return (!required || text.length) && text.length <= UINT8_MAX &&
           (!text.length || (text.data && !memchr(text.data, 0, text.length)));
}

static amqp_bytes_t bytes(ud_string value) {
    return (amqp_bytes_t){value.length, (void *)value.data};
}

static ud_status copy_bytes(rabbit_state *p, amqp_bytes_t input, ud_string *output) {
    if (input.len > UINT32_MAX)
        return UD_OVERFLOW;
    char *copy = plugin_bytes(&p->host, input.bytes, (uint32_t)input.len);
    if (!copy)
        return UD_RESOURCE;
    *output = (ud_string){copy, (uint32_t)input.len};
    return UD_OK;
}

static int header_matches(amqp_bytes_t name, const char *expected) {
    return name.len == strlen(expected) && !memcmp(name.bytes, expected, name.len);
}

static uint64_t monotonic_ms(void) {
#ifdef _WIN32
    return GetTickCount64();
#else
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000 + (uint64_t)now.tv_nsec / 1000000;
#endif
}

static ud_status read_frame(rabbit_state *p, uint64_t deadline, amqp_frame_t *frame) {
    for (;;) {
        ud_status status = control(p);
        if (status)
            return status;
        uint64_t now = monotonic_ms();
        uint64_t remaining = deadline > now ? deadline - now : 0;
        if (remaining > RABBIT_CONTROL_INTERVAL_MS)
            remaining = RABBIT_CONTROL_INTERVAL_MS;
        struct timeval timeout = {0, (long)remaining * 1000};
        int result = amqp_simple_wait_frame_noblock(p->connection, frame, &timeout);
        if (result == AMQP_STATUS_TIMEOUT) {
            if (monotonic_ms() >= deadline)
                return UD_BUSY;
            continue;
        }
        if (result != AMQP_STATUS_OK)
            return library_status(result);
        if (frame->frame_type == AMQP_FRAME_METHOD) {
            amqp_method_t method = frame->payload.method;
            if (method.id == AMQP_CHANNEL_CLOSE_METHOD || method.id == AMQP_CONNECTION_CLOSE_METHOD)
                return broker_status(close_code(method));
        }
        return frame->channel == RABBIT_CHANNEL ? UD_OK : UD_CORRUPT;
    }
}

static ud_status subscribe(void *state, const ud_message_subscription *options) {
    rabbit_state *p = state;
    if (!p->connection)
        return UD_IO;
    if (p->purpose)
        return UD_BUSY;
    if (options->source.kind != UD_MESSAGE_QUEUE || options->group.length ||
        options->subscription.length || options->filter.length || options->source.routing_key.length ||
        options->initial_position != UD_MESSAGE_POSITION_REQUIRE)
        return UD_UNSUPPORTED;
    if (!short_text(options->source.name, 1) || !options->prefetch ||
        options->prefetch > UINT16_MAX || !options->max_message_bytes ||
        options->max_message_bytes > options->max_in_flight_bytes)
        return UD_INVALID;
    ud_status status = control(p);
    if (!status)
        status = copy_bytes(p, bytes(options->source.name), &p->queue);
    if (!status) {
        p->unacked = p->host.allocate(p->host.context, options->prefetch * sizeof(*p->unacked));
        if (!p->unacked)
            status = UD_RESOURCE;
    }
    if (!status) {
        amqp_basic_qos(p->connection, RABBIT_CHANNEL, 0, (uint16_t)options->prefetch, 0);
        status = rpc_status(amqp_get_rpc_reply(p->connection));
    }
    if (!status)
        status = control(p);
    if (!status) {
        amqp_basic_consume(p->connection, RABBIT_CHANNEL, bytes(p->queue), amqp_empty_bytes, 0, 0,
                           0, amqp_empty_table);
        status = rpc_status(amqp_get_rpc_reply(p->connection));
    }
    if (status) {
        retire(p);
        return p->host.report(p->host.context, status, "RabbitMQ queue subscription failed");
    }
    p->purpose = RABBIT_CONSUMER;
    p->prefetch = options->prefetch;
    p->max_message = options->max_message_bytes;
    p->max_in_flight = options->max_in_flight_bytes;
    return UD_OK;
}

static ud_status header_value(rabbit_state *p, const amqp_table_entry_t *entry,
                              ud_string *out) {
    switch (entry->value.kind) {
    case AMQP_FIELD_KIND_VOID:
        return UD_OK;
    case AMQP_FIELD_KIND_BYTES:
    case AMQP_FIELD_KIND_UTF8:
        return copy_bytes(p, entry->value.value.bytes, out);
    default:
        return UD_UNSUPPORTED;
    }
}

static ud_status read_headers(rabbit_state *p, const amqp_basic_properties_t *properties) {
    ud_message *message = &p->incoming;
    ud_status status = UD_OK;
    if (properties->_flags & AMQP_BASIC_MESSAGE_ID_FLAG)
        status = copy_bytes(p, properties->message_id, &message->id);
    if (properties->_flags & AMQP_BASIC_TIMESTAMP_FLAG) {
        if (properties->timestamp > (uint64_t)INT64_MAX / 1000)
            return UD_OVERFLOW;
        message->timestamp_ms = (int64_t)properties->timestamp * 1000;
    }
    if (status || !(properties->_flags & AMQP_BASIC_HEADERS_FLAG))
        return status;
    const amqp_table_t *table = &properties->headers;
    if (!table->num_entries)
        return UD_OK;
    size_t entries_size = (size_t)table->num_entries * sizeof(*message->headers);
    message->headers = p->host.allocate(p->host.context, entries_size);
    if (!message->headers)
        return UD_RESOURCE;
    memset(message->headers, 0, entries_size);
    int has_precision = 0;
    for (int i = 0; i < table->num_entries; i++) {
        const amqp_table_entry_t *entry = table->entries + i;
        if (header_matches(entry->key, key_header)) {
            if (entry->value.kind != AMQP_FIELD_KIND_BYTES || message->key.data)
                return UD_CORRUPT;
            status = copy_bytes(p, entry->value.value.bytes, &message->key);
        } else if (header_matches(entry->key, null_header)) {
            if (entry->value.kind != AMQP_FIELD_KIND_VOID || p->null_payload || p->body_size)
                return UD_CORRUPT;
            p->null_payload = 1;
        } else if (header_matches(entry->key, timestamp_header)) {
            if (entry->value.kind != AMQP_FIELD_KIND_I64 || has_precision)
                return UD_CORRUPT;
            int64_t timestamp = entry->value.value.i64;
            if (message->timestamp_ms < 0 || timestamp < 0 ||
                timestamp / 1000 != message->timestamp_ms / 1000)
                return UD_CORRUPT;
            has_precision = 1;
            message->timestamp_ms = timestamp;
        } else {
            ud_message_header *header = message->headers + message->header_count++;
            status = copy_bytes(p, entry->key, &header->name);
            if (!status)
                status = header_value(p, entry, &header->value);
        }
        if (status)
            return status;
    }
    return UD_OK;
}

static ud_status receive_header(rabbit_state *p, const amqp_frame_t *frame) {
    if (frame->frame_type != AMQP_FRAME_HEADER ||
        frame->payload.properties.class_id != AMQP_BASIC_CLASS)
        return UD_CORRUPT;
    if (frame->payload.properties.body_size > p->max_message)
        return UD_OVERFLOW;
    p->body_size = (uint32_t)frame->payload.properties.body_size;
    ud_status status = read_headers(p, frame->payload.properties.decoded);
    if (status)
        return status;
    const ud_message *m = &p->incoming;
    p->incoming_bytes = (uint64_t)p->body_size + m->id.length + m->address.length + m->key.length;
    for (uint32_t i = 0; i < m->header_count; i++)
        p->incoming_bytes += (uint64_t)m->headers[i].name.length + m->headers[i].value.length;
    if (p->incoming_bytes > p->max_message)
        return UD_OVERFLOW;
    p->header_ready = 1;
    return UD_OK;
}

static ud_status receive_content(rabbit_state *p, uint64_t deadline) {
    if (p->incoming_bytes > p->max_in_flight - p->in_flight)
        return UD_BUSY;
    if (!p->null_payload && !p->incoming.payload.data) {
        size_t size = p->body_size ? p->body_size : 1;
        char *payload = p->host.allocate(p->host.context, size);
        if (!payload)
            return UD_RESOURCE;
        p->incoming.payload = (ud_string){payload, p->body_size};
    }
    while (p->body_read < p->body_size) {
        amqp_frame_t frame;
        ud_status status = read_frame(p, deadline, &frame);
        if (status)
            return status;
        if (frame.frame_type != AMQP_FRAME_BODY || !frame.payload.body_fragment.len ||
            frame.payload.body_fragment.len > p->body_size - p->body_read)
            return UD_CORRUPT;
        amqp_bytes_t fragment = frame.payload.body_fragment;
        memcpy((char *)p->incoming.payload.data + p->body_read, fragment.bytes, fragment.len);
        p->body_read += (uint32_t)fragment.len;
        amqp_maybe_release_buffers(p->connection);
    }
    return UD_OK;
}

static ud_status receive_frames(rabbit_state *p, uint64_t deadline) {
    amqp_frame_t frame;
    if (!p->incoming_tag) {
        ud_status status = read_frame(p, deadline, &frame);
        if (status)
            return status;
        if (frame.frame_type != AMQP_FRAME_METHOD)
            return UD_CORRUPT;
        if (frame.payload.method.id == AMQP_BASIC_CANCEL_METHOD)
            return UD_TRANSIENT;
        if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD)
            return UD_CORRUPT;
        const amqp_basic_deliver_t *delivery = frame.payload.method.decoded;
        p->incoming_tag = delivery->delivery_tag;
        if (!p->incoming_tag)
            return UD_CORRUPT;
        p->incoming.redelivered = delivery->redelivered;
        p->incoming.partition = -1;
        p->incoming.offset = -1;
        p->incoming.timestamp_ms = -1;
        status = copy_bytes(p, bytes(p->queue), &p->incoming.address);
        if (status)
            return status;
        amqp_maybe_release_buffers(p->connection);
    }
    if (!p->header_ready) {
        ud_status status = read_frame(p, deadline, &frame);
        if (status)
            return status;
        status = receive_header(p, &frame);
        if (status)
            return status;
        amqp_maybe_release_buffers(p->connection);
    }
    return receive_content(p, deadline);
}

static ud_status receive(void *state, uint32_t timeout_ms, ud_message *out) {
    rabbit_state *p = state;
    if (!p->connection || p->purpose != RABBIT_CONSUMER)
        return UD_INVALID;
    ud_status status = control(p);
    if (!status && p->outstanding == p->prefetch)
        return UD_BUSY;
    if (!status)
        status = receive_frames(p, monotonic_ms() + timeout_ms);
    if (status == UD_BUSY)
        return UD_BUSY;
    if (status) {
        retire(p);
        return p->host.report(p->host.context, status, "RabbitMQ message receive failed");
    }
    uint64_t handle = atomic_fetch_add(&next_handle, 1);
    p->unacked[p->outstanding++] = (rabbit_delivery){handle, p->incoming_tag, p->incoming_bytes, 0};
    p->in_flight += p->incoming_bytes;
    p->incoming.delivery = handle;
    *out = p->incoming;
    p->incoming = (ud_message){0};
    p->incoming_tag = p->incoming_bytes = 0;
    p->header_ready = p->null_payload = 0;
    p->body_size = p->body_read = 0;
    return UD_OK;
}

static rabbit_delivery *find_delivery(rabbit_state *p, uint64_t handle) {
    uint32_t first = 0, end = p->outstanding;
    while (first < end) {
        uint32_t mid = first + (end - first) / 2;
        if (p->unacked[mid].handle < handle)
            first = mid + 1;
        else
            end = mid;
    }
    return first < p->outstanding && p->unacked[first].handle == handle ? p->unacked + first : NULL;
}

static ud_status acknowledge(void *state, const uint64_t *handles, uint32_t count) {
    rabbit_state *p = state;
    if (!p->connection || p->purpose != RABBIT_CONSUMER || count > p->outstanding)
        return UD_INVALID;
    uint32_t selected = 0;
    for (; selected < count; selected++) {
        rabbit_delivery *delivery = find_delivery(p, handles[selected]);
        if (!delivery || delivery->selected)
            break;
        delivery->selected = 1;
    }
    if (selected < count) {
        for (uint32_t i = 0; i < selected; i++)
            find_delivery(p, handles[i])->selected = 0;
        return UD_INVALID;
    }
    ud_status status = UD_OK;
    uint32_t remaining = 0, sent = 0;
    for (uint32_t i = 0; i < p->outstanding; i++) {
        rabbit_delivery delivery = p->unacked[i];
        if (!delivery.selected) {
            p->unacked[remaining++] = delivery;
            continue;
        }
        status = control(p);
        if (status) {
            if (sent)
                status = UD_COMMIT_UNKNOWN;
            break;
        }
        if (amqp_basic_ack(p->connection, RABBIT_CHANNEL, delivery.tag, 0) != AMQP_STATUS_OK) {
            status = UD_COMMIT_UNKNOWN;
            break;
        }
        sent++;
        p->in_flight -= delivery.bytes;
    }
    if (status) {
        retire(p);
        return p->host.report(p->host.context, status, "RabbitMQ acknowledgement stopped");
    }
    p->outstanding = remaining;
    return UD_OK;
}

static ud_status unsubscribe(void *state) {
    rabbit_state *p = state;
    if (p->purpose != RABBIT_CONSUMER)
        return UD_INVALID;
    retire(p);
    return UD_OK;
}

static ud_status publish_target(const ud_message_target *target, amqp_bytes_t *exchange,
                                amqp_bytes_t *routing) {
    if (!short_text(target->name, 1))
        return UD_INVALID;
    switch (target->kind) {
    case UD_MESSAGE_QUEUE:
        if (target->routing_key.length)
            return UD_INVALID;
        *exchange = amqp_empty_bytes;
        *routing = bytes(target->name);
        return UD_OK;
    case UD_MESSAGE_EXCHANGE:
        if (!short_text(target->routing_key, 0))
            return UD_INVALID;
        *exchange = bytes(target->name);
        *routing = bytes(target->routing_key);
        return UD_OK;
    default:
        return UD_UNSUPPORTED;
    }
}

static amqp_table_entry_t publish_header(amqp_bytes_t key, uint8_t kind, amqp_bytes_t value) {
    amqp_table_entry_t entry = {.key = key, .value = {.kind = kind}};
    entry.value.value.bytes = value;
    return entry;
}

static ud_status publish_properties(rabbit_state *p, const ud_message *message,
                                    amqp_basic_properties_t *properties) {
    if ((!message->payload.data && message->payload.length) ||
        (!message->key.data && message->key.length) || !short_text(message->id, 0) ||
        (message->header_count && !message->headers) || message->timestamp_ms < -1)
        return UD_INVALID;
    properties->_flags = AMQP_BASIC_DELIVERY_MODE_FLAG;
    properties->delivery_mode = 2;
    if (message->id.data) {
        properties->_flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
        properties->message_id = bytes(message->id);
    }
    int precision = message->timestamp_ms >= 0 && message->timestamp_ms % 1000;
    if (message->timestamp_ms >= 0) {
        properties->_flags |= AMQP_BASIC_TIMESTAMP_FLAG;
        properties->timestamp = (uint64_t)message->timestamp_ms / 1000;
    }
    uint64_t entry_count =
        (uint64_t)message->header_count + (message->key.data != NULL) + !message->payload.data +
        precision;
    if (!entry_count)
        return UD_OK;
    /* A header table fits a single content-header frame. Each entry needs its key length,
     * at least one byte of key and a field type before any value bytes. */
    if (entry_count > (uint64_t)amqp_get_frame_max(p->connection) / 3)
        return UD_OVERFLOW;
    /* Includes frame envelope, flags, delivery mode and the table length prefix. */
    uint64_t wire_size = RABBIT_CONTENT_HEADER_OVERHEAD + 1 + 4;
    if (message->id.data)
        wire_size += 1 + message->id.length;
    if (message->timestamp_ms >= 0)
        wire_size += 8;
    if (message->key.data)
        wire_size += sizeof(key_header) - 1 + 2 + 4 + (uint64_t)message->key.length;
    if (!message->payload.data)
        wire_size += sizeof(null_header) - 1 + 2;
    if (precision)
        wire_size += sizeof(timestamp_header) - 1 + 2 + 8;
    amqp_table_entry_t *entries =
        p->host.allocate(p->host.context, (size_t)entry_count * sizeof(*entries));
    if (!entries)
        return UD_RESOURCE;
    properties->_flags |= AMQP_BASIC_HEADERS_FLAG;
    properties->headers.entries = entries;
    for (uint32_t i = 0; i < message->header_count; i++) {
        const ud_message_header *header = message->headers + i;
        if (!short_text(header->name, 1) || (!header->value.data && header->value.length))
            return UD_INVALID;
        amqp_bytes_t key = bytes(header->name);
        if (header_matches(key, key_header) || header_matches(key, null_header) ||
            header_matches(key, timestamp_header))
            return UD_INVALID;
        wire_size += 2 + key.len;
        if (header->value.data)
            wire_size += 4 + (uint64_t)header->value.length;
        if (wire_size > (uint64_t)amqp_get_frame_max(p->connection))
            return UD_OVERFLOW;
        entries[i] = publish_header(key,
                                    header->value.data ? AMQP_FIELD_KIND_BYTES : AMQP_FIELD_KIND_VOID,
                                    bytes(header->value));
    }
    uint32_t i = message->header_count;
    if (message->key.data)
        entries[i++] = publish_header(amqp_cstring_bytes(key_header), AMQP_FIELD_KIND_BYTES,
                                      bytes(message->key));
    if (!message->payload.data)
        entries[i++] = publish_header(amqp_cstring_bytes(null_header), AMQP_FIELD_KIND_VOID,
                                      amqp_empty_bytes);
    if (precision)
        entries[i] = (amqp_table_entry_t){.key = amqp_cstring_bytes(timestamp_header),
                                         .value = {.kind = AMQP_FIELD_KIND_I64,
                                                   .value.i64 = message->timestamp_ms}};
    properties->headers.num_entries = (int)entry_count;
    return wire_size <= (uint64_t)amqp_get_frame_max(p->connection) ? UD_OK : UD_OVERFLOW;
}

static ud_status discard_return(rabbit_state *p, uint64_t deadline) {
    amqp_frame_t frame;
    ud_status status = read_frame(p, deadline, &frame);
    if (status)
        return status;
    if (frame.frame_type != AMQP_FRAME_HEADER ||
        frame.payload.properties.class_id != AMQP_BASIC_CLASS)
        return UD_CORRUPT;
    uint64_t remaining = frame.payload.properties.body_size;
    amqp_maybe_release_buffers(p->connection);
    while (remaining) {
        status = read_frame(p, deadline, &frame);
        if (status)
            return status;
        if (frame.frame_type != AMQP_FRAME_BODY || !frame.payload.body_fragment.len ||
            frame.payload.body_fragment.len > remaining)
            return UD_CORRUPT;
        remaining -= frame.payload.body_fragment.len;
        amqp_maybe_release_buffers(p->connection);
    }
    return UD_OK;
}

static ud_status await_confirm(rabbit_state *p, ud_message_result *result) {
    uint64_t deadline = monotonic_ms() + RABBIT_IO_TIMEOUT_MS;
    for (;;) {
        amqp_frame_t frame = {0};
        ud_status status = read_frame(p, deadline, &frame);
        if (status) {
            if (frame.frame_type == AMQP_FRAME_METHOD &&
                frame.payload.method.id == AMQP_CHANNEL_CLOSE_METHOD) {
                const amqp_channel_close_t *closed = frame.payload.method.decoded;
                if (closed->class_id == AMQP_BASIC_CLASS &&
                    closed->method_id == RABBIT_BASIC_PUBLISH_METHOD_ID &&
                    (closed->reply_code == AMQP_NOT_FOUND ||
                     closed->reply_code == AMQP_ACCESS_REFUSED))
                    *result = (ud_message_result){UD_MESSAGE_REJECTED, status};
            }
            return status;
        }
        if (frame.frame_type != AMQP_FRAME_METHOD)
            return UD_CORRUPT;
        amqp_method_t method = frame.payload.method;
        if (method.id == AMQP_BASIC_RETURN_METHOD) {
            if (result->state == UD_MESSAGE_REJECTED)
                return UD_CORRUPT;
            *result = (ud_message_result){UD_MESSAGE_REJECTED, UD_IO};
            amqp_maybe_release_buffers(p->connection);
            status = discard_return(p, deadline);
            if (status)
                return status;
            continue;
        }
        uint64_t tag;
        int multiple;
        if (method.id == AMQP_BASIC_ACK_METHOD) {
            const amqp_basic_ack_t *ack = method.decoded;
            tag = ack->delivery_tag;
            multiple = ack->multiple;
        } else if (method.id == AMQP_BASIC_NACK_METHOD) {
            const amqp_basic_nack_t *nack = method.decoded;
            tag = nack->delivery_tag;
            multiple = nack->multiple;
        } else {
            return UD_CORRUPT;
        }
        if (tag != p->published && !(multiple && !tag))
            return UD_CORRUPT;
        if (result->state != UD_MESSAGE_REJECTED) {
            *result = method.id == AMQP_BASIC_ACK_METHOD
                          ? (ud_message_result){UD_MESSAGE_ACCEPTED, UD_OK}
                          : (ud_message_result){UD_MESSAGE_REJECTED, UD_IO};
        }
        amqp_maybe_release_buffers(p->connection);
        return UD_OK;
    }
}

static ud_status publish(void *state, const ud_message_target *target, const ud_message *messages,
                         uint32_t count, ud_message_result *results) {
    rabbit_state *p = state;
    amqp_bytes_t exchange, routing;
    ud_status status = publish_target(target, &exchange, &routing);
    if (!status && !p->connection)
        status = UD_IO;
    if (!status && p->purpose == RABBIT_CONSUMER)
        status = UD_BUSY;
    if (!status)
        status = control(p);
    for (uint32_t i = 0; i < count; i++)
        results[i] = (ud_message_result){UD_MESSAGE_NOT_SENT, status};
    if (status || !count)
        return status;
    if (!p->purpose) {
        amqp_confirm_select(p->connection, RABBIT_CHANNEL);
        status = rpc_status(amqp_get_rpc_reply(p->connection));
        if (status) {
            for (uint32_t i = 0; i < count; i++)
                results[i].result = status;
            retire(p);
            return p->host.report(p->host.context, status, "RabbitMQ publisher confirm setup failed");
        }
        p->purpose = RABBIT_PUBLISHER;
    }
    ud_status first_error = UD_OK;
    uint32_t i = 0;
    for (; i < count; i++) {
        status = control(p);
        if (status)
            break;
        amqp_basic_properties_t properties = {0};
        status = publish_properties(p, messages + i, &properties);
        if (status) {
            p->host.deallocate(properties.headers.entries);
            results[i].result = status;
            if (!first_error)
                first_error = status;
            continue;
        }
        /* Keeping one outstanding publish disambiguates Basic.Return, which has no delivery tag. */
        int sent = amqp_basic_publish(p->connection, RABBIT_CHANNEL, exchange, routing, 1, 0,
                                      &properties, bytes(messages[i].payload));
        p->host.deallocate(properties.headers.entries);
        results[i] = (ud_message_result){UD_MESSAGE_UNCERTAIN, UD_COMMIT_UNKNOWN};
        p->published++;
        status = sent == AMQP_STATUS_OK ? await_confirm(p, results + i) : library_status(sent);
        if (status) {
            retire(p);
            status = results[i].state == UD_MESSAGE_UNCERTAIN ? UD_COMMIT_UNKNOWN : results[i].result;
            i++;
            break;
        }
        if (!first_error)
            first_error = results[i].result;
    }
    for (; i < count; i++)
        results[i].result = status;
    if (!status)
        status = first_error;
    return status ? p->host.report(p->host.context, status, "RabbitMQ publish was not fully accepted")
                  : UD_OK;
}

static const ud_message_api messaging = {
    .struct_size = sizeof(messaging),
    .abi_version = UD_PLUGIN_ABI,
    .subscribe = subscribe,
    .receive = receive,
    .acknowledge = acknowledge,
    .unsubscribe = unsubscribe,
    .publish = publish,
};

static const ud_connector_api connector = {
    .struct_size = sizeof(connector),
    .abi_version = UD_PLUGIN_ABI,
    .capabilities = UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_PUBLISH,
    .test = test,
    .cancel = cancel,
    .set_control = set_control,
    .message = &messaging,
};

UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){.struct_size = sizeof(*out),
                           .abi_version = UD_PLUGIN_ABI,
                           .id = "org.universaldata.connector.rabbitmq",
                           .version = UD_SDK_VERSION,
                           .roles = UD_PLUGIN_CONNECTOR,
                           .capabilities = UD_CAP_READ | UD_CAP_WRITE | UD_CAP_THREAD_SAFE,
                           .initialize = initialize,
                           .destroy = destroy,
                           .shutdown = plugin_shutdown,
                           .connector = &connector,
                           .field_size = sizeof(ud_field),
                           .value_size = sizeof(ud_value)};
    return UD_OK;
}
