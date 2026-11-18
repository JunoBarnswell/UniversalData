#include "common.h"
#include "ud/message.h"
#include <proton/connection.h>
#include <proton/delivery.h>
#include <proton/disposition.h>
#include <proton/error.h>
#include <proton/link.h>
#include <proton/message.h>
#include <proton/proactor.h>
#include <proton/sasl.h>
#include <proton/session.h>
#include <proton/ssl.h>
#include <proton/terminus.h>
#include <proton/transport.h>
#include <stdatomic.h>
#include <stdio.h>

enum {
    AMQP_IDLE, AMQP_RECEIVER, AMQP_SENDER, AMQP_TEST,
    AMQP_MAX_CREDIT = 256, AMQP_MAX_MESSAGE = 16 * 1024 * 1024,
    AMQP_MAX_WINDOW = 64 * 1024 * 1024, AMQP_TIMEOUT = 10000, AMQP_POLL = 50,
    AMQP_MAX_ADDRESS = 1024, AMQP_MAX_HEADERS = 1024
};

typedef struct amqp_unsettled {
    pn_delivery_t *delivery;
    uint64_t handle;
} amqp_unsettled;

typedef struct amqp_state {
    const ud_host_api *host;
    void *control_user;
    ud_status (*check_io)(void *);
    char *hostname, *username, *password, *ca_file;
    char port[6];
    int tls, classic, mode, durable, remote_open, in_connection_batch, closing, closed;
    atomic_int cancelled;
    pn_proactor_t *proactor;
    pn_event_batch_t *events;
    pn_connection_t *connection;
    pn_link_t *link;
    pn_delivery_t *outgoing;
    uint64_t outcome, sequence;
    int outcome_settled;
    amqp_unsettled unsettled[AMQP_MAX_CREDIT];
    uint32_t credit, occupied, maximum, incoming_window;
    char *address, *logical_address;
    char *incoming;
    size_t received;
} amqp_state;

static ud_status check_io(amqp_state *p) {
    if (atomic_load(&p->cancelled) || p->host->cancelled(p->host->context))
        return UD_CANCELLED;
    return p->check_io ? p->check_io(p->control_user) : UD_OK;
}

static void cancel(void *state) {
    amqp_state *p = state;
    atomic_store(&p->cancelled, 1);
}

static void set_control(void *state, void *user, ud_status (*check)(void *)) {
    amqp_state *p = state;
    p->control_user = user;
    p->check_io = check;
}

static void end_batch(amqp_state *p) {
    if (p->events) {
        pn_proactor_done(p->proactor, p->events);
        p->events = NULL;
        p->in_connection_batch = 0;
    }
}

static void disconnect(amqp_state *p) {
    const ud_host_api *h = p->host;
    end_batch(p);
    if (p->proactor) {
        /* The peer CLOSE does not retire the transport. Dispatch all remaining
         * batches so the proactor no longer owns any socket or timer context. */
        pn_proactor_cancel_timeout(p->proactor);
        pn_proactor_disconnect(p->proactor, NULL);
        int inactive = 0;
        while (!inactive) {
            pn_event_batch_t *batch = pn_proactor_wait(p->proactor);
            pn_event_t *event;
            while ((event = pn_event_batch_next(batch)) != NULL)
                if (pn_event_type(event) == PN_PROACTOR_INACTIVE)
                    inactive = 1;
            pn_proactor_done(p->proactor, batch);
        }
        pn_proactor_free(p->proactor);
    }
    h->deallocate(p->address);
    h->deallocate(p->logical_address);
    h->deallocate(p->incoming);
    p->address = p->logical_address = p->incoming = NULL;
    p->proactor = NULL;
    p->connection = NULL;
    p->link = NULL;
    p->outgoing = NULL;
    p->outcome = 0;
    p->outcome_settled = p->remote_open = p->durable = 0;
    p->closing = p->closed = 0;
    p->received = p->occupied = 0;
    p->mode = AMQP_IDLE;
    memset(p->unsettled, 0, sizeof(p->unsettled));
}

static void destroy(void *state) {
    if (!state)
        return;
    amqp_state *p = state;
    const ud_host_api *h = p->host;
    disconnect(p);
    h->deallocate(p->hostname);
    h->deallocate(p->username);
    h->deallocate(p->password);
    h->deallocate(p->ca_file);
    h->deallocate(p);
}

static ud_status read_text(const ud_host_api *h, const ud_json *doc, const char *name, char **out,
                           uint32_t maximum, int required) {
    const ud_json *node = h->json_find(doc, name, (uint32_t)strlen(name));
    if (!node)
        return required ? UD_SCHEMA : UD_OK;
    const char *data;
    uint32_t length;
    if (h->json_kind(node) != UD_JSON_STRING || h->json_text(node, &data, &length) ||
        !length || length > maximum || memchr(data, 0, length))
        return UD_SCHEMA;
    *out = plugin_string(h, data, length);
    return *out ? UD_OK : UD_RESOURCE;
}

static int valid_hostname(const char *name) {
    if (name[0] == '.' || name[0] == '-')
        return 0;
    /* Accept DNS or IPv4 without delimiters that can change the authorized URI. */
    for (; *name; ++name)
        if (!((*name >= 'a' && *name <= 'z') || (*name >= 'A' && *name <= 'Z') ||
              (*name >= '0' && *name <= '9') || *name == '.' || *name == '-'))
            return 0;
    return 1;
}

static ud_status initialize(const ud_host_api *h, const char *config, uint32_t length, void **out) {
    static const char *const keys[] = {"variant", "host", "port", "tls", "username", "password",
                                       "caFile"};
    ud_json *doc = NULL;
    ud_status status = h->json_parse(h->context, config, length, &doc, NULL);
    if (status)
        return status;
    status = h->json_keys(doc, keys, sizeof(keys) / sizeof(*keys));
    amqp_state *p = NULL;
    if (!status) {
        p = h->allocate(h->context, sizeof(*p));
        if (!p)
            status = UD_RESOURCE;
    }
    if (status) {
        h->json_destroy(doc);
        return status;
    }
    memset(p, 0, sizeof(*p));
    atomic_init(&p->cancelled, 0);
    p->host = h;
    p->control_user = h->control_user;
    p->check_io = h->check_io;
    char *variant = NULL;
    status = read_text(h, doc, "variant", &variant, 7, 1);
    if (!status && strcmp(variant, "classic") && strcmp(variant, "artemis"))
        status = UD_SCHEMA;
    if (!status)
        p->classic = !strcmp(variant, "classic");
    h->deallocate(variant);
    if (!status)
        status = read_text(h, doc, "host", &p->hostname, 253, 1);
    if (!status && !valid_hostname(p->hostname))
        status = UD_SCHEMA;
    if (!status)
        status = read_text(h, doc, "username", &p->username, 1024, 0);
    if (!status)
        status = read_text(h, doc, "password", &p->password, 4096, 0);
    if (!status)
        status = read_text(h, doc, "caFile", &p->ca_file, 4096, 0);
    const ud_json *tls = h->json_find(doc, "tls", 3), *port = h->json_find(doc, "port", 4);
    const char *value;
    uint32_t n;
    p->tls = 1;
    if (!status && tls) {
        if (h->json_kind(tls) != UD_JSON_BOOL || h->json_text(tls, &value, &n))
            status = UD_SCHEMA;
        else
            p->tls = n == 4;
    }
    if (!status && ((!!p->username != !!p->password) || (!p->tls && p->ca_file)))
        status = UD_SCHEMA;
    if (!status && port) {
        unsigned number = 0;
        if (h->json_kind(port) != UD_JSON_NUMBER || h->json_text(port, &value, &n) ||
            !n || n > 5)
            status = UD_SCHEMA;
        for (uint32_t i = 0; !status && i < n; ++i) {
            if (value[i] < '0' || value[i] > '9')
                status = UD_SCHEMA;
            else
                number = number * 10 + (unsigned)(value[i] - '0');
        }
        if (!status && (!number || number > 65535))
            status = UD_SCHEMA;
        if (!status)
            snprintf(p->port, sizeof(p->port), "%u", number);
    } else if (!status) {
        memcpy(p->port, p->tls ? "5671" : "5672", 5);
    }
    h->json_destroy(doc);
    if (status)
        destroy(p);
    else
        *out = p;
    return status;
}

static ud_status remote_failure(pn_condition_t *condition) {
    const char *name = pn_condition_get_name(condition);
    if (name && (!strcmp(name, "amqp:unauthorized-access") || !strcmp(name, "amqp:authentication-error")))
        return UD_AUTH;
    if (name && !strcmp(name, "amqp:resource-locked"))
        return UD_BUSY;
    if (name && !strcmp(name, "amqp:link:message-size-exceeded"))
        return UD_OVERFLOW;
    return UD_IO;
}

/* Stop on connection events so callbacks act within the owning Proton event batch. */
static ud_status next_event(amqp_state *p, int64_t deadline) {
    for (;;) {
        ud_status status = check_io(p);
        if (status)
            return status;
        pn_event_t *event = p->events ? pn_event_batch_next(p->events) : NULL;
        if (!event) {
            end_batch(p);
            int64_t left = deadline - pn_proactor_now_64();
            if (left <= 0) {
                p->events = pn_proactor_get(p->proactor);
                if (!p->events)
                    return UD_TIMEOUT;
            } else {
                pn_proactor_set_timeout(p->proactor, (pn_millis_t)(left < AMQP_POLL ? left : AMQP_POLL));
                p->events = pn_proactor_wait(p->proactor);
            }
            continue;
        }
        p->in_connection_batch = pn_event_connection(event) == p->connection;
        switch (pn_event_type(event)) {
        case PN_CONNECTION_REMOTE_OPEN:
        case PN_LINK_REMOTE_OPEN:
            if (p->link == pn_event_link(event))
                p->remote_open = 1;
            return UD_OK;
        case PN_DELIVERY:
            if (p->outgoing && pn_event_delivery(event) == p->outgoing) {
                p->outcome = pn_delivery_remote_state(p->outgoing);
                p->outcome_settled = pn_delivery_settled(p->outgoing);
            }
            return UD_OK;
        case PN_LINK_REMOTE_CLOSE:
        case PN_LINK_REMOTE_DETACH:
            if (p->closing && !pn_condition_is_set(pn_link_remote_condition(pn_event_link(event))))
                return UD_OK;
            return remote_failure(pn_link_remote_condition(pn_event_link(event)));
        case PN_SESSION_REMOTE_CLOSE:
            if (p->closing && !pn_condition_is_set(pn_session_remote_condition(pn_event_session(event))))
                return UD_OK;
            return remote_failure(pn_session_remote_condition(pn_event_session(event)));
        case PN_CONNECTION_REMOTE_CLOSE:
            if (p->closing && !pn_condition_is_set(pn_connection_remote_condition(p->connection))) {
                p->closed = 1;
                return UD_OK;
            }
            return remote_failure(pn_connection_remote_condition(pn_event_connection(event)));
        case PN_TRANSPORT_ERROR:
        case PN_TRANSPORT_CLOSED:
            if (pn_sasl_outcome(pn_sasl(pn_event_transport(event))) == PN_SASL_AUTH)
                return UD_AUTH;
            return remote_failure(pn_transport_condition(pn_event_transport(event)));
        case PN_PROACTOR_TIMEOUT:
            if (pn_proactor_now_64() >= deadline)
                return UD_TIMEOUT;
            break;
        case PN_PROACTOR_INACTIVE:
            return UD_IO;
        default:
            if (p->in_connection_batch)
                return UD_OK;
        }
    }
}

static void close_connection(amqp_state *p) {
    int64_t deadline = pn_proactor_now_64() + AMQP_TIMEOUT;
    ud_status status = check_io(p);
    if (!status && !p->in_connection_batch) {
        pn_connection_wake(p->connection);
        status = next_event(p, deadline);
    }
    if (!status) {
        /* Closing the connection leaves a durable topic source intact on both brokers. */
        p->closing = 1;
        pn_connection_close(p->connection);
        while (!status && !p->closed)
            status = next_event(p, deadline);
    }
    disconnect(p);
}

static ud_status open_transport(amqp_state *p, pn_transport_t *transport) {
    pn_sasl_t *sasl = pn_sasl(transport);
    if (!sasl)
        return UD_RESOURCE;
    pn_sasl_allowed_mechs(sasl, p->username ? "PLAIN" : "ANONYMOUS");
    pn_sasl_set_allow_insecure_mechs(sasl, !p->tls);
    if (!p->tls)
        return UD_OK;
    pn_ssl_domain_t *domain = pn_ssl_domain(PN_SSL_MODE_CLIENT);
    if (!domain)
        return UD_UNSUPPORTED;
    pn_ssl_t *ssl = pn_ssl(transport);
    int result = !ssl ? -1 : pn_ssl_domain_set_peer_authentication(domain, PN_SSL_VERIFY_PEER_NAME, NULL);
    if (!result && p->ca_file)
        result = pn_ssl_domain_set_trusted_ca_db(domain, p->ca_file);
    if (!result)
        result = pn_ssl_init(ssl, domain, NULL);
    if (!result)
        result = pn_ssl_set_peer_hostname(ssl, p->hostname);
    pn_ssl_domain_free(domain);
    return result ? UD_AUTH : UD_OK;
}

static ud_status connect_broker(amqp_state *p, const char *container, const char *link_name,
                                const ud_message_subscription *subscription, int32_t kind) {
    ud_status status = check_io(p);
    char endpoint[280];
    int n = snprintf(endpoint, sizeof(endpoint), "%s://%s:%s",
                     p->tls ? "amqps" : "amqp", p->hostname, p->port);
    if (!status)
        status = p->host->authorize_endpoint
                     ? p->host->authorize_endpoint(p->control_user, (ud_string){endpoint, (uint32_t)n})
                     : UD_AUTH;
    if (status)
        return status;
    p->proactor = pn_proactor();
    pn_connection_t *connection = pn_connection();
    pn_transport_t *transport = pn_transport();
    status = p->proactor && connection && transport ? open_transport(p, transport) : UD_RESOURCE;
    if (!status) {
        p->connection = connection;
        pn_connection_set_container(connection, container);
        pn_connection_set_hostname(connection, p->hostname);
        if (p->username) {
            pn_connection_set_user(connection, p->username);
            pn_connection_set_password(connection, p->password);
        }
        pn_transport_set_idle_timeout(transport, 30000);
        uint32_t window = subscription ? p->incoming_window : 65536;
        pn_transport_set_max_frame(transport, window < 65536 ? window : 65536);
        if (link_name) {
            pn_session_t *session = pn_session(connection);
            if (session) {
                p->link = subscription ? pn_receiver(session, link_name) : pn_sender(session, link_name);
                pn_session_set_incoming_capacity(session, window);
                pn_session_open(session);
            }
            if (!p->link)
                status = UD_RESOURCE;
        }
    }
    if (!status && p->link) {
        pn_terminus_t *terminus = subscription ? pn_link_source(p->link) : pn_link_target(p->link);
        int failed = pn_terminus_set_address(terminus, p->address);
        if (!failed && !p->classic) {
            pn_data_t *capabilities = pn_terminus_capabilities(terminus);
            failed = pn_data_put_array(capabilities, false, PN_SYMBOL) ||
                     !pn_data_enter(capabilities) ||
                     pn_data_put_symbol(capabilities,
                                        pn_bytes(5, kind == UD_MESSAGE_TOPIC ? "topic" : "queue")) ||
                     !pn_data_exit(capabilities);
        }
        pn_link_set_snd_settle_mode(p->link, PN_SND_UNSETTLED);
        pn_link_set_rcv_settle_mode(p->link, PN_RCV_FIRST);
        if (subscription) {
            pn_link_set_max_message_size(p->link, p->maximum);
            pn_terminus_set_distribution_mode(terminus, kind == UD_MESSAGE_TOPIC ?
                                             PN_DIST_MODE_COPY : PN_DIST_MODE_MOVE);
            if (p->durable) {
                pn_terminus_set_durability(terminus, PN_DELIVERIES);
                pn_terminus_set_expiry_policy(terminus, PN_EXPIRE_NEVER);
            }
            if (!failed && subscription->filter.length) {
                pn_data_t *filter = pn_terminus_filter(terminus);
                static const char selector[] = "apache.org:selector-filter:string";
                pn_bytes_t descriptor = pn_bytes(sizeof(selector) - 1, selector);
                failed = pn_data_put_map(filter) || !pn_data_enter(filter) ||
                         pn_data_put_symbol(filter, descriptor) || pn_data_put_described(filter) ||
                         !pn_data_enter(filter) || pn_data_put_symbol(filter, descriptor) ||
                         pn_data_put_string(filter, pn_bytes(subscription->filter.length,
                                                            subscription->filter.data)) ||
                         !pn_data_exit(filter) || !pn_data_exit(filter);
            }
        }
        if (failed)
            status = UD_RESOURCE;
    }
    if (status) {
        if (connection)
            pn_connection_free(connection);
        if (transport)
            pn_transport_free(transport);
        return status;
    }
    if (p->link)
        pn_link_open(p->link);
    pn_connection_open(connection);
    char address[PN_MAX_ADDR];
    pn_proactor_addr(address, sizeof(address), p->hostname, p->port);
    pn_proactor_connect2(p->proactor, connection, transport, address);
    int64_t deadline = pn_proactor_now_64() + AMQP_TIMEOUT;
    do {
        status = next_event(p, deadline);
    } while (!status && !p->remote_open);
    if (status)
        return status;
    if (p->link) {
        pn_terminus_t *remote = subscription ? pn_link_remote_source(p->link) : pn_link_remote_target(p->link);
        if (!pn_terminus_get_address(remote)) {
            do {
                status = next_event(p, deadline);
            } while (!status);
            return status == UD_TIMEOUT ? UD_IO : status;
        }
        if (subscription && pn_link_remote_snd_settle_mode(p->link) == PN_SND_SETTLED)
            return UD_UNSUPPORTED;
        if (subscription && subscription->filter.length && !pn_data_size(pn_terminus_filter(remote)))
            return UD_UNSUPPORTED;
    }
    return UD_OK;
}

static ud_status unique_container(amqp_state *p, char container[36]) {
    static const char hex[] = "0123456789abcdef";
    uint8_t bytes[16];
    ud_status status = p->host->random_bytes(p->host->context, bytes, sizeof(bytes));
    if (status)
        return status;
    memcpy(container, "ud-", 3);
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        container[3 + 2 * i] = hex[bytes[i] >> 4];
        container[4 + 2 * i] = hex[bytes[i] & 15];
    }
    container[35] = 0;
    return UD_OK;
}

static ud_status connector_test(void *state) {
    amqp_state *p = state;
    if (p->mode != AMQP_IDLE)
        return UD_BUSY;
    char container[36];
    ud_status status = unique_container(p, container);
    if (!status) {
        p->mode = AMQP_TEST;
        status = connect_broker(p, container, NULL, NULL, 0);
        if (status)
            disconnect(p);
        else
            close_connection(p);
    }
    return status;
}

static int valid_text(ud_string s) {
    return s.data && s.length && s.length <= AMQP_MAX_ADDRESS && !memchr(s.data, 0, s.length);
}

static int valid_destination(ud_string s) {
    if (!valid_text(s) || memchr(s.data, ',', s.length) || memchr(s.data, '?', s.length) ||
        memchr(s.data, '*', s.length) || memchr(s.data, '>', s.length) || memchr(s.data, '#', s.length))
        return 0;
    for (uint32_t i = 0; i + 1 < s.length; ++i)
        if (s.data[i] == ':' && (s.data[i + 1] == ':' || s.data[i + 1] == '/'))
            return 0;
    return 1;
}

static ud_status set_destination(amqp_state *p, const ud_message_target *target, ud_string shared) {
    if (target->kind != UD_MESSAGE_QUEUE && target->kind != UD_MESSAGE_TOPIC)
        return UD_UNSUPPORTED;
    if (target->routing_key.length || !valid_destination(target->name) ||
        (shared.length && !valid_destination(shared)))
        return UD_INVALID;
    const char *prefix = p->classic ? (target->kind == UD_MESSAGE_TOPIC && !shared.length ?
                                      "topic://" : "queue://") : "";
    const char *name = p->classic && shared.length ? shared.data : target->name.data;
    uint32_t length = p->classic && shared.length ? shared.length : target->name.length;
    char address[AMQP_MAX_ADDRESS * 2 + 10];
    int n = snprintf(address, sizeof(address), "%s%.*s%s%.*s", prefix, (int)length, name,
                     !p->classic && shared.length ? "::" : "",
                     p->classic ? 0 : (int)shared.length, shared.length ? shared.data : "");
    p->address = plugin_string(p->host, address, (uint32_t)n);
    p->logical_address = plugin_string(p->host, target->name.data, target->name.length);
    return p->address && p->logical_address ? UD_OK : UD_RESOURCE;
}

static ud_status subscribe(void *state, const ud_message_subscription *options) {
    amqp_state *p = state;
    if (p->mode != AMQP_IDLE)
        return UD_BUSY;
    if ((options->group.length && !valid_text(options->group)) ||
        (options->subscription.length && !valid_text(options->subscription)) ||
        (options->filter.length && !valid_text(options->filter)))
        return UD_INVALID;
    if ((options->source.kind == UD_MESSAGE_QUEUE && (options->group.length || options->subscription.length)) ||
        (options->subscription.length && !options->group.length))
        return UD_INVALID;
    ud_string shared = options->subscription.length ? (ud_string){0} : options->group;
    int ephemeral = options->source.kind == UD_MESSAGE_TOPIC && !options->group.length;
    if (options->initial_position != (ephemeral ? UD_MESSAGE_POSITION_LATEST : UD_MESSAGE_POSITION_REQUIRE))
        return UD_UNSUPPORTED;
    p->maximum = options->max_message_bytes;
    uint64_t budget = options->max_in_flight_bytes;
    uint32_t prefetch = options->prefetch;
    if (!p->maximum || p->maximum > AMQP_MAX_MESSAGE || budget < p->maximum ||
        budget > AMQP_MAX_WINDOW || !prefetch || prefetch > AMQP_MAX_CREDIT)
        return UD_INVALID;
    p->incoming_window = (uint32_t)(budget < 512 ? 512 : budget);
    uint64_t by_bytes = budget / p->maximum;
    p->credit = prefetch < by_bytes ? prefetch : (uint32_t)by_bytes;
    ud_status status = set_destination(p, &options->source, shared);
    p->durable = options->subscription.length != 0;
    char container[4 * AMQP_MAX_ADDRESS + 5], link_name[AMQP_MAX_ADDRESS + 1];
    if (!status && p->durable) {
        /* A broker durable identity must never repoint a subscription on another topic. */
        static const char hex[] = "0123456789abcdef";
        const ud_string identity[] = {options->group, options->source.name};
        char *next = container;
        *next++ = 'u';
        *next++ = 'd';
        for (size_t part = 0; part < sizeof(identity) / sizeof(*identity); ++part) {
            *next++ = '-';
            for (uint32_t i = 0; i < identity[part].length; ++i) {
                uint8_t value = (uint8_t)identity[part].data[i];
                *next++ = hex[value >> 4];
                *next++ = hex[value & 15];
            }
        }
        *next = 0;
        memcpy(link_name, options->subscription.data, options->subscription.length);
        link_name[options->subscription.length] = 0;
    } else if (!status) {
        status = unique_container(p, container);
        memcpy(link_name, "ud-receiver", 12);
    }
    if (!status) {
        p->incoming = p->host->allocate(p->host->context, p->maximum);
        if (!p->incoming)
            status = UD_RESOURCE;
    }
    if (!status) {
        p->mode = AMQP_RECEIVER;
        status = connect_broker(p, container, link_name, options,
                                shared.length ? UD_MESSAGE_QUEUE : options->source.kind);
    }
    if (!status)
        pn_link_flow(p->link, (int)p->credit);
    else
        disconnect(p);
    return status;
}
static void clear_message(const ud_host_api *h, ud_message *m) {
    for (uint32_t i = 0; i < m->header_count; ++i) {
        h->deallocate((void *)m->headers[i].name.data);
        h->deallocate((void *)m->headers[i].value.data);
    }
    h->deallocate(m->headers);
    h->deallocate((void *)m->id.data);
    h->deallocate((void *)m->address.data);
    h->deallocate((void *)m->payload.data);
    h->deallocate((void *)m->key.data);
    memset(m, 0, sizeof(*m));
}

static ud_status copy_bytes(const ud_host_api *h, pn_bytes_t value, ud_string *out) {
    if (value.size > UINT32_MAX)
        return UD_OVERFLOW;
    *out = (ud_string){plugin_bytes(h, value.start, (uint32_t)value.size), (uint32_t)value.size};
    return out->data ? UD_OK : UD_RESOURCE;
}

static ud_status decode_id(const ud_host_api *h, pn_msgid_t id, ud_string *out) {
    char number[24], hex[32];
    static const char digits[] = "0123456789abcdef";
    switch (id.type) {
    case PN_NULL:
        return UD_OK;
    case PN_STRING:
        return copy_bytes(h, id.u.as_bytes, out);
    case PN_ULONG: {
        int n = snprintf(number, sizeof(number), "%llu", (unsigned long long)id.u.as_ulong);
        return copy_bytes(h, pn_bytes((size_t)n, number), out);
    }
    case PN_UUID:
        for (size_t i = 0; i < sizeof(id.u.as_uuid.bytes); ++i) {
            uint8_t value = (uint8_t)id.u.as_uuid.bytes[i];
            hex[2 * i] = digits[value >> 4];
            hex[2 * i + 1] = digits[value & 15];
        }
        return copy_bytes(h, pn_bytes(sizeof(hex), hex), out);
    case PN_BINARY:
        return copy_bytes(h, id.u.as_bytes, out);
    default:
        return UD_SCHEMA;
    }
}

static ud_status decode_bytes(const ud_host_api *h, pn_data_t *data, ud_string *out) {
    switch (pn_data_type(data)) {
    case PN_NULL:
        return UD_OK;
    case PN_BINARY:
        return copy_bytes(h, pn_data_get_binary(data), out);
    case PN_STRING:
        return copy_bytes(h, pn_data_get_string(data), out);
    default:
        return UD_UNSUPPORTED;
    }
}

/* Proton's message decoder retains the last body section. Data sections carry
 * consecutive bytes, while an AMQP Value supplies a single nullable body. */
static ud_status decode_payload(amqp_state *p, ud_string *out) {
    pn_data_t *section = pn_data(0);
    if (!section)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    size_t offset = 0;
    char *assembled = NULL;
    int body = 0;
    while (!status && offset < p->received) {
        pn_data_clear(section);
        ssize_t consumed = pn_data_decode(section, p->incoming + offset, p->received - offset);
        pn_data_rewind(section);
        if (consumed <= 0 || !pn_data_next(section) || pn_data_type(section) != PN_DESCRIBED ||
            !pn_data_enter(section) || !pn_data_next(section)) {
            status = UD_PARSE;
            break;
        }
        offset += (size_t)consumed;
        uint64_t descriptor = pn_data_get_ulong(section);
        if (pn_data_type(section) == PN_SYMBOL) {
            pn_bytes_t symbol = pn_data_get_symbol(section);
            if (symbol.size == 16 && !memcmp(symbol.start, "amqp:data:binary", 16))
                descriptor = 0x75;
            else if (symbol.size == 23 && !memcmp(symbol.start, "amqp:amqp-sequence:list", 23))
                descriptor = 0x76;
            else if (symbol.size == 17 && !memcmp(symbol.start, "amqp:amqp-value:*", 17))
                descriptor = 0x77;
        }
        if (descriptor < 0x75 || descriptor > 0x77)
            continue;
        if (!pn_data_next(section) || (body && (body != 0x75 || descriptor != 0x75))) {
            status = UD_PARSE;
            break;
        }
        if (descriptor == 0x76) {
            status = UD_UNSUPPORTED;
        } else if (descriptor == 0x77) {
            status = decode_bytes(p->host, section, out);
        } else if (pn_data_type(section) != PN_BINARY) {
            status = UD_PARSE;
        } else {
            pn_bytes_t bytes = pn_data_get_binary(section);
            if (!body) {
                status = copy_bytes(p->host, bytes, out);
            } else {
                if (!assembled) {
                    assembled = p->host->allocate(p->host->context, p->received);
                    if (!assembled) {
                        status = UD_RESOURCE;
                        break;
                    }
                    memcpy(assembled, out->data, out->length);
                    p->host->deallocate((void *)out->data);
                    out->data = assembled;
                }
                if (bytes.size)
                    memcpy(assembled + out->length, bytes.start, bytes.size);
                out->length += (uint32_t)bytes.size;
            }
        }
        body = (int)descriptor;
    }
    pn_data_free(section);
    return status;
}

static const char key_annotation[] = "x-opt-ud-key";

static ud_status decode_key(const ud_host_api *h, pn_data_t *annotations, ud_string *out) {
    pn_data_rewind(annotations);
    if (!pn_data_next(annotations))
        return UD_OK;
    if (pn_data_type(annotations) != PN_MAP || !pn_data_enter(annotations))
        return UD_SCHEMA;
    while (pn_data_next(annotations)) {
        pn_bytes_t key = pn_data_get_symbol(annotations);
        if (!pn_data_next(annotations))
            return UD_SCHEMA;
        if (key.size == sizeof(key_annotation) - 1 &&
            !memcmp(key.start, key_annotation, sizeof(key_annotation) - 1))
            return decode_bytes(h, annotations, out);
    }
    return UD_OK;
}

static ud_status decode_headers(const ud_host_api *h, pn_data_t *properties, ud_message *out) {
    pn_data_rewind(properties);
    if (!pn_data_next(properties))
        return UD_OK;
    if (pn_data_type(properties) != PN_MAP)
        return UD_SCHEMA;
    size_t values = pn_data_get_map(properties);
    if (!values)
        return UD_OK;
    if (values % 2 || values / 2 > AMQP_MAX_HEADERS ||
        !pn_data_enter(properties))
        return UD_SCHEMA;
    out->headers = h->allocate(h->context, values / 2 * sizeof(*out->headers));
    if (!out->headers)
        return UD_RESOURCE;
    memset(out->headers, 0, values / 2 * sizeof(*out->headers));
    for (size_t i = 0; i < values / 2; ++i) {
        ud_message_header *entry = &out->headers[out->header_count++];
        if (!pn_data_next(properties) || pn_data_type(properties) != PN_STRING)
            return UD_SCHEMA;
        pn_bytes_t key = pn_data_get_string(properties);
        if (!key.size || memchr(key.start, 0, key.size))
            return UD_SCHEMA;
        ud_status status = copy_bytes(h, key, &entry->name);
        if (status)
            return status;
        if (!pn_data_next(properties))
            return UD_SCHEMA;
        status = decode_bytes(h, properties, &entry->value);
        if (status)
            return status;
    }
    return UD_OK;
}

static ud_status decode_message(amqp_state *p, ud_message *out) {
    pn_message_t *message = pn_message();
    if (!message)
        return UD_RESOURCE;
    ud_status status = pn_message_decode(message, p->incoming, p->received) ? UD_PARSE : UD_OK;
    const ud_host_api *h = p->host;
    if (!status)
        status = decode_payload(p, &out->payload);
    if (!status)
        status = decode_key(h, pn_message_annotations(message), &out->key);
    if (!status)
        status = decode_headers(h, pn_message_properties(message), out);
    if (!status)
        status = decode_id(h, pn_message_get_id(message), &out->id);
    if (!status) {
        status = copy_bytes(h, pn_bytes(strlen(p->logical_address), p->logical_address), &out->address);
        int64_t timestamp = pn_message_get_creation_time(message);
        out->timestamp_ms = timestamp ? timestamp : -1;
        out->redelivered = pn_message_get_delivery_count(message) > 0;
        out->partition = -1;
        out->offset = -1;
    }
    pn_message_free(message);
    if (status)
        clear_message(h, out);
    return status;
}

static ud_status read_delivery(amqp_state *p, pn_delivery_t *delivery, ud_message *out) {
    if (pn_delivery_aborted(delivery)) {
        p->received = 0;
        pn_delivery_settle(delivery);
        pn_link_flow(p->link, 1);
        return UD_MORE;
    }
    if (pn_delivery_settled(delivery))
        return UD_INCOMPATIBLE;
    size_t pending = pn_delivery_pending(delivery);
    if (pending > p->maximum - p->received)
        return UD_OVERFLOW;
    if (pending) {
        ssize_t n = pn_link_recv(p->link, p->incoming + p->received, pending);
        if (n <= 0)
            return UD_IO;
        p->received += (size_t)n;
    }
    if (pn_delivery_partial(delivery) || pn_delivery_pending(delivery))
        return UD_MORE;
    if (p->sequence == UINT64_MAX || p->occupied >= p->credit)
        return UD_OVERFLOW;
    ud_status status = decode_message(p, out);
    if (status)
        return status;
    for (uint32_t i = 0; i < p->credit; ++i) {
        if (!p->unsettled[i].delivery) {
            out->delivery = ++p->sequence;
            p->unsettled[i] = (amqp_unsettled){delivery, out->delivery};
            ++p->occupied;
            break;
        }
    }
    p->received = 0;
    pn_link_advance(p->link);
    return UD_OK;
}

static ud_status receive(void *state, uint32_t timeout_ms, ud_message *out) {
    amqp_state *p = state;
    if (p->mode != AMQP_RECEIVER)
        return UD_INVALID;
    int64_t deadline = pn_proactor_now_64() + timeout_ms;
    ud_status status = check_io(p);
    while (!status) {
        pn_delivery_t *delivery = p->in_connection_batch ? pn_link_current(p->link) : NULL;
        if (delivery) {
            status = read_delivery(p, delivery, out);
            if (status != UD_MORE)
                break;
        }
        status = next_event(p, deadline);
        if (status == UD_TIMEOUT)
            return UD_BUSY;
    }
    if (status)
        disconnect(p);
    return status;
}

static ud_status acknowledge(void *state, const uint64_t *handles, uint32_t count) {
    amqp_state *p = state;
    if (p->mode != AMQP_RECEIVER || count > p->occupied)
        return UD_INVALID;
    uint32_t slots[AMQP_MAX_CREDIT];
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t index = 0;
        while (index < p->credit &&
               (!p->unsettled[index].delivery || p->unsettled[index].handle != handles[i]))
            ++index;
        if (index == p->credit)
            return UD_INVALID;
        for (uint32_t j = 0; j < i; ++j)
            if (slots[j] == index)
                return UD_INVALID;
        slots[i] = index;
    }
    ud_status status = check_io(p);
    if (!status && !p->in_connection_batch) {
        pn_connection_wake(p->connection);
        status = next_event(p, pn_proactor_now_64() + AMQP_TIMEOUT);
    }
    if (status) {
        disconnect(p);
        return status;
    }
    for (uint32_t i = 0; i < count; ++i)
        if (pn_delivery_settled(p->unsettled[slots[i]].delivery)) {
            disconnect(p);
            return UD_IO;
        }
    for (uint32_t i = 0; i < count; ++i) {
        amqp_unsettled *item = &p->unsettled[slots[i]];
        pn_delivery_update(item->delivery, PN_ACCEPTED);
        pn_delivery_settle(item->delivery);
        *item = (amqp_unsettled){0};
    }
    p->occupied -= count;
    pn_link_flow(p->link, (int)count);
    end_batch(p);
    return UD_OK;
}

static ud_status unsubscribe(void *state) {
    amqp_state *p = state;
    if (p->mode == AMQP_RECEIVER)
        close_connection(p);
    return UD_OK;
}

static int put_bytes(pn_data_t *data, ud_string bytes) {
    return bytes.data ? pn_data_put_binary(data, pn_bytes(bytes.length, bytes.data)) :
                        pn_data_put_null(data);
}

static ud_status encode_message(amqp_state *p, const ud_message *input, pn_message_t *message,
                                int32_t kind, size_t *capacity) {
    uint64_t length = 512 + strlen(p->address) + (uint64_t)input->payload.length +
                      input->key.length + input->id.length;
    if ((!input->payload.data && input->payload.length) || (!input->key.data && input->key.length) ||
        (!input->id.data && input->id.length) || (!input->headers && input->header_count) ||
        input->header_count > AMQP_MAX_HEADERS)
        return UD_INVALID;
    for (uint32_t i = 0; i < input->header_count; ++i) {
        const ud_message_header *entry = &input->headers[i];
        if (!valid_text(entry->name) || (!entry->value.data && entry->value.length))
            return UD_INVALID;
        length += 16 + (uint64_t)entry->name.length + entry->value.length;
    }
    if (length > AMQP_MAX_MESSAGE)
        return UD_OVERFLOW;
    *capacity = (size_t)length;
    int failed = pn_message_set_address(message, p->address) || pn_message_set_durable(message, true);
    if (!failed && input->id.data) {
        pn_msgid_t id = {.type = PN_STRING, .u.as_bytes = pn_bytes(input->id.length, input->id.data)};
        failed = pn_message_set_id(message, id);
    }
    if (!failed && input->timestamp_ms > 0)
        failed = pn_message_set_creation_time(message, input->timestamp_ms);
    if (!failed && input->payload.data) {
        pn_message_set_inferred(message, true);
        failed = put_bytes(pn_message_body(message), input->payload);
    }
    if (!failed && (input->key.data || !p->classic)) {
        pn_data_t *annotations = pn_message_annotations(message);
        failed = pn_data_put_map(annotations) || !pn_data_enter(annotations);
        if (!failed && input->key.data)
            failed = pn_data_put_symbol(annotations, pn_bytes(sizeof(key_annotation) - 1, key_annotation)) ||
                     put_bytes(annotations, input->key);
        if (!failed && !p->classic)
            failed = pn_data_put_symbol(annotations, pn_bytes(14, "x-opt-jms-dest")) ||
                     pn_data_put_byte(annotations, kind == UD_MESSAGE_TOPIC ? 1 : 0);
        if (!failed)
            failed = !pn_data_exit(annotations);
    }
    if (!failed && input->header_count) {
        pn_data_t *properties = pn_message_properties(message);
        failed = pn_data_put_map(properties) || !pn_data_enter(properties);
        for (uint32_t i = 0; !failed && i < input->header_count; ++i) {
            const ud_message_header *entry = &input->headers[i];
            for (uint32_t j = 0; j < i; ++j)
                if (entry->name.length == input->headers[j].name.length &&
                    !memcmp(entry->name.data, input->headers[j].name.data, entry->name.length))
                    return UD_INVALID;
            failed = pn_data_put_string(properties, pn_bytes(entry->name.length, entry->name.data)) ||
                     put_bytes(properties, entry->value);
        }
        if (!failed)
            failed = !pn_data_exit(properties);
    }
    return failed ? UD_RESOURCE : UD_OK;
}

static ud_status publish_one(amqp_state *p, const ud_message *input, int32_t kind,
                             ud_message_result *result) {
    pn_message_t *message = pn_message();
    if (!message)
        return UD_RESOURCE;
    size_t length = 0;
    ud_status status = encode_message(p, input, message, kind, &length);
    char *encoded = NULL;
    if (!status) {
        encoded = p->host->allocate(p->host->context, length);
        if (!encoded)
            status = UD_RESOURCE;
        else if (pn_message_encode(message, encoded, &length))
            status = UD_OVERFLOW;
    }
    pn_message_free(message);
    int64_t deadline = pn_proactor_now_64() + AMQP_TIMEOUT;
    while (!status && (!p->in_connection_batch || pn_link_credit(p->link) <= 0))
        status = next_event(p, deadline);
    if (!status)
        status = check_io(p);
    if (!status && pn_link_remote_max_message_size(p->link) &&
        length > pn_link_remote_max_message_size(p->link))
        status = UD_OVERFLOW;
    if (!status && p->sequence == UINT64_MAX)
        status = UD_OVERFLOW;
    if (!status) {
        uint64_t tag = ++p->sequence;
        p->outgoing = pn_delivery(p->link, pn_dtag((const char *)&tag, sizeof(tag)));
        if (!p->outgoing)
            status = UD_RESOURCE;
    }
    if (!status) {
        if (pn_link_send(p->link, encoded, length) != (ssize_t)length || !pn_link_advance(p->link)) {
            pn_delivery_abort(p->outgoing);
            status = UD_IO;
        } else {
            result->state = UD_MESSAGE_UNCERTAIN;
            p->outcome = 0;
            p->outcome_settled = 0;
            while (!status && (!p->outcome || p->outcome == PN_RECEIVED) && !p->outcome_settled)
                status = next_event(p, deadline);
            if (p->outcome == PN_ACCEPTED) {
                result->state = UD_MESSAGE_ACCEPTED;
                status = UD_OK;
            } else if (p->outcome == PN_REJECTED || p->outcome == PN_RELEASED ||
                       p->outcome == PN_MODIFIED) {
                result->state = UD_MESSAGE_REJECTED;
                status = p->outcome == PN_REJECTED ?
                             remote_failure(pn_disposition_condition(pn_delivery_remote(p->outgoing))) :
                             UD_TRANSIENT;
            } else {
                status = UD_COMMIT_UNKNOWN;
            }
            if (result->state != UD_MESSAGE_UNCERTAIN)
                pn_delivery_settle(p->outgoing);
        }
        p->outgoing = NULL;
    }
    p->host->deallocate(encoded);
    return status;
}

static ud_status publish(void *state, const ud_message_target *target, const ud_message *messages,
                         uint32_t count, ud_message_result *results) {
    amqp_state *p = state;
    ud_status status = p->mode == AMQP_RECEIVER ? UD_BUSY : UD_OK;
    /* One sender session is scoped to one broker address. Replacing it never replays a send. */
    if (!status && p->mode == AMQP_SENDER)
        disconnect(p);
    if (!status)
        status = set_destination(p, target, (ud_string){0});
    char container[36];
    if (!status)
        status = unique_container(p, container);
    if (!status) {
        p->mode = AMQP_SENDER;
        status = connect_broker(p, container, "ud-sender", NULL, target->kind);
    }
    ud_status first_error = status;
    for (uint32_t i = 0; i < count; ++i) {
        results[i] = (ud_message_result){UD_MESSAGE_NOT_SENT, status};
        if (status)
            continue;
        ud_status sent = publish_one(p, &messages[i], target->kind, &results[i]);
        results[i].result = sent;
        if (sent && !first_error)
            first_error = sent;
        if (sent && results[i].state != UD_MESSAGE_REJECTED)
            status = sent;
    }
    if (p->mode == AMQP_SENDER && !status)
        close_connection(p);
    else if (p->mode != AMQP_RECEIVER)
        disconnect(p);
    return first_error;
}

static const ud_message_api messaging = {
    .struct_size = sizeof(messaging),
    .abi_version = UD_PLUGIN_ABI,
    .subscribe = subscribe,
    .receive = receive,
    .acknowledge = acknowledge,
    .unsubscribe = unsubscribe,
    .publish = publish
};

static const ud_connector_api connector = {
    .struct_size = sizeof(connector),
    .abi_version = UD_PLUGIN_ABI,
    .capabilities = UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_PUBLISH,
    .test = connector_test,
    .cancel = cancel,
    .set_control = set_control,
    .message = &messaging
};

UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){
        sizeof(*out), UD_PLUGIN_ABI, "org.universaldata.connector.activemq", UD_SDK_VERSION,
        UD_PLUGIN_CONNECTOR, UD_CAP_THREAD_SAFE | UD_CAP_READ | UD_CAP_WRITE,
        initialize, destroy, plugin_shutdown, &connector, NULL,
        sizeof(ud_field), sizeof(ud_value)
    };
    return UD_OK;
}
