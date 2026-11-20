#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <netdb.h>
#include <time.h>
#endif
#include "common.h"
#include "ud/message.h"
#include <librdkafka/rdkafka.h>
#include <stdatomic.h>
#include <stdio.h>

enum {
    KAFKA_LIMIT_RECORD = 16 * 1024 * 1024,
    KAFKA_LIMIT_TOTAL = 64 * 1024 * 1024,
    KAFKA_LIMIT_TOKENS = 256,
    KAFKA_LIMIT_HEADERS = 1024,
    KAFKA_LIMIT_TOPIC = 249,
    KAFKA_LIMIT_PARTITIONS = 65536,
    KAFKA_LIMIT_CLUSTER = 128,
    KAFKA_LIMIT_METADATA = 4096,
    KAFKA_TIMEOUT = 10000,
    KAFKA_POLL = 50
};

typedef struct kafka_token {
    uint64_t handle, bytes;
    int32_t partition;
    int64_t offset;
} kafka_token;

typedef struct kafka_partition_cut {
    int64_t end, low, latest;
    int assigned, reached, paused, metadata_seen;
} kafka_partition_cut;

typedef struct kafka_frontier {
    int64_t committed, start, candidate;
} kafka_frontier;

typedef struct kafka_state {
    const ud_host_api *host;
    void *control_user;
    ud_status (*check_io)(void *);
    char *servers, *username, *password, *ca_file, *topic, *transactional_id;
    int tls, assigned, transaction;
    int32_t initial_position, has_frozen_offset;
    int64_t frozen_offset;
    atomic_int cancelled, unauthorized;
    ud_status rebalance_error;
    rd_kafka_t *consumer, *producer;
    rd_kafka_message_t *pending;
    kafka_token tokens[KAFKA_LIMIT_TOKENS];
    kafka_partition_cut *cuts;
    uint32_t cut_count, cuts_remaining;
    kafka_frontier *frontier;
    uint32_t frontier_count, fixed_partition;
    int exclusive_transaction, frontier_frozen;
    uint64_t sequence, held, budget, assignment;
    uint32_t count, credit, maximum;
    char producer_cluster[KAFKA_LIMIT_CLUSTER + 1];
} kafka_state;

typedef struct kafka_delivery {
    ud_message_result result;
    int done;
} kafka_delivery;

static uint64_t now_ms(void) {
#ifdef _WIN32
    return GetTickCount64();
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
#endif
}

static int wait_ms(uint64_t deadline) {
    uint64_t now = now_ms();
    return now >= deadline ? 0 : (int)(deadline - now < KAFKA_POLL ? deadline - now : KAFKA_POLL);
}

static ud_status control(kafka_state *p) {
    if (atomic_load(&p->cancelled) || p->host->cancelled(p->host->context))
        return UD_CANCELLED;
    if (atomic_load(&p->unauthorized))
        return UD_AUTH;
    return p->check_io ? p->check_io(p->control_user) : UD_OK;
}

static void set_control(void *state, void *user, ud_status (*check)(void *)) {
    kafka_state *p = state;
    p->control_user = user;
    p->check_io = check;
}

static void cancel(void *state) {
    kafka_state *p = state;
    atomic_store(&p->cancelled, 1);
}

static ud_status kafka_status(rd_kafka_resp_err_t err) {
    switch (err) {
    case RD_KAFKA_RESP_ERR_NO_ERROR:
        return UD_OK;
    case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR__AUTHENTICATION:
    case RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED:
    case RD_KAFKA_RESP_ERR__SSL:
        return UD_AUTH;
    case RD_KAFKA_RESP_ERR__FENCED:
    case RD_KAFKA_RESP_ERR_PRODUCER_FENCED:
    case RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH:
    case RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID:
    case RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION:
    case RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID:
    case RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS:
    case RD_KAFKA_RESP_ERR__MAX_POLL_EXCEEDED:
    case RD_KAFKA_RESP_ERR_FENCED_MEMBER_EPOCH:
        return UD_FENCED;
    case RD_KAFKA_RESP_ERR__TIMED_OUT:
    case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
    case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
        return UD_TIMEOUT;
    case RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET:
    case RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE:
        return UD_INCOMPATIBLE;
    case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
        return UD_OVERFLOW;
    case RD_KAFKA_RESP_ERR__QUEUE_FULL:
        return UD_BUSY;
    case RD_KAFKA_RESP_ERR__TRANSPORT:
    case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
    case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
    case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
    case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
        return UD_TRANSIENT;
    case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
    case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
    case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
        return UD_INVALID;
    default:
        return UD_IO;
    }
}

static int valid_endpoint_host(const char *host, size_t n, int ipv6) {
    if (!n || n > 253)
        return 0;
    for (size_t i = 0; i < n; ++i) {
        char c = host[i];
        if (c >= '0' && c <= '9')
            continue;
        if (ipv6 ? (c != ':' && c != '.' && !(c >= 'a' && c <= 'f') && !(c >= 'A' && c <= 'F')) :
                   (c != '.' && c != '-' && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z')))
            return 0;
    }
    return 1;
}

static ud_status authorize(kafka_state *p, const char *host, size_t length, const char *service,
                          size_t service_len, int ipv6) {
    if (!valid_endpoint_host(host, length, ipv6) || !service_len || service_len > 5)
        return UD_SCHEMA;
    unsigned port = 0;
    for (size_t i = 0; i < service_len; ++i) {
        if (service[i] < '0' || service[i] > '9')
            return UD_SCHEMA;
        port = port * 10 + (unsigned)(service[i] - '0');
    }
    if (!port || port > 65535)
        return UD_SCHEMA;
    char endpoint[280];
    int n = snprintf(endpoint, sizeof(endpoint), "kafka%s://%s%.*s%s:%u", p->tls ? "s" : "",
                     ipv6 ? "[" : "", (int)length, host, ipv6 ? "]" : "", port);
    return p->host->authorize_endpoint ?
        p->host->authorize_endpoint(p->host->control_user, (ud_string){endpoint, (uint32_t)n}) :
        UD_AUTH;
}

static ud_status authorize_bootstrap(kafka_state *p) {
    const char *item = p->servers;
    for (;;) {
        const char *comma = strchr(item, ','), *end = comma ? comma : item + strlen(item);
        int ipv6 = *item == '[';
        const char *host = item + ipv6;
        const char *split = memchr(host, ipv6 ? ']' : ':', (size_t)(end - host));
        if (!split || (ipv6 && (split + 1 >= end || split[1] != ':')))
            return UD_SCHEMA;
        const char *service = split + 1 + ipv6;
        ud_status status = authorize(p, host, (size_t)(split - host), service,
                                     (size_t)(end - service), ipv6);
        if (status || !comma)
            return status;
        item = comma + 1;
    }
}

static int resolve(const char *node, const char *service, const struct addrinfo *hints,
                   struct addrinfo **result, void *opaque) {
    kafka_state *p = opaque;
    if (!node) {
        freeaddrinfo(*result);
        return 0;
    }
    if (!service || authorize(p, node, strlen(node), service, strlen(service), strchr(node, ':') != NULL)) {
        atomic_store(&p->unauthorized, 1);
        return EAI_FAIL;
    }
    if (atomic_load(&p->cancelled))
        return EAI_FAIL;
    return getaddrinfo(node, service, hints, result);
}

static ud_status config_text(const ud_host_api *h, const ud_json *doc, const char *name,
                             uint32_t limit, int required, char **out) {
    const ud_json *node = h->json_find(doc, name, (uint32_t)strlen(name));
    if (!node)
        return required ? UD_SCHEMA : UD_OK;
    const char *value;
    uint32_t n;
    if (h->json_kind(node) != UD_JSON_STRING || h->json_text(node, &value, &n) || !n ||
        n > limit || memchr(value, 0, n))
        return UD_SCHEMA;
    *out = plugin_string(h, value, n);
    return *out ? UD_OK : UD_RESOURCE;
}

static void invalidate(kafka_state *p) {
    ++p->assignment;
    p->assigned = 0;
    p->count = 0;
    p->held = 0;
    p->cuts_remaining = 0;
    for (uint32_t i = 0; i < p->cut_count; ++i) {
        p->cuts[i].assigned = p->cuts[i].reached = p->cuts[i].paused = 0;
        p->cuts[i].low = p->cuts[i].latest = -1;
    }
    if (p->pending)
        rd_kafka_message_destroy(p->pending);
    p->pending = NULL;
}

static void close_consumer(kafka_state *p) {
    if (p->consumer) {
        invalidate(p);
        (void)rd_kafka_consumer_close(p->consumer);
        rd_kafka_destroy_flags(p->consumer, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        p->consumer = NULL;
    }
    p->host->deallocate(p->topic);
    p->host->deallocate(p->cuts);
    p->host->deallocate(p->frontier);
    p->topic = NULL;
    p->cuts = NULL;
    p->cut_count = 0;
    p->frontier = NULL;
    p->frontier_count = 0;
    p->fixed_partition = 0;
    p->exclusive_transaction = 0;
    p->frontier_frozen = 0;
    p->rebalance_error = UD_OK;
}

static void close_producer(kafka_state *p) {
    if (p->producer)
        rd_kafka_destroy(p->producer);
    p->producer = NULL;
    p->producer_cluster[0] = 0;
    p->transaction = 0;
}

static void destroy(void *state) {
    kafka_state *p = state;
    if (!p)
        return;
    close_consumer(p);
    close_producer(p);
    const ud_host_api *h = p->host;
    h->deallocate(p->servers);
    h->deallocate(p->username);
    h->deallocate(p->password);
    h->deallocate(p->ca_file);
    h->deallocate(p->transactional_id);
    h->deallocate(p);
}

static ud_status initialize(const ud_host_api *h, const char *data, uint32_t length, void **out) {
    static const char *const keys[] = {"bootstrapServers", "tls", "username", "password", "caFile"};
    ud_json *doc = NULL;
    ud_status status = h->json_parse(h->context, data, length, &doc, NULL);
    if (status)
        return status;
    status = h->json_keys(doc, keys, sizeof(keys) / sizeof(*keys));
    kafka_state *p = status ? NULL : h->allocate(h->context, sizeof(*p));
    if (!p) {
        h->json_destroy(doc);
        return status ? status : UD_RESOURCE;
    }
    memset(p, 0, sizeof(*p));
    p->host = h;
    p->control_user = h->control_user;
    p->check_io = h->check_io;
    p->tls = 1;
    atomic_init(&p->cancelled, 0);
    atomic_init(&p->unauthorized, 0);
    status = config_text(h, doc, "bootstrapServers", 8192, 1, &p->servers);
    if (!status)
        status = config_text(h, doc, "username", 1024, 0, &p->username);
    if (!status)
        status = config_text(h, doc, "password", 4096, 0, &p->password);
    if (!status)
        status = config_text(h, doc, "caFile", 4096, 0, &p->ca_file);
    const ud_json *tls = h->json_find(doc, "tls", 3);
    if (!status && tls) {
        const char *value;
        uint32_t n;
        if (h->json_kind(tls) != UD_JSON_BOOL || h->json_text(tls, &value, &n))
            status = UD_SCHEMA;
        else
            p->tls = n == 4;
    }
    if (!status && ((!!p->username != !!p->password) || (!p->tls && p->ca_file)))
        status = UD_SCHEMA;
    h->json_destroy(doc);
    if (status)
        destroy(p);
    else
        *out = p;
    return status;
}

static int conf_text(rd_kafka_conf_t *conf, const char *name, const char *value) {
    return rd_kafka_conf_set(conf, name, value, NULL, 0) == RD_KAFKA_CONF_OK;
}

static int conf_number(rd_kafka_conf_t *conf, const char *name, uint32_t value) {
    char text[16];
    snprintf(text, sizeof(text), "%u", value);
    return conf_text(conf, name, text);
}

static void client_log(const rd_kafka_t *rk, int level, const char *facility, const char *message) {
    (void)rk;
    (void)level;
    (void)facility;
    (void)message;
}

static void client_error(rd_kafka_t *rk, int code, const char *message, void *opaque) {
    (void)rk;
    (void)message;
    kafka_state *p = opaque;
    if (kafka_status((rd_kafka_resp_err_t)code) == UD_AUTH)
        atomic_store(&p->unauthorized, 1);
}

static uint32_t partition_slot(const kafka_state *p, int32_t partition) {
    if (!p->fixed_partition)
        return (uint32_t)partition;
    return (uint32_t)partition + 1 == p->fixed_partition ? 0 : UINT32_MAX;
}

static ud_status source_partition_count(kafka_state *p, uint32_t *count) {
    rd_kafka_topic_t *topic = rd_kafka_topic_new(p->consumer, p->topic, NULL);
    if (!topic)
        return kafka_status(rd_kafka_last_error());
    const struct rd_kafka_metadata *metadata = NULL;
    rd_kafka_resp_err_t error = rd_kafka_metadata(p->consumer, 0, topic, &metadata, KAFKA_TIMEOUT);
    rd_kafka_topic_destroy(topic);
    ud_status status = control(p);
    if (!status)
        status = kafka_status(error);
    if (!status && (!metadata || metadata->topic_cnt != 1 ||
                    strcmp(metadata->topics[0].topic, p->topic)))
        status = UD_INCOMPATIBLE;
    if (!status) {
        const struct rd_kafka_metadata_topic *mapped = &metadata->topics[0];
        status = kafka_status(mapped->err);
        if (!status && (mapped->partition_cnt <= 0 || mapped->partition_cnt > KAFKA_LIMIT_PARTITIONS))
            status = UD_INCOMPATIBLE;
        uint32_t owned = p->fixed_partition ? 1 : (uint32_t)mapped->partition_cnt;
        if (!status && ((p->cut_count && owned != p->cut_count) ||
                        (p->frontier_count && owned != p->frontier_count)))
            status = UD_FENCED;
        for (uint32_t i = 0; i < p->cut_count; ++i)
            p->cuts[i].metadata_seen = 0;
        uint32_t matched = 0;
        for (int i = 0; (p->fixed_partition || p->cut_count) && !status && i < mapped->partition_cnt; ++i) {
            const struct rd_kafka_metadata_partition *partition = &mapped->partitions[i];
            uint32_t slot = partition_slot(p, partition->id);
            if (p->fixed_partition && slot == UINT32_MAX)
                continue;
            if (slot >= owned || (p->cut_count && p->cuts[slot].metadata_seen)) {
                status = UD_INCOMPATIBLE;
            } else {
                matched++;
                if (p->cut_count)
                    p->cuts[slot].metadata_seen = 1;
            }
        }
        if (!status && p->fixed_partition && matched != 1)
            status = UD_INVALID;
        if (!status && count)
            *count = owned;
    }
    if (metadata)
        rd_kafka_metadata_destroy(metadata);
    return status;
}

static ud_status query_cut_offsets(kafka_state *p, rd_kafka_topic_partition_list_t *assigned,
                                   rd_kafka_OffsetSpec_t spec) {
    int latest = spec == RD_KAFKA_OFFSET_SPEC_LATEST;
    rd_kafka_topic_partition_list_t *query = rd_kafka_topic_partition_list_new(assigned->cnt);
    if (!query)
        return UD_RESOURCE;
    for (int i = 0; i < assigned->cnt; ++i) {
        const rd_kafka_topic_partition_t *partition = &assigned->elems[i];
        if (!latest || partition->offset == RD_KAFKA_OFFSET_INVALID)
            rd_kafka_topic_partition_list_add(query, p->topic, partition->partition)->offset = spec;
    }
    if (!query->cnt) {
        rd_kafka_topic_partition_list_destroy(query);
        return UD_OK;
    }
    rd_kafka_queue_t *queue = rd_kafka_queue_new(p->consumer);
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(p->consumer,
                                                                 RD_KAFKA_ADMIN_OP_LISTOFFSETS);
    ud_status status = queue && options ? control(p) : UD_RESOURCE;
    if (!status) {
        rd_kafka_error_t *error = rd_kafka_AdminOptions_set_isolation_level(
            options, RD_KAFKA_ISOLATION_LEVEL_READ_COMMITTED);
        if (error) {
            status = kafka_status(rd_kafka_error_code(error));
            rd_kafka_error_destroy(error);
        }
    }
    if (!status)
        status = kafka_status(rd_kafka_AdminOptions_set_request_timeout(options, KAFKA_TIMEOUT,
                                                                       NULL, 0));
    rd_kafka_event_t *event = NULL;
    if (!status) {
        rd_kafka_ListOffsets(p->consumer, query, options, queue);
        uint64_t deadline = now_ms() + KAFKA_TIMEOUT;
        while (!event && !(status = control(p)) && now_ms() < deadline)
            event = rd_kafka_queue_poll(queue, wait_ms(deadline));
        if (!status)
            status = control(p);
        if (!status)
            status = event ? kafka_status(rd_kafka_event_error(event)) : UD_TIMEOUT;
    }
    if (!status) {
        const rd_kafka_ListOffsets_result_t *result = rd_kafka_event_ListOffsets_result(event);
        size_t count = 0;
        const rd_kafka_ListOffsetsResultInfo_t **items =
            result ? rd_kafka_ListOffsets_result_infos(result, &count) : NULL;
        if (!items || count != (size_t)query->cnt)
            status = UD_INCOMPATIBLE;
        for (size_t i = 0; !status && i < count; ++i) {
            const rd_kafka_topic_partition_t *partition =
                rd_kafka_ListOffsetsResultInfo_topic_partition(items[i]);
            uint32_t partitions = p->cut_count ? p->cut_count : p->frontier_count;
            uint32_t slot = partition ? partition_slot(p, partition->partition) : UINT32_MAX;
            if (!partition || !partition->topic || strcmp(partition->topic, p->topic) ||
                slot >= partitions || (p->cut_count && !p->cuts[slot].assigned)) {
                status = UD_INCOMPATIBLE;
            } else {
                status = kafka_status(partition->err);
                if (!status) {
                    int64_t *value;
                    if (p->cut_count) {
                        kafka_partition_cut *cut = &p->cuts[slot];
                        value = latest ? &cut->latest : &cut->low;
                    } else {
                        value = &p->frontier[slot].candidate;
                    }
                    if (*value >= 0 || partition->offset < 0)
                        status = UD_INCOMPATIBLE;
                    else
                        *value = partition->offset;
                }
            }
        }
    }
    if (event)
        rd_kafka_event_destroy(event);
    if (options)
        rd_kafka_AdminOptions_destroy(options);
    if (queue)
        rd_kafka_queue_destroy(queue);
    rd_kafka_topic_partition_list_destroy(query);
    return status;
}

static ud_status prepare_cut_assignment(kafka_state *p,
                                        rd_kafka_topic_partition_list_t *partitions) {
    for (int i = 0; i < partitions->cnt; ++i) {
        const rd_kafka_topic_partition_t *part = &partitions->elems[i];
        uint32_t slot = partition_slot(p, part->partition);
        if (!part->topic || strcmp(part->topic, p->topic) || slot >= p->cut_count ||
            p->cuts[slot].assigned)
            return UD_FENCED;
        p->cuts[slot].assigned = 1;
    }
    if (!partitions->cnt)
        return UD_OK;
    ud_status status = query_cut_offsets(p, partitions, RD_KAFKA_OFFSET_SPEC_EARLIEST);
    if (!status && !p->frontier_count) {
        rd_kafka_resp_err_t error = rd_kafka_committed(p->consumer, partitions, KAFKA_TIMEOUT);
        status = control(p);
        if (!status)
            status = kafka_status(error);
    }
    if (!status && p->initial_position == UD_MESSAGE_POSITION_LATEST)
        status = query_cut_offsets(p, partitions, RD_KAFKA_OFFSET_SPEC_LATEST);
    for (int i = 0; !status && i < partitions->cnt; ++i) {
        rd_kafka_topic_partition_t *part = &partitions->elems[i];
        uint32_t slot = partition_slot(p, part->partition);
        kafka_partition_cut *cut = &p->cuts[slot];
        status = kafka_status(part->err);
        if (status)
            break;
        if (part->offset == RD_KAFKA_OFFSET_INVALID)
            part->offset = cut->end == cut->low ||
                           p->initial_position == UD_MESSAGE_POSITION_EARLIEST ? cut->low :
                           p->initial_position == UD_MESSAGE_POSITION_LATEST ? cut->latest : -1;
        if (cut->low < 0 || cut->end < cut->low || part->offset < cut->low)
            status = UD_FENCED;
        else if (part->offset >= cut->end)
            cut->reached = 1;
        else
            ++p->cuts_remaining;
        if (!status && p->frontier_count && p->frontier[slot].committed < 0 &&
            (p->fixed_partition || (p->initial_position == UD_MESSAGE_POSITION_LATEST && cut->end > cut->low)))
            p->frontier[slot].start = part->offset;
    }
    return status;
}

static ud_status prepare_exclusive_assignment(kafka_state *p,
                                              rd_kafka_topic_partition_list_t *partitions) {
    if (partitions->cnt != (int)p->frontier_count)
        return UD_FENCED;
    for (uint32_t i = 0; i < p->frontier_count; ++i)
        p->frontier[i].candidate = -1;
    for (int i = 0; i < partitions->cnt; ++i) {
        const rd_kafka_topic_partition_t *part = &partitions->elems[i];
        uint32_t slot = partition_slot(p, part->partition);
        if (!part->topic || strcmp(part->topic, p->topic) || slot >= p->frontier_count ||
            p->frontier[slot].candidate == 0)
            return UD_FENCED;
        p->frontier[slot].candidate = 0;
    }
    rd_kafka_resp_err_t error = rd_kafka_committed(p->consumer, partitions, KAFKA_TIMEOUT);
    ud_status status = control(p);
    if (!status)
        status = kafka_status(error);
    for (int i = 0; !status && i < partitions->cnt; ++i) {
        rd_kafka_topic_partition_t *part = &partitions->elems[i];
        kafka_frontier *entry = &p->frontier[partition_slot(p, part->partition)];
        status = kafka_status(part->err);
        if (!status && part->offset < 0 && part->offset != RD_KAFKA_OFFSET_INVALID)
            status = UD_FENCED;
        int64_t committed = part->offset == RD_KAFKA_OFFSET_INVALID ? -1 : part->offset;
        if (!status && p->frontier_frozen && entry->committed != committed)
            status = UD_FENCED;
        if (!status && !p->frontier_frozen && p->has_frozen_offset) {
            if (committed >= 0 && committed != p->frozen_offset)
                status = UD_FENCED;
            else if (committed < 0)
                entry->start = p->frozen_offset;
        }
        if (!status)
            entry->committed = committed;
        if (!status && committed < 0 && entry->start >= 0)
            part->offset = entry->start;
    }
    if (!status && p->fixed_partition && !p->cut_count) {
        kafka_frontier *entry = p->frontier;
        rd_kafka_topic_partition_t *part = partitions->elems;
        entry->candidate = -1;
        status = query_cut_offsets(p, partitions, RD_KAFKA_OFFSET_SPEC_EARLIEST);
        if (!status && part->offset >= 0 && part->offset < entry->candidate)
            status = UD_FENCED;
        if (!status && part->offset == RD_KAFKA_OFFSET_INVALID &&
            p->initial_position == UD_MESSAGE_POSITION_EARLIEST)
            entry->start = part->offset = entry->candidate;
    }
    if (!status && !p->cut_count && p->initial_position == UD_MESSAGE_POSITION_LATEST) {
        for (uint32_t i = 0; i < p->frontier_count; ++i)
            p->frontier[i].candidate = -1;
        status = query_cut_offsets(p, partitions, RD_KAFKA_OFFSET_SPEC_LATEST);
    }
    for (int i = 0; !status && !p->cut_count && i < partitions->cnt; ++i) {
        rd_kafka_topic_partition_t *part = &partitions->elems[i];
        if (part->offset != RD_KAFKA_OFFSET_INVALID)
            continue;
        kafka_frontier *entry = &p->frontier[partition_slot(p, part->partition)];
        if (p->initial_position == UD_MESSAGE_POSITION_EARLIEST) {
            part->offset = RD_KAFKA_OFFSET_BEGINNING;
        } else if (p->initial_position == UD_MESSAGE_POSITION_LATEST && entry->candidate >= 0) {
            entry->start = part->offset = entry->candidate;
        } else {
            status = UD_FENCED;
        }
    }
    if (!status)
        p->frontier_frozen = 1;
    return status;
}

static ud_status pause_cut_partitions(kafka_state *p, rd_kafka_topic_partition_list_t *partitions) {
    ud_status status = kafka_status(rd_kafka_pause_partitions(p->consumer, partitions));
    for (int i = 0; !status && i < partitions->cnt; ++i) {
        const rd_kafka_topic_partition_t *partition = &partitions->elems[i];
        status = kafka_status(partition->err);
        if (!status)
            p->cuts[partition_slot(p, partition->partition)].paused = 1;
    }
    return status;
}

static ud_status assign_partitions(kafka_state *p, rd_kafka_topic_partition_list_t *partitions) {
    ud_status status = p->frontier_count ? prepare_exclusive_assignment(p, partitions) : UD_OK;
    if (!status && p->cut_count)
        status = prepare_cut_assignment(p, partitions);
    if (!status)
        status = kafka_status(rd_kafka_assign(p->consumer, partitions));
    if (!status && p->cut_count && (uint32_t)partitions->cnt > p->cuts_remaining) {
        rd_kafka_topic_partition_list_t *pause =
            rd_kafka_topic_partition_list_new(partitions->cnt - (int)p->cuts_remaining);
        if (!pause) {
            status = UD_RESOURCE;
        } else {
            for (int i = 0; i < partitions->cnt; ++i) {
                int32_t partition = partitions->elems[i].partition;
                if (p->cuts[partition_slot(p, partition)].reached)
                    rd_kafka_topic_partition_list_add(pause, p->topic, partition);
            }
            status = pause_cut_partitions(p, pause);
            rd_kafka_topic_partition_list_destroy(pause);
        }
    }
    if (status)
        rd_kafka_assign(p->consumer, NULL);
    p->assigned = !status && (!p->cut_count || partitions->cnt > 0);
    return status;
}

static void rebalance(rd_kafka_t *consumer, rd_kafka_resp_err_t error,
                      rd_kafka_topic_partition_list_t *partitions, void *opaque) {
    kafka_state *p = opaque;
    ud_status bounded_failure = (p->cut_count || p->frontier_count) ?
        (p->count ? UD_FENCED : p->rebalance_error) : UD_OK;
    invalidate(p);
    if (error == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS && !bounded_failure) {
        p->rebalance_error = assign_partitions(p, partitions);
    } else {
        rd_kafka_resp_err_t unassign = rd_kafka_assign(consumer, NULL);
        p->rebalance_error = bounded_failure ? bounded_failure : unassign ? kafka_status(unassign) :
                             error == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS ? UD_OK : kafka_status(error);
    }
}

static void delivery_report(rd_kafka_t *rk, const rd_kafka_message_t *message, void *opaque) {
    (void)rk;
    (void)opaque;
    kafka_delivery *delivery = message->_private;
    if (!delivery)
        return;
    if (!message->err && rd_kafka_message_status(message) == RD_KAFKA_MSG_STATUS_PERSISTED)
        delivery->result = (ud_message_result){UD_MESSAGE_ACCEPTED, UD_OK};
    else if (message->err && rd_kafka_message_status(message) == RD_KAFKA_MSG_STATUS_NOT_PERSISTED)
        delivery->result = (ud_message_result){UD_MESSAGE_REJECTED, kafka_status(message->err)};
    else
        delivery->result = (ud_message_result){UD_MESSAGE_UNCERTAIN, UD_COMMIT_UNKNOWN};
    delivery->done = 1;
}

static ud_status create_client(kafka_state *p, rd_kafka_type_t mode, const char *identity,
                               int32_t position, rd_kafka_t **out) {
    ud_status status = control(p);
    if (!status)
        status = authorize_bootstrap(p);
    if (status)
        return status;
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (!conf)
        return UD_RESOURCE;
    rd_kafka_conf_set_log_cb(conf, client_log);
    rd_kafka_conf_set_error_cb(conf, client_error);
    rd_kafka_conf_set_opaque(conf, p);
    rd_kafka_conf_set_resolve_cb(conf, resolve);
    const char *protocol = p->tls ? (p->username ? "SASL_SSL" : "SSL") :
                                  (p->username ? "SASL_PLAINTEXT" : "PLAINTEXT");
    int valid = conf_text(conf, "bootstrap.servers", p->servers) &&
                conf_text(conf, "security.protocol", protocol) &&
                conf_text(conf, "allow.auto.create.topics", "false") &&
                conf_text(conf, "socket.timeout.ms", "5000") &&
                conf_text(conf, "socket.connection.setup.timeout.ms", "5000") &&
                conf_text(conf, "broker.address.ttl", "0");
    if (valid && p->tls)
        valid = conf_text(conf, "enable.ssl.certificate.verification", "true") &&
                conf_text(conf, "ssl.endpoint.identification.algorithm", "https") &&
                (!p->ca_file || conf_text(conf, "ssl.ca.location", p->ca_file));
    if (valid && p->username)
        valid = conf_text(conf, "sasl.mechanisms", "PLAIN") &&
                conf_text(conf, "sasl.username", p->username) &&
                conf_text(conf, "sasl.password", p->password);
    if (valid && mode == RD_KAFKA_CONSUMER) {
        /* A reader without an identity uses direct partition assignment and
         * never instantiates a group or an offset commit queue. */
        uint32_t maximum = identity ? p->maximum : KAFKA_LIMIT_RECORD;
        uint32_t budget = identity ? (uint32_t)p->budget : KAFKA_LIMIT_RECORD;
        uint32_t credit = identity ? p->credit : 1;
        uint32_t max_frame = maximum < 1000 ? 1000 : maximum;
        uint32_t fetch_budget = budget < max_frame ? max_frame : budget;
        if (identity) {
            if (!p->fixed_partition)
                rd_kafka_conf_set_rebalance_cb(conf, rebalance);
            valid = conf_text(conf, "group.id", identity) &&
                    (p->fixed_partition ? conf_text(conf, "group.protocol", "classic") :
                                          conf_text(conf, "partition.assignment.strategy", "range")) &&
                    (!p->cut_count || conf_text(conf, "enable.partition.eof", "true"));
        } else {
            valid = conf_text(conf, "enable.partition.eof", "true");
        }
        valid = valid && conf_text(conf, "enable.auto.commit", "false") &&
                conf_text(conf, "enable.auto.offset.store", "false") &&
                conf_text(conf, "isolation.level", "read_committed") &&
                conf_text(conf, "auto.offset.reset", (p->cut_count || p->exclusive_transaction) && identity ? "error" :
                          position == UD_MESSAGE_POSITION_EARLIEST ?
                          "earliest" : position == UD_MESSAGE_POSITION_LATEST ? "latest" : "error") &&
                conf_number(conf, "queued.min.messages", credit) &&
                conf_number(conf, "queued.max.messages.kbytes", (budget + 1023) / 1024) &&
                conf_number(conf, "message.max.bytes", max_frame) &&
                conf_number(conf, "fetch.max.bytes", fetch_budget) &&
                conf_number(conf, "max.partition.fetch.bytes", maximum) &&
                conf_number(conf, "receive.message.max.bytes",
                            fetch_budget < 1024 * 1024 ? 1024 * 1024 + 512 : fetch_budget + 512);
    } else if (valid) {
        rd_kafka_conf_set_dr_msg_cb(conf, delivery_report);
        valid = conf_text(conf, "enable.idempotence", "true") &&
                conf_text(conf, "acks", "all") &&
                conf_text(conf, "delivery.timeout.ms", "8000") &&
                conf_text(conf, "queue.buffering.max.kbytes", "65536") &&
                conf_number(conf, "message.max.bytes", KAFKA_LIMIT_RECORD) &&
                (!identity || (conf_text(conf, "transactional.id", identity) &&
                               conf_text(conf, "transaction.timeout.ms", "30000")));
    }
    if (!valid) {
        rd_kafka_conf_destroy(conf);
        return UD_UNSUPPORTED;
    }
    *out = rd_kafka_new(mode, conf, NULL, 0);
    if (!*out) {
        rd_kafka_conf_destroy(conf);
        return UD_IO;
    }
    return UD_OK;
}

static ud_status cluster_id(kafka_state *p, rd_kafka_t *rk, char result[KAFKA_LIMIT_CLUSTER + 1]) {
    ud_status status = control(p);
    if (status)
        return status;
    char *value = rd_kafka_clusterid(rk, 0);
    if (!value) {
        const struct rd_kafka_metadata *metadata = NULL;
        rd_kafka_resp_err_t error = rd_kafka_metadata(rk, 0, NULL, &metadata, KAFKA_TIMEOUT);
        if (rd_kafka_type(rk) == RD_KAFKA_PRODUCER)
            rd_kafka_poll(rk, 0);
        if (metadata)
            rd_kafka_metadata_destroy(metadata);
        status = control(p);
        if (status || error)
            return status ? status : kafka_status(error);
        value = rd_kafka_clusterid(rk, 0);
    }
    if (!value)
        return UD_INCOMPATIBLE;
    size_t n = strlen(value);
    if (!n || n > KAFKA_LIMIT_CLUSTER)
        status = UD_INCOMPATIBLE;
    else
        memcpy(result, value, n + 1);
    rd_kafka_mem_free(rk, value);
    return status;
}

static ud_status test(void *state) {
    kafka_state *p = state;
    if (p->producer || p->consumer || p->transaction)
        return UD_BUSY;
    ud_status status = create_client(p, RD_KAFKA_PRODUCER, NULL, 0, &p->producer);
    if (!status)
        status = cluster_id(p, p->producer, p->producer_cluster);
    close_producer(p);
    return status;
}

static int valid_topic(ud_string s) {
    if (!s.data || !s.length || s.length > KAFKA_LIMIT_TOPIC ||
        (s.length <= 2 && s.data[0] == '.' && (s.length == 1 || s.data[1] == '.')))
        return 0;
    for (uint32_t i = 0; i < s.length; ++i) {
        char c = s.data[i];
        if (!(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') &&
            c != '-' && c != '_' && c != '.')
            return 0;
    }
    return 1;
}

static ud_status bind_cuts(kafka_state *p, const ud_message_subscription *options) {
    uint32_t count = options->end_offset_count;
    if (!count)
        return options->end_offsets ? UD_INVALID : UD_OK;
    if (!options->end_offsets || count > KAFKA_LIMIT_PARTITIONS || (p->fixed_partition && count != 1))
        return UD_INVALID;
    p->cuts = p->host->allocate(p->host->context, (size_t)count * sizeof(*p->cuts));
    if (!p->cuts)
        return UD_RESOURCE;
    p->cut_count = count;
    for (uint32_t i = 0; i < count; ++i)
        p->cuts[i].end = -1;
    for (uint32_t i = 0; i < count; ++i) {
        const ud_message_partition_end *cut = &options->end_offsets[i];
        uint32_t slot = partition_slot(p, cut->partition);
        if (slot >= count || cut->next_offset < 0 || p->cuts[slot].end >= 0)
            return UD_INVALID;
        p->cuts[slot].end = cut->next_offset;
    }
    return UD_OK;
}

static ud_status subscribe_at(void *state, const ud_message_subscription *options, int64_t offset) {
    kafka_state *p = state;
    if (p->consumer || p->producer || p->transaction)
        return UD_BUSY;
    if (options->source.kind != UD_MESSAGE_TOPIC || options->source.routing_key.length ||
        options->subscription.length || options->filter.length)
        return UD_UNSUPPORTED;
    ud_string group = options->group;
    if (!valid_topic(options->source.name) || !group.data || !group.length || group.length > 1024 ||
        !options->max_message_bytes || options->max_message_bytes > KAFKA_LIMIT_RECORD ||
        options->max_in_flight_bytes < options->max_message_bytes ||
        options->max_in_flight_bytes > KAFKA_LIMIT_TOTAL || !options->prefetch ||
        options->prefetch > KAFKA_LIMIT_TOKENS || options->fixed_partition > KAFKA_LIMIT_PARTITIONS ||
        (offset >= 0 && !options->fixed_partition))
        return UD_INVALID;
    const ud_host_api *h = p->host;
    p->topic = plugin_string(h, options->source.name.data, options->source.name.length);
    char *group_id = plugin_string(h, group.data, group.length);
    p->maximum = options->max_message_bytes;
    p->budget = options->max_in_flight_bytes;
    p->credit = options->prefetch;
    p->initial_position = options->initial_position;
    p->has_frozen_offset = offset >= 0;
    p->frozen_offset = offset;
    p->fixed_partition = options->fixed_partition;
    p->exclusive_transaction = options->exclusive_transaction != 0 || p->fixed_partition;
    ud_status status = group_id && p->topic ? bind_cuts(p, options) : UD_RESOURCE;
    if (!status)
        status = create_client(p, RD_KAFKA_CONSUMER, group_id, options->initial_position, &p->consumer);
    h->deallocate(group_id);
    if (!status)
        status = kafka_status(rd_kafka_poll_set_consumer(p->consumer));
    if (!status && (p->cut_count || p->exclusive_transaction)) {
        uint32_t count = 0;
        status = source_partition_count(p, &count);
        if (!status && p->exclusive_transaction) {
            p->frontier = h->allocate(h->context, (size_t)count * sizeof(*p->frontier));
            if (!p->frontier) {
                status = UD_RESOURCE;
            } else {
                p->frontier_count = count;
                for (uint32_t i = 0; i < count; ++i)
                    p->frontier[i].committed = p->frontier[i].start = -1;
            }
        }
    }
    if (!status && !p->fixed_partition) {
        rd_kafka_topic_partition_list_t *list = rd_kafka_topic_partition_list_new(1);
        if (!list) {
            status = UD_RESOURCE;
        } else {
            rd_kafka_topic_partition_list_add(list, p->topic, RD_KAFKA_PARTITION_UA);
            status = kafka_status(rd_kafka_subscribe(p->consumer, list));
            rd_kafka_topic_partition_list_destroy(list);
        }
    }
    if (status)
        close_consumer(p);
    return status;
}

static ud_status subscribe(void *state, const ud_message_subscription *options) {
    return subscribe_at(state,options,-1);
}

static ud_status check_cut_record(kafka_state *p, const rd_kafka_message_t *record) {
    if (p->rebalance_error)
        return p->rebalance_error;
    if (!p->assigned || rd_kafka_assignment_lost(p->consumer))
        return UD_FENCED;
    uint32_t slot = partition_slot(p, record->partition);
    if (!record->rkt || strcmp(rd_kafka_topic_name(record->rkt), p->topic) ||
        slot >= p->cut_count || !p->cuts[slot].assigned || record->offset < 0)
        return UD_FENCED;
    kafka_partition_cut *cut = &p->cuts[slot];
    if (record->offset < cut->end)
        return record->err == RD_KAFKA_RESP_ERR__PARTITION_EOF ? UD_BUSY :
               cut->reached ? UD_FENCED : UD_OK;
    if (!cut->reached) {
        --p->cuts_remaining;
        cut->reached = 1;
    }
    if (!cut->paused) {
        rd_kafka_topic_partition_list_t *pause = rd_kafka_topic_partition_list_new(1);
        if (!pause)
            return UD_RESOURCE;
        rd_kafka_topic_partition_list_add(pause, p->topic, record->partition);
        ud_status status = pause_cut_partitions(p, pause);
        rd_kafka_topic_partition_list_destroy(pause);
        if (status)
            return status;
    }
    return UD_BUSY;
}

static ud_status poll_consumer(kafka_state *p, int timeout) {
    if (p->pending)
        return UD_OK;
    if (p->rebalance_error)
        return p->rebalance_error;
    if (p->fixed_partition && !p->assigned) {
        /* Bind on first use so recovery can install its expected broker frontier first. */
        rd_kafka_topic_partition_list_t *list = rd_kafka_topic_partition_list_new(1);
        if (!list)
            return UD_RESOURCE;
        rd_kafka_topic_partition_list_add(list, p->topic, (int32_t)(p->fixed_partition - 1))->offset =
            RD_KAFKA_OFFSET_INVALID;
        invalidate(p);
        p->rebalance_error = assign_partitions(p, list);
        rd_kafka_topic_partition_list_destroy(list);
        if (p->rebalance_error)
            return p->rebalance_error;
    }
    rd_kafka_message_t *message = rd_kafka_consumer_poll(p->consumer, timeout);
    if (!message)
        return p->rebalance_error ? p->rebalance_error : UD_BUSY;
    if (p->cut_count && (!message->err || message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)) {
        ud_status status = check_cut_record(p, message);
        if (status) {
            rd_kafka_message_destroy(message);
            return status;
        }
    }
    if (message->err) {
        rd_kafka_resp_err_t error = message->err;
        rd_kafka_message_destroy(message);
        return error == RD_KAFKA_RESP_ERR__PARTITION_EOF ? UD_BUSY : kafka_status(error);
    }
    p->pending = message;
    return UD_OK;
}

static ud_status copy_value(const ud_host_api *h, const void *data, size_t length, ud_string *out) {
    if (length > UINT32_MAX)
        return UD_OVERFLOW;
    if (!data)
        return length ? UD_CORRUPT : UD_OK;
    *out = (ud_string){plugin_bytes(h, data, (uint32_t)length), (uint32_t)length};
    return out->data ? UD_OK : UD_RESOURCE;
}

static void clear_epoch(const ud_host_api *h, ud_message_epoch *epoch) {
    h->deallocate((void *)epoch->cluster_id.data);
    h->deallocate((void *)epoch->group_metadata.data);
    for (uint32_t i = 0; i < epoch->offset_count; ++i)
        h->deallocate((void *)epoch->offsets[i].topic.data);
    h->deallocate(epoch->offsets);
    memset(epoch, 0, sizeof(*epoch));
}

static ud_status size_message(const rd_kafka_message_t *record, rd_kafka_headers_t **headers,
                              size_t *header_count, uint64_t *bytes) {
    if (record->len > KAFKA_LIMIT_RECORD || record->key_len > KAFKA_LIMIT_RECORD)
        return UD_OVERFLOW;
    *bytes = (uint64_t)record->len + record->key_len;
    rd_kafka_resp_err_t err = rd_kafka_message_headers(record, headers);
    if (err == RD_KAFKA_RESP_ERR__NOENT)
        return UD_OK;
    if (err)
        return kafka_status(err);
    *header_count = rd_kafka_header_cnt(*headers);
    if (*header_count > KAFKA_LIMIT_HEADERS)
        return UD_OVERFLOW;
    for (size_t i = 0; i < *header_count; ++i) {
        const char *name;
        const void *data;
        size_t length;
        err = rd_kafka_header_get_all(*headers, i, &name, &data, &length);
        if (err)
            return kafka_status(err);
        if (length > KAFKA_LIMIT_RECORD)
            return UD_OVERFLOW;
        *bytes += strlen(name) + (uint64_t)length + 8;
    }
    return UD_OK;
}

static ud_status materialize(const ud_host_api *h, const char *topic, const rd_kafka_message_t *record,
                             rd_kafka_headers_t *headers, size_t header_count, ud_message *out) {
    ud_status status = copy_value(h, record->payload, record->len, &out->payload);
    if (!status)
        status = copy_value(h, record->key, record->key_len, &out->key);
    if (!status)
        status = copy_value(h, topic, strlen(topic), &out->address);
    if (!status && header_count) {
        out->headers = h->allocate(h->context, header_count * sizeof(*out->headers));
        if (!out->headers)
            status = UD_RESOURCE;
        else
            memset(out->headers, 0, header_count * sizeof(*out->headers));
    }
    for (size_t i = 0; !status && i < header_count; ++i) {
        const char *name;
        const void *data;
        size_t length;
        ud_message_header *header = &out->headers[out->header_count++];
        rd_kafka_header_get_all(headers, i, &name, &data, &length);
        status = copy_value(h, name, strlen(name), &header->name);
        if (!status)
            status = copy_value(h, data, length, &header->value);
    }
    if (!status) {
        out->partition = record->partition;
        out->offset = record->offset;
        out->timestamp_ms = rd_kafka_message_timestamp(record, NULL);
    }
    return status;
}

static ud_status visit_record(kafka_state *p, const char *topic, const rd_kafka_message_t *record,
                              void *user, ud_message_visit visit) {
    ud_message message = {.partition = -1, .offset = -1, .timestamp_ms = -1};
    uint64_t bytes = 0;
    size_t header_count = 0;
    rd_kafka_headers_t *headers = NULL;
    ud_status status = size_message(record, &headers, &header_count, &bytes);
    if (!status && bytes > KAFKA_LIMIT_RECORD)
        status = UD_OVERFLOW;
    if (!status)
        status = materialize(p->host, topic, record, headers, header_count, &message);
    if (!status)
        status = visit(user, &message);
    const ud_host_api *h = p->host;
    for (uint32_t i = 0; i < message.header_count; ++i) {
        h->deallocate((void *)message.headers[i].name.data);
        h->deallocate((void *)message.headers[i].value.data);
    }
    h->deallocate(message.headers);
    h->deallocate((void *)message.payload.data);
    h->deallocate((void *)message.key.data);
    h->deallocate((void *)message.address.data);
    return status;
}

static ud_status scan_partition(kafka_state *p, rd_kafka_t *reader, rd_kafka_topic_t *native_topic,
                                 const char *topic, int32_t partition, void *user,
                                 ud_message_visit visit) {
    if (rd_kafka_consume_start(native_topic, partition, RD_KAFKA_OFFSET_BEGINNING))
        return kafka_status(rd_kafka_last_error());
    uint64_t deadline = now_ms() + KAFKA_TIMEOUT;
    int64_t previous = -1;
    ud_status status;
    while (!(status = control(p))) {
        if (now_ms() >= deadline) {
            status = UD_TIMEOUT;
            break;
        }
        rd_kafka_poll(reader, 0);
        rd_kafka_message_t *record = rd_kafka_consume(native_topic, partition, wait_ms(deadline));
        if (!record) {
            rd_kafka_resp_err_t error = rd_kafka_last_error();
            if (error == RD_KAFKA_RESP_ERR__TIMED_OUT)
                continue;
            status = error ? kafka_status(error) : UD_IO;
            break;
        }
        status = control(p);
        int finished = !status && record->err == RD_KAFKA_RESP_ERR__PARTITION_EOF;
        if (!status && !finished) {
            if (record->err)
                status = kafka_status(record->err);
            else if (record->rkt != native_topic || record->partition != partition ||
                     record->offset <= previous)
                status = UD_INCOMPATIBLE;
            else {
                status = visit_record(p, topic, record, user, visit);
                previous = record->offset;
                deadline = now_ms() + KAFKA_TIMEOUT;
            }
        }
        rd_kafka_message_destroy(record);
        if (status || finished)
            break;
    }
    rd_kafka_consume_stop(native_topic, partition);
    return status;
}

static ud_status visit_committed(void *state, ud_string name, int32_t partition, void *user,
                                 ud_message_visit visit) {
    kafka_state *p = state;
    if (!valid_topic(name) || partition < 0 || !visit)
        return UD_INVALID;
    if (p->consumer)
        return UD_BUSY;
    char topic[KAFKA_LIMIT_TOPIC + 1];
    memcpy(topic, name.data, name.length);
    topic[name.length] = 0;
    rd_kafka_t *reader = NULL;
    ud_status status = create_client(p, RD_KAFKA_CONSUMER, NULL, UD_MESSAGE_POSITION_REQUIRE, &reader);
    if (status)
        return status;
    char actual[KAFKA_LIMIT_CLUSTER + 1];
    status = cluster_id(p, reader, actual);
    if (!status && p->producer_cluster[0] && strcmp(p->producer_cluster, actual))
        status = UD_INCOMPATIBLE;
    rd_kafka_topic_t *native_topic = NULL;
    if (!status) {
        native_topic = rd_kafka_topic_new(reader, topic, NULL);
        status = native_topic ? scan_partition(p, reader, native_topic, topic, partition, user, visit) :
                                kafka_status(rd_kafka_last_error());
    }
    if (native_topic)
        rd_kafka_topic_destroy(native_topic);
    rd_kafka_destroy_flags(reader, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
    return status;
}

static ud_status receive(void *state, uint32_t timeout, ud_message *out) {
    kafka_state *p = state;
    if (!p->consumer)
        return UD_INVALID;
    if (p->count == p->credit)
        return UD_BUSY;
    uint64_t deadline = now_ms() + timeout;
    ud_status status;
    do {
        status = control(p);
        if (status)
            return status;
        status = poll_consumer(p, wait_ms(deadline));
        if (status == UD_BUSY && p->cut_count && p->assigned &&
            rd_kafka_assignment_lost(p->consumer))
            return UD_FENCED;
        if (status == UD_BUSY && p->cut_count && p->assigned && !p->cuts_remaining && !p->count) {
            uint64_t generation = p->assignment;
            status = source_partition_count(p, NULL);
            if (!status) {
                status = poll_consumer(p, 0);
                if (status == UD_BUSY && p->assigned && p->assignment == generation &&
                    !rd_kafka_assignment_lost(p->consumer)) {
                    status = control(p);
                    return status ? status : UD_END;
                }
            }
        }
    } while (status == UD_BUSY && now_ms() < deadline);
    if (!status)
        status = control(p);
    if (status)
        return status;
    if (!p->assigned || rd_kafka_assignment_lost(p->consumer))
        return UD_FENCED;
    rd_kafka_message_t *record = p->pending;
    if (!record->rkt || strcmp(rd_kafka_topic_name(record->rkt), p->topic) ||
        record->partition < 0 || (p->fixed_partition && partition_slot(p, record->partition) != 0) ||
        record->offset < 0 || record->offset == INT64_MAX) {
        close_consumer(p);
        return UD_INCOMPATIBLE;
    }
    uint64_t bytes = 0;
    size_t header_count = 0;
    rd_kafka_headers_t *headers = NULL;
    status = size_message(record, &headers, &header_count, &bytes);
    if (!status && (bytes > p->maximum || p->sequence == UINT64_MAX))
        status = UD_OVERFLOW;
    if (status) {
        close_consumer(p);
        return status;
    }
    if (bytes > p->budget - p->held)
        return UD_BUSY;
    status = materialize(p->host, p->topic, record, headers, header_count, out);
    if (status)
        return status;
    out->delivery = ++p->sequence;
    p->tokens[p->count++] = (kafka_token){out->delivery, bytes, record->partition, record->offset};
    p->held += bytes;
    rd_kafka_message_destroy(record);
    p->pending = NULL;
    return UD_OK;
}

static ud_status verify_ownership(kafka_state *p) {
    ud_status status = control(p);
    if (status)
        return status;
    uint64_t generation = p->assignment;
    status = poll_consumer(p, 0);
    if (status && status != UD_BUSY)
        return status;
    if (!p->assigned || rd_kafka_assignment_lost(p->consumer) || p->assignment != generation)
        return UD_FENCED;
    return control(p);
}

static ud_status prefix(kafka_state *p, const uint64_t *handles, uint32_t count,
                        uint8_t selected[KAFKA_LIMIT_TOKENS], rd_kafka_topic_partition_list_t *offsets) {
    if (!count || count > KAFKA_LIMIT_TOKENS)
        return UD_INVALID;
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t index = 0;
        while (index < p->count && p->tokens[index].handle != handles[i])
            ++index;
        if (index == p->count)
            return handles[i] && handles[i] <= p->sequence ? UD_FENCED : UD_INVALID;
        if (selected[index])
            return UD_INVALID;
        selected[index] = 1;
    }
    for (uint32_t i = 0; i < p->count; ++i) {
        if (!selected[i])
            continue;
        const kafka_token *entry = &p->tokens[i];
        for (uint32_t j = 0; j < i; ++j)
            if (!selected[j] && p->tokens[j].partition == entry->partition)
                return UD_INVALID;
        rd_kafka_topic_partition_t *offset = rd_kafka_topic_partition_list_find(offsets, p->topic,
                                                                                entry->partition);
        if (!offset)
            offset = rd_kafka_topic_partition_list_add(offsets, p->topic, entry->partition);
        offset->offset = entry->offset + 1;
    }
    return UD_OK;
}

static void release_selected(kafka_state *p, const uint8_t selected[KAFKA_LIMIT_TOKENS]) {
    uint32_t remaining = 0;
    for (uint32_t i = 0; i < p->count; ++i) {
        if (selected[i])
            p->held -= p->tokens[i].bytes;
        else
            p->tokens[remaining++] = p->tokens[i];
    }
    p->count = remaining;
}

static void confirm_frontier(kafka_state *p, const rd_kafka_topic_partition_list_t *offsets) {
    for (uint32_t i = 0; i < p->frontier_count; ++i)
        if (p->frontier[i].committed < 0)
            p->frontier[i].committed = p->frontier[i].start;
    for (int i = 0; p->frontier_count && i < offsets->cnt; ++i) {
        const rd_kafka_topic_partition_t *offset = &offsets->elems[i];
        p->frontier[partition_slot(p, offset->partition)].committed = offset->offset;
    }
}

typedef struct kafka_commit_reply {
    const rd_kafka_topic_partition_list_t *expected;
    ud_status status;
    int complete;
} kafka_commit_reply;

static void offset_commit_result(rd_kafka_t *client, rd_kafka_resp_err_t error,
                                 rd_kafka_topic_partition_list_t *result, void *context) {
    (void)client;
    kafka_commit_reply *reply = context;
    ud_status status = kafka_status(error);
    int committed = 0;
    for (int i = 0; result && i < result->cnt; ++i) {
        const rd_kafka_topic_partition_t *reported = &result->elems[i];
        const rd_kafka_topic_partition_t *expected = rd_kafka_topic_partition_list_find(
            reply->expected, reported->topic, reported->partition);
        ud_status item = !expected || expected->offset != reported->offset ?
                             UD_COMMIT_UNKNOWN : kafka_status(reported->err);
        if (item && !status)
            status = item;
        committed += item == UD_OK;
    }
    if (status == UD_TIMEOUT || status == UD_TRANSIENT ||
        (!status && (!result || result->cnt != reply->expected->cnt)) ||
        (status && result && committed > 0 && committed < result->cnt))
        status = UD_COMMIT_UNKNOWN;
    reply->status = status;
    reply->complete = 1;
}

static ud_status commit_offsets(kafka_state *p, rd_kafka_topic_partition_list_t *offsets) {
    rd_kafka_queue_t *queue = rd_kafka_queue_new(p->consumer);
    if (!queue)
        return UD_RESOURCE;
    kafka_commit_reply reply = {.expected = offsets, .status = UD_COMMIT_UNKNOWN};
    ud_status status = kafka_status(rd_kafka_commit_queue(p->consumer, offsets, queue,
                                                        offset_commit_result, &reply));
    uint64_t deadline = now_ms() + KAFKA_TIMEOUT;
    if (!status) {
        do {
            rd_kafka_queue_poll_callback(queue, wait_ms(deadline));
        } while (!reply.complete && !control(p) && now_ms() < deadline);
        status = reply.status;
    }
    rd_kafka_queue_destroy(queue);
    return status;
}

static ud_status acknowledge(void *state, const uint64_t *handles, uint32_t count) {
    kafka_state *p = state;
    if (!p->consumer)
        return UD_INVALID;
    ud_status status = verify_ownership(p);
    uint8_t selected[KAFKA_LIMIT_TOKENS] = {0};
    if (status)
        return status;
    rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new((int)p->count);
    if (!offsets)
        return UD_RESOURCE;
    status = prefix(p, handles, count, selected, offsets);
    if (!status)
        status = commit_offsets(p, offsets);
    if (!status && p->fixed_partition)
        confirm_frontier(p, offsets);
    rd_kafka_topic_partition_list_destroy(offsets);
    if (!status)
        release_selected(p, selected);
    else if (status == UD_COMMIT_UNKNOWN || status == UD_FENCED)
        close_consumer(p);
    return status;
}

static ud_status unsubscribe(void *state) {
    close_consumer(state);
    return UD_OK;
}

static ud_status transaction_error(kafka_state *p, rd_kafka_error_t *error, int committing) {
    if (!error)
        return UD_OK;
    ud_status status = kafka_status(rd_kafka_error_code(error));
    if (rd_kafka_error_is_fatal(error) || rd_kafka_error_txn_requires_abort(error)) {
        if (status != UD_FENCED && status != UD_AUTH)
            status = UD_IO;
    } else if (committing && (rd_kafka_error_is_retriable(error) || status == UD_TIMEOUT ||
                             status == UD_TRANSIENT)) {
        status = UD_COMMIT_UNKNOWN;
    } else if (rd_kafka_error_is_retriable(error)) {
        status = UD_TRANSIENT;
    }
    rd_kafka_error_destroy(error);
    if (atomic_load(&p->unauthorized) && status != UD_COMMIT_UNKNOWN)
        return UD_AUTH;
    return status;
}

static ud_status capture_epoch(void *state, const uint64_t *handles, uint32_t count,
                                ud_message_epoch *epoch) {
    kafka_state *p = state;
    if (!p->consumer || (!count && !p->frontier_count))
        return UD_INVALID;
    if (!count && p->count)
        return UD_BUSY;
    ud_status status = verify_ownership(p);
    if (status)
        return status;
    uint64_t generation = p->assignment;
    const ud_host_api *h = p->host;
    uint8_t selected[KAFKA_LIMIT_TOKENS] = {0};
    rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new((int)p->count);
    if (!offsets)
        return UD_RESOURCE;
    if (count)
        status = prefix(p, handles, count, selected, offsets);
    uint32_t frontier_count = 0;
    if (!status && p->frontier_count) {
        for (uint32_t i = 0; i < p->frontier_count; ++i)
            p->frontier[i].candidate = p->frontier[i].committed >= 0 ?
                    p->frontier[i].committed : p->frontier[i].start;
        for (int i = 0; !status && i < offsets->cnt; ++i) {
            const rd_kafka_topic_partition_t *offset = &offsets->elems[i];
            uint32_t slot = partition_slot(p, offset->partition);
            if (slot >= p->frontier_count || offset->offset < p->frontier[slot].committed)
                status = UD_FENCED;
            else
                p->frontier[slot].candidate = offset->offset;
        }
        if (!count && p->fixed_partition && p->cut_count && !p->cuts_remaining &&
            p->frontier[0].candidate < p->cuts[0].end)
            p->frontier[0].candidate = p->cuts[0].end;
        for (uint32_t i = 0; i < p->frontier_count; ++i)
            frontier_count += p->frontier[i].candidate >= 0;
    }
    char cluster[KAFKA_LIMIT_CLUSTER + 1];
    if (!status)
        status = cluster_id(p, p->consumer, cluster);
    if (!status)
        status = verify_ownership(p);
    rd_kafka_consumer_group_metadata_t *metadata = NULL;
    void *serialized = NULL;
    size_t metadata_size = 0;
    if (!status) {
        metadata = rd_kafka_consumer_group_metadata(p->consumer);
        if (!metadata)
            status = UD_FENCED;
    }
    if (!status)
        status = transaction_error(p, rd_kafka_consumer_group_metadata_write(metadata, &serialized,
                                                                                 &metadata_size), 0);
    if (!status && (rd_kafka_assignment_lost(p->consumer) || p->assignment != generation))
        status = UD_FENCED;
    if (!status && (!metadata_size || metadata_size > KAFKA_LIMIT_METADATA))
        status = UD_INCOMPATIBLE;
    if (!status)
        status = copy_value(h, serialized, metadata_size, &epoch->group_metadata);
    if (!status)
        status = copy_value(h, cluster, strlen(cluster), &epoch->cluster_id);
    uint32_t result_count = p->frontier_count ? frontier_count : (uint32_t)offsets->cnt;
    if (!status && result_count) {
        epoch->offsets = h->allocate(h->context, (size_t)result_count * sizeof(*epoch->offsets));
        if (!epoch->offsets)
            status = UD_RESOURCE;
        else
            memset(epoch->offsets, 0, (size_t)result_count * sizeof(*epoch->offsets));
    }
    uint32_t entries = p->frontier_count ? p->frontier_count : (uint32_t)offsets->cnt;
    for (uint32_t i = 0; !status && i < entries; ++i) {
        int64_t next = p->frontier_count ? p->frontier[i].candidate : offsets->elems[i].offset;
        if (next < 0)
            continue;
        ud_message_offset *output = &epoch->offsets[epoch->offset_count++];
        output->partition = p->fixed_partition ? (int32_t)(p->fixed_partition - 1) :
                            p->frontier_count ? (int32_t)i : offsets->elems[i].partition;
        output->next_offset = next;
        status = copy_value(h, p->topic, strlen(p->topic), &output->topic);
    }
    if (serialized)
        rd_kafka_mem_free(p->consumer, serialized);
    if (metadata)
        rd_kafka_consumer_group_metadata_destroy(metadata);
    rd_kafka_topic_partition_list_destroy(offsets);
    if (status)
        clear_epoch(h, epoch);
    return status;
}

static ud_status release_epoch(void *state, const uint64_t *handles, uint32_t count) {
    kafka_state *p = state;
    if (!p->consumer)
        return UD_INVALID;
    uint8_t selected[KAFKA_LIMIT_TOKENS] = {0};
    rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new((int)p->count);
    if (!offsets)
        return UD_RESOURCE;
    ud_status status = prefix(p, handles, count, selected, offsets);
    if (!status)
        confirm_frontier(p, offsets);
    rd_kafka_topic_partition_list_destroy(offsets);
    if (!status)
        release_selected(p, selected);
    return status;
}
static ud_status begin_epoch(void *state, ud_string transactional_id, ud_string expected_cluster) {
    kafka_state *p = state;
    if (p->consumer || p->transaction)
        return UD_BUSY;
    if (!transactional_id.data || !transactional_id.length || transactional_id.length > 1024 ||
        memchr(transactional_id.data, 0, transactional_id.length) ||
        (expected_cluster.length && !expected_cluster.data) || expected_cluster.length > KAFKA_LIMIT_CLUSTER)
        return UD_INVALID;
    if (p->transactional_id && (strlen(p->transactional_id) != transactional_id.length ||
        memcmp(p->transactional_id, transactional_id.data, transactional_id.length)))
        return UD_INCOMPATIBLE;
    ud_status status = control(p);
    int initialize_producer = !p->producer;
    char *identity = NULL;
    if (!status && initialize_producer && !p->transactional_id) {
        identity = plugin_string(p->host, transactional_id.data, transactional_id.length);
        if (!identity)
            status = UD_RESOURCE;
    }
    if (!status && initialize_producer) {
        status = create_client(p, RD_KAFKA_PRODUCER, p->transactional_id ? p->transactional_id :
                               identity, 0, &p->producer);
        if (!status)
            status = cluster_id(p, p->producer, p->producer_cluster);
    }
    if (!status && expected_cluster.length && (strlen(p->producer_cluster) != expected_cluster.length ||
        memcmp(p->producer_cluster, expected_cluster.data, expected_cluster.length)))
        status = UD_INCOMPATIBLE;
    if (!status && initialize_producer) {
        status = control(p);
        if (!status)
            status = transaction_error(p, rd_kafka_init_transactions(p->producer, KAFKA_TIMEOUT), 0);
        if (!status && identity) {
            p->transactional_id = identity;
            identity = NULL;
        }
    }
    p->host->deallocate(identity);
    if (status) {
        if (initialize_producer)
            close_producer(p);
        return status;
    }
    status = control(p);
    if (!status)
        status = transaction_error(p, rd_kafka_begin_transaction(p->producer), 0);
    if (!status)
        p->transaction = 1;
    else
        close_producer(p);
    return status;
}

static ud_status verify_epoch(void *state, const ud_message_epoch *epoch) {
    kafka_state *p = state;
    uint32_t limit = p->frontier_count ? p->frontier_count : KAFKA_LIMIT_TOKENS;
    if (!p->consumer || !epoch || !epoch->cluster_id.data || !epoch->cluster_id.length ||
        epoch->cluster_id.length > KAFKA_LIMIT_CLUSTER || (epoch->offset_count && !epoch->offsets) ||
        (!epoch->offset_count && !p->frontier_count) || epoch->offset_count > limit)
        return UD_INVALID;
    if (p->frontier_count && (p->assigned || p->frontier_frozen))
        return UD_BUSY;
    ud_status status = control(p);
    if (status)
        return status;
    size_t topic_length = strlen(p->topic);
    char actual[KAFKA_LIMIT_CLUSTER + 1];
    status = cluster_id(p, p->consumer, actual);
    if (!status && (strlen(actual) != epoch->cluster_id.length ||
                    memcmp(actual, epoch->cluster_id.data, epoch->cluster_id.length)))
        status = UD_FENCED;
    if (status)
        return status;
    if (p->frontier_count) {
        for (uint32_t i = 0; i < p->frontier_count; ++i)
            p->frontier[i].committed = -1;
        for (uint32_t i = 0; i < epoch->offset_count; ++i) {
            const ud_message_offset *offset = &epoch->offsets[i];
            uint32_t slot = partition_slot(p, offset->partition);
            if (!offset->topic.data || offset->topic.length != topic_length ||
                memcmp(offset->topic.data, p->topic, topic_length) || offset->next_offset < 0 ||
                slot >= p->frontier_count || p->frontier[slot].committed >= 0)
                return UD_INVALID;
            p->frontier[slot].committed = offset->next_offset;
        }
        /* Compare the entire frontier inside the actual assignment callback,
         * then assign the same numeric positions used for that proof. */
        p->frontier_frozen = 1;
        uint64_t deadline = now_ms() + KAFKA_TIMEOUT;
        while (!p->assigned && now_ms() < deadline) {
            status = control(p);
            if (status)
                return status;
            status = poll_consumer(p, wait_ms(deadline));
            if (status && status != UD_BUSY)
                return status;
        }
        if (!p->assigned)
            return UD_TIMEOUT;
        return rd_kafka_assignment_lost(p->consumer) ? UD_FENCED : control(p);
    }
    rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new((int)epoch->offset_count);
    if (!offsets)
        return UD_RESOURCE;
    for (uint32_t i = 0; !status && i < epoch->offset_count; ++i) {
        const ud_message_offset *offset = &epoch->offsets[i];
        if (!offset->topic.data || offset->topic.length != topic_length ||
            memcmp(offset->topic.data, p->topic, topic_length) ||
            offset->partition < 0 || offset->next_offset < 0 ||
            rd_kafka_topic_partition_list_find(offsets, p->topic, offset->partition)) {
            status = UD_INVALID;
        } else {
            rd_kafka_topic_partition_list_add(offsets, p->topic, offset->partition)->offset =
                RD_KAFKA_OFFSET_INVALID;
        }
    }
    if (!status)
        status = control(p);
    if (!status) {
        /* OffsetFetch uses this consumer's configured group and isolation level.
         * Recovery does not depend on the old member's serialized metadata. */
        rd_kafka_resp_err_t error = rd_kafka_committed(p->consumer, offsets, KAFKA_TIMEOUT);
        status = control(p);
        if (!status)
            status = error == RD_KAFKA_RESP_ERR_UNSTABLE_OFFSET_COMMIT ?
                         UD_COMMIT_UNKNOWN : kafka_status(error);
    }
    for (uint32_t i = 0; !status && i < epoch->offset_count; ++i) {
        const ud_message_offset *expected = &epoch->offsets[i];
        const rd_kafka_topic_partition_t *actual_offset =
            rd_kafka_topic_partition_list_find(offsets, p->topic, expected->partition);
        if (!actual_offset || actual_offset->err == RD_KAFKA_RESP_ERR_UNSTABLE_OFFSET_COMMIT)
            status = UD_COMMIT_UNKNOWN;
        else if (actual_offset->err)
            status = kafka_status(actual_offset->err);
        else if (actual_offset->offset < 0)
            status = UD_COMMIT_UNKNOWN;
        else if (actual_offset->offset < expected->next_offset ||
                 (!epoch->allow_advanced_offsets && actual_offset->offset != expected->next_offset))
            status = UD_FENCED;
    }
    rd_kafka_topic_partition_list_destroy(offsets);
    return status;
}

static ud_status commit_epoch(void *state, const ud_message_epoch *epoch) {
    kafka_state *p = state;
    if (!p->producer || p->transaction != 1)
        return UD_INVALID;
    if (!epoch->cluster_id.data || strlen(p->producer_cluster) != epoch->cluster_id.length ||
        memcmp(p->producer_cluster, epoch->cluster_id.data, epoch->cluster_id.length))
        return UD_INCOMPATIBLE;
    if (!epoch->group_metadata.data || !epoch->group_metadata.length ||
        epoch->group_metadata.length > KAFKA_LIMIT_METADATA ||
        (epoch->offset_count && !epoch->offsets) || epoch->offset_count > KAFKA_LIMIT_PARTITIONS)
        return UD_INVALID;
    ud_status status = control(p);
    rd_kafka_topic_partition_list_t *offsets = NULL;
    if (!status && epoch->offset_count) {
        offsets = rd_kafka_topic_partition_list_new((int)epoch->offset_count);
        if (!offsets)
            status = UD_RESOURCE;
    }
    for (uint32_t i = 0; !status && i < epoch->offset_count; ++i) {
        const ud_message_offset *offset = &epoch->offsets[i];
        if (!valid_topic(offset->topic) || offset->partition < 0 || offset->next_offset < 0) {
            status = UD_INVALID;
            break;
        }
        char topic[KAFKA_LIMIT_TOPIC + 1];
        memcpy(topic, offset->topic.data, offset->topic.length);
        topic[offset->topic.length] = 0;
        rd_kafka_topic_partition_list_add(offsets, topic, offset->partition)->offset = offset->next_offset;
    }
    rd_kafka_consumer_group_metadata_t *metadata = NULL;
    if (!status && offsets) {
        rd_kafka_topic_partition_list_sort(offsets, NULL, NULL);
        for (int i = 1; !status && i < offsets->cnt; ++i)
            if (offsets->elems[i].partition == offsets->elems[i - 1].partition &&
                !strcmp(offsets->elems[i].topic, offsets->elems[i - 1].topic))
                status = UD_INVALID;
    }
    if (!status && offsets) {
        rd_kafka_error_t *error = rd_kafka_consumer_group_metadata_read(&metadata,
                                 epoch->group_metadata.data, epoch->group_metadata.length);
        if (error) {
            rd_kafka_error_destroy(error);
            status = UD_INCOMPATIBLE;
        }
    }
    if (!status && offsets)
        status = transaction_error(p, rd_kafka_send_offsets_to_transaction(p->producer, offsets,
                                                                          metadata, KAFKA_TIMEOUT), 0);
    if (metadata)
        rd_kafka_consumer_group_metadata_destroy(metadata);
    if (offsets)
        rd_kafka_topic_partition_list_destroy(offsets);
    if (!status)
        status = control(p);
    if (!status)
        status = transaction_error(p, rd_kafka_commit_transaction(p->producer, KAFKA_TIMEOUT), 1);
    if (!status) {
        p->transaction = 0;
    } else if (status == UD_COMMIT_UNKNOWN || status == UD_FENCED) {
        close_producer(p);
        p->transaction = 1;
    }
    return status;
}

static ud_status abort_epoch(void *state) {
    kafka_state *p = state;
    if (!p->producer || !p->transaction) {
        p->transaction = 0;
        return UD_OK;
    }
    ud_status status = transaction_error(p, rd_kafka_abort_transaction(p->producer, KAFKA_TIMEOUT), 0);
    if (status)
        close_producer(p);
    else
        p->transaction = 0;
    return status;
}

static ud_status produce_record(kafka_state *p, const char *topic, const ud_message *input,
                                kafka_delivery *delivery) {
    uint64_t bytes = 128 + (uint64_t)input->payload.length + input->key.length;
    if ((!input->payload.data && input->payload.length) || (!input->key.data && input->key.length) ||
        (!input->headers && input->header_count) || input->header_count > KAFKA_LIMIT_HEADERS ||
        input->partition < RD_KAFKA_PARTITION_UA)
        return UD_INVALID;
    for (uint32_t i = 0; i < input->header_count; ++i) {
        const ud_message_header *header = &input->headers[i];
        if (!header->name.data || !header->name.length || header->name.length > 1024 ||
            memchr(header->name.data, 0, header->name.length) || (!header->value.data && header->value.length))
            return UD_INVALID;
        bytes += 16 + (uint64_t)header->name.length + header->value.length;
    }
    if (bytes > KAFKA_LIMIT_RECORD)
        return UD_OVERFLOW;
    rd_kafka_headers_t *headers = NULL;
    if (input->header_count) {
        headers = rd_kafka_headers_new(input->header_count);
        if (!headers)
            return UD_RESOURCE;
        for (uint32_t i = 0; i < input->header_count; ++i) {
            const ud_message_header *item = &input->headers[i];
            rd_kafka_resp_err_t err = rd_kafka_header_add(headers, item->name.data, item->name.length,
                                                         item->value.data, item->value.length);
            if (err) {
                rd_kafka_headers_destroy(headers);
                return kafka_status(err);
            }
        }
    }
    rd_kafka_resp_err_t err = rd_kafka_producev(p->producer, RD_KAFKA_V_TOPIC(topic),
        RD_KAFKA_V_PARTITION(input->partition), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE((void *)input->payload.data, input->payload.length),
        RD_KAFKA_V_KEY(input->key.data, input->key.length),
        RD_KAFKA_V_TIMESTAMP(input->timestamp_ms > 0 ? input->timestamp_ms : 0),
        RD_KAFKA_V_HEADERS(headers), RD_KAFKA_V_OPAQUE(delivery), RD_KAFKA_V_END);
    if (err && headers)
        rd_kafka_headers_destroy(headers);
    return kafka_status(err);
}

static ud_status publish(void *state, const ud_message_target *target, const ud_message *messages,
                          uint32_t count, ud_message_result *results) {
    kafka_state *p = state;
    ud_status status = p->consumer ? UD_BUSY : control(p);
    if (!status && p->transactional_id && !p->transaction)
        status = UD_BUSY;
    if (!status && p->transaction && !p->producer)
        status = UD_FENCED;
    if (!status && (target->kind != UD_MESSAGE_TOPIC || target->routing_key.length))
        status = UD_UNSUPPORTED;
    if (!status && !valid_topic(target->name))
        status = UD_INVALID;
#if SIZE_MAX <= UINT32_MAX
    if (!status && count > SIZE_MAX / sizeof(kafka_delivery))
        status = UD_OVERFLOW;
#endif
    int temporary = !p->transactional_id;
    if (!status && temporary)
        status = create_client(p, RD_KAFKA_PRODUCER, NULL, 0, &p->producer);
    kafka_delivery *delivery = status ? NULL : p->host->allocate(p->host->context,
                                                               (size_t)count * sizeof(*delivery));
    if (!status && !delivery)
        status = UD_RESOURCE;
    if (status) {
        if (temporary)
            close_producer(p);
        for (uint32_t i = 0; i < count; ++i)
            results[i] = (ud_message_result){UD_MESSAGE_NOT_SENT, status};
        return status;
    }
    memset(delivery, 0, (size_t)count * sizeof(*delivery));
    char topic[KAFKA_LIMIT_TOPIC + 1];
    memcpy(topic, target->name.data, target->name.length);
    topic[target->name.length] = 0;
    uint32_t queued = 0;
    uint64_t deadline = now_ms() + KAFKA_TIMEOUT;
    for (uint32_t i = 0; i < count; ++i) {
        status = control(p);
        if (!status && now_ms() >= deadline)
            status = UD_TIMEOUT;
        while (!status) {
            status = produce_record(p, topic, &messages[i], &delivery[i]);
            if (status != UD_BUSY || now_ms() >= deadline)
                break;
            rd_kafka_poll(p->producer, wait_ms(deadline));
            status = control(p);
        }
        if (status) {
            delivery[i].result = (ud_message_result){UD_MESSAGE_NOT_SENT, status};
            delivery[i].done = 1;
        } else {
            delivery[i].result = (ud_message_result){UD_MESSAGE_UNCERTAIN, UD_COMMIT_UNKNOWN};
            ++queued;
        }
        rd_kafka_poll(p->producer, 0);
    }
    while (queued && rd_kafka_outq_len(p->producer) && now_ms() < deadline && !control(p))
        rd_kafka_poll(p->producer, wait_ms(deadline));
    rd_kafka_poll(p->producer, 0);
    uint32_t unresolved = 0;
    int uncertain = 0;
    ud_status first = UD_OK;
    for (uint32_t i = 0; i < count; ++i) {
        results[i] = delivery[i].result;
        unresolved += !delivery[i].done;
        uncertain |= results[i].state == UD_MESSAGE_UNCERTAIN;
        if (results[i].result && !first)
            first = results[i].result;
    }
    /* Delivery reports refer into this call's array. Destruction joins the
     * native producer before unresolved callback storage can be released. */
    if (unresolved || temporary) {
        close_producer(p);
        if (!temporary)
            p->transaction = 1;
    }
    p->host->deallocate(delivery);
    return uncertain ? UD_COMMIT_UNKNOWN : first;
}

static ud_status partition_positions(void *state, ud_string topic, ud_string group,
    ud_message_partition_position *positions, uint32_t capacity, uint32_t *length) {
    kafka_state *p = state;
    if (p->consumer || p->producer || p->transaction)
        return UD_BUSY;
    if (!valid_topic(topic) || !group.length || group.length > 1024)
        return UD_INVALID;
    p->topic = plugin_string(p->host, topic.data, topic.length);
    char *group_id = plugin_string(p->host, group.data, group.length);
    /* The group client only reads offsets and never subscribes to records. */
    p->maximum = 1024;
    p->budget = 1024;
    p->credit = 1;
    uint32_t count = 0;
    ud_status status = p->topic && group_id
        ? create_client(p, RD_KAFKA_CONSUMER, group_id, UD_MESSAGE_POSITION_REQUIRE, &p->consumer)
        : UD_RESOURCE;
    p->host->deallocate(group_id);
    if (!status) status = source_partition_count(p, &count);
    if (!status && (!count || count > capacity)) status = UD_RESOURCE;
    rd_kafka_topic_partition_list_t *list = NULL;
    if (!status) {
        list = rd_kafka_topic_partition_list_new((int)count);
        p->frontier = p->host->allocate(p->host->context, count * sizeof(*p->frontier));
        if (!list || !p->frontier) status = UD_RESOURCE;
    }
    if (!status) {
        p->frontier_count = count;
        for (uint32_t i = 0; i < count; i++) {
            rd_kafka_topic_partition_list_add(list,p->topic,(int32_t)i)->offset = RD_KAFKA_OFFSET_INVALID;
            p->frontier[i].candidate = -1;
        }
        status = kafka_status(rd_kafka_committed(p->consumer, list, KAFKA_TIMEOUT));
    }
    for (uint32_t i = 0; !status && i < count; i++) {
        const rd_kafka_topic_partition_t *entry = &list->elems[i];
        status = kafka_status(entry->err);
        if (!status && entry->offset < 0 && entry->offset != RD_KAFKA_OFFSET_INVALID)
            status = UD_FENCED;
        positions[i] = (ud_message_partition_position){(int32_t)i, -1, -1,
            entry->offset == RD_KAFKA_OFFSET_INVALID ? -1 : entry->offset};
        list->elems[i].offset = RD_KAFKA_OFFSET_INVALID;
    }
    if (!status) status = query_cut_offsets(p,list,RD_KAFKA_OFFSET_SPEC_EARLIEST);
    for (uint32_t i = 0; !status && i < count; i++) {
        positions[i].earliest = p->frontier[i].candidate;
        p->frontier[i].candidate = -1;
    }
    if (!status) status = query_cut_offsets(p,list,RD_KAFKA_OFFSET_SPEC_LATEST);
    for (uint32_t i = 0; !status && i < count; i++) {
        positions[i].latest = p->frontier[i].candidate;
        if (positions[i].earliest < 0 || positions[i].latest < positions[i].earliest)
            status = UD_FENCED;
    }
    if (list) rd_kafka_topic_partition_list_destroy(list);
    close_consumer(p);
    if (!status) *length = count;
    return status;
}

static const ud_message_api messages = {
    .struct_size = sizeof(messages),
    .abi_version = UD_PLUGIN_ABI,
    .subscribe = subscribe,
    .receive = receive,
    .acknowledge = acknowledge,
    .unsubscribe = unsubscribe,
    .publish = publish,
    .capture_epoch = capture_epoch,
    .begin_epoch = begin_epoch,
    .commit_epoch = commit_epoch,
    .abort_epoch = abort_epoch,
    .release_epoch = release_epoch,
    .visit_committed = visit_committed,
    .verify_epoch = verify_epoch
};

static const ud_connector_api connector = {
    .struct_size = sizeof(connector),
    .abi_version = UD_PLUGIN_ABI,
    .capabilities = UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_PUBLISH |
                    UD_CONNECT_MESSAGE_TRANSACTION,
    .test = test,
    .cancel = cancel,
    .set_control = set_control,
    .message = &messages
};

UD_PLUGIN_EXPORT const ud_message_partition_api *ud_plugin_partition_api(uint32_t version) {
    static const ud_message_partition_api api = {
        sizeof(api), UD_PLUGIN_ABI, subscribe_at, partition_positions
    };
    return version == UD_PLUGIN_ABI ? &api : NULL;
}

UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){
        sizeof(*out), UD_PLUGIN_ABI, "org.universaldata.connector.kafka", UD_SDK_VERSION,
        UD_PLUGIN_CONNECTOR, UD_CAP_THREAD_SAFE | UD_CAP_READ | UD_CAP_WRITE,
        initialize, destroy, plugin_shutdown, &connector, NULL,
        sizeof(ud_field), sizeof(ud_value)
    };
    return UD_OK;
}
