#include "ud/plugin.h"
#include "../engines/package/internal.h"

enum { CASE_BINARY, CASE_JSON, CASE_REDELIVERY, CASE_INVALID, CASES };
enum { CASE_AVRO = CASES };
enum { BROKER_RABBITMQ, BROKER_KAFKA, BROKER_ROCKETMQ, BROKER_CLASSIC, BROKER_ARTEMIS };
enum { MAX_MESSAGE = 65536, MAX_IN_FLIGHT = 262144, BROKER_WAIT_MS = 45000 };

typedef struct registry_fixture {
    ud_string source, target, url, reference, credentials, subject;
    uint32_t reader_version, writer_id, reader_id, unknown_id;
} registry_fixture;

typedef struct broker_fixture {
    ud_context *memory;
    ud_plugin *plugin;
    const ud_json *endpoints;
    const char *driver;
    ud_string credentials, group, scratch;
    ud_string ingress[CASES], egress[2];
    ud_string transaction_source, transaction_target, checkpoint_topic;
    registry_fixture registry;
    int broker, deny_network;
    atomic_int deny_observed;
    ud_string deny_target;
} broker_fixture;

static const ud_string tenant = {"mq-fixture", 10};
static const ud_string resource = {"mq-fixture-resource", 19};
static const ud_string reference = {"fixture", 7};
static const ud_string header_name = {"fixture-header", 14};
static const ud_string header_value = {"fixture-value", 13};
static const ud_string message_key = {"fixture-key", 11};
static const char *const broker_names[] = {
    "rabbitmq", "kafka", "rocketmq", "activemq-classic", "activemq-artemis",
};
static const char structured_schema[] =
    "{\"version\":" UD_PROTOCOL_VERSION_TEXT
    ",\"fields\":[{\"name\":\"id\",\"type\":\"int64\",\"nullable\":false},"
                 "{\"name\":\"label\",\"type\":\"utf8\",\"nullable\":true}]}";
static const char registry_row_schema[] =
    "{\"version\":" UD_PROTOCOL_VERSION_TEXT
    ",\"fields\":[{\"name\":\"id\",\"type\":\"int64\",\"nullable\":false},"
                 "{\"name\":\"label\",\"type\":\"utf8\",\"nullable\":false}]}";

static ud_string text(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}

static int topic(const broker_fixture *f) {
    return f->broker == BROKER_KAFKA || f->broker == BROKER_ROCKETMQ;
}

static int fifo(const broker_fixture *f) {
    /* A dedicated Kafka partition or an exclusively consumed FIFO queue. RocketMQ NORMAL
     * can route a batch across several queues and does not promise a global order. */
    return f->broker != BROKER_ROCKETMQ;
}

static ud_status authorize(void *user, ud_string requested_tenant, ud_string requested_resource,
                            int32_t action) {
    broker_fixture *f = user;
    if (!ud_same(requested_tenant, tenant))
        return UD_AUTH;
    if (action == UD_ACCESS_WRITE && f->deny_target.length &&
        ud_same(f->deny_target, requested_resource)) {
        atomic_fetch_add(&f->deny_observed, 1);
        return UD_AUTH;
    }
    if (action != UD_ACCESS_NETWORK || ud_same(requested_resource, resource))
        return UD_OK;
    if (f->deny_network) {
        atomic_fetch_add(&f->deny_observed, 1);
        return UD_AUTH;
    }
    ud_string registry = f->registry.url;
    if (registry.length && ud_same(registry, requested_resource))
        return UD_OK;
    if (registry.length && requested_resource.length > registry.length &&
        !memcmp(requested_resource.data, registry.data, registry.length)) {
        ud_string path = {requested_resource.data + registry.length,
                          requested_resource.length - registry.length};
        if ((path.length > 10 && !memcmp(path.data, "/subjects/", 10)) ||
            (path.length > 13 && !memcmp(path.data, "/schemas/ids/", 13)))
            return UD_OK;
    }
    for (uint32_t i = 0; i < ud_json_size(f->endpoints); i++) {
        ud_string endpoint;
        if (!ud_json_string(ud_json_at(f->endpoints, i), &endpoint) &&
            ud_same(endpoint, requested_resource))
            return UD_OK;
    }
    return UD_AUTH;
}

static ud_status resolve(void *user, ud_context *memory, ud_string requested_tenant,
                          ud_string requested_resource, ud_string requested_reference,
                          ud_string *out) {
    broker_fixture *f = user;
    if (!ud_same(requested_tenant, tenant))
        return UD_AUTH;
    ud_string secret;
    if (ud_same(requested_resource, resource) && ud_same(requested_reference, reference))
        secret = f->credentials;
    else if (f->registry.url.length && ud_same(requested_resource, f->registry.url) &&
             ud_same(requested_reference, f->registry.reference))
        secret = f->registry.credentials;
    else
        return UD_AUTH;
    char *credentials = ud_strdup_n(memory, secret.data, secret.length);
    if (!credentials)
        return UD_RESOURCE;
    *out = (ud_string){credentials, secret.length};
    return UD_OK;
}

static ud_status configure_registry(broker_fixture *f, const ud_json *config) {
    const char *keys[] = {"source", "target", "url", "credentialReference", "credentials", "subject",
                          "readerVersion", "writerId", "readerId", "unknownId"};
    UD_TRY(ud_json_keys(config, keys, sizeof(keys) / sizeof(*keys)));
    if (f->broker != BROKER_KAFKA)
        return UD_SCHEMA;
    registry_fixture *registry = &f->registry;
    ud_string *strings[] = {&registry->source, &registry->target, &registry->url,
                           &registry->reference, &registry->subject};
    const uint32_t indices[] = {0, 1, 2, 3, 5};
    for (uint32_t i = 0; i < sizeof(strings) / sizeof(*strings); i++) {
        UD_TRY(ud_json_string(ud_member(config, keys[indices[i]]), strings[i]));
        if (!strings[i]->length || memchr(strings[i]->data, 0, strings[i]->length))
            return UD_SCHEMA;
    }
    while (registry->url.length && registry->url.data[registry->url.length - 1] == '/')
        registry->url.length--;
    if (ud_same(registry->source, registry->target) ||
        !((registry->url.length > 7 && !memcmp(registry->url.data, "http://", 7)) ||
          (registry->url.length > 8 && !memcmp(registry->url.data, "https://", 8))) ||
        memchr(registry->url.data, '?', registry->url.length) ||
        memchr(registry->url.data, '#', registry->url.length))
        return UD_SCHEMA;
    uint32_t *numbers[] = {&registry->reader_version, &registry->writer_id, &registry->reader_id,
                           &registry->unknown_id};
    for (uint32_t i = 0; i < sizeof(numbers) / sizeof(*numbers); i++) {
        uint64_t value;
        UD_TRY(ud_json_uint(ud_member(config, keys[i + 6]), &value));
        if (!value || value > INT32_MAX)
            return UD_SCHEMA;
        *numbers[i] = (uint32_t)value;
    }
    const ud_json *credentials = ud_member(config, "credentials");
    if (registry->reader_version < 2 || registry->writer_id == registry->reader_id ||
        registry->writer_id == registry->unknown_id || registry->reader_id == registry->unknown_id ||
        ud_json_kind(ud_member(credentials, "headers")) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    char *secret = NULL;
    UD_TRY(ud_json_serialize(credentials, &secret, &registry->credentials.length));
    registry->credentials.data = secret;
    return UD_OK;
}

static ud_status configure(broker_fixture *f, const ud_json *config, int registry_mode) {
    const char *keys[] = {"broker", "endpoints", "credentials", "group", "scratchDirectory",
                          "binarySource", "binaryTarget", "jsonSource", "jsonTarget",
                          "redeliverySource", "invalidSource", "transactionSource",
                          "transactionTarget", "checkpointTopic", "registry"};
    UD_TRY(ud_json_keys(config, keys, sizeof(keys) / sizeof(*keys)));
    ud_string broker;
    UD_TRY(ud_json_string(ud_member(config, "broker"), &broker));
    f->broker = -1;
    for (uint32_t i = 0; i < sizeof(broker_names) / sizeof(*broker_names); i++)
        if (ud_named(broker, broker_names[i]))
            f->broker = (int)i;
    if (f->broker < 0)
        return UD_SCHEMA;
    f->endpoints = ud_member(config, "endpoints");
    const ud_json *credentials = ud_member(config, "credentials");
    if (ud_json_kind(f->endpoints) != UD_JSON_ARRAY || !ud_json_size(f->endpoints) ||
        ud_json_kind(credentials) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    if (f->broker == BROKER_CLASSIC || f->broker == BROKER_ARTEMIS) {
        ud_string variant;
        UD_TRY(ud_json_string(ud_member(credentials, "variant"), &variant));
        if (!ud_named(variant, f->broker == BROKER_CLASSIC ? "classic" : "artemis"))
            return UD_SCHEMA;
    }
    for (uint32_t i = 0; i < ud_json_size(f->endpoints); i++) {
        ud_string endpoint;
        UD_TRY(ud_json_string(ud_json_at(f->endpoints, i), &endpoint));
        if (!endpoint.length || memchr(endpoint.data, 0, endpoint.length))
            return UD_SCHEMA;
    }
    UD_TRY(ud_json_string(ud_member(config, "scratchDirectory"), &f->scratch));
    if (!f->scratch.length || memchr(f->scratch.data, 0, f->scratch.length))
        return UD_SCHEMA;
    if (topic(f)) {
        UD_TRY(ud_json_string(ud_member(config, "group"), &f->group));
        if (!f->group.length)
            return UD_SCHEMA;
    } else if (ud_member(config, "group")) {
        return UD_SCHEMA;
    }
    char *secret = NULL;
    UD_TRY(ud_json_serialize(credentials, &secret, &f->credentials.length));
    f->credentials.data = secret;
    if (registry_mode)
        return configure_registry(f, ud_member(config, "registry"));
    ud_string *addresses[] = {f->ingress + CASE_BINARY, f->egress + CASE_BINARY,
                             f->ingress + CASE_JSON, f->egress + CASE_JSON,
                             f->ingress + CASE_REDELIVERY, f->ingress + CASE_INVALID,
                             &f->transaction_source, &f->transaction_target, &f->checkpoint_topic};
    uint32_t count = f->broker == BROKER_KAFKA ? sizeof(addresses) / sizeof(*addresses) : 6;
    for (uint32_t i = count; i < sizeof(addresses) / sizeof(*addresses); i++)
        if (ud_member(config, keys[i + 5]))
            return UD_SCHEMA;
    for (uint32_t i = 0; i < count; i++) {
        UD_TRY(ud_json_string(ud_member(config, keys[i + 5]), addresses[i]));
        if (!addresses[i]->length || memchr(addresses[i]->data, 0, addresses[i]->length))
            return UD_SCHEMA;
        for (uint32_t j = 0; j < i; j++)
            if (ud_same(*addresses[i], *addresses[j]))
                return UD_SCHEMA;
    }
    return UD_OK;
}

static ud_message_target target(const broker_fixture *f, ud_string address) {
    return (ud_message_target){.kind = topic(f) ? UD_MESSAGE_TOPIC : UD_MESSAGE_QUEUE,
                                .name = address};
}

static ud_status open_connection(broker_fixture *f, ud_connection **out) {
    ud_connection_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION,
                                     .tenant = tenant, .resource = resource,
                                     .credential_reference = reference,
                                     .authorize_user = f, .authorize = authorize,
                                     .resolve_user = f, .resolve = resolve};
    return ud_connection_open(f->plugin, &options, out);
}

static ud_status open_subscription(broker_fixture *f, ud_string address, ud_connection **out) {
    UD_TRY(open_connection(f, out));
    ud_message_subscription subscription = {
        .struct_size = sizeof(subscription), .abi_version = UD_ABI_VERSION,
        .source = target(f, address), .group = f->group,
        .initial_position = f->broker == BROKER_KAFKA ? UD_MESSAGE_POSITION_EARLIEST :
                                                       UD_MESSAGE_POSITION_REQUIRE,
        .prefetch = 4, .max_message_bytes = MAX_MESSAGE, .max_in_flight_bytes = MAX_IN_FLIGHT};
    ud_status status = ud_connection_message_subscribe(*out, &subscription);
    if (status) {
        (void)ud_connection_close(*out);
        *out = NULL;
    }
    return status;
}

static ud_message test_message(const broker_fixture *f, ud_string bytes, ud_message_header *header) {
    return (ud_message){.payload = bytes, .key = message_key, .headers = header, .header_count = 1,
                        .partition = f->broker == BROKER_KAFKA ? 0 : -1,
                        .offset = -1, .timestamp_ms = -1};
}

static ud_status publish_at(broker_fixture *f, ud_string address, const ud_string *values,
                             uint32_t count, int32_t partition) {
    enum { capacity = 4 };
    ud_message messages[capacity];
    ud_message_result results[capacity] = {{0}};
    if (count > capacity)
        return UD_INVALID;
    ud_connection *connection = NULL;
    UD_TRY(open_connection(f, &connection));
    ud_message_header header = {header_name, header_value};
    for (uint32_t i = 0; i < count; i++) {
        messages[i] = test_message(f, values[i], &header);
        if (partition >= 0)
            messages[i].partition = partition;
    }
    ud_message_target destination = target(f, address);
    ud_status status = ud_connection_message_publish(connection, &destination, messages,
                                                     count, results);
    for (uint32_t i = 0; i < count; i++)
        if (results[i].state != UD_MESSAGE_ACCEPTED || results[i].result) {
            fprintf(stderr, "Publish entry %u state=%d result=%d\n", i, results[i].state,
                    results[i].result);
            if (!status)
                status = results[i].result ? results[i].result : UD_IO;
        }
    ud_status closed = ud_connection_close(connection);
    return status ? status : closed;
}

static ud_status publish(broker_fixture *f, ud_string address, const ud_string *values,
                          uint32_t count) {
    return publish_at(f, address, values, count, -1);
}

static ud_status receive_until(ud_connection *connection, uint32_t timeout, ud_message *message) {
    uint64_t deadline = ud_now_ms() + timeout;
    ud_status status;
    do {
        status = ud_connection_message_receive(connection, 250, message);
    } while (status == UD_BUSY && ud_now_ms() < deadline);
    return status;
}

static ud_status assert_empty(broker_fixture *f, ud_string address, uint32_t timeout) {
    ud_connection *connection = NULL;
    UD_TRY(open_subscription(f, address, &connection));
    ud_message message = {0};
    ud_status status = receive_until(connection, timeout, &message);
    ud_message_clear(&message);
    ud_status closed = ud_connection_close(connection);
    /* Intentionally leave an unexpected delivery unsettled. */
    return status == UD_BUSY ? closed : status ? status : UD_CORRUPT;
}

static ud_status verify_metadata(const ud_message *message) {
    if (!message->delivery || !ud_same(message->key, message_key) ||
        !message->address.length)
        return UD_CORRUPT;
    /* Queue addresses may be broker-qualified. The target is separately proved by
     * opening a dedicated authorized subscription for each known address. */
    for (uint32_t i = 0; i < message->header_count; i++)
        if (ud_same(message->headers[i].name, header_name))
            return ud_same(message->headers[i].value, header_value) ? UD_OK : UD_CORRUPT;
    return UD_CORRUPT;
}

static ud_status json_row(broker_fixture *f, ud_string value, uint32_t *index) {
    ud_json *row = NULL;
    ud_status status = ud_json_parse(f->memory, value.data, value.length, &row, NULL);
    if (!status) {
        uint64_t id = 0;
        status = ud_json_uint(ud_member(row, "id"), &id);
        const ud_json *label = ud_member(row, "label");
        ud_string label_value = {0};
        if (!status && (ud_json_kind(row) != UD_JSON_OBJECT || ud_json_size(row) != 2 ||
                        (id != 1 && id != 2) ||
                        (id == 1 && (ud_json_string(label, &label_value) ||
                                     !ud_named(label_value, "broker-record"))) ||
                        (id == 2 && ud_json_kind(label) != UD_JSON_NULL)))
            status = UD_CORRUPT;
        if (!status)
            *index = (uint32_t)id - 1;
    }
    ud_json_destroy(row);
    return status;
}

static ud_status output_matches(broker_fixture *f, ud_string address, int format,
                                const ud_string *expected, uint32_t count) {
    ud_connection *connection = NULL;
    UD_TRY(open_subscription(f, address, &connection));
    ud_status status = UD_OK;
    uint32_t seen = 0;
    uint64_t deliveries[3] = {0};
    int64_t previous_offset = -1;
    for (uint32_t i = 0; !status && i < count; i++) {
        ud_message message = {0};
        status = receive_until(connection, BROKER_WAIT_MS, &message);
        if (!status)
            status = verify_metadata(&message);
        uint32_t index = count;
        if (!status && format == CASE_JSON)
            status = json_row(f, message.payload, &index);
        if (!status && format == CASE_BINARY)
            for (uint32_t j = 0; j < count; j++)
                if (!!message.payload.data == !!expected[j].data &&
                    ud_same(message.payload, expected[j])) {
                    index = j;
                    break;
                }
        if (!status &&
            (index >= count || (seen & (UINT32_C(1) << index)) || (fifo(f) && index != i)))
            status = UD_CORRUPT;
        if (!status && f->broker == BROKER_KAFKA &&
            (message.partition != 0 || message.offset <= previous_offset))
            status = UD_CORRUPT;
        if (!status) {
            seen |= UINT32_C(1) << index;
            deliveries[i] = message.delivery;
            previous_offset = message.offset;
        }
        ud_message_clear(&message);
    }
    if (!status)
        status = ud_connection_message_acknowledge(connection, deliveries, count);
    ud_status closed = ud_connection_close(connection);
    return status ? status : closed;
}

static ud_status append(ud_bytes *json, const char *value) {
    return ud_bytes_put(json, value, strlen(value));
}

static ud_status quoted(ud_bytes *json, ud_string value) {
    return ud_json_quote(json, value.data, value.length);
}

static ud_status registry_codec(ud_bytes *json, const registry_fixture *registry) {
    UD_TRY(append(json, ",\"registryConnection\":\"schemas\",\"schemaSubject\":"));
    UD_TRY(quoted(json, registry->subject));
    char version[48];
    snprintf(version, sizeof(version), ",\"schemaVersion\":%u", (unsigned)registry->reader_version);
    return append(json, version);
}

static ud_status package_document(broker_fixture *f, int format, ud_string input, ud_string output,
                                   ud_bytes *json) {
    const char *encoding = format == CASE_AVRO ? "avro" : format == CASE_JSON ? "json" : "binary";
    UD_TRY(append(json, "{\"version\":" UD_PROTOCOL_VERSION_TEXT
                        ",\"id\":\"broker-fixture\",\"connections\":[{\"id\":\"broker\",\"driver\":"));
    UD_TRY(quoted(json, text(f->driver)));
    UD_TRY(append(json, ",\"resource\":\"mq-fixture-resource\",\"credentialReference\":\"fixture\"}"));
    if (format == CASE_AVRO) {
        UD_TRY(append(json, ",{\"id\":\"schemas\",\"driver\":\"http\",\"resource\":"));
        UD_TRY(quoted(json, f->registry.url));
        UD_TRY(append(json, ",\"credentialReference\":"));
        UD_TRY(quoted(json, f->registry.reference));
        UD_TRY(append(json, "}"));
    }
    UD_TRY(append(json, "],\"controlFlow\":{\"tasks\":[{\"id\":\"execute\",\"kind\":\"dataFlow\","
                        "\"config\":{\"pipeline\":\"messages\"}}],\"constraints\":[]},"
                        "\"dataFlows\":[{\"id\":\"messages\",\"components\":[{\"id\":\"input\","
                        "\"component\":\"mq.source\",\"config\":{\"connection\":\"broker\","
                        "\"targetType\":"));
    UD_TRY(quoted(json, text(topic(f) ? "topic" : "queue")));
    UD_TRY(append(json, ",\"address\":"));
    UD_TRY(quoted(json, input));
    UD_TRY(append(json, ",\"format\":"));
    UD_TRY(quoted(json, text(encoding)));
    UD_TRY(append(json, ",\"prefetch\":4,\"maxMessageBytes\":65536,\"maxInFlightBytes\":262144"));
    if (topic(f)) {
        UD_TRY(append(json, ",\"group\":"));
        UD_TRY(quoted(json, f->group));
    }
    if (f->broker == BROKER_KAFKA)
        UD_TRY(append(json, ",\"initialPosition\":\"earliest\""));
    if (format != CASE_BINARY) {
        UD_TRY(append(json, ",\"schema\":"));
        UD_TRY(append(json, format == CASE_AVRO ? registry_row_schema : structured_schema));
    }
    if (format == CASE_AVRO)
        UD_TRY(registry_codec(json, &f->registry));
    UD_TRY(append(json, "}},{\"id\":\"output\",\"component\":\"mq.destination\",\"config\":{"
                        "\"connection\":\"broker\",\"targetType\":"));
    UD_TRY(quoted(json, text(topic(f) ? "topic" : "queue")));
    UD_TRY(append(json, ",\"address\":"));
    UD_TRY(quoted(json, output));
    UD_TRY(append(json, ",\"format\":"));
    UD_TRY(quoted(json, text(encoding)));
    if (format == CASE_AVRO)
        UD_TRY(registry_codec(json, &f->registry));
    return append(json, ",\"maxMessageBytes\":65536}}],"
                        "\"paths\":[{\"from\":\"input\",\"output\":0,\"to\":\"output\",\"input\":0}]}]}");
}

static ud_status await_commit_and_stop(ud_execution *execution, uint32_t count) {
    ud_execution_metrics metrics = {.struct_size = sizeof(metrics), .abi_version = UD_ABI_VERSION};
    uint64_t deadline = ud_now_ms() + BROKER_WAIT_MS;
    while (ud_now_ms() < deadline) {
        ud_status status = ud_execution_wait(execution, 100);
        if (status != UD_BUSY)
            return status ? status : UD_CORRUPT;
        UD_TRY(ud_execution_get_metrics(execution, &metrics));
        if (metrics.committed_rows > count)
            return UD_CORRUPT;
        if (metrics.committed_rows == count) {
            ud_execution_stop(execution);
            status = ud_execution_wait(execution, BROKER_WAIT_MS);
            if (status == UD_BUSY)
                return UD_TIMEOUT;
            UD_TRY(status);
            UD_TRY(ud_execution_get_metrics(execution, &metrics));
            return metrics.state == UD_EXEC_STOPPED && metrics.committed_rows == count &&
                           metrics.rejected_rows == 0 ? UD_OK : UD_CORRUPT;
        }
    }
    return UD_TIMEOUT;
}

static ud_status pipeline_case(broker_fixture *f, int format, ud_string input, ud_string output,
                                const char *journal, ud_status expected_failure, uint32_t count) {
    ud_bytes json = {.ctx = f->memory, .limit = 65536};
    char *state = NULL;
    ud_status status = package_document(f, format, input, output, &json);
    if (!status)
        status = ud_join_path(f->memory, f->scratch.data, journal, &state);
    ud_execution_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION,
        .context = {.struct_size = sizeof(ud_context_options), .abi_version = UD_ABI_VERSION,
                    .memory_limit = 128 * 1024 * 1024}, .edge_capacity = 2,
        .max_batch_rows = 4, .max_depth = 32, .max_concurrency = 2, .timeout_ms = 100000,
        .tenant = tenant, .authorize = authorize, .authorize_user = f,
        .resolve = resolve, .resolve_user = f};
    if (!status)
        options.state_directory = text(state);
    ud_execution *execution = NULL;
    if (!status)
        status = ud_execution_create_json((ud_string){(char *)json.data, (uint32_t)json.size},
                                          &options, &execution);
    if (!status)
        status = ud_execution_start(execution);
    if (!status) {
        if (expected_failure) {
            ud_status failed = ud_execution_wait(execution, BROKER_WAIT_MS);
            ud_execution_metrics metrics = {.struct_size = sizeof(metrics),
                                             .abi_version = UD_ABI_VERSION};
            status = ud_execution_get_metrics(execution, &metrics);
            if (!status && (metrics.state != UD_EXEC_FAILED || metrics.committed_rows ||
                            failed != expected_failure)) {
                fprintf(stderr, "Expected %s (%d), got %s (%d), execution state %d\n",
                        ud_status_name(expected_failure), expected_failure, ud_status_name(failed),
                        failed, metrics.state);
                status = UD_CORRUPT;
            }
        } else {
            status = await_commit_and_stop(execution, count);
        }
        if (status) {
            ud_execution_cancel(execution);
            (void)ud_execution_wait(execution, BROKER_WAIT_MS);
            char *diagnostics = NULL;
            uint32_t length = 0;
            if (!ud_execution_diagnostics(execution, &diagnostics, &length)) {
                fprintf(stderr, "Broker pipeline diagnostics: %.*s\n", (int)length, diagnostics);
                ud_free(diagnostics);
            }
        }
    }
    if (execution) {
        ud_status destroyed = ud_execution_destroy(execution);
        if (!status)
            status = destroyed;
    }
    ud_free(state);
    ud_bytes_clear(&json);
    return status;
}

static ud_status pipeline(broker_fixture *f, int format, int invalid, uint32_t count) {
    const char *journal = invalid ? "invalid-journal" :
                          format == CASE_JSON ? "json-journal" : "binary-journal";
    return pipeline_case(f, format, f->ingress[invalid ? CASE_INVALID : format], f->egress[format],
                         journal, invalid ? UD_PARSE : UD_OK, count);
}

static ud_status expect_denial(broker_fixture *f, int endpoint) {
    char target_resource[1024];
    ud_string address = f->ingress[CASE_BINARY];
    int length = snprintf(target_resource, sizeof(target_resource), "%.*s/%s/%.*s",
                          (int)resource.length, resource.data, topic(f) ? "topic" : "queue",
                          (int)address.length, address.data);
    if (length < 0 || (size_t)length >= sizeof(target_resource))
        return UD_OVERFLOW;
    f->deny_network = endpoint;
    f->deny_target = endpoint ? (ud_string){0} : text(target_resource);
    atomic_store(&f->deny_observed, 0);
    ud_connection *connection = NULL;
    ud_status status = open_connection(f, &connection);
    ud_message_result result = {UD_MESSAGE_NOT_SENT, UD_OK};
    if (!status) {
        ud_message_header header = {header_name, header_value};
        ud_message message = test_message(f, text("unauthorized"), &header);
        ud_message_target destination = target(f, address);
        status = ud_connection_message_publish(connection, &destination, &message, 1, &result);
    }
    if (connection)
        (void)ud_connection_close(connection);
    f->deny_network = 0;
    f->deny_target = (ud_string){0};
    return status == UD_AUTH && atomic_load(&f->deny_observed) &&
                   result.state == UD_MESSAGE_NOT_SENT ? UD_OK : UD_CORRUPT;
}

static ud_status receive_test(broker_fixture *f, ud_string address, ud_string expected,
                              uint32_t timeout, ud_message *message,
                              ud_connection **connection) {
    UD_TRY(open_subscription(f, address, connection));
    UD_TRY(receive_until(*connection, timeout, message));
    UD_TRY(verify_metadata(message));
    return ud_same(message->payload, expected) ? UD_OK : UD_CORRUPT;
}

static ud_status redelivery(broker_fixture *f, int invalid) {
    ud_string address = f->ingress[invalid ? CASE_INVALID : CASE_REDELIVERY];
    ud_string value = text(invalid ? "{\"id\":1,\"label\":" : "do-not-ack");
    UD_TRY(publish(f, address, &value, 1));
    if (invalid)
        UD_TRY(pipeline(f, CASE_JSON, 1, 0));
    ud_connection *connection = NULL;
    ud_message first = {0}, repeated = {0};
    ud_status status = UD_OK;
    if (!invalid) {
        status = receive_test(f, address, value, BROKER_WAIT_MS, &first, &connection);
        if (connection) {
            ud_status closed = ud_connection_close(connection);
            connection = NULL;
            if (!status)
                status = closed;
        }
    }
    if (!status)
        status = receive_test(f, address, value,
                              f->broker == BROKER_ROCKETMQ ? 90000 : BROKER_WAIT_MS,
                              &repeated, &connection);
    if (!status && f->broker == BROKER_KAFKA) {
        if (repeated.partition != 0 || repeated.offset < 0 ||
            (!invalid && (first.partition != repeated.partition || first.offset != repeated.offset)))
            status = UD_CORRUPT;
    } else if (!status && !repeated.redelivered && f->broker != BROKER_CLASSIC &&
               f->broker != BROKER_ARTEMIS) {
        status = UD_CORRUPT;
    }
    /* Only settle a verified record, after proving unsuccessful processing left it visible. */
    if (!status)
        status = ud_connection_message_acknowledge(connection, &repeated.delivery, 1);
    if (connection) {
        ud_status closed = ud_connection_close(connection);
        if (!status)
            status = closed;
    }
    ud_message_clear(&first);
    ud_message_clear(&repeated);
    if (!status && invalid)
        status = assert_empty(f, f->egress[CASE_JSON], 5000);
    return status;
}

static ud_status rabbit_confirmation_gap(broker_fixture *f) {
    ud_string address = f->ingress[CASE_REDELIVERY];
    const ud_string values[] = {text("first"), text("second"), text("unconfirmed"), text("fourth")};
    UD_TRY(publish(f, address, values, 4));
    ud_connection *connection = NULL;
    UD_TRY(open_subscription(f, address, &connection));
    uint64_t handles[4] = {0};
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < 4; i++) {
        ud_message message = {0};
        status = receive_until(connection, BROKER_WAIT_MS, &message);
        if (!status && !ud_same(message.payload, values[i]))
            status = UD_CORRUPT;
        handles[i] = message.delivery;
        ud_message_clear(&message);
    }
    const uint64_t confirmed[] = {handles[3], handles[1], handles[0]};
    if (!status)
        status = ud_connection_message_acknowledge(connection, confirmed, 3);
    ud_status closed = ud_connection_close(connection);
    UD_TRY(status ? status : closed);
    return output_matches(f, address, CASE_BINARY, values + 2, 1);
}

static ud_status round_trip(broker_fixture *f, int format) {
    const uint8_t binary[] = {0, 127, 255};
    const ud_string binary_values[] = {{(const char *)binary, sizeof(binary)}, {"", 0}, {NULL, 0}};
    const ud_string rocket_values[] = {{(const char *)binary, sizeof(binary)}, {"second", 6}};
    const ud_string json_values[] = {
        {"{\"id\":1,\"label\":\"broker-record\"}", sizeof("{\"id\":1,\"label\":\"broker-record\"}") - 1},
        {"{\"id\":2,\"label\":null}", sizeof("{\"id\":2,\"label\":null}") - 1},
    };
    const int rocket = f->broker == BROKER_ROCKETMQ;
    const ud_string *expected = format == CASE_JSON ? json_values :
                                rocket ? rocket_values : binary_values;
    uint32_t count = format == CASE_JSON || rocket ? 2 : 3;
    UD_TRY(publish(f, f->ingress[format], expected, count));
    UD_TRY(pipeline(f, format, 0, count));
    UD_TRY(output_matches(f, f->egress[format], format, expected, count));
    UD_TRY(assert_empty(f, f->egress[format], 5000));
    /* RocketMQ deliveries are invisible for the driver's 60-second receipt lease.
     * Observe beyond it to distinguish ACK from a lease merely hiding the input. */
    return assert_empty(f, f->ingress[format], rocket ? 75000 : 5000);
}


static ud_status transaction_document(broker_fixture *f, ud_bytes *json) {
    UD_TRY(append(json, "{\"version\":" UD_PROTOCOL_VERSION_TEXT
                        ",\"id\":\"broker-transaction-fixture\",\"connections\":[{\"id\":\"broker\",\"driver\":"));
    UD_TRY(quoted(json, text(f->driver)));
    UD_TRY(append(json, ",\"resource\":\"mq-fixture-resource\",\"credentialReference\":\"fixture\"}],"
                        "\"controlFlow\":{\"tasks\":[{\"id\":\"execute\",\"kind\":\"dataFlow\","
                        "\"config\":{\"pipeline\":\"messages\"}}],\"constraints\":[]},"
                        "\"dataFlows\":[{\"id\":\"messages\",\"delivery\":{\"mode\":\"kafkaTransaction\","
                        "\"checkpointPartition\":0,\"checkpointTopic\":"));
    UD_TRY(quoted(json, f->checkpoint_topic));
    UD_TRY(append(json, "},\"components\":[{\"id\":\"input\",\"component\":\"mq.source\","
                        "\"config\":{\"connection\":\"broker\",\"targetType\":\"topic\",\"address\":"));
    UD_TRY(quoted(json, f->transaction_source));
    UD_TRY(append(json, ",\"group\":"));
    UD_TRY(quoted(json, f->group));
    UD_TRY(append(json, ",\"format\":\"json\",\"initialPosition\":\"earliest\","
                        "\"prefetch\":4,\"maxMessageBytes\":65536,\"maxInFlightBytes\":262144,"
                        "\"partition\":1,\"endOffsets\":[{\"partition\":1,\"nextOffset\":1}],"
                        "\"schema\":{\"version\":" UD_PROTOCOL_VERSION_TEXT
                        ",\"fields\":[{\"name\":\"time\",\"type\":\"int64\",\"nullable\":false},"
                        "{\"name\":\"key\",\"type\":\"utf8\",\"nullable\":false},"
                        "{\"name\":\"value\",\"type\":\"int64\",\"nullable\":false}]} }},"
                        "{\"id\":\"window\",\"component\":\"eventWindow\",\"config\":{\"mode\":\"tumbling\","
                        "\"timeField\":\"time\",\"sizeMicros\":10,\"latePolicy\":\"fail\",\"plan\":{"
                        "\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"group\":[\"key\"],\"aggregates\":[{"
                        "\"function\":\"sum\",\"column\":\"value\",\"name\":\"total\"}]} }},"
                        "{\"id\":\"output\",\"component\":\"mq.destination\",\"config\":{"
                        "\"connection\":\"broker\",\"targetType\":\"topic\",\"address\":"));
    UD_TRY(quoted(json, f->transaction_target));
    return append(json, ",\"format\":\"json\",\"maxMessageBytes\":65536,"
                        "\"repartition\":{\"partitions\":2,\"keys\":[\"key\"]}}}],"
                        "\"paths\":[{\"from\":\"input\",\"output\":0,\"to\":\"window\",\"input\":0},"
                        "{\"from\":\"window\",\"output\":0,\"to\":\"output\",\"input\":0}]}]}");
}

static void pause_committed_owner(void *unused, const ud_execution_event *event) {
    (void)unused;
    if (event->kind == UD_EXEC_EVENT_TARGET_COMMITTED && ud_named(event->component_id, "output")) {
        puts("KAFKA_OWNER_COMMITTED");
        fflush(stdout);
        ud_sleep_ms(60000);
    }
}

static ud_status run_transaction(broker_fixture *f, ud_string document, int restore,
                                 ud_execution_observer observer) {
    char *state = NULL;
    UD_TRY(ud_join_path(f->memory, f->scratch.data,
                        restore ? "transaction-restore-journal" : "transaction-journal", &state));
    ud_execution_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION,
        .context = {.struct_size = sizeof(ud_context_options), .abi_version = UD_ABI_VERSION,
                    .memory_limit = 128 * 1024 * 1024}, .edge_capacity = 2,
        .max_batch_rows = 4, .max_depth = 32, .max_concurrency = 2, .timeout_ms = 100000,
        .tenant = tenant, .authorize = authorize, .authorize_user = f,
        .resolve = resolve, .resolve_user = f, .state_directory = text(state),
        .execution_id = text("574109d965174f89b539bf34023e1f7dd"), .broker_restore = restore,
        .observe = observer};
    ud_execution *execution = NULL;
    ud_status status = ud_execution_create_json(document, &options, &execution);
    if (!status)
        status = ud_execution_start(execution);
    if (!status) {
        status = ud_execution_wait(execution, BROKER_WAIT_MS);
        if (status == UD_BUSY) {
            ud_execution_cancel(execution);
            (void)ud_execution_wait(execution, BROKER_WAIT_MS);
            status = UD_TIMEOUT;
        }
        if (!status) {
            ud_execution_metrics metrics = {.struct_size = sizeof(metrics), .abi_version = UD_ABI_VERSION};
            status = ud_execution_get_metrics(execution, &metrics);
            if (!status && (metrics.state != UD_EXEC_SUCCEEDED || metrics.rejected_rows))
                status = UD_CORRUPT;
        }
    }
    if (execution) {
        ud_status destroyed = ud_execution_destroy(execution);
        if (!status)
            status = destroyed;
    }
    ud_free(state);
    return status;
}

static ud_status transaction_output(broker_fixture *f) {
    ud_connection *connection = NULL;
    UD_TRY(open_subscription(f, f->transaction_target, &connection));
    ud_message message = {0};
    ud_json *row = NULL;
    ud_status status = receive_until(connection, BROKER_WAIT_MS, &message);
    if (!status)
        status = ud_json_parse(f->memory, message.payload.data, message.payload.length, &row, NULL);
    uint64_t start = 0, end = 0, total = 0;
    ud_string key = {0};
    static const ud_field partition_field = {.type = UD_UTF8};
    static const ud_value partition_key = {.type = UD_UTF8, .text = "fixture", .text_length = 7};
    int32_t partition = (int32_t)(ud_field_value_hash(&partition_field, &partition_key, UD_VALUE_HASH_SEED) % 2);
    if (!status && (message.partition != partition || !message.delivery || ud_json_kind(row) != UD_JSON_OBJECT ||
                    ud_json_size(row) != 4 || ud_json_uint(ud_member(row, "_window_start"), &start) ||
                    ud_json_uint(ud_member(row, "_window_end"), &end) ||
                    ud_json_uint(ud_member(row, "total"), &total) ||
                    ud_json_string(ud_member(row, "key"), &key) ||
                    start != 0 || end != 10 || total != 7 || !ud_named(key, "fixture")))
        status = UD_CORRUPT;
    if (!status)
        status = ud_connection_message_acknowledge(connection, &message.delivery, 1);
    ud_json_destroy(row);
    ud_message_clear(&message);
    ud_status closed = ud_connection_close(connection);
    return status ? status : closed;
}

static ud_status untouched_input(broker_fixture *f, ud_string expected) {
    ud_connection *connection = NULL;
    ud_message message = {0};
    ud_status status = receive_test(f, f->transaction_source, expected, BROKER_WAIT_MS,
                                    &message, &connection);
    if (!status && (message.partition != 0 || message.offset != 0))
        status = UD_CORRUPT;
    if (!status)
        status = ud_connection_message_acknowledge(connection, &message.delivery, 1);
    ud_message_clear(&message);
    ud_status closed = ud_connection_close(connection);
    return status ? status : closed;
}

enum { TRANSACTION_ROUND_TRIP, TRANSACTION_CRASH_OWNER, TRANSACTION_TAKEOVER };

static ud_status kafka_transaction(broker_fixture *f, int mode) {
    if (mode != TRANSACTION_TAKEOVER) {
        UD_TRY(assert_empty(f, f->transaction_source, 5000));
        UD_TRY(assert_empty(f, f->transaction_target, 5000));
        UD_TRY(assert_empty(f, f->checkpoint_topic, 5000));
    }
    const ud_string input = text("{\"time\":5,\"key\":\"fixture\",\"value\":7}");
    const ud_string untouched = text("{\"time\":5,\"key\":\"untouched\",\"value\":99}");
    ud_bytes json = {.ctx = f->memory, .limit = 65536};
    ud_status status = transaction_document(f, &json);
    ud_string document = {(const char *)json.data, (uint32_t)json.size};
    if (mode != TRANSACTION_TAKEOVER) {
        if (!status)
            status = publish_at(f, f->transaction_source, &input, 1, 1);
        if (!status)
            status = publish_at(f, f->transaction_source, &untouched, 1, 0);
        if (!status)
            status = run_transaction(f, document, 0,
                mode == TRANSACTION_CRASH_OWNER ? pause_committed_owner : NULL);
    }
    if (mode == TRANSACTION_CRASH_OWNER) {
        ud_bytes_clear(&json);
        return status ? status : UD_TIMEOUT;
    }
    if (!status && mode == TRANSACTION_ROUND_TRIP)
        status = transaction_output(f);
    if (!status)
        status = run_transaction(f, document, 1, NULL);
    if (!status && mode == TRANSACTION_TAKEOVER)
        status = transaction_output(f);
    if (!status)
        status = assert_empty(f, f->transaction_target, 5000);
    if (!status)
        status = untouched_input(f, untouched);
    if (!status)
        status = assert_empty(f, f->transaction_source, 5000);
    ud_bytes_clear(&json);
    return status;
}

static void registry_wire_id(uint8_t *prefix, uint32_t id) {
    prefix[1] = (uint8_t)(id >> 24);
    prefix[2] = (uint8_t)(id >> 16);
    prefix[3] = (uint8_t)(id >> 8);
    prefix[4] = (uint8_t)id;
}

static ud_status registry_unsettled(broker_fixture *f, ud_string expected, int64_t *offset) {
    ud_message delivery = {0};
    ud_connection *connection = NULL;
    ud_status status = receive_test(f, f->registry.source, expected, BROKER_WAIT_MS,
                                    &delivery, &connection);
    if (!status && (delivery.partition != 0 || delivery.offset < 0 ||
                    (*offset >= 0 && delivery.offset != *offset)))
        status = UD_CORRUPT;
    if (!status)
        *offset = delivery.offset;
    ud_message_clear(&delivery);
    /* An unknown schema must remain unsettled both before and after ETL. */
    ud_status closed = connection ? ud_connection_close(connection) : UD_OK;
    return status ? status : closed;
}

static ud_status kafka_registry(broker_fixture *f) {
    const registry_fixture *registry = &f->registry;
    UD_TRY(assert_empty(f, registry->source, 5000));
    UD_TRY(assert_empty(f, registry->target, 5000));

    /* Independent golden datum: long(7), followed in v2 by string("reader-default"). */
    uint8_t body[] = {0, 0, 0, 0, 0, 14};
    uint8_t evolved[] = {0, 0, 0, 0, 0, 14, 28, 'r', 'e', 'a', 'd', 'e', 'r', '-',
                         'd', 'e', 'f', 'a', 'u', 'l', 't'};
    const ud_string input = {(const char *)body, sizeof(body)};
    const ud_string output = {(const char *)evolved, sizeof(evolved)};
    registry_wire_id(body, registry->writer_id);
    registry_wire_id(evolved, registry->reader_id);
    UD_TRY(publish(f, registry->source, &input, 1));
    UD_TRY(pipeline_case(f, CASE_AVRO, registry->source, registry->target,
                         "registry-journal", UD_OK, 1));
    UD_TRY(output_matches(f, registry->target, CASE_BINARY, &output, 1));
    UD_TRY(assert_empty(f, registry->source, 5000));
    puts("kafka schema registry Avro v1 to v2: PASS");

    registry_wire_id(body, registry->unknown_id);
    UD_TRY(publish(f, registry->source, &input, 1));
    int64_t offset = -1;
    UD_TRY(registry_unsettled(f, input, &offset));
    UD_TRY(pipeline_case(f, CASE_AVRO, registry->source, registry->target,
                         "registry-unknown-journal", UD_QUALITY, 0));
    UD_TRY(registry_unsettled(f, input, &offset));
    UD_TRY(assert_empty(f, registry->target, 5000));
    puts("kafka schema registry unknown ID redelivery: PASS");
    return UD_OK;
}

static ud_status run_cases(broker_fixture *f) {
    const struct test_case { const char *name; ud_status (*run)(broker_fixture *, int); int argument; }
        cases[] = {{"address authorization", expect_denial, 0},
                   {"endpoint authorization", expect_denial, 1},
                   {"binary pipeline", round_trip, CASE_BINARY},
                   {"JSON pipeline", round_trip, CASE_JSON},
                   {"abandoned-session redelivery", redelivery, 0},
                   {"decode failure redelivery", redelivery, 1}};
    /* Prime configured groups and verify the operator allocated empty, dedicated resources. */
    for (uint32_t i = 0; i < CASES; i++)
        UD_TRY(assert_empty(f, f->ingress[i], 5000));
    for (uint32_t i = 0; i < 2; i++)
        UD_TRY(assert_empty(f, f->egress[i], 5000));
    for (uint32_t i = 0; i < sizeof(cases) / sizeof(*cases); i++) {
        ud_status status = cases[i].run(f, cases[i].argument);
        fprintf(status ? stderr : stdout, "%s %s: %s (%d)\n", broker_names[f->broker],
                cases[i].name, status ? ud_status_name(status) : "PASS", status);
        if (status)
            return status;
    }
    if (f->broker == BROKER_RABBITMQ) {
        UD_TRY(rabbit_confirmation_gap(f));
        UD_TRY(assert_empty(f, f->ingress[CASE_REDELIVERY], 5000));
        puts("rabbitmq confirmed prefix and unconfirmed gap: PASS");
    }
    if (f->broker == BROKER_KAFKA) {
        ud_status status = kafka_transaction(f, TRANSACTION_ROUND_TRIP);
        fprintf(status ? stderr : stdout, "kafka bounded transaction and broker recovery: %s (%d)\n",
                status ? ud_status_name(status) : "PASS", status);
        return status;
    }
    return UD_OK;
}

int main(int argc, char **argv) {
    int registry_mode = argc == 4 && !strcmp(argv[3], "--registry");
    int crash_mode = argc == 4 && !strcmp(argv[3], "--crash-owner");
    int takeover_mode = argc == 4 && !strcmp(argv[3], "--takeover");
    if (argc != 3 && !registry_mode && !crash_mode && !takeover_mode) {
        fputs("Usage: test_message_brokers MODULE CONFIG [--registry|--crash-owner|--takeover]\n", stderr);
        return 2;
    }
    broker_fixture fixture = {.driver = argv[1]};
    atomic_init(&fixture.deny_observed, 0);
    ud_status status = ud_context_create(NULL, &fixture.memory);
    ud_bytes input = {.ctx = fixture.memory, .limit = 1024 * 1024};
    ud_json *document = NULL;
    if (!status)
        status = ud_read_file(fixture.memory, argv[2], &input);
    if (!status)
        status = ud_json_parse(fixture.memory, (const char *)input.data, (uint32_t)input.size,
                               &document, NULL);
    if (!status)
        status = configure(&fixture, document, registry_mode);
    ud_bytes_clear(&input);
    if (!status)
        status = ud_plugin_load(fixture.memory, fixture.driver, 0, &fixture.plugin);
    if (!status) {
        static const char *const ids[] = {"org.universaldata.connector.rabbitmq",
            "org.universaldata.connector.kafka", "org.universaldata.connector.rocketmq",
            "org.universaldata.connector.activemq", "org.universaldata.connector.activemq"};
        ud_plugin_api api = {.struct_size = sizeof(api), .abi_version = UD_PLUGIN_ABI};
        status = ud_plugin_info(fixture.plugin, &api);
        if (!status && strcmp(api.id, ids[fixture.broker]))
            status = UD_INCOMPATIBLE;
        if (!status) {
            if (registry_mode)
                status = kafka_registry(&fixture);
            else if (crash_mode || takeover_mode)
                status = fixture.broker == BROKER_KAFKA ? kafka_transaction(&fixture,
                    crash_mode ? TRANSACTION_CRASH_OWNER : TRANSACTION_TAKEOVER) : UD_SCHEMA;
            else
                status = run_cases(&fixture);
        }
    }
    if (fixture.plugin) {
        ud_status unloaded = ud_plugin_unload(fixture.plugin);
        if (!status)
            status = unloaded;
    }
    ud_free((void *)fixture.credentials.data);
    ud_free((void *)fixture.registry.credentials.data);
    ud_json_destroy(document);
    if (fixture.memory) {
        ud_status destroyed = ud_context_destroy(fixture.memory);
        if (!status)
            status = destroyed;
    }
    if (status)
        fprintf(stderr, "Broker fixture failed: %s (%d)\n", ud_status_name(status), status);
    return status ? 1 : 0;
}
