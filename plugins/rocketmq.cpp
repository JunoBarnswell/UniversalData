#include "ud/plugin.h"
#include "ud/message.h"
#include "apache/rocketmq/v2/service.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <openssl/hmac.h>
#include <zlib.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <climits>
#include <cstring>
#include <ctime>
#include <future>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <vector>

namespace {
namespace mq = apache::rocketmq::v2;
using Clock = std::chrono::system_clock;
using namespace std::chrono_literals;

constexpr auto rpc_timeout = 5s;
constexpr auto pop_timeout = 5s;
constexpr auto invisible_time = 60s;
constexpr auto renew_after = 30s;
constexpr uint32_t broker_max_pop_batch = 32;

std::string value(ud_string text) {
    return text.data ? std::string(text.data, text.length) : std::string{};
}

ud_string borrowed(const std::string &text) {
    return {text.data(), static_cast<uint32_t>(text.size())};
}

std::string hex(const unsigned char *bytes, size_t length) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string text(length * 2, '0');
    for (size_t i = 0; i < length; ++i) {
        text[i * 2] = digits[bytes[i] >> 4];
        text[i * 2 + 1] = digits[bytes[i] & 15];
    }
    return text;
}

bool valid_text(const std::string &text) {
    const auto *p = reinterpret_cast<const unsigned char *>(text.data());
    const auto *end = p + text.size();
    while (p != end) {
        const unsigned lead = *p++;
        if (!lead)
            return false;
        if (lead < 0x80)
            continue;
        if (lead < 0xc2 || lead > 0xf4 || p == end)
            return false;
        unsigned remaining = lead < 0xe0 ? 1 : lead < 0xf0 ? 2 : 3;
        if ((lead == 0xe0 && *p < 0xa0) || (lead == 0xed && *p >= 0xa0) ||
            (lead == 0xf0 && *p < 0x90) || (lead == 0xf4 && *p >= 0x90))
            return false;
        for (; remaining; --remaining)
            if (p == end || (*p++ & 0xc0) != 0x80)
                return false;
    }
    return true;
}

bool endpoint(const std::string &text, mq::Endpoints &out) {
    const auto colon = text.rfind(':');
    if (colon == std::string::npos || !colon)
        return false;
    int port = 0;
    const auto parse = std::from_chars(text.data() + colon + 1, text.data() + text.size(), port);
    if (parse.ec != std::errc{} || parse.ptr != text.data() + text.size() || port < 1 ||
        port > 65535)
        return false;
    auto host = text.substr(0, colon);
    if (host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
        if (host.find(':') == std::string::npos ||
            host.find_first_not_of("0123456789abcdefABCDEF:.") != std::string::npos)
            return false;
        out.set_scheme(mq::IPv6);
    } else {
        if (host.find_first_not_of(
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-") !=
            std::string::npos)
            return false;
        out.set_scheme(host.find_first_not_of("0123456789.") == std::string::npos
                           ? mq::IPv4
                           : mq::DOMAIN_NAME);
    }
    auto *address = out.add_addresses();
    address->set_host(std::move(host));
    address->set_port(port);
    return true;
}

ud_status transport_status(const grpc::Status &status) {
    switch (status.error_code()) {
    case grpc::StatusCode::OK:
        return UD_OK;
    case grpc::StatusCode::CANCELLED:
        return UD_CANCELLED;
    case grpc::StatusCode::DEADLINE_EXCEEDED:
        return UD_TIMEOUT;
    case grpc::StatusCode::PERMISSION_DENIED:
    case grpc::StatusCode::UNAUTHENTICATED:
        return UD_AUTH;
    case grpc::StatusCode::INVALID_ARGUMENT:
        return UD_INVALID;
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
        return UD_RESOURCE;
    case grpc::StatusCode::UNAVAILABLE:
        return UD_TRANSIENT;
    case grpc::StatusCode::UNIMPLEMENTED:
        return UD_UNSUPPORTED;
    default:
        return UD_IO;
    }
}

ud_status broker_status(mq::Code code) {
    const auto family = static_cast<int>(code) / 100;
    switch (family) {
    case 200:
        return UD_OK;
    case 400:
        return code == mq::INVALID_RECEIPT_HANDLE ? UD_FENCED : UD_INVALID;
    case 401:
    case 403:
        return UD_AUTH;
    case 404:
        return code == mq::MESSAGE_NOT_FOUND ? UD_BUSY : UD_SCHEMA;
    case 408:
    case 504:
        return UD_TIMEOUT;
    case 413:
    case 431:
        return UD_OVERFLOW;
    case 429:
        return UD_TRANSIENT;
    case 501:
    case 505:
        return UD_UNSUPPORTED;
    default:
        return UD_IO;
    }
}

ud_message_result send_result(mq::Code code) {
    const auto status = broker_status(code);
    if (status == UD_OK)
        return {UD_MESSAGE_ACCEPTED, UD_OK};
    if ((code >= mq::BAD_REQUEST && code < mq::INTERNAL_ERROR && code != mq::REQUEST_TIMEOUT) ||
        status == UD_UNSUPPORTED)
        return {UD_MESSAGE_REJECTED, status};
    return {UD_MESSAGE_UNCERTAIN, UD_COMMIT_UNKNOWN};
}

std::string new_id(const ud_host_api &host) {
    unsigned char bytes[16];
    return host.random_bytes(host.context, bytes, sizeof(bytes)) ? std::string{}
                                                                 : hex(bytes, sizeof(bytes));
}

ud_status decode_body(mq::Message &message, uint32_t limit) {
    if (message.system_properties().body_encoding() == mq::ENCODING_UNSPECIFIED ||
        message.system_properties().body_encoding() == mq::IDENTITY)
        return message.body().size() <= limit ? UD_OK : UD_OVERFLOW;
    if (message.system_properties().body_encoding() != mq::GZIP)
        return UD_UNSUPPORTED;
    z_stream stream{};
    if (inflateInit2(&stream, 15 + 32) != Z_OK)
        return UD_RESOURCE;
    stream.next_in = reinterpret_cast<Bytef *>(message.mutable_body()->data());
    stream.avail_in = static_cast<uInt>(message.body().size());
    std::string body;
    ud_status status = UD_OK;
    unsigned char chunk[8192];
    int result;
    do {
        stream.next_out = chunk;
        stream.avail_out = sizeof(chunk);
        result = inflate(&stream, Z_NO_FLUSH);
        const auto size = sizeof(chunk) - stream.avail_out;
        if (size > limit - body.size()) {
            status = UD_OVERFLOW;
            break;
        }
        body.append(reinterpret_cast<const char *>(chunk), size);
    } while (result == Z_OK);
    const bool complete = result == Z_STREAM_END && stream.avail_in == 0;
    inflateEnd(&stream);
    if (status)
        return status;
    if (!complete)
        return UD_CORRUPT;
    message.set_body(std::move(body));
    return UD_OK;
}

struct Route {
    mq::MessageQueue queue;
    std::string authority;
};

struct Delivery {
    mq::Message message;
    std::string authority;
    uint64_t bytes;
    Clock::time_point renew;
    Clock::time_point expires;
    bool exposed = false;
    bool valid = true;
};

struct ReceiveResult {
    ud_status status;
    std::vector<Delivery> deliveries;
};

struct ReceiveCall {
    grpc::ClientContext context;
    std::string authority;
    std::future<ReceiveResult> pending;
};

class RocketMqSession {
  public:
    explicit RocketMqSession(const ud_host_api &host) : host_(host) {}
    ~RocketMqSession() {
        abandon_receive();
    }

    ud_status configure(const char *, uint32_t);
    ud_status test();
    ud_status subscribe(const ud_message_subscription &);
    ud_status receive(uint32_t, ud_message &);
    ud_status acknowledge(const uint64_t *, uint32_t);
    ud_status unsubscribe();
    ud_status publish(const ud_message_target &, const ud_message *, uint32_t, ud_message_result *);
    void cancel();
    void set_control(void *, ud_status (*)(void *));

  private:
    using Stub = mq::MessagingService::Stub;
    ud_status check() const {
        return cancelled_.load() ? UD_CANCELLED
               : host_.check_io  ? host_.check_io(host_.control_user)
                                 : UD_OK;
    }
    mq::Resource resource(const std::string &name) const {
        mq::Resource result;
        result.set_name(name);
        result.set_resource_namespace(namespace_);
        return result;
    }
    ud_status authorize(const std::string &) const;
    ud_status stub(const std::string &, Stub *&);
    ud_status prepare(grpc::ClientContext &, Clock::time_point, bool consumer = false,
                      bool receiving = false);
    void complete();
    auto guard_rpc(grpc::ClientContext &context) {
        const auto release = [this](grpc::ClientContext *) { complete(); };
        return std::unique_ptr<grpc::ClientContext, decltype(release)>(&context, release);
    }
    template <typename Request, typename Response>
    ud_status unary(Stub &remote,
                    grpc::Status (Stub::*method)(grpc::ClientContext *, const Request &,
                                                 Response *),
                    const Request &request, Response &response, Clock::time_point deadline,
                    bool *started = nullptr, bool consumer = false) {
        grpc::ClientContext context;
        const auto status = prepare(context, deadline, consumer);
        if (status)
            return status;
        const auto guard = guard_rpc(context);
        if (started)
            *started = true;
        const auto result = (remote.*method)(&context, request, &response);
        return transport_status(result);
    }
    ud_status queue_routes(const mq::Resource &, const mq::Resource *, std::vector<Route> &,
                           Clock::time_point);
    ud_status register_client(Stub &, const mq::Resource &, bool, Clock::time_point);
    ud_status heartbeat(Stub &, bool, Clock::time_point);
    ud_status fetch(const Route &, uint32_t, Clock::time_point);
    static ReceiveResult read_stream(Stub &, ReceiveCall &, const mq::ReceiveMessageRequest &,
                                     uint32_t, Clock::time_point);
    ud_status collect_receive(Clock::time_point);
    void abandon_receive();
    ud_status export_message(uint64_t, const Delivery &, ud_message &);
    void drop(uint64_t);
    void renew(Clock::time_point);

    ud_host_api host_;
    std::string bootstrap_, namespace_, access_key_, secret_key_, token_, client_id_;
    mq::Endpoints access_point_;
    bool tls_ = true;
    std::map<std::string, std::unique_ptr<Stub>> stubs_;
    mq::Resource topic_, group_;
    mq::FilterExpression filter_;
    std::vector<Route> routes_;
    std::map<std::string, Clock::time_point> consumer_heartbeat_;
    Clock::time_point assignment_refresh_;
    size_t next_route_ = 0;
    uint32_t prefetch_ = 0, message_limit_ = 0;
    uint64_t buffer_limit_ = 0, held_bytes_ = 0, next_delivery_ = 0;
    std::map<uint64_t, Delivery> held_;
    std::deque<uint64_t> ready_;
    std::mutex operations_, rpc_mutex_;
    grpc::ClientContext *active_rpc_ = nullptr;
    grpc::ClientContext *receive_rpc_ = nullptr;
    std::unique_ptr<ReceiveCall> receive_call_;
    std::atomic<bool> cancelled_{false};
};

ud_status RocketMqSession::configure(const char *json, uint32_t length) {
    ud_json *document = nullptr;
    if (auto status = host_.json_parse(host_.context, json, length, &document, nullptr))
        return status;
    const std::unique_ptr<ud_json, decltype(host_.json_destroy)> config(document,
                                                                        host_.json_destroy);
    const char *const keys[] = {"endpoint",  "tls",      "namespace", "accessKey",
                                "secretKey", "username", "password",  "securityToken"};
    if (host_.json_keys(document, keys, sizeof(keys) / sizeof(keys[0])))
        return UD_INVALID;
    const auto field = [this, document](const char *key) {
        return host_.json_find(document, key, static_cast<uint32_t>(std::strlen(key)));
    };
    const auto read = [this, &field](const char *key, std::string &output) {
        const auto *item = field(key);
        if (!item)
            return true;
        ud_string raw{};
        if (host_.json_kind(item) != UD_JSON_STRING ||
            host_.json_text(item, &raw.data, &raw.length))
            return false;
        output = value(raw);
        return !output.empty() && output.find('\0') == std::string::npos;
    };
    if (!read("endpoint", bootstrap_) || !endpoint(bootstrap_, access_point_) ||
        !read("namespace", namespace_) || !read("securityToken", token_) ||
        !read("accessKey", access_key_) || !read("secretKey", secret_key_))
        return UD_INVALID;
    if (field("username") || field("password")) {
        if (field("accessKey") || field("secretKey") || !read("username", access_key_) ||
            !read("password", secret_key_) || access_key_.empty())
            return UD_INVALID;
    }
    if (access_key_.empty() != secret_key_.empty() || (!token_.empty() && access_key_.empty()) ||
        access_key_.find_first_of(",= \t\r\n") != std::string::npos ||
        token_.find_first_of("\r\n") != std::string::npos || secret_key_.size() > INT_MAX)
        return UD_INVALID;
    if (const auto *setting = field("tls")) {
        ud_string raw{};
        if (host_.json_kind(setting) != UD_JSON_BOOL ||
            host_.json_text(setting, &raw.data, &raw.length))
            return UD_INVALID;
        tls_ = value(raw) == "true";
    }
    const auto &address = access_point_.addresses(0);
    bootstrap_ =
        (access_point_.scheme() == mq::IPv6 ? "[" + address.host() + "]" : address.host()) + ":" +
        std::to_string(address.port());
    client_id_ = new_id(host_);
    if (client_id_.empty())
        return UD_RESOURCE;
    return authorize(bootstrap_);
}

ud_status RocketMqSession::authorize(const std::string &authority) const {
    if (const auto status = check())
        return status;
    if (!host_.authorize_endpoint)
        return UD_AUTH;
    const auto resource = (tls_ ? "grpcs://" : "grpc://") + authority;
    return host_.authorize_endpoint(host_.control_user, borrowed(resource));
}

ud_status RocketMqSession::stub(const std::string &authority, Stub *&remote) {
    if (const auto status = authorize(authority))
        return status;
    auto &entry = stubs_[authority];
    if (!entry) {
        grpc::ChannelArguments args;
        args.SetInt("grpc.enable_retries", 0);
        args.SetMaxReceiveMessageSize(16 * 1024 * 1024);
        entry = mq::MessagingService::NewStub(
            grpc::CreateCustomChannel(authority,
                                      tls_ ? grpc::SslCredentials(grpc::SslCredentialsOptions{})
                                           : grpc::InsecureChannelCredentials(),
                                      args));
    }
    remote = entry.get();
    return UD_OK;
}

ud_status RocketMqSession::prepare(grpc::ClientContext &context, Clock::time_point deadline,
                                   bool consumer, bool receiving) {
    if (const auto status = check())
        return status;
    if (Clock::now() >= deadline)
        return UD_TIMEOUT;
    const auto now = std::time(nullptr);
    std::tm utc{};
#ifdef _WIN32
    if (gmtime_s(&utc, &now))
#else
    if (!gmtime_r(&now, &utc))
#endif
        return UD_IO;
    char datetime[20];
    if (!std::strftime(datetime, sizeof(datetime), "%Y%m%dT%H%M%SZ", &utc))
        return UD_IO;
    const auto request_id = new_id(host_);
    if (request_id.empty())
        return UD_RESOURCE;
    context.set_deadline(deadline);
    context.AddMetadata("x-mq-client-id", consumer ? client_id_ + "-consumer" : client_id_);
    context.AddMetadata("x-mq-request-id", request_id);
    context.AddMetadata("x-mq-language", "CPP");
    context.AddMetadata("x-mq-client-version", UD_SDK_VERSION);
    context.AddMetadata("x-mq-protocol", "v2");
    context.AddMetadata("x-mq-date-time", datetime);
    if (!namespace_.empty())
        context.AddMetadata("x-mq-namespace", namespace_);
    if (!access_key_.empty()) {
        unsigned char digest[EVP_MAX_MD_SIZE];
        unsigned length = 0;
        if (!HMAC(EVP_sha1(), secret_key_.data(), static_cast<int>(secret_key_.size()),
                  reinterpret_cast<const unsigned char *>(datetime), std::strlen(datetime), digest,
                  &length))
            return UD_IO;
        context.AddMetadata("authorization",
                            "MQv2-HMAC-SHA1 Credential=" + access_key_ +
                                ", SignedHeaders=x-mq-date-time, Signature=" + hex(digest, length));
        if (!token_.empty())
            context.AddMetadata("x-mq-session-token", token_);
    }
    const std::lock_guard<std::mutex> lock(rpc_mutex_);
    if (cancelled_.load())
        return UD_CANCELLED;
    (receiving ? receive_rpc_ : active_rpc_) = &context;
    return UD_OK;
}

void RocketMqSession::complete() {
    const std::lock_guard<std::mutex> lock(rpc_mutex_);
    active_rpc_ = nullptr;
}

void RocketMqSession::cancel() {
    const std::lock_guard<std::mutex> lock(rpc_mutex_);
    cancelled_.store(true);
    if (active_rpc_)
        active_rpc_->TryCancel();
    if (receive_rpc_)
        receive_rpc_->TryCancel();
}

void RocketMqSession::set_control(void *user, ud_status (*check_io)(void *)) {
    const std::lock_guard<std::mutex> lock(operations_);
    host_.control_user = user;
    host_.check_io = check_io;
}

ud_status RocketMqSession::test() {
    const std::lock_guard<std::mutex> lock(operations_);
    Stub *remote;
    if (const auto status = stub(bootstrap_, remote))
        return status;
    // An empty publishing settings list probes the RocketMQ service without a topic grant.
    return register_client(*remote, mq::Resource{}, false, Clock::now() + rpc_timeout);
}

ud_status RocketMqSession::queue_routes(const mq::Resource &topic, const mq::Resource *group,
                                        std::vector<Route> &routes, Clock::time_point deadline) {
    Stub *remote;
    if (const auto status = stub(bootstrap_, remote))
        return status;
    std::vector<mq::MessageQueue> queues;
    if (group) {
        mq::QueryAssignmentRequest request;
        *request.mutable_topic() = topic;
        *request.mutable_group() = *group;
        *request.mutable_endpoints() = access_point_;
        mq::QueryAssignmentResponse response;
        auto status =
            unary(*remote, &Stub::QueryAssignment, request, response, deadline, nullptr, true);
        if (status || (status = broker_status(response.status().code())))
            return status;
        for (const auto &assignment : response.assignments())
            queues.push_back(assignment.message_queue());
    } else {
        mq::QueryRouteRequest request;
        *request.mutable_topic() = topic;
        *request.mutable_endpoints() = access_point_;
        mq::QueryRouteResponse response;
        auto status = unary(*remote, &Stub::QueryRoute, request, response, deadline);
        if (status || (status = broker_status(response.status().code())))
            return status;
        for (const auto &queue : response.message_queues())
            queues.push_back(queue);
    }
    bool denied = false;
    for (auto &queue : queues) {
        if (queue.topic().name() != topic.name() || queue.broker().id() != 0)
            continue;
        if (queue.permission() != mq::READ_WRITE &&
            queue.permission() != (group ? mq::READ : mq::WRITE))
            continue;
        if (!group && !queue.accept_message_types().empty() &&
            std::find(queue.accept_message_types().begin(), queue.accept_message_types().end(),
                      mq::NORMAL) == queue.accept_message_types().end())
            continue;
        std::string selected;
        for (const auto &address : queue.broker().endpoints().addresses()) {
            const auto &host = address.host();
            auto authority = (host.find(':') == std::string::npos ? host : "[" + host + "]") + ":" +
                             std::to_string(address.port());
            mq::Endpoints validated;
            if (!endpoint(authority, validated))
                return UD_CORRUPT;
            if (!authorize(authority)) {
                selected = std::move(authority);
                break;
            }
            denied = true;
        }
        if (!selected.empty())
            routes.push_back({std::move(queue), std::move(selected)});
    }
    return routes.empty() ? (denied ? UD_AUTH : UD_UNSUPPORTED) : UD_OK;
}

ud_status RocketMqSession::register_client(Stub &remote, const mq::Resource &topic, bool consumer,
                                           Clock::time_point deadline) {
    mq::TelemetryCommand command;
    auto *settings = command.mutable_settings();
    *settings->mutable_access_point() = access_point_;
    settings->set_client_type(consumer ? mq::SIMPLE_CONSUMER : mq::PRODUCER);
    settings->mutable_request_timeout()->set_seconds(rpc_timeout.count());
    settings->mutable_user_agent()->set_language(mq::CPP);
    settings->mutable_user_agent()->set_version(UD_SDK_VERSION);
    if (consumer) {
        auto *subscription = settings->mutable_subscription();
        *subscription->mutable_group() = group_;
        auto *entry = subscription->add_subscriptions();
        *entry->mutable_topic() = topic;
        *entry->mutable_expression() = filter_;
    } else {
        auto *publishing = settings->mutable_publishing();
        if (!topic.name().empty())
            *publishing->add_topics() = topic;
    }
    grpc::ClientContext context;
    if (const auto status = prepare(context, deadline, consumer))
        return status;
    const auto guard = guard_rpc(context);
    const auto stream = remote.Telemetry(&context);
    mq::TelemetryCommand answer;
    ud_status status = UD_OK;
    bool closed = false;
    if (stream->Write(command)) {
        while (stream->Read(&answer)) {
            if (!answer.has_settings() && !answer.has_status())
                continue;
            status = answer.has_status() ? broker_status(answer.status().code()) : UD_OK;
            if (!status && consumer && answer.has_settings() &&
                answer.settings().subscription().fifo())
                status = UD_UNSUPPORTED;
            // The Proxy processes Settings asynchronously. Close the write side
            // after the reply so the Proxy can register the client's settings.
            closed = stream->WritesDone();
            while (stream->Read(&answer)) {
            }
            break;
        }
    }
    const auto transport = stream->Finish();
    if (status)
        return status;
    if (!transport.ok())
        return transport_status(transport);
    return closed ? UD_OK : UD_IO;
}

ud_status RocketMqSession::heartbeat(Stub &remote, bool consumer, Clock::time_point deadline) {
    mq::HeartbeatRequest request;
    request.set_client_type(consumer ? mq::SIMPLE_CONSUMER : mq::PRODUCER);
    if (consumer)
        *request.mutable_group() = group_;
    mq::HeartbeatResponse response;
    const auto status =
        unary(remote, &Stub::Heartbeat, request, response, deadline, nullptr, consumer);
    return status ? status : broker_status(response.status().code());
}

ud_status RocketMqSession::subscribe(const ud_message_subscription &options) {
    const std::lock_guard<std::mutex> lock(operations_);
    if (prefetch_)
        return UD_BUSY;
    if (options.source.kind != UD_MESSAGE_TOPIC || options.subscription.length ||
        options.source.routing_key.length ||
        options.initial_position != UD_MESSAGE_POSITION_REQUIRE)
        return UD_UNSUPPORTED;
    if (!options.source.name.length || !options.group.length || !options.prefetch ||
        !options.max_message_bytes || options.max_in_flight_bytes < options.max_message_bytes)
        return UD_INVALID;
    topic_ = resource(value(options.source.name));
    group_ = resource(value(options.group));
    if (!valid_text(topic_.name()) || !valid_text(group_.name()) ||
        !valid_text(value(options.filter)))
        return UD_INVALID;
    filter_.set_type(mq::TAG);
    filter_.set_expression(options.filter.length ? value(options.filter) : "*");
    std::vector<Route> routes;
    const auto deadline = Clock::now() + rpc_timeout;
    if (const auto status = queue_routes(topic_, &group_, routes, deadline))
        return status;
    Stub *remote;
    auto status = stub(routes.front().authority, remote);
    if (!status)
        status = register_client(*remote, topic_, true, deadline);
    if (status)
        return status;
    consumer_heartbeat_[routes.front().authority] = Clock::now() + 15s;
    routes_ = std::move(routes);
    assignment_refresh_ = Clock::now() + 30s;
    prefetch_ = options.prefetch;
    message_limit_ = options.max_message_bytes;
    buffer_limit_ = options.max_in_flight_bytes;
    return UD_OK;
}

ud_status RocketMqSession::fetch(const Route &route, uint32_t count, Clock::time_point deadline) {
    Stub *remote;
    if (const auto status = stub(route.authority, remote))
        return status;
    auto &next_heartbeat = consumer_heartbeat_[route.authority];
    const auto now = Clock::now();
    if (next_heartbeat <= now) {
        auto status = next_heartbeat == Clock::time_point{}
                          ? register_client(*remote, topic_, true, deadline)
                          : heartbeat(*remote, true, deadline);
        if (status == UD_INVALID)
            status = register_client(*remote, topic_, true, deadline);
        if (status)
            return status;
        next_heartbeat = Clock::now() + 15s;
    }
    mq::ReceiveMessageRequest request;
    *request.mutable_group() = group_;
    *request.mutable_message_queue() = route.queue;
    *request.mutable_filter_expression() = filter_;
    request.set_batch_size(static_cast<int32_t>(count));
    request.mutable_invisible_duration()->set_seconds(invisible_time.count());
    request.mutable_long_polling_timeout()->set_seconds(pop_timeout.count());
    auto receive = std::make_unique<ReceiveCall>();
    receive->authority = route.authority;
    const auto started = Clock::now();
    if (const auto status = prepare(receive->context, started + pop_timeout + 2s, true, true))
        return status;
    receive_call_ = std::move(receive);
    try {
        auto &active = *receive_call_;
        active.pending =
            std::async(std::launch::async, &RocketMqSession::read_stream, std::ref(*remote),
                       std::ref(active), std::move(request), message_limit_, started);
    } catch (...) {
        abandon_receive();
        throw;
    }
    return UD_OK;
}

ReceiveResult RocketMqSession::read_stream(Stub &remote, ReceiveCall &call,
                                           const mq::ReceiveMessageRequest &request,
                                           uint32_t limit_bytes, Clock::time_point start) {
    const auto reader = remote.ReceiveMessage(&call.context, request);
    ReceiveResult result{UD_OK, {}};
    mq::ReceiveMessageResponse response;
    ud_status status = UD_OK;
    bool status_received = false;
    while (reader->Read(&response)) {
        if (response.has_status()) {
            status_received = true;
            const auto code = response.status().code();
            status = code == mq::ILLEGAL_POLLING_TIME ? UD_BUSY : broker_status(code);
            if (status)
                break;
        }
        if (!response.has_message())
            continue;
        auto &message = *response.mutable_message();
        const auto &props = message.system_properties();
        if (result.deliveries.size() >= static_cast<size_t>(request.batch_size()) ||
            props.receipt_handle().empty() || props.message_id().empty() ||
            message.topic().name() != request.message_queue().topic().name()) {
            status = UD_CORRUPT;
            break;
        }
        uint64_t metadata_bytes = props.message_id().size() + message.topic().name().size();
        if (!props.keys().empty())
            metadata_bytes += props.keys(0).size();
        if (props.has_tag())
            metadata_bytes += sizeof("TAGS") - 1 + props.tag().size();
        for (const auto &property : message.user_properties())
            metadata_bytes += property.first.size() + property.second.size();
        if (metadata_bytes > limit_bytes) {
            status = UD_OVERFLOW;
            break;
        }
        const auto limit = static_cast<uint32_t>(limit_bytes - metadata_bytes);
        status = decode_body(message, limit);
        if (status)
            break;
        const auto bytes = metadata_bytes + message.body().size();
        const int64_t seconds = props.has_invisible_duration()
                                    ? props.invisible_duration().seconds()
                                    : invisible_time.count();
        const auto duration =
            std::chrono::seconds(std::clamp<int64_t>(seconds, 1, invisible_time.count()));
        result.deliveries.push_back(Delivery{std::move(message), call.authority, bytes,
                                             start + duration / 2, start + duration});
    }
    if (status)
        call.context.TryCancel();
    const auto transport = reader->Finish();
    if (status)
        result.status = status;
    else if (!transport.ok())
        result.status = transport_status(transport);
    else if (result.deliveries.empty())
        result.status = status_received ? UD_BUSY : UD_CORRUPT;
    return result;
}

void RocketMqSession::abandon_receive() {
    if (!receive_call_)
        return;
    {
        const std::lock_guard<std::mutex> lock(rpc_mutex_);
        receive_rpc_->TryCancel();
        receive_rpc_ = nullptr;
    }
    if (receive_call_->pending.valid())
        receive_call_->pending.wait();
    receive_call_.reset();
}

ud_status RocketMqSession::collect_receive(Clock::time_point deadline) {
    if (const auto status = authorize(receive_call_->authority)) {
        abandon_receive();
        return status;
    }
    if (receive_call_->pending.wait_until(deadline) != std::future_status::ready)
        return UD_BUSY;
    {
        const std::lock_guard<std::mutex> lock(rpc_mutex_);
        receive_rpc_ = nullptr;
    }
    auto completed = std::move(receive_call_);
    auto response = completed->pending.get();
    if (const auto status = check())
        return status;
    for (auto &delivery : response.deliveries) {
        const auto id = ++next_delivery_;
        const auto bytes = delivery.bytes;
        held_.emplace(id, std::move(delivery));
        held_bytes_ += bytes;
        ready_.push_back(id);
    }
    return response.status;
}

ud_status RocketMqSession::export_message(uint64_t id, const Delivery &delivery, ud_message &out) {
    const auto &message = delivery.message;
    const auto &props = message.system_properties();
    auto *allocator = host_.allocate;
    const auto copy = [this, allocator](const std::string &source, ud_string &target) {
        if (source.size() > UINT32_MAX)
            return false;
        auto *storage = static_cast<char *>(allocator(host_.context, source.size() + 1));
        if (!storage)
            return false;
        std::memcpy(storage, source.data(), source.size());
        storage[source.size()] = '\0';
        target = {storage, static_cast<uint32_t>(source.size())};
        return true;
    };

    ud_message output{};
    output.header_count = static_cast<uint32_t>(message.user_properties_size()) + props.has_tag();
    if (output.header_count) {
        output.headers = static_cast<ud_message_header *>(
            allocator(host_.context, output.header_count * sizeof(ud_message_header)));
        if (!output.headers)
            return UD_RESOURCE;
        std::fill_n(output.headers, output.header_count, ud_message_header{});
    }
    bool ok = copy(message.body(), output.payload) && copy(props.message_id(), output.id) &&
              copy(message.topic().name(), output.address) &&
              (props.keys().empty() || copy(props.keys(0), output.key));
    uint32_t index = 0;
    for (const auto &property : message.user_properties()) {
        auto &header = output.headers[index++];
        ok = ok && copy(property.first, header.name) && copy(property.second, header.value);
    }
    if (props.has_tag()) {
        auto &header = output.headers[index];
        ok = ok && copy("TAGS", header.name) && copy(props.tag(), header.value);
    }
    if (!ok) {
        const auto release = [this](ud_string text) {
            if (text.data)
                host_.deallocate(const_cast<char *>(text.data));
        };
        release(output.payload);
        release(output.key);
        release(output.id);
        release(output.address);
        for (uint32_t i = 0; i < output.header_count; ++i) {
            release(output.headers[i].name);
            release(output.headers[i].value);
        }
        if (output.headers)
            host_.deallocate(output.headers);
        return UD_RESOURCE;
    }
    output.timestamp_ms = -1;
    output.partition = props.queue_id();
    output.offset = props.has_queue_offset() ? props.queue_offset() : -1;
    const auto &timestamp = props.born_timestamp();
    if (props.has_born_timestamp() && timestamp.seconds() >= -62135596800LL &&
        timestamp.seconds() <= 253402300799LL)
        output.timestamp_ms = timestamp.seconds() * 1000 + timestamp.nanos() / 1000000;
    output.delivery = id;
    output.redelivered = props.delivery_attempt() > 1;
    out = output;
    return UD_OK;
}

void RocketMqSession::drop(uint64_t id) {
    const auto item = held_.find(id);
    held_bytes_ -= item->second.bytes;
    held_.erase(item);
}

ud_status RocketMqSession::receive(uint32_t timeout_ms, ud_message &output) {
    const std::lock_guard<std::mutex> lock(operations_);
    if (auto status = check())
        return status;
    if (!prefetch_)
        return UD_INVALID;
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms ? timeout_ms : 1);
    renew(deadline);
    // Exposed deliveries lose their local byte credit when their broker lease expires.
    for (auto item = held_.begin(); item != held_.end();) {
        const auto &delivery = item->second;
        if (delivery.exposed && (!delivery.valid || delivery.expires <= Clock::now())) {
            held_bytes_ -= delivery.bytes;
            item = held_.erase(item);
        } else {
            ++item;
        }
    }
    if (ready_.empty() && !receive_call_) {
        const auto available = buffer_limit_ - held_bytes_;
        if (held_.size() == prefetch_ || available < message_limit_)
            return UD_BUSY;
        if (Clock::now() >= assignment_refresh_) {
            std::vector<Route> routes;
            if (const auto status = queue_routes(topic_, &group_, routes, deadline))
                return status;
            routes_ = std::move(routes);
            next_route_ = 0;
            assignment_refresh_ = Clock::now() + 30s;
        }
        const auto count = static_cast<uint32_t>(std::min<uint64_t>(
            {prefetch_ - held_.size(), available / message_limit_, broker_max_pop_batch}));
        const auto &route = routes_[next_route_++ % routes_.size()];
        if (const auto status = fetch(route, count, deadline))
            return status == UD_TIMEOUT ? UD_BUSY : status;
    }
    if (ready_.empty() && receive_call_) {
        const auto status = collect_receive(deadline);
        if (status && status != UD_BUSY && status != UD_TIMEOUT)
            return status;
    }
    while (!ready_.empty()) {
        const auto id = ready_.front();
        auto &delivery = held_.at(id);
        if (!delivery.valid || delivery.expires <= Clock::now()) {
            ready_.pop_front();
            drop(id);
            continue;
        }
        if (const auto status = export_message(id, delivery, output))
            return status;
        delivery.exposed = true;
        ready_.pop_front();
        return UD_OK;
    }
    return UD_BUSY;
}

void RocketMqSession::renew(Clock::time_point deadline) {
    const auto now = Clock::now();
    for (auto &item : held_) {
        auto &delivery = item.second;
        if (Clock::now() >= deadline)
            break;
        if (!delivery.valid || delivery.renew > now)
            continue;
        if (delivery.expires <= Clock::now()) {
            delivery.valid = false;
            continue;
        }
        Stub *remote;
        if (stub(delivery.authority, remote))
            return;
        auto *props = delivery.message.mutable_system_properties();
        mq::ChangeInvisibleDurationRequest request;
        *request.mutable_group() = group_;
        *request.mutable_topic() = topic_;
        request.set_message_id(props->message_id());
        request.set_receipt_handle(props->receipt_handle());
        request.mutable_invisible_duration()->set_seconds(invisible_time.count());
        mq::ChangeInvisibleDurationResponse response;
        const auto started = Clock::now();
        bool started_rpc = false;
        const auto status = unary(*remote, &Stub::ChangeInvisibleDuration, request, response,
                                  std::min({deadline, started + rpc_timeout, delivery.expires}),
                                  &started_rpc, true);
        if (status || response.status().code() != mq::OK) {
            // A lost renewal response can conceal a replacement handle. Fence it locally.
            if (started_rpc)
                delivery.valid = false;
            continue;
        }
        if (!response.receipt_handle().empty())
            props->set_receipt_handle(response.receipt_handle());
        delivery.renew = started + renew_after;
        delivery.expires = started + invisible_time;
    }
}

ud_status RocketMqSession::acknowledge(const uint64_t *ids, uint32_t count) {
    const std::lock_guard<std::mutex> lock(operations_);
    if (const auto status = check())
        return status;
    if (!prefetch_ || !count)
        return UD_INVALID;
    std::map<std::string, std::map<std::string, uint64_t>> groups;
    const auto deadline = Clock::now() + rpc_timeout;
    renew(deadline);
    const auto now = Clock::now();
    for (uint32_t index = 0; index < count; ++index) {
        auto item = held_.find(ids[index]);
        if (item == held_.end())
            return ids[index] && ids[index] <= next_delivery_ ? UD_FENCED : UD_INVALID;
        if (!item->second.exposed)
            return UD_INVALID;
        if (!item->second.valid || item->second.expires <= now) {
            drop(ids[index]);
            return UD_FENCED;
        }
        const auto &delivery = item->second;
        const auto &props = delivery.message.system_properties();
        if (!groups[delivery.authority].emplace(props.receipt_handle(), ids[index]).second)
            return UD_INVALID;
    }

    ud_status overall = UD_OK;
    uint32_t confirmed = 0;
    for (const auto &group : groups) {
        Stub *remote;
        auto status = stub(group.first, remote);
        if (status)
            return confirmed ? UD_COMMIT_UNKNOWN : status;
        mq::AckMessageRequest request;
        *request.mutable_group() = group_;
        *request.mutable_topic() = topic_;
        for (const auto &item : group.second) {
            auto *entry = request.add_entries();
            entry->set_message_id(held_.at(item.second).message.system_properties().message_id());
            entry->set_receipt_handle(item.first);
        }
        mq::AckMessageResponse response;
        bool started = false;
        status = unary(*remote, &Stub::AckMessage, request, response, deadline, &started, true);
        if (status)
            return started || confirmed ? UD_COMMIT_UNKNOWN : status;
        const auto code = response.status().code();
        if (!response.entries_size()) {
            const auto result = send_result(code);
            return result.state == UD_MESSAGE_REJECTED && !confirmed ? result.result
                                                                     : UD_COMMIT_UNKNOWN;
        }
        for (const auto &entry : response.entries()) {
            const auto expected = group.second.find(entry.receipt_handle());
            if (expected == group.second.end()) {
                overall = UD_COMMIT_UNKNOWN;
                continue;
            }
            const auto message = held_.find(expected->second);
            if (message == held_.end() ||
                entry.message_id() != message->second.message.system_properties().message_id()) {
                overall = UD_COMMIT_UNKNOWN;
                continue;
            }
            const auto result = send_result(entry.status().code());
            if (result.state == UD_MESSAGE_ACCEPTED) {
                ++confirmed;
                drop(expected->second);
            } else if (overall != UD_COMMIT_UNKNOWN) {
                overall = result.result;
            }
        }
        if (static_cast<size_t>(response.entries_size()) != group.second.size())
            overall = UD_COMMIT_UNKNOWN;
    }
    if (confirmed == count)
        return overall;
    return confirmed || !overall ? UD_COMMIT_UNKNOWN : overall;
}

ud_status RocketMqSession::unsubscribe() {
    const std::lock_guard<std::mutex> lock(operations_);
    abandon_receive();
    prefetch_ = 0;
    held_bytes_ = 0;
    routes_.clear();
    held_.clear();
    ready_.clear();
    consumer_heartbeat_.clear();
    topic_.Clear();
    group_.Clear();
    return UD_OK;
}

ud_status RocketMqSession::publish(const ud_message_target &target, const ud_message *messages,
                                   uint32_t count, ud_message_result *results) {
    const std::lock_guard<std::mutex> lock(operations_);
    std::fill_n(results, count, ud_message_result{UD_MESSAGE_NOT_SENT, UD_OK});
    if (target.kind != UD_MESSAGE_TOPIC)
        return UD_UNSUPPORTED;
    const auto topic = resource(value(target.name));
    const auto tag = value(target.routing_key);
    if (topic.name().empty() || !valid_text(topic.name()) || !valid_text(tag))
        return UD_INVALID;

    mq::SendMessageRequest request;
    std::map<std::string, uint32_t> indexes;
    for (uint32_t i = 0; i < count; ++i) {
        const auto &input = messages[i];
        auto key = value(input.key);
        auto id = value(input.id);
        if (!input.payload.data || (input.key.data && (key.empty() || !valid_text(key))) ||
            !valid_text(id)) {
            results[i] = {UD_MESSAGE_NOT_SENT, UD_INVALID};
            continue;
        }
        auto *message = request.add_messages();
        for (uint32_t j = 0; j < input.header_count; ++j) {
            auto name = value(input.headers[j].name);
            auto text = value(input.headers[j].value);
            auto &properties = *message->mutable_user_properties();
            if (name.empty() || !input.headers[j].value.data || !valid_text(name) ||
                !valid_text(text) || properties.find(name) != properties.end()) {
                results[i] = {UD_MESSAGE_NOT_SENT, UD_INVALID};
                break;
            }
            properties[std::move(name)] = std::move(text);
        }
        if (results[i].result != UD_OK) {
            request.mutable_messages()->RemoveLast();
            continue;
        }
        if (id.empty()) {
            id = new_id(host_);
            if (id.empty())
                return UD_RESOURCE;
        }
        if (!indexes.emplace(id, i).second) {
            request.mutable_messages()->RemoveLast();
            results[i] = {UD_MESSAGE_NOT_SENT, UD_INVALID};
            continue;
        }
        *message->mutable_topic() = topic;
        message->set_body(input.payload.data, input.payload.length);
        auto *props = message->mutable_system_properties();
        props->set_message_id(id);
        props->set_message_type(mq::NORMAL);
        props->set_body_encoding(mq::IDENTITY);
        if (!tag.empty())
            props->set_tag(tag);
        if (input.key.data)
            props->add_keys(std::move(key));
        const auto milliseconds =
            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch())
                .count();
        props->mutable_born_timestamp()->set_seconds(milliseconds / 1000);
        props->mutable_born_timestamp()->set_nanos(static_cast<int32_t>(milliseconds % 1000) *
                                                   1000000);
    }
    if (indexes.empty())
        return UD_INVALID;
    const auto deadline = Clock::now() + rpc_timeout;
    const auto fail = [&indexes, results](ud_status status, int32_t state) {
        for (const auto &entry : indexes)
            results[entry.second] = {state, status};
        return status;
    };
    std::vector<Route> routes;
    if (const auto status = queue_routes(topic, nullptr, routes, deadline))
        return fail(status, UD_MESSAGE_NOT_SENT);
    const auto &route = routes[next_route_++ % routes.size()];
    Stub *remote;
    auto status = stub(route.authority, remote);
    if (!status)
        status = register_client(*remote, topic, false, deadline);
    if (status)
        return fail(status, UD_MESSAGE_NOT_SENT);
    for (auto &message : *request.mutable_messages())
        message.mutable_system_properties()->set_queue_id(route.queue.id());
    mq::SendMessageResponse response;
    bool started = false;
    status = unary(*remote, &Stub::SendMessage, request, response, deadline, &started);
    if (status) {
        if (started && status == UD_AUTH)
            return fail(status, UD_MESSAGE_REJECTED);
        return started ? fail(UD_COMMIT_UNKNOWN, UD_MESSAGE_UNCERTAIN)
                       : fail(status, UD_MESSAGE_NOT_SENT);
    }
    const auto top = send_result(response.status().code());
    for (const auto &entry : indexes)
        results[entry.second] = top.state == UD_MESSAGE_REJECTED
                                    ? top
                                    : ud_message_result{UD_MESSAGE_UNCERTAIN, UD_COMMIT_UNKNOWN};
    std::map<std::string, bool> seen;
    for (const auto &entry : response.entries()) {
        const auto index = indexes.find(entry.message_id());
        if (index == indexes.end())
            continue;
        const auto result = seen.emplace(entry.message_id(), true).second
                                ? send_result(entry.status().code())
                                : ud_message_result{UD_MESSAGE_UNCERTAIN, UD_COMMIT_UNKNOWN};
        results[index->second] = result;
    }
    ud_status overall = UD_OK;
    for (uint32_t i = 0; i < count; ++i)
        if (results[i].result == UD_COMMIT_UNKNOWN || (!overall && results[i].result))
            overall = results[i].result;
    return overall;
}

template <typename Operation> ud_status invoke(void *session, Operation operation) {
    try {
        return operation(*static_cast<RocketMqSession *>(session));
    } catch (const std::bad_alloc &) {
        return UD_RESOURCE;
    } catch (...) {
        return UD_IO;
    }
}

ud_status initialize(const ud_host_api *host, const char *configuration, uint32_t length,
                     void **out) {
    *out = nullptr;
    try {
        auto session = std::make_unique<RocketMqSession>(*host);
        const auto status = session->configure(configuration, length);
        if (status)
            return status;
        *out = session.release();
        return UD_OK;
    } catch (const std::bad_alloc &) {
        return UD_RESOURCE;
    }
}

const ud_message_api message_api = {
    sizeof(ud_message_api),
    UD_PLUGIN_ABI,
    [](void *s, const ud_message_subscription *options) {
        return invoke(s, [options](auto &session) { return session.subscribe(*options); });
    },
    [](void *s, uint32_t timeout_ms, ud_message *out) {
        return invoke(s, [&](auto &session) { return session.receive(timeout_ms, *out); });
    },
    [](void *s, const uint64_t *ids, uint32_t count) {
        return invoke(s, [&](auto &session) { return session.acknowledge(ids, count); });
    },
    [](void *s) { return invoke(s, [](auto &session) { return session.unsubscribe(); }); },
    [](void *s, const ud_message_target *target, const ud_message *messages, uint32_t count,
       ud_message_result *results) {
        return invoke(
            s, [&](auto &session) { return session.publish(*target, messages, count, results); });
    }};

const ud_connector_api connector_api = [] {
    ud_connector_api connector{};
    connector.struct_size = sizeof(connector);
    connector.abi_version = UD_PLUGIN_ABI;
    connector.capabilities = UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_PUBLISH;
    connector.test = [](void *s) {
        return invoke(s, [](auto &session) { return session.test(); });
    };
    connector.cancel = [](void *s) { static_cast<RocketMqSession *>(s)->cancel(); };
    connector.set_control = [](void *s, void *user, ud_status (*check)(void *)) {
        static_cast<RocketMqSession *>(s)->set_control(user, check);
    };
    connector.message = &message_api;
    return connector;
}();
} // namespace

extern "C" UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = {sizeof(*out),
            UD_PLUGIN_ABI,
            "org.universaldata.connector.rocketmq",
            UD_SDK_VERSION,
            UD_PLUGIN_CONNECTOR,
            UD_CAP_READ | UD_CAP_WRITE | UD_CAP_THREAD_SAFE | UD_CAP_PROCESS_LIFETIME,
            initialize,
            [](void *s) { delete static_cast<RocketMqSession *>(s); },
            nullptr,
            &connector_api,
            nullptr,
            sizeof(ud_field),
            sizeof(ud_value)};
    return UD_OK;
}
