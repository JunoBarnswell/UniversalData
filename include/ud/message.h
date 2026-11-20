#ifndef UD_MESSAGE_H
#define UD_MESSAGE_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif

typedef struct ud_connection ud_connection;

enum { UD_MESSAGE_QUEUE = 1, UD_MESSAGE_TOPIC, UD_MESSAGE_EXCHANGE };
enum { UD_MESSAGE_POSITION_REQUIRE = 0, UD_MESSAGE_POSITION_EARLIEST, UD_MESSAGE_POSITION_LATEST };
enum { UD_MESSAGE_NOT_SENT = 0, UD_MESSAGE_ACCEPTED, UD_MESSAGE_REJECTED, UD_MESSAGE_UNCERTAIN };

typedef struct ud_message_target {
    int32_t kind;
    ud_string name, routing_key;
} ud_message_target;

typedef struct ud_message_partition_end {
    int32_t partition;
    int64_t next_offset; /* Exclusive end of the immutable input cut. */
} ud_message_partition_end;

typedef struct ud_message_subscription {
    uint32_t struct_size, abi_version;
    ud_message_target source;
    ud_string group, subscription, filter;
    int32_t initial_position;
    uint32_t prefetch, max_message_bytes;
    uint64_t max_in_flight_bytes;
    const ud_message_partition_end *end_offsets;
    uint32_t end_offset_count;
    int32_t exclusive_transaction; /* One transactional consumer owns the entire topic. */
    uint32_t fixed_partition; /* Partition + 1. Zero uses broker assignment. */
} ud_message_subscription;

typedef struct ud_message_partition_position {
    int32_t partition;
    int64_t earliest, latest, committed; /* committed is -1 when the group has no offset. */
} ud_message_partition_position;

typedef struct ud_message_header {
    ud_string name;
    ud_string value; /* Binary; a NULL pointer represents a null header value. */
} ud_message_header;

typedef struct ud_message {
    ud_string payload, key; /* Binary. NULL differs from a non-NULL empty value. */
    ud_string id, address;
    ud_message_header *headers;
    uint32_t header_count;
    int32_t partition;
    int64_t offset, timestamp_ms;
    uint64_t delivery; /* Session-local handle. Never used as a recovery position. */
    int32_t redelivered;
} ud_message;

typedef struct ud_message_result {
    int32_t state;
    ud_status result;
} ud_message_result;

typedef struct ud_message_offset {
    ud_string topic;
    int32_t partition;
    int64_t next_offset;
} ud_message_offset;

/* Offsets and consumer group metadata are captured from the same live
 * assignment. An empty frontier commits no offsets. All output strings and the
 * offsets array use host allocation. */
typedef struct ud_message_epoch {
    ud_string cluster_id, group_metadata;
    ud_message_offset *offsets;
    uint32_t offset_count;
    int32_t allow_advanced_offsets; /* Stateless slot recovery may observe offsets advanced by a successor. */
} ud_message_epoch;
typedef ud_status (*ud_message_visit)(void *user, const ud_message *message);

/* Receive returns host-owned storage, or UD_BUSY if no delivery is ready.
 * Send returns one known or uncertain outcome per request entry. An unsettled
 * subscription is abandoned at close; close never acknowledges messages. */
typedef struct ud_message_api {
    uint32_t struct_size, abi_version;
    ud_status (*subscribe)(void *, const ud_message_subscription *);
    ud_status (*receive)(void *, uint32_t timeout_ms, ud_message *);
    ud_status (*acknowledge)(void *, const uint64_t *, uint32_t count);
    ud_status (*unsubscribe)(void *);
    ud_status (*publish)(void *, const ud_message_target *, const ud_message *,
                         uint32_t count, ud_message_result *);
    ud_status (*capture_epoch)(void *, const uint64_t *, uint32_t, ud_message_epoch *);
    ud_status (*begin_epoch)(void *, ud_string transactional_id, ud_string cluster_id);
    ud_status (*commit_epoch)(void *, const ud_message_epoch *);
    ud_status (*abort_epoch)(void *);
    ud_status (*release_epoch)(void *, const uint64_t *, uint32_t);
    ud_status (*visit_committed)(void *, ud_string topic, int32_t partition,
                                  void *user, ud_message_visit visit);
    ud_status (*verify_epoch)(void *, const ud_message_epoch *);

} ud_message_api;

/* Optional provider extension exported as ud_plugin_partition_api. It retains
 * the ordinary Message ABI and adds subscription at a frozen first offset. */
typedef struct ud_message_partition_api {
    uint32_t struct_size, abi_version;
    ud_status (*subscribe)(void *, const ud_message_subscription *, int64_t);
    ud_status (*positions)(void *, ud_string, ud_string, ud_message_partition_position *, uint32_t, uint32_t *);
} ud_message_partition_api;
typedef const ud_message_partition_api *(*ud_message_partition_entry)(uint32_t);
UD_API ud_status ud_connection_message_subscribe_partition(ud_connection *, const ud_message_subscription *, int64_t start);
UD_API ud_status ud_connection_message_partition_positions(ud_connection *, ud_string topic, ud_string group,
    ud_message_partition_position *, uint32_t capacity, uint32_t *);
UD_API void ud_message_clear(ud_message *);
UD_API void ud_message_epoch_clear(ud_message_epoch *);
UD_API ud_status ud_connection_message_subscribe(ud_connection *, const ud_message_subscription *);
UD_API ud_status ud_connection_message_receive(ud_connection *, uint32_t timeout_ms, ud_message *);
UD_API ud_status ud_connection_message_acknowledge(ud_connection *, const uint64_t *, uint32_t count);
UD_API ud_status ud_connection_message_unsubscribe(ud_connection *);
UD_API ud_status ud_connection_message_publish(ud_connection *, const ud_message_target *,
                                                 const ud_message *, uint32_t count,
                                                 ud_message_result *);
UD_API ud_status ud_connection_message_capture_epoch(ud_connection *, const uint64_t *, uint32_t,
                                                       ud_message_epoch *);
UD_API ud_status ud_connection_message_begin_epoch(ud_connection *, ud_string, ud_string);
UD_API ud_status ud_connection_message_commit_epoch(ud_connection *, const ud_message_epoch *);
UD_API ud_status ud_connection_message_abort_epoch(ud_connection *);
UD_API ud_status ud_connection_message_release_epoch(ud_connection *, const uint64_t *, uint32_t);
UD_API ud_status ud_connection_message_visit_committed(ud_connection *, ud_string, int32_t,
                                                         void *, ud_message_visit);
UD_API ud_status ud_connection_message_verify_epoch(ud_connection *, const ud_message_epoch *);

#ifdef __cplusplus
}
#endif
#endif
