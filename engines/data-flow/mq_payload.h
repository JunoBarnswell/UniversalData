#ifndef UD_MQ_PAYLOAD_H
#define UD_MQ_PAYLOAD_H
#include "builtins_internal.h"
#include "../../connectors/schema_registry.h"

enum { UD_MQ_BINARY = 1, UD_MQ_JSON, UD_MQ_AVRO };
typedef struct ud_mq_settings {
    ud_string connection, registry_connection, subject;
    ud_message_target target;
    uint32_t schema_version, maximum_bytes, fixed_partition;
    int format, partition_by_worker;
} ud_mq_settings;

#define UD_MQ_PAYLOAD_PROPERTIES                                                                   \
    "\"connection\":{\"type\":\"string\",\"minLength\":1,\"x-role\":\"resource-reference\"},"                 \
    "\"targetType\":{\"enum\":[\"queue\",\"topic\",\"exchange\"]},\"address\":{\"type\":\"string\"},"             \
    "\"routingKey\":{\"type\":\"string\"},\"partition\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":65535}," \
    "\"partitionByWorker\":{\"type\":\"boolean\",\"default\":false}," \
    "\"format\":{\"enum\":[\"binary\",\"json\",\"avro\"],"                                                       \
    "\"default\":\"binary\"},\"maxMessageBytes\":{\"type\":\"integer\",\"minimum\":1,"                        \
    "\"maximum\":16777216,\"default\":1048576},\"registryConnection\":{\"type\":\"string\","                 \
    "\"x-role\":\"resource-reference\"},\"schemaSubject\":{\"type\":\"string\"},"                            \
    "\"schemaVersion\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":2147483647}"

ud_status ud_mq_settings_parse(const ud_component_context *, const ud_json *, ud_mq_settings *);
ud_status ud_mq_schema(const ud_component_context *, const ud_json *, const ud_mq_settings *,
                       ud_batch **);
ud_status ud_mq_decode(const ud_message *, int format, int strict, ud_schema_registry *,
                       ud_batch *, ud_id *);
ud_status ud_mq_encode(const ud_batch *, uint32_t row, int, ud_schema_registry *, ud_message *);
ud_status ud_mq_project_payload(const ud_batch *, ud_batch **);
ud_status ud_mq_encode_metadata(const ud_batch *, uint32_t row, ud_message *);
#endif
