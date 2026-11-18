#ifndef UD_KAFKA_STAGE_H
#define UD_KAFKA_STAGE_H

#include "../package/internal.h"
#include "ud/message.h"

void ud_kafka_stage_identity(const ud_component_context *, ud_string pipeline, uint8_t digest[32]);
ud_status ud_kafka_stage_tag(ud_context *, ud_message *, const uint8_t digest[32], uint32_t member,
                            int32_t kind, int64_t watermark);
ud_status ud_kafka_stage_read(ud_message *, const uint8_t digest[32], uint32_t members,
                             uint32_t *member, int32_t *kind, int64_t *watermark);
int ud_kafka_stage_enabled(const ud_json *);

#endif
