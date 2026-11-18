#include "kafka_stage.h"

static const char header_name[] = "ud.stage.v1";
enum { STAGE_ID_SIZE = 32, STAGE_TAG_SIZE = 42 };

int ud_kafka_stage_enabled(const ud_json *json) {
    const char *value;
    uint32_t length;
    return ud_json_kind(json) == UD_JSON_BOOL && !ud_json_text(json, &value, &length) &&
           length == 4 && !memcmp(value, "true", 4);
}

void ud_kafka_stage_identity(const ud_component_context *context, ud_string pipeline, uint8_t digest[32]) {
    ud_sha256 hash;
    ud_sha256_init(&hash);
    const ud_execution_options *options = &context->execution->options;
    const ud_string parts[] = {options->tenant, options->worker_run_id, pipeline};
    const uint8_t *package = context->activation->package->fingerprint;
    ud_sha256_update(&hash, package, 32);
    for (uint32_t i = 0; i < sizeof(parts) / sizeof(*parts); i++) {
        uint8_t length[4];
        for (uint32_t j = 0; j < sizeof(length); j++)
            length[j] = (uint8_t)(parts[i].length >> (8 * j));
        ud_sha256_update(&hash, length, sizeof(length));
        if (parts[i].length)
            ud_sha256_update(&hash, parts[i].data, parts[i].length);
    }
    ud_sha256_finish(&hash, digest);
}

ud_status ud_kafka_stage_tag(ud_context *memory, ud_message *message, const uint8_t digest[32],
                            uint32_t member, int32_t kind, int64_t watermark) {
    ud_message_header *headers = ud_alloc(memory, (message->header_count + 1) * sizeof(*headers));
    char *name = ud_strdup_n(memory, header_name, sizeof(header_name) - 1);
    uint8_t *value = ud_alloc(memory, STAGE_TAG_SIZE);
    if (!headers || !name || !value) {
        ud_free(headers);
        ud_free(name);
        ud_free(value);
        return UD_RESOURCE;
    }
    if (message->header_count)
        memcpy(headers, message->headers, message->header_count * sizeof(*headers));
    ud_free(message->headers);
    memcpy(value, digest, STAGE_ID_SIZE);
    value[32] = (uint8_t)member;
    value[33] = (uint8_t)kind;
    for (uint32_t i = 0; i < 8; i++)
        value[34 + i] = (uint8_t)((uint64_t)watermark >> (8 * i));
    headers[message->header_count++] = (ud_message_header){
        {name, sizeof(header_name) - 1}, {(const char *)value, STAGE_TAG_SIZE}};
    message->headers = headers;
    return UD_OK;
}

ud_status ud_kafka_stage_read(ud_message *message, const uint8_t digest[32], uint32_t members,
                             uint32_t *member, int32_t *kind, int64_t *watermark) {
    if (!message->header_count)
        return UD_INCOMPATIBLE;
    ud_message_header *header = message->headers + message->header_count - 1;
    if (!ud_named(header->name, header_name) || header->value.length != STAGE_TAG_SIZE)
        return UD_INCOMPATIBLE;
    const uint8_t *value = (const uint8_t *)header->value.data;
    if (memcmp(value, digest, STAGE_ID_SIZE))
        return UD_END;
    *member = value[32];
    *kind = value[33];
    if (*member >= members || (*kind != UD_COMPONENT_DATA && *kind != UD_COMPONENT_INPUT_END &&
        *kind != UD_COMPONENT_WATERMARK && *kind != UD_COMPONENT_IDLE && *kind != UD_COMPONENT_ACTIVE))
        return UD_CORRUPT;
    uint64_t encoded = 0;
    for (uint32_t i = 0; i < 8; i++)
        encoded |= (uint64_t)value[34 + i] << (8 * i);
    memcpy(watermark, &encoded, sizeof(encoded));
    ud_free((void *)header->name.data);
    ud_free((void *)header->value.data);
    message->header_count--;
    return UD_OK;
}
