#include "mq_payload.h"
#include "../../connectors/avro_record.h"

static const ud_field header_fields[] = {
    {.name = "name", .name_length = 4, .type = UD_UTF8},
    {.name = "value", .name_length = 5, .type = UD_BINARY, .nullable = 1},
};
static const ud_field header_item = {.name = "item", .name_length = 4, .type = UD_STRUCT,
                                    .children = header_fields, .child_count = 2};
enum { META_ID, META_ADDRESS, META_KEY, META_HEADERS, META_PARTITION, META_OFFSET,
       META_TIMESTAMP, META_REDELIVERED, META_COUNT };
static const ud_field metadata_fields[META_COUNT] = {
    {.name = "id", .name_length = 2, .type = UD_UTF8, .nullable = 1},
    {.name = "address", .name_length = 7, .type = UD_UTF8, .nullable = 1},
    {.name = "key", .name_length = 3, .type = UD_BINARY, .nullable = 1},
    {.name = "headers", .name_length = 7, .type = UD_LIST, .children = &header_item,
     .child_count = 1},
    {.name = "partition", .name_length = 9, .type = UD_INT64, .nullable = 1},
    {.name = "offset", .name_length = 6, .type = UD_INT64, .nullable = 1},
    {.name = "timestampMs", .name_length = 11, .type = UD_INT64, .nullable = 1},
    {.name = "redelivered", .name_length = 11, .type = UD_BOOL},
};
static const ud_field metadata_field = {.name = "_message", .name_length = 8, .type = UD_STRUCT,
                                        .children = metadata_fields, .child_count = META_COUNT};
static const ud_field payload_field = {.name = "payload", .name_length = 7, .type = UD_BINARY,
                                       .nullable = 1};

ud_status ud_mq_settings_parse(const ud_component_context *context, const ud_json *config,
                               ud_mq_settings *settings) {
    memset(settings, 0, sizeof(*settings));
    UD_TRY(ud_json_string(ud_member(config, "connection"), &settings->connection));
    UD_TRY(ud_builtin_connection_defined(context, settings->connection));
    ud_string target_kind;
    UD_TRY(ud_json_string(ud_member(config, "targetType"), &target_kind));
    settings->target.kind = ud_named(target_kind, "queue") ? UD_MESSAGE_QUEUE :
                            ud_named(target_kind, "topic") ? UD_MESSAGE_TOPIC : UD_MESSAGE_EXCHANGE;
    UD_TRY(ud_json_string(ud_member(config, "address"), &settings->target.name));
    if (ud_member(config, "routingKey"))
        UD_TRY(ud_json_string(ud_member(config, "routingKey"), &settings->target.routing_key));
    if (!settings->target.name.length ||
        (settings->target.routing_key.length && settings->target.kind != UD_MESSAGE_EXCHANGE))
        return UD_SCHEMA;
    const ud_json *fixed = ud_member(config, "partition");
    const ud_json *worker = ud_member(config, "partitionByWorker");
    if (worker) {
        const char *value;
        uint32_t length;
        UD_TRY(ud_json_text(worker, &value, &length));
        settings->partition_by_worker = length == 4 && !memcmp(value, "true", 4);
    }
    if (settings->partition_by_worker) {
        if (fixed || settings->target.kind != UD_MESSAGE_TOPIC)
            return UD_SCHEMA;
        settings->fixed_partition = context->execution->options.worker_member + 1;
    }
    if (fixed) {
        uint64_t partition;
        UD_TRY(ud_json_uint(fixed, &partition));
        if (settings->target.kind != UD_MESSAGE_TOPIC || partition >= 65536)
            return UD_SCHEMA;
        settings->fixed_partition = (uint32_t)partition + 1;
    }
    ud_string format;
    UD_TRY(ud_json_string(ud_member(config, "format"), &format));
    settings->format = ud_named(format, "binary") ? UD_MQ_BINARY :
                       ud_named(format, "json") ? UD_MQ_JSON : UD_MQ_AVRO;
    uint64_t bytes = 0;
    UD_TRY(ud_json_uint(ud_member(config, "maxMessageBytes"), &bytes));
    settings->maximum_bytes = (uint32_t)bytes;
    if (settings->format != UD_MQ_AVRO)
        return ud_member(config, "registryConnection") || ud_member(config, "schemaSubject") ||
                       ud_member(config, "schemaVersion") ? UD_SCHEMA : UD_OK;
    if (settings->target.kind != UD_MESSAGE_TOPIC)
        return UD_SCHEMA;
    UD_TRY(ud_json_string(ud_member(config, "registryConnection"), &settings->registry_connection));
    UD_TRY(ud_builtin_connection_defined(context, settings->registry_connection));
    UD_TRY(ud_json_string(ud_member(config, "schemaSubject"), &settings->subject));
    uint64_t version = 0;
    UD_TRY(ud_json_uint(ud_member(config, "schemaVersion"), &version));
    if (!settings->subject.length || !version)
        return UD_SCHEMA;
    settings->schema_version = (uint32_t)version;
    return UD_OK;
}

ud_status ud_mq_schema(const ud_component_context *context, const ud_json *config,
                       const ud_mq_settings *settings, ud_batch **out) {
    *out = NULL;
    if (settings->format == UD_MQ_BINARY) {
        if (ud_member(config, "schema"))
            return UD_SCHEMA;
        ud_field fields[] = {payload_field, metadata_field};
        return ud_batch_create(context->memory, fields, 2, 0, out);
    }
    ud_batch *payload = NULL;
    UD_TRY(ud_builtin_schema_config(context, config, &payload));
    uint32_t columns = payload->columns;
    ud_field *fields = ud_alloc(context->memory, ((size_t)columns + 1) * sizeof(*fields));
    ud_status status = fields ? UD_OK : UD_RESOURCE;
    for (uint32_t i = 0; !status && i < columns; i++) {
        fields[i] = payload->schema->fields[i];
        if (fields[i].name_length == metadata_field.name_length &&
            !memcmp(fields[i].name, metadata_field.name, metadata_field.name_length))
            status = UD_SCHEMA;
    }
    if (!status) {
        fields[columns] = metadata_field;
        status = ud_batch_create(context->memory, fields, columns + 1, 0, out);
    }
    ud_free(fields);
    ud_batch_destroy(payload);
    return status;
}

static ud_value text_value(int32_t type, ud_string value) {
    return (ud_value){.type = type, .text = value.data, .text_length = value.length,
                      .is_null = !value.data};
}

static ud_value position_value(int64_t value) {
    return (ud_value){.type = UD_INT64, .integer = value, .is_null = value < 0};
}

static ud_status avro_decode(const ud_message *message, ud_schema_registry *registry,
                             ud_batch *target, ud_batch **values) {
    const unsigned char *prefix = (const unsigned char *)message->payload.data;
    if (!prefix || message->payload.length < 5 || prefix[0])
        return UD_QUALITY;
    uint32_t id = ((uint32_t)prefix[1] << 24) | ((uint32_t)prefix[2] << 16) |
                  ((uint32_t)prefix[3] << 8) | prefix[4];
    if (!id || id > INT32_MAX)
        return UD_QUALITY;
    ud_string writer_schema;
    UD_TRY(ud_schema_registry_find(registry, id, &writer_schema));
    UD_TRY(ud_batch_create(target->ctx, target->schema->fields, target->columns - 1, 1, values));
    ud_status status = ud_avro_record_decode(target->ctx, writer_schema,
        ud_schema_registry_schema(registry),
        (ud_string){message->payload.data + 5, message->payload.length - 5}, *values);
    return status == UD_CORRUPT ? UD_QUALITY : status;
}

ud_status ud_mq_decode(const ud_message *message, int format, ud_schema_registry *registry,
                       ud_batch *batch, ud_id *column) {
    uint32_t count = batch->columns - 1;
    ud_context *ctx = batch->ctx;
    ud_value *values = ud_alloc(ctx, (size_t)batch->columns * sizeof(*values));
    if (!values)
        return UD_RESOURCE;
    ud_json *object = NULL;
    ud_batch *avro = NULL;
    ud_status status = UD_OK;
    if (format == UD_MQ_BINARY)
        values[0] = text_value(UD_BINARY, message->payload);
    else if (format == UD_MQ_JSON) {
        status = message->payload.data ? ud_json_parse(ctx, message->payload.data,
                                                       message->payload.length, &object, NULL)
                                       : UD_QUALITY;
        if (!status && ud_json_kind(object) != UD_JSON_OBJECT)
            status = UD_SCHEMA;
        for (uint32_t i = 0; !status && i < count; i++) {
            const ud_field *f = batch->schema->fields + i;
            status = ud_json_value_decode(ctx, f, ud_json_find(object, f->name, f->name_length),
                                          UD_ROUND_EXACT, values + i);
            if (status)
                *column = f->identity;
        }
    } else {
        status = avro_decode(message, registry, batch, &avro);
        for (uint32_t i = 0; !status && i < count; i++)
            status = ud_batch_get(avro, 0, i, values + i);
    }
    uint32_t headers = message->header_count;
    ud_value *entries = !status && headers ? ud_alloc(ctx, (size_t)headers * 3 * sizeof(*entries))
                                           : NULL;
    if (!status && headers && !entries)
        status = UD_RESOURCE;
    if (!status) {
        for (uint32_t i = 0; i < headers; i++) {
            ud_value *cells = entries + headers + (size_t)i * 2;
            cells[0] = text_value(UD_UTF8, message->headers[i].name);
            cells[1] = text_value(UD_BINARY, message->headers[i].value);
            entries[i] = (ud_value){.type = UD_STRUCT, .children = cells, .child_count = 2};
        }
        ud_value metadata[META_COUNT] = {
            text_value(UD_UTF8, message->id), text_value(UD_UTF8, message->address),
            text_value(UD_BINARY, message->key),
            {.type = UD_LIST, .children = entries, .child_count = headers},
            position_value(message->partition), position_value(message->offset),
            position_value(message->timestamp_ms),
            {.type = UD_BOOL, .integer = !!message->redelivered},
        };
        values[count] = (ud_value){.type = UD_STRUCT, .children = metadata, .child_count = META_COUNT};
        status = ud_batch_append(batch, values, batch->columns);
    }
    ud_free(entries);
    if (format == UD_MQ_JSON)
        for (uint32_t i = 0; i < count; i++)
            ud_clear_value(values + i);
    ud_free(values);
    ud_json_destroy(object);
    ud_batch_destroy(avro);
    return status;
}

ud_status ud_mq_project_payload(const ud_batch *batch, ud_batch **out) {
    *out = NULL;
    uint32_t count = batch->columns;
    if (count && batch->schema->fields[count - 1].name_length == 8 &&
        !memcmp(batch->schema->fields[count - 1].name, "_message", 8))
        count--;
    if (!count)
        return UD_SCHEMA;
    uint32_t *indices = ud_alloc(batch->ctx, count * sizeof(*indices));
    if (!indices)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < count; i++)
        indices[i] = i;
    ud_status status = ud_batch_project(batch->ctx, batch, indices, batch->schema->fields, count, out);
    ud_free(indices);
    return status;
}

static ud_status copy_value_bytes(ud_context *ctx, const ud_value *value, ud_string *out) {
    if (value->type != UD_UTF8 && value->type != UD_BINARY)
        return UD_SCHEMA;
    if (value->is_null)
        return UD_OK;
    char *copy = ud_strdup_n(ctx, value->text, value->text_length);
    if (!copy)
        return UD_RESOURCE;
    *out = (ud_string){copy, value->text_length};
    return UD_OK;
}

ud_status ud_mq_encode_metadata(const ud_batch *batch, uint32_t row, ud_message *message) {
    if (!batch->columns)
        return UD_SCHEMA;
    uint32_t index = batch->columns - 1;
    const ud_field *field = batch->schema->fields + index;
    message->partition = -1;
    message->offset = message->timestamp_ms = -1;
    if (field->name_length != 8 || memcmp(field->name, "_message", 8))
        return UD_OK;
    if (field->type != UD_STRUCT || field->child_count != META_COUNT)
        return UD_SCHEMA;
    ud_value metadata;
    UD_TRY(ud_batch_get(batch, row, index, &metadata));
    if (metadata.is_null)
        return UD_OK;
    UD_TRY(copy_value_bytes(batch->ctx, metadata.children + META_ID, &message->id));
    UD_TRY(copy_value_bytes(batch->ctx, metadata.children + META_KEY, &message->key));
    const ud_value *timestamp = metadata.children + META_TIMESTAMP;
    if (!timestamp->is_null)
        message->timestamp_ms = timestamp->integer;
    const ud_value *headers = metadata.children + META_HEADERS;
    if (headers->type != UD_LIST)
        return UD_SCHEMA;
    if (headers->child_count) {
        message->headers = ud_alloc(batch->ctx, headers->child_count * sizeof(*message->headers));
        if (!message->headers)
            return UD_RESOURCE;
        message->header_count = headers->child_count;
    }
    for (uint32_t i = 0; i < headers->child_count; i++) {
        const ud_value *entry = headers->children + i;
        if (entry->is_null || entry->type != UD_STRUCT || entry->child_count != 2)
            return UD_SCHEMA;
        UD_TRY(copy_value_bytes(batch->ctx, entry->children, &message->headers[i].name));
        UD_TRY(copy_value_bytes(batch->ctx, entry->children + 1, &message->headers[i].value));
    }
    return UD_OK;
}

ud_status ud_mq_encode(const ud_batch *batch, uint32_t row, int format,
                       ud_schema_registry *registry, ud_message *message) {
    if (format == UD_MQ_BINARY) {
        ud_value value;
        UD_TRY(ud_batch_get(batch, row, 0, &value));
        if (value.type != UD_BINARY)
            return UD_SCHEMA;
        char *copy = value.is_null ? NULL : ud_strdup_n(batch->ctx, value.text, value.text_length);
        if (!value.is_null && !copy)
            return UD_RESOURCE;
        message->payload = (ud_string){copy, value.text_length};
        return UD_OK;
    }
    ud_bytes bytes = {.ctx = batch->ctx};
    ud_status status;
    if (format == UD_MQ_AVRO) {
        ud_string avro = {0};
        uint32_t id = ud_schema_registry_id(registry);
        uint8_t prefix[] = {0, (uint8_t)(id >> 24), (uint8_t)(id >> 16), (uint8_t)(id >> 8), (uint8_t)id};
        status = ud_avro_record_encode(batch, row, ud_schema_registry_schema(registry), &avro);
        if (!status)
            status = ud_bytes_put(&bytes, prefix, sizeof(prefix));
        if (!status)
            status = ud_bytes_put(&bytes, avro.data, avro.length);
        ud_free((void *)avro.data);
    } else {
        status = ud_bytes_put(&bytes, "{", 1);
        for (uint32_t i = 0; !status && i < batch->columns; i++) {
            const ud_field *f = batch->schema->fields + i;
            ud_value value;
            status = ud_batch_get(batch, row, i, &value);
            if (!status && i)
                status = ud_bytes_put(&bytes, ",", 1);
            if (!status)
                status = ud_json_quote(&bytes, f->name, f->name_length);
            if (!status)
                status = ud_bytes_put(&bytes, ":", 1);
            if (!status)
                status = ud_json_value_encode(&bytes, f, &value);
        }
        if (!status)
            status = ud_bytes_put(&bytes, "}", 1);
    }
    if (status || bytes.size > UINT32_MAX) {
        ud_bytes_clear(&bytes);
        return status ? status : UD_OVERFLOW;
    }
    message->payload = (ud_string){(const char *)bytes.data, (uint32_t)bytes.size};
    return UD_OK;
}
