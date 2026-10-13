#include "../internal.h"
#include "ud/error.h"
int32_t ud_record_error_kind(ud_status code) {
    return code == UD_TRUNCATED                                           ? UD_RECORD_TRUNCATION
           : code == UD_QUALITY                                           ? UD_RECORD_CONSTRAINT
           : code == UD_PARSE || code == UD_OVERFLOW || code == UD_SCHEMA ? UD_RECORD_CONVERSION
                                                                          : UD_RECORD_EXECUTION;
}
int32_t ud_record_error_recoverable(ud_status code) {
    return ud_record_error_kind(code) != UD_RECORD_EXECUTION;
}
ud_status ud_record_options_validate(ud_context *ctx, const ud_record_options *options) {
    if (!options)
        return UD_OK;
    if (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION ||
        options->errors.struct_size != sizeof(ud_error_context) ||
        options->errors.abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (options->errors.memory != ctx ||
        (!options->source_scope.high && !options->source_scope.low) ||
        options->policy < UD_ERROR_FAIL || options->policy > UD_ERROR_IGNORE ||
        options->include_input < 0 || options->include_input > 1)
        return UD_INVALID;
    return UD_OK;
}
ud_status ud_error_schema(const ud_error_context *c, const ud_batch *input, int include,
                          ud_batch **out) {
    (void)input;
    if (!c || !out || !c->memory || (include != 0 && include != 1))
        return UD_INVALID;
    if (c->struct_size != sizeof(*c) || c->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    static const char *names[] = {"executionId", "taskId",      "pipelineId", "componentId",
                                  "ruleId",      "ruleVersion", "columnId",   "sourceRowId",
                                  "errorCode",   "policy",      "errorKind",  "providerCode",
                                  "inputFormat"};
    uint32_t n = 13 + (include ? 1 : 0);
    ud_field *fields = ud_alloc(c->memory, (size_t)n * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    char scope[33];
    ud_id_format(c->field_scope, scope);
    for (uint32_t i = 0; !status && i < 13; i++) {
        fields[i] = (ud_field){.name = names[i],
                               .name_length = (uint32_t)strlen(names[i]),
                               .type = i == 5 || i == 8 ? UD_INT64 : UD_UTF8};
        status = ud_id_make(c->memory, (ud_string){scope, 32},
                            (ud_string){names[i], fields[i].name_length}, &fields[i].identity);
    }
    if (!status && include) {
        fields[13] = (ud_field){.name = "input", .name_length = 5, .type = UD_BINARY};
        status = ud_id_make(c->memory, (ud_string){scope, 32}, (ud_string){"input", 5},
                            &fields[13].identity);
    }
    if (!status)
        status = ud_batch_create(c->memory, fields, n, 0, out);
    ud_free(fields);
    return status;
}
ud_status ud_error_append(const ud_error_context *c, ud_batch *out, const ud_record_error *error) {
    if (!c || !out || !error || !c->memory || out->ctx != c->memory ||
        (out->columns != 13 && out->columns != 14) || error->policy < 0 || error->policy > 2 ||
        error->kind < 0 || error->kind > UD_RECORD_EXECUTION || !error->rule.data ||
        !error->rule.length || (error->raw_length && !error->raw))
        return UD_INVALID;
    if (c->struct_size != sizeof(*c) || c->abi_version != UD_ABI_VERSION ||
        error->struct_size != sizeof(*error) || error->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!ud_record_error_recoverable(error->code))
        return error->code ? error->code : UD_INVALID;
    const ud_batch *input = error->input;
    ud_row_info info = error->source;
    if (input)
        UD_TRY(ud_batch_row_info(input, error->row, &info));
    if (!info.id.high && !info.id.low)
        return UD_SCHEMA;
    char source[33], field[33];
    ud_id_format(info.id, source);
    ud_id_format(error->column, field);
    const char *policies[] = {"fail", "redirect", "ignore"};
    static const char *kinds[] = {"", "conversion", "truncation", "constraint", "execution"};
    int kind = error->kind ? error->kind : ud_record_error_kind(error->code);
    if (kind == UD_RECORD_EXECUTION)
        return error->code;
    ud_string format = error->input_format.length ? error->input_format
                       : input                    ? (ud_string){"Wire", 4}
                                                  : (ud_string){"Raw", 3};
    ud_string text[] = {c->execution_id,
                        c->task_id,
                        c->pipeline_id,
                        c->component_id,
                        error->rule,
                        {0},
                        {field, 32},
                        {source, 32},
                        {0},
                        {policies[error->policy], (uint32_t)strlen(policies[error->policy])},
                        {kinds[kind], (uint32_t)strlen(kinds[kind])},
                        error->provider_code,
                        format};
    ud_value *values = ud_alloc(c->memory, (size_t)out->columns * sizeof(*values));
    if (!values)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < 13; i++)
        values[i] =
            i == 5 || i == 8
                ? (ud_value){.type = UD_INT64,
                             .integer = i == 5 ? error->rule_version : error->code}
                : (ud_value){.type = UD_UTF8, .text = text[i].data, .text_length = text[i].length};
    ud_status status = UD_OK;
    uint8_t *payload = NULL;
    uint64_t payload_size = 0;
    if (out->columns == 14) {
        if (input) {
            ud_batch *record = NULL;
            status = ud_batch_create(c->memory, input->schema->fields, input->columns, 1, &record);
            if (!status)
                status = ud_batch_append_from(record, input, error->row);
            if (!status)
                status = ud_batch_serialize(record, &payload, &payload_size);
            ud_batch_destroy(record);
        }
        if (!status && payload_size > UINT32_MAX)
            status = UD_RESOURCE;
        values[13] = (ud_value){.type = UD_BINARY,
                                .text = input ? (const char *)payload : (const char *)error->raw,
                                .text_length = input ? (uint32_t)payload_size : error->raw_length};
    }
    if (!status)
        status = ud_batch_append(out, values, out->columns);
    if (!status) {
        info.flags |= UD_ROW_ERROR;
        info.operation = 0;
        status = ud_error_rule_token(c, error->rule, error->rule_version, &info.rule);
        if (!status)
            status = ud_batch_set_row_info(out, out->rows - 1, &info);
    }
    ud_free(payload);
    ud_free(values);
    return status;
}
ud_status ud_error_rule_token(const ud_error_context *c, ud_string rule, uint32_t version,
                              ud_id *out) {
    ud_bytes identity = {.ctx = c->memory};
    ud_string parts[] = {c->package_id, c->pipeline_id, c->component_id, rule};
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < 4; i++)
        status = ud_put_string(&identity, parts[i].data, parts[i].length);
    if (!status)
        status = ud_put_u64(&identity, version);
    if (!status) {
        uint8_t digest[32];
        ud_hash(identity.data, identity.size, digest);
        *out = (ud_id){0};
        for (uint32_t i = 0; i < 8; i++) {
            out->high = (out->high << 8) | digest[i];
            out->low = (out->low << 8) | digest[i + 8];
        }
    }
    ud_bytes_clear(&identity);
    return status;
}
