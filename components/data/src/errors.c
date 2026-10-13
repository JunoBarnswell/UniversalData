#include "../internal.h"
#include "ud/error.h"
ud_status ud_error_schema(const ud_error_context *c, const ud_batch *input, int include,
                          ud_batch **out) {
    static const char *names[] = {"executionId", "taskId",      "pipelineId", "componentId",
                                  "ruleId",      "ruleVersion", "columnId",   "sourceRowId",
                                  "errorCode",   "policy"};
    uint32_t n = 10 + (include ? 1 : 0);
    ud_field *fields = ud_alloc(c->memory, (size_t)n * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    char scope[33];
    ud_id_format(c->field_scope, scope);
    for (uint32_t i = 0; !status && i < 10; i++) {
        fields[i] = (ud_field){.name = names[i],
                               .name_length = (uint32_t)strlen(names[i]),
                               .type = i == 5 || i == 8 ? UD_INT64 : UD_UTF8};
        status = ud_id_make(c->memory, (ud_string){scope, 32},
                            (ud_string){names[i], fields[i].name_length}, &fields[i].identity);
    }
    if (!status && include) {
        fields[10] = (ud_field){.name = "input",
                                .name_length = 5,
                                .type = UD_BINARY};
        status = ud_id_make(c->memory, (ud_string){scope, 32}, (ud_string){"input", 5},
                            &fields[10].identity);
    }
    if (!status)
        status = ud_batch_create(c->memory, fields, n, 0, out);
    ud_free(fields);
    return status;
}
ud_status ud_error_append(const ud_error_context *c, ud_batch *out, const ud_batch *input,
                          uint32_t row, ud_string rule, uint32_t version, ud_id column,
                          ud_status code, int policy) {
    ud_row_info info;
    UD_TRY(ud_batch_row_info(input, row, &info));
    if (!info.id.high && !info.id.low)
        return UD_SCHEMA;
    char source[33], field[33];
    ud_id_format(info.id, source);
    ud_id_format(column, field);
    const char *policies[] = {"fail", "redirect", "ignore"};
    if (policy < 0 || policy > 2)
        return UD_INVALID;
    ud_string text[] = {c->execution_id,
                        c->task_id,
                        c->pipeline_id,
                        c->component_id,
                        rule,
                        {0},
                        {field, 32},
                        {source, 32},
                        {0},
                        {policies[policy], (uint32_t)strlen(policies[policy])}};
    ud_value *values = ud_alloc(c->memory, (size_t)out->columns * sizeof(*values));
    if (!values)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < 10; i++)
        values[i] =
            i == 5 || i == 8
                ? (ud_value){.type = UD_INT64, .integer = i == 5 ? version : code}
                : (ud_value){.type = UD_UTF8, .text = text[i].data, .text_length = text[i].length};
    ud_status status = UD_OK;
    uint8_t *payload = NULL;uint64_t payload_size=0;
    if (out->columns == 11) {
        ud_batch *record=NULL;
        status=ud_batch_create(c->memory,input->schema->fields,input->columns,1,&record);
        if(!status)status=ud_batch_append_from(record,input,row);
        if(!status)status=ud_batch_serialize(record,&payload,&payload_size);
        ud_batch_destroy(record);
        if(!status&&payload_size>UINT32_MAX)status=UD_RESOURCE;
        values[10]=(ud_value){.type=UD_BINARY,.text=(const char*)payload,.text_length=(uint32_t)payload_size};
    }
    if (!status)
        status = ud_batch_append(out, values, out->columns);
    if (!status) {
        info.flags |= UD_ROW_ERROR;
        info.operation = 0;
        status = ud_error_rule_token(c, rule, version, &info.rule);
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
