#include "../package/internal.h"
ud_status ud_error_policy(const ud_json *config, const char *key, int fallback, int *out) {
    *out = fallback;
    const ud_json *value = ud_member(config, key);
    if (!value)
        return UD_OK;
    ud_string text;
    UD_TRY(ud_json_string(value, &text));
    *out = ud_named(text, "fail")       ? UD_ERROR_FAIL
           : ud_named(text, "redirect") ? UD_ERROR_REDIRECT
           : ud_named(text, "ignore")   ? UD_ERROR_IGNORE
                                        : -1;
    return *out < 0 ? UD_SCHEMA : UD_OK;
}
ud_status ud_error_schema(const ud_component_context *c, const ud_batch *input, int include,
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
    status = ud_pipeline_field_namespace(c, scope);
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
                                .type = UD_STRUCT,
                                .children = input->schema->fields,
                                .child_count = input->columns};
        status = ud_id_make(c->memory, (ud_string){scope, 32}, (ud_string){"input", 5},
                            &fields[10].identity);
    }
    if (!status)
        status = ud_batch_create(c->memory, fields, n, 0, out);
    ud_free(fields);
    return status;
}
ud_status ud_error_append(const ud_component_context *c, ud_batch *out, const ud_batch *input,
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
    ud_string text[] = {c->execution->options.execution_id,
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
    ud_value *payload = NULL;
    if (out->columns == 11) {
        payload = ud_alloc(c->memory, (size_t)input->columns * sizeof(*payload));
        if (!payload)
            status = UD_RESOURCE;
        for (uint32_t i = 0; !status && i < input->columns; i++)
            status = ud_batch_get(input, row, i, payload + i);
        values[10] =
            (ud_value){.type = UD_STRUCT, .children = payload, .child_count = input->columns};
    }
    if (!status)
        status = ud_batch_append(out, values, out->columns);
    if (!status) {
        info.flags |= UD_ROW_ERROR;
        status = ud_rule_token(c, rule, version, &info.rule);
        if (!status)
            status = ud_batch_set_row_info(out, out->rows - 1, &info);
    }
    ud_free(payload);
    ud_free(values);
    return status;
}
