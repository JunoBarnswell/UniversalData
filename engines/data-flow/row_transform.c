#include "builtins_internal.h"
/* A single conversion plan is used for configuration and row execution. Policies
 * apply to fields; ignore never silently removes a complete input record. */
ud_status ud_builtin_conversion_policy(const ud_json *config, const ud_batch *input,
                                       uint32_t column, int *error, int *truncation) {
    UD_TRY(ud_error_policy(config, "errorPolicy", UD_ERROR_FAIL, error));
    UD_TRY(ud_error_policy(config, "truncationPolicy", *error, truncation));
    const ud_json *columns = ud_member(config, "columns");
    if (columns && ud_json_kind(columns) != UD_JSON_ARRAY)
        return UD_SCHEMA;
    int matched = 0;
    for (uint32_t j = 0; j < ud_json_size(columns); j++) {
        const ud_json *item = ud_json_at(columns, j);
        static const char *keys[] = {"input", "errorDisposition", "truncationDisposition"};
        UD_TRY(ud_json_keys(item, keys, 3));
        ud_string name = {0};
        UD_TRY(ud_json_string(ud_member(item, "input"), &name));
        int at = -1;
        ud_id id = {0};
        int identity = name.length == 32 && !ud_id_parse(name, &id);
        for (uint32_t k = 0; k < input->columns; k++) {
            const ud_field *f = input->schema->fields + k;
            if (identity
                    ? !ud_id_compare(f->identity, id)
                    : (name.length == f->name_length && !memcmp(name.data, f->name, name.length)))
                at = (int)k;
        }
        if (at < 0)
            return UD_SCHEMA;
        int ep, tp;
        UD_TRY(ud_error_policy(item, "errorDisposition", *error, &ep));
        UD_TRY(ud_error_policy(item, "truncationDisposition", *truncation, &tp));
        if ((uint32_t)at == column) {
            if (matched++)
                return UD_SCHEMA;
            *error = ep;
            *truncation = tp;
        }
    }
    return UD_OK;
}
ud_status ud_builtin_conversion_schema(const ud_component_context *context, const ud_json *config,
                                       const ud_batch *input, ud_batch **out) {
    ud_batch *bound = *out;
    ud_field *fields = ud_alloc(context->memory, bound->columns * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (uint32_t j = 0; !status && j < bound->columns; j++) {
        fields[j] = bound->schema->fields[j];
        int ep, tp;
        status = ud_builtin_conversion_policy(config, input, j, &ep, &tp);
        if (!status && (ep == UD_ERROR_IGNORE || tp == UD_ERROR_IGNORE))
            fields[j].nullable = 1;
        if (!status && (ep == UD_ERROR_REDIRECT || tp == UD_ERROR_REDIRECT) &&
            !(context->connected_outputs & UINT64_C(2)))
            status = UD_SCHEMA;
    }
    ud_batch *replacement = NULL;
    if (!status)
        status = ud_batch_create(context->memory, fields, bound->columns, 0, &replacement);
    ud_free(fields);
    if (!status) {
        *out = replacement;
        ud_batch_destroy(bound);
    }
    return status;
}
ud_status ud_builtin_cast_rows(builtin *b, const ud_batch *input) {
    uint64_t rounding = 0;
    if (ud_member(b->config, "rounding"))
        UD_TRY(ud_json_uint(ud_member(b->config, "rounding"), &rounding));
    ud_batch *accepted = NULL, *errors = NULL;
    ud_status status =
        ud_batch_create(b->context.memory, b->fields, b->columns, input->rows, &accepted);
    if (!status)
        status = ud_batch_create(b->context.memory, b->errors->schema->fields, b->errors->columns,
                                 input->rows, &errors);
    ud_value *values = ud_alloc(b->context.memory, b->columns * sizeof(*values));
    int *policies = ud_alloc(b->context.memory, b->columns * 2 * sizeof(*policies));
    if (!status && (!values || !policies))
        status = UD_RESOURCE;
    for (uint32_t col = 0; !status && col < b->columns; col++)
        status = ud_builtin_conversion_policy(b->config, input, col, policies + col * 2,
                                              policies + col * 2 + 1);
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        status = ud_activation_check(b->context.activation);
        int redirected = 0;
        for (uint32_t col = 0; !status && !redirected && col < b->columns; col++) {
            ud_value original = {0};
            status = ud_batch_get(input, row, col, &original);
            ud_status conversion = status;
            if (!conversion)
                conversion =
                    ud_value_cast_field(b->context.memory, input->schema->fields + col, &original,
                                        b->fields + col, (int32_t)rounding, 0, values + col);
            int policy = policies[col * 2 + (conversion == UD_TRUNCATED)];
            if (!status && conversion && ud_record_error_recoverable(conversion) &&
                policy == UD_ERROR_IGNORE) {
                if (conversion == UD_TRUNCATED)
                    conversion = ud_value_cast_field(b->context.memory, input->schema->fields + col,
                                                     &original, b->fields + col, (int32_t)rounding,
                                                     1, values + col);
                else {
                    values[col] = (ud_value){.type = b->fields[col].type,
                                             .is_null = 1,
                                             .precision = b->fields[col].precision,
                                             .scale = b->fields[col].scale};
                    conversion = UD_OK;
                }
            }
            if (!status && conversion) {
                if (!ud_record_error_recoverable(conversion) || policy != UD_ERROR_REDIRECT)
                    status = conversion;
                else {
                    status = ud_error_append(
                        &b->context.errors, errors,
                        &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                           .abi_version = UD_ABI_VERSION,
                                           .input = input,
                                           .row = row,
                                           .rule = {"cast", 4},
                                           .rule_version = 2,
                                           .column = input->schema->fields[col].identity,
                                           .code = conversion,
                                           .policy = policy});
                    redirected = 1;
                    if (!status)
                        ud_task_rows(b->context.activation, 0, 0, 1);
                }
            }
        }
        if (!status && !redirected) {
            status = ud_batch_append(accepted, values, b->columns);
            if (!status)
                status = ud_batch_copy_row_info(accepted, accepted->rows - 1, input, row);
        }
        for (uint32_t col = 0; col < b->columns; col++)
            ud_clear_value(values + col);
    }
    if (!status && accepted->rows)
        status = b->context.emit(b->context.emit_user, 0, accepted);
    if (!status && errors->rows)
        status = b->context.emit(b->context.emit_user, 1, errors);
    ud_free(values);
    ud_free(policies);
    ud_batch_destroy(accepted);
    ud_batch_destroy(errors);
    return status;
}

ud_status ud_builtin_transformed(builtin *b, const ud_batch *in) {
    int kind = b->kind;
    if (kind == DERIVE)
        return ud_derive_rows(b->derive, in, b->errors);
    ud_status s = UD_OK;
    uint32_t output_count = b->context.output_count;
    if (kind == BALANCED) {
        uint32_t selected = (uint32_t)(b->number % output_count);
        if (b->number == UINT64_MAX)
            return UD_RESOURCE;
        UD_TRY(b->context.emit(b->context.emit_user, selected, (ud_batch *)in));
        b->number++;
        return UD_OK;
    }
    ud_batch **output = ud_alloc(b->context.memory, (output_count + 1) * sizeof(*output));
    if (!output)
        return UD_RESOURCE;
    if (kind == PASS || kind == MULTICAST) {
        for (uint32_t p = 0; !s && p < output_count; p++)
            s = b->context.emit(b->context.emit_user, p, (ud_batch *)in);
        ud_free(output);
        return s;
    }
    if (kind == UNION_ALL) {
        uint32_t *indices = ud_alloc(b->context.memory, b->columns * sizeof(*indices));
        if (!indices) {
            ud_free(output);
            return UD_RESOURCE;
        }
        for (uint32_t j = 0; j < b->columns; j++)
            indices[j] = j;
        s = ud_batch_project(b->context.memory, in, indices, b->fields, b->columns, output);
        ud_free(indices);
        if (!s)
            s = b->context.emit(b->context.emit_user, 0, output[0]);
        ud_batch_destroy(output[0]);
        ud_free(output);
        return s;
    }
    if (kind == CAST) {
        ud_free(output);
        return ud_builtin_cast_rows(b, in);
    }
    int policy = UD_ERROR_FAIL;
    s = ud_error_policy(b->config, "errorPolicy", UD_ERROR_FAIL, &policy);
    uint32_t error_port = output_count - 1;
    {
        for (uint32_t p = 0; !s && p < output_count; p++)
            s = p == error_port ? ud_batch_create(b->context.memory, b->errors->schema->fields,
                                                  b->errors->columns, in->rows, output + p)
                                : ud_batch_create(b->context.memory, b->fields, b->columns,
                                                  in->rows, output + p);
        ud_value *values = ud_alloc(b->context.memory, b->columns * sizeof(*values));
        if (!values && !s)
            s = UD_RESOURCE;
        for (uint32_t row = 0; !s && row < in->rows; row++) {
            s = ud_activation_check(b->context.activation);
            for (uint32_t j = 0; !s && j < in->columns; j++)
                s = ud_batch_get(in, row, j, values + j);
            uint32_t port = 0;
            ud_status record_status = UD_OK;
            ud_id column = {0};
            if (kind == SPLIT) {
                port = b->expression_count;
                for (uint32_t p = 0; !s && !record_status && p < b->expression_count; p++) {
                    ud_value value = {0};
                    record_status = ud_expression_evaluate(b->expressions[p], in, row, &value);
                    if (!record_status && !value.is_null && value.integer) {
                        port = p;
                        break;
                    }
                }
            }
            if (!s && record_status) {
                if (policy == UD_ERROR_FAIL || !ud_record_error_recoverable(record_status))
                    s = record_status;
                else if (policy == UD_ERROR_REDIRECT)
                    s = ud_error_append(
                        &b->context.errors, output[error_port],
                        &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                           .abi_version = UD_ABI_VERSION,
                                           .input = in,
                                           .row = row,
                                           .rule = kind == SPLIT
                                                       ? (ud_string){"split.condition", 15}
                                                       : (ud_string){"derive", 6},
                                           .rule_version = 1,
                                           .column = column,
                                           .code = record_status,
                                           .policy = policy});
                continue;
            }
            if (!s)
                s = ud_batch_append(output[port], values, b->columns);
            if (!s)
                s = ud_batch_copy_row_info(output[port], output[port]->rows - 1, in, row);
        }
        ud_free(values);
    }
    for (uint32_t i = 0; !s && i < output_count; i++)
        if (output[i] && output[i]->rows)
            s = b->context.emit(b->context.emit_user, i, output[i]);
    for (uint32_t p = 0; p < output_count; p++)
        ud_batch_destroy(output[p]);
    ud_free(output);
    return s;
}

ud_status ud_builtin_blocking(builtin *b) {
    ud_batch *output = NULL;
    ud_status s = ud_operator_pull(b->operator, & output, NULL);
    if (!s)
        s = b->context.emit(b->context.emit_user, 0, output);
    ud_batch_destroy(output);
    return s == UD_BUSY ? UD_OK : s ? s : UD_MORE;
}
