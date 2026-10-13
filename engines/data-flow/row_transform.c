#include "builtins_internal.h"
ud_status ud_builtin_cast_rows(builtin *b, const ud_batch *input) {
    int policy;
    UD_TRY(ud_error_policy(b->config, "errorPolicy", UD_ERROR_FAIL, &policy));
    uint64_t rounding = 0;
    if (ud_member(b->config, "rounding"))
        UD_TRY(ud_json_uint(ud_member(b->config, "rounding"), &rounding));
    ud_batch *accepted = NULL, *errors = NULL;
    ud_status status =
        ud_batch_create(b->context.memory, b->fields, b->columns, input->rows, &accepted);
    if (!status && policy == UD_ERROR_REDIRECT)
        status = ud_batch_create(b->context.memory, b->errors->schema->fields, b->errors->columns,
                                 input->rows, &errors);
    ud_value *values = ud_alloc(b->context.memory, (size_t)b->columns * sizeof(*values));
    if (!status && !values)
        status = UD_RESOURCE;
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        ud_status conversion = UD_OK;
        uint32_t bad = 0;
        status = ud_activation_check(b->context.activation);
        for (uint32_t col = 0; !status && !conversion && col < b->columns; col++) {
            ud_value original;
            status = ud_batch_get(input, row, col, &original);
            if (!status) {
                conversion = ud_cast_value(b->context.memory, &input->column[col].field, &original,
                                           b->fields + col, (int32_t)rounding, values + col, 0);
                bad = col;
            }
        }
        if (!status && !conversion) {
            status = ud_batch_append(accepted, values, b->columns);
            if (!status)
                status = ud_batch_copy_row_info(accepted, accepted->rows - 1, input, row);
        } else if (!status && (policy == UD_ERROR_FAIL || !ud_record_error_recoverable(conversion)))
            status = conversion;
        else if (!status && policy == UD_ERROR_REDIRECT) {
            status = ud_error_append(&b->context.errors, errors,
                                     &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                                        .abi_version = UD_ABI_VERSION,
                                                        .input = input,
                                                        .row = row,
                                                        .rule = (ud_string){"cast", 4},
                                                        .rule_version = 1,
                                                        .column = input->column[bad].field.identity,
                                                        .code = conversion,
                                                        .policy = policy});
            if (!status)
                ud_task_rows(b->context.activation, 0, 0, 1);
        }
        for (uint32_t col = 0; col < b->columns; col++)
            ud_clear_value(values + col);
    }
    if (!status && accepted->rows)
        status = b->context.emit(b->context.emit_user, 0, accepted);
    if (!status && errors && errors->rows)
        status = b->context.emit(b->context.emit_user, 1, errors);
    ud_free(values);
    ud_batch_destroy(accepted);
    ud_batch_destroy(errors);
    return status;
}

ud_status ud_builtin_transformed(builtin *b, const ud_batch *in) {
    int kind = b->kind;
    ud_status s = UD_OK;
    uint32_t output_count = b->context.output_count;
    if (kind == BALANCED) {
        uint32_t selected = (uint32_t)(b->number % output_count);
        if (b->number == UINT64_MAX)
            return UD_RESOURCE;
        UD_TRY(b->context.emit(b->context.emit_user, selected, in));
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
            } else
                for (uint32_t j = 0; !s && !record_status && j < b->expression_count; j++) {
                    column = b->fields[in->columns + j].identity;
                    record_status = ud_expression_evaluate(b->expressions[j], in, row,
                                                           values + in->columns + j);
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
