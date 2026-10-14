#include "builtins_internal.h"
typedef struct derive_write {
    ud_expression *expression;
    uint32_t output, input;
    int replace, error, truncation;
} derive_write;
struct ud_derive_plan {
    ud_component_context context;
    ud_batch *schema;
    uint32_t original, count;
    derive_write *writes;
};
static ud_status input_column(const ud_batch *input, ud_string value, uint32_t *out) {
    ud_id id = {0};
    int identity = value.length == 32 && !ud_id_parse(value, &id);
    for (uint32_t j = 0; identity && j < input->columns; j++)
        if (!ud_id_compare(input->column[j].field.identity, id)) {
            *out = j;
            return UD_OK;
        }
    if (!identity)
        for (uint32_t j = 0; j < input->columns; j++)
            if (value.length == input->column[j].field.name_length &&
                !memcmp(value.data, input->column[j].field.name, value.length)) {
                *out = j;
                return UD_OK;
            }
    return UD_SCHEMA;
}
void ud_derive_destroy(ud_derive_plan *plan) {
    if (!plan)
        return;
    for (uint32_t j = 0; plan->writes && j < plan->count; j++)
        ud_expression_destroy(plan->writes[j].expression);
    ud_free(plan->writes);
    ud_batch_destroy(plan->schema);
    ud_free(plan);
}
ud_status ud_derive_bind(const ud_component_context *context, const ud_json *config,
                         const ud_batch *input, ud_derive_plan **out, ud_batch **schema) {
    if (!out || !schema)
        return UD_INVALID;
    *out = NULL;
    *schema = NULL;
    const ud_json *columns = ud_member(config, "columns");
    uint32_t count = ud_json_size(columns);
    if (ud_json_kind(columns) != UD_JSON_ARRAY || !count || count > 65536)
        return UD_SCHEMA;
    ud_derive_plan *plan = ud_alloc(context->memory, sizeof(*plan));
    if (!plan)
        return UD_RESOURCE;
    plan->context = *context;
    plan->count = count;
    plan->original = input->columns;
    plan->writes = ud_alloc(context->memory, count * sizeof(*plan->writes));
    ud_field *fields = ud_alloc(
                 context->memory,
                 (size_t)(input->columns + count > 65536 ? 65536 : input->columns + count) *
                     sizeof(*fields)),
             *derived = ud_alloc(context->memory, count * sizeof(*derived));
    ud_status status = plan->writes && fields && derived ? UD_OK : UD_RESOURCE;
    uint32_t total = input->columns;
    for (uint32_t j = 0; !status && j < total; j++)
        fields[j] = input->column[j].field;
    char scope[33];
    if (!status)
        status = ud_pipeline_field_namespace(context, scope);
    int default_error = UD_ERROR_FAIL;
    if (!status)
        status = ud_error_policy(config, "errorPolicy", UD_ERROR_FAIL, &default_error);
    for (uint32_t j = 0; !status && j < count; j++) {
        const ud_json *column = ud_json_at(columns, j);
        static const char *keys[] = {"name",
                                     "action",
                                     "input",
                                     "inputIdentity",
                                     "expression",
                                     "friendlyExpression",
                                     "errorDisposition",
                                     "truncationDisposition"};
        status = ud_json_keys(column, keys, 8);
        derive_write *write = plan->writes + j;
        ud_string name = {0}, action = {0}, target = {0};
        if (!status)
            status = ud_json_string(ud_member(column, "name"), &name);
        if (!status && ud_member(column, "action"))
            status = ud_json_string(ud_member(column, "action"), &action);
        if (!status && action.length && !ud_named(action, "add") && !ud_named(action, "replace"))
            status = UD_SCHEMA;
        write->replace = ud_named(action, "replace");
        write->input = UINT32_MAX;
        if (!status)
            status = ud_error_policy(column, "errorDisposition", default_error, &write->error);
        if (!status)
            status =
                ud_error_policy(column, "truncationDisposition", default_error, &write->truncation);
        if (!status &&
            (write->error == UD_ERROR_REDIRECT || write->truncation == UD_ERROR_REDIRECT) &&
            !(context->connected_outputs & UINT64_C(2)))
            status = UD_SCHEMA;
        if (!status && write->replace) {
            const ud_json *reference = ud_member(column, "inputIdentity");
            if (!reference)
                reference = ud_member(column, "input");
            if (reference)
                status = ud_json_string(reference, &target);
            else
                target = name;
            if (!status)
                status = input_column(input, target, &write->input);
            write->output = write->input;
            for (uint32_t prior = 0; !status && prior < j; prior++)
                if (plan->writes[prior].replace && plan->writes[prior].input == write->input)
                    status = UD_SCHEMA;
            if (!status && (name.length != input->column[write->input].field.name_length ||
                            memcmp(name.data, input->column[write->input].field.name, name.length)))
                status = UD_SCHEMA;
        } else if (!status) {
            if (total == 65536)
                status = UD_SCHEMA;
            else
                write->output = total++;
        }
        if (!status)
            status = ud_builtin_bind_expr(context, ud_member(column, "expression"), input,
                                          &write->expression);
        if (!status && !write->replace) {
            ud_field result;
            status = ud_expression_field(write->expression, &result);
            if (!status)
                status = ud_field_derive(context->memory, &result, (ud_string){scope, 32}, name,
                                         derived + j);
            if (!status) {
                fields[write->output] = derived[j];
                if (write->error == UD_ERROR_IGNORE)
                    fields[write->output].nullable = 1;
            }
        }
        if (!status && write->replace) {
            ud_field expression = {0};
            status = ud_expression_field(write->expression, &expression);
            ud_field spec = input->schema->fields[write->input];
            spec.physical_identity = (ud_id){0};
            spec.lineage = expression.lineage;
            spec.lineage_count = expression.lineage_count;
            if (!status)
                status = ud_field_copy(context->memory, &spec, (ud_id){0}, derived + j);
            if (!status)
                fields[write->output] = derived[j];
        }
    }
    if (!status)
        status = ud_batch_create(context->memory, fields, total, 0, &plan->schema);
    if (!status)
        status = ud_builtin_clone_schema(context->memory, plan->schema, schema);
    for (uint32_t j = 0; derived && j < count; j++)
        ud_field_clear(derived + j);
    ud_free(derived);
    ud_free(fields);
    if (status)
        ud_derive_destroy(plan);
    else
        *out = plan;
    return status;
}
ud_status ud_derive_rows(ud_derive_plan *plan, const ud_batch *input,
                         const ud_batch *error_schema) {
    if (!plan || !input || !error_schema)
        return UD_INVALID;
    const ud_component_context *context = &plan->context;
    ud_batch *normal = NULL, *errors = NULL;
    ud_status status = ud_batch_create(context->memory, plan->schema->schema->fields,
                                       plan->schema->columns, input->rows, &normal);
    if (!status)
        status = ud_batch_create(context->memory, error_schema->schema->fields,
                                 error_schema->columns, input->rows, &errors);
    ud_value *values = ud_alloc(context->memory, plan->schema->columns * sizeof(*values));
    uint8_t *owned = ud_alloc(context->memory, plan->schema->columns);
    if (!status && (!values || !owned))
        status = UD_RESOURCE;
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        status = ud_activation_check(context->activation);
        memset(values, 0, plan->schema->columns * sizeof(*values));
        memset(owned, 0, plan->schema->columns);
        for (uint32_t col = 0; !status && col < input->columns; col++)
            status = ud_batch_get(input, row, col, values + col);
        int redirected = 0;
        for (uint32_t j = 0; !status && !redirected && j < plan->count; j++) {
            derive_write *write = plan->writes + j;
            ud_value result = {0}, converted = {0};
            ud_field from = {0};
            ud_status record = ud_expression_evaluate(write->expression, input, row, &result);
            if (!record)
                record = ud_expression_field(write->expression, &from);
            const ud_field *to = plan->schema->schema->fields + write->output;
            if (!record)
                record = ud_value_cast_field(context->memory, &from, &result, to,
                                             UD_ROUND_HALF_AWAY, 0, &converted);
            int policy = record == UD_TRUNCATED ? write->truncation : write->error;
            int borrowed = 0;
            if (record && policy == UD_ERROR_IGNORE && ud_record_error_recoverable(record)) {
                if (record == UD_TRUNCATED)
                    record = ud_value_cast_field(context->memory, &from, &result, to,
                                                 UD_ROUND_HALF_AWAY, 1, &converted);
                else if (write->replace) {
                    converted = values[write->output];
                    record = UD_OK;
                    borrowed = 1; /* Borrow original input. */
                } else {
                    converted = (ud_value){.type = to->type,
                                           .is_null = 1,
                                           .precision = to->precision,
                                           .scale = to->scale};
                    record = UD_OK;
                }
            }
            if (!record) {
                values[write->output] = converted;
                owned[write->output] = (uint8_t)!borrowed;
            } else if (policy == UD_ERROR_REDIRECT && ud_record_error_recoverable(record)) {
                status = ud_error_append(&context->errors, errors,
                                         &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                                            .abi_version = UD_ABI_VERSION,
                                                            .input = input,
                                                            .row = row,
                                                            .rule = {"derive", 6},
                                                            .rule_version = 2,
                                                            .column = to->identity,
                                                            .code = record,
                                                            .policy = policy});
                redirected = 1;
                if (!status)
                    ud_task_rows(context->activation, 0, 0, 1);
            } else
                status = record;
        }
        if (!status && !redirected) {
            status = ud_batch_append(normal, values, normal->columns);
            if (!status)
                status = ud_batch_copy_row_info(normal, normal->rows - 1, input, row);
        }
        for (uint32_t col = 0; col < plan->schema->columns; col++)
            if (owned[col])
                ud_clear_value(values + col);
    }
    if (!status && normal->rows)
        status = context->emit(context->emit_user, 0, normal);
    if (!status && errors->rows)
        status = context->emit(context->emit_user, 1, errors);
    ud_free(values);
    ud_free(owned);
    ud_batch_destroy(normal);
    ud_batch_destroy(errors);
    return status;
}
