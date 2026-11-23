#include "../package/internal.h"

#define ANALYSIS_MAX_FIELDS 64
#define ANALYSIS_CELL_BYTES 4096
#define ANALYSIS_MAX_ISSUES 8
#define ANALYSIS_DEFAULT_MAX_ROWS 100000
#define ANALYSIS_ABSOLUTE_MAX_ROWS 30000000

enum analysis_role {
    ANALYSIS_TEXT,
    ANALYSIS_CATEGORY,
    ANALYSIS_IDENTIFIER,
    ANALYSIS_DATE,
    ANALYSIS_AMOUNT,
    ANALYSIS_CURRENCY
};

typedef struct analysis_column {
    uint32_t source;
    enum analysis_role role;
    ud_field field;
} analysis_column;

typedef struct analysis_model {
    ud_component_context context;
    ud_json *config;
    const ud_batch *input, *output, *summary;
    analysis_column columns[ANALYSIS_MAX_FIELDS];
    uint32_t count, date_column;
    uint64_t rows, affected;
    uint32_t issue_count;
    struct { uint64_t row; uint32_t column; } issues[ANALYSIS_MAX_ISSUES];
    ud_string date_format, currency;
    int32_t offset_minutes;
    int decimal_point, grouping, allow_empty_amount;
    uint64_t max_rows;
} analysis_model;

static const ud_field summary_fields[] = {
    {.name = "scannedRows", .name_length = 11, .type = UD_UINT64},
    {.name = "affectedRows", .name_length = 12, .type = UD_UINT64},
    {.name = "issues", .name_length = 6, .type = UD_UTF8},
    {.name = "schema", .name_length = 6, .type = UD_UTF8}};

static ud_status option(const ud_json *config, const char *key, ud_string fallback, ud_string *out) {
    const ud_json *value = ud_member(config, key);
    *out = fallback;
    return value ? ud_json_string(value, out) : UD_OK;
}

static ud_status bind(analysis_model *model, const ud_batch *input, ud_batch **outputs) {
    static const char *keys[] = {"fields", "dateFormat", "offsetMinutes", "amountFormat",
                                 "currency", "allowEmptyAmount", "maxRows"};
    UD_TRY(ud_json_keys(model->config, keys, sizeof(keys) / sizeof(*keys)));
    const ud_json *fields = ud_member(model->config, "fields");
    model->count = ud_json_size(fields);
    if (!input || ud_json_kind(fields) != UD_JSON_ARRAY || !model->count ||
        model->count > ANALYSIS_MAX_FIELDS)
        return UD_SCHEMA;
    model->input = input;
    model->date_column = UINT32_MAX;
    UD_TRY(option(model->config, "dateFormat", (ud_string){"iso", 3}, &model->date_format));
    if (!ud_named(model->date_format, "iso") && !ud_named(model->date_format, "dmy") &&
        !ud_named(model->date_format, "mdy") && !ud_named(model->date_format, "timestamp"))
        return UD_SCHEMA;
    ud_string format;
    UD_TRY(option(model->config, "amountFormat", (ud_string){"point", 5}, &format));
    if (ud_named(format, "point") || ud_named(format, "pointGrouped")) {
        model->decimal_point = '.';
        model->grouping = ud_named(format, "pointGrouped") ? ',' : 0;
    } else if (ud_named(format, "comma") || ud_named(format, "commaGrouped")) {
        model->decimal_point = ',';
        model->grouping = ud_named(format, "commaGrouped") ? '.' : 0;
    } else
        return UD_SCHEMA;
    UD_TRY(option(model->config, "currency", (ud_string){0}, &model->currency));
    if (model->currency.length && model->currency.length != 3)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < model->currency.length; i++)
        if (model->currency.data[i] < 'A' || model->currency.data[i] > 'Z')
            return UD_SCHEMA;
    model->allow_empty_amount = 1;
    const ud_json *missing = ud_member(model->config, "allowEmptyAmount");
    if (missing) {
        ud_string value;
        if (ud_json_kind(missing) != UD_JSON_BOOL)
            return UD_SCHEMA;
        UD_TRY(ud_json_text(missing, &value.data, &value.length));
        model->allow_empty_amount = ud_named(value, "true");
    }
    const ud_json *offset = ud_member(model->config, "offsetMinutes");
    if (offset) {
        ud_string text;
        ud_value value;
        UD_TRY(ud_json_text(offset, &text.data, &text.length));
        UD_TRY(ud_value_parse(model->context.memory, UD_INT64, text.data, text.length, &value));
        int64_t minutes = value.integer;
        if (minutes < -840 || minutes > 840)
            return UD_SCHEMA;
        model->offset_minutes = (int32_t)minutes;
    }
    model->max_rows = ANALYSIS_DEFAULT_MAX_ROWS;
    const ud_json *maximum_rows = ud_member(model->config, "maxRows");
    if (maximum_rows) {
        ud_string text;
        ud_value value;
        if (ud_json_kind(maximum_rows) != UD_JSON_NUMBER)
            return UD_SCHEMA;
        UD_TRY(ud_json_text(maximum_rows, &text.data, &text.length));
        UD_TRY(ud_value_parse(model->context.memory, UD_UINT64, text.data, text.length, &value));
        if (!value.unsigned_integer || value.unsigned_integer > ANALYSIS_ABSOLUTE_MAX_ROWS)
            return UD_SCHEMA;
        model->max_rows = value.unsigned_integer;
    }

    ud_field schema[ANALYSIS_MAX_FIELDS + 2];
    uint64_t used = 0;
    if (input->columns > ANALYSIS_MAX_FIELDS)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < model->count; i++) {
        const ud_json *definition = ud_json_at(fields, i);
        static const char *members[] = {"source", "id", "role"};
        UD_TRY(ud_json_keys(definition, members, 3));
        ud_string source, id, role;
        UD_TRY(ud_json_string(ud_member(definition, "source"), &source));
        UD_TRY(ud_json_string(ud_member(definition, "id"), &id));
        UD_TRY(ud_json_string(ud_member(definition, "role"), &role));
        uint32_t at = 0;
        while (at < input->columns &&
               !ud_same(source, (ud_string){input->column[at].field.name,
                                           input->column[at].field.name_length}))
            at++;
        if (at == input->columns || used & (UINT64_C(1) << at) || id.length != 33 || id.data[0] != 'f')
            return UD_SCHEMA;
        used |= UINT64_C(1) << at;
        ud_id identity;
        UD_TRY(ud_id_parse((ud_string){id.data + 1, id.length - 1}, &identity));
        analysis_column *column = model->columns + i;
        column->source = at;
        column->field = (ud_field){.name = id.data, .name_length = id.length, .nullable = 1,
                                   .identity = identity, .type = UD_UTF8};
        if (ud_named(role, "date")) {
            if (model->date_column != UINT32_MAX)
                return UD_SCHEMA;
            model->date_column = i;
            column->role = ANALYSIS_DATE;
            column->field.type = UD_DATE32;
            column->field.nullable = 0;
        } else if (ud_named(role, "amount")) {
            if (!model->currency.length)
                return UD_SCHEMA;
            column->role = ANALYSIS_AMOUNT;
            column->field.type = UD_DECIMAL128;
            column->field.precision = 28;
            column->field.scale = 2;
            column->field.nullable = model->allow_empty_amount;
        } else if (ud_named(role, "identifier")) {
            column->role = ANALYSIS_IDENTIFIER;
            column->field.nullable = 0;
        } else if (ud_named(role, "currency")) {
            if (!model->currency.length)
                return UD_SCHEMA;
            column->role = ANALYSIS_CURRENCY;
            column->field.nullable = 0;
        } else if (ud_named(role, "category"))
            column->role = ANALYSIS_CATEGORY;
        else if (!ud_named(role, "text"))
            return UD_SCHEMA;
        schema[i] = column->field;
    }
    uint32_t count = model->count;
    if (model->date_column != UINT32_MAX)
        schema[count++] = (ud_field){.name = "calendarMonth", .name_length = 13, .type = UD_DATE32};
    schema[count++] = (ud_field){.name = "rowKey", .name_length = 6, .type = UD_INT64};
    if (outputs) {
        UD_TRY(ud_batch_create(model->context.memory, schema, count, 0, outputs));
        return ud_batch_create(model->context.memory, summary_fields, 4, 0, outputs + 1);
    }
    return UD_OK;
}

static ud_status configure(void *user, const ud_component_context *context, ud_string config,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    (void)user;
    analysis_model model = {.context = *context};
    UD_TRY(ud_json_parse(context->memory, config.data, config.length, &model.config, NULL));
    ud_status status = bind(&model, inputs[0], outputs);
    ud_json_destroy(model.config);
    return status;
}

static void dispose(void *instance, ud_status status) {
    (void)status;
    analysis_model *model = instance;
    if (model) {
        ud_json_destroy(model->config);
        ud_free(model);
    }
}

static ud_status prepare(void *user, const ud_component_context *context, ud_string config,
                         const ud_batch *const *inputs, const ud_batch *const *outputs, void **instance) {
    (void)user;
    *instance = NULL;
    analysis_model *model = ud_alloc(context->memory, sizeof(*model));
    if (!model)
        return UD_RESOURCE;
    model->context = *context;
    model->output = outputs[0];
    model->summary = outputs[1];
    ud_status status = ud_json_parse(context->memory, config.data, config.length, &model->config, NULL);
    if (!status)
        status = bind(model, inputs[0], NULL);
    if (status)
        dispose(model, status);
    else
        *instance = model;
    return status;
}

static ud_string trim(ud_string value) {
    while (value.length && (value.data[0] == ' ' || value.data[0] == '\t')) {
        value.data++;
        value.length--;
    }
    while (value.length && (value.data[value.length - 1] == ' ' || value.data[value.length - 1] == '\t'))
        value.length--;
    return value;
}

static int digit(char value) {
    return value >= '0' && value <= '9';
}

static ud_status decimal_text(analysis_model *model, ud_string text, ud_value *out) {
    char digits[48];
    uint32_t at = 0, used = 0, group_digits = 0, fraction = 0;
    int decimal = 0, grouped = 0;
    if (text.length && (text.data[0] == '+' || text.data[0] == '-')) {
        if (text.data[0] == '-')
            digits[used++] = '-';
        at++;
    }
    for (; at < text.length; at++) {
        char c = text.data[at];
        if (used >= sizeof(digits) - 1)
            return UD_PARSE;
        if (digit(c)) {
            digits[used++] = c;
            if (decimal)
                fraction++;
            else
                group_digits++;
        } else if (c == model->decimal_point && !decimal && group_digits &&
                   (!grouped || group_digits == 3)) {
            digits[used++] = '.';
            decimal = 1;
        } else if (model->grouping && c == model->grouping && !decimal &&
                   group_digits && (grouped ? group_digits == 3 : group_digits <= 3)) {
            grouped = 1;
            group_digits = 0;
        } else
            return UD_PARSE;
    }
    if (!group_digits || (grouped && group_digits != 3) || (decimal && !fraction))
        return UD_PARSE;
    return ud_decimal_parse(digits, used, 28, 2, UD_ROUND_EXACT, &out->decimal);
}

static ud_status date_value(analysis_model *model, const ud_field *field, const ud_value *input,
                            ud_string text, ud_value *out) {
    ud_context *ctx = model->context.memory;
    if (input->type == UD_DATE32) {
        out->integer = input->integer;
        return UD_OK;
    }
    int timestamp = input->type == UD_TIMESTAMP || input->type == UD_DATETIME ||
                    ud_named(model->date_format, "timestamp");
    ud_field date = {.type = UD_DATE32};
    if (timestamp) {
        ud_value instant;
        ud_field utc = {.type = UD_TIMESTAMP, .timezone = "UTC", .timezone_length = 3};
        ud_status status = input->type == UD_UTF8
            ? ud_value_parse_field(ctx, &utc, text.data, text.length, UD_ROUND_EXACT, &instant)
            : ud_value_cast_field(ctx, field, input, &utc, UD_ROUND_EXACT, 0, &instant);
        if (status)
            return status;
        int64_t adjustment = (int64_t)model->offset_minutes * INT64_C(60000000);
        if ((adjustment > 0 && instant.integer > INT64_MAX - adjustment) ||
            (adjustment < 0 && instant.integer < INT64_MIN - adjustment))
            return UD_OVERFLOW;
        int64_t local = instant.integer + adjustment;
        out->integer = local / INT64_C(86400000000);
        if (local % INT64_C(86400000000) < 0)
            out->integer--;
        return UD_OK;
    }
    if (text.length != 10)
        return UD_PARSE;
    if (ud_named(model->date_format, "iso"))
        return text.data[4] == '-' && text.data[7] == '-'
            ? ud_value_parse_field(ctx, &date, text.data, text.length, UD_ROUND_EXACT, out)
            : UD_PARSE;
    if (text.data[2] != '/' || text.data[5] != '/')
        return UD_PARSE;
    uint32_t month = ud_named(model->date_format, "dmy") ? 3 : 0;
    uint32_t day = month ? 0 : 3;
    char iso[] = {text.data[6], text.data[7], text.data[8], text.data[9], '-',
                  text.data[month], text.data[month + 1], '-', text.data[day], text.data[day + 1]};
    return ud_value_parse_field(ctx, &date, iso, sizeof(iso), UD_ROUND_EXACT, out);
}

static ud_status normalize(analysis_model *model, const analysis_column *column, const ud_value *input,
                           char *buffer, ud_value *out) {
    if (input->is_absent)
        return UD_QUALITY;
    *out = (ud_value){.type = column->field.type, .precision = column->field.precision,
                      .scale = column->field.scale};
    ud_string text = {0};
    const ud_field *source = &model->input->column[column->source].field;
    if (!input->is_null) {
        if (input->type == UD_UTF8)
            text = (ud_string){input->text, input->text_length};
        else if (column->role == ANALYSIS_TEXT || column->role == ANALYSIS_CATEGORY ||
                 column->role == ANALYSIS_IDENTIFIER) {
            uint32_t size;
            UD_TRY(ud_value_format(input, buffer, 128, &size));
            text = (ud_string){buffer, size - 1};
        }
    }
    ud_string trimmed = trim(text);
    if (input->is_null || (input->type == UD_UTF8 && !trimmed.length)) {
        if (!column->field.nullable)
            return UD_QUALITY;
        out->is_null = 1;
        return UD_OK;
    }
    if (text.length > ANALYSIS_CELL_BYTES)
        return UD_QUALITY;
    if (column->role == ANALYSIS_DATE)
        return date_value(model, source, input, trimmed, out);
    if (column->role == ANALYSIS_AMOUNT) {
        if (input->type == UD_UTF8)
            return decimal_text(model, trimmed, out);
        if (input->type != UD_DECIMAL128 && input->type != UD_INT64 && input->type != UD_UINT64)
            return UD_QUALITY;
        return ud_value_cast_field(model->context.memory, source, input, &column->field,
                                   UD_ROUND_EXACT, 0, out);
    }
    if (column->role == ANALYSIS_CURRENCY && !ud_same(text, model->currency))
        return UD_QUALITY;
    out->text = text.data;
    out->text_length = text.length;
    return UD_OK;
}

static ud_status summary(analysis_model *model) {
    ud_context *ctx = model->context.memory;
    char facts[512];
    int length = snprintf(facts, sizeof(facts), "[");
    for (uint32_t i = 0; i < model->issue_count; i++) {
        length += snprintf(facts + length, sizeof(facts) - (size_t)length,
                           "%s{\"row\":%llu,\"column\":%u}", i ? "," : "",
                           (unsigned long long)model->issues[i].row, model->issues[i].column);
    }
    facts[length++] = ']';
    char *schema = NULL;
    uint32_t schema_length;
    UD_TRY(ud_schema_to_json(model->output, &schema, &schema_length));
    ud_value values[] = {
        {.type = UD_UINT64, .unsigned_integer = model->rows},
        {.type = UD_UINT64, .unsigned_integer = model->affected},
        {.type = UD_UTF8, .text = facts, .text_length = (uint32_t)length},
        {.type = UD_UTF8, .text = schema, .text_length = schema_length}};
    ud_batch *batch = NULL;
    ud_status status = ud_batch_create(ctx, model->summary->schema->fields, 4, 1, &batch);
    if (!status)
        status = ud_batch_append(batch, values, 4);
    ud_free(schema);
    if (!status)
        status = ud_batch_seal(batch);
    if (!status)
        status = model->context.emit(model->context.emit_user, 1, batch);
    ud_batch_destroy(batch);
    return status;
}

static ud_status step(void *instance, const ud_component_event *event) {
    analysis_model *model = instance;
    if (event->kind == UD_COMPONENT_POLL)
        return UD_END;
    if (event->kind == UD_COMPONENT_INPUT_END)
        return summary(model);
    if (event->kind != UD_COMPONENT_DATA)
        return UD_OK;
    ud_context *ctx = model->context.memory;
    const ud_batch *input = event->batch;
    if (model->rows > UINT64_MAX - input->rows)
        return ud_fail(ctx, UD_OVERFLOW, "Analysis row count overflow");
    if (model->rows > model->max_rows || input->rows > model->max_rows - model->rows)
        return ud_fail(ctx, UD_RESOURCE, "Analysis exceeds configured row limit");
    ud_batch *output = NULL;
    UD_TRY(ud_batch_create(ctx, model->output->schema->fields, model->output->columns,
                           input->rows, &output));
    ud_status status = UD_OK;
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        model->rows++;
        ud_value values[ANALYSIS_MAX_FIELDS + 2];
        char buffers[ANALYSIS_MAX_FIELDS][128];
        int invalid = 0;
        for (uint32_t i = 0; !status && i < model->count; i++) {
            const analysis_column *column = &model->columns[i];
            ud_value input_value;
            status = ud_batch_get(input, row, column->source, &input_value);
            if (!status)
                status = normalize(model, column, &input_value, buffers[i], &values[i]);
            if (status && (ud_record_error_recoverable(status) || status == UD_QUALITY)) {
                if (model->issue_count < ANALYSIS_MAX_ISSUES) {
                    model->issues[model->issue_count].row = model->rows;
                    model->issues[model->issue_count++].column = column->source;
                }
                invalid = 1;
                status = UD_OK;
            }
        }
        if (status)
            break;
        if (invalid) {
            model->affected++;
            continue;
        }
        uint32_t length = model->count;
        if (model->date_column != UINT32_MAX) {
            char iso[32];
            uint32_t written;
            status = ud_value_format(&values[model->date_column], iso, sizeof(iso), &written);
            if (!status && written == 11) {
                iso[8] = '0';
                iso[9] = '1';
                status = ud_value_parse_field(ctx, &model->output->schema->fields[length],
                                              iso, 10, UD_ROUND_EXACT, &values[length]);
                length++;
            } else if (!status)
                status = UD_PARSE;
        }
        values[length++] = (ud_value){.type = UD_INT64, .integer = (int64_t)model->rows};
        if (!status)
            status = ud_batch_append(output, values, length);
    }
    if (!status && output->rows) {
        status = ud_batch_seal(output);
        if (!status)
            status = model->context.emit(model->context.emit_user, 0, output);
    }
    ud_batch_destroy(output);
    return status;
}

static ud_status finish(void *instance) {
    (void)instance;
    return UD_OK;
}

ud_status ud_analysis_component_register(ud_registry *registry) {
    static const char configuration[] =
        "{\"type\":\"object\",\"required\":[\"fields\"],\"properties\":{"
        "\"fields\":{\"type\":\"array\"},\"dateFormat\":{\"type\":\"string\"},"
        "\"offsetMinutes\":{\"type\":\"integer\"},\"amountFormat\":{\"type\":\"string\"},"
        "\"currency\":{\"type\":\"string\"},\"allowEmptyAmount\":{\"type\":\"boolean\"},"
        "\"maxRows\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":30000000,\"default\":100000}},"
        "\"additionalProperties\":false}";
    const ud_component_descriptor descriptor = {
        .struct_size = sizeof(descriptor),
        .abi_version = UD_ABI_VERSION,
        .id = {"analysis.prepare", 16},
        .name = {"Prepare analysis model", 22},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
        .configuration_schema = {configuration, sizeof(configuration) - 1},
        .category = UD_COMPONENT_TRANSFORM,
        .capabilities = UD_COMPONENT_REPLAY_SAFE,
        .inputs = 1,
        .outputs = 2,
        .synchronous_input = -1,
        .error_output = -1,
        .configure = configure,
        .prepare = prepare,
        .step = step,
        .finish = finish,
        .dispose = dispose};
    return ud_registry_register(registry, &descriptor);
}
