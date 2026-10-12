#include "ud/component.h"
#include "../../data/internal.h"
#include <math.h>

enum { ABS, LENGTH, TRIM, UPPER, LOWER, CONCAT, COALESCE, SUBSTRING };
static ud_status bind_function(void *user, const ud_field *args, uint32_t count, ud_field *out) {
    int kind = (int)(intptr_t)user;
    *out = args[0];
    if (kind == ABS) {
        if (args[0].type != UD_INT64 && args[0].type != UD_FLOAT64 && args[0].type != UD_DECIMAL128)
            return UD_SCHEMA;
    } else if (kind == COALESCE) {
        out->nullable = 1;
        for (uint32_t i = 0; i < count; i++) {
            if (!ud_field_matches(args, args + i, UD_FIELD_VALUE_TYPE))
                return UD_SCHEMA;
            if (!args[i].nullable)
                out->nullable = 0;
        }
    } else {
        if (args[0].type != UD_UTF8)
            return UD_SCHEMA;
        if (kind == LENGTH)
            *out = (ud_field){.type = UD_INT64, .nullable = args[0].nullable};
        if (kind == CONCAT && args[1].type != UD_UTF8)
            return UD_SCHEMA;
        if (kind == SUBSTRING && (args[1].type != UD_INT64 || args[2].type != UD_INT64))
            return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status evaluate_function(void *user, ud_context *ctx, const ud_value *args,
                                   uint32_t count, const ud_field *field, ud_value *out) {
    int kind = (int)(intptr_t)user;
    memset(out, 0, sizeof(*out));
    out->type = field->type;
    if (kind == COALESCE) {
        for (uint32_t i = 0; i < count; i++)
            if (!args[i].is_null)
                return ud_copy_value(ctx, args + i, out, 0);
        out->is_null = 1;
        return UD_OK;
    }
    for (uint32_t i = 0; i < count; i++)
        if (args[i].is_null) {
            out->is_null = 1;
            return UD_OK;
        }
    if (kind == ABS) {
        ud_value zero = {.type = field->type, .precision = field->precision, .scale = field->scale};
        if (ud_value_compare(args, &zero) >= 0)
            return ud_copy_value(ctx, args, out, 0);
        return ud_value_numeric(UD_NUMERIC_SUBTRACT, &zero, args, field, out);
    }
    const char *text = args[0].text;
    uint32_t length = args[0].text_length;
    if (kind == LENGTH) {
        /* UTF8 code-point count; embedded NUL is a valid character. */
        for (uint32_t i = 0; i < length; i++)
            if (((unsigned char)text[i] & 0xc0) != 0x80)
                out->integer++;
        return UD_OK;
    }
    if (kind == TRIM) {
        while (length && (text[0] == ' ' || text[0] == '\t' || text[0] == '\r' || text[0] == '\n'))
            text++, length--;
        while (length && (text[length - 1] == ' ' || text[length - 1] == '\t' ||
                          text[length - 1] == '\r' || text[length - 1] == '\n'))
            length--;
    }
    if (kind == SUBSTRING) {
        if (args[1].integer < 0 || args[2].integer < 0)
            return UD_INVALID;
        uint64_t skip = (uint64_t)args[1].integer, take = (uint64_t)args[2].integer;
        uint32_t begin = length, end = length;
        uint64_t position = 0;
        for (uint32_t i = 0; i < length; i++)
            if (((unsigned char)text[i] & 0xc0) != 0x80) {
                if (position == skip)
                    begin = i;
                if (position >= skip && position - skip == take) {
                    end = i;
                    break;
                }
                position++;
            }
        text += begin;
        length = end - begin;
    }
    uint64_t total = length;
    if (kind == CONCAT)
        total += args[1].text_length;
    if (total > UINT32_MAX)
        return UD_RESOURCE;
    char *owned = ud_alloc(ctx, (size_t)total + 1);
    if (!owned)
        return UD_RESOURCE;
    if (length)
        memcpy(owned, text, length);
    if (kind == CONCAT && args[1].text_length)
        memcpy(owned + length, args[1].text, args[1].text_length);
    for (uint32_t i = 0; i < total; i++) {
        if (kind == UPPER && owned[i] >= 'a' && owned[i] <= 'z')
            owned[i] -= 'a' - 'A';
        if (kind == LOWER && owned[i] >= 'A' && owned[i] <= 'Z')
            owned[i] += 'a' - 'A';
    }
    owned[total] = 0;
    out->text = owned;
    out->text_length = (uint32_t)total;
    return UD_OK;
}
ud_status ud_expression_register_functions(ud_registry *registry) {
    static const char *ids[] = {"abs",         "length", "trim",     "upper_ascii",
                                "lower_ascii", "concat", "coalesce", "substring"};
    ud_component_descriptor descriptors[8] = {{0}};
    for (uint32_t i = 0; i < 8; i++) {
        ud_component_descriptor *d = descriptors + i;
        d->struct_size = sizeof(*d);
        d->abi_version = UD_ABI_VERSION;
        d->id = d->name = (ud_string){ids[i], (uint32_t)strlen(ids[i])};
        d->version = (ud_string){"1.0.0", 5};
        d->configuration_schema = (ud_string){"{}", 2};
        d->category = UD_COMPONENT_FUNCTION;
        d->synchronous_input = d->error_output = -1;
        d->capabilities = UD_FUNCTION_PURE | UD_FUNCTION_DETERMINISTIC | UD_COMPONENT_THREAD_SAFE;
        d->user = (void *)(intptr_t)i;
        d->min_arguments = d->max_arguments = i == CONCAT ? 2 : i == SUBSTRING ? 3 : 1;
        if (i == COALESCE) {
            d->min_arguments = 2;
            d->max_arguments = 16;
        }
        d->function_bind = bind_function;
        d->function_evaluate = evaluate_function;
    }
    return ud_registry_register_owned(registry, descriptors, 8, NULL, NULL, NULL);
}