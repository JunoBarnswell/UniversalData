#include "../internal.h"
#include <math.h>
ud_status ud_numeric_result_field(int32_t op, const ud_field *a, const ud_field *b, ud_field *out) {
    if (!a || !b || !out || op < UD_NUMERIC_ADD || op > UD_NUMERIC_MULTIPLY)
        return UD_INVALID;
    if (a->dictionary_kind || b->dictionary_kind)
        return UD_UNSUPPORTED;
    if (!ud_field_matches(a, b, UD_FIELD_VALUE_TYPE) ||
        (a->type != UD_INT64 && a->type != UD_UINT64 && a->type != UD_DURATION &&
         a->type != UD_INTERVAL && a->type != UD_FLOAT64 && a->type != UD_DECIMAL128))
        return UD_SCHEMA;
    if (op == UD_NUMERIC_MULTIPLY && (a->type == UD_DURATION || a->type == UD_INTERVAL))
        return UD_UNSUPPORTED;
    *out = *a;
    out->nullable = a->nullable || b->nullable;
    out->identity = (ud_id){0};
    out->lineage = NULL;
    out->lineage_count = 0;
    if (op == UD_NUMERIC_MULTIPLY && a->type == UD_DECIMAL128) {
        int scale = a->scale + b->scale;
        if (scale < -38 || scale > 38)
            return UD_OVERFLOW;
        out->scale = scale;
        out->precision = a->precision + b->precision;
        if (out->precision > 38)
            out->precision = 38;
    }
    return UD_OK;
}
static ud_decimal128 negate(ud_decimal128 v) {
    ud_decimal128 out = {.low = ~v.low + 1};
    uint64_t high = ~(uint64_t)v.high + (v.low == 0);
    memcpy(&out.high, &high, 8);
    return out;
}
static uint32_t digits(ud_decimal128 value, uint8_t out[39]) {
    if (value.high < 0)
        value = negate(value);
    uint32_t count = 0;
    do {
        uint32_t words[4] = {(uint32_t)((uint64_t)value.high >> 32), (uint32_t)value.high,
                             (uint32_t)(value.low >> 32), (uint32_t)value.low};
        uint64_t rem = 0;
        for (uint32_t j = 0; j < 4; j++) {
            uint64_t n = (rem << 32) | words[j];
            words[j] = (uint32_t)(n / 10);
            rem = n % 10;
        }
        out[count++] = (uint8_t)rem;
        uint64_t high = ((uint64_t)words[0] << 32) | words[1];
        memcpy(&value.high, &high, 8);
        value.low = ((uint64_t)words[2] << 32) | words[3];
    } while (value.high || value.low);
    return count;
}
static ud_status multiply(ud_decimal128 a, ud_decimal128 b, int32_t precision, ud_decimal128 *out) {
    uint8_t x[39] = {0}, y[39] = {0};
    uint32_t product[78] = {0};
    uint32_t xn = digits(a, x), yn = digits(b, y);
    for (uint32_t i = 0; i < xn; i++)
        for (uint32_t j = 0; j < yn; j++)
            product[i + j] += (uint32_t)x[i] * y[j];
    uint32_t n = xn + yn;
    for (uint32_t j = 0; j < n; j++) {
        product[j + 1] += product[j] / 10;
        product[j] %= 10;
    }
    while (n > 1 && !product[n - 1])
        n--;
    if (n > (uint32_t)precision)
        return UD_OVERFLOW;
    char text[80];
    uint32_t at = 0;
    if ((a.high < 0) != (b.high < 0))
        text[at++] = '-';
    for (uint32_t j = n; j; j--)
        text[at++] = (char)('0' + product[j - 1]);
    return ud_decimal_parse(text, at, precision, 0, UD_ROUND_EXACT, out);
}
ud_status ud_value_numeric(int32_t op, const ud_value *a, const ud_value *b, const ud_field *field,
                           ud_value *out) {
    if (!a || !b || !field || !out || op < UD_NUMERIC_ADD || op > UD_NUMERIC_MULTIPLY)
        return UD_INVALID;
    if (a->is_absent || b->is_absent)
        return UD_INCOMPATIBLE;
    if (field->dictionary_kind)
        return UD_UNSUPPORTED;
    if (a->type != b->type || a->type != field->type ||
        (field->type != UD_INT64 && field->type != UD_UINT64 && field->type != UD_DURATION &&
         field->type != UD_INTERVAL && field->type != UD_FLOAT64 && field->type != UD_DECIMAL128))
        return UD_SCHEMA;
    /* Inputs may alias the output accumulator. */
    ud_value left = *a, right = *b;
    memset(out, 0, sizeof(*out));
    out->type = field->type;
    out->precision = field->precision;
    out->scale = field->scale;
    if (left.is_null || right.is_null) {
        out->is_null = 1;
        return UD_OK;
    }
    if (field->type == UD_UINT64) {
        uint64_t x = left.unsigned_integer, y = right.unsigned_integer;
        if (op == UD_NUMERIC_ADD) {
            if (x > UINT64_MAX - y)
                return UD_OVERFLOW;
            out->unsigned_integer = x + y;
        } else if (op == UD_NUMERIC_SUBTRACT) {
            if (x < y)
                return UD_OVERFLOW;
            out->unsigned_integer = x - y;
        } else {
            if (y && x > UINT64_MAX / y)
                return UD_OVERFLOW;
            out->unsigned_integer = x * y;
        }
        return UD_OK;
    }
    if (field->type == UD_INTERVAL) {
        if (op == UD_NUMERIC_MULTIPLY)
            return UD_UNSUPPORTED;
        int64_t a_parts[] = {left.interval.months, left.interval.days, left.interval.microseconds};
        int64_t b_parts[] = {right.interval.months, right.interval.days,
                             right.interval.microseconds},
                result[3];
        ud_field part_field = {.type = UD_INT64};
        for (uint32_t i = 0; i < 3; i++) {
            ud_value a_value = {.type = UD_INT64, .integer = a_parts[i]},
                     b_value = {.type = UD_INT64, .integer = b_parts[i]}, v;
            UD_TRY(ud_value_numeric(op, &a_value, &b_value, &part_field, &v));
            result[i] = v.integer;
        }
        if (result[0] < INT32_MIN || result[0] > INT32_MAX || result[1] < INT32_MIN ||
            result[1] > INT32_MAX)
            return UD_OVERFLOW;
        out->interval = (ud_interval){(int32_t)result[0], (int32_t)result[1], result[2]};
        return UD_OK;
    }
    if (field->type == UD_DURATION && op == UD_NUMERIC_MULTIPLY)
        return UD_UNSUPPORTED;
    if (field->type == UD_FLOAT64) {
        out->real = op == UD_NUMERIC_ADD        ? left.real + right.real
                    : op == UD_NUMERIC_SUBTRACT ? left.real - right.real
                                                : left.real * right.real;
        return isfinite(out->real) || !isfinite(left.real) || !isfinite(right.real) ? UD_OK
                                                                                    : UD_OVERFLOW;
    }
    if (field->type == UD_DECIMAL128) {
        if (left.precision < 1 || left.precision > 38 || right.precision != left.precision ||
            left.scale != right.scale || left.scale < -38 || left.scale > 38 ||
            field->precision < 1 || field->precision > 38)
            return UD_SCHEMA;
        ud_decimal128 checked;
        UD_TRY(ud_decimal_rescale(left.decimal, left.scale, left.precision, left.scale,
                                  UD_ROUND_EXACT, &checked));
        UD_TRY(ud_decimal_rescale(right.decimal, right.scale, right.precision, right.scale,
                                  UD_ROUND_EXACT, &checked));
        if (op == UD_NUMERIC_MULTIPLY) {
            if (field->scale != left.scale + right.scale)
                return UD_SCHEMA;
            return multiply(left.decimal, right.decimal, field->precision, &out->decimal);
        }
        if (field->scale != left.scale)
            return UD_SCHEMA;
        return ud_decimal_add(left.decimal,
                              op == UD_NUMERIC_SUBTRACT ? negate(right.decimal) : right.decimal,
                              field->precision, &out->decimal);
    }
    int64_t x = left.integer, y = right.integer;
    if (op == UD_NUMERIC_ADD) {
        if ((y > 0 && x > INT64_MAX - y) || (y < 0 && x < INT64_MIN - y))
            return UD_OVERFLOW;
        out->integer = x + y;
    } else if (op == UD_NUMERIC_SUBTRACT) {
        if ((y < 0 && x > INT64_MAX + y) || (y > 0 && x < INT64_MIN + y))
            return UD_OVERFLOW;
        out->integer = x - y;
    } else {
        if (x && y &&
            ((x == -1 && y == INT64_MIN) || (y == -1 && x == INT64_MIN) ||
             (x > 0 ? (y > 0 ? x > INT64_MAX / y : y < INT64_MIN / x)
                    : (y > 0 ? x < INT64_MIN / y : x < INT64_MAX / y))))
            return UD_OVERFLOW;
        out->integer = x * y;
    }
    return UD_OK;
}
