#include "ud/component.h"
#include "ud/text.h"
#include "ud/format.h"
#include "../../data/internal.h"
#include <math.h>
#include <time.h>
#include <unicode/ustring.h>
#include <unicode/utf16.h>
#include <unicode/uloc.h>
enum {
    F_ABS,
    F_EXP,
    F_CEILING,
    F_FLOOR,
    F_LN,
    F_LOG,
    F_POWER,
    F_ROUND,
    F_SIGN,
    F_SQUARE,
    F_SQRT,
    F_CODEPOINT,
    F_FINDSTRING,
    F_HEX,
    F_LEN,
    F_LEFT,
    F_LOWER,
    F_LTRIM,
    F_REPLACE,
    F_REPLICATE,
    F_REVERSE,
    F_RIGHT,
    F_RTRIM,
    F_SUBSTRING,
    F_TRIM,
    F_UPPER,
    F_DATEADD,
    F_DATEDIFF,
    F_DATEPART,
    F_DAY,
    F_GETDATE,
    F_GETUTCDATE,
    F_MONTH,
    F_YEAR,
    F_ISNULL,
    F_REPLACENULL,
    F_TOKEN,
    F_TOKENCOUNT,
    F_LENGTH,
    F_ASCII_UPPER,
    F_ASCII_LOWER,
    F_CONCAT,
    F_COALESCE,
    F_NORMALIZE,
    F_UNICODE_TRIM,
    F_CASEFOLD,
    F_WIRE_JSON,
    F_COUNT
};
static int integer(const ud_field *f) {
    return (f->type == UD_INT64 || f->type == UD_UINT64) && !f->dictionary_kind &&
           f->scalar.kind != UD_DT_FILETIME;
}
static int numeric(const ud_field *f) {
    return integer(f) || f->type == UD_DECIMAL128 ||
           (f->type == UD_FLOAT64 && f->scalar.kind != UD_DT_DATE);
}
static int temporal(const ud_field *f) {
    return f->type == UD_DATETIME || f->type == UD_TIMESTAMP || f->type == UD_DATE32 ||
           f->type == UD_TIME64 || f->scalar.kind == UD_DT_DATE || f->scalar.kind == UD_DT_FILETIME;
}
static ud_status bind_field(void *user, const ud_field *args, uint32_t count, ud_field *out) {
    int k = (int)(intptr_t)user;
    *out = count ? args[0] : (ud_field){0};
    out->name = "function";
    out->name_length = 8;
    out->identity = out->physical_identity = (ud_id){0};
    out->lineage = NULL;
    out->lineage_count = 0;
    out->key_order = 0;
    if (k == F_WIRE_JSON) {
        if (args[0].type != UD_BINARY)
            return UD_SCHEMA;
        *out = (ud_field){.type = UD_UTF8, .nullable = args[0].nullable};
    } else if (k <= F_SQRT) {
        if (!numeric(args))
            return UD_SCHEMA;
        if (k == F_POWER && (!numeric(args + 1)))
            return UD_SCHEMA;
        if (k == F_ROUND && !integer(args + 1))
            return UD_SCHEMA;
        if (k == F_SIGN)
            *out = (ud_field){.type = UD_INT64, .scalar = {.kind = UD_DT_I4}};
        else if (k == F_EXP || k == F_LN || k == F_LOG || k == F_POWER || k == F_SQUARE ||
                 k == F_SQRT)
            *out = (ud_field){.type = UD_FLOAT64, .scalar = {.kind = UD_DT_R8}};
    } else if (k >= F_DATEADD && k <= F_YEAR) {
        if (k == F_GETDATE || k == F_GETUTCDATE)
            *out = (ud_field){.type = UD_DATETIME,
                              .scalar = {.kind = UD_DT_DBTIMESTAMP, .fractional_digits = 3}};
        else {
            uint32_t at = k == F_DATEADD ? 2 : k == F_DATEDIFF || k == F_DATEPART ? 1 : 0;
            if (!temporal(args + at) && args[at].type != UD_UTF8)
                return UD_SCHEMA;
            if (k == F_DATEDIFF && !temporal(args + 2) && args[2].type != UD_UTF8)
                return UD_SCHEMA;
            if ((k == F_DATEADD || k == F_DATEPART || k == F_DATEDIFF) && args[0].type != UD_UTF8)
                return UD_SCHEMA;
            if (k == F_DATEADD) {
                if (!integer(args + 1))
                    return UD_SCHEMA;
                *out = (ud_field){.type = UD_DATETIME,
                                  .scalar = {.kind = UD_DT_DBTIMESTAMP, .fractional_digits = 3}};
            } else
                *out = (ud_field){.type = UD_INT64, .scalar = {.kind = UD_DT_I4}};
        }
    } else if (k == F_ISNULL)
        *out = (ud_field){.type = UD_BOOL, .scalar = {.kind = UD_DT_BOOL}};
    else if (k == F_REPLACENULL || k == F_COALESCE) {
        for (uint32_t j = 1; k==F_COALESCE&&j < count; j++) {
            ud_field common = {0};
            UD_TRY(ud_scalar_common(out, args + j, &common));
            *out = common;
        }
        if (k == F_REPLACENULL &&
            (args[0].scalar.kind == UD_DT_TEXT || args[0].scalar.kind == UD_DT_NTEXT ||
             args[0].scalar.kind == UD_DT_IMAGE || args[1].scalar.kind == UD_DT_TEXT ||
             args[1].scalar.kind == UD_DT_NTEXT || args[1].scalar.kind == UD_DT_IMAGE))
            return UD_SCHEMA;
    } else if (k == F_HEX) {
        if (!integer(args))
            return UD_SCHEMA;
        *out = (ud_field){.type = UD_UTF8, .scalar = {.kind = UD_DT_WSTR, .length = 16}};
    } else {
        if (args[0].type != UD_UTF8 && !(k == F_LEN && args[0].type == UD_BINARY))
            return UD_SCHEMA;
        if (k == F_LEN || k == F_LENGTH || k == F_FINDSTRING || k == F_TOKENCOUNT)
            *out = (ud_field){.type = UD_INT64, .scalar = {.kind = k == F_LENGTH ? 0 : UD_DT_I4}};
        else if (k == F_CODEPOINT)
            *out = (ud_field){.type = UD_UINT64, .scalar = {.kind = UD_DT_UI2}};
        else {
            out->scalar = (ud_scalar_metadata){0};
            if (args[0].scalar.kind) {
                out->scalar.kind = UD_DT_WSTR;
                out->scalar.length = args[0].scalar.length;
                out->scalar.locale_id = args[0].scalar.locale_id;
                if (!out->scalar.length)
                    out->scalar.kind = UD_DT_NTEXT;
            }
        }
        if ((k == F_REPLACE || k == F_CONCAT) && args[1].type != UD_UTF8)
            return UD_SCHEMA;
        if (k == F_REPLACE && args[2].type != UD_UTF8)
            return UD_SCHEMA;
        if ((k == F_TOKEN || k == F_TOKENCOUNT || k == F_FINDSTRING) && args[1].type != UD_UTF8)
            return UD_SCHEMA;
        if ((k == F_LEFT || k == F_RIGHT || k == F_REPLICATE) && !integer(args + 1))
            return UD_SCHEMA;
        if ((k == F_SUBSTRING && (!integer(args + 1) || !integer(args + 2))) ||
            ((k == F_TOKEN || k == F_FINDSTRING) && !integer(args + 2)))
            return UD_SCHEMA;
        /* Dynamic repetition/replacement lengths have no finite declared bound.
         * Keep a Native unbounded Unicode domain instead of a false input bound. */
        if (k == F_REPLICATE || k == F_REPLACE)
            out->scalar =
                (ud_scalar_metadata){.kind = UD_DT_NTEXT, .locale_id = args[0].scalar.locale_id};
        if (k == F_CONCAT && out->scalar.kind) {
            uint64_t length = (uint64_t)args[0].scalar.length + args[1].scalar.length;
            if (length > INT32_MAX)
                return UD_OVERFLOW;
            out->scalar.length = (uint32_t)length;
        }
    }
    out->nullable = k == F_ISNULL || k == F_TOKENCOUNT ? 0 : 0;
    for (uint32_t j = 0; j < count; j++)
        if (k != F_ISNULL && k != F_TOKENCOUNT)
            out->nullable |= args[j].nullable;
    if (k == F_COALESCE || k == F_REPLACENULL) {
        out->nullable = 1;
        for (uint32_t j = 0; j < count; j++)
            out->nullable &= args[j].nullable;
    }
    if (k == F_TOKEN)
        out->nullable = 1;
    return UD_OK;
}
static ud_status wide(ud_context *ctx, const ud_value *v, UChar **out, int32_t *length) {
    UErrorCode error = U_ZERO_ERROR;
    if (v->text_length > INT32_MAX)
        return UD_RESOURCE;
    u_strFromUTF8(NULL, 0, length, v->text, (int32_t)v->text_length, &error);
    if (error != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(error))
        return UD_PARSE;
    *out = ud_alloc(ctx, ((size_t)*length + 1) * 2);
    if (!*out)
        return UD_RESOURCE;
    error = U_ZERO_ERROR;
    u_strFromUTF8(*out, *length + 1, length, v->text, (int32_t)v->text_length, &error);
    if (U_FAILURE(error)) {
        ud_free(*out);
        *out = NULL;
        return UD_PARSE;
    }
    return UD_OK;
}
static ud_status utf8(ud_context *ctx, const UChar *text, int32_t length, ud_value *out) {
    UErrorCode error = U_ZERO_ERROR;
    int32_t bytes = 0;
    u_strToUTF8(NULL, 0, &bytes, text, length, &error);
    if (error != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(error))
        return UD_PARSE;
    char *value = ud_alloc(ctx, (size_t)bytes + 1);
    if (!value)
        return UD_RESOURCE;
    error = U_ZERO_ERROR;
    u_strToUTF8(value, bytes + 1, &bytes, text, length, &error);
    if (U_FAILURE(error)) {
        ud_free(value);
        return UD_PARSE;
    }
    out->text = value;
    out->text_length = (uint32_t)bytes;
    return UD_OK;
}
static ud_status integer_value(const ud_value *v, int64_t *out) {
    if (v->type == UD_UINT64 && v->unsigned_integer > INT64_MAX)
        return UD_OVERFLOW;
    *out = v->integer;
    return UD_OK;
}
static ud_status double_value(ud_context *ctx, const ud_value *v, double *out) {
    if (v->type == UD_FLOAT64)
        *out = v->real;
    else if (v->type == UD_INT64)
        *out = (double)v->integer;
    else if (v->type == UD_UINT64)
        *out = (double)v->unsigned_integer;
    else {
        char text[96];
        uint32_t n = 0;
        UD_TRY(ud_value_format(v, text, sizeof(text), &n));
        ud_value parsed;
        UD_TRY(ud_value_parse(ctx, UD_FLOAT64, text, n - 1, &parsed));
        *out = parsed.real;
    }
    return isfinite(*out) ? UD_OK : UD_OVERFLOW;
}
static ud_status numeric_function(int k, ud_context *ctx, const ud_field *args,
                                  const ud_value *values, const ud_field *field, ud_value *out) {
    (void)args;
    if (k == F_ABS) {
        ud_value zero = {.type = field->type, .precision = field->precision, .scale = field->scale};
        if (ud_value_compare(values, &zero) >= 0)
            return ud_copy_value(ctx, values, out, 0);
        return ud_value_numeric(UD_NUMERIC_SUBTRACT, &zero, values, field, out);
    }
    if ((k == F_ROUND || k == F_CEILING || k == F_FLOOR) && values[0].type == UD_DECIMAL128) {
        int64_t places = 0;
        if (k == F_ROUND) {
            UD_TRY(integer_value(values + 1, &places));
            if (places < 0 || places > 38)
                return UD_PARSE;
        }
        if (places >= values[0].scale)
            return ud_copy_value(ctx, values, out, 0);
        ud_decimal128 rounded;
        UD_TRY(ud_decimal_rescale(values[0].decimal, values[0].scale, 38, (int32_t)places,
                                  k == F_CEILING ? UD_ROUND_CEILING
                                  : k == F_FLOOR ? UD_ROUND_FLOOR
                                                 : UD_ROUND_HALF_AWAY,
                                  &rounded));
        return ud_decimal_rescale(rounded, (int32_t)places, field->precision, field->scale,
                                  UD_ROUND_EXACT, &out->decimal);
    }
    if (k == F_SIGN) {
        ud_value zero = {
            .type = values[0].type, .precision = values[0].precision, .scale = values[0].scale};
        out->integer = ud_value_compare(values, &zero);
        return UD_OK;
    }
    double x = 0, y = 0;
    UD_TRY(double_value(ctx, values, &x));
    if (k == F_POWER)
        UD_TRY(double_value(ctx, values + 1, &y));
    double result = 0;
    if (k == F_ROUND) {
        int64_t places;
        UD_TRY(integer_value(values + 1, &places));
        if (places < 0 || places > 308)
            return UD_PARSE;
        double factor = pow(10, (double)places);
        if (!isfinite(x * factor))
            result = x;
        else
            result = round(x * factor) / factor;
    } else if (k == F_CEILING)
        result = ceil(x);
    else if (k == F_FLOOR)
        result = floor(x);
    else if (k == F_EXP)
        result = exp(x);
    else if (k == F_LN || k == F_LOG) {
        if (x <= 0)
            return UD_PARSE;
        result = k == F_LN ? log(x) : log10(x);
    } else if (k == F_POWER)
        result = pow(x, y);
    else if (k == F_SQUARE)
        result = x * x;
    else if (k == F_SQRT) {
        if (x < 0)
            return UD_PARSE;
        result = sqrt(x);
    } else
        return UD_UNSUPPORTED;
    if (!isfinite(result))
        return UD_OVERFLOW;
    if (field->type == UD_FLOAT64) {
        out->real = field->scalar.kind == UD_DT_R4 ? (double)(float)result : result;
        return UD_OK;
    }
    /* Integral CEILING/FLOOR/ROUND retain their exact input representation. */
    return ud_copy_value(ctx, values, out, 0);
}
static ud_status as_date(const ud_field *f, const ud_value *v, ud_datetime *out) {
    if (f->type == UD_UTF8) {
        ud_field target = {.type = UD_DATETIME};
        return ud_datetime_parse((ud_string){v->text, v->text_length}, &target, out);
    }
    return ud_datetime_from_value(f, v, out);
}
static ud_status date_function(int k, const ud_field *args, const ud_value *values,
                               const ud_field *field, ud_value *out) {
    ud_datetime a = {0}, b = {0};
    if (k == F_GETDATE || k == F_GETUTCDATE) {
        struct timespec now;
        if (timespec_get(&now, TIME_UTC) != TIME_UTC)
            return UD_IO;
        time_t seconds = now.tv_sec;
        struct tm parts;
#ifdef _WIN32
        if ((k == F_GETDATE ? localtime_s(&parts, &seconds) : gmtime_s(&parts, &seconds)))
            return UD_IO;
#else
        if (!(k == F_GETDATE ? localtime_r(&seconds, &parts) : gmtime_r(&seconds, &parts)))
            return UD_IO;
#endif
        char text[48];
        int n = snprintf(text, sizeof(text), "%04d-%02d-%02dT%02d:%02d:%02d.%03ld",
                         parts.tm_year + 1900, parts.tm_mon + 1, parts.tm_mday, parts.tm_hour,
                         parts.tm_min, parts.tm_sec, now.tv_nsec / 1000000);
        UD_TRY(ud_datetime_parse((ud_string){text, (uint32_t)n}, field, &a));
        out->datetime = a;
        return UD_OK;
    }
    if (k == F_DATEADD) {
        int64_t amount;
        UD_TRY(integer_value(values + 1, &amount));
        UD_TRY(as_date(args + 2, values + 2, &a));
        UD_TRY(ud_datetime_add((ud_string){values[0].text, values[0].text_length}, amount, a, &b));
        b.has_offset = 0;
        b.offset_minutes = 0;
        b.ticks = b.ticks / 10000 * 10000;
        out->datetime = b;
        return UD_OK;
    }
    if (k == F_DATEDIFF) {
        UD_TRY(as_date(args + 1, values + 1, &a));
        UD_TRY(as_date(args + 2, values + 2, &b));
        return ud_datetime_diff((ud_string){values[0].text, values[0].text_length}, a, b,
                                &out->integer);
    }
    uint32_t at = k == F_DATEPART ? 1 : 0;
    UD_TRY(as_date(args + at, values + at, &a));
    ud_string part = k == F_DATEPART ? (ud_string){values[0].text, values[0].text_length}
                     : k == F_DAY    ? (ud_string){"day", 3}
                     : k == F_MONTH  ? (ud_string){"month", 5}
                                     : (ud_string){"year", 4};
    return ud_datetime_part(part, a, &out->integer);
}
static int delimiter(UChar c, const UChar *set, int32_t count) {
    for (int32_t j = 0; j < count; j++)
        if (c == set[j])
            return 1;
    return 0;
}
static ud_status evaluate_function(void *user, ud_context *ctx, const ud_field *args,
                                   const ud_value *values, uint32_t count, const ud_field *field,
                                   ud_value *out) {
    int k = (int)(intptr_t)user;
    memset(out, 0, sizeof(*out));
    out->type = field->type;
    out->precision = field->precision;
    out->scale = field->scale;
    if (k == F_ISNULL) {
        out->integer = values[0].is_null;
        return UD_OK;
    }
    if (k == F_COALESCE || k == F_REPLACENULL) {
        for (uint32_t j = 0; j < count; j++)
            if (!values[j].is_null)
                return ud_value_cast_field(ctx, args + j, values + j, field, UD_ROUND_HALF_AWAY, 0,
                                           out);
        out->is_null = 1;
        return UD_OK;
    }
    for (uint32_t j = 0; j < count; j++)
        if (values[j].is_null) {
            if (k == F_TOKENCOUNT)
                return UD_OK;
            out->is_null = 1;
            return UD_OK;
        }
    if (k == F_WIRE_JSON) {
        ud_batch *record = NULL;
        ud_status status = ud_batch_deserialize(ctx, (const uint8_t *)values[0].text,
                                                values[0].text_length, &record);
        if (!status && record->rows != 1)
            status = UD_SCHEMA;
        if (!status) {
            char *text = NULL;
            status = ud_batch_to_json(record, &text, &out->text_length);
            out->text = text;
        }
        ud_batch_destroy(record);
        return status;
    }
    if (k <= F_SQRT)
        return numeric_function(k, ctx, args, values, field, out);
    if (k >= F_DATEADD && k <= F_YEAR)
        return date_function(k, args, values, field, out);
    if (k == F_HEX) {
        uint64_t v = values[0].unsigned_integer;
        unsigned width = args[0].scalar.kind >= UD_DT_I1 && args[0].scalar.kind <= UD_DT_I4
                             ? 8u << (args[0].scalar.kind - UD_DT_I1)
                         : args[0].scalar.kind >= UD_DT_UI1 && args[0].scalar.kind <= UD_DT_UI4
                             ? 8u << (args[0].scalar.kind - UD_DT_UI1)
                             : 64;
        if (width < 64)
            v &= (UINT64_C(1) << width) - 1;
        char text[17];
        int n = snprintf(text, sizeof(text), "%llX", (unsigned long long)v);
        out->text = ud_strdup_n(ctx, text, (size_t)n);
        out->text_length = (uint32_t)n;
        return out->text ? UD_OK : UD_RESOURCE;
    }
    if (k == F_NORMALIZE || k == F_UNICODE_TRIM || k == F_CASEFOLD) {
        char *text = NULL;
        uint32_t flags = k == F_NORMALIZE      ? UD_TEXT_NFC
                         : k == F_UNICODE_TRIM ? UD_TEXT_TRIM
                                               : UD_TEXT_NFKC | UD_TEXT_FOLD;
        ud_status s = ud_text_transform(ctx, (ud_string){values[0].text, values[0].text_length},
                                        flags, &text, &out->text_length);
        out->text = text;
        return s;
    }
    if (k == F_LENGTH) {
        for (uint32_t j = 0; j < values[0].text_length; j++)
            if (((unsigned char)values[0].text[j] & 0xc0) != 0x80)
                out->integer++;
        return UD_OK;
    }
    if (k == F_LEN && (args[0].type == UD_BINARY || args[0].scalar.kind == UD_DT_TEXT ||
                       args[0].scalar.kind == UD_DT_NTEXT)) {
        uint32_t bytes = values[0].text_length;
        if (args[0].type != UD_BINARY)
            UD_TRY(ud_scalar_text_bytes(
                ctx, args, (ud_string){values[0].text, values[0].text_length}, &bytes));
        out->integer = bytes;
        return UD_OK;
    }
    UChar *text = NULL, *second = NULL, *third = NULL;
    int32_t length = 0, sn = 0, tn = 0;
    ud_status s = wide(ctx, values, &text, &length);
    if (!s &&
        (k == F_REPLACE || k == F_TOKEN || k == F_TOKENCOUNT || k == F_FINDSTRING || k == F_CONCAT))
        s = wide(ctx, values + 1, &second, &sn);
    if (!s && k == F_REPLACE)
        s = wide(ctx, values + 2, &third, &tn);
    int64_t number = 0;
    if (!s && (k == F_LEFT || k == F_RIGHT || k == F_REPLICATE))
        s = integer_value(values + 1, &number);
    int32_t begin = 0, end = length;
    UChar *owned = NULL;
    ud_bytes bytes = {.ctx = ctx};
    if (!s && (k == F_LEN))
        out->integer = length;
    else if (!s && k == F_CODEPOINT) {
        if (!length)
            s = UD_PARSE;
        else
            out->unsigned_integer = text[0];
    } else if (!s && (k == F_LEFT || k == F_RIGHT)) {
        if (number < 0)
            s = UD_PARSE;
        else if (number < length) {
            if (k == F_LEFT)
                end = (int32_t)number;
            else
                begin = length - (int32_t)number;
        }
    } else if (!s && k == F_SUBSTRING) {
        int64_t start, take;
        s = integer_value(values + 1, &start);
        if (!s)
            s = integer_value(values + 2, &take);
        if (!s && (start < 1 || take < 0))
            s = UD_PARSE;
        if (!s) {
            begin = start > length ? length : (int32_t)(start - 1);
            end = take > length - begin ? length : begin + (int32_t)take;
        }
    } else if (!s && (k == F_TRIM || k == F_LTRIM || k == F_RTRIM)) {
        if (k != F_RTRIM)
            while (begin < end && text[begin] == 0x20)
                begin++;
        if (k != F_LTRIM)
            while (end > begin && text[end - 1] == 0x20)
                end--;
    } else if (!s && (k == F_UPPER || k == F_LOWER || k == F_ASCII_UPPER || k == F_ASCII_LOWER)) {
        if (k == F_ASCII_UPPER || k == F_ASCII_LOWER) {
            for (int32_t j = 0; j < length; j++) {
                if (k == F_ASCII_UPPER && text[j] >= 'a' && text[j] <= 'z')
                    text[j] -= 32;
                if (k == F_ASCII_LOWER && text[j] >= 'A' && text[j] <= 'Z')
                    text[j] += 32;
            }
        } else {
            char locale[ULOC_FULLNAME_CAPACITY] = "";
            UErrorCode error = U_ZERO_ERROR;
            if (field->scalar.locale_id)
                uloc_getLocaleForLCID(field->scalar.locale_id, locale, sizeof(locale), &error);
            if (U_FAILURE(error))
                s = UD_UNSUPPORTED;
            int32_t n = 0;
            if (!s) {
                error = U_ZERO_ERROR;
                n = k == F_UPPER ? u_strToUpper(NULL, 0, text, length, locale, &error)
                                 : u_strToLower(NULL, 0, text, length, locale, &error);
                if (error != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(error))
                    s = UD_PARSE;
            }
            if (!s) {
                owned = ud_alloc(ctx, ((size_t)n + 1) * 2);
                if (!owned)
                    s = UD_RESOURCE;
                else {
                    error = U_ZERO_ERROR;
                    end = k == F_UPPER ? u_strToUpper(owned, n + 1, text, length, locale, &error)
                                       : u_strToLower(owned, n + 1, text, length, locale, &error);
                    if (U_FAILURE(error))
                        s = UD_PARSE;
                }
            }
        }
    } else if (!s && k == F_REVERSE) {
        owned = ud_alloc(ctx, ((size_t)length + 1) * 2);
        if (!owned)
            s = UD_RESOURCE;
        else {
            int32_t at = 0;
            for (int32_t j = length; j > 0;) {
                UChar32 cp;
                U16_PREV(text, 0, j, cp);
                U16_APPEND_UNSAFE(owned, at, cp);
            }
            end = at;
        }
    } else if (!s && k == F_REPLICATE) {
        if (number < 0)
            s = UD_PARSE;
        else if (number && (uint64_t)length > (uint64_t)INT32_MAX / (uint64_t)number)
            s = UD_RESOURCE;
        else
            for (int64_t j = 0; !s && j < number; j++) {
                if (!(j & 255) && ud_is_cancelled(ctx)) {
                    s = UD_CANCELLED;
                    break;
                }
                s = ud_bytes_put(&bytes, text, (size_t)length * 2);
            }
        end = (int32_t)(bytes.size / 2);
    } else if (!s && k == F_CONCAT) {
        s = ud_bytes_put(&bytes, text, (size_t)length * 2);
        if (!s)
            s = ud_bytes_put(&bytes, second, (size_t)sn * 2);
        end = (int32_t)(bytes.size / 2);
    } else if (!s && k == F_REPLACE) {
        for (int32_t j = 0; !s && j < length;) {
            if (sn && j <= length - sn && !memcmp(text + j, second, (size_t)sn * 2)) {
                s = ud_bytes_put(&bytes, third, (size_t)tn * 2);
                j += sn;
            } else {
                s = ud_bytes_put(&bytes, text + j, 2);
                j++;
            }
        }
        if (bytes.size / 2 > INT32_MAX)
            s = UD_RESOURCE;
        end = (int32_t)(bytes.size / 2);
    } else if (!s && k == F_FINDSTRING) {
        int64_t occurrence;
        s = integer_value(values + 2, &occurrence);
        if (!s && occurrence <= 0)
            s = UD_PARSE;
        int64_t found = 0;
        for (int32_t j = 0; !s && sn && j <= length - sn; j++)
            if (!memcmp(text + j, second, (size_t)sn * 2) && ++found == occurrence) {
                out->integer = j + 1;
                break;
            }
    } else if (!s && (k == F_TOKEN || k == F_TOKENCOUNT)) {
        int64_t wanted = 0;
        if (k == F_TOKEN) {
            s = integer_value(values + 2, &wanted);
            if (!s && wanted <= 0)
                s = UD_PARSE;
        }
        int64_t tokens = 0;
        int matched = 0;
        for (int32_t j = 0; !s && j < length;) {
            while (j < length && delimiter(text[j], second, sn))
                j++;
            if (j == length)
                break;
            int32_t first = j;
            while (j < length && !delimiter(text[j], second, sn))
                j++;
            tokens++;
            if (k == F_TOKEN && tokens == wanted) {
                begin = first;
                end = j;
                matched = 1;
                break;
            }
        }
        if (k == F_TOKENCOUNT)
            out->integer = tokens;
        else if (!matched)
            out->is_null = 1;
    }
    if (!s && field->type == UD_UTF8 && !out->is_null) {
        const UChar *value = owned ? owned : bytes.data ? (const UChar *)bytes.data : text + begin;
        s = utf8(ctx, value, end - begin, out);
    }
    ud_free(text);
    ud_free(second);
    ud_free(third);
    ud_free(owned);
    ud_bytes_clear(&bytes);
    return s;
}
static ud_status bind_function(void *user, ud_context *ctx, const ud_field *args, uint32_t count,
                               ud_batch **out) {
    *out = NULL;
    ud_field field = {0};
    UD_TRY(bind_field(user, args, count, &field));
    field.name = "result";
    field.name_length = 6;
    return ud_batch_create(ctx, &field, 1, 0, out);
}
ud_status ud_expression_register_functions(ud_registry *registry) {
    static const char *const ids[] = {
        "abs",        "exp",        "ceiling",      "floor",       "ln",          "log",
        "power",      "round",      "sign",         "square",      "sqrt",        "codepoint",
        "findstring", "hex",        "len",          "left",        "lower",       "ltrim",
        "replace",    "replicate",  "reverse",      "right",       "rtrim",       "substring",
        "trim",       "upper",      "dateadd",      "datediff",    "datepart",    "day",
        "getdate",    "getutcdate", "month",        "year",        "isnull",      "replacenull",
        "token",      "tokencount", "length",       "upper_ascii", "lower_ascii", "concat",
        "coalesce",   "normalize",  "trim_unicode", "casefold",    "ud.wire_json"};
    ud_component_descriptor descriptors[F_COUNT] = {{0}};
    for (uint32_t j = 0; j < F_COUNT; j++) {
        ud_component_descriptor *d = descriptors + j;
        d->struct_size = sizeof(*d);
        d->abi_version = UD_ABI_VERSION;
        d->id = d->name = (ud_string){ids[j], (uint32_t)strlen(ids[j])};
        d->version = (ud_string){UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1};
        d->configuration_schema = (ud_string){"{}", 2};
        d->category = UD_COMPONENT_FUNCTION;
        d->synchronous_input = d->error_output = -1;
        d->capabilities =
            UD_COMPONENT_THREAD_SAFE |
            (j == F_GETDATE || j == F_GETUTCDATE ? UD_FUNCTION_CONTEXTUAL
                                                 : UD_FUNCTION_PURE | UD_FUNCTION_DETERMINISTIC);
        d->user = (void *)(intptr_t)j;
        d->min_arguments = d->max_arguments =
            j == F_GETDATE || j == F_GETUTCDATE ? 0
            : j == F_POWER || j == F_ROUND || j == F_LEFT || j == F_RIGHT || j == F_REPLICATE ||
                    j == F_REPLACENULL || j == F_TOKENCOUNT || j == F_DATEPART || j == F_CONCAT
                ? 2
            : j == F_REPLACE || j == F_SUBSTRING || j == F_FINDSTRING || j == F_TOKEN ||
                    j == F_DATEADD || j == F_DATEDIFF
                ? 3
                : 1;
        if (j == F_COALESCE) {
            d->min_arguments = 2;
            d->max_arguments = 16;
        }
        d->function_bind = bind_function;
        d->function_evaluate = evaluate_function;
    }
    return ud_registry_register_owned(registry, descriptors, F_COUNT, NULL, NULL, NULL);
}
