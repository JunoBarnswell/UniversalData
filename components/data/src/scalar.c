#include "../internal.h"
#include <math.h>
#include <float.h>
#include <unicode/ucnv.h>
#include <unicode/ucnv_err.h>
#include <unicode/ustring.h>
#include <unicode/utf16.h>
static const char *const names[] = {"DT_BOOL",        "DT_I1",           "DT_I2",
                                    "DT_I4",          "DT_I8",           "DT_UI1",
                                    "DT_UI2",         "DT_UI4",          "DT_UI8",
                                    "DT_R4",          "DT_R8",           "DT_DECIMAL",
                                    "DT_NUMERIC",     "DT_CY",           "DT_DATE",
                                    "DT_DBDATE",      "DT_DBTIME",       "DT_DBTIME2",
                                    "DT_DBTIMESTAMP", "DT_DBTIMESTAMP2", "DT_DBTIMESTAMPOFFSET",
                                    "DT_FILETIME",    "DT_STR",          "DT_WSTR",
                                    "DT_TEXT",        "DT_NTEXT",        "DT_BYTES",
                                    "DT_IMAGE",       "DT_GUID"};
const char *ud_scalar_type_name(uint32_t kind) {
    return kind && kind <= 29 ? names[kind - 1] : NULL;
}
ud_status ud_scalar_type_parse(ud_string name, uint32_t *out) {
    if (!out || !name.data)
        return UD_INVALID;
    *out = 0;
    for (uint32_t k = 0; k < 29; k++) {
        if (strlen(names[k]) != name.length)
            continue;
        uint32_t j = 0;
        for (; j < name.length; j++) {
            char c = name.data[j];
            if (c >= 'a' && c <= 'z')
                c -= 32;
            if (c != names[k][j])
                break;
        }
        if (j == name.length) {
            *out = k + 1;
            return UD_OK;
        }
    }
    return UD_SCHEMA;
}
static ud_type storage(uint32_t kind) {
    return kind == UD_DT_BOOL                                                   ? UD_BOOL
           : kind >= UD_DT_I1 && kind <= UD_DT_I8                               ? UD_INT64
           : (kind >= UD_DT_UI1 && kind <= UD_DT_UI8) || kind == UD_DT_FILETIME ? UD_UINT64
           : kind == UD_DT_R4 || kind == UD_DT_R8 || kind == UD_DT_DATE         ? UD_FLOAT64
           : kind >= UD_DT_DECIMAL && kind <= UD_DT_CY                          ? UD_DECIMAL128
           : kind >= UD_DT_DBDATE && kind <= UD_DT_DBTIMESTAMPOFFSET            ? UD_DATETIME
           : kind >= UD_DT_STR && kind <= UD_DT_NTEXT                           ? UD_UTF8
           : kind >= UD_DT_BYTES && kind <= UD_DT_GUID                          ? UD_BINARY
                                                                                : 0;
}
ud_status ud_scalar_field(ud_string name, uint32_t length, int32_t precision, int32_t scale,
                          uint32_t cp, uint32_t locale, ud_field *out) {
    if (!out)
        return UD_INVALID;
    uint32_t kind = 0;
    UD_TRY(ud_scalar_type_parse(name, &kind));
    *out = (ud_field){
        .type = storage(kind),
        .nullable = 1,
        .scalar = {.kind = kind, .length = length, .code_page = cp, .locale_id = locale}};
    if (out->type == UD_DECIMAL128) {
        out->precision = kind == UD_DT_CY ? 19 : kind == UD_DT_DECIMAL ? 29 : precision;
        out->scale = kind == UD_DT_CY ? 4 : scale;
    }
    if (kind == UD_DT_GUID)
        out->scalar.length = 16;
    if (kind == UD_DT_DBTIME2 || kind == UD_DT_DBTIMESTAMP2 || kind == UD_DT_DBTIMESTAMPOFFSET)
        out->scalar.fractional_digits = (uint32_t)scale;
    else if (kind == UD_DT_DBTIMESTAMP)
        out->scalar.fractional_digits = 3;
    return ud_scalar_field_validate(out);
}
ud_status ud_scalar_field_validate(const ud_field *f) {
    if (!f)
        return UD_INVALID;
    const ud_scalar_metadata *m = &f->scalar;
    if (!m->kind)
        return m->length || m->code_page || m->locale_id || m->comparison_flags ||
                       m->fractional_digits
                   ? UD_SCHEMA
                   : UD_OK;
    if (m->kind > 29 || storage(m->kind) != f->type || m->fractional_digits > 7)
        return UD_SCHEMA;
    if (m->comparison_flags & ~127u)
        return UD_SCHEMA;
    if ((m->kind == UD_DT_STR || m->kind == UD_DT_WSTR || m->kind == UD_DT_BYTES) && !m->length)
        return UD_SCHEMA;
    if ((m->kind == UD_DT_STR || m->kind == UD_DT_TEXT) != !!m->code_page)
        return UD_SCHEMA;
    if (m->kind == UD_DT_CY && (f->precision != 19 || f->scale != 4))
        return UD_SCHEMA;
    if (m->kind == UD_DT_DECIMAL && (f->precision != 29 || f->scale < 0 || f->scale > 28))
        return UD_SCHEMA;
    if (m->kind == UD_DT_NUMERIC &&
        (f->precision < 1 || f->precision > 38 || f->scale < 0 || f->scale > f->precision))
        return UD_SCHEMA;
    if (m->kind == UD_DT_GUID && m->length != 16)
        return UD_SCHEMA;
    return UD_OK;
}
static ud_status unicode(ud_context *ctx, ud_string input, UChar **out, int32_t *count) {
    if (input.length > INT32_MAX)
        return UD_RESOURCE;
    UErrorCode error = U_ZERO_ERROR;
    u_strFromUTF8(NULL, 0, count, input.data, (int32_t)input.length, &error);
    if (error != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(error))
        return UD_PARSE;
    *out = ud_alloc(ctx, ((size_t)*count + 1) * 2);
    if (!*out)
        return UD_RESOURCE;
    error = U_ZERO_ERROR;
    u_strFromUTF8(*out, *count + 1, count, input.data, (int32_t)input.length, &error);
    if (U_FAILURE(error)) {
        ud_free(*out);
        *out = NULL;
        return UD_PARSE;
    }
    return UD_OK;
}
static UConverter *code_page(uint32_t cp, UErrorCode *error) {
    char encoding[40];
    snprintf(encoding, sizeof(encoding), "windows-%u", cp);
    if (cp == 65001)
        strcpy(encoding, "UTF-8");
    else if (cp == 1200)
        strcpy(encoding, "UTF-16LE");
    else if (cp == 1201)
        strcpy(encoding, "UTF-16BE");
    UConverter *converter = ucnv_open(encoding, error);
    if (U_SUCCESS(*error))
        ucnv_setFromUCallBack(converter, UCNV_FROM_U_CALLBACK_STOP, NULL, NULL, NULL, error);
    return converter;
}
ud_status ud_scalar_text_bytes(ud_context *ctx, const ud_field *field, ud_string input,
                               uint32_t *out) {
    if (!ctx || !field || !out)
        return UD_INVALID;
    UChar *text = NULL;
    int32_t count = 0;
    UD_TRY(unicode(ctx, input, &text, &count));
    ud_status status = UD_OK;
    if (field->scalar.code_page) {
        UErrorCode error = U_ZERO_ERROR;
        UConverter *converter = code_page(field->scalar.code_page, &error);
        if (U_FAILURE(error))
            status = UD_UNSUPPORTED;
        if (!status) {
            error = U_ZERO_ERROR;
            int32_t n = ucnv_fromUChars(converter, NULL, 0, text, count, &error);
            if (error != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(error))
                status = UD_PARSE;
            else
                *out = (uint32_t)n;
        }
        if (converter)
            ucnv_close(converter);
    } else if ((uint32_t)count > UINT32_MAX / 2)
        status = UD_OVERFLOW;
    else
        *out = (uint32_t)count * 2;
    ud_free(text);
    return status;
}
ud_status ud_scalar_text(ud_context *ctx, const ud_field *f, ud_string input, int truncate,
                         char **out, uint32_t *length) {
    if (!ctx || !f || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    UChar *wide = NULL;
    int32_t count = 0;
    UD_TRY(unicode(ctx, input, &wide, &count));
    ud_status status = UD_OK;
    if (f->scalar.code_page) {
        UErrorCode error = U_ZERO_ERROR;
        UConverter *converter = code_page(f->scalar.code_page, &error);
        if (U_FAILURE(error))
            status = UD_UNSUPPORTED;
        int32_t bytes = 0;
        char *encoded = NULL;
        if (!status) {
            error = U_ZERO_ERROR;
            bytes = ucnv_fromUChars(converter, NULL, 0, wide, count, &error);
            if (error != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(error))
                status = UD_PARSE;
        }
        if (!status) {
            encoded = ud_alloc(ctx, (size_t)bytes + 1);
            if (!encoded)
                status = UD_RESOURCE;
        }
        if (!status) {
            error = U_ZERO_ERROR;
            ucnv_fromUChars(converter, encoded, bytes + 1, wide, count, &error);
            if (U_FAILURE(error))
                status = UD_PARSE;
        }
        if (!status && f->scalar.length && (uint32_t)bytes > f->scalar.length) {
            if (!truncate)
                status = UD_TRUNCATED;
            else {
                const char *cursor = encoded, *end = encoded + bytes;
                int32_t kept = 0;
                ucnv_resetToUnicode(converter);
                while (cursor < end) {
                    const char *before = cursor;
                    error = U_ZERO_ERROR;
                    UChar32 cp = ucnv_getNextUChar(converter, &cursor, end, &error);
                    if (U_FAILURE(error)) {
                        status = UD_PARSE;
                        break;
                    }
                    if ((uint32_t)(cursor - encoded) > f->scalar.length) {
                        cursor = before;
                        break;
                    }
                    kept += U16_LENGTH(cp);
                }
                count = kept;
            }
        }
        ud_free(encoded);
        if (converter)
            ucnv_close(converter);
    } else if (f->scalar.length && (uint32_t)count > f->scalar.length) {
        if (!truncate)
            status = UD_TRUNCATED;
        else {
            count = (int32_t)f->scalar.length;
            if (count && U16_IS_LEAD(wide[count - 1]))
                count--;
        }
    }
    int32_t bytes = 0;
    if (!status) {
        UErrorCode error = U_ZERO_ERROR;
        u_strToUTF8(NULL, 0, &bytes, wide, count, &error);
        if (error != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(error))
            status = UD_PARSE;
    }
    if (!status) {
        *out = ud_alloc(ctx, (size_t)bytes + 1);
        if (!*out)
            status = UD_RESOURCE;
    }
    if (!status) {
        UErrorCode error = U_ZERO_ERROR;
        u_strToUTF8(*out, bytes + 1, &bytes, wide, count, &error);
        if (U_FAILURE(error))
            status = UD_PARSE;
        else
            *length = (uint32_t)bytes;
    }
    ud_free(wide);
    if (status) {
        ud_free(*out);
        *out = NULL;
    }
    return status;
}
ud_status ud_value_validate_field(ud_context *ctx, const ud_field *f, const ud_value *v) {
    if (!f || !v || v->type != f->type)
        return UD_SCHEMA;
    UD_TRY(ud_scalar_field_validate(f));
    if (v->is_null)
        return f->nullable || v->is_absent ? UD_OK : UD_SCHEMA;
    uint32_t k = f->scalar.kind;
    if (!k && f->type != UD_DATETIME)
        return UD_OK;
    if (k >= UD_DT_I1 && k <= UD_DT_I4) {
        unsigned bits = 8u << (k - UD_DT_I1);
        int64_t max = (INT64_C(1) << (bits - 1)) - 1;
        if (v->integer < -max - 1 || v->integer > max)
            return UD_OVERFLOW;
    }
    if (k >= UD_DT_UI1 && k <= UD_DT_UI4) {
        unsigned bits = 8u << (k - UD_DT_UI1);
        if (v->unsigned_integer > ((UINT64_C(1) << bits) - 1))
            return UD_OVERFLOW;
    }
    if (k == UD_DT_CY) {
        if (!((v->decimal.high == 0 && v->decimal.low <= INT64_MAX) ||
              (v->decimal.high == -1 && v->decimal.low >= UINT64_C(0x8000000000000000))))
            return UD_OVERFLOW;
    }
    if (k == UD_DT_DECIMAL) {
        uint64_t low = v->decimal.low, high = (uint64_t)v->decimal.high;
        if (v->decimal.high < 0) {
            low = ~low + 1;
            high = ~high + (low == 0);
        }
        if (high > UINT32_MAX)
            return UD_OVERFLOW;
    }
    if (k == UD_DT_R4 && isfinite(v->real) &&
        (!isfinite((float)v->real) || (double)(float)v->real != v->real))
        return UD_OVERFLOW;
    if (f->type == UD_UTF8) {
        char *copy = NULL;
        uint32_t n = 0;
        ud_status s = ud_scalar_text(ctx, f, (ud_string){v->text, v->text_length}, 0, &copy, &n);
        ud_free(copy);
        return s;
    }
    if (f->type == UD_BINARY && f->scalar.length && v->text_length > f->scalar.length)
        return UD_TRUNCATED;
    if (k == UD_DT_GUID && v->text_length != 16)
        return UD_PARSE;
    if (f->type == UD_DATETIME) {
        ud_datetime dt = v->datetime;
        if (dt.ticks < 0 || dt.ticks > INT64_C(3155378975999999999) || dt.has_offset < 0 ||
            dt.has_offset > 1 || dt.offset_minutes < -840 || dt.offset_minutes > 840 ||
            (!dt.has_offset && dt.offset_minutes))
            return UD_OVERFLOW;
        if (k && (k == UD_DT_DBTIMESTAMPOFFSET) != !!dt.has_offset)
            return UD_SCHEMA;
        if ((k == UD_DT_DBTIME || k == UD_DT_DBTIME2) && dt.ticks >= INT64_C(864000000000))
            return UD_OVERFLOW;
        if (k == UD_DT_DBDATE && dt.ticks % INT64_C(864000000000))
            return UD_TRUNCATED;
        uint64_t quantum = 1;
        for (uint32_t j = f->scalar.fractional_digits; j < 7; j++)
            quantum *= 10;
        if (k && k != UD_DT_DBDATE && (uint64_t)dt.ticks % quantum)
            return UD_TRUNCATED;
    }
    if (k == UD_DT_DATE || k == UD_DT_FILETIME) {
        ud_datetime dt;
        return ud_datetime_from_value(f, v, &dt);
    }
    return UD_OK;
}
static int hex(char c) {
    return c >= '0' && c <= '9'   ? c - '0'
           : c >= 'a' && c <= 'f' ? c - 'a' + 10
           : c >= 'A' && c <= 'F' ? c - 'A' + 10
                                  : -1;
}
static ud_status guid(ud_context *ctx, ud_string s, ud_value *out) {
    if (s.length == 38 && s.data[0] == '{' && s.data[37] == '}') {
        s.data++;
        s.length -= 2;
    }
    if (s.length != 36)
        return UD_PARSE;
    char *bytes = ud_alloc(ctx, 16);
    if (!bytes)
        return UD_RESOURCE;
    static const unsigned order[] = {3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15};
    uint32_t at = 0;
    for (uint32_t j = 0; j < 16; j++) {
        if (at == 8 || at == 13 || at == 18 || at == 23) {
            if (s.data[at++] != '-') {
                ud_free(bytes);
                return UD_PARSE;
            }
        }
        int a = hex(s.data[at++]), b = hex(s.data[at++]);
        if (a < 0 || b < 0) {
            ud_free(bytes);
            return UD_PARSE;
        }
        bytes[order[j]] = (char)(a * 16 + b);
    }
    out->type = UD_BINARY;
    out->text = bytes;
    out->text_length = 16;
    return UD_OK;
}
ud_status ud_value_parse_field(ud_context *ctx, const ud_field *f, const char *s, uint32_t n,
                               int32_t rounding, ud_value *out) {
    if (!ctx || !f || !s || !out)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    out->type = f->type;
    if (f->type == UD_DATETIME) {
        UD_TRY(ud_datetime_parse((ud_string){s, n}, f, &out->datetime));
        return ud_value_validate_field(ctx, f, out);
    }
    /* Textual input borrows its bytes. Code page validation never transcodes the
     * stored UTF8 representation or takes ownership from a CSV/XLSX reader. */
    UD_TRY(ud_parse_field_base(ctx, f, s, n, rounding, out));
    if (f->scalar.kind == UD_DT_R4) {
        if (isfinite(out->real) && !isfinite((float)out->real))
            return UD_OVERFLOW;
        out->real = (double)(float)out->real;
    }
    return ud_value_validate_field(ctx, f, out);
}
static int date_field(const ud_field *f) {
    return f->type == UD_DATETIME || f->type == UD_DATE32 || f->type == UD_TIMESTAMP ||
           f->type == UD_TIME64 || f->scalar.kind == UD_DT_DATE || f->scalar.kind == UD_DT_FILETIME;
}
static ud_status cast_field(ud_context *ctx, const ud_field *from, const ud_value *value,
                            const ud_field *to, int32_t rounding, int32_t truncate, ud_value *out,
                            uint32_t depth) {
    if (depth > 32)
        return UD_SCHEMA;
    if (!ctx || !from || !value || !to || !out || value->type != from->type)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    out->type = to->type;
    out->precision = to->precision;
    out->scale = to->scale;
    out->is_null = value->is_null;
    out->is_absent = value->is_absent;
    UD_TRY(ud_scalar_field_validate(to));
    if (value->is_null)
        return to->nullable || value->is_absent ? UD_OK : UD_SCHEMA;
    ud_status s = UD_OK;
    if (to->scalar.kind == UD_DT_GUID && from->type == UD_UTF8)
        s = guid(ctx, (ud_string){value->text, value->text_length}, out);
    else if (from->scalar.kind == UD_DT_GUID && to->type == UD_UTF8) {
        static const char hexes[] = "0123456789ABCDEF";
        static const unsigned order[] = {3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15};
        if (value->text_length != 16)
            return UD_SCHEMA;
        char *text = ud_alloc(ctx, 37);
        if (!text)
            return UD_RESOURCE;
        uint32_t at = 0;
        for (uint32_t j = 0; j < 16; j++) {
            if (j == 4 || j == 6 || j == 8 || j == 10)
                text[at++] = '-';
            unsigned byte = (unsigned char)value->text[order[j]];
            text[at++] = hexes[byte >> 4];
            text[at++] = hexes[byte & 15];
        }
        text[at] = 0;
        out->text = text;
        out->text_length = 36;
    } else if (date_field(to) && from->type == UD_UTF8) {
        ud_datetime dt;
        s = ud_datetime_parse((ud_string){value->text, value->text_length}, to, &dt);
        if (!s)
            s = ud_datetime_to_value(to, dt, out);
    } else if (date_field(from) && date_field(to)) {
        ud_datetime dt;
        s = ud_datetime_from_value(from, value, &dt);
        if (!s)
            s = ud_datetime_to_value(to, dt, out);
    } else if (from->type == UD_DATETIME && to->type == UD_UTF8) {
        char buffer[64];
        uint32_t required;
        s = ud_datetime_format(value->datetime, buffer, sizeof(buffer), &required);
        if (!s) {
            const char *start = buffer;
            uint32_t n = required - 1;
            if (from->scalar.kind == UD_DT_DBDATE)
                n = 10;
            else if (from->scalar.kind == UD_DT_DBTIME || from->scalar.kind == UD_DT_DBTIME2) {
                start += 11;
                n -= 11;
            }
            out->text = ud_strdup_n(ctx, start, n);
            out->text_length = n;
            if (!out->text)
                s = UD_RESOURCE;
        }
    } else if (from->type == UD_BOOL && to->type == UD_INT64 && to->scalar.kind) {
        out->integer = value->integer ? -1 : 0;
    } else if (to->type == UD_BOOL && to->scalar.kind &&
               (from->type == UD_INT64 || from->type == UD_UINT64 || from->type == UD_FLOAT64)) {
        out->integer = from->type == UD_FLOAT64 ? value->real != 0 : value->unsigned_integer != 0;
    } else if (from->type == UD_DECIMAL128 && to->type == UD_FLOAT64 && to->scalar.kind) {
        char buffer[96];
        uint32_t n;
        s = ud_value_format(value, buffer, sizeof(buffer), &n);
        if (!s)
            s = ud_value_parse(ctx, UD_FLOAT64, buffer, n - 1, out);
    } else
        s = ud_cast_value_base(ctx, from, value, to, rounding, out, depth);
    if (!s && to->scalar.kind == UD_DT_R4) {
        if (isfinite(out->real) && !isfinite((float)out->real))
            s = UD_OVERFLOW;
        else
            out->real = (double)(float)out->real;
    }
    if (!s && to->type == UD_DATETIME && truncate && to->scalar.kind != UD_DT_DBDATE) {
        int64_t q = 1;
        for (uint32_t j = to->scalar.fractional_digits; j < 7; j++)
            q *= 10;
        out->datetime.ticks = out->datetime.ticks / q * q;
    }
    if (!s && to->type == UD_UTF8 && to->scalar.kind) {
        char *text = NULL;
        uint32_t n = 0;
        s = ud_scalar_text(ctx, to, (ud_string){out->text, out->text_length}, truncate, &text, &n);
        if (!s) {
            ud_free((void *)out->text);
            out->text = text;
            out->text_length = n;
        }
    }
    if (!s && to->type == UD_BINARY && to->scalar.length && out->text_length > to->scalar.length &&
        truncate)
        out->text_length = to->scalar.length;
    if (!s)
        s = ud_value_validate_field(ctx, to, out);
    if (s)
        ud_clear_value(out);
    return s;
}
ud_status ud_cast_value(ud_context *ctx, const ud_field *from, const ud_value *value,
                        const ud_field *to, int32_t rounding, ud_value *out, uint32_t depth) {
    return cast_field(ctx, from, value, to, rounding, 0, out, depth);
}

ud_status ud_value_cast_field(ud_context *ctx, const ud_field *from, const ud_value *value,
                              const ud_field *to, int32_t rounding, int32_t truncate,
                              ud_value *out) {
    return cast_field(ctx, from, value, to, rounding, truncate, out, 0);
}
