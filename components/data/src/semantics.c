#include "../internal.h"
#include <string.h>
#include <math.h>
int32_t ud_truth_and(int32_t a, int32_t b) {
    return !a || !b ? 0 : a < 0 || b < 0 ? -1 : 1;
}
int32_t ud_truth_or(int32_t a, int32_t b) {
    return a > 0 || b > 0 ? 1 : a < 0 || b < 0 ? -1 : 0;
}
int32_t ud_truth_not(int32_t a) {
    return a < 0 ? -1 : !a;
}
int32_t ud_value_compare(const ud_value *a, const ud_value *b) {
    if (a->is_absent || b->is_absent)
        return a->is_absent && b->is_absent ? 0 : a->is_absent ? -1 : 1;
    if (a->is_null || b->is_null)
        return a->is_null && b->is_null ? 0 : a->is_null ? -1 : 1;
    if (a->type == UD_UTF8 || a->type == UD_BINARY) {
        uint32_t n = a->text_length < b->text_length ? a->text_length : b->text_length;
        int v = n ? memcmp(a->text, b->text, n) : 0;
        return v ? (v > 0 ? 1 : -1)
                 : (a->text_length > b->text_length) - (a->text_length < b->text_length);
    }
    if (a->type == UD_DATETIME) {
        if (a->datetime.has_offset != b->datetime.has_offset)
            return (a->datetime.has_offset > b->datetime.has_offset) ? 1 : -1;
        int64_t x = a->datetime.ticks - (int64_t)a->datetime.offset_minutes * 600000000,
                y = b->datetime.ticks - (int64_t)b->datetime.offset_minutes * 600000000;
        return (x > y) - (x < y);
    }
    if (a->type == UD_FLOAT64) {
        if (isnan(a->real) || isnan(b->real))
            return isnan(a->real) ? (isnan(b->real) ? 0 : 1) : -1;
        return (a->real > b->real) - (a->real < b->real);
    }
    if (a->type == UD_DECIMAL128)
        return ud_decimal_compare(a->decimal, b->decimal);
    if (a->type == UD_UINT64)
        return (a->unsigned_integer > b->unsigned_integer) -
               (a->unsigned_integer < b->unsigned_integer);
    if (a->type == UD_INTERVAL) {
        if (a->interval.months != b->interval.months)
            return (a->interval.months > b->interval.months) -
                   (a->interval.months < b->interval.months);
        if (a->interval.days != b->interval.days)
            return (a->interval.days > b->interval.days) - (a->interval.days < b->interval.days);
        return (a->interval.microseconds > b->interval.microseconds) -
               (a->interval.microseconds < b->interval.microseconds);
    }
    if (a->type >= UD_STRUCT) {
        uint32_t count = a->child_count < b->child_count ? a->child_count : b->child_count;
        for (uint32_t j = 0; j < count; j++) {
            int cmp = ud_value_compare(a->children + j, b->children + j);
            if (cmp)
                return cmp;
        }
        return (a->child_count > b->child_count) - (a->child_count < b->child_count);
    }
    return (a->integer > b->integer) - (a->integer < b->integer);
}
static uint64_t hash_byte(uint64_t hash, uint8_t byte) {
    return (hash ^ byte) * UINT64_C(1099511628211);
}
static ud_status canonical_integer(ud_status (*emit)(void *, const void *, size_t), void *user,
                                   uint64_t v) {
    uint8_t bytes[8];
    for (uint32_t i = 0; i < 8; i++)
        bytes[i] = (uint8_t)(v >> (8 * i));
    return emit(user, bytes, 8);
}
ud_status ud_value_canonical(const ud_value *v, ud_status (*emit)(void *, const void *, size_t),
                             void *user, uint32_t depth) {
    if (!v || !emit || depth > 32)
        return UD_SCHEMA;
    UD_TRY(canonical_integer(emit, user, (uint64_t)v->type));
    uint8_t absent = (uint8_t)!!v->is_absent;
    UD_TRY(emit(user, &absent, 1));
    uint8_t null = (uint8_t)!!v->is_null;
    UD_TRY(emit(user, &null, 1));
    if (null)
        return UD_OK;
    if (v->type == UD_UTF8 || v->type == UD_BINARY) {
        UD_TRY(canonical_integer(emit, user, v->text_length));
        return emit(user, v->text, v->text_length);
    }
    if (v->type >= UD_STRUCT) {
        UD_TRY(canonical_integer(emit, user, v->child_count));
        for (uint32_t i = 0; i < v->child_count; i++)
            UD_TRY(ud_value_canonical(v->children + i, emit, user, depth + 1));
        return UD_OK;
    }
    if (v->type == UD_DECIMAL128) {
        UD_TRY(canonical_integer(emit, user, v->decimal.low));
        return canonical_integer(emit, user, (uint64_t)v->decimal.high);
    }
    if (v->type == UD_DATETIME) {
        UD_TRY(canonical_integer(emit, user, (uint64_t)v->datetime.has_offset));
        return canonical_integer(
            emit, user,
            (uint64_t)(v->datetime.ticks - (int64_t)v->datetime.offset_minutes * 600000000));
    }
    if (v->type == UD_INTERVAL) {
        UD_TRY(canonical_integer(emit, user, (uint64_t)(int64_t)v->interval.months));
        UD_TRY(canonical_integer(emit, user, (uint64_t)(int64_t)v->interval.days));
        return canonical_integer(emit, user, (uint64_t)v->interval.microseconds);
    }
    if (v->type == UD_UINT64)
        return canonical_integer(emit, user, v->unsigned_integer);
    uint64_t bits = (uint64_t)v->integer;
    if (v->type == UD_FLOAT64) {
        bits = 0;
        if (isnan(v->real))
            bits = UINT64_C(0x7ff8000000000000);
        else if (v->real != 0)
            memcpy(&bits, &v->real, 8);
    }
    return canonical_integer(emit, user, bits);
}
static ud_status hash_bytes(void *user, const void *bytes, size_t length) {
    uint64_t *hash = user;
    const uint8_t *p = bytes;
    for (size_t i = 0; i < length; i++)
        *hash = hash_byte(*hash, p[i]);
    return UD_OK;
}
uint64_t ud_value_hash(const ud_value *v, uint64_t hash) {
    ud_value_canonical(v, hash_bytes, &hash, 0);
    return hash;
}

static int numeric(const ud_field *f) {
    return !f->dictionary_kind && f->scalar.kind != UD_DT_DATE &&
           f->scalar.kind != UD_DT_FILETIME &&
           (f->type == UD_INT64 || f->type == UD_UINT64 || f->type == UD_FLOAT64 ||
            f->type == UD_DECIMAL128);
}
static unsigned bits(const ud_field *f) {
    uint32_t k = f->scalar.kind;
    return k >= UD_DT_I1 && k <= UD_DT_I4     ? 8u << (k - UD_DT_I1)
           : k >= UD_DT_UI1 && k <= UD_DT_UI4 ? 8u << (k - UD_DT_UI1)
                                              : 64;
}
static int digits(const ud_field *f) {
    return f->type == UD_DECIMAL128 ? f->precision
           : f->type == UD_UINT64   ? (bits(f) == 64   ? 20
                                       : bits(f) == 32 ? 10
                                       : bits(f) == 16 ? 5
                                                       : 3)
           : bits(f) == 64          ? 19
           : bits(f) == 32          ? 10
           : bits(f) == 16          ? 5
                                    : 3;
}
ud_status ud_scalar_common(const ud_field *a, const ud_field *b, ud_field *out) {
    if (!a || !b || !out)
        return UD_INVALID;
    ud_field left = *a, right = *b;
    a = &left;
    b = &right;
    *out = *a;
    out->name = "value";
    out->name_length = 5;
    out->identity = (ud_id){0};
    out->physical_identity = (ud_id){0};
    out->lineage = NULL;
    out->lineage_count = 0;
    out->key_order = 0;
    out->nullable = a->nullable || b->nullable;
    if (a->type == UD_UTF8 && b->type == UD_UTF8) {
        out->scalar = (ud_scalar_metadata){0};
        if (a->scalar.kind && b->scalar.kind) {
            out->scalar.kind = !a->scalar.length || !b->scalar.length ? UD_DT_NTEXT : UD_DT_WSTR;
            out->scalar.length =
                a->scalar.length > b->scalar.length ? a->scalar.length : b->scalar.length;
            out->scalar.locale_id = a->scalar.locale_id ? a->scalar.locale_id : b->scalar.locale_id;
        }
        return UD_OK;
    }
    if (a->type == UD_BOOL && b->type == UD_BOOL) {
        out->scalar =
            (ud_scalar_metadata){.kind = (a->scalar.kind || b->scalar.kind) ? UD_DT_BOOL : 0};
        return UD_OK;
    }
    if (a->type == UD_DATETIME && b->type == UD_DATETIME) {
        if ((a->scalar.kind == UD_DT_DBTIMESTAMPOFFSET) !=
            (b->scalar.kind == UD_DT_DBTIMESTAMPOFFSET))
            return UD_SCHEMA;
        if (!a->scalar.kind || !b->scalar.kind)
            out->scalar = (ud_scalar_metadata){0};
        else {
            int time_a = a->scalar.kind == UD_DT_DBTIME || a->scalar.kind == UD_DT_DBTIME2,
                time_b = b->scalar.kind == UD_DT_DBTIME || b->scalar.kind == UD_DT_DBTIME2;
            if (time_a != time_b)
                return UD_SCHEMA;
            out->scalar.kind = a->scalar.kind == UD_DT_DBTIMESTAMPOFFSET ? UD_DT_DBTIMESTAMPOFFSET
                               : time_a                                  ? UD_DT_DBTIME2
                                                                         : UD_DT_DBTIMESTAMP2;
            out->scalar.fractional_digits =
                a->scalar.fractional_digits > b->scalar.fractional_digits
                    ? a->scalar.fractional_digits
                    : b->scalar.fractional_digits;
        }
        return UD_OK;
    }
    if (!numeric(a) || !numeric(b)) {
        return ud_field_matches(a, b, UD_FIELD_VALUE_TYPE) ? UD_OK : UD_SCHEMA;
    }
    out->scalar = (ud_scalar_metadata){0};
    out->precision = out->scale = 0;
    if (a->type == UD_FLOAT64 || b->type == UD_FLOAT64) {
        out->type = UD_FLOAT64;
        if (a->scalar.kind || b->scalar.kind)
            out->scalar.kind =
                a->scalar.kind == UD_DT_R4 && b->scalar.kind == UD_DT_R4 ? UD_DT_R4 : UD_DT_R8;
    } else if (a->type == UD_DECIMAL128 || b->type == UD_DECIMAL128) {
        out->type = UD_DECIMAL128;
        out->scale = a->scale > b->scale ? a->scale : b->scale;
        int whole = digits(a) - a->scale > digits(b) - b->scale ? digits(a) - a->scale
                                                                : digits(b) - b->scale;
        out->precision = whole + out->scale;
        if (out->precision > 38)
            return UD_OVERFLOW;
        if (a->scalar.kind || b->scalar.kind)
            out->scalar.kind = UD_DT_NUMERIC;
    } else {
        unsigned width = bits(a) > bits(b) ? bits(a) : bits(b);
        int sign = a->type == UD_INT64 || b->type == UD_INT64;
        if (a->type != b->type) {
            unsigned uns = a->type == UD_UINT64 ? bits(a) : bits(b),
                     signed_bits = a->type == UD_INT64 ? bits(a) : bits(b);
            if (uns >= signed_bits) {
                if (uns == 64)
                    return UD_SCHEMA;
                width = uns * 2;
            }
        }
        out->type = sign ? UD_INT64 : UD_UINT64;
        if (a->scalar.kind || b->scalar.kind)
            out->scalar.kind = (sign ? UD_DT_I1 : UD_DT_UI1) + (width == 8    ? 0
                                                                : width == 16 ? 1
                                                                : width == 32 ? 2
                                                                              : 3);
    }
    return UD_OK;
}
