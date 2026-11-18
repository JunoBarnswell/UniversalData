#include "../internal.h"
#include <string.h>
#include <math.h>
#include <stdlib.h>
#include "../../foundation/thread.h"
#include <unicode/ucol.h>
#include <unicode/uchar.h>
#include <unicode/unorm2.h>
#include <unicode/utf16.h>
#include <unicode/uloc.h>
#include <unicode/ustring.h>
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

#ifndef _WIN32
typedef struct collation_cache {
    struct collation_cache *next;
    uint32_t lcid, flags;
    UCollator *collator;
} collation_cache;

static ud_mutex collators_lock = UD_MUTEX_INITIALIZER;
static collation_cache *collators;

static const UCollator *cached_collator(uint32_t lcid, uint32_t flags) {
    uint32_t sort = flags & UD_COMPARE_STRING_SORT;
    ud_mutex_lock(&collators_lock);
    collation_cache *entry = collators;
    while (entry && (entry->lcid != lcid || entry->flags != sort))
        entry = entry->next;
    if (!entry) {
        UErrorCode error = U_ZERO_ERROR;
        char locale[ULOC_FULLNAME_CAPACITY] = "";
        if (lcid)
            uloc_getLocaleForLCID(lcid, locale, sizeof(locale), &error);
        UCollator *collator = U_SUCCESS(error) ? ucol_open(locale, &error) : NULL;
        if (collator && U_SUCCESS(error)) {
            ucol_setAttribute(collator, UCOL_NORMALIZATION_MODE, UCOL_ON, &error);
            ucol_setAttribute(collator, UCOL_ALTERNATE_HANDLING,
                              sort ? UCOL_NON_IGNORABLE : UCOL_SHIFTED, &error);
            ucol_setStrength(collator, UCOL_IDENTICAL);
            entry = U_SUCCESS(error) ? malloc(sizeof(*entry)) : NULL;
        }
        if (entry) {
            *entry = (collation_cache){collators, lcid, sort, collator};
            collators = entry;
        } else if (collator)
            ucol_close(collator);
    }
    const UCollator *result = entry ? entry->collator : NULL;
    ud_mutex_unlock(&collators_lock);
    return result;
}

/* ICU's strength levels combine case, kana, and width. Fold each requested
 * distinction explicitly so one flag cannot also discard another distinction. */
static UChar *collation_text(const ud_scalar_metadata *policy, const ud_value *value,
                            UChar scratch[256], int32_t *units) {
    uint32_t flags = policy->comparison_flags;
    if (value->text_length >= INT32_MAX)
        return NULL;
    int32_t capacity = (int32_t)value->text_length + 1;
    UChar *text = capacity <= 256 ? scratch : malloc((size_t)capacity * sizeof(*text));
    if (!text)
        return NULL;
    UErrorCode error = U_ZERO_ERROR;
    u_strFromUTF8(text, capacity, units, value->text ? value->text : "",
                  (int32_t)value->text_length, &error);
    if (U_SUCCESS(error) && (flags & UD_COMPARE_IGNORE_WIDTH)) {
        const UNormalizer2 *compatibility = unorm2_getNFKCInstance(&error);
        for (int32_t i = 0; U_SUCCESS(error) && i < *units; i++) {
            int32_t width = u_getIntPropertyValue(text[i], UCHAR_DECOMPOSITION_TYPE);
            if (width == U_DT_NARROW || width == U_DT_WIDE) {
                UChar equivalent[2];
                int32_t n = unorm2_getRawDecomposition(compatibility, text[i], equivalent, 2,
                                                        &error);
                if (n == 1)
                    text[i] = equivalent[0];
            }
        }
    }
    if (U_SUCCESS(error) && (flags & UD_COMPARE_IGNORE_KANA)) {
        for (int32_t i = 0; i < *units; i++)
            if ((text[i] >= 0x30a1 && text[i] <= 0x30f6) || text[i] == 0x30fd ||
                text[i] == 0x30fe)
                text[i] -= 0x60;
    }
    if (U_SUCCESS(error) && (flags & (UD_COMPARE_IGNORE_CASE | UD_COMPARE_LINGUISTIC_IGNORE_CASE))) {
        char locale[ULOC_FULLNAME_CAPACITY] = "";
        if (policy->locale_id)
            uloc_getLocaleForLCID(policy->locale_id, locale, sizeof(locale), &error);
        UChar local[256];
        UChar *folded = local;
        uint32_t turkic = !strncmp(locale, "tr", 2) || !strncmp(locale, "az", 2);
        int32_t n = u_strFoldCase(local, 256, text, *units, turkic ? U_FOLD_CASE_EXCLUDE_SPECIAL_I
                                                                  : U_FOLD_CASE_DEFAULT, &error);
        if (error == U_BUFFER_OVERFLOW_ERROR && n < INT32_MAX) {
            folded = malloc(((size_t)n + 1) * sizeof(*folded));
            if (folded) {
                error = U_ZERO_ERROR;
                u_strFoldCase(folded, n + 1, text, *units, turkic ? U_FOLD_CASE_EXCLUDE_SPECIAL_I
                                                                 : U_FOLD_CASE_DEFAULT, &error);
            }
        }
        if (U_SUCCESS(error)) {
            if (n >= capacity) {
                if (text != scratch)
                    free(text);
                capacity = n + 1;
                text = malloc((size_t)capacity * sizeof(*text));
                if (!text)
                    error = U_MEMORY_ALLOCATION_ERROR;
            }
            if (text)
                memcpy(text, folded, (size_t)n * sizeof(*text));
            *units = n;
        }
        if (folded != local)
            free(folded);
    }
    if (U_SUCCESS(error) &&
        (flags & (UD_COMPARE_IGNORE_NONSPACE | UD_COMPARE_LINGUISTIC_IGNORE_DIACRITIC))) {
        const UNormalizer2 *canonical = unorm2_getNFDInstance(&error);
        int32_t n = U_SUCCESS(error) ? unorm2_normalize(canonical, text, *units, NULL, 0, &error) : 0;
        if (error == U_BUFFER_OVERFLOW_ERROR)
            error = U_ZERO_ERROR;
        if (U_SUCCESS(error) && n) {
            UChar stack[256];
            UChar *normalized = n <= 256 ? stack : malloc((size_t)n * sizeof(*normalized));
            if (!normalized)
                error = U_MEMORY_ALLOCATION_ERROR;
            else {
                unorm2_normalize(canonical, text, *units, normalized, n, &error);
                if (n > capacity && U_SUCCESS(error)) {
                    if (text != scratch)
                        free(text);
                    text = malloc((size_t)n * sizeof(*text));
                    if (!text)
                        error = U_MEMORY_ALLOCATION_ERROR;
                }
                if (U_SUCCESS(error)) {
                    memcpy(text, normalized, (size_t)n * sizeof(*text));
                    *units = n;
                }
            }
            if (normalized != stack)
                free(normalized);
        }
    }
    if (U_FAILURE(error)) {
        if (text != scratch)
            free(text);
        return NULL;
    }
    if (flags & (UD_COMPARE_IGNORE_NONSPACE | UD_COMPARE_LINGUISTIC_IGNORE_DIACRITIC |
                 UD_COMPARE_IGNORE_SYMBOLS)) {
        int32_t end = 0;
        for (int32_t i = 0; i < *units;) {
            UChar32 cp;
            U16_NEXT(text, i, *units, cp);
            uint32_t mask = U_GET_GC_MASK(cp);
            if ((flags & UD_COMPARE_IGNORE_SYMBOLS) &&
                (mask & (U_GC_P_MASK | U_GC_S_MASK | U_GC_Z_MASK) || u_isUWhiteSpace(cp)))
                continue;
            if (((flags & UD_COMPARE_IGNORE_NONSPACE) && (mask & U_GC_MN_MASK)) ||
                ((flags & UD_COMPARE_LINGUISTIC_IGNORE_DIACRITIC) &&
                 u_hasBinaryProperty(cp, UCHAR_DIACRITIC)))
                continue;
            U16_APPEND_UNSAFE(text, end, cp);
        }
        *units = end;
    }
    return text;
}
#endif

typedef struct text_key {
    _Alignas(uint16_t) uint8_t inline_bytes[512];
    uint8_t *bytes;
    uint32_t length;
} text_key;

static void field_text_key(const ud_scalar_metadata *policy, const ud_value *value, text_key *key) {
    UChar wide_buffer[256];
    key->bytes = key->inline_bytes;
#ifdef _WIN32
    UErrorCode error = U_ZERO_ERROR;
    int32_t units = 0;
    UChar *wide = wide_buffer;
    if (value->text_length >= 256)
        wide = malloc(((size_t)value->text_length + 1) * sizeof(*wide));
    int32_t n = 0;
    WCHAR locale[LOCALE_NAME_MAX_LENGTH] = {0};
    if (wide && value->text_length < INT32_MAX &&
        (!policy->locale_id || LCIDToLocaleName(policy->locale_id, locale, LOCALE_NAME_MAX_LENGTH,
                                                LOCALE_ALLOW_NEUTRAL_NAMES))) {
        u_strFromUTF8(wide, value->text_length < 256 ? 256 : (int32_t)value->text_length + 1,
                      &units, value->text ? value->text : "", (int32_t)value->text_length, &error);
        if (U_SUCCESS(error)) {
            DWORD flags = LCMAP_SORTKEY | policy->comparison_flags;
            n = LCMapStringEx(locale, flags, (const WCHAR *)wide, units ? units : -1,
                              (WCHAR *)key->inline_bytes, sizeof(key->inline_bytes), NULL, NULL, 0);
            if (!n && GetLastError() == ERROR_INSUFFICIENT_BUFFER) {
                n = LCMapStringEx(locale, flags, (const WCHAR *)wide, units ? units : -1,
                                  NULL, 0, NULL, NULL, 0);
                if (n)
                    key->bytes = malloc((size_t)n);
                n = key->bytes ? LCMapStringEx(locale, flags, (const WCHAR *)wide, units ? units : -1,
                                               (WCHAR *)key->bytes, n, NULL, NULL, 0) : 0;
            }
        }
    }
    if (wide != wide_buffer)
        free(wide);
#else
    int32_t n = 0, units = 0;
    UChar *wide = collation_text(policy, value, wide_buffer, &units);
    const UCollator *collator = cached_collator(policy->locale_id, policy->comparison_flags);
    if (wide && collator) {
        n = ucol_getSortKey(collator, wide, units, key->inline_bytes, sizeof(key->inline_bytes));
        if ((size_t)n > sizeof(key->inline_bytes)) {
            key->bytes = malloc((size_t)n);
            n = key->bytes ? ucol_getSortKey(collator, wide, units, key->bytes, n) : 0;
        }
    }
    if (wide != wide_buffer)
        free(wide);
#endif
    key->length = n > 0 ? (uint32_t)n : 0;
}

static void clear_text_key(text_key *key) {
    if (key->bytes != key->inline_bytes)
        free(key->bytes);
}

int32_t ud_field_value_compare(const ud_field *af, const ud_value *a,
                               const ud_field *bf, const ud_value *b) {
    if (!a->is_null && !b->is_null && af->scalar.kind == UD_DT_DATE &&
        bf->scalar.kind == UD_DT_DATE) {
        ud_value x = {.type = UD_DATETIME}, y = {.type = UD_DATETIME};
        if (!ud_datetime_from_value(af, a, &x.datetime) &&
            !ud_datetime_from_value(bf, b, &y.datetime))
            return ud_value_compare(&x, &y);
    }
    if (a->is_null || b->is_null || a->type != UD_UTF8 || b->type != UD_UTF8)
        return ud_value_compare(a, b);
    const ud_scalar_metadata *ap = &af->scalar, *bp = &bf->scalar;
    if (ap->locale_id != bp->locale_id)
        return (ap->locale_id > bp->locale_id) - (ap->locale_id < bp->locale_id);
    if (ap->comparison_flags != bp->comparison_flags)
        return (ap->comparison_flags > bp->comparison_flags) -
               (ap->comparison_flags < bp->comparison_flags);
    if (!ap->locale_id && !ap->comparison_flags)
        return ud_value_compare(a, b);
    text_key x, y;
    field_text_key(ap, a, &x);
    field_text_key(bp, b, &y);
    int cmp;
    if (!x.length && !y.length)
        cmp = ud_value_compare(a, b);
    else {
        uint32_t size = x.length < y.length ? x.length : y.length;
        cmp = size ? memcmp(x.bytes, y.bytes, size) : 0;
        if (!cmp)
            cmp = (x.length > y.length) - (x.length < y.length);
    }
    clear_text_key(&x);
    clear_text_key(&y);
    return (cmp > 0) - (cmp < 0);
}

uint64_t ud_field_value_hash(const ud_field *field, const ud_value *value, uint64_t seed) {
    if (!value->is_null && field->scalar.kind == UD_DT_DATE) {
        ud_value date = {.type = UD_DATETIME};
        if (!ud_datetime_from_value(field, value, &date.datetime))
            return ud_value_hash(&date, seed);
    }
    if (value->is_null || value->type != UD_UTF8 ||
        (!field->scalar.locale_id && !field->scalar.comparison_flags))
        return ud_value_hash(value, seed);
    text_key key;
    field_text_key(&field->scalar, value, &key);
    if (!key.length)
        seed = ud_value_hash(value, seed);
    for (uint32_t i = 0; i < key.length; i++)
        seed = hash_byte(seed, key.bytes[i]);
    clear_text_key(&key);
    return seed;
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
