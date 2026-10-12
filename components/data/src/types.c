#include "../internal.h"
#include <math.h>
#include <locale.h>
#include <errno.h>

/* No compiler extensions, floating point or process locale in Decimal operations. */
static int negative(ud_decimal128 x) { return x.high < 0; }
static int nonzero(ud_decimal128 x) { return x.low || x.high; }
static ud_decimal128 negate(ud_decimal128 x) {
    x.low = ~x.low + 1;
    uint64_t hi = ~(uint64_t)x.high + (x.low == 0);
    memcpy(&x.high, &hi, sizeof(hi));
    return x;
}
static void limbs(ud_decimal128 x, uint32_t d[4]) {
    if (negative(x)) x = negate(x);
    d[0] = (uint32_t)x.low; d[1] = (uint32_t)(x.low >> 32);
    d[2] = (uint32_t)(uint64_t)x.high; d[3] = (uint32_t)((uint64_t)x.high >> 32);
}
static ud_decimal128 unlimbs(const uint32_t d[4], int neg) {
    ud_decimal128 x = {(uint64_t)d[0] | ((uint64_t)d[1] << 32), 0};
    uint64_t hi = (uint64_t)d[2] | ((uint64_t)d[3] << 32);
    memcpy(&x.high, &hi, 8);
    return neg && nonzero(x) ? negate(x) : x;
}
static uint32_t coefficient(ud_decimal128 x, char out[40]) {
    uint32_t d[4]; limbs(x, d);
    char reverse[40]; uint32_t n = 0;
    do {
        uint64_t rem = 0;
        for (int i = 3; i >= 0; i--) {
            uint64_t v = (rem << 32) | d[i];
            d[i] = (uint32_t)(v / 10); rem = v % 10;
        }
        reverse[n++] = (char)('0' + rem);
    } while (d[0] || d[1] || d[2] || d[3]);
    for (uint32_t i = 0; i < n; i++) out[i] = reverse[n - 1 - i];
    out[n] = 0;
    return n;
}
int32_t ud_decimal_compare(ud_decimal128 a, ud_decimal128 b) {
    if (a.high != b.high) return a.high > b.high ? 1 : -1;
    return (a.low > b.low) - (a.low < b.low);
}
ud_status ud_decimal_parse(const char *text, uint32_t length, int32_t precision, int32_t scale,
                           int32_t rounding, ud_decimal128 *out) {
    if (!out || !text || !length || precision < 1 || precision > 38 || scale < -38 || scale > 38 ||
        rounding < UD_ROUND_EXACT || rounding > UD_ROUND_CEILING) return UD_INVALID;
    /* Length is an explicit parser resource limit, unrelated to numeric precision. */
    if (length > 4096) return UD_RESOURCE;
    char digits[4097]; int n = 0, fraction = 0, dot = 0, seen = 0, neg = 0;
    uint32_t i = 0;
    if (text[i] == '-' || text[i] == '+') neg = text[i++] == '-';
    for (; i < length; i++) {
        char ch = text[i];
        if (ch >= '0' && ch <= '9') {
            seen = 1; if (dot) fraction++;
            if (n || ch != '0') digits[n++] = ch;
        } else if (ch == '.' && !dot) dot = 1;
        else break;
    }
    if (!seen) return UD_PARSE;
    int exponent = 0, expneg = 0;
    if (i < length && (text[i] == 'e' || text[i] == 'E')) {
        i++;
        if (i < length && (text[i] == '-' || text[i] == '+')) expneg = text[i++] == '-';
        uint32_t first = i;
        for (; i < length && text[i] >= '0' && text[i] <= '9'; i++) {
            if (exponent < 100000) exponent = exponent * 10 + text[i] - '0';
        }
        if (i == first) return UD_PARSE;
    }
    if (i != length) return UD_PARSE;
    if (!n) { *out = (ud_decimal128){0, 0}; return UD_OK; }
    int shift = (expneg ? -exponent : exponent) - fraction + scale;
    int keep = n + shift;
    if (keep > precision) return UD_OVERFLOW;
    int discarded = 0, half = -1;
    if (shift < 0) {
        int first = keep > 0 ? keep : 0;
        for (int j = first; j < n; j++) if (digits[j] != '0') discarded = 1;
        if (keep >= 0 && keep < n) {
            half = digits[keep] < '5' ? -1 : digits[keep] > '5' ? 1 : 0;
            if (half == 0) for (int j = keep + 1; j < n; j++) if (digits[j] != '0') half = 1;
        }
    }
    if (discarded && rounding == UD_ROUND_EXACT) return UD_PARSE;
    char result[40]; int used = keep > 0 ? keep : 1;
    if (keep <= 0) result[0] = '0';
    else for (int j = 0; j < keep; j++) result[j] = j < n ? digits[j] : '0';
    int increment = discarded &&
        ((rounding == UD_ROUND_FLOOR && neg) || (rounding == UD_ROUND_CEILING && !neg) ||
         (rounding == UD_ROUND_HALF_AWAY && half >= 0) ||
         (rounding == UD_ROUND_HALF_EVEN && (half > 0 || (half == 0 && (result[used - 1] & 1)))));
    if (increment) {
        int j = used - 1;
        while (j >= 0 && result[j] == '9') result[j--] = '0';
        if (j >= 0) result[j]++;
        else { memmove(result + 1, result, (size_t)used); result[0] = '1'; used++; }
    }
    if (used > precision) return UD_OVERFLOW;
    uint32_t d[4] = {0};
    for (int j = 0; j < used; j++) {
        uint64_t carry = (uint64_t)(result[j] - '0');
        for (int k = 0; k < 4; k++) {
            uint64_t v = (uint64_t)d[k] * 10 + carry;
            d[k] = (uint32_t)v; carry = v >> 32;
        }
        if (carry || (d[3] & UINT32_C(0x80000000))) return UD_OVERFLOW;
    }
    *out = unlimbs(d, neg);
    return UD_OK;
}
ud_status ud_decimal_rescale(ud_decimal128 x, int32_t from, int32_t precision, int32_t to,
                             int32_t rounding, ud_decimal128 *out) {
    if (from < -38 || from > 38) return UD_INVALID;
    char number[40], text[64]; coefficient(x, number);
    int n = snprintf(text, sizeof(text), "%s%se%d", negative(x) ? "-" : "", number, -from);
    return ud_decimal_parse(text, (uint32_t)n, precision, to, rounding, out);
}
ud_status ud_decimal_add(ud_decimal128 a, ud_decimal128 b, int32_t precision, ud_decimal128 *out) {
    if (!out || precision < 1 || precision > 38) return UD_INVALID;
    ud_decimal128 x = {a.low + b.low, 0};
    uint64_t hi = (uint64_t)a.high + (uint64_t)b.high + (x.low < a.low);
    memcpy(&x.high, &hi, 8);
    if (negative(a) == negative(b) && negative(a) != negative(x)) return UD_OVERFLOW;
    char text[40];
    if (coefficient(x, text) > (uint32_t)precision) return UD_OVERFLOW;
    *out = x; return UD_OK;
}
static int leap(int y) { return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0); }
static int month_days(int y, int m) {
    static const int days[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    return m == 2 ? 28 + leap(y) : m >= 1 && m <= 12 ? days[m - 1] : 0;
}
static int digit_part(const char *s, int n) {
    int v = 0;
    for (int i = 0; i < n; i++) {
        if (s[i] < '0' || s[i] > '9') return -1;
        v = v * 10 + s[i] - '0';
    }
    return v;
}
static int64_t days_from_date(int y, int m, int d) {
    y -= m <= 2;
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned year = (unsigned)(y - era * 400);
    unsigned day = (153 * (unsigned)(m + (m > 2 ? -3 : 9)) + 2) / 5 + (unsigned)d - 1;
    return (int64_t)era * 146097 + year * 365 + year / 4 - year / 100 + day - 719468;
}
static void date_from_days(int64_t days, int *y, int *m, int *d) {
    int64_t z = days + 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int year = (int)(yoe + era * 400);
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    *d = (int)(doy - (153 * mp + 2) / 5 + 1);
    *m = (int)mp + (mp < 10 ? 3 : -9); *y = year + (*m <= 2);
}
ud_status ud_value_parse_field(ud_context *ctx, const ud_field *f, const char *s, uint32_t n,
                               int32_t rounding, ud_value *out) {
    if (!f || !s || !out) return UD_INVALID;
    memset(out, 0, sizeof(*out)); out->type = f->type;
    if (f->type == UD_BINARY) { out->text = s; out->text_length = n; return UD_OK; }
    if (f->type <= UD_UTF8) return ud_value_parse(ctx, f->type, s, n, out);
    if (f->type == UD_DECIMAL128) {
        out->precision = f->precision; out->scale = f->scale;
        return ud_decimal_parse(s, n, f->precision, f->scale, rounding, &out->decimal);
    }
    if (f->type != UD_DATE32 && f->type != UD_TIME64 && f->type != UD_TIMESTAMP) return UD_UNSUPPORTED;
    if (f->type == UD_TIMESTAMP && (!f->timezone || !f->timezone_length ||
        memchr(f->timezone, 0, f->timezone_length) || !ud_utf8_valid(f->timezone, f->timezone_length))) return UD_SCHEMA;
    uint32_t pos = 0; int64_t days = 0;
    if (f->type != UD_TIME64) {
        if (n < 10 || s[4] != '-' || s[7] != '-') return UD_PARSE;
        int y = digit_part(s, 4), m = digit_part(s + 5, 2), d = digit_part(s + 8, 2);
        if (y < 1 || m < 1 || m > 12 || d < 1 || d > month_days(y, m)) return UD_PARSE;
        days = days_from_date(y, m, d); pos = 10;
        if (f->type == UD_DATE32) {
            if (n != 10) return UD_PARSE;
            out->integer = days; return UD_OK;
        }
        if (pos == n || s[pos++] != 'T') return UD_PARSE;
    }
    if (n - pos < 8 || s[pos + 2] != ':' || s[pos + 5] != ':') return UD_PARSE;
    int hour = digit_part(s + pos, 2), min = digit_part(s + pos + 3, 2), sec = digit_part(s + pos + 6, 2);
    if (hour < 0 || hour > 23 || min < 0 || min > 59 || sec < 0 || sec > 59) return UD_PARSE;
    pos += 8; int64_t micros = 0; int digits = 0;
    if (pos < n && s[pos] == '.') {
        pos++;
        while (pos < n && s[pos] >= '0' && s[pos] <= '9') {
            if (++digits > 6) return UD_PARSE;
            micros = micros * 10 + s[pos++] - '0';
        }
        if (!digits) return UD_PARSE;
    }
    while (digits++ < 6) micros *= 10;
    int offset = 0;
    if (f->type == UD_TIMESTAMP) {
        if (pos == n) return UD_PARSE; /* No implicit local timezone. */
        if (s[pos] == 'Z') pos++;
        else if (s[pos] == '+' || s[pos] == '-') {
            int sign = s[pos++] == '-' ? -1 : 1;
            if (n - pos != 5 || s[pos + 2] != ':') return UD_PARSE;
            int h = digit_part(s + pos, 2), m = digit_part(s + pos + 3, 2);
            if (h < 0 || h > 14 || m < 0 || m > 59 || (h == 14 && m)) return UD_PARSE;
            offset = sign * (h * 60 + m); pos += 5;
        } else return UD_PARSE;
    }
    if (pos != n) return UD_PARSE;
    out->integer = days * INT64_C(86400000000) +
        ((int64_t)hour * 3600 + min * 60 + sec - offset * 60) * 1000000 + micros;
    return UD_OK;
}
ud_status ud_value_format(const ud_value *v, char *out, uint32_t cap, uint32_t *required) {
    if (!v || !required || (!out && cap)) return UD_INVALID;
    char text[160]; uint32_t n = 0;
    const char *p = text;
    if (v->is_null) { p = "NULL"; n = 4; }
    else if (v->type == UD_UTF8 || v->type == UD_BINARY) { p = v->text; n = v->text_length; }
    else if (v->type == UD_BOOL) { if (v->integer != 0 && v->integer != 1) return UD_PARSE; p = v->integer ? "true" : "false"; n = (uint32_t)strlen(p); }
    else if (v->type == UD_INT64) n = (uint32_t)snprintf(text, sizeof(text), "%lld", (long long)v->integer);
    else if (v->type == UD_FLOAT64) {
        if (!isfinite(v->real)) return UD_OVERFLOW;
        int count;
#ifdef _WIN32
        _locale_t numeric=_create_locale(LC_NUMERIC,"C");if(!numeric) return UD_RESOURCE;
        count=_snprintf_l(text,sizeof(text),"%.17g",numeric,v->real);_free_locale(numeric);
#else
        locale_t numeric=newlocale(LC_NUMERIC_MASK,"C",(locale_t)0);if(!numeric) return UD_RESOURCE;
        locale_t old=uselocale(numeric);if(!old) { freelocale(numeric);return UD_RESOURCE; }
        count=snprintf(text,sizeof(text),"%.17g",v->real);uselocale(old);freelocale(numeric);
#endif
        if(count<0 || count>=(int)sizeof(text)) return UD_OVERFLOW;n=(uint32_t)count;
    } else if (v->type == UD_DECIMAL128) {
        if (v->precision < 1 || v->precision > 38 || v->scale < -38 || v->scale > 38) return UD_SCHEMA;
        char d[40]; int count = (int)coefficient(v->decimal, d);
        if (count > v->precision) return UD_OVERFLOW;
        if (negative(v->decimal)) text[n++] = '-';
        if (v->scale <= 0) {
            memcpy(text + n, d, (size_t)count); n += (uint32_t)count;
            for (int i = 0; i < -v->scale; i++) text[n++] = '0';
        } else {
            int whole = count - v->scale;
            if (whole > 0) { memcpy(text + n, d, (size_t)whole); n += (uint32_t)whole; }
            else text[n++] = '0';
            text[n++] = '.';
            for (int i = whole; i < 0; i++) text[n++] = '0';
            int start = whole > 0 ? whole : 0;
            memcpy(text + n, d + start, (size_t)(count - start)); n += (uint32_t)(count - start);
        }
    } else if (v->type == UD_DATE32 || v->type == UD_TIMESTAMP || v->type == UD_TIME64) {
        int64_t time = v->integer, days = 0;
        if (v->type == UD_DATE32) days = time;
        else if (v->type == UD_TIMESTAMP) {
            days = time / INT64_C(86400000000); time %= INT64_C(86400000000);
            if (time < 0) { time += INT64_C(86400000000); days--; }
        } else if (time < 0 || time >= INT64_C(86400000000)) return UD_OVERFLOW;
        if (v->type != UD_TIME64) {
            /* Restrict formatting to the public ISO grammar's 0001..9999 range. */
            if (days < -719162 || days > 2932896) return UD_OVERFLOW;
            int y, m, d; date_from_days(days, &y, &m, &d);
            n = (uint32_t)snprintf(text, sizeof(text), "%04d-%02d-%02d", y, m, d);
            if (v->type == UD_TIMESTAMP) text[n++] = 'T';
        }
        if (v->type != UD_DATE32) n += (uint32_t)snprintf(text + n, sizeof(text) - n,
            "%02d:%02d:%02d.%06d%s", (int)(time / 3600000000LL), (int)(time / 60000000 % 60),
            (int)(time / 1000000 % 60), (int)(time % 1000000), v->type == UD_TIMESTAMP ? "Z" : "");
    } else return UD_UNSUPPORTED;
    if (n == UINT32_MAX || (!p && n)) return UD_INVALID;
    *required = n + 1;
    if (cap <= n) return UD_RESOURCE;
    if (n) memcpy(out, p, n);
    out[n] = 0; return UD_OK;
}
static int temporal(ud_type t) { return t == UD_DATE32 || t == UD_TIME64 || t == UD_TIMESTAMP; }
static ud_status integer_to_float(int64_t value,int32_t rounding,double *out) {
    int negative=value<0;uint64_t magnitude=negative?UINT64_C(0)-(uint64_t)value:(uint64_t)value;
    uint32_t bits=0;for(uint64_t n=magnitude;n;n>>=1) bits++;
    if(bits<=53) { *out=(double)value;return UD_OK; }
    uint32_t shift=bits-53;uint64_t fraction=magnitude&((UINT64_C(1)<<shift)-1),whole=magnitude>>shift;
    if(fraction && rounding==UD_ROUND_EXACT) return UD_PARSE;
    int up=0;
    if(rounding==UD_ROUND_CEILING) up=!negative && fraction;
    else if(rounding==UD_ROUND_FLOOR) up=negative && fraction;
    else if(rounding==UD_ROUND_HALF_AWAY || rounding==UD_ROUND_HALF_EVEN) {
        uint64_t half=UINT64_C(1)<<(shift-1);up=fraction>half || (fraction==half && (rounding==UD_ROUND_HALF_AWAY || (whole&1)));
    }
    if(up) whole++;*out=ldexp((double)whole,(int)shift);if(negative) *out=-*out;return UD_OK;
}
static ud_status float_to_integer(double value,int32_t rounding,int64_t *out) {
    if(!isfinite(value) || value>=9223372036854775808.0 || value < -9223372036854775808.0) return UD_OVERFLOW;
    double whole=trunc(value),fraction=value-whole;
    if(rounding==UD_ROUND_EXACT && fraction!=0) return UD_PARSE;
    if(rounding==UD_ROUND_FLOOR && fraction<0) whole-=1;
    else if(rounding==UD_ROUND_CEILING && fraction>0) whole+=1;
    else if(rounding==UD_ROUND_HALF_EVEN || rounding==UD_ROUND_HALF_AWAY) {
        double magnitude=fabs(fraction);
        if(magnitude>0.5 || (magnitude==0.5 && (rounding==UD_ROUND_HALF_AWAY || fmod(fabs(whole),2.0)!=0))) whole+=value<0?-1:1;
    }
    if(whole>=9223372036854775808.0 || whole < -9223372036854775808.0) return UD_OVERFLOW;
    *out=(int64_t)whole;return UD_OK;
}
ud_status ud_cast_value(ud_context *ctx, const ud_field *from, const ud_value *v,
                             const ud_field *to, int32_t rounding, ud_value *out, uint32_t depth) {
    if (depth > 32 || v->type != from->type) return UD_SCHEMA;
    out->type = to->type; out->is_null = v->is_null;
    out->precision = to->precision; out->scale = to->scale;
    if (v->is_null) return to->nullable ? UD_OK : UD_SCHEMA;
    if (from->type >= UD_STRUCT || to->type >= UD_STRUCT) {
        if (from->type != to->type || from->child_count != to->child_count) return UD_SCHEMA;
        if ((v->child_count && !v->children) || (from->type==UD_STRUCT && v->child_count!=from->child_count)) return UD_SCHEMA;
        if (!v->child_count) return UD_OK;
        ud_value *children = ud_alloc(ctx, (size_t)v->child_count * sizeof(*children));
        if (!children) return UD_RESOURCE;
        out->children = children; out->child_count = v->child_count;
        for (uint32_t j = 0; j < v->child_count; j++) {
            uint32_t field = to->type == UD_STRUCT ? j : 0;
            UD_TRY(ud_cast_value(ctx, from->children + field, v->children + j,
                               to->children + field, rounding, children + j, depth + 1));
        }
        return UD_OK;
    }
    if (from->type == UD_DECIMAL128 && to->type == UD_DECIMAL128)
        return ud_decimal_rescale(v->decimal, from->scale, to->precision, to->scale, rounding, &out->decimal);
    if (from->type == UD_INT64 && to->type == UD_DECIMAL128) {
        ud_decimal128 x = {(uint64_t)v->integer, v->integer < 0 ? -1 : 0};
        return ud_decimal_rescale(x, 0, to->precision, to->scale, rounding, &out->decimal);
    }
    if (from->type == UD_DECIMAL128 && to->type == UD_INT64) {
        ud_decimal128 x;
        UD_TRY(ud_decimal_rescale(v->decimal, from->scale, 19, 0, rounding, &x));
        if ((x.high == 0 && x.low <= INT64_MAX) ||
            (x.high == -1 && x.low >= UINT64_C(0x8000000000000000))) {
            memcpy(&out->integer, &x.low, 8); return UD_OK;
        }
        return UD_OVERFLOW;
    }
    if(from->type==UD_INT64 && to->type==UD_FLOAT64) return integer_to_float(v->integer,rounding,&out->real);
    if(from->type==UD_FLOAT64 && to->type==UD_INT64) return float_to_integer(v->real,rounding,&out->integer);
    if (from->type == to->type ||
        ((from->type == UD_UTF8 || from->type == UD_BINARY) && (to->type == UD_UTF8 || to->type == UD_BINARY))) {
        UD_TRY(ud_copy_value(ctx, v, out, 0)); out->type = to->type;
        return UD_OK;
    }
    if ((temporal(from->type) && to->type == UD_INT64) || (from->type == UD_INT64 && temporal(to->type))) {
        out->integer = v->integer; return UD_OK;
    }
    if ((from->type == UD_BOOL && to->type == UD_INT64) || (from->type == UD_INT64 && to->type == UD_BOOL)) {
        if (to->type == UD_BOOL && v->integer != 0 && v->integer != 1) return UD_PARSE;
        out->integer = v->integer; return UD_OK;
    }
    if (from->type == UD_UTF8) {
        ud_value parsed;
        UD_TRY(ud_value_parse_field(ctx, to, v->text, v->text_length, rounding, &parsed));
        return ud_copy_value(ctx, &parsed, out, 0);
    }
    if (to->type != UD_UTF8 && ((temporal(from->type) || temporal(to->type)) ||
        from->type == UD_BINARY || to->type == UD_BINARY ||
        (from->type == UD_DECIMAL128 && to->type == UD_FLOAT64))) return UD_UNSUPPORTED;
    uint32_t required = 0;
    ud_status s = ud_value_format(v, NULL, 0, &required);
    if (s != UD_RESOURCE) return s;
    char *text = ud_alloc(ctx, required);
    if (!text) return UD_RESOURCE;
    s = ud_value_format(v, text, required, &required);
    ud_value parsed = {0};
    if (!s) s = ud_value_parse_field(ctx, to, text, required - 1, rounding, &parsed);
    if (!s) s = ud_copy_value(ctx, &parsed, out, 0);
    ud_free(text); return s;
}
ud_status ud_batch_cast(ud_context *ctx, const ud_batch *input, const ud_field *target,
                         uint32_t columns, int32_t rounding, ud_batch **out) {
    if (!out) return UD_INVALID;
    *out = NULL;
    if (!input || columns != input->columns || rounding < UD_ROUND_EXACT || rounding > UD_ROUND_CEILING) return UD_INVALID;
    UD_TRY(ud_batch_create(ctx, target, columns, input->rows, out));
    ud_value *values = ud_alloc(ctx, columns * sizeof(*values));
    ud_status s = values ? UD_OK : UD_RESOURCE;
    for (uint32_t r = 0; !s && r < input->rows; r++) {
        for (uint32_t j = 0; !s && j < columns; j++) {
            ud_value value; s = ud_batch_get(input, r, j, &value);
            if (!s) s = ud_cast_value(ctx, &input->column[j].field, &value, target + j, rounding, values + j, 0);
        }
        if (!s) s = ud_batch_append(*out, values, columns);
        for (uint32_t j = 0; j < columns; j++) ud_clear_value(values + j);
    }
    ud_free(values);
    if (s) { ud_batch_destroy(*out); *out = NULL; }
    return s;
}

ud_status ud_value_parse(ud_context *c, ud_type type, const char *text, uint32_t n, ud_value *out) {
    if (!out || !text || !ud_utf8_valid(text, n))
        return ud_fail(c, UD_PARSE, "invalid UTF-8 value");
    memset(out, 0, sizeof(*out));
    out->type = type;
    if (type == UD_UTF8) {
        out->text = text;
        out->text_length = n;
        return UD_OK;
    }
    if (!n || memchr(text, 0, n))
        return ud_fail(c, UD_PARSE, "empty or NUL numeric value");
    if (type == UD_BOOL) {
        if ((n == 4 && !memcmp(text, "true", 4)) || (n == 1 && *text == '1'))
            out->integer = 1;
        else if (!((n == 5 && !memcmp(text, "false", 5)) || (n == 1 && *text == '0')))
            return ud_fail(c, UD_PARSE, "invalid boolean");
        return UD_OK;
    }
    if (type == UD_INT64) {
        uint32_t i = 0;
        int neg = text[0] == '-';
        if (neg || text[0] == '+')
            i++;
        if (i == n)
            return UD_PARSE;
        uint64_t v = 0, limit = neg ? (uint64_t)INT64_MAX + 1 : INT64_MAX;
        for (; i < n; i++) {
            unsigned d = (unsigned char)text[i] - '0';
            if (d > 9)
                return ud_fail(c, UD_PARSE, "invalid int64");
            if (v > (limit - d) / 10)
                return ud_fail(c, UD_OVERFLOW, "int64 overflow");
            v = v * 10 + d;
        }
        out->integer = neg ? (v == (uint64_t)INT64_MAX + 1 ? INT64_MIN : -(int64_t)v) : (int64_t)v;
        return UD_OK;
    }
    if (type != UD_FLOAT64)
        return ud_fail(c, UD_UNSUPPORTED, "unsupported type %d", type);
    /* Validate decimal grammar before libc conversion; exclude hex/NaN/Infinity.
     * libc performs correct binary rounding in a private C numeric locale. */
    uint32_t i=0;int nonzero_digit=0,digits=0;
    if(text[i]=='-' || text[i]=='+') i++;
    while(i<n && text[i]>='0' && text[i]<='9') { nonzero_digit|=text[i]!='0';digits=1;i++; }
    if(i<n && text[i]=='.') {
        i++;
        while(i<n && text[i]>='0' && text[i]<='9') { nonzero_digit|=text[i]!='0';digits=1;i++; }
    }
    if(!digits) return UD_PARSE;
    if(i<n && (text[i]=='e' || text[i]=='E')) {
        i++;if(i<n && (text[i]=='+' || text[i]=='-')) i++;
        uint32_t start=i;while(i<n && text[i]>='0' && text[i]<='9') i++;
        if(start==i) return UD_PARSE;
    }
    if(i!=n) return ud_fail(c,UD_PARSE,"invalid float64");
    char *copy=ud_strdup_n(c,text,n);if(!copy) return UD_RESOURCE;
    char *end=NULL;ud_status status=UD_OK;
#ifdef _WIN32
    _locale_t numeric=_create_locale(LC_NUMERIC,"C");
    if(!numeric) status=UD_RESOURCE;
    else { out->real=_strtod_l(copy,&end,numeric);_free_locale(numeric); }
#else
    locale_t numeric=newlocale(LC_NUMERIC_MASK,"C",(locale_t)0);
    if(!numeric) status=UD_RESOURCE;
    else {
        locale_t old=uselocale(numeric);
        if(!old) status=UD_RESOURCE;
        else { out->real=strtod(copy,&end);uselocale(old); }
        freelocale(numeric);
    }
#endif
    if(!status && end!=copy+n) status=UD_PARSE;
    if(!status && (!isfinite(out->real) || (nonzero_digit && out->real==0))) status=UD_OVERFLOW;
    ud_free(copy);return status;
}
