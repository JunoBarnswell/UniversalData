#include "../internal.h"
#include <math.h>
#define DAY_TICKS INT64_C(864000000000)
#define EPOCH_TICKS INT64_C(621355968000000000)
#define MAX_TICKS INT64_C(3155378975999999999)
static int leap(int y) {
    return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
}
static int mdays(int y, int m) {
    static const int n[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return n[m - 1] + (m == 2 && leap(y));
}
static int64_t civil(int y, int m, int d) {
    y -= m <= 2;
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yo = (unsigned)(y - era * 400);
    unsigned doy = (153 * (unsigned)(m + (m > 2 ? -3 : 9)) + 2) / 5 + (unsigned)d - 1;
    return (int64_t)era * 146097 + yo * 365 + yo / 4 - yo / 100 + doy - 719468 + 719162;
}
static void calendar(int64_t days, int *y, int *m, int *d) {
    int64_t z = days - 719162 + 719468, era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097),
             yo = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    *y = (int)(yo + era * 400);
    unsigned doy = doe - (365 * yo + yo / 4 - yo / 100), mp = (5 * doy + 2) / 153;
    *d = (int)(doy - (153 * mp + 2) / 5 + 1);
    *m = (int)mp + (mp < 10 ? 3 : -9);
    *y += *m <= 2;
}
static int number(const char *s, uint32_t n) {
    int v = 0;
    for (uint32_t j = 0; j < n; j++) {
        if (s[j] < '0' || s[j] > '9')
            return -1;
        v = v * 10 + s[j] - '0';
    }
    return v;
}
static int time_only(const ud_field *f) {
    return f->scalar.kind == UD_DT_DBTIME || f->scalar.kind == UD_DT_DBTIME2;
}
static ud_status valid(ud_datetime v) {
    return v.ticks < 0 || v.ticks > MAX_TICKS || v.has_offset < 0 || v.has_offset > 1 ||
                   v.offset_minutes < -840 || v.offset_minutes > 840 ||
                   (!v.has_offset && v.offset_minutes)
               ? UD_OVERFLOW
               : UD_OK;
}
ud_status ud_datetime_parse(ud_string text, const ud_field *field, ud_datetime *out) {
    if (!field || !out || !text.data)
        return UD_INVALID;
    *out = (ud_datetime){0};
    const char *s = text.data;
    uint32_t n = text.length, p = 0;
    int64_t days = 0;
    if (!time_only(field)) {
        if (n < 10 || s[4] != '-' || s[7] != '-')
            return UD_PARSE;
        int y = number(s, 4), m = number(s + 5, 2), d = number(s + 8, 2);
        if (y < 1 || y > 9999 || m < 1 || m > 12 || d < 1 || d > mdays(y, m))
            return UD_PARSE;
        days = civil(y, m, d);
        p = 10;
        if (p == n) {
            out->ticks = days * DAY_TICKS;
            return field->scalar.kind == UD_DT_DBTIMESTAMPOFFSET ? UD_PARSE : UD_OK;
        }
        if (s[p] != 'T' && s[p] != ' ')
            return UD_PARSE;
        p++;
    }
    if (n - p < 8 || s[p + 2] != ':' || s[p + 5] != ':')
        return UD_PARSE;
    int h = number(s + p, 2), m = number(s + p + 3, 2), sec = number(s + p + 6, 2);
    p += 8;
    if (h < 0 || h > 23 || m < 0 || m > 59 || sec < 0 || sec > 59)
        return UD_PARSE;
    int64_t fraction = 0;
    uint32_t count = 0;
    if (p < n && s[p] == '.') {
        p++;
        while (p < n && s[p] >= '0' && s[p] <= '9') {
            if (++count > 7)
                return UD_TRUNCATED;
            fraction = fraction * 10 + s[p++] - '0';
        }
        if (!count)
            return UD_PARSE;
    }
    while (count++ < 7)
        fraction *= 10;
    out->ticks = days * DAY_TICKS + ((int64_t)h * 3600 + m * 60 + sec) * 10000000 + fraction;
    if (p < n && s[p] == ' ')
        p++;
    if (p < n) {
        if (s[p] == 'Z') {
            out->has_offset = 1;
            p++;
        } else if ((s[p] == '+' || s[p] == '-') && (n - p == 5 || n - p == 6) && s[n - 3] == ':') {
            uint32_t hour_digits = n - p - 4;
            int sign = s[p] == '-' ? -1 : 1, hours = number(s + p + 1, hour_digits),
                minutes = number(s + n - 2, 2);
            if (hours < 0 || hours > 14 || minutes < 0 || minutes > 59 || (hours == 14 && minutes))
                return UD_PARSE;
            out->has_offset = 1;
            out->offset_minutes = sign * (hours * 60 + minutes);
            p = n;
        } else
            return UD_PARSE;
    }
    if (p != n || (!out->has_offset && field->scalar.kind == UD_DT_DBTIMESTAMPOFFSET) ||
        (out->has_offset && field->scalar.kind && field->scalar.kind != UD_DT_DBTIMESTAMPOFFSET))
        return UD_PARSE;
    return valid(*out);
}
ud_status ud_datetime_format(ud_datetime value, char *out, uint32_t cap, uint32_t *required) {
    if (!required || (!out && cap))
        return UD_INVALID;
    UD_TRY(valid(value));
    int y, m, d;
    calendar(value.ticks / DAY_TICKS, &y, &m, &d);
    int64_t day = value.ticks % DAY_TICKS;
    char buffer[64];
    int n = snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d.%07d", y, m, d,
                     (int)(day / 36000000000), (int)(day / 600000000 % 60),
                     (int)(day / 10000000 % 60), (int)(day % 10000000));
    if (value.has_offset) {
        int offset = value.offset_minutes;
        n += snprintf(buffer + n, sizeof(buffer) - (size_t)n, "%c%02d:%02d", offset < 0 ? '-' : '+',
                      abs(offset) / 60, abs(offset) % 60);
    }
    *required = (uint32_t)n + 1;
    if (cap < *required)
        return UD_RESOURCE;
    memcpy(out, buffer, *required);
    return UD_OK;
}
ud_status ud_datetime_from_value(const ud_field *field, const ud_value *value, ud_datetime *out) {
    if (!field || !value || !out || value->is_null || value->is_absent)
        return UD_INVALID;
    *out = (ud_datetime){0};
    if (value->type == UD_DATETIME)
        *out = value->datetime;
    else if (field->scalar.kind == UD_DT_DATE) {
        if (!isfinite(value->real))
            return UD_OVERFLOW;
        double whole;
        double fraction = fabs(modf(value->real, &whole));
        if (whole < -657434 || whole > 2958465)
            return UD_OVERFLOW;
        out->ticks = (civil(1899, 12, 30) + (int64_t)whole) * DAY_TICKS +
                     (int64_t)floor(fraction * (double)DAY_TICKS + .5);
    } else if (field->scalar.kind == UD_DT_FILETIME) {
        if (value->unsigned_integer > (uint64_t)(MAX_TICKS - INT64_C(504911232000000000)))
            return UD_OVERFLOW;
        out->ticks = (int64_t)value->unsigned_integer + INT64_C(504911232000000000);
        out->has_offset = 1;
    } else if (value->type == UD_TIMESTAMP) {
        if (value->integer < -EPOCH_TICKS / 10 || value->integer > (MAX_TICKS - EPOCH_TICKS) / 10)
            return UD_OVERFLOW;
        out->ticks = EPOCH_TICKS + value->integer * 10;
        out->has_offset = 1;
    } else if (value->type == UD_DATE32) {
        if (value->integer < -719162 || value->integer > 2932896)
            return UD_OVERFLOW;
        out->ticks = (value->integer + 719162) * DAY_TICKS;
    } else if (value->type == UD_TIME64) {
        if (value->integer < 0 || value->integer >= DAY_TICKS / 10)
            return UD_OVERFLOW;
        out->ticks = value->integer * 10;
    } else
        return UD_SCHEMA;
    return valid(*out);
}
ud_status ud_datetime_to_value(const ud_field *field, ud_datetime value, ud_value *out) {
    if (!field || !out)
        return UD_INVALID;
    UD_TRY(valid(value));
    memset(out, 0, sizeof(*out));
    out->type = field->type;
    if (field->type == UD_DATETIME) {
        if (time_only(field))
            value.ticks %= DAY_TICKS;
        if (field->scalar.kind == UD_DT_DBDATE)
            value.ticks = value.ticks / DAY_TICKS * DAY_TICKS;
        if (field->scalar.kind == UD_DT_DBTIMESTAMPOFFSET) {
            if (!value.has_offset)
                value.has_offset = 1;
        } else if (field->scalar.kind) {
            value.has_offset = 0;
            value.offset_minutes = 0;
        }
        out->datetime = value;
    } else if (field->scalar.kind == UD_DT_DATE) {
        double days = (double)(value.ticks / DAY_TICKS - civil(1899, 12, 30));
        double fraction = (double)(value.ticks % DAY_TICKS) / (double)DAY_TICKS;
        out->real = days < 0 ? days - fraction : days + fraction;
    } else if (field->scalar.kind == UD_DT_FILETIME) {
        int64_t ticks =
            value.ticks - (int64_t)value.offset_minutes * 600000000 - INT64_C(504911232000000000);
        if (ticks < 0)
            return UD_OVERFLOW;
        out->unsigned_integer = (uint64_t)ticks;
    } else if (field->type == UD_DATE32)
        out->integer = value.ticks / DAY_TICKS - 719162;
    else if (field->type == UD_TIMESTAMP) {
        int64_t ticks = value.ticks - (int64_t)value.offset_minutes * 600000000 - EPOCH_TICKS;
        if (ticks % 10)
            return UD_TRUNCATED;
        out->integer = ticks / 10;
    } else if (field->type == UD_TIME64) {
        if (value.ticks % 10)
            return UD_TRUNCATED;
        out->integer = value.ticks % DAY_TICKS / 10;
    } else
        return UD_SCHEMA;
    return UD_OK;
}
static int part(ud_string name) {
    static const char *const groups[] = {"year|yy|yyyy",   "quarter|qq|q",  "month|mm|m",
                                         "dayofyear|dy|y", "day|dd|d",      "week|wk|ww",
                                         "weekday|dw|w",   "hour|hh",       "minute|mi|n",
                                         "second|ss|s",    "millisecond|ms"};
    char lower[32];
    if (!name.data || !name.length || name.length >= sizeof(lower))
        return -1;
    for (uint32_t j = 0; j < name.length; j++)
        lower[j] =
            (char)(name.data[j] >= 'A' && name.data[j] <= 'Z' ? name.data[j] + 32 : name.data[j]);
    lower[name.length] = 0;
    for (int i = 0; i < 11; i++) {
        const char *at = groups[i];
        while (*at) {
            const char *end = strchr(at, '|');
            size_t n = end ? (size_t)(end - at) : strlen(at);
            if (n == name.length && !memcmp(at, lower, n))
                return i;
            if (!end)
                break;
            at = end + 1;
        }
    }
    return -1;
}
ud_status ud_datetime_part(ud_string name, ud_datetime value, int64_t *out) {
    if (!out)
        return UD_INVALID;
    UD_TRY(valid(value));
    int p = part(name), y, m, d;
    int64_t days = value.ticks / DAY_TICKS;
    calendar(days, &y, &m, &d);
    int64_t units[] = {y,
                       (m - 1) / 3 + 1,
                       m,
                       days - civil(y, 1, 1) + 1,
                       d,
                       (days - civil(y, 1, 1) + (civil(y, 1, 1) + 1) % 7) / 7 + 1,
                       (days + 1) % 7 + 1,
                       value.ticks / 36000000000 % 24,
                       value.ticks / 600000000 % 60,
                       value.ticks / 10000000 % 60,
                       value.ticks / 10000 % 1000};
    if (p < 0)
        return UD_SCHEMA;
    *out = units[p];
    return UD_OK;
}
ud_status ud_datetime_add(ud_string name, int64_t amount, ud_datetime value, ud_datetime *out) {
    if (!out)
        return UD_INVALID;
    UD_TRY(valid(value));
    int p = part(name);
    if (p < 0)
        return UD_SCHEMA;
    *out = value;
    if (p <= 2) {
        int y, m, d;
        calendar(value.ticks / DAY_TICKS, &y, &m, &d);
        int64_t factor = p == 0 ? 12 : p == 1 ? 3 : 1;
        if (amount < -120000 || amount > 120000)
            return UD_OVERFLOW;
        int64_t months = (int64_t)(y - 1) * 12 + m - 1 + amount * factor;
        if (months < 0 || months >= 119988)
            return UD_OVERFLOW;
        y = (int)(months / 12 + 1);
        m = (int)(months % 12 + 1);
        if (d > mdays(y, m))
            d = mdays(y, m);
        out->ticks = civil(y, m, d) * DAY_TICKS + value.ticks % DAY_TICKS;
    } else {
        int64_t factor = p <= 6   ? (p == 5 ? 7 * DAY_TICKS : DAY_TICKS)
                         : p == 7 ? INT64_C(36000000000)
                         : p == 8 ? INT64_C(600000000)
                         : p == 9 ? INT64_C(10000000)
                                  : 10000;
        if (amount > INT64_MAX / factor || amount < INT64_MIN / factor)
            return UD_OVERFLOW;
        int64_t delta = amount * factor;
        if ((delta > 0 && value.ticks > MAX_TICKS - delta) || (delta < 0 && value.ticks < -delta))
            return UD_OVERFLOW;
        out->ticks += delta;
    }
    return valid(*out);
}
ud_status ud_datetime_diff(ud_string name, ud_datetime a, ud_datetime b, int64_t *out) {
    if (!out)
        return UD_INVALID;
    UD_TRY(valid(a));
    UD_TRY(valid(b));
    int p = part(name);
    if (p < 0)
        return UD_SCHEMA;
    int ay, am, ad, by, bm, bd;
    calendar(a.ticks / DAY_TICKS, &ay, &am, &ad);
    calendar(b.ticks / DAY_TICKS, &by, &bm, &bd);
    if (p == 0)
        *out = by - ay;
    else if (p == 1)
        *out = (by - ay) * 4 + (bm - 1) / 3 - (am - 1) / 3;
    else if (p == 2)
        *out = (by - ay) * 12 + bm - am;
    else if (p == 5)
        *out = (b.ticks / DAY_TICKS + 1) / 7 - (a.ticks / DAY_TICKS + 1) / 7;
    else {
        int64_t factor = p <= 6   ? DAY_TICKS
                         : p == 7 ? INT64_C(36000000000)
                         : p == 8 ? INT64_C(600000000)
                         : p == 9 ? INT64_C(10000000)
                                  : 10000;
        *out = b.ticks / factor - a.ticks / factor;
    }
    return UD_OK;
}
