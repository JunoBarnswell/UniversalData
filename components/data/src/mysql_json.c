#include "../internal.h"
#include "ud/format.h"
#include <math.h>

/* MySQL binary JSON offsets are relative to the container payload, not to its
 * type byte. Each recursive call is bounded to that container's declared size. */
static uint64_t little(const uint8_t *data, uint32_t width) {
    uint64_t value = 0;
    for (uint32_t j = 0; j < width; j++)
        value |= (uint64_t)data[j] << (8 * j);
    return value;
}
static uint64_t big(const uint8_t *data, uint32_t width) {
    uint64_t value = 0;
    for (uint32_t j = 0; j < width; j++)
        value = (value << 8) | data[j];
    return value;
}
static ud_status variable(const uint8_t *data, uint32_t size, uint32_t *at, uint32_t *out) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28; shift += 7) {
        if (*at >= size)
            return UD_CORRUPT;
        uint8_t byte = data[(*at)++];
        if (shift == 28 && (byte & 0xf0))
            return UD_CORRUPT;
        result |= (uint32_t)(byte & 127) << shift;
        if (!(byte & 128)) {
            *out = result;
            return UD_OK;
        }
    }
    return UD_CORRUPT;
}
static ud_status opaque_decimal(ud_bytes *out, const uint8_t *data, uint32_t size) {
    if (size < 3)
        return UD_CORRUPT;
    uint32_t precision = data[0], scale = data[1];
    if (!precision || precision > 65 || scale > 30 || scale > precision)
        return UD_CORRUPT;
    static const uint32_t widths[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
    uint32_t whole = precision - scale;
    uint32_t bytes = whole / 9 * 4 + widths[whole % 9] + scale / 9 * 4 + widths[scale % 9];
    if (size != bytes + 2 || bytes > 32)
        return UD_CORRUPT;
    uint8_t coefficient[32];
    int positive = !!(data[2] & 128);
    for (uint32_t j = 0; j < bytes; j++)
        coefficient[j] = data[j + 2] ^ (positive ? 0 : 255);
    coefficient[0] ^= 128;
    char text[80];
    uint32_t at = 0, length = 0;
    if (!positive)
        text[length++] = '-';
    int leading = 1;
    for (uint32_t part = 0; part < 2; part++) {
        uint32_t digits = part ? scale : whole;
        if (part && digits)
            text[length++] = '.';
        uint32_t groups = digits / 9, remainder = digits % 9;
        uint32_t iterations = groups + (remainder != 0);
        for (uint32_t j = 0; j < iterations; j++) {
            uint32_t group_digits =
                (!part && !j && remainder) || (part && j == groups && remainder) ? remainder : 9;
            uint32_t value = (uint32_t)big(coefficient + at, widths[group_digits]);
            at += widths[group_digits];
            uint32_t limit = 1;
            for (uint32_t k = 0; k < group_digits; k++)
                limit *= 10;
            if (value >= limit)
                return UD_CORRUPT;
            length += !part && leading
                          ? (uint32_t)snprintf(text + length, sizeof(text) - length, "%u", value)
                          : (uint32_t)snprintf(text + length, sizeof(text) - length, "%0*u",
                                               (int)group_digits, value);
            leading = 0;
        }
        if (!part && leading) {
            text[length++] = '0';
            leading = 0;
        }
    }
    return ud_bytes_put(out, text, length);
}
static ud_status opaque(ud_bytes *out, uint8_t type, const uint8_t *data, uint32_t size) {
    if (type == 246)
        return opaque_decimal(out, data, size);
    if (type == 7 || type == 10 || type == 11 || type == 12) {
        if (size != 8)
            return UD_CORRUPT;
        uint64_t bits = little(data, 8);
        int64_t signed_value;
        memcpy(&signed_value, &bits, 8);
        uint64_t magnitude = signed_value < 0 ? 0 - bits : bits;
        uint32_t micros = (uint32_t)(magnitude & 0xffffff);
        uint64_t packed = magnitude >> 24;
        if (micros >= 1000000)
            return UD_CORRUPT;
        char text[96];
        int length;
        if (type == 11) {
            uint32_t hours = (uint32_t)(packed >> 12), minutes = (uint32_t)(packed >> 6) & 63,
                     seconds = (uint32_t)packed & 63;
            if (hours > 838 || minutes >= 60 || seconds >= 60)
                return UD_CORRUPT;
            length = snprintf(text, sizeof(text), "%s%02u:%02u:%02u.%06u",
                              signed_value < 0 ? "-" : "", hours, minutes, seconds, micros);
        } else {
            if (signed_value < 0)
                return UD_CORRUPT;
            uint32_t ymd = (uint32_t)(packed >> 17), ym = ymd >> 5;
            uint32_t year = ym / 13, month = ym % 13, day = ymd & 31;
            uint32_t hour = (uint32_t)(packed >> 12) & 31, minute = (uint32_t)(packed >> 6) & 63,
                     second = (uint32_t)packed & 63;
            if (year > 9999 || month > 12 || day > 31 || hour >= 24 || minute >= 60 || second >= 60)
                return UD_CORRUPT;
            length = type == 10 ? snprintf(text, sizeof(text), "%04u-%02u-%02u", year, month, day)
                                : snprintf(text, sizeof(text), "%04u-%02u-%02u %02u:%02u:%02u.%06u",
                                           year, month, day, hour, minute, second, micros);
        }
        return ud_json_quote(out, text, (uint32_t)length);
    }
    static const char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    char prefix[40];
    int length = snprintf(prefix, sizeof(prefix), "\"base64:type%u:", type);
    UD_TRY(ud_bytes_put(out, prefix, (size_t)length));
    for (uint32_t at = 0; at < size;) {
        if (!(at & 4095) && ud_is_cancelled(out->ctx))
            return UD_CANCELLED;
        uint32_t left = size - at, bits = (uint32_t)data[at++] << 16;
        if (left > 1)
            bits |= (uint32_t)data[at++] << 8;
        if (left > 2)
            bits |= data[at++];
        char encoded[] = {alphabet[bits >> 18], alphabet[(bits >> 12) & 63],
                          left > 1 ? alphabet[(bits >> 6) & 63] : '=',
                          left > 2 ? alphabet[bits & 63] : '='};
        UD_TRY(ud_bytes_put(out, encoded, 4));
    }
    return ud_bytes_put(out, "\"", 1);
}
static ud_status decode(ud_bytes *out, uint8_t type, const uint8_t *data, uint32_t size,
                        uint32_t depth, uint32_t *consumed) {
    *consumed = 0;
    if (depth > 32)
        return UD_RESOURCE;
    if (ud_is_cancelled(out->ctx))
        return UD_CANCELLED;
    if (type <= 3) {
        int object = type < 2, large = type & 1;
        uint32_t width = large ? 4 : 2;
        if (size < 2 * width)
            return UD_CORRUPT;
        uint32_t count = (uint32_t)little(data, width),
                 length = (uint32_t)little(data + width, width);
        uint64_t header =
            (uint64_t)2 * width + (uint64_t)count * ((object ? width + 2 : 0) + width + 1);
        if (length > size || header > length)
            return UD_CORRUPT;
        UD_TRY(ud_bytes_put(out, object ? "{" : "[", 1));
        uint32_t values = 2 * width + (object ? count * (width + 2) : 0);
        for (uint32_t j = 0; j < count; j++) {
            if (j)
                UD_TRY(ud_bytes_put(out, ",", 1));
            if (object) {
                uint32_t entry = 2 * width + j * (width + 2);
                uint32_t offset = (uint32_t)little(data + entry, width);
                uint32_t key_size = (uint32_t)little(data + entry + width, 2);
                if (offset < header || offset > length || key_size > length - offset ||
                    !ud_utf8_valid((const char *)data + offset, key_size))
                    return UD_CORRUPT;
                UD_TRY(ud_json_quote(out, (const char *)data + offset, key_size));
                UD_TRY(ud_bytes_put(out, ":", 1));
            }
            uint32_t entry = values + j * (width + 1);
            uint8_t child = data[entry];
            int inline_value =
                child == 4 || child == 5 || child == 6 || (large && (child == 7 || child == 8));
            uint32_t offset = inline_value ? entry + 1 : (uint32_t)little(data + entry + 1, width);
            if ((!inline_value && offset < header) || offset >= length)
                return UD_CORRUPT;
            uint32_t used;
            UD_TRY(decode(out, child, data + offset, inline_value ? width : length - offset,
                          depth + 1, &used));
        }
        UD_TRY(ud_bytes_put(out, object ? "}" : "]", 1));
        *consumed = length;
        return UD_OK;
    }
    if (type == 4) {
        if (!size || data[0] > 2)
            return UD_CORRUPT;
        const char *literal = data[0] == 0 ? "null" : data[0] == 1 ? "true" : "false";
        *consumed = 1;
        return ud_bytes_put(out, literal, strlen(literal));
    }
    if (type >= 5 && type <= 11) {
        uint32_t width = type <= 6 ? 2 : type <= 8 ? 4 : 8;
        if (size < width)
            return UD_CORRUPT;
        uint64_t bits = little(data, width);
        ud_value value = {.type = type == 11 ? UD_FLOAT64 : type & 1 ? UD_INT64 : UD_UINT64};
        if (type == 11) {
            memcpy(&value.real, &bits, 8);
            if (!isfinite(value.real))
                return UD_CORRUPT;
        } else if (value.type == UD_INT64) {
            if (width < 8 && (bits & (UINT64_C(1) << (width * 8 - 1))))
                bits |= UINT64_MAX << (width * 8);
            memcpy(&value.integer, &bits, 8);
        } else
            value.unsigned_integer = bits;
        char text[64];
        uint32_t required;
        UD_TRY(ud_value_format(&value, text, sizeof(text), &required));
        *consumed = width;
        UD_TRY(ud_bytes_put(out, text, required - 1));
        if (type == 11 && !strchr(text, '.') && !strchr(text, 'e') && !strchr(text, 'E'))
            return ud_bytes_put(out, ".0", 2);
        return UD_OK;
    }
    if (type == 12 || type == 15) {
        uint32_t at = 0, length;
        uint8_t subtype = 0;
        if (type == 15) {
            if (!size)
                return UD_CORRUPT;
            subtype = data[at++];
        }
        UD_TRY(variable(data, size, &at, &length));
        if (length > size - at)
            return UD_CORRUPT;
        *consumed = at + length;
        if (type == 15)
            return opaque(out, subtype, data + at, length);
        if (!ud_utf8_valid((const char *)data + at, length))
            return UD_CORRUPT;
        return ud_json_quote(out, (const char *)data + at, length);
    }
    return UD_UNSUPPORTED;
}
ud_status ud_mysql_json_decode(ud_context *ctx, const uint8_t *data, uint32_t size, char **text,
                               uint32_t *length) {
    if (!ctx || !data || !size || !text || !length)
        return UD_INVALID;
    *text = NULL;
    *length = 0;
    ud_bytes bytes = {.ctx = ctx, .limit = UINT32_MAX};
    uint32_t consumed;
    ud_status status = decode(&bytes, data[0], data + 1, size - 1, 0, &consumed);
    if (!status && consumed != size - 1)
        status = UD_CORRUPT;
    if (!status && bytes.size >= UINT32_MAX)
        status = UD_RESOURCE;
    if (!status) {
        *length = (uint32_t)bytes.size;
        status = ud_bytes_put(&bytes, "\0", 1);
    }
    if (!status) {
        *text = (char *)bytes.data;
        bytes.data = NULL;
    } else
        *length = 0;
    ud_bytes_clear(&bytes);
    return status;
}
