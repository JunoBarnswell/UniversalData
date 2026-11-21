#include "../text_stream.h"
ud_status ud_text_encoding(ud_string name, int *out) {
    static const char *names[] = {"UTF-8", "UTF-16LE", "UTF-16BE", "ISO-8859-1"};
    if (!out)
        return UD_INVALID;
    if (!name.data) {
        *out = UD_TEXT_UTF8;
        return UD_OK;
    }
    for (int at = 0; at < 4; at++)
        if (name.length == strlen(names[at]) && !memcmp(name.data, names[at], name.length)) {
            *out = at + 1;
            return UD_OK;
        }
    return UD_UNSUPPORTED;
}
ud_status ud_text_start(ud_text_input *input, ud_context *context, FILE *file, int encoding) {
    *input = (ud_text_input){.context = context, .file = file, .encoding = encoding};
    uint8_t bytes[3];
    size_t size = fread(bytes, 1, 3, file);
    uint64_t skip = 0;
    if (size >= 2 && bytes[0] == 255 && bytes[1] == 254) {
        if (encoding != UD_TEXT_UTF16LE)
            return UD_INCOMPATIBLE;
        skip = 2;
    } else if (size >= 2 && bytes[0] == 254 && bytes[1] == 255) {
        if (encoding != UD_TEXT_UTF16BE)
            return UD_INCOMPATIBLE;
        skip = 2;
    } else if (size == 3 && !memcmp(bytes, "\xef\xbb\xbf", 3)) {
        if (encoding != UD_TEXT_UTF8)
            return UD_INCOMPATIBLE;
        skip = 3;
    }
    return ud_seek(file, skip) ? UD_IO : UD_OK;
}
static ud_status byte(ud_text_input *input, uint32_t *out, int first) {
    /* The stateful decoder owns this stream for the entire read. */
#ifdef _WIN32
    int value = _getc_nolock(input->file);
#else
    int value = getc_unlocked(input->file);
#endif
    if (value == EOF)
        return ferror(input->file) ? UD_IO : first ? UD_END : UD_PARSE;
    if (input->raw_size == 4)
        return UD_CORRUPT;
    input->raw[input->raw_size++] = (uint8_t)value;
    *out = (uint32_t)value;
    return UD_OK;
}
ud_status ud_text_next(ud_text_input *input, uint32_t *out) {
    if (ud_is_cancelled(input->context))
        return UD_CANCELLED;
    if (input->pushed) {
        input->pushed = 0;
        *out = input->pushed_scalar;
        input->raw_size = input->pushed_size;
        memcpy(input->raw, input->pushed_raw, input->raw_size);
        return UD_OK;
    }
    input->raw_size = 0;
    uint32_t first;
    UD_TRY(byte(input, &first, 1));
    if (input->encoding == UD_TEXT_LATIN1) {
        *out = first;
        return UD_OK;
    }
    if (input->encoding == UD_TEXT_UTF16LE || input->encoding == UD_TEXT_UTF16BE) {
        uint32_t second;
        UD_TRY(byte(input, &second, 0));
        uint32_t value =
            input->encoding == UD_TEXT_UTF16LE ? first | (second << 8) : (first << 8) | second;
        if (value >= 0xdc00 && value <= 0xdfff)
            return UD_PARSE;
        if (value >= 0xd800 && value <= 0xdbff) {
            uint32_t a, b;
            UD_TRY(byte(input, &a, 0));
            UD_TRY(byte(input, &b, 0));
            uint32_t low = input->encoding == UD_TEXT_UTF16LE ? a | (b << 8) : (a << 8) | b;
            if (low < 0xdc00 || low > 0xdfff)
                return UD_PARSE;
            value = 0x10000 + ((value - 0xd800) << 10) + (low - 0xdc00);
        }
        *out = value;
        return UD_OK;
    }
    if (first < 128) {
        *out = first;
        return UD_OK;
    }
    uint32_t count = first >= 0xc2 && first <= 0xdf   ? 1
                     : first >= 0xe0 && first <= 0xef ? 2
                     : first >= 0xf0 && first <= 0xf4 ? 3
                                                      : 0;
    if (!count)
        return UD_PARSE;
    uint32_t value = first & ((1u << (6 - count)) - 1);
    for (uint32_t at = 0; at < count; at++) {
        uint32_t next;
        UD_TRY(byte(input, &next, 0));
        if ((next & 0xc0) != 0x80)
            return UD_PARSE;
        value = (value << 6) | (next & 63);
    }
    if ((count == 1 && value < 128) || (count == 2 && value < 2048) ||
        (count == 3 && value < 65536) || value > 0x10ffff || (value >= 0xd800 && value <= 0xdfff))
        return UD_PARSE;
    *out = value;
    return UD_OK;
}
void ud_text_unread(ud_text_input *input, uint32_t value) {
    input->pushed = 1;
    input->pushed_scalar = value;
    input->pushed_size = input->raw_size;
    memcpy(input->pushed_raw, input->raw, input->raw_size);
}
uint64_t ud_text_offset(const ud_text_input *input) {
    int64_t offset = ud_tell(input->file);
    if (offset < 0 || (uint64_t)offset < input->pushed_size * (uint32_t)input->pushed)
        return UINT64_MAX;
    return (uint64_t)offset - (input->pushed ? input->pushed_size : 0);
}
ud_status ud_text_utf8(ud_bytes *out, uint32_t value) {
    uint8_t bytes[4];
    uint32_t size = value < 128 ? 1 : value < 2048 ? 2 : value < 65536 ? 3 : 4;
    if (value > 0x10ffff || (value >= 0xd800 && value <= 0xdfff))
        return UD_PARSE;
    if (size == 1)
        bytes[0] = (uint8_t)value;
    else {
        uint32_t copy = value;
        for (uint32_t at = size - 1; at; at--) {
            bytes[at] = (uint8_t)(0x80 | (copy & 63));
            copy >>= 6;
        }
        bytes[0] = (uint8_t)((size == 2 ? 0xc0 : size == 3 ? 0xe0 : 0xf0) | copy);
    }
    return ud_bytes_put(out, bytes, size);
}
ud_status ud_text_write(FILE *file, int encoding, const char *text, size_t size) {
    if (encoding == UD_TEXT_UTF8)
        return fwrite(text, 1, size, file) == size ? UD_OK : UD_IO;
    for (size_t at = 0; at < size;) {
        uint32_t value = (uint8_t)text[at++], count = value < 128                      ? 0
                                                      : value >= 0xc2 && value <= 0xdf ? 1
                                                      : value >= 0xe0 && value <= 0xef ? 2
                                                      : value >= 0xf0 && value <= 0xf4 ? 3
                                                                                       : 4;
        if (count == 4 || size - at < count)
            return UD_PARSE;
        uint32_t original = value;
        if (count)
            value &= (1u << (6 - count)) - 1;
        for (uint32_t n = 0; n < count; n++) {
            uint8_t next = (uint8_t)text[at++];
            if ((next & 0xc0) != 0x80)
                return UD_PARSE;
            value = (value << 6) | (next & 63);
        }
        if ((count == 1 && value < 128) || (count == 2 && value < 2048) ||
            (count == 3 && value < 65536) || value > 0x10ffff ||
            (value >= 0xd800 && value <= 0xdfff))
            return UD_PARSE;
        (void)original;
        if (encoding == UD_TEXT_LATIN1) {
            if (value > 255)
                return UD_TRUNCATED;
            if (fputc((int)value, file) == EOF)
                return UD_IO;
        } else {
            uint16_t units[2];
            uint32_t n = 1;
            units[0] = (uint16_t)value;
            if (value > 65535) {
                value -= 65536;
                units[0] = (uint16_t)(0xd800 + (value >> 10));
                units[1] = (uint16_t)(0xdc00 + (value & 1023));
                n = 2;
            }
            for (uint32_t i = 0; i < n; i++) {
                uint8_t bytes[2];
                bytes[encoding == UD_TEXT_UTF16LE ? 0 : 1] = (uint8_t)units[i];
                bytes[encoding == UD_TEXT_UTF16LE ? 1 : 0] = (uint8_t)(units[i] >> 8);
                if (fwrite(bytes, 1, 2, file) != 2)
                    return UD_IO;
            }
        }
    }
    return UD_OK;
}
ud_status ud_text_bom(FILE *file, int encoding) {
    const char *text = encoding == UD_TEXT_UTF16LE   ? "\xff\xfe"
                       : encoding == UD_TEXT_UTF16BE ? "\xfe\xff"
                                                     : "\xef\xbb\xbf";
    size_t size = encoding == UD_TEXT_UTF8 ? 3 : 2;
    return fwrite(text, 1, size, file) == size ? UD_OK : UD_IO;
}
