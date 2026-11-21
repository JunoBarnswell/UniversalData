#include "../row_range.h"
#include "../../foundation/internal.h"

static const ud_json *member(const ud_json *object, const char *name) {
    return ud_json_find(object, name, (uint32_t)strlen(name));
}

static ud_status count_rows(const ud_json *value, uint64_t *out) {
    if (ud_json_kind(value) != UD_JSON_NUMBER)
        return UD_INVALID;
    const char *text;
    uint32_t length;
    UD_TRY(ud_json_text(value, &text, &length));
    uint64_t count = 0;
    for (uint32_t i = 0; i < length; i++) {
        if (text[i] < '0' || text[i] > '9')
            return UD_INVALID;
        unsigned digit = (unsigned)(text[i] - '0');
        if (count > ((uint64_t)INT64_MAX - digit) / 10)
            return UD_OVERFLOW;
        count = count * 10 + digit;
    }
    *out = count;
    return UD_OK;
}

ud_status ud_row_range_bind(const ud_json *ranges, uint64_t *offset, uint64_t *limit) {
    if (ud_json_kind(ranges) != UD_JSON_ARRAY || !ud_json_size(ranges))
        return UD_SCHEMA;
    *offset = 0;
    *limit = UINT64_MAX;
    for (uint32_t i = 0; i < ud_json_size(ranges); i++) {
        const ud_json *entry = ud_json_at(ranges, i);
        const ud_json *operation = member(entry, "op");
        if (ud_json_kind(entry) != UD_JSON_OBJECT || ud_json_size(entry) != 2 ||
            ud_json_kind(operation) != UD_JSON_STRING)
            return UD_SCHEMA;
        const char *name;
        uint32_t length;
        uint64_t count;
        UD_TRY(ud_json_text(operation, &name, &length));
        UD_TRY(count_rows(member(entry, "count"), &count));
        if (length == 4 && !memcmp(name, "take", 4)) {
            if (count < *limit)
                *limit = count;
        } else if (length == 4 && !memcmp(name, "skip", 4)) {
            uint64_t skipped = count < *limit ? count : *limit;
            if (*offset > UINT64_MAX - skipped)
                return UD_OVERFLOW;
            *offset += skipped;
            if (*limit != UINT64_MAX)
                *limit -= skipped;
        } else
            return UD_SCHEMA;
    }
    return UD_OK;
}
