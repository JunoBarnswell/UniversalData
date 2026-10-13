#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include "ud/regex.h"
#include "../internal.h"
struct ud_regex {
    ud_context *ctx;
    pcre2_code *code;
    pcre2_general_context *general;
    ud_regex_options options;
    uint32_t captures;
};
typedef struct match_control {
    const ud_regex *regex;
    uint64_t deadline;
    ud_status status;
} match_control;
static void *allocate(PCRE2_SIZE bytes, void *user) {
    return ud_alloc(user, (size_t)bytes);
}
static void release(void *pointer, void *user) {
    (void)user;
    ud_free(pointer);
}
static int callout(pcre2_callout_block *block, void *user) {
    (void)block;
    match_control *c = user;
    if (ud_is_cancelled(c->regex->ctx))
        c->status = UD_CANCELLED;
    if (!c->status && c->regex->options.check)
        c->status = c->regex->options.check(c->regex->options.user);
    if (!c->status && c->deadline && ud_now_ms() >= c->deadline)
        c->status = UD_TIMEOUT;
    return c->status ? PCRE2_ERROR_CALLOUT : 0;
}
static ud_status result(int rc, const match_control *control) {
    if (control->status)
        return control->status;
    if (rc >= 0)
        return UD_OK;
    if (rc == PCRE2_ERROR_NOMATCH)
        return UD_END;
    if (rc == PCRE2_ERROR_MATCHLIMIT || rc == PCRE2_ERROR_DEPTHLIMIT || rc == PCRE2_ERROR_HEAPLIMIT)
        return UD_TIMEOUT;
    if (rc == PCRE2_ERROR_NOMEMORY)
        return UD_RESOURCE;
    return UD_PARSE;
}
static pcre2_match_context *match_context(const ud_regex *r, match_control *control) {
    *control = (match_control){
        .regex = r, .deadline = r->options.timeout_ms ? ud_now_ms() + r->options.timeout_ms : 0};
    pcre2_match_context *ctx = pcre2_match_context_create(r->general);
    if (!ctx)
        return NULL;
    pcre2_set_match_limit(ctx, r->options.max_steps);
    pcre2_set_depth_limit(ctx, r->options.max_depth);
    pcre2_set_heap_limit(ctx, (uint32_t)((r->options.heap_bytes + 1023) / 1024));
    pcre2_set_callout(ctx, callout, control);
    return ctx;
}
ud_status ud_regex_compile(ud_context *ctx, const char *pattern, uint32_t length,
                           const ud_regex_options *options, ud_regex **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !pattern || length > 4096 || !ud_utf8_valid(pattern, length))
        return UD_PARSE;
    ud_regex *r = ud_alloc(ctx, sizeof(*r));
    if (!r)
        return UD_RESOURCE;
    r->ctx = ctx;
    r->options = (ud_regex_options){.struct_size = sizeof(r->options),
                                    .abi_version = UD_ABI_VERSION,
                                    .max_steps = 1000000,
                                    .max_depth = 64,
                                    .heap_bytes = 1048576};
    if (options) {
        if (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION) {
            ud_free(r);
            return UD_INCOMPATIBLE;
        }
        r->options = *options;
    }
    if (!r->options.max_steps || !r->options.max_depth || r->options.max_depth > 256 ||
        r->options.heap_bytes < 1024 || r->options.heap_bytes > UINT32_MAX - 1023) {
        ud_free(r);
        return UD_SCHEMA;
    }
    r->general = pcre2_general_context_create(allocate, release, ctx);
    if (!r->general) {
        ud_free(r);
        return UD_RESOURCE;
    }
    int error = 0;
    PCRE2_SIZE offset = 0;
    pcre2_compile_context *cc = pcre2_compile_context_create(r->general);
    if (!cc) {
        ud_regex_destroy(r);
        return UD_RESOURCE;
    }
    pcre2_set_parens_nest_limit(cc, r->options.max_depth);
    r->code = pcre2_compile((PCRE2_SPTR)pattern, length, PCRE2_UTF | PCRE2_UCP | PCRE2_AUTO_CALLOUT,
                            &error, &offset, cc);
    pcre2_compile_context_free(cc);
    if (!r->code) {
        ud_regex_destroy(r);
        return error == PCRE2_ERROR_NOMEMORY ? UD_RESOURCE : UD_PARSE;
    }
    pcre2_pattern_info(r->code, PCRE2_INFO_CAPTURECOUNT, &r->captures);
    if (r->captures > 64) {
        ud_regex_destroy(r);
        return UD_RESOURCE;
    }
    *out = r;
    return UD_OK;
}
uint32_t ud_regex_capture_count(const ud_regex *r) {
    return r ? r->captures : 0;
}
ud_status ud_regex_group_index(const ud_regex *r, ud_string name, uint32_t *out) {
    if (!r || !out || !name.data || !name.length || name.length > 255 ||
        memchr(name.data, 0, name.length))
        return UD_SCHEMA;
    char text[256];
    memcpy(text, name.data, name.length);
    text[name.length] = 0;
    int i = pcre2_substring_number_from_name(r->code, (PCRE2_SPTR)text);
    if (i < 0)
        return UD_SCHEMA;
    *out = (uint32_t)i;
    return UD_OK;
}
ud_status ud_regex_search(const ud_regex *r, const char *text, uint32_t length, uint32_t offset,
                          ud_regex_span *spans, uint32_t count, int32_t *matched) {
    if (!r || !matched || (!text && length) || offset > length || (count && !spans))
        return UD_INVALID;
    *matched = 0;
    for (uint32_t i = 0; i < count; i++)
        spans[i] = (ud_regex_span){UINT32_MAX, UINT32_MAX};
    if (!ud_utf8_valid(text, length))
        return UD_PARSE;
    match_control control;
    pcre2_match_context *ctx = match_context(r, &control);
    if (!ctx)
        return UD_RESOURCE;
    pcre2_match_data *data = pcre2_match_data_create_from_pattern(r->code, r->general);
    if (!data) {
        pcre2_match_context_free(ctx);
        return UD_RESOURCE;
    }
    int rc = pcre2_match(r->code, (PCRE2_SPTR)text, length, offset, 0, data, ctx);
    ud_status status = result(rc, &control);
    if (status == UD_END)
        status = UD_OK;
    else if (!status) {
        *matched = 1;
        PCRE2_SIZE *pairs = pcre2_get_ovector_pointer(data);
        uint32_t total = r->captures + 1;
        if (total > count)
            total = count;
        for (uint32_t i = 0; i < total; i++)
            if (pairs[2 * i] != PCRE2_UNSET)
                spans[i] = (ud_regex_span){(uint32_t)pairs[2 * i], (uint32_t)pairs[2 * i + 1]};
    }
    pcre2_match_data_free(data);
    pcre2_match_context_free(ctx);
    return status;
}
ud_status ud_regex_test(const ud_regex *r, const char *text, uint32_t length, int32_t *matched) {
    return ud_regex_search(r, text, length, 0, NULL, 0, matched);
}
ud_status ud_regex_replace(const ud_regex *r, ud_string input, ud_string replacement, char **out,
                           uint32_t *length) {
    if (!r || !out || !length || (!input.data && input.length) ||
        (!replacement.data && replacement.length))
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (!ud_utf8_valid(input.data, input.length) ||
        !ud_utf8_valid(replacement.data, replacement.length))
        return UD_PARSE;
    match_control control;
    pcre2_match_context *ctx = match_context(r, &control);
    if (!ctx)
        return UD_RESOURCE;
    PCRE2_SIZE size = 0;
    uint32_t flags = PCRE2_SUBSTITUTE_GLOBAL | PCRE2_SUBSTITUTE_OVERFLOW_LENGTH |
                     PCRE2_SUBSTITUTE_UNSET_EMPTY | PCRE2_SUBSTITUTE_EXTENDED;
    int rc = pcre2_substitute(r->code, (PCRE2_SPTR)input.data, input.length, 0, flags, NULL, ctx,
                              (PCRE2_SPTR)replacement.data, replacement.length, NULL, &size);
    ud_status s = rc == PCRE2_ERROR_NOMEMORY && !control.status ? UD_OK : result(rc, &control);
    if (!s && (size > 16u * 1024u * 1024u || size >= UINT32_MAX))
        s = UD_RESOURCE;
    char *value = !s ? ud_alloc(r->ctx, (size_t)size + 1) : NULL;
    if (!s && !value)
        s = UD_RESOURCE;
    if (!s) {
        PCRE2_SIZE capacity = size + 1;
        rc = pcre2_substitute(r->code, (PCRE2_SPTR)input.data, input.length, 0, flags, NULL, ctx,
                              (PCRE2_SPTR)replacement.data, replacement.length,
                              (PCRE2_UCHAR *)value, &capacity);
        s = result(rc, &control);
        if (!s) {
            value[capacity] = 0;
            *length = (uint32_t)capacity;
            *out = value;
            value = NULL;
        }
    }
    ud_free(value);
    pcre2_match_context_free(ctx);
    return s;
}
void ud_regex_destroy(ud_regex *r) {
    if (!r)
        return;
    pcre2_code_free(r->code);
    pcre2_general_context_free(r->general);
    ud_free(r);
}
