#include "../engines/package/internal.h"
#include "http_internal.h"

void ud_http_clear_secret(ud_string *value) {
    volatile char *bytes = (volatile char *)value->data;
    for (uint32_t i = 0; i < value->length; ++i)
        bytes[i] = 0;
    ud_free((void *)value->data);
    *value = (ud_string){0};
}

static ud_status oauth_definition(const ud_json *definition, ud_string *endpoint, ud_string *id,
                                  ud_string *reference, ud_string *scope, ud_string *method) {
    static const char *keys[] = {"tokenEndpoint", "clientId", "credentialReference", "scope",
                                 "authentication"};
    UD_TRY(ud_json_keys(definition, keys, sizeof(keys) / sizeof(*keys)));
    UD_TRY(ud_json_string(ud_member(definition, "tokenEndpoint"), endpoint));
    UD_TRY(ud_json_string(ud_member(definition, "clientId"), id));
    UD_TRY(ud_json_string(ud_member(definition, "credentialReference"), reference));
    *scope = (ud_string){0};
    *method = (ud_string){"basic", 5};
    if (ud_member(definition, "scope"))
        UD_TRY(ud_json_string(ud_member(definition, "scope"), scope));
    if (ud_member(definition, "authentication"))
        UD_TRY(ud_json_string(ud_member(definition, "authentication"), method));
    if (endpoint->length <= 8 || memcmp(endpoint->data, "https://", 8) || id->length > 4096 ||
        !id->length || !reference->length || scope->length > 4096 ||
        !(ud_named(*method, "basic") || ud_named(*method, "post")))
        return UD_SCHEMA;

    return UD_OK;
}

ud_status ud_http_oauth_validate(const ud_json *definition) {
    ud_string endpoint, id, reference, scope, method;
    return oauth_definition(definition, &endpoint, &id, &reference, &scope, &method);
}

#ifdef UD_WITH_HTTP
static int token_character(unsigned char byte) {
    return (byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
           (byte >= '0' && byte <= '9') || (byte && strchr("-._~+/=", byte));
}

static ud_status encode_form(ud_bytes *form, ud_string input) {
    static const char digits[] = "0123456789ABCDEF";
    for (uint32_t i = 0; i < input.length; ++i) {
        unsigned char byte = (unsigned char)input.data[i];
        if (token_character(byte) && !strchr("+/=", byte))
            UD_TRY(ud_bytes_put(form, &byte, 1));
        else {
            char escaped[] = {'%', digits[byte >> 4], digits[byte & 15]};
            UD_TRY(ud_bytes_put(form, escaped, sizeof(escaped)));
        }
    }
    return UD_OK;
}

static ud_status form_parameter(ud_bytes *form, const char *name, ud_string value) {
    UD_TRY(ud_bytes_put(form, name, strlen(name)));
    return encode_form(form, value);
}

static ud_status basic_credentials(ud_context *memory, ud_string id, ud_string secret,
                                   ud_bytes *header) {
    const char alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    ud_bytes credentials = {.ctx = memory, .limit = 131072};
    ud_status status = encode_form(&credentials, id);
    if (!status)
        status = ud_bytes_put(&credentials, ":", 1);
    if (!status)
        status = encode_form(&credentials, secret);
    if (!status)
        status = ud_bytes_put(header, "Authorization: Basic ", 21);
    for (size_t offset = 0; !status && offset < credentials.size; offset += 3) {
        size_t remaining = credentials.size - offset;
        uint32_t value = (uint32_t)credentials.data[offset] << 16;
        if (remaining > 1)
            value |= (uint32_t)credentials.data[offset + 1] << 8;
        if (remaining > 2)
            value |= credentials.data[offset + 2];
        char text[] = {alphabet[value >> 18], alphabet[(value >> 12) & 63],
                       remaining > 1 ? alphabet[(value >> 6) & 63] : '=',
                       remaining > 2 ? alphabet[value & 63] : '='};
        status = ud_bytes_put(header, text, sizeof(text));
    }
    ud_string raw = {(const char *)credentials.data, (uint32_t)credentials.size};
    ud_http_clear_secret(&raw);
    return status;
}

static ud_status accept_token(ud_context *memory, const ud_http_response *response,
                              ud_http_oauth_cache *cache) {
    ud_json *document = NULL;
    UD_TRY(ud_json_parse(memory, response->body.data, response->body.length, &document, NULL));
    ud_string kind = {0}, token = {0};
    uint64_t seconds = 0;
    ud_status status = ud_json_string(ud_member(document, "access_token"), &token);
    if (!status)
        status = ud_json_string(ud_member(document, "token_type"), &kind);
    if (!status && !(ud_named(kind, "Bearer") || ud_named(kind, "bearer")))
        status = UD_UNSUPPORTED;
    if (!status && (!token.length || token.length > 8192))
        status = UD_SCHEMA;
    for (uint32_t i = 0; !status && i < token.length; ++i)
        if (!token_character((unsigned char)token.data[i]))
            status = UD_SCHEMA;
    const ud_json *expiry = ud_member(document, "expires_in");
    if (!status && expiry)
        status = ud_json_uint(expiry, &seconds);
    if (!status) {
        char *copy = ud_strdup_n(memory, token.data, token.length);
        if (!copy)
            status = UD_RESOURCE;
        else {
            ud_http_clear_secret(&cache->token);
            cache->token = (ud_string){copy, token.length};
            if (seconds > 7 * 86400)
                seconds = 7 * 86400;
            uint64_t margin = seconds > 60 ? 30 : seconds / 2;
            cache->expires_at = ud_now_ms() + (seconds - margin) * 1000;
        }
    }
    ud_json_destroy(document);
    return status;
}

ud_status ud_http_oauth_bearer(const ud_component_context *context, ud_connection_lease *lease,
                              const ud_http_options *options, const ud_json *definition,
                              ud_http_oauth_cache *cache, ud_string *out) {
    *out = (ud_string){0};
    ud_string endpoint, id, reference, scope, method;
    UD_TRY(oauth_definition(definition, &endpoint, &id, &reference, &scope, &method));

    ud_string secret;
    UD_TRY(ud_http_resolve_reference(lease, endpoint, reference, &secret));
    if (cache->token.length && ud_now_ms() < cache->expires_at) {
        ud_http_clear_secret(&secret);
        *out = cache->token;
        return UD_OK;
    }
    ud_bytes body = {.ctx = context->memory, .limit = 131072},
             authorization = {.ctx = context->memory, .limit = 131072};
    ud_status status = ud_bytes_put(&body, "grant_type=client_credentials", 29);
    if (!status && scope.length)
        status = form_parameter(&body, "&scope=", scope);
    if (!status && ud_named(method, "basic"))
        status = basic_credentials(context->memory, id, secret, &authorization);
    else if (!status) {
        status = form_parameter(&body, "&client_id=", id);
        if (!status)
            status = form_parameter(&body, "&client_secret=", secret);
    }
    ud_http_clear_secret(&secret);
    ud_string encoded = {(const char *)body.data, (uint32_t)body.size};
    ud_string header = {(const char *)authorization.data, (uint32_t)authorization.size};
    ud_http_response response = {0};
    if (!status)
        status = ud_http_token_exchange(context, lease, options, endpoint, encoded, header, &response);
    ud_http_clear_secret(&header);
    ud_http_clear_secret(&encoded);
    if (!status)
        status = accept_token(context->memory, &response, cache);
    ud_http_clear_secret(&response.body);
    ud_http_response_clear(&response);
    if (!status)
        *out = cache->token;
    return status;
}
#endif
