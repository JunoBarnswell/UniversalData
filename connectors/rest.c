#include "../engines/package/internal.h"
#include "rest.h"
#include "ud/format.h"
#include "ud/sql.h"
/* REST exposes Native typed batches and receipts. Paging is a transport cursor;
 * source sealing and confirmation stay in the existing DataFlow snapshot chain. */
struct ud_rest_reader {
    ud_component_context context;
    ud_connection_lease *lease;
    ud_http_options options;
    ud_json *config;
    ud_batch *schema, *page;
    uint32_t row;
    uint64_t pages, maximum_pages;
    ud_string cursor, resolved_url;
    int ended;
};
struct ud_rest_target {
    ud_component_context context;
    ud_connection_lease *lease;
    ud_http_options options;
    ud_json *config;
    ud_string receipt_url;
    int32_t delivery;
};
ud_status ud_rest_delivery(const ud_json *config, int32_t *delivery) {
    if (!config || !delivery)
        return UD_INVALID;
    ud_string mode = {"response", 8};
    if (ud_member(config, "delivery"))
        UD_TRY(ud_json_string(ud_member(config, "delivery"), &mode));
    *delivery = ud_named(mode, "response")     ? UD_DELIVERY_RESPONSE
                : ud_named(mode, "idempotent") ? UD_DELIVERY_IDEMPOTENT
                : ud_named(mode, "receipt")    ? UD_DELIVERY_RECEIPT
                                               : 0;
    if (!*delivery)
        return UD_SCHEMA;
    if ((*delivery == UD_DELIVERY_RECEIPT) != (ud_member(config, "receiptUrl") != NULL))
        return UD_SCHEMA;
    return UD_OK;
}
int32_t ud_rest_target_delivery(const ud_rest_target *target) {
    return target ? target->delivery : 0;
}
static ud_status copy_config(ud_context *c, const ud_json *input, ud_json **out) {
    char *text = NULL;
    uint32_t n;
    UD_TRY(ud_json_serialize(input, &text, &n));
    ud_status s = ud_json_parse(c, text, n, out, NULL);
    ud_free(text);
    return s;
}
static ud_status options(const ud_json *config, ud_http_options *out) {
    *out = (ud_http_options){.struct_size = sizeof(*out),
                             .abi_version = UD_ABI_VERSION,
                             .method = {"GET", 3},
                             .timeout = 60000,
                             .maximum = 16777216,
                             .attempts = 3,
                             .delay = 100,
                             .headers = ud_member(config, "headers"),
                             .response_headers = ud_member(config, "responseHeaders")};
    UD_TRY(ud_http_validate_headers(out->headers, out->response_headers));
    const char *strings[] = {"caConnection", "url"};
    ud_string *fields[] = {&out->ca, &out->url};
    for (uint32_t i = 0; i < 2; i++)
        if (ud_member(config, strings[i]))
            UD_TRY(ud_json_string(ud_member(config, strings[i]), fields[i]));
    const char *names[] = {"timeoutMs", "maxResponseBytes", "maxAttempts", "retryDelayMs"};
    uint64_t *values[] = {&out->timeout, &out->maximum, &out->attempts, &out->delay};
    for (uint32_t i = 0; i < 4; i++)
        if (ud_member(config, names[i]))
            UD_TRY(ud_json_uint(ud_member(config, names[i]), values[i]));
    return UD_OK;
}
static ud_status encode(ud_bytes *b, ud_string text) {
    const char hex[] = "0123456789ABCDEF";
    for (uint32_t i = 0; i < text.length; i++) {
        unsigned char c = (unsigned char)text.data[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
            c == '-' || c == '_' || c == '.' || c == '~')
            UD_TRY(ud_bytes_put(b, &c, 1));
        else {
            char escaped[] = {'%', hex[c >> 4], hex[c & 15]};
            UD_TRY(ud_bytes_put(b, escaped, 3));
        }
    }
    return UD_OK;
}
static ud_status query_url(ud_context *c, ud_string base, const ud_string *keys,
                           const ud_string *values, uint32_t count, ud_string *out) {
    ud_bytes b = {.ctx = c, .limit = 1048576};
    ud_status s = ud_bytes_put(&b, base.data, base.length);
    for (uint32_t i = 0; !s && i < count; i++) {
        s = ud_bytes_put(&b, i || memchr(base.data, '?', base.length) ? "&" : "?", 1);
        if (!s)
            s = encode(&b, keys[i]);
        if (!s)
            s = ud_bytes_put(&b, "=", 1);
        if (!s)
            s = encode(&b, values[i]);
    }
    if (!s) {
        out->length = (uint32_t)b.size;
        s = ud_bytes_put(&b, "", 1);
    }
    if (!s) {
        out->data = (const char *)b.data;
        b.data = NULL;
    }
    ud_bytes_clear(&b);
    return s;
}
ud_status ud_rest_reader_open(const ud_component_context *c, ud_connection_lease *lease,
                              const ud_json *config, const ud_batch *schema, ud_rest_reader **out) {
    *out = NULL;
    ud_rest_reader *r = ud_alloc(c->memory, sizeof(*r));
    if (!r)
        return UD_RESOURCE;
    r->context = *c;
    r->lease = lease;
    r->maximum_pages = 10000;
    ud_status s = copy_config(c->memory, config, &r->config);
    if (!s)
        s = options(r->config, &r->options);
    if (!s && (!r->options.url.length || r->options.url.data[0] == '/')) {
        ud_string base = {0};
        s = ud_http_connection_resource(lease, &base);
        if (!s)
            s = ud_http_resolve_url(c->memory, base, r->options.url, &r->resolved_url);
        if (!s)
            r->options.url = r->resolved_url;
    }
    if (!s)
        s = ud_batch_create(c->memory, schema->schema->fields, schema->columns, 0, &r->schema);
    if (!s && ud_member(config, "maxPages"))
        s = ud_json_uint(ud_member(config, "maxPages"), &r->maximum_pages);
    if (s) {
        ud_rest_reader_close(r);
        return s;
    }
    *out = r;
    return UD_OK;
}
static ud_status next_page(ud_rest_reader *r) {
    if (r->ended)
        return UD_END;
    if (r->pages == r->maximum_pages)
        return UD_RESOURCE;
    ud_http_options request = r->options;
    ud_string url = {0};
    ud_string key = {"cursor", 6};
    if (ud_member(r->config, "cursorParameter"))
        UD_TRY(ud_json_string(ud_member(r->config, "cursorParameter"), &key));
    if (r->cursor.length) {
        UD_TRY(query_url(r->context.memory, r->options.url, &key, &r->cursor, 1, &url));
        request.url = url;
    }
    ud_http_response response = {0};
    ud_status s = ud_http_call(&r->context, r->lease, &request, &response);
    ud_free((void *)url.data);
    ud_json *document = NULL;
    if (!s)
        s = ud_json_parse(r->context.memory, response.body.data, response.body.length, &document,
                          NULL);
    ud_string rows_name = {"rows", 4}, cursor_name = {"nextCursor", 10};
    const ud_json *rows_field = ud_member(r->config, "rowsField");
    if (!s && rows_field)
        s = ud_json_string(rows_field, &rows_name);
    if (!s && ud_member(r->config, "cursorField"))
        s = ud_json_string(ud_member(r->config, "cursorField"), &cursor_name);
    const ud_json *rows = NULL;
    if (!s)
        rows = !rows_field && ud_json_kind(document) == UD_JSON_ARRAY
                   ? document
                   : ud_json_find(document, rows_name.data, rows_name.length);
    const ud_json *next = !s ? ud_json_find(document, cursor_name.data, cursor_name.length) : NULL;
    char *json = NULL;
    uint32_t length = 0;
    if (!s && ud_json_kind(rows) != UD_JSON_ARRAY)
        s = UD_SCHEMA;
    if (!s)
        s = ud_json_serialize(rows, &json, &length);
    if (!s)
        s = ud_batch_from_json(r->context.memory, r->schema->schema->fields, r->schema->columns,
                               json, length, UD_ROUND_EXACT, &r->page);
    ud_free(json);
    ud_string token = {0};
    if (!s && next && ud_json_kind(next) != UD_JSON_NULL)
        s = ud_json_string(next, &token);
    if (!s && token.length && ud_same(token, r->cursor))
        s = UD_FENCED;
    if (!s) {
        ud_free((void *)r->cursor.data);
        r->cursor = (ud_string){
            token.length ? ud_strdup_n(r->context.memory, token.data, token.length) : NULL,
            token.length};
        if (token.length && !r->cursor.data)
            s = UD_RESOURCE;
        else {
            r->ended = !token.length;
            r->row = 0;
            r->pages++;
        }
    }
    ud_json_destroy(document);
    ud_http_response_clear(&response);
    return s;
}
ud_status ud_rest_reader_next(ud_rest_reader *r, ud_batch **out) {
    if (!r || !out)
        return UD_INVALID;
    *out = NULL;
    ud_status s = UD_OK;
    while (!r->page || r->row == r->page->rows) {
        ud_batch_destroy(r->page);
        r->page = NULL;
        s = next_page(r);
        if (s)
            return s;
        if (r->page->rows)
            break;
    }
    uint32_t count = r->page->rows - r->row;
    if (count > r->context.max_batch_rows)
        count = r->context.max_batch_rows;
    s = ud_batch_create(r->context.memory, r->schema->schema->fields, r->schema->columns, count,
                        out);
    for (uint32_t i = 0; !s && i < count; i++)
        s = ud_batch_append_from(*out, r->page, r->row + i);
    if (!s) {
        r->row += count;
        s = ud_batch_seal(*out);
    }
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
void ud_rest_reader_close(ud_rest_reader *r) {
    if (!r)
        return;
    ud_batch_destroy(r->schema);
    ud_batch_destroy(r->page);
    ud_free((void *)r->cursor.data);
    ud_free((void *)r->resolved_url.data);
    ud_json_destroy(r->config);
    ud_free(r);
}
static ud_status receipt_parse(ud_rest_target *t, const ud_http_response *response,
                               ud_commit_receipt *out) {
    memset(out, 0, sizeof(*out));
    ud_json *doc = NULL;
    ud_status s =
        ud_json_parse(t->context.memory, response->body.data, response->body.length, &doc, NULL);
    const ud_json *committed = ud_member(doc, "committed");
    if (!s && ud_json_kind(committed) != UD_JSON_BOOL)
        s = UD_SCHEMA;
    const char *value = NULL;
    uint32_t n;
    if (!s)
        s = ud_json_text(committed, &value, &n);
    if (!s)
        out->committed = n == 4 && !memcmp(value, "true", 4);
    if (!s && out->committed) {
        ud_string digest, position;
        s = ud_json_string(ud_member(doc, "digest"), &digest);
        if (!s && digest.length != 64)
            s = UD_CORRUPT;
        for (uint32_t i = 0; !s && i < 32; i++) {
            uint32_t byte = 0;
            for (uint32_t k = 0; k < 2; k++) {
                char c = digest.data[i * 2 + k];
                int v = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1;
                if (v < 0) {
                    s = UD_CORRUPT;
                    break;
                }
                byte = byte * 16 + (uint32_t)v;
            }
            out->digest[i] = (uint8_t)byte;
        }
        if (!s)
            s = ud_json_string(ud_member(doc, "sourcePosition"), &position);
        if (!s) {
            out->source_position = (ud_string){
                ud_strdup_n(t->context.memory, position.data, position.length), position.length};
            if (!out->source_position.data)
                s = UD_RESOURCE;
        }
    }
    ud_json_destroy(doc);
    if (s) {
        ud_free((void *)out->source_position.data);
        memset(out, 0, sizeof(*out));
    }
    return s;
}
ud_status ud_rest_target_open(const ud_component_context *c, ud_connection_lease *lease,
                              const ud_json *config, ud_rest_target **out) {
    *out = NULL;
    ud_rest_target *t = ud_alloc(c->memory, sizeof(*t));
    if (!t)
        return UD_RESOURCE;
    t->context = *c;
    t->lease = lease;
    ud_status s = copy_config(c->memory, config, &t->config);
    if (!s)
        s = options(t->config, &t->options);
    if (!s)
        s = ud_rest_delivery(t->config, &t->delivery);
    if (!s && t->delivery == UD_DELIVERY_RECEIPT)
        s = ud_json_string(ud_member(t->config, "receiptUrl"), &t->receipt_url);
    if (!s && t->delivery == UD_DELIVERY_RESPONSE) {
        if (ud_member(t->config, "maxAttempts") && t->options.attempts != 1)
            s = UD_SCHEMA;
        t->options.attempts = 1;
    }
    if (s) {
        ud_rest_target_close(t);
        return s;
    }
    *out = t;
    return UD_OK;
}
ud_status ud_rest_target_receipt(ud_rest_target *t, ud_string job, ud_string batch,
                                 ud_commit_receipt *out) {
    if (!t || t->delivery != UD_DELIVERY_RECEIPT)
        return UD_UNSUPPORTED;
    ud_string keys[] = {{"tenant", 6}, {"job", 3}, {"batch", 5}},
              values[] = {t->context.execution->options.tenant, job, batch}, url = {0};
    UD_TRY(query_url(t->context.memory, t->receipt_url, keys, values, 3, &url));
    ud_http_options request = t->options;
    request.url = url;
    request.method = (ud_string){"GET", 3};
    request.body = (ud_string){0};
    ud_http_response response = {0};
    ud_status s = ud_http_call(&t->context, t->lease, &request, &response);
    ud_free((void *)url.data);
    if (response.status == 404) {
        memset(out, 0, sizeof(*out));
        s = UD_OK;
    } else if (!s)
        s = receipt_parse(t, &response, out);
    ud_http_response_clear(&response);
    return s;
}
static ud_status member(ud_bytes *b, const char *name, ud_string value) {
    UD_TRY(ud_bytes_put(b, name, strlen(name)));
    return ud_json_quote(b, value.data, value.length);
}
ud_status ud_rest_target_write(ud_rest_target *t, const ud_write_options *o, const ud_batch *b,
                               const uint8_t digest[32]) {
    UD_TRY(ud_write_validate(o, b));
    ud_bytes body = {.ctx = t->context.memory, .limit = 64u * 1024u * 1024u};
    char hash[65];
    for (uint32_t i = 0; i < 32; i++)
        snprintf(hash + 2 * i, 3, "%02x", digest[i]);
    ud_status s = ud_bytes_put(&body, "{\"protocol\":" UD_PROTOCOL_VERSION_TEXT,
                               strlen("{\"protocol\":" UD_PROTOCOL_VERSION_TEXT));
    if (!s)
        s = member(&body, ",\"tenant\":", t->context.execution->options.tenant);
    if (!s)
        s = member(&body, ",\"table\":", o->table);
    if (!s)
        s = member(&body, ",\"job\":", o->job);
    if (!s)
        s = member(&body, ",\"batchId\":", o->batch_id);
    if (!s)
        s = member(&body, ",\"sourcePosition\":", o->source_position);
    if (!s)
        s = member(&body, ",\"digest\":", (ud_string){hash, 64});
    char mode[96];
    int n = snprintf(mode, sizeof(mode), ",\"mode\":%d,\"currentColumn\":%u,\"keys\":[", o->mode,
                     o->current_column);
    if (!s)
        s = ud_bytes_put(&body, mode, (size_t)n);
    for (uint32_t i = 0; !s && i < o->key_count; i++) {
        n = snprintf(mode, sizeof(mode), i ? ",%u" : "%u", o->keys[i]);
        s = ud_bytes_put(&body, mode, (size_t)n);
    }
    const char *start = "],\"operations\":[";
    if (!s)
        s = ud_bytes_put(&body, start, strlen(start));
    for (uint32_t i = 0; !s && i < b->rows; i++) {
        n = snprintf(mode, sizeof(mode), i ? ",%d" : "%d",
                     o->mode == UD_WRITE_CHANGESET ? o->operations[i] : 0);
        s = ud_bytes_put(&body, mode, (size_t)n);
    }
    start = "],\"schema\":";
    if (!s)
        s = ud_bytes_put(&body, start, strlen(start));
    char *json = NULL;
    uint32_t length;
    if (!s)
        s = ud_schema_to_json(b, &json, &length);
    if (!s)
        s = ud_bytes_put(&body, json, length);
    ud_free(json);
    json = NULL;
    if (!s)
        s = ud_bytes_put(&body, ",\"rows\":", 8);
    if (!s)
        s = ud_batch_to_json(b, &json, &length);
    if (!s)
        s = ud_bytes_put(&body, json, length);
    ud_free(json);
    json = NULL;
    if (!s)
        s = ud_bytes_put(&body, ",\"before\":", 10);
    if (!s && o->before)
        s = ud_batch_to_json(o->before, &json, &length);
    if (!s)
        s = ud_bytes_put(&body, json ? json : "null", json ? length : 4);
    ud_free(json);
    if (!s)
        s = ud_bytes_put(&body, ",\"rowInfo\":[", 12);
    for (uint32_t row = 0; !s && row < b->rows; row++) {
        char *info = NULL;
        uint32_t info_length = 0;
        if (row)
            s = ud_bytes_put(&body, ",", 1);
        if (!s)
            s = ud_batch_row_info_json(b, row, &info, &info_length);
        if (!s)
            s = ud_bytes_put(&body, info, info_length);
        ud_free(info);
    }
    if (!s)
        s = ud_bytes_put(&body, "],\"beforeRowInfo\":[", 19);
    for (uint32_t row = 0; !s && o->before && row < o->before->rows; row++) {
        char *info = NULL;
        uint32_t info_length = 0;
        if (row)
            s = ud_bytes_put(&body, ",", 1);
        if (!s)
            s = ud_batch_row_info_json(o->before, row, &info, &info_length);
        if (!s)
            s = ud_bytes_put(&body, info, info_length);
        ud_free(info);
    }
    if (!s)
        s = ud_bytes_put(&body, "]}", 2);
    ud_http_options request = t->options;
    request.method = (ud_string){"POST", 4};
    request.body = (ud_string){(char *)body.data, (uint32_t)body.size};
    request.content_type = (ud_string){"application/vnd.universaldata.batch+json", 40};
    request.content_type.length = (uint32_t)strlen(request.content_type.data);
    request.idempotency_key =
        t->delivery == UD_DELIVERY_RESPONSE ? (ud_string){0} : (ud_string){hash, 64};
    ud_http_response response = {0};
    if (!s)
        s = ud_http_call(&t->context, t->lease, &request, &response);
    if (!s && t->delivery == UD_DELIVERY_RECEIPT) {
        ud_commit_receipt receipt = {0};
        s = receipt_parse(t, &response, &receipt);
        if (!s && (!receipt.committed || memcmp(receipt.digest, digest, 32) ||
                   !ud_same(receipt.source_position, o->source_position)))
            s = UD_COMMIT_UNKNOWN;
        ud_free((void *)receipt.source_position.data);
    }
    if (s && response.status == 409)
        s = UD_FENCED;
    else if (s && response.attempted &&
             (!response.status || response.status >= 500 || response.status == 408))
        s = UD_COMMIT_UNKNOWN;
    ud_http_response_clear(&response);
    ud_bytes_clear(&body);
    return s;
}
void ud_rest_target_close(ud_rest_target *t) {
    if (t) {
        ud_json_destroy(t->config);
        ud_free(t);
    }
}
