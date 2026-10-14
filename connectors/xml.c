#include "xml.h"
#include <expat.h>
#include <libxml/xpathInternals.h>

/* Expat's allocation callbacks lack user data. Parsing is synchronous; this
 * scoped TLS pointer attributes every allocation to the calling Native domain. */
static _Thread_local ud_context *parser_memory;
typedef struct xml_allocation {
    size_t bytes;
} xml_allocation;
static void *parser_sized_alloc(size_t bytes);
static void *parser_realloc(void *old, size_t bytes) {
    /* Expat only needs byte-preserving realloc. Our own prefix records payload size. */
    if (!old)
        return parser_sized_alloc(bytes);
    if (bytes > SIZE_MAX - sizeof(xml_allocation))
        return NULL;
    xml_allocation *allocation = (xml_allocation *)old - 1;
    xml_allocation *next = ud_alloc(parser_memory, sizeof(*next) + bytes);
    if (!next)
        return NULL;
    next->bytes = bytes;
    memcpy(next + 1, old, allocation->bytes < bytes ? allocation->bytes : bytes);
    ud_free(allocation);
    return next + 1;
}
static void *parser_sized_alloc(size_t bytes) {
    if (bytes > SIZE_MAX - sizeof(xml_allocation))
        return NULL;
    xml_allocation *allocation = ud_alloc(parser_memory, sizeof(*allocation) + bytes);
    if (!allocation)
        return NULL;
    allocation->bytes = bytes;
    return allocation + 1;
}
static void parser_free(void *pointer) {
    if (pointer)
        ud_free((xml_allocation *)pointer - 1);
}
static XML_Parser create_parser(ud_context *memory) {
    ud_context *previous = parser_memory;
    parser_memory = memory;
    XML_Memory_Handling_Suite suite = {parser_sized_alloc, parser_realloc, parser_free};
    XML_Parser parser = XML_ParserCreate_MM("UTF-8", &suite, NULL);
    parser_memory = previous;
    return parser;
}
static enum XML_Status parse(ud_context *memory, XML_Parser parser, const char *bytes, int count,
                             int final, int resume) {
    ud_context *previous = parser_memory;
    parser_memory = memory;
    enum XML_Status status =
        resume ? XML_ResumeParser(parser) : XML_Parse(parser, bytes, count, final);
    parser_memory = previous;
    return status;
}
typedef struct xml_limits {
    const ud_component_context *context;
    XML_Parser parser;
    ud_status status;
    uint32_t depth, nodes;
} xml_limits;
static void XMLCALL limits_start(void *pointer, const char *name, const char **attributes) {
    (void)name;
    xml_limits *limits = pointer;
    if (++limits->depth > 64 || ++limits->nodes > 1000000)
        limits->status = UD_RESOURCE;
    for (uint32_t at = 0; attributes[at]; at += 2)
        if (++limits->nodes > 1000000)
            limits->status = UD_RESOURCE;
    if (!limits->status)
        limits->status = ud_activation_check(limits->context->activation);
    if (limits->status)
        XML_StopParser(limits->parser, XML_FALSE);
}
static void XMLCALL limits_end(void *pointer, const char *name) {
    (void)name;
    ((xml_limits *)pointer)->depth--;
}
static void XMLCALL reject_doctype(void *pointer, const char *name, const char *sysid,
                                   const char *pubid, int internal) {
    (void)name;
    (void)sysid;
    (void)pubid;
    (void)internal;
    xml_limits *limits = pointer;
    limits->status = UD_AUTH;
    XML_StopParser(limits->parser, XML_FALSE);
}
ud_status ud_xml_document(const ud_component_context *context, const uint8_t *bytes, uint32_t size,
                          xmlDocPtr *out, ud_memory_reservation **reservation) {
    *out = NULL;
    *reservation = NULL;
    XML_Parser parser = create_parser(context->memory);
    if (!parser)
        return UD_RESOURCE;
    xml_limits limits = {.context = context, .parser = parser};
    XML_SetUserData(parser, &limits);
    XML_SetElementHandler(parser, limits_start, limits_end);
    XML_SetStartDoctypeDeclHandler(parser, reject_doctype);
    enum XML_Status parsed = XML_STATUS_OK;
    for (uint32_t at = 0; !limits.status && at < size;) {
        uint32_t length = size - at > 16384 ? 16384 : size - at;
        parsed = parse(context->memory, parser, (const char *)bytes + at, (int)length,
                       at + length == size, 0);
        if (parsed == XML_STATUS_ERROR)
            break;
        at += length;
    }
    ud_status status = limits.status                      ? limits.status
                       : parsed != XML_STATUS_OK || !size ? UD_PARSE
                                                          : UD_OK;
    XML_ParserFree(parser);
    /* libxml DOM overhead is reserved before construction. Nodes and input are
     * independently capped, and no expansion or external documents are allowed. */
    if (!status)
        status = ud_memory_reserve(context->memory,
                                   (uint64_t)size * 4 + (uint64_t)limits.nodes * 512 + 65536,
                                   reservation);
    if (!status) {
        *out = xmlReadMemory((const char *)bytes, (int)size, NULL, "UTF-8",
                             XML_PARSE_NONET | XML_PARSE_NOERROR | XML_PARSE_NOWARNING);
        if (!*out)
            status = UD_PARSE;
    }
    if (status) {
        ud_memory_release(*reservation);
        *reservation = NULL;
    }
    return status;
}
ud_status ud_xml_read_document(const ud_component_context *context, ud_connection_lease *lease,
                               uint32_t maximum, xmlDocPtr *out,
                               ud_memory_reservation **reservation) {
    *out = NULL;
    *reservation = NULL;
    ud_string path;
    UD_TRY(ud_connection_lease_path(lease, &path));
    char *name = ud_strdup_n(context->memory, path.data, path.length);
    if (!name)
        return UD_RESOURCE;
    FILE *file = NULL;
    ud_status status = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
    if (!status)
        status = ud_open_regular(context->memory, name, &file);
    ud_bytes bytes = {.ctx = context->memory, .limit = maximum};
    uint8_t buffer[16384];
    while (!status) {
        status = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
        if (!status)
            status = ud_activation_check(context->activation);
        if (status)
            break;
        size_t size = fread(buffer, 1, sizeof(buffer), file);
        status = ud_bytes_put(&bytes, buffer, size);
        if (size < sizeof(buffer)) {
            if (ferror(file))
                status = UD_IO;
            break;
        }
    }
    if (file && fclose(file) && !status)
        status = UD_IO;
    if (!status)
        status = ud_xml_document(context, bytes.data, (uint32_t)bytes.size, out, reservation);
    ud_bytes_clear(&bytes);
    ud_free(name);
    return status;
}
ud_status ud_xml_namespaces(ud_context *memory, xmlXPathContextPtr xpath, const ud_json *names) {
    if (!names)
        return UD_OK;
    if (ud_json_kind(names) != UD_JSON_OBJECT || ud_json_size(names) > 64)
        return UD_SCHEMA;
    for (uint32_t at = 0; at < ud_json_size(names); at++) {
        ud_string prefix, uri;
        UD_TRY(ud_json_key(names, at, &prefix.data, &prefix.length));
        UD_TRY(ud_json_string(ud_json_at(names, at), &uri));
        if (!prefix.length || !uri.length || prefix.length > 128 || uri.length > 4096 ||
            memchr(prefix.data, 0, prefix.length) || memchr(uri.data, 0, uri.length))
            return UD_SCHEMA;
        char *p = ud_strdup_n(memory, prefix.data, prefix.length);
        char *u = ud_strdup_n(memory, uri.data, uri.length);
        ud_status status = !p || !u                                   ? UD_RESOURCE
                           : xmlValidateNCName((const xmlChar *)p, 0) ? UD_SCHEMA
                           : xmlXPathRegisterNs(xpath, (const xmlChar *)p, (const xmlChar *)u)
                               ? UD_RESOURCE
                               : UD_OK;
        ud_free(p);
        ud_free(u);
        if (status)
            return status;
    }
    return UD_OK;
}
ud_status ud_xml_xpath(ud_context *memory, xmlXPathContextPtr context, ud_string expression,
                       xmlXPathObjectPtr *out) {
    *out = NULL;
    if (!expression.length || expression.length > 16384 ||
        memchr(expression.data, 0, expression.length))
        return UD_SCHEMA;
    char *text = ud_strdup_n(memory, expression.data, expression.length);
    if (!text)
        return UD_RESOURCE;
    /* No extension functions are registered: XPath cannot read files or URLs. */
    context->opLimit = 1000000;
    *out = xmlXPathEvalExpression((const xmlChar *)text, context);
    ud_free(text);
    return *out ? UD_OK : UD_PARSE;
}
typedef struct xml_namespace {
    char *name, *value;
    uint32_t depth;
    struct xml_namespace *next;
} xml_namespace;
struct ud_xml_stream {
    ud_component_context context;
    ud_connection_lease *lease;
    XML_Parser parser;
    FILE *file;
    ud_bytes pending, path, declarations;
    xml_namespace *namespaces;
    ud_string record_path;
    uint64_t offset, pending_start, record_begin, record_end, ordinal;
    uint32_t depth, record_depth, maximum;
    size_t path_lengths[64];
    int capturing, complete, suspended, final, ended;
    ud_status status;
};
static ud_status namespace_escape(ud_bytes *bytes, const char *text) {
    for (; *text; text++) {
        const char *escaped = *text == '&'   ? "&amp;"
                              : *text == '"' ? "&quot;"
                              : *text == '<' ? "&lt;"
                                             : NULL;
        UD_TRY(ud_bytes_put(bytes, escaped ? escaped : text, escaped ? strlen(escaped) : 1));
    }
    return UD_OK;
}
static void namespace_pop(ud_xml_stream *stream, uint32_t depth) {
    while (stream->namespaces && stream->namespaces->depth >= depth) {
        xml_namespace *namespace = stream->namespaces;
        stream->namespaces = namespace->next;
        ud_free(namespace->name);
        ud_free(namespace->value);
        ud_free(namespace);
    }
}
static ud_status namespace_capture(ud_xml_stream *stream) {
    stream->declarations.size = 0;
    uint32_t count = 0;
    for (xml_namespace *namespace = stream->namespaces; namespace; namespace = namespace->next) {
        int hidden = 0;
        for (xml_namespace *newer = stream->namespaces; newer != namespace; newer = newer->next)
            if (!strcmp(newer->name, namespace->name))
                hidden = 1;
        if (hidden)
            continue;
        if (++count > 64)
            return UD_RESOURCE;
        UD_TRY(ud_bytes_put(&stream->declarations, " ", 1));
        UD_TRY(ud_bytes_put(&stream->declarations, namespace->name, strlen(namespace->name)));
        UD_TRY(ud_bytes_put(&stream->declarations, "=\"", 2));
        UD_TRY(namespace_escape(&stream->declarations, namespace->value));
        UD_TRY(ud_bytes_put(&stream->declarations, "\"", 1));
    }
    return UD_OK;
}
static void XMLCALL stream_start(void *pointer, const char *name, const char **attributes) {
    ud_xml_stream *stream = pointer;
    if (stream->depth == 64) {
        stream->status = UD_RESOURCE;
        XML_StopParser(stream->parser, XML_FALSE);
        return;
    }
    stream->path_lengths[stream->depth++] = stream->path.size;
    uint32_t attributes_count = 0;
    for (uint32_t at = 0; !stream->status && attributes[at]; at += 2) {
        if (++attributes_count > 1024) {
            stream->status = UD_RESOURCE;
            break;
        }
        if (strcmp(attributes[at], "xmlns") && strncmp(attributes[at], "xmlns:", 6))
            continue;
        xml_namespace *namespace = ud_alloc(stream->context.memory, sizeof(*namespace));
        if (!namespace) {
            stream->status = UD_RESOURCE;
            break;
        }
        namespace->depth = stream->depth;
        namespace->name =
            ud_strdup_n(stream->context.memory, attributes[at], strlen(attributes[at]));
        namespace->value =
            ud_strdup_n(stream->context.memory, attributes[at + 1], strlen(attributes[at + 1]));
        namespace->next = stream->namespaces;
        stream->namespaces = namespace;
        if (!namespace->name || !namespace->value)
            stream->status = UD_RESOURCE;
    }
    if (!stream->status)
        stream->status = ud_bytes_put(&stream->path, "/", 1);
    if (!stream->status)
        stream->status = ud_bytes_put(&stream->path, name, strlen(name));
    if (!stream->status && !stream->capturing && stream->path.size == stream->record_path.length &&
        !memcmp(stream->path.data, stream->record_path.data, stream->path.size)) {
        stream->capturing = 1;
        stream->record_depth = stream->depth;
        stream->record_begin = (uint64_t)XML_GetCurrentByteIndex(stream->parser);
        stream->status = namespace_capture(stream);
    }
    if (stream->status)
        XML_StopParser(stream->parser, XML_FALSE);
}
static void XMLCALL stream_end(void *pointer, const char *name) {
    (void)name;
    ud_xml_stream *stream = pointer;
    if (stream->capturing && stream->depth == stream->record_depth) {
        stream->record_end = (uint64_t)XML_GetCurrentByteIndex(stream->parser) +
                             (uint64_t)XML_GetCurrentByteCount(stream->parser);
        stream->capturing = 0;
        stream->complete = 1;
        XML_StopParser(stream->parser, XML_TRUE);
    }
    namespace_pop(stream, stream->depth);
    if (stream->depth)
        stream->path.size = stream->path_lengths[--stream->depth];
}
static void XMLCALL stream_doctype(void *pointer, const char *name, const char *sys,
                                   const char *pub, int internal) {
    (void)name;
    (void)sys;
    (void)pub;
    (void)internal;
    ud_xml_stream *stream = pointer;
    stream->status = UD_AUTH;
    XML_StopParser(stream->parser, XML_FALSE);
}
ud_status ud_xml_stream_open(const ud_component_context *context, ud_connection_lease *lease,
                             const char *path, ud_string record_path, uint32_t maximum,
                             ud_xml_stream **out) {
    *out = NULL;
    ud_xml_stream *stream = ud_alloc(context->memory, sizeof(*stream));
    if (!stream)
        return UD_RESOURCE;
    stream->context = *context;
    stream->lease = lease;
    stream->maximum = maximum;
    stream->record_path = record_path;
    stream->pending = (ud_bytes){.ctx = context->memory, .limit = (size_t)maximum + 16384};
    stream->path = (ud_bytes){.ctx = context->memory, .limit = 65536};
    stream->declarations = (ud_bytes){.ctx = context->memory, .limit = 65536};
    ud_status status = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
    if (!status)
        status = ud_open_regular(context->memory, path, &stream->file);
    if (!status) {
        stream->parser = create_parser(context->memory);
        if (!stream->parser)
            status = UD_RESOURCE;
    }
    if (!status) {
        XML_SetUserData(stream->parser, stream);
        XML_SetElementHandler(stream->parser, stream_start, stream_end);
        XML_SetStartDoctypeDeclHandler(stream->parser, stream_doctype);
        *out = stream;
    } else
        ud_xml_stream_close(stream);
    return status;
}
ud_status ud_xml_stream_next(ud_xml_stream *stream, ud_bytes *record) {
    record->size = 0;
    if (stream->status)
        return stream->status;
    if (stream->ended)
        return UD_END;
    while (!stream->status) {
        stream->status = ud_connection_lease_authorize(stream->lease, UD_ACCESS_READ);
        if (!stream->status)
            stream->status = ud_activation_check(stream->context.activation);
        if (stream->status)
            break;
        enum XML_Status parsed;
        if (stream->suspended) {
            stream->suspended = 0;
            parsed = parse(stream->context.memory, stream->parser, NULL, 0, 0, 1);
        } else {
            uint8_t bytes[16384];
            size_t count = fread(bytes, 1, sizeof(bytes), stream->file);
            if (ferror(stream->file)) {
                stream->status = UD_IO;
                break;
            }
            stream->final = count < sizeof(bytes);
            stream->status = ud_bytes_put(&stream->pending, bytes, count);
            if (stream->status)
                break;
            stream->offset += count;
            parsed = parse(stream->context.memory, stream->parser, (const char *)bytes, (int)count,
                           stream->final, 0);
        }
        if (parsed == XML_STATUS_ERROR && !stream->status)
            stream->status = UD_PARSE;
        if (stream->status)
            break;
        stream->suspended = parsed == XML_STATUS_SUSPENDED;
        if (stream->complete) {
            stream->complete = 0;
            uint64_t length = stream->record_end - stream->record_begin;
            if (stream->record_begin < stream->pending_start || length > stream->maximum ||
                stream->record_end > stream->pending_start + stream->pending.size)
                return stream->status = UD_RESOURCE;
            stream->status = ud_bytes_put(
                record,
                stream->pending.data + (size_t)(stream->record_begin - stream->pending_start),
                (size_t)length);
            if (stream->status)
                break;
            size_t used = (size_t)(stream->record_end - stream->pending_start);
            memmove(stream->pending.data, stream->pending.data + used, stream->pending.size - used);
            stream->pending.size -= used;
            stream->pending_start = stream->record_end;
            if (stream->ordinal == UINT64_MAX)
                return stream->status = UD_RESOURCE;
            stream->ordinal++;
            return UD_OK;
        }
        /* Keep a token split across input chunks: Expat may not call start until
         * the next chunk completes the opening tag. */
        uint64_t parsed_through = (uint64_t)XML_GetCurrentByteIndex(stream->parser) +
                                  (uint64_t)XML_GetCurrentByteCount(stream->parser);
        uint64_t keep = stream->capturing ? stream->record_begin : parsed_through;
        if (stream->final && !stream->suspended)
            keep = stream->offset;
        if (keep < stream->pending_start || keep > stream->pending_start + stream->pending.size)
            return stream->status = UD_CORRUPT;
        size_t consumed = (size_t)(keep - stream->pending_start);
        memmove(stream->pending.data, stream->pending.data + consumed,
                stream->pending.size - consumed);
        stream->pending.size -= consumed;
        stream->pending_start = keep;
        if (stream->pending.size > stream->maximum)
            return stream->status = UD_RESOURCE;
        if (stream->final && !stream->suspended) {
            stream->ended = 1;
            return UD_END;
        }
    }
    return stream->status;
}
uint64_t ud_xml_stream_record(const ud_xml_stream *stream) {
    return stream->ordinal;
}
ud_string ud_xml_stream_namespace_declarations(const ud_xml_stream *stream) {
    return (ud_string){(const char *)stream->declarations.data,
                       (uint32_t)stream->declarations.size};
}
void ud_xml_stream_close(ud_xml_stream *stream) {
    if (!stream)
        return;
    if (stream->parser)
        XML_ParserFree(stream->parser);
    if (stream->file)
        fclose(stream->file);
    ud_bytes_clear(&stream->pending);
    ud_bytes_clear(&stream->path);
    ud_bytes_clear(&stream->declarations);
    namespace_pop(stream, 0);
    ud_free(stream);
}
