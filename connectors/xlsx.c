#include "ud/xlsx.h"
#include "../components/data/internal.h"
#include <zlib.h>
#include <expat.h>
#include <math.h>
/* OOXML ZIP and SAX only. No Office process, macro/formula evaluator, temporary
 * extracted tree, private cache, or independent spill store. */
static _Thread_local ud_context *xml_context;
typedef union xml_allocation {
    long double alignment;
    size_t size;
} xml_allocation;
static void *xml_allocate(size_t n) {
    if (n > SIZE_MAX - sizeof(xml_allocation))
        return NULL;
    xml_allocation *a = ud_alloc(xml_context, n + sizeof(*a));
    if (!a)
        return NULL;
    a->size = n;
    return a + 1;
}
static void xml_deallocate(void *p) {
    if (p)
        ud_free((xml_allocation *)p - 1);
}
static void *xml_resize(void *p, size_t n) {
    if (!p)
        return xml_allocate(n);
    void *v = xml_allocate(n);
    if (!v)
        return NULL;
    size_t bytes = ((xml_allocation *)p - 1)->size;
    memcpy(v, p, bytes < n ? bytes : n);
    xml_deallocate(p);
    return v;
}
static voidpf z_allocate(voidpf ctx, uInt count, uInt size) {
    if (size && count > SIZE_MAX / size)
        return NULL;
    return ud_alloc(ctx, (size_t)count * size);
}
static void z_deallocate(voidpf ctx, voidpf p) {
    (void)ctx;
    ud_free(p);
}
typedef struct zip_entry {
    char *name;
    uint32_t crc, compressed, size, offset;
    uint16_t method, flags;
} zip_entry;
typedef struct zip {
    ud_context *ctx;
    FILE *file;
    zip_entry *entries;
    uint32_t count;
    uint64_t length, limit, compressed_limit;
    ud_xlsx_options options;
} zip;
typedef struct xml_stream {
    zip *zip;
    zip_entry *entry;
    XML_Parser parser;
    z_stream inflater;
    uint32_t remaining, produced, crc;
    int deflated, finished, suspended;
    unsigned char input[8192], output[8192];
    ud_status status;
    uint32_t depth;
} xml_stream;
typedef struct string_page {
    ud_buffer_block *block;
    uint64_t first;
    uint32_t count;
} string_page;
struct ud_xlsx_reader {
    ud_context *ctx;
    ud_xlsx_options options;
    ud_json *config;
    zip archive;
    xml_stream xml;
    ud_batch *schema, *result, *strings;
    string_page *pages;
    uint32_t page_count, page_capacity;
    uint64_t string_count;
    ud_buffer_view *string_view;
    uint32_t string_page;
    char *sheet, *relation, *sheet_path;
    int date1904, header, formula_policy, parse_kind, inside_text, inside_si, inside_value, formula,
        cell_error, row_error, validate_header;
    uint32_t header_fields;
    uint32_t first_col, last_col, first_row, last_row, row_number, cell_column, cell_style,
        row_last_column, styles[4096], style_count, current_style;
    uint64_t rows, cells, formulas, missing_formula, rejected, max_cells, max_string, max_xml;
    ud_value *values, *raw_values;
    ud_status record_error;
    uint32_t error_column;
    ud_bytes content, cell, formula_text;
    int row_open, cell_open, done, error_policy, result_rejected, deferred, range_finished;
    char cell_type[16];
    ud_batch *reject_schema;
    uint32_t columns;
    ud_status status;
    ud_id scope;
    char location[64];
};
typedef struct written_entry {
    const char *name;
    uint32_t crc, size, compressed, offset;
} written_entry;
struct ud_xlsx_writer {
    ud_context *ctx;
    ud_xlsx_options options;
    ud_json *config;
    FILE *file;
    ud_batch *schema;
    written_entry *entries;
    uint32_t count, capacity;
    zip original;
    ud_bytes tail;
    z_stream compressor;
    int compressed, header, date1904;
    uint64_t row, bytes, reserved;
    char *sheet;
    ud_status status;
};
static const ud_json *member(const ud_json *j, const char *k) {
    return ud_json_find(j, k, (uint32_t)strlen(k));
}
static int named(const ud_json *j, const char *s) {
    const char *p;
    uint32_t n;
    return j && !ud_json_text(j, &p, &n) && n == strlen(s) && !memcmp(p, s, n);
}
static ud_status uint_config(const ud_json *j, uint64_t default_value, uint64_t *out) {
    if (!j) {
        *out = default_value;
        return UD_OK;
    }
    const char *p;
    uint32_t n;
    UD_TRY(ud_json_text(j, &p, &n));
    if (ud_json_kind(j) != UD_JSON_NUMBER || !n)
        return UD_SCHEMA;
    uint64_t v = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (p[i] < '0' || p[i] > '9' || v > (UINT64_MAX - (uint32_t)(p[i] - '0')) / 10)
            return UD_SCHEMA;
        v = v * 10 + (uint32_t)(p[i] - '0');
    }
    *out = v;
    return UD_OK;
}
static ud_status control(const ud_xlsx_options *o) {
    return o->check ? o->check(o->user) : UD_OK;
}
static uint16_t u16(const unsigned char *p) {
    return (uint16_t)(p[0] | (uint16_t)p[1] << 8);
}
static uint32_t u32(const unsigned char *p) {
    return p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}
static void p16(unsigned char *p, uint32_t n) {
    p[0] = (unsigned char)n;
    p[1] = (unsigned char)(n >> 8);
}
static void p32(unsigned char *p, uint32_t n) {
    for (uint32_t i = 0; i < 4; i++)
        p[i] = (unsigned char)(n >> (8 * i));
}
static int safe_path(const char *p, uint32_t n) {
    if (!n || n > 512 || p[0] == '/' || memchr(p, 0, n) || memchr(p, '\\', n) || memchr(p, ':', n))
        return 0;
    uint32_t at = 0;
    for (uint32_t i = 0; i <= n; i++)
        if (i == n || p[i] == '/') {
            uint32_t k = i - at;
            if ((k == 1 && p[at] == '.') || (k == 2 && p[at] == '.' && p[at + 1] == '.'))
                return 0;
            at = i + 1;
        }
    return ud_utf8_valid(p, n);
}
static zip_entry *entry(zip *z, const char *name) {
    for (uint32_t i = 0; i < z->count; i++)
        if (!strcmp(z->entries[i].name, name))
            return z->entries + i;
    return NULL;
}
static void zip_close(zip *z) {
    if (z->file)
        fclose(z->file);
    for (uint32_t i = 0; i < z->count; i++)
        ud_free(z->entries[i].name);
    ud_free(z->entries);
    memset(z, 0, sizeof(*z));
}
static ud_status zip_open(ud_context *ctx, const char *path, const ud_xlsx_options *o,
                          const ud_json *config, zip *z) {
    z->ctx = ctx;
    z->options = *o;
    UD_TRY(uint_config(member(config, "maxCompressedBytes"), UINT64_C(536870912),
                       &z->compressed_limit));
    UD_TRY(uint_config(member(config, "maxDecompressedBytes"), UINT64_C(2147483648), &z->limit));
    uint64_t max_entries;
    UD_TRY(uint_config(member(config, "maxEntries"), 1024, &max_entries));
    if (!max_entries || max_entries > 4096 || !z->compressed_limit || !z->limit)
        return UD_SCHEMA;
    UD_TRY(ud_open_regular(ctx, path, &z->file));
    if (fseek(z->file, 0, SEEK_END))
        return UD_IO;
    int64_t length = ud_tell(z->file);
    if (length < 22 || (uint64_t)length > z->compressed_limit)
        return UD_RESOURCE;
    z->length = (uint64_t)length;
    uint32_t tail = (uint32_t)(z->length < 65557 ? z->length : 65557);
    unsigned char *bytes = ud_alloc(ctx, tail);
    if (!bytes)
        return UD_RESOURCE;
    ud_status s = ud_seek(z->file, z->length - tail) || fread(bytes, 1, tail, z->file) != tail
                      ? UD_IO
                      : UD_OK;
    int found = -1;
    for (int i = (int)tail - 22; !s && i >= 0; i--)
        if (u32(bytes + i) == UINT32_C(0x06054b50) && i + 22 + u16(bytes + i + 20) == (int)tail) {
            found = i;
            break;
        }
    if (!s && found < 0)
        s = UD_CORRUPT;
    uint32_t count = !s ? u16(bytes + found + 10) : 0, central = !s ? u32(bytes + found + 16) : 0,
             size = !s ? u32(bytes + found + 12) : 0;
    if (!s && (u16(bytes + found + 4) || u16(bytes + found + 6) ||
               count != u16(bytes + found + 8) || count == 65535 || central == UINT32_MAX ||
               (uint64_t)central + size > z->length - tail + (uint32_t)found))
        s = UD_UNSUPPORTED;
    if (!s && (!count || count > max_entries))
        s = UD_RESOURCE;
    ud_free(bytes);
    if (s)
        return s;
    z->entries = ud_alloc(ctx, count * sizeof(*z->entries));
    if (!z->entries)
        return UD_RESOURCE;
    if (ud_seek(z->file, central))
        return UD_IO;
    uint64_t total = 0;
    for (uint32_t i = 0; i < count; i++) {
        UD_TRY(control(o));
        unsigned char h[46];
        if (fread(h, 1, 46, z->file) != 46 || u32(h) != UINT32_C(0x02014b50))
            return UD_CORRUPT;
        uint32_t n = u16(h + 28), extra = u16(h + 30), comment = u16(h + 32);
        if (n > 512 || !n)
            return UD_CORRUPT;
        zip_entry *e = z->entries + i;
        z->count = i + 1;
        e->name = ud_alloc(ctx, n + 1);
        if (!e->name)
            return UD_RESOURCE;
        if (fread(e->name, 1, n, z->file) != n)
            return UD_CORRUPT;
        e->name[n] = 0;
        if (!safe_path(e->name, n))
            return UD_CORRUPT;
        for (uint32_t j = 0; j < i; j++)
            if (!strcmp(e->name, z->entries[j].name))
                return UD_CORRUPT;
        e->flags = u16(h + 8);
        e->method = u16(h + 10);
        e->crc = u32(h + 16);
        e->compressed = u32(h + 20);
        e->size = u32(h + 24);
        e->offset = u32(h + 42);
        if ((e->flags & 1) || (e->method != 0 && e->method != 8) || e->size == UINT32_MAX ||
            e->compressed == UINT32_MAX || e->offset == UINT32_MAX || u16(h + 34))
            return UD_UNSUPPORTED;
        if ((uint64_t)e->offset + 30 + e->compressed > central || e->size > z->limit - total ||
            (e->size > 1048576 &&
             (!e->compressed || (uint64_t)e->size > (uint64_t)e->compressed * 1000)))
            return UD_RESOURCE;
        total += e->size;
        if (strstr(e->name, "vbaProject") || strstr(e->name, "externalLinks/") ||
            strstr(e->name, "macros/"))
            return UD_UNSUPPORTED;
        if (fseek(z->file, (long)(extra + comment), SEEK_CUR))
            return UD_IO;
    }
    if (!entry(z, "[Content_Types].xml") || !entry(z, "xl/workbook.xml") ||
        !entry(z, "xl/_rels/workbook.xml.rels"))
        return UD_CORRUPT;
    return UD_OK;
}
static ud_status xml_open(xml_stream *x, zip *z, zip_entry *e) {
    memset(x, 0, sizeof(*x));
    x->zip = z;
    x->entry = e;
    if (!e)
        return UD_CORRUPT;
    unsigned char h[30];
    if (ud_seek(z->file, e->offset) || fread(h, 1, 30, z->file) != 30 ||
        u32(h) != UINT32_C(0x04034b50) || u16(h + 8) != e->method || (u16(h + 6) & 1))
        return UD_CORRUPT;
    uint32_t n = u16(h + 26), extra = u16(h + 28);
    if (n != strlen(e->name))
        return UD_CORRUPT;
    char name[513];
    if (fread(name, 1, n, z->file) != n || memcmp(name, e->name, n) ||
        fseek(z->file, (long)extra, SEEK_CUR))
        return UD_CORRUPT;
    xml_context = z->ctx;
    XML_Memory_Handling_Suite memory = {xml_allocate, xml_resize, xml_deallocate};
    x->parser = XML_ParserCreate_MM(NULL, &memory, "|");
    if (!x->parser)
        return UD_RESOURCE;
    XML_SetParamEntityParsing(x->parser, XML_PARAM_ENTITY_PARSING_NEVER);
    x->remaining = e->compressed;
    x->crc = (uint32_t)crc32(0, NULL, 0);
    if (e->method == 8) {
        x->inflater.zalloc = z_allocate;
        x->inflater.zfree = z_deallocate;
        x->inflater.opaque = z->ctx;
        if (inflateInit2(&x->inflater, -MAX_WBITS) != Z_OK)
            return UD_RESOURCE;
        x->deflated = 1;
    }
    return UD_OK;
}
static void xml_close(xml_stream *x) {
    if (x->deflated)
        inflateEnd(&x->inflater);
    if (x->parser)
        XML_ParserFree(x->parser);
    memset(x, 0, sizeof(*x));
}
static ud_status zip_chunk(xml_stream *x, uint32_t *count) {
    UD_TRY(control(&x->zip->options));
    uint32_t n = 0;
    if (x->deflated) {
        if (!x->inflater.avail_in && x->remaining) {
            uint32_t count = x->remaining > sizeof(x->input) ? sizeof(x->input) : x->remaining;
            if (fread(x->input, 1, count, x->zip->file) != count)
                return UD_CORRUPT;
            x->remaining -= count;
            x->inflater.next_in = x->input;
            x->inflater.avail_in = count;
        }
        x->inflater.next_out = x->output;
        x->inflater.avail_out = sizeof(x->output);
        int s = inflate(&x->inflater, Z_NO_FLUSH);
        n = sizeof(x->output) - x->inflater.avail_out;
        if (s == Z_STREAM_END) {
            if (x->remaining || x->inflater.avail_in)
                return UD_CORRUPT;
            x->finished = 1;
        } else if (s != Z_OK || (!n && !x->remaining && !x->inflater.avail_in))
            return UD_CORRUPT;
    } else {
        n = x->remaining > sizeof(x->output) ? sizeof(x->output) : x->remaining;
        if (n && fread(x->output, 1, n, x->zip->file) != n)
            return UD_CORRUPT;
        x->remaining -= n;
        x->finished = !x->remaining;
    }
    if (n > x->entry->size - x->produced)
        return UD_RESOURCE;
    x->produced += n;
    x->crc = (uint32_t)crc32(x->crc, x->output, n);
    if (x->finished && (x->produced != x->entry->size || x->crc != x->entry->crc))
        return UD_CORRUPT;
    *count = n;
    return UD_OK;
}
static ud_status xml_pump(xml_stream *x) {
    xml_context = x->zip->ctx;
    if (x->status)
        return x->status;
    if (x->suspended) {
        x->suspended = 0;
        enum XML_Status s = XML_ResumeParser(x->parser);
        if (s == XML_STATUS_SUSPENDED) {
            x->suspended = 1;
            return UD_OK;
        }
        if (s != XML_STATUS_OK)
            return x->status ? x->status : UD_PARSE;
    }
    while (!x->finished && !x->suspended) {
        uint32_t n = 0;
        UD_TRY(zip_chunk(x, &n));
        enum XML_Status status = XML_Parse(x->parser, (char *)x->output, (int)n, x->finished);
        if (status == XML_STATUS_SUSPENDED)
            x->suspended = 1;
        else if (status != XML_STATUS_OK)
            return x->status ? x->status : UD_PARSE;
        if (x->finished && (x->produced != x->entry->size || x->crc != x->entry->crc))
            return UD_CORRUPT;
    }
    return x->status;
}
static const char *local(const char *name) {
    const char *p = strrchr(name, '|');
    return p ? p + 1 : name;
}
static const char *attribute(const char **atts, const char *name) {
    for (uint32_t i = 0; atts && atts[i]; i += 2)
        if (!strcmp(local(atts[i]), name))
            return atts[i + 1];
    return NULL;
}
static uint32_t decimal(const char *p) {
    if (!p || !*p)
        return 0;
    uint64_t n = 0;
    for (; *p; p++) {
        if (*p < '0' || *p > '9' || n > (UINT32_MAX - (uint32_t)(*p - '0')) / 10)
            return UINT32_MAX;
        n = n * 10 + (uint32_t)(*p - '0');
    }
    return (uint32_t)n;
}
static ud_status cell_reference(const char *p, uint32_t *col, uint32_t *row) {
    uint32_t c = 0;
    while (*p >= 'A' && *p <= 'Z') {
        if (c > 16384)
            return UD_SCHEMA;
        c = c * 26 + (uint32_t)(*p - 'A' + 1);
        p++;
    }
    uint32_t r = decimal(p);
    if (!c || c > 16384 || !r || r > 1048576)
        return UD_SCHEMA;
    *col = c;
    *row = r;
    return UD_OK;
}
static void abort_xml(ud_xlsx_reader *r, ud_status status) {
    if (!r->status)
        r->status = status;
    r->xml.status = r->status;
    if (r->xml.parser)
        XML_StopParser(r->xml.parser, XML_FALSE);
}
static void XMLCALL doctype(void *user, const XML_Char *name, const XML_Char *sys,
                            const XML_Char *pub, int internal) {
    (void)name;
    (void)sys;
    (void)pub;
    (void)internal;
    abort_xml(user, UD_UNSUPPORTED);
}
static int XMLCALL external(XML_Parser parser, const XML_Char *context, const XML_Char *base,
                            const XML_Char *system, const XML_Char *public_id) {
    (void)context;
    (void)base;
    (void)system;
    (void)public_id;
    ud_xlsx_reader *r = XML_GetUserData(parser);
    abort_xml(r, UD_UNSUPPORTED);
    return XML_STATUS_ERROR;
}
static ud_status add_string(ud_xlsx_reader *r) {
    if (!r->strings) {
        ud_field f = {.name = "sharedString", .name_length = 12, .type = UD_UTF8};
        UD_TRY(ud_batch_create(r->ctx, &f, 1, 256, &r->strings));
    }
    ud_value v = {
        .type = UD_UTF8, .text = (char *)r->content.data, .text_length = (uint32_t)r->content.size};
    UD_TRY(ud_batch_append(r->strings, &v, 1));
    r->string_count++;
    return UD_OK;
}
static ud_status flush_strings(ud_xlsx_reader *r) {
    if (!r->strings || !r->strings->rows)
        return UD_OK;
    if (r->page_count == r->page_capacity) {
        uint32_t n = r->page_capacity ? r->page_capacity * 2 : 32;
        string_page *p = ud_alloc(r->ctx, n * sizeof(*p));
        if (!p)
            return UD_RESOURCE;
        if (r->pages)
            memcpy(p, r->pages, r->page_count * sizeof(*p));
        ud_free(r->pages);
        r->pages = p;
        r->page_capacity = n;
    }
    string_page *p = r->pages + r->page_count;
    p->first = r->string_count - r->strings->rows;
    p->count = r->strings->rows;
    UD_TRY(ud_batch_seal(r->strings));
    UD_TRY(ud_buffer_store(r->options.buffers, r->strings, &p->block));
    r->page_count++;
    ud_batch_destroy(r->strings);
    r->strings = NULL;
    return UD_OK;
}
static ud_status shared_string(ud_xlsx_reader *r, uint64_t index, ud_value *v) {
    if (index >= r->string_count)
        return UD_CORRUPT;
    uint32_t page_index = (uint32_t)(index / 256);
    if (page_index >= r->page_count)
        return UD_CORRUPT;
    if (!r->string_view || r->string_page != page_index) {
        ud_buffer_view_release(r->string_view);
        r->string_view = NULL;
        UD_TRY(ud_buffer_read(r->pages[page_index].block, &r->string_view));
        r->string_page = page_index;
    }
    return ud_batch_get(ud_buffer_view_batch(r->string_view),
                        (uint32_t)(index - r->pages[page_index].first), 0, v);
}
static ud_status ooxml_text(ud_context *ctx, const char *p, uint32_t n, ud_bytes *out) {
    for (uint32_t i = 0; i < n;) {
        if (i + 7 <= n && p[i] == '_' && p[i + 1] == 'x' && p[i + 6] == '_') {
            uint32_t code = 0;
            int valid = 1;
            for (uint32_t j = 2; j < 6; j++) {
                char c = p[i + j];
                int d = c >= '0' && c <= '9'   ? c - '0'
                        : c >= 'A' && c <= 'F' ? c - 'A' + 10
                        : c >= 'a' && c <= 'f' ? c - 'a' + 10
                                               : -1;
                if (d < 0) {
                    valid = 0;
                    break;
                }
                code = (code << 4) | (uint32_t)d;
            }
            if (valid) {
                i += 7;
                if (code >= 0xD800 && code <= 0xDBFF) {
                    if (i + 7 > n || p[i] != '_' || p[i + 1] != 'x' || p[i + 6] != '_')
                        return UD_PARSE;
                    uint32_t low = 0;
                    for (uint32_t j = 2; j < 6; j++) {
                        char c = p[i + j];
                        int d = c >= '0' && c <= '9'   ? c - '0'
                                : c >= 'A' && c <= 'F' ? c - 'A' + 10
                                : c >= 'a' && c <= 'f' ? c - 'a' + 10
                                                       : -1;
                        if (d < 0)
                            return UD_PARSE;
                        low = (low << 4) | (uint32_t)d;
                    }
                    if (low < 0xDC00 || low > 0xDFFF)
                        return UD_PARSE;
                    code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                    i += 7;
                } else if (code >= 0xDC00 && code <= 0xDFFF)
                    return UD_PARSE;
                char b[4];
                uint32_t size = code < 0x80 ? 1 : code < 0x800 ? 2 : code < 0x10000 ? 3 : 4;
                if (size == 1)
                    b[0] = (char)code;
                else {
                    for (uint32_t k = size - 1; k; k--) {
                        b[k] = (char)(0x80 | (code & 63));
                        code >>= 6;
                    }
                    b[0] = (char)((size == 2 ? 0xC0 : size == 3 ? 0xE0 : 0xF0) | code);
                }
                UD_TRY(ud_bytes_put(out, b, size));
                continue;
            }
        }
        UD_TRY(ud_bytes_put(out, p + i, 1));
        i++;
    }
    (void)ctx;
    return UD_OK;
}
static ud_status cell_text(ud_xlsx_reader *r, const char *type, ud_value *raw) {
    *raw = (ud_value){
        .type = UD_UTF8, .text = (char *)r->cell.data, .text_length = (uint32_t)r->cell.size};
    if (type && !strcmp(type, "s")) {
        uint64_t index = 0;
        for (uint32_t i = 0; i < raw->text_length; i++) {
            if (raw->text[i] < '0' || raw->text[i] > '9' ||
                index > (UINT64_MAX - (uint32_t)(raw->text[i] - '0')) / 10)
                return UD_PARSE;
            index = index * 10 + (uint32_t)(raw->text[i] - '0');
        }
        UD_TRY(shared_string(r, index, raw));
    }
    if (type && !strcmp(type, "e"))
        return UD_QUALITY;
    return UD_OK;
}
static ud_status cell_value(ud_xlsx_reader *r, const char *type, uint32_t target, ud_value *out) {
    const ud_field *f = &r->schema->column[target].field;
    ud_value raw;
    UD_TRY(cell_text(r, type, &raw));
    if (r->formula && !r->cell.size) {
        r->missing_formula++;
        if (r->formula_policy == 2) {
            *out = (ud_value){.type = f->type, .is_null = 1};
            return UD_OK;
        }
        return UD_QUALITY;
    }
    if (!raw.text_length && !r->formula) {
        *out = (ud_value){.type = f->type, .is_null = 1};
        return UD_OK;
    }
    ud_bytes decoded = {.ctx = r->ctx, .limit = (size_t)r->max_string};
    ud_status s = UD_OK;
    int string = type && (!strcmp(type, "s") || !strcmp(type, "str") || !strcmp(type, "inlineStr"));
    if (string) {
        s = ooxml_text(r->ctx, raw.text, raw.text_length, &decoded);
        raw.text = (char *)decoded.data;
        raw.text_length = (uint32_t)decoded.size;
    }
    if (!s && f->type == UD_BINARY) {
        if (!string || raw.text_length % 4)
            s = UD_PARSE;
        else {
            uint32_t bytes = raw.text_length / 4 * 3;
            char *binary = ud_alloc(r->ctx, bytes + 1);
            if (!binary)
                s = UD_RESOURCE;
            uint32_t used = 0;
            for (uint32_t i = 0; !s && i < raw.text_length; i += 4) {
                uint32_t word = 0, padding = 0;
                for (uint32_t k = 0; k < 4; k++) {
                    char c = raw.text[i + k];
                    int d = c >= 'A' && c <= 'Z'   ? c - 'A'
                            : c >= 'a' && c <= 'z' ? c - 'a' + 26
                            : c >= '0' && c <= '9' ? c - '0' + 52
                            : c == '+'             ? 62
                            : c == '/'             ? 63
                            : c == '='             ? 0
                                                   : -1;
                    if (d < 0 || (c == '=' && (i + 4 != raw.text_length || k < 2)))
                        s = UD_PARSE;
                    if (c == '=')
                        padding++;
                    word = (word << 6) | (uint32_t)(d < 0 ? 0 : d);
                }
                if (!s) {
                    binary[used++] = (char)(word >> 16);
                    if (padding < 2)
                        binary[used++] = (char)(word >> 8);
                    if (!padding)
                        binary[used++] = (char)word;
                }
            }
            if (!s)
                *out = (ud_value){.type = UD_BINARY, .text = binary, .text_length = used};
            else
                ud_free(binary);
        }
    } else if (!s && (f->type == UD_TIMESTAMP || f->type == UD_DATE32 || f->type == UD_DATETIME) &&
               !string && (!type || strcmp(type, "d"))) {
        ud_decimal128 d;
        s = ud_decimal_parse(raw.text, raw.text_length, 38, 15, UD_ROUND_HALF_EVEN, &d);
        ud_decimal128 days = {0};
        if (!s)
            s = ud_decimal_rescale(d, 15, 19, 0, UD_ROUND_ZERO, &days);
        if (!s && (days.high || days.low > 2958465))
            s = UD_PARSE;
        int64_t day = (int64_t)days.low;
        if (!s && !r->date1904 && day == 60)
            s = UD_PARSE;
        if (!s) {
            ud_field a = {.type = UD_DECIMAL128, .precision = 38, .scale = 15},
                     factor = {.type = UD_DECIMAL128, .precision = 11, .scale = 0}, result = {0};
            ud_value value = {.type = UD_DECIMAL128, .precision = 38, .scale = 15, .decimal = d},
                     multiplier = {.type = UD_DECIMAL128,
                                   .precision = 11,
                                   .decimal = {UINT64_C(86400000000), 0}},
                     product = {0};
            s = ud_numeric_result_field(UD_NUMERIC_MULTIPLY, &a, &factor, &result);
            if (!s)
                s = ud_value_numeric(UD_NUMERIC_MULTIPLY, &value, &multiplier, &result, &product);
            ud_decimal128 rounded = {0};
            if (!s)
                s = ud_decimal_rescale(product.decimal, result.scale, 19, 0, UD_ROUND_HALF_EVEN,
                                       &rounded);
            if (!s && (rounded.high || rounded.low > INT64_MAX))
                s = UD_OVERFLOW;
            if (!s) {
                int64_t offset = r->date1904 ? 24107 : 25569;
                int64_t micro = (int64_t)rounded.low +
                                (int64_t)(!r->date1904 && day < 60) * INT64_C(86400000000) -
                                offset * INT64_C(86400000000);
                *out = (ud_value){.type = f->type,
                                  .integer = f->type == UD_DATE32
                                                 ? day - offset + (!r->date1904 && day < 60 ? 1 : 0)
                                                 : micro};
                if (f->type == UD_DATETIME) {
                    ud_field from = {.type = UD_TIMESTAMP};
                    ud_value value = {.type = UD_TIMESTAMP, .integer = micro};
                    ud_datetime datetime;
                    s = ud_datetime_from_value(&from, &value, &datetime);
                    if (!s)
                        s = ud_datetime_to_value(f, datetime, out);
                }
            }
        }
    } else if (!s && f->type == UD_BOOL && type && !strcmp(type, "b")) {
        if (raw.text_length != 1 || (raw.text[0] != '0' && raw.text[0] != '1'))
            s = UD_PARSE;
        else
            *out = (ud_value){.type = UD_BOOL, .integer = raw.text[0] == '1'};
    } else if (!s) {
        ud_value parsed;
        s = ud_value_parse_field(r->ctx, f, raw.text, raw.text_length, UD_ROUND_EXACT, &parsed);
        if (!s)
            s = ud_copy_value(r->ctx, &parsed, out, 0);
    }
    ud_bytes_clear(&decoded);
    return s;
}
static void XMLCALL start(void *user, const char *qualified, const char **atts) {
    ud_xlsx_reader *r = user;
    const char *name = local(qualified);
    if (++r->xml.depth > 32) {
        abort_xml(r, UD_RESOURCE);
        return;
    }
    if (r->parse_kind == 1) {
        if (!strcmp(name, "workbookPr")) {
            const char *d = attribute(atts, "date1904");
            r->date1904 = d && (!strcmp(d, "1") || !strcmp(d, "true"));
        }
        if (!strcmp(name, "sheet")) {
            const char *n = attribute(atts, "name"), *id = attribute(atts, "id");
            if (n && id && (!r->sheet || !strcmp(r->sheet, n)) && !r->relation) {
                if (!r->sheet)
                    r->sheet = ud_strdup_n(r->ctx, n, strlen(n));
                r->relation = ud_strdup_n(r->ctx, id, strlen(id));
                if (!r->sheet || !r->relation)
                    abort_xml(r, UD_RESOURCE);
            }
        }
    } else if (r->parse_kind == 2) {
        if (!strcmp(name, "Relationship")) {
            const char *mode = attribute(atts, "TargetMode"), *target = attribute(atts, "Target"),
                       *id = attribute(atts, "Id");
            if (mode && !strcmp(mode, "External")) {
                abort_xml(r, UD_UNSUPPORTED);
                return;
            }
            if (target && id && r->relation && !strcmp(id, r->relation) && !r->sheet_path) {
                if (!strncmp(target, "/xl/", 4))
                    target++;
                if (!safe_path(target, (uint32_t)strlen(target))) {
                    abort_xml(r, UD_CORRUPT);
                    return;
                }
                if (!strncmp(target, "xl/", 3))
                    r->sheet_path = ud_strdup_n(r->ctx, target, strlen(target));
                else {
                    size_t n = strlen(target) + 4;
                    r->sheet_path = ud_alloc(r->ctx, n);
                    if (r->sheet_path)
                        snprintf(r->sheet_path, n, "xl/%s", target);
                }
                if (!r->sheet_path)
                    abort_xml(r, UD_RESOURCE);
            }
        }
    } else if (r->parse_kind == 4) {
        if (!strcmp(name, "si")) {
            r->inside_si = 1;
            r->content.size = 0;
        }
        if (!strcmp(name, "t") && r->inside_si)
            r->inside_text = 1;
    } else if (r->parse_kind == 6) {
        const char *t = attribute(atts, "ContentType");
        if (t && (strstr(t, "macroEnabled") || strstr(t, "vbaProject")))
            abort_xml(r, UD_UNSUPPORTED);
    } else if (r->parse_kind == 5) {
        if (!strcmp(name, "row")) {
            uint32_t row = decimal(attribute(atts, "r"));
            if (!row)
                row = r->row_number + 1;
            if (row <= r->row_number || row > 1048576) {
                abort_xml(r, UD_CORRUPT);
                return;
            }
            r->row_number = row;
            r->row_open = 1;
            r->row_error = 0;
            r->record_error = UD_OK;
            r->error_column = UINT32_MAX;
            r->row_last_column = 0;
            for (uint32_t i = 0; i < r->schema->columns; i++) {
                ud_clear_value(r->values + i);
                ud_clear_value(r->raw_values + i);
                r->values[i] = (ud_value){.type = r->schema->column[i].field.type, .is_null = 1};
            }
        } else if (!strcmp(name, "c")) {
            if (!r->row_open || r->cell_open) {
                abort_xml(r, UD_CORRUPT);
                return;
            }
            const char *ref = attribute(atts, "r");
            uint32_t row = 0, col = r->row_last_column + 1;
            ud_status s = ref ? cell_reference(ref, &col, &row) : UD_OK;
            if (s || (ref && row != r->row_number) || col <= r->row_last_column || col > 16384) {
                abort_xml(r, UD_CORRUPT);
                return;
            }
            r->cell_column = col;
            r->row_last_column = col;
            r->cell_open = 1;
            r->formula = 0;
            r->cell_error = 0;
            r->cell.size = 0;
            r->formula_text.size = 0;
            const char *type = attribute(atts, "t");
            if (type && strlen(type) >= sizeof(r->cell_type)) {
                abort_xml(r, UD_CORRUPT);
                return;
            }
            snprintf(r->cell_type, sizeof(r->cell_type), "%s", type ? type : "");
            r->cell_style = decimal(attribute(atts, "s"));
            if (++r->cells > r->max_cells)
                abort_xml(r, UD_RESOURCE);
        } else if (!strcmp(name, "v") || (!strcmp(name, "t") && r->cell_open))
            r->inside_value = 1;
        else if (!strcmp(name, "f")) {
            r->formula = 1;
            r->formulas++;
        }
    }
}
static void XMLCALL characters(void *user, const char *p, int n) {
    ud_xlsx_reader *r = user;
    if (n < 0) {
        abort_xml(r, UD_PARSE);
        return;
    }
    ud_bytes *b = r->parse_kind == 4 && r->inside_text    ? &r->content
                  : r->parse_kind == 5 && r->inside_value ? &r->cell
                                                          : NULL;
    if (b) {
        if ((uint64_t)n > r->max_string - b->size) {
            abort_xml(r, UD_RESOURCE);
            return;
        }
        ud_status s = ud_bytes_put(b, p, (size_t)n);
        if (s)
            abort_xml(r, s);
    }
}
static void XMLCALL end(void *user, const char *qualified) {
    ud_xlsx_reader *r = user;
    const char *name = local(qualified);
    if (r->parse_kind == 4) {
        if (!strcmp(name, "t"))
            r->inside_text = 0;
        if (!strcmp(name, "si")) {
            r->inside_si = 0;
            ud_status s = add_string(r);
            if (!s && r->strings->rows == 256)
                s = flush_strings(r);
            if (s)
                abort_xml(r, s);
        }
    } else if (r->parse_kind == 5) {
        if (!strcmp(name, "v") || !strcmp(name, "t"))
            r->inside_value = 0;
        if (!strcmp(name, "c")) {
            r->cell_open = 0;
            if (r->validate_header && r->header && r->row_number == r->first_row &&
                r->cell_column >= r->first_col && r->cell_column <= r->last_col) {
                const ud_field *field = &r->schema->column[r->cell_column - r->first_col].field;
                ud_value raw;
                ud_bytes decoded = {.ctx = r->ctx, .limit = (size_t)r->max_string};
                ud_status status = r->formula ? UD_INCOMPATIBLE : cell_text(r, r->cell_type, &raw);
                if (!status)
                    status = ooxml_text(r->ctx, raw.text, raw.text_length, &decoded);
                if (!status && (decoded.size != field->name_length ||
                                 memcmp(decoded.data, field->name, decoded.size)))
                    status = UD_INCOMPATIBLE;
                ud_bytes_clear(&decoded);
                if (status)
                    abort_xml(r, status);
                else
                    r->header_fields++;
            }
            if (r->row_number >= r->first_row && r->row_number <= r->last_row &&
                r->cell_column >= r->first_col && r->cell_column <= r->last_col &&
                !(r->header && r->row_number == r->first_row)) {
                uint32_t column = r->cell_column - r->first_col;
                ud_value raw_cell = {0};
                ud_status s = cell_text(r, r->cell_type, &raw_cell);
                if (!s) {
                    raw_cell.type = UD_BINARY;
                    s = ud_copy_value(r->ctx, &raw_cell, r->raw_values + column, 0);
                }
                if (!s)
                    s = cell_value(r, r->cell_type, column, r->values + column);
                if (s) {
                    snprintf(r->location, sizeof(r->location), "row:%u/column:%u", r->row_number,
                             r->cell_column);
                    if (ud_record_error_recoverable(s) &&
                        ((r->formula_policy == 1 && r->formula && !r->cell.size) ||
                         r->error_policy == 1)) {
                        if (!r->record_error) {
                            r->record_error = s;
                            r->error_column = column;
                        }
                        r->row_error = 1;
                        ud_clear_value(r->values + column);
                        r->values[column] =
                            (ud_value){.type = r->schema->column[column].field.type, .is_null = 1};
                    } else
                        abort_xml(r, s);
                }
            }
        }
        if (!strcmp(name, "row")) {
            r->row_open = 0;
            if (r->validate_header && r->header && r->row_number == r->first_row &&
                (r->header_fields != r->columns || r->row_last_column != r->last_col))
                abort_xml(r, UD_INCOMPATIBLE);
            if (!r->status && r->row_number >= r->first_row && r->row_number <= r->last_row &&
                !(r->header && r->row_number == r->first_row)) {
                if (r->result && r->result->rows &&
                    r->row_error) { /* Defer this row until the next caller-owned batch. */
                    r->deferred = 1;
                    XML_StopParser(r->xml.parser, XML_TRUE);
                } else {
                    ud_status s = UD_OK;
                    if (r->row_error) {
                        ud_batch_destroy(r->result);
                        r->result = NULL;
                        s = ud_batch_create(r->ctx, r->reject_schema->schema->fields,
                                            r->reject_schema->columns, 1, &r->result);
                        r->result_rejected = 1;
                        r->rejected++;
                    }
                    if (!s)
                        s = ud_batch_append(r->result, r->values, r->schema->columns);
                    if (!s) {
                        ud_row_info info = {0};
                        s = ud_row_info_source(
                            r->scope,
                            (uint64_t)r->row_number - r->first_row - (r->header ? 1u : 0u), &info);
                        if (!s)
                            s = ud_batch_set_row_info(r->result, r->result->rows - 1, &info);
                    }
                    if (s)
                        abort_xml(r, s);
                    else {
                        r->rows++;
                        if (r->result_rejected || r->result->rows == r->options.batch_size)
                            XML_StopParser(r->xml.parser, XML_TRUE);
                    }
                }
            }
            if (r->row_number >= r->last_row)
                r->range_finished = 1;
        }
    }
    if (r->xml.depth)
        r->xml.depth--;
}
static ud_status parse_part(ud_xlsx_reader *r, const char *name, int kind) {
    zip_entry *e = entry(&r->archive, name);
    if (!e)
        return kind == 4 ? UD_OK : UD_CORRUPT;
    r->parse_kind = kind;
    UD_TRY(xml_open(&r->xml, &r->archive, e));
    XML_SetUserData(r->xml.parser, r);
    XML_SetElementHandler(r->xml.parser, start, end);
    XML_SetCharacterDataHandler(r->xml.parser, characters);
    XML_SetStartDoctypeDeclHandler(r->xml.parser, doctype);
    XML_SetExternalEntityRefHandler(r->xml.parser, external);
    ud_status s = xml_pump(&r->xml);
    if (kind != 5)
        xml_close(&r->xml);
    return s;
}
ud_status ud_xlsx_reader_open(ud_context *ctx, const char *path, const ud_field *fields,
                              uint32_t columns, const ud_xlsx_options *o, ud_xlsx_reader **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !path || !o || o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION ||
        !o->batch_size || o->batch_size > 65536 || !columns || columns > 16384 || !o->buffers)
        return UD_INVALID;
    ud_xlsx_reader *r = ud_alloc(ctx, sizeof(*r));
    if (!r)
        return UD_RESOURCE;
    r->ctx = ctx;
    r->columns = columns;
    r->options = *o;
    r->content.ctx = r->cell.ctx = r->formula_text.ctx = ctx;
    ud_status s =
        ud_json_parse(ctx, o->configuration.data, o->configuration.length, &r->config, NULL);
    if (!s)
        s = ud_batch_create(ctx, fields, columns, 0, &r->schema);
    if (!s) {
        ud_field *nullable = ud_alloc(ctx, columns * sizeof(*nullable));
        if (!nullable)
            s = UD_RESOURCE;
        else {
            for (uint32_t i = 0; i < columns; i++) {
                nullable[i] = fields[i];
                nullable[i].nullable = 1;
            }
            s = ud_batch_create(ctx, nullable, columns, 0, &r->reject_schema);
            ud_free(nullable);
        }
    }
    r->first_col = 1;
    r->last_col = columns;
    r->first_row = 1;
    r->last_row = 1048576;
    if (!s && member(r->config, "sheet")) {
        const char *p;
        uint32_t n;
        s = ud_json_text(member(r->config, "sheet"), &p, &n);
        if (!s && (ud_json_kind(member(r->config, "sheet")) != UD_JSON_STRING || !n || n > 31 ||
                   memchr(p, 0, n)))
            s = UD_SCHEMA;
        if (!s) {
            r->sheet = ud_strdup_n(ctx, p, n);
            if (!r->sheet)
                s = UD_RESOURCE;
        }
    }
    if (!s && member(r->config, "range")) {
        const char *p;
        uint32_t n;
        s = ud_json_text(member(r->config, "range"), &p, &n);
        if (!s && (n > 64 || memchr(p, 0, n)))
            s = UD_SCHEMA;
        char range[65];
        if (!s) {
            memcpy(range, p, n);
            range[n] = 0;
            char *sep = strchr(range, ':');
            if (!sep)
                s = UD_SCHEMA;
            else {
                *sep = 0;
                s = cell_reference(range, &r->first_col, &r->first_row);
                if (!s)
                    s = cell_reference(sep + 1, &r->last_col, &r->last_row);
                if (!s && (r->last_col < r->first_col || r->last_row < r->first_row ||
                           r->last_col - r->first_col + 1 != columns))
                    s = UD_SCHEMA;
            }
        }
    }
    r->header = named(member(r->config, "header"), "true");
    const ud_json *fp = member(r->config, "formulaPolicy");
    r->formula_policy = !fp || named(fp, "fail") ? 0
                        : named(fp, "redirect")  ? 1
                        : named(fp, "null")      ? 2
                                                 : -1;
    if (r->formula_policy < 0)
        s = UD_SCHEMA;
    const ud_json *ep = member(r->config, "errorPolicy");
    r->error_policy = !ep || named(ep, "fail") ? 0 : named(ep, "redirect") ? 1 : -1;
    if (r->error_policy < 0)
        s = UD_SCHEMA;
    if (!s)
        s = uint_config(member(r->config, "maxCells"), UINT64_C(17179869184), &r->max_cells);
    if (!s)
        s = uint_config(member(r->config, "maxStringBytes"), 1048576, &r->max_string);
    if (!s && (r->max_string > 16777216 || !r->max_string))
        s = UD_RESOURCE;
    if (!s)
        s = zip_open(ctx, path, o, r->config, &r->archive);
    if (!s)
        s = parse_part(r, "[Content_Types].xml", 6);
    if (!s)
        s = parse_part(r, "xl/workbook.xml", 1);
    if (!s && !r->relation)
        s = UD_SCHEMA;
    if (!s)
        s = parse_part(r, "xl/_rels/workbook.xml.rels", 2);
    if (!s && !r->sheet_path)
        s = UD_CORRUPT;
    for (uint32_t i = 0; !s && i < r->archive.count; i++) {
        const char *name = r->archive.entries[i].name;
        size_t n = strlen(name);
        if (n >= 5 && !strcmp(name + n - 5, ".rels") && strcmp(name, "xl/_rels/workbook.xml.rels"))
            s = parse_part(r, name, 2);
    }
    const ud_json *date = member(r->config, "datePolicy");
    if (!s && date && !named(date, "workbook") && !named(date, r->date1904 ? "1904" : "1900"))
        s = UD_INCOMPATIBLE;
    if (!s)
        s = parse_part(r, "xl/sharedStrings.xml", 4);
    if (!s)
        s = flush_strings(r);
    if (!s) {
        r->values = ud_alloc(ctx, columns * sizeof(*r->values));
        r->raw_values = ud_alloc(ctx, columns * sizeof(*r->raw_values));
        if (!r->values || !r->raw_values)
            s = UD_RESOURCE;
    }
    if (!s) {
        uint8_t digest[32];
        s = ud_file_hash(ctx, path, digest);
        char hash[65];
        static const char hex[] = "0123456789abcdef";
        for (uint32_t i = 0; i < 32; i++) {
            hash[2 * i] = hex[digest[i] >> 4];
            hash[2 * i + 1] = hex[digest[i] & 15];
        }
        hash[64] = 0;
        ud_bytes scope = {.ctx = ctx};
        if (!s)
            s = ud_put_string(&scope, r->sheet, strlen(r->sheet));
        if (!s)
            s = ud_put_u64(&scope, r->first_col);
        if (!s)
            s = ud_put_u64(&scope, r->first_row);
        if (!s)
            s = ud_put_u64(&scope, r->last_col);
        if (!s)
            s = ud_put_u64(&scope, r->last_row);
        uint8_t h[32];
        if (!s) {
            ud_hash(scope.data, scope.size, h);
            char symbol[65];
            for (uint32_t i = 0; i < 32; i++) {
                symbol[i * 2] = hex[h[i] >> 4];
                symbol[i * 2 + 1] = hex[h[i] & 15];
            }
            symbol[64] = 0;
            s = ud_id_make(ctx, (ud_string){hash, 64}, (ud_string){symbol, 64}, &r->scope);
        }
        ud_bytes_clear(&scope);
    }
    if (!s) {
        r->parse_kind = 5;
        s = xml_open(&r->xml, &r->archive, entry(&r->archive, r->sheet_path));
        if (!s) {
            XML_SetUserData(r->xml.parser, r);
            XML_SetElementHandler(r->xml.parser, start, end);
            XML_SetCharacterDataHandler(r->xml.parser, characters);
            XML_SetStartDoctypeDeclHandler(r->xml.parser, doctype);
            XML_SetExternalEntityRefHandler(r->xml.parser, external);
        }
    }
    if (s) {
        ud_xlsx_reader_close(r);
        return s;
    }
    *out = r;
    return UD_OK;
}
ud_status ud_xlsx_reader_next(ud_xlsx_reader *r, ud_batch **out, int32_t *rejected) {
    if (!r || !out)
        return UD_INVALID;
    *out = NULL;
    if (rejected)
        *rejected = 0;
    if (r->done)
        return UD_END;
    r->result_rejected = 0;
    UD_TRY(ud_batch_create(r->ctx, r->schema->schema->fields, r->schema->columns,
                           r->options.batch_size, &r->result));
    ud_status s = UD_OK;
    if (r->deferred) {
        r->deferred = 0;
        ud_batch_destroy(r->result);
        r->result = NULL;
        s = ud_batch_create(r->ctx, r->reject_schema->schema->fields, r->reject_schema->columns, 1,
                            &r->result);
        if (!s)
            s = ud_batch_append(r->result, r->values, r->schema->columns);
        if (!s) {
            ud_row_info info = {0};
            s = ud_row_info_source(
                r->scope, (uint64_t)r->row_number - r->first_row - (r->header ? 1u : 0u), &info);
            if (!s)
                s = ud_batch_set_row_info(r->result, 0, &info);
        }
        r->result_rejected = 1;
        r->rejected++;
        r->rows++;
    } else {
        s = xml_pump(&r->xml);
        r->done = r->xml.finished && !r->xml.suspended;
    }
    if (!s && r->result->rows) {
        s = ud_batch_seal(r->result);
        if (!s) {
            *out = r->result;
            r->result = NULL;
            if (rejected)
                *rejected = r->result_rejected;
        }
    } else if (!s && r->done)
        s = UD_END;
    else if (!s)
        s = UD_CORRUPT;
    ud_batch_destroy(r->result);
    r->result = NULL;
    return s;
}
ud_status ud_xlsx_reader_metadata(const ud_xlsx_reader *r, char **out, uint32_t *length) {
    if (!r || !out || !length)
        return UD_INVALID;
    ud_bytes b = {.ctx = r->ctx};
    const char *prefix = "{\"format\":\"xlsx\",\"sheet\":";
    ud_status s = ud_bytes_put(&b, prefix, strlen(prefix));
    if (!s)
        s = ud_json_quote(&b, r->sheet, (uint32_t)strlen(r->sheet));
    char n[512];
    int count =
        snprintf(n, sizeof(n),
                 ",\"dateSystem\":%u,\"firstColumn\":%u,\"lastColumn\":%u,\"firstRow\":%u,"
                 "\"lastRow\":%u,\"rows\":%llu,\"cells\":%llu,\"sharedStrings\":%llu,\"formulas\":%"
                 "llu,\"missingCachedFormula\":%llu,\"rejected\":%llu,\"lastErrorLocation\":",
                 r->date1904 ? 1904 : 1900, r->first_col, r->last_col, r->first_row, r->last_row,
                 (unsigned long long)r->rows, (unsigned long long)r->cells,
                 (unsigned long long)r->string_count, (unsigned long long)r->formulas,
                 (unsigned long long)r->missing_formula, (unsigned long long)r->rejected);
    if (!s)
        s = ud_bytes_put(&b, n, (size_t)count);
    if (!s)
        s = ud_json_quote(&b, r->location, (uint32_t)strlen(r->location));
    if (!s)
        s = ud_bytes_put(&b, "}", 1);
    if (!s)
        s = ud_bytes_put(&b, "\0", 1);
    if (!s) {
        *length = (uint32_t)b.size - 1;
        *out = (char *)b.data;
        b.data = NULL;
    }
    ud_bytes_clear(&b);
    return s;
}
void ud_xlsx_reader_close(ud_xlsx_reader *r) {
    if (!r)
        return;
    xml_close(&r->xml);
    zip_close(&r->archive);
    ud_buffer_view_release(r->string_view);
    for (uint32_t i = 0; i < r->page_count; i++)
        ud_buffer_release(r->pages[i].block);
    ud_free(r->pages);
    ud_batch_destroy(r->strings);
    ud_batch_destroy(r->result);
    ud_batch_destroy(r->schema);
    ud_batch_destroy(r->reject_schema);
    if (r->values)
        for (uint32_t i = 0; i < r->columns; i++)
            ud_clear_value(r->values + i);
    if (r->raw_values) {
        for (uint32_t j = 0; j < r->columns; j++)
            ud_clear_value(r->raw_values + j);
        ud_free(r->raw_values);
    }
    ud_free(r->values);
    ud_free(r->sheet);
    ud_free(r->relation);
    ud_free(r->sheet_path);
    ud_bytes_clear(&r->content);
    ud_bytes_clear(&r->cell);
    ud_bytes_clear(&r->formula_text);
    ud_json_destroy(r->config);
    ud_free(r);
}
static ud_status raw_write(ud_xlsx_writer *w, const void *bytes, size_t n) {
    UD_TRY(control(&w->options));
    uint64_t limit = UINT64_C(536870912);
    UD_TRY(uint_config(member(w->config, "maxCompressedBytes"), limit, &limit));
    if (n > limit - w->bytes || n > UINT32_MAX - w->bytes)
        return UD_RESOURCE;
    if (w->options.buffers) {
        UD_TRY(ud_buffer_reserve_external(w->options.buffers, n));
        if (w->options.stage_bytes)
            *w->options.stage_bytes += n;
        else
            w->reserved += n;
    }
    w->bytes += n;
    if (fwrite(bytes, 1, n, w->file) != n)
        return UD_IO;
    return UD_OK;
}
static ud_status deflate_data(ud_xlsx_writer *w, const void *data, uint32_t n, int finish) {
    written_entry *e = w->entries + w->count;
    if ((uint64_t)e->size + n > UINT32_MAX)
        return UD_RESOURCE;
    e->size += n;
    if (n)
        e->crc = (uint32_t)crc32(e->crc, data, n);
    w->compressor.next_in = (Bytef *)data;
    w->compressor.avail_in = n;
    int done = 0;
    do {
        unsigned char output[8192];
        w->compressor.next_out = output;
        w->compressor.avail_out = sizeof(output);
        int s = deflate(&w->compressor, finish ? Z_FINISH : Z_NO_FLUSH);
        if (s != Z_OK && s != Z_STREAM_END)
            return UD_RESOURCE;
        uint32_t count = sizeof(output) - w->compressor.avail_out;
        if (count > UINT32_MAX - e->compressed)
            return UD_OVERFLOW;
        UD_TRY(raw_write(w, output, count));
        e->compressed += count;
        done = s == Z_STREAM_END;
    } while (w->compressor.avail_in || (finish && !done));
    return UD_OK;
}
static ud_status entry_begin(ud_xlsx_writer *w, const char *name) {
    if (w->count >= w->capacity)
        return UD_RESOURCE;
    written_entry *e = w->entries + w->count;
    *e = (written_entry){
        .name = name, .offset = (uint32_t)w->bytes, .crc = (uint32_t)crc32(0, NULL, 0)};
    unsigned char h[30] = {0};
    p32(h, UINT32_C(0x04034b50));
    p16(h + 4, 20);
    p16(h + 6, 0x808);
    p16(h + 8, 8);
    p16(h + 26, (uint32_t)strlen(name));
    UD_TRY(raw_write(w, h, sizeof(h)));
    UD_TRY(raw_write(w, name, strlen(name)));
    memset(&w->compressor, 0, sizeof(w->compressor));
    w->compressor.zalloc = z_allocate;
    w->compressor.zfree = z_deallocate;
    w->compressor.opaque = w->ctx;
    if (deflateInit2(&w->compressor, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -MAX_WBITS, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK)
        return UD_RESOURCE;
    w->compressed = 1;
    return UD_OK;
}
static ud_status entry_end(ud_xlsx_writer *w) {
    UD_TRY(deflate_data(w, NULL, 0, 1));
    deflateEnd(&w->compressor);
    w->compressed = 0;
    written_entry *e = w->entries + w->count;
    unsigned char h[16];
    p32(h, UINT32_C(0x08074b50));
    p32(h + 4, e->crc);
    p32(h + 8, e->compressed);
    p32(h + 12, e->size);
    UD_TRY(raw_write(w, h, 16));
    w->count++;
    return UD_OK;
}
static ud_status write_text(ud_xlsx_writer *w, const char *text) {
    return deflate_data(w, text, (uint32_t)strlen(text), 0);
}
static ud_status write_part(ud_xlsx_writer *w, const char *name, const char *text) {
    UD_TRY(entry_begin(w, name));
    UD_TRY(write_text(w, text));
    return entry_end(w);
}
static ud_status escape(ud_xlsx_writer *w, const char *p, uint32_t n, int ooxml) {
    ud_bytes b = {.ctx = w->ctx, .limit = 16777216};
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < n; i++) {
        unsigned char c = (unsigned char)p[i];
        const char *entity = c == '&'    ? "&amp;"
                             : c == '<'  ? "&lt;"
                             : c == '>'  ? "&gt;"
                             : c == '\"' ? "&quot;"
                                         : NULL;
        if (entity)
            s = ud_bytes_put(&b, entity, strlen(entity));
        else if (ooxml && ((c < 32 && c != 9 && c != 10 && c != 13) ||
                           (c == '_' && i + 7 <= n && p[i + 1] == 'x' && p[i + 6] == '_'))) {
            char e[8];
            snprintf(e, sizeof(e), "_x%04X_", c);
            s = ud_bytes_put(&b, e, 7);
        } else if (!ooxml && c < 32 && c != 9 && c != 10 && c != 13)
            s = UD_SCHEMA;
        else
            s = ud_bytes_put(&b, p + i, 1);
    }
    if (!s)
        s = deflate_data(w, b.data, (uint32_t)b.size, 0);
    ud_bytes_clear(&b);
    return s;
}
static void column_name(uint32_t column, char out[8]) {
    char reversed[8];
    uint32_t n = 0;
    while (column) {
        reversed[n++] = (char)('A' + (column - 1) % 26);
        column = (column - 1) / 26;
    }
    for (uint32_t i = 0; i < n; i++)
        out[i] = reversed[n - i - 1];
    out[n] = 0;
}
static ud_status write_cell(ud_xlsx_writer *w, uint32_t column, const ud_value *v) {
    if (v->is_null)
        return UD_OK;
    char c[8], tag[128];
    column_name(column + 1, c);
    int string = v->type == UD_UTF8 || v->type == UD_BINARY || v->type == UD_DECIMAL128 ||
                 v->type == UD_UINT64 || v->type == UD_DURATION || v->type == UD_INTERVAL;
    snprintf(tag, sizeof(tag), "<c r=\"%s%llu\"%s>", c, (unsigned long long)w->row,
             string                                            ? " t=\"inlineStr\""
             : v->type == UD_BOOL                              ? " t=\"b\""
             : v->type == UD_DATE32 || v->type == UD_TIMESTAMP ? " t=\"d\""
                                                               : "");
    UD_TRY(write_text(w, tag));
    if (string)
        UD_TRY(write_text(w, "<is><t xml:space=\"preserve\">"));
    else
        UD_TRY(write_text(w, "<v>"));
    if (v->type == UD_UTF8)
        UD_TRY(escape(w, v->text, v->text_length, 1));
    else if (v->type == UD_BINARY) {
        static const char base[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        ud_bytes b = {.ctx = w->ctx, .limit = 16777216};
        for (uint32_t i = 0; i < v->text_length; i += 3) {
            uint32_t remain = v->text_length - i;
            uint32_t code = (uint32_t)(unsigned char)v->text[i] << 16;
            if (remain > 1)
                code |= (uint32_t)(unsigned char)v->text[i + 1] << 8;
            if (remain > 2)
                code |= (unsigned char)v->text[i + 2];
            char q[4] = {base[code >> 18], base[(code >> 12) & 63],
                         remain > 1 ? base[(code >> 6) & 63] : '=',
                         remain > 2 ? base[code & 63] : '='};
            ud_status s = ud_bytes_put(&b, q, 4);
            if (s) {
                ud_bytes_clear(&b);
                return s;
            }
        }
        ud_status s = deflate_data(w, b.data, (uint32_t)b.size, 0);
        ud_bytes_clear(&b);
        UD_TRY(s);
    } else if (v->type == UD_BOOL)
        UD_TRY(write_text(w, v->integer ? "1" : "0"));
    else {
        if (v->type == UD_FLOAT64 && !isfinite(v->real))
            return UD_SCHEMA;
        if (v->type >= UD_STRUCT || v->type == UD_TIME64)
            return UD_UNSUPPORTED;
        char value[256];
        uint32_t required;
        UD_TRY(ud_value_format(v, value, sizeof(value), &required));
        UD_TRY(escape(w, value, required - 1, string));
    }
    UD_TRY(write_text(w, string ? "</t></is></c>" : "</v></c>"));
    return UD_OK;
}

static ud_status write_header(ud_xlsx_writer *writer, const ud_field *fields, uint32_t columns) {
    writer->row = 1;
    UD_TRY(write_text(writer, "<row r=\"1\">"));
    for (uint32_t at = 0; at < columns; at++) {
        ud_value value = {.type = UD_UTF8, .text = fields[at].name,
                           .text_length = fields[at].name_length};
        UD_TRY(write_cell(writer, at, &value));
    }
    return write_text(writer, "</row>");
}
typedef struct append_xml {
    XML_Parser parser;
    ud_status status;
    uint32_t depth, maximum_row;
    size_t data_start, data_end, data_open_length, dimension_start, dimension_end;
    int found, closed, empty;
    ud_bytes close_name;
} append_xml;
static void XMLCALL append_start(void *user, const char *name, const char **attributes) {
    append_xml *state = user;
    if (++state->depth > 64) {
        state->status = UD_RESOURCE;
        XML_StopParser(state->parser, XML_FALSE);
        return;
    }
    const char *tag = local(name);
    if (!strcmp(tag, "sheetData")) {
        if (state->found++) {
            state->status = UD_SCHEMA;
            XML_StopParser(state->parser, XML_FALSE);
            return;
        }
        state->data_start = (size_t)XML_GetCurrentByteIndex(state->parser);
        state->data_open_length = (size_t)XML_GetCurrentByteCount(state->parser);
    } else if (!strcmp(tag, "row")) {
        uint32_t row = decimal(attribute(attributes, "r"));
        if (!row || row > 1048576) {
            state->status = UD_SCHEMA;
            XML_StopParser(state->parser, XML_FALSE);
            return;
        }
        if (row > state->maximum_row)
            state->maximum_row = row;
    } else if (!strcmp(tag, "dimension"))
        state->dimension_start = (size_t)XML_GetCurrentByteIndex(state->parser);
}
static void XMLCALL append_end(void *user, const char *name) {
    append_xml *state = user;
    const char *tag = local(name);
    size_t at = (size_t)XML_GetCurrentByteIndex(state->parser),
           count = (size_t)XML_GetCurrentByteCount(state->parser);
    if (!strcmp(tag, "sheetData")) {
        state->data_end = at;
        state->closed = 1;
        state->empty = count == 0;
    } else if (!strcmp(tag, "dimension"))
        state->dimension_end = at + count;
    if (state->depth)
        state->depth--;
}
static void XMLCALL append_doctype(void *user, const char *name, const char *system,
                                   const char *public_id, int subset) {
    (void)name;
    (void)system;
    (void)public_id;
    (void)subset;
    append_xml *state = user;
    state->status = UD_UNSUPPORTED;
    XML_StopParser(state->parser, XML_FALSE);
}
static ud_status read_part(ud_xlsx_writer *writer, zip_entry *entry, ud_bytes *bytes) {
    xml_stream stream = {0};
    ud_status status = xml_open(&stream, &writer->original, entry);
    while (!status && !stream.finished) {
        uint32_t size = 0;
        status = zip_chunk(&stream, &size);
        if (!status)
            status = ud_bytes_put(bytes, stream.output, size);
    }
    xml_close(&stream);
    return status;
}
static ud_status copy_part(ud_xlsx_writer *writer, zip_entry *part) {
    UD_TRY(entry_begin(writer, part->name));
    xml_stream stream = {0};
    ud_status status = xml_open(&stream, &writer->original, part);
    while (!status && !stream.finished) {
        uint32_t size = 0;
        status = zip_chunk(&stream, &size);
        if (!status)
            status = deflate_data(writer, stream.output, size, 0);
    }
    xml_close(&stream);
    if (!status)
        status = entry_end(writer);
    return status;
}
static ud_status append_workbook(ud_xlsx_writer *writer, const ud_field *fields, uint32_t count) {
    if (member(writer->config, "range"))
        return UD_UNSUPPORTED;
    ud_xlsx_options options = writer->options;
    options.original_path = NULL;
    options.stage_bytes = NULL;
    ud_bytes configuration = {.ctx = writer->ctx};
    char *text = NULL;
    uint32_t text_size = 0;
    ud_status configured = ud_json_serialize(writer->config, &text, &text_size);
    if (!configured && !member(writer->config, "header")) {
        configured = ud_bytes_put(&configuration, text, text_size - 1);
        if (!configured)
            configured = ud_bytes_put(&configuration, ",\"header\":true}", 15);
    } else if (!configured)
        configured = ud_bytes_put(&configuration, text, text_size);
    ud_free(text);
    if (configured) {
        ud_bytes_clear(&configuration);
        return configured;
    }
    options.configuration =
        (ud_string){(const char *)configuration.data, (uint32_t)configuration.size};
    ud_xlsx_reader *reader = NULL;
    ud_status status = ud_xlsx_reader_open(writer->ctx, writer->options.original_path, fields,
                                           count, &options, &reader);
    if (!status)
        reader->validate_header = writer->header;
    char *sheet = NULL;
    if (!status) {
        sheet = ud_strdup_n(writer->ctx, reader->sheet_path, strlen(reader->sheet_path));
        if (!sheet)
            status = UD_RESOURCE;
    }
    while (!status) {
        ud_batch *batch = NULL;
        int32_t rejected = 0;
        status = ud_xlsx_reader_next(reader, &batch, &rejected);
        ud_batch_destroy(batch);
        if (status == UD_END) {
            status = UD_OK;
            break;
        }
        if (!status && rejected)
            status = UD_QUALITY;
    }
    ud_xlsx_reader_close(reader);
    options.configuration = writer->options.configuration;
    ud_bytes_clear(&configuration);
    if (!status)
        status = zip_open(writer->ctx, writer->options.original_path, &options, writer->config,
                          &writer->original);
    if (!status) {
        writer->capacity = writer->original.count;
        writer->entries = ud_alloc(writer->ctx, writer->capacity * sizeof(*writer->entries));
        if (!writer->entries)
            status = UD_RESOURCE;
    }
    ud_bytes xml = {.ctx = writer->ctx, .limit = 134217728};
    if (!status)
        status = read_part(writer, entry(&writer->original, sheet), &xml);
    if (!status &&
        (!ud_utf8_valid((const char *)xml.data, xml.size) || memchr(xml.data, 0, xml.size)))
        status = UD_UNSUPPORTED;
    append_xml state = {0};
    if (!status) {
        xml_context = writer->ctx;
        XML_Memory_Handling_Suite memory = {xml_allocate, xml_resize, xml_deallocate};
        state.parser = XML_ParserCreate_MM(NULL, &memory, "|");
        if (!state.parser)
            status = UD_RESOURCE;
    }
    if (!status) {
        XML_SetUserData(state.parser, &state);
        XML_SetElementHandler(state.parser, append_start, append_end);
        XML_SetStartDoctypeDeclHandler(state.parser, append_doctype);
        XML_SetParamEntityParsing(state.parser, XML_PARAM_ENTITY_PARSING_NEVER);
        for (size_t at = 0; !status && at < xml.size;) {
            size_t size = xml.size - at > 16384 ? 16384 : xml.size - at;
            status = control(&options);
            if (!status && XML_Parse(state.parser, (const char *)xml.data + at, (int)size,
                                     at + size == xml.size) != XML_STATUS_OK)
                status = state.status ? state.status : UD_PARSE;
            at += size;
        }
        if (!status && (!state.found || !state.closed || state.data_end > xml.size ||
                        state.data_start + state.data_open_length > xml.size))
            status = UD_SCHEMA;
    }
    if (!status)
        for (uint32_t at = 0; !status && at < writer->original.count; at++)
            if (strcmp(writer->original.entries[at].name, sheet))
                status = copy_part(writer, writer->original.entries + at);
    if (!status)
        status = entry_begin(writer, entry(&writer->original, sheet)->name);
    size_t prefix = state.empty ? state.data_start + state.data_open_length - 2 : state.data_end;
    if (!status && state.dimension_end && state.dimension_start < state.dimension_end &&
        state.dimension_end <= prefix) {
        status = deflate_data(writer, xml.data, (uint32_t)state.dimension_start, 0);
        if (!status)
            status = deflate_data(writer, xml.data + state.dimension_end,
                                  (uint32_t)(prefix - state.dimension_end), 0);
    } else if (!status)
        status = deflate_data(writer, xml.data, (uint32_t)prefix, 0);
    writer->tail = (ud_bytes){.ctx = writer->ctx, .limit = 134217728};
    if (!status && state.empty) {
        status = write_text(writer, ">");
        size_t name_start = state.data_start + 1, name_end = name_start;
        while (name_end < xml.size && xml.data[name_end] != ' ' && xml.data[name_end] != '\t' &&
               xml.data[name_end] != '/' && xml.data[name_end] != '>')
            name_end++;
        if (!status)
            status = ud_bytes_put(&writer->tail, "</", 2);
        if (!status)
            status = ud_bytes_put(&writer->tail, xml.data + name_start, name_end - name_start);
        if (!status)
            status = ud_bytes_put(&writer->tail, ">", 1);
        if (!status)
            status =
                ud_bytes_put(&writer->tail, xml.data + state.data_start + state.data_open_length,
                             xml.size - state.data_start - state.data_open_length);
    } else if (!status)
        status = ud_bytes_put(&writer->tail, xml.data + state.data_end, xml.size - state.data_end);
    writer->row = state.maximum_row;
    if (!status && writer->header && !writer->row)
        status = write_header(writer, fields, count);
    if (state.parser)
        XML_ParserFree(state.parser);
    ud_free(sheet);
    ud_bytes_clear(&xml);
    return status;
}
ud_status ud_xlsx_writer_open(ud_context *ctx, const char *path, const ud_field *fields,
                              uint32_t columns, const ud_xlsx_options *o, ud_xlsx_writer **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !path || !o || o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION ||
        !columns || columns > 16384)
        return UD_INVALID;
    ud_xlsx_writer *w = ud_alloc(ctx, sizeof(*w));
    if (!w)
        return UD_RESOURCE;
    w->ctx = ctx;
    w->options = *o;
    ud_status s =
        ud_json_parse(ctx, o->configuration.data, o->configuration.length, &w->config, NULL);
    if (!s)
        s = ud_batch_create(ctx, fields, columns, 0, &w->schema);
    const char *sheet = "Sheet1";
    uint32_t n = 6;
    if (!s && member(w->config, "sheet"))
        s = ud_json_text(member(w->config, "sheet"), &sheet, &n);
    if (!s && (!n || n > 31 || memchr(sheet, 0, n)))
        s = UD_SCHEMA;
    for (uint32_t i = 0; !s && i < n; i++)
        if (strchr("[]:*?/\\", sheet[i]))
            s = UD_SCHEMA;
    if (!s) {
        w->sheet = ud_strdup_n(ctx, sheet, n);
        if (!w->sheet)
            s = UD_RESOURCE;
    }
    w->header = !member(w->config, "header") || named(member(w->config, "header"), "true");
    w->date1904 = named(member(w->config, "datePolicy"), "1904");
    const ud_json *date = member(w->config, "datePolicy");
    if (date && !named(date, "1900") && !named(date, "1904") && !named(date, "workbook"))
        s = UD_SCHEMA;
    if (!s) {
        w->file = ud_fopen(path, "wb");
        if (!w->file)
            s = UD_IO;
    }
    if (!s && o->original_path) {
        s = append_workbook(w, fields, columns);
        if (s) {
            w->status = s;
            ud_xlsx_writer_close(w);
            return s;
        }
        *out = w;
        return UD_OK;
    }
    if (!s) {
        w->capacity = 8;
        w->entries = ud_alloc(ctx, w->capacity * sizeof(*w->entries));
        if (!w->entries)
            s = UD_RESOURCE;
    }
    if (!s)
        s = write_part(w, "[Content_Types].xml",
                       "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Types "
                       "xmlns=\"http://schemas.openxmlformats.org/package/2006/"
                       "content-types\"><Default Extension=\"rels\" "
                       "ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/"
                       "><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override "
                       "PartName=\"/xl/workbook.xml\" "
                       "ContentType=\"application/"
                       "vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/"
                       "><Override PartName=\"/xl/worksheets/sheet1.xml\" "
                       "ContentType=\"application/"
                       "vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>");
    if (!s)
        s = write_part(w, "_rels/.rels",
                       "<?xml version=\"1.0\"?><Relationships "
                       "xmlns=\"http://schemas.openxmlformats.org/package/2006/"
                       "relationships\"><Relationship Id=\"rId1\" "
                       "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/"
                       "officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>");
    if (!s)
        s = entry_begin(w, "xl/workbook.xml");
    if (!s)
        s = write_text(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><workbook "
                          "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
                          "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/"
                          "relationships\"><workbookPr date1904=\"");
    if (!s)
        s = write_text(w, w->date1904 ? "1" : "0");
    if (!s)
        s = write_text(w, "\"/><sheets><sheet name=\"");
    if (!s)
        s = escape(w, w->sheet, (uint32_t)strlen(w->sheet), 0);
    if (!s)
        s = write_text(w, "\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>");
    if (!s)
        s = entry_end(w);
    if (!s)
        s = write_part(w, "xl/_rels/workbook.xml.rels",
                       "<?xml version=\"1.0\"?><Relationships "
                       "xmlns=\"http://schemas.openxmlformats.org/package/2006/"
                       "relationships\"><Relationship Id=\"rId1\" "
                       "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/"
                       "worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>");
    if (!s)
        s = entry_begin(w, "xl/worksheets/sheet1.xml");
    if (!s)
        s = write_text(
            w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><worksheet "
               "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>");
    if (!s && w->header)
        s = write_header(w, fields, columns);
    if (s) {
        w->status = s;
        ud_xlsx_writer_close(w);
        return s;
    }
    *out = w;
    return UD_OK;
}
ud_status ud_xlsx_writer_append(ud_xlsx_writer *w, const ud_batch *b) {
    if (!w || !b)
        return UD_INVALID;
    if (w->status)
        return w->status;
    if (b->columns != w->schema->columns)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < b->columns; i++)
        if (!ud_field_matches(&w->schema->column[i].field, &b->column[i].field, UD_FIELD_PORT))
            return UD_SCHEMA;
    ud_status s = UD_OK;
    for (uint32_t row = 0; !s && row < b->rows; row++) {
        if (w->row >= 1048576) {
            s = UD_RESOURCE;
            break;
        }
        w->row++;
        char tag[160];
        snprintf(
            tag, sizeof(tag),
            "<row xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" r=\"%llu\">",
            (unsigned long long)w->row);
        s = write_text(w, tag);
        for (uint32_t c = 0; !s && c < b->columns; c++) {
            ud_value v;
            s = ud_batch_get(b, row, c, &v);
            if (!s)
                s = write_cell(w, c, &v);
        }
        if (!s)
            s = write_text(w, "</row>");
    }
    w->status = s;
    return s;
}
ud_status ud_xlsx_writer_close(ud_xlsx_writer *w) {
    if (!w)
        return UD_OK;
    ud_status s = w->status;
    if (!s)
        s = w->options.original_path ? deflate_data(w, w->tail.data, (uint32_t)w->tail.size, 0)
                                     : write_text(w, "</sheetData></worksheet>");
    if (!s)
        s = entry_end(w);
    uint32_t central = (uint32_t)w->bytes;
    for (uint32_t i = 0; !s && i < w->count; i++) {
        written_entry *e = w->entries + i;
        unsigned char h[46] = {0};
        p32(h, UINT32_C(0x02014b50));
        p16(h + 4, 20);
        p16(h + 6, 20);
        p16(h + 8, 0x808);
        p16(h + 10, 8);
        p32(h + 16, e->crc);
        p32(h + 20, e->compressed);
        p32(h + 24, e->size);
        p16(h + 28, (uint32_t)strlen(e->name));
        p32(h + 42, e->offset);
        s = raw_write(w, h, 46);
        if (!s)
            s = raw_write(w, e->name, strlen(e->name));
    }
    if (!s) {
        unsigned char h[22] = {0};
        p32(h, UINT32_C(0x06054b50));
        p16(h + 8, w->count);
        p16(h + 10, w->count);
        p32(h + 12, (uint32_t)w->bytes - central);
        p32(h + 16, central);
        s = raw_write(w, h, 22);
    }
    if (!s)
        s = ud_flush(w->ctx, w->file);
    if (w->compressed)
        deflateEnd(&w->compressor);
    if (w->file && fclose(w->file) && !s)
        s = UD_IO;
    if (w->reserved)
        ud_buffer_release_external(w->options.buffers, w->reserved);
    ud_batch_destroy(w->schema);
    ud_json_destroy(w->config);
    ud_free(w->sheet);
    ud_free(w->entries);
    zip_close(&w->original);
    ud_bytes_clear(&w->tail);
    ud_free(w);
    return s;
}

ud_status ud_xlsx_reader_rejected_record(const ud_xlsx_reader *r, ud_status *code, ud_id *column,
                                         ud_batch **out) {
    if (!r || !code || !column || !out || !r->result_rejected)
        return UD_INVALID;
    *out = NULL;
    *code = r->record_error ? r->record_error : UD_PARSE;
    *column = r->error_column < r->schema->columns
                  ? r->schema->column[r->error_column].field.identity
                  : (ud_id){0};
    ud_field *fields = ud_alloc(r->ctx, r->columns * sizeof(*fields));
    ud_value *values = ud_alloc(r->ctx, r->columns * sizeof(*values));
    ud_status status = fields && values ? UD_OK : UD_RESOURCE;
    for (uint32_t j = 0; !status && j < r->columns; j++) {
        const ud_field *f = r->schema->schema->fields + j;
        fields[j] = (ud_field){.name = f->name,
                               .name_length = f->name_length,
                               .type = UD_BINARY,
                               .nullable = 1,
                               .identity = f->identity};
        values[j] = r->raw_values[j];
        values[j].type = UD_BINARY;
        if (!values[j].text)
            values[j].is_null = 1;
    }
    if (!status)
        status = ud_batch_create(r->ctx, fields, r->columns, 1, out);
    if (!status)
        status = ud_batch_append(*out, values, r->columns);
    ud_row_info info;
    if (!status)
        status = ud_row_info_source(
            r->scope, (uint64_t)r->row_number - r->first_row - (r->header ? 1u : 0u), &info);
    if (!status)
        status = ud_batch_set_row_info(*out, 0, &info);
    ud_free(values);
    ud_free(fields);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
