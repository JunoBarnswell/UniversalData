#include "ud/csv.h"
#include "../internal.h"
#include "../text_stream.h"
#include <math.h>
#include <locale.h>
typedef struct csv_layout {
    ud_json *document;
    ud_string marker;
    int encoding, header, bom, trim, crlf;
    uint32_t delimiter, qualifier, skip, columns;
    uint32_t *widths, *positions;
} csv_layout;
static int csv_bool(const ud_json *value, int fallback) {
    const char *text;
    uint32_t size;
    if (!value)
        return fallback;
    return !ud_json_text(value, &text, &size) && size == 4 && !memcmp(text, "true", 4);
}
static ud_status csv_uint(const ud_json *value, uint32_t *out) {
    const char *text;
    uint32_t size;
    uint64_t number = 0;
    UD_TRY(ud_json_text(value, &text, &size));
    if (!size)
        return UD_SCHEMA;
    for (uint32_t at = 0; at < size; at++) {
        if (text[at] < '0' || text[at] > '9')
            return UD_SCHEMA;
        number = number * 10 + (uint32_t)(text[at] - '0');
        if (number > UINT32_MAX)
            return UD_SCHEMA;
    }
    *out = (uint32_t)number;
    return UD_OK;
}
static ud_status csv_scalar(ud_string text, uint32_t *out) {
    if (!text.length) {
        *out = 0;
        return UD_OK;
    }
    if (!ud_utf8_valid(text.data, text.length))
        return UD_SCHEMA;
    const uint8_t *bytes = (const uint8_t *)text.data;
    uint32_t count = bytes[0] < 128 ? 1 : bytes[0] < 224 ? 2 : bytes[0] < 240 ? 3 : 4;
    if (count != text.length)
        return UD_UNSUPPORTED;
    uint32_t value = bytes[0] & ((1u << (8 - count - !!(count > 1))) - 1);
    for (uint32_t at = 1; at < count; at++)
        value = (value << 6) | (bytes[at] & 63);
    if (!value || value == '\r' || value == '\n')
        return UD_SCHEMA;
    *out = value;
    return UD_OK;
}
static void layout_clear(csv_layout *layout) {
    ud_json_destroy(layout->document);
    ud_free(layout->widths);
    ud_free(layout->positions);
    memset(layout, 0, sizeof(*layout));
}
static ud_status layout_open(ud_context *context, const ud_csv_options *options, uint32_t columns,
                             csv_layout *out) {
    ud_json *schema = NULL, *config = NULL;
    ud_status status = ud_json_parse(context, UD_CSV_FORMAT_SCHEMA,
                                     sizeof(UD_CSV_FORMAT_SCHEMA) - 1, &schema, NULL);
    if (!status)
        status = ud_json_parse(
            context, options && options->format.data ? options->format.data : "{}",
            options && options->format.data ? options->format.length : 2, &config, NULL);
    if (!status)
        status = ud_json_bind_schema(context, schema, config, &out->document);
    ud_json_destroy(schema);
    ud_json_destroy(config);
    if (status)
        return status;
    const ud_json *doc = out->document;
    const char *text;
    uint32_t size;
    if (!status)
        status = ud_json_text(ud_json_find(doc, "encoding", 8), &text, &size);
    if (!status)
        status = ud_text_encoding((ud_string){text, size}, &out->encoding);
    if (!status)
        status = ud_json_text(ud_json_find(doc, "delimiter", 9), &text, &size);
    if (!status)
        status = csv_scalar((ud_string){text, size}, &out->delimiter);
    if (!status && !out->delimiter)
        status = UD_SCHEMA;
    if (!status)
        status = ud_json_text(ud_json_find(doc, "qualifier", 9), &text, &size);
    if (!status)
        status = csv_scalar((ud_string){text, size}, &out->qualifier);
    if (!status && out->qualifier == out->delimiter)
        status = UD_SCHEMA;
    if (!status)
        status = ud_json_text(ud_json_find(doc, "nullMarker", 10), &out->marker.data,
                              &out->marker.length);
    if (!status && (!out->marker.length || !ud_utf8_valid(out->marker.data, out->marker.length)))
        status = UD_SCHEMA;
    out->header = csv_bool(ud_json_find(doc, "header", 6), 1);
    out->trim = csv_bool(ud_json_find(doc, "trimFixedPadding", 16), 1);
    out->bom = csv_bool(ud_json_find(doc, "writeBom", 8),
                        out->encoding == UD_TEXT_UTF16LE || out->encoding == UD_TEXT_UTF16BE);
    if (!status && out->bom && out->encoding == UD_TEXT_LATIN1)
        status = UD_UNSUPPORTED;
    if (!status)
        status = csv_uint(ud_json_find(doc, "skipLines", 9), &out->skip);
    if (!status && !out->skip)
        out->skip = ud_json_size(ud_json_find(doc, "preamble", 8));
    if (!status)
        status = ud_json_text(ud_json_find(doc, "rowDelimiter", 12), &text, &size);
    out->crlf = !status && size == 4 && !memcmp(text, "CRLF", 4);
    const ud_json *widths = ud_json_find(doc, "widths", 6),
                  *positions = ud_json_find(doc, "positions", 9);
    uint32_t count = ud_json_size(widths), mapped = ud_json_size(positions);
    out->columns = columns == UINT32_MAX ? count : columns;
    if (!status && count && columns != UINT32_MAX && count != columns)
        status = UD_SCHEMA;
    if (!status && mapped && columns != UINT32_MAX && mapped != columns)
        status = UD_SCHEMA;
    if (!status && count) {
        out->widths = ud_alloc(context, count * sizeof(*out->widths));
        if (!out->widths)
            status = UD_RESOURCE;
    }
    if (!status && mapped) {
        out->positions = ud_alloc(context, mapped * sizeof(*out->positions));
        if (!out->positions)
            status = UD_RESOURCE;
    }
    uint64_t total_width = 0;
    for (uint32_t at = 0; !status && at < count; at++) {
        status = csv_uint(ud_json_at(widths, at), out->widths + at);
        if (!status) {
            total_width += out->widths[at];
            if (total_width > 16777216)
                status = UD_RESOURCE;
        }
    }
    for (uint32_t at = 0; !status && at < mapped; at++) {
        status = csv_uint(ud_json_at(positions, at), out->positions + at);
        if (!status && out->positions[at] >= mapped)
            status = UD_SCHEMA;
        for (uint32_t prior = 0; !status && prior < at; prior++)
            if (out->positions[prior] == out->positions[at])
                status = UD_SCHEMA;
    }
    if (status)
        layout_clear(out);
    return status;
}
static uint32_t physical_field(const csv_layout *layout, uint32_t logical) {
    if (!layout->positions)
        return logical;
    for (uint32_t at = 0; at < layout->columns; at++)
        if (layout->positions[at] == logical)
            return at;
    return UINT32_MAX;
}
struct ud_csv_reader {
    ud_context *ctx;
    FILE *file;
    char *path, *null_marker;
    uint32_t null_length, batch_size, columns;
    ud_field *fields;
    ud_batch *schema;
    uint64_t record;
    uint32_t max_record_bytes;
    ud_record_options records;
    int poisoned;
    csv_layout layout;
    ud_text_input text;
    uint64_t data_record;
};
typedef struct {
    char **text;
    uint32_t *length;
    uint32_t n;
    uint32_t stored, bytes;
    ud_bytes raw;
    ud_status conversion;
} record;
static void record_free(record *r) {
    for (uint32_t i = 0; i < r->stored; i++)
        ud_free(r->text[i]);
    ud_free(r->text);
    ud_free(r->length);
    ud_bytes_clear(&r->raw);
    memset(r, 0, sizeof(*r));
}
static ud_status finish_field(ud_csv_reader *r, record *row, ud_bytes *field) {
    if (row->n == 65536 || field->size > UINT32_MAX)
        return UD_RESOURCE;
    if (row->n >= r->columns) {
        row->n++;
        field->size = 0;
        return UD_OK;
    }
    char *s = ud_strdup_n(r->ctx, field->data ? (char *)field->data : "", field->size);
    if (!s)
        return UD_RESOURCE;
    row->text[row->n] = s;
    row->length[row->n] = (uint32_t)field->size;
    row->n++;
    row->stored++;
    field->size = 0;
    return UD_OK;
}
static ud_status record_byte(ud_csv_reader *reader, record *row, uint32_t value) {
    (void)value;
    if (reader->text.raw_size > reader->max_record_bytes - row->bytes)
        return UD_RESOURCE;
    row->bytes += reader->text.raw_size;
    return reader->records.include_input
               ? ud_bytes_put(&row->raw, reader->text.raw, reader->text.raw_size)
               : UD_OK;
}
static ud_status next_record(ud_csv_reader *reader, record *row) {
    row->raw.ctx = reader->ctx;
    row->text = ud_alloc(reader->ctx, reader->columns * sizeof(char *));
    row->length = ud_alloc(reader->ctx, reader->columns * sizeof(uint32_t));
    if (!row->text || !row->length) {
        record_free(row);
        return UD_RESOURCE;
    }
    ud_bytes field = {.ctx = reader->ctx, .limit = reader->max_record_bytes};
    int quoted = 0, closed = 0, start = 1, any = 0;
    uint32_t width = 0;
    ud_status status = UD_OK;
    for (;;) {
        uint32_t ch;
        ud_status read = ud_text_next(&reader->text, &ch);
        if (read == UD_END) {
            if (reader->layout.widths && any) {
                if (width) {
                    status = finish_field(reader, row, &field);
                    row->conversion = UD_TRUNCATED;
                }
                if (row->n != reader->layout.columns)
                    row->conversion = UD_TRUNCATED;
            } else
                status = quoted ? UD_PARSE : !any ? UD_END : finish_field(reader, row, &field);
            if (!any)
                status = UD_END;
            break;
        }
        if (read) {
            status = read;
            break;
        }
        status = record_byte(reader, row, ch);
        if (status)
            break;
        any = 1;
        if (reader->layout.widths) {
            if (ch == '\r' || ch == '\n') {
                if (width || row->n != reader->layout.columns)
                    row->conversion = UD_TRUNCATED;
                if (ch == '\r') {
                    uint32_t next;
                    read = ud_text_next(&reader->text, &next);
                    if (!read && next == '\n')
                        status = record_byte(reader, row, next);
                    else if (!read)
                        ud_text_unread(&reader->text, next);
                    else if (read != UD_END)
                        status = read;
                }
                break;
            }
            if (row->n >= reader->layout.columns) {
                row->conversion = UD_SCHEMA;
                continue;
            }
            status = ud_text_utf8(&field, ch);
            width++;
            if (!status && width == reader->layout.widths[row->n]) {
                if (reader->layout.trim)
                    while (field.size && field.data[field.size - 1] == ' ')
                        field.size--;
                status = finish_field(reader, row, &field);
                width = 0;
            }
        } else if (quoted) {
            if (ch == reader->layout.qualifier) {
                uint32_t next;
                read = ud_text_next(&reader->text, &next);
                if (!read && next == reader->layout.qualifier) {
                    status = record_byte(reader, row, next);
                    if (!status)
                        status = ud_text_utf8(&field, ch);
                } else {
                    quoted = 0;
                    closed = 1;
                    if (!read)
                        ud_text_unread(&reader->text, next);
                    else if (read != UD_END)
                        status = read;
                }
            } else
                status = ud_text_utf8(&field, ch);
        } else if (ch == reader->layout.delimiter || ch == '\r' || ch == '\n') {
            status = finish_field(reader, row, &field);
            start = 1;
            closed = 0;
            if (ch != reader->layout.delimiter) {
                if (ch == '\r') {
                    uint32_t next;
                    read = ud_text_next(&reader->text, &next);
                    if (!read && next == '\n')
                        status = record_byte(reader, row, next);
                    else if (!read)
                        ud_text_unread(&reader->text, next);
                    else if (read != UD_END)
                        status = read;
                }
                break;
            }
        } else if (reader->layout.qualifier && ch == reader->layout.qualifier && start) {
            quoted = 1;
            start = 0;
        } else if (closed || (reader->layout.qualifier && ch == reader->layout.qualifier)) {
            status = UD_PARSE;
            break;
        } else {
            status = ud_text_utf8(&field, ch);
            start = 0;
        }
        if (status)
            break;
    }
    ud_bytes_clear(&field);
    if (status == UD_END) {
        record_free(row);
        return status;
    }
    if (status) {
        record_free(row);
        reader->poisoned = 1;
        return status;
    }
    if (reader->record == UINT64_MAX) {
        record_free(row);
        return UD_RESOURCE;
    }
    reader->record++;
    return UD_OK;
}
static ud_status csv_skip(ud_csv_reader *reader) {
    for (uint32_t line = 0; line < reader->layout.skip; line++) {
        for (;;) {
            uint32_t value;
            ud_status status = ud_text_next(&reader->text, &value);
            if (status)
                return status == UD_END ? UD_SCHEMA : status;
            if (value == '\n')
                break;
            if (value == '\r') {
                uint32_t next;
                status = ud_text_next(&reader->text, &next);
                if (!status && next != '\n')
                    ud_text_unread(&reader->text, next);
                else if (status != UD_END && status)
                    return status;
                break;
            }
        }
        reader->record++;
    }
    return UD_OK;
}
ud_status ud_csv_discover(ud_context *ctx, const char *path, ud_string scope,
                          const ud_csv_options *options, uint32_t sample_rows, ud_batch **out) {
    if (!ctx || !path || !out || !sample_rows || sample_rows > 100000 ||
        (options &&
         (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION)))
        return UD_INVALID;
    *out = NULL;
    ud_csv_reader reader = {.ctx = ctx,
                            .columns = 65536,
                            .max_record_bytes = options && options->max_record_bytes
                                                    ? options->max_record_bytes
                                                    : 16777216};
    ud_status status = layout_open(ctx, options, UINT32_MAX, &reader.layout);
    if (!status) {
        reader.file = ud_fopen(path, "rb");
        if (!reader.file)
            status = UD_IO;
    }
    if (!status)
        status = ud_text_start(&reader.text, ctx, reader.file, reader.layout.encoding);
    if (!status)
        status = csv_skip(&reader);
    record header = {0};
    if (!status)
        status = next_record(&reader, &header);
    if (status == UD_END)
        status = UD_SCHEMA;
    if (!status && reader.layout.positions &&
        ud_json_size(ud_json_find(reader.layout.document, "positions", 9)) != header.n)
        status = UD_SCHEMA;
    reader.columns = header.n;
    reader.layout.columns = header.n;
    ud_field *fields = !status ? ud_alloc(ctx, header.n * sizeof(*fields)) : NULL;
    char **names = !status ? ud_alloc(ctx, header.n * sizeof(*names)) : NULL;
    if (!status && (!fields || !names))
        status = UD_RESOURCE;
    for (uint32_t logical = 0; !status && logical < header.n; logical++) {
        uint32_t physical = physical_field(&reader.layout, logical);
        if (reader.layout.header) {
            if (!header.length[physical]) {
                status = UD_SCHEMA;
                break;
            }
            fields[logical].name = header.text[physical];
            fields[logical].name_length = header.length[physical];
        } else {
            char name[32];
            int size = snprintf(name, sizeof(name), "column_%u", logical + 1);
            names[logical] = ud_strdup_n(ctx, name, (size_t)size);
            if (!names[logical])
                status = UD_RESOURCE;
            fields[logical].name = names[logical];
            fields[logical].name_length = (uint32_t)size;
        }
    }
    for (uint32_t row = 0; !status && row < sample_rows; row++) {
        record values = {0};
        record *data = &values;
        if (!reader.layout.header && row == 0)
            data = &header;
        else
            status = next_record(&reader, &values);
        if (status == UD_END) {
            status = UD_OK;
            record_free(&values);
            break;
        }
        if (!status && (data->conversion || data->n != header.n))
            status = UD_SCHEMA;
        for (uint32_t logical = 0; !status && logical < header.n; logical++) {
            uint32_t physical = physical_field(&reader.layout, logical);
            if (data->length[physical] == reader.layout.marker.length &&
                !memcmp(data->text[physical], reader.layout.marker.data,
                        reader.layout.marker.length))
                fields[logical].nullable = 1;
            else
                status = ud_infer_scalar(ctx, fields + logical, data->text[physical],
                                         data->length[physical], -1);
        }
        record_free(&values);
    }
    if (!status)
        status = ud_infer_schema(ctx, fields, header.n, scope, out);
    if (names)
        for (uint32_t at = 0; at < header.n; at++)
            ud_free(names[at]);
    ud_free(names);
    ud_free(fields);
    record_free(&header);
    layout_clear(&reader.layout);
    if (reader.file)
        fclose(reader.file);
    return status;
}
ud_status ud_csv_open(ud_context *ctx, const char *path, const ud_field *fields, uint32_t columns,
                      const ud_csv_options *options, ud_csv_reader **out) {
    if (!ctx || !path || !fields || !columns || !out ||
        (options &&
         (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION)))
        return UD_INVALID;
    *out = NULL;
    for (uint32_t at = 0; at < columns; at++)
        if (fields[at].type == UD_BINARY || fields[at].type >= UD_STRUCT)
            return UD_UNSUPPORTED;
    ud_csv_reader *reader = ud_alloc(ctx, sizeof(*reader));
    if (!reader)
        return UD_RESOURCE;
    reader->ctx = ctx;
    reader->columns = columns;
    reader->batch_size = options && options->batch_size ? options->batch_size : 4096;
    reader->max_record_bytes =
        options && options->max_record_bytes ? options->max_record_bytes : 16777216;
    ud_status status = layout_open(ctx, options, columns, &reader->layout);
    if (!status)
        status = ud_record_options_validate(ctx, options ? options->records : NULL);
    if (!status)
        status = ud_batch_create(ctx, fields, columns, 0, &reader->schema);
    if (!status) {
        reader->fields = reader->schema->schema->fields;
        if (options && options->records)
            reader->records = *options->records;
        reader->null_length = reader->layout.marker.length;
        reader->null_marker = ud_strdup_n(ctx, reader->layout.marker.data, reader->null_length);
        reader->path = ud_strdup_n(ctx, path, strlen(path));
        if (!reader->path || !reader->null_marker)
            status = UD_RESOURCE;
    }
    if (!status) {
        reader->file = ud_fopen(path, "rb");
        if (!reader->file)
            status = UD_IO;
    }
    if (!status)
        status = ud_text_start(&reader->text, ctx, reader->file, reader->layout.encoding);
    if (!status)
        status = csv_skip(reader);
    if (!status && reader->layout.header) {
        record header = {0};
        status = next_record(reader, &header);
        if (status == UD_END)
            status = UD_SCHEMA;
        if (!status && (header.conversion || header.n != columns))
            status = UD_SCHEMA;
        for (uint32_t logical = 0; !status && logical < columns; logical++) {
            uint32_t physical = physical_field(&reader->layout, logical);
            if (header.length[physical] != fields[logical].name_length ||
                memcmp(header.text[physical], fields[logical].name, fields[logical].name_length))
                status = UD_SCHEMA;
        }
        record_free(&header);
    }
    if (status) {
        ud_csv_close(reader);
        return status;
    }
    *out = reader;
    return UD_OK;
}
void ud_csv_close(ud_csv_reader *r) {
    if (!r)
        return;
    if (r->file)
        fclose(r->file);
    ud_batch_destroy(r->schema);
    ud_free(r->path);
    ud_free(r->null_marker);
    layout_clear(&r->layout);
    ud_free(r);
}
uint64_t ud_csv_offset(const ud_csv_reader *reader) {
    return reader ? ud_text_offset(&reader->text) : UINT64_MAX;
}
uint64_t ud_csv_record(const ud_csv_reader *reader) {
    return reader ? reader->record : 0;
}
ud_status ud_csv_resume(ud_csv_reader *reader, uint64_t offset, uint64_t number) {
    uint64_t prefix = reader ? (uint64_t)reader->layout.skip + (uint32_t)reader->layout.header : 0;
    if (!reader || number < prefix || offset == UINT64_MAX || ud_seek(reader->file, offset))
        return UD_INVALID;
    reader->text.pushed = 0;
    reader->text.raw_size = 0;
    reader->record = number;
    reader->data_record = number - prefix;
    return UD_OK;
}
ud_status ud_csv_next(ud_csv_reader *r, ud_batch **out, ud_batch **errors) {
    if (!r || !out || (r->records.policy == UD_ERROR_REDIRECT && !errors))
        return UD_INVALID;
    *out = NULL;
    if (errors)
        *errors = NULL;
    if (r->poisoned)
        return UD_CORRUPT;
    ud_batch *b = NULL;
    UD_TRY(ud_batch_create(r->ctx, r->fields, r->columns, r->batch_size, &b));
    ud_batch *rejected = NULL, *error_schema = NULL;
    ud_status s = UD_OK;
    if (r->records.policy == UD_ERROR_REDIRECT)
        s = ud_error_schema(&r->records.errors, NULL, r->records.include_input, &error_schema);
    if (!s && error_schema)
        s = ud_batch_create(r->ctx, error_schema->schema->fields, error_schema->columns,
                            r->batch_size, &rejected);
    ud_batch_destroy(error_schema);
    ud_value *v = ud_alloc(r->ctx, r->columns * sizeof(*v));
    if (!v) {
        ud_batch_destroy(b);
        ud_batch_destroy(rejected);
        return UD_RESOURCE;
    }
    uint32_t processed = 0;
    while (!s && processed < r->batch_size) {
        if (ud_is_cancelled(r->ctx)) {
            s = UD_CANCELLED;
            break;
        }
        record row = {0};
        s = next_record(r, &row);
        if (s == UD_END) {
            s = UD_OK;
            break;
        }
        if (s)
            break;
        processed++;
        if (r->data_record == UINT64_MAX) {
            record_free(&row);
            s = UD_RESOURCE;
            break;
        }
        uint64_t source_record = r->data_record++;
        ud_status conversion = row.conversion        ? row.conversion
                               : row.n == r->columns ? UD_OK
                                                     : UD_SCHEMA;
        uint32_t bad = UINT32_MAX;
        ud_row_info provenance = {0};
        if (r->records.struct_size)
            s = ud_row_info_source(r->records.source_scope, source_record, &provenance);
        memset(v, 0, r->columns * sizeof(*v));
        for (uint32_t i = 0; !s && !conversion && i < r->columns; i++) {
            v[i].type = r->fields[i].type;
            uint32_t physical = physical_field(&r->layout, i);
            if (row.length[physical] == r->null_length &&
                !memcmp(row.text[physical], r->null_marker, r->null_length)) {
                v[i].is_null = 1;
                if (!r->fields[i].nullable)
                    conversion = UD_QUALITY;
            } else
                conversion = ud_value_parse_field(r->ctx, r->fields + i, row.text[physical],
                                                  row.length[physical], UD_ROUND_EXACT, v + i);
            if (conversion)
                bad = i;
        }
        if (!s && !conversion) {
            s = ud_batch_append(b, v, r->columns);
            if (!s && r->records.struct_size)
                s = ud_batch_set_row_info(b, b->rows - 1, &provenance);
        } else if (!s && (r->records.policy == UD_ERROR_FAIL ||
                          !ud_record_error_recoverable(conversion))) {
            s = ud_fail(r->ctx, conversion, "%s: record %llu field %u: invalid typed value",
                        r->path, (unsigned long long)r->record, bad + 1);
        } else if (!s && r->records.policy == UD_ERROR_REDIRECT) {
            s = ud_error_append(&r->records.errors, rejected,
                                &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                                   .abi_version = UD_ABI_VERSION,
                                                   .source = provenance,
                                                   .rule = {"csv.parse", 9},
                                                   .rule_version = 1,
                                                   .column = bad < r->columns
                                                                 ? r->fields[bad].identity
                                                                 : (ud_id){0},
                                                   .code = conversion,
                                                   .policy = r->records.policy,
                                                   .input_format = {"CSV", 3},
                                                   .raw = row.raw.data,
                                                   .raw_length = (uint32_t)row.raw.size});
        }
        record_free(&row);
        if (s)
            break;
    }
    ud_free(v);
    if (s || !processed) {
        if (s)
            r->poisoned = 1;
        ud_batch_destroy(b);
        ud_batch_destroy(rejected);
        return s ? s : UD_END;
    }
    *out = b;
    if (errors)
        *errors = rejected;
    return UD_OK;
}
struct ud_csv_writer {
    ud_context *ctx;
    FILE *file;
    ud_batch *schema;
    csv_layout layout;
    uint32_t columns, max_record_bytes;
};
static ud_status csv_field(ud_csv_writer *writer, ud_bytes *record, uint32_t physical,
                           const char *text, uint32_t size) {
    csv_layout *layout = &writer->layout;
    if (layout->widths) {
        uint32_t count = 0;
        for (uint32_t at = 0; at < size; at++)
            if (((uint8_t)text[at] & 0xc0) != 0x80)
                count++;
        if (!ud_utf8_valid(text, size))
            return UD_PARSE;
        if (count > layout->widths[physical])
            return UD_TRUNCATED;
        UD_TRY(ud_bytes_put(record, text, size));
        for (; count < layout->widths[physical]; count++)
            UD_TRY(ud_bytes_put(record, " ", 1));
        return UD_OK;
    }
    if (physical)
        UD_TRY(ud_text_utf8(record, layout->delimiter));
    ud_bytes delimiter = {.ctx = writer->ctx}, qualifier = {.ctx = writer->ctx};
    ud_status status = ud_text_utf8(&delimiter, layout->delimiter);
    if (!status && layout->qualifier)
        status = ud_text_utf8(&qualifier, layout->qualifier);
    int quoted = 0;
    for (uint32_t at = 0; !status && at < size; at++)
        if (text[at] == '\n' || text[at] == '\r' ||
            (at + delimiter.size <= size && !memcmp(text + at, delimiter.data, delimiter.size)) ||
            (qualifier.size && at + qualifier.size <= size &&
             !memcmp(text + at, qualifier.data, qualifier.size)))
            quoted = 1;
    if (!status && quoted && !qualifier.size)
        status = UD_TRUNCATED;
    if (!status && quoted)
        status = ud_bytes_put(record, qualifier.data, qualifier.size);
    for (uint32_t at = 0; !status && at < size;) {
        if (qualifier.size && at + qualifier.size <= size &&
            !memcmp(text + at, qualifier.data, qualifier.size)) {
            status = ud_bytes_put(record, qualifier.data, qualifier.size);
            if (!status)
                status = ud_bytes_put(record, qualifier.data, qualifier.size);
            at += (uint32_t)qualifier.size;
        } else
            status = ud_bytes_put(record, text + at++, 1);
    }
    if (!status && quoted)
        status = ud_bytes_put(record, qualifier.data, qualifier.size);
    ud_bytes_clear(&delimiter);
    ud_bytes_clear(&qualifier);
    return status;
}
static ud_status csv_line(ud_csv_writer *writer, ud_bytes *record) {
    UD_TRY(ud_bytes_put(record, writer->layout.crlf ? "\r\n" : "\n", writer->layout.crlf ? 2 : 1));
    return ud_text_write(writer->file, writer->layout.encoding, (const char *)record->data,
                         record->size);
}
ud_status ud_csv_writer_open(ud_context *ctx, const char *path, const ud_field *fields,
                             uint32_t columns, const ud_csv_options *options, ud_csv_writer **out) {
    if (!ctx || !path || !out || !fields || !columns ||
        (options &&
         (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION)))
        return UD_INVALID;
    *out = NULL;
    for (uint32_t at = 0; at < columns; at++)
        if (fields[at].type == UD_BINARY || fields[at].type >= UD_STRUCT)
            return UD_UNSUPPORTED;
    ud_csv_writer *writer = ud_alloc(ctx, sizeof(*writer));
    if (!writer)
        return UD_RESOURCE;
    writer->ctx = ctx;
    writer->columns = columns;
    writer->max_record_bytes =
        options && options->max_record_bytes ? options->max_record_bytes : 16777216;
    ud_status status = layout_open(ctx, options, columns, &writer->layout);
    if (!status)
        status = ud_batch_create(ctx, fields, columns, 0, &writer->schema);
    if (!status) {
        writer->file = ud_fopen(path, "wb");
        if (!writer->file)
            status = UD_IO;
    }
    if (!status && writer->layout.bom)
        status = ud_text_bom(writer->file, writer->layout.encoding);
    const ud_json *preamble = ud_json_find(writer->layout.document, "preamble", 8);
    for (uint32_t at = 0; !status && at < ud_json_size(preamble); at++) {
        const char *line;
        uint32_t size;
        status = ud_json_text(ud_json_at(preamble, at), &line, &size);
        if (!status && (memchr(line, '\r', size) || memchr(line, '\n', size)))
            status = UD_SCHEMA;
        if (!status)
            status = ud_text_write(writer->file, writer->layout.encoding, line, size);
        if (!status)
            status =
                ud_text_write(writer->file, writer->layout.encoding,
                              writer->layout.crlf ? "\r\n" : "\n", writer->layout.crlf ? 2 : 1);
    }
    if (!status && writer->layout.skip != ud_json_size(preamble))
        status = UD_SCHEMA;
    if (!status && writer->layout.header) {
        ud_bytes row = {.ctx = ctx, .limit = writer->max_record_bytes};
        for (uint32_t physical = 0; !status && physical < columns; physical++) {
            uint32_t logical =
                writer->layout.positions ? writer->layout.positions[physical] : physical;
            status = csv_field(writer, &row, physical, fields[logical].name,
                               fields[logical].name_length);
        }
        if (!status)
            status = csv_line(writer, &row);
        ud_bytes_clear(&row);
    }
    if (status) {
        ud_csv_writer_close(writer);
        return status;
    }
    *out = writer;
    return UD_OK;
}
ud_status ud_csv_writer_append(ud_csv_writer *writer, const ud_batch *batch) {
    if (!writer || !batch || batch->ctx != writer->ctx || batch->columns != writer->columns)
        return UD_INVALID;
    if (ud_batch_has_absent(batch))
        return UD_INCOMPATIBLE;
    for (uint32_t at = 0; at < batch->columns; at++)
        if (!ud_field_matches(&writer->schema->column[at].field, &batch->column[at].field,
                              UD_FIELD_PORT))
            return UD_SCHEMA;
    for (uint32_t at = 0; at < batch->rows; at++) {
        if (ud_is_cancelled(writer->ctx))
            return UD_CANCELLED;
        ud_bytes record = {.ctx = writer->ctx, .limit = writer->max_record_bytes};
        ud_status status = UD_OK;
        for (uint32_t physical = 0; !status && physical < batch->columns; physical++) {
            uint32_t logical =
                writer->layout.positions ? writer->layout.positions[physical] : physical;
            ud_value value;
            status = ud_batch_get(batch, at, logical, &value);
            char local[256], *allocated = NULL;
            const char *text = NULL;
            uint32_t size = 0;
            if (!status && value.is_null) {
                text = writer->layout.marker.data;
                size = writer->layout.marker.length;
            } else if (!status && value.type == UD_UTF8) {
                text = value.text;
                size = value.text_length;
            } else if (!status) {
                status = ud_value_format(&value, local, sizeof(local), &size);
                if (status == UD_RESOURCE) {
                    allocated = ud_alloc(writer->ctx, (size_t)size + 1);
                    if (!allocated)
                        status = UD_RESOURCE;
                    else
                        status = ud_value_format(&value, allocated, size + 1, &size);
                }
                text = allocated ? allocated : local;
                if (!status) {
                    if (!size)
                        status = UD_CORRUPT;
                    else
                        size--;
                }
            }
            if (!status && !value.is_null && size == writer->layout.marker.length &&
                !memcmp(text, writer->layout.marker.data, size))
                status = UD_QUALITY;
            if (!status)
                status = csv_field(writer, &record, physical, text, size);
            ud_free(allocated);
        }
        if (!status)
            status = csv_line(writer, &record);
        ud_bytes_clear(&record);
        if (status)
            return status;
    }
    return UD_OK;
}
ud_status ud_csv_writer_close(ud_csv_writer *writer) {
    if (!writer)
        return UD_OK;
    ud_status status = UD_OK;
    if (writer->file) {
        status = ud_flush(writer->ctx, writer->file);
        if (fclose(writer->file))
            status = UD_IO;
    }
    ud_batch_destroy(writer->schema);
    layout_clear(&writer->layout);
    ud_free(writer);
    return status;
}
ud_status ud_csv_write(ud_context *ctx, const char *path, const ud_batch *batch,
                       const ud_csv_options *options) {
    if (!batch)
        return UD_INVALID;
    ud_csv_writer *writer = NULL;
    ud_status status =
        ud_csv_writer_open(ctx, path, batch->schema->fields, batch->columns, options, &writer);
    if (!status)
        status = ud_csv_writer_append(writer, batch);
    ud_status closed = ud_csv_writer_close(writer);
    return status ? status : closed;
}
