#include "ud/csv.h"
#include "../internal.h"
#include <math.h>
#include <locale.h>
struct ud_csv_reader {
    ud_context *ctx;
    FILE *file;
    char *path, *null_marker;
    uint32_t null_length, batch_size, columns;
    ud_field *fields;
    uint64_t record;
};
typedef struct {
    char **text;
    uint32_t *length;
    uint32_t n;
} record;
static void record_free(record *r) {
    for (uint32_t i = 0; i < r->n; i++)
        ud_free(r->text[i]);
    ud_free(r->text);
    ud_free(r->length);
    memset(r, 0, sizeof(*r));
}
static ud_status finish_field(ud_csv_reader *r, record *row, ud_bytes *field) {
    if (row->n == r->columns)
        return UD_SCHEMA;
    if (field->size > UINT32_MAX || !ud_utf8_valid((char *)field->data, field->size))
        return UD_PARSE;
    char *s = ud_strdup_n(r->ctx, field->data ? (char *)field->data : "", field->size);
    if (!s)
        return UD_RESOURCE;
    row->text[row->n] = s;
    row->length[row->n] = (uint32_t)field->size;
    row->n++;
    field->size = 0;
    return UD_OK;
}
static ud_status next_record(ud_csv_reader *r, record *row) {
    row->text = ud_alloc(r->ctx, r->columns * sizeof(char *));
    row->length = ud_alloc(r->ctx, r->columns * sizeof(uint32_t));
    if (!row->text || !row->length) {
        record_free(row);
        return UD_RESOURCE;
    }
    ud_bytes field = {.ctx = r->ctx};
    int quoted = 0, closed = 0, start = 1, any = 0;
    ud_status s = UD_OK;
    for (;;) {
        int ch = fgetc(r->file);
        if (ch == EOF) {
            if (ferror(r->file))
                s = UD_IO;
            else if (quoted)
                s = UD_PARSE;
            else if (!any)
                s = UD_END;
            else
                s = finish_field(r, row, &field);
            break;
        }
        any = 1;
        if (quoted) {
            if (ch == '"') {
                int next = fgetc(r->file);
                if (next == '"') {
                    char q = '"';
                    s = ud_bytes_put(&field, &q, 1);
                } else {
                    quoted = 0;
                    closed = 1;
                    if (next != EOF)
                        ungetc(next, r->file);
                    else if (ferror(r->file))
                        s = UD_IO;
                }
            } else {
                char c = (char)ch;
                s = ud_bytes_put(&field, &c, 1);
            }
        } else if (ch == ',' || ch == '\n' || ch == '\r') {
            s = finish_field(r, row, &field);
            start = 1;
            closed = 0;
            if (ch != ',') {
                if (ch == '\r') {
                    int next = fgetc(r->file);
                    if (next != '\n' && next != EOF)
                        ungetc(next, r->file);
                }
                break;
            }
        } else if (ch == '"' && start) {
            quoted = 1;
            start = 0;
        } else if (closed || ch == '"')
            s = UD_PARSE;
        else {
            char c = (char)ch;
            s = ud_bytes_put(&field, &c, 1);
            start = 0;
        }
        if (s)
            break;
    }
    ud_bytes_clear(&field);
    if (!s && row->n != r->columns)
        s = UD_SCHEMA;
    if (s) {
        uint32_t col = row->n + 1;
        record_free(row);
        if (s != UD_END)
            return ud_fail(r->ctx, s,
                           "%s: record %llu field %u: invalid CSV, encoding, or field count",
                           r->path, (unsigned long long)(r->record + 1), col);
    } else
        r->record++;
    return s;
}
ud_status ud_csv_open(ud_context *c, const char *path, const ud_field *f, uint32_t n,
                      const ud_csv_options *o, ud_csv_reader **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!path || !f || !n || (o && (o->struct_size < sizeof(*o) || o->abi_version != UD_ABI_VERSION)))
        return UD_INVALID;
    for (uint32_t i = 0; i < n; i++)
        if (f[i].type == UD_BINARY || f[i].type >= UD_STRUCT)
            return ud_fail(c, UD_UNSUPPORTED, "CSV complex / binary fields require an explicit format transform");
    ud_batch *check = NULL;
    UD_TRY(ud_batch_create(c, f, n, 0, &check));
    ud_batch_destroy(check);
    ud_csv_reader *r = ud_alloc(c, sizeof(*r));
    if (!r)
        return UD_RESOURCE;
    r->ctx = c;
    r->columns = n;
    r->batch_size = o && o->batch_size ? o->batch_size : 4096;
    const char *marker = o && o->null_marker ? o->null_marker : "\\N";
    r->null_length = o && o->null_marker ? o->null_marker_length : 2;
    if (!r->null_length || memchr(marker, 0, r->null_length) ||
        memchr(marker, ',', r->null_length) || memchr(marker, '\n', r->null_length) ||
        memchr(marker, '\r', r->null_length) || memchr(marker, '"', r->null_length) ||
        !ud_utf8_valid(marker, r->null_length)) {
        ud_csv_close(r);
        return ud_fail(c, UD_INVALID, "invalid null marker");
    }
    r->null_marker = ud_strdup_n(c, marker, r->null_length);
    r->path = ud_strdup_n(c, path, strlen(path));
    r->fields = ud_alloc(c, n * sizeof(*f));
    if (!r->null_marker || !r->path || !r->fields) {
        ud_csv_close(r);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < n; i++) {
        r->fields[i] = f[i];
        r->fields[i].name = ud_strdup_n(c, f[i].name, f[i].name_length);
        r->fields[i].timezone = NULL;
        if (f[i].timezone_length) r->fields[i].timezone = ud_strdup_n(c, f[i].timezone, f[i].timezone_length);
        if (!r->fields[i].name || (f[i].timezone_length && !r->fields[i].timezone)) {
            ud_csv_close(r);
            return UD_RESOURCE;
        }
    }
    r->file = ud_fopen(path, "rb");
    if (!r->file) {
        ud_csv_close(r);
        return ud_fail(c, UD_IO, "cannot open CSV: %s", path);
    }
    /* Optional UTF-8 BOM only at byte zero. */
    unsigned char bom[3];
    size_t k = fread(bom, 1, 3, r->file);
    if (k != 3 || memcmp(bom, "\xef\xbb\xbf", 3)) {
        if (ud_seek(r->file, 0)) {
            ud_csv_close(r);
            return UD_IO;
        }
    }
    record header = {0};
    ud_status s = next_record(r, &header);
    if (s == UD_END)
        s = ud_fail(c, UD_SCHEMA, "missing CSV header: %s", path);
    if (!s)
        for (uint32_t i = 0; i < n; i++)
            if (header.length[i] != f[i].name_length ||
                memcmp(header.text[i], f[i].name, f[i].name_length)) {
                s = ud_fail(c, UD_SCHEMA, "%s: header field %u mismatch", path, i + 1);
                break;
            }
    record_free(&header);
    if (s) {
        ud_csv_close(r);
        return s;
    }
    *out = r;
    return UD_OK;
}
void ud_csv_close(ud_csv_reader *r) {
    if (!r)
        return;
    if (r->file)
        fclose(r->file);
    if (r->fields)
        for (uint32_t i = 0; i < r->columns; i++)
            { ud_free((void *)r->fields[i].name); ud_free((void *)r->fields[i].timezone); }
    ud_free(r->fields);
    ud_free(r->path);
    ud_free(r->null_marker);
    ud_free(r);
}
uint64_t ud_csv_offset(const ud_csv_reader *r) {
    int64_t p = r && r->file ? ud_tell(r->file) : -1;
    return p < 0 ? UINT64_MAX : (uint64_t)p;
}
uint64_t ud_csv_record(const ud_csv_reader *r) {
    return r ? r->record : 0;
}
ud_status ud_csv_resume(ud_csv_reader *r, uint64_t offset, uint64_t record_number) {
    if (!r || !record_number || offset == UINT64_MAX || ud_seek(r->file, offset))
        return UD_INVALID;
    r->record = record_number;
    return UD_OK;
}
ud_status ud_csv_next(ud_csv_reader *r, ud_batch **out) {
    if (!r || !out)
        return UD_INVALID;
    *out = NULL;
    ud_batch *b = NULL;
    UD_TRY(ud_batch_create(r->ctx, r->fields, r->columns, r->batch_size, &b));
    ud_value *v = ud_alloc(r->ctx, r->columns * sizeof(*v));
    if (!v) {
        ud_batch_destroy(b);
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    while (b->rows < r->batch_size) {
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
        memset(v, 0, r->columns * sizeof(*v));
        for (uint32_t i = 0; i < r->columns; i++) {
            v[i].type = r->fields[i].type;
            if (row.length[i] == r->null_length &&
                !memcmp(row.text[i], r->null_marker, r->null_length)) {
                v[i].is_null = 1;
                if (!r->fields[i].nullable)
                    s = UD_SCHEMA;
            } else
                s = ud_value_parse_field(r->ctx, r->fields + i, row.text[i], row.length[i],
                                          UD_ROUND_EXACT, v + i);
            if (s) {
                ud_fail(r->ctx, s, "%s: record %llu field %u: invalid typed value", r->path,
                        (unsigned long long)r->record, i + 1);
                break;
            }
        }
        if (!s)
            s = ud_batch_append(b, v, r->columns);
        record_free(&row);
        if (s)
            break;
    }
    ud_free(v);
    if (s || !b->rows) {
        ud_batch_destroy(b);
        return s ? s : UD_END;
    }
    *out = b;
    return UD_OK;
}
static int write_field(FILE *f, const char *s, size_t n) {
    int quote = memchr(s, ',', n) || memchr(s, '"', n) || memchr(s, '\n', n) || memchr(s, '\r', n);
    if (quote && fputc('"', f) == EOF)
        return 0;
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '"' && fputc('"', f) == EOF)
            return 0;
        if (fputc((unsigned char)s[i], f) == EOF)
            return 0;
    }
    return !quote || fputc('"', f) != EOF;
}
struct ud_csv_writer {
    ud_context *ctx;
    FILE *file;
    char *marker;
    ud_batch *schema;
    int failed;
};
ud_status ud_csv_writer_open(ud_context *c, const char *path, const ud_field *fields,
                             uint32_t columns, const char *marker, ud_csv_writer **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!path || !marker || !*marker || strpbrk(marker, ",\"\r\n") ||
        !ud_utf8_valid(marker, strlen(marker)))
        return UD_INVALID;
    ud_csv_writer *w = ud_alloc(c, sizeof(*w));
    if (!w)
        return UD_RESOURCE;
    w->ctx = c;
    w->marker = ud_strdup_n(c, marker, strlen(marker));
    ud_status s = w->marker ? ud_batch_create(c, fields, columns, 0, &w->schema) : UD_RESOURCE;
    if (!s) {
        w->file = ud_fopen(path, "wb");
        if (!w->file)
            s = ud_fail(c, UD_IO, "cannot write CSV: %s", path);
    }
    if (!s) {
        for (uint32_t i = 0; i < columns; i++)
            if ((i && fputc(',', w->file) == EOF) ||
                !write_field(w->file, fields[i].name, fields[i].name_length))
                s = UD_IO;
        if (fputc('\n', w->file) == EOF)
            s = UD_IO;
    }
    if (s) {
        ud_csv_writer_close(w);
        return s;
    }
    *out = w;
    return UD_OK;
}
ud_status ud_csv_writer_append(ud_csv_writer *w, const ud_batch *b) {
    if (!w || !b || w->failed)
        return UD_INVALID;
    ud_context *c = w->ctx;
    FILE *f = w->file;
    const char *marker = w->marker;
    ud_status s = UD_OK;
    if (b->columns != w->schema->columns)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < b->columns; i++) {
        ud_field x, y;
        ud_batch_field(b, i, &x);
        ud_batch_field(w->schema, i, &y);
        if (!ud_field_equal(&x, &y, 1))
            return UD_SCHEMA;
    }
    for (uint32_t r = 0; !s && r < b->rows; r++) {
        for (uint32_t i = 0; i < b->columns; i++) {
            ud_value v;
            ud_batch_get(b, r, i, &v);
            char num[128];
            const char *p = num;
            size_t n = 0;
            if (v.is_null) {
                p = marker;
                n = strlen(marker);
            } else if (v.type == UD_UTF8) {
                p = v.text;
                n = v.text_length;
                if (n == strlen(marker) && !memcmp(p, marker, n)) {
                    s = ud_fail(c, UD_INVALID,
                                "text value equals null marker; choose another marker");
                    break;
                }
            } else if (v.type == UD_BINARY) {
                s = ud_fail(c, UD_UNSUPPORTED, "Binary CSV requires an explicit encoding transform");
                break;
            } else {
                uint32_t required = 0;
                s = ud_value_format(&v, num, sizeof(num), &required);
                if (s) break;
                n = required - 1;
            }
            if ((i && fputc(',', f) == EOF) || !write_field(f, p, n)) {
                s = UD_IO;
                break;
            }
        }
        if (!s && fputc('\n', f) == EOF)
            s = UD_IO;
    }
    if (s)
        w->failed = 1;
    return s;
}
ud_status ud_csv_writer_close(ud_csv_writer *w) {
    if (!w)
        return UD_OK;
    ud_status s = w->failed ? UD_IO : UD_OK;
    if (w->file) {
        if (!s)
            s = ud_flush(w->ctx, w->file);
        if (fclose(w->file) && !s)
            s = UD_IO;
    }
    ud_batch_destroy(w->schema);
    ud_free(w->marker);
    ud_free(w);
    return s;
}
ud_status ud_csv_write(ud_context *c, const char *path, const ud_batch *b, const char *marker) {
    if (!b)
        return UD_INVALID;
    ud_field *fields = ud_alloc(c, b->columns * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < b->columns; i++)
        ud_batch_field(b, i, fields + i);
    ud_csv_writer *w = NULL;
    ud_status s = ud_csv_writer_open(c, path, fields, b->columns, marker, &w);
    ud_free(fields);
    if (!s)
        s = ud_csv_writer_append(w, b);
    ud_status closed = ud_csv_writer_close(w);
    return s ? s : closed;
}
