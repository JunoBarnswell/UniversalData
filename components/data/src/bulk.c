#include "../internal.h"
#include "ud/sql.h"
#include <math.h>
/* One streaming scalar encoder, owned by Data, for vendor bulk transports. */
struct ud_bulk_reader {
    ud_context *ctx;
    ud_batch *batch;
    int32_t dialect;
    uint32_t row, column, phase, at, size, pending_at, pending_size;
    ud_value value;
    const char *text;
    char scalar[256], pending[4];
    int escape, binary_hex;
};
ud_status ud_sql_bulk_open(ud_context *ctx, const ud_batch *batch, int32_t dialect,
                           ud_bulk_reader **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !batch || batch->ctx != ctx || !batch->sealed)
        return UD_INVALID;
    if (dialect != UD_SQL_POSTGRESQL && dialect != UD_SQL_MYSQL && dialect != UD_SQL_MARIADB)
        return UD_UNSUPPORTED;
    for (uint32_t i = 0; i < batch->columns; i++)
        if (batch->column[i].field.type >= UD_STRUCT)
            return UD_UNSUPPORTED;
    ud_bulk_reader *r = ud_alloc(ctx, sizeof(*r));
    if (!r)
        return UD_RESOURCE;
    ud_status s = ud_batch_retain((ud_batch *)batch);
    if (s) {
        ud_free(r);
        return s;
    }
    r->ctx = ctx;
    r->batch = (ud_batch *)batch;
    r->dialect = dialect;
    *out = r;
    return UD_OK;
}
static ud_status cell(ud_bulk_reader *r) {
    UD_TRY(ud_batch_get(r->batch, r->row, r->column, &r->value));
    r->at = 0;
    r->pending_at = r->pending_size = 0;
    r->escape = 0;
    r->binary_hex = 0;
    if (r->value.is_null) {
        r->text = "\\N";
        r->size = 2;
    } else if (r->value.type == UD_UTF8 || r->value.type == UD_BINARY) {
        r->text = r->value.text;
        r->size = r->value.text_length;
        if (r->dialect == UD_SQL_POSTGRESQL && r->value.type == UD_BINARY) {
            r->binary_hex = 1;
            memcpy(r->pending, "\\\\x", 3);
            r->pending_size = 3;
        } else {
            r->escape = 1;
            if (r->dialect == UD_SQL_POSTGRESQL && r->size && memchr(r->text, 0, r->size))
                return UD_PARSE; /* PostgreSQL text cannot represent NUL. */
        }
    } else {
        uint32_t required = 0;
        if (r->dialect != UD_SQL_POSTGRESQL && r->value.type == UD_FLOAT64 &&
            !isfinite(r->value.real))
            return UD_UNSUPPORTED;
        if (r->dialect != UD_SQL_POSTGRESQL && r->value.type == UD_BOOL) {
            r->scalar[0] = r->value.integer ? '1' : '0';
            r->scalar[1] = 0;
            required = 2;
        } else
            UD_TRY(ud_value_format(&r->value, r->scalar, sizeof(r->scalar), &required));
        r->text = r->scalar;
        r->size = required - 1;
        if (r->dialect != UD_SQL_POSTGRESQL && r->value.type == UD_TIMESTAMP) {
            if (r->size < 20 || r->scalar[10] != 'T' || r->scalar[r->size - 1] != 'Z')
                return UD_SCHEMA;
            r->scalar[10] = ' ';
            r->scalar[--r->size] = 0;
        }
    }
    return UD_OK;
}
ud_status ud_sql_bulk_read(ud_bulk_reader *r, void *output, uint32_t capacity, uint32_t *length) {
    if (!r || !output || !capacity || !length)
        return UD_INVALID;
    *length = 0;
    if (ud_is_cancelled(r->ctx))
        return UD_CANCELLED;
    char *out = output;
    while (*length < capacity && r->row < r->batch->rows) {
        if (!r->phase) {
            UD_TRY(cell(r));
            r->phase = 1;
        }
        if (r->pending_at < r->pending_size) {
            out[(*length)++] = r->pending[r->pending_at++];
            continue;
        }
        if (r->at < r->size) {
            unsigned char ch = (unsigned char)r->text[r->at++];
            r->pending_at = 0;
            r->pending_size = 0;
            if (r->binary_hex) {
                static const char hex[] = "0123456789abcdef";
                r->pending[0] = hex[ch >> 4];
                r->pending[1] = hex[ch & 15];
                r->pending_size = 2;
            } else if (r->escape &&
                       (ch == '\\' || ch == '\t' || ch == '\n' || ch == '\r' || ch == 0 ||
                        ch == 26 || ch == '\b' ||
                        (r->dialect == UD_SQL_POSTGRESQL && (ch == '\f' || ch == '\v')))) {
                r->pending[0] = '\\';
                r->pending[1] = ch == '\t'                                    ? 't'
                                : ch == '\n'                                  ? 'n'
                                : ch == '\r'                                  ? 'r'
                                : ch == 0                                     ? '0'
                                : ch == 26 && r->dialect != UD_SQL_POSTGRESQL ? 'Z'
                                : ch == '\b'                                  ? 'b'
                                : ch == '\f'                                  ? 'f'
                                : ch == '\v'                                  ? 'v'
                                                                              : (char)ch;
                if (ch == 26 && r->dialect == UD_SQL_POSTGRESQL) {
                    r->pending[1] = '0';
                    r->pending[2] = '3';
                    r->pending[3] = '2';
                    r->pending_size = 4;
                } else
                    r->pending_size = 2;
            } else {
                r->pending[0] = (char)ch;
                r->pending_size = 1;
            }
            continue;
        }
        out[(*length)++] = r->column + 1 == r->batch->columns ? '\n' : '\t';
        if (++r->column == r->batch->columns) {
            r->column = 0;
            r->row++;
        }
        r->phase = 0;
    }
    return *length ? UD_OK : UD_END;
}
void ud_sql_bulk_close(ud_bulk_reader *r) {
    if (r) {
        ud_batch_destroy(r->batch);
        ud_free(r);
    }
}
