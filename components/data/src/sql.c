#include "../internal.h"
#include "ud/sql.h"
typedef struct writer {
    ud_context *ctx;
    ud_bytes text;
    ud_sql_statement *statement;
    const ud_write_options *options;
    const ud_batch *batch;
    int dialect, style;
    uint32_t capacity;
    uint32_t *selection;
} writer;
static ud_status put(writer *w, const char *s) {
    return ud_bytes_put(&w->text, s, strlen(s));
}
static ud_status quote(writer *w, ud_string s) {
    if (!s.data || !s.length || memchr(s.data, 0, s.length))
        return UD_SCHEMA;
    char q = w->dialect == UD_SQL_MYSQL || w->dialect == UD_SQL_MARIADB ? '`' : '"';
    UD_TRY(ud_bytes_put(&w->text, &q, 1));
    for (uint32_t i = 0; i < s.length; i++) {
        if (s.data[i] == q)
            UD_TRY(ud_bytes_put(&w->text, &q, 1));
        UD_TRY(ud_bytes_put(&w->text, s.data + i, 1));
    }
    return ud_bytes_put(&w->text, &q, 1);
}
static ud_status table(writer *w) {
    ud_string name = w->options->table;
    const char *dot = memchr(name.data, '.', name.length);
    if (!dot)
        return quote(w, name);
    if (memchr(dot + 1, '.', name.length - (uint32_t)(dot - name.data) - 1))
        return UD_SCHEMA;
    UD_TRY(quote(w, (ud_string){name.data, (uint32_t)(dot - name.data)}));
    UD_TRY(put(w, "."));
    return quote(w, (ud_string){dot + 1, name.length - (uint32_t)(dot - name.data) - 1});
}
static ud_status column(writer *w, uint32_t index, const char *prefix) {
    if (index >= w->batch->columns)
        return UD_SCHEMA;
    if (prefix)
        UD_TRY(put(w, prefix));
    const ud_field *f = &w->batch->column[index].field;
    return quote(w, (ud_string){f->name, f->name_length});
}
static ud_status parameter(writer *w, uint32_t row, uint32_t col, uint32_t source) {
    ud_sql_statement *s = w->statement;
    if (s->parameter_count == 65535)
        return UD_RESOURCE;
    if (s->parameter_count == w->capacity) {
        uint32_t cap = w->capacity ? w->capacity * 2 : 32;
        if (cap > 65535)
            cap = 65535;
        ud_sql_parameter *p = ud_alloc(w->ctx, (size_t)cap * sizeof(*p));
        if (!p)
            return UD_RESOURCE;
        if (s->parameter_count)
            memcpy(p, s->parameters, (size_t)s->parameter_count * sizeof(*p));
        ud_free(s->parameters);
        s->parameters = p;
        w->capacity = cap;
    }
    s->parameters[s->parameter_count++] = (ud_sql_parameter){row, col, source};
    char marker[24];
    int n = w->style == UD_SQL_PARAMETER_QUESTION
                ? snprintf(marker, sizeof(marker), "?")
                : snprintf(marker, sizeof(marker),
                           w->style == UD_SQL_PARAMETER_NAMED ? ":p%u" : "$%u", s->parameter_count);
    return ud_bytes_put(&w->text, marker, (size_t)n);
}
static int is_key(const writer *w, uint32_t column_index) {
    for (uint32_t k = 0; k < w->options->key_count; k++)
        if (w->options->keys[k] == column_index)
            return 1;
    return 0;
}
static ud_status keys_where(writer *w, uint32_t row, const char *left, const char *right) {
    for (uint32_t k = 0; k < w->options->key_count; k++) {
        if (k)
            UD_TRY(put(w, " AND "));
        UD_TRY(column(w, w->options->keys[k], left));
        UD_TRY(put(w, "="));
        if (right)
            UD_TRY(column(w, w->options->keys[k], right));
        else
            UD_TRY(parameter(w, row, w->options->keys[k], UD_SQL_AFTER));
    }
    return UD_OK;
}
static ud_status preimage(writer *w, uint32_t row) {
    const ud_write_options *o = w->options;
    if (!o->before)
        return keys_where(w, row, NULL, NULL);
    for (uint32_t i = 0; i < w->batch->columns; i++) {
        if (i)
            UD_TRY(put(w, " AND "));
        ud_type t = w->batch->column[i].field.type;
        if (w->dialect == UD_SQL_ORACLE) {
            if (t == UD_BINARY)
                return UD_UNSUPPORTED;
            UD_TRY(put(w, "DECODE("));
            UD_TRY(column(w, i, NULL));
            UD_TRY(put(w, ","));
            UD_TRY(parameter(w, row, i, UD_SQL_BEFORE));
            UD_TRY(put(w, ",1,0)=1"));
        } else if (w->dialect == UD_SQL_MYSQL || w->dialect == UD_SQL_MARIADB) {
            if (t == UD_UTF8 || t == UD_BINARY)
                UD_TRY(put(w, "BINARY "));
            UD_TRY(column(w, i, NULL));
            UD_TRY(put(w, " <=> "));
            if (t == UD_UTF8 || t == UD_BINARY)
                UD_TRY(put(w, "BINARY "));
            UD_TRY(parameter(w, row, i, UD_SQL_BEFORE));
        } else if (w->dialect == UD_SQL_SQLITE) {
            UD_TRY(column(w, i, NULL));
            UD_TRY(put(w, " COLLATE BINARY IS "));
            UD_TRY(parameter(w, row, i, UD_SQL_BEFORE));
        } else if (w->dialect == UD_SQL_POSTGRESQL || w->dialect == UD_SQL_DUCKDB ||
                   w->dialect == UD_SQL_DB2) {
            UD_TRY(column(w, i, NULL));
            UD_TRY(put(w, " IS NOT DISTINCT FROM "));
            UD_TRY(parameter(w, row, i, UD_SQL_BEFORE));
        } else {
            UD_TRY(put(w, "(("));
            UD_TRY(column(w, i, NULL));
            if (t == UD_UTF8)
                UD_TRY(put(w, " COLLATE Latin1_General_100_BIN2"));
            UD_TRY(put(w, "="));
            UD_TRY(parameter(w, row, i, UD_SQL_BEFORE));
            if (t == UD_UTF8 || t == UD_BINARY) {
                UD_TRY(put(w, " AND DATALENGTH("));
                UD_TRY(column(w, i, NULL));
                UD_TRY(put(w, ")=DATALENGTH("));
                UD_TRY(parameter(w, row, i, UD_SQL_BEFORE));
                UD_TRY(put(w, ")"));
            }
            UD_TRY(put(w, ") OR ("));
            UD_TRY(column(w, i, NULL));
            UD_TRY(put(w, " IS NULL AND "));
            UD_TRY(parameter(w, row, i, UD_SQL_BEFORE));
            UD_TRY(put(w, " IS NULL))"));
        }
    }
    return UD_OK;
}
static ud_status fields(writer *w, const char *prefix) {
    for (uint32_t i = 0; i < w->batch->columns; i++) {
        if (i)
            UD_TRY(put(w, ","));
        UD_TRY(column(w, i, prefix));
    }
    return UD_OK;
}
static uint32_t row_at(const writer *w, uint32_t first, uint32_t row) {
    return w->selection ? w->selection[row] : first + row;
}
static ud_status tuple(writer *w, uint32_t row) {
    UD_TRY(put(w, "("));
    for (uint32_t i = 0; i < w->batch->columns; i++) {
        if (i)
            UD_TRY(put(w, ","));
        UD_TRY(parameter(w, row, i, UD_SQL_AFTER));
    }
    return put(w, ")");
}
static ud_status insert(writer *w, uint32_t first, uint32_t rows, int guard) {
    UD_TRY(put(w, "INSERT INTO "));
    UD_TRY(table(w));
    UD_TRY(put(w, "("));
    UD_TRY(fields(w, NULL));
    UD_TRY(put(w, ") "));
    if (guard) {
        UD_TRY(put(w, "SELECT "));
        for (uint32_t i = 0; i < w->batch->columns; i++) {
            if (i)
                UD_TRY(put(w, ","));
            UD_TRY(parameter(w, first, i, UD_SQL_AFTER));
        }
        if (w->dialect == UD_SQL_ORACLE)
            UD_TRY(put(w, " FROM dual"));
        UD_TRY(put(w, " WHERE NOT EXISTS(SELECT 1 FROM "));
        UD_TRY(table(w));
        UD_TRY(put(w, " WHERE "));
        UD_TRY(column(w, w->options->current_column, NULL));
        UD_TRY(put(w, w->dialect == UD_SQL_POSTGRESQL || w->dialect == UD_SQL_DUCKDB ? "=TRUE AND "
                                                                                     : "=1 AND "));
        UD_TRY(keys_where(w, first, NULL, NULL));
        return put(w, ")");
    }
    if (w->dialect == UD_SQL_ORACLE) {
        for (uint32_t r = 0; r < rows; r++) {
            if (r)
                UD_TRY(put(w, " UNION ALL "));
            UD_TRY(put(w, "SELECT "));
            for (uint32_t i = 0; i < w->batch->columns; i++) {
                if (i)
                    UD_TRY(put(w, ","));
                UD_TRY(parameter(w, row_at(w, first, r), i, UD_SQL_AFTER));
            }
            UD_TRY(put(w, " FROM dual"));
        }
        return UD_OK;
    }
    UD_TRY(put(w, "VALUES "));
    for (uint32_t row = 0; row < rows; row++) {
        if (row)
            UD_TRY(put(w, ","));
        UD_TRY(tuple(w, row_at(w, first, row)));
    }
    return UD_OK;
}
static ud_status upsert(writer *w, uint32_t first, uint32_t rows) {
    if (w->dialect == UD_SQL_SQLSERVER || w->dialect == UD_SQL_ORACLE || w->dialect == UD_SQL_DB2) {
        UD_TRY(put(w, w->dialect == UD_SQL_SQLSERVER ? "MERGE INTO " : "MERGE INTO "));
        UD_TRY(table(w));
        if (w->dialect == UD_SQL_SQLSERVER)
            UD_TRY(put(w, " WITH (HOLDLOCK)"));
        UD_TRY(put(w, " t USING ("));
        if (w->dialect == UD_SQL_SQLSERVER) {
            UD_TRY(put(w, "VALUES "));
            for (uint32_t row = 0; row < rows; row++) {
                if (row)
                    UD_TRY(put(w, ","));
                UD_TRY(tuple(w, row_at(w, first, row)));
            }
            UD_TRY(put(w, ") s("));
            UD_TRY(fields(w, NULL));
            UD_TRY(put(w, ")"));
        } else {
            for (uint32_t row = 0; row < rows; row++) {
                if (row)
                    UD_TRY(put(w, " UNION ALL "));
                UD_TRY(put(w, "SELECT "));
                for (uint32_t i = 0; i < w->batch->columns; i++) {
                    if (i)
                        UD_TRY(put(w, ","));
                    UD_TRY(parameter(w, row_at(w, first, row), i, UD_SQL_AFTER));
                    UD_TRY(put(w, " AS "));
                    UD_TRY(column(w, i, NULL));
                }
                UD_TRY(
                    put(w, w->dialect == UD_SQL_ORACLE ? " FROM dual" : " FROM sysibm.sysdummy1"));
            }
            UD_TRY(put(w, ") s"));
        }
        UD_TRY(put(w, " ON ("));
        UD_TRY(keys_where(w, first, "t.", "s."));
        UD_TRY(put(w, ") "));
        int n = 0;
        for (uint32_t i = 0; i < w->batch->columns; i++)
            n += !is_key(w, i);
        if (n) {
            UD_TRY(put(w, "WHEN MATCHED THEN UPDATE SET "));
            uint32_t written = 0;
            for (uint32_t i = 0; i < w->batch->columns; i++)
                if (!is_key(w, i)) {
                    if (written++)
                        UD_TRY(put(w, ","));
                    UD_TRY(column(w, i, w->dialect == UD_SQL_SQLSERVER ? NULL : "t."));
                    UD_TRY(put(w, "="));
                    UD_TRY(column(w, i, "s."));
                }
            UD_TRY(put(w, " "));
        }
        UD_TRY(put(w, "WHEN NOT MATCHED THEN INSERT("));
        UD_TRY(fields(w, NULL));
        UD_TRY(put(w, ") VALUES("));
        UD_TRY(fields(w, "s."));
        return put(w, ");");
    }

    UD_TRY(insert(w, first, rows, 0));
    if (w->dialect == UD_SQL_MYSQL || w->dialect == UD_SQL_MARIADB) {
        UD_TRY(put(w, " ON DUPLICATE KEY UPDATE "));
        uint32_t n = 0;
        for (uint32_t i = 0; i < w->batch->columns; i++)
            if (!is_key(w, i)) {
                if (n++)
                    UD_TRY(put(w, ","));
                UD_TRY(column(w, i, NULL));
                UD_TRY(put(w, "=VALUES("));
                UD_TRY(column(w, i, NULL));
                UD_TRY(put(w, ")"));
            }
        if (!n) {
            UD_TRY(column(w, w->options->keys[0], NULL));
            UD_TRY(put(w, "=VALUES("));
            UD_TRY(column(w, w->options->keys[0], NULL));
            UD_TRY(put(w, ")"));
        }
        return UD_OK;
    }
    UD_TRY(put(w, " ON CONFLICT("));
    for (uint32_t k = 0; k < w->options->key_count; k++) {
        if (k)
            UD_TRY(put(w, ","));
        UD_TRY(column(w, w->options->keys[k], NULL));
    }
    uint32_t n = 0;
    for (uint32_t i = 0; i < w->batch->columns; i++)
        n += !is_key(w, i);
    UD_TRY(put(w, n ? ") DO UPDATE SET " : ") DO NOTHING"));
    uint32_t written = 0;
    for (uint32_t i = 0; i < w->batch->columns; i++)
        if (!is_key(w, i)) {
            if (written++)
                UD_TRY(put(w, ","));
            UD_TRY(column(w, i, NULL));
            UD_TRY(put(w, "=excluded."));
            UD_TRY(column(w, i, NULL));
        }
    return UD_OK;
}
typedef struct row_key {
    uint64_t hash;
    uint32_t row, next;
} row_key;
static ud_status last_rows(writer *w, uint32_t first, uint32_t *count) {
    uint32_t rows = *count, capacity = 16;
    while ((uint64_t)capacity < (uint64_t)rows * 2 && capacity < 65536)
        capacity *= 2;
    uint32_t *buckets = ud_alloc(w->ctx, (size_t)capacity * sizeof(*buckets));
    row_key *nodes = ud_alloc(w->ctx, (size_t)rows * sizeof(*nodes));
    uint32_t *selection = ud_alloc(w->ctx, (size_t)rows * sizeof(*selection));
    if (!buckets || !nodes || !selection) {
        ud_free(buckets);
        ud_free(nodes);
        ud_free(selection);
        return UD_RESOURCE;
    }
    memset(buckets, 255, (size_t)capacity * sizeof(*buckets));
    uint32_t used = 0;
    ud_status status = UD_OK;
    for (uint32_t row = first; !status && row < first + rows; row++) {
        uint64_t hash = UD_VALUE_HASH_SEED;
        for (uint32_t k = 0; !status && k < w->options->key_count; k++) {
            ud_value value;
            status = ud_batch_get(w->batch, row, w->options->keys[k], &value);
            if (!status && value.is_null)
                status = UD_SCHEMA;
            if (!status)
                hash = ud_value_hash(&value, hash);
        }
        uint32_t bucket = (uint32_t)(hash & (capacity - 1)), at = buckets[bucket];
        int equal = 0;
        for (; !status && at != UINT32_MAX; at = nodes[at].next) {
            if (nodes[at].hash != hash)
                continue;
            equal = 1;
            for (uint32_t k = 0; !status && equal && k < w->options->key_count; k++) {
                ud_value a, b;
                status = ud_batch_get(w->batch, row, w->options->keys[k], &a);
                if (!status)
                    status = ud_batch_get(w->batch, nodes[at].row, w->options->keys[k], &b);
                if (!status)
                    equal = !ud_value_compare(&a, &b);
            }
            if (equal)
                break;
        }
        if (status)
            break;
        if (equal)
            nodes[at].row = row;
        else {
            nodes[used] = (row_key){hash, row, buckets[bucket]};
            buckets[bucket] = used++;
        }
    }
    if (!status) {
        memset(selection, 0, (size_t)rows * sizeof(*selection));
        for (uint32_t i = 0; i < used; i++)
            selection[nodes[i].row - first] = 1;
        uint32_t n = 0;
        for (uint32_t i = 0; i < rows; i++)
            if (selection[i])
                selection[n++] = first + i;
        *count = n;
        w->selection = selection;
        selection = NULL;
    }
    ud_free(buckets);
    ud_free(nodes);
    ud_free(selection);
    return status;
}
ud_status ud_sql_compile_write(ud_context *ctx, int32_t dialect, int32_t style,
                               const ud_write_options *o, const ud_batch *b, uint32_t first,
                               uint32_t rows, ud_sql_statement *out) {
    if (!ctx || !o || !b || !out || ctx != b->ctx || o->struct_size != sizeof(*o) ||
        o->abi_version != UD_ABI_VERSION || first > b->rows || rows > b->rows - first ||
        dialect < UD_SQL_SQLITE || dialect > UD_SQL_DUCKDB || style < 1 || style > 3)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    if (o->mode < UD_WRITE_APPEND || o->mode > UD_WRITE_CHANGESET || !o->table.data ||
        !o->table.length || o->key_count > b->columns || (o->key_count && !o->keys))
        return UD_SCHEMA;
    if (o->mode != UD_WRITE_APPEND && !o->key_count)
        return UD_SCHEMA;
    for (uint32_t k = 0; k < o->key_count; k++) {
        if (o->keys[k] >= b->columns)
            return UD_SCHEMA;
        for (uint32_t j = 0; j < k; j++)
            if (o->keys[j] == o->keys[k])
                return UD_SCHEMA;
    }
    if (o->before &&
        (o->before->ctx != ctx || o->before->rows != b->rows || o->before->columns != b->columns))
        return UD_SCHEMA;
    if (o->mode == UD_WRITE_CHANGESET) {
        if (!o->operations || rows != 1 || o->operations[first] < UD_CHANGE_INSERT ||
            o->operations[first] > UD_CHANGE_UPSERT)
            return UD_SCHEMA;
        if (o->current_column != UINT32_MAX &&
            (o->current_column >= b->columns || b->column[o->current_column].field.type != UD_BOOL))
            return UD_SCHEMA;
    }
    for (uint32_t row = first; row < first + rows; row++)
        for (uint32_t k = 0; k < o->key_count && o->mode != UD_WRITE_APPEND; k++) {
            ud_value value;
            ud_status status = ud_batch_get(b, row, o->keys[k], &value);
            if (status)
                return status;
            if (value.is_null)
                return UD_SCHEMA;
        }
    if (!rows)
        return UD_OK;
    writer w = {.ctx = ctx,
                .text = {.ctx = ctx, .limit = 16u * 1024u * 1024u},
                .statement = out,
                .options = o,
                .batch = b,
                .dialect = dialect,
                .style = style};
    int mode = o->mode, action = 0;
    if (mode == UD_WRITE_CHANGESET) {
        if (rows != 1 || !o->operations)
            return UD_INVALID;
        action = o->operations[first];
        if (action == UD_CHANGE_UPSERT)
            mode = UD_WRITE_UPSERT;
        else if (action == UD_CHANGE_INSERT)
            mode = UD_WRITE_APPEND;
        else if (action == UD_CHANGE_DELETE)
            mode = UD_WRITE_DELETE;
    }
    ud_status s = UD_OK;
    if (mode == UD_WRITE_UPSERT && rows > 1)
        s = last_rows(&w, first, &rows);
    if (!s && mode == UD_WRITE_APPEND)
        s = insert(&w, first, rows, action == UD_CHANGE_INSERT && o->current_column != UINT32_MAX);
    else if (!s && mode == UD_WRITE_UPSERT)
        s = upsert(&w, first, rows);
    else if (!s && mode == UD_WRITE_DELETE) {
        s = put(&w, "DELETE FROM ");
        if (!s)
            s = table(&w);
        if (!s)
            s = put(&w, " WHERE ");
        for (uint32_t row = 0; !s && row < rows; row++) {
            if (row)
                s = put(&w, " OR ");
            if (!s)
                s = put(&w, "(");
            if (!s)
                s = action == UD_CHANGE_DELETE ? preimage(&w, first + row)
                                               : keys_where(&w, first + row, NULL, NULL);
            if (!s)
                s = put(&w, ")");
        }
    } else if (!s && action == UD_CHANGE_UPDATE) {
        s = put(&w, "UPDATE ");
        if (!s)
            s = table(&w);
        if (!s)
            s = put(&w, " SET ");
        for (uint32_t i = 0; !s && i < b->columns; i++) {
            if (i)
                s = put(&w, ",");
            if (!s)
                s = column(&w, i, NULL);
            if (!s)
                s = put(&w, "=");
            if (!s)
                s = parameter(&w, first, i, UD_SQL_AFTER);
        }
        if (!s)
            s = put(&w, " WHERE ");
        if (!s)
            s = preimage(&w, first);
    } else if (!s)
        s = UD_UNSUPPORTED;
    if (!s) {
        out->sql.length = (uint32_t)w.text.size;
        s = ud_bytes_put(&w.text, "", 1);
    }
    if (!s) {
        out->sql.data = (const char *)w.text.data;
        w.text.data = NULL;
    }
    ud_free(w.selection);
    ud_bytes_clear(&w.text);
    if (s)
        ud_sql_statement_clear(out);
    return s;
}
void ud_sql_statement_clear(ud_sql_statement *s) {
    if (!s)
        return;
    ud_free((void *)s->sql.data);
    ud_free(s->parameters);
    memset(s, 0, sizeof(*s));
}

ud_status ud_sql_compile_preimage(ud_context *ctx, int32_t dialect, int32_t style,
                                  const ud_write_options *options, const ud_batch *batch,
                                  uint32_t row, ud_sql_statement *out) {
    if (!ctx || !options || !batch || !out || row >= batch->rows || !options->key_count)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    writer w = {.ctx = ctx,
                .text = {.ctx = ctx, .limit = 16u * 1024u * 1024u},
                .statement = out,
                .options = options,
                .batch = batch,
                .dialect = dialect,
                .style = style};
    ud_status status = put(&w, "SELECT ");
    if (!status)
        status = fields(&w, NULL);
    if (!status)
        status = put(&w, " FROM ");
    if (!status)
        status = table(&w);
    if (!status)
        status = put(&w, " WHERE ");
    for (uint32_t k = 0; !status && k < options->key_count; k++) {
        if (k)
            status = put(&w, " AND ");
        if (!status)
            status = column(&w, options->keys[k], NULL);
        if (!status)
            status = put(&w, "=");
        if (!status)
            status = parameter(&w, row, options->keys[k],
                               options->before ? UD_SQL_BEFORE : UD_SQL_AFTER);
    }
    if (!status && dialect != UD_SQL_SQLITE)
        status = put(&w, " FOR UPDATE");
    if (!status) {
        out->sql.length = (uint32_t)w.text.size;
        status = ud_bytes_put(&w.text, "", 1);
    }
    if (!status) {
        out->sql.data = (const char *)w.text.data;
        w.text.data = NULL;
    }
    ud_bytes_clear(&w.text);
    if (status)
        ud_sql_statement_clear(out);
    return status;
}

ud_status ud_write_validate(const ud_write_options *o, const ud_batch *b) {
    if (!o || !b || o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!b->sealed || !o->table.data || !o->table.length || !o->job.data || !o->job.length ||
        !o->batch_id.data || !o->batch_id.length ||
        (o->source_position.length && !o->source_position.data) || o->mode < UD_WRITE_APPEND ||
        o->mode > UD_WRITE_CHANGESET || o->key_count > b->columns || (o->key_count && !o->keys))
        return UD_INVALID;
    if (o->mode != UD_WRITE_APPEND && !o->key_count)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < o->key_count; i++) {
        if (o->keys[i] >= b->columns)
            return UD_SCHEMA;
        for (uint32_t k = 0; k < i; k++)
            if (o->keys[k] == o->keys[i])
                return UD_SCHEMA;
        for (uint32_t row = 0; row < b->rows && o->mode != UD_WRITE_APPEND; row++) {
            ud_value v;
            UD_TRY(ud_batch_get(b, row, o->keys[i], &v));
            if (v.is_null)
                return UD_SCHEMA;
        }
    }
    if (o->mode == UD_WRITE_CHANGESET) {
        if (!o->operations)
            return UD_SCHEMA;
        if (o->before && (!o->before->sealed || o->before->ctx != b->ctx ||
                          o->before->columns != b->columns || o->before->rows != b->rows))
            return UD_SCHEMA;
        for (uint32_t i = 0; o->before && i < b->columns; i++)
            if (!ud_field_matches(&o->before->column[i].field, &b->column[i].field,
                                  UD_FIELD_VALUE_TYPE))
                return UD_SCHEMA;
        if (o->current_column != UINT32_MAX &&
            (o->current_column >= b->columns || b->column[o->current_column].field.type != UD_BOOL))
            return UD_SCHEMA;
        for (uint32_t row = 0; row < b->rows; row++)
            if (o->operations[row] < UD_CHANGE_INSERT || o->operations[row] > UD_CHANGE_UPSERT)
                return UD_SCHEMA;
    }
    return UD_OK;
}
ud_status ud_write_digest(ud_context *ctx, ud_string tenant, ud_string resource,
                          const ud_write_options *options, const ud_batch *batch, uint8_t out[32]) {
    if (!ctx || !out)
        return UD_INVALID;
    UD_TRY(ud_write_validate(options, batch));
    ud_bytes wire = {.ctx = ctx};
    ud_status s = ud_put_u64(&wire, UD_CONFIG_VERSION);
    ud_string strings[] = {tenant,       resource,          options->table,
                           options->job, options->batch_id, options->source_position};
    for (uint32_t j = 0; !s && j < 6; j++)
        s = ud_put_string(&wire, strings[j].data, strings[j].length);
    if (!s)
        s = ud_put_u64(&wire, (uint64_t)options->mode);
    if (!s)
        s = ud_put_u64(&wire, options->key_count);
    for (uint32_t j = 0; !s && j < options->key_count; j++)
        s = ud_put_u64(&wire, options->keys[j]);
    if (!s && options->mode == UD_WRITE_CHANGESET) {
        if (!options->operations || (options->before && options->before->rows != batch->rows))
            s = UD_SCHEMA;
        if (!s)
            s = ud_put_u64(&wire, options->current_column);
        for (uint32_t row = 0; !s && row < batch->rows; row++)
            s = ud_put_u64(&wire, (uint64_t)options->operations[row]);
        if (!s)
            s = ud_put_u64(&wire, options->before ? 1 : 0);
        if (!s && options->before)
            s = ud_encode_batch(&wire, options->before);
    }
    if (!s)
        s = ud_encode_batch(&wire, batch);
    if (!s)
        ud_hash(wire.data, wire.size, out);
    ud_bytes_clear(&wire);
    return s;
}

void ud_cdc_event_clear(ud_cdc_event *event) {
    if (!event)
        return;
    ud_free((void *)event->position.data);
    ud_free((void *)event->transaction_id.data);
    ud_batch_destroy(event->after);
    ud_batch_destroy(event->before);
    ud_free(event->operations);
    memset(event, 0, sizeof(*event));
}

ud_status ud_sql_compile_lookup(ud_context *ctx, int32_t dialect, int32_t style,
                                ud_string table_name, const ud_batch *reference,
                                const uint32_t *reference_keys, const ud_batch *probe,
                                const uint32_t *keys, uint32_t key_count, uint32_t first,
                                uint32_t rows, ud_sql_statement *out) {
    if (!ctx || !reference || !probe || !out || !reference_keys || !keys || !key_count ||
        first > probe->rows || rows > probe->rows - first)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    if (!rows)
        return UD_OK;
    ud_write_options options = {
        .table = table_name, .keys = reference_keys, .key_count = key_count};
    writer w = {.ctx = ctx,
                .text = {.ctx = ctx, .limit = 16u * 1024u * 1024u},
                .statement = out,
                .options = &options,
                .batch = reference,
                .dialect = dialect,
                .style = style};
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < key_count; i++)
        if (reference_keys[i] >= reference->columns || keys[i] >= probe->columns ||
            !ud_field_matches(&reference->column[reference_keys[i]].field,
                              &probe->column[keys[i]].field, UD_FIELD_VALUE_TYPE))
            status = UD_SCHEMA;
    if (!status)
        status = put(&w, "SELECT ");
    if (!status)
        status = fields(&w, NULL);
    if (!status)
        status = put(&w, " FROM ");
    if (!status)
        status = table(&w);
    if (!status)
        status = put(&w, " WHERE ");
    for (uint32_t row = 0; !status && row < rows; row++) {
        if (row)
            status = put(&w, " OR ");
        if (!status)
            status = put(&w, "(");
        for (uint32_t k = 0; !status && k < key_count; k++) {
            if (k)
                status = put(&w, " AND ");
            if (!status)
                status = column(&w, reference_keys[k], NULL);
            if (!status)
                status = put(&w, "=");
            if (!status)
                status = parameter(&w, first + row, keys[k], UD_SQL_AFTER);
        }
        if (!status)
            status = put(&w, ")");
    }
    if (!status) {
        out->sql.length = (uint32_t)w.text.size;
        status = ud_bytes_put(&w.text, "", 1);
    }
    if (!status) {
        out->sql.data = (const char *)w.text.data;
        w.text.data = NULL;
    }
    ud_bytes_clear(&w.text);
    if (status)
        ud_sql_statement_clear(out);
    return status;
}

ud_status ud_sql_compile_scan(ud_context *ctx, int32_t dialect, ud_string table_name,
                              const ud_batch *schema, ud_string *out) {
    if (!ctx || !schema || !out || !schema->columns || !table_name.length)
        return UD_INVALID;
    *out = (ud_string){0};
    ud_write_options options = {.table = table_name};
    ud_sql_statement statement = {0};
    writer w = {.ctx = ctx,
                .text = {.ctx = ctx},
                .statement = &statement,
                .options = &options,
                .batch = schema,
                .dialect = dialect};
    ud_status status = put(&w, "SELECT ");
    if (!status)
        status = fields(&w, NULL);
    if (!status)
        status = put(&w, " FROM ");
    if (!status)
        status = table(&w);
    if (!status) {
        out->length = (uint32_t)w.text.size;
        status = ud_bytes_put(&w.text, "", 1);
    }
    if (!status) {
        out->data = (char *)w.text.data;
        w.text.data = NULL;
    }
    ud_bytes_clear(&w.text);
    return status;
}
