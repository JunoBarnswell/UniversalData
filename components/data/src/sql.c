#include "../internal.h"
#include <ctype.h>
#include "ud/sql.h"
typedef struct writer {
    ud_context *ctx;
    ud_bytes text;
    ud_sql_statement *statement;
    const ud_write_options *options;
    const ud_batch *batch;
    const ud_batch *parameter_batch;
    int dialect, style;
    uint32_t capacity;
    uint32_t *selection;
    int filter_absent;
    uint32_t presence_row;
} writer;
static int command_word(const char *word,uint32_t length,const char *name) {
    if (length != strlen(name)) return 0;
    for (uint32_t at = 0; at < length; at++) {
        unsigned char c = (unsigned char)word[at]; if (c >= 'a' && c <= 'z') c -= 'a'-'A';
        if (c != (unsigned char)name[at]) return 0;
    }
    return 1;
}
enum { COMMAND_WORD=256, COMMAND_IDENTIFIER, COMMAND_LITERAL, COMMAND_PARAMETER };
typedef struct command_token { int kind; const char *text; uint32_t length; } command_token;
static int token_word(command_token token,const char *word) {
    return token.kind == COMMAND_WORD && command_word(token.text,token.length,word);
}
static ud_status command_next(ud_string sql,uint32_t *position,command_token *out) {
    uint32_t at=*position; *out=(command_token){0};
    while (at < sql.length) {
        unsigned char c=(unsigned char)sql.data[at];
        if (isspace(c)) { at++; continue; }
        if (at+1 < sql.length && c=='-' && sql.data[at+1]=='-') {
            if (at+2 < sql.length && !isspace((unsigned char)sql.data[at+2])) return UD_UNSUPPORTED;
            while (at < sql.length && sql.data[at]!='\n') at++;
            continue;
        }
        if (at+1 < sql.length && c=='/' && sql.data[at+1]=='*') {
            at+=2;
            if (at < sql.length && (sql.data[at]=='!' ||
                (at+1 < sql.length && (sql.data[at]=='M' || sql.data[at]=='m') && sql.data[at+1]=='!'))) return UD_UNSUPPORTED;
            int closed=0;
            while (at+1 < sql.length) {
                if (sql.data[at]=='/' && sql.data[at+1]=='*') return UD_UNSUPPORTED;
                if (sql.data[at]=='*' && sql.data[at+1]=='/') { at+=2; closed=1; break; }
                at++;
            }
            if (!closed) return UD_SCHEMA;
            continue;
        }
        uint32_t start=at++;
        int kind=c;
        if (c=='#') return UD_UNSUPPORTED;
        if (c=='\'' || c=='"' || c=='`' || c=='[') {
            char end=c=='[' ? ']' : (char)c; int closed=0;
            kind=c=='\'' ? COMMAND_LITERAL : COMMAND_IDENTIFIER;
            while (at < sql.length) {
                if (sql.data[at]=='\\') return UD_UNSUPPORTED; /* Do not guess a session's escape mode. */
                if (sql.data[at++]==end) {
                    if (at < sql.length && sql.data[at]==end) { at++; continue; }
                    closed=1; break;
                }
            }
            if (!closed) return UD_SCHEMA;
        } else if ((c==':' || c=='@' || c=='$') && at < sql.length &&
                   (isalnum((unsigned char)sql.data[at]) || sql.data[at]=='_')) {
            kind=COMMAND_PARAMETER;
            while (at < sql.length && (isalnum((unsigned char)sql.data[at]) || sql.data[at]=='_')) at++;
            if (at < sql.length && sql.data[at]=='$') return UD_UNSUPPORTED;
        } else if (c=='$') return UD_UNSUPPORTED;
        else if (isalpha(c) || c=='_' || c>=128) {
            kind=COMMAND_WORD;
            while (at < sql.length && (isalnum((unsigned char)sql.data[at]) || sql.data[at]=='_' ||
                                       (unsigned char)sql.data[at]>=128)) at++;
        }
        *out=(command_token){kind,sql.data+start,at-start}; *position=at; return UD_OK;
    }
    *position=at; return UD_OK;
}
ud_status ud_sql_validate_record_command(ud_string sql) {
    if (!sql.data || !sql.length || sql.length > 16777216 || memchr(sql.data,0,sql.length) ||
        !ud_utf8_valid(sql.data,sql.length)) return UD_INVALID;
    uint32_t depth=0,at=0; int first=1,with=0,dml=0,closed_cte=0,terminated=0;
    static const char *forbidden[]={"BEGIN","COMMIT","ROLLBACK","SAVEPOINT","RELEASE",
        "CALL","EXEC","EXECUTE","DECLARE","CREATE","ALTER","DROP","TRUNCATE","GRANT","REVOKE","LOCK","UNLOCK","OUTFILE","DUMPFILE"};
    for (;;) {
        command_token token; UD_TRY(command_next(sql,&at,&token));
        if (!token.kind) break;
        if (terminated) return UD_UNSUPPORTED;
        if (token.kind==';') { if (depth) return UD_UNSUPPORTED; terminated=1; continue; }
        if (token.kind=='(') { if (++depth > 128) return UD_RESOURCE; }
        else if (token.kind==')') { if (!depth) return UD_SCHEMA; if (!--depth) closed_cte=1; }
        if (token.kind==COMMAND_WORD) {
            for (uint32_t index=0; index<sizeof(forbidden)/sizeof(*forbidden); index++)
                if (token_word(token,forbidden[index])) return UD_UNSUPPORTED;
            int write=token_word(token,"INSERT") || token_word(token,"UPDATE") || token_word(token,"DELETE") || token_word(token,"MERGE");
            if (first) {
                with=token_word(token,"WITH"); dml=write; first=0;
                if (!with && !dml) return UD_UNSUPPORTED;
            } else if (with && !depth && closed_cte && write) dml=1;
        } else if (first) return UD_SCHEMA;
    }
    return dml && !depth ? UD_OK : UD_UNSUPPORTED;
}
static ud_status command_identifier(ud_context *ctx,command_token token,ud_string *out) {
    /* MySQL's double-quote interpretation depends on SQL_MODE. Backticks are
     * unambiguous and decoded here, without parsing identifiers in the driver. */
    if (token.kind!=COMMAND_WORD && (token.kind!=COMMAND_IDENTIFIER || token.text[0]!='`')) return UD_UNSUPPORTED;
    uint32_t first=token.kind==COMMAND_IDENTIFIER,limit=token.length-first;
    char *text=ud_alloc(ctx,(size_t)token.length+1); if (!text) return UD_RESOURCE;
    uint32_t count=0;
    for (uint32_t at=first; at<limit; at++) {
        text[count++]=token.text[at];
        if (first && token.text[at]=='`') at++;
    }
    if (!count || memchr(text,'.',count)) { ud_free(text); return UD_UNSUPPORTED; }
    *out=(ud_string){text,count}; return UD_OK;
}
ud_status ud_sql_record_target(ud_context *ctx,ud_string sql,ud_string *schema,ud_string *table) {
    if (!ctx || !schema || !table) return UD_INVALID;
    *schema=(ud_string){0}; *table=(ud_string){0};
    UD_TRY(ud_sql_validate_record_command(sql));
    uint32_t at=0; command_token token;
    UD_TRY(command_next(sql,&at,&token));
    int insert=token_word(token,"INSERT"),update=token_word(token,"UPDATE"),remove=token_word(token,"DELETE");
    if (!insert && !update && !remove) return UD_UNSUPPORTED;
    UD_TRY(command_next(sql,&at,&token));
    if (insert || remove) {
        if (!token_word(token,insert ? "INTO" : "FROM")) return UD_UNSUPPORTED;
        UD_TRY(command_next(sql,&at,&token));
    }
    UD_TRY(command_identifier(ctx,token,table));
    ud_status status=command_next(sql,&at,&token);
    if (!status && token.kind=='.') {
        *schema=*table; *table=(ud_string){0};
        status=command_next(sql,&at,&token);
        if (!status) status=command_identifier(ctx,token,table);
        if (!status) status=command_next(sql,&at,&token);
    }
    if (!status && ((update && !token_word(token,"SET")) ||
        (remove && token.kind && token.kind!=';' && !token_word(token,"WHERE") && !token_word(token,"ORDER") && !token_word(token,"LIMIT")) ||
        (insert && token.kind!='(' && !token_word(token,"VALUES") && !token_word(token,"VALUE") && !token_word(token,"SET") && !token_word(token,"SELECT")))) status=UD_UNSUPPORTED;
    /* Reject procedural calls in this conservative, provably single-target
     * subset. These tokens are grammar or standard pure scalar functions. */
    static const char *pure[]={"VALUES","VALUE","IN","EXISTS","SELECT","WHERE","AND","OR","NOT","AS",
        "CAST","CONVERT","COALESCE","IFNULL","NULLIF","ABS","CEIL","CEILING","FLOOR","ROUND","TRUNCATE",
        "LOWER","UPPER","TRIM","LTRIM","RTRIM","CONCAT","CONCAT_WS","LENGTH","CHAR_LENGTH","SUBSTRING","REPLACE"};
    command_token previous={0},before_previous={0}; uint32_t depth=0;
    for (; !status && token.kind; ) {
        if (token.kind=='(') {
            if (previous.kind==COMMAND_WORD) {
                int safe=0;
                if (before_previous.kind=='.') status=UD_UNSUPPORTED;
                for (uint32_t index=0; index<sizeof(pure)/sizeof(*pure); index++) safe|=token_word(previous,pure[index]);
                if (!safe) status=UD_UNSUPPORTED;
            }
            depth++;
        } else if (token.kind==')' && depth) depth--;
        if (token_word(token,"SELECT") && insert && !depth) status=UD_UNSUPPORTED;
        before_previous=previous; previous=token;
        if (!status) status=command_next(sql,&at,&token);
    }
    if (status) { ud_free((void *)schema->data); ud_free((void *)table->data); *schema=*table=(ud_string){0}; }
    return status;
}
static int included(const writer *w, uint32_t column) {
    if (!w->filter_absent)
        return 1;
    uint32_t row = w->presence_row + w->batch->row_offset;
    const uint8_t *absence = w->batch->column[column].absence;
    return !absence || !(absence[row / 8] & (1u << (row % 8)));
}
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
    const ud_batch *image = source == UD_SQL_BEFORE ? w->options->before
                            : w->parameter_batch    ? w->parameter_batch
                                                    : w->batch;
    ud_value value;
    if (!image)
        return UD_SCHEMA;
    UD_TRY(ud_batch_get(image, row, col, &value));
    if (value.is_absent)
        return UD_INCOMPATIBLE;
    uint32_t maximum = ud_sql_parameter_limit(w->dialect);
    if (!maximum || w->statement->parameter_count >= maximum)
        return UD_RESOURCE;

    ud_sql_statement *s = w->statement;
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
    uint32_t count = 0;
    for (uint32_t i = 0; i < w->batch->columns; i++) {
        ud_value value;
        UD_TRY(ud_batch_get(o->before, row, i, &value));
        if (value.is_absent)
            continue;
        if (count++)
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
    return count ? UD_OK : UD_SCHEMA;
}
static ud_status fields(writer *w, const char *prefix) {
    uint32_t count = 0;
    for (uint32_t i = 0; i < w->batch->columns; i++) {
        if (!included(w, i))
            continue;
        if (count++)
            UD_TRY(put(w, ","));
        UD_TRY(column(w, i, prefix));
    }
    return UD_OK;
}
static ud_status scan_fields(writer *w, const char *prefix) {
    for (uint32_t i = 0; i < w->batch->columns; i++) {
        if (i)
            UD_TRY(put(w, ","));
        int dictionary = w->batch->column[i].field.dictionary_kind &&
                         (w->dialect == UD_SQL_MYSQL || w->dialect == UD_SQL_MARIADB);
        if (dictionary)
            UD_TRY(put(w, "CAST("));
        UD_TRY(column(w, i, prefix));
        if (dictionary) {
            UD_TRY(put(w, " AS UNSIGNED) AS "));
            UD_TRY(column(w, i, NULL));
        }
    }
    return UD_OK;
}
static ud_status bulk_variable(writer *w, uint32_t index) {
    char text[40];
    int length = snprintf(text, sizeof(text), "@ud_bulk_%u", index);
    return ud_bytes_put(&w->text, text, (size_t)length);
}
static uint32_t row_at(const writer *w, uint32_t first, uint32_t row) {
    return w->selection ? w->selection[row] : first + row;
}
static ud_status tuple(writer *w, uint32_t row) {
    UD_TRY(put(w, "("));
    uint32_t count = 0;
    for (uint32_t i = 0; i < w->batch->columns; i++) {
        if (!included(w, i))
            continue;
        if (count++)
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
        uint32_t count = 0;
        for (uint32_t i = 0; i < w->batch->columns; i++) {
            if (!included(w, i))
                continue;
            if (count++)
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
            uint32_t count = 0;
            for (uint32_t i = 0; i < w->batch->columns; i++) {
                if (!included(w, i))
                    continue;
                if (count++)
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
uint32_t ud_sql_parameter_limit(int32_t dialect) {
    switch (dialect) {
    case UD_SQL_SQLSERVER:
        return 2100;
    case UD_SQL_SQLITE:
        return 32766;
    case UD_SQL_POSTGRESQL:
    case UD_SQL_MYSQL:
    case UD_SQL_MARIADB:
    case UD_SQL_ORACLE:
    case UD_SQL_DB2:
    case UD_SQL_DUCKDB:
        return 65535;
    default:
        return 0;
    }
}
static ud_status write_action(writer *w, uint32_t first, uint32_t rows) {
    const ud_write_options *o = w->options;
    const ud_batch *b = w->batch;
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
    w->filter_absent = action == UD_CHANGE_INSERT;
    w->presence_row = first;
    if (o->mode != UD_WRITE_CHANGESET && ud_batch_has_absent(b))
        return UD_INCOMPATIBLE;
    if (action == UD_CHANGE_UPSERT && ud_batch_has_absent(b))
        for (uint32_t i = 0; i < b->columns; i++) {
            ud_value v;
            UD_TRY(ud_batch_get(b, first, i, &v));
            if (v.is_absent)
                return UD_INCOMPATIBLE;
        }
    ud_status s = UD_OK;
    if (mode == UD_WRITE_UPSERT && rows > 1)
        s = last_rows(w, first, &rows);
    if (!s && action == UD_CHANGE_TRUNCATE) {
        if (!o->allow_truncate || o->current_column != UINT32_MAX)
            s = UD_UNSUPPORTED;
        else {
            s = put(w, "DELETE FROM ");
            if (!s)
                s = table(w);
        }
    } else if (!s && mode == UD_WRITE_APPEND)
        s = insert(w, first, rows, action == UD_CHANGE_INSERT && o->current_column != UINT32_MAX);
    else if (!s && mode == UD_WRITE_UPSERT)
        s = upsert(w, first, rows);
    else if (!s && mode == UD_WRITE_DELETE) {
        s = put(w, "DELETE FROM ");
        if (!s)
            s = table(w);
        if (!s)
            s = put(w, " WHERE ");
        for (uint32_t row = 0; !s && row < rows; row++) {
            if (row)
                s = put(w, " OR ");
            if (!s)
                s = put(w, "(");
            if (!s)
                s = action == UD_CHANGE_DELETE ? preimage(w, first + row)
                                               : keys_where(w, first + row, NULL, NULL);
            if (!s)
                s = put(w, ")");
        }
    } else if (!s && action == UD_CHANGE_UPDATE) {
        s = put(w, "UPDATE ");
        if (!s)
            s = table(w);
        if (!s)
            s = put(w, " SET ");
        uint32_t count = 0;
        for (uint32_t i = 0; !s && i < b->columns; i++) {
            ud_value value;
            s = ud_batch_get(b, first, i, &value);
            if (s || value.is_absent)
                continue;
            if (count++)
                s = put(w, ",");
            if (!s)
                s = column(w, i, NULL);
            if (!s)
                s = put(w, "=");
            if (!s)
                s = parameter(w, first, i, UD_SQL_AFTER);
        }
        if (!s && !count)
            s = UD_SCHEMA;
        if (!s)
            s = put(w, " WHERE ");
        if (!s)
            s = preimage(w, first);
    } else if (!s)
        s = UD_UNSUPPORTED;
    return s;
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
        if (!o->operations || !rows || (rows != 1 && dialect != UD_SQL_SQLSERVER))
            return UD_SCHEMA;
        for (uint32_t row = first; row < first + rows; row++)
            if (o->operations[row] < UD_CHANGE_INSERT || o->operations[row] > UD_CHANGE_TRUNCATE)
                return UD_SCHEMA;
        if (o->current_column != UINT32_MAX &&
            (o->current_column >= b->columns || b->column[o->current_column].field.type != UD_BOOL))
            return UD_SCHEMA;
    }
    for (uint32_t row = first; row < first + rows; row++)
        for (uint32_t k = 0; k < o->key_count && o->mode != UD_WRITE_APPEND; k++) {
            if (o->mode == UD_WRITE_CHANGESET && o->operations[row] == UD_CHANGE_TRUNCATE)
                continue;
            ud_value value;
            ud_status status = ud_batch_get(b, row, o->keys[k], &value);
            if (status)
                return status;
            if (value.is_absent && o->mode == UD_WRITE_CHANGESET && o->before && o->operations &&
                o->operations[row] == UD_CHANGE_UPDATE) {
                status = ud_batch_get(o->before, row, o->keys[k], &value);
                if (status)
                    return status;
            }
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
    ud_status s = UD_OK;
    if (o->mode == UD_WRITE_CHANGESET && dialect == UD_SQL_SQLSERVER) {
        /* Keep every DML and its row-count assertion in source order, even
           for repeated keys and primary-key changes in the same transaction. */
        for (uint32_t row = first; !s && row < first + rows; row++) {
            s = write_action(&w, row, 1);
            if (!s)
                s = put(&w, ";");
            if (!s && o->operations[row] != UD_CHANGE_UPSERT &&
                o->operations[row] != UD_CHANGE_TRUNCATE)
                s = put(&w, "IF @@ROWCOUNT<>1 THROW 51079,N'changeset preimage conflict',1;");
        }
    } else
        s = write_action(&w, first, rows);
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
        status = scan_fields(&w, NULL);
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
        o->mode > UD_WRITE_CHANGESET || o->load_mode < UD_LOAD_AUTO ||
        o->load_mode > UD_LOAD_NATIVE || o->key_count > b->columns || (o->key_count && !o->keys))
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
            if (o->mode == UD_WRITE_CHANGESET && o->operations &&
                o->operations[row] == UD_CHANGE_TRUNCATE)
                continue;
            ud_value v;
            UD_TRY(ud_batch_get(b, row, o->keys[i], &v));
            if (v.is_absent && o->mode == UD_WRITE_CHANGESET && o->before && o->operations &&
                o->operations[row] == UD_CHANGE_UPDATE)
                UD_TRY(ud_batch_get(o->before, row, o->keys[i], &v));
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
            if (o->operations[row] < UD_CHANGE_INSERT || o->operations[row] > UD_CHANGE_TRUNCATE ||
                (o->operations[row] == UD_CHANGE_TRUNCATE && !o->allow_truncate))
                return UD_SCHEMA;
        for (uint32_t row = 0; o->before && row < b->rows; row++)
            if (o->operations[row] == UD_CHANGE_UPDATE || o->operations[row] == UD_CHANGE_DELETE)
                for (uint32_t key = 0; key < o->key_count; key++) {
                    ud_value value;
                    UD_TRY(ud_batch_get(o->before, row, o->keys[key], &value));
                    if (value.is_null)
                        return UD_SCHEMA;
                }
    } else if (ud_batch_has_absent(b)) {
        return UD_INCOMPATIBLE;
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
        if (!s)
            s = ud_put_u64(&wire, (uint64_t)options->allow_truncate);
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

void ud_sql_lookup_ordinal_name(const ud_batch *reference, char name[48]) {
    uint32_t suffix = 0;
    strcpy(name, "__ud_lookup_ordinal");
    for (uint32_t i = 0; i < reference->columns;) {
        const ud_field *field = &reference->column[i++].field;
        if (strlen(name) == field->name_length && !memcmp(name, field->name, field->name_length)) {
            snprintf(name, 48, "__ud_lookup_ordinal_%u", ++suffix);
            i = 0;
        }
    }
}

ud_status ud_sql_compile_lookup(ud_context *ctx, int32_t dialect, int32_t style,
                                ud_string table_name, const ud_batch *reference,
                                const uint32_t *reference_keys, const ud_batch *probe,
                                const uint32_t *keys, uint32_t key_count, uint32_t first,
                                uint32_t rows, ud_sql_statement *out) {
    if (!ctx || !reference || !probe || !out || !reference_keys || !keys || !key_count ||
        !table_name.data || !table_name.length ||
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
                .parameter_batch = probe,
                .dialect = dialect,
                .style = style};
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < key_count; i++)
        if (reference_keys[i] >= reference->columns || keys[i] >= probe->columns ||
            !ud_field_matches(&reference->column[reference_keys[i]].field,
                              &probe->column[keys[i]].field, UD_FIELD_VALUE_TYPE))
            status = UD_SCHEMA;
    /* The ordinal identifies the probe under the reference column's actual
       database collation. It is local to this bounded parameter request. */
    int selects = dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB ||
                  dialect == UD_SQL_ORACLE || dialect == UD_SQL_SQLSERVER;
    int collides = table_name.length == 8;
    for (uint32_t i = 0; collides && i < 8; i++)
        collides = tolower((unsigned char)table_name.data[i]) == "ud_probe"[i];
    const char *probe_name = collides ? "ud_probe_1" : "ud_probe";
    if (!status)
        status = put(&w, "WITH ");
    if (!status)
        status = put(&w, probe_name);
    if (!status)
        status = put(&w, "(ord");
    char generated[48];
    for (uint32_t k = 0; !status && k < key_count; k++) {
        snprintf(generated, sizeof(generated), ",k%u", k);
        status = put(&w, generated);
    }
    if (!status)
        status = put(&w, selects ? ") AS (" : ") AS (VALUES ");
    for (uint32_t row = 0; !status && row < rows; row++) {
        if (row)
            status = put(&w, selects ? " UNION ALL " : ",");
        snprintf(generated, sizeof(generated), selects ? "SELECT %u" : "(%u", row);
        if (!status)
            status = put(&w, generated);
        for (uint32_t k = 0; !status && k < key_count; k++) {
            status = put(&w, ",");
            if (!status)
                status = parameter(&w, first + row, keys[k], UD_SQL_AFTER);
        }
        if (!status && !selects)
            status = put(&w, ")");
        if (!status && dialect == UD_SQL_ORACLE)
            status = put(&w, " FROM dual");
    }
    if (!status)
        status = put(&w, ") SELECT ");
    if (!status)
        status = scan_fields(&w, "r.");
    if (!status) {
        const char *cast = dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB ? "SIGNED"
                           : dialect == UD_SQL_ORACLE ? "NUMBER(19)" : "BIGINT";
        snprintf(generated, sizeof(generated), ",CAST(p.ord AS %s) AS ", cast);
        status = put(&w, generated);
    }
    if (!status) {
        ud_sql_lookup_ordinal_name(reference, generated);
        status = quote(&w, (ud_string){generated, (uint32_t)strlen(generated)});
    }
    if (!status)
        status = put(&w, " FROM ");
    if (!status)
        status = table(&w);
    if (!status)
        status = put(&w, " r JOIN ");
    if (!status)
        status = put(&w, probe_name);
    if (!status)
        status = put(&w, " p ON ");
    for (uint32_t k = 0; !status && k < key_count; k++) {
        if (k)
            status = put(&w, " AND ");
        if (!status)
            status = column(&w, reference_keys[k], "r.");
        if (!status) {
            snprintf(generated, sizeof(generated), "=p.k%u", k);
            status = put(&w, generated);
        }
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
        status = scan_fields(&w, NULL);
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

/* Transport grammar is compiled by the same Native SQL owner as parameterized writes. */
ud_status ud_sql_compile_bulk(ud_context *ctx, int32_t dialect, const ud_write_options *options,
                              const ud_batch *batch, ud_string *out) {
    if (!ctx || !out)
        return UD_INVALID;
    *out = (ud_string){0};
    UD_TRY(ud_write_validate(options, batch));
    if (options->mode != UD_WRITE_APPEND ||
        (dialect != UD_SQL_POSTGRESQL && dialect != UD_SQL_MYSQL && dialect != UD_SQL_MARIADB))
        return UD_UNSUPPORTED;
    writer w = {
        .ctx = ctx, .text = {.ctx = ctx}, .options = options, .batch = batch, .dialect = dialect};
    ud_status s = put(&w, dialect == UD_SQL_POSTGRESQL
                              ? "COPY "
                              : "LOAD DATA LOCAL INFILE 'ud-native-batch' INTO TABLE ");
    if (!s)
        s = table(&w);
    if (!s && dialect != UD_SQL_POSTGRESQL)
        s = put(&w, " CHARACTER SET binary FIELDS TERMINATED BY X'09' ESCAPED BY X'5C' LINES "
                    "TERMINATED BY X'0A' ");
    if (!s)
        s = put(&w, "(");
    if (!s && dialect == UD_SQL_POSTGRESQL)
        s = fields(&w, NULL);
    else if (!s)
        for (uint32_t index = 0; !s && index < batch->columns; index++) {
            if (index)
                s = put(&w, ",");
            if (!s)
                s = batch->column[index].field.dictionary_kind ? bulk_variable(&w, index)
                                                               : column(&w, index, NULL);
        }
    if (!s)
        s = put(&w, ")");
    if (!s && dialect == UD_SQL_POSTGRESQL)
        s = put(&w, " FROM STDIN WITH (FORMAT text)");
    uint32_t dictionaries = 0;
    for (uint32_t index = 0; !s && dialect != UD_SQL_POSTGRESQL && index < batch->columns;
         index++) {
        if (!batch->column[index].field.dictionary_kind)
            continue;
        s = put(&w, dictionaries++ ? "," : " SET ");
        if (!s)
            s = column(&w, index, NULL);
        if (!s)
            s = put(&w, "=CAST(");
        if (!s)
            s = bulk_variable(&w, index);
        if (!s)
            s = put(&w, " AS UNSIGNED)");
    }
    if (!s)
        s = ud_bytes_put(&w.text, "", 1);
    if (!s && w.text.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s) {
        *out = (ud_string){(char *)w.text.data, (uint32_t)w.text.size - 1};
        w.text.data = NULL;
    }
    ud_bytes_clear(&w.text);
    return s;
}
ud_status ud_sql_compile_bulk_release(ud_context *ctx, int32_t dialect, const ud_batch *batch,
                                      ud_string *out) {
    if (!ctx || !batch || !out)
        return UD_INVALID;
    *out = (ud_string){0};
    if (dialect != UD_SQL_MYSQL && dialect != UD_SQL_MARIADB)
        return UD_UNSUPPORTED;
    writer w = {.ctx = ctx, .text = {.ctx = ctx}, .batch = batch, .dialect = dialect};
    ud_status status = UD_OK;
    uint32_t count = 0;
    for (uint32_t index = 0; !status && index < batch->columns; index++)
        if (batch->column[index].field.dictionary_kind) {
            status = put(&w, count++ ? "," : "SET ");
            if (!status)
                status = bulk_variable(&w, index);
            if (!status)
                status = put(&w, "=NULL");
        }
    if (!status && count) {
        status = ud_bytes_put(&w.text, "", 1);
        if (!status) {
            *out = (ud_string){(const char *)w.text.data, (uint32_t)w.text.size - 1};
            w.text.data = NULL;
        }
    }
    ud_bytes_clear(&w.text);
    return status;
}
ud_status ud_sql_table_identifier(ud_context *ctx, int32_t dialect, ud_string name,
                                  ud_string *out) {
    if (!ctx || !out || !name.data || !name.length || memchr(name.data, 0, name.length) ||
        dialect < UD_SQL_CANONICAL || dialect > UD_SQL_DUCKDB)
        return UD_INVALID;
    *out = (ud_string){0};
    ud_write_options options = {.table = name};
    writer w = {.ctx = ctx, .text = {.ctx = ctx}, .options = &options, .dialect = dialect};
    ud_status s = table(&w);
    if (!s)
        s = ud_bytes_put(&w.text, "", 1);
    if (!s && w.text.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s) {
        *out = (ud_string){(char *)w.text.data, (uint32_t)w.text.size - 1};
        w.text.data = NULL;
    }
    ud_bytes_clear(&w.text);
    return s;
}
ud_status ud_sql_compile_stage(ud_context *ctx, int32_t dialect, const ud_write_options *options,
                               const ud_batch *batch, ud_string stage, int32_t phase,
                               ud_string *out) {
    if (!ctx || !options || !batch || !out)
        return UD_INVALID;
    *out = (ud_string){0};
    if (dialect != UD_SQL_SQLSERVER && dialect != UD_SQL_ORACLE)
        return UD_UNSUPPORTED;
    if (phase < UD_SQL_STAGE_CREATE || phase > UD_SQL_STAGE_DROP || !stage.length ||
        memchr(stage.data, '.', stage.length))
        return UD_SCHEMA;
    UD_TRY(ud_write_validate(options, batch));
    if (options->mode != UD_WRITE_APPEND || ud_batch_has_absent(batch))
        return UD_UNSUPPORTED;
    writer w = {
        .ctx = ctx, .text = {.ctx = ctx}, .options = options, .batch = batch, .dialect = dialect};
    ud_status s = UD_OK;
    if (phase == UD_SQL_STAGE_DROP) {
        s = put(&w, "DROP TABLE ");
        if (!s)
            s = quote(&w, stage);
    } else if (phase == UD_SQL_STAGE_CREATE && dialect == UD_SQL_SQLSERVER) {
        s = put(&w, "SELECT TOP(0) ");
        if (!s)
            s = fields(&w, "ud_source.");
        if (!s)
            s = put(&w, " INTO ");
        if (!s)
            s = quote(&w, stage);
        if (!s)
            s = put(&w, " FROM ");
        if (!s)
            s = table(&w);
        if (!s)
            s = put(&w, " AS ud_source CROSS JOIN (SELECT 1 AS seed) AS ud_seed");
    } else if (phase == UD_SQL_STAGE_CREATE) {
        s = put(&w, "CREATE TABLE ");
        if (!s)
            s = quote(&w, stage);
        if (!s)
            s = put(&w, " AS SELECT ");
        if (!s)
            s = fields(&w, NULL);
        if (!s)
            s = put(&w, " FROM ");
        if (!s)
            s = table(&w);
        if (!s)
            s = put(&w, " WHERE 1=0");
    } else {
        s = put(&w, "INSERT INTO ");
        if (!s)
            s = table(&w);
        if (!s)
            s = put(&w, "(");
        if (!s)
            s = fields(&w, NULL);
        if (!s)
            s = put(&w, ") SELECT ");
        if (!s)
            s = fields(&w, NULL);
        if (!s)
            s = put(&w, " FROM ");
        if (!s)
            s = quote(&w, stage);
    }
    if (!s)
        s = ud_bytes_put(&w.text, "", 1);
    if (!s && w.text.size > UINT32_MAX)
        s = UD_RESOURCE;
    if (!s) {
        *out = (ud_string){(const char *)w.text.data, (uint32_t)w.text.size - 1};
        w.text.data = NULL;
    }
    ud_bytes_clear(&w.text);
    return s;
}
ud_status ud_sql_compile_savepoint(ud_context *ctx, int32_t dialect, ud_string name, int32_t phase,
                                   ud_string *out) {
    if (!ctx || !out)
        return UD_INVALID;
    *out = (ud_string){0};
    if (!name.data || !name.length || name.length > 64 || phase < UD_SAVEPOINT_BEGIN ||
        phase > UD_SAVEPOINT_RELEASE)
        return UD_SCHEMA;
    if (dialect == UD_SQL_CANONICAL)
        return UD_UNSUPPORTED;
    if (phase == UD_SAVEPOINT_RELEASE && (dialect == UD_SQL_SQLSERVER || dialect == UD_SQL_ORACLE))
        return UD_OK; /* These engines retire savepoints at transaction completion. */
    writer w = {.ctx = ctx, .text = {.ctx = ctx}, .dialect = dialect};
    const char *operation = phase == UD_SAVEPOINT_BEGIN
                                ? (dialect == UD_SQL_SQLSERVER ? "SAVE TRANSACTION " : "SAVEPOINT ")
                            : phase == UD_SAVEPOINT_RELEASE ? "RELEASE SAVEPOINT "
                            : dialect == UD_SQL_SQLSERVER   ? "ROLLBACK TRANSACTION "
                                                            : "ROLLBACK TO SAVEPOINT ";
    ud_status status = put(&w, operation);
    if (!status)
        status = quote(&w, name);
    if (!status)
        status = ud_bytes_put(&w.text, "", 1);
    if (!status) {
        *out = (ud_string){(const char *)w.text.data, (uint32_t)w.text.size - 1};
        w.text.data = NULL;
    }
    ud_bytes_clear(&w.text);
    return status;
}
