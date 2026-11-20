#include "internal.h"

enum { parameter_limit = 16 };
typedef struct catalog_parameter {
    const char *data;
    int length, format;
    char integer[24];
    char *owned;
} catalog_parameter;

struct catalog_statement {
    ud_catalog *catalog;
    sqlite3_stmt *sqlite;
#ifdef UD_WITH_POSTGRESQL_CATALOG
    PGresult *result;
    char *sql;
    catalog_parameter parameters[parameter_limit];
    Oid types[parameter_limit];
    int count, row;
    unsigned char *decoded;
    size_t decoded_length;
    int decoded_column;
#endif
};

ud_status catalog_sqlite_status(int rc) {
    switch (rc & 255) {
    case SQLITE_OK: case SQLITE_ROW: case SQLITE_DONE: return UD_OK;
    case SQLITE_BUSY: case SQLITE_LOCKED: return UD_BUSY;
    case SQLITE_NOMEM: case SQLITE_FULL: case SQLITE_TOOBIG: return UD_RESOURCE;
    case SQLITE_CORRUPT: case SQLITE_NOTADB: case SQLITE_CONSTRAINT: return UD_CORRUPT;
    case SQLITE_PERM: case SQLITE_AUTH: case SQLITE_READONLY: return UD_AUTH;
    default: return UD_IO;
    }
}

ud_status ud_catalog_sql(ud_catalog *catalog, const char *sql) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (catalog->postgresql) {
        PGresult *result = PQexec(catalog->postgresql, sql);
        ud_status status = catalog_postgresql_status(catalog->postgresql, result);
        PQclear(result);
        return status;
    }
#endif
    return catalog_sqlite_status(sqlite3_exec(catalog->database, sql, NULL, NULL, NULL));
}

ud_status catalog_prepare(ud_catalog *catalog, const char *sql, catalog_statement **out) {
    *out = NULL;
    catalog_statement *statement = ud_alloc(catalog->ctx, sizeof(*statement));
    if (!statement)
        return UD_RESOURCE;
    statement->catalog = catalog;
    ud_status status = UD_OK;
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (catalog->postgresql) {
        statement->sql = ud_strdup_n(catalog->ctx, sql, strlen(sql));
        if (!statement->sql)
            status = UD_RESOURCE;
        /* Catalog SQL uses numbered bind positions. No user text is part of SQL. */
        for (char *at = statement->sql; at && *at; at++)
            if (*at == '?' && at[1] >= '0' && at[1] <= '9')
                *at = '$';
        statement->row = -1;
        statement->decoded_column = -1;
    } else
#endif
        status = catalog_sqlite_status(sqlite3_prepare_v2(catalog->database, sql, -1, &statement->sqlite, NULL));
    if (status)
        catalog_finalize(statement);
    else
        *out = statement;
    return status;
}

#ifdef UD_WITH_POSTGRESQL_CATALOG
static catalog_parameter *parameter(catalog_statement *statement, int at, Oid type) {
    if (at < 1 || at > parameter_limit)
        return NULL;
    if (at > statement->count)
        statement->count = at;
    statement->types[at - 1] = type;
    return &statement->parameters[at - 1];
}
#endif

ud_status catalog_text(catalog_statement *statement, int at, ud_string text) {
    const char *data = text.data ? text.data : "";
    if (text.length > INT_MAX)
        return UD_RESOURCE;
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (!statement->sqlite) {
        catalog_parameter *value = parameter(statement, at, 0);
        if (!value || value->data)
            return UD_INVALID;
        value->owned = ud_strdup_n(statement->catalog->ctx, data, text.length);
        value->data = value->owned;
        return value->data ? UD_OK : UD_RESOURCE;
    }
#endif
    return catalog_sqlite_status(sqlite3_bind_text(statement->sqlite, at, data, (int)text.length, SQLITE_TRANSIENT));
}

ud_status catalog_int(catalog_statement *statement, int at, int64_t number) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (!statement->sqlite) {
        catalog_parameter *value = parameter(statement, at, 20);
        if (!value || value->data)
            return UD_INVALID;
        snprintf(value->integer, sizeof(value->integer), "%lld", (long long)number);
        value->data = value->integer;
        return UD_OK;
    }
#endif
    return catalog_sqlite_status(sqlite3_bind_int64(statement->sqlite, at, number));
}

ud_status catalog_blob(catalog_statement *statement, int at, const void *bytes, int length) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (!statement->sqlite) {
        catalog_parameter *value = parameter(statement, at, 17);
        if (!value || value->data)
            return UD_INVALID;
        value->data = bytes;
        value->length = length;
        value->format = 1;
        return UD_OK;
    }
#endif
    return catalog_sqlite_status(sqlite3_bind_blob(statement->sqlite, at, bytes, length, SQLITE_TRANSIENT));
}

ud_status catalog_step(catalog_statement *statement) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (!statement->sqlite) {
        if (!statement->result) {
            const char *values[parameter_limit];
            int lengths[parameter_limit], formats[parameter_limit];
            for (int i = 0; i < statement->count; i++) {
                values[i] = statement->parameters[i].data;
                lengths[i] = statement->parameters[i].length;
                formats[i] = statement->parameters[i].format;
            }
            statement->result = PQexecParams(statement->catalog->postgresql, statement->sql,
                statement->count, statement->types, values, lengths, formats, 0);
            UD_TRY(catalog_postgresql_status(statement->catalog->postgresql, statement->result));
            const char *count = PQcmdTuples(statement->result);
            statement->catalog->changed = *count ? strtoull(count, NULL, 10) : 0;
        }
        if (statement->decoded)
            PQfreemem(statement->decoded);
        statement->decoded = NULL;
        statement->decoded_column = -1;
        return ++statement->row < PQntuples(statement->result) ? UD_OK : UD_END;
    }
#endif
    int rc = sqlite3_step(statement->sqlite);
    statement->catalog->changed = (uint64_t)sqlite3_changes64(statement->catalog->database);
    return rc == SQLITE_DONE ? UD_END : catalog_sqlite_status(rc);
}

ud_status catalog_execute(catalog_statement *statement) {
    ud_status status = catalog_step(statement);
    return status == UD_END ? UD_OK : status;
}

const char *catalog_column_text(catalog_statement *statement, int column) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (!statement->sqlite)
        return PQgetisnull(statement->result, statement->row, column) ? NULL :
            PQgetvalue(statement->result, statement->row, column);
#endif
    return (const char *)sqlite3_column_text(statement->sqlite, column);
}

const void *catalog_column_blob(catalog_statement *statement, int column) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (!statement->sqlite) {
        if (statement->decoded_column != column) {
            if (statement->decoded)
                PQfreemem(statement->decoded);
            const char *encoded = catalog_column_text(statement, column);
            statement->decoded = encoded ? PQunescapeBytea((const unsigned char *)encoded, &statement->decoded_length) : NULL;
            statement->decoded_column = column;
        }
        return statement->decoded;
    }
#endif
    return sqlite3_column_blob(statement->sqlite, column);
}

int catalog_column_bytes(catalog_statement *statement, int column) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (!statement->sqlite) {
        if (PQftype(statement->result, column) == 17)
            return catalog_column_blob(statement, column) ? (int)statement->decoded_length : 0;
        return PQgetlength(statement->result, statement->row, column);
    }
#endif
    return sqlite3_column_bytes(statement->sqlite, column);
}

int64_t catalog_column_int(catalog_statement *statement, int column) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (!statement->sqlite) {
        const char *text = catalog_column_text(statement, column);
        return text ? strtoll(text, NULL, 10) : 0;
    }
#endif
    return sqlite3_column_int64(statement->sqlite, column);
}

void catalog_finalize(catalog_statement *statement) {
    if (!statement)
        return;
    sqlite3_finalize(statement->sqlite);
#ifdef UD_WITH_POSTGRESQL_CATALOG
    PQclear(statement->result);
    ud_free(statement->sql);
    if (statement->decoded)
        PQfreemem(statement->decoded);
    for (int i = 0; i < statement->count; i++)
        ud_free(statement->parameters[i].owned);
#endif
    ud_free(statement);
}
