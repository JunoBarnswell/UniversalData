#include "common.h"
#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
typedef struct {
    ud_host_api host;
    SQLHENV env;
    SQLHDBC db;
    SQLHSTMT read;
    int transactions, failed;
} odbc_state;
static ud_status diagnostic(odbc_state *p, SQLSMALLINT type, SQLHANDLE handle) {
    SQLCHAR state[6] = {0}, message[2] = {0};
    SQLINTEGER native = 0;
    SQLSMALLINT length = 0;
    SQLGetDiagRecA(type, handle, 1, state, &native, message, 1, &length);
    char text[96];
    snprintf(text, sizeof(text), "ODBC failure SQLSTATE %.5s native %ld", (const char *)state,
             (long)native);
    return p->host.report(p->host.context, UD_IO, text); /* Never echo connection strings. */
}
static SQLWCHAR *to_wide(const ud_host_api *h, const char *s, uint32_t n, uint32_t *units) {
    *units = 0;
    if (n > INT32_MAX || (n && memchr(s, 0, n)))
        return NULL;
    SQLWCHAR *w = h->allocate(h->context, ((size_t)n + 1) * sizeof(SQLWCHAR));
    if (!w)
        return NULL;
    for (uint32_t i = 0; i < n;) {
        uint32_t cp;
        unsigned char a = (unsigned char)s[i++];
        unsigned more = 0;
        if (a < 128)
            cp = a;
        else if (a >= 0xc2 && a <= 0xdf) {
            cp = a & 31;
            more = 1;
        } else if (a >= 0xe0 && a <= 0xef) {
            cp = a & 15;
            more = 2;
        } else if (a >= 0xf0 && a <= 0xf4) {
            cp = a & 7;
            more = 3;
        } else
            goto invalid;
        if (more > n - i)
            goto invalid;
        unsigned count = more;
        while (more--) {
            a = (unsigned char)s[i++];
            if ((a & 0xc0) != 0x80)
                goto invalid;
            cp = (cp << 6) | (a & 63);
        }
        if ((count == 2 && cp < 0x800) || (count == 3 && cp < 0x10000) || cp > 0x10ffff ||
            (cp >= 0xd800 && cp <= 0xdfff))
            goto invalid;
        if (sizeof(SQLWCHAR) == 2 && cp > 0xffff) {
            cp -= 0x10000;
            w[(*units)++] = (SQLWCHAR)(0xd800 + (cp >> 10));
            w[(*units)++] = (SQLWCHAR)(0xdc00 + (cp & 1023));
        } else
            w[(*units)++] = (SQLWCHAR)cp;
    }
    return w;
invalid:
    h->deallocate(w);
    return NULL;
}
static char *from_wide(const ud_host_api *h, const SQLWCHAR *w, uint32_t n, uint32_t *bytes) {
    *bytes = 0;
    char *s = h->allocate(h->context, (size_t)n * 4 + 1);
    if (!s)
        return NULL;
    for (uint32_t i = 0; i < n; i++) {
        uint32_t cp = w[i];
        if (sizeof(SQLWCHAR) == 2 && cp >= 0xd800 && cp <= 0xdbff) {
            if (i + 1 == n || w[i + 1] < 0xdc00 || w[i + 1] > 0xdfff)
                goto invalid;
            cp = 0x10000 + ((cp - 0xd800) << 10) + (w[++i] - 0xdc00);
        } else if (cp >= 0xd800 && cp <= 0xdfff)
            goto invalid;
        if (cp > 0x10ffff)
            goto invalid;
        if (cp < 0x80)
            s[(*bytes)++] = (char)cp;
        else if (cp < 0x800) {
            s[(*bytes)++] = (char)(0xc0 | (cp >> 6));
            s[(*bytes)++] = (char)(0x80 | (cp & 63));
        } else if (cp < 0x10000) {
            s[(*bytes)++] = (char)(0xe0 | (cp >> 12));
            s[(*bytes)++] = (char)(0x80 | ((cp >> 6) & 63));
            s[(*bytes)++] = (char)(0x80 | (cp & 63));
        } else {
            s[(*bytes)++] = (char)(0xf0 | (cp >> 18));
            s[(*bytes)++] = (char)(0x80 | ((cp >> 12) & 63));
            s[(*bytes)++] = (char)(0x80 | ((cp >> 6) & 63));
            s[(*bytes)++] = (char)(0x80 | (cp & 63));
        }
    }
    return s;
invalid:
    h->deallocate(s);
    return NULL;
}
static void destroy(void *ptr) {
    odbc_state *p = ptr;
    if (!p)
        return;
    if (p->read)
        SQLFreeHandle(SQL_HANDLE_STMT, p->read);
    if (p->db) {
        SQLDisconnect(p->db);
        SQLFreeHandle(SQL_HANDLE_DBC, p->db);
    }
    if (p->env)
        SQLFreeHandle(SQL_HANDLE_ENV, p->env);
    p->host.deallocate(p);
}
static ud_status initialize(const ud_host_api *h, const char *config, uint32_t n, void **out) {
    *out = NULL;
    odbc_state *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    uint32_t units;
    SQLWCHAR *connection = to_wide(h, config, n, &units);
    if (!connection || !units || units > INT16_MAX) {
        h->deallocate(connection);
        destroy(p);
        return UD_INVALID;
    }
    ud_status s = UD_OK;
    if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &p->env)))
        s = UD_IO;
    if (!s &&
        !SQL_SUCCEEDED(SQLSetEnvAttr(p->env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0)))
        s = diagnostic(p, SQL_HANDLE_ENV, p->env);
    if (!s && !SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, p->env, &p->db)))
        s = UD_IO;
    if (!s && !SQL_SUCCEEDED(SQLDriverConnectW(p->db, NULL, connection, (SQLSMALLINT)units, NULL, 0,
                                               NULL, SQL_DRIVER_NOPROMPT)))
        s = diagnostic(p, SQL_HANDLE_DBC, p->db);
    h->deallocate(connection);
    SQLUSMALLINT capable = SQL_TC_NONE;
    if (!s && !SQL_SUCCEEDED(SQLGetInfo(p->db, SQL_TXN_CAPABLE, &capable, sizeof(capable), NULL)))
        s = diagnostic(p, SQL_HANDLE_DBC, p->db);
    if (s) {
        destroy(p);
        return s;
    }
    p->transactions = capable != SQL_TC_NONE;
    *out = p;
    return UD_OK;
}
static int compatible(ud_type t, SQLSMALLINT sql) {
    if (t == UD_BOOL)
        return sql == SQL_BIT;
    if (t == UD_INT64)
        return sql == SQL_TINYINT || sql == SQL_SMALLINT || sql == SQL_INTEGER || sql == SQL_BIGINT;
    if (t == UD_FLOAT64)
        return sql == SQL_REAL || sql == SQL_FLOAT || sql == SQL_DOUBLE;
    return sql == SQL_CHAR || sql == SQL_VARCHAR || sql == SQL_LONGVARCHAR || sql == SQL_WCHAR ||
           sql == SQL_WVARCHAR || sql == SQL_WLONGVARCHAR;
}
static ud_status read_batch(odbc_state *p, const ud_batch *schema, const char *sql, uint32_t n,
                            ud_batch **out) {
    ud_host_api *h = &p->host;
    if (!schema || h->columns(schema) > INT16_MAX || (p->read && n) || (!p->read && !n))
        return UD_INVALID;
    ud_status s = UD_OK;
    if (!p->read) {
        uint32_t units;
        SQLWCHAR *wide = to_wide(h, sql, n, &units);
        if (!wide)
            return UD_INVALID;
        if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, p->db, &p->read)))
            s = UD_IO;
        if (!s && !SQL_SUCCEEDED(SQLExecDirectW(p->read, wide, (SQLINTEGER)units)))
            s = diagnostic(p, SQL_HANDLE_STMT, p->read);
        h->deallocate(wide);
    }
    uint32_t columns = h->columns(schema);
    SQLSMALLINT actual = 0;
    if (!s &&
        (!SQL_SUCCEEDED(SQLNumResultCols(p->read, &actual)) || actual != (SQLSMALLINT)columns))
        s = UD_SCHEMA;
    ud_field *fields = NULL;
    if (!s)
        s = plugin_fields(h, schema, &fields);
    for (uint32_t i = 0; !s && i < columns; i++) {
        SQLWCHAR name[256];
        SQLSMALLINT length, type, scale, nullable;
        SQLULEN size;
        SQLRETURN rc = SQLDescribeColW(p->read, (SQLUSMALLINT)(i + 1), name, 256, &length, &type,
                                       &size, &scale, &nullable);
        if (rc != SQL_SUCCESS || length < 0 || length >= 256 || !compatible(fields[i].type, type)) {
            s = UD_SCHEMA;
            break;
        }
        uint32_t bytes;
        char *utf8 = from_wide(h, name, (uint32_t)length, &bytes);
        if (!utf8)
            s = UD_PARSE;
        else if (bytes != fields[i].name_length || memcmp(utf8, fields[i].name, bytes))
            s = UD_SCHEMA;
        h->deallocate(utf8);
    }
    if (!s)
        s = h->batch_create(h->context, fields, columns, 4096, out);
    ud_value *v = h->allocate(h->context, columns * sizeof(*v));
    if (!v && !s)
        s = UD_RESOURCE;
    while (!s && h->rows(*out) < 4096) {
        if (h->cancelled(h->context)) {
            SQLCancel(p->read);
            s = UD_CANCELLED;
            break;
        }
        SQLRETURN rc = SQLFetch(p->read);
        if (rc == SQL_NO_DATA) {
            SQLFreeHandle(SQL_HANDLE_STMT, p->read);
            p->read = SQL_NULL_HSTMT;
            break;
        }
        if (rc != SQL_SUCCESS) {
            s = diagnostic(p, SQL_HANDLE_STMT, p->read);
            break;
        }
        memset(v, 0, columns * sizeof(*v));
        for (uint32_t i = 0; !s && i < columns; i++) {
            ud_value *x = v + i;
            x->type = fields[i].type;
            SQLLEN indicator = 0;
            if (x->type == UD_UTF8) {
                SQLWCHAR text[8192];
                memset(text, 0, sizeof(text));
                rc = SQLGetData(p->read, (SQLUSMALLINT)(i + 1), SQL_C_WCHAR, text, sizeof(text),
                                &indicator);
                if (rc == SQL_SUCCESS_WITH_INFO) {
                    s = h->report(h->context, UD_RESOURCE,
                                  "ODBC text exceeds 8191 code units; no truncation accepted");
                    break;
                }
                if (indicator != SQL_NULL_DATA && SQL_SUCCEEDED(rc)) {
                    if (indicator < 0 || (uint64_t)indicator >= sizeof(text) ||
                        indicator % (SQLLEN)sizeof(SQLWCHAR)) {
                        s = UD_SCHEMA;
                        break;
                    }
                    uint32_t bytes;
                    x->text = from_wide(h, text, (uint32_t)(indicator / sizeof(SQLWCHAR)), &bytes);
                    x->text_length = bytes;
                    if (!x->text)
                        s = UD_PARSE;
                }
            } else if (x->type == UD_FLOAT64)
                rc = SQLGetData(p->read, (SQLUSMALLINT)(i + 1), SQL_C_DOUBLE, &x->real,
                                sizeof(x->real), &indicator);
            else if (x->type == UD_BOOL) {
                unsigned char bit = 0;
                rc = SQLGetData(p->read, (SQLUSMALLINT)(i + 1), SQL_C_BIT, &bit, sizeof(bit),
                                &indicator);
                x->integer = bit;
            } else
                rc = SQLGetData(p->read, (SQLUSMALLINT)(i + 1), SQL_C_SBIGINT, &x->integer,
                                sizeof(x->integer), &indicator);
            if (rc != SQL_SUCCESS)
                s = diagnostic(p, SQL_HANDLE_STMT, p->read);
            x->is_null = indicator == SQL_NULL_DATA;
        }
        if (!s)
            s = h->append(*out, v, columns);
        for (uint32_t i = 0; i < columns; i++)
            if (v[i].type == UD_UTF8)
                h->deallocate((void *)v[i].text);
    }
    h->deallocate(fields);
    h->deallocate(v);
    if (s && p->read) {
        SQLFreeHandle(SQL_HANDLE_STMT, p->read);
        p->read = SQL_NULL_HSTMT;
    }
    if (!s && !h->rows(*out)) {
        h->batch_destroy(*out);
        *out = NULL;
        return UD_END;
    }
    return s;
}
typedef struct {
    int64_t integer;
    double real;
    unsigned char bit;
    SQLWCHAR *text;
    SQLLEN indicator;
} parameter;
static ud_status write_batch(odbc_state *p, const ud_batch *b, const char *sql, uint32_t n) {
    ud_host_api *h = &p->host;
    if (!p->transactions)
        return h->report(h->context, UD_UNSUPPORTED, "ODBC driver has no transaction capability");
    if (!b || !n || p->read || h->columns(b) > INT16_MAX)
        return UD_INVALID;
    uint32_t units;
    SQLWCHAR *wide = to_wide(h, sql, n, &units);
    if (!wide)
        return UD_INVALID;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    ud_status s = UD_OK;
    if (!SQL_SUCCEEDED(
            SQLSetConnectAttr(p->db, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0)))
        s = diagnostic(p, SQL_HANDLE_DBC, p->db);
    if (!s && !SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, p->db, &stmt)))
        s = UD_IO;
    if (!s && !SQL_SUCCEEDED(SQLPrepareW(stmt, wide, (SQLINTEGER)units)))
        s = diagnostic(p, SQL_HANDLE_STMT, stmt);
    h->deallocate(wide);
    uint32_t columns = h->columns(b);
    SQLSMALLINT num = 0;
    if (!s && (!SQL_SUCCEEDED(SQLNumParams(stmt, &num)) || num != (SQLSMALLINT)columns))
        s = UD_SCHEMA;
    parameter *params = h->allocate(h->context, columns * sizeof(*params));
    if (!params && !s)
        s = UD_RESOURCE;
    for (uint32_t r = 0; !s && r < h->rows(b); r++) {
        if (h->cancelled(h->context)) {
            s = UD_CANCELLED;
            break;
        }
        SQLFreeStmt(stmt, SQL_RESET_PARAMS);
        for (uint32_t i = 0; !s && i < columns; i++) {
            parameter *a = params + i;
            h->deallocate(a->text);
            a->text = NULL;
            ud_value v;
            h->get(b, r, i, &v);
            a->indicator = v.is_null ? SQL_NULL_DATA : 0;
            SQLSMALLINT ctype, sqltype;
            SQLPOINTER value;
            SQLLEN length;
            SQLULEN size;
            if (v.type == UD_UTF8) {
                uint32_t len = 0;
                a->text = to_wide(h, v.text ? v.text : "", v.is_null ? 0 : v.text_length, &len);
                if (!a->text) {
                    s = UD_PARSE;
                    break;
                }
                ctype = SQL_C_WCHAR;
                sqltype = SQL_WVARCHAR;
                value = a->text;
                length = ((SQLLEN)len + 1) * sizeof(SQLWCHAR);
                size = len ? len : 1;
                if (!v.is_null)
                    a->indicator = (SQLLEN)len * sizeof(SQLWCHAR);
            } else if (v.type == UD_FLOAT64) {
                a->real = v.real;
                ctype = SQL_C_DOUBLE;
                sqltype = SQL_DOUBLE;
                value = &a->real;
                length = sizeof(a->real);
                size = 15;
            } else if (v.type == UD_BOOL) {
                a->bit = (unsigned char)v.integer;
                ctype = SQL_C_BIT;
                sqltype = SQL_BIT;
                value = &a->bit;
                length = 1;
                size = 1;
            } else {
                a->integer = v.integer;
                ctype = SQL_C_SBIGINT;
                sqltype = SQL_BIGINT;
                value = &a->integer;
                length = sizeof(a->integer);
                size = 19;
            }
            if (!SQL_SUCCEEDED(SQLBindParameter(stmt, (SQLUSMALLINT)(i + 1), SQL_PARAM_INPUT, ctype,
                                                sqltype, size, 0, value, length, &a->indicator)))
                s = diagnostic(p, SQL_HANDLE_STMT, stmt);
        }
        if (!s && SQLExecute(stmt) != SQL_SUCCESS)
            s = diagnostic(p, SQL_HANDLE_STMT, stmt);
        if (!s)
            SQLFreeStmt(stmt, SQL_CLOSE);
    }
    if (params) {
        for (uint32_t i = 0; i < columns; i++)
            h->deallocate(params[i].text);
        h->deallocate(params);
    }
    if (stmt)
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    if (!SQL_SUCCEEDED(SQLEndTran(SQL_HANDLE_DBC, p->db, s ? SQL_ROLLBACK : SQL_COMMIT))) {
        p->failed = 1;
        return h->report(h->context, UD_IO,
                         "ODBC commit/rollback failed; outcome unknown, dispose connection");
    }
    if (!SQL_SUCCEEDED(
            SQLSetConnectAttr(p->db, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0))) {
        p->failed = 1;
        return diagnostic(p, SQL_HANDLE_DBC, p->db);
    }
    return s;
}
static ud_status execute(void *ptr, int32_t op, const ud_batch *b, const char *arg, uint32_t n,
                         ud_batch **out) {
    odbc_state *p = ptr;
    *out = NULL;
    if (p->failed)
        return UD_INCOMPATIBLE;
    if (op == UD_OP_READ)
        return read_batch(p, b, arg, n, out);
    if (op == UD_OP_WRITE)
        return write_batch(p, b, arg, n);
    return UD_UNSUPPORTED;
}
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != 1 || !out || out->struct_size < sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){sizeof(*out),
                           1,
                           "org.universaldata.connector.odbc",
                           "0.1.0",
                           UD_PLUGIN_SOURCE | UD_PLUGIN_SINK,
                           UD_CAP_READ | UD_CAP_WRITE | UD_CAP_TRANSACTION,
                           initialize,
                           execute,
                           destroy,
                           plugin_shutdown};
    return UD_OK;
}
