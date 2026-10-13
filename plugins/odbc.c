#include "common.h"
#include <sql.h>
#include <sqlext.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>
#ifdef UD_WITH_MSSQL_BCP
#include <msodbcsql.h>
#endif
/* One ODBC owner for every external Driver Manager provider. SQLGetInfo and
 * SQLGetFunctions establish actual capabilities before a lease is admitted. */
#include "odbc_internal.h"
ud_status ud_odbc_control(odbc_state *p) {
    if (atomic_load(&p->cancelled) || p->host.cancelled(p->host.context))
        return UD_CANCELLED;
    return p->check           ? p->check(p->check_user)
           : p->host.check_io ? p->host.check_io(p->host.control_user)
                              : UD_OK;
}
static ud_status failure(odbc_state *p, SQLSMALLINT kind, SQLHANDLE handle) {
    SQLCHAR message[2];
    SQLSMALLINT length = 0;
    SQLGetDiagRecA(kind, handle, 1, (SQLCHAR *)p->sqlstate, &p->native_error, message,
                   sizeof(message), &length);
    p->sqlstate[5] = 0;
    char safe[96];
    snprintf(safe, sizeof(safe), "ODBC operation failed (SQLSTATE %.5s, native %ld)", p->sqlstate,
             (long)p->native_error);
    p->host.report(p->host.context, UD_IO, safe);
    if (!strncmp(p->sqlstate, "08", 2)) {
        p->broken = 1;
        return UD_TRANSIENT;
    }
    if (!strncmp(p->sqlstate, "28", 2))
        return UD_AUTH;
    if (!strncmp(p->sqlstate, "23", 2))
        return UD_QUALITY;
    if (!strcmp(p->sqlstate, "22001"))
        return UD_TRUNCATED;
    if (!strcmp(p->sqlstate, "22003"))
        return UD_OVERFLOW;
    if (!strncmp(p->sqlstate, "22", 2))
        return UD_PARSE;
    if (!strncmp(p->sqlstate, "42", 2))
        return UD_SCHEMA;
    if (!strcmp(p->sqlstate, "40001"))
        return UD_TRANSIENT;
    if (!strcmp(p->sqlstate, "HYT00") || !strcmp(p->sqlstate, "HYT01"))
        return UD_TIMEOUT;
    if (!strcmp(p->sqlstate, "HY008"))
        return UD_CANCELLED;
    if (!strcmp(p->sqlstate, "HY001"))
        return UD_RESOURCE;
    return UD_IO;
}
/* Registration protects every live statement against concurrent cancellation.
 * Free and ud_odbc_cancel hold the same short ownership lock; I/O does not hold it. */
static SQLRETURN statement_allocate(odbc_state *p, SQLHSTMT *out) {
    pthread_mutex_lock(&p->handles_lock);
    uint32_t i = 0;
    while (i < 16 && p->handles[i])
        i++;
    SQLRETURN rc = i == 16 ? SQL_ERROR : SQLAllocHandle(SQL_HANDLE_STMT, p->connection, out);
    if (SQL_SUCCEEDED(rc))
        p->handles[i] = *out;
    pthread_mutex_unlock(&p->handles_lock);
    return rc;
}
static void statement_release(odbc_state *p, SQLHSTMT handle) {
    if (!handle)
        return;
    pthread_mutex_lock(&p->handles_lock);
    for (uint32_t i = 0; i < 16; i++)
        if (p->handles[i] == handle) {
            p->handles[i] = NULL;
            break;
        }
    SQLFreeHandle(SQL_HANDLE_STMT, handle);
    pthread_mutex_unlock(&p->handles_lock);
}
static ud_status parameter_get(odbc_state *p, const ud_batch *batch, uint32_t first, uint32_t row,
                               const uint32_t *mapping, uint32_t i, const ud_write_options *write,
                               const ud_sql_statement *plan, ud_value *out) {
    if (!plan)
        return p->host.get(batch, first + row, mapping ? mapping[i] : i, out);
    ud_sql_parameter ref = plan->parameters[i];
    return p->host.get(ref.source == UD_SQL_BEFORE ? write->before : batch, ref.row + row,
                       ref.column, out);
}
static ud_status wide(odbc_state *p, const char *text, uint32_t length, SQLWCHAR **out,
                      SQLINTEGER *units) {
    SQLWCHAR *v = p->host.allocate(p->host.context, ((size_t)length + 1) * sizeof(*v));
    if (!v)
        return UD_RESOURCE;
    uint32_t n = 0;
    ud_status status = UD_OK;
    for (uint32_t i = 0; i < length;) {
        uint32_t c = (unsigned char)text[i++], extra = 0;
        if (c >= 0x80) {
            extra = c >= 0xF0 && c <= 0xF4   ? 3
                    : c >= 0xE0 && c <= 0xEF ? 2
                    : c >= 0xC2 && c <= 0xDF ? 1
                                             : 99;
            if (extra > length - i) {
                status = UD_PARSE;
                break;
            }
            c &= extra == 3 ? 7 : extra == 2 ? 15 : 31;
            for (uint32_t j = 0; j < extra; j++) {
                unsigned char b = (unsigned char)text[i++];
                if ((b & 0xC0) != 0x80) {
                    status = UD_PARSE;
                    break;
                }
                c = (c << 6) | (b & 63);
            }
            if (status)
                break;
            if (c > 0x10FFFF || (c >= 0xD800 && c <= 0xDFFF) || (extra == 1 && c < 0x80) ||
                (extra == 2 && c < 0x800) || (extra == 3 && c < 0x10000)) {
                status = UD_PARSE;
                break;
            }
        }
        if (p->ansi && c > 127) {
            status = UD_PARSE;
            break;
        }
        if (sizeof(SQLWCHAR) == 2 && c > 0xFFFF) {
            c -= 0x10000;
            v[n++] = (SQLWCHAR)(0xD800 + (c >> 10));
            v[n++] = (SQLWCHAR)(0xDC00 + (c & 1023));
        } else
            v[n++] = (SQLWCHAR)c;
    }
    if (status) {
        p->host.deallocate(v);
        return status;
    }
    v[n] = 0;
    *out = v;
    *units = (SQLINTEGER)n;
    return UD_OK;
}
static ud_status utf8(odbc_state *p, const SQLWCHAR *text, uint32_t units, char **out,
                      uint32_t *bytes) {
    char *v = p->host.allocate(p->host.context, (size_t)units * 4 + 1);
    if (!v)
        return UD_RESOURCE;
    uint32_t at = 0;
    for (uint32_t i = 0; i < units; i++) {
        uint32_t c = text[i];
        if (sizeof(SQLWCHAR) == 2 && c >= 0xD800 && c <= 0xDBFF) {
            if (i + 1 >= units || text[i + 1] < 0xDC00 || text[i + 1] > 0xDFFF) {
                p->host.deallocate(v);
                return UD_PARSE;
            }
            c = 0x10000 + ((c - 0xD800) << 10) + (text[++i] - 0xDC00);
        } else if (c >= 0xD800 && c <= 0xDFFF) {
            p->host.deallocate(v);
            return UD_PARSE;
        }
        if (c > 0x10FFFF || (p->ansi && c > 127)) {
            p->host.deallocate(v);
            return UD_PARSE;
        }
        if (c < 0x80)
            v[at++] = (char)c;
        else if (c < 0x800) {
            v[at++] = (char)(0xC0 | (c >> 6));
            v[at++] = (char)(0x80 | (c & 63));
        } else if (c < 0x10000) {
            v[at++] = (char)(0xE0 | (c >> 12));
            v[at++] = (char)(0x80 | ((c >> 6) & 63));
            v[at++] = (char)(0x80 | (c & 63));
        } else {
            v[at++] = (char)(0xF0 | (c >> 18));
            v[at++] = (char)(0x80 | ((c >> 12) & 63));
            v[at++] = (char)(0x80 | ((c >> 6) & 63));
            v[at++] = (char)(0x80 | (c & 63));
        }
    }
    v[at] = 0;
    *out = v;
    *bytes = at;
    return UD_OK;
}
static ud_status prepare(odbc_state *p, ud_string sql, SQLHSTMT *out) {
    *out = SQL_NULL_HSTMT;
    ud_status s = ud_odbc_control(p);
    if (s)
        return s;
    if (!SQL_SUCCEEDED(statement_allocate(p, out)))
        return UD_RESOURCE;
    SQLRETURN rc;
    if (p->ansi) {
        for (uint32_t i = 0; i < sql.length; i++)
            if ((unsigned char)sql.data[i] > 127) {
                statement_release(p, *out);
                *out = SQL_NULL_HSTMT;
                return UD_PARSE;
            }
        rc = SQLPrepareA(*out, (SQLCHAR *)sql.data, (SQLINTEGER)sql.length);
    } else {
        SQLWCHAR *text = NULL;
        SQLINTEGER n;
        s = wide(p, sql.data, sql.length, &text, &n);
        if (s) {
            statement_release(p, *out);
            *out = SQL_NULL_HSTMT;
            return s;
        }
        rc = SQLPrepareW(*out, text, n);
        p->host.deallocate(text);
    }
    if (!SQL_SUCCEEDED(rc)) {
        s = failure(p, SQL_HANDLE_STMT, *out);
        statement_release(p, *out);
        *out = SQL_NULL_HSTMT;
    }
    return s;
}
typedef struct parameter {
    void *data;
    SQLLEN *lengths;
    SQLLEN stride;
    SQLSMALLINT c_type, sql_type;
    SQLULEN precision;
    SQLSMALLINT scale;
} parameter;
static void parameters_free(odbc_state *p, parameter *v, uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        p->host.deallocate(v[i].data);
        p->host.deallocate(v[i].lengths);
    }
    p->host.deallocate(v);
}
static ud_status parameters_bind(odbc_state *p, SQLHSTMT stmt, const ud_batch *batch,
                                 uint32_t first, uint32_t rows, const uint32_t *mapping,
                                 uint32_t count, const ud_write_options *write,
                                 const ud_sql_statement *plan, parameter **out) {
    *out = NULL;
    SQLSMALLINT n = 0;
    if (!SQL_SUCCEEDED(SQLNumParams(stmt, &n)))
        return failure(p, SQL_HANDLE_STMT, stmt);
    if (n != (SQLSMALLINT)count || count > 32767)
        return UD_SCHEMA;
    if (!count)
        return UD_OK;
    ud_host_api *h = &p->host;
    parameter *v = h->allocate(h->context, count * sizeof(*v));
    if (!v)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < count; i++) {
        ud_field f;
        s = h->field(batch, plan ? plan->parameters[i].column : mapping ? mapping[i] : i, &f);
        if (s)
            break;
        parameter *q = v + i;
        q->scale = (SQLSMALLINT)(f.type == UD_TIMESTAMP || f.type == UD_TIME64 ? 6 : f.scale);
        q->precision = f.type == UD_UINT64       ? 20
                       : f.type == UD_DECIMAL128 ? (SQLULEN)f.precision
                       : f.type == UD_TIMESTAMP  ? 26
                       : f.type == UD_TIME64     ? 15
                                                 : 0;
        q->sql_type = f.type == UD_BOOL                                  ? SQL_BIT
                      : f.type == UD_INT64                               ? SQL_BIGINT
                      : f.type == UD_FLOAT64                             ? SQL_DOUBLE
                      : f.type == UD_BINARY                              ? SQL_VARBINARY
                      : (f.type == UD_DECIMAL128 || f.type == UD_UINT64) ? SQL_DECIMAL
                      : f.type == UD_DATE32                              ? SQL_TYPE_DATE
                      : f.type == UD_TIME64                              ? SQL_TYPE_TIME
                      : f.type == UD_TIMESTAMP                           ? SQL_TYPE_TIMESTAMP
                      : f.type == UD_UTF8 ? (p->ansi ? SQL_VARCHAR : SQL_WVARCHAR)
                                          : 0;
        if (!q->sql_type) {
            s = UD_UNSUPPORTED;
            break;
        }
        q->c_type = f.type == UD_BOOL                ? SQL_C_BIT
                    : f.type == UD_INT64             ? SQL_C_SBIGINT
                    : f.type == UD_FLOAT64           ? SQL_C_DOUBLE
                    : f.type == UD_BINARY            ? SQL_C_BINARY
                    : (p->ansi || f.type != UD_UTF8) ? SQL_C_CHAR
                                                     : SQL_C_WCHAR;
        q->stride = q->c_type == SQL_C_BIT                                    ? 1
                    : q->c_type == SQL_C_SBIGINT || q->c_type == SQL_C_DOUBLE ? 8
                                                                              : 256;
        if (f.type == UD_UTF8 || f.type == UD_BINARY) {
            uint32_t maximum = 1;
            for (uint32_t row = 0; !s && row < rows; row++) {
                ud_value value;
                s = parameter_get(p, batch, first, row, mapping, i, write, plan, &value);
                if (!s && !value.is_null && value.text_length > maximum)
                    maximum = value.text_length;
            }
            if (maximum > 16777216) {
                s = UD_RESOURCE;
                break;
            }
            q->stride = (SQLLEN)((maximum + 1) * (q->c_type == SQL_C_WCHAR ? sizeof(SQLWCHAR) : 1));
            q->precision = maximum;
        }
        if ((uint64_t)q->stride > SIZE_MAX / rows) {
            s = UD_RESOURCE;
            break;
        }
        q->data = h->allocate(h->context, (size_t)q->stride * rows);
        q->lengths = h->allocate(h->context, rows * sizeof(SQLLEN));
        if (!q->data || !q->lengths) {
            s = UD_RESOURCE;
            break;
        }
        for (uint32_t row = 0; !s && row < rows; row++) {
            ud_value value;
            s = parameter_get(p, batch, first, row, mapping, i, write, plan, &value);
            if (s)
                break;
            char *dest = (char *)q->data + (size_t)q->stride * row;
            if (value.is_null) {
                q->lengths[row] = SQL_NULL_DATA;
                continue;
            }
            if (q->c_type == SQL_C_BIT) {
                *dest = (char)!!value.integer;
                q->lengths[row] = 1;
            } else if (q->c_type == SQL_C_SBIGINT) {
                memcpy(dest, &value.integer, 8);
                q->lengths[row] = 8;
            } else if (q->c_type == SQL_C_DOUBLE) {
                memcpy(dest, &value.real, 8);
                q->lengths[row] = 8;
            } else if (q->c_type == SQL_C_BINARY) {
                if (value.text_length)
                    memcpy(dest, value.text, value.text_length);
                q->lengths[row] = value.text_length;
            } else if (q->c_type == SQL_C_WCHAR) {
                SQLWCHAR *converted = NULL;
                SQLINTEGER units;
                s = wide(p, value.text, value.text_length, &converted, &units);
                if (!s) {
                    memcpy(dest, converted, (size_t)(units + 1) * sizeof(SQLWCHAR));
                    q->lengths[row] = (SQLLEN)units * sizeof(SQLWCHAR);
                }
                h->deallocate(converted);
            } else if (value.type == UD_UTF8) {
                for (uint32_t j = 0; j < value.text_length; j++)
                    if ((unsigned char)value.text[j] > 127)
                        s = UD_PARSE;
                if (!s) {
                    if (value.text_length)
                        memcpy(dest, value.text, value.text_length);
                    q->lengths[row] = value.text_length;
                }
            } else {
                uint32_t required;
                s = h->format_value(&value, dest, (uint32_t)q->stride, &required);
                if (!s && value.type == UD_TIMESTAMP) {
                    if (!p->utc) {
                        s = UD_UNSUPPORTED;
                        break;
                    }
                    for (uint32_t j = 0; j < required - 1; j++)
                        if (dest[j] == 'T')
                            dest[j] = ' ';
                    if (required > 1 && dest[required - 2] == 'Z') {
                        dest[required - 2] = 0;
                        required--;
                    }
                }
                if (!s)
                    q->lengths[row] = required - 1;
            }
        }
        if (!s && !SQL_SUCCEEDED(SQLBindParameter(stmt, (SQLUSMALLINT)i + 1, SQL_PARAM_INPUT,
                                                  q->c_type, q->sql_type, q->precision, q->scale,
                                                  q->data, q->stride, q->lengths)))
            s = failure(p, SQL_HANDLE_STMT, stmt);
    }
    if (s) {
        parameters_free(p, v, count);
        return s;
    }
    *out = v;
    return UD_OK;
}
static ud_status execute_bound(odbc_state *p, SQLHSTMT stmt, const ud_batch *parameters,
                               uint32_t first, uint32_t rows, const uint32_t *mapping,
                               uint32_t columns, uint64_t *affected, const ud_write_options *write,
                               const ud_sql_statement *plan) {
    if (affected)
        *affected = 0;
    if (!rows)
        return UD_OK;
    ud_status s = ud_odbc_control(p);
    if (s)
        return s;
    SQLUSMALLINT *statuses = p->host.allocate(p->host.context, rows * sizeof(*statuses));
    SQLULEN processed = 0;
    if (!statuses)
        return UD_RESOURCE;
    int array = rows > 1 && p->array_parameters;
    if (!array && rows > 1) {
        p->host.deallocate(statuses);
        for (uint32_t row = 0; row < rows; row++) {
            uint64_t count = 0;
            s = execute_bound(p, stmt, parameters, first + row, 1, mapping, columns, &count, write,
                              plan);
            if (s)
                return s;
            if (affected)
                *affected += count;
            SQLFreeStmt(stmt, SQL_CLOSE);
            SQLFreeStmt(stmt, SQL_RESET_PARAMS);
        }
        return UD_OK;
    }
    if (array) {
        if (!SQL_SUCCEEDED(SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE,
                                          (SQLPOINTER)SQL_PARAM_BIND_BY_COLUMN, 0)) ||
            !SQL_SUCCEEDED(
                SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)rows, 0)) ||
            !SQL_SUCCEEDED(SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, statuses, 0)) ||
            !SQL_SUCCEEDED(SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &processed, 0)))
            s = failure(p, SQL_HANDLE_STMT, stmt);
    }
    parameter *bound = NULL;
    if (!s)
        s = parameters_bind(p, stmt, parameters, first, rows, mapping, columns, write, plan,
                            &bound);
    SQLRETURN rc = !s ? SQLExecute(stmt) : SQL_ERROR;
    if (!s && !SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA)
        s = failure(p, SQL_HANDLE_STMT, stmt);
    if (!s && array) {
        SQLLEN initial_count = 0;
        if (affected && SQL_SUCCEEDED(SQLRowCount(stmt, &initial_count)) && initial_count > 0)
            *affected += (uint64_t)initial_count;
        SQLRETURN more;
        while (SQL_SUCCEEDED(more = SQLMoreResults(stmt))) {
            SQLLEN count = 0;
            if (affected && SQL_SUCCEEDED(SQLRowCount(stmt, &count)) && count > 0)
                *affected += (uint64_t)count;
        }
        if (more != SQL_NO_DATA)
            s = failure(p, SQL_HANDLE_STMT, stmt);
    }
    if (!s && array) {
        if (processed != rows)
            s = UD_IO;
        for (uint32_t i = 0; !s && i < rows; i++)
            if (statuses[i] != SQL_PARAM_SUCCESS && statuses[i] != SQL_PARAM_SUCCESS_WITH_INFO)
                s = UD_IO;
    }
    if (!s && !array) {
        SQLLEN count = 0;
        if (!SQL_SUCCEEDED(SQLRowCount(stmt, &count)))
            s = failure(p, SQL_HANDLE_STMT, stmt);
        else if (affected && count > 0)
            *affected = (uint64_t)count;
    }
    SQLFreeStmt(stmt, SQL_RESET_PARAMS);
    parameters_free(p, bound, columns);
    if (array) {
        SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, NULL, 0);
        SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, NULL, 0);
        SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)1, 0);
    }
    p->host.deallocate(statuses);
    return s;
}
static ud_status command(odbc_state *p, ud_string sql, const ud_batch *parameters,
                         uint64_t *count) {
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    ud_status s = prepare(p, sql, &stmt);
    if (!s)
        s = execute_bound(p, stmt, parameters, 0, parameters ? p->host.rows(parameters) : 1, NULL,
                          parameters ? p->host.columns(parameters) : 0, count, NULL, NULL);
    if (stmt)
        statement_release(p, stmt);
    return s;
}
ud_status ud_odbc_command(odbc_state *p, const char *sql) {
    return command(p, (ud_string){sql, (uint32_t)strlen(sql)}, NULL, NULL);
}
static ud_status get_cell(odbc_state *p, SQLHSTMT stmt, uint32_t column, int binary, char **out,
                          uint32_t *length, int *null) {
    *out = NULL;
    *length = 0;
    *null = 0;
    uint32_t capacity = 8192, used = 0;
    char *data = p->host.allocate(p->host.context, capacity);
    if (!data)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (;;) {
        SQLLEN indicator = 0;
        union {
            SQLWCHAR aligned[4096];
            unsigned char bytes[8192];
        } chunk;
        memset(&chunk, 0, sizeof(chunk));
        SQLRETURN rc = SQLGetData(stmt, (SQLUSMALLINT)column + 1,
                                  binary    ? SQL_C_BINARY
                                  : p->ansi ? SQL_C_CHAR
                                            : SQL_C_WCHAR,
                                  chunk.bytes, sizeof(chunk.bytes), &indicator);
        if (indicator == SQL_NULL_DATA) {
            *null = 1;
            break;
        }
        if (rc == SQL_NO_DATA)
            break;
        if (!SQL_SUCCEEDED(rc)) {
            status = failure(p, SQL_HANDLE_STMT, stmt);
            break;
        }
        uint32_t terminator = binary    ? 0
                              : p->ansi ? 1
                                        : (uint32_t)sizeof(SQLWCHAR),
                 bytes = sizeof(chunk.bytes) - terminator;
        if (rc == SQL_SUCCESS) {
            if (indicator < 0 || (uint64_t)indicator > bytes) {
                status = UD_UNSUPPORTED;
                break;
            }
            bytes = (uint32_t)indicator;
        }
        if (!binary && !p->ansi && bytes % sizeof(SQLWCHAR)) {
            status = UD_PARSE;
            break;
        }
        if (bytes > 16777216 - used) {
            status = UD_RESOURCE;
            break;
        }
        if (used + bytes + sizeof(SQLWCHAR) > capacity) {
            uint32_t next = (used + bytes + (uint32_t)sizeof(SQLWCHAR)) * 2;
            char *larger = p->host.allocate(p->host.context, next);
            if (!larger) {
                status = UD_RESOURCE;
                break;
            }
            memcpy(larger, data, used);
            p->host.deallocate(data);
            data = larger;
            capacity = next;
        }
        if (bytes)
            memcpy(data + used, chunk.bytes, bytes);
        used += bytes;
        if (rc == SQL_SUCCESS)
            break;
    }
    if (status) {
        p->host.deallocate(data);
        return status;
    }
    if (!binary && !p->ansi) {
        char *converted = NULL;
        uint32_t bytes = 0;
        status = utf8(p, (SQLWCHAR *)data, used / (uint32_t)sizeof(SQLWCHAR), &converted, &bytes);
        p->host.deallocate(data);
        if (status)
            return status;
        data = converted;
        used = bytes;
    }
    if (p->ansi && !binary)
        for (uint32_t i = 0; i < used; i++)
            if ((unsigned char)data[i] > 127) {
                p->host.deallocate(data);
                return UD_PARSE;
            }
    data[used] = 0;
    *out = data;
    *length = used;
    return UD_OK;
}

ud_status ud_odbc_read_close(void *state) {
    odbc_state *p = state;
    if (p->cursor)
        statement_release(p, p->cursor);
    p->cursor = SQL_NULL_HSTMT;
    p->host.batch_destroy(p->schema);
    p->schema = NULL;
    return UD_OK;
}
ud_status ud_odbc_read_open(void *state, const ud_read_options *o, const ud_batch *schema) {
    odbc_state *p = state;
    if (p->cursor)
        return UD_BUSY;
    ud_status s = prepare(p, o->sql, &p->cursor);
    if (!s)
        s = execute_bound(p, p->cursor, o->parameters, o->parameter_row, 1, NULL,
                          o->parameters ? p->host.columns(o->parameters) : 0, NULL, NULL, NULL);
    SQLSMALLINT columns = 0;
    if (!s && !SQL_SUCCEEDED(SQLNumResultCols(p->cursor, &columns)))
        s = failure(p, SQL_HANDLE_STMT, p->cursor);
    if (!s && (uint32_t)columns != p->host.columns(schema))
        s = UD_SCHEMA;
    for (SQLSMALLINT i = 0; !s && i < columns; i++) {
        SQLWCHAR name[512];
        SQLSMALLINT length, type, scale, nullable;
        SQLULEN precision;
        SQLRETURN rc = SQLDescribeColW(p->cursor, (SQLUSMALLINT)i + 1, name, 512, &length, &type,
                                       &precision, &scale, &nullable);
        if (!SQL_SUCCEEDED(rc) || length >= 512)
            s = UD_SCHEMA;
        else {
            char *utf = NULL;
            uint32_t n = 0;
            s = utf8(p, name, (uint32_t)length, &utf, &n);
            ud_field expected;
            if (!s)
                s = p->host.field(schema, (uint32_t)i, &expected);
            if (!s && (expected.name_length != n || memcmp(expected.name, utf, n)))
                s = UD_SCHEMA;
            p->host.deallocate(utf);
        }
    }
    ud_field *fields = NULL;
    if (!s)
        s = plugin_fields(&p->host, schema, &fields);
    if (!s)
        s = p->host.batch_create(p->host.context, fields, (uint32_t)columns, 0, &p->schema);
    p->host.deallocate(fields);
    p->batch_size = o->batch_size;
    if (s)
        ud_odbc_read_close(p);
    return s;
}
ud_status ud_odbc_read_next(void *state, ud_batch **out) {
    odbc_state *p = state;
    *out = NULL;
    if (!p->cursor)
        return UD_INVALID;
    ud_field *fields = NULL;
    ud_status s = plugin_fields(&p->host, p->schema, &fields);
    uint32_t columns = p->host.columns(p->schema);
    if (!s)
        s = p->host.batch_create(p->host.context, fields, columns, p->batch_size, out);
    ud_value *values = p->host.allocate(p->host.context, columns * sizeof(*values));
    char **owned = p->host.allocate(p->host.context, columns * sizeof(*owned));
    if (!values || !owned)
        s = UD_RESOURCE;
    while (!s && p->host.rows(*out) < p->batch_size) {
        s = ud_odbc_control(p);
        if (s)
            break;
        SQLRETURN rc = SQLFetch(p->cursor);
        if (rc == SQL_NO_DATA)
            break;
        if (!SQL_SUCCEEDED(rc)) {
            s = failure(p, SQL_HANDLE_STMT, p->cursor);
            break;
        }
        for (uint32_t i = 0; !s && i < columns; i++) {
            uint32_t n = 0;
            int null = 0;
            s = get_cell(p, p->cursor, i, fields[i].type == UD_BINARY, owned + i, &n, &null);
            values[i] = (ud_value){.type = fields[i].type, .is_null = null};
            if (s || null)
                continue;
            if (fields[i].type == UD_BINARY) {
                values[i].text = owned[i];
                values[i].text_length = n;
            } else if (fields[i].type == UD_TIMESTAMP && n >= 19 && owned[i][10] == ' ' &&
                       n < 128) {
                char time[132];
                uint32_t normalized = 0;
                int zoned = 0;
                for (uint32_t at = 0; at < n; at++) {
                    char ch = owned[i][at];
                    if (at == 10)
                        ch = 'T';
                    else if (at > 18 && ch == ' ')
                        continue;
                    if (at > 18 && (ch == '+' || ch == '-' || ch == 'Z'))
                        zoned = 1;
                    time[normalized++] = ch;
                }
                if (!zoned) {
                    if (!p->utc) {
                        s = UD_UNSUPPORTED;
                        break;
                    }
                    time[normalized++] = 'Z';
                }
                n = normalized;
                s = p->host.parse_field(p->host.context, fields + i, time, n, UD_ROUND_EXACT,
                                        values + i);
            } else if (fields[i].type == UD_BOOL) {
                if (n != 1 || (owned[i][0] != '0' && owned[i][0] != '1'))
                    s = UD_PARSE;
                else
                    values[i] = (ud_value){.type = UD_BOOL, .integer = owned[i][0] == '1'};
            } else
                s = p->host.parse_field(p->host.context, fields + i, owned[i], n, UD_ROUND_EXACT,
                                        values + i);
        }
        if (!s)
            s = p->host.append(*out, values, columns);
        for (uint32_t i = 0; i < columns; i++) {
            p->host.deallocate(owned[i]);
            owned[i] = NULL;
        }
    }
    p->host.deallocate(fields);
    p->host.deallocate(values);
    p->host.deallocate(owned);
    if (s || !p->host.rows(*out)) {
        p->host.batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_END;
    }
    return UD_OK;
}
ud_status ud_odbc_begin(void *state, int32_t isolation) {
    odbc_state *p = state;
    if (p->transaction)
        return UD_INVALID;
    SQLUINTEGER level = isolation == UD_ISOLATION_SERIALIZABLE      ? SQL_TXN_SERIALIZABLE
                        : isolation == UD_ISOLATION_REPEATABLE_READ ? SQL_TXN_REPEATABLE_READ
                        : isolation == UD_ISOLATION_READ_COMMITTED  ? SQL_TXN_READ_COMMITTED
                                                                    : 0;
    if (!level)
        return UD_INVALID;
    if (!SQL_SUCCEEDED(SQLSetConnectAttr(p->connection, SQL_ATTR_TXN_ISOLATION,
                                         (SQLPOINTER)(uintptr_t)level, 0)) ||
        !SQL_SUCCEEDED(SQLSetConnectAttr(p->connection, SQL_ATTR_AUTOCOMMIT,
                                         (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0)))
        return failure(p, SQL_HANDLE_DBC, p->connection);
    p->transaction = 1;
    return UD_OK;
}
static ud_status end_transaction(odbc_state *p, int commit_requested) {
    if (!p->transaction)
        return UD_INVALID;
    SQLRETURN rc =
        SQLEndTran(SQL_HANDLE_DBC, p->connection, commit_requested ? SQL_COMMIT : SQL_ROLLBACK);
    if (!SQL_SUCCEEDED(rc)) {
        failure(p, SQL_HANDLE_DBC, p->connection);
        return UD_COMMIT_UNKNOWN;
    }
    p->transaction = 0;
    while (p->bulk_sessions) {
        odbc_state *bulk = p->bulk_sessions;
        p->bulk_sessions = bulk->bulk_next;
        ud_odbc_destroy(bulk);
    }
    if (!SQL_SUCCEEDED(SQLSetConnectAttr(p->connection, SQL_ATTR_AUTOCOMMIT,
                                         (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0))) {
        p->broken = 1;
        return commit_requested ? UD_COMMIT_UNKNOWN : UD_IO;
    }
    return UD_OK;
}
ud_status ud_odbc_commit(void *state) {
    return end_transaction(state, 1);
}
ud_status ud_odbc_rollback(void *state) {
    return end_transaction(state, 0);
}
static char *identifier(odbc_state *p, ud_string value) {
    if (!value.length || memchr(value.data, 0, value.length) || value.length > 1048576)
        return NULL;
    char open = p->quote ? p->quote : '"', close = open == '[' ? ']' : open;
    char *name = p->host.allocate(p->host.context, (size_t)value.length * 2 + 8);
    if (!name)
        return NULL;
    size_t n = 0;
    name[n++] = open;
    for (uint32_t i = 0; i < value.length; i++) {
        char c = value.data[i];
        if (c == '.') {
            name[n++] = close;
            name[n++] = '.';
            name[n++] = open;
            continue;
        }
        if (c == close)
            name[n++] = close;
        name[n++] = c;
    }
    name[n++] = close;
    name[n] = 0;
    return name;
}
/* Emit the actual provider's declaration. The shared Columnar SQL grammar
 * owns canonical types; ODBC numeric codes cannot erase provider semantics. */
static ud_status sql_declaration(odbc_state *p, int type, int precision, int scale,
                                 const char *name, char out[80]) {
    if (!name || !*name || strlen(name) >= 64)
        return UD_UNSUPPORTED;
    char lower[64];
    for (size_t i = 0; i <= strlen(name); i++)
        lower[i] = name[i] >= 'A' && name[i] <= 'Z' ? name[i] - 'A' + 'a' : name[i];
    if (type == SQL_DECIMAL || type == SQL_NUMERIC) {
        if (precision < 1 || precision > 38 || scale < -38 || scale > 38)
            return UD_UNSUPPORTED;
        snprintf(out, 80, "%s(%d,%d)", p->dialect == DIALECT_ORACLE ? "number" : "decimal",
                 precision, scale);
        return UD_OK;
    }
    if (p->dialect == DIALECT_POSTGRES) {
        if (!strcmp(lower, "int8"))
            strcpy(lower, "bigint");
        else if (!strcmp(lower, "int4"))
            strcpy(lower, "integer");
        else if (!strcmp(lower, "int2"))
            strcpy(lower, "smallint");
        else if (!strcmp(lower, "float8"))
            strcpy(lower, "double precision");
        else if (!strcmp(lower, "float4"))
            strcpy(lower, "real");
        else if (!strcmp(lower, "bool"))
            strcpy(lower, "boolean");
        else if (!strcmp(lower, "varchar"))
            strcpy(lower, "character varying");
        else if (!strcmp(lower, "timestamptz"))
            strcpy(lower, "timestamp with time zone");
        else if (!strcmp(lower, "timestamp"))
            return UD_UNSUPPORTED;
        else if (!strcmp(lower, "time"))
            strcpy(lower, "time without time zone");
    } else if (p->dialect == DIALECT_MSSQL) {
        if (!strcmp(lower, "datetime2") || !strcmp(lower, "datetime") ||
            !strcmp(lower, "smalldatetime")) {
            if (!p->utc)
                return UD_UNSUPPORTED;
            snprintf(out, 80, "%s_utc", !strcmp(lower, "datetime2") ? "datetime2" : "datetime");
            return UD_OK;
        }
    } else if (p->dialect == DIALECT_MYSQL && !strncmp(lower, "datetime", 8)) {
        if (!p->utc)
            return UD_UNSUPPORTED;
        strcpy(lower, "datetime_utc");
    } else if (p->dialect == DIALECT_ORACLE &&
               (!strcmp(lower, "date") || !strcmp(lower, "timestamp"))) {
        if (!p->utc)
            return UD_UNSUPPORTED;
        snprintf(out, 80, "%s_utc", lower);
        return UD_OK;
    }
    snprintf(out, 80, "%s", lower);
    return UD_OK;
}
static ud_status columns_query(odbc_state *p, ud_string table, SQLHSTMT *stmt, int primary) {
    *stmt = SQL_NULL_HSTMT;
    if (!SQL_SUCCEEDED(statement_allocate(p, stmt)))
        return UD_RESOURCE;
    SQLWCHAR *name = NULL;
    SQLINTEGER n = 0;
    ud_status s = wide(p, table.data, table.length, &name, &n);
    SQLRETURN rc = SQL_ERROR;
    if (!s) {
        if (p->ansi) {
            if (primary)
                rc = SQLPrimaryKeysA(*stmt, NULL, 0, NULL, 0, (SQLCHAR *)table.data,
                                     (SQLSMALLINT)table.length);
            else
                rc = SQLColumnsA(*stmt, NULL, 0, NULL, 0, (SQLCHAR *)table.data,
                                 (SQLSMALLINT)table.length, NULL, 0);
        } else if (primary)
            rc = SQLPrimaryKeysW(*stmt, NULL, 0, NULL, 0, name, (SQLSMALLINT)n);
        else
            rc = SQLColumnsW(*stmt, NULL, 0, NULL, 0, name, (SQLSMALLINT)n, NULL, 0);
    }
    p->host.deallocate(name);
    if (!s && !SQL_SUCCEEDED(rc))
        s = failure(p, SQL_HANDLE_STMT, *stmt);
    if (s) {
        statement_release(p, *stmt);
        *stmt = SQL_NULL_HSTMT;
    }
    return s;
}
ud_status ud_odbc_discover(void *state, ud_string table, ud_batch **out) {
    odbc_state *p = state;
    *out = NULL;
    char **primary = p->host.allocate(p->host.context, 64 * sizeof(*primary));
    uint32_t count = 0;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    ud_status s = primary ? columns_query(p, table, &stmt, 1) : UD_RESOURCE;
    while (!s) {
        SQLRETURN rc = SQLFetch(stmt);
        if (rc == SQL_NO_DATA)
            break;
        if (!SQL_SUCCEEDED(rc)) {
            s = failure(p, SQL_HANDLE_STMT, stmt);
            break;
        }
        char *name = NULL, *sequence = NULL;
        uint32_t n = 0;
        int null = 0;
        s = get_cell(p, stmt, 3, 0, &name, &n, &null);
        if (!s)
            s = get_cell(p, stmt, 4, 0, &sequence, &n, &null);
        uint32_t order = !s ? (uint32_t)strtoul(sequence, NULL, 10) : 0;
        if (!s && (!order || order > 64 || primary[order - 1]))
            s = UD_SCHEMA;
        if (!s) {
            primary[order - 1] = name;
            name = NULL;
            if (order > count)
                count = order;
        }
        p->host.deallocate(name);
        p->host.deallocate(sequence);
    }
    if (stmt)
        statement_release(p, stmt);
    stmt = SQL_NULL_HSTMT;
    ud_field fields[] = {{.name = "name", .name_length = 4, .type = UD_UTF8},
                         {.name = "declared_type", .name_length = 13, .type = UD_UTF8},
                         {.name = "nullable", .name_length = 8, .type = UD_BOOL},
                         {.name = "key_order", .name_length = 9, .type = UD_INT64}};
    if (!s)
        s = p->host.batch_create(p->host.context, fields, 4, 4096, out);
    if (!s)
        s = columns_query(p, table, &stmt, 0);
    while (!s) {
        SQLRETURN rc = SQLFetch(stmt);
        if (rc == SQL_NO_DATA)
            break;
        if (!SQL_SUCCEEDED(rc)) {
            s = failure(p, SQL_HANDLE_STMT, stmt);
            break;
        }
        char *values[7] = {0};
        uint32_t columns[] = {3, 4, 5, 6, 8, 10, 16};
        int null = 0;
        uint32_t length = 0;
        for (uint32_t i = 0; !s && i < 7; i++)
            s = get_cell(p, stmt, columns[i], 0, values + i, &length, &null);
        char type[80];
        if (!s)
            s = sql_declaration(p, atoi(values[1]), atoi(values[3]), atoi(values[4]), values[2],
                                type);
        int key = 0;
        for (uint32_t k = 0; !s && k < count; k++)
            if (primary[k] && !strcmp(primary[k], values[0]))
                key = (int)k + 1;
        if (!s) {
            ud_value row[] = {
                {.type = UD_UTF8, .text = values[0], .text_length = (uint32_t)strlen(values[0])},
                {.type = UD_UTF8, .text = type, .text_length = (uint32_t)strlen(type)},
                {.type = UD_BOOL, .integer = atoi(values[5]) != SQL_NO_NULLS},
                {.type = UD_INT64, .integer = key}};
            s = p->host.append(*out, row, 4);
        }
        for (uint32_t i = 0; i < 7; i++)
            p->host.deallocate(values[i]);
    }
    if (stmt)
        statement_release(p, stmt);
    for (uint32_t k = 0; k < 64; k++)
        p->host.deallocate(primary ? primary[k] : NULL);
    p->host.deallocate(primary);
    if (!s && !p->host.rows(*out))
        s = UD_SCHEMA;
    if (s) {
        p->host.batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
static ud_status receipt(void *state, ud_string tenant, ud_string job, ud_string id,
                         ud_commit_receipt *out) {
    odbc_state *p = state;
    memset(out, 0, sizeof(*out));
    ud_field fields[] = {{.name = "tenant", .name_length = 6, .type = UD_UTF8},
                         {.name = "job", .name_length = 3, .type = UD_UTF8},
                         {.name = "batch", .name_length = 5, .type = UD_UTF8}};
    ud_batch *parameters = NULL;
    ud_status s = p->host.batch_create(p->host.context, fields, 3, 1, &parameters);
    ud_value values[] = {{.type = UD_UTF8, .text = tenant.data, .text_length = tenant.length},
                         {.type = UD_UTF8, .text = job.data, .text_length = job.length},
                         {.type = UD_UTF8, .text = id.data, .text_length = id.length}};
    if (!s)
        s = p->host.append(parameters, values, 3);
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    const char *sql = "SELECT digest,source_position FROM ud_sdk_receipts_v10 WHERE tenant=? AND "
                      "job=? AND batch_id=?";
    if (!s)
        s = prepare(p, (ud_string){sql, (uint32_t)strlen(sql)}, &stmt);
    if (!s)
        s = execute_bound(p, stmt, parameters, 0, 1, NULL, 3, NULL, NULL, NULL);
    if (!s) {
        SQLRETURN rc = SQLFetch(stmt);
        if (rc != SQL_NO_DATA) {
            if (!SQL_SUCCEEDED(rc))
                s = failure(p, SQL_HANDLE_STMT, stmt);
            else {
                char *digest = NULL, *position = NULL;
                uint32_t length = 0;
                int null = 0;
                s = get_cell(p, stmt, 0, 1, &digest, &length, &null);
                if (!s && (null || length != 32))
                    s = UD_CORRUPT;
                if (!s)
                    memcpy(out->digest, digest, 32);
                if (!s)
                    s = get_cell(p, stmt, 1, 0, &position, &length, &null);
                if (!s && null && p->dialect != DIALECT_ORACLE)
                    s = UD_CORRUPT;
                if (!s) {
                    out->source_position = (ud_string){position, length};
                    position = NULL;
                    out->committed = 1;
                }
                p->host.deallocate(digest);
                p->host.deallocate(position);
            }
        }
    }
    if (stmt)
        statement_release(p, stmt);
    p->host.batch_destroy(parameters);
    return s;
}
static ud_status provision(void *state) {
    odbc_state *p = state;
    if (p->transaction && (p->dialect == DIALECT_MYSQL || p->dialect == DIALECT_ORACLE))
        return UD_UNSUPPORTED;
    const char *sql =
        p->dialect == DIALECT_MSSQL
            ? "IF OBJECT_ID(N'ud_sdk_receipts_v10',N'U') IS NULL CREATE TABLE "
              "ud_sdk_receipts_v10(tenant NVARCHAR(128) NOT NULL,job NVARCHAR(256) NOT "
              "NULL,batch_id NVARCHAR(128) NOT NULL,digest BINARY(32) NOT NULL,source_position "
              "NVARCHAR(1024) NOT NULL,PRIMARY KEY(tenant,job,batch_id))"
        : p->dialect == DIALECT_MYSQL
            ? "CREATE TABLE IF NOT EXISTS ud_sdk_receipts_v10(tenant VARCHAR(128) CHARACTER SET "
              "utf8mb4 COLLATE utf8mb4_bin NOT NULL,job VARCHAR(256) CHARACTER SET utf8mb4 COLLATE "
              "utf8mb4_bin NOT NULL,batch_id VARCHAR(128) CHARACTER SET utf8mb4 COLLATE "
              "utf8mb4_bin NOT NULL,digest BINARY(32) NOT NULL,source_position TEXT CHARACTER SET "
              "utf8mb4 NOT NULL,PRIMARY KEY(tenant,job,batch_id)) ENGINE=InnoDB"
        : p->dialect == DIALECT_POSTGRES
            ? "CREATE TABLE IF NOT EXISTS ud_sdk_receipts_v10(tenant TEXT NOT NULL,job TEXT NOT "
              "NULL,batch_id TEXT NOT NULL,digest BYTEA NOT NULL,source_position TEXT NOT "
              "NULL,PRIMARY KEY(tenant,job,batch_id))"
        : p->dialect == DIALECT_ORACLE
            ? "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE ud_sdk_receipts_v10(tenant "
              "VARCHAR2(128 BYTE) NOT NULL,job VARCHAR2(256 BYTE) NOT NULL,batch_id "
              "VARCHAR2(128 BYTE) NOT NULL,digest RAW(32) NOT NULL,source_position "
              "CLOB,PRIMARY KEY(tenant,job,batch_id))'; "
              "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;"
        : p->dialect == DIALECT_SQLITE || p->dialect == DIALECT_DUCKDB
            ? "CREATE TABLE IF NOT EXISTS ud_sdk_receipts_v10(tenant TEXT NOT NULL,job TEXT NOT "
              "NULL,batch_id TEXT NOT NULL,digest BLOB NOT NULL,source_position TEXT NOT "
              "NULL,PRIMARY KEY(tenant,job,batch_id))"
            : NULL;
    ud_status status = sql ? ud_odbc_command(p, sql) : UD_UNSUPPORTED;
    if (!status && p->dialect == DIALECT_MSSQL && (p->capabilities & UD_CONNECT_NATIVE_CDC))
        status = ud_mssql_cdc_provision(p);
    return status;
}
static int native_dialect(const odbc_state *p) {
    return p->dialect == DIALECT_MSSQL      ? UD_SQL_SQLSERVER
           : p->dialect == DIALECT_MYSQL    ? UD_SQL_MYSQL
           : p->dialect == DIALECT_POSTGRES ? UD_SQL_POSTGRESQL
           : p->dialect == DIALECT_SQLITE   ? UD_SQL_SQLITE
           : p->dialect == DIALECT_ORACLE   ? UD_SQL_ORACLE
           : p->dialect == DIALECT_DB2      ? UD_SQL_DB2
           : p->dialect == DIALECT_DUCKDB   ? UD_SQL_DUCKDB
                                            : UD_SQL_CANONICAL;
}
static ud_status write_batch(void *state, ud_string tenant, const ud_write_options *o,
                             const ud_batch *batch, const uint8_t digest[32]) {
    odbc_state *p = state;
    if (p->cursor)
        return UD_BUSY;
    int own = !p->transaction;
    ud_status s = own ? ud_odbc_begin(p, UD_ISOLATION_SERIALIZABLE) : UD_OK;
    ud_commit_receipt previous = {0};
    if (!s)
        s = receipt(p, tenant, o->job, o->batch_id, &previous);
    int existing = previous.committed;
    if (!s && existing &&
        (memcmp(previous.digest, digest, 32) ||
         previous.source_position.length != o->source_position.length ||
         memcmp(previous.source_position.data, o->source_position.data, o->source_position.length)))
        s = UD_CORRUPT;
    p->host.deallocate((void *)previous.source_position.data);
    odbc_state *bulk = NULL;
    ud_string stage = {0};
    int native = !s && !existing && o->mode == UD_WRITE_APPEND &&
                 o->load_mode != UD_LOAD_PARAMETERS && (p->capabilities & UD_CONNECT_NATIVE_BULK);
#ifdef UD_WITH_MSSQL_BCP
    if (native) {
        s = ud_mssql_bulk_stage(p, o, batch, digest, &bulk, &stage);
        if (s == UD_UNSUPPORTED && o->load_mode == UD_LOAD_AUTO) {
            s = UD_OK;
            native = 0;
        }
        ud_string apply = {0};
        if (!s && native)
            s = p->host.compile_stage(p->host.context, UD_SQL_SQLSERVER, o, batch, stage,
                                      UD_SQL_STAGE_APPLY, &apply);
        uint64_t affected = 0;
        if (!s && native)
            s = command(p, apply, NULL, &affected);
        if (!s && native && affected != p->host.rows(batch))
            s = UD_FENCED;
        p->host.deallocate((void *)apply.data);
    }
#endif
    uint32_t width = o->mode == UD_WRITE_DELETE ? o->key_count : p->host.columns(batch);
    /* SQL Server's prepared RPC also carries the handle, SQL and declarations.
     * Reserve their wire parameters before deriving a deterministic row chunk. */
    uint32_t budget = p->host.parameter_limit(native_dialect(p));
    if (p->dialect == DIALECT_MSSQL && budget >= 4)
        budget -= 4;
    uint32_t limit = width ? budget / width : 0;
    if (!limit)
        s = UD_RESOURCE;
    if (limit > 1000)
        limit = 1000;
    for (uint32_t row = 0; !s && !existing && !native && row < p->host.rows(batch);) {
        uint32_t count = o->mode == UD_WRITE_CHANGESET ? 1 : p->host.rows(batch) - row;
        if (count > limit)
            count = limit;
        ud_sql_statement plan = {0};
        SQLHSTMT stmt = SQL_NULL_HSTMT;
        int array = p->array_parameters && o->mode == UD_WRITE_APPEND;
        s = p->host.compile_write(p->host.context, native_dialect(p), UD_SQL_PARAMETER_QUESTION, o,
                                  batch, row, array ? 1 : count, &plan);
        if (!s)
            s = prepare(p, plan.sql, &stmt);
        uint64_t affected = 0;
        if (!s)
            s = execute_bound(p, stmt, batch, 0, array ? count : 1, NULL, plan.parameter_count,
                              &affected, o, &plan);
        if (!s && o->mode == UD_WRITE_CHANGESET && o->operations[row] != UD_CHANGE_UPSERT &&
            affected != 1)
            s = p->host.report(p->host.context, UD_FENCED, "changeset preimage conflict");
        statement_release(p, stmt);
        p->host.statement_clear(&plan);
        row += count;
    }
    if (!s && !existing) {
        ud_field f[] = {{.name = "tenant", .name_length = 6, .type = UD_UTF8},
                        {.name = "job", .name_length = 3, .type = UD_UTF8},
                        {.name = "batch", .name_length = 5, .type = UD_UTF8},
                        {.name = "digest", .name_length = 6, .type = UD_BINARY},
                        {.name = "position", .name_length = 8, .type = UD_UTF8, .nullable = 1}};
        ud_value v[] = {
            {.type = UD_UTF8, .text = tenant.data, .text_length = tenant.length},
            {.type = UD_UTF8, .text = o->job.data, .text_length = o->job.length},
            {.type = UD_UTF8, .text = o->batch_id.data, .text_length = o->batch_id.length},
            {.type = UD_BINARY, .text = (const char *)digest, .text_length = 32},
            {.type = UD_UTF8,
             .text = o->source_position.data,
             .text_length = o->source_position.length,
             .is_null = p->dialect == DIALECT_ORACLE && !o->source_position.length}};
        ud_batch *b = NULL;
        s = p->host.batch_create(p->host.context, f, 5, 1, &b);
        if (!s)
            s = p->host.append(b, v, 5);
        const char *insert =
            "INSERT INTO ud_sdk_receipts_v10(tenant,job,batch_id,digest,source_position) "
            "VALUES(?,?,?,?,?)";
        if (!s)
            s = command(p, (ud_string){insert, (uint32_t)strlen(insert)}, b, NULL);
        p->host.batch_destroy(b);
    }
    if (!s)
        s = ud_odbc_control(p);
    if (own) {
        if (!s)
            s = ud_odbc_commit(p);
        else if (ud_odbc_rollback(p) != UD_OK)
            s = UD_COMMIT_UNKNOWN;
    }
    if (bulk && (!own || p->transaction)) {
        bulk->bulk_next = p->bulk_sessions;
        p->bulk_sessions = bulk;
    } else if (bulk)
        ud_odbc_destroy(bulk);
    p->host.deallocate((void *)stage.data);
    return s;
}
static ud_status metadata(void *state, const ud_metadata_options *o, ud_batch **out) {
    odbc_state *p = state;
    *out = NULL;
    if (o->kind == UD_METADATA_COLUMNS)
        return ud_odbc_discover(p, o->parent, out);
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    if (!SQL_SUCCEEDED(statement_allocate(p, &stmt)))
        return UD_RESOURCE;
    SQLRETURN rc;
    if (o->kind == UD_METADATA_CATALOGS)
        rc = SQLTablesA(stmt, (SQLCHAR *)SQL_ALL_CATALOGS, SQL_NTS, (SQLCHAR *)"", 0, (SQLCHAR *)"",
                        0, NULL, 0);
    else if (o->kind == UD_METADATA_SCHEMAS)
        rc = SQLTablesA(stmt, (SQLCHAR *)"", 0, (SQLCHAR *)SQL_ALL_SCHEMAS, SQL_NTS, (SQLCHAR *)"",
                        0, NULL, 0);
    else if (o->kind == UD_METADATA_TABLES)
        rc = SQLTablesA(stmt, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR *)"TABLE,VIEW", SQL_NTS);
    else if (o->kind == UD_METADATA_PRIMARY_KEYS)
        rc = SQLPrimaryKeysA(stmt, NULL, 0, NULL, 0, (SQLCHAR *)o->parent.data,
                             (SQLSMALLINT)o->parent.length);
    else if (o->kind == UD_METADATA_FOREIGN_KEYS)
        rc = SQLForeignKeysA(stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0, NULL, 0,
                             (SQLCHAR *)o->parent.data, (SQLSMALLINT)o->parent.length);
    else if (o->kind == UD_METADATA_INDEXES)
        rc = SQLStatisticsA(stmt, NULL, 0, NULL, 0, (SQLCHAR *)o->parent.data,
                            (SQLSMALLINT)o->parent.length, SQL_INDEX_ALL, SQL_ENSURE);
    else {
        statement_release(p, stmt);
        return UD_UNSUPPORTED;
    }
    ud_status s = SQL_SUCCEEDED(rc) ? UD_OK : failure(p, SQL_HANDLE_STMT, stmt);
    SQLSMALLINT columns = 0;
    if (!s && !SQL_SUCCEEDED(SQLNumResultCols(stmt, &columns)))
        s = failure(p, SQL_HANDLE_STMT, stmt);
    ud_field *f = p->host.allocate(p->host.context, (size_t)(columns + 1) * sizeof(*f));
    char(*names)[256] = p->host.allocate(p->host.context, (size_t)(columns + 1) * 256);
    if (!f || !names)
        s = UD_RESOURCE;
    for (int i = 0; !s && i < columns; i++) {
        SQLSMALLINT n, type, scale, nullable;
        SQLULEN size;
        if (!SQL_SUCCEEDED(SQLDescribeColA(stmt, (SQLUSMALLINT)i + 1, (SQLCHAR *)names[i], 256, &n,
                                           &type, &size, &scale, &nullable)))
            s = failure(p, SQL_HANDLE_STMT, stmt);
        if (!s)
            f[i] = (ud_field){
                .name = names[i], .name_length = (uint32_t)n, .type = UD_UTF8, .nullable = 1};
    }
    if (!s)
        s = p->host.batch_create(p->host.context, f, (uint32_t)columns, o->limit, out);
    ud_value *v = p->host.allocate(p->host.context, (size_t)(columns + 1) * sizeof(*v));
    char **owned = p->host.allocate(p->host.context, (size_t)(columns + 1) * sizeof(*owned));
    if (!v || !owned)
        s = UD_RESOURCE;
    uint64_t at = 0;
    while (!s && p->host.rows(*out) < o->limit) {
        s = ud_odbc_control(p);
        if (s)
            break;
        rc = SQLFetch(stmt);
        if (rc == SQL_NO_DATA)
            break;
        if (!SQL_SUCCEEDED(rc)) {
            s = failure(p, SQL_HANDLE_STMT, stmt);
            break;
        }
        if (at++ < o->offset)
            continue;
        for (int i = 0; !s && i < columns; i++) {
            uint32_t n;
            int null;
            s = get_cell(p, stmt, (uint32_t)i, 0, owned + i, &n, &null);
            v[i] = (ud_value){.type = UD_UTF8, .is_null = null, .text = owned[i], .text_length = n};
        }
        if (!s)
            s = p->host.append(*out, v, (uint32_t)columns);
        for (int i = 0; i < columns; i++) {
            p->host.deallocate(owned[i]);
            owned[i] = NULL;
        }
    }
    p->host.deallocate(v);
    p->host.deallocate(owned);
    p->host.deallocate(f);
    p->host.deallocate(names);
    statement_release(p, stmt);
    if (s) {
        p->host.batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_odbc_execute(void *state, const ud_read_options *o, uint64_t *processed) {
    return command(state, o->sql, o->parameters, processed);
}
static ud_status test(void *state) {
    odbc_state *p = state;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    ud_status s = prepare(p, (ud_string){"SELECT 1", 8}, &stmt);
    if (!s && !SQL_SUCCEEDED(SQLExecute(stmt)))
        s = failure(p, SQL_HANDLE_STMT, stmt);
    if (!s && !SQL_SUCCEEDED(SQLFetch(stmt)))
        s = failure(p, SQL_HANDLE_STMT, stmt);
    if (stmt)
        statement_release(p, stmt);
    return s;
}
void ud_odbc_cancel(void *state) {
    odbc_state *p = state;
    atomic_store(&p->cancelled, 1);
    pthread_mutex_lock(&p->handles_lock);
    ud_mssql_cdc_cancel(p);
    for (uint32_t i = 0; i < 16; i++)
        if (p->handles[i])
            SQLCancelHandle(SQL_HANDLE_STMT, p->handles[i]);
    pthread_mutex_unlock(&p->handles_lock);
}
static void set_control(void *state, void *user, ud_status (*check)(void *)) {
    odbc_state *p = state;
    p->check_user = user;
    p->check = check;
    p->host.control_user = user;
    p->host.check_io = check;
    atomic_store(&p->cancelled, 0);
}
static ud_status reset(void *state) {
    odbc_state *p = state;
    if (p->cursor || p->transaction || p->cdc || p->broken || p->bulk_sessions)
        return UD_CORRUPT;
    atomic_store(&p->cancelled, 0);
    if (p->dialect == DIALECT_POSTGRES)
        return ud_odbc_command(p, "DISCARD ALL");
    if (p->dialect == DIALECT_MSSQL) {
        SQLRETURN rc = SQLSetConnectAttr(p->connection, SQL_ATTR_RESET_CONNECTION,
                                         (SQLPOINTER)(uintptr_t)SQL_RESET_CONNECTION_YES, 0);
        return SQL_SUCCEEDED(rc) ? UD_OK : failure(p, SQL_HANDLE_DBC, p->connection);
    }
    return UD_UNSUPPORTED;
}
static ud_status record_diagnostic(void *state, ud_string *out) {
    odbc_state *p = state;
    uint32_t length = (uint32_t)strlen(p->sqlstate);
    *out = (ud_string){length ? plugin_string(&p->host, p->sqlstate, length) : NULL, length};
    if (length && !out->data)
        return UD_RESOURCE;
    p->sqlstate[0] = 0;
    return UD_OK;
}
static uint64_t capabilities(void *state) {
    return ((odbc_state *)state)->capabilities;
}
void ud_odbc_destroy(void *state) {
    odbc_state *p = state;
    if (!p)
        return;
    ud_mssql_cdc_close(p);
    ud_odbc_read_close(p);
    if (p->transaction)
        SQLEndTran(SQL_HANDLE_DBC, p->connection, SQL_ROLLBACK);
    while (p->bulk_sessions) {
        odbc_state *bulk = p->bulk_sessions;
        p->bulk_sessions = bulk->bulk_next;
        ud_odbc_destroy(bulk);
    }
    if (p->connection) {
        SQLDisconnect(p->connection);
        SQLFreeHandle(SQL_HANDLE_DBC, p->connection);
    }
    if (p->environment)
        SQLFreeHandle(SQL_HANDLE_ENV, p->environment);
    pthread_mutex_destroy(&p->handles_lock);
    p->host.deallocate(p->configuration);
    p->host.deallocate(p);
}
static int contains(const char *text, const char *word) {
    for (; *text; text++) {
        const char *a = text, *b = word;
        while (*a && *b) {
            char x = *a >= 'A' && *a <= 'Z' ? (char)(*a - 'A' + 'a') : *a,
                 y = *b >= 'A' && *b <= 'Z' ? (char)(*b - 'A' + 'a') : *b;
            if (x != y)
                break;
            a++;
            b++;
        }
        if (!*b)
            return 1;
    }
    return 0;
}
static ud_status initialize(const ud_host_api *h, const char *configuration, uint32_t length,
                            int bulk, void **out) {
    *out = NULL;
    odbc_state *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    p->bulk_session = bulk;
    p->configuration = plugin_string(h, configuration, length);
    p->configuration_length = length;
    if (!p->configuration) {
        h->deallocate(p);
        return UD_RESOURCE;
    }
    if (pthread_mutex_init(&p->handles_lock, NULL)) {
        h->deallocate(p);
        return UD_RESOURCE;
    }
    p->check = h->check_io;
    p->check_user = h->control_user;
    atomic_init(&p->cancelled, 0);
    ud_json *config = NULL;
    ud_status s = h->json_parse(h->context, configuration, length, &config, NULL);
    const char *connection = NULL, *mode = NULL, *timezone = NULL;
    uint32_t n = 0, a = 0, b = 0;
    const ud_json *value = !s ? h->json_find(config, "connectionString", 16) : NULL;
    if (!s)
        s = h->json_text(value, &connection, &n);
    if (!s && (h->json_kind(value) != UD_JSON_STRING || !n || memchr(connection, 0, n)))
        s = UD_SCHEMA;
    value = !s ? h->json_find(config, "mode", 4) : NULL;
    if (value)
        s = h->json_text(value, &mode, &a);
    if (!s && mode) {
        if (a == 4 && !memcmp(mode, "ansi", 4))
            p->ansi = 1;
        else if (a != 7 || memcmp(mode, "unicode", 7))
            s = UD_SCHEMA;
    }
    value = !s ? h->json_find(config, "timestampTimezone", 17) : NULL;
    if (value)
        s = h->json_text(value, &timezone, &b);
    if (!s && timezone) {
        if (b == 3 && !memcmp(timezone, "UTC", 3))
            p->utc = 1;
        else
            s = UD_UNSUPPORTED;
    }
    if (!s)
        s = ud_odbc_control(p);
    if (!s && !SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &p->environment)))
        s = UD_RESOURCE;
    if (!s && !SQL_SUCCEEDED(SQLSetEnvAttr(p->environment, SQL_ATTR_ODBC_VERSION,
                                           (SQLPOINTER)SQL_OV_ODBC3, 0)))
        s = failure(p, SQL_HANDLE_ENV, p->environment);
    if (!s && !SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, p->environment, &p->connection)))
        s = UD_RESOURCE;
    if (!s)
        SQLSetConnectAttr(p->connection, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)15, 0);
#ifdef UD_WITH_MSSQL_BCP
    if (!s && bulk &&
        !SQL_SUCCEEDED(SQLSetConnectAttr(p->connection, SQL_COPT_SS_BCP, (SQLPOINTER)SQL_BCP_ON,
                                         SQL_IS_INTEGER)))
        s = UD_UNSUPPORTED;
#else
    if (!s && bulk)
        s = UD_UNSUPPORTED;
#endif
    if (!s) {
        SQLRETURN rc;
        if (p->ansi)
            rc = SQLDriverConnectA(p->connection, NULL, (SQLCHAR *)connection, (SQLSMALLINT)n, NULL,
                                   0, NULL, SQL_DRIVER_NOPROMPT);
        else {
            SQLWCHAR *text = NULL;
            SQLINTEGER units;
            s = wide(p, connection, n, &text, &units);
            rc = !s ? SQLDriverConnectW(p->connection, NULL, text, (SQLSMALLINT)units, NULL, 0,
                                        NULL, SQL_DRIVER_NOPROMPT)
                    : SQL_ERROR;
            h->deallocate(text);
        }
        if (!s && !SQL_SUCCEEDED(rc))
            s = failure(p, SQL_HANDLE_DBC, p->connection);
    }
    if (!s) {
        SQLSMALLINT size = 0;
        SQLGetInfoA(p->connection, SQL_DBMS_NAME, p->dbms, sizeof(p->dbms), &size);
        SQLGetInfoA(p->connection, SQL_DBMS_VER, p->version, sizeof(p->version), &size);
        SQLGetInfoA(p->connection, SQL_DRIVER_VER, p->client, sizeof(p->client), &size);
        char quote[8] = {0};
        SQLGetInfoA(p->connection, SQL_IDENTIFIER_QUOTE_CHAR, quote, sizeof(quote), &size);
        p->quote = quote[0] == ' ' || !quote[0] ? '"' : quote[0];
        p->dialect = contains(p->dbms, "Microsoft")                               ? DIALECT_MSSQL
                     : contains(p->dbms, "MariaDB") || contains(p->dbms, "MySQL") ? DIALECT_MYSQL
                     : contains(p->dbms, "PostgreSQL")                            ? DIALECT_POSTGRES
                     : contains(p->dbms, "SQLite")                                ? DIALECT_SQLITE
                     : contains(p->dbms, "Oracle")                                ? DIALECT_ORACLE
                     : contains(p->dbms, "DB2")                                   ? DIALECT_DB2
                     : contains(p->dbms, "DuckDB")                                ? DIALECT_DUCKDB
                                                                                  : DIALECT_GENERIC;
        SQLUSMALLINT transactions = SQL_TC_NONE, prepare_function = SQL_FALSE;
        SQLUINTEGER arrays = SQL_PARC_NO_BATCH;
        SQLGetInfo(p->connection, SQL_TXN_CAPABLE, &transactions, sizeof(transactions), &size);
        SQLGetInfo(p->connection, SQL_PARAM_ARRAY_ROW_COUNTS, &arrays, sizeof(arrays), &size);
        SQLGetFunctions(p->connection, SQL_API_SQLBINDPARAMETER, &prepare_function);
        p->array_parameters = prepare_function == SQL_TRUE && arrays == SQL_PARC_BATCH;
        p->capabilities =
            UD_CONNECT_READ | UD_CONNECT_EXECUTE_SQL | UD_CONNECT_METADATA | UD_CONNECT_PREPARED;
        if (transactions != SQL_TC_NONE && p->dialect != DIALECT_GENERIC) {
            p->capabilities |= UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE |
                               UD_CONNECT_TRANSACTION | UD_CONNECT_RECEIPTS | UD_CONNECT_PROVISION |
                               UD_CONNECT_CHANGESET | UD_CONNECT_BATCH_WRITE;
            if (p->array_parameters)
                p->capabilities |= UD_CONNECT_PARAMETER_ARRAY;
        }
        if (p->dialect == DIALECT_POSTGRES || p->dialect == DIALECT_MSSQL)
            p->capabilities |= UD_CONNECT_POOL;
        if (p->dialect == DIALECT_MSSQL) {
#ifdef UD_WITH_MSSQL_BCP
            char driver_name[256] = {0};
            if (SQL_SUCCEEDED(SQLGetInfoA(p->connection, SQL_DRIVER_NAME, driver_name,
                                          sizeof(driver_name), &size)) &&
                contains(driver_name, "msodbcsql"))
                p->capabilities |= UD_CONNECT_NATIVE_BULK;
#endif
            SQLUINTEGER encrypted = 0, trust = 1;
            if (!SQL_SUCCEEDED(
                    SQLGetConnectAttr(p->connection, 1223, &encrypted, sizeof(encrypted), NULL)) ||
                !SQL_SUCCEEDED(
                    SQLGetConnectAttr(p->connection, 1228, &trust, sizeof(trust), NULL)) ||
                !encrypted || trust)
                s = UD_AUTH;
        }
    }
    if (!s && p->dialect == DIALECT_MSSQL) {
        int enabled = 0;
        s = ud_mssql_cdc_enabled(p, &enabled);
        if (!s && enabled)
            p->capabilities |= UD_CONNECT_CDC | UD_CONNECT_NATIVE_CDC | UD_CONNECT_SNAPSHOT;
    }
    h->json_destroy(config);
    if (s) {
        ud_odbc_destroy(p);
        return s;
    }
    *out = p;
    return UD_OK;
}
ud_status ud_odbc_initialize(const ud_host_api *host, const char *configuration, uint32_t length,
                             void **out) {
    return initialize(host, configuration, length, 0, out);
}
ud_status ud_odbc_initialize_bulk(const ud_host_api *host, const char *configuration,
                                  uint32_t length, void **out) {
    return initialize(host, configuration, length, 1, out);
}
static int32_t session_dialect(void *state) {
    return native_dialect(state);
}
static const ud_connector_api connector = {
    .struct_size = sizeof(ud_connector_api),
    .abi_version = UD_ABI_VERSION,
    .capabilities = UD_CONNECT_READ | UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE |
                    UD_CONNECT_EXECUTE_SQL | UD_CONNECT_TRANSACTION | UD_CONNECT_RECEIPTS |
                    UD_CONNECT_METADATA | UD_CONNECT_POOL | UD_CONNECT_PREPARED |
                    UD_CONNECT_BATCH_WRITE | UD_CONNECT_PROVISION | UD_CONNECT_CHANGESET |
                    UD_CONNECT_CDC | UD_CONNECT_NATIVE_CDC | UD_CONNECT_SNAPSHOT |
                    UD_CONNECT_NATIVE_BULK | UD_CONNECT_PARAMETER_ARRAY,
    .test = test,
    .discover = ud_odbc_discover,
    .read_open = ud_odbc_read_open,
    .read_next = ud_odbc_read_next,
    .read_close = ud_odbc_read_close,
    .write = write_batch,
    .receipt = receipt,
    .cancel = ud_odbc_cancel,
    .execute = ud_odbc_execute,
    .begin = ud_odbc_begin,
    .commit = ud_odbc_commit,
    .rollback = ud_odbc_rollback,
    .metadata = metadata,
    .set_control = set_control,
    .reset_session = reset,
    .session_capabilities = capabilities,
    .record_diagnostic = record_diagnostic,
    .provision = provision,
    .parameter_style = UD_SQL_PARAMETER_QUESTION,
    .metadata_dialect = UD_SQL_CANONICAL,
    .session_dialect = session_dialect,
    .cdc_open = ud_mssql_cdc_open,
    .cdc_next = ud_mssql_cdc_next,
    .cdc_confirm = ud_mssql_cdc_confirm,
    .cdc_close = ud_mssql_cdc_close};
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t requested, ud_plugin_api *api) {
    if (requested != UD_PLUGIN_ABI || !api || api->struct_size != sizeof(*api))
        return UD_INCOMPATIBLE;
    *api = (ud_plugin_api){.struct_size = sizeof(*api),
                           .abi_version = UD_PLUGIN_ABI,
                           .id = "odbc",
                           .version = UD_SDK_VERSION,
                           .roles = UD_PLUGIN_CONNECTOR,
                           .capabilities = UD_CAP_READ | UD_CAP_WRITE | UD_CAP_SQL |
                                           UD_CAP_TRANSACTION | UD_CAP_THREAD_SAFE,
                           .initialize = ud_odbc_initialize,
                           .destroy = ud_odbc_destroy,
                           .shutdown = plugin_shutdown,
                           .connector = &connector};
    return UD_OK;
}
