#ifdef _WIN32
#include <winsock2.h>
#else
#include <poll.h>
#endif
#include "postgresql_metadata.h"
#include "common.h"
#include "postgresql_cdc.h"
#include <libpq-fe.h>
#include <stdatomic.h>
#include "../components/foundation/thread.h"
static const ud_connector_api connector;
typedef struct {
    ud_host_api host;
    PGconn *db;
    ud_pg_cdc *cdc;
    char *configuration;
    PGcancelConn *cancel;
    ud_mutex ownership;
    ud_batch *schema;
    uint32_t batch_size;
    ud_read_options record_options;
    int reading, read_cursor_declared, finished, scalar;
    atomic_int cancelled;
    void *control_user;
    ud_status (*check)(void *);
    ud_status control_status;
    int transaction, cleaning, broken;
    int prepared_enabled;
    char branch_id[65];
    char record_code[6];
} postgres_state;
static void interrupt_result(postgres_state *);
static ud_status control(postgres_state *p) {
    if (p->cleaning)
        return UD_OK;
    if (p->check) {
        ud_status status = p->check(p->control_user);
        if (status)
            return status;
    }
    return atomic_load(&p->cancelled) || p->host.cancelled(p->host.context) ? UD_CANCELLED : UD_OK;
}
static void set_control(void *state, void *user, ud_status (*check)(void *)) {
    postgres_state *p = state;
    p->control_user = user;
    p->check = check;
    p->control_status = UD_OK;
    atomic_store(&p->cancelled, 0);
}
/* PQgetResult may block even on a nonblocking connection. Every result,
 * including single-row cursor results, must pass this readiness check. */
static int wait_result(postgres_state *p) {
    p->control_status = control(p);
    if (p->control_status) {
        interrupt_result(p);
        return 0;
    }
    if (p->broken)
        return 0;
    int flushing = PQflush(p->db);
    if (flushing < 0) {
        p->broken = 1;
        return 0;
    }
    uint32_t cleanup_waits = 0;
    while (flushing || PQisBusy(p->db)) {
        if (p->cleaning && ++cleanup_waits > 200) {
            p->control_status = UD_TIMEOUT;
            p->broken = 1;
            return 0;
        }
        p->control_status = control(p);
        if (p->control_status) {
            interrupt_result(p);
            return 0;
        }
        int socket = PQsocket(p->db);
        if (socket < 0) {
            p->broken = 1;
            return 0;
        }
#ifdef _WIN32
        fd_set reads, writes;
        FD_ZERO(&reads);
        FD_ZERO(&writes);
        FD_SET((SOCKET)socket, &reads);
        if (flushing)
            FD_SET((SOCKET)socket, &writes);
        struct timeval timeout = {0, 25000};
        int ready = select(socket + 1, &reads, flushing ? &writes : NULL, NULL, &timeout);
#else
        struct pollfd fd = {socket, (short)(POLLIN | (flushing ? POLLOUT : 0)), 0};
        int ready = poll(&fd, 1, 25);
#endif
        if (ready < 0 || (ready && !PQconsumeInput(p->db))) {
            p->broken = 1;
            return 0;
        }
        if (ready && flushing)
            flushing = PQflush(p->db);
        if (flushing < 0) {
            p->broken = 1;
            return 0;
        }
    }
    return 1;
}
/* The I/O owner sends cancellation and drains the server acknowledgement before
 * retiring the session. Merely closing a socket leaves server-side work alive. */
static void interrupt_result(postgres_state *p) {
    ud_status interrupted = p->control_status;
    PostgresPollingStatusType progress = PGRES_POLLING_FAILED;
    if (p->cancel) {
        PQcancelReset(p->cancel);
        if (PQcancelStart(p->cancel))
            progress = PQcancelPoll(p->cancel);
    }
    for (uint32_t waits = 0;
         waits < 200 && progress != PGRES_POLLING_OK && progress != PGRES_POLLING_FAILED; waits++) {
        int socket = PQcancelSocket(p->cancel);
        if (socket < 0)
            break;
#ifdef _WIN32
        fd_set reads, writes;
        FD_ZERO(&reads);
        FD_ZERO(&writes);
        FD_SET((SOCKET)socket, &reads);
        FD_SET((SOCKET)socket, &writes);
        struct timeval timeout = {0, 25000};
        int ready = select(socket + 1, &reads, progress == PGRES_POLLING_WRITING ? &writes : NULL,
                           NULL, &timeout);
#else
        struct pollfd fd = {socket,
                            (short)(POLLIN | (progress == PGRES_POLLING_WRITING ? POLLOUT : 0)), 0};
        int ready = poll(&fd, 1, 25);
#endif
        if (ready < 0)
            break;
        if (ready || progress == PGRES_POLLING_ACTIVE)
            progress = PQcancelPoll(p->cancel);
    }
    if (progress == PGRES_POLLING_OK) {
        p->cleaning = 1;
        p->broken = 0;
        PGresult *result;
        while (wait_result(p) && (result = PQgetResult(p->db)) != NULL)
            PQclear(result);
        p->cleaning = 0;
    }
    p->control_status = interrupted;
    p->broken = 1;
}
/* One bounded cursor/session, socket readiness with cancellation/deadline checks.
 * No separate SQL executor or buffered full-result fallback is introduced. */
static PGresult *query(postgres_state *p, const char *sql, int n, const Oid *types,
                       const char *const *values, const int *lengths, const int *formats,
                       int result_format) {
    p->control_status = control(p);
    if (p->control_status || p->broken)
        return NULL;
    if (!PQsendQueryParams(p->db, sql, n, types, values, lengths, formats, result_format))
        return NULL;
    if (!wait_result(p))
        return NULL;
    /* A completed query drains its protocol; interrupted sessions are retired. */
    PGresult *result = PQgetResult(p->db), *extra;
    /* COPY hands protocol ownership to the streaming transport. The server
     * cannot complete this command until the client sends/consumes its data;
     * attempting the ordinary result drain here deadlocks both sides. */
    if (result &&
        (PQresultStatus(result) == PGRES_COPY_IN || PQresultStatus(result) == PGRES_COPY_OUT ||
         PQresultStatus(result) == PGRES_COPY_BOTH))
        return result;
    while (wait_result(p) && (extra = PQgetResult(p->db)) != NULL) {
        PQclear(result);
        result = extra;
    }
    if (p->control_status || p->broken) {
        PQclear(result);
        return NULL;
    }
    return result;
}
static ud_status pg_failure(postgres_state *p, const PGresult *result) {
    const char *code = result ? PQresultErrorField(result, PG_DIAG_SQLSTATE) : NULL;
    memset(p->record_code, 0, sizeof(p->record_code));
    if (code)
        memcpy(p->record_code, code, 5);
    ud_status status = PQstatus(p->db) != CONNECTION_OK ? UD_TRANSIENT : UD_IO;
    if (p->control_status)
        status = p->control_status;
    else if (atomic_load(&p->cancelled))
        status = UD_CANCELLED;
    else if (code) {
        if (!strcmp(code, "42501") || !strncmp(code, "28", 2))
            status = UD_AUTH;
        else if (!strcmp(code, "40001") || !strcmp(code, "40P01") || !strncmp(code, "08", 2))
            status = UD_TRANSIENT;
        else if (!strcmp(code, "22003"))
            status = UD_OVERFLOW;
        else if (!strcmp(code, "22001"))
            status = UD_TRUNCATED;
        else if (!strncmp(code, "22", 2))
            status = UD_PARSE;
        else if (!strncmp(code, "23", 2))
            status = UD_QUALITY;
        else if (!strncmp(code, "42", 2))
            status = UD_SCHEMA;
        else if (!strcmp(code, "57014"))
            status = UD_CANCELLED;
    }
    /* Database diagnostics can echo literal data / credentials; expose classification only. */
    return p->host.report(p->host.context, status,
                          code ? code : "PostgreSQL connection / protocol failure");
}
static ud_status record_diagnostic(void *state, ud_string *out) {
    postgres_state *p = state;
    uint32_t length = (uint32_t)strlen(p->record_code);
    *out = (ud_string){length ? plugin_string(&p->host, p->record_code, length) : NULL, length};
    if (length && !out->data)
        return UD_RESOURCE;
    memset(p->record_code, 0, sizeof(p->record_code));
    return UD_OK;
}
static ud_status command(postgres_state *p, const char *sql) {
    PGresult *r = query(p, sql, 0, NULL, NULL, NULL, NULL, 0);
    ud_status s = r && PQresultStatus(r) == PGRES_COMMAND_OK ? UD_OK : pg_failure(p, r);
    PQclear(r);
    return s;
}
static ud_status session_settings(postgres_state *p) {
    ud_status status = command(p, "SET TIME ZONE 'UTC'");
    if (!status)
        status = command(p, "SET DateStyle TO ISO, YMD");
    if (!status)
        status = command(p, "SET extra_float_digits TO 3");
    if (!status)
        status = command(p, "SET IntervalStyle TO iso_8601");
    return status;
}
static ud_status initialize(const ud_host_api *host, const char *config, uint32_t length,
                            void **out) {
    *out = NULL;
    postgres_state *p = host->allocate(host->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *host;
    ud_mutex_init(&p->ownership);
    p->control_user = host->control_user;
    p->check = host->check_io;
    atomic_init(&p->cancelled, 0);
    char *copy = plugin_string(host, config, length);
    if (!copy) {
        ud_mutex_destroy(&p->ownership);
        host->deallocate(p);
        return UD_RESOURCE;
    }
    const char *keys[] = {"dbname", "connect_timeout", "application_name", NULL};
    const char *values[] = {copy, "10", "UniversalDataSDK/" UD_SDK_VERSION, NULL};
    p->configuration = plugin_string(host, config, length);
    if (!p->configuration) {
        host->deallocate(copy);
        ud_mutex_destroy(&p->ownership);
        host->deallocate(p);
        return UD_RESOURCE;
    }
    p->db = PQconnectStartParams(keys, values, 1);
    host->deallocate(copy);
    ud_status s = !p->db ? UD_RESOURCE : UD_OK;
    PostgresPollingStatusType progress = PGRES_POLLING_WRITING;
    while (!s && progress != PGRES_POLLING_OK) {
        s = control(p);
        if (s)
            break;
        int socket = PQsocket(p->db);
        if (socket < 0) {
            s = UD_IO;
            break;
        }
#ifdef _WIN32
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET((SOCKET)socket, &fds);
        struct timeval timeout = {0, 25000};
        int ready = select(socket + 1, progress == PGRES_POLLING_READING ? &fds : NULL,
                           progress == PGRES_POLLING_WRITING ? &fds : NULL, NULL, &timeout);
#else
        struct pollfd fd = {socket, progress == PGRES_POLLING_READING ? POLLIN : POLLOUT, 0};
        int ready = poll(&fd, 1, 25);
#endif
        if (ready < 0) {
            s = UD_IO;
            break;
        }
        if (ready)
            progress = PQconnectPoll(p->db);
        if (progress == PGRES_POLLING_FAILED)
            s = UD_IO;
    }
    if (!s && PQsetnonblocking(p->db, 1))
        s = UD_IO;
    if (!s && PQsetClientEncoding(p->db, "UTF8"))
        s = UD_SCHEMA;
    if (!s)
        s = session_settings(p);
    if (!s) {
        PGresult *probe = query(p, "SHOW max_prepared_transactions", 0, NULL, NULL, NULL, NULL, 0);
        if (!probe || PQresultStatus(probe) != PGRES_TUPLES_OK || PQntuples(probe) != 1)
            s = pg_failure(p, probe);
        else {
            char *end = NULL;
            unsigned long limit = strtoul(PQgetvalue(probe, 0, 0), &end, 10);
            if (!end || *end)
                s = UD_CORRUPT;
            else
                p->prepared_enabled = limit != 0;
        }
        PQclear(probe);
    }
    if (!s) {
        p->cancel = PQcancelCreate(p->db);
        if (!p->cancel)
            s = UD_RESOURCE;
    }
    if (s) {
        if (p->db)
            PQfinish(p->db);
        if (p->cancel)
            PQcancelFinish(p->cancel);
        host->deallocate(p->configuration);
        ud_mutex_destroy(&p->ownership);
        host->deallocate(p);
        return host->report(host->context, s,
                            "PostgreSQL initialization failed; connection string is redacted");
    }
    *out = p;
    return UD_OK;
}
static ud_status test_connection(void *state) {
    postgres_state *p = state;
    if (p->broken)
        return UD_TRANSIENT;
    PGresult *r = query(p, "SELECT 1", 0, NULL, NULL, NULL, NULL, 0);
    ud_status s = r && PQresultStatus(r) == PGRES_TUPLES_OK ? UD_OK : pg_failure(p, r);
    PQclear(r);
    return s;
}
static ud_status table_name(postgres_state *p, ud_string table, char **out) {
    ud_string name = {0};
    ud_status status = p->host.table_identifier(p->host.context, UD_SQL_POSTGRESQL, table, &name);
    *out = status ? NULL : (char *)name.data;
    return status;
}
static ud_status discover(void *state, ud_string table, ud_batch **out) {
    postgres_state *p = state;
    char *quoted = NULL;
    ud_status status = table_name(p, table, &quoted);
    if (status)
        return status;
    const char *args[] = {quoted};
    PGresult *result = query(p, ud_pg_metadata_query(), 1, NULL, args, NULL, NULL, 0);
    p->host.deallocate(quoted);
    if (!result || PQresultStatus(result) != PGRES_TUPLES_OK)
        status = pg_failure(p, result);
    else
        status = ud_pg_metadata_decode(&p->host, result, out);
    PQclear(result);
    return status;
}
static ud_status close_read(void *state) {
    postgres_state *p = state;
    memset(&p->record_options, 0, sizeof(p->record_options));
    p->cleaning = 1;
    ud_status s = UD_OK;
    if (p->reading && !p->broken) {
        if (!p->transaction)
            s = command(p, "ROLLBACK");
        else if (p->read_cursor_declared)
            s = command(p, "CLOSE ud_sdk_cursor");
    }
    p->cleaning = 0;
    p->reading = 0;
    p->read_cursor_declared = 0;
    p->finished = 1;
    p->scalar = 0;
    p->host.batch_destroy(p->schema);
    p->schema = NULL;
    return s;
}
static int compatible_oid(ud_type t, Oid oid) {
    return t == UD_BOOL                           ? oid == 16
           : t == UD_INT64                        ? oid == 20 || oid == 21 || oid == 23
           : t == UD_FLOAT64                      ? oid == 700 || oid == 701
           : t == UD_UTF8                         ? oid == 18 || oid == 19 || oid == 25 ||
                                                         oid == 1042 || oid == 1043 || oid == 114 ||
                                                         oid == 2950 || oid == 3802
           : t == UD_DECIMAL128 || t == UD_UINT64 ? oid == 1700
           : t == UD_INTERVAL || t == UD_DURATION ? oid == 1186
           : t == UD_DATE32                       ? oid == 1082
           : t == UD_TIME64                       ? oid == 1083 || oid == 1266
           : t == UD_TIMESTAMP                    ? oid == 1114 || oid == 1184
           : t == UD_BINARY                       ? oid == 17
                                                  : 0;
}
static ud_type pg_result_type(Oid oid) {
    switch (oid) {
    case 16:
        return UD_BOOL;
    case 20:
    case 21:
    case 23:
        return UD_INT64;
    case 700:
    case 701:
        return UD_FLOAT64;
    case 17:
        return UD_BINARY;
    case 1082:
        return UD_DATE32;
    case 1083:
    case 1266:
        return UD_TIME64;
    case 1114:
    case 1184:
        return UD_TIMESTAMP;
    case 1186:
        return UD_INTERVAL;
    case 1700:
        return UD_DECIMAL128;
    case 18:
    case 19:
    case 25:
    case 1042:
    case 1043:
    case 114:
    case 2950:
    case 3802:
        return UD_UTF8;
    default:
        return UD_STRUCT;
    }
}
typedef struct pg_parameters {
    uint32_t count;
    const char **values;
    char **owned;
    Oid *types;
    int *lengths, *formats;
} pg_parameters;
static void parameters_clear(postgres_state *p, pg_parameters *v) {
    for (uint32_t i = 0; i < v->count; i++)
        p->host.deallocate(v->owned ? v->owned[i] : NULL);
    p->host.deallocate(v->values);
    p->host.deallocate(v->owned);
    p->host.deallocate(v->types);
    p->host.deallocate(v->lengths);
    p->host.deallocate(v->formats);
    memset(v, 0, sizeof(*v));
}
static ud_status parameters_allocate(postgres_state *p, uint32_t count, pg_parameters *v) {
    memset(v, 0, sizeof(*v));
    if (count > 65535)
        return UD_RESOURCE;
    if (!count)
        return UD_OK;
    ud_host_api *h = &p->host;
    v->count = count;
    v->values = h->allocate(h->context, count * sizeof(*v->values));
    v->owned = h->allocate(h->context, count * sizeof(*v->owned));
    v->types = h->allocate(h->context, count * sizeof(*v->types));
    v->lengths = h->allocate(h->context, count * sizeof(*v->lengths));
    v->formats = h->allocate(h->context, count * sizeof(*v->formats));
    if (!v->values || !v->owned || !v->types || !v->lengths || !v->formats) {
        parameters_clear(p, v);
        return UD_RESOURCE;
    }
    return UD_OK;
}
static ud_status parameter_value(postgres_state *p, const ud_value *value, pg_parameters *v,
                                 uint32_t i) {
    ud_host_api *h = &p->host;
    v->types[i] = value->type == UD_BOOL                                     ? 16
                  : value->type == UD_INT64                                  ? 20
                  : value->type == UD_FLOAT64                                ? 701
                  : value->type == UD_DECIMAL128 || value->type == UD_UINT64 ? 1700
                  : value->type == UD_INTERVAL || value->type == UD_DURATION ? 1186
                  : value->type == UD_BINARY                                 ? 17
                  : value->type == UD_DATE32                                 ? 1082
                  : value->type == UD_TIME64                                 ? 1083
                  : value->type == UD_TIMESTAMP                              ? 1184
                  : value->type == UD_UTF8                                   ? 25
                                                                             : 0;
    if (!v->types[i])
        return UD_UNSUPPORTED;
    if (value->is_null)
        return UD_OK;
    if (value->type == UD_BINARY) {
        if (value->text_length > INT_MAX)
            return UD_RESOURCE;
        v->values[i] = value->text ? value->text : "";
        v->formats[i] = 1;
        v->lengths[i] = (int)value->text_length;
        return UD_OK;
    }
    const char *data;
    uint32_t length;
    char text[256];
    if (value->type == UD_UTF8) {
        data = value->text;
        length = value->text_length;
        if (length && memchr(data, 0, length))
            return UD_PARSE;
    } else {
        uint32_t required;
        ud_status status = h->format_value(value, text, sizeof(text), &required);
        if (status)
            return status;
        data = text;
        length = required - 1;
    }
    v->owned[i] = plugin_string(h, data, length);
    if (!v->owned[i])
        return UD_RESOURCE;
    v->values[i] = v->owned[i];
    return UD_OK;
}
static ud_status parameters_create(postgres_state *p, const ud_batch *b, uint32_t row,
                                   pg_parameters *v) {
    uint32_t count = b ? p->host.columns(b) : 0;
    if (b && row >= p->host.rows(b))
        return UD_SCHEMA;
    ud_status status = parameters_allocate(p, count, v);
    for (uint32_t i = 0; !status && i < count; i++) {
        ud_value value;
        status = p->host.get(b, row, i, &value);
        if (!status)
            status = parameter_value(p, &value, v, i);
    }
    if (status)
        parameters_clear(p, v);
    return status;
}
static ud_status parameters_statement(postgres_state *p, const ud_write_options *options,
                                      const ud_batch *batch, const ud_sql_statement *plan,
                                      pg_parameters *v) {
    ud_status status = parameters_allocate(p, plan->parameter_count, v);
    for (uint32_t i = 0; !status && i < plan->parameter_count; i++) {
        ud_sql_parameter ref = plan->parameters[i];
        ud_value value;
        const ud_batch *source = ref.source == UD_SQL_BEFORE ? options->before : batch;
        status = p->host.get(source, ref.row, ref.column, &value);
        if (!status)
            status = parameter_value(p, &value, v, i);
    }
    if (status)
        parameters_clear(p, v);
    return status;
}

static ud_status open_read(void *state, const ud_read_options *options, const ud_batch *schema) {
    postgres_state *p = state;
    ud_host_api *h = &p->host;
    if (p->reading || options->sql.length > SIZE_MAX - 64)
        return UD_INVALID;
    atomic_store(&p->cancelled, 0);
    ud_status s =
        p->transaction ? UD_OK : command(p, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    if (s)
        return s;
    p->reading = 1;
    size_t length = (size_t)options->sql.length + 64;
    char *sql = h->allocate(h->context, length);
    if (!sql)
        s = UD_RESOURCE;
    else {
        const char *prefix = "DECLARE ud_sdk_cursor NO SCROLL CURSOR FOR ";
        size_t n = strlen(prefix);
        memcpy(sql, prefix, n);
        memcpy(sql + n, options->sql.data, options->sql.length);
        pg_parameters parameters = {0};
        s = parameters_create(p, options->parameters, options->parameter_row, &parameters);
        PGresult *prepared =
            !s ? query(p, sql, (int)parameters.count, parameters.types, parameters.values,
                       parameters.lengths, parameters.formats, 0)
               : NULL;
        if (!s && (!prepared || PQresultStatus(prepared) != PGRES_COMMAND_OK))
            s = pg_failure(p, prepared);
        if (!s)
            p->read_cursor_declared = 1;
        PQclear(prepared);
        parameters_clear(p, &parameters);
        h->deallocate(sql);
    }
    PGresult *r =
        !s ? query(p, "FETCH FORWARD 0 FROM ud_sdk_cursor", 0, NULL, NULL, NULL, NULL, 0) : NULL;
    if (!s && (!r || PQresultStatus(r) != PGRES_TUPLES_OK))
        s = pg_failure(p, r);
    uint32_t columns = 0;
    ud_field *fields = NULL;
    if (!s) {
        columns = (uint32_t)PQnfields(r);
        if (schema) {
            if (h->columns(schema) != columns)
                s = UD_SCHEMA;
            else
                s = plugin_fields(h, schema, &fields);
        } else {
            fields = h->allocate(h->context, columns * sizeof(*fields));
            if (!fields && columns)
                s = UD_RESOURCE;
            for (uint32_t j = 0; !s && j < columns; j++) {
                const char *name = PQfname(r, (int)j);
                ud_type type = pg_result_type(PQftype(r, (int)j));
                if (!name || type >= UD_STRUCT)
                    s = UD_UNSUPPORTED;
                else {
                    fields[j] = (ud_field){.name = name,
                                           .name_length = (uint32_t)strlen(name),
                                           .type = type,
                                           .nullable = 1};
                    if (type == UD_DECIMAL128 && PQfmod(r, (int)j) >= 4) {
                        int modifier = PQfmod(r, (int)j);
                        fields[j].precision = ((modifier - 4) >> 16) & 65535;
                        fields[j].scale = (((modifier - 4) & 2047) ^ 1024) - 1024;
                    }
                }
            }
        }
    }
    if (!s && options->result_mode == UD_READ_SCALAR && columns != 1)
        s = UD_SCHEMA;
    for (uint32_t j = 0; !s && j < columns; j++)
        if (strcmp(fields[j].name, PQfname(r, (int)j)) ||
            !compatible_oid(fields[j].type, PQftype(r, (int)j)))
            s = UD_SCHEMA;
    if (!s)
        s = plugin_schema_outputs(h, fields, columns, &p->schema, options->result_schema);
    h->deallocate(fields);
    PQclear(r);
    if (s) {
        close_read(p);
        return s;
    }
    p->scalar = options->result_mode == UD_READ_SCALAR;
    p->batch_size = p->scalar ? 1 : options->batch_size;
    p->record_options = *options;
    p->finished = 0;
    return UD_OK;
}
static ud_status parse_row(postgres_state *p, const PGresult *r, ud_batch *batch) {
    ud_host_api *h = &p->host;
    uint32_t columns = h->columns(p->schema);
    ud_value *values = h->allocate(h->context, columns * sizeof(*values));
    ud_value *raw = h->allocate(h->context, columns * sizeof(*raw));
    uint32_t bad = UINT32_MAX;
    ud_field *fields = NULL;
    void **binary = h->allocate(h->context, columns * sizeof(*binary));
    if (!values || !binary || !raw) {
        h->deallocate(values);
        h->deallocate(binary);
        h->deallocate(raw);
        return UD_RESOURCE;
    }
    ud_status s = PQnfields(r) != (int)columns ? UD_SCHEMA : UD_OK;
    if (!s)
        s = plugin_fields(h, p->schema, &fields);
    for (uint32_t j = 0; !s && j < columns; j++)
        raw[j] = (ud_value){.type = UD_BINARY,
                            .is_null = PQgetisnull(r, 0, (int)j),
                            .text = PQgetvalue(r, 0, (int)j),
                            .text_length = (uint32_t)PQgetlength(r, 0, (int)j)};
    ud_status conversion = UD_OK;
    for (uint32_t j = 0; !s && j < columns; j++) {
        ud_field field;
        h->field(p->schema, j, &field);
        if (strcmp(field.name, PQfname(r, (int)j)) ||
            !compatible_oid(field.type, PQftype(r, (int)j))) {
            s = UD_SCHEMA;
            break;
        }
        ud_value *v = values + j;
        v->type = field.type;
        v->precision = field.precision;
        v->scale = field.scale;
        if (PQgetisnull(r, 0, (int)j)) {
            v->is_null = 1;
            continue;
        }
        const char *text = PQgetvalue(r, 0, (int)j);
        uint32_t length = (uint32_t)PQgetlength(r, 0, (int)j);
        if (field.type == UD_BINARY) {
            size_t size = 0;
            binary[j] = PQunescapeBytea((const unsigned char *)text, &size);
            if (!binary[j] || size > UINT32_MAX)
                s = UD_RESOURCE;
            else {
                v->text = binary[j];
                v->text_length = (uint32_t)size;
            }
        } else
            s = ud_pg_decode_scalar(h, &field, text, length, v);
        if (plugin_record_conversion(s)) {
            if (!conversion) {
                conversion = s;
                bad = j;
            }
            s = UD_OK;
            v->is_absent = 1;
            v->is_null = 1;
        }
    }
    if (!s)
        s = plugin_record_finish(h, &p->record_options, batch, fields, values, raw, columns,
                                 conversion, bad);
    for (uint32_t j = 0; j < columns; j++)
        if (binary[j])
            PQfreemem(binary[j]);
    h->deallocate(values);
    h->deallocate(binary);
    h->deallocate(raw);
    h->deallocate(fields);
    return s;
}
static ud_status next_read(void *state, ud_batch **out) {
    postgres_state *p = state;
    ud_host_api *h = &p->host;
    *out = NULL;
    if (!p->reading || !p->schema)
        return UD_INVALID;
    if (p->finished)
        return UD_END;
    ud_status s = control(p);
    if (s)
        return s;
    ud_field *fields = NULL;
    s = plugin_fields(h, p->schema, &fields);
    if (!s)
        s = h->batch_create(h->context, fields, h->columns(p->schema), p->batch_size, out);
    h->deallocate(fields);
    if (s)
        return s;
    char sql[96];
    snprintf(sql, sizeof(sql), "FETCH FORWARD %u FROM ud_sdk_cursor", p->batch_size);
    if (!PQsendQueryParams(p->db, sql, 0, NULL, NULL, NULL, NULL, 0))
        s = pg_failure(p, NULL);
    else if (!PQsetSingleRowMode(p->db))
        s = UD_INCOMPATIBLE;
    PGresult *r;
    uint32_t fetched = 0;
    while (!s) {
        if (!wait_result(p)) {
            s = pg_failure(p, NULL);
            break;
        }
        r = PQgetResult(p->db);
        if (!r)
            break;
        ExecStatusType result = PQresultStatus(r);
        if (!s && result == PGRES_SINGLE_TUPLE) {
            fetched++;
            s = parse_row(p, r, *out);
        } else if (!s && result != PGRES_TUPLES_OK)
            s = pg_failure(p, r);
        PQclear(r);
    }
    /* A partial/failed cursor response must never re-enter the session pool. */
    if (s)
        p->broken = 1;
    if (s)
        return s;
    uint32_t rows = h->rows(*out);
    if (p->scalar && fetched)
        p->finished = 1;
    else if (fetched < p->batch_size)
        p->finished = 1;
    if (!rows && !fetched) {
        h->batch_destroy(*out);
        *out = NULL;
        return UD_END;
    }
    return UD_OK;
}
static void cancel_operation(void *state) {
    postgres_state *p = state;
    atomic_store(&p->cancelled, 1);
    ud_mutex_lock(&p->ownership);
    if (p->cdc)
        ud_pg_cdc_cancel(p->cdc);
    ud_mutex_unlock(&p->ownership);
}
static ud_status receipt(void *state, ud_string tenant, ud_string job, ud_string batch_id,
                         ud_commit_receipt *out) {
    postgres_state *p = state;
    ud_host_api *h = &p->host;
    memset(out, 0, sizeof(*out));
    ud_string strings[] = {tenant, job, batch_id};
    char *owned[3] = {0};
    ud_status s = UD_OK;
    for (int j = 0; !s && j < 3; j++) {
        owned[j] = plugin_string(h, strings[j].data, strings[j].length);
        if (!owned[j])
            s = UD_RESOURCE;
    }
    const char *args[] = {owned[0], owned[1], owned[2]};
    PGresult *r =
        !s ? query(p,
                   "SELECT encode(digest,'hex'),source_position FROM public.ud_sdk_receipts_v11 "
                   "WHERE tenant=$1 AND job=$2 AND batch_id=$3",
                   3, NULL, args, NULL, NULL, 0)
           : NULL;
    if (!s && (!r || PQresultStatus(r) != PGRES_TUPLES_OK))
        s = h->report(h->context, UD_COMMIT_UNKNOWN,
                      "receipt store is unavailable; source position must not advance");
    if (!s && PQntuples(r) > 1)
        s = UD_CORRUPT;
    if (!s && PQntuples(r) == 1) {
        const char *hex = PQgetvalue(r, 0, 0);
        if (PQgetisnull(r, 0, 0) || PQgetlength(r, 0, 0) != 64 || PQgetisnull(r, 0, 1))
            s = UD_CORRUPT;
        else {
            for (int j = 0; !s && j < 32; j++) {
                unsigned value = 0;
                for (int k = 0; k < 2; k++) {
                    unsigned char ch = (unsigned char)hex[j * 2 + k];
                    if (ch >= '0' && ch <= '9')
                        value = value * 16 + ch - '0';
                    else if (ch >= 'a' && ch <= 'f')
                        value = value * 16 + ch - 'a' + 10;
                    else
                        s = UD_CORRUPT;
                }
                out->digest[j] = (uint8_t)value;
            }
            uint32_t length = (uint32_t)PQgetlength(r, 0, 1);
            char *position = !s ? plugin_string(h, PQgetvalue(r, 0, 1), length) : NULL;
            if (!s && !position)
                s = UD_RESOURCE;
            if (!s) {
                out->source_position = (ud_string){position, length};
                out->committed = 1;
            }
        }
    }
    PQclear(r);
    for (int j = 0; j < 3; j++)
        h->deallocate(owned[j]);
    return s;
}
static ud_status validate_target(postgres_state *p, ud_string table, const ud_batch *batch) {
    ud_host_api *h = &p->host;
    char *name = NULL;
    ud_status named = table_name(p, table, &name);
    if (named)
        return named;
    const char *args[] = {name};
    PGresult *r = query(p,
                        "SELECT attname,atttypid::text,atttypmod::text FROM pg_attribute WHERE "
                        "attrelid=$1::regclass AND attnum>0 AND NOT attisdropped",
                        1, NULL, args, NULL, NULL, 0);
    h->deallocate(name);
    if (!r || PQresultStatus(r) != PGRES_TUPLES_OK) {
        ud_status s = pg_failure(p, r);
        PQclear(r);
        return s;
    }
    ud_status s = UD_OK;
    for (uint32_t j = 0; !s && j < h->columns(batch); j++) {
        ud_field field;
        h->field(batch, j, &field);
        int found = 0;
        for (int k = 0; k < PQntuples(r); k++)
            if (!strcmp(field.name, PQgetvalue(r, k, 0))) {
                found = 1;
                Oid oid = (Oid)strtoul(PQgetvalue(r, k, 1), NULL, 10);
                int modifier = (int)strtol(PQgetvalue(r, k, 2), NULL, 10);
                if (!compatible_oid(field.type, oid))
                    s = UD_SCHEMA;
                if (!s && field.type == UD_DECIMAL128 && modifier >= 4) {
                    int precision = ((modifier - 4) >> 16) & 65535;
                    int scale = (((modifier - 4) & 2047) ^ 1024) - 1024;
                    if (precision != field.precision || scale != field.scale)
                        s = UD_SCHEMA;
                }
                if (!s && field.type == UD_UTF8 && oid == 1043 && modifier >= 4) {
                    uint32_t limit = (uint32_t)(modifier - 4);
                    for (uint32_t row = 0; !s && row < h->rows(batch); row++) {
                        ud_value value;
                        h->get(batch, row, j, &value);
                        uint32_t characters = 0;
                        if (!value.is_null)
                            for (uint32_t b = 0; b < value.text_length; b++)
                                if (((unsigned char)value.text[b] & 192) != 128)
                                    characters++;
                        if (characters > limit)
                            s = UD_SCHEMA;
                    }
                }
            }
        if (!found)
            s = UD_SCHEMA;
    }
    PQclear(r);
    return s ? h->report(h->context, s, "target schema would narrow or coerce a typed value")
             : UD_OK;
}
/* COPY transports the same sealed typed Batch over the authenticated libpq
 * session. Receipt reservation and business writes stay in the same transaction. */
static ud_status copy_batch(postgres_state *p, const ud_write_options *options,
                            const ud_batch *batch, ud_bulk_reader *reader,
                            uint64_t *affected) {
    ud_host_api *h = &p->host;
    ud_string sql = {0};
    ud_status status = h->compile_bulk(h->context, UD_SQL_POSTGRESQL, options, batch, &sql);
    PGresult *result = !status ? query(p, sql.data, 0, NULL, NULL, NULL, NULL, 0) : NULL;
    h->deallocate((void *)sql.data);
    if (!status && (!result || PQresultStatus(result) != PGRES_COPY_IN))
        status = pg_failure(p, result);
    int copying = !status;
    PQclear(result);
    char buffer[65536];
    while (!status) {
        status = control(p);
        uint32_t length = 0;
        if (!status)
            status = h->bulk_read(reader, buffer, sizeof(buffer), &length);
        if (status == UD_END) {
            status = UD_OK;
            break;
        }
        if (status)
            break;
        int accepted;
        do {
            accepted = PQputCopyData(p->db, buffer, (int)length);
            if (!accepted && !wait_result(p)) {
                status = pg_failure(p, NULL);
                break;
            }
        } while (!accepted);
        if (!status && accepted < 0)
            status = pg_failure(p, NULL);
    }
    if (copying) {
        int finished;
        do {
            finished = PQputCopyEnd(p->db, status ? "Native bulk input failed" : NULL);
            if (!finished && !wait_result(p)) {
                if (!status)
                    status = pg_failure(p, NULL);
                break;
            }
        } while (!finished);
        if (finished < 0 && !status)
            status = pg_failure(p, NULL);
        while (wait_result(p) && (result = PQgetResult(p->db)) != NULL) {
            if (!status && PQresultStatus(result) != PGRES_COMMAND_OK)
                status = pg_failure(p, result);
            if (!status) {
                uint64_t count = strtoull(PQcmdTuples(result), NULL, 10);
                if (count != h->rows(batch))
                    status = UD_CORRUPT;
                else if (affected)
                    *affected += count;
            }
            PQclear(result);
        }
        if (!status && (p->control_status || p->broken))
            status = pg_failure(p, NULL);
    }
    return status;
}
/* Pipeline the original ordered statements inside the receipt transaction.
 * Each command retains its own preimage check, trigger and affected-row result. */
static ud_status write_changes(postgres_state *p, const ud_write_options *options,
                               const ud_batch *batch, uint64_t *affected) {
    ud_host_api *h = &p->host;
    if (!PQenterPipelineMode(p->db))
        return pg_failure(p, NULL);
    ud_status status = UD_OK;
    const uint32_t total = h->rows(batch);
    for (uint32_t first = 0; !status && first < total;) {
        uint32_t sent = 0;
        size_t bytes = 0;
        while (!status && first + sent < total && sent < 128 && bytes < 1048576) {
            ud_sql_statement plan = {0};
            pg_parameters params = {0};
            status = control(p);
            if (!status)
                status = h->compile_write(h->context, UD_SQL_POSTGRESQL, UD_SQL_PARAMETER_NUMBERED,
                                          options, batch, first + sent, 1, &plan);
            if (!status)
                status = parameters_statement(p, options, batch, &plan, &params);
            if (!status && !PQsendQueryParams(p->db, plan.sql.data, (int)params.count, params.types,
                                               params.values, params.lengths, params.formats, 0))
                status = pg_failure(p, NULL);
            if (!status) {
                bytes += plan.sql.length;
                for (uint32_t i = 0; i < params.count; i++)
                    if (params.values[i])
                        bytes += params.formats[i] ? (size_t)params.lengths[i] : strlen(params.values[i]);
                sent++;
            }
            parameters_clear(p, &params);
            h->statement_clear(&plan);
        }
        if (!sent)
            break;
        if (!PQpipelineSync(p->db)) {
            p->broken = 1;
            if (!status)
                status = pg_failure(p, NULL);
            break;
        }
        uint32_t received = 0;
        int synced = 0;
        while (!synced && wait_result(p)) {
            PGresult *result = PQgetResult(p->db);
            if (!result)
                continue; /* libpq separates successive command results with NULL. */
            ExecStatusType kind = PQresultStatus(result);
            if (kind == PGRES_PIPELINE_SYNC)
                synced = 1;
            else {
                if (!status && (received >= sent || kind != PGRES_COMMAND_OK))
                    status = received >= sent ? UD_CORRUPT : pg_failure(p, result);
                if (!status && affected)
                    *affected += strtoull(PQcmdTuples(result), NULL, 10);
                if (!status && options->operations[first + received] != UD_CHANGE_UPSERT &&
                    options->operations[first + received] != UD_CHANGE_TRUNCATE &&
                    strcmp(PQcmdTuples(result), "1"))
                    status = h->report(h->context, UD_FENCED, "changeset preimage or current business-key conflict");
                received++;
            }
            PQclear(result);
        }
        if (!synced || received != sent) {
            p->broken = 1;
            if (!status)
                status = p->control_status ? p->control_status : UD_TRANSIENT;
        }
        first += sent;
    }
    if (!PQexitPipelineMode(p->db)) {
        p->broken = 1;
        if (!status)
            status = pg_failure(p, NULL);
    }
    return status;
}

static ud_status write_batch(void *state, ud_string tenant, const ud_write_options *options,
                             const ud_batch *batch, const uint8_t digest[32]) {
    postgres_state *p = state;
    ud_host_api *h = &p->host;
    if (p->reading)
        return UD_BUSY;
    if (h->columns(batch) > 65535)
        return UD_RESOURCE;
    int receipted = options->delivery != UD_DELIVERY_RESPONSE;
    if (receipted && !digest)
        return UD_INVALID;
    atomic_store(&p->cancelled, 0);
    ud_bulk_reader *bulk = NULL;
    ud_status s = UD_OK;
    if (options->mode == UD_WRITE_APPEND && options->load_mode != UD_LOAD_PARAMETERS) {
        s = h->bulk_open(h->context, batch, UD_SQL_POSTGRESQL, &bulk);
        if (s == UD_UNSUPPORTED && options->load_mode == UD_LOAD_AUTO)
            s = UD_OK;
    }
    if (s)
        return s;
    int own = !p->transaction;
    s = own ? command(p, "BEGIN") : UD_OK;
    if (s) {
        h->bulk_close(bulk);
        return s;
    }
    char *table = NULL;
    if (!s)
        s = table_name(p, options->table, &table);
    if (!s) {
        size_t size = strlen(table) + 64;
        char *lock = h->allocate(h->context, size);
        if (!lock)
            s = UD_RESOURCE;
        else {
            snprintf(lock, size,
                     options->mode == UD_WRITE_CHANGESET
                         ? "LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE"
                         : "LOCK TABLE %s IN ROW EXCLUSIVE MODE",
                     table);
            s = command(p, lock);
            h->deallocate(lock);
        }
    }
    h->deallocate(table);
    if (!s)
        s = validate_target(p, options->table, batch);
    uint64_t affected = 0;
    ud_string strings[] = {tenant, options->job, options->batch_id, options->source_position};
    char *identity[4] = {0};
    for (int j = 0; !s && receipted && j < 4; j++) {
        identity[j] = plugin_string(h, strings[j].data ? strings[j].data : "", strings[j].length);
        if (!identity[j])
            s = UD_RESOURCE;
    }
    const char *reserve[] = {identity[0], identity[1], identity[2], (const char *)digest,
                             identity[3]};
    Oid reserve_types[] = {25, 25, 25, 17, 25};
    int reserve_lengths[] = {0, 0, 0, 32, 0}, reserve_formats[] = {0, 0, 0, 1, 0};
    PGresult *result =
        !s && receipted ? query(p,
                   "INSERT INTO "
                   "public.ud_sdk_receipts_v11(tenant,job,batch_id,digest,source_position) "
                   "VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING batch_id",
                   5, reserve_types, reserve, reserve_lengths, reserve_formats, 0)
           : NULL;
    if (!s && receipted && (!result || PQresultStatus(result) != PGRES_TUPLES_OK))
        s = pg_failure(p, result);
    int existing = !s && receipted && PQntuples(result) == 0;
    PQclear(result);
    if (!s && receipted && existing) {
        ud_commit_receipt previous = {0};
        s = receipt(p, tenant, options->job, options->batch_id, &previous);
        if (!s && (!previous.committed || memcmp(previous.digest, digest, 32)))
            s = UD_CORRUPT;
        h->deallocate((void *)previous.source_position.data);
    }
    uint32_t columns = options->mode == UD_WRITE_DELETE ? options->key_count : h->columns(batch);
    uint32_t limit = plugin_write_chunk_rows(h, UD_SQL_POSTGRESQL, options, batch, columns, 1024);
    if (!limit)
        s = UD_RESOURCE;
    if (limit > 1024)
        limit = 1024;
    if (!s && !existing && bulk)
        s = copy_batch(p, options, batch, bulk, &affected);
    int changes = options->mode == UD_WRITE_CHANGESET;
    if (!s && !existing && changes)
        s = write_changes(p, options, batch, &affected);
    int ordinary = !changes && !bulk;
    h->bulk_close(bulk);
    for (uint32_t row = 0; !s && !existing && ordinary && row < h->rows(batch);) {
        s = control(p);
        uint32_t count = h->rows(batch) - row;
        if (count > limit)
            count = limit;
        ud_sql_statement plan = {0};
        pg_parameters parameters = {0};
        if (!s)
            s = h->compile_write(h->context, UD_SQL_POSTGRESQL, UD_SQL_PARAMETER_NUMBERED, options,
                                 batch, row, count, &plan);
        if (!s)
            s = parameters_statement(p, options, batch, &plan, &parameters);
        PGresult *changed = !s ? query(p, plan.sql.data, (int)parameters.count, parameters.types,
                                       parameters.values, parameters.lengths, parameters.formats, 0)
                               : NULL;
        if (!s && (!changed || PQresultStatus(changed) != PGRES_COMMAND_OK))
            s = pg_failure(p, changed);
        if (!s)
            affected += strtoull(PQcmdTuples(changed), NULL, 10);
        PQclear(changed);
        parameters_clear(p, &parameters);
        h->statement_clear(&plan);
        row += count;
    }
    for (int j = 0; j < 4; j++)
        h->deallocate(identity[j]);
    if (!s && (h->cancelled(h->context) || atomic_load(&p->cancelled)))
        s = UD_CANCELLED;
    if (!s && own) {
        s = command(p, "COMMIT");
        if (s) {
            command(p, "ROLLBACK");
            return h->report(
                h->context, UD_COMMIT_UNKNOWN,
                "commit outcome requires receipt verification before advancing source position");
        }
    } else if (s && own && command(p, "ROLLBACK") != UD_OK)
        return h->report(h->context, UD_COMMIT_UNKNOWN,
                         "rollback outcome is unknown; receipt verification is required");
    if (!s && options->affected_rows)
        *options->affected_rows = affected;
    return s;
}
static void destroy(void *state) {
    postgres_state *p = state;
    if (!p)
        return;
    p->host.batch_destroy(p->schema);
    p->schema = NULL;
    ud_mutex_lock(&p->ownership);
    ud_pg_cdc *child = p->cdc;
    p->cdc = NULL;
    ud_mutex_unlock(&p->ownership);
    ud_pg_cdc_close(child);
    p->host.deallocate(p->configuration);
    if (p->cancel)
        PQcancelFinish(p->cancel);
    if (p->db)
        PQfinish(p->db);
    ud_mutex_destroy(&p->ownership);
    p->host.deallocate(p);
}
static int transaction_statement(const char *sql) {
    const unsigned char *p = (const unsigned char *)sql;
    for (;;) {
        while (*p && (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t'))
            p++;
        if (p[0] == '-' && p[1] == '-') {
            while (*p && *p != '\n')
                p++;
            continue;
        }
        if (p[0] == '/' && p[1] == '*') {
            unsigned depth = 1;
            p += 2;
            while (*p && depth) {
                if (p[0] == '/' && p[1] == '*') {
                    depth++;
                    p += 2;
                } else if (p[0] == '*' && p[1] == '/') {
                    depth--;
                    p += 2;
                } else
                    p++;
            }
            continue;
        }
        break;
    }
    char token[32];
    unsigned n = 0;
    while (*p && n + 1 < sizeof(token) && ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z'))) {
        token[n++] = (char)(*p >= 'a' && *p <= 'z' ? *p - 'a' + 'A' : *p);
        p++;
    }
    token[n] = 0;
    return !strcmp(token, "BEGIN") || !strcmp(token, "START") || !strcmp(token, "COMMIT") ||
           !strcmp(token, "ROLLBACK") || !strcmp(token, "SAVEPOINT") || !strcmp(token, "RELEASE") ||
           !strcmp(token, "END") || !strcmp(token, "PREPARE");
}
static ud_status savepoint(void *state, ud_string statement) {
    postgres_state *p = state;
    if (!p->transaction)
        return UD_UNSUPPORTED;
    char *sql = plugin_string(&p->host, statement.data, statement.length);
    if (!sql)
        return UD_RESOURCE;
    ud_status status = command(p, sql);
    p->host.deallocate(sql);
    return status;
}
static ud_status execute_command(void *state, const ud_read_options *options, uint64_t *processed) {
    postgres_state *p = state;
    if (processed)
        *processed = 0;
    char *sql = plugin_string(&p->host, options->sql.data, options->sql.length);
    if (!sql)
        return UD_RESOURCE;
    ud_status s = transaction_statement(sql) ? UD_UNSUPPORTED : UD_OK;
    uint32_t count = options->parameters ? p->host.rows(options->parameters) : 1;
    for (uint32_t row = 0; !s && row < count; row++) {
        pg_parameters parameters = {0};
        s = parameters_create(p, options->parameters, row, &parameters);
        PGresult *result = !s ? query(p, sql, (int)parameters.count, parameters.types,
                                      parameters.values, parameters.lengths, parameters.formats, 0)
                              : NULL;
        if (!s && (!result || PQresultStatus(result) != PGRES_COMMAND_OK))
            s = pg_failure(p, result);
        if (!s && processed) {
            const char *n = PQcmdTuples(result);
            if (n && *n)
                *processed += strtoull(n, NULL, 10);
        }
        PQclear(result);
        parameters_clear(p, &parameters);
    }
    if (s && s != UD_UNSUPPORTED && (p->broken || PQstatus(p->db) != CONNECTION_OK))
        s = UD_COMMIT_UNKNOWN;
    p->host.deallocate(sql);
    return s;
}

static ud_status begin_transaction(void *state, int32_t isolation) {
    postgres_state *p = state;
    if (p->transaction)
        return UD_INVALID;
    const char *sql =
        isolation == UD_ISOLATION_SERIALIZABLE      ? "BEGIN ISOLATION LEVEL SERIALIZABLE"
        : isolation == UD_ISOLATION_REPEATABLE_READ ? "BEGIN ISOLATION LEVEL REPEATABLE READ"
        : isolation == UD_ISOLATION_READ_COMMITTED  ? "BEGIN ISOLATION LEVEL READ COMMITTED"
        : isolation == UD_ISOLATION_READ_SNAPSHOT   ? "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
                                                    : NULL;
    if (!sql)
        return UD_INVALID;
    ud_status status = command(p, sql);
    if (!status)
        p->transaction = 1;
    return status;
}
static ud_status commit_transaction(void *state) {
    postgres_state *p = state;
    if (!p->transaction)
        return UD_INVALID;
    ud_status status = command(p, "COMMIT");
    if (status)
        return UD_COMMIT_UNKNOWN;
    p->transaction = 0;
    return UD_OK;
}
static ud_status rollback_transaction(void *state) {
    postgres_state *p = state;
    if (!p->transaction)
        return UD_INVALID;
    p->cleaning = 1;
    ud_status status = command(p, "ROLLBACK");
    p->cleaning = 0;
    if (status)
        return UD_COMMIT_UNKNOWN;
    p->transaction = 0;
    return UD_OK;
}
static ud_status metadata(void *state, const ud_metadata_options *o, ud_batch **out) {
    postgres_state *p = state;
    ud_host_api *h = &p->host;
    if (o->offset > INT64_MAX)
        return UD_INVALID;
    const char *queries[] = {
        NULL,
        "SELECT "
        "current_database(),''::text,'catalog'::text,0::text,'f'::text,0::text,''::text,'f'::text "
        "LIMIT $2 OFFSET $3",
        "SELECT nspname,''::text,'schema'::text,0::text,'f'::text,0::text,''::text,'f'::text FROM "
        "pg_namespace WHERE has_schema_privilege(oid,'USAGE') ORDER BY nspname LIMIT $2 OFFSET $3",
        "SELECT table_name,table_schema,table_type,0::text,'f'::text,0::text,''::text,'f'::text "
        "FROM information_schema.tables WHERE ($1='' OR table_schema=$1) ORDER BY "
        "table_schema,table_name LIMIT $2 OFFSET $3",
        "SELECT "
        "column_name,table_schema||'.'||table_name,data_type,ordinal_position::text,(is_nullable='"
        "YES')::text,0::text,''::text,'f'::text FROM information_schema.columns WHERE "
        "table_name=$1 OR table_schema||'.'||table_name=$1 ORDER BY ordinal_position LIMIT $2 "
        "OFFSET $3",
        "SELECT "
        "k.column_name,k.table_schema||'.'||k.table_name,'primary_key'::text,k.ordinal_position::"
        "text,'f'::text,k.ordinal_position::text,''::text,'t'::text FROM "
        "information_schema.table_constraints c JOIN information_schema.key_column_usage k "
        "USING(constraint_catalog,constraint_schema,constraint_name) WHERE "
        "c.constraint_type='PRIMARY KEY' AND (k.table_name=$1 OR "
        "k.table_schema||'.'||k.table_name=$1) ORDER BY k.ordinal_position LIMIT $2 OFFSET $3",
        "SELECT "
        "k.column_name,k.table_schema||'.'||k.table_name,'foreign_key'::text,k.ordinal_position::"
        "text,'t'::text,k.ordinal_position::text,u.table_schema||'.'||u.table_name||'.'||u.column_"
        "name,'f'::text FROM information_schema.table_constraints c JOIN "
        "information_schema.key_column_usage k "
        "USING(constraint_catalog,constraint_schema,constraint_name) JOIN "
        "information_schema.constraint_column_usage u "
        "USING(constraint_catalog,constraint_schema,constraint_name) WHERE "
        "c.constraint_type='FOREIGN KEY' AND (k.table_name=$1 OR "
        "k.table_schema||'.'||k.table_name=$1) ORDER BY k.constraint_name,k.ordinal_position LIMIT "
        "$2 OFFSET $3",
        "SELECT "
        "indexname,schemaname||'.'||tablename,'index'::text,0::text,'f'::text,0::text,indexdef,("
        "indexdef LIKE 'CREATE UNIQUE%')::text FROM pg_indexes WHERE (tablename=$1 OR "
        "schemaname||'.'||tablename=$1) AND "
        "has_table_privilege((quote_ident(schemaname)||'.'||quote_ident(tablename))::regclass,'"
        "SELECT') ORDER BY indexname LIMIT $2 OFFSET $3"};
    if (o->kind < UD_METADATA_CATALOGS || o->kind > UD_METADATA_INDEXES)
        return UD_INVALID;
    char *parent = plugin_string(h, o->parent.data ? o->parent.data : "", o->parent.length);
    if (!parent)
        return UD_RESOURCE;
    char limit[32], offset[32];
    snprintf(limit, sizeof(limit), "%u", o->limit);
    snprintf(offset, sizeof(offset), "%llu", (unsigned long long)o->offset);
    const char *args[] = {parent, limit, offset};
    const Oid types[] = {25, 20, 20};
    PGresult *r = query(p, queries[o->kind], 3, types, args, NULL, NULL, 0);
    h->deallocate(parent);
    if (!r || PQresultStatus(r) != PGRES_TUPLES_OK) {
        ud_status status = pg_failure(p, r);
        PQclear(r);
        return status;
    }
    ud_field fields[] = {
        {.name = "name", .name_length = 4, .type = UD_UTF8, .nullable = 1},
        {.name = "parent", .name_length = 6, .type = UD_UTF8, .nullable = 1},
        {.name = "declared_type", .name_length = 13, .type = UD_UTF8, .nullable = 1},
        {.name = "ordinal", .name_length = 7, .type = UD_INT64},
        {.name = "nullable", .name_length = 8, .type = UD_BOOL},
        {.name = "key_order", .name_length = 9, .type = UD_INT64},
        {.name = "reference", .name_length = 9, .type = UD_UTF8, .nullable = 1},
        {.name = "unique", .name_length = 6, .type = UD_BOOL}};
    ud_status status = h->batch_create(h->context, fields, 8, (uint32_t)PQntuples(r), out);
    for (int row = 0; !status && row < PQntuples(r); row++) {
        ud_value v[8] = {{0}};
        for (int i = 0; i < 8; i++) {
            v[i].type = fields[i].type;
            v[i].is_null = fields[i].nullable && PQgetisnull(r, row, i);
            const char *value = PQgetvalue(r, row, i);
            if (fields[i].type == UD_UTF8) {
                v[i].text = value;
                v[i].text_length = (uint32_t)PQgetlength(r, row, i);
            } else if (fields[i].type == UD_BOOL)
                v[i].integer = !strcmp(value, "t") || !strcmp(value, "true");
            else
                v[i].integer = strtoll(value, NULL, 10);
        }
        status = h->append(*out, v, 8);
    }
    PQclear(r);
    return status;
}
static ud_status reset_session(void *state) {
    postgres_state *p = state;
    if (p->reading || p->transaction || p->cdc || p->broken || p->branch_id[0] ||
        PQtransactionStatus(p->db) != PQTRANS_IDLE)
        return UD_CORRUPT;
    p->cleaning = 1;
    ud_status status = command(p, "DISCARD ALL");
    if (!status)
        status = session_settings(p);
    p->cleaning = 0;
    return status;
}
static ud_status provision(void *state) {
    postgres_state *p = state;
    if (p->transaction || p->reading || p->branch_id[0])
        return UD_BUSY;
    ud_status status = command(
        state, "CREATE TABLE IF NOT EXISTS public.ud_sdk_receipts_v11(tenant TEXT NOT NULL,job "
               "TEXT NOT NULL,batch_id TEXT NOT NULL,digest BYTEA NOT NULL "
               "CHECK(octet_length(digest)=32),source_position TEXT NOT NULL,PRIMARY "
               "KEY(tenant,job,batch_id))");
    if (!status)
        status =
            command(state, "CREATE TABLE IF NOT EXISTS public.ud_sdk_tx_receipts_v11("
                           "branch_id TEXT COLLATE \"C\" PRIMARY KEY CHECK(length(branch_id)=64),"
                           "digest BYTEA NOT NULL CHECK(octet_length(digest)=32))");
    return status;
}
static uint64_t session_capabilities(void *state) {
    postgres_state *p = state;
    return p->prepared_enabled
               ? connector.capabilities
               : connector.capabilities & ~(uint64_t)UD_CONNECT_TRANSACTION_PARTICIPANT;
}
static ud_status transaction_recover_state(void *state, const ud_transaction_branch *branch,
                                           int32_t *result) {
    postgres_state *p = state;
    *result = UD_TX_UNKNOWN;
    if (p->transaction && !memcmp(p->branch_id, branch->id.data, 64)) {
        *result = UD_TX_ACTIVE;
        return UD_OK;
    }
    char identity[65];
    memcpy(identity, branch->id.data, 64);
    identity[64] = 0;
    const char *values[] = {identity};
    PGresult *query_result = query(p,
                                   "SELECT owner=current_user FROM pg_prepared_xacts "
                                   "WHERE gid=$1 AND database=current_database()",
                                   1, NULL, values, NULL, NULL, 0);
    ud_status status = query_result && PQresultStatus(query_result) == PGRES_TUPLES_OK
                           ? UD_OK
                           : pg_failure(p, query_result);
    if (!status && PQntuples(query_result) > 1)
        status = UD_CORRUPT;
    if (!status && PQntuples(query_result) == 1) {
        if (PQgetisnull(query_result, 0, 0) || strcmp(PQgetvalue(query_result, 0, 0), "t"))
            status = UD_AUTH;
        else
            *result = UD_TX_PREPARED;
    }
    PQclear(query_result);
    if (status || *result == UD_TX_PREPARED)
        return status;
    query_result = query(p,
                         "SELECT encode(digest,'hex') FROM public.ud_sdk_tx_receipts_v11 "
                         "WHERE branch_id=$1",
                         1, NULL, values, NULL, NULL, 0);
    status = query_result && PQresultStatus(query_result) == PGRES_TUPLES_OK
                 ? UD_OK
                 : pg_failure(p, query_result);
    if (!status && PQntuples(query_result) > 1)
        status = UD_CORRUPT;
    if (!status && PQntuples(query_result) == 1) {
        const char *hex = PQgetvalue(query_result, 0, 0);
        if (PQgetisnull(query_result, 0, 0) || PQgetlength(query_result, 0, 0) != 64)
            status = UD_CORRUPT;
        static const char alphabet[] = "0123456789abcdef";
        for (uint32_t byte = 0; !status && byte < 32; byte++)
            if (hex[2 * byte] != alphabet[branch->digest[byte] >> 4] ||
                hex[2 * byte + 1] != alphabet[branch->digest[byte] & 15])
                status = UD_FENCED;
        if (!status)
            *result = UD_TX_COMMITTED;
    } else if (!status)
        *result = UD_TX_NOT_FOUND;
    PQclear(query_result);
    return status;
}
static ud_status transaction_recover(void *state, const ud_transaction_branch *branch,
                                     int32_t *result) {
    postgres_state *postgres = state;
    int cleaning = postgres->cleaning;
    postgres->cleaning = 1;
    ud_status status = transaction_recover_state(state, branch, result);
    postgres->cleaning = cleaning;
    return status;
}
static ud_status transaction_begin(void *state, const ud_transaction_branch *branch) {
    postgres_state *p = state;
    if (!p->prepared_enabled)
        return UD_UNSUPPORTED;
    if (p->branch_id[0])
        return UD_BUSY;
    ud_status status = begin_transaction(p, branch->isolation);
    char identity[65];
    memcpy(identity, branch->id.data, 64);
    identity[64] = 0;
    const char *values[] = {identity, (const char *)branch->digest};
    Oid types[] = {25, 17};
    int lengths[] = {64, 32}, formats[] = {0, 1};
    PGresult *result = !status
                           ? query(p,
                                   "INSERT INTO public.ud_sdk_tx_receipts_v11(branch_id,digest) "
                                   "VALUES($1,$2) ON CONFLICT DO NOTHING RETURNING branch_id",
                                   2, types, values, lengths, formats, 0)
                           : NULL;
    if (!status && (!result || PQresultStatus(result) != PGRES_TUPLES_OK))
        status = pg_failure(p, result);
    if (!status && PQntuples(result) != 1)
        status = UD_FENCED;
    PQclear(result);
    if (!status)
        memcpy(p->branch_id, identity, sizeof(identity));
    else if (p->transaction) {
        ud_status rolled_back = rollback_transaction(p);
        if (rolled_back)
            status = UD_COMMIT_UNKNOWN;
    }
    return status;
}
static ud_status transaction_prepare(void *state, const ud_transaction_branch *branch) {
    postgres_state *p = state;
    if (!p->transaction || memcmp(p->branch_id, branch->id.data, 64))
        return UD_FENCED;
    char statement[96];
    snprintf(statement, sizeof(statement), "PREPARE TRANSACTION '%.*s'", 64, branch->id.data);
    ud_status status = command(p, statement);
    if (!status)
        p->transaction = 0;
    return status;
}
static ud_status transaction_resolve(void *state, const ud_transaction_branch *branch,
                                     int32_t decision) {
    postgres_state *p = state;
    ud_status status;
    if (p->transaction) {
        if (decision != UD_TX_ROLLBACK || memcmp(p->branch_id, branch->id.data, 64))
            return UD_FENCED;
        p->cleaning = 1;
        status = rollback_transaction(p);
    } else {
        p->cleaning = 1;
        char statement[96];
        snprintf(statement, sizeof(statement), "%s PREPARED '%.*s'",
                 decision == UD_TX_COMMIT ? "COMMIT" : "ROLLBACK", 64, branch->id.data);
        status = command(p, statement);
    }
    if (!status)
        memset(p->branch_id, 0, sizeof(p->branch_id));
    p->cleaning = 0;
    return status;
}
static ud_status cdc_open(void *state, const ud_cdc_options *options) {
    postgres_state *p = state;
    if (p->reading || p->transaction || p->cdc)
        return UD_BUSY;
    ud_host_api host = p->host;
    host.check_io = p->check;
    host.control_user = p->control_user;
    ud_pg_cdc *child = NULL;
    ud_status status = ud_pg_cdc_open(&host, p->configuration, options, &child);
    if (!status) {
        ud_mutex_lock(&p->ownership);
        p->cdc = child;
        if (atomic_load(&p->cancelled))
            ud_pg_cdc_cancel(child);
        ud_mutex_unlock(&p->ownership);
    }
    return status;
}
static ud_status cdc_next(void *state, ud_cdc_event *out) {
    postgres_state *p = state;
    return p->cdc ? ud_pg_cdc_next(p->cdc, out) : UD_INVALID;
}
static ud_status cdc_rebind(void *state, const ud_batch *schema, uint64_t version) {
    postgres_state *p = state;
    return p->cdc ? ud_pg_cdc_rebind(p->cdc, schema, version) : UD_INVALID;
}
static ud_status cdc_confirm(void *state, ud_string position) {
    postgres_state *p = state;
    return p->cdc ? ud_pg_cdc_confirm(p->cdc, position) : UD_INVALID;
}
static ud_status cdc_close(void *state) {
    postgres_state *p = state;
    ud_mutex_lock(&p->ownership);
    ud_pg_cdc *child = p->cdc;
    p->cdc = NULL;
    ud_mutex_unlock(&p->ownership);
    return ud_pg_cdc_close(child);
}
static const ud_connector_api connector = {
    .struct_size = sizeof(connector),
    .abi_version = UD_PLUGIN_ABI,
    .capabilities =
        UD_CONNECT_READ_SNAPSHOT | UD_CONNECT_READONLY_SCAN | UD_CONNECT_TRANSACTIONAL_DML |
        UD_CONNECT_READ |
        UD_CONNECT_RECORD_ERRORS | UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE |
        UD_CONNECT_RECEIPTS | UD_CONNECT_SNAPSHOT | UD_CONNECT_EXECUTE_SQL |
        UD_CONNECT_TRANSACTION | UD_CONNECT_METADATA | UD_CONNECT_POOL | UD_CONNECT_CHANGESET |
        UD_CONNECT_CDC | UD_CONNECT_NATIVE_CDC | UD_CONNECT_PREPARED | UD_CONNECT_BATCH_WRITE |
        UD_CONNECT_PROVISION | UD_CONNECT_NATIVE_BULK | UD_CONNECT_TRANSACTION_PARTICIPANT,
    .test = test_connection,
    .discover = discover,
    .read_open = open_read,
    .read_next = next_read,
    .read_close = close_read,
    .write = write_batch,
    .receipt = receipt,
    .cancel = cancel_operation,
    .execute = execute_command,
    .begin = begin_transaction,
    .commit = commit_transaction,
    .rollback = rollback_transaction,
    .metadata = metadata,
    .set_control = set_control,
    .reset_session = reset_session,
    .session_capabilities = session_capabilities,
    .record_diagnostic = record_diagnostic,
    .savepoint = savepoint,
    .provision = provision,
    .metadata_dialect = UD_SQL_POSTGRESQL,
    .parameter_style = UD_SQL_PARAMETER_NUMBERED,
    .cdc_open = cdc_open,
    .cdc_next = cdc_next,
    .cdc_confirm = cdc_confirm,
    .cdc_close = cdc_close,
    .cdc_rebind = cdc_rebind,
    .transaction_begin = transaction_begin,
    .transaction_prepare = transaction_prepare,
    .transaction_resolve = transaction_resolve,
    .transaction_recover = transaction_recover};
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){sizeof(*out),
                           UD_PLUGIN_ABI,
                           "org.universaldata.connector.postgresql",
                           UD_SDK_VERSION,
                           UD_PLUGIN_CONNECTOR,
                           UD_CAP_TRANSACTION | UD_CAP_THREAD_SAFE,
                           initialize,
                           destroy,
                           plugin_shutdown,
                           &connector,
                           NULL,
                           sizeof(ud_field),
                           sizeof(ud_value)};
    return UD_OK;
}
