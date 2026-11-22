#ifdef UD_WITH_POSTGRESQL_CATALOG
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <errno.h>
#include <poll.h>
#endif
#endif
#include "internal.h"

#ifdef UD_WITH_POSTGRESQL_CATALOG
#include "postgresql_schema.h"

enum { catalog_io_ms = 5000, replay_poll_ms = 20 };

static uint64_t deadline_after(uint32_t milliseconds) {
    return ud_monotonic_ns() + (uint64_t)milliseconds * UINT64_C(1000000);
}

static int socket_ready(PGconn *connection, int writing, uint64_t deadline) {
    int socket = PQsocket(connection);
    if (socket < 0)
        return 0;
    for (;;) {
        uint64_t now = ud_monotonic_ns();
        if (now >= deadline)
            return 0;
        int remaining = (int)((deadline - now + UINT64_C(999999)) / UINT64_C(1000000));
#ifdef _WIN32
        WSAPOLLFD fd = {.fd = (SOCKET)socket, .events = writing ? POLLWRNORM : POLLRDNORM};
        int ready = WSAPoll(&fd, 1, remaining);
        if (ready < 0 && WSAGetLastError() == WSAEINTR)
            continue;
#else
        struct pollfd fd = {.fd = socket, .events = writing ? POLLOUT : POLLIN};
        int ready = poll(&fd, 1, remaining);
        if (ready < 0 && errno == EINTR)
            continue;
#endif
        return ready > 0 && ud_monotonic_ns() < deadline;
    }
}

static ud_status disconnect(ud_catalog *catalog, ud_status status) {
    PQfinish(catalog->postgresql);
    catalog->postgresql = NULL;
    catalog->postgresql_deadline = 0;
    return status;
}

static ud_status result_status(const PGresult *result) {
    ExecStatusType code = PQresultStatus(result);
    if (code == PGRES_COMMAND_OK || code == PGRES_TUPLES_OK)
        return UD_OK;
    const char *state = PQresultErrorField(result, PG_DIAG_SQLSTATE);
    if (!state)
        return UD_IO;
    if (!strncmp(state, "08", 2) || !strncmp(state, "57P", 3) || !strcmp(state, "57014") ||
        !strcmp(state, "40003"))
        return UD_TRANSIENT;
    if (!strncmp(state, "40", 2) || !strcmp(state, "55P03"))
        return UD_BUSY;
    if (!strncmp(state, "53", 2))
        return UD_RESOURCE;
    if (!strncmp(state, "23", 2))
        return UD_CORRUPT;
    if (!strncmp(state, "28", 2) || !strcmp(state, "42501"))
        return UD_AUTH;
    if (!strcmp(state, "25006"))
        return UD_FENCED;
    if (!strcmp(state, "42P01"))
        return UD_INCOMPATIBLE;
    return UD_IO;
}

ud_status catalog_postgresql_query(ud_catalog *catalog, const char *sql, int count, const Oid *types,
                                   const char *const *values, const int *lengths, const int *formats,
                                   PGresult **out) {
    *out = NULL;
    PGconn *connection = catalog->postgresql;
    if (!connection)
        return UD_TRANSIENT;
    uint64_t deadline = catalog->postgresql_deadline;
    if (!deadline)
        deadline = deadline_after(catalog_io_ms);
    if (ud_monotonic_ns() >= deadline)
        return disconnect(catalog, UD_TRANSIENT);

    catalog->postgresql_commit_warning = 0;
    int sent = types ? PQsendQueryParams(connection, sql, count, types, values, lengths, formats, 0)
                     : PQsendQuery(connection, sql);
    if (!sent)
        return disconnect(catalog, UD_TRANSIENT);
    int flush;
    while ((flush = PQflush(connection)) == 1)
        if (!socket_ready(connection, 1, deadline))
            return disconnect(catalog, UD_TRANSIENT);
    if (flush < 0)
        return disconnect(catalog, UD_TRANSIENT);

    ud_status status = UD_OK;
    PGresult *last = NULL;
    for (;;) {
        while (PQisBusy(connection)) {
            if (!socket_ready(connection, 0, deadline) || !PQconsumeInput(connection))
                goto lost;
        }
        if (ud_monotonic_ns() >= deadline)
            goto lost;
        PGresult *result = PQgetResult(connection);
        if (!result)
            break;
        ExecStatusType code = PQresultStatus(result);
        if (code == PGRES_COPY_IN || code == PGRES_COPY_OUT || code == PGRES_COPY_BOTH) {
            PQclear(result);
            goto lost;
        }
        if (!status) {
            status = result_status(result);
            PQclear(last);
            last = result;
        } else {
            PQclear(result);
        }
    }
    if (!last || PQstatus(connection) != CONNECTION_OK)
        goto lost;
    *out = last;
    return status;

lost:
    PQclear(last);
    return disconnect(catalog, UD_TRANSIENT);
}

static void receive_notice(void *user, const PGresult *notice) {
    ud_catalog *catalog = user;
    const char *state = PQresultErrorField(notice, PG_DIAG_SQLSTATE);
    /* PostgreSQL can finish COMMIT locally and report cancellation of its
     * synchronous replication wait as a warning followed by CommandComplete. */
    if (state && !strcmp(state, "57014"))
        catalog->postgresql_commit_warning = 1;
}

#define PRIMARY_SETTINGS                                                                         \
    "NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off' "                 \
    "AND current_setting('fsync')='on' AND current_setting('full_page_writes')='on' "              \
    "AND current_setting('synchronous_commit')='remote_apply' "                                   \
    "AND current_setting('synchronous_standby_names')<>''"
#define SYNCHRONOUS_SENDERS                                                                      \
    " FROM pg_stat_replication WHERE state='streaming' AND sync_state IN ('sync','quorum')"

static ud_status standby_confirmation(ud_catalog *catalog, const char *lsn, int *confirmed) {
    static const char sql[] =
        "SELECT (" PRIMARY_SETTINGS " AND EXISTS(SELECT 1" SYNCHRONOUS_SENDERS
        " AND ($1::pg_lsn IS NULL OR replay_lsn >= $1::pg_lsn)))";
    const Oid type = 0;
    PGresult *result = NULL;
    ud_status status = catalog_postgresql_query(catalog, sql, 1, &type, &lsn, NULL, NULL, &result);
    if (!status)
        *confirmed = PQntuples(result) == 1 && !strcmp(PQgetvalue(result, 0, 0), "t");
    PQclear(result);
    return status;
}

ud_status catalog_postgresql_admit(ud_catalog *catalog) {
    int confirmed = 0;
    UD_TRY(standby_confirmation(catalog, NULL, &confirmed));
    return confirmed ? UD_OK : UD_TRANSIENT;
}

ud_status catalog_postgresql_begin(ud_catalog *catalog, const char *statement) {
    if (!catalog->postgresql_deadline)
        catalog->postgresql_deadline = deadline_after(catalog_io_ms);
    ud_status status = ud_catalog_sql(catalog, statement);
    if (status)
        catalog->postgresql_deadline = 0;
    return status;
}

static ud_status confirm_committed_wal(ud_catalog *catalog) {
    /* A synchronous standby may disappear or the settings can change during
     * COMMIT. Capture a WAL bound after it returned and prove that bound on a
     * currently synchronous sender before returning success. */
    PGresult *result = NULL;
    ud_status status = catalog_postgresql_query(catalog, "SELECT pg_current_wal_flush_lsn()::text",
                                                 0, NULL, NULL, NULL, NULL, &result);
    if (!status && (PQntuples(result) != 1 || PQgetisnull(result, 0, 0)))
        status = UD_IO;
    const char *lsn = !status ? PQgetvalue(result, 0, 0) : NULL;
    while (!status) {
        int confirmed = 0;
        status = standby_confirmation(catalog, lsn, &confirmed);
        if (status || confirmed)
            break;
        if (deadline_after(replay_poll_ms) >= catalog->postgresql_deadline) {
            status = UD_TRANSIENT;
            break;
        }
        ud_sleep_ms(replay_poll_ms);
    }
    PQclear(result);
    return status;
}

static int commit_rejected(const PGresult *result) {
    if (!result || PQresultStatus(result) != PGRES_FATAL_ERROR)
        return 0;
    const char *state = PQresultErrorField(result, PG_DIAG_SQLSTATE);
    return state && (!strncmp(state, "23", 2) ||
        (!strncmp(state, "40", 2) && strcmp(state, "40003")) ||
        !strcmp(state, "55P03") || !strcmp(state, "25006") || !strcmp(state, "42501"));
}

static ud_status commit(ud_catalog *catalog) {
    if (!catalog->postgresql || ud_monotonic_ns() >= catalog->postgresql_deadline)
        return disconnect(catalog, UD_TRANSIENT);
    if (PQtransactionStatus(catalog->postgresql) != PQTRANS_INTRANS)
        return UD_IO;
    PGresult *result = NULL;
    ud_status status = catalog_postgresql_query(catalog, "COMMIT", 0, NULL, NULL, NULL, NULL, &result);
    if (status && !commit_rejected(result))
        status = UD_COMMIT_UNKNOWN;
    if (!status && strcmp(PQcmdStatus(result), "COMMIT"))
        status = !strcmp(PQcmdStatus(result), "ROLLBACK") ? UD_IO : UD_COMMIT_UNKNOWN;
    PQclear(result);
    if (status == UD_COMMIT_UNKNOWN || catalog->postgresql_commit_warning ||
        (!status && confirm_committed_wal(catalog)))
        return disconnect(catalog, UD_COMMIT_UNKNOWN);
    return status;
}

ud_status catalog_postgresql_finish(ud_catalog *catalog, ud_status status) {
    if (!catalog->postgresql_deadline)
        catalog->postgresql_deadline = deadline_after(catalog_io_ms);
    if (!status)
        status = commit(catalog);
    if (status && catalog->postgresql &&
        PQtransactionStatus(catalog->postgresql) != PQTRANS_IDLE &&
        ud_catalog_sql(catalog, "ROLLBACK"))
        (void)disconnect(catalog, UD_TRANSIENT);
    catalog->postgresql_deadline = 0;
    return status;
}

static const char *option(const PQconninfoOption *options, const char *name) {
    for (; options->keyword; options++)
        if (!strcmp(options->keyword, name))
            return options->val ? options->val : "";
    return "";
}

static int numeric_address(const char *host) {
    unsigned char address[16];
    return inet_pton(AF_INET, host, address) == 1 || inet_pton(AF_INET6, host, address) == 1;
}

static ud_status connect_endpoint(ud_catalog *catalog, const char *connection, uint64_t deadline) {
    PQconninfoOption *options = PQconninfoParse(connection, NULL);
    if (!options)
        return UD_INVALID;
    const char *host = option(options, "host");
    const char *address = option(options, "hostaddr");
    if (!*address && numeric_address(host))
        address = host;
    /* libpq's connect poll can otherwise block in system DNS resolution.
     * Retain the logical host for TLS certificate and password-file matching. */
    int valid = !*option(options, "service") && !strchr(host, ',') &&
        (*address ? numeric_address(address) : host[0] == '/');
    if (valid) {
        const char *keys[] = {"dbname", "hostaddr", "target_session_attrs", "client_encoding",
                              "gssencmode", NULL};
        const char *values[] = {connection, address, "read-write", "UTF8", "disable", NULL};
        catalog->postgresql = PQconnectStartParams(keys, values, 1);
    }
    PQconninfoFree(options);
    if (!valid)
        return UD_INVALID;
    if (!catalog->postgresql || PQstatus(catalog->postgresql) == CONNECTION_BAD)
        return disconnect(catalog, UD_TRANSIENT);
    PostgresPollingStatusType ready = PGRES_POLLING_WRITING;
    while (ready == PGRES_POLLING_READING || ready == PGRES_POLLING_WRITING) {
        if (!socket_ready(catalog->postgresql, ready == PGRES_POLLING_WRITING, deadline))
            return disconnect(catalog, UD_TRANSIENT);
        ready = PQconnectPoll(catalog->postgresql);
    }
    if (ready != PGRES_POLLING_OK || PQsetnonblocking(catalog->postgresql, 1))
        return disconnect(catalog, UD_TRANSIENT);
    return UD_OK;
}
#endif

ud_status catalog_postgresql_open(ud_catalog *catalog, ud_string connection) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    char *value = ud_strdup_n(catalog->ctx, connection.data, connection.length);
    if (!value)
        return UD_RESOURCE;
    uint64_t deadline = deadline_after(catalog_io_ms);
    ud_status status = connect_endpoint(catalog, value, deadline);
    memset(value, 0, connection.length);
    ud_free(value);
    UD_TRY(status);
    if (PQserverVersion(catalog->postgresql) < 170000)
        return UD_INCOMPATIBLE;

    catalog->postgresql_deadline = deadline;
    PQsetNoticeReceiver(catalog->postgresql, receive_notice, catalog);
    UD_TRY(ud_catalog_sql(catalog, "SET search_path=public;SET lock_timeout='2s';"
        "SET statement_timeout='4s';SET idle_in_transaction_session_timeout='10s';"
        "SET synchronous_commit=remote_apply"));
    UD_TRY(catalog_postgresql_admit(catalog));
    if (catalog->options.create_directory) {
        UD_TRY(catalog_postgresql_begin(catalog, "BEGIN"));
        status = ud_catalog_sql(catalog, "SELECT pg_advisory_xact_lock(1430537025)");
        if (!status)
            status = ud_catalog_sql(catalog, UD_POSTGRESQL_SCHEMA);
        UD_TRY(catalog_postgresql_finish(catalog, status));
        catalog->postgresql_deadline = deadline;
    }
    catalog_statement *statement = NULL;
    status = catalog_prepare(catalog, "SELECT version FROM catalog_schema WHERE id=1", &statement);
    if (!status) {
        status = catalog_step(statement);
        if (status == UD_END || (!status && catalog_column_int(statement, 0) != 1))
            status = UD_INCOMPATIBLE;
    }
    catalog_finalize(statement);
    catalog->postgresql_deadline = 0;
    return status;
#else
    (void)catalog;
    (void)connection;
    return UD_UNSUPPORTED;
#endif
}
