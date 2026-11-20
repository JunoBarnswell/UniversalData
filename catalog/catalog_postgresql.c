#include "internal.h"

#ifdef UD_WITH_POSTGRESQL_CATALOG
#include "postgresql_schema.h"

ud_status catalog_postgresql_status(PGconn *connection, const PGresult *result) {
    if (!result || PQstatus(connection) != CONNECTION_OK)
        return UD_TRANSIENT;
    ExecStatusType code = PQresultStatus(result);
    if (code == PGRES_COMMAND_OK || code == PGRES_TUPLES_OK || code == PGRES_SINGLE_TUPLE)
        return UD_OK;
    const char *state = PQresultErrorField(result, PG_DIAG_SQLSTATE);
    if (!state) return UD_IO;
    if (!strncmp(state,"08",2) || !strncmp(state,"57P",3) || !strcmp(state,"57014"))
        return UD_TRANSIENT;
    if (!strncmp(state,"40",2) || !strcmp(state,"55P03")) return UD_BUSY;
    if (!strncmp(state,"53",2)) return UD_RESOURCE;
    if (!strncmp(state,"23",2)) return UD_CORRUPT;
    if (!strncmp(state,"28",2) || !strcmp(state,"42501")) return UD_AUTH;
    if (!strcmp(state,"25006")) return UD_FENCED;
    if (!strcmp(state,"42P01")) return UD_INCOMPATIBLE;
    return UD_IO;
}

static void discard_notice(void *unused, const char *message) {
    (void)unused;
    (void)message;
}
#endif

ud_status catalog_postgresql_open(ud_catalog *catalog, ud_string connection) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    char *value = ud_strdup_n(catalog->ctx, connection.data, connection.length);
    if (!value)
        return UD_RESOURCE;
    const char *keys[] = {"dbname", "target_session_attrs", "connect_timeout", "client_encoding", NULL};
    const char *values[] = {value, "read-write", "2", "UTF8", NULL};
    catalog->postgresql = PQconnectdbParams(keys, values, 1);
    memset(value, 0, connection.length);
    ud_free(value);
    if (!catalog->postgresql || PQstatus(catalog->postgresql) != CONNECTION_OK)
        return UD_TRANSIENT;
    if (PQserverVersion(catalog->postgresql) < 170000)
        return UD_INCOMPATIBLE;
    PQsetNoticeProcessor(catalog->postgresql, discard_notice, NULL);
    UD_TRY(ud_catalog_sql(catalog, "SET search_path=public;SET lock_timeout='2s';"
        "SET statement_timeout='4s';SET idle_in_transaction_session_timeout='10s';SET synchronous_commit=on"));
    if (catalog->options.create_directory) {
        ud_status status = ud_catalog_sql(catalog, "BEGIN;SELECT pg_advisory_xact_lock(1430537025);");
        if (!status) status = ud_catalog_sql(catalog, UD_POSTGRESQL_SCHEMA);
        if (!status) status = ud_catalog_sql(catalog, "COMMIT");
        if (status) {
            (void)ud_catalog_sql(catalog, "ROLLBACK");
            return status;
        }
    }
    catalog_statement *statement = NULL;
    ud_status status = catalog_prepare(catalog, "SELECT version FROM catalog_schema WHERE id=1", &statement);
    if (!status) {
        status = catalog_step(statement);
        if (status == UD_END || (!status && catalog_column_int(statement, 0) != 1))
            status = UD_INCOMPATIBLE;
    }
    catalog_finalize(statement);
    return status;
#else
    (void)catalog;
    (void)connection;
    return UD_UNSUPPORTED;
#endif
}
