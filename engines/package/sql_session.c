#include "internal.h"
#include "ud/connection_manager.h"
#include "ud/sql.h"
#include "../../connectors/sql_config.h"

typedef struct standalone_connection {
    ud_context *context;
    ud_string plugin_path;
    ud_string credentials;
} standalone_connection;

struct ud_sql_session {
    ud_execution *execution;
    ud_activation *activation;
    ud_mutex lease_lock;
    ud_connection_lease *lease;
    ud_string connection;
    standalone_connection *standalone;
    ud_batch *query_arguments;
    ud_batch *query_schema;
    ud_string query;
    int cursor;
    int32_t dialect;
    int transaction;
    int rollback_only;
    int enlisted;
    int auto_close;
    int writing;
};

static void clear_query(ud_sql_session *);

static void borrowed_lease_ended(void *user) {
    ud_sql_session *session = user;
    ud_mutex_lock(&session->lease_lock);
    session->lease = NULL;
    session->activation = NULL;
    session->cursor = 0;
    clear_query(session);
    ud_mutex_unlock(&session->lease_lock);
}

static ud_status authorize_standalone(void *user, ud_string tenant, ud_string resource,
                                     int32_t action) {
    const standalone_connection *connection = user;
    if (!ud_named(tenant, "sql-sdk"))
        return UD_AUTH;
    if (action == UD_ACCESS_OPEN && ud_same(resource, connection->plugin_path))
        return UD_OK;
    return ud_named(resource, "database") &&
                   (action == UD_ACCESS_OPEN || action == UD_ACCESS_DISCOVER ||
                    action == UD_ACCESS_READ || action == UD_ACCESS_WRITE)
               ? UD_OK
               : UD_AUTH;
}

static ud_status resolve_standalone(void *user, ud_context *context, ud_string tenant,
                                   ud_string resource, ud_string reference, ud_string *out) {
    const standalone_connection *connection = user;
    if (!ud_named(tenant, "sql-sdk") || !ud_named(resource, "database") ||
        !ud_named(reference, "sql-sdk"))
        return UD_AUTH;
    char *text = ud_strdup_n(context, connection->credentials.data, connection->credentials.length);
    if (!text)
        return UD_RESOURCE;
    *out = (ud_string){text, connection->credentials.length};
    return UD_OK;
}

static void standalone_destroy(standalone_connection *connection) {
    if (!connection)
        return;
    ud_context *context = connection->context;
    volatile char *secret = (volatile char *)connection->credentials.data;
    for (uint32_t i = 0; i < connection->credentials.length; i++)
        secret[i] = 0;
    ud_free((void *)connection->credentials.data);
    ud_free((void *)connection->plugin_path.data);
    ud_free(connection);
    ud_context_destroy(context);
}

static ud_status acquire_lease(ud_sql_session *session) {
    if (session->lease)
        return UD_OK;
    if (session->enlisted && !session->activation)
        return UD_FENCED;
    ud_connection_lease *lease = NULL;
    ud_status status = session->enlisted
        ? ud_transaction_sql_lease(session->activation, session->connection, session,
                                   borrowed_lease_ended, &lease)
        : ud_activation_acquire_connection(&session->execution->root, session->connection,
                                           UD_CONNECT_READ, &lease);
    if (status)
        return status;
    int32_t dialect = ud_connection_lease_dialect(lease);
    if (dialect <= 0) {
        ud_connection_lease_release(lease);
        return UD_UNSUPPORTED;
    }
    ud_mutex_lock(&session->lease_lock);
    session->lease = lease;
    session->dialect = dialect;
    ud_mutex_unlock(&session->lease_lock);
    return UD_OK;
}

static ud_status release_lease(ud_sql_session *session) {
    ud_mutex_lock(&session->lease_lock);
    ud_connection_lease *lease = session->lease;
    session->lease = NULL;
    ud_mutex_unlock(&session->lease_lock);
    ud_status status = ud_connection_lease_release(lease);
    if (status == UD_BUSY) {
        ud_mutex_lock(&session->lease_lock);
        session->lease = lease;
        ud_mutex_unlock(&session->lease_lock);
    }
    return status;
}

static ud_status release_idle(ud_sql_session *session) {
    if (!session->auto_close || session->cursor || session->transaction)
        return UD_OK;
    return release_lease(session);
}

ud_status ud_sql_session_set_auto_close(ud_sql_session *session, int32_t enabled) {
    if (!session || (enabled != 0 && enabled != 1))
        return UD_INVALID;
    if (session->writing)
        return UD_BUSY;
    if (session->enlisted && enabled)
        return UD_UNSUPPORTED;
    session->auto_close = enabled;
    return release_idle(session);
}

static ud_status open_session(ud_execution *e, ud_activation *activation, ud_string connection,
                              ud_sql_session **out) {
    if (!e || !out || !connection.data || !connection.length ||
        memchr(connection.data, 0, connection.length))
        return UD_INVALID;
    *out = NULL;
    ud_mutex_lock(&e->events_lock);
    int state = activation ? UD_EXEC_RUNNING : UD_EXEC_READY;
    if (atomic_load(&e->state) != state || e->sql_sessions == UINT32_MAX) {
        ud_mutex_unlock(&e->events_lock);
        return UD_BUSY;
    }
    e->sql_sessions++;
    ud_mutex_unlock(&e->events_lock);
    ud_sql_session *session = ud_alloc(e->ctx, sizeof(*session));
    ud_status status = session ? UD_OK : UD_RESOURCE;
    if (!status) {
        *session = (ud_sql_session){.execution = e, .activation = activation,
                                    .enlisted = activation != NULL};
        ud_mutex_init(&session->lease_lock);
        char *id = ud_strdup_n(e->ctx, connection.data, connection.length);
        if (!id)
            status = UD_RESOURCE;
        else {
            session->connection = (ud_string){id, connection.length};
            status = acquire_lease(session);
        }
    }
    if (!status)
        *out = session;
    else {
        if (session) {
            release_lease(session);
            ud_mutex_destroy(&session->lease_lock);
            ud_free((void *)session->connection.data);
        }
        ud_free(session);
        ud_mutex_lock(&e->events_lock);
        e->sql_sessions--;
        ud_mutex_unlock(&e->events_lock);
    }
    return status;
}

ud_status ud_execution_sql_session_open(ud_execution *e, ud_string connection,
                                         ud_sql_session **out) {
    return open_session(e, NULL, connection, out);
}

ud_status ud_activation_sql_session_open(ud_activation *activation, ud_string connection,
                                         ud_sql_session **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ud_transaction_active(activation))
        return UD_UNSUPPORTED;
    UD_TRY(ud_activation_check(activation));
    return open_session(activation->execution, activation, connection, out);
}

ud_status ud_sql_session_open(int32_t dialect, ud_string plugin_path, ud_string connection_string,
                              ud_sql_session **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (dialect < UD_SQL_SQLITE || dialect > UD_SQL_ORACLE || !plugin_path.data ||
        !plugin_path.length || memchr(plugin_path.data, 0, plugin_path.length))
        return UD_INVALID;
    ud_context_options memory = {.struct_size = sizeof(memory), .abi_version = UD_ABI_VERSION,
                                 .memory_limit = UINT64_C(268435456)};
    ud_context *context = NULL;
    UD_TRY(ud_context_create(&memory, &context));
    standalone_connection *connection = ud_alloc(context, sizeof(*connection));
    if (!connection) {
        ud_context_destroy(context);
        return UD_RESOURCE;
    }
    *connection = (standalone_connection){.context = context};
    ud_status status = ud_sql_connection_config(context, dialect, connection_string,
                                                &connection->credentials);
    if (!status) {
        char *path = ud_strdup_n(context, plugin_path.data, plugin_path.length);
        if (!path)
            status = UD_RESOURCE;
        else
            connection->plugin_path = (ud_string){path, plugin_path.length};
    }
    const char prefix[] = "{\"version\":" UD_PROTOCOL_VERSION_TEXT
        ",\"id\":\"sql-sdk\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},"
        "\"dataFlows\":[],\"connections\":[{\"id\":\"database\",\"driver\":";
    const char suffix[] = ",\"resource\":\"database\",\"credentialReference\":\"sql-sdk\"}]}";
    ud_bytes package = {.ctx = context};
    if (!status)
        status = ud_bytes_put(&package, prefix, sizeof(prefix) - 1);
    if (!status)
        status = ud_json_quote(&package, plugin_path.data, plugin_path.length);
    if (!status)
        status = ud_bytes_put(&package, suffix, sizeof(suffix) - 1);
    ud_execution *execution = NULL;
    ud_execution_options options = {
        .struct_size = sizeof(options), .abi_version = UD_ABI_VERSION, .context = memory,
        .edge_capacity = 8, .max_batch_rows = 65536, .max_depth = 8,
        .parameters = {"{}", 2}, .tenant = {"sql-sdk", 7},
        .authorize = authorize_standalone, .authorize_user = connection,
        .resolve = resolve_standalone, .resolve_user = connection};
    if (!status)
        status = ud_execution_create_json(
            (ud_string){(const char *)package.data, (uint32_t)package.size}, &options, &execution);
    ud_bytes_clear(&package);
    if (!status)
        status = ud_execution_sql_session_open(execution, (ud_string){"database", 8}, out);
    if (!status && (*out)->dialect != dialect) {
        ud_sql_session_close(*out);
        *out = NULL;
        status = UD_INCOMPATIBLE;
    }
    if (status) {
        ud_execution_destroy(execution);
        standalone_destroy(connection);
    } else
        (*out)->standalone = connection;
    return status;
}

ud_context *ud_sql_session_context(ud_sql_session *session) {
    return session ? session->execution->ctx : NULL;
}

int32_t ud_sql_session_dialect(const ud_sql_session *session) {
    return session ? session->dialect : 0;
}

static void clear_query(ud_sql_session *session) {
    ud_free((void *)session->query.data);
    ud_batch_destroy(session->query_arguments);
    ud_batch_destroy(session->query_schema);
    session->query = (ud_string){0};
    session->query_arguments = session->query_schema = NULL;
}

static void mark_failed(ud_sql_session *session, ud_status status) {
    if (!status || status == UD_END || status == UD_BUSY)
        return;
    if (session->enlisted)
        ud_transaction_sql_failed(session->activation, status);
    else if (session->transaction)
        session->rollback_only = 1;
}

ud_status ud_sql_session_read_open(ud_sql_session *session, ud_string sql, ud_string query_json,
                                   ud_string schema_json, const ud_batch *parameters,
                                   uint32_t batch_rows, int32_t result_mode, ud_batch **shape) {
    if (!session || !shape || ((sql.length != 0) == (query_json.length != 0)) || !batch_rows ||
        result_mode < UD_READ_TYPED || result_mode > UD_READ_SCALAR ||
        (!schema_json.length && (query_json.length || result_mode == UD_READ_TYPED)))
        return UD_INVALID;
    *shape = NULL;
    if (session->cursor || session->writing || session->rollback_only)
        return UD_BUSY;
    if (sql.length)
        UD_TRY(ud_sql_validate_read_query(sql));
    if (batch_rows > session->execution->options.max_batch_rows)
        batch_rows = session->execution->options.max_batch_rows;
    UD_TRY(acquire_lease(session));
    ud_context *context = session->execution->ctx;
    int32_t dialect = ud_connection_lease_dialect(session->lease);
    int32_t style = ud_connection_lease_parameter_style(session->lease);
    ud_status status = UD_OK;
    if (schema_json.length)
        status = ud_schema_from_json(context, schema_json.data, schema_json.length,
                                     (ud_string){0}, &session->query_schema);
    if (!status && sql.length)
        status = ud_sql_bind_command(context, dialect, style, sql, parameters, &session->query,
                                     &session->query_arguments);
    if (!status && query_json.length)
        status = ud_sql_compile_table_query(context, dialect, style, query_json,
                                            session->query_schema, parameters, &session->query,
                                            &session->query_arguments);
    if (!status) {
        ud_read_options read = {.struct_size = sizeof(read), .abi_version = UD_ABI_VERSION,
                                .batch_size = result_mode == UD_READ_SCALAR ? 1 : batch_rows,
                                .sql = session->query, .parameters = session->query_arguments,
                                .result_mode = result_mode, .result_schema = shape};
        status = ud_connection_lease_read_open(session->lease, &read, session->query_schema);
        mark_failed(session, status);
    }
    if (!status)
        session->cursor = 1;
    if (!status && !*shape)
        status = session->query_schema
                  ? ud_batch_create_like(context, session->query_schema, 0, shape)
                  : UD_UNSUPPORTED;
    if (status) {
        if (session->cursor)
            (void)ud_sql_session_read_close(session);
        else
            clear_query(session);
        ud_batch_destroy(*shape);
        *shape = NULL;
        (void)release_idle(session);
    }
    return status;
}

ud_status ud_sql_session_read_next(ud_sql_session *session, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!session || !session->cursor)
        return UD_INVALID;
    ud_status status = ud_connection_lease_read_next(session->lease, out);
    mark_failed(session, status);
    return status;
}

ud_status ud_sql_session_read_close(ud_sql_session *session) {
    if (!session)
        return UD_INVALID;
    if (!session->cursor)
        return UD_OK;
    ud_status status = ud_connection_lease_read_close(session->lease);
    mark_failed(session, status);
    if (!status) {
        session->cursor = 0;
        clear_query(session);
        (void)release_idle(session);
    }
    return status;
}

ud_status ud_sql_session_execute(ud_sql_session *session, ud_string sql,
                                const ud_batch *parameters, uint64_t *affected) {
    if (!session || !affected)
        return UD_INVALID;
    *affected = 0;
    if (session->cursor || session->rollback_only || session->writing)
        return UD_BUSY;
    ud_string statement = {0};
    ud_batch *arguments = NULL;
    UD_TRY(ud_sql_validate_session_command(sql, session->transaction || session->enlisted));
    UD_TRY(acquire_lease(session));
    ud_status status = ud_sql_bind_command(session->execution->ctx,
        ud_connection_lease_dialect(session->lease),
        ud_connection_lease_parameter_style(session->lease), sql, parameters, &statement, &arguments);
    if (!status) {
        ud_read_options command = {.struct_size = sizeof(command), .abi_version = UD_ABI_VERSION,
                                   .sql = statement, .parameters = arguments};
        status = ud_connection_lease_execute_command(session->lease, &command, affected);
        mark_failed(session, status);
    }
    if (status)
        *affected = 0;
    ud_batch_destroy(arguments);
    ud_free((void *)statement.data);
    (void)release_idle(session);
    return status;
}

static ud_status session_write(ud_sql_session *session, int32_t mode, int32_t load_mode,
                               ud_string table, const uint32_t *keys, uint32_t key_count,
                               uint32_t chunk_rows, const ud_batch *batch, uint64_t *affected) {
    if (!session || !batch || !affected || batch->ctx != session->execution->ctx ||
        (mode != UD_WRITE_APPEND && mode != UD_WRITE_DELETE && mode != UD_WRITE_UPDATE))
        return UD_INVALID;
    *affected = 0;
    if (session->cursor || session->rollback_only)
        return UD_BUSY;
    if (session->enlisted && load_mode == UD_LOAD_NATIVE)
        return UD_UNSUPPORTED;
    UD_TRY(acquire_lease(session));
    ud_write_options write = {
        .struct_size = sizeof(write), .abi_version = UD_ABI_VERSION, .mode = mode, .table = table,
        .keys = keys, .key_count = key_count, .current_column = UINT32_MAX,
        .load_mode = session->enlisted ? UD_LOAD_PARAMETERS : load_mode,
        .delivery = UD_DELIVERY_RESPONSE, .chunk_rows = chunk_rows, .affected_rows = affected};
    ud_status status = ud_connection_lease_write(session->lease, &write, batch);
    mark_failed(session, status);
    (void)release_idle(session);
    return status;
}

static ud_status session_transaction(ud_sql_session *session, int32_t operation, int32_t isolation) {
    if (!session || operation < UD_SQL_SESSION_BEGIN || operation > UD_SQL_SESSION_ROLLBACK)
        return UD_INVALID;
    if (session->enlisted)
        return UD_UNSUPPORTED;
    if (operation == UD_SQL_SESSION_BEGIN && session->transaction)
        return UD_BUSY;
    if (operation != UD_SQL_SESSION_BEGIN && !session->transaction)
        return UD_INVALID;
    if (operation == UD_SQL_SESSION_COMMIT && session->rollback_only)
        return UD_BUSY;
    if (session->cursor) {
        if (operation != UD_SQL_SESSION_ROLLBACK)
            return UD_BUSY;
        UD_TRY(ud_sql_session_read_close(session));
    }
    UD_TRY(acquire_lease(session));
    if (operation == UD_SQL_SESSION_BEGIN && !isolation)
        isolation = ud_connection_lease_dialect(session->lease) == UD_SQL_ORACLE
                        ? UD_ISOLATION_READ_COMMITTED
                        : UD_ISOLATION_SERIALIZABLE;
    ud_status status = ud_connection_lease_sql_transaction(session->lease, operation, isolation);
    if (!status) {
        session->transaction = operation == UD_SQL_SESSION_BEGIN;
        session->rollback_only = 0;
    } else if (operation == UD_SQL_SESSION_COMMIT)
        session->rollback_only = 1;
    (void)release_idle(session);
    return status;
}

ud_status ud_sql_session_write(ud_sql_session *session, int32_t mode, int32_t load_mode,
                               ud_string table, const uint32_t *keys, uint32_t key_count,
                               uint32_t chunk_rows, const ud_batch *batch, uint64_t *affected) {
    if (session && session->writing)
        return UD_BUSY;
    return session_write(session, mode, load_mode, table, keys, key_count, chunk_rows,
                         batch, affected);
}

ud_status ud_sql_session_transaction(ud_sql_session *session, int32_t operation, int32_t isolation) {
    if (session && session->writing)
        return UD_BUSY;
    return session_transaction(session, operation, isolation);
}

ud_status ud_sql_session_write_stream(ud_sql_session *session, int32_t mode, int32_t load_mode,
                                     ud_string table, const uint32_t *keys, uint32_t key_count,
                                     uint32_t chunk_rows, ud_sql_batch_source next, void *user,
                                     uint64_t *affected) {
    if (!session || !next || !affected || !chunk_rows)
        return UD_INVALID;
    *affected = 0;
    if (session->writing || session->cursor || session->rollback_only)
        return UD_BUSY;
    if (session->enlisted && load_mode == UD_LOAD_NATIVE)
        return UD_UNSUPPORTED;
    if (session->enlisted)
        UD_TRY(acquire_lease(session));
    uint32_t capacity = session->execution->options.max_batch_rows;
    if (capacity > chunk_rows)
        capacity = chunk_rows;
    uint64_t total = 0;
    int32_t last = 0;
    const ud_batch *batch = NULL;
    session->writing = 1;
    ud_status status = next(user, capacity, &batch, &last);
    int own = 0, attempted = 0;
    if (status == UD_END)
        status = UD_OK;
    else if (!status) {
        if (!last && !session->transaction && !session->enlisted) {
            int32_t isolation =
                session->dialect == UD_SQL_SQLITE || session->dialect == UD_SQL_MYSQL ||
                        session->dialect == UD_SQL_MARIADB
                    ? UD_ISOLATION_SERIALIZABLE
                    : UD_ISOLATION_READ_COMMITTED;
            status = session_transaction(session, UD_SQL_SESSION_BEGIN, isolation);
            own = !status;
        }
        while (!status) {
            if (!batch || !batch->rows || batch->rows > capacity || (last != 0 && last != 1)) {
                status = UD_INVALID;
                break;
            }
            uint64_t written = 0;
            attempted = 1;
            status = session_write(session, mode, load_mode, table, keys, key_count,
                                   chunk_rows, batch, &written);
            if (!status && UINT64_MAX - total < written)
                status = UD_OVERFLOW;
            if (status)
                break;
            total += written;
            if (last)
                break;
            batch = NULL;
            status = next(user, capacity, &batch, &last);
            if (status == UD_END)
                status = UD_INVALID;
        }
        if (!status && own)
            status = session_transaction(session, UD_SQL_SESSION_COMMIT, 0);
        if (status && own && status != UD_COMMIT_UNKNOWN) {
            if (session_transaction(session, UD_SQL_SESSION_ROLLBACK, 0))
                status = UD_COMMIT_UNKNOWN;
        } else if (status && attempted)
            mark_failed(session, status);
    }
    session->writing = 0;
    if (!status)
        *affected = total;
    return status;
}

void ud_sql_session_cancel(ud_sql_session *session) {
    if (!session)
        return;
    ud_mutex_lock(&session->lease_lock);
    if (session->lease && session->enlisted)
        mark_failed(session, UD_CANCELLED);
    ud_connection_lease_cancel(session->lease);
    ud_mutex_unlock(&session->lease_lock);
}

ud_status ud_sql_session_close(ud_sql_session *session) {
    if (!session)
        return UD_OK;
    if (session->writing)
        return UD_BUSY;
    ud_execution *e = session->execution;
    standalone_connection *standalone = session->standalone;
    ud_status status = ud_sql_session_read_close(session);
    if (!status && session->transaction)
        status = ud_sql_session_transaction(session, UD_SQL_SESSION_ROLLBACK, 0);
    ud_status released = release_lease(session);
    if (released == UD_BUSY)
        return released;
    clear_query(session);
    ud_mutex_destroy(&session->lease_lock);
    ud_free((void *)session->connection.data);
    ud_free(session);
    ud_mutex_lock(&e->events_lock);
    e->sql_sessions--;
    ud_mutex_unlock(&e->events_lock);
    if (!status)
        status = released;
    if (standalone) {
        ud_status destroyed = ud_execution_destroy(e);
        standalone_destroy(standalone);
        if (!status)
            status = destroyed;
    }
    return status;
}
