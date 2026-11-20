/* Real participant recovery: each phase runs in a fresh operating-system process. */
#include "check.h"
#include "ud/plugin.h"
#include "ud/sql.h"
#include <ctype.h>
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
static ud_context *context;
static ud_string string(const char *value) {
    return (ud_string){value, (uint32_t)strlen(value)};
}
static ud_status grant(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)user;
    (void)tenant;
    (void)resource;
    (void)action;
    return UD_OK;
}
static ud_status configuration(void *user, ud_context *ctx, ud_string tenant, ud_string resource,
                               ud_string reference, ud_string *out) {
    (void)tenant;
    (void)resource;
    (void)reference;
    FILE *file = fopen(user, "rb");
    if (!file)
        return UD_IO;
    if (fseek(file, 0, SEEK_END)) {
        fclose(file);
        return UD_IO;
    }
    long size = ftell(file);
    rewind(file);
    if (size < 1 || size > 65536) {
        fclose(file);
        return UD_SCHEMA;
    }
    char *data = ud_alloc(ctx, (size_t)size + 1);
    if (!data) {
        fclose(file);
        return UD_RESOURCE;
    }
    size_t count = fread(data, 1, (size_t)size, file);
    fclose(file);
    if (count != (size_t)size) {
        ud_free(data);
        return UD_IO;
    }
    *out = (ud_string){data, (uint32_t)size};
    return UD_OK;
}
static ud_status command(ud_connection *connection, const char *sql) {
    ud_read_options options = {
        .struct_size = sizeof(options), .abi_version = UD_ABI_VERSION, .sql = string(sql)};
    return ud_connection_execute_command(connection, &options, NULL);
}
static ud_status count_rows(ud_connection *connection, const char *table, uint64_t *count) {
    ud_batch *schema = NULL;
    ud_status discovered =
        ud_connection_schema(connection, string(table), string("tx.fixture"), &schema);
    if (discovered)
        return discovered;
    char sql[160];
    snprintf(sql, sizeof(sql), "SELECT * FROM %s", table);
    ud_read_options options = {.struct_size = sizeof(options),
                               .abi_version = UD_ABI_VERSION,
                               .batch_size = 32,
                               .sql = string(sql)};
    ud_status status = ud_connection_read_open(connection, &options, schema);
    *count = 0;
    while (!status) {
        ud_batch *batch = NULL;
        status = ud_connection_read_next(connection, &batch);
        if (!status)
            *count += ud_batch_rows(batch);
        ud_batch_destroy(batch);
    }
    if (status == UD_END)
        status = UD_OK;
    ud_status closed = ud_connection_read_close(connection);
    ud_batch_destroy(schema);
    return status ? status : closed;
}
#undef OK
#define OK(call)                                                                                   \
    do {                                                                                           \
        ud_status status_ = (call);                                                                \
        if (status_) {                                                                             \
            fprintf(stderr, "participant status=%d line=%d: %s\n", status_, __LINE__,              \
                    ud_error(context));                                                            \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)
int main(int argc, char **argv) {
    CHECK((argc == 5 || argc == 7 || argc == 9) && strlen(argv[4]) == 64);
    OK(ud_context_create(NULL, &context));
    uint32_t count = argc == 9 ? 3 : 2;
    ud_plugin *plugins[3] = {0};
    ud_connection *connections[3] = {0};
    ud_transaction_branch branches[3] = {0};
    char identities[3][65], tables[3][31];
    for (uint32_t index = 0; index < count; index++) {
        int argument = index && argc > 5 ? 3 + 2 * (int)index : 1;
        OK(ud_plugin_load(context, argv[argument], 0, plugins + index));
        ud_connection_options options = {.struct_size = sizeof(options),
                                         .abi_version = UD_ABI_VERSION,
                                         .tenant = {"transaction-fixture", 19},
                                         .resource = {"independent-participant", 23},
                                         .credential_reference = {"fixture", 7},
                                         .authorize = grant,
                                         .resolve_user = argv[argument + 1],
                                         .resolve = configuration};
        OK(ud_connection_open(plugins[index], &options, connections + index));
        if (!(ud_connection_capabilities(connections[index]) &
              UD_CONNECT_TRANSACTION_PARTICIPANT)) {
            fprintf(stderr, "Blocked: resource has no durable participant capability (%s)\n",ud_error(context));
            return 77;
        }
        memcpy(identities[index], argv[4], 65);
        identities[index][63] = (char)('a' + index);
        branches[index] = (ud_transaction_branch){.struct_size = sizeof(branches[index]),
                                                  .abi_version = UD_ABI_VERSION,
                                                  .id = {identities[index], 64},
                                                  .isolation = UD_ISOLATION_READ_COMMITTED};
        memset(branches[index].digest, index + 17, sizeof(branches[index].digest));
        snprintf(tables[index], sizeof(tables[index]), "ud_xa_%.16s_%c", argv[4], 'a' + (int)index);
        if (ud_connection_dialect(connections[index]) == UD_SQL_ORACLE)
            for (char *at = tables[index]; *at; ++at)
                *at = (char)toupper((unsigned char)*at);
    }
    int active_rollback = !strcmp(argv[3], "active-rollback");
    int active_kill = !strcmp(argv[3], "active-kill");
    int prepare_kill = !strcmp(argv[3], "prepare-kill") || !strcmp(argv[3], "partial-commit-kill");
    if (!strcmp(argv[3], "setup")) {
        for (uint32_t index = 0; index < count; index++) {
            char sql[160];
            snprintf(sql, sizeof(sql), "CREATE TABLE %s (id INTEGER PRIMARY KEY)", tables[index]);
            OK(command(connections[index], sql));
            OK(ud_connection_provision(connections[index]));
        }
    } else {
        if (active_rollback || active_kill || prepare_kill) {
            for (uint32_t index = 0; index < count; index++) {
                OK(ud_connection_transaction_begin(connections[index], branches + index));
                char sql[160];
                snprintf(sql, sizeof(sql), "INSERT INTO %s (id) VALUES (1)", tables[index]);
                OK(command(connections[index], sql));
            }
        }
        if (prepare_kill) {
            for (uint32_t index = 0; index < count; index++) {
                OK(ud_connection_transaction_prepare(connections[index], branches + index));
                int32_t state = UD_TX_UNKNOWN;
                OK(ud_connection_transaction_recover(connections[index], branches + index, &state));
                CHECK(state == UD_TX_PREPARED);
            }
            if (!strcmp(argv[3], "partial-commit-kill"))
                OK(ud_connection_transaction_resolve(connections[0], branches, UD_TX_COMMIT));
        }
        if (active_kill || prepare_kill) {
            fflush(NULL);
            _exit(86); /* No SDK cleanup or driver close: real recovery boundary. */
        }
        int commit = !strcmp(argv[3], "recover-commit");
        int abandoned = !strcmp(argv[3], "recover-abandoned");
        CHECK(active_rollback || abandoned || commit || !strcmp(argv[3], "recover-rollback"));
        for (uint32_t index = 0; index < count; index++) {
            int32_t state = UD_TX_UNKNOWN;
            OK(ud_connection_transaction_recover(connections[index], branches + index, &state));
            if (active_rollback)
                CHECK(state == UD_TX_ACTIVE);
            else if (abandoned)
                CHECK(state == UD_TX_ACTIVE || state == UD_TX_NOT_FOUND);
            else
                CHECK(state == UD_TX_PREPARED || (commit && state == UD_TX_COMMITTED));
            OK(ud_connection_transaction_resolve(connections[index], branches + index,
                                                 commit ? UD_TX_COMMIT : UD_TX_ROLLBACK));
            uint64_t rows = UINT64_MAX;
            OK(count_rows(connections[index], tables[index], &rows));
            CHECK(rows == (uint64_t)commit);
            if (commit) {
                OK(ud_connection_transaction_recover(connections[index], branches + index, &state));
                CHECK(state == UD_TX_COMMITTED);
                ud_transaction_branch wrong = branches[index];
                wrong.digest[0] ^= 1;
                CHECK(ud_connection_transaction_recover(connections[index], &wrong, &state) ==
                      UD_FENCED);
            }
            char sql[160];
            snprintf(sql, sizeof(sql), "DROP TABLE %s", tables[index]);
            OK(command(connections[index], sql));
        }
    }
    for (uint32_t index = 0; index < count; index++) {
        OK(ud_connection_close(connections[index]));
        OK(ud_plugin_unload(plugins[index]));
    }
    ud_context_destroy(context);
    return 0;
}
