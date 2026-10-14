/* Public SDK consumer. Run only against a dedicated acceptance database. */
#define main advanced_fixture_main
#include "../tests/advanced_sql.c"
#undef main
#include <time.h>
#ifndef _WIN32
#include <sys/resource.h>
#endif
static double seconds(void) {
    struct timespec now;
    timespec_get(&now, TIME_UTC);
    return now.tv_sec + now.tv_nsec / 1e9;
}
int main(int argc, char **argv) {
    CHECK(argc >= 3 && argc <= 5);
    uint64_t total = argc > 3 ? strtoull(argv[3], NULL, 10) : 1000000;
    CHECK(total && total <= 100000000);
    OK(ud_context_create(NULL, &ctx));
    ud_plugin *plugin = NULL;
    OK(ud_plugin_load(ctx, argv[1], 0, &plugin));
    int allowed = 1;
    ud_connection_options options = {.struct_size = sizeof(options),
                                     .abi_version = UD_ABI_VERSION,
                                     .tenant = {"bulk-acceptance", 15},
                                     .resource = {"isolated-database", 17},
                                     .credential_reference = {"fixture", 7},
                                     .authorize = authorize,
                                     .authorize_user = &allowed,
                                     .resolve = resolve,
                                     .resolve_user = argv[2]};
    ud_connection *connection = NULL;
    OK(ud_connection_open(plugin, &options, &connection));
    OK(ud_connection_test(connection));
    int dialect = ud_connection_dialect(connection);
    uint64_t capabilities = ud_connection_capabilities(connection);
    if (!(capabilities & UD_CONNECT_NATIVE_BULK)) {
        fprintf(stderr, "Blocked: driver/resource has no native bulk transport\n");
        return 77;
    }
    OK(ud_connection_provision(connection));
    double durations[2] = {0};
    const uint32_t capacity = 16384;
    for (int pass = 0; pass < 2; pass++) {
        const char *table = pass ? "ud_bulk_native" : "ud_bulk_parameters";
        char sql_table[64];
        snprintf(sql_table, sizeof(sql_table), dialect == UD_SQL_ORACLE ? "\"%s\"" : "%s", table);
        char statement[512];
        const char *text_type = dialect == UD_SQL_SQLSERVER ? "NVARCHAR(128)"
                                : dialect == UD_SQL_ORACLE  ? "NVARCHAR2(128)"
                                : dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB
                                    ? "VARCHAR(128) CHARACTER SET utf8mb4"
                                    : "VARCHAR(128)";
        snprintf(statement, sizeof(statement),
                 "CREATE TABLE %s (id BIGINT NOT NULL PRIMARY KEY,amount BIGINT NOT NULL,"
                 "label %s NOT NULL)",
                 table, text_type);
        if (dialect == UD_SQL_ORACLE)
            snprintf(statement, sizeof(statement),
                     "CREATE TABLE %s (\"id\" NUMBER(19,0) NOT NULL PRIMARY KEY,\"amount\" "
                     "NUMBER(19,0) NOT NULL,\"label\" %s NOT NULL)",
                     sql_table, text_type);
        OK(command(connection, statement));
        ud_batch *schema = NULL;
        OK(ud_connection_schema(connection, str(table), str("bulk"), &schema));
        ud_field fields[3];
        for (uint32_t at = 0; at < 3; at++)
            OK(ud_batch_field(schema, at, fields + at));
        double elapsed = 0, generation = 0;
        clock_t cpu = clock();
        uint64_t batches = 0;
        for (uint64_t begin = 0; begin < total; begin += capacity) {
            uint32_t count = total - begin < capacity ? (uint32_t)(total - begin) : capacity;
            double generated = seconds();
            ud_batch *batch = NULL;
            OK(ud_batch_create(ctx, fields, 3, count, &batch));
            for (uint32_t at = 0; at < count; at++) {
                char id[32], amount[32];
                int id_size = snprintf(id, sizeof(id), "%llu", (unsigned long long)(begin + at));
                int amount_size = snprintf(amount, sizeof(amount), "%llu",
                                           (unsigned long long)((begin + at) * 3));
                ud_value row[3] = {{0}};
                OK(ud_value_parse_field(ctx, fields, id, id_size, UD_ROUND_EXACT, row));
                OK(ud_value_parse_field(ctx, fields + 1, amount, amount_size, UD_ROUND_EXACT,
                                        row + 1));
                static const char label[] = "远程\tbulk\n\\copy 😀";
                row[2] =
                    (ud_value){.type = UD_UTF8, .text = label, .text_length = sizeof(label) - 1};
                OK(ud_batch_append(batch, row, 3));
            }
            OK(ud_batch_seal(batch));
            generation += seconds() - generated;
            char identity[32], position[32];
            snprintf(identity, sizeof(identity), "%llu", (unsigned long long)++batches);
            snprintf(position, sizeof(position), "%llu", (unsigned long long)(begin + count));
            ud_write_options write = {.struct_size = sizeof(write),
                                      .abi_version = UD_ABI_VERSION,
                                      .mode = UD_WRITE_APPEND,
                                      .table = str(table),
                                      .job = str(table),
                                      .batch_id = str(identity),
                                      .source_position = str(position),
                                      .current_column = UINT32_MAX,
                                      .load_mode = pass ? UD_LOAD_NATIVE : UD_LOAD_PARAMETERS};
            double started = seconds();
            OK(ud_connection_write(connection, &write, batch));
            elapsed += seconds() - started;
            ud_commit_receipt receipt = {0};
            OK(ud_connection_receipt(connection, write.job, write.batch_id, &receipt));
            CHECK(receipt.committed && receipt.source_position.length == strlen(position) &&
                  !memcmp(receipt.source_position.data, position, strlen(position)));
            ud_free((void *)receipt.source_position.data);
            ud_batch_destroy(batch);
        }
        double cpu_seconds = (double)(clock() - cpu) / CLOCKS_PER_SEC;
        snprintf(statement, sizeof(statement),
                 "SELECT COUNT(*) AS rows_loaded,MIN(id) AS low_id,MAX(id) AS high_id,"
                 "SUM(CASE WHEN amount=id*3 THEN 0 ELSE 1 END) AS invalid_rows FROM %s",
                 sql_table);
        if (dialect == UD_SQL_ORACLE)
            snprintf(statement, sizeof(statement),
                     "SELECT COUNT(*) AS \"rows_loaded\",MIN(\"id\") AS \"low_id\","
                     "MAX(\"id\") AS \"high_id\",SUM(CASE WHEN \"amount\"=\"id\"*3 THEN 0 "
                     "ELSE 1 END) AS \"invalid_rows\" FROM %s",
                     sql_table);
        ud_field checks[] = {{.name = "rows_loaded", .name_length = 11, .type = UD_INT64},
                             {.name = "low_id", .name_length = 6, .type = UD_INT64},
                             {.name = "high_id", .name_length = 7, .type = UD_INT64},
                             {.name = "invalid_rows", .name_length = 12, .type = UD_INT64}};
        ud_batch *check_schema = NULL, *actual = NULL;
        OK(ud_batch_create(ctx, checks, 4, 0, &check_schema));
        ud_read_options read = {.struct_size = sizeof(read),
                                .abi_version = UD_ABI_VERSION,
                                .batch_size = 1,
                                .sql = str(statement)};
        OK(ud_connection_read_open(connection, &read, check_schema));
        OK(ud_connection_read_next(connection, &actual));
        uint64_t expected[] = {total, 0, total - 1, 0};
        for (uint32_t at = 0; at < 4; at++) {
            ud_value value;
            OK(ud_batch_get(actual, 0, at, &value));
            CHECK(!value.is_null && value.integer >= 0 && (uint64_t)value.integer == expected[at]);
        }
        OK(ud_connection_read_close(connection));
        ud_batch_destroy(actual);
        ud_batch_destroy(check_schema);
        ud_batch_destroy(schema);
        durations[pass] = elapsed;
        long rss = 0;
#ifndef _WIN32
        struct rusage usage;
        CHECK(!getrusage(RUSAGE_SELF, &usage));
        rss = usage.ru_maxrss;
#endif
        printf("{\"mode\":\"%s\",\"dialect\":%d,\"rows\":%llu,\"batches\":%llu,"
               "\"batchRows\":%u,\"writeSeconds\":%.6f,\"generationSeconds\":%.6f,"
               "\"cpuSeconds\":%.6f,\"peakRssKiB\":%ld,\"rowsPerSecond\":%.0f,"
               "\"verifiedRowsAndReceipts\":true}\n",
               pass ? "native" : "parameters", dialect, (unsigned long long)total,
               (unsigned long long)batches, capacity, elapsed, generation, cpu_seconds, rss,
               total / elapsed);
        fflush(stdout);
        if (argc < 5 || strcmp(argv[4], "retain")) {
            snprintf(statement, sizeof(statement), "DROP TABLE %s", sql_table);
            OK(command(connection, statement));
        }
    }
    printf("{\"nativeSpeedup\":%.3f}\n", durations[0] / durations[1]);
    OK(ud_connection_close(connection));
    OK(ud_plugin_unload(plugin));
    OK(ud_context_destroy(ctx));
    return 0;
}
