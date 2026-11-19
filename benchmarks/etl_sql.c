#ifndef _WIN32
#define _POSIX_C_SOURCE 200809L
#endif
#include "ud/package.h"
#include "ud/sql.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#include <psapi.h>
#else
#include <sys/resource.h>
#include <unistd.h>
#endif

typedef struct workload {
    ud_sql_session *session;
    ud_context *context;
    ud_string table;
    uint32_t rows, read_batch, width, consumer_delay_ms;
    uint64_t peak;
} workload;

static ud_string text(const char *data) {
    return (ud_string){data, (uint32_t)strlen(data)};
}

static uint64_t now_ns(void) {
#ifdef _WIN32
    LARGE_INTEGER value, frequency;
    QueryPerformanceCounter(&value);
    QueryPerformanceFrequency(&frequency);
    return (uint64_t)(1000000000.0 * value.QuadPart / frequency.QuadPart);
#else
    struct timespec value;
    clock_gettime(CLOCK_MONOTONIC, &value);
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
#endif
}

static uint64_t cpu_ns(void) {
#ifdef _WIN32
    FILETIME created, exited, kernel, user;
    if (!GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user))
        return 0;
    ULARGE_INTEGER system = {.LowPart = kernel.dwLowDateTime, .HighPart = kernel.dwHighDateTime};
    ULARGE_INTEGER client = {.LowPart = user.dwLowDateTime, .HighPart = user.dwHighDateTime};
    return (system.QuadPart + client.QuadPart) * 100;
#else
    struct timespec value;
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &value);
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
#endif
}

static uint64_t peak_rss(void) {
#ifdef _WIN32
    PROCESS_MEMORY_COUNTERS info = {.cb = sizeof(info)};
    return GetProcessMemoryInfo(GetCurrentProcess(), &info, sizeof(info)) ? info.PeakWorkingSetSize : 0;
#else
    struct rusage info;
    if (getrusage(RUSAGE_SELF, &info))
        return 0;
    return (uint64_t)info.ru_maxrss * 1024;
#endif
}

static unsigned process_id(void) {
#ifdef _WIN32
    return (unsigned)GetCurrentProcessId();
#else
    return (unsigned)getpid();
#endif
}

static void consume_delay(uint32_t milliseconds) {
    if (!milliseconds)
        return;
#ifdef _WIN32
    Sleep(milliseconds);
#else
    const struct timespec delay = {.tv_sec = milliseconds / 1000,
                                   .tv_nsec = (long)(milliseconds % 1000) * 1000000};
    nanosleep(&delay, NULL);
#endif
}

static ud_status command(ud_sql_session *session, const char *sql) {
    uint64_t affected;
    return ud_sql_session_execute(session, text(sql), NULL, &affected);
}

static ud_status warm_round_trip(ud_sql_session *session, int32_t dialect, uint64_t *elapsed) {
    ud_status status = ud_sql_session_set_auto_close(session, 1);
    if (status)
        return status;
    const char *query = dialect == UD_SQL_ORACLE ? "SELECT 1 FROM dual" : "SELECT 1";
    ud_batch *schema = NULL, *result = NULL;
    uint64_t started = now_ns();
    status = ud_sql_session_read_open(session, text(query), (ud_string){0}, (ud_string){0},
                                     NULL, 1, UD_READ_SCALAR, &schema);
    if (!status)
        status = ud_sql_session_read_next(session, &result);
    if (!status && ud_batch_rows(result) != 1)
        status = UD_CORRUPT;
    ud_batch_destroy(result);
    ud_batch_destroy(schema);
    ud_status closed = ud_sql_session_read_close(session);
    *elapsed = now_ns() - started;
    if (!status)
        status = closed;
    return status ? status : ud_sql_session_set_auto_close(session, 0);
}

static int compare_ns(const void *left, const void *right) {
    uint64_t a = *(const uint64_t *)left, b = *(const uint64_t *)right;
    return a > b ? 1 : a < b ? -1 : 0;
}

static ud_status make_batch(workload *work, uint32_t offset, uint32_t count,
                            const char *value, ud_batch **batch) {
    ud_field fields[] = {{.name = "id", .name_length = 2, .type = UD_INT64, .key_order = 1},
                         {.name = "payload", .name_length = 7, .type = UD_UTF8}};
    ud_status status = ud_batch_create(work->context, fields, 2, count, batch);
    for (uint32_t row = 0; !status && row < count; row++) {
        ud_value values[] = {{.type = UD_INT64, .integer = (int64_t)offset + row + 1},
                             {.type = UD_UTF8, .text = value, .text_length = work->width}};
        status = ud_batch_append(*batch, values, 2);
    }
    return status ? status : ud_batch_seal(*batch);
}

static void sample_native(workload *work) {
    uint64_t bytes = ud_memory_used(work->context);
    if (work->peak < bytes)
        work->peak = bytes;
}

static ud_status mutate(workload *work, int32_t mode, uint32_t batch_rows, const char *value,
                        uint64_t *elapsed, uint32_t *calls) {
    uint32_t key = 0;
    *calls = 0;
    uint64_t started = now_ns();
    ud_status status = ud_sql_session_transaction(work->session, UD_SQL_SESSION_BEGIN, 0);
    int active = !status;
    for (uint32_t offset = 0; !status && offset < work->rows;) {
        uint32_t count = work->rows - offset;
        if (count > batch_rows)
            count = batch_rows;
        ud_batch *batch = NULL;
        status = make_batch(work, offset, count, value, &batch);
        uint64_t affected = 0;
        if (!status) {
            sample_native(work);
            status = ud_sql_session_write(work->session, mode, UD_LOAD_PARAMETERS, work->table,
                                          &key, mode == UD_WRITE_APPEND ? 0 : 1,
                                          batch_rows, batch, &affected);
            (*calls)++;
        }
        ud_batch_destroy(batch);
        if (!status && affected != count)
            status = UD_CORRUPT;
        offset += count;
    }
    if (active) {
        ud_status done = ud_sql_session_transaction(work->session,
            status ? UD_SQL_SESSION_ROLLBACK : UD_SQL_SESSION_COMMIT, 0);
        if (!status)
            status = done;
    }
    *elapsed = now_ns() - started;
    return status;
}

static ud_status scan(workload *work, const char *query, const char *payload, uint32_t expected,
                      uint64_t *elapsed, uint64_t *first, uint64_t *p95, uint64_t *checksum) {
    static const char shape[] = "{\"version\":" UD_PROTOCOL_VERSION_TEXT
        ",\"fields\":[{\"name\":\"id\",\"type\":\"int64\",\"nullable\":false},"
        "{\"name\":\"payload\",\"type\":\"utf8\",\"nullable\":false}]}";
    ud_batch *schema = NULL;
    uint64_t start = now_ns();
    ud_status status = ud_sql_session_read_open(work->session, text(query), (ud_string){0},
        text(shape), NULL, work->read_batch, UD_READ_TYPED, &schema);
    ud_batch_destroy(schema);
    uint32_t count = 0, samples = 0;
    size_t capacity = (size_t)expected / work->read_batch + 2;
    uint64_t *latencies = calloc(capacity, sizeof(*latencies));
    if (!latencies && !status)
        status = UD_RESOURCE;
    *first = *p95 = *checksum = 0;
    while (!status) {
        ud_batch *batch = NULL;
        uint64_t previous = now_ns();
        status = ud_sql_session_read_next(work->session, &batch);
        uint64_t observed = now_ns();
        if (status == UD_END) {
            status = UD_OK;
            break;
        }
        if (!status && !count)
            *first = observed - start;
        if (!status && samples < capacity)
            latencies[samples++] = observed - previous;
        sample_native(work);
        for (uint32_t row = 0; !status && row < ud_batch_rows(batch); row++) {
            ud_value id = {0}, value = {0};
            status = ud_batch_get(batch, row, 0, &id);
            if (!status)
                status = ud_batch_get(batch, row, 1, &value);
            if (!status && (id.integer != (int64_t)count + 1 || id.is_null || value.is_null ||
                            value.text_length != work->width ||
                            memcmp(value.text, payload, work->width)))
                status = UD_CORRUPT;
            if (!status) {
                *checksum = *checksum * UINT64_C(1099511628211) + (uint64_t)id.integer;
                count++;
            }
        }
        if (!status)
            consume_delay(work->consumer_delay_ms);
        ud_batch_destroy(batch);
    }
    ud_status closed = ud_sql_session_read_close(work->session);
    *elapsed = now_ns() - start;
    if (samples) {
        qsort(latencies, samples, sizeof(*latencies), compare_ns);
        *p95 = latencies[((uint64_t)samples * 95 + 99) / 100 - 1];
    }
    free(latencies);
    return status ? status : closed ? closed : count == expected ? UD_OK : UD_CORRUPT;
}

int main(int argc, char **argv) {
    if (argc != 8)
        return 2;
    const char *config_file = getenv("UD_SQL_BENCHMARK_CONFIG");
    if (!config_file)
        return 77;
    FILE *input = fopen(config_file, "rb");
    if (!input)
        return 77;
    char configuration[65536];
    size_t bytes = fread(configuration, 1, sizeof(configuration), input);
    fclose(input);
    if (!bytes || bytes == sizeof(configuration))
        return 2;
    int32_t dialect = (int32_t)strtol(argv[1], NULL, 10);
    uint32_t rows = (uint32_t)strtoul(argv[3], NULL, 10);
    uint32_t batch = (uint32_t)strtoul(argv[4], NULL, 10);
    uint32_t width = (uint32_t)strtoul(argv[5], NULL, 10);
    uint32_t read_batch = (uint32_t)strtoul(argv[6], NULL, 10);
    uint32_t delay = (uint32_t)strtoul(argv[7], NULL, 10);
    if (!rows || !batch || batch > 512 || !width || width > 2000 ||
        !read_batch || read_batch > 65536 || delay > 1000)
        return 2;
    uint64_t begin = now_ns(), cpu = cpu_ns();
    workload work = {.rows = rows, .read_batch = read_batch, .width = width,
                     .consumer_delay_ms = delay};
    ud_status status = ud_sql_session_open(dialect, text(argv[2]),
        (ud_string){configuration, (uint32_t)bytes}, &work.session);
    uint64_t open_ns = now_ns() - begin;
    volatile char *secret = configuration;
    while (bytes)
        secret[--bytes] = 0;
    if (status) {
        fprintf(stderr, "Native SQL benchmark connection failed (%d)\n", status);
        return 1;
    }
    work.context = ud_sql_session_context(work.session);
    uint64_t warm_ns = 0;
    status = warm_round_trip(work.session, dialect, &warm_ns);
    char table[32], create[224], select[180], drop[72];
    snprintf(table, sizeof(table), "ud_perf_%x_%x", process_id(), (unsigned)(begin & 0xffffff));
    work.table = text(table);
    int mysql = dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB;
    char quote = mysql ? 96 : '"';
    const char *key_type = dialect == UD_SQL_ORACLE ? "NUMBER(19,0)" : "BIGINT";
    const char *value_type = dialect == UD_SQL_ORACLE ? "VARCHAR2(2000)"
                            : dialect == UD_SQL_SQLSERVER ? "NVARCHAR(2000)" : "VARCHAR(2000)";
    snprintf(create, sizeof(create), "CREATE TABLE %c%s%c (%cid%c %s PRIMARY KEY, "
        "%cpayload%c %s NOT NULL)", quote, table, quote, quote, quote, key_type, quote, quote,
        value_type);
    snprintf(select, sizeof(select), "SELECT %cid%c,%cpayload%c FROM %c%s%c ORDER BY %cid%c",
        quote, quote, quote, quote, quote, table, quote, quote, quote);
    snprintf(drop, sizeof(drop), "DROP TABLE %c%s%c", quote, table, quote);
    if (!status)
        status = command(work.session, create);
    int created = !status;
    char payload[2000];
    memset(payload, 'a', width);
    uint64_t insert_ns = 0, update_ns = 0, delete_ns = 0, read_ns = 0;
    uint64_t first_ns = 0, p95_ns = 0, checksum = 0, ignored = 0;
    uint32_t insert_calls = 0, update_calls = 0, delete_calls = 0;
    if (!status)
        status = mutate(&work, UD_WRITE_APPEND, batch, payload, &insert_ns, &insert_calls);
    memset(payload, 'b', width);
    if (!status)
        status = mutate(&work, UD_WRITE_UPDATE, batch, payload, &update_ns, &update_calls);
    if (!status)
        status = scan(&work, select, payload, rows, &read_ns, &first_ns, &p95_ns, &checksum);
    if (!status)
        status = mutate(&work, UD_WRITE_DELETE, batch, payload, &delete_ns, &delete_calls);
    if (!status)
        status = scan(&work, select, payload, 0, &ignored, &ignored, &ignored, &ignored);
    if (created) {
        ud_status dropped = command(work.session, drop);
        if (!status)
            status = dropped;
    }
    ud_status closed = ud_sql_session_close(work.session);
    if (!status)
        status = closed;
    if (status) {
        fprintf(stderr, "Native SQL benchmark did not validate (%d)\n", status);
        return 1;
    }
    printf("{\"rows\":%u,\"writeBatchRows\":%u,\"readBatchRows\":%u,\"width\":%u,"
           "\"consumerDelayMs\":%u,\"openNs\":%" PRIu64 ",\"warmRoundTripNs\":%" PRIu64
           ",\"insertNs\":%" PRIu64 ",\"updateNs\":%" PRIu64 ",\"deleteNs\":%" PRIu64
           ",\"readNs\":%" PRIu64 ",\"firstBatchNs\":%" PRIu64 ",\"fetchP95Ns\":%" PRIu64
           ",\"cpuNs\":%" PRIu64 ",\"peakRssBytes\":%" PRIu64 ",\"observedNativeBytes\":%" PRIu64
           ",\"insertCalls\":%u,\"updateCalls\":%u,\"deleteCalls\":%u,\"readChecksum\":\"%016" PRIx64 "\"}\n",
           rows, batch, read_batch, width, delay, open_ns, warm_ns, insert_ns, update_ns, delete_ns, read_ns, first_ns,
           p95_ns, cpu_ns() - cpu, peak_rss(), work.peak, insert_calls, update_calls, delete_calls,
           checksum);
    return 0;
}
