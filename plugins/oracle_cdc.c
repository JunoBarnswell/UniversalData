#include "oracle_internal.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#define TRY(call)                                                                                  \
    do {                                                                                           \
        ud_status status = (call);                                                                 \
        if (status)                                                                                \
            return status;                                                                         \
    } while (0)
/* LogMiner is the actual redo consumer. An AS OF SCN snapshot is followed by
 * committed transactions mined from the pre-snapshot active-transaction floor.
 * Cursor includes database incarnation, commit SCN, XID tie break and log floor.
 * No SQL_REDO parsing, timestamp cursor, private scheduler or private journal. */
struct ud_oracle_cdc {
    oracle *owner;
    ud_batch *schema, *log_schema, *chunk;
    char *table, *schema_name, *table_name, *transaction, *container;
    uint8_t *temporal;
    uint64_t dbid, incarnation, position, confirmed, floor, next_floor, high, transaction_scn;
    char xid[17], confirmed_xid[17];
    uint32_t batch_rows, row;
    int snapshot, snapshot_started, mining, reading, in_transaction, fence;
};
static ud_status scalar(oracle *p, const char *sql, ud_type type, ud_value *out) {
    ud_field field = {.name = "value", .name_length = 5, .type = type, .nullable = 1};
    ud_batch *batch = NULL;
    ud_status status = ud_oci_query(p, sql, NULL, 0, &field, 1, &batch);
    if (!status && p->host.rows(batch) != 1)
        status = UD_CORRUPT;
    if (!status)
        status = p->host.get(batch, 0, 0, out);
    if (!status && !out->is_null && (type == UD_UTF8 || type == UD_BINARY)) {
        out->text = plugin_bytes(&p->host, out->text, out->text_length);
        if (!out->text)
            status = UD_RESOURCE;
    }
    p->host.batch_destroy(batch);
    return status;
}
static ud_status unsigned_value(oracle *p, const char *sql, uint64_t *out) {
    ud_value value;
    TRY(scalar(p, sql, UD_INT64, &value));
    if (value.is_null || value.integer < 0)
        return UD_CORRUPT;
    *out = (uint64_t)value.integer;
    return UD_OK;
}
static ud_status command(oracle *p, const char *sql, const ud_batch *parameters) {
    ud_read_options options = {.struct_size = sizeof(options),
                               .abi_version = UD_ABI_VERSION,
                               .sql = {sql, (uint32_t)strlen(sql)},
                               .parameters = parameters};
    return ud_oci_state_command(p, &options);
}
static ud_status params(oracle *p, const ud_value *values, uint32_t count, ud_batch **out) {
    ud_field *fields = p->host.allocate(p->host.context, count * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (uint32_t i = 0; i < count; i++) {
        char name[24];
        int n = snprintf(name, sizeof(name), "p%u", i + 1);
        fields[i] = (ud_field){.name = plugin_string(&p->host, name, (uint32_t)n),
                               .name_length = (uint32_t)n,
                               .type = values[i].type,
                               .precision = values[i].precision,
                               .scale = values[i].scale,
                               .nullable = values[i].is_null};
        if (!fields[i].name)
            status = UD_RESOURCE;
    }
    if (!status)
        status = p->host.batch_create(p->host.context, fields, count, 1, out);
    if (!status)
        status = p->host.append(*out, values, count);
    for (uint32_t i = 0; i < count; i++)
        p->host.deallocate((void *)fields[i].name);
    p->host.deallocate(fields);
    return status;
}
static ud_status position_json(struct ud_oracle_cdc *c, uint64_t scn, const char *xid,
                               uint64_t floor, ud_string *out) {
    char text[256];
    int length =
        snprintf(text, sizeof(text),
                 "{\"dbid\":%llu,\"incarnation\":%llu,\"scn\":%llu,\"xid\":\"%s\",\"floor\":%llu}",
                 (unsigned long long)c->dbid, (unsigned long long)c->incarnation,
                 (unsigned long long)scn, xid, (unsigned long long)floor);
    if (length < 0 || (size_t)length >= sizeof(text))
        return UD_RESOURCE;
    *out = (ud_string){plugin_string(&c->owner->host, text, (uint32_t)length), (uint32_t)length};
    return out->data ? UD_OK : UD_RESOURCE;
}
static ud_status number(oracle *p, const ud_json *document, const char *name, uint64_t *out) {
    const ud_json *value = p->host.json_find(document, name, (uint32_t)strlen(name));
    const char *text;
    uint32_t n;
    TRY(p->host.json_text(value, &text, &n));
    if (p->host.json_kind(value) != UD_JSON_NUMBER || !n)
        return UD_SCHEMA;
    *out = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (text[i] < '0' || text[i] > '9' || *out > (INT64_MAX - (uint64_t)(text[i] - '0')) / 10)
            return UD_SCHEMA;
        *out = *out * 10 + (uint64_t)(text[i] - '0');
    }
    return UD_OK;
}
static ud_status read_position(struct ud_oracle_cdc *c, ud_string text, uint64_t *scn,
                               uint64_t *floor, char xid[17]) {
    oracle *p = c->owner;
    ud_json *document = NULL;
    ud_status status = p->host.json_parse(p->host.context, text.data, text.length, &document, NULL);
    const char *keys[] = {"dbid", "incarnation", "scn", "xid", "floor"};
    if (!status)
        status = p->host.json_keys(document, keys, 5);
    uint64_t dbid = 0, incarnation = 0;
    if (!status)
        status = number(p, document, "dbid", &dbid);
    if (!status)
        status = number(p, document, "incarnation", &incarnation);
    if (!status && (dbid != c->dbid || incarnation != c->incarnation))
        status = UD_FENCED;
    if (!status)
        status = number(p, document, "scn", scn);
    if (!status)
        status = number(p, document, "floor", floor);
    const char *value = NULL;
    uint32_t length = 0;
    if (!status)
        status = p->host.json_text(p->host.json_find(document, "xid", 3), &value, &length);
    if (!status && (length != 16 || *floor > *scn))
        status = UD_CORRUPT;
    for (uint32_t i = 0; !status && i < 16; i++) {
        char ch = value[i];
        if (!((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F')))
            status = UD_CORRUPT;
        else
            xid[i] = ch;
    }
    xid[16] = 0;
    p->host.json_destroy(document);
    return status;
}
static ud_status stop_mining(struct ud_oracle_cdc *c) {
    oracle *p = c->owner;
    ud_oci_read_close(p);
    c->reading = 0;
    if (!c->mining)
        return UD_OK;
    ud_status status = command(p, "BEGIN DBMS_LOGMNR.END_LOGMNR; END;", NULL);
    if (!status)
        c->mining = 0;
    return status;
}
static ud_status preflight(struct ud_oracle_cdc *c) {
    oracle *p = c->owner;
    ud_value value;
    TRY(scalar(p, "SELECT log_mode AS \"value\" FROM v$database", UD_UTF8, &value));
    int archive =
        !value.is_null && value.text_length == 10 && !memcmp(value.text, "ARCHIVELOG", 10);
    p->host.deallocate((void *)value.text);
    if (!archive)
        return UD_UNSUPPORTED;
    TRY(scalar(p, "SELECT supplemental_log_data_all AS \"value\" FROM v$database", UD_UTF8,
               &value));
    int full = !value.is_null && value.text_length == 3 && !memcmp(value.text, "YES", 3);
    p->host.deallocate((void *)value.text);
    if (!full)
        return UD_UNSUPPORTED;
    ud_string args[] = {{c->schema_name, (uint32_t)strlen(c->schema_name)},
                        {c->table_name, (uint32_t)strlen(c->table_name)}};
    ud_field field = {.name = "value", .name_length = 5, .type = UD_INT64};
    ud_batch *proof = NULL;
    ud_status status = ud_oci_query(
        p,
        "SELECT COUNT(*) AS \"value\" FROM all_tab_columns WHERE owner=:p1 AND table_name=:p2 AND "
        "data_type IN('BLOB','CLOB','NCLOB','LONG','LONG RAW','BFILE','XMLTYPE')",
        args, 2, &field, 1, &proof);
    if (!status)
        status = p->host.get(proof, 0, 0, &value);
    if (!status && (value.is_null || value.integer))
        status = UD_UNSUPPORTED;
    p->host.batch_destroy(proof);
    proof = NULL;
    uint32_t columns = p->host.columns(c->schema);
    c->temporal = p->host.allocate(p->host.context, columns);
    if (!c->temporal && !status)
        status = UD_RESOURCE;
    ud_field facts[] = {{.name = "name", .name_length = 4, .type = UD_UTF8},
                        {.name = "type", .name_length = 4, .type = UD_UTF8}};
    if (!status)
        status =
            ud_oci_query(p,
                         "SELECT column_name AS \"name\",data_type AS \"type\" FROM "
                         "all_tab_columns WHERE owner=:p1 AND table_name=:p2 ORDER BY column_id",
                         args, 2, facts, 2, &proof);
    if (!status && p->host.rows(proof) != columns)
        status = UD_SCHEMA;
    for (uint32_t i = 0; !status && i < columns; i++) {
        ud_field expected;
        ud_value name, type;
        status = p->host.field(c->schema, i, &expected);
        if (!status)
            status = p->host.get(proof, i, 0, &name);
        if (!status)
            status = p->host.get(proof, i, 1, &type);
        if (!status && (name.is_null || type.is_null || name.text_length != expected.name_length ||
                        memcmp(name.text, expected.name, name.text_length)))
            status = UD_SCHEMA;
        if (!status && (expected.type == UD_TIMESTAMP || expected.type == UD_DATE32)) {
            if (type.text_length == 4 && !memcmp(type.text, "DATE", 4))
                c->temporal[i] = 1;
            else if (type.text_length >= 9 && !memcmp(type.text, "TIMESTAMP", 9)) {
                int timezone = 0;
                for (uint32_t j = 0; j + 9 <= type.text_length; j++)
                    if (!memcmp(type.text + j, "TIME ZONE", 9))
                        timezone = 1;
                c->temporal[i] = timezone ? 3 : 2;
            } else
                status = UD_SCHEMA;
        }
    }
    p->host.batch_destroy(proof);
    return status;
}
static ud_status start_window(struct ud_oracle_cdc *c) {
    oracle *p = c->owner;
    TRY(unsigned_value(p,
                       "SELECT COALESCE((SELECT MIN(start_scn) FROM v$transaction),current_scn) AS "
                       "\"value\" FROM v$database",
                       &c->next_floor));
    TRY(unsigned_value(p, "SELECT current_scn AS \"value\" FROM v$database", &c->high));
    if (c->high < c->position)
        return UD_FENCED;
    if (c->high == c->position)
        return UD_BUSY;
    ud_value boundaries[] = {{.type = UD_INT64, .integer = (int64_t)c->high},
                             {.type = UD_INT64, .integer = (int64_t)c->floor},
                             {.type = UD_INT64, .integer = (int64_t)c->high},
                             {.type = UD_INT64, .integer = (int64_t)c->floor}};
    ud_batch *parameters = NULL, *schema = NULL, *files = NULL;
    ud_status status = params(p, boundaries, 4, &parameters);
    ud_field field = {.name = "name", .name_length = 4, .type = UD_UTF8};
    if (!status)
        status = p->host.batch_create(p->host.context, &field, 1, 0, &schema);
    const char *sql =
        "SELECT DISTINCT name AS \"name\" FROM v$archived_log WHERE name IS NOT NULL AND "
        "deleted='NO' AND status='A' AND first_change#<=:p1 AND next_change#>:p2 UNION SELECT "
        "MIN(f.member) AS \"name\" FROM v$log l JOIN v$logfile f ON f.group#=l.group# WHERE "
        "l.first_change#<=:p3 AND l.next_change#>:p4 GROUP BY l.group#";
    ud_read_options options = {.struct_size = sizeof(options),
                               .abi_version = UD_ABI_VERSION,
                               .batch_size = 1024,
                               .sql = {sql, (uint32_t)strlen(sql)},
                               .parameters = parameters};
    if (!status)
        status = ud_oci_read_open(p, &options, schema);
    if (!status)
        status = ud_oci_read_next(p, &files);
    ud_oci_read_close(p);
    p->host.batch_destroy(parameters);
    p->host.batch_destroy(schema);
    if (status == UD_END)
        status = UD_FENCED;
    if (!status && p->host.rows(files) == 1024)
        status = UD_RESOURCE;
    for (uint32_t row = 0; !status && row < p->host.rows(files); row++) {
        ud_value file;
        status = p->host.get(files, row, 0, &file);
        ud_batch *argument = NULL;
        if (!status)
            status = params(p, &file, 1, &argument);
        if (!status)
            status = command(
                p,
                row ? "BEGIN "
                      "DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME=>:p1,OPTIONS=>DBMS_LOGMNR.ADDFILE); END;"
                    : "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME=>:p1,OPTIONS=>DBMS_LOGMNR.NEW); "
                      "END;",
                argument);
        p->host.batch_destroy(argument);
    }
    p->host.batch_destroy(files);
    ud_value limits[] = {{.type = UD_INT64, .integer = (int64_t)c->floor},
                         {.type = UD_INT64, .integer = (int64_t)c->high}};
    parameters = NULL;
    if (!status)
        status = params(p, limits, 2, &parameters);
    if (!status)
        status = command(p,
                         "BEGIN "
                         "DBMS_LOGMNR.START_LOGMNR(STARTSCN=>:p1,ENDSCN=>:p2,OPTIONS=>DBMS_LOGMNR."
                         "DICT_FROM_ONLINE_CATALOG+DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;",
                         parameters);
    p->host.batch_destroy(parameters);
    if (!status)
        c->mining = 1;
    return status;
}
/* Mining projection uses documented MINE_VALUE/COLUMN_PRESENT over typed redo
 * and undo. Repeated placeholders have distinct positional binds in OCI. */
static ud_status projection(struct ud_oracle_cdc *c, char **sql_out, ud_batch **parameters) {
    oracle *p = c->owner;
    uint32_t columns = p->host.columns(c->schema), count = 10 + columns * 7;
    size_t capacity = 4096 + (size_t)columns * 2000;
    ud_value *values = p->host.allocate(p->host.context, count * sizeof(*values));
    char *text = p->host.allocate(p->host.context, capacity);
    if (!values || !text) {
        p->host.deallocate(values);
        p->host.deallocate(text);
        return UD_RESOURCE;
    }
    ud_status status = UD_OK;
    uint32_t made = 0;
    /* Stored redo names in the root use the ordinary three-part identifier. */
    const char *container = c->container && strcmp(c->container, "CDB$ROOT") ? c->container : NULL;
    for (uint32_t i = 0; !status && i < columns; i++) {
        ud_field field;
        status = p->host.field(c->schema, i, &field);
        if (status)
            break;
        char *name = plugin_identifier(&p->host, (ud_string){field.name, field.name_length});
        size_t prefix = container ? strlen(container) + 1 : 0;
        size_t n = name ? prefix + strlen(c->table) + strlen(name) + 2 : 0;
        char *path = n ? p->host.allocate(p->host.context, n) : NULL;
        if (path)
            snprintf(path, n, "%s%s%s.%s", container ? container : "",
                     container ? ":" : "", c->table, name);
        p->host.deallocate(name);
        if (!path) {
            status = UD_RESOURCE;
            break;
        }
        values[i] =
            (ud_value){.type = UD_UTF8, .text = path, .text_length = (uint32_t)strlen(path)};
        for (uint32_t j = 0; j < 6; j++)
            values[columns + i * 6 + j] = values[i];
        made++;
    }
    size_t at = 0;
    if (!status)
        at = (size_t)snprintf(text, capacity,
                              "SELECT CASE WHEN OPERATION_CODE=5 THEN SCN ELSE COMMIT_SCN END AS "
                              "\"commitScn\",NVL(RAWTOHEX(XID),'0000000000000000') AS "
                              "\"xid\",OPERATION_CODE AS \"operation\",STATUS AS \"status\",CSF AS "
                              "\"continued\",CASE WHEN OPERATION_CODE IN(2,3) THEN (");
    for (uint32_t i = 0; !status && i < columns; i++) {
        int n =
            snprintf(text + at, capacity - at,
                     "%sCASE WHEN DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,:p%u)=1 THEN 0 ELSE 1 END",
                     i ? "+" : "", i + 1);
        if (n < 0 || (size_t)n >= capacity - at)
            status = UD_RESOURCE;
        else
            at += (size_t)n;
    }
    if (!status) {
        int n = snprintf(text + at, capacity - at, ") ELSE 0 END AS \"missingColumns\"");
        if (n < 0 || (size_t)n >= capacity - at)
            status = UD_RESOURCE;
        else
            at += (size_t)n;
    }
    for (uint32_t i = 0; !status && i < columns; i++) {
        ud_field f;
        status = p->host.field(c->schema, i, &f);
        if (status)
            break;
        const char *convert = f.type == UD_BOOL || f.type == UD_INT64 || f.type == UD_DECIMAL128 ||
                                      f.type == UD_FLOAT64
                                  ? "TO_NUMBER"
                              : f.type == UD_BINARY    ? "HEXTORAW"
                              : f.type == UD_DATE32    ? "TO_DATE"
                              : f.type == UD_TIMESTAMP ? c->temporal[i] == 1   ? "TO_DATE"
                                                         : c->temporal[i] == 2 ? "TO_TIMESTAMP"
                                                                               : "TO_TIMESTAMP_TZ"
                                                       : "";
        const char *format = f.type == UD_DATE32 || (f.type == UD_TIMESTAMP && c->temporal[i] == 1)
                                 ? ",'YYYY-MM-DD\"T\"HH24:MI:SS'"
                             : f.type == UD_TIMESTAMP && c->temporal[i] == 2
                                 ? ",'YYYY-MM-DD\"T\"HH24:MI:SS.FF6'"
                             : f.type == UD_TIMESTAMP ? ",'YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM'"
                                                      : "";
        if (f.type == UD_TIME64 || f.type > UD_BINARY) {
            status = UD_UNSUPPORTED;
            break;
        }
        uint32_t base = columns + i * 6 + 1;
        int n = snprintf(text + at, capacity - at,
                         ",CASE WHEN OPERATION_CODE IN(1,2,3) THEN %s%sCASE WHEN "
                         "DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE,:p%u)=1 THEN "
                         "DBMS_LOGMNR.MINE_VALUE(REDO_VALUE,:p%u) ELSE "
                         "DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,:p%u) END%s%s END AS \"after%u\","
                         "CASE WHEN OPERATION_CODE IN(2,3) THEN %s%sCASE WHEN "
                         "DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,:p%u)=1 THEN "
                         "DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,:p%u) ELSE "
                         "DBMS_LOGMNR.MINE_VALUE(REDO_VALUE,:p%u) END%s%s END AS \"before%u\"",
                         convert, *convert ? "(" : "", base, base + 1, base + 2, format,
                         *convert ? ")" : "", i, convert, *convert ? "(" : "", base + 3, base + 4,
                         base + 5, format, *convert ? ")" : "", i);
        if (n < 0 || (size_t)n >= capacity - at)
            status = UD_RESOURCE;
        else
            at += (size_t)n;
    }
    if (!status) {
        uint32_t base = columns * 7;
        values[base] = (ud_value){.type = UD_INT64, .integer = (int64_t)c->position};
        values[base + 1] = values[base];
        values[base + 2] = (ud_value){.type = UD_UTF8, .text = c->xid, .text_length = 16};
        values[base + 3] = (ud_value){.type = UD_INT64, .integer = (int64_t)c->high};
        values[base + 4] = values[base];
        values[base + 5] = values[base + 3];
        values[base + 6] = (ud_value){.type = UD_UTF8,
                                      .text = c->schema_name,
                                      .text_length = (uint32_t)strlen(c->schema_name)};
        values[base + 7] = (ud_value){
            .type = UD_UTF8, .text = c->table_name, .text_length = (uint32_t)strlen(c->table_name)};
        values[base + 8] = (ud_value){.type = UD_UTF8, .text = c->container,
                                     .text_length = c->container ? (uint32_t)strlen(c->container) : 0,
                                     .is_null = c->container == NULL};
        values[base + 9] = values[base + 8];
        base++;
        int n =
            snprintf(text + at, capacity - at,
                     " FROM V$LOGMNR_CONTENTS WHERE (((COMMIT_SCN>:p%u OR (COMMIT_SCN=:p%u AND "
                     "RAWTOHEX(XID)>:p%u)) AND COMMIT_SCN<=:p%u) OR (OPERATION_CODE=5 AND SCN>:p%u "
                     "AND SCN<=:p%u)) AND SEG_OWNER=:p%u AND TABLE_NAME=:p%u "
                     "AND (:p%u IS NULL OR NVL(SRC_CON_NAME,'CDB$ROOT')=:p%u) ORDER BY "
                     "\"commitScn\",\"xid\",SCN,RS_ID,SSN",
                     base, base + 1, base + 2, base + 3, base + 4, base + 5, base + 6, base + 7,
                     base + 8, base + 9);
        if (n < 0 || (size_t)n >= capacity - at)
            status = UD_RESOURCE;
        if (!status)
            status = params(p, values, count, parameters);
    }
    for (uint32_t i = 0; i < made; i++)
        p->host.deallocate((void *)values[i].text);
    p->host.deallocate(values);
    if (status) {
        p->host.deallocate(text);
        return status;
    }
    *sql_out = text;
    return UD_OK;
}
static ud_status log_open(struct ud_oracle_cdc *c) {
    oracle *p = c->owner;
    TRY(start_window(c));
    char *sql = NULL;
    ud_batch *parameters = NULL;
    ud_status status = projection(c, &sql, &parameters);
    ud_read_options options = {.struct_size = sizeof(options),
                               .abi_version = UD_ABI_VERSION,
                               .batch_size = c->batch_rows,
                               .sql = {sql, sql ? (uint32_t)strlen(sql) : 0},
                               .parameters = parameters};
    if (!status)
        status = ud_oci_read_open(p, &options, c->log_schema);
    p->host.batch_destroy(parameters);
    p->host.deallocate(sql);
    if (!status)
        c->reading = 1;
    return status;
}
static ud_status row_available(struct ud_oracle_cdc *c) {
    oracle *p = c->owner;
    if (c->chunk && c->row < p->host.rows(c->chunk))
        return UD_OK;
    p->host.batch_destroy(c->chunk);
    c->chunk = NULL;
    c->row = 0;
    ud_status status = ud_oci_read_next(p, &c->chunk);
    if (status == UD_END) {
        ud_oci_read_close(p);
        c->reading = 0;
    }
    return status;
}
static ud_status append_change(struct ud_oracle_cdc *c, ud_batch *after, ud_batch *before,
                               uint32_t row, int32_t *action) {
    oracle *p = c->owner;
    uint32_t columns = p->host.columns(c->schema);
    ud_value operation, validity, continued, missing;
    TRY(p->host.get(c->chunk, row, 2, &operation));
    TRY(p->host.get(c->chunk, row, 3, &validity));
    TRY(p->host.get(c->chunk, row, 4, &continued));
    TRY(p->host.get(c->chunk, row, 5, &missing));
    if (validity.is_null || continued.is_null || missing.is_null)
        return UD_CORRUPT;
    if (validity.integer || continued.integer || missing.integer) {
        char message[160];
        snprintf(message, sizeof(message),
                 "Oracle LogMiner cannot reconstruct the row (status %lld, continued %lld, "
                 "missing before columns %lld)", (long long)validity.integer,
                 (long long)continued.integer, (long long)missing.integer);
        return p->host.report(p->host.context, UD_UNSUPPORTED, message);
    }
    if (operation.integer == 5)
        return UD_INCOMPATIBLE;
    if (operation.integer < 1 || operation.integer > 3)
        return UD_UNSUPPORTED;
    ud_value *values = p->host.allocate(p->host.context, columns * 2 * sizeof(*values));
    if (!values)
        return UD_RESOURCE;
    ud_status result = UD_OK;
    for (uint32_t i = 0; !result && i < columns; i++) {
        result = p->host.get(c->chunk, row, 6 + i * 2, values + i);
        if (!result)
            result = p->host.get(c->chunk, row, 7 + i * 2, values + columns + i);
        if (!result && operation.integer == 2)
            values[i] = values[columns + i];
        if (!result && operation.integer == 1)
            values[columns + i] = values[i];
    }
    if (!result)
        result = p->host.append(after, values, columns);
    if (!result)
        result = p->host.append(before, values + columns, columns);
    p->host.deallocate(values);
    *action = operation.integer == 1   ? UD_CHANGE_INSERT
              : operation.integer == 2 ? UD_CHANGE_DELETE
                                       : UD_CHANGE_UPDATE;
    return result;
}
ud_status ud_oracle_cdc_open(void *state, const ud_cdc_options *o) {
    oracle *p = state;
    if (p->cursor || p->transaction || p->cdc)
        return UD_BUSY;
    if (!o->schema || !o->table.length || p->host.columns(o->schema) > 4096)
        return UD_SCHEMA;
    const char *dot = memchr(o->table.data, '.', o->table.length);
    if (dot && (dot == o->table.data || dot + 1 == o->table.data + o->table.length ||
                memchr(dot + 1, '.', (size_t)(o->table.data + o->table.length - dot - 1))))
        return UD_SCHEMA;
    struct ud_oracle_cdc *c = p->host.allocate(p->host.context, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    p->cdc = c;
    c->owner = p;
    c->batch_rows = o->batch_rows;
    ud_status status = UD_OK;
    if (dot) {
        c->schema_name = plugin_string(&p->host, o->table.data, (uint32_t)(dot - o->table.data));
        c->table_name =
            plugin_string(&p->host, dot + 1, o->table.length - (uint32_t)(dot - o->table.data) - 1);
    } else {
        ud_value owner = {0};
        status = scalar(p, "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') AS \"value\" FROM dual",
                        UD_UTF8, &owner);
        if (!status && (owner.is_null || !owner.text_length))
            status = UD_SCHEMA;
        if (!status)
            c->schema_name = plugin_string(&p->host, owner.text, owner.text_length);
        p->host.deallocate((void *)owner.text);
        c->table_name = plugin_string(&p->host, o->table.data, o->table.length);
    }
    char *owner = c->schema_name
                      ? plugin_identifier(
                            &p->host, (ud_string){c->schema_name, (uint32_t)strlen(c->schema_name)})
                      : NULL,
         *table = c->table_name
                      ? plugin_identifier(
                            &p->host, (ud_string){c->table_name, (uint32_t)strlen(c->table_name)})
                      : NULL;
    c->table = owner && table ? p->host.allocate(p->host.context, strlen(owner) + strlen(table) + 2)
                              : NULL;
    if (c->table)
        snprintf(c->table, strlen(owner) + strlen(table) + 2, "%s.%s", owner, table);
    p->host.deallocate(owner);
    p->host.deallocate(table);
    ud_field *fields = NULL;
    uint32_t columns = p->host.columns(o->schema);
    if (!status)
        status = c->table ? plugin_fields(&p->host, o->schema, &fields) : UD_RESOURCE;
    if (!status)
        status = p->host.batch_create(p->host.context, fields, columns, 0, &c->schema);
    ud_field *log = p->host.allocate(p->host.context, (6 + columns * 2) * sizeof(*log));
    if (!log && !status)
        status = UD_RESOURCE;
    if (!status) {
        log[0] = (ud_field){.name = "commitScn", .name_length = 9, .type = UD_INT64};
        log[1] = (ud_field){.name = "xid", .name_length = 3, .type = UD_UTF8};
        log[2] = (ud_field){.name = "operation", .name_length = 9, .type = UD_INT64};
        log[3] = (ud_field){.name = "status", .name_length = 6, .type = UD_INT64};
        log[4] = (ud_field){.name = "continued", .name_length = 9, .type = UD_INT64};
        log[5] = (ud_field){.name = "missingColumns", .name_length = 14, .type = UD_INT64};
        for (uint32_t i = 0; i < columns; i++) {
            if (fields[i].type == UD_TIMESTAMP && !p->utc) {
                status = UD_UNSUPPORTED;
                break;
            }
            for (uint32_t side = 0; side < 2; side++) {
                char name[24];
                int n = snprintf(name, sizeof(name), side ? "before%u" : "after%u", i);
                log[6 + i * 2 + side] = fields[i];
                log[6 + i * 2 + side].name = plugin_string(&p->host, name, (uint32_t)n);
                log[6 + i * 2 + side].name_length = (uint32_t)n;
                log[6 + i * 2 + side].identity = (ud_id){0};
                log[6 + i * 2 + side].lineage = NULL;
                log[6 + i * 2 + side].lineage_count = 0;
                log[6 + i * 2 + side].key_order = 0;
                log[6 + i * 2 + side].nullable = 1;
                if (!log[6 + i * 2 + side].name)
                    status = UD_RESOURCE;
            }
        }
        if (!status)
            status = p->host.batch_create(p->host.context, log, 6 + columns * 2, 0, &c->log_schema);
    }
    for (uint32_t i = 6; log && i < 6 + columns * 2; i++)
        p->host.deallocate((void *)log[i].name);
    p->host.deallocate(log);
    p->host.deallocate(fields);
    if (!status)
        status = unsigned_value(p, "SELECT dbid AS \"value\" FROM v$database", &c->dbid);
    if (!status)
        status = unsigned_value(p, "SELECT resetlogs_change# AS \"value\" FROM v$database",
                                &c->incarnation);
    if (!status) {
        ud_value container = {0};
        status = scalar(p, "SELECT CASE WHEN cdb='YES' THEN SYS_CONTEXT('USERENV','CON_NAME') "
                           "END AS \"value\" FROM v$database", UD_UTF8, &container);
        if (!status && !container.is_null) {
            c->container = plugin_string(&p->host, container.text, container.text_length);
            if (!c->container)
                status = UD_RESOURCE;
        }
        p->host.deallocate((void *)container.text);
    }
    if (!status)
        status = preflight(c);
    if (!status && o->mode == UD_CDC_SNAPSHOT_STREAM) {
        status = unsigned_value(p,
                                "SELECT COALESCE((SELECT MIN(start_scn) FROM v$transaction),current_scn) "
                                "AS \"value\" FROM v$database",
                                &c->floor);
        if (!status)
            status =
                unsigned_value(p, "SELECT current_scn AS \"value\" FROM v$database", &c->position);
        memcpy(c->xid, "FFFFFFFFFFFFFFFF", 17);
        c->confirmed = c->position;
        memcpy(c->confirmed_xid, c->xid, 17);
        size_t capacity = strlen(c->table) + 128;
        for (uint32_t i = 0; i < columns; i++) {
            ud_field field;
            p->host.field(c->schema, i, &field);
            capacity += (size_t)field.name_length * 2 + 4;
        }
        char *sql = p->host.allocate(p->host.context, capacity);
        if (!sql && !status)
            status = UD_RESOURCE;
        size_t at = 0;
        if (!status)
            at = (size_t)snprintf(sql, capacity, "SELECT ");
        for (uint32_t i = 0; !status && i < columns; i++) {
            ud_field field;
            status = p->host.field(c->schema, i, &field);
            char *name = plugin_identifier(&p->host, (ud_string){field.name, field.name_length});
            if (!name)
                status = UD_RESOURCE;
            if (!status)
                at += (size_t)snprintf(sql + at, capacity - at, "%s%s", i ? "," : "", name);
            p->host.deallocate(name);
        }
        ud_value value = {.type = UD_INT64, .integer = (int64_t)c->position};
        ud_batch *parameters = NULL;
        if (!status)
            status = params(p, &value, 1, &parameters);
        if (!status) {
            at += (size_t)snprintf(sql + at, capacity - at, " FROM %s AS OF SCN :p1", c->table);
            ud_read_options options = {.struct_size = sizeof(options),
                                       .abi_version = UD_ABI_VERSION,
                                       .batch_size = c->batch_rows,
                                       .sql = {sql, (uint32_t)at},
                                       .parameters = parameters};
            status = ud_oci_read_open(p, &options, c->schema);
        }
        p->host.batch_destroy(parameters);
        p->host.deallocate(sql);
        if (!status)
            c->snapshot = 1;
    } else if (!status) {
        status = read_position(c, o->position, &c->position, &c->floor, c->xid);
        c->confirmed = c->position;
        memcpy(c->confirmed_xid, c->xid, 17);
    }
    if (status) {
        ud_oracle_cdc_close(p);
        return status;
    }
    return UD_OK;
}
static ud_status commit_event(struct ud_oracle_cdc *c, ud_cdc_event *out) {
    out->kind = UD_CDC_COMMIT;
    if (c->fence) {
        c->position = c->high;
        c->floor = c->next_floor;
        memcpy(c->xid, "FFFFFFFFFFFFFFFF", 17);
        c->fence = 0;
    } else {
        c->position = c->transaction_scn;
        if (!c->transaction || strlen(c->transaction) != 16)
            return UD_CORRUPT;
        memcpy(c->xid, c->transaction, 17);
    }
    c->in_transaction = 0;
    return position_json(c, c->position, c->xid, c->floor, &out->position);
}
ud_status ud_oracle_cdc_next(void *state, ud_cdc_event *out) {
    oracle *p = state;
    struct ud_oracle_cdc *c = p->cdc;
    if (!c)
        return UD_INVALID;
    TRY(ud_oci_control(p));
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);
    out->abi_version = UD_ABI_VERSION;
    out->schema_version = 1;
    if (c->snapshot) {
        if (!c->snapshot_started) {
            c->snapshot_started = 1;
            out->kind = UD_CDC_BEGIN;
            out->transaction_id = (ud_string){plugin_string(&p->host, "snapshot", 8), 8};
            return out->transaction_id.data ? UD_OK : UD_RESOURCE;
        }
        ud_status status = ud_oci_read_next(p, &out->after);
        if (status == UD_END) {
            ud_oci_read_close(p);
            c->snapshot = 0;
            out->kind = UD_CDC_COMMIT;
            return position_json(c, c->position, c->xid, c->floor, &out->position);
        }
        if (status)
            return status;
        uint32_t rows = p->host.rows(out->after);
        out->operations = p->host.allocate(p->host.context, rows * sizeof(*out->operations));
        if (!out->operations)
            return UD_RESOURCE;
        for (uint32_t i = 0; i < rows; i++)
            out->operations[i] = UD_CHANGE_INSERT;
        out->kind = UD_CDC_ROWS;
        return position_json(c, c->position, c->xid, c->floor, &out->position);
    }
    if (!c->reading && c->in_transaction)
        return commit_event(c, out);
    if (!c->reading && !c->mining) {
        ud_status status = log_open(c);
        if (status)
            return status;
    }
    ud_status status = c->reading ? row_available(c) : UD_END;
    if (status == UD_END) {
        if (c->in_transaction)
            return commit_event(c, out);
        TRY(stop_mining(c));
        c->fence = 1;
        c->in_transaction = 1;
        out->kind = UD_CDC_BEGIN;
        out->transaction_id = (ud_string){plugin_string(&p->host, "logminer-fence", 14), 14};
        return out->transaction_id.data ? UD_OK : UD_RESOURCE;
    }
    if (status)
        return status;
    ud_value scn, xid;
    TRY(p->host.get(c->chunk, c->row, 0, &scn));
    TRY(p->host.get(c->chunk, c->row, 1, &xid));
    if (scn.is_null || scn.integer < 0 || xid.is_null || xid.text_length != 16)
        return UD_CORRUPT;
    if (c->in_transaction &&
        ((uint64_t)scn.integer != c->transaction_scn || memcmp(xid.text, c->transaction, 16)))
        return commit_event(c, out);
    if (!c->in_transaction) {
        c->in_transaction = 1;
        c->transaction_scn = (uint64_t)scn.integer;
        p->host.deallocate(c->transaction);
        c->transaction = plugin_string(&p->host, xid.text, 16);
        out->kind = UD_CDC_BEGIN;
        out->transaction_id = (ud_string){plugin_string(&p->host, xid.text, 16), 16};
        return c->transaction && out->transaction_id.data ? UD_OK : UD_RESOURCE;
    }
    ud_field *fields = NULL;
    TRY(plugin_fields(&p->host, c->schema, &fields));
    uint32_t columns = p->host.columns(c->schema);
    status = p->host.batch_create(p->host.context, fields, columns, c->batch_rows, &out->after);
    if (!status)
        status =
            p->host.batch_create(p->host.context, fields, columns, c->batch_rows, &out->before);
    p->host.deallocate(fields);
    out->operations = p->host.allocate(p->host.context, c->batch_rows * sizeof(*out->operations));
    if (!out->operations && !status)
        status = UD_RESOURCE;
    while (!status && p->host.rows(out->after) < c->batch_rows) {
        status = row_available(c);
        if (status == UD_END) {
            status = UD_OK;
            break;
        }
        if (status)
            break;
        status = p->host.get(c->chunk, c->row, 0, &scn);
        if (!status)
            status = p->host.get(c->chunk, c->row, 1, &xid);
        if (!status && (scn.is_null || xid.is_null || xid.text_length != 16))
            status = UD_CORRUPT;
        if (status || (uint64_t)scn.integer != c->transaction_scn ||
            memcmp(xid.text, c->transaction, 16))
            break;
        status = append_change(c, out->after, out->before, c->row,
                               out->operations + p->host.rows(out->after));
        if (!status)
            c->row++;
    }
    if (!status && !p->host.rows(out->after))
        status = UD_BUSY;
    if (!status) {
        out->kind = UD_CDC_ROWS;
        status = position_json(c, c->transaction_scn, c->transaction, c->floor, &out->position);
    }
    return status;
}
ud_status ud_oracle_cdc_confirm(void *state, ud_string text) {
    oracle *p = state;
    struct ud_oracle_cdc *c = p->cdc;
    if (!c)
        return UD_INVALID;
    uint64_t scn, floor;
    char xid[17];
    TRY(read_position(c, text, &scn, &floor, xid));
    if (scn < c->confirmed || scn > c->position ||
        (scn == c->confirmed && strcmp(xid, c->confirmed_xid) < 0) ||
        (scn == c->position && strcmp(xid, c->xid) > 0))
        return UD_FENCED;
    c->confirmed = scn;
    memcpy(c->confirmed_xid, xid, 17);
    return UD_OK;
}
ud_status ud_oracle_cdc_close(void *state) {
    oracle *p = state;
    struct ud_oracle_cdc *c = p->cdc;
    if (!c)
        return UD_OK;
    p->cdc = NULL;
    ud_status status = stop_mining(c);
    p->host.batch_destroy(c->schema);
    p->host.batch_destroy(c->log_schema);
    p->host.batch_destroy(c->chunk);
    p->host.deallocate(c->schema_name);
    p->host.deallocate(c->table_name);
    p->host.deallocate(c->table);
    p->host.deallocate(c->transaction);
    p->host.deallocate(c->temporal);
    p->host.deallocate(c->container);
    p->host.deallocate(c);
    return status;
}
