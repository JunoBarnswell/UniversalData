#include "postgresql_metadata.h"
#include "postgresql_cdc.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <stdatomic.h>
#include <time.h>
#define UD_TRY(call)                                                                               \
    do {                                                                                           \
        ud_status status = (call);                                                                 \
        if (status)                                                                                \
            return status;                                                                         \
    } while (0)
/* pgoutput logical replication and an exported MVCC snapshot share one fenced
 * source position. Both sockets belong to the same leased Native driver. */
struct ud_pg_cdc {
    ud_host_api host;
    PGconn *replication, *snapshot;
    PGcancelConn *cancel, *snapshot_cancel;
    ud_batch *schema;
    char *slot, *publication, *table, *position, *transaction;
    uint64_t lsn, confirmed, version;
    uint32_t relation, batch_rows, phase, timeline;
    char system_id[32];
    uint32_t *oids;
    uint8_t *key_columns, *pending_keys;
    uint32_t *pending_oids;
    uint32_t pending_columns;
    char *field_namespace;
    int schema_changed, pending_invalid;
    int snapshot_mode, streaming;
    atomic_int cancelled;
};
static ud_status check(ud_pg_cdc *p) {
    if (atomic_load(&p->cancelled) || p->host.cancelled(p->host.context))
        return UD_CANCELLED;
    return p->host.check_io ? p->host.check_io(p->host.control_user) : UD_OK;
}
static ud_status pg_status(ud_pg_cdc *p, PGresult *r) {
    ud_status s = check(p);
    if (s)
        return s;
    const char *code = r ? PQresultErrorField(r, PG_DIAG_SQLSTATE) : NULL;
    return code && !strncmp(code, "28", 2)   ? UD_AUTH
           : code && !strcmp(code, "42501")  ? UD_AUTH
           : code && !strcmp(code, "42710")  ? UD_BUSY
           : code && !strncmp(code, "08", 2) ? UD_TRANSIENT
                                             : UD_IO;
}
static ud_status sql(ud_pg_cdc *p, PGconn *c, const char *command) {
    PGresult *r = PQexec(c, command);
    ud_status s =
        r && (PQresultStatus(r) == PGRES_COMMAND_OK || PQresultStatus(r) == PGRES_TUPLES_OK)
            ? UD_OK
            : pg_status(p, r);
    PQclear(r);
    return s;
}
static char *identifier(ud_pg_cdc *p, PGconn *c, ud_string value) {
    char *q = PQescapeIdentifier(c, value.data, value.length);
    if (!q)
        return NULL;
    char *out = plugin_string(&p->host, q, (uint32_t)strlen(q));
    PQfreemem(q);
    return out;
}
static ud_status parse_lsn(ud_string text, uint64_t *out) {
    if (!text.data || !text.length)
        return UD_SCHEMA;
    uint64_t high = 0, low = 0;
    int slash = 0;
    for (uint32_t i = 0; i < text.length; i++) {
        char c = text.data[i];
        if (c == '/' && !slash) {
            slash = 1;
            continue;
        }
        int v = c >= '0' && c <= '9'   ? c - '0'
                : c >= 'A' && c <= 'F' ? c - 'A' + 10
                : c >= 'a' && c <= 'f' ? c - 'a' + 10
                                       : -1;
        uint64_t *n = slash ? &low : &high;
        if (v < 0 || *n > UINT32_MAX / 16)
            return UD_SCHEMA;
        *n = *n * 16 + (unsigned)v;
    }
    if (!slash)
        return UD_SCHEMA;
    *out = (high << 32) | low;
    return UD_OK;
}
static void lsn_text(uint64_t lsn, char out[32]) {
    snprintf(out, 32, "%X/%X", (uint32_t)(lsn >> 32), (uint32_t)lsn);
}
static PGconn *connect_peer(ud_pg_cdc *p, const char *config, int replication) {
    const char *keys[] = {"dbname", "replication", "connect_timeout", "application_name", NULL};
    const char *values[] = {config, replication ? "database" : "false", "10", "UniversalDataCDC",
                            NULL};
    PGconn *c = PQconnectdbParams(keys, values, 1);
    if (!c || PQstatus(c) != CONNECTION_OK) {
        if (c)
            PQfinish(c);
        return NULL;
    }
    PQconninfoOption *info = PQconninfo(c);
    int verified = 0;
    for (PQconninfoOption *i = info; i && i->keyword; i++)
        if (!strcmp(i->keyword, "sslmode") && i->val && !strcmp(i->val, "verify-full"))
            verified = 1;
    PQconninfoFree(info);
    if (!verified || !PQsslInUse(c)) {
        PQfinish(c);
        return NULL;
    }
    return c;
}
static ud_status position_text(ud_pg_cdc *p, uint64_t lsn, ud_string *out) {
    char position[32];
    lsn_text(lsn, position);
    char text[256];
    int n = snprintf(text, sizeof(text), "{\"systemId\":\"%s\",\"timeline\":%u,\"lsn\":\"%s\"}",
                     p->system_id, p->timeline, position);
    if (n < 0 || (size_t)n >= sizeof(text))
        return UD_RESOURCE;
    *out = (ud_string){plugin_string(&p->host, text, (uint32_t)n), (uint32_t)n};
    return out->data ? UD_OK : UD_RESOURCE;
}
static ud_status position_read(ud_pg_cdc *p, ud_string text, uint64_t *out) {
    ud_json *document = NULL;
    ud_status status = p->host.json_parse(p->host.context, text.data, text.length, &document, NULL);
    const char *keys[] = {"systemId", "timeline", "lsn"};
    if (!status)
        status = p->host.json_keys(document, keys, 3);
    const char *value;
    uint32_t n;
    if (!status)
        status = p->host.json_text(p->host.json_find(document, "systemId", 8), &value, &n);
    if (!status && (n != strlen(p->system_id) || memcmp(value, p->system_id, n)))
        status = UD_FENCED;
    if (!status)
        status = p->host.json_text(p->host.json_find(document, "timeline", 8), &value, &n);
    uint64_t timeline = 0;
    for (uint32_t i = 0; !status && i < n; i++) {
        if (value[i] < '0' || value[i] > '9' || timeline > UINT32_MAX / 10)
            status = UD_SCHEMA;
        else
            timeline = timeline * 10 + (unsigned)(value[i] - '0');
    }
    if (!status && timeline != p->timeline)
        status = UD_FENCED;
    if (!status)
        status = p->host.json_text(p->host.json_find(document, "lsn", 3), &value, &n);
    if (!status)
        status = parse_lsn((ud_string){value, n}, out);
    p->host.json_destroy(document);
    return status;
}
static ud_status preflight(ud_pg_cdc *p, const ud_cdc_options *o, int managed) {
    const char *values[] = {p->table, p->publication};
    PGresult *r = PQexecParams(
        p->snapshot,
        "SELECT (c.relreplident='f' OR (c.relreplident IN ('d','i') AND EXISTS(SELECT 1 "
        "FROM pg_index ri WHERE ri.indrelid=c.oid AND "
        "((c.relreplident='d' AND ri.indisprimary) OR (c.relreplident='i' AND "
        "ri.indisreplident))))) "
        "AND c.relkind='r' AND NOT c.relrowsecurity AND pub.pubinsert "
        "AND pub.pubupdate AND pub.pubdelete AND (pub.puballtables OR (pr.prrelid IS NOT NULL AND "
        "pr.prqual IS NULL AND pr.prattrs IS NULL)) FROM pg_class c JOIN pg_namespace n ON "
        "n.oid=c.relnamespace CROSS JOIN pg_publication pub LEFT JOIN pg_publication_rel pr ON "
        "pr.prpubid=pub.oid AND pr.prrelid=c.oid WHERE c.oid=to_regclass($1) AND pub.pubname=$2",
        2, NULL, values, NULL, NULL, 0);
    ud_status status = r && PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) == 1 &&
                               !PQgetisnull(r, 0, 0) && !strcmp(PQgetvalue(r, 0, 0), "t")
                           ? UD_OK
                       : r && PQresultStatus(r) == PGRES_TUPLES_OK ? UD_UNSUPPORTED
                                                                   : pg_status(p, r);
    PQclear(r);
    if (status)
        return status;
    r = PQexecParams(p->snapshot,
                     "SELECT a.atttypid,EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c "
                     "ON c.oid=i.indrelid WHERE i.indrelid=a.attrelid AND a.attnum=ANY(i.indkey) "
                     "AND ((c.relreplident='i' AND i.indisreplident) OR "
                     "(c.relreplident IN ('d','f') AND i.indisprimary))) FROM pg_attribute a "
                     "WHERE a.attrelid=to_regclass($1) AND a.attnum>0 AND NOT a.attisdropped "
                     "ORDER BY a.attnum",
                     1, NULL, values, NULL, NULL, 0);
    if (!r || PQresultStatus(r) != PGRES_TUPLES_OK ||
        PQntuples(r) != (int)p->host.columns(p->schema)) {
        status = r && PQresultStatus(r) == PGRES_TUPLES_OK ? UD_INCOMPATIBLE : pg_status(p, r);
        PQclear(r);
        return status;
    }
    p->oids = p->host.allocate(p->host.context, p->host.columns(p->schema) * sizeof(*p->oids));
    p->key_columns = p->host.allocate(p->host.context, p->host.columns(p->schema));
    if (!p->oids || !p->key_columns) {
        PQclear(r);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < p->host.columns(p->schema); i++) {
        char *end = NULL;
        unsigned long oid = strtoul(PQgetvalue(r, (int)i, 0), &end, 10);
        if (!end || *end || oid > UINT32_MAX) {
            status = UD_CORRUPT;
            break;
        }
        p->oids[i] = (uint32_t)oid;
        p->key_columns[i] = !strcmp(PQgetvalue(r, (int)i, 1), "t");
    }
    PQclear(r);
    if (status)
        return status;
    char *slot_name = plugin_string(&p->host, o->slot.data, o->slot.length);
    if (!slot_name)
        return UD_RESOURCE;
    const char *slot_values[] = {slot_name};
    r = PQexecParams(p->snapshot,
                     "SELECT plugin,active FROM pg_replication_slots WHERE slot_name=$1", 1, NULL,
                     slot_values, NULL, NULL, 0);
    p->host.deallocate(slot_name);
    if (!r || PQresultStatus(r) != PGRES_TUPLES_OK) {
        status = pg_status(p, r);
        PQclear(r);
        return status;
    }
    if (PQntuples(r)) {
        if (strcmp(PQgetvalue(r, 0, 0), "pgoutput") || strcmp(PQgetvalue(r, 0, 1), "f")) {
            PQclear(r);
            return UD_BUSY;
        }
        PQclear(r);
        if (o->mode == UD_CDC_SNAPSHOT_STREAM) {
            if (!managed)
                return UD_FENCED;
            size_t n = strlen(p->slot) + 64;
            char *sql = p->host.allocate(p->host.context, n);
            if (!sql)
                return UD_RESOURCE;
            snprintf(sql, n, "DROP_REPLICATION_SLOT %s", p->slot);
            status = sql ? UD_OK : UD_RESOURCE;
            PGresult *drop = PQexec(p->replication, sql);
            if (!drop || PQresultStatus(drop) != PGRES_COMMAND_OK)
                status = pg_status(p, drop);
            PQclear(drop);
            p->host.deallocate(sql);
        }
        return status;
    }
    PQclear(r);
    return o->mode == UD_CDC_STREAM ? UD_FENCED : UD_OK;
}
static ud_status tuple_value(ud_pg_cdc *p, const ud_field *f, const char *data, uint32_t n,
                             ud_value *out) {
    if (f->type == UD_BINARY) {
        if (n < 2 || data[0] != '\\' || data[1] != 'x' || (n - 2) % 2)
            return UD_PARSE;
        char *bytes = p->host.allocate(p->host.context, (n - 2) / 2 + 1);
        if (!bytes)
            return UD_RESOURCE;
        for (uint32_t i = 2; i < n; i += 2) {
            uint32_t value = 0;
            for (uint32_t k = 0; k < 2; k++) {
                char c = data[i + k];
                int v = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1;
                if (v < 0) {
                    p->host.deallocate(bytes);
                    return UD_PARSE;
                }
                value = value * 16 + (unsigned)v;
            }
            bytes[(i - 2) / 2] = (char)value;
        }
        *out = (ud_value){.type = UD_BINARY, .text = bytes, .text_length = (n - 2) / 2};
        return UD_OK;
    }
    return ud_pg_decode_scalar(&p->host, f, data, n, out);
}
static ud_status column_value(ud_pg_cdc *p, uint32_t column, const ud_field *field,
                              const char *data, uint32_t n, ud_value *out) {
    ud_status status = tuple_value(p, field, data, n, out);
    if (!status && field->type == UD_FLOAT64 && p->oids && p->oids[column] == 700)
        out->real = (double)(float)out->real;
    return status;
}
static ud_status snapshot_next(ud_pg_cdc *p, ud_cdc_event *out) {
    if (p->phase == 0) {
        p->phase = 1;
        out->kind = UD_CDC_BEGIN;
        out->transaction_id = (ud_string){plugin_string(&p->host, "snapshot", 8), 8};
        return out->transaction_id.data ? UD_OK : UD_RESOURCE;
    }
    char command[80];
    snprintf(command, sizeof(command), "FETCH FORWARD %u FROM ud_cdc_snapshot", p->batch_rows);
    PGresult *r = PQexec(p->snapshot, command);
    if (!r || PQresultStatus(r) != PGRES_TUPLES_OK) {
        ud_status s = pg_status(p, r);
        PQclear(r);
        return s;
    }
    uint32_t rows = (uint32_t)PQntuples(r), columns = p->host.columns(p->schema);
    ud_status s = PQnfields(r) != (int)columns ? UD_SCHEMA : UD_OK;
    if (!s && !rows) {
        PQclear(r);
        s = sql(p, p->snapshot, "COMMIT");
        if (!s) {
            p->phase = 2;
            p->snapshot_mode = 0;
            out->kind = UD_CDC_COMMIT;
            char position[32];
            lsn_text(p->lsn, position);
            s = position_text(p, p->lsn, &out->position);
        }
        return s;
    }
    ud_field *f = NULL;
    ud_value *values = NULL;
    if (!s)
        s = plugin_fields(&p->host, p->schema, &f);
    if (!s)
        s = p->host.batch_create(p->host.context, f, columns, rows, &out->after);
    if (!s) {
        values = p->host.allocate(p->host.context, columns * sizeof(*values));
        out->operations = p->host.allocate(p->host.context, rows * sizeof(*out->operations));
        if (!values || !out->operations)
            s = UD_RESOURCE;
    }
    for (uint32_t row = 0; !s && row < rows; row++) {
        for (uint32_t i = 0; !s && i < columns; i++) {
            values[i] = (ud_value){.type = f[i].type,
                                   .precision = f[i].precision,
                                   .scale = f[i].scale,
                                   .is_null = PQgetisnull(r, row, i)};
            if (!values[i].is_null)
                s = column_value(p, i, f + i, PQgetvalue(r, row, i),
                                 (uint32_t)PQgetlength(r, row, i), values + i);
        }
        if (!s)
            s = p->host.append(out->after, values, columns);
        for (uint32_t i = 0; i < columns; i++)
            if (values[i].type == UD_BINARY && !values[i].is_null)
                p->host.deallocate((void *)values[i].text);
        if (!s)
            out->operations[row] = UD_CHANGE_INSERT;
    }
    p->host.deallocate(f);
    p->host.deallocate(values);
    PQclear(r);
    out->kind = UD_CDC_ROWS;
    out->position = (ud_string){plugin_string(&p->host, "snapshot", 8), 8};
    return s;
}
typedef struct packet {
    const uint8_t *data;
    size_t length, at;
} packet;
static int take(packet *p, uint32_t n, const uint8_t **out) {
    if (n > p->length - p->at)
        return 0;
    *out = p->data + p->at;
    p->at += n;
    return 1;
}
static int integer(packet *p, uint32_t bytes, uint64_t *out) {
    const uint8_t *data;
    if (!take(p, bytes, &data))
        return 0;
    *out = 0;
    for (uint32_t i = 0; i < bytes; i++)
        *out = (*out << 8) | data[i];
    return 1;
}
static int string(packet *p, const char **out, uint32_t *n) {
    const uint8_t *end = memchr(p->data + p->at, 0, p->length - p->at);
    if (!end)
        return 0;
    *out = (const char *)p->data + p->at;
    *n = (uint32_t)(end - p->data - p->at);
    p->at += (size_t)*n + 1;
    return 1;
}
static ud_status tuple(ud_pg_cdc *p, packet *input, ud_value *values, const ud_value *prior,
                       int key_only) {
    uint64_t columns;
    if (!integer(input, 2, &columns) || columns != p->host.columns(p->schema))
        return UD_SCHEMA;
    for (uint32_t i = 0; i < columns; i++) {
        uint64_t kind;
        ud_field field;
        if (!integer(input, 1, &kind))
            return UD_CORRUPT;
        ud_status s = p->host.field(p->schema, i, &field);
        if (s)
            return s;
        values[i] = (ud_value){
            .type = field.type, .precision = field.precision, .scale = field.scale, .is_null = 1};
        if (key_only && !p->key_columns[i]) {
            if (kind != 'n')
                return UD_CORRUPT;
            values[i].is_absent = 1;
            continue;
        }
        if (kind == 'n')
            continue;
        if (kind == 'u') {
            if (prior && !prior[i].is_absent)
                values[i] = prior[i];
            else
                values[i].is_absent = 1;
            continue;
        }
        uint64_t n;
        const uint8_t *text;
        if (kind != 't' || !integer(input, 4, &n) || n > UINT32_MAX ||
            !take(input, (uint32_t)n, &text))
            return UD_CORRUPT;
        ud_status status = column_value(p, i, &field, (const char *)text, (uint32_t)n, values + i);
        if (status)
            return status;
    }
    return UD_OK;
}
static ud_status decode(ud_pg_cdc *p, packet *input, ud_cdc_event *out) {
    uint64_t kind;
    if (!integer(input, 1, &kind))
        return UD_CORRUPT;
    uint64_t value;
    if (kind == 'B') {
        uint64_t final, time, xid;
        if (!integer(input, 8, &final) || !integer(input, 8, &time) || !integer(input, 4, &xid))
            return UD_CORRUPT;
        char id[32];
        int n = snprintf(id, sizeof(id), "%llu", (unsigned long long)xid);
        p->host.deallocate(p->transaction);
        p->transaction = plugin_string(&p->host, id, (uint32_t)n);
        out->kind = UD_CDC_BEGIN;
        out->transaction_id = (ud_string){plugin_string(&p->host, id, (uint32_t)n), (uint32_t)n};
        return out->transaction_id.data ? UD_OK : UD_RESOURCE;
    }
    if (kind == 'C') {
        uint64_t flags, commit, end, time;
        if (!integer(input, 1, &flags) || !integer(input, 8, &commit) || !integer(input, 8, &end) ||
            !integer(input, 8, &time))
            return UD_CORRUPT;
        if (end < p->lsn)
            return UD_FENCED;
        p->lsn = end;
        char position[32];
        lsn_text(end, position);
        out->kind = UD_CDC_COMMIT;
        ud_status status = position_text(p, p->lsn, &out->position);
        out->watermark = (int64_t)time + INT64_C(946684800000000);
        return status;
    }
    if (kind == 'R') {
        const char *schema, *table;
        uint32_t ns, nt;
        uint64_t relation, identity, columns;
        if (!integer(input, 4, &relation) || !string(input, &schema, &ns) ||
            !string(input, &table, &nt) || !integer(input, 1, &identity) ||
            !integer(input, 2, &columns))
            return UD_CORRUPT;
        char *qualified = p->host.allocate(p->host.context, (size_t)ns + nt + 2);
        if (!qualified)
            return UD_RESOURCE;
        memcpy(qualified, schema, ns);
        qualified[ns] = '.';
        memcpy(qualified + ns + 1, table, nt);
        qualified[ns + nt + 1] = 0;
        int match =
            !strcmp(p->table, qualified) ||
            (!strchr(p->table, '.') && strlen(p->table) == nt && !memcmp(p->table, table, nt));
        p->host.deallocate(qualified);
        if (!match)
            return UD_BUSY;
        if (columns > 65536 || !columns || (identity != 'f' && identity != 'd' && identity != 'i'))
            return UD_INCOMPATIBLE;
        const char *values[] = {p->table};
        PGresult *result =
            PQexecParams(p->snapshot, ud_pg_metadata_query(), 1, NULL, values, NULL, NULL, 0);
        ud_batch *metadata = NULL, *actual = NULL, *aligned = NULL;
        ud_status status = result && PQresultStatus(result) == PGRES_TUPLES_OK
                               ? ud_pg_metadata_decode(&p->host, result, &metadata)
                               : pg_status(p, result);
        PQclear(result);
        if (!status)
            status = p->host.schema_from_metadata(
                p->host.context, metadata,
                (ud_string){p->field_namespace, (uint32_t)strlen(p->field_namespace)},
                p->host.resource_namespace,UD_SQL_POSTGRESQL, &actual);
        if (!status)
            status = p->host.schema_bind_identity(p->host.context, p->schema, actual, &aligned);
        p->host.batch_destroy(metadata);
        p->host.batch_destroy(actual);
        uint32_t *oids =
            !status ? p->host.allocate(p->host.context, (size_t)columns * sizeof(*oids)) : NULL;
        uint8_t *keys = !status ? p->host.allocate(p->host.context, (size_t)columns) : NULL;
        if (!status && (!oids || !keys))
            status = UD_RESOURCE;
        int changed = !status && columns != p->host.columns(p->schema),
            invalid = !status && columns != p->host.columns(aligned);
        for (uint32_t at = 0; !status && at < columns; at++) {
            uint64_t flags, oid, modifier;
            const char *name;
            uint32_t length;
            if (!integer(input, 1, &flags) || !string(input, &name, &length) ||
                !integer(input, 4, &oid) || !integer(input, 4, &modifier) ||
                (flags & ~UINT64_C(1))) {
                status = UD_CORRUPT;
                break;
            }
            ud_field field = {0};
            oids[at] = (uint32_t)oid;
            keys[at] = (uint8_t)(flags & 1);
            if (at < p->host.columns(aligned)) {
                status = p->host.field(aligned, at, &field);
                if (!status && (field.name_length != length || memcmp(field.name, name, length)))
                    invalid = 1;
                if (identity == 'f')
                    keys[at] = field.key_order != 0;
            }
            if (!status && at < p->host.columns(p->schema)) {
                ud_field before;
                status = p->host.field(p->schema, at, &before);
                if (!status && (!p->host.field_matches(&before, &field, UD_FIELD_PORT) ||
                                p->oids[at] != oids[at] || p->key_columns[at] != keys[at]))
                    changed = 1;
                if (!status && p->key_columns[at] != keys[at])
                    invalid = 1;
            }
            (void)modifier;
        }
        p->relation = (uint32_t)relation;
        if (!status && (changed || invalid)) {
            if (p->version == UINT64_MAX)
                status = UD_RESOURCE;
            else {
                p->version++;
                out->kind = UD_CDC_SCHEMA_CHANGE;
                out->schema_version = p->version;
                out->after = aligned;
                aligned = NULL;
                p->pending_oids = oids;
                p->pending_keys = keys;
                p->pending_columns = (uint32_t)columns;
                oids = NULL;
                keys = NULL;
                p->schema_changed = 1;
                p->pending_invalid = invalid;
                status = position_text(p, p->lsn, &out->position);
            }
        }
        p->host.deallocate(oids);
        p->host.deallocate(keys);
        p->host.batch_destroy(aligned);
        return status ? status : changed || invalid ? UD_OK : UD_BUSY;
    }
    if (kind == 'I' || kind == 'U' || kind == 'D') {
        uint64_t relation, tag;
        if (!integer(input, 4, &relation) || !integer(input, 1, &tag))
            return UD_CORRUPT;
        if (relation != p->relation)
            return UD_BUSY;
        uint32_t columns = p->host.columns(p->schema);
        ud_field *fields = NULL;
        ud_value *before = p->host.allocate(p->host.context, columns * sizeof(*before)),
                 *after = p->host.allocate(p->host.context, columns * sizeof(*after));
        ud_status s = !before || !after ? UD_RESOURCE : UD_OK;
        int has_before = kind != 'I' && tag != 'N';
        if (!s && has_before) {
            if (tag != 'O' && tag != 'K')
                s = UD_CORRUPT;
            else
                s = tuple(p, input, before, NULL, tag == 'K');
            if (!s && kind == 'U' && !integer(input, 1, &tag))
                s = UD_CORRUPT;
        }
        if (!s && kind != 'D') {
            if (tag != 'N')
                s = UD_CORRUPT;
            else
                s = tuple(p, input, after, has_before ? before : NULL, 0);
        }
        if (!s && kind == 'D')
            memcpy(after, before, columns * sizeof(*after));
        if (!s && kind == 'U' && !has_before) {
            for (uint32_t i = 0; !s && i < columns; i++) {
                ud_field field;
                s = p->host.field(p->schema, i, &field);
                if (!s)
                    before[i] = (ud_value){.type = field.type,
                                           .precision = field.precision,
                                           .scale = field.scale,
                                           .is_null = 1,
                                           .is_absent = 1};
                if (!s && p->key_columns[i]) {
                    if (after[i].is_null)
                        s = UD_SCHEMA;
                    else
                        before[i] = after[i];
                }
            }
            has_before = 1;
        }
        if (!s)
            s = plugin_fields(&p->host, p->schema, &fields);
        if (!s)
            s = p->host.batch_create(p->host.context, fields, columns, 1, &out->after);
        if (!s)
            s = p->host.append(out->after, after, columns);
        if (!s && has_before)
            s = p->host.batch_create(p->host.context, fields, columns, 1, &out->before);
        if (!s && has_before)
            s = p->host.append(out->before, before, columns);
        if (!s) {
            out->operations = p->host.allocate(p->host.context, sizeof(*out->operations));
            if (!out->operations)
                s = UD_RESOURCE;
            else
                *out->operations = kind == 'I'   ? UD_CHANGE_INSERT
                                   : kind == 'U' ? UD_CHANGE_UPDATE
                                                 : UD_CHANGE_DELETE;
        }
        for (uint32_t i = 0; i < columns; i++) {
            if (before && before[i].type == UD_BINARY && !before[i].is_null)
                p->host.deallocate((void *)before[i].text);
            if (after && after[i].type == UD_BINARY && !after[i].is_null &&
                (!has_before || after[i].text != before[i].text))
                p->host.deallocate((void *)after[i].text);
        }
        p->host.deallocate(before);
        p->host.deallocate(after);
        p->host.deallocate(fields);
        out->kind = UD_CDC_ROWS;
        char position[32];
        lsn_text(p->lsn, position);
        if (!s)
            s = position_text(p, p->lsn, &out->position);
        return s;
    }
    if (kind == 'T') {
        uint64_t count, flags;
        if (!integer(input, 4, &count) || !integer(input, 1, &flags) || (flags & ~UINT64_C(3)))
            return UD_CORRUPT;
        int matches = 0;
        for (uint64_t index = 0; index < count; index++) {
            uint64_t relation;
            if (!integer(input, 4, &relation))
                return UD_CORRUPT;
            if (relation == p->relation)
                matches = 1;
        }
        if (!matches)
            return UD_BUSY;
        out->kind = UD_CDC_TRUNCATE;
        return position_text(p, p->lsn, &out->position);
    }
    if (kind == 'Y' || kind == 'O')
        return UD_BUSY;
    (void)value;
    return UD_UNSUPPORTED;
}
static ud_status start_stream(ud_pg_cdc *p) {
    char position[32];
    lsn_text(p->lsn, position);
    char *publication = PQescapeLiteral(p->replication, p->publication, strlen(p->publication));
    if (!publication)
        return UD_RESOURCE;
    size_t size = strlen(p->slot) + strlen(publication) + 256;
    char *command = p->host.allocate(p->host.context, size);
    if (!command) {
        PQfreemem(publication);
        return UD_RESOURCE;
    }
    snprintf(command, size,
             "START_REPLICATION SLOT %s LOGICAL %s (proto_version '1',publication_names %s)",
             p->slot, position, publication);
    PGresult *r = PQexec(p->replication, command);
    ud_status s = r && PQresultStatus(r) == PGRES_COPY_BOTH ? UD_OK : pg_status(p, r);
    PQclear(r);
    p->host.deallocate(command);
    PQfreemem(publication);
    if (!s && PQsetnonblocking(p->replication, 1))
        s = UD_IO;
    if (!s)
        p->streaming = 1;
    return s;
}
ud_status ud_pg_cdc_next(ud_pg_cdc *p, ud_cdc_event *out) {
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);
    out->abi_version = UD_ABI_VERSION;
    out->schema_version = p->version;
    ud_status s = check(p);
    if (s)
        return s;
    if (p->schema_changed)
        return UD_FENCED;
    if (p->snapshot_mode)
        return snapshot_next(p, out);
    if (!p->streaming) {
        s = start_stream(p);
        if (s)
            return s;
    }
    if (!PQconsumeInput(p->replication))
        return UD_TRANSIENT;
    char *data = NULL;
    int n = PQgetCopyData(p->replication, &data, 1);
    if (!n)
        return UD_BUSY;
    if (n < 0)
        return n == -1 ? UD_END : UD_TRANSIENT;
    packet packet = {(const uint8_t *)data, (size_t)n, 0};
    uint64_t tag;
    if (!integer(&packet, 1, &tag))
        s = UD_CORRUPT;
    else if (tag == 'k') {
        uint64_t end, time, reply;
        if (!integer(&packet, 8, &end) || !integer(&packet, 8, &time) ||
            !integer(&packet, 1, &reply))
            s = UD_CORRUPT;
        else {
            if (reply) {
                ud_string confirmed = {0};
                s = position_text(p, p->confirmed, &confirmed);
                if (!s)
                    s = ud_pg_cdc_confirm(p, confirmed);
                p->host.deallocate((void *)confirmed.data);
            }
            out->kind = UD_CDC_HEARTBEAT;
            out->watermark = (int64_t)time + INT64_C(946684800000000);
        }
    } else if (tag == 'w') {
        uint64_t start, end, time;
        if (!integer(&packet, 8, &start) || !integer(&packet, 8, &end) ||
            !integer(&packet, 8, &time))
            s = UD_CORRUPT;
        else
            s = decode(p, &packet, out);
    } else
        s = UD_CORRUPT;
    PQfreemem(data);
    return s;
}
ud_status ud_pg_cdc_confirm(ud_pg_cdc *p, ud_string position) {
    uint64_t lsn;
    ud_status s = position_read(p, position, &lsn);
    if (s)
        return s;
    if (lsn < p->confirmed || lsn > p->lsn)
        return UD_FENCED;
    /* Replaying a sealed epoch can confirm before the first call to next.
       Start the slot connection so the final position is sent to PostgreSQL. */
    if (!p->streaming && !p->snapshot_mode) {
        s = start_stream(p);
        if (s)
            return s;
    }
    uint8_t message[34] = {0};
    message[0] = 'r';
    uint64_t fields[] = {lsn, lsn, lsn, (uint64_t)time(NULL) * 1000000 - UINT64_C(946684800000000)};
    for (uint32_t i = 0; i < 4; i++)
        for (uint32_t k = 0; k < 8; k++)
            message[1 + i * 8 + k] = (uint8_t)(fields[i] >> (56 - k * 8));
    if (p->streaming && PQputCopyData(p->replication, (const char *)message, sizeof(message)) != 1)
        return UD_TRANSIENT;
    if (p->streaming) {
        int flushed = PQflush(p->replication);
        if (flushed > 0) {
            if (PQsetnonblocking(p->replication, 0))
                return UD_TRANSIENT;
            flushed = PQflush(p->replication);
            if (PQsetnonblocking(p->replication, 1))
                return UD_TRANSIENT;
        }
        if (flushed)
            return UD_TRANSIENT;
    }
    p->confirmed = lsn;
    return UD_OK;
}
ud_status ud_pg_cdc_open(const ud_host_api *h, const char *config, const ud_cdc_options *o,
                         ud_pg_cdc **out) {
    *out = NULL;
    if (!o->publication.length || !o->table.length || !o->schema)
        return UD_SCHEMA;
    ud_cdc_options options = *o;
    char managed_slot[40];
    int managed = 0;
    if (o->source_owner.length == 64) {
        snprintf(managed_slot, sizeof(managed_slot), "ud_%.32s", o->source_owner.data);
        if (!o->slot.length) {
            options.slot = (ud_string){managed_slot, 35};
            managed = 1;
        } else
            managed = o->slot.length == 35 && !memcmp(o->slot.data, managed_slot, 35);
    }
    o = &options;
    if (!o->slot.length)
        return UD_SCHEMA;
    ud_pg_cdc *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    p->batch_rows = o->batch_rows;
    p->version = 1;
    atomic_init(&p->cancelled, 0);
    p->replication = connect_peer(p, config, 1);
    ud_status s = p->replication ? UD_OK : UD_AUTH;
    if (!s)
        p->slot = identifier(p, p->replication, o->slot);
    p->publication = plugin_string(h, o->publication.data, o->publication.length);
    p->table = plugin_string(h, o->table.data, o->table.length);
    p->field_namespace =
        plugin_string(h, o->field_namespace.data ? o->field_namespace.data : "cdc.preflight",
                      o->field_namespace.data ? o->field_namespace.length : 13);
    if (!p->field_namespace)
        s = UD_RESOURCE;
    if (!s && (!p->slot || !p->publication || !p->table))
        s = UD_RESOURCE;
    ud_field *f = NULL;
    if (!s)
        s = plugin_fields(h, o->schema, &f);
    if (!s)
        s = h->batch_create(h->context, f, h->columns(o->schema), 0, &p->schema);
    h->deallocate(f);
    if (!s) {
        PGresult *identity = PQexec(p->replication, "IDENTIFY_SYSTEM");
        if (!identity || PQresultStatus(identity) != PGRES_TUPLES_OK || PQntuples(identity) != 1 ||
            PQgetlength(identity, 0, 0) >= 32)
            s = pg_status(p, identity);
        else {
            snprintf(p->system_id, sizeof(p->system_id), "%s", PQgetvalue(identity, 0, 0));
            char *end = NULL;
            unsigned long timeline = strtoul(PQgetvalue(identity, 0, 1), &end, 10);
            if (!end || *end || !timeline || timeline > UINT32_MAX)
                s = UD_CORRUPT;
            else
                p->timeline = (uint32_t)timeline;
        }
        PQclear(identity);
    }
    if (!s) {
        p->snapshot = connect_peer(p, config, 0);
        if (!p->snapshot)
            s = UD_AUTH;
    }
    if (!s)
        s = sql(p, p->snapshot,
                "SET TIME ZONE 'UTC'; SET DateStyle TO ISO,YMD; SET extra_float_digits=3; SET "
                "bytea_output=hex; SET IntervalStyle TO iso_8601");
    if (!s)
        s = preflight(p, o, managed);
    if (!s && o->mode == UD_CDC_SNAPSHOT_STREAM) {

        size_t length = strlen(p->slot) + 128;
        char *command = !s ? h->allocate(h->context, length) : NULL;
        if (!s && !command)
            s = UD_RESOURCE;
        PGresult *r = NULL;
        if (!s) {
            snprintf(command, length, "CREATE_REPLICATION_SLOT %s LOGICAL pgoutput EXPORT_SNAPSHOT",
                     p->slot);
            r = PQexec(p->replication, command);
            if (!r || PQresultStatus(r) != PGRES_TUPLES_OK || PQntuples(r) != 1)
                s = pg_status(p, r);
        }
        char *snapshot = NULL;
        if (!s) {
            s = parse_lsn((ud_string){PQgetvalue(r, 0, 1), (uint32_t)PQgetlength(r, 0, 1)},
                          &p->lsn);
            snapshot =
                PQescapeLiteral(p->snapshot, PQgetvalue(r, 0, 2), (size_t)PQgetlength(r, 0, 2));
            if (!snapshot)
                s = UD_RESOURCE;
        }
        PQclear(r);
        h->deallocate(command);
        if (!s)
            s = sql(p, p->snapshot, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
        if (!s) {
            length = strlen(snapshot) + 64;
            command = h->allocate(h->context, length);
            if (!command)
                s = UD_RESOURCE;
            else {
                snprintf(command, length, "SET TRANSACTION SNAPSHOT %s", snapshot);
                s = sql(p, p->snapshot, command);
                h->deallocate(command);
            }
        }
        if (snapshot)
            PQfreemem(snapshot);
        if (!s)
            s = sql(p, p->snapshot, "SET TIME ZONE 'UTC'");
        if (!s)
            s = sql(p, p->snapshot, "SET DateStyle TO ISO,YMD");
        char *table = NULL;
        if (!s) {
            const char *dot = memchr(o->table.data, '.', o->table.length);
            if (!dot)
                table = identifier(p, p->snapshot, o->table);
            else {
                char *a = identifier(p, p->snapshot,
                                     (ud_string){o->table.data, (uint32_t)(dot - o->table.data)}),
                     *b = identifier(p, p->snapshot,
                                     (ud_string){dot + 1, o->table.length -
                                                              (uint32_t)(dot - o->table.data) - 1});
                if (a && b) {
                    table = h->allocate(h->context, strlen(a) + strlen(b) + 2);
                    if (table)
                        snprintf(table, strlen(a) + strlen(b) + 2, "%s.%s", a, b);
                }
                h->deallocate(a);
                h->deallocate(b);
            }
            if (!table)
                s = UD_RESOURCE;
        }
        if (!s) {
            length = strlen(table) + 128;
            command = h->allocate(h->context, length);
            if (!command)
                s = UD_RESOURCE;
            else {
                snprintf(command, length,
                         "DECLARE ud_cdc_snapshot NO SCROLL CURSOR FOR SELECT * FROM %s", table);
                s = sql(p, p->snapshot, command);
                h->deallocate(command);
            }
        }
        h->deallocate(table);
        if (!s)
            p->snapshot_mode = 1;
    } else if (!s) {
        s = position_read(p, o->position, &p->lsn);
        p->confirmed = p->lsn;
    }
    if (!s)
        p->snapshot_cancel = PQcancelCreate(p->snapshot);
    if (!s && !p->snapshot_cancel)
        s = UD_RESOURCE;
    if (!s)
        p->cancel = PQcancelCreate(p->replication);
    if (!s && !p->cancel)
        s = UD_RESOURCE;
    if (s) {
        ud_pg_cdc_close(p);
        return s;
    }
    *out = p;
    return UD_OK;
}
ud_status ud_pg_cdc_rebind(ud_pg_cdc *p, const ud_batch *schema, uint64_t version) {
    if (!p || !schema || !version || p->pending_invalid)
        return UD_INCOMPATIBLE;
    uint32_t columns = p->host.columns(schema);
    if (p->schema_changed ? columns != p->pending_columns : columns != p->host.columns(p->schema))
        return UD_INCOMPATIBLE;
    ud_field *fields = NULL;
    ud_batch *owned = NULL;
    ud_status status = plugin_fields(&p->host, schema, &fields);
    if (!status)
        status = p->host.batch_create(p->host.context, fields, columns, 0, &owned);
    p->host.deallocate(fields);
    if (!status) {
        p->host.batch_destroy(p->schema);
        p->schema = owned;
        owned = NULL;
        p->version = version;
        if (p->schema_changed) {
            p->host.deallocate(p->oids);
            p->host.deallocate(p->key_columns);
            p->oids = p->pending_oids;
            p->key_columns = p->pending_keys;
            p->pending_oids = NULL;
            p->pending_keys = NULL;
        }
        p->schema_changed = 0;
    }
    p->host.batch_destroy(owned);
    return status;
}
void ud_pg_cdc_cancel(ud_pg_cdc *p) {
    if (!p)
        return;
    atomic_store(&p->cancelled, 1);
    if (p->cancel)
        PQcancelBlocking(p->cancel);
    if (p->snapshot_cancel)
        PQcancelBlocking(p->snapshot_cancel);
}
ud_status ud_pg_cdc_close(ud_pg_cdc *p) {
    if (!p)
        return UD_OK;
    if (p->cancel)
        PQcancelFinish(p->cancel);
    if (p->snapshot_cancel)
        PQcancelFinish(p->snapshot_cancel);
    if (p->snapshot)
        PQfinish(p->snapshot);
    if (p->replication)
        PQfinish(p->replication);
    p->host.batch_destroy(p->schema);
    p->host.deallocate(p->slot);
    p->host.deallocate(p->publication);
    p->host.deallocate(p->table);
    p->host.deallocate(p->position);
    p->host.deallocate(p->transaction);
    p->host.deallocate(p->oids);
    p->host.deallocate(p->key_columns);
    p->host.deallocate(p->pending_oids);
    p->host.deallocate(p->pending_keys);
    p->host.deallocate(p->field_namespace);
    p->host.deallocate(p);
    return UD_OK;
}
