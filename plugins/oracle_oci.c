#include "common.h"
#include <oci.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#define OCI_TRY(call)                                                                              \
    do {                                                                                           \
        ud_status result = (call);                                                                 \
        if (result)                                                                                \
            return result;                                                                         \
    } while (0)
/* Independent OCI driver; every external handle belongs to the existing leased
 * connection. Values use OCI binds/defines, never SQL literal interpolation. */
#include "oracle_internal.h"
ud_status ud_oci_control(oracle *p) {
    if (atomic_load(&p->cancelled) || p->host.cancelled(p->host.context))
        return UD_CANCELLED;
    return p->check ? p->check(p->user) : UD_OK;
}
static ud_status failure(oracle *p, sword rc) {
    ud_status s = ud_oci_control(p);
    if (s)
        return s;
    if (rc == OCI_SUCCESS || rc == OCI_SUCCESS_WITH_INFO)
        return UD_OK;
    if (!p->error)
        return UD_RESOURCE;
    sb4 code = 0;
    OraText text[2];
    OCIErrorGet(p->error, 1, NULL, &code, text, sizeof(text), OCI_HTYPE_ERROR);
    if (code == 1291 || code == 1284 || code == 1327 || code == 1555)
        return UD_FENCED;
    if (code == 1013)
        return UD_CANCELLED;
    if (code == 1017 || code == 1031 || code == 28000)
        return UD_AUTH;
    if (code == 60 || code == 8177)
        return UD_TRANSIENT;
    if (code == 12170 || code == 3136)
        return UD_TIMEOUT;
    if (code == 3113 || code == 3114 || code == 3135) {
        p->broken = 1;
        return UD_TRANSIENT;
    }
    if (code == 1)
        return UD_QUALITY;
    if (code == 942 || code == 904)
        return UD_SCHEMA;
    return p->host.report(p->host.context, UD_IO,
                          "OCI operation failed; diagnostic payload redacted");
}
static void cells_clear(oracle *p, cell *v, uint32_t n) {
    if (!v)
        return;
    for (uint32_t i = 0; i < n; i++) {
        if (v[i].descriptor) {
            if (v[i].type == SQLT_BLOB || v[i].type == SQLT_CLOB) {
                boolean temporary = FALSE;
                OCILobIsTemporary(p->env, p->error, v[i].descriptor, &temporary);
                if (temporary)
                    OCILobFreeTemporary(p->service, p->error, v[i].descriptor);
                OCIDescriptorFree(v[i].descriptor, OCI_DTYPE_LOB);
            } else
                OCIDescriptorFree(v[i].descriptor, OCI_DTYPE_TIMESTAMP_TZ);
        }
        p->host.deallocate(v[i].data);
    }
    p->host.deallocate(v);
}
static ud_status statement(oracle *p, ud_string sql, OCIStmt **out) {
    *out = NULL;
    OCI_TRY(ud_oci_control(p));
    return failure(p, OCIStmtPrepare2(p->service, out, p->error, (const OraText *)sql.data,
                                      sql.length, NULL, 0, OCI_NTV_SYNTAX, OCI_DEFAULT));
}
static void release(oracle *p, OCIStmt *s) {
    if (s)
        OCIStmtRelease(s, p->error, NULL, 0, OCI_DEFAULT);
}
static ud_status bind_value(oracle *p, OCIStmt *stmt, uint32_t at, const ud_value *v, cell *c) {
    c->indicator = v->is_null ? -1 : 0;
    c->field.type = v->type;
    c->length = 0;
    sb8 size = 0;
    void *data = NULL;
    if (v->type == UD_INT64 || v->type == UD_BOOL) {
        c->type = SQLT_INT;
        c->integer = v->integer;
        data = &c->integer;
        size = 8;
    } else if (v->type == UD_FLOAT64) {
        c->type = SQLT_BDOUBLE;
        c->real = v->real;
        data = &c->real;
        size = 8;
    } else if (v->type == UD_TIMESTAMP || v->type == UD_DATE32) {
        if (v->type == UD_TIMESTAMP && !p->utc)
            return UD_UNSUPPORTED;
        c->type = SQLT_TIMESTAMP_TZ;
        sword rc = OCIDescriptorAlloc(p->env, &c->descriptor, OCI_DTYPE_TIMESTAMP_TZ, 0, NULL);
        if (rc != OCI_SUCCESS)
            return failure(p, rc);
        if (!v->is_null) {
            char text[128];
            uint32_t required;
            OCI_TRY(p->host.format_value(v, text, sizeof(text), &required));
            char iso[128];
            if (v->type == UD_DATE32)
                snprintf(iso, sizeof(iso), "%sT00:00:00.000000+00:00", text);
            else {
                if (required < 2 || text[required - 2] != 'Z')
                    return UD_SCHEMA;
                text[required - 2] = 0;
                snprintf(iso, sizeof(iso), "%s+00:00", text);
            }
            const char *format = "YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM";
            OCI_TRY(failure(p, OCIDateTimeFromText(p->env, p->error, (const OraText *)iso,
                                                   strlen(iso), (const OraText *)format,
                                                   (ub1)strlen(format), NULL, 0, c->descriptor)));
        }
        data = &c->descriptor;
        size = sizeof(void *);
    } else if ((v->type == UD_BINARY && (c->type == SQLT_BLOB || v->text_length > 32767)) ||
               (v->type == UD_UTF8 && c->type == SQLT_CLOB)) {
        c->type = v->type == UD_BINARY ? SQLT_BLOB : SQLT_CLOB;
        OCI_TRY(failure(p, OCIDescriptorAlloc(p->env, &c->descriptor, OCI_DTYPE_LOB, 0, NULL)));
        if (!v->is_null) {
            OCI_TRY(failure(
                p, OCILobCreateTemporary(p->service, p->error, c->descriptor, 0, SQLCS_IMPLICIT,
                                         c->type == SQLT_BLOB ? OCI_TEMP_BLOB : OCI_TEMP_CLOB,
                                         FALSE, OCI_DURATION_SESSION)));
            if (v->text_length) {
                oraub8 bytes = v->text_length, chars = 0;
                OCI_TRY(
                    failure(p, OCILobWrite2(p->service, p->error, c->descriptor, &bytes, &chars, 1,
                                            (void *)v->text, v->text_length, OCI_ONE_PIECE, NULL,
                                            NULL, c->type == SQLT_CLOB ? 873 : 0, SQLCS_IMPLICIT)));
            }
        }
        data = &c->descriptor;
        size = sizeof(void *);
    } else if (v->type == UD_UTF8 || v->type == UD_BINARY || v->type == UD_DECIMAL128) {
        if (!v->is_null && (v->type == UD_UTF8 || v->type == UD_BINARY) && !v->text_length)
            return UD_UNSUPPORTED;
        c->type = v->type == UD_BINARY ? SQLT_BIN : SQLT_CHR;
        char formatted[128];
        const char *text = v->text;
        uint32_t length = v->text_length;
        if (v->type == UD_DECIMAL128 && !v->is_null) {
            uint32_t n;
            OCI_TRY(p->host.format_value(v, formatted, sizeof(formatted), &n));
            text = formatted;
            length = n - 1;
        }
        if (v->type == UD_UTF8 && !v->is_null && !length)
            return p->host.report(
                p->host.context, UD_UNSUPPORTED,
                "Oracle treats empty UTF8 as NULL; exact empty-string writes are rejected");
        c->length = v->is_null ? 0 : length;
        size = c->length ? c->length : 1;
        c->data = p->host.allocate(p->host.context, (size_t)size);
        if (!c->data)
            return UD_RESOURCE;
        if (c->length)
            memcpy(c->data, text, c->length);
        data = c->data;
    } else
        return UD_UNSUPPORTED;
    return failure(p, OCIBindByPos2(stmt, &c->bind, p->error, at + 1, data, size, c->type,
                                    &c->indicator, &c->length, &c->code, 0, NULL, OCI_DEFAULT));
}
static ud_status bind(oracle *p, OCIStmt *stmt, const ud_batch *b, uint32_t row,
                      const ud_write_options *options, const ud_sql_statement *plan, cell **out) {
    *out = NULL;
    uint32_t n = plan ? plan->parameter_count : b ? p->host.columns(b) : 0;
    if (!n)
        return UD_OK;
    cell *c = p->host.allocate(p->host.context, (size_t)n * sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < n; i++) {
        ud_value value;
        const ud_batch *source = b;
        uint32_t column = i, r = row;
        if (plan) {
            ud_sql_parameter ref = plan->parameters[i];
            source = ref.source == UD_SQL_BEFORE ? options->before : b;
            column = ref.column;
            r = ref.row;
        }
        s = p->host.get(source, r, column, &value);
        if (!s && options && column < p->write_count)
            c[i].type = p->write_types[column];
        if (!s)
            s = bind_value(p, stmt, i, &value, c + i);
    }
    if (s) {
        cells_clear(p, c, n);
        return s;
    }
    *out = c;
    return UD_OK;
}
static ud_status execute_statement(oracle *p, OCIStmt *stmt, ub4 iterations, uint64_t *affected) {
    OCI_TRY(ud_oci_control(p));
    ud_status s = failure(
        p, OCIStmtExecute(p->service, stmt, p->error, iterations, 0, NULL, NULL, OCI_DEFAULT));
    if (!s && affected) {
        oraub8 rows = 0;
        s = failure(
            p, OCIAttrGet(stmt, OCI_HTYPE_STMT, &rows, NULL, OCI_ATTR_UB8_ROW_COUNT, p->error));
        *affected = rows;
    }
    return s ? s : ud_oci_control(p);
}
static ud_status literal(oracle *p, const char *sql) {
    OCIStmt *stmt = NULL;
    ud_status s = statement(p, (ud_string){sql, (uint32_t)strlen(sql)}, &stmt);
    if (!s)
        s = execute_statement(p, stmt, 1, NULL);
    release(p, stmt);
    return s;
}
ud_status ud_oci_read_close(void *state) {
    oracle *p = state;
    release(p, p->cursor);
    p->cursor = NULL;
    cells_clear(p, p->columns, p->count);
    p->columns = NULL;
    p->count = 0;
    p->host.batch_destroy(p->schema);
    p->schema = NULL;
    return UD_OK;
}
ud_status ud_oci_read_open(void *state, const ud_read_options *o, const ud_batch *schema) {
    oracle *p = state;
    if (p->cursor)
        return UD_BUSY;
    ud_status s = statement(p, o->sql, &p->cursor);
    ub2 kind = 0;
    if (!s)
        s = failure(
            p, OCIAttrGet(p->cursor, OCI_HTYPE_STMT, &kind, NULL, OCI_ATTR_STMT_TYPE, p->error));
    if (!s && kind != OCI_STMT_SELECT)
        s = UD_UNSUPPORTED;
    cell *parameters = NULL;
    if (!s)
        s = bind(p, p->cursor, o->parameters, o->parameter_row, NULL, NULL, &parameters);
    if (!s)
        s = execute_statement(p, p->cursor, 0, NULL);
    cells_clear(p, parameters, o->parameters ? p->host.columns(o->parameters) : 0);
    ub4 count = 0;
    if (!s)
        s = failure(
            p, OCIAttrGet(p->cursor, OCI_HTYPE_STMT, &count, NULL, OCI_ATTR_PARAM_COUNT, p->error));
    if (!s && count != p->host.columns(schema))
        s = UD_SCHEMA;
    p->batch = o->batch_size;
    p->count = count;
    if (!s) {
        p->host.batch_destroy(p->schema);
        p->schema = NULL;
        ud_field *f = NULL;
        s = plugin_fields(&p->host, schema, &f);
        if (!s)
            s = p->host.batch_create(p->host.context, f, count, 0, &p->schema);
        p->host.deallocate(f);
    }
    if (!s) {
        p->columns = p->host.allocate(p->host.context, count * sizeof(*p->columns));
        if (!p->columns)
            s = UD_RESOURCE;
    }
    for (uint32_t i = 0; !s && i < count; i++) {
        cell *c = p->columns + i;
        s = p->host.field(schema, i, &c->field);
        OCIParam *param = NULL;
        OraText *name = NULL;
        ub4 length = 0;
        ub2 native = 0;
        if (!s)
            s = failure(p,
                        OCIParamGet(p->cursor, OCI_HTYPE_STMT, p->error, (void **)&param, i + 1));
        if (!s)
            s = failure(
                p, OCIAttrGet(param, OCI_DTYPE_PARAM, &name, &length, OCI_ATTR_NAME, p->error));
        if (!s && (length != c->field.name_length || memcmp(name, c->field.name, length)))
            s = UD_SCHEMA;
        if (!s)
            s = failure(
                p, OCIAttrGet(param, OCI_DTYPE_PARAM, &native, NULL, OCI_ATTR_DATA_TYPE, p->error));
        if (param)
            OCIDescriptorFree(param, OCI_DTYPE_PARAM);
        void *data = NULL;
        sb8 size = 0;
        if (c->field.type == UD_INT64 || c->field.type == UD_BOOL) {
            c->type = SQLT_INT;
            data = &c->integer;
            size = 8;
        } else if (c->field.type == UD_FLOAT64) {
            c->type = SQLT_BDOUBLE;
            data = &c->real;
            size = 8;
        } else if (c->field.type == UD_TIMESTAMP || c->field.type == UD_DATE32) {
            if (!p->utc && c->field.type == UD_TIMESTAMP) {
                s = UD_UNSUPPORTED;
                break;
            }
            c->type = SQLT_TIMESTAMP_TZ;
            s = failure(
                p, OCIDescriptorAlloc(p->env, &c->descriptor, OCI_DTYPE_TIMESTAMP_TZ, 0, NULL));
            data = &c->descriptor;
            size = sizeof(void *);
        } else if (native == SQLT_CLOB || native == SQLT_BLOB) {
            c->type = native;
            s = failure(p, OCIDescriptorAlloc(p->env, &c->descriptor, OCI_DTYPE_LOB, 0, NULL));
            data = &c->descriptor;
            size = sizeof(void *);
        } else {
            c->type = c->field.type == UD_BINARY ? SQLT_BIN : SQLT_CHR;
            size = 65536;
            c->data = p->host.allocate(p->host.context, (size_t)size);
            if (!c->data)
                s = UD_RESOURCE;
            data = c->data;
        }
        if (!s)
            s = failure(p,
                        OCIDefineByPos2(p->cursor, &c->define, p->error, i + 1, data, size, c->type,
                                        &c->indicator, &c->length, &c->code, OCI_DEFAULT));
    }
    if (s)
        ud_oci_read_close(p);
    return s;
}
static ud_status temporal_value(oracle *p, cell *c, ud_value *v) {
    sb2 year;
    ub1 month, day, hour, minute, second;
    ub4 fraction;
    sb1 zone_hour, zone_minute;
    OCI_TRY(failure(p, OCIDateTimeGetDate(p->env, p->error, c->descriptor, &year, &month, &day)));
    OCI_TRY(failure(p, OCIDateTimeGetTime(p->env, p->error, c->descriptor, &hour, &minute, &second,
                                          &fraction)));
    OCI_TRY(failure(p, OCIDateTimeGetTimeZoneOffset(p->env, p->error, c->descriptor, &zone_hour,
                                                    &zone_minute)));
    if (year < 1 || year > 9999 || fraction % 1000)
        return UD_UNSUPPORTED;
    char date[32], time[48];
    snprintf(date, sizeof(date), "%04d-%02u-%02u", year, month, day);
    snprintf(time, sizeof(time), "%02u:%02u:%02u.%06u", hour, minute, second, fraction / 1000);
    ud_value d, t;
    OCI_TRY(p->host.parse_field(p->host.context, &(ud_field){.type = UD_DATE32}, date,
                                (uint32_t)strlen(date), UD_ROUND_EXACT, &d));
    OCI_TRY(p->host.parse_field(p->host.context, &(ud_field){.type = UD_TIME64}, time,
                                (uint32_t)strlen(time), UD_ROUND_EXACT, &t));
    if (v->type == UD_DATE32) {
        if (t.integer || zone_hour || zone_minute)
            return UD_SCHEMA;
        v->integer = d.integer;
        return UD_OK;
    }
    v->integer = d.integer * INT64_C(86400000000) + t.integer -
                 ((int64_t)zone_hour * 60 + zone_minute) * INT64_C(60000000);
    return UD_OK;
}
ud_status ud_oci_read_next(void *state, ud_batch **out) {
    oracle *p = state;
    *out = NULL;
    if (!p->cursor)
        return UD_INVALID;
    ud_field *f = NULL;
    ud_status s = plugin_fields(&p->host, p->schema, &f);
    if (!s)
        s = p->host.batch_create(p->host.context, f, p->count, p->batch, out);
    p->host.deallocate(f);
    ud_value *v = p->host.allocate(p->host.context, p->count * sizeof(*v));
    void **owned = p->host.allocate(p->host.context, p->count * sizeof(*owned));
    if (!v || !owned)
        s = UD_RESOURCE;
    int end = 0;
    while (!s && p->host.rows(*out) < p->batch) {
        s = ud_oci_control(p);
        if (s)
            break;
        sword rc = OCIStmtFetch2(p->cursor, p->error, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT);
        if (rc == OCI_NO_DATA) {
            end = 1;
            break;
        }
        s = failure(p, rc);
        if (s)
            break;
        for (uint32_t i = 0; !s && i < p->count; i++) {
            cell *c = p->columns + i;
            v[i] = (ud_value){.type = c->field.type,
                              .precision = c->field.precision,
                              .scale = c->field.scale,
                              .is_null = c->indicator == -1};
            if (v[i].is_null)
                continue;
            if (c->indicator == -2 || c->code) {
                s = UD_SCHEMA;
                break;
            }
            if (v[i].type == UD_INT64 || v[i].type == UD_BOOL) {
                v[i].integer = c->integer;
                if (v[i].type == UD_BOOL && c->integer != 0 && c->integer != 1)
                    s = UD_SCHEMA;
            } else if (v[i].type == UD_FLOAT64)
                v[i].real = c->real;
            else if (v[i].type == UD_TIMESTAMP || v[i].type == UD_DATE32)
                s = temporal_value(p, c, v + i);
            else {
                const char *text = c->data;
                uint32_t length = c->length;
                if (c->type == SQLT_CLOB || c->type == SQLT_BLOB) {
                    oraub8 lob = 0;
                    s = failure(p, OCILobGetLength2(p->service, p->error, c->descriptor, &lob));
                    if (!s && lob > 16777216)
                        s = UD_RESOURCE;
                    size_t capacity = c->type == SQLT_CLOB ? (size_t)lob * 4 + 1 : (size_t)lob + 1;
                    if (!s) {
                        owned[i] = p->host.allocate(p->host.context, capacity);
                        if (!owned[i])
                            s = UD_RESOURCE;
                    }
                    oraub8 bytes = capacity - 1, chars = c->type == SQLT_CLOB ? lob : 0;
                    if (!s && lob)
                        s = failure(p, OCILobRead2(p->service, p->error, c->descriptor, &bytes,
                                                   &chars, 1, owned[i], capacity - 1, OCI_ONE_PIECE,
                                                   NULL, NULL, 873, SQLCS_IMPLICIT));
                    if (!s) {
                        text = owned[i];
                        length = lob ? (uint32_t)bytes : 0;
                    }
                }
                if (!s && v[i].type == UD_BINARY) {
                    v[i].text = text;
                    v[i].text_length = length;
                } else if (!s)
                    s = p->host.parse_field(p->host.context, &c->field, text, length,
                                            UD_ROUND_EXACT, v + i);
            }
        }
        if (!s)
            s = p->host.append(*out, v, p->count);
        for (uint32_t i = 0; i < p->count; i++) {
            p->host.deallocate(owned[i]);
            owned[i] = NULL;
        }
    }
    p->host.deallocate(v);
    p->host.deallocate(owned);
    if (s || (!p->host.rows(*out) && end)) {
        p->host.batch_destroy(*out);
        *out = NULL;
    }
    if (end)
        ud_oci_read_close(p);
    return s ? s : *out ? UD_OK : UD_END;
}
ud_status ud_oci_query(oracle *p, const char *sql, const ud_string *args, uint32_t count,
                       const ud_field *fields, uint32_t n, ud_batch **out) {
    ud_batch *params = NULL, *schema = NULL;
    ud_field *pf = count ? p->host.allocate(p->host.context, count * sizeof(*pf)) : NULL;
    ud_value *pv = count ? p->host.allocate(p->host.context, count * sizeof(*pv)) : NULL;
    ud_status s = count && (!pf || !pv) ? UD_RESOURCE : UD_OK;
    for (uint32_t i = 0; !s && i < count; i++) {
        char name[24];
        int length = snprintf(name, sizeof(name), "p%u", i);
        pf[i] = (ud_field){.name = plugin_string(&p->host, name, (uint32_t)length),
                           .name_length = (uint32_t)length,
                           .type = UD_UTF8,
                           .nullable = 1};
        pv[i] = (ud_value){.type = UD_UTF8,
                           .text = args[i].data,
                           .text_length = args[i].length,
                           .is_null = !args[i].length};
        if (!pf[i].name)
            s = UD_RESOURCE;
    }
    if (!s && count)
        s = p->host.batch_create(p->host.context, pf, count, 1, &params);
    if (!s && count)
        s = p->host.append(params, pv, count);
    if (!s)
        s = p->host.batch_create(p->host.context, fields, n, 0, &schema);
    ud_read_options o = {.struct_size = sizeof(o),
                         .abi_version = UD_ABI_VERSION,
                         .batch_size = 65536,
                         .sql = {sql, (uint32_t)strlen(sql)},
                         .parameters = params};
    if (!s)
        s = ud_oci_read_open(p, &o, schema);
    if (!s)
        s = ud_oci_read_next(p, out);
    if (s == UD_END)
        s = p->host.batch_create(p->host.context, fields, n, 0, out);
    ud_oci_read_close(p);
    p->host.batch_destroy(params);
    p->host.batch_destroy(schema);
    for (uint32_t i = 0; pf && i < count; i++)
        p->host.deallocate((void *)pf[i].name);
    p->host.deallocate(pf);
    p->host.deallocate(pv);
    return s;
}
static ud_status discover(void *state, ud_string table, ud_batch **out) {
    oracle *p = state;
    const char *dot = memchr(table.data, '.', table.length);
    ud_string args[2] = {{"", 0}, table};
    if (dot) {
        args[0] = (ud_string){table.data, (uint32_t)(dot - table.data)};
        args[1] = (ud_string){dot + 1, table.length - (uint32_t)(dot - table.data) - 1};
    }
    ud_field f[] = {{.name = "name", .name_length = 4, .type = UD_UTF8},
                    {.name = "declared_type", .name_length = 13, .type = UD_UTF8},
                    {.name = "nullable", .name_length = 8, .type = UD_BOOL},
                    {.name = "key_order", .name_length = 9, .type = UD_INT64},
                    {.name = "default", .name_length = 7, .type = UD_UTF8, .nullable = 1},
                    {.name = "identity", .name_length = 8, .type = UD_BOOL}};
    const char *sql =
        "SELECT c.column_name AS \"name\",CASE WHEN c.data_type='NUMBER' THEN "
        "'NUMBER('||c.data_precision||','||c.data_scale||')' WHEN c.data_type='DATE' THEN "
        "'DATE_UTC' WHEN c.data_type LIKE 'TIMESTAMP%' AND c.data_type NOT LIKE '%TIME ZONE%' THEN "
        "'TIMESTAMP_UTC' ELSE c.data_type END AS \"declared_type\",CASE WHEN c.nullable='Y' THEN 1 "
        "ELSE 0 END AS \"nullable\",NVL((SELECT k.position FROM all_constraints a JOIN "
        "all_cons_columns k ON k.owner=a.owner AND k.constraint_name=a.constraint_name WHERE "
        "a.owner=c.owner AND a.table_name=c.table_name AND a.constraint_type='P' AND "
        "k.column_name=c.column_name),0) AS \"key_order\",c.data_default_vc AS \"default\",CASE "
        "WHEN c.identity_column='YES' THEN 1 ELSE 0 END AS \"identity\" FROM all_tab_columns c "
        "WHERE c.owner=NVL(:p1,SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) AND c.table_name=:p2 ORDER "
        "BY c.column_id";
    return ud_oci_query(p, sql, args, 2, f, 6, out);
}
static ud_status metadata(void *state, const ud_metadata_options *o, ud_batch **out) {
    oracle *p = state;
    if (o->kind == UD_METADATA_COLUMNS)
        return discover(p, o->parent, out);
    ud_field f[] = {
        {.name = "name", .name_length = 4, .type = UD_UTF8},
        {.name = "column", .name_length = 6, .type = UD_UTF8},
        {.name = "ordinal", .name_length = 7, .type = UD_INT64},
        {.name = "unique", .name_length = 6, .type = UD_BOOL},
        {.name = "referenced_table", .name_length = 16, .type = UD_UTF8, .nullable = 1},
        {.name = "referenced_column", .name_length = 17, .type = UD_UTF8, .nullable = 1}};
    const char *sql = NULL;
    uint32_t n = 1, params = 0;
    ud_string arg = o->parent;
    if (o->kind == UD_METADATA_CATALOGS)
        sql = "SELECT SYS_CONTEXT('USERENV','DB_NAME') AS \"name\" FROM dual";
    else if (o->kind == UD_METADATA_SCHEMAS)
        sql = "SELECT username AS \"name\" FROM all_users ORDER BY username";
    else if (o->kind == UD_METADATA_TABLES) {
        sql = "SELECT table_name AS \"name\" FROM all_tables WHERE owner=:p1 ORDER BY table_name";
        params = 1;
    } else if (o->kind == UD_METADATA_PRIMARY_KEYS) {
        sql = "SELECT k.constraint_name AS \"name\",k.column_name AS \"column\",k.position AS "
              "\"ordinal\" FROM all_cons_columns k JOIN all_constraints c ON c.owner=k.owner AND "
              "c.constraint_name=k.constraint_name WHERE "
              "c.owner=SYS_CONTEXT('USERENV','CURRENT_SCHEMA') AND c.table_name=:p1 AND "
              "c.constraint_type='P' ORDER BY k.position";
        n = 3;
        params = 1;
    } else if (o->kind == UD_METADATA_INDEXES) {
        sql =
            "SELECT c.index_name AS \"name\",c.column_name AS \"column\",c.column_position AS "
            "\"ordinal\",CASE WHEN i.uniqueness='UNIQUE' THEN 1 ELSE 0 END AS \"unique\" FROM "
            "all_ind_columns c JOIN all_indexes i ON i.owner=c.index_owner AND "
            "i.index_name=c.index_name WHERE c.table_owner=SYS_CONTEXT('USERENV','CURRENT_SCHEMA') "
            "AND c.table_name=:p1 ORDER BY c.index_name,c.column_position";
        n = 4;
        params = 1;
    } else if (o->kind == UD_METADATA_FOREIGN_KEYS) {
        sql = "SELECT c.constraint_name AS \"name\",k.column_name AS \"column\",k.position AS "
              "\"ordinal\",0 AS \"unique\",r.table_name AS \"referenced_table\",rk.column_name AS "
              "\"referenced_column\" FROM all_constraints c JOIN all_cons_columns k ON "
              "k.owner=c.owner AND k.constraint_name=c.constraint_name JOIN all_constraints r ON "
              "r.owner=c.r_owner AND r.constraint_name=c.r_constraint_name JOIN all_cons_columns "
              "rk ON rk.owner=r.owner AND rk.constraint_name=r.constraint_name AND "
              "rk.position=k.position WHERE c.owner=SYS_CONTEXT('USERENV','CURRENT_SCHEMA') AND "
              "c.table_name=:p1 AND c.constraint_type='R' ORDER BY c.constraint_name,k.position";
        n = 6;
        params = 1;
    } else
        return UD_UNSUPPORTED;
    return ud_oci_query(p, sql, &arg, params, f, n, out);
}
static ud_status begin(void *state, int32_t isolation) {
    oracle *p = state;
    if (p->transaction || p->cursor)
        return UD_BUSY;
    if (isolation == UD_ISOLATION_REPEATABLE_READ)
        return UD_UNSUPPORTED;
    ud_status s = failure(p, OCITransStart(p->service, p->error, 0,
                                           OCI_TRANS_NEW | (isolation == UD_ISOLATION_SERIALIZABLE
                                                                ? OCI_TRANS_SERIALIZABLE
                                                                : 0)));
    if (!s)
        p->transaction = 1;
    return s;
}
static ud_status rollback(void *state) {
    oracle *p = state;
    ud_status s = failure(p, OCITransRollback(p->service, p->error, OCI_DEFAULT));
    if (!s)
        p->transaction = 0;
    return s;
}
static ud_status commit(void *state) {
    oracle *p = state;
    ud_status s = failure(p, OCITransCommit(p->service, p->error, OCI_DEFAULT));
    if (!s)
        p->transaction = 0;
    else {
        s = UD_COMMIT_UNKNOWN;
        p->broken = 1;
    }
    return s;
}
static ud_status receipt(void *state, ud_string tenant, ud_string job, ud_string batch,
                         ud_commit_receipt *out) {
    oracle *p = state;
    memset(out, 0, sizeof(*out));
    ud_string args[] = {tenant, job, batch};
    ud_field f[] = {{.name = "digest", .name_length = 6, .type = UD_BINARY},
                    {.name = "source_position", .name_length = 15, .type = UD_UTF8, .nullable = 1}};
    ud_batch *b = NULL;
    ud_status s =
        ud_oci_query(p,
                     "SELECT digest AS \"digest\",source_position AS \"source_position\" FROM "
                     "ud_sdk_receipts_v2 WHERE tenant=:p1 AND job=:p2 AND batch_id=:p3 FOR UPDATE",
                     args, 3, f, 2, &b);
    if (!s && p->host.rows(b)) {
        ud_value d, v;
        s = p->host.get(b, 0, 0, &d);
        if (!s)
            s = p->host.get(b, 0, 1, &v);
        if (!s && (d.is_null || d.text_length != 32))
            s = UD_CORRUPT;
        if (!s) {
            out->committed = 1;
            memcpy(out->digest, d.text, 32);
            out->source_position = (ud_string){
                plugin_string(&p->host, v.text ? v.text : "", v.is_null ? 0 : v.text_length),
                v.is_null ? 0 : v.text_length};
            if (!out->source_position.data)
                s = UD_RESOURCE;
        }
    }
    p->host.batch_destroy(b);
    return s;
}
static ud_status provision(void *state) {
    oracle *p = state;
    if (p->transaction || p->cursor)
        return UD_BUSY;
    return literal(
        p, "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE ud_sdk_receipts_v2(tenant VARCHAR2(256 BYTE) NOT "
           "NULL,job VARCHAR2(256 BYTE) NOT NULL,batch_id VARCHAR2(256 BYTE) NOT NULL,digest "
           "RAW(32) NOT NULL,source_position CLOB,PRIMARY KEY(tenant,job,batch_id))'; EXCEPTION "
           "WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;");
}
static ud_status execute(void *state, const ud_read_options *o, uint64_t *affected) {
    oracle *p = state;
    if (affected)
        *affected = 0;
    if (p->cursor)
        return UD_BUSY;
    OCIStmt *stmt = NULL;
    ud_status s = statement(p, o->sql, &stmt);
    ub2 kind = 0;
    if (!s)
        s = failure(p, OCIAttrGet(stmt, OCI_HTYPE_STMT, &kind, NULL, OCI_ATTR_STMT_TYPE, p->error));
    if (!s && p->transaction &&
        (kind == OCI_STMT_CREATE || kind == OCI_STMT_DROP || kind == OCI_STMT_ALTER ||
         kind == OCI_STMT_BEGIN || kind == OCI_STMT_DECLARE))
        s = UD_UNSUPPORTED;
    int own = !p->transaction;
    if (!s && own)
        s = begin(p, UD_ISOLATION_READ_COMMITTED);
    uint32_t count = o->parameters ? p->host.rows(o->parameters) : 1;
    for (uint32_t row = 0; !s && row < count; row++) {
        cell *values = NULL;
        s = bind(p, stmt, o->parameters, row, NULL, NULL, &values);
        uint64_t n = 0;
        if (!s)
            s = execute_statement(p, stmt, 1, &n);
        cells_clear(p, values, o->parameters ? p->host.columns(o->parameters) : 0);
        if (!s && affected)
            *affected += n;
    }
    release(p, stmt);
    if (own) {
        if (!s)
            s = commit(p);
        else if (rollback(p))
            s = UD_COMMIT_UNKNOWN;
    }
    return s;
}
static ud_status validate_target(oracle *p, const ud_write_options *o, const ud_batch *b) {
    ud_batch *metadata = NULL;
    ud_status s = discover(p, o->table, &metadata);
    p->host.deallocate(p->write_types);
    p->write_types =
        p->host.allocate(p->host.context, p->host.columns(b) * sizeof(*p->write_types));
    p->write_count = p->host.columns(b);
    if (!p->write_types && !s)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < p->host.columns(b); i++) {
        ud_field field;
        s = p->host.field(b, i, &field);
        int found = 0;
        for (uint32_t r = 0; !s && r < p->host.rows(metadata); r++) {
            ud_value name, type;
            s = p->host.get(metadata, r, 0, &name);
            if (!s)
                s = p->host.get(metadata, r, 1, &type);
            if (!s && name.text_length == field.name_length &&
                !memcmp(name.text, field.name, name.text_length)) {
                found = 1;
                if (type.text_length == 4 && !memcmp(type.text, "BLOB", 4))
                    p->write_types[i] = SQLT_BLOB;
                else if ((type.text_length == 4 && !memcmp(type.text, "CLOB", 4)) ||
                         (type.text_length == 5 && !memcmp(type.text, "NCLOB", 5)))
                    p->write_types[i] = SQLT_CLOB;
                ud_field actual = {0};
                s = p->host.field_from_sql((ud_string){type.text, type.text_length}, UD_SQL_ORACLE,
                                           &actual);
                if (!s && (actual.type != field.type ||
                           (field.type == UD_DECIMAL128 &&
                            (actual.scale != field.scale || actual.precision < field.precision))))
                    s = UD_SCHEMA;
                if (!s && field.type == UD_FLOAT64 && type.text_length == 12 &&
                    !memcmp(type.text, "BINARY_FLOAT", 12))
                    s = UD_SCHEMA;
                if (!s && field.type == UD_TIMESTAMP && type.text_length == 8 &&
                    !memcmp(type.text, "DATE_UTC", 8))
                    for (uint32_t row = 0; !s && row < p->host.rows(b); row++) {
                        ud_value v;
                        s = p->host.get(b, row, i, &v);
                        if (!s && !v.is_null && v.integer % 1000000)
                            s = UD_SCHEMA;
                    }
            }
        }
        if (!found)
            s = UD_SCHEMA;
    }
    p->host.batch_destroy(metadata);
    return s;
}
static ud_status verify_preimage(oracle *p, const ud_write_options *options, const ud_batch *b,
                                 uint32_t row) {
    if (!options->before || options->operations[row] == UD_CHANGE_INSERT ||
        options->operations[row] == UD_CHANGE_UPSERT)
        return UD_OK;
    ud_sql_statement select = {0};
    ud_field *fields = NULL;
    ud_batch *schema = NULL, *actual = NULL;
    ud_status s = p->host.compile_preimage(p->host.context, UD_SQL_ORACLE, UD_SQL_PARAMETER_NAMED,
                                           options, b, row, &select);
    ud_field *pf =
        !s ? p->host.allocate(p->host.context, select.parameter_count * sizeof(*pf)) : NULL;
    ud_value *pv =
        !s ? p->host.allocate(p->host.context, select.parameter_count * sizeof(*pv)) : NULL;
    ud_batch *params = NULL;
    if (!s && (!pf || !pv))
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < select.parameter_count; i++) {
        ud_sql_parameter ref = select.parameters[i];
        s = p->host.field(options->before, ref.column, pf + i);
        char name[24];
        int n = snprintf(name, sizeof(name), "p%u", i);
        pf[i].name = plugin_string(&p->host, name, (uint32_t)n);
        pf[i].name_length = (uint32_t)n;
        pf[i].identity = (ud_id){0};
        pf[i].key_order = 0;
        pf[i].lineage = NULL;
        pf[i].lineage_count = 0;
        if (!s && !pf[i].name)
            s = UD_RESOURCE;
        if (!s)
            s = p->host.get(options->before, ref.row, ref.column, pv + i);
    }
    if (!s)
        s = p->host.batch_create(p->host.context, pf, select.parameter_count, 1, &params);
    if (!s)
        s = p->host.append(params, pv, select.parameter_count);
    if (!s)
        s = plugin_fields(&p->host, b, &fields);
    if (!s)
        s = p->host.batch_create(p->host.context, fields, p->host.columns(b), 0, &schema);
    ud_read_options read = {.struct_size = sizeof(read),
                            .abi_version = UD_ABI_VERSION,
                            .batch_size = 2,
                            .sql = select.sql,
                            .parameters = params};
    if (!s)
        s = ud_oci_read_open(p, &read, schema);
    if (!s)
        s = ud_oci_read_next(p, &actual);
    if (s == UD_END)
        s = UD_FENCED;
    if (!s && p->host.rows(actual) != 1)
        s = UD_FENCED;
    for (uint32_t i = 0; !s && i < p->host.columns(b); i++) {
        ud_value expected, value;
        s = p->host.get(options->before, row, i, &expected);
        if (!s)
            s = p->host.get(actual, 0, i, &value);
        if (!s && p->host.compare_value(&expected, &value))
            s = UD_FENCED;
    }
    ud_oci_read_close(p);
    for (uint32_t i = 0; pf && i < select.parameter_count; i++)
        p->host.deallocate((void *)pf[i].name);
    p->host.deallocate(pf);
    p->host.deallocate(pv);
    p->host.deallocate(fields);
    p->host.batch_destroy(params);
    p->host.batch_destroy(schema);
    p->host.batch_destroy(actual);
    p->host.statement_clear(&select);
    return s;
}
static ud_status write_batch(void *state, ud_string tenant, const ud_write_options *o,
                             const ud_batch *b, const uint8_t digest[32]) {
    oracle *p = state;
    if (p->cursor)
        return UD_BUSY;
    if (p->transaction && o->mode == UD_WRITE_CHANGESET)
        return UD_UNSUPPORTED;
    int own = !p->transaction;
    ud_status s = own ? begin(p, UD_ISOLATION_SERIALIZABLE) : UD_OK;
    if (!s && o->mode == UD_WRITE_CHANGESET && o->current_column != UINT32_MAX) {
        char *table = plugin_identifier(&p->host, o->table);
        size_t n = table ? strlen(table) + 40 : 0;
        char *sql = n ? p->host.allocate(p->host.context, n) : NULL;
        if (!table || !sql)
            s = UD_RESOURCE;
        else {
            snprintf(sql, n, "LOCK TABLE %s IN EXCLUSIVE MODE", table);
            s = literal(p, sql);
        }
        p->host.deallocate(table);
        p->host.deallocate(sql);
    }
    if (!s)
        s = validate_target(p, o, b);
    ud_commit_receipt previous = {0};
    if (!s)
        s = receipt(p, tenant, o->job, o->batch_id, &previous);
    int exists = previous.committed;
    if (!s && exists &&
        (memcmp(previous.digest, digest, 32) ||
         previous.source_position.length != o->source_position.length ||
         (o->source_position.length && memcmp(previous.source_position.data,
                                              o->source_position.data, o->source_position.length))))
        s = UD_CORRUPT;
    p->host.deallocate((void *)previous.source_position.data);
    uint32_t limit = p->host.columns(b) ? 65535 / p->host.columns(b) : 0;
    if (!limit)
        s = UD_RESOURCE;
    if (limit > 128)
        limit = 128;
    for (uint32_t row = 0; !s && !exists && row < p->host.rows(b);) {
        uint32_t count = o->mode == UD_WRITE_CHANGESET ? 1 : p->host.rows(b) - row;
        if (count > limit)
            count = limit;
        ud_sql_statement plan = {0};
        OCIStmt *stmt = NULL;
        cell *values = NULL;
        ud_write_options write = *o;
        if (o->mode == UD_WRITE_CHANGESET) {
            s = verify_preimage(p, o, b, row);
            if (!s && o->before) {
                for (uint32_t k = 0; !s && k < o->key_count; k++) {
                    ud_value old, key;
                    s = p->host.get(o->before, row, o->keys[k], &old);
                    if (!s)
                        s = p->host.get(b, row, o->keys[k], &key);
                    if (!s && o->operations[row] == UD_CHANGE_UPDATE &&
                        p->host.compare_value(&old, &key))
                        s = UD_UNSUPPORTED;
                }
                write.before = NULL;
            }
        }
        if (!s)
            s = p->host.compile_write(p->host.context, UD_SQL_ORACLE, UD_SQL_PARAMETER_NAMED,
                                      &write, b, row, count, &plan);
        if (!s)
            s = statement(p, plan.sql, &stmt);
        if (!s)
            s = bind(p, stmt, b, 0, o, &plan, &values);
        uint64_t affected = 0;
        if (!s)
            s = execute_statement(p, stmt, 1, &affected);
        if (!s && o->mode == UD_WRITE_CHANGESET && o->operations[row] != UD_CHANGE_UPSERT &&
            affected != 1)
            s = UD_FENCED;
        release(p, stmt);
        cells_clear(p, values, plan.parameter_count);
        p->host.statement_clear(&plan);
        row += count;
    }
    if (!s && !exists) {
        ud_field fields[] = {
            {.name = "tenant", .name_length = 6, .type = UD_UTF8},
            {.name = "job", .name_length = 3, .type = UD_UTF8},
            {.name = "batch_id", .name_length = 8, .type = UD_UTF8},
            {.name = "digest", .name_length = 6, .type = UD_BINARY},
            {.name = "source_position", .name_length = 15, .type = UD_UTF8, .nullable = 1}};
        ud_value values[] = {
            {.type = UD_UTF8, .text = tenant.data, .text_length = tenant.length},
            {.type = UD_UTF8, .text = o->job.data, .text_length = o->job.length},
            {.type = UD_UTF8, .text = o->batch_id.data, .text_length = o->batch_id.length},
            {.type = UD_BINARY, .text = (const char *)digest, .text_length = 32},
            {.type = UD_UTF8,
             .text = o->source_position.data,
             .text_length = o->source_position.length,
             .is_null = !o->source_position.length}};
        ud_batch *params = NULL;
        s = p->host.batch_create(p->host.context, fields, 5, 1, &params);
        if (!s)
            s = p->host.append(params, values, 5);
        ud_read_options insert = {
            .sql = {"INSERT INTO ud_sdk_receipts_v2(tenant,job,batch_id,digest,source_position) "
                    "VALUES(:p1,:p2,:p3,:p4,:p5)",
                    97},
            .parameters = params};
        insert.sql.length = (uint32_t)strlen(insert.sql.data);
        if (!s)
            s = execute(p, &insert, NULL);
        p->host.batch_destroy(params);
    }
    if (own) {
        if (!s)
            s = commit(p);
        else if (rollback(p))
            s = UD_COMMIT_UNKNOWN;
    }
    p->host.deallocate(p->write_types);
    p->write_types = NULL;
    p->write_count = 0;
    return s;
}
static ud_status test(void *state) {
    ud_batch *b = NULL;
    ud_field f = {.name = "value", .name_length = 5, .type = UD_INT64};
    ud_status s = ud_oci_query(state, "SELECT 1 AS \"value\" FROM dual", NULL, 0, &f, 1, &b);
    ((oracle *)state)->host.batch_destroy(b);
    return s;
}
static void cancel(void *state) {
    oracle *p = state;
    atomic_store(&p->cancelled, 1);
    pthread_mutex_lock(&p->ownership);
    if (p->service)
        OCIBreak(p->service, p->error);
    pthread_mutex_unlock(&p->ownership);
}
static void set_control(void *state, void *user, ud_status (*check)(void *)) {
    oracle *p = state;
    p->user = user;
    p->check = check;
    p->host.control_user = user;
    p->host.check_io = check;
    atomic_store(&p->cancelled, 0);
}
static ud_status session_policy(oracle *p) {
    ud_status s = literal(p, "ALTER SESSION SET TIME_ZONE='+00:00'");
    if (!s)
        s = literal(p, "ALTER SESSION SET NLS_NUMERIC_CHARACTERS='.,'");
    if (!s)
        s = literal(p, "ALTER SESSION SET NLS_COMP=BINARY");
    if (!s)
        s = literal(p, "ALTER SESSION SET NLS_SORT=BINARY");
    if (!s)
        s = literal(p, "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS'");
    if (!s)
        s = literal(p, "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF6'");
    if (!s)
        s = literal(
            p, "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM'");
    return s;
}
static ud_status reset(void *state) {
    oracle *p = state;
    if (p->broken || p->cursor || p->cdc)
        return UD_IO;
    atomic_store(&p->cancelled, 0);
    ud_status s = rollback(p);
    if (!s)
        s = literal(p, "BEGIN DBMS_SESSION.RESET_PACKAGE; END;");
    return s ? s : session_policy(p);
}
void ud_oci_destroy(void *state) {
    oracle *p = state;
    if (!p)
        return;
    ud_oracle_cdc_close(p);
    ud_oci_read_close(p);
    pthread_mutex_lock(&p->ownership);
    if (p->service)
        OCILogoff(p->service, p->error);
    p->service = NULL;
    pthread_mutex_unlock(&p->ownership);
    if (p->error)
        OCIHandleFree(p->error, OCI_HTYPE_ERROR);
    if (p->env)
        OCIHandleFree(p->env, OCI_HTYPE_ENV);
    pthread_mutex_destroy(&p->ownership);
    p->host.deallocate(p->write_types);
    p->host.deallocate(p);
}
static ud_status initialize(const ud_host_api *h, const char *text, uint32_t length, void **out) {
    *out = NULL;
    oracle *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    p->batch = 4096;
    p->user = h->control_user;
    p->check = h->check_io;
    atomic_init(&p->cancelled, 0);
    if (pthread_mutex_init(&p->ownership, NULL)) {
        h->deallocate(p);
        return UD_RESOURCE;
    }
    ud_json *config = NULL;
    ud_status s = h->json_parse(h->context, text, length, &config, NULL);
    const char *allowed[] = {"connectString", "user", "password", "timestampTimezone",
                             "callTimeoutMs"};
    if (!s)
        s = h->json_keys(config, allowed, 5);
    const char *values[3] = {0};
    uint32_t sizes[3] = {0};
    for (uint32_t i = 0; !s && i < 3; i++)
        s = h->json_text(h->json_find(config, allowed[i], (uint32_t)strlen(allowed[i])), values + i,
                         sizes + i);
    const ud_json *zone = h->json_find(config, "timestampTimezone", 17);
    if (!s && zone) {
        const char *v;
        uint32_t n;
        s = h->json_text(zone, &v, &n);
        p->utc = !s && n == 3 && !memcmp(v, "UTC", 3);
        if (!p->utc)
            s = UD_SCHEMA;
    }
    for (uint32_t i = 0; !s && i < 3; i++)
        if (memchr(values[i], 0, sizes[i]))
            s = UD_SCHEMA;
    if (!s && (!sizes[0] || !sizes[1] || !sizes[2] || !strstr(values[0], "TCPS") ||
               !strstr(values[0], "SSL_SERVER_DN_MATCH=YES")))
        s = h->report(h->context, UD_AUTH,
                      "OCI requires a TCPS descriptor with server-certificate matching configured "
                      "by the Oracle wallet");
    if (!s)
        s = failure(p, OCIEnvNlsCreate(&p->env, OCI_THREADED | OCI_OBJECT, NULL, NULL, NULL, NULL,
                                       0, NULL, 873, 2000));
    if (!s)
        s = failure(p, OCIHandleAlloc(p->env, (void **)&p->error, OCI_HTYPE_ERROR, 0, NULL));
    if (!s)
        s = failure(p, OCILogon2(p->env, p->error, &p->service, (const OraText *)values[1],
                                 sizes[1], (const OraText *)values[2], sizes[2],
                                 (const OraText *)values[0], sizes[0], OCI_DEFAULT));
    ub4 timeout = 30000;
    const ud_json *t = h->json_find(config, "callTimeoutMs", 13);
    if (!s && t) {
        const char *v;
        uint32_t n;
        s = h->json_text(t, &v, &n);
        timeout = 0;
        for (uint32_t i = 0; !s && i < n; i++) {
            if (v[i] < '0' || v[i] > '9' || timeout > 600000)
                s = UD_SCHEMA;
            else
                timeout = timeout * 10 + (unsigned)(v[i] - '0');
        }
        if (!timeout || timeout > 600000)
            s = UD_SCHEMA;
    }
    if (!s)
        s = failure(p, OCIAttrSet(p->service, OCI_HTYPE_SVCCTX, &timeout, 0, OCI_ATTR_CALL_TIMEOUT,
                                  p->error));
    if (!s)
        s = session_policy(p);
    h->json_destroy(config);
    if (s) {
        ud_oci_destroy(p);
        return s;
    }
    *out = p;
    return UD_OK;
}
static const ud_connector_api api = {
    .struct_size = sizeof(api),
    .abi_version = UD_ABI_VERSION,
    .capabilities = UD_CONNECT_READ | UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE |
                    UD_CONNECT_CHANGESET | UD_CONNECT_RECEIPTS | UD_CONNECT_EXECUTE_SQL |
                    UD_CONNECT_TRANSACTION | UD_CONNECT_METADATA | UD_CONNECT_PREPARED |
                    UD_CONNECT_BATCH_WRITE | UD_CONNECT_PROVISION | UD_CONNECT_POOL |
                    UD_CONNECT_CDC | UD_CONNECT_NATIVE_CDC | UD_CONNECT_SNAPSHOT,
    .test = test,
    .discover = discover,
    .read_open = ud_oci_read_open,
    .read_next = ud_oci_read_next,
    .read_close = ud_oci_read_close,
    .write = write_batch,
    .receipt = receipt,
    .cancel = cancel,
    .execute = execute,
    .begin = begin,
    .commit = commit,
    .rollback = rollback,
    .metadata = metadata,
    .set_control = set_control,
    .reset_session = reset,
    .provision = provision,
    .parameter_style = UD_SQL_PARAMETER_NAMED,
    .metadata_dialect = UD_SQL_ORACLE,
    .cdc_open = ud_oracle_cdc_open,
    .cdc_next = ud_oracle_cdc_next,
    .cdc_confirm = ud_oracle_cdc_confirm,
    .cdc_close = ud_oracle_cdc_close};
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){.struct_size = sizeof(*out),
                           .abi_version = UD_PLUGIN_ABI,
                           .id = "org.universaldata.connector.oracle.oci",
                           .version = UD_SDK_VERSION,
                           .roles = UD_PLUGIN_CONNECTOR,
                           .capabilities = UD_CAP_READ | UD_CAP_WRITE | UD_CAP_SQL |
                                           UD_CAP_TRANSACTION | UD_CAP_THREAD_SAFE,
                           .initialize = initialize,
                           .destroy = ud_oci_destroy,
                           .shutdown = plugin_shutdown,
                           .connector = &api};
    return UD_OK;
}

ud_status ud_oci_state_command(oracle *p, const ud_read_options *options) {
    if (p->cursor || p->transaction)
        return UD_BUSY;
    OCIStmt *stmt = NULL;
    cell *values = NULL;
    ud_status status = statement(p, options->sql, &stmt);
    if (!status)
        status = bind(p, stmt, options->parameters, 0, NULL, NULL, &values);
    if (!status)
        status = execute_statement(p, stmt, 1, NULL);
    cells_clear(p, values, options->parameters ? p->host.columns(options->parameters) : 0);
    release(p, stmt);
    return status;
}
