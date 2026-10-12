#include "connector_common.h"
#include <sqlite3.h>
#include <ctype.h>
#include <stdatomic.h>
#include <math.h>
typedef struct {
    ud_host_api host;
    sqlite3 *db;
    sqlite3_stmt *read;
    ud_batch *cursor_schema;
    uint32_t cursor_batch_size;
    int cursor_finished;
    atomic_int cancelled;
} sqlite_state;
static ud_status failure(sqlite_state *p, int code) {
    return p->host.report(p->host.context,
                          code == SQLITE_BUSY || code == SQLITE_LOCKED ? UD_BUSY :
                          code == SQLITE_READONLY || code == SQLITE_AUTH || code == SQLITE_PERM ? UD_AUTH :
                          code == SQLITE_INTERRUPT ? UD_CANCELLED : UD_IO,
                          "SQLite operation failed (diagnostic values redacted)");
}
static ud_status initialize(const ud_host_api *h, const char *config, uint32_t n, void **out) {
    *out = NULL;
    sqlite_state *p = h->allocate(h->context, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->host = *h;
    p->cursor_batch_size = 4096;
    atomic_init(&p->cancelled,0);
    char *path = plugin_string(h, config, n);
    if (!path || !n) {
        h->deallocate(path);
        h->deallocate(p);
        return UD_INVALID;
    }
    int rc = sqlite3_open_v2(
        path, &p->db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
    h->deallocate(path);
    if (rc != SQLITE_OK) {
        ud_status s = failure(p, rc);
        sqlite3_close(p->db);
        h->deallocate(p);
        return s;
    }
    *out = p;
    return UD_OK;
}
static ud_status read_batch(sqlite_state *p, const ud_batch *schema, const char *sql, uint32_t n,
                            ud_batch **out) {
    ud_host_api *h = &p->host;
    if (!schema || (p->read && n) || (!p->read && !n) || n > INT_MAX)
        return UD_INVALID;
    if (!p->read) {
        const char *tail = NULL;
        int rc = sqlite3_prepare_v2(p->db, sql, (int)n, &p->read, &tail);
        if (rc != SQLITE_OK)
            return failure(p, rc);
        while (tail < sql + n && (*tail == ' ' || *tail == '\n' || *tail == '\r' || *tail == '\t'))
            tail++;
        if (tail != sql + n || !p->read || !sqlite3_stmt_readonly(p->read)) {
            sqlite3_finalize(p->read);
            p->read = NULL;
            return UD_INVALID;
        }
    }
    uint32_t columns = h->columns(schema);
    if (sqlite3_column_count(p->read) != (int)columns)
        return UD_SCHEMA;
    ud_field *fields = NULL;
    ud_status s = plugin_fields(h, schema, &fields);
    for (uint32_t i = 0; !s && i < columns; i++)
        if (fields[i].type > UD_BINARY || strcmp(fields[i].name, sqlite3_column_name(p->read, (int)i)))
            s = UD_SCHEMA;
    if (!s)
        s = h->batch_create(h->context, fields, columns, p->cursor_batch_size, out);
    ud_value *v = h->allocate(h->context, columns * sizeof(*v));
    if (!v && !s)
        s = UD_RESOURCE;
    while (!s && h->rows(*out) < p->cursor_batch_size) {
        if (h->cancelled(h->context) || atomic_load(&p->cancelled)) {
            s = UD_CANCELLED;
            break;
        }
        int rc = sqlite3_step(p->read);
        if (rc == SQLITE_DONE) {
            sqlite3_finalize(p->read);
            p->read = NULL;
            break;
        }
        if (rc != SQLITE_ROW) {
            s = failure(p, rc);
            break;
        }
        memset(v, 0, columns * sizeof(*v));
        for (uint32_t i = 0; i < columns; i++) {
            ud_value *x = v + i;
            int type = sqlite3_column_type(p->read, (int)i);
            x->type = fields[i].type;
            x->precision = fields[i].precision; x->scale = fields[i].scale;
            x->is_null = type == SQLITE_NULL;
            if (x->is_null)
                continue;
            if (x->type == UD_UTF8 && type == SQLITE_TEXT) {
                x->text = (const char *)sqlite3_column_text(p->read, (int)i);
                x->text_length = (uint32_t)sqlite3_column_bytes(p->read, (int)i);
            } else if (x->type == UD_BINARY && type == SQLITE_BLOB) {
                x->text = (const char *)sqlite3_column_blob(p->read, (int)i);
                x->text_length = (uint32_t)sqlite3_column_bytes(p->read, (int)i);
            } else if (x->type == UD_DECIMAL128 && type == SQLITE_TEXT) {
                s = h->parse_field(h->context, fields + i, (const char *)sqlite3_column_text(p->read, (int)i),
                    (uint32_t)sqlite3_column_bytes(p->read, (int)i), UD_ROUND_EXACT, x);
            } else if ((x->type == UD_INT64 || x->type == UD_BOOL || x->type == UD_DATE32 ||
                       x->type == UD_TIME64 || x->type == UD_TIMESTAMP) && type == SQLITE_INTEGER)
                x->integer = sqlite3_column_int64(p->read, (int)i);
            else if (x->type == UD_FLOAT64 && type == SQLITE_FLOAT)
                x->real = sqlite3_column_double(p->read, (int)i);
            else {
                s = h->report(h->context, UD_SCHEMA,
                              "SQLite value type differs from explicit schema");
                break;
            }
        }
        if (!s)
            s = h->append(*out, v, columns);
    }
    h->deallocate(fields);
    h->deallocate(v);
    if (s && p->read) {
        sqlite3_finalize(p->read);
        p->read = NULL;
    }
    if (!s && !h->rows(*out)) {
        h->batch_destroy(*out);
        *out = NULL;
        return UD_END;
    }
    return s;
}
static void destroy(void *ptr) {
    sqlite_state *p = ptr;
    if (!p)
        return;
    sqlite3_finalize(p->read);
    p->host.batch_destroy(p->cursor_schema);
    sqlite3_close(p->db);
    p->host.deallocate(p);
}
static ud_status connector_test(void *state) {
    sqlite_state *p = state;
    int rc = sqlite3_exec(p->db,"SELECT 1",NULL,NULL,NULL);
    return rc == SQLITE_OK ? UD_OK : failure(p,rc);
}
static ud_status connector_discover(void *state, ud_string table, ud_batch **out) {
    sqlite_state *p = state; ud_host_api *h = &p->host;
    ud_field fields[] = {{.name="name",.name_length=4,.type=UD_UTF8},
        {.name="declared_type",.name_length=13,.type=UD_UTF8},
        {.name="nullable",.name_length=8,.type=UD_BOOL},
        {.name="key_order",.name_length=9,.type=UD_INT64},
        {.name="readable",.name_length=8,.type=UD_BOOL},
        {.name="appendable",.name_length=10,.type=UD_BOOL},
        {.name="updatable",.name_length=9,.type=UD_BOOL},
        {.name="deletable",.name_length=9,.type=UD_BOOL}};
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(p->db,"SELECT name,type,\"notnull\",pk FROM pragma_table_xinfo(?1) WHERE hidden=0 ORDER BY cid",-1,&stmt,NULL);
    if (rc != SQLITE_OK) return failure(p,rc);
    rc = sqlite3_bind_text64(stmt,1,table.data,table.length,SQLITE_TRANSIENT,SQLITE_UTF8);
    ud_status s = rc == SQLITE_OK ? h->batch_create(h->context,fields,8,65536,out) : failure(p,rc);
    while (!s && (rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        ud_value values[8] = {{.type=UD_UTF8},{.type=UD_UTF8},{.type=UD_BOOL},{.type=UD_INT64},
            {.type=UD_BOOL,.integer=1},{.type=UD_BOOL},{.type=UD_BOOL},{.type=UD_BOOL}};
        for (int j=0;j<2;j++) { values[j].text=(const char *)sqlite3_column_text(stmt,j); values[j].text_length=(uint32_t)sqlite3_column_bytes(stmt,j); }
        values[2].integer=!sqlite3_column_int(stmt,2);
        values[3].integer=sqlite3_column_int(stmt,3);
        for(uint32_t j=5;j<8;j++) values[j].integer=sqlite3_db_readonly(p->db,"main")==0;
        s=h->append(*out,values,8);
    }
    if (!s && rc != SQLITE_DONE) s=failure(p,rc);
    sqlite3_finalize(stmt);
    if (!s && !h->rows(*out)) s=UD_SCHEMA;
    return s;
}
static ud_status connector_read_close(void *state) {
    sqlite_state *p=state; int rc=sqlite3_finalize(p->read); p->read=NULL;
    p->host.batch_destroy(p->cursor_schema);p->cursor_schema=NULL;p->cursor_finished=1;
    return rc==SQLITE_OK ? UD_OK : failure(p,rc);
}
static ud_status connector_read_open(void *state,const ud_read_options *options,const ud_batch *schema) {
    sqlite_state *p=state;ud_host_api *h=&p->host;
    if(p->read || p->cursor_schema || options->sql.length>INT_MAX) return UD_INVALID;
    atomic_store(&p->cancelled,0);
    const char *tail=NULL;
    int rc=sqlite3_prepare_v2(p->db,options->sql.data,(int)options->sql.length,&p->read,&tail);
    if(rc!=SQLITE_OK) return failure(p,rc);
    while(tail<options->sql.data+options->sql.length && (*tail==' '||*tail=='\n'||*tail=='\r'||*tail=='\t')) tail++;
    ud_status s=tail!=options->sql.data+options->sql.length || !p->read || !sqlite3_stmt_readonly(p->read) ? UD_INVALID : UD_OK;
    uint32_t columns=h->columns(schema);ud_field *fields=NULL;
    if(!s && sqlite3_column_count(p->read)!=(int)columns) s=UD_SCHEMA;
    if(!s) s=plugin_fields(h,schema,&fields);
    for(uint32_t j=0;!s && j<columns;j++)
        if(fields[j].type>UD_BINARY || strcmp(fields[j].name,sqlite3_column_name(p->read,(int)j))) s=UD_SCHEMA;
    if(!s) s=h->batch_create(h->context,fields,columns,0,&p->cursor_schema);
    h->deallocate(fields);
    if(s) { connector_read_close(p);return s; }
    p->cursor_finished=0;p->cursor_batch_size=options->batch_size;return UD_OK;
}
static ud_status connector_read_next(void *state,ud_batch **out) {
    sqlite_state *p=state;*out=NULL;
    if(p->cursor_finished) return UD_END;
    if(atomic_load(&p->cancelled)) return UD_CANCELLED;
    ud_status s=read_batch(p,p->cursor_schema,"",0,out);
    if(!p->read) p->cursor_finished=1;
    return s;
}
static void connector_cancel(void *state) {
    sqlite_state *p=state;atomic_store(&p->cancelled,1);sqlite3_interrupt(p->db);
}
static ud_status connector_receipt(void *state,ud_string tenant,ud_string job,ud_string batch_id,ud_commit_receipt *out) {
    sqlite_state *p=state;memset(out,0,sizeof(*out));sqlite3_stmt *stmt=NULL;
    int rc=sqlite3_prepare_v2(p->db,"SELECT digest,source_position FROM ud_sdk_receipts_v2 WHERE tenant=?1 AND job=?2 AND batch_id=?3",-1,&stmt,NULL);
    if(rc!=SQLITE_OK) return p->host.report(p->host.context,UD_COMMIT_UNKNOWN,"receipt store is unavailable; commit outcome cannot be proved");
    ud_string keys[]={tenant,job,batch_id};
    for(int j=0;j<3 && rc==SQLITE_OK;j++) rc=sqlite3_bind_text64(stmt,j+1,keys[j].data,keys[j].length,SQLITE_TRANSIENT,SQLITE_UTF8);
    if(rc==SQLITE_OK) rc=sqlite3_step(stmt);
    ud_status s=UD_OK;
    if(rc==SQLITE_ROW) {
        if(sqlite3_column_type(stmt,0)!=SQLITE_BLOB || sqlite3_column_bytes(stmt,0)!=32 || sqlite3_column_type(stmt,1)!=SQLITE_TEXT) s=UD_CORRUPT;
        else {
            memcpy(out->digest,sqlite3_column_blob(stmt,0),32);
            const char *position=(const char *)sqlite3_column_text(stmt,1);uint32_t length=(uint32_t)sqlite3_column_bytes(stmt,1);
            char *owned=plugin_string(&p->host,position,length);
            if(!owned) s=UD_RESOURCE;
            else { out->source_position=(ud_string){owned,length};out->committed=1; }
        }
    } else if(rc!=SQLITE_DONE) s=UD_COMMIT_UNKNOWN;
    sqlite3_finalize(stmt);return s;
}
static ud_status bind_connector_value(sqlite_state *p,sqlite3_stmt *stmt,int index,const ud_value *v) {
    int rc;
    if(v->is_null) rc=sqlite3_bind_null(stmt,index);
    else if(v->type==UD_UTF8) rc=sqlite3_bind_text64(stmt,index,v->text,v->text_length,SQLITE_TRANSIENT,SQLITE_UTF8);
    else if(v->type==UD_BINARY) rc=sqlite3_bind_blob64(stmt,index,v->text?v->text:"",v->text_length,SQLITE_TRANSIENT);
    else if(v->type==UD_DECIMAL128) {
        char text[160];uint32_t length;
        ud_status s=p->host.format_value(v,text,sizeof(text),&length);
        if(s) return s;
        rc=sqlite3_bind_text(stmt,index,text,(int)length-1,SQLITE_TRANSIENT);
    } else if (v->type == UD_FLOAT64) {
        if (isnan(v->real))
            return p->host.report(
                p->host.context, UD_UNSUPPORTED,
                "SQLite cannot preserve Float64 NaN; refusing SQL NULL conversion");
        rc = sqlite3_bind_double(stmt, index, v->real);
    } else if (v->type <= UD_TIMESTAMP)
        rc = sqlite3_bind_int64(stmt, index, v->integer);
    else return UD_UNSUPPORTED;
    return rc==SQLITE_OK ? UD_OK : failure(p,rc);
}
static ud_status validate_target(sqlite_state *p,ud_string table,const ud_batch *batch) {
    ud_host_api *h=&p->host; sqlite3_stmt *stmt=NULL;
    int rc=sqlite3_prepare_v2(p->db,"SELECT name,type FROM pragma_table_xinfo(?1) WHERE hidden=0",-1,&stmt,NULL);
    if(rc!=SQLITE_OK) return failure(p,rc);
    rc=sqlite3_bind_text64(stmt,1,table.data,table.length,SQLITE_TRANSIENT,SQLITE_UTF8);
    uint32_t columns=h->columns(batch);
    uint8_t *seen=h->allocate(h->context,columns);
    ud_status s=!seen ? UD_RESOURCE : rc!=SQLITE_OK ? failure(p,rc) : UD_OK;
    while(!s && (rc=sqlite3_step(stmt))==SQLITE_ROW) {
        const char *name=(const char *)sqlite3_column_text(stmt,0);
        const char *declared=(const char *)sqlite3_column_text(stmt,1);
        int length=sqlite3_column_bytes(stmt,1);char upper[128];
        if(!declared || length<0 || length>=(int)sizeof(upper)) { s=UD_UNSUPPORTED;break; }
        for(int j=0;j<length;j++) upper[j]=(char)toupper((unsigned char)declared[j]);upper[length]=0;
        int affinity=strstr(upper,"INT") ? 1 : strstr(upper,"CHAR") || strstr(upper,"CLOB") || strstr(upper,"TEXT") ? 2 :
            !length || strstr(upper,"BLOB") ? 3 : strstr(upper,"REAL") || strstr(upper,"FLOA") || strstr(upper,"DOUB") ? 4 : 5;
        for(uint32_t j=0;j<columns;j++) {
            ud_field f;h->field(batch,j,&f);
            if(strcmp(name,f.name)) continue;
            seen[j]=1;
            int expected=f.type==UD_UTF8 || f.type==UD_DECIMAL128 ? 2 : f.type==UD_BINARY ? 3 : f.type==UD_FLOAT64 ? 4 : 1;
            if(expected!=affinity) s=h->report(h->context,UD_SCHEMA,"target SQLite affinity would coerce a typed value; use TEXT for Decimal and INTEGER for UTC microseconds");
        }
    }
    if(!s && rc!=SQLITE_DONE) s=failure(p,rc);
    for(uint32_t j=0;!s && j<columns;j++) if(!seen[j]) s=UD_SCHEMA;
    h->deallocate(seen);sqlite3_finalize(stmt);return s;
}
static ud_status connector_write(void *state,ud_string tenant,const ud_write_options *options,
                                  const ud_batch *batch,const uint8_t digest[32]) {
    sqlite_state *p=state;ud_host_api *h=&p->host;
    if(p->read || p->cursor_schema) return UD_BUSY;
    atomic_store(&p->cancelled,0);
    char *sql=NULL;ud_status s=connector_write_sql(h,options,batch,0,&sql);
    if(s) return s;
    int rc=sqlite3_exec(p->db,"BEGIN IMMEDIATE",NULL,NULL,NULL);
    if(rc!=SQLITE_OK) { h->deallocate(sql);return rc==SQLITE_BUSY || rc==SQLITE_LOCKED ? UD_TRANSIENT : failure(p,rc); }
    rc=sqlite3_exec(p->db,"CREATE TABLE IF NOT EXISTS ud_sdk_receipts_v2(tenant TEXT NOT NULL,job TEXT NOT NULL,batch_id TEXT NOT NULL,digest BLOB NOT NULL CHECK(length(digest)=32),source_position TEXT NOT NULL,PRIMARY KEY(tenant,job,batch_id))",NULL,NULL,NULL);
    if(rc!=SQLITE_OK) s=failure(p,rc);
    if(!s) s=validate_target(p,options->table,batch);
    ud_commit_receipt receipt={0};
    if(!s) s=connector_receipt(p,tenant,options->job,options->batch_id,&receipt);
    int existing=receipt.committed;
    if(!s && existing && memcmp(receipt.digest,digest,32)) s=h->report(h->context,UD_CORRUPT,"batch id was reused with a different payload / cursor / destination");
    h->deallocate((void *)receipt.source_position.data);
    sqlite3_stmt *stmt=NULL;
    if(!s && !existing) {
        rc=sqlite3_prepare_v2(p->db,sql,-1,&stmt,NULL);
        if(rc!=SQLITE_OK) s=failure(p,rc);
    }
    uint32_t columns=options->mode==UD_WRITE_DELETE ? options->key_count : h->columns(batch);
    for(uint32_t r=0;!s && !existing && r<h->rows(batch);r++) {
        if(h->cancelled(h->context) || atomic_load(&p->cancelled)) { s=UD_CANCELLED;break; }
        sqlite3_reset(stmt);sqlite3_clear_bindings(stmt);
        for(uint32_t j=0;!s && j<options->key_count;j++) {
            ud_value value;h->get(batch,r,options->keys[j],&value);
            if(value.is_null && options->mode!=UD_WRITE_APPEND) s=UD_SCHEMA;
        }
        for(uint32_t j=0;!s && j<columns;j++) {
            ud_value value;h->get(batch,r,options->mode==UD_WRITE_DELETE ? options->keys[j] : j,&value);
            s=bind_connector_value(p,stmt,(int)j+1,&value);
        }
        if(!s && (rc=sqlite3_step(stmt))!=SQLITE_DONE) s=failure(p,rc);
    }
    sqlite3_finalize(stmt);stmt=NULL;
    if(!s && !existing) {
        rc=sqlite3_prepare_v2(p->db,"INSERT INTO ud_sdk_receipts_v2(tenant,job,batch_id,digest,source_position) VALUES(?1,?2,?3,?4,?5)",-1,&stmt,NULL);
        if(rc!=SQLITE_OK) s=failure(p,rc);
        ud_string strings[]={tenant,options->job,options->batch_id};
        for(int j=0;!s && j<3;j++) { rc=sqlite3_bind_text64(stmt,j+1,strings[j].data,strings[j].length,SQLITE_TRANSIENT,SQLITE_UTF8);if(rc!=SQLITE_OK) s=failure(p,rc); }
        if(!s) { rc=sqlite3_bind_blob(stmt,4,digest,32,SQLITE_TRANSIENT);if(rc!=SQLITE_OK) s=failure(p,rc); }
        if(!s) { rc=sqlite3_bind_text64(stmt,5,options->source_position.data?options->source_position.data:"",options->source_position.length,SQLITE_TRANSIENT,SQLITE_UTF8);if(rc!=SQLITE_OK) s=failure(p,rc); }
        if(!s && (rc=sqlite3_step(stmt))!=SQLITE_DONE) s=failure(p,rc);
    }
    sqlite3_finalize(stmt);h->deallocate(sql);
    if(!s && (h->cancelled(h->context) || atomic_load(&p->cancelled))) s=UD_CANCELLED;
    if(!s) {
        rc=sqlite3_exec(p->db,"COMMIT",NULL,NULL,NULL);
        if(rc!=SQLITE_OK) {
            sqlite3_exec(p->db,"ROLLBACK",NULL,NULL,NULL);
            return h->report(h->context,UD_COMMIT_UNKNOWN,"commit returned an error; query the receipt before advancing source position");
        }
    } else if(sqlite3_exec(p->db,"ROLLBACK",NULL,NULL,NULL)!=SQLITE_OK && !sqlite3_get_autocommit(p->db))
        return h->report(h->context,UD_COMMIT_UNKNOWN,"rollback failed; query the receipt before any replay");
    return s;
}
static ud_status connector_execute_sql(void *state,ud_string statement) {
    sqlite_state *p=state;char *sql=plugin_string(&p->host,statement.data,statement.length);
    if(!sql) return UD_RESOURCE;int rc=sqlite3_exec(p->db,sql,NULL,NULL,NULL);p->host.deallocate(sql);
    return rc==SQLITE_OK?UD_OK:failure(p,rc);
}
static const ud_connector_api connector={sizeof(connector),UD_PLUGIN_ABI,
    UD_CONNECT_READ|UD_CONNECT_APPEND|UD_CONNECT_UPSERT|UD_CONNECT_DELETE|UD_CONNECT_RECEIPTS|UD_CONNECT_EXECUTE_SQL,
    connector_test,connector_discover,connector_read_open,connector_read_next,connector_read_close,
    connector_write,connector_receipt,connector_cancel,connector_execute_sql};

UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version, ud_plugin_api *out) {
    if (version != UD_PLUGIN_ABI || !out || out->struct_size != sizeof(*out))
        return UD_INCOMPATIBLE;
    *out = (ud_plugin_api){sizeof(*out),
                           UD_PLUGIN_ABI,
                           "org.universaldata.connector.sqlite",
                           "1.0.0",
                           UD_PLUGIN_CONNECTOR,
                           UD_CAP_READ | UD_CAP_WRITE | UD_CAP_TRANSACTION,
                           initialize,
                           destroy,
                           plugin_shutdown,
                           &connector, NULL};
    return UD_OK;
}
