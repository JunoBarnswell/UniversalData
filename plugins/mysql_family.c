#include "mysql_family.h"
#include "binlog.h"
#if defined(UD_MARIADB_CLIENT)
#include <mariadb_rpl.h>
#endif
#include <stdatomic.h>
#include <stdio.h>
#include <math.h>
#include <pthread.h>
#if defined(UD_MYSQL_CLIENT)
typedef bool ud_mysql_bool;
#else
typedef my_bool ud_mysql_bool;
#endif
#define UD_MYSQL_CELL_LIMIT (16u * 1024u * 1024u)
typedef struct mysql_cdc {MYSQL*stream;ud_binlog*decoder;ud_batch*schema;char*file,*table,*gtid;uint64_t position,confirmed;uint32_t phase,rows;int snapshot,opened;
#if defined(UD_MYSQL_CLIENT)
    MYSQL_RPL rpl;
#else
    MARIADB_RPL*rpl;MARIADB_RPL_EVENT*event;
#endif
} mysql_cdc;
typedef struct mysql_state {
    ud_host_api host;
    MYSQL *db, *cancel_db;
    mysql_cdc *cdc;
    MYSQL_STMT *cursor;
    ud_batch *schema;
    uint32_t batch_size;
    char *host_name, *user, *password, *database, *ca, *cert, *key;
    uint32_t port, timeout;
    int utc, transaction, broken;
    atomic_int cancelled;
    pthread_mutex_t cancellation;
    void *check_user;
    ud_status (*check)(void *);
} mysql_state;
typedef struct bindings {
    mysql_state *state;
    MYSQL_BIND *bind;
    unsigned long *length;
    ud_mysql_bool *nulls;
    char **owned;
    ud_value *values;
    uint32_t count;
} bindings;
static ud_status check(mysql_state *p) {
    if (atomic_load(&p->cancelled) || p->host.cancelled(p->host.context)) return UD_CANCELLED;
    return p->check ? p->check(p->check_user) : UD_OK;
}
static ud_status failure(mysql_state *p, MYSQL_STMT *stmt) {
    unsigned int code = stmt ? mysql_stmt_errno(stmt) : mysql_errno(p->db);
    const char *state = stmt ? mysql_stmt_sqlstate(stmt) : mysql_sqlstate(p->db);
    if (atomic_load(&p->cancelled)) return UD_CANCELLED;
    if (code == 2006 || code == 2013 || code == 2055) { p->broken = 1; return UD_TRANSIENT; }
    if (code == 1045 || (state && !strncmp(state, "28", 2))) return UD_AUTH;
    if (code == 1205) return UD_TIMEOUT;
    if (code == 1213 || (state && !strcmp(state, "40001"))) return UD_TRANSIENT;
    if (state && !strncmp(state, "23", 2)) return UD_QUALITY;
    if (state && !strncmp(state, "22", 2)) return UD_PARSE;
    if (state && !strncmp(state, "42", 2)) return UD_SCHEMA;
    return UD_IO;
}
static void bindings_free(bindings *b) {
    if (!b->state) return;
    for (uint32_t i = 0; i < b->count; i++) b->state->host.deallocate(b->owned ? b->owned[i] : NULL);
    b->state->host.deallocate(b->owned); b->state->host.deallocate(b->values);
    b->state->host.deallocate(b->bind); b->state->host.deallocate(b->length);
    b->state->host.deallocate(b->nulls); memset(b, 0, sizeof(*b));
}
static ud_status bindings_create(mysql_state *p, uint32_t n, bindings *b) {
    *b = (bindings){.state = p, .count = n};
    if (!n) return UD_OK;
    b->bind = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->bind));
    b->length = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->length));
    b->nulls = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->nulls));
    b->owned = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->owned));
    b->values = p->host.allocate(p->host.context, (size_t)n * sizeof(*b->values));
    if (!b->bind || !b->length || !b->nulls || !b->owned || !b->values) { bindings_free(b); return UD_RESOURCE; }
    return UD_OK;
}
static ud_status bind_value(bindings *b, uint32_t at, ud_value v) {
    if (at >= b->count) return UD_SCHEMA;
    mysql_state *p = b->state;
    b->values[at] = v; b->nulls[at] = !!v.is_null;
    MYSQL_BIND *bind = b->bind + at;
    bind->is_null = b->nulls + at; bind->length = b->length + at;
    if (v.is_null) { bind->buffer_type = MYSQL_TYPE_NULL; return UD_OK; }
    if (v.type == UD_INT64 || v.type == UD_BOOL) {
        if (v.type == UD_BOOL && v.integer != 0 && v.integer != 1) return UD_SCHEMA;
        bind->buffer_type = MYSQL_TYPE_LONGLONG; bind->buffer = &b->values[at].integer;
        bind->buffer_length = sizeof(int64_t); return UD_OK;
    }
    if (v.type == UD_FLOAT64) {
        if (!isfinite(v.real)) return UD_UNSUPPORTED;
        bind->buffer_type = MYSQL_TYPE_DOUBLE; bind->buffer = &b->values[at].real;
        bind->buffer_length = sizeof(double); return UD_OK;
    }
    if (v.type == UD_UTF8 || v.type == UD_BINARY) {
        bind->buffer_type = v.type == UD_BINARY ? MYSQL_TYPE_BLOB : MYSQL_TYPE_STRING;
        bind->buffer = (void *)v.text; bind->buffer_length = b->length[at] = v.text_length; return UD_OK;
    }
    if (v.type == UD_DECIMAL128 || v.type == UD_DATE32 || v.type == UD_TIME64 || v.type == UD_TIMESTAMP) {
        char value[160]; uint32_t n = 0;
        ud_status status = p->host.format_value(&v, value, sizeof(value), &n);
        if (status) return status;
        if (!n || n > sizeof(value)) return UD_RESOURCE;
        n--;
        if (v.type == UD_TIMESTAMP) {
            if (n < 20 || value[n - 1] != 'Z') return UD_SCHEMA;
            value[10] = ' '; value[--n] = 0;
        }
        b->owned[at] = plugin_string(&p->host, value, n);
        if (!b->owned[at]) return UD_RESOURCE;
        bind->buffer_type = MYSQL_TYPE_STRING; bind->buffer = b->owned[at];
        bind->buffer_length = b->length[at] = n; return UD_OK;
    }
    return UD_UNSUPPORTED;
}
static ud_status bind_batch(bindings *b, uint32_t offset, const ud_batch *batch, uint32_t row,
                            const uint32_t *columns, uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        ud_value value; ud_status s = b->state->host.get(batch, row, columns ? columns[i] : i, &value);
        if (!s) s = bind_value(b, offset + i, value);
        if (s) return s;
    }
    return UD_OK;
}
static ud_status statement(mysql_state *p, ud_string sql, MYSQL_STMT **out) {
    *out = NULL;
    ud_status s = check(p); if (s) return s;
    if (!sql.data || !sql.length || memchr(sql.data, 0, sql.length)) return UD_SCHEMA;
    MYSQL_STMT *stmt = mysql_stmt_init(p->db);
    if (!stmt) return UD_RESOURCE;
    if (mysql_stmt_prepare(stmt, sql.data, sql.length)) { s = failure(p, stmt); mysql_stmt_close(stmt); return s; }
    *out = stmt; return UD_OK;
}
static ud_status execute_bound(mysql_state *p, MYSQL_STMT *stmt, bindings *b, uint64_t *affected) {
    ud_status s = check(p); if (s) return s;
    if (mysql_stmt_param_count(stmt) != b->count) return UD_SCHEMA;
    if (b->count && mysql_stmt_bind_param(stmt, b->bind)) return failure(p, stmt);
    if (mysql_stmt_execute(stmt)) return failure(p, stmt);
    if (affected) *affected = (uint64_t)mysql_stmt_affected_rows(stmt);
    return check(p);
}
static ud_status command(mysql_state *p, const char *sql) {
    ud_status s = check(p);
    if (!s && mysql_real_query(p->db, sql, (unsigned long)strlen(sql))) s = failure(p, NULL);
    return s;
}
static ud_status session_policy(mysql_state *p) {
    if (mysql_set_character_set(p->db, "utf8mb4")) return failure(p, NULL);
    ud_status s = command(p, "SET SESSION time_zone='+00:00'");
    if (!s) s = command(p, "SET SESSION sql_mode='STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'");
    return s;
}
static ud_status read_close(void *state) {
    mysql_state *p = state;
    if (p->cursor) mysql_stmt_close(p->cursor);
    p->cursor = NULL; p->host.batch_destroy(p->schema); p->schema = NULL; return UD_OK;
}
static ud_status read_open(void *state, const ud_read_options *o, const ud_batch *schema) {
    mysql_state *p = state; if (p->cursor) return UD_BUSY;
    MYSQL_STMT *stmt = NULL; ud_status s = statement(p, o->sql, &stmt);
    unsigned long cursor = CURSOR_TYPE_READ_ONLY, prefetch = o->batch_size;
    if (!s && (mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &cursor) ||
               mysql_stmt_attr_set(stmt, STMT_ATTR_PREFETCH_ROWS, &prefetch))) s = failure(p, stmt);
    if (!s && !mysql_stmt_field_count(stmt)) s = UD_INVALID;
    bindings b = {0}; uint32_t count = o->parameters ? p->host.columns(o->parameters) : 0;
    if (!s) s = bindings_create(p, count, &b);
    if (!s && count) s = bind_batch(&b, 0, o->parameters, o->parameter_row, NULL, count);
    if (!s) s = execute_bound(p, stmt, &b, NULL);
    bindings_free(&b);
    MYSQL_RES *meta = !s ? mysql_stmt_result_metadata(stmt) : NULL;
    uint32_t columns = p->host.columns(schema);
    if (!s && (!meta || mysql_num_fields(meta) != columns)) s = UD_SCHEMA;
    MYSQL_FIELD *actual = meta ? mysql_fetch_fields(meta) : NULL;
    ud_field *fields = NULL; if (!s) s = plugin_fields(&p->host, schema, &fields);
    for (uint32_t i = 0; !s && i < columns; i++) {
        if (fields[i].type > UD_BINARY || fields[i].name_length != actual[i].name_length ||
            memcmp(fields[i].name, actual[i].name, fields[i].name_length)) s = UD_SCHEMA;
        if (!s && fields[i].type == UD_TIMESTAMP && !p->utc && actual[i].type == MYSQL_TYPE_DATETIME) s = UD_UNSUPPORTED;
    }
    if (!s) s = p->host.batch_create(p->host.context, fields, columns, 0, &p->schema);
    p->host.deallocate(fields); if (meta) mysql_free_result(meta);
    if (s) { if (stmt) mysql_stmt_close(stmt); return s; }
    p->cursor = stmt; p->batch_size = o->batch_size; return UD_OK;
}
static ud_status read_next(void *state, ud_batch **out) {
    mysql_state *p = state; *out = NULL; if (!p->cursor || !p->schema) return UD_END;
    uint32_t n = p->host.columns(p->schema); bindings b = {0};
    ud_status s = bindings_create(p, n, &b); ud_field *fields = NULL;
    if (!s) s = plugin_fields(&p->host, p->schema, &fields);
    for (uint32_t i = 0; !s && i < n; i++) {
        b.owned[i] = p->host.allocate(p->host.context, 257);
        if (!b.owned[i]) { s = UD_RESOURCE; break; }
        b.bind[i].buffer_type = fields[i].type == UD_BINARY ? MYSQL_TYPE_BLOB : MYSQL_TYPE_STRING;
        b.bind[i].buffer = b.owned[i]; b.bind[i].buffer_length = 256;
        b.bind[i].length = b.length + i; b.bind[i].is_null = b.nulls + i;
    }
    if (!s && mysql_stmt_bind_result(p->cursor, b.bind)) s = failure(p, p->cursor);
    if (!s) s = p->host.batch_create(p->host.context, fields, n, p->batch_size, out);
    int ended = 0;
    while (!s && p->host.rows(*out) < p->batch_size) {
        s = check(p); if (s) break;
        int rc = mysql_stmt_fetch(p->cursor);
        if (rc == MYSQL_NO_DATA) { ended = 1; break; }
        if (rc && rc != MYSQL_DATA_TRUNCATED) { s = failure(p, p->cursor); break; }
        for (uint32_t i = 0; !s && i < n; i++) {
            ud_value *v = b.values + i; *v = (ud_value){.type=fields[i].type,.precision=fields[i].precision,.scale=fields[i].scale,.is_null=!!b.nulls[i]};
            if (v->is_null) continue;
            if (b.length[i] > UD_MYSQL_CELL_LIMIT) { s = UD_RESOURCE; break; }
            if (b.length[i] > b.bind[i].buffer_length) {
                char *next = p->host.allocate(p->host.context, (size_t)b.length[i] + 1);
                if (!next) { s = UD_RESOURCE; break; }
                p->host.deallocate(b.owned[i]); b.owned[i] = next;
                b.bind[i].buffer = next; b.bind[i].buffer_length = b.length[i];
                if (mysql_stmt_fetch_column(p->cursor, b.bind + i, i, 0)) { s = failure(p, p->cursor); break; }
            }
            if (v->type == UD_BINARY) { v->text = b.owned[i]; v->text_length = (uint32_t)b.length[i]; }
            else if (v->type == UD_TIMESTAMP) {
                if (b.length[i] < 19 || b.length[i] > 40 || !memcmp(b.owned[i], "0000-00-00", 10)) { s = UD_PARSE; break; }
                char time[48]; memcpy(time, b.owned[i], b.length[i]); time[10]='T'; time[b.length[i]]='Z';
                s = p->host.parse_field(p->host.context, fields + i, time, (uint32_t)b.length[i]+1, UD_ROUND_EXACT, v);
            } else if (v->type == UD_BOOL) {
                if (b.length[i] != 1 || (b.owned[i][0]!='0' && b.owned[i][0]!='1')) s = UD_SCHEMA;
                else v->integer = b.owned[i][0]-'0';
            } else s = p->host.parse_field(p->host.context, fields + i, b.owned[i], (uint32_t)b.length[i], UD_ROUND_EXACT, v);
        }
        if (!s) s = p->host.append(*out, b.values, n);
        /* Bind pointers are refreshed after a truncated column grows. */
        if (!s && mysql_stmt_bind_result(p->cursor, b.bind)) s = failure(p, p->cursor);
    }
    p->host.deallocate(fields); bindings_free(&b);
    if (s || (ended && !p->host.rows(*out))) { p->host.batch_destroy(*out); *out=NULL; read_close(p); return s ? s : UD_END; }
    if (ended) { mysql_stmt_close(p->cursor); p->cursor=NULL; }
    return UD_OK;
}
static ud_status parameters(mysql_state *p, const ud_string *values, uint32_t count, ud_batch **out) {
    ud_field f[8]; ud_value v[8]; if (count > 8) return UD_INVALID;
    memset(f,0,sizeof(f)); memset(v,0,sizeof(v)); char names[8][8];
    for (uint32_t i=0;i<count;i++) { snprintf(names[i],sizeof(names[i]),"p%u",i); f[i]=(ud_field){.name=names[i],.name_length=(uint32_t)strlen(names[i]),.type=UD_UTF8};v[i]=(ud_value){.type=UD_UTF8,.text=values[i].data,.text_length=values[i].length}; }
    ud_status s=p->host.batch_create(p->host.context,f,count,1,out); if(!s)s=p->host.append(*out,v,count); return s;
}
static ud_status query(mysql_state *p, const char *sql, const ud_string *parameters_in, uint32_t count,
                        const ud_field *fields, uint32_t n, ud_batch **out) {
    if (p->cursor) return UD_BUSY;
    ud_batch *schema=NULL,*params=NULL;
    ud_status s=p->host.batch_create(p->host.context,fields,n,0,&schema);
    if(!s && count)s=parameters(p,parameters_in,count,&params);
    ud_read_options o={.struct_size=sizeof(o),.abi_version=UD_ABI_VERSION,.batch_size=65536,.sql={sql,(uint32_t)strlen(sql)},.parameters=params};
    if(!s)s=read_open(p,&o,schema); if(!s)s=read_next(p,out);
    if(s==UD_END) {s=p->host.batch_create(p->host.context,fields,n,0,out);}
    read_close(p);p->host.batch_destroy(schema);p->host.batch_destroy(params); return s;
}
static ud_status table_parts(mysql_state *p, ud_string table, ud_string values[2]) {
    const char *dot=memchr(table.data,'.',table.length);
    values[0]=(ud_string){p->database,(uint32_t)strlen(p->database)};values[1]=table;
    if(dot) {values[0]=(ud_string){table.data,(uint32_t)(dot-table.data)};values[1]=(ud_string){dot+1,table.length-(uint32_t)(dot-table.data)-1};}
    return values[0].length && values[1].length ? UD_OK : UD_SCHEMA;
}
static ud_status discover(void *state, ud_string table, ud_batch **out) {
    mysql_state *p=state; ud_string args[2];ud_status s=table_parts(p,table,args);
    ud_field f[]={{.name="name",.name_length=4,.type=UD_UTF8},{.name="declared_type",.name_length=13,.type=UD_UTF8},{.name="nullable",.name_length=8,.type=UD_BOOL},{.name="key_order",.name_length=9,.type=UD_INT64}, {.name="default",.name_length=7,.type=UD_UTF8,.nullable=1}, {.name="identity",.name_length=8,.type=UD_BOOL}};
    const char *sql=p->utc ?
      "SELECT c.COLUMN_NAME AS name,IF(c.DATA_TYPE='datetime',CONCAT('datetime_utc(',c.DATETIME_PRECISION,')'),c.COLUMN_TYPE) AS declared_type,(c.IS_NULLABLE='YES') AS nullable,COALESCE(k.ORDINAL_POSITION,0) AS key_order,c.COLUMN_DEFAULT AS `default`,(c.EXTRA LIKE '%auto_increment%') AS identity FROM information_schema.COLUMNS c LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND k.COLUMN_NAME=c.COLUMN_NAME AND k.CONSTRAINT_NAME='PRIMARY' WHERE c.TABLE_SCHEMA=? AND c.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION" :
      "SELECT c.COLUMN_NAME AS name,c.COLUMN_TYPE AS declared_type,(c.IS_NULLABLE='YES') AS nullable,COALESCE(k.ORDINAL_POSITION,0) AS key_order,c.COLUMN_DEFAULT AS `default`,(c.EXTRA LIKE '%auto_increment%') AS identity FROM information_schema.COLUMNS c LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND k.COLUMN_NAME=c.COLUMN_NAME AND k.CONSTRAINT_NAME='PRIMARY' WHERE c.TABLE_SCHEMA=? AND c.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION";
    return s ? s : query(p,sql,args,2,f,6,out);
}
static ud_status metadata(void *state,const ud_metadata_options *o,ud_batch **out) {
    mysql_state *p=state;
    if(o->kind==UD_METADATA_COLUMNS) return discover(p,o->parent,out);
    ud_field f[]={{.name="name",.name_length=4,.type=UD_UTF8},{.name="parent",.name_length=6,.type=UD_UTF8},{.name="definition",.name_length=10,.type=UD_UTF8}};
    ud_string args[2];uint32_t count=0;const char *sql=NULL;
    if(o->kind==UD_METADATA_CATALOGS || o->kind==UD_METADATA_SCHEMAS)sql="SELECT SCHEMA_NAME AS name,'' AS parent,DEFAULT_CHARACTER_SET_NAME AS definition FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME";
    else if(o->kind==UD_METADATA_TABLES) {args[0]=o->parent;count=1;sql="SELECT TABLE_NAME AS name,TABLE_SCHEMA AS parent,COALESCE(ENGINE,TABLE_TYPE) AS definition FROM information_schema.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME";}
    else {ud_status s=table_parts(p,o->parent,args);if(s)return s;count=2;
      if(o->kind==UD_METADATA_PRIMARY_KEYS)sql="SELECT COLUMN_NAME AS name,TABLE_NAME AS parent,CAST(ORDINAL_POSITION AS CHAR) AS definition FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION";
      else if(o->kind==UD_METADATA_FOREIGN_KEYS)sql="SELECT COLUMN_NAME AS name,TABLE_NAME AS parent,CONCAT(CONSTRAINT_NAME,':',ORDINAL_POSITION,':',REFERENCED_TABLE_SCHEMA,'.',REFERENCED_TABLE_NAME,'.',REFERENCED_COLUMN_NAME) AS definition FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND REFERENCED_TABLE_NAME IS NOT NULL ORDER BY CONSTRAINT_NAME,ORDINAL_POSITION";
      else if(o->kind==UD_METADATA_INDEXES)sql="SELECT INDEX_NAME AS name,TABLE_NAME AS parent,CONCAT(SEQ_IN_INDEX,':',COLUMN_NAME,':',NON_UNIQUE,':',INDEX_TYPE) AS definition FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX";
      else return UD_UNSUPPORTED; }
    ud_batch *all=NULL;ud_status s=query(p,sql,args,count,f,3,&all);
    uint64_t rows=!s?p->host.rows(all):0,begin=o->offset<rows?o->offset:rows,end=begin+o->limit;
    if(end>rows)end=rows;
    if(!s)s=p->host.batch_create(p->host.context,f,3,(uint32_t)(end-begin),out);
    for(uint64_t row=begin;!s&&row<end;row++){ud_value v[3];for(uint32_t j=0;!s&&j<3;j++)s=p->host.get(all,(uint32_t)row,j,v+j);if(!s)s=p->host.append(*out,v,3);}
    p->host.batch_destroy(all);return s;
}
static ud_status begin(void *state,int32_t isolation) {
    mysql_state *p=state;if(p->transaction || p->cursor)return UD_BUSY;
    const char *sql=isolation==UD_ISOLATION_SERIALIZABLE?"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE":isolation==UD_ISOLATION_REPEATABLE_READ?"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ":isolation==UD_ISOLATION_READ_COMMITTED?"SET TRANSACTION ISOLATION LEVEL READ COMMITTED":NULL;
    if(!sql)return UD_INVALID;ud_status s=command(p,sql);if(!s)s=command(p,"START TRANSACTION");if(!s)p->transaction=1;return s;
}
static ud_status commit(void *state){mysql_state *p=state;if(!p->transaction)return UD_INVALID;ud_status s=check(p);if(!s&&mysql_commit(p->db)){p->broken=1;s=UD_COMMIT_UNKNOWN;}if(!s)p->transaction=0;return s;}
static ud_status rollback(void *state){mysql_state *p=state;if(!p->transaction)return UD_INVALID;int rc=mysql_rollback(p->db);if(rc){p->broken=1;return failure(p,NULL);}p->transaction=0;return UD_OK;}
static ud_status test(void *state){mysql_state *p=state;ud_status s=check(p);return s?s:mysql_ping(p->db)?failure(p,NULL):UD_OK;}
static void set_control(void *state,void *user,ud_status(*fn)(void*)){mysql_state*p=state;p->check_user=user;p->check=fn;if(!fn)atomic_store(&p->cancelled,0);}
static ud_status execute(void *state,const ud_read_options *o,uint64_t *processed){mysql_state*p=state;if(p->cursor)return UD_BUSY;MYSQL_STMT*stmt=NULL;ud_status s=statement(p,o->sql,&stmt);uint32_t n=o->parameters?p->host.columns(o->parameters):0,rows=o->parameters?p->host.rows(o->parameters):1;uint64_t affected=0;for(uint32_t row=0;!s&&row<rows;row++){bindings b={0};s=bindings_create(p,n,&b);if(!s&&n)s=bind_batch(&b,0,o->parameters,row,NULL,n);uint64_t a=0;if(!s)s=execute_bound(p,stmt,&b,&a);bindings_free(&b);if(!s&&mysql_stmt_field_count(stmt))s=UD_INVALID;if(!s){if(a>UINT64_MAX-affected)s=UD_OVERFLOW;else affected+=a;}}if(stmt)mysql_stmt_close(stmt);if(processed&&!s)*processed=affected;return s;}
static ud_status receipt(void *state,ud_string tenant,ud_string job,ud_string batch,ud_commit_receipt*out) {
    mysql_state*p=state;memset(out,0,sizeof(*out));ud_string args[]={tenant,job,batch};
    ud_field f[]={{.name="digest",.name_length=6,.type=UD_BINARY},{.name="source_position",.name_length=15,.type=UD_UTF8}};
    ud_batch*rows=NULL;const char*sql=p->transaction?"SELECT digest,source_position FROM ud_sdk_receipts_v2 WHERE tenant=? AND job=? AND batch_id=? FOR UPDATE":"SELECT digest,source_position FROM ud_sdk_receipts_v2 WHERE tenant=? AND job=? AND batch_id=?";
    ud_status s=query(p,sql,args,3,f,2,&rows);
    if(!s&&p->host.rows(rows)>1)s=UD_CORRUPT;
    if(!s&&p->host.rows(rows)){ud_value d,pos;s=p->host.get(rows,0,0,&d);if(!s)s=p->host.get(rows,0,1,&pos);if(!s&&(d.is_null||d.text_length!=32||pos.is_null))s=UD_CORRUPT;
      if(!s){out->source_position.data=plugin_string(&p->host,pos.text,pos.text_length);out->source_position.length=pos.text_length;if(!out->source_position.data)s=UD_RESOURCE;else{memcpy(out->digest,d.text,32);out->committed=1;}}}
    p->host.batch_destroy(rows);return s;
}
static ud_status provision(void *state){mysql_state*p=state;if(p->transaction||p->cursor)return UD_BUSY;return command(p,"CREATE TABLE IF NOT EXISTS ud_sdk_receipts_v2(tenant VARBINARY(256) NOT NULL,job VARBINARY(256) NOT NULL,batch_id VARBINARY(256) NOT NULL,digest BINARY(32) NOT NULL,source_position LONGTEXT CHARACTER SET utf8mb4 NOT NULL,PRIMARY KEY(tenant,job,batch_id)) ENGINE=InnoDB");}
static ud_status prove_target(mysql_state*p,const ud_write_options*o,const ud_batch*batch){
    ud_string args[2];ud_status s=table_parts(p,o->table,args);ud_field f={.name="engine",.name_length=6,.type=UD_UTF8};ud_batch*engine=NULL;
    if(!s)s=query(p,"SELECT ENGINE AS engine FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",args,2,&f,1,&engine);
    if(!s&&p->host.rows(engine)!=1)s=UD_SCHEMA;ud_value e={0};if(!s)s=p->host.get(engine,0,0,&e);
    if(!s&&(e.is_null||e.text_length!=6||memcmp(e.text,"InnoDB",6)))s=UD_UNSUPPORTED;
    p->host.batch_destroy(engine);
    ud_batch*meta=NULL;if(!s)s=discover(p,o->table,&meta);
    uint32_t columns=p->host.columns(batch);
    for(uint32_t i=0;!s&&i<columns;i++){
      ud_field field;s=p->host.field(batch,i,&field);int found=0;
      for(uint32_t row=0;!s&&row<p->host.rows(meta);row++){
        ud_value name,type,key;s=p->host.get(meta,row,0,&name);if(!s)s=p->host.get(meta,row,1,&type);if(!s)s=p->host.get(meta,row,3,&key);
        if(!s&&field.name_length==name.text_length&&!memcmp(field.name,name.text,field.name_length)){
          found=1;ud_field actual;s=p->host.field_from_sql((ud_string){type.text,type.text_length},UD_MYSQL_DIALECT,&actual);
          if(!s&&(actual.type!=field.type||(actual.type==UD_DECIMAL128&&(actual.precision!=field.precision||actual.scale!=field.scale))))s=UD_SCHEMA;
          /* Boolean is an explicit value policy for integer 0/1, never a discovery inference. */
          if(s==UD_SCHEMA&&actual.type==UD_INT64&&field.type==UD_BOOL)s=UD_OK;
          for(uint32_t k=0;!s&&k<o->key_count;k++)if(o->keys[k]==i&&key.integer!=(int64_t)k+1)s=UD_SCHEMA;
          break;
        }
      }
      if(!s&&!found)s=UD_SCHEMA;
    }
    p->host.batch_destroy(meta);return s;
}
static ud_status receipt_insert(mysql_state*p,ud_string tenant,const ud_write_options*o,const uint8_t digest[32]){
    const char*sql="INSERT INTO ud_sdk_receipts_v2(tenant,job,batch_id,digest,source_position) VALUES(?,?,?,?,?)";MYSQL_STMT*stmt=NULL;ud_status s=statement(p,(ud_string){sql,(uint32_t)strlen(sql)},&stmt);bindings b={0};if(!s)s=bindings_create(p,5,&b);
    ud_string values[]={tenant,o->job,o->batch_id,o->source_position};
    for(uint32_t i=0;!s&&i<4;i++){uint32_t at=i<3?i:4;s=bind_value(&b,at,(ud_value){.type=UD_UTF8,.text=values[i].data,.text_length=values[i].length});}
    if(!s)s=bind_value(&b,3,(ud_value){.type=UD_BINARY,.text=(const char*)digest,.text_length=32});
    if(!s)s=execute_bound(p,stmt,&b,NULL);bindings_free(&b);if(stmt)mysql_stmt_close(stmt);return s;
}
static ud_status apply_statement(mysql_state *p,const ud_write_options *options,
                                  const ud_batch *batch,uint32_t first,uint32_t rows) {
    ud_sql_statement plan = {0};
    ud_status status=p->host.compile_write(p->host.context,UD_MYSQL_DIALECT,UD_SQL_PARAMETER_QUESTION,
                                           options,batch,first,rows,&plan);
    MYSQL_STMT *stmt=NULL;bindings params={0};
    if(!status && plan.sql.length) status=statement(p,plan.sql,&stmt);
    if(!status)status=bindings_create(p,plan.parameter_count,&params);
    for(uint32_t i=0;!status && i<plan.parameter_count;i++) {
        const ud_sql_parameter *ref=plan.parameters+i;
        const ud_batch *source=ref->source==UD_SQL_BEFORE?options->before:batch;
        ud_value value;status=source?p->host.get(source,ref->row,ref->column,&value):UD_SCHEMA;
        if(!status)status=bind_value(&params,i,value);
    }
    uint64_t affected=0;
    if(!status && stmt)status=execute_bound(p,stmt,&params,&affected);
    if(!status && options->mode==UD_WRITE_CHANGESET && options->operations[first]!=UD_CHANGE_UPSERT && affected!=1)status=UD_FENCED;
    bindings_free(&params);if(stmt)mysql_stmt_close(stmt);p->host.statement_clear(&plan);
    return status;
}
static ud_status write_batch(void*state,ud_string tenant,const ud_write_options*o,const ud_batch*batch,const uint8_t digest[32]){
    mysql_state*p=state;if(p->cursor)return UD_BUSY;if(tenant.length>256||o->job.length>256||o->batch_id.length>256)return UD_RESOURCE;
    if(o->mode==UD_WRITE_CHANGESET&&p->transaction)return UD_UNSUPPORTED;
    ud_status s=check(p);int own=!p->transaction;
    /* Prove InnoDB before creating the receipt intent or mutating any business row. */
    if(!s)s=prove_target(p,o,batch);
    if(!s&&own)s=begin(p,UD_ISOLATION_SERIALIZABLE);
    ud_commit_receipt existing={0};if(!s)s=receipt(p,tenant,o->job,o->batch_id,&existing);
    if(!s&&existing.committed&&(memcmp(existing.digest,digest,32)||existing.source_position.length!=o->source_position.length||memcmp(existing.source_position.data,o->source_position.data,o->source_position.length)))s=UD_CORRUPT;
    if(!s&&!existing.committed){uint32_t rows=p->host.rows(batch),width=o->mode==UD_WRITE_DELETE?o->key_count:p->host.columns(batch);uint32_t chunk=width?65535/width:0;if(chunk>1024)chunk=1024;if(!chunk)s=UD_RESOURCE;
      for(uint32_t row=0;!s&&row<rows;){s=check(p);if(s)break;if(o->mode==UD_WRITE_CHANGESET){s=apply_statement(p,o,batch,row++,1);}else{uint32_t take=rows-row<chunk?rows-row:chunk;s=apply_statement(p,o,batch,row,take);row+=take;}}
      if(!s)s=receipt_insert(p,tenant,o,digest);}
    p->host.deallocate((void*)existing.source_position.data);
    if(own&&p->transaction){if(!s)s=commit(p);else{ud_status clean=rollback(p);if(clean)s=clean;}}
    return s;
}
static MYSQL *connect_session(mysql_state*p){
    MYSQL*db=mysql_init(NULL);if(!db)return NULL;
    unsigned int local=0,timeout=p->timeout;
    unsigned int protocol=MYSQL_PROTOCOL_TCP;mysql_options(db,MYSQL_OPT_PROTOCOL,&protocol);ud_mysql_bool reconnect=0;
    mysql_options(db,MYSQL_OPT_CONNECT_TIMEOUT,&timeout);mysql_options(db,MYSQL_OPT_READ_TIMEOUT,&timeout);mysql_options(db,MYSQL_OPT_WRITE_TIMEOUT,&timeout);
    mysql_options(db,MYSQL_OPT_LOCAL_INFILE,&local);mysql_options(db,MYSQL_OPT_RECONNECT,&reconnect);
#if defined(UD_MYSQL_CLIENT)
    unsigned int mode=SSL_MODE_VERIFY_IDENTITY;if(mysql_options(db,MYSQL_OPT_SSL_MODE,&mode)){mysql_close(db);return NULL;}
#else
    ud_mysql_bool yes=1;
    if(mysql_options(db,MYSQL_OPT_SSL_ENFORCE,&yes)||mysql_options(db,MYSQL_OPT_SSL_VERIFY_SERVER_CERT,&yes)){mysql_close(db);return NULL;}
#endif
    if(mysql_ssl_set(db,p->key,p->cert,p->ca,NULL,NULL)){mysql_close(db);return NULL;}
    if(!mysql_real_connect(db,p->host_name,p->user,p->password,p->database,p->port,NULL,CLIENT_FOUND_ROWS)){mysql_close(db);return NULL;}
    if(!mysql_get_ssl_cipher(db)){mysql_close(db);return NULL;}return db;
}
static ud_status cdc_close(void*state);
static ud_status binlog_open(mysql_state*p){mysql_cdc*c=p->cdc;
#if defined(UD_MYSQL_CLIENT)
    memset(&c->rpl,0,sizeof(c->rpl));c->rpl.file_name=c->file;c->rpl.file_name_length=strlen(c->file);c->rpl.start_position=c->position;c->rpl.server_id=c->rows;c->rpl.flags=1;
    if(mysql_binlog_open(c->stream,&c->rpl))return mysql_errno(c->stream)==1236?UD_FENCED:UD_IO;
#else
    if(c->rpl)mariadb_rpl_close(c->rpl);c->rpl=mariadb_rpl_init(c->stream);if(!c->rpl)return UD_RESOURCE;
    mariadb_rpl_optionsv(c->rpl,MARIADB_RPL_FILENAME,c->file,strlen(c->file));mariadb_rpl_optionsv(c->rpl,MARIADB_RPL_START,(unsigned long)c->position);mariadb_rpl_optionsv(c->rpl,MARIADB_RPL_SERVER_ID,c->rows);mariadb_rpl_optionsv(c->rpl,MARIADB_RPL_FLAGS,1u);mariadb_rpl_optionsv(c->rpl,MARIADB_RPL_VERIFY_CHECKSUM,1u);mariadb_rpl_optionsv(c->rpl,MARIADB_RPL_UNCOMPRESS,1u);
    if(mariadb_rpl_open(c->rpl))return c->rpl->error_no==1236?UD_FENCED:UD_IO;
#endif
    c->opened=1;return UD_OK;
}
static ud_status cdc_position(mysql_state*p,ud_string position){ud_json*json=NULL;ud_status status=p->host.json_parse(p->host.context,position.data,position.length,&json,NULL);const char*file=NULL,*number=NULL,*vendor=NULL;uint32_t n=0,k=0,v=0;if(!status)status=p->host.json_text(p->host.json_find(json,"file",4),&file,&n);if(!status)status=p->host.json_text(p->host.json_find(json,"position",8),&number,&k);if(!status)status=p->host.json_text(p->host.json_find(json,"vendor",6),&vendor,&v);
#if defined(UD_MYSQL_CLIENT)
    if(!status&&(v!=5||memcmp(vendor,"mysql",5)))status=UD_FENCED;
#else
    if(!status&&(v!=7||memcmp(vendor,"mariadb",7)))status=UD_FENCED;
#endif
    uint64_t value=0;for(uint32_t i=0;!status&&i<k;i++){if(number[i]<'0'||number[i]>'9'||value>(UINT64_MAX-(unsigned)(number[i]-'0'))/10)status=UD_CORRUPT;else value=value*10+(unsigned)(number[i]-'0');}
    for(uint32_t i=0;!status&&i<n;i++)if(!((file[i]>='a'&&file[i]<='z')||(file[i]>='A'&&file[i]<='Z')||(file[i]>='0'&&file[i]<='9')||file[i]=='.'||file[i]=='_'||file[i]=='-'))status=UD_SCHEMA;
    if(!status&&(!n||value<4))status=UD_FENCED;if(!status){p->cdc->file=plugin_string(&p->host,file,n);p->cdc->position=value;p->cdc->confirmed=value;if(!p->cdc->file)status=UD_RESOURCE;}p->host.json_destroy(json);return status;
}
static ud_status cdc_open(void*state,const ud_cdc_options*options){mysql_state*p=state;if(p->cdc||p->cursor||p->transaction||!options->server_id)return UD_BUSY;mysql_cdc*c=p->host.allocate(p->host.context,sizeof(*c));if(!c)return UD_RESOURCE;p->cdc=c;c->rows=options->server_id;
    c->stream=connect_session(p);ud_status status=c->stream?UD_OK:UD_AUTH;c->table=plugin_string(&p->host,options->table.data,options->table.length);if(!c->table)status=UD_RESOURCE;ud_field*fields=NULL;if(!status)status=plugin_fields(&p->host,options->schema,&fields);if(!status)status=p->host.batch_create(p->host.context,fields,p->host.columns(options->schema),0,&c->schema);p->host.deallocate(fields);
    if(!status)status=command(p,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    if(!status&&mysql_query(p->db,"SELECT @@global.binlog_format,@@global.binlog_row_image,@@global.log_bin"))status=failure(p,NULL);MYSQL_RES*probe=!status?mysql_store_result(p->db):NULL;MYSQL_ROW row=probe?mysql_fetch_row(probe):NULL;if(!status&&(!row||strcmp(row[0],"ROW")||strcmp(row[1],"FULL")||strcmp(row[2],"1")))status=UD_UNSUPPORTED;if(probe)mysql_free_result(probe);
    if(!status&&options->mode==UD_CDC_SNAPSHOT_STREAM){status=command(p,"FLUSH TABLES WITH READ LOCK");int locked=!status;if(!status)status=command(p,"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
#if defined(UD_MYSQL_CLIENT)
        const char*show="SHOW BINARY LOG STATUS";
#else
        const char*show="SHOW MASTER STATUS";
#endif
        if(!status&&mysql_query(p->db,show))status=failure(p,NULL);MYSQL_RES*result=!status?mysql_store_result(p->db):NULL;MYSQL_ROW record=result?mysql_fetch_row(result):NULL;if(!status&&(!record||!record[0]||!record[1]))status=UD_SCHEMA;
        if(!status){c->file=plugin_string(&p->host,record[0],(uint32_t)strlen(record[0]));c->position=0;for(const char*at=record[1];*at;at++){if(*at<'0'||*at>'9'||c->position>UINT64_MAX/10){status=UD_CORRUPT;break;}c->position=c->position*10+(unsigned)(*at-'0');}if(!c->file)status=UD_RESOURCE;}if(result)mysql_free_result(result);if(locked){ud_status released=command(p,"UNLOCK TABLES");if(!status)status=released;}
        if(!status){size_t size=options->table.length*2+64;char*query=p->host.allocate(p->host.context,size);if(!query)status=UD_RESOURCE;else{size_t n=0;memcpy(query,"SELECT * FROM `",15);n=15;for(uint32_t i=0;i<options->table.length;i++){char ch=options->table.data[i];if(ch=='.'){memcpy(query+n,"`.`",3);n+=3;}else{if(ch=='`')query[n++]='`';query[n++]=ch;}}query[n++]='`';query[n]=0;ud_read_options read={.struct_size=sizeof(read),.abi_version=UD_ABI_VERSION,.batch_size=options->batch_rows,.sql={query,(uint32_t)n}};status=read_open(p,&read,options->schema);p->host.deallocate(query);}}
        if(!status)c->snapshot=1;
    }else if(!status)status=cdc_position(p,options->position);
    if(!status)status=ud_binlog_create(&p->host,options,
#if defined(UD_MYSQL_CLIENT)
      0,
#else
      1,
#endif
      &c->decoder);
    if(status){cdc_close(p);return status;}return UD_OK;
}
static ud_status cdc_next(void*state,ud_cdc_event*out){mysql_state*p=state;mysql_cdc*c=p->cdc;if(!c)return UD_INVALID;memset(out,0,sizeof(*out));out->struct_size=sizeof(*out);out->abi_version=UD_ABI_VERSION;out->schema_version=1;ud_status status=check(p);if(status)return status;
    if(c->snapshot){if(!c->phase){c->phase=1;out->kind=UD_CDC_BEGIN;out->transaction_id=(ud_string){plugin_string(&p->host,"snapshot",8),8};return out->transaction_id.data?UD_OK:UD_RESOURCE;}status=read_next(p,&out->after);if(!status){uint32_t rows=p->host.rows(out->after);out->operations=p->host.allocate(p->host.context,rows*sizeof(*out->operations));if(!out->operations)return UD_RESOURCE;for(uint32_t i=0;i<rows;i++)out->operations[i]=UD_CHANGE_INSERT;out->kind=UD_CDC_ROWS;out->position=(ud_string){plugin_string(&p->host,"snapshot",8),8};return UD_OK;}if(status!=UD_END)return status;status=command(p,"COMMIT");if(status)return status;c->snapshot=0;out->kind=UD_CDC_COMMIT;char position[512];int n=snprintf(position,sizeof(position),"{\"file\":\"%s\",\"position\":%llu,\"gtid\":\"\",\"vendor\":\"%s\"}",c->file,(unsigned long long)c->position,
#if defined(UD_MYSQL_CLIENT)
      "mysql"
#else
      "mariadb"
#endif
      );out->position=(ud_string){plugin_string(&p->host,position,(uint32_t)n),(uint32_t)n};return out->position.data?UD_OK:UD_RESOURCE;}
    if(!c->opened){status=binlog_open(p);if(status)return status;}
    const unsigned char*raw=NULL;size_t length=0;
#if defined(UD_MYSQL_CLIENT)
    if(mysql_binlog_fetch(c->stream,&c->rpl)){return mysql_errno(c->stream)==1236?UD_FENCED:UD_IO;}if(!c->rpl.size){mysql_binlog_close(c->stream,&c->rpl);c->opened=0;return UD_BUSY;}raw=c->rpl.buffer;length=c->rpl.size;
#else
    if(c->event)mariadb_free_rpl_event(c->event);c->event=mariadb_rpl_fetch(c->rpl,NULL);if(!c->event){if(c->rpl->error_no)return c->rpl->error_no==1236?UD_FENCED:UD_IO;c->opened=0;return UD_BUSY;}raw=c->event->raw_data;length=c->event->raw_data_size;
#endif
    const unsigned char*event=raw;size_t size=length;if(size>=20&&event[0]==0){event++;size--;}
    if(size<19)return UD_CORRUPT;uint32_t kind=event[4];uint64_t next=0;for(uint32_t i=0;i<4;i++)next|=(uint64_t)event[13+i]<<(8*i);
    status=ud_binlog_decode(c->decoder,raw,length,(ud_string){c->file,(uint32_t)strlen(c->file)},out);if(status==UD_OK||status==UD_BUSY){if(next)c->position=next;}
    if(kind==4&&size>=27){uint64_t offset=0;for(uint32_t i=0;i<8;i++)offset|=(uint64_t)event[19+i]<<(8*i);size_t n=size-27;if(n>4)n-=4;char*file=plugin_string(&p->host,(const char*)event+27,(uint32_t)n);if(!file)return UD_RESOURCE;p->host.deallocate(c->file);c->file=file;c->position=offset;}
    return status;
}
static ud_status cdc_confirm(void*state,ud_string position){mysql_state*p=state;mysql_cdc*c=p->cdc;if(!c)return UD_INVALID;ud_json*document=NULL;ud_status status=p->host.json_parse(p->host.context,position.data,position.length,&document,NULL);const char*value;uint32_t n;if(!status)status=p->host.json_text(p->host.json_find(document,"position",8),&value,&n);uint64_t confirmed=0;for(uint32_t i=0;!status&&i<n;i++){if(value[i]<'0'||value[i]>'9'||confirmed>UINT64_MAX/10)status=UD_CORRUPT;else confirmed=confirmed*10+(unsigned)(value[i]-'0');}if(!status)c->confirmed=confirmed;p->host.json_destroy(document);return status;}
static ud_status cdc_close(void*state){mysql_state*p=state;mysql_cdc*c=p->cdc;if(!c)return UD_OK;read_close(p);
#if defined(UD_MYSQL_CLIENT)
    if(c->opened)mysql_binlog_close(c->stream,&c->rpl);
#else
    if(c->event)mariadb_free_rpl_event(c->event);if(c->rpl)mariadb_rpl_close(c->rpl);
#endif
    if(c->stream)mysql_close(c->stream);ud_binlog_destroy(c->decoder);p->host.batch_destroy(c->schema);p->host.deallocate(c->file);p->host.deallocate(c->table);p->host.deallocate(c->gtid);p->host.deallocate(c);p->cdc=NULL;return UD_OK;
}
static void cancel(void*state){mysql_state*p=state;atomic_store(&p->cancelled,1);p->broken=1;pthread_mutex_lock(&p->cancellation);if(!p->cancel_db)p->cancel_db=connect_session(p);if(p->cancel_db && p->db)mysql_kill(p->cancel_db,mysql_thread_id(p->db));if(p->cancel_db&&p->cdc&&p->cdc->stream)mysql_kill(p->cancel_db,mysql_thread_id(p->cdc->stream));pthread_mutex_unlock(&p->cancellation);}
static ud_status reset(void*state){mysql_state*p=state;if(p->broken||p->cursor||p->transaction)return UD_UNSUPPORTED;if(mysql_reset_connection(p->db))return failure(p,NULL);atomic_store(&p->cancelled,0);return session_policy(p);}
void ud_mysql_destroy(void*state){mysql_state*p=state;if(!p)return;cdc_close(p);read_close(p);if(p->db)mysql_close(p->db);if(p->cancel_db)mysql_close(p->cancel_db);pthread_mutex_destroy(&p->cancellation);char*strings[]={p->host_name,p->user,p->password,p->database,p->ca,p->cert,p->key};for(uint32_t i=0;i<7;i++)p->host.deallocate(strings[i]);p->host.deallocate(p);}
static ud_status config_string(mysql_state*p,const ud_json*j,const char*key,int required,char**out){const ud_json*v=p->host.json_find(j,key,(uint32_t)strlen(key));if(!v)return required?UD_SCHEMA:UD_OK;const char*s;uint32_t n;if(p->host.json_kind(v)!=UD_JSON_STRING)return UD_SCHEMA;ud_status status=p->host.json_text(v,&s,&n);if(!status){*out=plugin_string(&p->host,s,n);if(!*out)status=UD_RESOURCE;}return status;}
ud_status ud_mysql_initialize(const ud_host_api*h,const char*configuration,uint32_t length,void**out){
    *out=NULL;if(!h||h->struct_size!=sizeof(*h)||h->abi_version!=UD_PLUGIN_ABI)return UD_INCOMPATIBLE;
    mysql_state*p=h->allocate(h->context,sizeof(*p));if(!p)return UD_RESOURCE;p->host=*h;p->port=3306;p->timeout=30;p->check=h->check_io;p->check_user=h->control_user;atomic_init(&p->cancelled,0);pthread_mutex_init(&p->cancellation,NULL);
    ud_json*j=NULL;ud_status s=h->json_parse(h->context,configuration,length,&j,NULL);static const char*keys[]={"host","port","user","password","database","ca","cert","key","timestampTimezone","timeoutSeconds"};if(!s)s=h->json_keys(j,keys,10);
    const char*names[]={"host","user","password","database","ca","cert","key"};char**targets[]={&p->host_name,&p->user,&p->password,&p->database,&p->ca,&p->cert,&p->key};for(uint32_t i=0;!s&&i<7;i++)s=config_string(p,j,names[i],i<5,targets[i]);
    const char*numkeys[]={"port","timeoutSeconds"};for(uint32_t i=0;!s&&i<2;i++){const ud_json*v=h->json_find(j,numkeys[i],(uint32_t)strlen(numkeys[i]));if(v){const char*t;uint32_t n;s=h->json_kind(v)!=UD_JSON_NUMBER?UD_SCHEMA:h->json_text(v,&t,&n);unsigned long val=0;for(uint32_t k=0;!s&&k<n;k++){if(t[k]<'0'||t[k]>'9'||val>65535/10)s=UD_SCHEMA;else val=val*10+(unsigned)(t[k]-'0');}if(!s&&(!val||val>65535))s=UD_SCHEMA;if(!s){if(i)p->timeout=(uint32_t)val;else p->port=(uint32_t)val;}}}
    const ud_json*zone=!s?h->json_find(j,"timestampTimezone",17):NULL;if(!s&&zone){const char*t;uint32_t n;s=h->json_text(zone,&t,&n);if(!s&&(n!=3||memcmp(t,"UTC",3)))s=UD_SCHEMA;if(!s)p->utc=1;}
    if(!s){p->db=connect_session(p);if(!p->db)s=UD_AUTH;}
    if(!s)s=session_policy(p);h->json_destroy(j);if(s){ud_mysql_destroy(p);return s;}*out=p;return UD_OK;
}
const ud_connector_api ud_mysql_connector={.struct_size=sizeof(ud_connector_api),.abi_version=UD_PLUGIN_ABI,
 .capabilities=UD_CONNECT_READ|UD_CONNECT_APPEND|UD_CONNECT_UPSERT|UD_CONNECT_DELETE|UD_CONNECT_RECEIPTS|UD_CONNECT_EXECUTE_SQL|UD_CONNECT_TRANSACTION|UD_CONNECT_METADATA|UD_CONNECT_POOL|UD_CONNECT_CHANGESET|UD_CONNECT_CDC|UD_CONNECT_NATIVE_CDC|UD_CONNECT_PREPARED|UD_CONNECT_BATCH_WRITE|UD_CONNECT_PROVISION,
 .test=test,.discover=discover,.read_open=read_open,.read_next=read_next,.read_close=read_close,.write=write_batch,.receipt=receipt,.cancel=cancel,.execute=execute,.begin=begin,.commit=commit,.rollback=rollback,.metadata=metadata,.set_control=set_control,.reset_session=reset,.provision=provision,.metadata_dialect=UD_MYSQL_DIALECT,.cdc_open=cdc_open,.cdc_next=cdc_next,.cdc_confirm=cdc_confirm,.cdc_close=cdc_close};
