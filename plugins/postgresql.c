#include "connector_common.h"
#include <libpq-fe.h>
#include <stdatomic.h>
typedef struct {
    ud_host_api host;
    PGconn *db;
    PGcancel *cancel;
    ud_batch *schema;
    uint32_t batch_size;
    int reading, finished;
    atomic_int cancelled;
} postgres_state;
static ud_status pg_failure(postgres_state *p,const PGresult *result) {
    const char *code=result ? PQresultErrorField(result,PG_DIAG_SQLSTATE) : NULL;
    ud_status status=PQstatus(p->db)!=CONNECTION_OK ? UD_TRANSIENT : UD_IO;
    if(atomic_load(&p->cancelled)) status=UD_CANCELLED;
    else if(code) {
        if(!strcmp(code,"42501") || !strncmp(code,"28",2)) status=UD_AUTH;
        else if(!strcmp(code,"40001") || !strcmp(code,"40P01") || !strncmp(code,"08",2)) status=UD_TRANSIENT;
        else if(!strcmp(code,"22003")) status=UD_OVERFLOW;
        else if(!strncmp(code,"22",2)) status=UD_PARSE;
        else if(!strncmp(code,"23",2) || !strncmp(code,"42",2)) status=UD_SCHEMA;
        else if(!strcmp(code,"57014")) status=UD_CANCELLED;
    }
    /* Database diagnostics can echo literal data / credentials; expose classification only. */
    return p->host.report(p->host.context,status,code ? code : "PostgreSQL connection / protocol failure");
}
static ud_status command(postgres_state *p,const char *sql) {
    PGresult *r=PQexecParams(p->db,sql,0,NULL,NULL,NULL,NULL,0);
    ud_status s=r && PQresultStatus(r)==PGRES_COMMAND_OK ? UD_OK : pg_failure(p,r);
    PQclear(r);return s;
}
static ud_status initialize(const ud_host_api *host,const char *config,uint32_t length,void **out) {
    *out=NULL;postgres_state *p=host->allocate(host->context,sizeof(*p));
    if(!p) return UD_RESOURCE;
    p->host=*host;atomic_init(&p->cancelled,0);
    char *copy=plugin_string(host,config,length);
    if(!copy) { host->deallocate(p);return UD_INVALID; }
    const char *keys[]={"dbname","connect_timeout","application_name",NULL};
    const char *values[]={copy,"10","UniversalDataSDK/1.0.0",NULL};
    p->db=PQconnectdbParams(keys,values,1);host->deallocate(copy);
    ud_status s=!p->db ? UD_RESOURCE : PQstatus(p->db)!=CONNECTION_OK ? UD_IO : UD_OK;
    if(!s && PQsetClientEncoding(p->db,"UTF8")) s=UD_SCHEMA;
    if(!s) s=command(p,"SET TIME ZONE 'UTC'");
    if(!s) s=command(p,"SET DateStyle TO ISO, YMD");
    if(!s) s=command(p,"SET extra_float_digits TO 3");
    if(!s) { p->cancel=PQgetCancel(p->db);if(!p->cancel) s=UD_RESOURCE; }
    if(s) {
        if(p->db) PQfinish(p->db);
        host->deallocate(p);
        return host->report(host->context,s,"PostgreSQL initialization failed; connection string is redacted");
    }
    *out=p;return UD_OK;
}
static ud_status test_connection(void *state) {
    postgres_state *p=state;PGresult *r=PQexecParams(p->db,"SELECT 1",0,NULL,NULL,NULL,NULL,0);
    ud_status s=r && PQresultStatus(r)==PGRES_TUPLES_OK ? UD_OK : pg_failure(p,r);
    PQclear(r);return s;
}
static ud_status discover(void *state,ud_string table,ud_batch **out) {
    postgres_state *p=state;ud_host_api *h=&p->host;char *quoted=plugin_identifier(h,table);
    if(!quoted) return UD_RESOURCE;
    const char *args[]={quoted};
    PGresult *r=PQexecParams(p->db,
        "SELECT a.attname,format_type(a.atttypid,a.atttypmod),NOT a.attnotnull,"
        "COALESCE(array_position(i.indkey::smallint[],a.attnum)-array_lower(i.indkey::smallint[],1)+1,0),"
        "has_column_privilege(a.attrelid,a.attname,'SELECT'),has_column_privilege(a.attrelid,a.attname,'INSERT'),"
        "has_column_privilege(a.attrelid,a.attname,'UPDATE'),has_table_privilege(a.attrelid,'DELETE') "
        "FROM pg_attribute a LEFT JOIN pg_index i ON i.indrelid=a.attrelid AND i.indisprimary "
        "WHERE a.attrelid=$1::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
        1,NULL,args,NULL,NULL,0);
    h->deallocate(quoted);
    if(!r || PQresultStatus(r)!=PGRES_TUPLES_OK) { ud_status s=pg_failure(p,r);PQclear(r);return s; }
    int count=PQntuples(r);
    if(count<1 || count>65536) { PQclear(r);return UD_SCHEMA; }
    ud_field fields[]={{.name="name",.name_length=4,.type=UD_UTF8},
        {.name="declared_type",.name_length=13,.type=UD_UTF8},
        {.name="nullable",.name_length=8,.type=UD_BOOL},
        {.name="key_order",.name_length=9,.type=UD_INT64},
        {.name="readable",.name_length=8,.type=UD_BOOL},
        {.name="appendable",.name_length=10,.type=UD_BOOL},
        {.name="updatable",.name_length=9,.type=UD_BOOL},
        {.name="deletable",.name_length=9,.type=UD_BOOL}};
    ud_status s=h->batch_create(h->context,fields,8,(uint32_t)count,out);
    for(int row=0;!s && row<count;row++) {
        ud_value v[8]={{.type=UD_UTF8},{.type=UD_UTF8},{.type=UD_BOOL},{.type=UD_INT64},
            {.type=UD_BOOL},{.type=UD_BOOL},{.type=UD_BOOL},{.type=UD_BOOL}};
        for(int j=0;j<2;j++) { v[j].text=PQgetvalue(r,row,j);v[j].text_length=(uint32_t)PQgetlength(r,row,j); }
        v[2].integer=!strcmp(PQgetvalue(r,row,2),"t");
        v[3].integer=strtoll(PQgetvalue(r,row,3),NULL,10);
        for(int j=4;j<8;j++) v[j].integer=!strcmp(PQgetvalue(r,row,j),"t");
        s=h->append(*out,v,8);
    }
    PQclear(r);return s;
}
static ud_status close_read(void *state) {
    postgres_state *p=state;ud_status s=p->reading ? command(p,"ROLLBACK") : UD_OK;
    p->reading=0;p->finished=1;p->host.batch_destroy(p->schema);p->schema=NULL;
    return s;
}
static int compatible_oid(ud_type t,Oid oid) {
    return t==UD_BOOL ? oid==16 : t==UD_INT64 ? oid==20 || oid==21 || oid==23 :
        t==UD_FLOAT64 ? oid==701 : t==UD_UTF8 ? oid==25 || oid==1043 :
        t==UD_DECIMAL128 ? oid==1700 : t==UD_DATE32 ? oid==1082 :
        t==UD_TIME64 ? oid==1083 : t==UD_TIMESTAMP ? oid==1184 : t==UD_BINARY ? oid==17 : 0;
}
static ud_status open_read(void *state,const ud_read_options *options,const ud_batch *schema) {
    postgres_state *p=state;ud_host_api *h=&p->host;
    if(p->reading || options->sql.length>SIZE_MAX-64) return UD_INVALID;
    atomic_store(&p->cancelled,0);
    ud_status s=command(p,"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    if(s) return s;
    p->reading=1;
    size_t length=(size_t)options->sql.length+64;
    char *sql=h->allocate(h->context,length);
    if(!sql) s=UD_RESOURCE;
    else {
        const char *prefix="DECLARE ud_sdk_cursor NO SCROLL CURSOR FOR ";size_t n=strlen(prefix);
        memcpy(sql,prefix,n);memcpy(sql+n,options->sql.data,options->sql.length);
        s=command(p,sql);h->deallocate(sql);
    }
    PGresult *r=!s ? PQexecParams(p->db,"FETCH FORWARD 0 FROM ud_sdk_cursor",0,NULL,NULL,NULL,NULL,0) : NULL;
    if(!s && (!r || PQresultStatus(r)!=PGRES_TUPLES_OK)) s=pg_failure(p,r);
    uint32_t columns=h->columns(schema);ud_field *fields=NULL;
    if(!s && PQnfields(r)!=(int)columns) s=UD_SCHEMA;
    if(!s) s=plugin_fields(h,schema,&fields);
    for(uint32_t j=0;!s && j<columns;j++)
        if(strcmp(fields[j].name,PQfname(r,(int)j)) || !compatible_oid(fields[j].type,PQftype(r,(int)j))) s=UD_SCHEMA;
    if(!s) s=h->batch_create(h->context,fields,columns,0,&p->schema);
    h->deallocate(fields);PQclear(r);
    if(s) { close_read(p);return s; }
    p->batch_size=options->batch_size;p->finished=0;return UD_OK;
}
static ud_status parse_row(postgres_state *p,const PGresult *r,ud_batch *batch) {
    ud_host_api *h=&p->host;uint32_t columns=h->columns(p->schema);
    ud_value *values=h->allocate(h->context,columns*sizeof(*values));
    void **binary=h->allocate(h->context,columns*sizeof(*binary));
    if(!values || !binary) { h->deallocate(values);h->deallocate(binary);return UD_RESOURCE; }
    ud_status s=PQnfields(r)!=(int)columns ? UD_SCHEMA : UD_OK;
    for(uint32_t j=0;!s && j<columns;j++) {
        ud_field field;h->field(p->schema,j,&field);
        if(strcmp(field.name,PQfname(r,(int)j)) || !compatible_oid(field.type,PQftype(r,(int)j))) { s=UD_SCHEMA;break; }
        ud_value *v=values+j;v->type=field.type;v->precision=field.precision;v->scale=field.scale;
        if(PQgetisnull(r,0,(int)j)) { v->is_null=1;continue; }
        const char *text=PQgetvalue(r,0,(int)j);uint32_t length=(uint32_t)PQgetlength(r,0,(int)j);
        if(field.type==UD_BOOL) {
            if(length!=1 || (*text!='t' && *text!='f')) s=UD_PARSE;
            else v->integer=*text=='t';
        } else if(field.type==UD_BINARY) {
            size_t size=0;binary[j]=PQunescapeBytea((const unsigned char *)text,&size);
            if(!binary[j] || size>UINT32_MAX) s=UD_RESOURCE;
            else { v->text=binary[j];v->text_length=(uint32_t)size; }
        } else if(field.type==UD_TIMESTAMP) {
            /* UTC session formatting has a +00 suffix; normalize only that proven offset. */
            char utc[64];
            if(length<22 || length>sizeof(utc)-1 || text[10]!=' ' || memcmp(text+length-3,"+00",3)) s=UD_PARSE;
            else {
                memcpy(utc,text,length-3);utc[10]='T';utc[length-3]='Z';
                s=h->parse_field(h->context,&field,utc,length-2,UD_ROUND_EXACT,v);
            }
        } else s=h->parse_field(h->context,&field,text,length,UD_ROUND_EXACT,v);
    }
    if(!s) s=h->append(batch,values,columns);
    for(uint32_t j=0;j<columns;j++) if(binary[j]) PQfreemem(binary[j]);
    h->deallocate(values);h->deallocate(binary);return s;
}
static ud_status next_read(void *state,ud_batch **out) {
    postgres_state *p=state;ud_host_api *h=&p->host;*out=NULL;
    if(!p->reading || !p->schema) return UD_INVALID;
    if(p->finished) return UD_END;
    if(atomic_load(&p->cancelled) || h->cancelled(h->context)) return UD_CANCELLED;
    ud_field *fields=NULL;ud_status s=plugin_fields(h,p->schema,&fields);
    if(!s) s=h->batch_create(h->context,fields,h->columns(p->schema),p->batch_size,out);
    h->deallocate(fields);if(s) return s;
    char sql[96];snprintf(sql,sizeof(sql),"FETCH FORWARD %u FROM ud_sdk_cursor",p->batch_size);
    if(!PQsendQueryParams(p->db,sql,0,NULL,NULL,NULL,NULL,0)) s=pg_failure(p,NULL);
    else if(!PQsetSingleRowMode(p->db)) s=UD_INCOMPATIBLE;
    PGresult *r;
    while((r=PQgetResult(p->db))!=NULL) {
        ExecStatusType result=PQresultStatus(r);
        if(!s && (atomic_load(&p->cancelled) || h->cancelled(h->context))) s=UD_CANCELLED;
        if(!s && result==PGRES_SINGLE_TUPLE) s=parse_row(p,r,*out);
        else if(!s && result!=PGRES_TUPLES_OK) s=pg_failure(p,r);
        PQclear(r);
    }
    if(s) return s;
    uint32_t rows=h->rows(*out);
    if(rows<p->batch_size) p->finished=1;
    if(!rows) { h->batch_destroy(*out);*out=NULL;return UD_END; }
    return UD_OK;
}
static void cancel_operation(void *state) {
    postgres_state *p=state;atomic_store(&p->cancelled,1);
    char error[256];if(p->cancel) PQcancel(p->cancel,error,sizeof(error));
}
static ud_status receipt(void *state,ud_string tenant,ud_string job,ud_string batch_id,ud_commit_receipt *out) {
    postgres_state *p=state;ud_host_api *h=&p->host;memset(out,0,sizeof(*out));
    ud_string strings[]={tenant,job,batch_id};char *owned[3]={0};ud_status s=UD_OK;
    for(int j=0;!s && j<3;j++) { owned[j]=plugin_string(h,strings[j].data,strings[j].length);if(!owned[j]) s=UD_RESOURCE; }
    const char *args[]={owned[0],owned[1],owned[2]};
    PGresult *r=!s ? PQexecParams(p->db,"SELECT encode(digest,'hex'),source_position FROM ud_sdk_receipts_v2 WHERE tenant=$1 AND job=$2 AND batch_id=$3",3,NULL,args,NULL,NULL,0) : NULL;
    if(!s && (!r || PQresultStatus(r)!=PGRES_TUPLES_OK)) s=h->report(h->context,UD_COMMIT_UNKNOWN,"receipt store is unavailable; source position must not advance");
    if(!s && PQntuples(r)>1) s=UD_CORRUPT;
    if(!s && PQntuples(r)==1) {
        const char *hex=PQgetvalue(r,0,0);
        if(PQgetisnull(r,0,0) || PQgetlength(r,0,0)!=64 || PQgetisnull(r,0,1)) s=UD_CORRUPT;
        else {
            for(int j=0;!s && j<32;j++) {
                unsigned value=0;
                for(int k=0;k<2;k++) {
                    unsigned char ch=(unsigned char)hex[j*2+k];
                    if(ch>='0' && ch<='9') value=value*16+ch-'0';
                    else if(ch>='a' && ch<='f') value=value*16+ch-'a'+10;
                    else s=UD_CORRUPT;
                }
                out->digest[j]=(uint8_t)value;
            }
            uint32_t length=(uint32_t)PQgetlength(r,0,1);char *position=!s ? plugin_string(h,PQgetvalue(r,0,1),length) : NULL;
            if(!s && !position) s=UD_RESOURCE;
            if(!s) { out->source_position=(ud_string){position,length};out->committed=1; }
        }
    }
    PQclear(r);for(int j=0;j<3;j++) h->deallocate(owned[j]);return s;
}
static ud_status validate_target(postgres_state *p,ud_string table,const ud_batch *batch) {
    ud_host_api *h=&p->host;char *name=plugin_identifier(h,table);
    if(!name) return UD_RESOURCE;
    const char *args[]={name};
    PGresult *r=PQexecParams(p->db,"SELECT attname,atttypid::text,atttypmod::text FROM pg_attribute WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped",1,NULL,args,NULL,NULL,0);
    h->deallocate(name);
    if(!r || PQresultStatus(r)!=PGRES_TUPLES_OK) { ud_status s=pg_failure(p,r);PQclear(r);return s; }
    ud_status s=UD_OK;
    for(uint32_t j=0;!s && j<h->columns(batch);j++) {
        ud_field field;h->field(batch,j,&field);int found=0;
        for(int k=0;k<PQntuples(r);k++) if(!strcmp(field.name,PQgetvalue(r,k,0))) {
            found=1;Oid oid=(Oid)strtoul(PQgetvalue(r,k,1),NULL,10);int modifier=(int)strtol(PQgetvalue(r,k,2),NULL,10);
            if(!compatible_oid(field.type,oid)) s=UD_SCHEMA;
            if(!s && field.type==UD_DECIMAL128 && modifier>=4) {
                int precision=((modifier-4)>>16)&65535;int scale=(((modifier-4)&2047)^1024)-1024;
                if(precision!=field.precision || scale!=field.scale) s=UD_SCHEMA;
            }
            if(!s && field.type==UD_UTF8 && oid==1043 && modifier>=4) {
                uint32_t limit=(uint32_t)(modifier-4);
                for(uint32_t row=0;!s && row<h->rows(batch);row++) {
                    ud_value value;h->get(batch,row,j,&value);uint32_t characters=0;
                    if(!value.is_null) for(uint32_t b=0;b<value.text_length;b++) if(((unsigned char)value.text[b]&192)!=128) characters++;
                    if(characters>limit) s=UD_SCHEMA;
                }
            }
        }
        if(!found) s=UD_SCHEMA;
    }
    PQclear(r);
    return s ? h->report(h->context,s,"target schema would narrow or coerce a typed value") : UD_OK;
}
static ud_status write_batch(void *state,ud_string tenant,const ud_write_options *options,const ud_batch *batch,const uint8_t digest[32]) {
    postgres_state *p=state;ud_host_api *h=&p->host;
    if(p->reading) return UD_BUSY;
    if(h->columns(batch)>65535) return UD_RESOURCE;
    atomic_store(&p->cancelled,0);
    char *sql=NULL;ud_status s=connector_write_sql(h,options,batch,1,&sql);
    if(s) return s;
    s=command(p,"BEGIN");
    if(s) { h->deallocate(sql);return s; }
    if(!s) s=command(p,"CREATE TABLE IF NOT EXISTS ud_sdk_receipts_v2(tenant TEXT NOT NULL,job TEXT NOT NULL,batch_id TEXT NOT NULL,digest BYTEA NOT NULL CHECK(octet_length(digest)=32),source_position TEXT NOT NULL,PRIMARY KEY(tenant,job,batch_id))");
    char *table=plugin_identifier(h,options->table);
    if(!s && !table) s=UD_RESOURCE;
    if(!s) {
        size_t size=strlen(table)+64;char *lock=h->allocate(h->context,size);
        if(!lock) s=UD_RESOURCE;
        else { snprintf(lock,size,"LOCK TABLE %s IN ROW EXCLUSIVE MODE",table);s=command(p,lock);h->deallocate(lock); }
    }
    h->deallocate(table);
    if(!s) s=validate_target(p,options->table,batch);
    ud_string strings[]={tenant,options->job,options->batch_id,options->source_position};char *identity[4]={0};
    for(int j=0;!s && j<4;j++) { identity[j]=plugin_string(h,strings[j].data?strings[j].data:"",strings[j].length);if(!identity[j]) s=UD_RESOURCE; }
    const char *reserve[]={identity[0],identity[1],identity[2],(const char *)digest,identity[3]};
    Oid reserve_types[]={25,25,25,17,25};int reserve_lengths[]={0,0,0,32,0},reserve_formats[]={0,0,0,1,0};
    PGresult *result=!s ? PQexecParams(p->db,"INSERT INTO ud_sdk_receipts_v2(tenant,job,batch_id,digest,source_position) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING batch_id",5,reserve_types,reserve,reserve_lengths,reserve_formats,0) : NULL;
    if(!s && (!result || PQresultStatus(result)!=PGRES_TUPLES_OK)) s=pg_failure(p,result);
    int existing=!s && PQntuples(result)==0;PQclear(result);
    if(!s && existing) {
        ud_commit_receipt previous={0};s=receipt(p,tenant,options->job,options->batch_id,&previous);
        if(!s && (!previous.committed || memcmp(previous.digest,digest,32))) s=UD_CORRUPT;
        h->deallocate((void *)previous.source_position.data);
    }
    uint32_t columns=options->mode==UD_WRITE_DELETE ? options->key_count : h->columns(batch);
    const char **parameters=h->allocate(h->context,columns*sizeof(*parameters));
    char **owned=h->allocate(h->context,columns*sizeof(*owned));
    Oid *types=h->allocate(h->context,columns*sizeof(*types));
    int *lengths=h->allocate(h->context,columns*sizeof(*lengths)),*formats=h->allocate(h->context,columns*sizeof(*formats));
    if(!s && (!parameters || !owned || !types || !lengths || !formats)) s=UD_RESOURCE;
    for(uint32_t row=0;!s && !existing && row<h->rows(batch);row++) {
        if(atomic_load(&p->cancelled) || h->cancelled(h->context)) { s=UD_CANCELLED;break; }
        memset(types,0,columns*sizeof(*types));memset(formats,0,columns*sizeof(*formats));memset(lengths,0,columns*sizeof(*lengths));
        for(uint32_t k=0;!s && k<options->key_count;k++) {
            ud_value key;h->get(batch,row,options->keys[k],&key);if(key.is_null && options->mode!=UD_WRITE_APPEND) s=UD_SCHEMA;
        }
        for(uint32_t j=0;!s && j<columns;j++) {
            ud_value value;h->get(batch,row,options->mode==UD_WRITE_DELETE ? options->keys[j] : j,&value);parameters[j]=NULL;
            if(value.is_null) continue;
            if(value.type==UD_BINARY) {
                if(value.text_length>INT_MAX) { s=UD_OVERFLOW;break; }
                parameters[j]=value.text?value.text:"";types[j]=17;formats[j]=1;lengths[j]=(int)value.text_length;
            } else if(value.type==UD_UTF8) {
                if(value.text_length && memchr(value.text,0,value.text_length)) { s=UD_PARSE;break; }
                owned[j]=plugin_string(h,value.text,value.text_length);
                if(!owned[j]) s=UD_RESOURCE;else parameters[j]=owned[j];
            } else {
                char number[160];uint32_t required;
                s=h->format_value(&value,number,sizeof(number),&required);
                if(!s) { owned[j]=plugin_string(h,number,required-1);if(!owned[j]) s=UD_RESOURCE;else parameters[j]=owned[j]; }
            }
        }
        result=!s ? PQexecParams(p->db,sql,(int)columns,types,parameters,lengths,formats,0) : NULL;
        if(!s && (!result || PQresultStatus(result)!=PGRES_COMMAND_OK)) s=pg_failure(p,result);
        PQclear(result);
        for(uint32_t j=0;j<columns;j++) { h->deallocate(owned[j]);owned[j]=NULL; }
    }
    h->deallocate(parameters);h->deallocate(owned);h->deallocate(types);h->deallocate(lengths);h->deallocate(formats);
    for(int j=0;j<4;j++) h->deallocate(identity[j]);h->deallocate(sql);
    if(!s && (h->cancelled(h->context) || atomic_load(&p->cancelled))) s=UD_CANCELLED;
    if(!s) {
        s=command(p,"COMMIT");
        if(s) { command(p,"ROLLBACK");return h->report(h->context,UD_COMMIT_UNKNOWN,"commit outcome requires receipt verification before advancing source position"); }
    } else if(command(p,"ROLLBACK")!=UD_OK) return h->report(h->context,UD_COMMIT_UNKNOWN,"rollback outcome is unknown; receipt verification is required");
    return s;
}
static ud_status execute(void *state,int32_t operation,const ud_batch *input,const char *argument,uint32_t length,ud_batch **out) {
    (void)state;(void)operation;(void)input;(void)argument;(void)length;*out=NULL;return UD_UNSUPPORTED;
}
static void destroy(void *state) {
    postgres_state *p=state;if(!p) return;
    p->host.batch_destroy(p->schema);if(p->cancel) PQfreeCancel(p->cancel);PQfinish(p->db);p->host.deallocate(p);
}
static ud_status execute_sql(void *state,ud_string statement) {
    postgres_state *p=state;char *sql=plugin_string(&p->host,statement.data,statement.length);
    if(!sql) return UD_RESOURCE;ud_status s=command(p,sql);p->host.deallocate(sql);return s;
}
static const ud_connector_api connector={sizeof(connector),UD_PLUGIN_ABI,
    UD_CONNECT_READ|UD_CONNECT_APPEND|UD_CONNECT_UPSERT|UD_CONNECT_DELETE|UD_CONNECT_RECEIPTS|UD_CONNECT_SNAPSHOT|UD_CONNECT_EXECUTE_SQL,
    test_connection,discover,open_read,next_read,close_read,write_batch,receipt,cancel_operation,execute_sql};
UD_PLUGIN_EXPORT ud_status ud_plugin_get_api(uint32_t version,ud_plugin_api *out) {
    if(version!=UD_PLUGIN_ABI || !out || out->struct_size!=sizeof(*out)) return UD_INCOMPATIBLE;
    *out=(ud_plugin_api){sizeof(*out),UD_PLUGIN_ABI,"org.universaldata.connector.postgresql","1.0.0",
        UD_PLUGIN_SOURCE|UD_PLUGIN_SINK,UD_CAP_TRANSACTION,initialize,execute,destroy,plugin_shutdown,&connector,NULL};
    return UD_OK;
}
