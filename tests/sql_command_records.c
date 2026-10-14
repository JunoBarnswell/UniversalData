#include "check.h"
#include "ud/package.h"
#include <sqlite3.h>
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
/* Actual provider state is inspected only by this fault-injection executable. */
typedef struct fixture { const char *database; sqlite3 *probe; int mode; } fixture;
static ud_status authorize(void *user,ud_string tenant,ud_string resource,int32_t action) {
    (void)user; (void)tenant; (void)resource; (void)action; return UD_OK;
}
static ud_status resolve(void *user,ud_context *ctx,ud_string tenant,ud_string resource,ud_string reference,ud_string *out) {
    (void)tenant; (void)resource; (void)reference;
    const char *database=((fixture *)user)->database;
    size_t length=strlen(database); char *copy=ud_alloc(ctx,length+1);
    if (!copy) return UD_RESOURCE;
    memcpy(copy,database,length+1); *out=(ud_string){copy,(uint32_t)length}; return UD_OK;
}
static int committed(fixture *f) {
    sqlite3_stmt *statement=NULL;
    int count=-1;
    if (sqlite3_prepare_v2(f->probe,"SELECT count(*) FROM commands",-1,&statement,NULL)==SQLITE_OK &&
        sqlite3_step(statement)==SQLITE_ROW) count=sqlite3_column_int(statement,0);
    sqlite3_finalize(statement); return count;
}
static void observe(void *user,const ud_execution_event *event) {
    fixture *f=user;
    if (!f->mode) return;
    if (f->mode==1 && event->kind==UD_EXEC_EVENT_BEFORE_CHECKPOINT && committed(f)==1) _exit(91);
    if (event->kind==UD_EXEC_EVENT_TARGET_COMMITTED && event->component_id.length==7 &&
        !memcmp(event->component_id.data,"command",7) &&
        ((f->mode==2 && committed(f)==1) || (f->mode==3 && committed(f)==3))) _exit(91);
    if (f->mode==4 && event->kind==UD_EXEC_EVENT_TARGET_COMMITTED && event->component_id.length==6 &&
        !memcmp(event->component_id.data,"reject",6)) _exit(91);
}
int main(int argc,char **argv) {
    CHECK(argc==8);
    fixture f={.database=argv[2],.mode=atoi(argv[5])};
    CHECK(sqlite3_open_v2(f.database,&f.probe,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK);
    FILE *file=fopen(argv[1],"rb"); CHECK(file && !fseek(file,0,SEEK_END));
    long length=ftell(file); CHECK(length>0 && !fseek(file,0,SEEK_SET));
    char *document=malloc((size_t)length); CHECK(document && fread(document,1,(size_t)length,file)==(size_t)length); fclose(file);
    ud_execution_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,
        .context={.struct_size=sizeof(ud_context_options),.abi_version=UD_ABI_VERSION,.memory_limit=67108864},
        .tenant={"fixture",7},.execution_id={"row-command",11},
        .state_directory={argv[3],(uint32_t)strlen(argv[3])},.resume=atoi(argv[4]),
        .max_batch_rows=8,.edge_capacity=1,.max_depth=32,.max_concurrency=(uint32_t)atoi(argv[7]),.timeout_ms=30000,
        .authorize=authorize,.resolve=resolve,.resolve_user=&f,.observe=observe,.observe_user=&f};
    ud_execution *execution=NULL;
    ud_status status=ud_execution_create_json((ud_string){document,(uint32_t)length},&options,&execution);
    free(document);
    if (!status) status=ud_execution_execute(execution);
    if (status!=atoi(argv[6])) fprintf(stderr,"actual=%s expected=%s\n",ud_status_name(status),ud_status_name(atoi(argv[6])));
    CHECK(status==atoi(argv[6]));
    if (execution) {
        ud_execution_metrics metrics={.struct_size=sizeof(metrics),.abi_version=UD_ABI_VERSION};
        OK(ud_execution_get_metrics(execution,&metrics));
        CHECK(metrics.state==(status==UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED : status ? UD_EXEC_FAILED : UD_EXEC_SUCCEEDED));
        printf("{\"result\":%d,\"state\":%d,\"errors\":%llu}\n",status,metrics.state,(unsigned long long)metrics.error_rows);
        OK(ud_execution_destroy(execution));
    }
    CHECK(sqlite3_close(f.probe)==SQLITE_OK); return 0;
}
