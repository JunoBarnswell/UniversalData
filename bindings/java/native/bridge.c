#include <jni.h>
#include "ud/etl.h"
#include "ud/format.h"
#include "ud/dag.h"
#include "ud/run.h"
#include <stdlib.h>
#include <string.h>
typedef struct {
    ud_context *ctx;
    ud_job *job;
    char *state;
} java_job;
static jstring utf8(JNIEnv *e, const char *text) {
    size_t n = strlen(text);
    if (n > INT32_MAX)
        return NULL;
    jbyteArray bytes = (*e)->NewByteArray(e, (jsize)n);
    if (!bytes)
        return NULL;
    (*e)->SetByteArrayRegion(e, bytes, 0, (jsize)n, (const jbyte *)text);
    jclass cls = (*e)->FindClass(e, "java/lang/String");
    if (!cls)
        return NULL;
    jmethodID ctor = (*e)->GetMethodID(e, cls, "<init>", "([BLjava/lang/String;)V");
    if (!ctor)
        return NULL;
    jstring encoding = (*e)->NewStringUTF(e, "UTF-8");
    if (!encoding)
        return NULL;
    return (jstring)(*e)->NewObject(e, cls, ctor, bytes, encoding);
}
static void fail(JNIEnv *e, int status, const char *message) {
    if ((*e)->ExceptionCheck(e))
        return;
    jclass cls = (*e)->FindClass(e, "org/universaldata/UniversalDataException");
    if (!cls)
        return;
    jmethodID ctor = (*e)->GetMethodID(e, cls, "<init>", "(ILjava/lang/String;)V");
    if (!ctor)
        return;
    jstring text = utf8(e, message);
    if (!text)
        return;
    jthrowable error = (*e)->NewObject(e, cls, ctor, (jint)status, text);
    if (error)
        (*e)->Throw(e, error);
}
static char *copy(JNIEnv *e, jbyteArray array, uint32_t *length) {
    *length = 0;
    if (!array)
        return NULL;
    jsize n = (*e)->GetArrayLength(e, array);
    char *p = calloc((size_t)n + 1, 1);
    if (!p)
        return NULL;
    (*e)->GetByteArrayRegion(e, array, 0, n, (jbyte *)p);
    if ((*e)->ExceptionCheck(e) || memchr(p, 0, (size_t)n)) {
        free(p);
        return NULL;
    }
    *length = (uint32_t)n;
    return p;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_CsvJob_nativeOpen(JNIEnv *e, jclass cls,
                                                                 jbyteArray input, jbyteArray state,
                                                                 jbyteArray schema,
                                                                 jbyteArray commands, jlong limit) {
    (void)cls;
    java_job *j = calloc(1, sizeof(*j));
    if (!j) {
        fail(e, UD_RESOURCE, "JNI allocation failed");
        return 0;
    }
    uint32_t in = 0, st = 0, sn = 0, cn = 0;
    char *i = copy(e, input, &in), *s = copy(e, state, &st), *sc = copy(e, schema, &sn),
         *co = copy(e, commands, &cn);
    ud_status status = UD_INVALID;
    if (i && s && sc && co && limit > 0) {
        ud_context_options opt = {sizeof(opt), UD_ABI_VERSION, (uint64_t)limit, NULL, NULL, NULL};
        status = ud_context_create(&opt, &j->ctx);
        if (!status) {
            ud_job_options o = {0};
            o.struct_size = sizeof(o);
            o.abi_version = UD_ABI_VERSION;
            o.input_directory = i;
            o.state_directory = s;
            o.schema = sc;
            o.schema_length = sn;
            o.commands = co;
            o.commands_length = cn;
            status = ud_job_create(j->ctx, &o, &j->job);
        }
    }
    free(i);
    free(sc);
    free(co);
    if (status) {
        fail(e, status, j->ctx ? ud_error(j->ctx) : "Invalid JNI configuration");
        ud_job_destroy(j->job);
        ud_context_destroy(j->ctx);
        free(s);
        free(j);
        return 0;
    }
    j->state = s;
    return (jlong)(intptr_t)j;
}
JNIEXPORT void JNICALL Java_org_universaldata_CsvJob_nativePoll(JNIEnv *e, jclass cls, jlong handle,
                                                                jboolean watch) {
    (void)cls;
    java_job *j = (java_job *)(intptr_t)handle;
    ud_status s = watch ? ud_job_run(j->job) : ud_job_poll(j->job);
    if (s != UD_OK && s != UD_CANCELLED)
        fail(e, s, ud_error(j->ctx));
}
JNIEXPORT void JNICALL Java_org_universaldata_CsvJob_nativeCancel(JNIEnv *e, jclass cls,
                                                                  jlong handle) {
    (void)e;
    (void)cls;
    java_job *j = (java_job *)(intptr_t)handle;
    ud_cancel(j->ctx);
}
JNIEXPORT jstring JNICALL Java_org_universaldata_CsvJob_nativeSnapshot(JNIEnv *e, jclass cls,
                                                                       jlong handle) {
    (void)cls;
    java_job *j = (java_job *)(intptr_t)handle;
    char *path = NULL;
    uint64_t gen;
    ud_status s = ud_snapshot_latest(j->ctx, j->state, &path, &gen);
    if (s == UD_END)
        return NULL;
    if (s) {
        fail(e, s, ud_error(j->ctx));
        return NULL;
    }
    jstring text = utf8(e, path);
    ud_free(path);
    return text;
}
JNIEXPORT void JNICALL Java_org_universaldata_CsvJob_nativeClose(JNIEnv *e, jclass cls,
                                                                 jlong handle) {
    (void)cls;
    java_job *j = (java_job *)(intptr_t)handle;
    ud_job_destroy(j->job);
    ud_status s = ud_context_destroy(j->ctx);
    free(j->state);
    free(j);
    if (s)
        fail(e, s, "JNI context still has live resources");
}
typedef struct java_data { ud_context *ctx;ud_batch *batch;ud_dag *dag; } java_data;
static java_data *data_create(JNIEnv *e,jlong limit) {
    if(limit<0) { fail(e,UD_INVALID,"Invalid memory limit");return NULL; }
    ud_context *ctx=NULL;ud_context_options opts={sizeof(opts),UD_ABI_VERSION,(uint64_t)limit,NULL,NULL,NULL};
    ud_status status=ud_context_create(&opts,&ctx);
    if(status) { fail(e,status,ud_status_name(status));return NULL; }
    java_data *d=ud_alloc(ctx,sizeof(*d));
    if(!d) { ud_context_destroy(ctx);fail(e,UD_RESOURCE,"JNI memory quota exceeded");return NULL; }
    d->ctx=ctx;return d;
}
static ud_status data_destroy(java_data *d) {
    if(!d) return UD_OK;ud_context *ctx=d->ctx;ud_batch_destroy(d->batch);ud_dag_destroy(d->dag);ud_free(d);return ud_context_destroy(ctx);
}
static char *copy_data(JNIEnv *e,ud_context *ctx,jbyteArray array,uint32_t *length) {
    *length=0;if(!array) return NULL;jsize n=(*e)->GetArrayLength(e,array);
    char *data=ud_alloc(ctx,(size_t)n+1);if(!data) { fail(e,UD_RESOURCE,"JNI input allocation failed");return NULL; }
    (*e)->GetByteArrayRegion(e,array,0,n,(jbyte *)data);if((*e)->ExceptionCheck(e)) { ud_free(data);return NULL; }
    *length=(uint32_t)n;return data;
}
static jlong data_result(JNIEnv *e,java_data *d,ud_status s) {
    if(s) { fail(e,s,ud_status_name(s));data_destroy(d);return 0; }return (jlong)(intptr_t)d;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Batch_nativeOpen(JNIEnv *e,jclass cls,jbyteArray schema,jbyteArray rows,jint rounding,jlong limit) {
    (void)cls;java_data *d=data_create(e,limit);if(!d) return 0;uint32_t sn=0,rn=0;
    char *sc=copy_data(e,d->ctx,schema,&sn),*data=copy_data(e,d->ctx,rows,&rn);
    ud_status s=sc && data?ud_batch_from_json_schema(d->ctx,sc,sn,data,rn,rounding,&d->batch):UD_INVALID;
    ud_free(sc);ud_free(data);return data_result(e,d,s);
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Batch_nativeFromWire(JNIEnv *e,jclass cls,jbyteArray wire,jlong limit) {
    (void)cls;java_data *d=data_create(e,limit);if(!d) return 0;uint32_t n=0;char *data=copy_data(e,d->ctx,wire,&n);
    ud_status s=data?ud_batch_deserialize(d->ctx,(const uint8_t *)data,n,&d->batch):UD_INVALID;ud_free(data);return data_result(e,d,s);
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Batch_nativeCount(JNIEnv *e,jclass cls,jlong handle,jboolean columns) {
    (void)e;(void)cls;java_data *d=(java_data *)(intptr_t)handle;return columns?ud_batch_columns(d->batch):ud_batch_rows(d->batch);
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Batch_nativeJson(JNIEnv *e,jclass cls,jlong handle,jboolean schema) {
    (void)cls;java_data *d=(java_data *)(intptr_t)handle;char *json=NULL;uint32_t length=0;
    ud_status s=schema?ud_schema_to_json(d->batch,&json,&length):ud_batch_to_json(d->batch,&json,&length);
    if(s) { fail(e,s,ud_status_name(s));return NULL; }jstring value=utf8(e,json);ud_free(json);return value;
}
JNIEXPORT jbyteArray JNICALL Java_org_universaldata_Batch_nativeWire(JNIEnv *e,jclass cls,jlong handle) {
    (void)cls;java_data *d=(java_data *)(intptr_t)handle;uint8_t *bytes=NULL;uint64_t length=0;
    ud_status s=ud_batch_serialize(d->batch,&bytes,&length);if(!s && length>INT32_MAX) s=UD_OVERFLOW;
    if(s) { ud_free(bytes);fail(e,s,ud_status_name(s));return NULL; }
    jbyteArray value=(*e)->NewByteArray(e,(jsize)length);
    if(value) (*e)->SetByteArrayRegion(e,value,0,(jsize)length,(const jbyte *)bytes);ud_free(bytes);return value;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Batch_nativeCast(JNIEnv *e,jclass cls,jlong handle,jbyteArray schema,jint rounding,jlong limit) {
    (void)cls;java_data *source=(java_data *)(intptr_t)handle,*d=data_create(e,limit);if(!d) return 0;
    uint32_t length=0;char *json=copy_data(e,d->ctx,schema,&length);
    ud_status s=json?ud_batch_cast_schema(d->ctx,source->batch,json,length,rounding,&d->batch):UD_INVALID;ud_free(json);return data_result(e,d,s);
}
JNIEXPORT void JNICALL Java_org_universaldata_Batch_nativeClose(JNIEnv *e,jclass cls,jlong handle) {
    (void)cls;ud_status status=data_destroy((java_data *)(intptr_t)handle);if(status) fail(e,status,"JNI context has live allocations");
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Dag_nativeOpen(JNIEnv *e,jclass cls,jbyteArray json,jlong limit) {
    (void)cls;java_data *d=data_create(e,limit);if(!d) return 0;uint32_t length=0;char *data=copy_data(e,d->ctx,json,&length);
    ud_status s=data?ud_dag_create(d->ctx,data,length,&d->dag):UD_INVALID;ud_free(data);return data_result(e,d,s);
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Dag_nativeCount(JNIEnv *e,jclass cls,jlong handle,jboolean edges) {
    (void)e;(void)cls;java_data *d=(java_data *)(intptr_t)handle;return edges?ud_dag_edges(d->dag):ud_dag_nodes(d->dag);
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Dag_nativeJson(JNIEnv *e,jclass cls,jlong handle) {
    (void)cls;java_data *d=(java_data *)(intptr_t)handle;char *json=NULL;uint32_t length=0;
    ud_status s=ud_dag_serialize(d->dag,&json,&length);if(s) { fail(e,s,ud_status_name(s));return NULL; }
    jstring result=utf8(e,json);ud_free(json);return result;
}
JNIEXPORT jbyteArray JNICALL Java_org_universaldata_Dag_nativeFingerprint(JNIEnv *e,jclass cls,jlong handle) {
    (void)cls;java_data *d=(java_data *)(intptr_t)handle;uint8_t hash[32];ud_dag_fingerprint(d->dag,hash);
    jbyteArray result=(*e)->NewByteArray(e,32);if(result) (*e)->SetByteArrayRegion(e,result,0,32,(const jbyte *)hash);return result;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Dag_nativeBind(JNIEnv *e,jclass cls,jlong handle,jbyteArray values,jlong limit) {
    (void)cls;java_data *source=(java_data *)(intptr_t)handle,*d=data_create(e,limit);if(!d) return 0;
    uint32_t length=0;char *json=copy_data(e,d->ctx,values,&length),*literal=NULL;uint32_t literal_length=0;ud_dag *bound=NULL;
    ud_status s=json?ud_dag_bind(source->dag,(ud_string){json,length},&bound):UD_INVALID;
    if(!s) s=ud_dag_serialize(bound,&literal,&literal_length);
    if(!s) s=ud_dag_create(d->ctx,literal,literal_length,&d->dag);
    ud_free(literal);ud_dag_destroy(bound);ud_free(json);return data_result(e,d,s);
}
JNIEXPORT void JNICALL Java_org_universaldata_Dag_nativeClose(JNIEnv *e,jclass cls,jlong handle) {
    (void)cls;ud_status status=data_destroy((java_data *)(intptr_t)handle);if(status) fail(e,status,"JNI context has live allocations");
}
typedef struct java_run {
    ud_context *ctx;ud_run *run;JavaVM *vm;
    jobject authorizer,read_access,write_access;jmethodID allow;
} java_run;
static ud_status java_authorize(void *user,ud_string tenant,ud_string resource,int32_t action) {
    java_run *j=user;if(!j->authorizer) return UD_AUTH;
    JNIEnv *e=NULL;int attached=0;jint status=(*j->vm)->GetEnv(j->vm,(void **)&e,JNI_VERSION_1_8);
    if(status==JNI_EDETACHED) { if((*j->vm)->AttachCurrentThread(j->vm,(void **)&e,NULL)!=JNI_OK) return UD_AUTH;attached=1; }
    else if(status!=JNI_OK) return UD_AUTH;
    int granted=0;
    if(!(*e)->PushLocalFrame(e,32)) {
        /* Native runtime guarantees explicit strings have a trailing NUL in
         * its private owned graph/tenant copies, and excludes embedded NUL. */
        jstring scope=utf8(e,tenant.data),path=utf8(e,resource.data);
        if(scope && path && !(*e)->ExceptionCheck(e)) {
            jobject access=action==UD_RUN_ACCESS_READ?j->read_access:j->write_access;
            granted=(*e)->CallBooleanMethod(e,j->authorizer,j->allow,scope,path,access)==JNI_TRUE;
        }
        if((*e)->ExceptionCheck(e)) { (*e)->ExceptionClear(e);granted=0; }
        (*e)->PopLocalFrame(e,NULL);
    } else if((*e)->ExceptionCheck(e)) (*e)->ExceptionClear(e);
    if(attached) (*j->vm)->DetachCurrentThread(j->vm);return granted?UD_OK:UD_AUTH;
}
static ud_status java_run_destroy(JNIEnv *e,java_run *j) {
    if(!j) return UD_OK;ud_context *ctx=j->ctx;ud_status status=UD_OK;
    if(j->run) {
        ud_run_cancel(j->run);ud_run_metrics m;
        do { ud_run_get_metrics(j->run,&m);if(m.state==UD_RUN_RUNNING) (void)ud_run_wait(j->run,100); } while(m.state==UD_RUN_RUNNING);
        status=ud_run_destroy(j->run);
    }
    if(j->authorizer) (*e)->DeleteGlobalRef(e,j->authorizer);
    if(j->read_access) (*e)->DeleteGlobalRef(e,j->read_access);
    if(j->write_access) (*e)->DeleteGlobalRef(e,j->write_access);
    ud_free(j);ud_status closed=ud_context_destroy(ctx);return status?status:closed;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Run_nativeOpen(JNIEnv *e,jclass cls,jbyteArray document,jbyteArray tenant,
    jobject authorizer,jbyteArray parameters,jint capacity,jint max_rows,jlong limit) {
    (void)cls;
    if(limit<0 || capacity<=0 || max_rows<=0) { fail(e,UD_INVALID,"Invalid run options");return 0; }
    ud_context_options context={sizeof(context),UD_ABI_VERSION,(uint64_t)limit,NULL,NULL,NULL};ud_context *ctx=NULL;
    ud_status status=ud_context_create(&context,&ctx);if(status) { fail(e,status,ud_status_name(status));return 0; }
    java_run *j=ud_alloc(ctx,sizeof(*j));if(!j) { ud_context_destroy(ctx);fail(e,UD_RESOURCE,"JNI run allocation failed");return 0; }j->ctx=ctx;
    if((*e)->GetJavaVM(e,&j->vm)!=JNI_OK) status=UD_RESOURCE;
    if(!status && authorizer) {
        j->authorizer=(*e)->NewGlobalRef(e,authorizer);jclass type=(*e)->GetObjectClass(e,authorizer);
        if(type) j->allow=(*e)->GetMethodID(e,type,"allow","(Ljava/lang/String;Ljava/lang/String;Lorg/universaldata/Run$Access;)Z");
        jclass access=(*e)->FindClass(e,"org/universaldata/Run$Access");
        if(access) {
            jfieldID read=(*e)->GetStaticFieldID(e,access,"READ","Lorg/universaldata/Run$Access;");
            jfieldID write=(*e)->GetStaticFieldID(e,access,"WRITE","Lorg/universaldata/Run$Access;");
            if(read && write) {
                jobject r=(*e)->GetStaticObjectField(e,access,read),w=(*e)->GetStaticObjectField(e,access,write);
                if(r) j->read_access=(*e)->NewGlobalRef(e,r);if(w) j->write_access=(*e)->NewGlobalRef(e,w);
            }
        }
        if(!j->authorizer || !j->allow || !j->read_access || !j->write_access || (*e)->ExceptionCheck(e)) status=UD_RESOURCE;
    }
    uint32_t dn=0,tn=0,pn=0;char *doc=NULL,*scope=NULL,*values=NULL;
    if(!status) {
        doc=copy_data(e,ctx,document,&dn);scope=copy_data(e,ctx,tenant,&tn);if(parameters) values=copy_data(e,ctx,parameters,&pn);
        if(!doc || !scope || (parameters && !values)) status=UD_INVALID;
    }
    if(!status) {
        ud_run_options options={0};options.struct_size=sizeof(options);options.abi_version=UD_ABI_VERSION;options.context=context;
        options.edge_capacity=(uint32_t)capacity;options.max_batch_rows=(uint32_t)max_rows;
        options.tenant=(ud_string){scope,tn};options.parameter_values=(ud_string){values,pn};options.authorize=java_authorize;options.authorize_user=j;
        status=ud_run_create_json(doc,dn,&options,&j->run);
    }
    ud_free(doc);ud_free(scope);ud_free(values);
    if(status) { java_run_destroy(e,j);fail(e,status,ud_status_name(status));return 0; }return (jlong)(intptr_t)j;
}
JNIEXPORT void JNICALL Java_org_universaldata_Run_nativeExecute(JNIEnv *e,jclass cls,jlong handle,jboolean async) {
    (void)cls;java_run *j=(java_run *)(intptr_t)handle;ud_status s=async?ud_run_start(j->run):ud_run_execute(j->run);if(s) fail(e,s,ud_status_name(s));
}
JNIEXPORT jboolean JNICALL Java_org_universaldata_Run_nativeWait(JNIEnv *e,jclass cls,jlong handle,jint timeout) {
    (void)cls;java_run *j=(java_run *)(intptr_t)handle;ud_status s=ud_run_wait(j->run,(uint32_t)timeout);
    if(s==UD_BUSY) return JNI_FALSE;if(s) fail(e,s,ud_status_name(s));return s?JNI_FALSE:JNI_TRUE;
}
JNIEXPORT void JNICALL Java_org_universaldata_Run_nativeCancel(JNIEnv *e,jclass cls,jlong handle) {
    (void)e;(void)cls;java_run *j=(java_run *)(intptr_t)handle;ud_run_cancel(j->run);
}
JNIEXPORT jlongArray JNICALL Java_org_universaldata_Run_nativeMetrics(JNIEnv *e,jclass cls,jlong handle) {
    (void)cls;java_run *j=(java_run *)(intptr_t)handle;ud_run_metrics m;ud_run_get_metrics(j->run,&m);
    jlong values[]={m.state,m.result,(jlong)m.input_rows,(jlong)m.output_rows,(jlong)m.batches,(jlong)m.queued_batches,(jlong)m.queue_high_water,(jlong)m.memory_used};
    jlongArray result=(*e)->NewLongArray(e,8);if(result) (*e)->SetLongArrayRegion(e,result,0,8,values);return result;
}
JNIEXPORT jint JNICALL Java_org_universaldata_Run_nativeNodeState(JNIEnv *e,jclass cls,jlong handle,jint node) {
    (void)cls;java_run *j=(java_run *)(intptr_t)handle;int32_t state=0;ud_status s=ud_run_node_state(j->run,(uint32_t)node,&state);
    if(s) fail(e,s,ud_status_name(s));return state;
}
JNIEXPORT void JNICALL Java_org_universaldata_Run_nativeClose(JNIEnv *e,jclass cls,jlong handle) {
    (void)cls;ud_status s=java_run_destroy(e,(java_run *)(intptr_t)handle);if(s) fail(e,s,ud_status_name(s));
}
