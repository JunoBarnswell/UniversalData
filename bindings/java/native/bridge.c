#include <jni.h>
#include "ud/etl.h"
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
        ud_context_options opt = {sizeof(opt), 1, (uint64_t)limit, NULL, NULL, NULL};
        status = ud_context_create(&opt, &j->ctx);
        if (!status) {
            ud_job_options o = {0};
            o.struct_size = sizeof(o);
            o.abi_version = 1;
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
