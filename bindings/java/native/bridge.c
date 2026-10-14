#include <jni.h>
#include "ud/package.h"
#include "ud/format.h"
#include "ud/compute.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
JNIEXPORT jint JNICALL Java_org_universaldata_ProductVersion_nativeAbi(JNIEnv *e, jclass cls) {
    (void)e;
    (void)cls;
    return UD_ABI_VERSION;
}
static jstring utf8_length(JNIEnv *e, const char *text, size_t n) {
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
static jstring utf8(JNIEnv *env, const char *text) {
    return utf8_length(env, text, strlen(text));
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
typedef struct java_data {
    ud_context *ctx;
    ud_batch *batch;
    ud_package *package;
} java_data;
static java_data *data_create(JNIEnv *e, jlong limit) {
    if (limit < 0) {
        fail(e, UD_INVALID, "Invalid memory limit");
        return NULL;
    }
    ud_context *ctx = NULL;
    ud_context_options opts = {sizeof(opts), UD_ABI_VERSION, (uint64_t)limit, NULL, NULL, NULL};
    ud_status status = ud_context_create(&opts, &ctx);
    if (status) {
        fail(e, status, ud_status_name(status));
        return NULL;
    }
    java_data *d = ud_alloc(ctx, sizeof(*d));
    if (!d) {
        ud_context_destroy(ctx);
        fail(e, UD_RESOURCE, "JNI memory quota exceeded");
        return NULL;
    }
    d->ctx = ctx;
    return d;
}
static ud_status data_destroy(java_data *d) {
    if (!d)
        return UD_OK;
    ud_context *ctx = d->ctx;
    ud_batch_destroy(d->batch);
    ud_package_destroy(d->package);
    ud_free(d);
    return ud_context_destroy(ctx);
}
static char *copy_data(JNIEnv *e, ud_context *ctx, jbyteArray array, uint32_t *length) {
    *length = 0;
    if (!array)
        return NULL;
    jsize n = (*e)->GetArrayLength(e, array);
    char *data = ud_alloc(ctx, (size_t)n + 1);
    if (!data) {
        fail(e, UD_RESOURCE, "JNI input allocation failed");
        return NULL;
    }
    (*e)->GetByteArrayRegion(e, array, 0, n, (jbyte *)data);
    if ((*e)->ExceptionCheck(e)) {
        ud_free(data);
        return NULL;
    }
    *length = (uint32_t)n;
    return data;
}
static jlong data_result(JNIEnv *e, java_data *d, ud_status s) {
    if (s) {
        fail(e, s, ud_status_name(s));
        data_destroy(d);
        return 0;
    }
    return (jlong)(intptr_t)d;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Batch_nativeOpen(JNIEnv *e, jclass cls,
                                                                jbyteArray schema, jbyteArray rows,
                                                                jint rounding, jlong limit) {
    (void)cls;
    java_data *d = data_create(e, limit);
    if (!d)
        return 0;
    uint32_t sn = 0, rn = 0;
    char *sc = copy_data(e, d->ctx, schema, &sn), *data = copy_data(e, d->ctx, rows, &rn);
    ud_status s = sc && data
                      ? ud_batch_from_json_schema(d->ctx, sc, sn, data, rn, rounding, &d->batch)
                      : UD_INVALID;
    ud_free(sc);
    ud_free(data);
    return data_result(e, d, s);
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Batch_nativeFromWire(JNIEnv *e, jclass cls,
                                                                    jbyteArray wire, jlong limit) {
    (void)cls;
    java_data *d = data_create(e, limit);
    if (!d)
        return 0;
    uint32_t n = 0;
    char *data = copy_data(e, d->ctx, wire, &n);
    ud_status s =
        data ? ud_batch_deserialize(d->ctx, (const uint8_t *)data, n, &d->batch) : UD_INVALID;
    ud_free(data);
    return data_result(e, d, s);
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Batch_nativeCount(JNIEnv *e, jclass cls,
                                                                 jlong handle, jboolean columns) {
    (void)e;
    (void)cls;
    java_data *d = (java_data *)(intptr_t)handle;
    return columns ? ud_batch_columns(d->batch) : ud_batch_rows(d->batch);
}
JNIEXPORT jboolean JNICALL Java_org_universaldata_Batch_nativeHasAbsent(JNIEnv *e, jclass cls,
                                                                        jlong handle) {
    (void)e;
    (void)cls;
    java_data *data = (java_data *)(intptr_t)handle;
    return ud_batch_has_absent(data->batch) ? JNI_TRUE : JNI_FALSE;
}
JNIEXPORT jboolean JNICALL Java_org_universaldata_Batch_nativePresent(JNIEnv *e, jclass cls,
                                                                      jlong handle, jint row,
                                                                      jint column) {
    (void)cls;
    java_data *data = (java_data *)(intptr_t)handle;
    int32_t present = 0;
    ud_status status = row < 0 || column < 0 ? UD_INVALID
                                             : ud_batch_cell_present(data->batch, (uint32_t)row,
                                                                     (uint32_t)column, &present);
    if (status)
        fail(e, status, ud_status_name(status));
    return !status && present ? JNI_TRUE : JNI_FALSE;
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Batch_nativeJson(JNIEnv *e, jclass cls,
                                                                  jlong handle, jboolean schema) {
    (void)cls;
    java_data *d = (java_data *)(intptr_t)handle;
    char *json = NULL;
    uint32_t length = 0;
    ud_status s = schema ? ud_schema_to_json(d->batch, &json, &length)
                         : ud_batch_to_json(d->batch, &json, &length);
    if (s) {
        fail(e, s, ud_status_name(s));
        return NULL;
    }
    jstring value = utf8(e, json);
    ud_free(json);
    return value;
}
static ud_status scalar(java_data *data, jint row, jint column, ud_type type, ud_value *value) {
    if (!data || row < 0 || column < 0)
        return UD_INVALID;
    ud_status status = ud_batch_get(data->batch, (uint32_t)row, (uint32_t)column, value);
    if (status)
        return status;
    return value->type == type && !value->is_null && !value->is_absent ? UD_OK : UD_INCOMPATIBLE;
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Batch_nativeUInt64(JNIEnv *env, jclass cls,
                                                                    jlong handle, jint row,
                                                                    jint column) {
    (void)cls;
    ud_value value;
    ud_status status = scalar((java_data *)(intptr_t)handle, row, column, UD_UINT64, &value);
    if (status) {
        fail(env, status, ud_status_name(status));
        return NULL;
    }
    char text[24];
    snprintf(text, sizeof(text), "%llu", (unsigned long long)value.unsigned_integer);
    return utf8(env, text);
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Batch_nativeDuration(JNIEnv *env, jclass cls,
                                                                    jlong handle, jint row,
                                                                    jint column) {
    (void)cls;
    ud_value value;
    ud_status status = scalar((java_data *)(intptr_t)handle, row, column, UD_DURATION, &value);
    if (status) {
        fail(env, status, ud_status_name(status));
        return 0;
    }
    return (jlong)value.integer;
}
JNIEXPORT jlongArray JNICALL Java_org_universaldata_Batch_nativeInterval(JNIEnv *env, jclass cls,
                                                                         jlong handle, jint row,
                                                                         jint column) {
    (void)cls;
    ud_value value;
    ud_status status = scalar((java_data *)(intptr_t)handle, row, column, UD_INTERVAL, &value);
    if (status) {
        fail(env, status, ud_status_name(status));
        return NULL;
    }
    jlong parts[] = {value.interval.months, value.interval.days, value.interval.microseconds};
    jlongArray result = (*env)->NewLongArray(env, 3);
    if (result)
        (*env)->SetLongArrayRegion(env, result, 0, 3, parts);
    return result;
}
JNIEXPORT jbyteArray JNICALL Java_org_universaldata_Batch_nativeWire(JNIEnv *e, jclass cls,
                                                                     jlong handle) {
    (void)cls;
    java_data *d = (java_data *)(intptr_t)handle;
    uint8_t *bytes = NULL;
    uint64_t length = 0;
    ud_status s = ud_batch_serialize(d->batch, &bytes, &length);
    if (!s && length > INT32_MAX)
        s = UD_OVERFLOW;
    if (s) {
        ud_free(bytes);
        fail(e, s, ud_status_name(s));
        return NULL;
    }
    jbyteArray value = (*e)->NewByteArray(e, (jsize)length);
    if (value)
        (*e)->SetByteArrayRegion(e, value, 0, (jsize)length, (const jbyte *)bytes);
    ud_free(bytes);
    return value;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Batch_nativeCast(JNIEnv *e, jclass cls, jlong handle,
                                                                jbyteArray schema, jint rounding,
                                                                jlong limit) {
    (void)cls;
    java_data *source = (java_data *)(intptr_t)handle, *d = data_create(e, limit);
    if (!d)
        return 0;
    uint32_t length = 0;
    char *json = copy_data(e, d->ctx, schema, &length);
    ud_status s =
        json ? ud_batch_cast_schema(d->ctx, source->batch, json, length, rounding, &d->batch)
             : UD_INVALID;
    ud_free(json);
    return data_result(e, d, s);
}
JNIEXPORT void JNICALL Java_org_universaldata_Batch_nativeClose(JNIEnv *e, jclass cls,
                                                                jlong handle) {
    (void)cls;
    ud_status status = data_destroy((java_data *)(intptr_t)handle);
    if (status)
        fail(e, status, "JNI context has live allocations");
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Package_nativeOpen(JNIEnv *e, jclass cls,
                                                                  jbyteArray document,
                                                                  jlong limit) {
    (void)cls;
    java_data *d = data_create(e, limit);
    if (!d)
        return 0;
    uint32_t n = 0;
    char *json = copy_data(e, d->ctx, document, &n);
    ud_status s = json ? ud_package_create(d->ctx, (ud_string){json, n}, &d->package) : UD_INVALID;
    ud_free(json);
    return data_result(e, d, s);
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Package_nativeJson(JNIEnv *e, jclass cls,
                                                                    jlong handle) {
    (void)cls;
    java_data *d = (java_data *)(intptr_t)handle;
    char *json = NULL;
    uint32_t n;
    ud_status s = ud_package_serialize(d->package, &json, &n);
    if (s) {
        fail(e, s, ud_status_name(s));
        return NULL;
    }
    jstring text = utf8(e, json);
    ud_free(json);
    return text;
}
JNIEXPORT void JNICALL Java_org_universaldata_Package_nativeClose(JNIEnv *e, jclass cls,
                                                                  jlong handle) {
    (void)cls;
    ud_status s = data_destroy((java_data *)(intptr_t)handle);
    if (s)
        fail(e, s, ud_status_name(s));
}
typedef struct java_registry {
    ud_context *memory;
    ud_registry *registry;
} java_registry;
typedef struct java_execution {
    ud_context *ctx;
    ud_execution *execution;
    JavaVM *vm;
    jobject authorizer, resolver, observer;
    jclass event_type;
    jmethodID allow, resolve, observed, event_constructor;
} java_execution;
static int attach(java_execution *j, JNIEnv **env, int *attached) {
    *attached = 0;
    if (!j->vm)
        return 0;
    jint status = (*j->vm)->GetEnv(j->vm, (void **)env, JNI_VERSION_1_8);
    if (status == JNI_EDETACHED) {
        if ((*j->vm)->AttachCurrentThread(j->vm, (void **)env, NULL) != JNI_OK)
            return 0;
        *attached = 1;
    } else if (status != JNI_OK)
        return 0;
    return 1;
}
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t action) {
    java_execution *j = user;
    if (!j->authorizer)
        return UD_AUTH;
    JNIEnv *e = NULL;
    int attached = 0;
    if (!attach(j, &e, &attached))
        return UD_AUTH;
    int yes = 0;
    if (!(*e)->PushLocalFrame(e, 16)) {
        jstring scope = utf8(e, tenant.data), path = utf8(e, resource.data);
        if (scope && path && !(*e)->ExceptionCheck(e))
            yes = (*e)->CallBooleanMethod(e, j->authorizer, j->allow, scope, path, (jint)action) ==
                  JNI_TRUE;
        if ((*e)->ExceptionCheck(e)) {
            (*e)->ExceptionClear(e);
            yes = 0;
        }
        (*e)->PopLocalFrame(e, NULL);
    } else if ((*e)->ExceptionCheck(e))
        (*e)->ExceptionClear(e);
    if (attached)
        (*j->vm)->DetachCurrentThread(j->vm);
    return yes ? UD_OK : UD_AUTH;
}
static ud_status resolve_credential(void *user, ud_context *ctx, ud_string tenant,
                                    ud_string resource, ud_string reference, ud_string *out) {
    *out = (ud_string){0};
    java_execution *j = user;
    if (!j->resolver)
        return UD_AUTH;
    JNIEnv *e = NULL;
    int attached = 0;
    if (!attach(j, &e, &attached))
        return UD_AUTH;
    ud_status s = UD_AUTH;
    if (!(*e)->PushLocalFrame(e, 32)) {
        jstring scope = utf8(e, tenant.data), path = utf8(e, resource.data),
                ref = utf8(e, reference.data);
        jstring value = NULL;
        if (scope && path && ref && !(*e)->ExceptionCheck(e))
            value = (*e)->CallObjectMethod(e, j->resolver, j->resolve, scope, path, ref);
        if (value && !(*e)->ExceptionCheck(e)) {
            jclass type = (*e)->FindClass(e, "java/lang/String");
            jmethodID bytes =
                type ? (*e)->GetMethodID(e, type, "getBytes", "(Ljava/lang/String;)[B") : NULL;
            jstring encoding = (*e)->NewStringUTF(e, "UTF-8");
            jbyteArray array =
                bytes && encoding ? (*e)->CallObjectMethod(e, value, bytes, encoding) : NULL;
            if (array && !(*e)->ExceptionCheck(e)) {
                uint32_t n = 0;
                char *data = copy_data(e, ctx, array, &n);
                if (data) {
                    *out = (ud_string){data, n};
                    s = UD_OK;
                } else
                    s = UD_RESOURCE;
            }
        }
        if ((*e)->ExceptionCheck(e)) {
            (*e)->ExceptionClear(e);
            s = UD_AUTH;
            ud_free((void *)out->data);
            *out = (ud_string){0};
        }
        (*e)->PopLocalFrame(e, NULL);
    } else if ((*e)->ExceptionCheck(e))
        (*e)->ExceptionClear(e);
    if (attached)
        (*j->vm)->DetachCurrentThread(j->vm);
    return s;
}
static void java_observe(void *user, const ud_execution_event *event) {
    java_execution *execution = user;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!attach(execution, &env, &attached)) {
        (void)ud_activation_report_fault(event->activation, UD_RESOURCE,
                                         (ud_string){"Java observer unavailable", 25});
        return;
    }
    ud_status status = UD_OK;
    if ((*env)->PushLocalFrame(env, 24) < 0)
        status = UD_RESOURCE;
    else {
        const ud_string texts[] = {event->package_id,  event->execution_id, event->task_id,
                                   event->pipeline_id, event->component_id, event->error_chain};
        jstring values[6] = {0};
        for (uint32_t at = 0; at < 6; at++)
            values[at] = utf8_length(env,
                                     texts[at].data ? texts[at].data
                                     : at == 5      ? "[]"
                                                    : "",
                                     texts[at].length ? texts[at].length
                                     : at == 5        ? 2
                                                      : 0);
        jobject value = NULL;
        if (!(*env)->ExceptionCheck(env))
            value = (*env)->NewObject(
                env, execution->event_type, execution->event_constructor, values[0], values[1],
                values[2], values[3], values[4], (jint)event->state, (jint)event->result,
                (jlong)event->timestamp_ms, (jint)event->kind, (jlong)event->sequence, values[5]);
        if (value && !(*env)->ExceptionCheck(env))
            (*env)->CallVoidMethod(env, execution->observer, execution->observed, value);
        if (!value || (*env)->ExceptionCheck(env))
            status = UD_IO;
        if ((*env)->ExceptionCheck(env))
            (*env)->ExceptionClear(env);
        (*env)->PopLocalFrame(env, NULL);
    }
    if ((*env)->ExceptionCheck(env))
        (*env)->ExceptionClear(env);
    if (status)
        (void)ud_activation_report_fault(event->activation, status,
                                         (ud_string){"Java observer failed", 20});
    if (attached)
        (*execution->vm)->DetachCurrentThread(execution->vm);
}
static ud_status execution_destroy(JNIEnv *e, java_execution *j) {
    if (!j)
        return UD_OK;
    ud_context *ctx = j->ctx;
    ud_status status = UD_OK;
    if (j->execution) {
        ud_execution_cancel(j->execution);
        do {
            status = ud_execution_destroy(j->execution);
            if (status == UD_BUSY)
                (void)ud_execution_wait(j->execution, 100);
        } while (status == UD_BUSY);
    }
    if (j->authorizer)
        (*e)->DeleteGlobalRef(e, j->authorizer);
    if (j->resolver)
        (*e)->DeleteGlobalRef(e, j->resolver);
    if (j->observer)
        (*e)->DeleteGlobalRef(e, j->observer);
    if (j->event_type)
        (*e)->DeleteGlobalRef(e, j->event_type);
    ud_free(j);
    ud_status clean = ud_context_destroy(ctx);
    return status ? status : clean;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Execution_nativeOpen(
    JNIEnv *e, jclass cls, jbyteArray document, jbyteArray tenant, jobject authorizer,
    jbyteArray parameters, jbyteArray identity, jobject resolver, jint capacity, jint rows,
    jlong limit, jlong timeout, jbyteArray state, jboolean resume, jint max_concurrency,
    jlong registry_handle, jobject observer, jlong event_start) {
    (void)cls;
    if (limit < 0 || timeout < 0 || capacity <= 0 || rows <= 0) {
        fail(e, UD_INVALID, "Invalid execution options");
        return 0;
    }
    ud_context *ctx = NULL;
    ud_context_options memory = {sizeof(memory), UD_ABI_VERSION, (uint64_t)limit, NULL, NULL, NULL};
    ud_status s = ud_context_create(&memory, &ctx);
    if (s) {
        fail(e, s, ud_status_name(s));
        return 0;
    }
    java_execution *j = ud_alloc(ctx, sizeof(*j));
    if (!j) {
        ud_context_destroy(ctx);
        fail(e, UD_RESOURCE, "JNI allocation failed");
        return 0;
    }
    j->ctx = ctx;
    if ((*e)->GetJavaVM(e, &j->vm) != JNI_OK)
        s = UD_RESOURCE;
    if (!s && authorizer) {
        j->authorizer = (*e)->NewGlobalRef(e, authorizer);
        jclass type = (*e)->GetObjectClass(e, authorizer);
        j->allow =
            type ? (*e)->GetMethodID(e, type, "allow", "(Ljava/lang/String;Ljava/lang/String;I)Z")
                 : NULL;
        if (!j->authorizer || !j->allow)
            s = UD_RESOURCE;
    }
    if (!s && resolver) {
        j->resolver = (*e)->NewGlobalRef(e, resolver);
        jclass type = (*e)->GetObjectClass(e, resolver);
        j->resolve =
            type ? (*e)->GetMethodID(
                       e, type, "resolve",
                       "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;")
                 : NULL;
        if (!j->resolver || !j->resolve)
            s = UD_RESOURCE;
    }
    if (!s && observer) {
        j->observer = (*e)->NewGlobalRef(e, observer);
        jclass type = (*e)->GetObjectClass(e, observer);
        j->observed =
            type ? (*e)->GetMethodID(e, type, "observe", "(Lorg/universaldata/Execution$Event;)V")
                 : NULL;
        jclass event_type = (*e)->FindClass(e, "org/universaldata/Execution$Event");
        j->event_type = event_type ? (*e)->NewGlobalRef(e, event_type) : NULL;
        j->event_constructor =
            event_type
                ? (*e)->GetMethodID(e, event_type, "<init>",
                                    "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/"
                                    "lang/String;Ljava/lang/String;IIJIJLjava/lang/String;)V")
                : NULL;
        if (!j->observer || !j->event_type || !j->event_constructor || !j->observed)
            s = UD_RESOURCE;
    }
    uint32_t dn = 0, tn = 0, pn = 0, in = 0, sn = 0;
    char *doc = NULL, *scope = NULL, *values = NULL, *id = NULL, *dir = NULL;
    if (!s) {
        doc = copy_data(e, ctx, document, &dn);
        scope = copy_data(e, ctx, tenant, &tn);
        values = copy_data(e, ctx, parameters, &pn);
        id = copy_data(e, ctx, identity, &in);
        dir = copy_data(e, ctx, state, &sn);
        if (!doc || !scope || !values || !id || !dir)
            s = UD_INVALID;
    }
    if (!s) {
        ud_execution_options options = {
            .struct_size = sizeof(options),
            .abi_version = UD_ABI_VERSION,
            .context = memory,
            .edge_capacity = (uint32_t)capacity,
            .max_batch_rows = (uint32_t)rows,
            .max_depth = 32,
            .timeout_ms = (uint64_t)timeout,
            .parameters = {values, pn},
            .tenant = {scope, tn},
            .execution_id = {id, in},
            .authorize = authorize,
            .authorize_user = j,
            .resolve = resolve_credential,
            .resolve_user = j,
            .state_directory = {dir, sn},
            .resume = resume ? 1 : 0,
            .max_concurrency = (uint32_t)max_concurrency,
            .registry =
                registry_handle ? ((java_registry *)(intptr_t)registry_handle)->registry : NULL,
            .observe = observer ? java_observe : NULL,
            .observe_user = j,
            .event_sequence_start = (uint64_t)event_start};
        s = ud_execution_create_json((ud_string){doc, dn}, &options, &j->execution);
    }
    ud_free(doc);
    ud_free(scope);
    ud_free(values);
    ud_free(id);
    ud_free(dir);
    if (s) {
        execution_destroy(e, j);
        fail(e, s, ud_status_name(s));
        return 0;
    }
    return (jlong)(intptr_t)j;
}
JNIEXPORT void JNICALL Java_org_universaldata_Execution_nativeExecute(JNIEnv *e, jclass cls,
                                                                      jlong handle,
                                                                      jboolean async) {
    (void)cls;
    java_execution *j = (java_execution *)(intptr_t)handle;
    ud_status s = async ? ud_execution_start(j->execution) : ud_execution_execute(j->execution);
    if (s)
        fail(e, s, ud_status_name(s));
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Execution_nativeId(JNIEnv *e, jclass cls,
                                                                    jlong handle) {
    (void)cls;
    java_execution *j = (java_execution *)(intptr_t)handle;
    ud_string id = {0};
    ud_status s = ud_execution_id(j->execution, &id);
    if (s) {
        fail(e, s, ud_status_name(s));
        return NULL;
    }
    return utf8(e, id.data);
}
JNIEXPORT jboolean JNICALL Java_org_universaldata_Execution_nativeWait(JNIEnv *e, jclass cls,
                                                                       jlong handle, jint timeout) {
    (void)cls;
    java_execution *j = (java_execution *)(intptr_t)handle;
    ud_status s = ud_execution_wait(j->execution, (uint32_t)timeout);
    if (s == UD_BUSY)
        return JNI_FALSE;
    if (s)
        fail(e, s, ud_status_name(s));
    return s ? JNI_FALSE : JNI_TRUE;
}
JNIEXPORT void JNICALL Java_org_universaldata_Execution_nativeCancel(JNIEnv *e, jclass cls,
                                                                     jlong handle) {
    (void)e;
    (void)cls;
    java_execution *j = (java_execution *)(intptr_t)handle;
    ud_execution_cancel(j->execution);
}
JNIEXPORT jlongArray JNICALL Java_org_universaldata_Execution_nativeMetrics(JNIEnv *e, jclass cls,
                                                                            jlong handle) {
    (void)cls;
    java_execution *j = (java_execution *)(intptr_t)handle;
    ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
    ud_status s = ud_execution_get_metrics(j->execution, &m);
    if (s) {
        fail(e, s, ud_status_name(s));
        return NULL;
    }
    jlong v[] = {m.state,
                 m.result,
                 (jlong)m.input_rows,
                 (jlong)m.output_rows,
                 (jlong)m.rejected_rows,
                 (jlong)m.batches,
                 (jlong)m.queued_batches,
                 (jlong)m.queue_high_water,
                 (jlong)m.memory_used,
                 (jlong)m.executed_tasks,
                 (jlong)m.durable_tasks,
                 (jlong)m.recovery_tasks,
                 (jlong)m.resumed_tasks,
                 (jlong)m.spill_bytes,
                 (jlong)m.pipeline_trees,
                 (jlong)m.tree_high_water,
                 (jlong)m.worker_high_water,
                 (jlong)m.committed_rows,
                 (jlong)m.acknowledged_rows,
                 (jlong)m.error_count,
                 (jlong)m.error_rows,
                 (jlong)m.committed_error_rows,
                 (jlong)m.acknowledged_error_rows};
    jlongArray result = (*e)->NewLongArray(e, 23);
    if (result)
        (*e)->SetLongArrayRegion(e, result, 0, 23, v);
    return result;
}
JNIEXPORT void JNICALL Java_org_universaldata_Execution_nativeClose(JNIEnv *e, jclass cls,
                                                                    jlong handle) {
    (void)cls;
    ud_status s = execution_destroy(e, (java_execution *)(intptr_t)handle);
    if (s)
        fail(e, s, ud_status_name(s));
}

JNIEXPORT jstring JNICALL Java_org_universaldata_Execution_nativeInspect(JNIEnv *env, jclass cls,
                                                                         jlong handle,
                                                                         jboolean diagnostics) {
    (void)cls;
    java_execution *j = (java_execution *)(intptr_t)handle;
    char *text = NULL;
    uint32_t size = 0;
    ud_status status = diagnostics ? ud_execution_diagnostics(j->execution, &text, &size)
                                   : ud_execution_explain(j->execution, &text, &size);
    if (status) {
        fail(env, status, ud_status_name(status));
        return NULL;
    }
    jstring result = utf8(env, text);
    ud_free(text);
    return result;
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Schema_nativeCompare(JNIEnv *env, jclass cls,
                                                                      jbyteArray expected,
                                                                      jbyteArray actual,
                                                                      jbyteArray policy) {
    (void)cls;
    java_data *d = data_create(env, 268435456);
    if (!d)
        return NULL;
    uint32_t a = 0, b = 0, c = 0;
    char *left = copy_data(env, d->ctx, expected, &a), *right = copy_data(env, d->ctx, actual, &b),
         *options = copy_data(env, d->ctx, policy, &c), *text = NULL;
    uint32_t length = 0;
    ud_status status =
        left && right && options
            ? ud_schema_compare_json(d->ctx, (ud_string){left, a}, (ud_string){right, b},
                                     (ud_string){options, c}, &text, &length)
            : UD_INVALID;
    jstring result = NULL;
    if (!status)
        result = utf8(env, text);
    else
        fail(env, status, ud_status_name(status));
    ud_free(left);
    ud_free(right);
    ud_free(options);
    ud_free(text);
    ud_status closed = data_destroy(d);
    if (closed)
        fail(env, closed, ud_status_name(closed));
    return result;
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Batch_nativeRowInfo(JNIEnv *env, jclass cls,
                                                                     jlong handle, jint row) {
    (void)cls;
    java_data *d = (java_data *)(intptr_t)handle;
    char *text = NULL;
    uint32_t length = 0;
    ud_status status =
        row < 0 ? UD_INVALID : ud_batch_row_info_json(d->batch, (uint32_t)row, &text, &length);
    if (status) {
        fail(env, status, ud_status_name(status));
        return NULL;
    }
    jstring result = utf8(env, text);
    ud_free(text);
    return result;
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Execution_nativeDiscover(JNIEnv *env, jclass cls,
                                                                          jlong handle,
                                                                          jbyteArray connection,
                                                                          jbyteArray table) {
    (void)cls;
    java_execution *e = (java_execution *)(intptr_t)handle;
    uint32_t a = 0, b = 0, length = 0;
    char *id = copy_data(env, e->ctx, connection, &a), *name = copy_data(env, e->ctx, table, &b),
         *text = NULL;
    ud_status status = id && name
                           ? ud_execution_discover_schema(e->execution, (ud_string){id, a},
                                                          (ud_string){name, b}, &text, &length)
                           : UD_INVALID;
    jstring result = NULL;
    if (status)
        fail(env, status, ud_status_name(status));
    else
        result = utf8(env, text);
    ud_free(id);
    ud_free(name);
    ud_free(text);
    return result;
}

JNIEXPORT jstring JNICALL Java_org_universaldata_Execution_nativeCatalog(JNIEnv *env, jclass cls,
                                                                         jlong handle) {
    (void)cls;
    java_execution *j = (java_execution *)(intptr_t)handle;
    char *text = NULL;
    uint32_t size = 0;
    ud_status status = ud_execution_component_catalog(j->execution, &text, &size);
    if (status) {
        fail(env, status, ud_status_name(status));
        return NULL;
    }
    jstring result = utf8(env, text);
    ud_free(text);
    return result;
}

JNIEXPORT jstring JNICALL Java_org_universaldata_Batch_nativePivotValues(JNIEnv *env, jclass cls,
                                                                         jlong handle, jint column,
                                                                         jbyteArray prefix,
                                                                         jint maximum) {
    (void)cls;
    java_data *d = (java_data *)(intptr_t)handle;
    uint32_t size = 0;
    char *name = copy_data(env, d->ctx, prefix, &size);
    if (!name)
        return NULL;
    char *text = NULL;
    uint32_t length = 0;
    ud_status status = column < 0 || maximum < 1 ? UD_INVALID : ud_batch_seal(d->batch);
    if (!status)
        status =
            ud_pivot_discover_values(d->ctx, d->batch, (uint32_t)column, (ud_string){name, size},
                                     (uint32_t)maximum, &text, &length);
    ud_free(name);
    if (status) {
        fail(env, status, ud_status_name(status));
        ud_free(text);
        return NULL;
    }
    jstring result = utf8(env, text);
    ud_free(text);
    return result;
}
JNIEXPORT void JNICALL Java_org_universaldata_Execution_nativeDebug(JNIEnv *env, jclass type,
                                                                    jlong handle,
                                                                    jbyteArray command,
                                                                    jboolean configure) {
    (void)type;
    java_execution *execution = (java_execution *)(intptr_t)handle;
    uint32_t length = 0;
    char *text = copy_data(env, execution->ctx, command, &length);
    ud_status status =
        text ? (configure
                    ? ud_execution_debug_configure(execution->execution, (ud_string){text, length})
                    : ud_execution_debug_control(execution->execution, (ud_string){text, length}))
             : UD_INVALID;
    ud_free(text);
    if (status)
        fail(env, status, ud_status_name(status));
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Execution_nativeDebugSnapshot(JNIEnv *env,
                                                                               jclass type,
                                                                               jlong handle) {
    (void)type;
    java_execution *execution = (java_execution *)(intptr_t)handle;
    char *text = NULL;
    uint32_t length = 0;
    ud_status status = ud_execution_debug_snapshot(execution->execution, &text, &length);
    if (status) {
        fail(env, status, ud_status_name(status));
        return NULL;
    }
    jstring result = utf8(env, text);
    ud_free(text);
    return result;
}
#include <stdatomic.h>
typedef struct java_registration {
    atomic_uint references;
    ud_context *memory;
    JavaVM *vm;
    jobject factory;
    jclass type;
    jmethodID prepare, invoke, lifecycle, configure, checkpoint, restore, epoch;
} java_registration;
typedef struct java_task {
    java_registration *owner;
    jobject task;
    const ud_activation *activation;
    ud_context *memory;
} java_task;
static int owner_attach(java_registration *owner, JNIEnv **env, int *attached) {
    java_execution projected = {.vm = owner->vm};
    return attach(&projected, env, attached);
}
static ud_status owner_retain(void *pointer) {
    java_registration *owner = pointer;
    atomic_fetch_add(&owner->references, 1);
    return UD_OK;
}
static void owner_release(void *pointer) {
    java_registration *owner = pointer;
    if (atomic_fetch_sub(&owner->references, 1) != 1)
        return;
    JNIEnv *env = NULL;
    int attached = 0;
    if (owner_attach(owner, &env, &attached)) {
        if (owner->factory)
            (*env)->DeleteGlobalRef(env, owner->factory);
        if (owner->type)
            (*env)->DeleteGlobalRef(env, owner->type);
        if (attached)
            (*owner->vm)->DetachCurrentThread(owner->vm);
    }
    ud_free(owner);
}
static ud_status callback_failure(JNIEnv *env, ud_context *memory) {
    if (!(*env)->ExceptionCheck(env))
        return UD_OK;
    (*env)->ExceptionClear(env);
    return ud_context_report(memory, UD_IO, (ud_string){"Java callback failed", 20});
}
static ud_status java_prepare(void *user, const ud_component_context *context,
                              ud_string configuration, const ud_batch *const *inputs,
                              const ud_batch *const *outputs, void **out) {
    (void)inputs;
    (void)outputs;
    *out = NULL;
    java_registration *owner = user;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    ud_status status = UD_OK;
    java_task *instance = NULL;
    if ((*env)->PushLocalFrame(env, 24) < 0)
        status = UD_RESOURCE;
    else {
        jstring task = utf8(env, context->task_id.data ? context->task_id.data : ""),
                pipeline = utf8(env, context->pipeline_id.data ? context->pipeline_id.data : ""),
                component = utf8(env, context->component_id.data ? context->component_id.data : ""),
                config = utf8(env, configuration.data);
        jobject object = NULL;
        if (task && pipeline && component && config)
            object = (*env)->CallStaticObjectMethod(
                env, owner->type, owner->prepare, owner->factory,
                (jlong)(intptr_t)context->activation, task, pipeline, component, config);
        status = callback_failure(env, context->memory);
        if (!status && !object)
            status = UD_RESOURCE;
        if (!status) {
            instance = ud_alloc(context->memory, sizeof(*instance));
            if (!instance)
                status = UD_RESOURCE;
        }
        if (!status) {
            instance->owner = owner;
            instance->activation = context->activation;
            instance->memory = context->memory;
            instance->task = (*env)->NewGlobalRef(env, object);
            if (!instance->task)
                status = UD_RESOURCE;
        }
        if (status && object) {
            (*env)->CallStaticVoidMethod(env, owner->type, owner->lifecycle, object, 2);
            (void)callback_failure(env, context->memory);
        }
        (*env)->PopLocalFrame(env, NULL);
    }
    (void)callback_failure(env, context->memory);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    if (status)
        ud_free(instance);
    else
        *out = instance;
    return status;
}
static ud_status java_invoke(void *pointer, const ud_component_context *context,
                             ud_string configuration) {
    java_task *instance = pointer;
    java_registration *owner = instance->owner;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    ud_status status = UD_RESOURCE;
    if ((*env)->PushLocalFrame(env, 24) >= 0) {
        jstring task = utf8(env, context->task_id.data ? context->task_id.data : ""),
                pipeline = utf8(env, context->pipeline_id.data ? context->pipeline_id.data : ""),
                component = utf8(env, context->component_id.data ? context->component_id.data : ""),
                config = utf8(env, configuration.data);
        if (task && pipeline && component && config) {
            (*env)->CallStaticVoidMethod(env, owner->type, owner->invoke, instance->task,
                                         (jlong)(intptr_t)context->activation, task, pipeline,
                                         component, config);
            status = callback_failure(env, context->memory);
        }
        (*env)->PopLocalFrame(env, NULL);
    }
    (void)callback_failure(env, context->memory);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return status;
}
static ud_status java_lifecycle(java_task *instance, int action) {
    java_registration *owner = instance->owner;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    (*env)->CallStaticVoidMethod(env, owner->type, owner->lifecycle, instance->task, action);
    ud_status status = callback_failure(env, instance->memory);
    if (action == 2) {
        (*env)->DeleteGlobalRef(env, instance->task);
        instance->task = NULL;
    }
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return status;
}
static ud_status java_finish(void *pointer) {
    return java_lifecycle(pointer, 0);
}
static void java_cancel_task(void *pointer) {
    java_task *instance = pointer;
    ud_status status = java_lifecycle(instance, 1);
    if (status)
        (void)ud_activation_report_fault(instance->activation, status,
                                         (ud_string){"Java cancel failed", 18});
}
static void java_dispose_task(void *pointer, ud_status result) {
    (void)result;
    java_task *instance = pointer;
    ud_status status = java_lifecycle(instance, 2);
    if (status)
        (void)ud_activation_report_fault(instance->activation, status,
                                         (ud_string){"Java dispose failed", 19});
    ud_free(instance);
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Registry_nativeOpen(JNIEnv *env, jclass type,
                                                                   jlong limit) {
    (void)type;
    if (limit < 0) {
        fail(env, UD_INVALID, "Invalid registry quota");
        return 0;
    }
    ud_context *memory = NULL;
    ud_context_options options = {.struct_size = sizeof(options),
                                  .abi_version = UD_ABI_VERSION,
                                  .memory_limit = (uint64_t)limit};
    ud_status status = ud_context_create(&options, &memory);
    java_registry *registry = !status ? ud_alloc(memory, sizeof(*registry)) : NULL;
    if (!status && !registry)
        status = UD_RESOURCE;
    if (!status) {
        registry->memory = memory;
        status = ud_registry_create(memory, &registry->registry);
    }
    if (status) {
        ud_free(registry);
        if (memory)
            ud_context_destroy(memory);
        fail(env, status, ud_status_name(status));
        return 0;
    }
    return (jlong)(intptr_t)registry;
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Registry_nativeDescribe(JNIEnv *env, jclass type,
                                                                         jlong handle) {
    (void)type;
    java_registry *registry = (java_registry *)(intptr_t)handle;
    char *text = NULL;
    uint32_t length;
    ud_status status = ud_registry_describe(registry->registry, &text, &length);
    if (status) {
        fail(env, status, ud_status_name(status));
        return NULL;
    }
    jstring out = utf8(env, text);
    ud_free(text);
    return out;
}
JNIEXPORT void JNICALL Java_org_universaldata_Registry_nativeClose(JNIEnv *env, jclass type,
                                                                   jlong handle) {
    (void)type;
    java_registry *registry = (java_registry *)(intptr_t)handle;
    ud_status status = ud_registry_destroy(registry->registry);
    if (status) {
        fail(env, status, ud_status_name(status));
        return;
    }
    ud_context *memory = registry->memory;
    ud_free(registry);
    status = ud_context_destroy(memory);
    if (status)
        fail(env, status, ud_status_name(status));
}
JNIEXPORT void JNICALL Java_org_universaldata_Registry_nativeCheck(JNIEnv *env, jclass type,
                                                                   jlong activation) {
    (void)type;
    ud_status status = ud_activation_check((const ud_activation *)(intptr_t)activation);
    if (status)
        fail(env, status, ud_status_name(status));
}
JNIEXPORT void JNICALL Java_org_universaldata_Registry_nativeRegister(
    JNIEnv *env, jclass type, jlong handle, jbyteArray id, jbyteArray name, jbyteArray version,
    jbyteArray schema, jboolean replay, jboolean threads, jint effects, jobject factory) {
    java_registry *registry = (java_registry *)(intptr_t)handle;
    uint32_t sizes[4] = {0};
    char *texts[4] = {0};
    jbyteArray arrays[4] = {id, name, version, schema};
    ud_status status = UD_OK;
    for (uint32_t at = 0; at < 4; at++) {
        texts[at] = copy_data(env, registry->memory, arrays[at], sizes + at);
        if (!texts[at])
            status = UD_RESOURCE;
    }
    java_registration *owner = !status ? ud_alloc(registry->memory, sizeof(*owner)) : NULL;
    if (!status && !owner)
        status = UD_RESOURCE;
    if (!status) {
        atomic_init(&owner->references, 1);
        owner->memory = registry->memory;
        if ((*env)->GetJavaVM(env, &owner->vm) != JNI_OK)
            status = UD_RESOURCE;
        owner->factory = (*env)->NewGlobalRef(env, factory);
        owner->type = (*env)->NewGlobalRef(env, type);
        owner->prepare = (*env)->GetStaticMethodID(
            env, type, "prepare",
            "(Lorg/universaldata/Registry$TaskFactory;JLjava/lang/String;Ljava/lang/String;Ljava/"
            "lang/String;Ljava/lang/String;)Lorg/universaldata/Registry$Task;");
        owner->invoke =
            (*env)->GetStaticMethodID(env, type, "invoke",
                                      "(Lorg/universaldata/Registry$Task;JLjava/lang/String;Ljava/"
                                      "lang/String;Ljava/lang/String;Ljava/lang/String;)V");
        owner->lifecycle = (*env)->GetStaticMethodID(env, type, "lifecycle",
                                                     "(Lorg/universaldata/Registry$Task;I)V");
        if (!owner->factory || !owner->type || !owner->prepare || !owner->invoke ||
            !owner->lifecycle)
            status = UD_RESOURCE;
    }
    ud_component_descriptor descriptor = {.struct_size = sizeof(descriptor),
                                          .abi_version = UD_ABI_VERSION,
                                          .id = {texts[0], sizes[0]},
                                          .name = {texts[1], sizes[1]},
                                          .version = {texts[2], sizes[2]},
                                          .configuration_schema = {texts[3], sizes[3]},
                                          .category = UD_COMPONENT_TASK,
                                          .synchronous_input = -1,
                                          .error_output = -1,
                                          .capabilities = (replay ? UD_COMPONENT_REPLAY_SAFE : 0) |
                                                          (threads ? UD_COMPONENT_THREAD_SAFE : 0),
                                          .effects = (uint32_t)effects,
                                          .user = owner,
                                          .prepare = java_prepare,
                                          .invoke = java_invoke,
                                          .finish = java_finish,
                                          .cancel = java_cancel_task,
                                          .dispose = java_dispose_task};
    if (!status)
        status = ud_registry_register_owned(registry->registry, &descriptor, 1, owner, owner_retain,
                                            owner_release);
    if (owner)
        owner_release(owner);
    for (uint32_t at = 0; at < 4; at++)
        ud_free(texts[at]);
    if (status)
        fail(env, status, ud_status_name(status));
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Execution_nativePorts(
    JNIEnv *env, jclass type, jlong handle, jbyteArray pipeline, jbyteArray component,
    jbyteArray kind, jbyteArray configuration) {
    (void)type;
    java_execution *execution = (java_execution *)(intptr_t)handle;
    jbyteArray arrays[4] = {pipeline, component, kind, configuration};
    uint32_t sizes[4] = {0};
    char *texts[4] = {0};
    ud_status status = UD_OK;
    for (uint32_t at = 0; at < 4; at++) {
        texts[at] = copy_data(env, execution->ctx, arrays[at], sizes + at);
        if (!texts[at])
            status = UD_RESOURCE;
    }
    char *output = NULL;
    uint32_t length = 0;
    if (!status)
        status = ud_execution_component_ports(
            execution->execution, (ud_string){texts[0], sizes[0]}, (ud_string){texts[1], sizes[1]},
            (ud_string){texts[2], sizes[2]}, (ud_string){texts[3], sizes[3]}, &output, &length);
    jstring result = !status ? utf8_length(env, output, length) : NULL;
    for (uint32_t at = 0; at < 4; at++)
        ud_free(texts[at]);
    ud_free(output);
    if (status)
        fail(env, status, ud_status_name(status));
    return result;
}
#include "ud/catalog.h"
typedef struct java_catalog {
    java_execution callbacks;
    ud_catalog *catalog;
} java_catalog;
JNIEXPORT jlong JNICALL Java_org_universaldata_Catalog_nativeOpen(JNIEnv *env, jclass type,
                                                                  jbyteArray directory,
                                                                  jbyteArray tenant,
                                                                  jobject authorizer, jlong limit) {
    (void)type;
    if (limit < 0) {
        fail(env, UD_INVALID, "Invalid Catalog quota");
        return 0;
    }
    ud_context *memory = NULL;
    ud_context_options options = {.struct_size = sizeof(options),
                                  .abi_version = UD_ABI_VERSION,
                                  .memory_limit = (uint64_t)limit};
    ud_status status = ud_context_create(&options, &memory);
    java_catalog *catalog = !status ? ud_alloc(memory, sizeof(*catalog)) : NULL;
    if (!status && !catalog)
        status = UD_RESOURCE;
    uint32_t sizes[2] = {0};
    char *path = NULL, *scope = NULL;
    if (!status) {
        catalog->callbacks.ctx = memory;
        if ((*env)->GetJavaVM(env, &catalog->callbacks.vm) != JNI_OK)
            status = UD_RESOURCE;
        catalog->callbacks.authorizer = (*env)->NewGlobalRef(env, authorizer);
        jclass grant = (*env)->GetObjectClass(env, authorizer);
        catalog->callbacks.allow =
            grant ? (*env)->GetMethodID(env, grant, "allow",
                                        "(Ljava/lang/String;Ljava/lang/String;I)Z")
                  : NULL;
        if (!catalog->callbacks.authorizer || !catalog->callbacks.allow)
            status = UD_RESOURCE;
    }
    if (!status) {
        path = copy_data(env, memory, directory, sizes);
        scope = copy_data(env, memory, tenant, sizes + 1);
        if (!path || !scope)
            status = UD_RESOURCE;
    }
    if (!status)
        status = ud_catalog_open(memory,
                                 &(ud_catalog_options){.struct_size = sizeof(ud_catalog_options),
                                                       .abi_version = UD_ABI_VERSION,
                                                       .directory = {path, sizes[0]},
                                                       .tenant = {scope, sizes[1]},
                                                       .authorize = authorize,
                                                       .authorize_user = &catalog->callbacks,
                                                       .create_directory = 1},
                                 &catalog->catalog);
    ud_free(path);
    ud_free(scope);
    if (status) {
        if (catalog)
            execution_destroy(env, &catalog->callbacks);
        else if (memory)
            ud_context_destroy(memory);
        fail(env, status, ud_status_name(status));
        return 0;
    }
    return (jlong)(intptr_t)catalog;
}
JNIEXPORT void JNICALL Java_org_universaldata_Catalog_nativeClose(JNIEnv *env, jclass type,
                                                                  jlong handle) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    ud_catalog_close(catalog->catalog);
    ud_status status = execution_destroy(env, &catalog->callbacks);
    if (status)
        fail(env, status, ud_status_name(status));
}
JNIEXPORT void JNICALL Java_org_universaldata_Catalog_nativePut(JNIEnv *env, jclass type,
                                                                jlong handle, jint kind,
                                                                jbyteArray id, jlong revision,
                                                                jbyteArray document) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    uint32_t sizes[2] = {0};
    char *name = copy_data(env, catalog->callbacks.ctx, id, sizes),
         *json = copy_data(env, catalog->callbacks.ctx, document, sizes + 1);
    ud_status status = name && json ? ud_catalog_put(catalog->catalog,
                                                     &(ud_catalog_key){sizeof(ud_catalog_key),
                                                                       UD_ABI_VERSION,
                                                                       kind,
                                                                       {name, sizes[0]},
                                                                       (uint64_t)revision},
                                                     (ud_string){json, sizes[1]})
                                    : UD_RESOURCE;
    ud_free(name);
    ud_free(json);
    if (status)
        fail(env, status, ud_status_name(status));
}
JNIEXPORT jlongArray JNICALL Java_org_universaldata_Catalog_nativeLease(JNIEnv *env,jclass type,
    jlong handle,jint operation,jint kind,jbyteArray id,jbyteArray owner,jlong generation,
    jint duration,jbyteArray pool,jint capacity) {
    (void)type;
    java_catalog *catalog=(java_catalog *)(intptr_t)handle;
    uint32_t sizes[3]={0};
    char *texts[]={copy_data(env,catalog->callbacks.ctx,id,sizes),
        copy_data(env,catalog->callbacks.ctx,owner,sizes+1),copy_data(env,catalog->callbacks.ctx,pool,sizes+2)};
    ud_status status=texts[0] && texts[1] && texts[2] ? UD_OK : UD_RESOURCE;
    uint64_t token=0,until=0;
    ud_catalog_lease lease={.struct_size=sizeof(lease),.abi_version=UD_ABI_VERSION,.kind=kind,
        .id={texts[0],sizes[0]},.owner={texts[1],sizes[1]},.generation=(uint64_t)generation};
    if (!status && (duration<0 || capacity<0 || generation<0)) status=UD_INVALID;
    if (!status) {
        if (operation==1) {
            ud_catalog_lease_request request={.struct_size=sizeof(request),.abi_version=UD_ABI_VERSION,
                .kind=kind,.id=lease.id,.owner=lease.owner,.pool={texts[2],sizes[2]},
                .duration_ms=(uint32_t)duration,.capacity=(uint32_t)capacity};
            status=ud_catalog_lease_acquire(catalog->catalog,&request,&token,&until);
        } else if (operation==2) { token=(uint64_t)generation;status=ud_catalog_lease_renew(catalog->catalog,&lease,(uint32_t)duration,&until); }
        else if (operation==3) status=ud_catalog_lease_release(catalog->catalog,&lease);
        else if (operation==4) { int32_t active=0;status=ud_catalog_lease_active(catalog->catalog,kind,lease.id,&active);token=(uint64_t)active; }
        else status=UD_INVALID;
    }
    for (uint32_t at=0;at<3;at++) ud_free(texts[at]);
    if (operation==1 && status==UD_BUSY) return NULL;
    if (status) { fail(env,status,ud_status_name(status));return NULL; }
    jlongArray result=(*env)->NewLongArray(env,2);
    if (result) { jlong values[]={(jlong)token,(jlong)until};(*env)->SetLongArrayRegion(env,result,0,2,values); }
    return result;
}
JNIEXPORT void JNICALL Java_org_universaldata_Catalog_nativeCompareExchange(JNIEnv *env,jclass type,
    jlong handle,jint kind,jbyteArray id,jlong expected,jbyteArray document,jbyteArray owner,jlong generation) {
    (void)type;
    java_catalog *catalog=(java_catalog *)(intptr_t)handle;
    uint32_t sizes[3]={0};
    char *texts[]={copy_data(env,catalog->callbacks.ctx,id,sizes),
        copy_data(env,catalog->callbacks.ctx,document,sizes+1),copy_data(env,catalog->callbacks.ctx,owner,sizes+2)};
    ud_catalog_key key={sizeof(key),UD_ABI_VERSION,kind,{texts[0],sizes[0]},(uint64_t)expected+1};
    ud_catalog_lease lease={.struct_size=sizeof(lease),.abi_version=UD_ABI_VERSION,.kind=kind,
        .id=key.id,.owner={texts[2],sizes[2]},.generation=(uint64_t)generation};
    ud_status status=texts[0] && texts[1] && texts[2] ?
        ud_catalog_compare_exchange(catalog->catalog,&key,(ud_string){texts[1],sizes[1]},(uint64_t)expected,generation ? &lease : NULL) : UD_RESOURCE;
    for (uint32_t at=0;at<3;at++) ud_free(texts[at]);
    if (status) fail(env,status,ud_status_name(status));
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Catalog_nativeGet(JNIEnv *env, jclass type,
                                                                   jlong handle, jint kind,
                                                                   jbyteArray id, jlong revision) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    uint32_t length = 0, size = 0;
    char *name = copy_data(env, catalog->callbacks.ctx, id, &size), *json = NULL;
    ud_status status = name ? ud_catalog_get(catalog->catalog,
                                             &(ud_catalog_key){sizeof(ud_catalog_key),
                                                               UD_ABI_VERSION,
                                                               kind,
                                                               {name, size},
                                                               (uint64_t)revision},
                                             &json, &length)
                            : UD_RESOURCE;
    jstring result = !status ? utf8_length(env, json, length) : NULL;
    ud_free(name);
    ud_free(json);
    if (status && status != UD_END)
        fail(env, status, ud_status_name(status));
    return result;
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Catalog_nativeQuery(JNIEnv *env, jclass type,
                                                                     jlong handle,
                                                                     jbyteArray query) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    uint32_t length = 0, size = 0;
    char *input = copy_data(env, catalog->callbacks.ctx, query, &size), *json = NULL;
    ud_status status =
        input ? ud_catalog_query(catalog->catalog, (ud_string){input, size}, &json, &length)
              : UD_RESOURCE;
    jstring result = !status ? utf8_length(env, json, length) : NULL;
    ud_free(input);
    ud_free(json);
    if (status)
        fail(env, status, ud_status_name(status));
    return result;
}
JNIEXPORT void JNICALL Java_org_universaldata_Catalog_nativeRecord(
    JNIEnv *env, jclass type, jlong handle, jbyteArray package, jbyteArray execution,
    jbyteArray task, jbyteArray pipeline, jbyteArray component, jint state, jint result,
    jlong timestamp, jint kind, jlong sequence, jbyteArray chain) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    jbyteArray arrays[6] = {package, execution, task, pipeline, component, chain};
    char *texts[6] = {0};
    uint32_t lengths[6] = {0};
    ud_status status = UD_OK;
    for (uint32_t at = 0; !status && at < 6; at++) {
        texts[at] = copy_data(env, catalog->callbacks.ctx, arrays[at], lengths + at);
        if (!texts[at]) status = UD_RESOURCE;
    }
    ud_execution_event event = {.struct_size = sizeof(event), .abi_version = UD_ABI_VERSION,
                                 .package_id = {texts[0], lengths[0]},
                                 .execution_id = {texts[1], lengths[1]},
                                 .task_id = {texts[2], lengths[2]},
                                 .pipeline_id = {texts[3], lengths[3]},
                                 .component_id = {texts[4], lengths[4]},
                                 .state = state, .result = result, .timestamp_ms = (uint64_t)timestamp,
                                 .kind = kind, .sequence = (uint64_t)sequence,
                                 .error_chain = {texts[5], lengths[5]}};
    if (!status)
        status = ud_catalog_record_event(catalog->catalog, &event, (uint64_t)sequence);
    for (uint32_t at = 0; at < 6; at++) ud_free(texts[at]);
    if (status) fail(env, status, ud_status_name(status));
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Catalog_nativeLatest(JNIEnv *env, jclass type,
                                                                    jlong handle, jint kind,
                                                                    jbyteArray id) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    uint32_t size = 0;
    char *input = copy_data(env, catalog->callbacks.ctx, id, &size);
    uint64_t revision = 0;
    ud_status status = input ? ud_catalog_latest_revision(catalog->catalog, kind,
                                                          (ud_string){input, size}, &revision)
                             : UD_RESOURCE;
    ud_free(input);
    if (status && status != UD_END)
        fail(env, status, ud_status_name(status));
    return (jlong)revision;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Catalog_nativePrune(JNIEnv *env, jclass type,
                                                                   jlong handle, jlong before,
                                                                   jint limit) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    uint64_t removed = 0;
    ud_status status =
        ud_catalog_prune_events(catalog->catalog, (uint64_t)before, (uint32_t)limit, &removed);
    if (status)
        fail(env, status, ud_status_name(status));
    return (jlong)removed;
}
JNIEXPORT jobjectArray JNICALL Java_org_universaldata_Catalog_nativeBind(
    JNIEnv *env, jclass type, jlong handle, jbyteArray deployment, jlong revision,
    jbyteArray package_id, jbyteArray project_parameters, jbyteArray package_parameters) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    jbyteArray arrays[4] = {deployment, package_id, project_parameters, package_parameters};
    char *texts[4] = {0};
    uint32_t sizes[4] = {0};
    ud_status status = UD_OK;
    for (uint32_t at = 0; at < 4; at++) {
        texts[at] = copy_data(env, catalog->callbacks.ctx, arrays[at], sizes + at);
        if (!texts[at])
            status = UD_RESOURCE;
    }
    ud_package *package = NULL;
    char *parameters = NULL, *json = NULL;
    uint32_t parameter_size = 0, json_size = 0;
    if (!status)
        status = ud_catalog_bind_package(
            catalog->catalog,
            &(ud_catalog_binding){.struct_size = sizeof(ud_catalog_binding),
                                  .abi_version = UD_ABI_VERSION,
                                  .deployment_id = {texts[0], sizes[0]},
                                  .deployment_revision = (uint64_t)revision,
                                  .package_id = {texts[1], sizes[1]},
                                  .project_parameters = {texts[2], sizes[2]},
                                  .package_parameters = {texts[3], sizes[3]}},
            &package, &parameters, &parameter_size);
    if (!status)
        status = ud_package_serialize(package, &json, &json_size);
    jclass string = !status ? (*env)->FindClass(env, "java/lang/String") : NULL;
    jobjectArray result = string ? (*env)->NewObjectArray(env, 2, string, NULL) : NULL;
    if (!status && !result)
        status = UD_RESOURCE;
    if (!status) {
        jstring a = utf8_length(env, json, json_size),
                b = utf8_length(env, parameters, parameter_size);
        if (a && b) {
            (*env)->SetObjectArrayElement(env, result, 0, a);
            (*env)->SetObjectArrayElement(env, result, 1, b);
        }
    }
    for (uint32_t at = 0; at < 4; at++)
        ud_free(texts[at]);
    ud_package_destroy(package);
    ud_free(parameters);
    ud_free(json);
    if (status)
        fail(env, status, ud_status_name(status));
    return result;
}

JNIEXPORT jstring JNICALL Java_org_universaldata_Catalog_nativeBindingSnapshot(
    JNIEnv *env,jclass type,jlong handle,jbyteArray deployment,jlong revision,jbyteArray package_id) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    uint32_t sizes[2] = {0};
    char *a = copy_data(env,catalog->callbacks.ctx,deployment,sizes);
    char *b = copy_data(env,catalog->callbacks.ctx,package_id,sizes + 1);
    char *json = NULL; uint32_t length = 0;
    ud_catalog_binding binding = {.struct_size = sizeof(binding),.abi_version = UD_ABI_VERSION,
        .deployment_id = {a,sizes[0]},.deployment_revision = (uint64_t)revision,.package_id = {b,sizes[1]}};
    ud_status status = a && b ? ud_catalog_binding_snapshot(catalog->catalog,&binding,&json,&length) : UD_RESOURCE;
    jstring result = !status ? utf8_length(env,json,length) : NULL;
    ud_free(a); ud_free(b); ud_free(json);
    if (status) fail(env,status,ud_status_name(status));
    return result;
}
JNIEXPORT void JNICALL Java_org_universaldata_Catalog_nativeProtectParameters(
    JNIEnv *env,jclass type,jlong handle,jbyteArray id,jlong revision,jbyteArray reference) {
    (void)type;
    java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    uint32_t sizes[2] = {0};
    char *a = copy_data(env,catalog->callbacks.ctx,id,sizes);
    char *b = copy_data(env,catalog->callbacks.ctx,reference,sizes + 1);
    ud_catalog_key key = {sizeof(key),UD_ABI_VERSION,UD_CATALOG_AUDIT,{a,sizes[0]},(uint64_t)revision};
    ud_status status = a && b ? ud_catalog_protect_parameters(catalog->catalog,&key,(ud_string){b,sizes[1]}) : UD_RESOURCE;
    ud_free(a); ud_free(b);
    if (status) fail(env,status,ud_status_name(status));
}
JNIEXPORT void JNICALL Java_org_universaldata_Catalog_nativeSecureCompact(JNIEnv *env,jclass type,jlong handle) {
    (void)type; java_catalog *catalog = (java_catalog *)(intptr_t)handle;
    ud_status status = ud_catalog_secure_compact(catalog->catalog);
    if (status) fail(env,status,ud_status_name(status));
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Registry_nativeFeatures(JNIEnv *env,jclass type) {
    (void)env; (void)type; return (jlong)ud_registry_features();
}

/* JNI is a lifecycle projection of the same Native function and Batch contract. */
static ud_status java_function_bind(void *user, ud_context *memory, const ud_field *fields,
                                    uint32_t count, ud_batch **out) {
    *out = NULL;
    java_registration *owner = user;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    ud_status s = UD_OK;
    ud_batch *arguments = NULL;
    char *schema = NULL;
    uint32_t length = 0;
    int frame = (*env)->PushLocalFrame(env, 16) >= 0;
    if (!frame)
        s = UD_RESOURCE;
    if (!s)
        s = ud_function_argument_schema(memory, fields, count, &arguments);
    if (!s && arguments)
        s = ud_schema_to_json(arguments, &schema, &length);
    jbyteArray result = NULL;
    if (!s) {
        jstring input = schema
                            ? utf8_length(env, schema, length)
                            : utf8(env, "{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"fields\":[]}");
        if (!input)
            s = UD_RESOURCE;
        else
            result = (*env)->CallStaticObjectMethod(env, owner->type, owner->prepare,
                                                    owner->factory, input);
    }
    if (!s && (*env)->ExceptionCheck(env)) {
        (*env)->ExceptionClear(env);
        s = ud_context_report(memory, UD_SCHEMA, (ud_string){"Java function binding failed", 28});
    }
    uint32_t n = 0;
    char *text = !s && result ? copy_data(env, memory, result, &n) : NULL;
    if (!s && !text)
        s = UD_SCHEMA;
    if (!s)
        s = ud_schema_from_json(memory, text, n, (ud_string){0}, out);
    ud_free(text);
    ud_free(schema);
    ud_batch_destroy(arguments);
    if (frame)
        (*env)->PopLocalFrame(env, NULL);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return s;
}
static ud_status java_function_evaluate(void *user, ud_context *memory, const ud_field *fields,
                                        const ud_value *values, uint32_t count,
                                        const ud_field *result, ud_value *out) {
    memset(out, 0, sizeof(*out));
    java_registration *owner = user;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    int frame = (*env)->PushLocalFrame(env, 16) >= 0;
    ud_batch *arguments = NULL;
    ud_status s =
        frame ? ud_function_arguments(memory, fields, values, count, &arguments) : UD_RESOURCE;
    if (!s)
        (*env)->CallStaticVoidMethod(env, owner->type, owner->invoke, owner->factory,
                                     (jlong)(intptr_t)memory, (jlong)(intptr_t)arguments,
                                     (jlong)(intptr_t)result, (jlong)(intptr_t)out);
    if ((*env)->ExceptionCheck(env)) {
        (*env)->ExceptionClear(env);
        s = ud_context_report(memory, UD_PARSE, (ud_string){"Java function evaluation failed", 31});
    }
    ud_batch_destroy(arguments);
    if (frame)
        (*env)->PopLocalFrame(env, NULL);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return s;
}
JNIEXPORT void JNICALL Java_org_universaldata_Registry_nativeRegisterFunction(
    JNIEnv *env, jclass type, jlong handle, jbyteArray id, jbyteArray name, jbyteArray version,
    jint minimum, jint maximum, jboolean deterministic, jboolean threads, jobject definition) {
    java_registry *registry = (java_registry *)(intptr_t)handle;
    char *text[3] = {0};
    uint32_t size[3] = {0};
    jbyteArray input[] = {id, name, version};
    ud_status s = UD_OK;
    for (uint32_t j = 0; j < 3; j++) {
        text[j] = copy_data(env, registry->memory, input[j], size + j);
        if (!text[j])
            s = UD_RESOURCE;
    }
    java_registration *owner = !s ? ud_alloc(registry->memory, sizeof(*owner)) : NULL;
    if (!s && !owner)
        s = UD_RESOURCE;
    if (!s) {
        atomic_init(&owner->references, 1);
        owner->memory = registry->memory;
        if ((*env)->GetJavaVM(env, &owner->vm) != JNI_OK)
            s = UD_RESOURCE;
        owner->factory = (*env)->NewGlobalRef(env, definition);
        owner->type = (*env)->NewGlobalRef(env, type);
        owner->prepare = (*env)->GetStaticMethodID(
            env, type, "bindFunction",
            "(Lorg/universaldata/Registry$FunctionDefinition;Ljava/lang/String;)[B");
        owner->invoke =
            (*env)->GetStaticMethodID(env, type, "evaluateFunction",
                                      "(Lorg/universaldata/Registry$FunctionDefinition;JJJJ)V");
        if (!owner->factory || !owner->type || !owner->prepare || !owner->invoke)
            s = UD_RESOURCE;
    }
    ud_component_descriptor descriptor = {
        .struct_size = sizeof(descriptor),
        .abi_version = UD_ABI_VERSION,
        .id = {text[0], size[0]},
        .name = {text[1], size[1]},
        .version = {text[2], size[2]},
        .configuration_schema = {"{}", 2},
        .category = UD_COMPONENT_FUNCTION,
        .synchronous_input = -1,
        .error_output = -1,
        .user = owner,
        .min_arguments = (uint32_t)minimum,
        .max_arguments = (uint32_t)maximum,
        .capabilities = UD_FUNCTION_PURE | (deterministic ? UD_FUNCTION_DETERMINISTIC : 0) |
                        (threads ? UD_COMPONENT_THREAD_SAFE : 0),
        .function_bind = java_function_bind,
        .function_evaluate = java_function_evaluate};
    if (!s)
        s = ud_registry_register_owned(registry->registry, &descriptor, 1, owner, owner_retain,
                                       owner_release);
    if (owner)
        owner_release(owner);
    for (uint32_t j = 0; j < 3; j++)
        ud_free(text[j]);
    if (s)
        fail(env, s, ud_status_name(s));
}
JNIEXPORT void JNICALL Java_org_universaldata_Registry_nativeFunctionCheck(JNIEnv *env, jclass type,
                                                                           jlong memory) {
    (void)type;
    if (ud_is_cancelled((ud_context *)(intptr_t)memory))
        fail(env, UD_CANCELLED, "Function cancelled");
}
JNIEXPORT jstring JNICALL Java_org_universaldata_Registry_nativeFunctionSchema(JNIEnv *env,
                                                                               jclass type,
                                                                               jlong memory,
                                                                               jlong field) {
    (void)type;
    ud_batch *batch = NULL;
    char *text = NULL;
    uint32_t n = 0;
    ud_status s = ud_batch_create((ud_context *)(intptr_t)memory, (const ud_field *)(intptr_t)field,
                                  1, 0, &batch);
    if (!s)
        s = ud_schema_to_json(batch, &text, &n);
    jstring result = !s ? utf8_length(env, text, n) : NULL;
    ud_free(text);
    ud_batch_destroy(batch);
    if (s)
        fail(env, s, ud_status_name(s));
    return result;
}
JNIEXPORT void JNICALL Java_org_universaldata_Registry_nativeFunctionResult(
    JNIEnv *env, jclass type, jlong memory, jlong batch, jlong field, jlong result) {
    (void)type;
    ud_status s =
        ud_function_result((ud_context *)(intptr_t)memory, (const ud_batch *)(intptr_t)batch,
                           (const ud_field *)(intptr_t)field, (ud_value *)(intptr_t)result);
    if (s)
        fail(env, s, ud_status_name(s));
}
JNIEXPORT jlong JNICALL Java_org_universaldata_CallbackBatch_nativeCreate(JNIEnv *env, jclass type,
                                                                          jlong memory,
                                                                          jbyteArray schema,
                                                                          jbyteArray rows) {
    (void)type;
    ud_context *ctx = (ud_context *)(intptr_t)memory;
    uint32_t sn = 0, rn = 0;
    char *sc = copy_data(env, ctx, schema, &sn), *data = copy_data(env, ctx, rows, &rn);
    ud_batch *batch = NULL;
    ud_status s = sc && data
                      ? ud_batch_from_json_schema(ctx, sc, sn, data, rn, UD_ROUND_EXACT, &batch)
                      : UD_RESOURCE;
    ud_free(sc);
    ud_free(data);
    if (s)
        fail(env, s, ud_status_name(s));
    return (jlong)(intptr_t)batch;
}
JNIEXPORT jstring JNICALL Java_org_universaldata_CallbackBatch_nativeText(JNIEnv *env, jclass type,
                                                                          jlong pointer, jint kind,
                                                                          jint row) {
    (void)type;
    ud_batch *batch = (ud_batch *)(intptr_t)pointer;
    char *text = NULL;
    uint32_t n = 0;
    ud_status s = kind == 0   ? ud_schema_to_json(batch, &text, &n)
                  : kind == 1 ? ud_batch_to_json(batch, &text, &n)
                  : row < 0   ? UD_INVALID
                              : ud_batch_row_info_json(batch, (uint32_t)row, &text, &n);
    jstring result = !s ? utf8_length(env, text, n) : NULL;
    ud_free(text);
    if (s)
        fail(env, s, ud_status_name(s));
    return result;
}
JNIEXPORT jint JNICALL Java_org_universaldata_CallbackBatch_nativeCount(JNIEnv *env, jclass type,
                                                                        jlong pointer,
                                                                        jboolean columns) {
    (void)env;
    (void)type;
    ud_batch *batch = (ud_batch *)(intptr_t)pointer;
    return (jint)(columns ? ud_batch_columns(batch) : ud_batch_rows(batch));
}
static ud_status callback_value(JNIEnv *env, jlong pointer, jint row, jint col, ud_value *v) {
    ud_status s = row < 0 || col < 0 ? UD_INVALID
                                     : ud_batch_get((ud_batch *)(intptr_t)pointer, (uint32_t)row,
                                                    (uint32_t)col, v);
    if (!s && v->is_absent)
        s = UD_INCOMPATIBLE;
    if (s)
        fail(env, s, ud_status_name(s));
    return s;
}
JNIEXPORT jboolean JNICALL Java_org_universaldata_CallbackBatch_nativeNull(JNIEnv *env, jclass type,
                                                                           jlong pointer, jint row,
                                                                           jint col) {
    (void)type;
    ud_value v = {0};
    return callback_value(env, pointer, row, col, &v) ? JNI_FALSE
           : v.is_null                                ? JNI_TRUE
                                                      : JNI_FALSE;
}
JNIEXPORT jstring JNICALL Java_org_universaldata_CallbackBatch_nativeUtf8(JNIEnv *env, jclass type,
                                                                          jlong pointer, jint row,
                                                                          jint col) {
    (void)type;
    ud_value v = {0};
    if (callback_value(env, pointer, row, col, &v))
        return NULL;
    if (v.type != UD_UTF8) {
        fail(env, UD_SCHEMA, "Argument is not UTF8");
        return NULL;
    }
    return v.is_null ? NULL : utf8_length(env, v.text, v.text_length);
}
JNIEXPORT jlong JNICALL Java_org_universaldata_CallbackBatch_nativeInt64(JNIEnv *env, jclass type,
                                                                         jlong pointer, jint row,
                                                                         jint col) {
    (void)type;
    ud_value v = {0};
    if (callback_value(env, pointer, row, col, &v))
        return 0;
    if (v.type != UD_INT64 || v.is_null) {
        fail(env, UD_SCHEMA, "Argument is not a present Int64");
        return 0;
    }
    return (jlong)v.integer;
}
JNIEXPORT void JNICALL Java_org_universaldata_CallbackBatch_nativeCopyIdentity(
    JNIEnv *env, jclass type, jlong pointer, jint row, jlong source, jint source_row) {
    (void)type;
    ud_row_info info;
    ud_status s = row < 0 || source_row < 0 ? UD_INVALID
                                            : ud_batch_row_info((const ud_batch *)(intptr_t)source,
                                                                (uint32_t)source_row, &info);
    if (!s)
        s = ud_batch_set_row_info((ud_batch *)(intptr_t)pointer, (uint32_t)row, &info);
    if (s)
        fail(env, s, ud_status_name(s));
}
JNIEXPORT void JNICALL Java_org_universaldata_CallbackBatch_nativeDestroy(JNIEnv *env, jclass type,
                                                                          jlong pointer) {
    (void)env;
    (void)type;
    ud_batch_destroy((ud_batch *)(intptr_t)pointer);
}

typedef struct java_component {
    java_registration *owner;
    jobject component;
    ud_component_context context;
} java_component;
static jobjectArray component_schemas(JNIEnv *env, const ud_batch *const *schemas, uint32_t count) {
    jclass string = (*env)->FindClass(env, "java/lang/String");
    if (!string)
        return NULL;
    jobjectArray out = (*env)->NewObjectArray(env, (jsize)count, string, NULL);
    if (!out)
        return NULL;
    for (uint32_t j = 0; j < count; j++) {
        if (!schemas[j])
            continue;
        char *text = NULL;
        uint32_t n;
        ud_status s = ud_schema_to_json(schemas[j], &text, &n);
        if (s) {
            fail(env, s, ud_status_name(s));
            return NULL;
        }
        jstring value = utf8_length(env, text, n);
        ud_free(text);
        if (!value)
            return NULL;
        (*env)->SetObjectArrayElement(env, out, (jsize)j, value);
        (*env)->DeleteLocalRef(env, value);
    }
    return out;
}
static ud_status java_component_configure(void *user, const ud_component_context *context,
                                          ud_string config, const ud_batch *const *inputs,
                                          ud_batch **outputs) {
    java_registration *owner = user;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    int frame = (*env)->PushLocalFrame(env, 32) >= 0;
    ud_status s = frame ? UD_OK : UD_RESOURCE;
    jobjectArray result = NULL;
    if (!s) {
        jobjectArray schemas = component_schemas(env, inputs, context->input_count);
        jstring configuration = utf8_length(env, config.data, config.length);
        if (!schemas || !configuration)
            s = UD_RESOURCE;
        else
            result = (*env)->CallStaticObjectMethod(env, owner->type, owner->configure,
                                                    owner->factory, configuration, schemas);
    }
    if (!s)
        s = callback_failure(env, context->memory);
    if (!s && (!result || (uint32_t)(*env)->GetArrayLength(env, result) != context->output_count))
        s = UD_SCHEMA;
    char *scope = NULL;
    uint32_t scope_size = 0;
    if (!s)
        s = ud_component_schema_namespace(context, &scope, &scope_size);
    for (uint32_t j = 0; !s && j < context->output_count; j++) {
        jbyteArray schema = (*env)->GetObjectArrayElement(env, result, (jsize)j);
        uint32_t n = 0;
        char *text = schema ? copy_data(env, context->memory, schema, &n) : NULL;
        s = text ? ud_schema_from_json(context->memory, text, n, (ud_string){scope, scope_size},
                                       outputs + j)
                 : UD_SCHEMA;
        ud_free(text);
        if (schema)
            (*env)->DeleteLocalRef(env, schema);
    }
    ud_free(scope);
    if (frame)
        (*env)->PopLocalFrame(env, NULL);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return s;
}
static ud_status java_component_prepare(void *user, const ud_component_context *context,
                                        ud_string config, const ud_batch *const *inputs,
                                        const ud_batch *const *outputs, void **out) {
    *out = NULL;
    java_registration *owner = user;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    int frame = (*env)->PushLocalFrame(env, 32) >= 0;
    ud_status s = frame ? UD_OK : UD_RESOURCE;
    java_component *instance = !s ? ud_alloc(context->memory, sizeof(*instance)) : NULL;
    if (!s && !instance)
        s = UD_RESOURCE;
    if (!s) {
        instance->owner = owner;
        instance->context = *context;
        jobjectArray in = component_schemas(env, inputs, context->input_count),
                     output = component_schemas(env, outputs, context->output_count);
        jstring configuration = utf8_length(env, config.data, config.length);
        jobject value = in && output && configuration
                            ? (*env)->CallStaticObjectMethod(
                                  env, owner->type, owner->prepare, owner->factory,
                                  (jlong)(intptr_t)&instance->context, configuration, in, output)
                            : NULL;
        s = callback_failure(env, context->memory);
        if (!s && !value)
            s = UD_RESOURCE;
        if (!s) {
            instance->component = (*env)->NewGlobalRef(env, value);
            if (!instance->component) {
                (*env)->CallStaticVoidMethod(env, owner->type, owner->lifecycle, value,
                                             (jlong)(intptr_t)&instance->context, 2);
                callback_failure(env, context->memory);
                s = UD_RESOURCE;
            }
        }
    }
    if (s)
        ud_free(instance);
    else
        *out = instance;
    if (frame)
        (*env)->PopLocalFrame(env, NULL);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return s;
}
static ud_status java_component_step(void *pointer, const ud_component_event *event) {
    java_component *instance = pointer;
    java_registration *owner = instance->owner;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    int frame = (*env)->PushLocalFrame(env, 16) >= 0;
    ud_status s = frame ? UD_OK : UD_RESOURCE;
    if (!s) {
        jstring position =
            utf8_length(env, event->source_position.data ? event->source_position.data : "",
                        event->source_position.length);
        if (!position)
            s = UD_RESOURCE;
        else
            s = (*env)->CallStaticIntMethod(env, owner->type, owner->invoke, instance->component,
                                            (jlong)(intptr_t)&instance->context, (jint)event->kind,
                                            (jint)event->input, (jlong)(intptr_t)event->batch,
                                            (jlong)event->epoch, (jlong)event->schema_version,
                                            (jlong)event->watermark, position);
        ud_status error = callback_failure(env, instance->context.memory);
        if (error)
            s = error;
    }
    if (frame)
        (*env)->PopLocalFrame(env, NULL);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return s;
}
static ud_status component_lifecycle(java_component *instance, int action) {
    java_registration *owner = instance->owner;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    int frame = (*env)->PushLocalFrame(env, 16) >= 0;
    ud_status s = frame ? UD_OK : UD_RESOURCE;
    if (!s) {
        (*env)->CallStaticVoidMethod(env, owner->type, owner->lifecycle, instance->component,
                                     (jlong)(intptr_t)&instance->context, (jint)action);
        s = callback_failure(env, instance->context.memory);
    }
    if (frame)
        (*env)->PopLocalFrame(env, NULL);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return s;
}
static ud_status java_component_finish(void *pointer) {
    return component_lifecycle(pointer, 0);
}
static ud_status java_component_confirm(void *pointer) {
    return component_lifecycle(pointer, 3);
}
static void java_component_cancel(void *pointer) {
    java_component *instance = pointer;
    ud_status s = component_lifecycle(instance, 1);
    if (s)
        ud_activation_report_fault(instance->context.activation, s,
                                   (ud_string){"Java component cancel failed", 28});
}
static void java_component_dispose(void *pointer, ud_status result) {
    (void)result;
    java_component *instance = pointer;
    java_registration *owner = instance->owner;
    ud_status s = component_lifecycle(instance, 2);
    if (s)
        ud_activation_report_fault(instance->context.activation, s,
                                   (ud_string){"Java component disposal failed", 30});
    JNIEnv *env = NULL;
    int attached = 0;
    if (owner_attach(owner, &env, &attached)) {
        (*env)->DeleteGlobalRef(env, instance->component);
        if (attached)
            (*owner->vm)->DetachCurrentThread(owner->vm);
    }
    ud_free(instance);
}
static ud_status java_component_checkpoint(void *pointer, ud_batch **out) {
    *out = NULL;
    java_component *instance = pointer;
    java_registration *owner = instance->owner;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    int frame = (*env)->PushLocalFrame(env, 16) >= 0;
    ud_status s = frame ? UD_OK : UD_RESOURCE;
    if (!s) {
        jlong batch =
            (*env)->CallStaticLongMethod(env, owner->type, owner->checkpoint, instance->component,
                                         (jlong)(intptr_t)&instance->context);
        s = callback_failure(env, instance->context.memory);
        if (!s)
            *out = (ud_batch *)(intptr_t)batch;
    }
    if (frame)
        (*env)->PopLocalFrame(env, NULL);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return s;
}
static ud_status java_component_restore(void *pointer, const ud_batch *state) {
    java_component *instance = pointer;
    java_registration *owner = instance->owner;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    int frame = (*env)->PushLocalFrame(env, 16) >= 0;
    ud_status s = frame ? UD_OK : UD_RESOURCE;
    if (!s) {
        (*env)->CallStaticVoidMethod(env, owner->type, owner->restore, instance->component,
                                     (jlong)(intptr_t)&instance->context, (jlong)(intptr_t)state);
        s = callback_failure(env, instance->context.memory);
    }
    if (frame)
        (*env)->PopLocalFrame(env, NULL);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return s;
}
static ud_status java_component_epoch(void *pointer, uint64_t epoch, ud_string position) {
    java_component *instance = pointer;
    java_registration *owner = instance->owner;
    JNIEnv *env = NULL;
    int attached = 0;
    if (!owner_attach(owner, &env, &attached))
        return UD_RESOURCE;
    int frame = (*env)->PushLocalFrame(env, 16) >= 0;
    ud_status s = frame ? UD_OK : UD_RESOURCE;
    if (!s) {
        jstring text = utf8_length(env, position.data ? position.data : "", position.length);
        if (!text)
            s = UD_RESOURCE;
        else {
            (*env)->CallStaticVoidMethod(env, owner->type, owner->epoch, instance->component,
                                         (jlong)epoch, text);
            s = callback_failure(env, instance->context.memory);
        }
    }
    if (frame)
        (*env)->PopLocalFrame(env, NULL);
    if (attached)
        (*owner->vm)->DetachCurrentThread(owner->vm);
    return s;
}
JNIEXPORT void JNICALL Java_org_universaldata_Registry_nativeRegisterComponent(
    JNIEnv *env, jclass type, jlong handle, jbyteArray id, jbyteArray name, jbyteArray version,
    jbyteArray schema, jint category, jint inputs, jint outputs, jint synchronous, jint error,
    jlong optional_inputs, jlong optional_outputs, jboolean replay, jboolean threads,
    jboolean stateful, jboolean blocking, jint state_version, jint effects, jboolean controls,
    jboolean final_ack, jboolean epoch_ack, jobject definition) {
    java_registry *registry = (java_registry *)(intptr_t)handle;
    char *text[4] = {0};
    uint32_t size[4] = {0};
    jbyteArray data[] = {id, name, version, schema};
    ud_status s = UD_OK;
    for (uint32_t j = 0; j < 4; j++) {
        text[j] = copy_data(env, registry->memory, data[j], size + j);
        if (!text[j])
            s = UD_RESOURCE;
    }
    java_registration *owner = !s ? ud_alloc(registry->memory, sizeof(*owner)) : NULL;
    if (!s && !owner)
        s = UD_RESOURCE;
    if (!s) {
        atomic_init(&owner->references, 1);
        owner->memory = registry->memory;
        if ((*env)->GetJavaVM(env, &owner->vm) != JNI_OK)
            s = UD_RESOURCE;
        owner->factory = (*env)->NewGlobalRef(env, definition);
        owner->type = (*env)->NewGlobalRef(env, type);
        owner->configure =
            (*env)->GetStaticMethodID(env, type, "configureComponent",
                                      "(Lorg/universaldata/Registry$DataFlowDefinition;Ljava/lang/"
                                      "String;[Ljava/lang/String;)[[B");
        owner->prepare = (*env)->GetStaticMethodID(
            env, type, "prepareComponent",
            "(Lorg/universaldata/Registry$DataFlowDefinition;JLjava/lang/String;[Ljava/lang/"
            "String;[Ljava/lang/String;)Lorg/universaldata/DataFlow;");
        owner->invoke = (*env)->GetStaticMethodID(
            env, type, "stepComponent", "(Lorg/universaldata/DataFlow;JIIJJJJLjava/lang/String;)I");
        owner->lifecycle = (*env)->GetStaticMethodID(env, type, "lifecycleComponent",
                                                     "(Lorg/universaldata/DataFlow;JI)V");
        owner->checkpoint = (*env)->GetStaticMethodID(env, type, "checkpointComponent",
                                                      "(Lorg/universaldata/DataFlow;J)J");
        owner->restore = (*env)->GetStaticMethodID(env, type, "restoreComponent",
                                                   "(Lorg/universaldata/DataFlow;JJ)V");
        owner->epoch =
            (*env)->GetStaticMethodID(env, type, "confirmEpochComponent",
                                      "(Lorg/universaldata/DataFlow;JLjava/lang/String;)V");
        if (!owner->factory || !owner->type || !owner->configure || !owner->prepare ||
            !owner->invoke || !owner->lifecycle || !owner->checkpoint || !owner->restore ||
            !owner->epoch)
            s = UD_RESOURCE;
    }
    ud_component_descriptor descriptor = {
        .struct_size = sizeof(descriptor),
        .abi_version = UD_ABI_VERSION,
        .id = {text[0], size[0]},
        .name = {text[1], size[1]},
        .version = {text[2], size[2]},
        .configuration_schema = {text[3], size[3]},
        .category = category,
        .inputs = (uint32_t)inputs,
        .outputs = (uint32_t)outputs,
        .synchronous_input = synchronous,
        .error_output = error,
        .optional_inputs = (uint64_t)optional_inputs,
        .optional_outputs = (uint64_t)optional_outputs,
        .state_version = (uint32_t)state_version,
        .effects = (uint32_t)effects,
        .user = owner,
        .capabilities =
            (replay ? UD_COMPONENT_REPLAY_SAFE : 0) | (threads ? UD_COMPONENT_THREAD_SAFE : 0) |
            (stateful ? UD_COMPONENT_STATEFUL : 0) | (blocking ? UD_COMPONENT_BLOCKING : 0) |
            (controls ? UD_COMPONENT_CONTROL_EVENTS : 0),
        .configure = java_component_configure,
        .prepare = java_component_prepare,
        .step = java_component_step,
        .finish = java_component_finish,
        .cancel = java_component_cancel,
        .dispose = java_component_dispose,
        .checkpoint = stateful ? java_component_checkpoint : NULL,
        .restore = stateful ? java_component_restore : NULL,
        .confirm = final_ack ? java_component_confirm : NULL,
        .confirm_epoch = epoch_ack ? java_component_epoch : NULL};
    if (!s)
        s = ud_registry_register_owned(registry->registry, &descriptor, 1, owner, owner_retain,
                                       owner_release);
    if (owner)
        owner_release(owner);
    for (uint32_t j = 0; j < 4; j++)
        ud_free(text[j]);
    if (s)
        fail(env, s, ud_status_name(s));
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Registry_nativeRetainComponentBatch(JNIEnv *env,
                                                                                   jclass type,
                                                                                   jlong pointer) {
    (void)type;
    ud_batch *batch = (ud_batch *)(intptr_t)pointer;
    ud_status s = ud_batch_seal(batch);
    if (!s)
        s = ud_batch_retain(batch);
    if (s) {
        fail(env, s, ud_status_name(s));
        return 0;
    }
    return pointer;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_DataFlowContext_nativeMemory(JNIEnv *env,
                                                                            jclass type,
                                                                            jlong pointer) {
    (void)env;
    (void)type;
    return (jlong)(intptr_t)((const ud_component_context *)(intptr_t)pointer)->memory;
}
JNIEXPORT void JNICALL Java_org_universaldata_DataFlowContext_nativeCheck(JNIEnv *env, jclass type,
                                                                          jlong pointer) {
    (void)type;
    ud_status s =
        ud_activation_check(((const ud_component_context *)(intptr_t)pointer)->activation);
    if (s)
        fail(env, s, ud_status_name(s));
}
JNIEXPORT jstring JNICALL Java_org_universaldata_DataFlowContext_nativeText(JNIEnv *env,
                                                                            jclass type,
                                                                            jlong pointer,
                                                                            jint kind, jint port) {
    (void)type;
    const ud_component_context *c = (const ud_component_context *)(intptr_t)pointer;
    ud_string value = kind == 0   ? c->task_id
                      : kind == 1 ? c->pipeline_id
                      : kind == 2 ? c->component_id
                                  : c->recovery_position;
    if (kind < 4)
        return utf8_length(env, value.data ? value.data : "", value.length);
    if (port < 0 || (uint32_t)port >= (kind == 4 ? c->output_count : c->input_count)) {
        fail(env, UD_INVALID, "Invalid component port");
        return NULL;
    }
    if (kind == 5) {
        const ud_component_plan *plan = c->input_plans + (uint32_t)port;
        return utf8_length(env, plan->physical_tree.data ? plan->physical_tree.data : "",
                           plan->physical_tree.length);
    }
    char *text = NULL;
    uint32_t length = 0;
    ud_status s = ud_schema_to_json(c->output_schemas[port], &text, &length);
    jstring out = !s ? utf8_length(env, text, length) : NULL;
    ud_free(text);
    if (s)
        fail(env, s, ud_status_name(s));
    return out;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_DataFlowContext_nativeNumber(JNIEnv *env,
                                                                            jclass type,
                                                                            jlong pointer,
                                                                            jint kind) {
    (void)env;
    (void)type;
    const ud_component_context *c = (const ud_component_context *)(intptr_t)pointer;
    return kind == 0   ? (jlong)c->recovery_epoch
           : kind == 1 ? c->recovery_terminal
                       : (jlong)(c->recommended_batch_rows ? c->recommended_batch_rows(c->emit_user)
                                                           : c->max_batch_rows);
}
JNIEXPORT void JNICALL Java_org_universaldata_DataFlowContext_nativeEmit(JNIEnv *env, jclass type,
                                                                         jlong pointer, jint output,
                                                                         jlong batch) {
    (void)type;
    const ud_component_context *c = (const ud_component_context *)(intptr_t)pointer;
    ud_batch *value = (ud_batch *)(intptr_t)batch;
    ud_status s =
        output < 0 || (uint32_t)output >= c->output_count ? UD_INVALID : ud_batch_seal(value);
    if (!s)
        s = c->emit(c->emit_user, (uint32_t)output, value);
    if (s)
        fail(env, s, ud_status_name(s));
}
JNIEXPORT void JNICALL Java_org_universaldata_DataFlowContext_nativeEmitControl(
    JNIEnv *env, jclass type, jlong pointer, jint output, jint kind, jlong batch, jlong epoch,
    jlong schema_version, jlong watermark, jbyteArray position) {
    (void)type;
    const ud_component_context *c = (const ud_component_context *)(intptr_t)pointer;
    uint32_t n = 0;
    char *text = copy_data(env, c->memory, position, &n);
    ud_status s = text && output >= 0 && c->emit_event ? UD_OK : UD_INVALID;
    if (!s && batch)
        s = ud_batch_seal((ud_batch *)(intptr_t)batch);
    ud_component_event event = {.struct_size = sizeof(event),
                                .abi_version = UD_ABI_VERSION,
                                .kind = kind,
                                .batch = (ud_batch *)(intptr_t)batch,
                                .epoch = (uint64_t)epoch,
                                .schema_version = (uint64_t)schema_version,
                                .watermark = watermark,
                                .source_position = {text, n}};
    if (!s)
        s = c->emit_event(c->emit_user, (uint32_t)output, &event);
    ud_free(text);
    if (s)
        fail(env, s, ud_status_name(s));
}
JNIEXPORT void JNICALL Java_org_universaldata_DataFlowContext_nativeSourceIdentity(
    JNIEnv *env, jclass type, jlong pointer, jlong batch, jint row, jlong position,
    jbyteArray source) {
    (void)type;
    const ud_component_context *c = (const ud_component_context *)(intptr_t)pointer;
    uint32_t n = 0;
    char *text = copy_data(env, c->memory, source, &n);
    ud_status s = text && row >= 0 && position >= 0
                      ? ud_component_source_identity(c, (ud_batch *)(intptr_t)batch, (uint32_t)row,
                                                     (uint64_t)position, (ud_string){text, n})
                      : UD_INVALID;
    ud_free(text);
    if (s)
        fail(env, s, ud_status_name(s));
}
JNIEXPORT jlong JNICALL Java_org_universaldata_DataFlowContext_nativeErrorBatch(
    JNIEnv *env, jclass type, jlong pointer, jint capacity, jboolean include) {
    (void)type;
    const ud_component_context *c = (const ud_component_context *)(intptr_t)pointer;
    ud_batch *batch = NULL;
    ud_status s = capacity < 0 ? UD_INVALID : ud_error_batch(&c->errors, include, capacity, &batch);
    if (s)
        fail(env, s, ud_status_name(s));
    return (jlong)(intptr_t)batch;
}
JNIEXPORT void JNICALL Java_org_universaldata_DataFlowContext_nativeAppendError(
    JNIEnv *env, jclass type, jlong pointer, jlong errors, jlong input, jint row, jbyteArray rule,
    jint version, jbyteArray field, jint code) {
    (void)type;
    const ud_component_context *c = (const ud_component_context *)(intptr_t)pointer;
    uint32_t n = 0, fn = 0;
    char *r = copy_data(env, c->memory, rule, &n), *f = copy_data(env, c->memory, field, &fn);
    ud_id id = {0};
    ud_status s = r && f && row >= 0 && version > 0 ? UD_OK : UD_INVALID;
    if (!s && fn)
        s = ud_id_parse((ud_string){f, fn}, &id);
    ud_record_error record = {.struct_size = sizeof(record),
                              .abi_version = UD_ABI_VERSION,
                              .input = (const ud_batch *)(intptr_t)input,
                              .row = (uint32_t)row,
                              .rule = {r, n},
                              .rule_version = (uint32_t)version,
                              .column = id,
                              .code = code,
                              .policy = UD_ERROR_REDIRECT};
    if (!s)
        s = ud_error_append(&c->errors, (ud_batch *)(intptr_t)errors, &record);
    ud_free(r);
    ud_free(f);
    if (s)
        fail(env, s, ud_status_name(s));
}

JNIEXPORT jstring JNICALL Java_org_universaldata_Execution_nativeExpression(
    JNIEnv *env, jclass type, jlong handle, jbyteArray schema, jbyteArray source) {
    (void)type;
    java_execution *execution = (java_execution *)(intptr_t)handle;
    uint32_t sn = 0, en = 0, n = 0;
    char *a = copy_data(env, execution->ctx, schema, &sn),
         *b = copy_data(env, execution->ctx, source, &en), *result = NULL;
    ud_status status =
        a && b ? ud_execution_compile_expression(execution->execution, (ud_string){a, sn},
                                                 (ud_string){b, en}, &result, &n)
               : UD_RESOURCE;
    jstring out = !status ? utf8_length(env, result, n) : NULL;
    ud_free(a);
    ud_free(b);
    ud_free(result);
    if (status)
        fail(env, status, ud_status_name(status));
    return out;
}
JNIEXPORT jlongArray JNICALL Java_org_universaldata_Batch_nativeDateTime(JNIEnv *env, jclass type,
                                                                         jlong handle, jint row,
                                                                         jint column) {
    (void)type;
    java_data *batch = (java_data *)(intptr_t)handle;
    ud_value value = {0};
    ud_status status = row < 0 || column < 0
                           ? UD_INVALID
                           : ud_batch_get(batch->batch, (uint32_t)row, (uint32_t)column, &value);
    if (!status && (value.type != UD_DATETIME || value.is_null || value.is_absent))
        status = UD_INCOMPATIBLE;
    if (status) {
        fail(env, status, ud_status_name(status));
        return NULL;
    }
    jlong parts[] = {value.datetime.ticks, value.datetime.offset_minutes,
                     value.datetime.has_offset};
    jlongArray result = (*env)->NewLongArray(env, 3);
    if (result)
        (*env)->SetLongArrayRegion(env, result, 0, 3, parts);
    return result;
}
