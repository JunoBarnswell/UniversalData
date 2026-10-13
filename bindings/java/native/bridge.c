#include <jni.h>
#include "ud/package.h"
#include "ud/format.h"
#include "ud/compute.h"
#include <stdlib.h>
#include <string.h>
JNIEXPORT jint JNICALL Java_org_universaldata_ProductVersion_nativeAbi(JNIEnv *e, jclass cls) {
    (void)e;
    (void)cls;
    return UD_ABI_VERSION;
}
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
typedef struct java_execution {
    ud_context *ctx;
    ud_execution *execution;
    JavaVM *vm;
    jobject authorizer, resolver;
    jmethodID allow, resolve;
} java_execution;
static int attach(java_execution *j, JNIEnv **env, int *attached) {
    *attached = 0;
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
    ud_free(j);
    ud_status clean = ud_context_destroy(ctx);
    return status ? status : clean;
}
JNIEXPORT jlong JNICALL Java_org_universaldata_Execution_nativeOpen(
    JNIEnv *e, jclass cls, jbyteArray document, jbyteArray tenant, jobject authorizer,
    jbyteArray parameters, jbyteArray identity, jobject resolver, jint capacity, jint rows,
    jlong limit, jlong timeout, jbyteArray state, jboolean resume, jint max_concurrency) {
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
        ud_execution_options options = {.struct_size = sizeof(options),
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
                                        .max_concurrency = (uint32_t)max_concurrency};
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
