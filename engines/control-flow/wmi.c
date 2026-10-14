#include "../package/internal.h"
#include "ud/connection_manager.h"
#include <limits.h>
#ifdef _WIN32
#define COBJMACROS
#include <windows.h>
#include <oleauto.h>
#include <wbemidl.h>
#endif
typedef struct wmi_config {
    ud_string connection, query, output;
    uint64_t maximum, timeout, rows;
} wmi_config;
static ud_status parse(const ud_component_context *context, ud_string text, ud_json **doc,
                       wmi_config *config) {
    *config = (wmi_config){.maximum = 1048576, .timeout = 60000, .rows = 1000};
    UD_TRY(ud_json_parse(context->memory, text.data, text.length, doc, NULL));
    static const char *keys[] = {"connection",       "query",    "resultVariable", "maxRows",
                                 "maxResponseBytes", "timeoutMs"};
    UD_TRY(ud_json_keys(*doc, keys, 6));
    UD_TRY(ud_json_string(ud_member(*doc, "connection"), &config->connection));
    UD_TRY(ud_json_string(ud_member(*doc, "query"), &config->query));
    UD_TRY(ud_json_string(ud_member(*doc, "resultVariable"), &config->output));
    if (!config->query.length || config->query.length > 65536 ||
        memchr(config->query.data, 0, config->query.length))
        return UD_SCHEMA;
    const char *numbers[] = {"maxRows", "maxResponseBytes", "timeoutMs"};
    uint64_t *values[] = {&config->rows, &config->maximum, &config->timeout};
    for (uint32_t at = 0; at < 3; at++)
        if (ud_member(*doc, numbers[at]))
            UD_TRY(ud_json_uint(ud_member(*doc, numbers[at]), values[at]));
    if (!config->rows || config->rows > 65536 || !config->maximum || config->maximum > 67108864 ||
        !config->timeout || config->timeout > 86400000)
        return UD_SCHEMA;
    ud_variable *variable;
    UD_TRY(ud_variable_writable(context->activation, config->output, &variable));
    if (variable->value.type != UD_UTF8)
        return UD_SCHEMA;
    const ud_json *definition;
    int owner;
    UD_TRY(ud_connection_definition(context->activation, config->connection, &definition, &owner));
    ud_string driver, resource;
    UD_TRY(ud_json_string(ud_member(definition, "driver"), &driver));
    UD_TRY(ud_json_string(ud_member(definition, "resource"), &resource));
    if (!ud_named(driver, "wmi") || resource.length < 5 || memcmp(resource.data, "root\\", 5) ||
        memchr(resource.data, 0, resource.length) || ud_member(definition, "credentialReference"))
        return UD_UNSUPPORTED;
#ifndef _WIN32
    return UD_UNSUPPORTED;
#else
    return UD_OK;
#endif
}
static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    (void)user;
    (void)in;
    (void)out;
    ud_json *doc = NULL;
    wmi_config config;
    ud_status status = parse(context, text, &doc, &config);
    ud_json_destroy(doc);
    return status;
}
#ifdef _WIN32
static BSTR wide(ud_string value) {
    int count =
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value.data, (int)value.length, NULL, 0);
    if (count <= 0)
        return NULL;
    BSTR out = SysAllocStringLen(NULL, (UINT)count);
    if (out)
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value.data, (int)value.length, out,
                            count);
    return out;
}
static ud_status quote_wide(ud_bytes *bytes, BSTR value) {
    int count = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, value, (int)SysStringLen(value),
                                    NULL, 0, NULL, NULL);
    if (count < 0)
        return UD_PARSE;
    char *text = ud_alloc(bytes->ctx, (size_t)count + 1);
    if (!text)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    if (count && !WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, value,
                                      (int)SysStringLen(value), text, count, NULL, NULL))
        status = UD_PARSE;
    if (!status)
        status = ud_json_quote(bytes, text, (uint32_t)count);
    ud_free(text);
    return status;
}
static ud_status object_json_depth(ud_bytes *, IWbemClassObject *, uint32_t);
static ud_status variant_json(ud_bytes *bytes, const VARIANT *value, CIMTYPE cim, uint32_t depth) {
    if (depth > 32)
        return UD_RESOURCE;
    if (V_VT(value) == VT_EMPTY || V_VT(value) == VT_NULL)
        return ud_bytes_put(bytes, "null", 4);
    if (V_VT(value) & VT_BYREF)
        return UD_UNSUPPORTED;
    if (V_VT(value) & VT_ARRAY) {
        SAFEARRAY *array = V_ARRAY(value);
        LONG first = 0, last = -1;
        if (!array || SafeArrayGetDim(array) != 1 ||
            FAILED(SafeArrayGetLBound(array, 1, &first)) ||
            FAILED(SafeArrayGetUBound(array, 1, &last)))
            return UD_SCHEMA;
        if ((int64_t)last - first + 1 > 65536)
            return UD_RESOURCE;
        VARTYPE type = V_VT(value) & VT_TYPEMASK;
        if (type != VT_VARIANT && type != VT_I1 && type != VT_UI1 && type != VT_I2 &&
            type != VT_UI2 && type != VT_I4 && type != VT_UI4 && type != VT_I8 &&
            type != VT_UI8 && type != VT_R4 && type != VT_R8 && type != VT_BOOL &&
            type != VT_DATE && type != VT_BSTR && type != VT_UNKNOWN && type != VT_DISPATCH)
            return UD_UNSUPPORTED;
        ud_status status = ud_bytes_put(bytes, "[", 1);
        for (LONG index = first; !status && index <= last; index++) {
            VARIANT element;
            VariantInit(&element);
            void *output;
            if (type == VT_VARIANT)
                output = &element;
            else {
                V_VT(&element) = type;
                output = &V_I8(&element); /* VARIANT scalar payload shares this union address. */
            }
            if (FAILED(SafeArrayGetElement(array, &index, output)))
                status = UD_IO;
            if (!status && index != first)
                status = ud_bytes_put(bytes, ",", 1);
            if (!status)
                status = variant_json(bytes, &element, cim & ~CIM_FLAG_ARRAY, depth + 1);
            VariantClear(&element);
            if (index == LONG_MAX)
                break;
        }
        return status ? status : ud_bytes_put(bytes, "]", 1);
    }
    if (cim == CIM_OBJECT && V_VT(value) == VT_UNKNOWN && V_UNKNOWN(value)) {
        IWbemClassObject *object = NULL;
        HRESULT result = IUnknown_QueryInterface(V_UNKNOWN(value), &IID_IWbemClassObject,
                                                 (void **)&object);
        if (FAILED(result))
            return UD_SCHEMA;
        ud_status status = object_json_depth(bytes, object, depth + 1);
        IWbemClassObject_Release(object);
        return status;
    }
    if (V_VT(value) == VT_BOOL)
        return ud_bytes_put(bytes, V_BOOL(value) ? "true" : "false", V_BOOL(value) ? 4 : 5);
    if (V_VT(value) == VT_BSTR && cim != CIM_SINT64 && cim != CIM_UINT64)
        return quote_wide(bytes, V_BSTR(value));
    VARIANT text;
    VariantInit(&text);
    HRESULT hr = VariantChangeTypeEx(&text, (VARIANT *)value, LOCALE_INVARIANT, 0, VT_BSTR);
    if (FAILED(hr))
        return UD_UNSUPPORTED;
    BSTR string = V_BSTR(&text);
    int length = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, string,
                                     (int)SysStringLen(string), NULL, 0, NULL, NULL);
    char *encoded = length >= 0 ? ud_alloc(bytes->ctx, (size_t)length + 1) : NULL;
    ud_status status = encoded ? UD_OK : UD_RESOURCE;
    if (!status && length &&
        !WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, string, (int)SysStringLen(string),
                             encoded, length, NULL, NULL))
        status = UD_PARSE;
    ud_value parsed;
    ud_type type = cim == CIM_UINT64                          ? UD_UINT64
                   : (cim == CIM_REAL32 || cim == CIM_REAL64) ? UD_FLOAT64
                                                              : UD_INT64;
    if (!status)
        status = ud_value_parse(bytes->ctx, type, encoded, (uint32_t)length, &parsed);
    if (!status)
        status = ud_bytes_put(bytes, encoded, (size_t)length);
    ud_free(encoded);
    VariantClear(&text);
    return status;
}
static ud_status object_json_depth(ud_bytes *bytes, IWbemClassObject *object, uint32_t depth) {
    if (depth > 32)
        return UD_RESOURCE;
    HRESULT hr = IWbemClassObject_BeginEnumeration(object, WBEM_FLAG_NONSYSTEM_ONLY);
    if (FAILED(hr))
        return UD_IO;
    ud_status status = ud_bytes_put(bytes, "{", 1);
    uint32_t count = 0;
    while (!status) {
        BSTR name = NULL;
        VARIANT value;
        VariantInit(&value);
        CIMTYPE type = 0;
        LONG flavor;
        hr = IWbemClassObject_Next(object, 0, &name, &value, &type, &flavor);
        if (hr == WBEM_S_NO_MORE_DATA) {
            VariantClear(&value);
            SysFreeString(name);
            break;
        }
        if (FAILED(hr))
            status = UD_IO;
        if (!status && count >= 65536)
            status = UD_RESOURCE;
        if (!status && count++)
            status = ud_bytes_put(bytes, ",", 1);
        if (!status)
            status = quote_wide(bytes, name);
        if (!status)
            status = ud_bytes_put(bytes, ":", 1);
        if (!status)
            status = variant_json(bytes, &value, type, depth);
        VariantClear(&value);
        SysFreeString(name);
    }
    (void)IWbemClassObject_EndEnumeration(object);
    if (!status)
        status = ud_bytes_put(bytes, "}", 1);
    return status;
}
static ud_status object_json(ud_bytes *bytes, IWbemClassObject *object) {
    return object_json_depth(bytes, object, 0);
}
static ud_status run(const ud_component_context *context, ud_connection_lease *lease,
                     const wmi_config *config, int event, ud_bytes *bytes) {
    HRESULT initialized = CoInitializeEx(NULL, COINIT_MULTITHREADED);
    if (FAILED(initialized))
        return UD_UNSUPPORTED;
    HRESULT hr = CoInitializeSecurity(NULL, -1, NULL, NULL, RPC_C_AUTHN_LEVEL_PKT_PRIVACY,
                                      RPC_C_IMP_LEVEL_IMPERSONATE, NULL, EOAC_NONE, NULL);
    ud_status status = FAILED(hr) && hr != RPC_E_TOO_LATE ? UD_AUTH : UD_OK;
    IWbemLocator *locator = NULL;
    IWbemServices *service = NULL;
    IEnumWbemClassObject *result = NULL;
    ud_string resource;
    BSTR space = NULL, query = NULL, language = NULL;
    if (!status)
        status = ud_connection_lease_resource(lease, &resource);
    if (!status) {
        space = wide(resource);
        query = wide(config->query);
        language = SysAllocString(L"WQL");
        if (!space || !query || !language)
            status = UD_RESOURCE;
    }
    if (!status) {
        hr = CoCreateInstance(&CLSID_WbemLocator, NULL, CLSCTX_INPROC_SERVER, &IID_IWbemLocator,
                              (void **)&locator);
        if (FAILED(hr))
            status = UD_IO;
    }
    if (!status) {
        hr = IWbemLocator_ConnectServer(locator, space, NULL, NULL, NULL,
                                        WBEM_FLAG_CONNECT_USE_MAX_WAIT, NULL, NULL, &service);
        if (FAILED(hr))
            status = UD_AUTH;
    }
    if (!status) {
        hr = CoSetProxyBlanket((IUnknown *)service, RPC_C_AUTHN_WINNT, RPC_C_AUTHZ_NONE, NULL,
                               RPC_C_AUTHN_LEVEL_PKT_PRIVACY, RPC_C_IMP_LEVEL_IMPERSONATE, NULL,
                               EOAC_NONE);
        if (FAILED(hr))
            status = UD_AUTH;
    }
    if (!status) {
        hr = event ? IWbemServices_ExecNotificationQuery(
                         service, language, query,
                         WBEM_FLAG_RETURN_IMMEDIATELY | WBEM_FLAG_FORWARD_ONLY, NULL, &result)
                   : IWbemServices_ExecQuery(service, language, query,
                                             WBEM_FLAG_RETURN_IMMEDIATELY | WBEM_FLAG_FORWARD_ONLY,
                                             NULL, &result);
        if (FAILED(hr))
            status = UD_SCHEMA;
    }
    if (!status)
        status = ud_bytes_put(bytes, "[", 1);
    uint64_t deadline = ud_now_ms() + config->timeout, count = 0;
    while (!status && count < config->rows) {
        status = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
        if (!status && ud_now_ms() >= deadline)
            status = UD_TIMEOUT;
        if (status)
            break;
        IWbemClassObject *object = NULL;
        ULONG returned = 0;
        hr = IEnumWbemClassObject_Next(result, 100, 1, &object, &returned);
        if (FAILED(hr))
            status = UD_IO;
        else if (!returned && hr == WBEM_S_FALSE)
            break;
        else if (returned) {
            if (count++)
                status = ud_bytes_put(bytes, ",", 1);
            if (!status)
                status = object_json(bytes, object);
        }
        if (object)
            IWbemClassObject_Release(object);
    }
    if (!status && !event && count == config->rows) {
        IWbemClassObject *extra = NULL;
        ULONG returned = 0;
        hr = IEnumWbemClassObject_Next(result, 100, 1, &extra, &returned);
        if (extra)
            IWbemClassObject_Release(extra);
        if (FAILED(hr))
            status = UD_IO;
        else if (returned || hr == WBEM_S_TIMEDOUT)
            status = UD_RESOURCE;
    }
    if (!status)
        status = ud_bytes_put(bytes, "]", 1);
    if (result)
        IEnumWbemClassObject_Release(result);
    if (service)
        IWbemServices_Release(service);
    if (locator)
        IWbemLocator_Release(locator);
    SysFreeString(space);
    SysFreeString(query);
    SysFreeString(language);
    CoUninitialize();
    return status;
}
#endif
static ud_status invoke(void *user, const ud_component_context *context, ud_string text) {
    wmi_config config;
    ud_json *doc = NULL;
    ud_connection_lease *lease = NULL;
    ud_status status = parse(context, text, &doc, &config);
    if (!status)
        status = ud_activation_acquire_grant(context->activation, config.connection, &lease);
    if (!status)
        status = ud_connection_lease_authorize(lease, UD_ACCESS_READ);
    ud_bytes bytes = {.ctx = context->memory, .limit = (size_t)config.maximum};
#ifdef _WIN32
    ud_memory_reservation *reservation = NULL;
    if (!status)
        status = ud_memory_reserve(context->memory, config.maximum * 2 + 1048576, &reservation);
    if (!status)
        status = run(context, lease, &config, (int)(intptr_t)user, &bytes);
    ud_memory_release(reservation);
#else
    (void)user;
#endif
    if (!status)
        status = ud_activation_set_variable(context->activation, config.output,
                                            &(ud_value){.type = UD_UTF8,
                                                        .text = (const char *)bytes.data,
                                                        .text_length = (uint32_t)bytes.size},
                                            NULL);
    ud_bytes_clear(&bytes);
    (void)ud_connection_lease_release(lease);
    ud_json_destroy(doc);
    return status;
}
ud_status ud_wmi_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"connection\","
        "\"query\",\"resultVariable\"],\"properties\":{"
        "\"connection\":{\"type\":\"string\",\"x-role\":\"resource-reference\"},\"query\":{"
        "\"type\":\"string\"},\"resultVariable\":{\"type\":\"string\"},"
        "\"maxRows\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":65536},\"maxResponseBytes\":{"
        "\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864},"
        "\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000}}}";
    const char *ids[] = {"wmi.query", "wmi.event"};
    for (uint32_t at = 0; at < 2; at++) {
        ud_component_descriptor descriptor = {
            .struct_size = sizeof(descriptor),
            .abi_version = UD_ABI_VERSION,
            .id = {ids[at], (uint32_t)strlen(ids[at])},
            .name = {ids[at], (uint32_t)strlen(ids[at])},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {schema, sizeof(schema) - 1},
            .category = UD_COMPONENT_TASK,
            .synchronous_input = -1,
            .error_output = -1,
            .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE,
            .effects = UD_EFFECT_EXTERNAL_READ,
            .user = (void *)(intptr_t)at,
            .configure = configure,
            .invoke = invoke};
        UD_TRY(ud_registry_register(registry, &descriptor));
    }
    return UD_OK;
}
