#include "oracle_internal.h"
#include <math.h>
/* Direct Path has its own commit behavior. It writes only a private CTAS
 * staging table; the ordinary leased transaction owns business rows/receipts. */
static ud_status result(oracle *p, sword code) {
    if (code == OCI_SUCCESS || code == OCI_SUCCESS_WITH_INFO)
        return UD_OK;
    sb4 number = 0;
    OraText ignored[2];
    OCIErrorGet(p->error, 1, NULL, &number, ignored, sizeof(ignored), OCI_HTYPE_ERROR);
    p->record_code = number;
    char message[64];
    snprintf(message, sizeof(message), "OCI Direct Path code %ld", (long)number);
    return p->host.report(p->host.context,
                          number == 12899  ? UD_TRUNCATED
                          : number == 1438 ? UD_OVERFLOW
                          : number == 1722 ? UD_PARSE
                                           : UD_IO,
                          message);
}
static ud_status statement(oracle *p, const ud_write_options *options, const ud_batch *batch,
                           ud_string stage, int phase) {
    ud_string sql = {0};
    ud_status status =
        p->host.compile_stage(p->host.context, UD_SQL_ORACLE, options, batch, stage, phase, &sql);
    if (!status)
        status = ud_oci_state_command(p, &(ud_read_options){.struct_size = sizeof(ud_read_options),
                                                            .abi_version = UD_ABI_VERSION,
                                                            .sql = sql});
    p->host.deallocate((void *)sql.data);
    return status;
}
typedef struct ud_oci_bulk_drop {
    ud_string sql;
    struct ud_oci_bulk_drop *next;
} ud_oci_bulk_drop;
ud_status ud_oci_bulk_defer(oracle *owner, const ud_write_options *options, const ud_batch *batch,
                            ud_string stage) {
    ud_oci_bulk_drop *drop = owner->host.allocate(owner->host.context, sizeof(*drop));
    if (!drop)
        return UD_RESOURCE;
    ud_status status = owner->host.compile_stage(owner->host.context, UD_SQL_ORACLE, options, batch,
                                                 stage, UD_SQL_STAGE_DROP, &drop->sql);
    if (status) {
        owner->host.deallocate(drop);
        return status;
    }
    drop->next = owner->bulk_drops;
    owner->bulk_drops = drop;
    return UD_OK;
}
ud_status ud_oci_bulk_cleanup(oracle *owner) {
    if (owner->transaction || owner->branch_id[0])
        return UD_BUSY;
    int cleanup = owner->cleanup;
    owner->cleanup = 1;
    ud_status status = UD_OK;
    while (!status && owner->bulk_drops) {
        ud_oci_bulk_drop *drop = owner->bulk_drops;
        status =
            ud_oci_state_command(owner, &(ud_read_options){.struct_size = sizeof(ud_read_options),
                                                           .abi_version = UD_ABI_VERSION,
                                                           .sql = drop->sql});
        if (!status) {
            owner->bulk_drops = drop->next;
            owner->host.deallocate((void *)drop->sql.data);
            owner->host.deallocate(drop);
        }
    }
    owner->cleanup = cleanup;
    return status;
}
void ud_oci_bulk_forget(oracle *owner) {
    while (owner->bulk_drops) {
        ud_oci_bulk_drop *drop = owner->bulk_drops;
        owner->bulk_drops = drop->next;
        owner->host.deallocate((void *)drop->sql.data);
        owner->host.deallocate(drop);
    }
}
ud_status ud_oci_bulk_dispose(oracle *p, const ud_write_options *options, const ud_batch *batch,
                              ud_string stage) {
    if (!p)
        return UD_OK;
    p->cleanup = 1;
    ud_status status = statement(p, options, batch, stage, UD_SQL_STAGE_DROP);
    ud_oci_destroy(p);
    return status;
}
ud_status ud_oci_bulk_stage(oracle *owner, const ud_write_options *options, const ud_batch *batch,
                            oracle **output, ud_string *stage) {
    *output = NULL;
    *stage = (ud_string){0};
    if (options->mode != UD_WRITE_APPEND)
        return UD_UNSUPPORTED;
    uint32_t columns = owner->host.columns(batch);
    if (columns > UINT16_MAX)
        return UD_UNSUPPORTED;
    for (uint32_t column = 0; column < columns; column++) {
        ud_field field;
        ud_status status = owner->host.field(batch, column, &field);
        if (status)
            return status;
        if (field.type >= UD_STRUCT || field.type == UD_INTERVAL || field.type == UD_DURATION ||
            field.type == UD_TIME64 || (field.type == UD_TIMESTAMP && !owner->utc))
            return UD_UNSUPPORTED;
    }
    uint8_t random[16];
    ud_status status = owner->host.random_bytes(owner->host.context, random, sizeof(random));
    char name[64] = "UD_SDK_LOAD_";
    size_t prefix = strlen(name);
    for (uint32_t index = 0; !status && index < sizeof(random); index++)
        snprintf(name + prefix + index * 2, 3, "%02X", random[index]);
    if (!status) {
        stage->length = (uint32_t)strlen(name);
        stage->data = plugin_string(&owner->host, name, stage->length);
        if (!stage->data)
            status = UD_RESOURCE;
    }
    oracle *p = NULL;
    if (!status)
        status = ud_oci_initialize(&owner->host, owner->configuration, owner->configuration_length,
                                   (void **)&p);
    int created = 0;
    if (!status) {
        status = statement(p, options, batch, *stage, UD_SQL_STAGE_CREATE);
        created = !status;
    }
    OCIDirPathCtx *context = NULL;
    OCIDirPathColArray *array = NULL;
    OCIDirPathStream *stream = NULL;
    if (!status)
        status =
            result(p, OCIHandleAlloc(p->env, (void **)&context, OCI_HTYPE_DIRPATH_CTX, 0, NULL));
    if (!status)
        status = result(p, OCIAttrSet(context, OCI_HTYPE_DIRPATH_CTX, (void *)stage->data,
                                      stage->length, OCI_ATTR_NAME, p->error));
    ub2 count = (ub2)columns;
    if (!status)
        status = result(
            p, OCIAttrSet(context, OCI_HTYPE_DIRPATH_CTX, &count, 0, OCI_ATTR_NUM_COLS, p->error));
    OCIParam *list = NULL;
    if (!status)
        status = result(p, OCIAttrGet(context, OCI_HTYPE_DIRPATH_CTX, &list, NULL,
                                      OCI_ATTR_LIST_COLUMNS, p->error));
    for (uint32_t column = 0; !status && column < columns; column++) {
        OCIParam *parameter = NULL;
        ud_field field;
        status = owner->host.field(batch, column, &field);
        if (!status)
            status = result(
                p, OCIParamGet(list, OCI_DTYPE_PARAM, p->error, (void **)&parameter, column + 1));
        char *quoted = !status
                           ? plugin_identifier(&p->host, (ud_string){field.name, field.name_length})
                           : NULL;
        if (!status && !quoted)
            status = UD_RESOURCE;
        if (!status)
            status = result(p, OCIAttrSet(parameter, OCI_DTYPE_PARAM, quoted,
                                          (ub4)strlen(quoted), OCI_ATTR_NAME, p->error));
        if (quoted)
            p->host.deallocate(quoted);
        ub2 type = field.type == UD_BINARY ? SQLT_BIN : SQLT_CHR;
        if (!status)
            status = result(
                p, OCIAttrSet(parameter, OCI_DTYPE_PARAM, &type, 0, OCI_ATTR_DATA_TYPE, p->error));
        ub4 maximum = 256;
        for (uint32_t row = 0; !status && row < owner->host.rows(batch); row++) {
            ud_value value;
            status = owner->host.get(batch, row, column, &value);
            if (!status && value.is_absent)
                status = UD_INCOMPATIBLE;
            if (!status && (value.type == UD_UTF8 || value.type == UD_BINARY) &&
                value.text_length > maximum)
                maximum = value.text_length;
        }
        if (!status)
            status = result(p, OCIAttrSet(parameter, OCI_DTYPE_PARAM, &maximum, 0,
                                          OCI_ATTR_DATA_SIZE, p->error));
        ub2 charset = 873;
        if (!status && field.type != UD_BINARY)
            status = result(p, OCIAttrSet(parameter, OCI_DTYPE_PARAM, &charset, 0,
                                          OCI_ATTR_CHARSET_ID, p->error));
        const char *format = field.type == UD_DATE32      ? "YYYY-MM-DD"
                             : field.type == UD_TIMESTAMP ? "YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM"
                                                          : NULL;
        if (!status && format)
            status = result(p, OCIAttrSet(parameter, OCI_DTYPE_PARAM, (void *)format,
                                          (ub4)strlen(format), OCI_ATTR_DATEFORMAT, p->error));
        if (parameter)
            OCIDescriptorFree(parameter, OCI_DTYPE_PARAM);
    }
    if (!status)
        status = result(p, OCIDirPathPrepare(context, p->service, p->error));
    if (!status)
        status = result(
            p, OCIHandleAlloc(context, (void **)&array, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, 0, NULL));
    if (!status)
        status =
            result(p, OCIHandleAlloc(context, (void **)&stream, OCI_HTYPE_DIRPATH_STREAM, 0, NULL));
    ub4 capacity = 0;
    if (!status)
        status = result(p, OCIAttrGet(array, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &capacity, NULL,
                                      OCI_ATTR_NUM_ROWS, p->error));
    if (!status && !capacity)
        status = UD_CORRUPT;
    if (capacity > 64)
        capacity = 64;
    uint64_t slots = (uint64_t)capacity * columns;
    char *formatted = !status && slots <= SIZE_MAX / 256
                          ? p->host.allocate(p->host.context, (size_t)slots * 256)
                          : NULL;
    if (!status && !formatted)
        status = UD_RESOURCE;
    for (uint32_t first = 0; !status && first < owner->host.rows(batch); first += capacity) {
        ub4 rows = owner->host.rows(batch) - first;
        if (rows > capacity)
            rows = capacity;
        status = ud_oci_control(p);
        if (!status)
            status = result(p, OCIDirPathColArrayReset(array, p->error));
        for (ub4 row = 0; !status && row < rows; row++)
            for (ub2 column = 0; !status && column < columns; column++) {
                ud_value value = {0};
                status = owner->host.get(batch, first + row, column, &value);
                const void *data = value.text;
                uint32_t length = value.text_length;
                char *text = formatted + ((size_t)row * columns + column) * 256;
                if (!status && !value.is_null && value.type != UD_UTF8 && value.type != UD_BINARY) {
                    if (value.type == UD_BOOL) {
                        text[0] = value.integer ? '1' : '0';
                        length = 1;
                    } else if (value.type == UD_FLOAT64 && !isfinite(value.real))
                        status = UD_UNSUPPORTED;
                    else {
                        status = p->host.format_value(&value, text, 256, &length);
                        if (!status && (!length || length > 256))
                            status = UD_CORRUPT;
                        if (!status)
                            length--;
                    }
                    if (!status && value.type == UD_TIMESTAMP && length &&
                        text[length - 1] == 'Z') {
                        memcpy(text + length - 1, "+00:00", 6);
                        length += 5;
                    }
                    data = text;
                }
                if (!status)
                    status = result(
                        p, OCIDirPathColArrayEntrySet(array, p->error, row, column, (ub1 *)data,
                                                      value.is_null ? 0 : length,
                                                      value.is_null ? OCI_DIRPATH_COL_NULL
                                                                    : OCI_DIRPATH_COL_COMPLETE));
            }
        ub4 offset = 0;
        while (!status && offset < rows) {
            status = ud_oci_control(p);
            if (!status)
                status = result(p, OCIDirPathStreamReset(stream, p->error));
            sword converted =
                !status ? OCIDirPathColArrayToStream(array, context, stream, p->error, rows, offset)
                        : OCI_ERROR;
            if (!status && converted != OCI_SUCCESS && converted != OCI_SUCCESS_WITH_INFO &&
                converted != OCI_CONTINUE)
                status = result(p, converted);
            ub4 completed = 0;
            if (!status)
                status = result(p, OCIAttrGet(array, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &completed,
                                              NULL, OCI_ATTR_ROW_COUNT, p->error));
            if (!status)
                status = result(p, OCIDirPathLoadStream(context, stream, p->error));
            if (!status && (!completed || completed > rows - offset))
                status = UD_CORRUPT;
            if (!status)
                offset += completed;
        }
    }
    if (!status)
        status = result(p, OCIDirPathFinish(context, p->error));
    else if (context)
        OCIDirPathAbort(context, p->error);
    if (p)
        p->host.deallocate(formatted);
    if (stream)
        OCIHandleFree(stream, OCI_HTYPE_DIRPATH_STREAM);
    if (array)
        OCIHandleFree(array, OCI_HTYPE_DIRPATH_COLUMN_ARRAY);
    if (context)
        OCIHandleFree(context, OCI_HTYPE_DIRPATH_CTX);
    if (status) {
        if (p && created)
            ud_oci_bulk_dispose(p, options, batch, *stage);
        else if (p)
            ud_oci_destroy(p);
        owner->host.deallocate((void *)stage->data);
        *stage = (ud_string){0};
    } else
        *output = p;
    return status;
}
