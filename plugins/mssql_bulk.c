#include "odbc_internal.h"
#include <msodbcsql.h>
#include <limits.h>
/* BCP commits only a controlled staging table on its own connection. Business
 * rows and the Native receipt still commit on the original leased connection. */
typedef struct bulk_cell {
    uint8_t *data;
    uint32_t capacity;
    int type;
} bulk_cell;
static ud_status sql(odbc_state *p, const ud_write_options *options, const ud_batch *batch,
                     ud_string stage, int phase) {
    ud_string statement = {0};
    ud_status status = p->host.compile_stage(p->host.context, UD_SQL_SQLSERVER, options, batch,
                                             stage, phase, &statement);
    if (!status)
        status = ud_odbc_command(p, statement.data);
    p->host.deallocate((void *)statement.data);
    return status;
}
static ud_status cell(odbc_state *p, const ud_value *value, bulk_cell *out, uint32_t *length) {
    const char *input = value->text;
    uint32_t size = value->text_length;
    char formatted[256];
    if (value->type != UD_UTF8 && value->type != UD_BINARY) {
        ud_status status = UD_OK;
        if (value->type == UD_BOOL) {
            formatted[0] = value->integer ? '1' : '0';
            size = 1;
        } else {
            status = p->host.format_value(value, formatted, sizeof(formatted), &size);
            if (!status && (!size || size > sizeof(formatted)))
                status = UD_CORRUPT;
            if (!status)
                size--; /* Native formatter reports required bytes including NUL. */
            if (!status && value->type == UD_TIMESTAMP && size > 19 &&
                formatted[10] == 'T' && formatted[size - 1] == 'Z' &&
                size + 6 <= sizeof(formatted)) {
                formatted[10] = ' ';
                memcpy(formatted + size - 1, " +00:00", 7);
                size += 6;
            }
        }
        if (status)
            return status;
        input = formatted;
    }
    if (size > INT_MAX / 2)
        return UD_RESOURCE;
    uint32_t capacity = size * 2 + 2;
    if (capacity > out->capacity) {
        uint8_t *allocated = p->host.allocate(p->host.context, capacity);
        if (!allocated)
            return UD_RESOURCE;
        p->host.deallocate(out->data);
        out->data = allocated;
        out->capacity = capacity;
    }
    if (value->type != UD_UTF8) {
        if (size)
            memcpy(out->data, input, size);
        *length = size;
        return UD_OK;
    }
    /* BCP SQLNCHAR consumes UTF-16LE bytes, independently of the server codepage. */
    uint32_t written = 0;
    for (uint32_t at = 0; at < size;) {
        uint32_t scalar = (unsigned char)input[at++], continuation = 0;
        if (scalar >= 0x80) {
            continuation = scalar >= 0xf0 && scalar <= 0xf4   ? 3
                           : scalar >= 0xe0 && scalar <= 0xef ? 2
                           : scalar >= 0xc2 && scalar <= 0xdf ? 1
                                                              : 99;
            if (continuation > size - at)
                return UD_PARSE;
            scalar &= continuation == 3 ? 7 : continuation == 2 ? 15 : 31;
            for (uint32_t index = 0; index < continuation; index++) {
                uint8_t byte = (uint8_t)input[at++];
                if ((byte & 0xc0) != 0x80)
                    return UD_PARSE;
                scalar = (scalar << 6) | (byte & 63);
            }
            if (scalar > 0x10ffff || (scalar >= 0xd800 && scalar <= 0xdfff) ||
                (continuation == 1 && scalar < 0x80) || (continuation == 2 && scalar < 0x800) ||
                (continuation == 3 && scalar < 0x10000))
                return UD_PARSE;
        }
        if (scalar > 0xffff) {
            scalar -= 0x10000;
            uint32_t high = 0xd800 + (scalar >> 10), low = 0xdc00 + (scalar & 1023);
            out->data[written++] = (uint8_t)high;
            out->data[written++] = (uint8_t)(high >> 8);
            out->data[written++] = (uint8_t)low;
            out->data[written++] = (uint8_t)(low >> 8);
        } else {
            out->data[written++] = (uint8_t)scalar;
            out->data[written++] = (uint8_t)(scalar >> 8);
        }
    }
    *length = written;
    return UD_OK;
}
ud_status ud_mssql_bulk_stage(odbc_state *owner, const ud_write_options *options,
                              const ud_batch *batch, const uint8_t digest[32], odbc_state **output,
                              ud_string *stage) {
    *output = NULL;
    *stage = (ud_string){0};
    if (options->mode != UD_WRITE_APPEND || owner->dialect != DIALECT_MSSQL ||
        !(owner->capabilities & UD_CONNECT_NATIVE_BULK))
        return UD_UNSUPPORTED;
    uint32_t columns = owner->host.columns(batch);
    for (uint32_t index = 0; index < columns; index++) {
        ud_field field;
        ud_status status = owner->host.field(batch, index, &field);
        if (status)
            return status;
        if (field.type >= UD_STRUCT || field.type == UD_INTERVAL || field.type == UD_DURATION)
            return UD_UNSUPPORTED; /* No narrowing or calendar-to-clock conversion. */
    }
    char name[96] = "##ud_sdk_bulk_";
    size_t prefix = strlen(name);
    uint8_t identifier[32];
    ud_status identifier_status = digest
                                     ? (memcpy(identifier, digest, sizeof(identifier)), UD_OK)
                                     : owner->host.random_bytes(owner->host.context, identifier,
                                                                sizeof(identifier));
    if (identifier_status)
        return identifier_status;
    for (uint32_t index = 0; index < sizeof(identifier); index++)
        snprintf(name + prefix + index * 2, 3, "%02x", identifier[index]);
    stage->data = plugin_string(&owner->host, name, (uint32_t)strlen(name));
    stage->length = (uint32_t)strlen(name);
    if (!stage->data)
        return UD_RESOURCE;
    odbc_state *p = NULL;
    ud_status status = ud_odbc_initialize_bulk(&owner->host, owner->configuration,
                                               owner->configuration_length, (void **)&p);
    if (!status)
        status = sql(p, options, batch, *stage, UD_SQL_STAGE_CREATE);
    /* The vendor BCP entry points take the connected ODBC application handle,
     * with SQL_COPT_SS_BCP enabled before connection establishment. */
    SQLHDBC driver_connection = p ? p->connection : SQL_NULL_HDBC;
    /* BCP takes an object name rather than an SQL identifier expression. The
     * name contains only this owner's fixed ASCII prefix and hexadecimal digest. */
    if (!status && bcp_initA(driver_connection, name, NULL, NULL, DB_IN) != SUCCEED) {
        status = ud_odbc_diagnostic(p);
    }
    bulk_cell *cells = !status ? p->host.allocate(p->host.context, columns * sizeof(*cells)) : NULL;
    if (!status && !cells)
        status = UD_RESOURCE;
    for (uint32_t index = 0; !status && index < columns; index++) {
        ud_field field;
        status = p->host.field(batch, index, &field);
        cells[index].type = field.type == UD_BINARY ? SQLBINARY
                            : field.type == UD_UTF8 ? SQLNCHAR
                                                    : SQLCHARACTER;
        static const uint8_t empty = 0;
        if (!status && bcp_bind(driver_connection, &empty, 0, 0, NULL, 0, cells[index].type,
                                (int)index + 1) != SUCCEED) {
            status = ud_odbc_diagnostic(p);
        }
    }
    for (uint32_t row = 0; !status && row < p->host.rows(batch); row++) {
        status = ud_odbc_control(p);
        for (uint32_t index = 0; !status && index < columns; index++) {
            ud_value value;
            status = p->host.get(batch, row, index, &value);
            if (!status && value.is_absent)
                status = UD_INCOMPATIBLE;
            uint32_t length = 0;
            if (!status && !value.is_null)
                status = cell(p, &value, cells + index, &length);
            static const uint8_t empty = 0;
            if (!status &&
                bcp_colptr(driver_connection, cells[index].data ? cells[index].data : &empty,
                           (int)index + 1) != SUCCEED)
                status = ud_odbc_diagnostic(p);
            if (!status &&
                bcp_collen(driver_connection, value.is_null ? SQL_NULL_DATA : (DBINT)length,
                           (int)index + 1) != SUCCEED)
                status = ud_odbc_diagnostic(p);
        }
        if (!status && bcp_sendrow(driver_connection) != SUCCEED) {
            status = ud_odbc_diagnostic(p);
        }
    }
    if (!status) {
        DBINT loaded = bcp_done(driver_connection);
        if (loaded < 0 || (uint64_t)loaded != p->host.rows(batch))
            status = loaded < 0 ? ud_odbc_diagnostic(p) : UD_FENCED;
    } else if (driver_connection) {
        int abort = 1;
        bcp_control(driver_connection, BCPABORT, &abort);
    }
    for (uint32_t index = 0; cells && index < columns; index++)
        p->host.deallocate(cells[index].data);
    if (p)
        p->host.deallocate(cells);
    if (status) {
        if (p)
            ud_odbc_destroy(p); /* Global temporary staging disappears with its owner. */
        owner->host.deallocate((void *)stage->data);
        *stage = (ud_string){0};
    } else
        *output = p; /* Keep the staging owner alive through business commit. */
    return status;
}
