#include "odbc_internal.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

/* Microsoft ODBC 17.3+ public XA request ABI, on Windows/Linux/macOS:
 * https://learn.microsoft.com/sql/connect/odbc/use-xa-with-dtc
 * These fields are 32-bit int, NOT the LP64 X/Open xa.h long layout.
 * No DTC proxy, Java runtime or independent transaction coordinator is used. */
typedef struct mssql_xid {
    int32_t format, global_length, branch_length;
    uint8_t data[128];
} mssql_xid;
typedef struct mssql_xa_call {
    uint32_t size;
    int32_t operation;
    mssql_xid xid;
    int32_t flags, status;
    uint32_t data_size, returned;
} mssql_xa_call;
_Static_assert(sizeof(mssql_xid) == 140, "Microsoft XA XID layout");
_Static_assert(sizeof(mssql_xa_call) == 164, "Microsoft XA request layout");
enum { XA_START, XA_END, XA_PREPARE, XA_COMMIT, XA_ROLLBACK, XA_FORGET, XA_RECOVER,
       XA_GET_TIMEOUT, XA_SET_TIMEOUT };
enum { XA_OK = 0, XA_RDONLY = 3, XA_RMERR = -3, XA_NOTA = -4, XA_RMFAIL = -7 };
enum { XA_SUCCESS = 0x04000000, XA_FAIL = 0x20000000,
       XA_SCAN_START = 0x01000000, XA_SCAN_END = 0x00800000 };
enum { PHASE_ACTIVE = 1, PHASE_ENDED, PHASE_PREPARED };
/* XA_RECOVER is read only. Bound the scan rather than treating a truncated
 * provider list as proof that a branch does not exist. */
#define XA_RECOVERY_PAGE (48u * (uint32_t)sizeof(mssql_xid))
#define XA_RECOVERY_LIMIT (8u * 1024u * 1024u)
#define XA_ATTRIBUTE 1208 /* SQL_ATTR_ENLIST_IN_XA / SQL_COPT_SS_ENLIST_IN_XA */
#define XA_FORMAT INT32_C(0x55441001)
#define XA_PROBE_FORMAT INT32_C(0x55441002)

static mssql_xid identity(const ud_transaction_branch *branch) {
    mssql_xid xid = {.format = XA_FORMAT, .global_length = 32, .branch_length = 32};
    for (uint32_t byte = 0; byte < 32; byte++) {
        unsigned a = (unsigned char)branch->id.data[2 * byte],
                 b = (unsigned char)branch->id.data[2 * byte + 1];
        a = a <= '9' ? a - '0' : a - 'a' + 10;
        b = b <= '9' ? b - '0' : b - 'a' + 10;
        xid.data[byte] = (uint8_t)((a << 4) | b);
    }
    memcpy(xid.data + 32, branch->digest, 32);
    return xid;
}
static ud_status xa_status(odbc_state *p, int result) {
    char text[96];
    snprintf(text, sizeof(text), "SQL Server XA operation failed (status %d)", result);
    ud_status status = result == XA_RMFAIL ? UD_TRANSIENT
                       : result == -8    ? UD_FENCED
                       : result == -5    ? UD_UNSUPPORTED
                       : result == 106   ? UD_TIMEOUT
                                         : UD_IO;
    return p->host.report(p->host.context, status, text);
}
/* Header size includes the trailing buffer, as in Microsoft's IssueXaCall.
 * returned is checked before any byte is consumed. Provider/XA status codes
 * are distinct; positive heuristic/rollback values never mean success. */
static ud_status call(odbc_state *p, const mssql_xid *xid, int operation, int flags,
                       void *data, uint32_t capacity, uint32_t *returned, int *result) {
    if (capacity && !data) return UD_INVALID;
    if (returned)
        *returned = 0;
    *result = XA_RMFAIL;
    mssql_xa_call *request = p->host.allocate(p->host.context, sizeof(*request) + capacity);
    if (!request)
        return UD_RESOURCE;
    request->size = (uint32_t)sizeof(*request) + capacity;
    request->operation = operation;
    request->flags = flags;
    request->data_size = capacity;
    request->status = XA_RMFAIL;
    if (xid)
        request->xid = *xid;
    if (capacity)
        memcpy(request + 1, data, capacity);
    SQLRETURN rc = SQLSetConnectAttr(p->connection, XA_ATTRIBUTE, request, SQL_IS_POINTER);
    *result = request->status;
    ud_status status = SQL_SUCCEEDED(rc) ? UD_OK : ud_odbc_diagnostic(p);
    if (status && !strcmp(p->sqlstate, "00000") && !p->native_error) {
        char diagnostic[112];
        snprintf(diagnostic, sizeof(diagnostic), "SQL Server XA request failed (ODBC %d, XA %d)",
                 (int)rc, request->status);
        status = p->host.report(p->host.context, UD_IO, diagnostic);
    }
    if (!status && request->returned > capacity)
        status = UD_CORRUPT;
    if (!status) {
        if (data && request->returned)
            memcpy(data, request + 1, request->returned);
        if (returned)
            *returned = request->returned;
    }
    p->host.deallocate(request);
    return status;
}
static ud_status request(odbc_state *p, const mssql_xid *xid, int operation, int flags) {
    int result = XA_RMFAIL;
    ud_status status = call(p, xid, operation, flags, NULL, 0, NULL, &result);
    return status ? status : result == XA_OK ? UD_OK : xa_status(p, result);
}
static ud_status scan(odbc_state *p, const mssql_xid *wanted, int *found, int *initial_result) {
    *found = 0;
    if (initial_result)
        *initial_result = XA_OK;
    uint8_t *data = p->host.allocate(p->host.context, XA_RECOVERY_PAGE);
    uint8_t *previous = p->host.allocate(p->host.context, XA_RECOVERY_PAGE);
    if (!data || !previous) {
        p->host.deallocate(data);
        p->host.deallocate(previous);
        return UD_RESOURCE;
    }
    uint32_t total = 0, previous_size = 0;
    int started = 0;
    ud_status status = UD_OK;
    while (!status) {
        memset(data, 0, XA_RECOVERY_PAGE);
        int result = XA_RMFAIL;
        uint32_t returned = 0;
        status = call(p, NULL, XA_RECOVER, started ? 0 : XA_SCAN_START, data, XA_RECOVERY_PAGE,
                      &returned, &result);
        if (!started && initial_result)
            *initial_result = result;
        if (result >= 0)
            started = 1;
        if (!status && result < 0)
            status = xa_status(p, result);
        if (status)
            break;
        if (returned % sizeof(mssql_xid) ||
            (result > 0 && (uint64_t)returned != (uint64_t)result * sizeof(mssql_xid)))
            status = UD_CORRUPT;
        if (returned && returned == previous_size && !memcmp(previous, data, returned))
            status = UD_CORRUPT;
        if (!status && returned > XA_RECOVERY_LIMIT - total)
            status = UD_RESOURCE;
        if (status || !returned)
            break;
        total += returned;
        memcpy(previous, data, returned);
        previous_size = returned;
        for (uint32_t at = 0; !status && at < returned / sizeof(mssql_xid); at++) {
            const mssql_xid *xid = (const mssql_xid *)data + at;
            if (xid->global_length < 0 || xid->global_length > 64 ||
                xid->branch_length < 0 || xid->branch_length > 64)
                status = UD_CORRUPT;
            else if (wanted && xid->format == wanted->format &&
                     xid->global_length == wanted->global_length &&
                     !memcmp(xid->data, wanted->data, (size_t)wanted->global_length)) {
                if (xid->branch_length != wanted->branch_length ||
                    memcmp(xid->data + xid->global_length, wanted->data + wanted->global_length,
                           (size_t)wanted->branch_length))
                    status = UD_FENCED;
                else if ((*found)++)
                    status = UD_CORRUPT;
            }
        }
    }
    if (started) {
        ud_status closed = request(p, NULL, XA_RECOVER, XA_SCAN_END);
        if (!status)
            status = closed;
    }
    p->host.deallocate(previous);
    p->host.deallocate(data);
    return status;
}

static ud_status start_resource_manager(odbc_state *p) {
    /* SQL Server on Linux can return XAER_RMERR from recovery until its first
     * XA START. Initialize it with a separate, empty branch and roll it back. */
    mssql_xid xid = {.format = XA_PROBE_FORMAT, .global_length = 32, .branch_length = 32};
    UD_TRY(ud_odbc_control(p));
    UD_TRY(p->host.random_bytes(p->host.context, xid.data, 64));
    UD_TRY(request(p, &xid, XA_START, 0));
    ud_status ended = request(p, &xid, XA_END, XA_FAIL);
    ud_status rolled_back = request(p, &xid, XA_ROLLBACK, 0);
    if (rolled_back)
        p->broken = 1;
    return rolled_back ? rolled_back : ended;
}

ud_status ud_mssql_xa_probe(odbc_state *p) {
    if (p->dialect != DIALECT_MSSQL || p->bulk_session ||
        !(p->capabilities & UD_CONNECT_TRANSACTION))
        return UD_OK;
    char driver[256]={0};SQLSMALLINT length=0;
    if (!SQL_SUCCEEDED(SQLGetInfoA(p->connection,SQL_DRIVER_NAME,driver,sizeof(driver),&length)))
        return UD_OK;
    for (size_t at=0;at<sizeof(driver) && driver[at];at++)
        if (driver[at]>='A' && driver[at]<='Z') driver[at]=(char)(driver[at]-'A'+'a');
    if (!strstr(driver,"msodbcsql")) return UD_OK;
    /* Probe the real provider API and server configuration before admission. */
    int found = 0, initial_result = XA_OK;
    ud_status status = scan(p, NULL, &found, &initial_result);
    if (status == UD_IO && !p->broken && initial_result == XA_RMERR) {
        status = start_resource_manager(p);
        if (!status)
            status = scan(p, NULL, &found, NULL);
    }
    if (!status)
        p->capabilities |= UD_CONNECT_TRANSACTION_PARTICIPANT;
    /* An unavailable XA facility leaves ordinary SQL/BCP available; a broken
     * transport cannot be reused even when only this optional probe failed. */
    return p->broken ? status : UD_OK;
}
static void clear_branch(odbc_state *p) {
    p->xa_phase = p->transaction = 0;
    memset(p->branch_id, 0, sizeof(p->branch_id));
    memset(p->branch_digest, 0, sizeof(p->branch_digest));
    ud_odbc_bulk_close(p);
}
ud_status ud_mssql_xa_begin(void *state, const ud_transaction_branch *branch) {
    odbc_state *p = state;
    if (!(p->capabilities & UD_CONNECT_TRANSACTION_PARTICIPANT))
        return UD_UNSUPPORTED;
    if (p->transaction || p->branch_id[0] || p->cursor || p->cdc)
        return UD_BUSY;
    SQLUINTEGER isolation = branch->isolation == UD_ISOLATION_SERIALIZABLE ? SQL_TXN_SERIALIZABLE
                            : branch->isolation == UD_ISOLATION_REPEATABLE_READ ? SQL_TXN_REPEATABLE_READ
                                                                              : SQL_TXN_READ_COMMITTED;
    if (!SQL_SUCCEEDED(SQLSetConnectAttr(p->connection, SQL_ATTR_TXN_ISOLATION,
                                         (SQLPOINTER)(uintptr_t)isolation, 0)))
        return ud_odbc_diagnostic(p);
    int32_t timeout = 600; /* Bound abandoned unprepared branches; prepared branches remain durable. */
    int result = XA_RMFAIL;
    ud_status status = call(p, NULL, XA_SET_TIMEOUT, 0, &timeout, sizeof(timeout), NULL, &result);
    if (!status && result != XA_OK)
        status = xa_status(p, result);
    mssql_xid xid = identity(branch);
    if (!status)
        status = request(p, &xid, XA_START, 0);
    if (!status) {
        p->xa_phase = PHASE_ACTIVE;
        p->transaction = 1;
        memcpy(p->branch_id, branch->id.data, 64);
        memcpy(p->branch_digest, branch->digest, 32);
        int found = 0;
        status = ud_odbc_tx_receipt(p, branch, 1, &found);
        if (status) {
            ud_status rolled_back = ud_mssql_xa_resolve(p, branch, UD_TX_ROLLBACK);
            if (rolled_back)
                status = UD_COMMIT_UNKNOWN;
        }
    }
    return status;
}
ud_status ud_mssql_xa_prepare(void *state, const ud_transaction_branch *branch) {
    odbc_state *p = state;
    if (p->xa_phase != PHASE_ACTIVE || memcmp(p->branch_id, branch->id.data, 64) ||
        memcmp(p->branch_digest, branch->digest, 32))
        return UD_FENCED;
    UD_TRY(ud_odbc_control(p));
    mssql_xid xid = identity(branch);
    ud_status status = request(p, &xid, XA_END, XA_SUCCESS);
    if (!status) {
        p->xa_phase = PHASE_ENDED;
        status = request(p, &xid, XA_PREPARE, 0);
    }
    if (!status) {
        p->xa_phase = PHASE_PREPARED;
        p->transaction = 0;
    }
    return status;
}
ud_status ud_mssql_xa_recover(void *state, const ud_transaction_branch *branch, int32_t *result) {
    odbc_state *p = state;
    *result = UD_TX_UNKNOWN;
    if (p->xa_phase && !memcmp(p->branch_id, branch->id.data, 64) &&
        memcmp(p->branch_digest, branch->digest, 32))
        return UD_FENCED;
    if (p->xa_phase && p->xa_phase != PHASE_PREPARED &&
        !memcmp(p->branch_id, branch->id.data, 64)) {
        *result = UD_TX_ACTIVE;
        return UD_OK;
    }
    mssql_xid xid = identity(branch);
    int found = 0, resolving = p->resolving;
    p->resolving = 1;
    ud_status status = scan(p, &xid, &found, NULL);
    if (!status && found)
        *result = UD_TX_PREPARED;
    else if (!status) {
        status = ud_odbc_tx_receipt(p, branch, 0, &found);
        if (!status)
            *result = found ? UD_TX_COMMITTED : UD_TX_NOT_FOUND;
    }
    p->resolving = resolving;
    return status;
}
ud_status ud_mssql_xa_resolve(void *state, const ud_transaction_branch *branch, int32_t decision) {
    odbc_state *p = state;
    if (decision == UD_TX_COMMIT && p->xa_phase && p->xa_phase != PHASE_PREPARED)
        return UD_FENCED;
    mssql_xid xid = identity(branch);
    int resolving = p->resolving;
    p->resolving = 1;
    ud_status status = UD_OK;
    if (p->xa_phase == PHASE_ACTIVE) {
        status = request(p, &xid, XA_END, XA_FAIL);
        if (!status)
            p->xa_phase = PHASE_ENDED;
    }
    if (!status)
        status = request(p, &xid, decision == UD_TX_COMMIT ? XA_COMMIT : XA_ROLLBACK, 0);
    if (!status)
        clear_branch(p);
    p->resolving = resolving;
    return status;
}
void ud_mssql_xa_close(odbc_state *p) {
    if (p->xa_phase == PHASE_ACTIVE || p->xa_phase == PHASE_ENDED) {
        ud_transaction_branch branch = {.id = {p->branch_id, 64}};
        memcpy(branch.digest, p->branch_digest, 32);
        (void)ud_mssql_xa_resolve(p, &branch, UD_TX_ROLLBACK);
    }
    /* Never resolve a prepared branch on transport destruction. */
}
