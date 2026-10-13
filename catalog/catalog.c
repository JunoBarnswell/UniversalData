#include "internal.h"
#include "ud/json.h"
/* Native SQLite is the sole Catalog storage path. Journals remain execution
 * recovery owners; a Catalog document is never a substitute for a commit fact. */
#define CATALOG_APPLICATION_ID 1430537025
static int kind_valid(int32_t kind) {
    return kind >= UD_CATALOG_PROJECT && kind <= UD_CATALOG_AUDIT;
}
static int string_valid(ud_string s, int required) {
    return (!required || s.length) && (!s.length || (s.data && !memchr(s.data, 0, s.length) &&
                                                     ud_utf8_valid(s.data, s.length)));
}
static ud_status grant(ud_catalog *c, int action) {
    return c->options.authorize &&
                   c->options.authorize(c->options.authorize_user, c->options.tenant,
                                        c->options.directory, action) == UD_OK
               ? UD_OK
               : UD_AUTH;
}
static ud_status key_check(const ud_catalog_key *key) {
    if (!key)
        return UD_INVALID;
    if (key->struct_size != sizeof(*key) || key->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    return kind_valid(key->kind) && string_valid(key->id, 1) && key->id.length <= 255 &&
                   key->revision
               ? UD_OK
               : UD_INVALID;
}
static ud_status sql_status(int rc) {
    switch (rc & 255) {
    case SQLITE_OK:
    case SQLITE_ROW:
    case SQLITE_DONE:
        return UD_OK;
    case SQLITE_BUSY:
    case SQLITE_LOCKED:
        return UD_BUSY;
    case SQLITE_NOMEM:
    case SQLITE_FULL:
    case SQLITE_TOOBIG:
        return UD_RESOURCE;
    case SQLITE_CORRUPT:
    case SQLITE_NOTADB:
    case SQLITE_CONSTRAINT:
        return UD_CORRUPT;
    case SQLITE_PERM:
    case SQLITE_AUTH:
    case SQLITE_READONLY:
        return UD_AUTH;
    default:
        return UD_IO;
    }
}
static ud_status sql(ud_catalog *c, const char *text) {
    return sql_status(sqlite3_exec(c->database, text, NULL, NULL, NULL));
}
static ud_status prepare(ud_catalog *c, const char *text, sqlite3_stmt **out) {
    return sql_status(sqlite3_prepare_v2(c->database, text, -1, out, NULL));
}
static ud_status bind_text(sqlite3_stmt *s, int at, ud_string text) {
    return sql_status(
        sqlite3_bind_text(s, at, text.data ? text.data : "", (int)text.length, SQLITE_TRANSIENT));
}
static void revision_text(uint64_t revision, char out[21]) {
    snprintf(out, 21, "%020llu", (unsigned long long)revision);
}
static ud_status bind_key(ud_catalog *c, sqlite3_stmt *s, const ud_catalog_key *key) {
    char revision[21];
    revision_text(key->revision, revision);
    UD_TRY(bind_text(s, 1, c->options.tenant));
    UD_TRY(sql_status(sqlite3_bind_int(s, 2, key->kind)));
    UD_TRY(bind_text(s, 3, key->id));
    return bind_text(s, 4, (ud_string){revision, 20});
}
static ud_status digest_document(ud_catalog *c, const ud_catalog_key *key, ud_string json,
                                 uint64_t timestamp, uint8_t out[32]) {
    ud_bytes bytes = {.ctx = c->ctx};
    ud_status s = ud_put_u64(&bytes, UD_CONFIG_VERSION);
    if (!s)
        s = ud_put_string(&bytes, c->tenant, strlen(c->tenant));
    if (!s)
        s = ud_put_u64(&bytes, (uint64_t)key->kind);
    if (!s)
        s = ud_put_string(&bytes, key->id.data, key->id.length);
    if (!s)
        s = ud_put_u64(&bytes, key->revision);
    if (!s)
        s = ud_put_u64(&bytes, timestamp);
    if (!s)
        s = ud_put_string(&bytes, json.data, json.length);
    if (!s)
        ud_hash(bytes.data, bytes.size, out);
    ud_bytes_clear(&bytes);
    return s;
}
/* SQL columns: id, revision, document, digest, timestamp_ms. Every returned row
 * is validated against its physical key, not just its JSON body. */
static ud_status row_check(ud_catalog *c, sqlite3_stmt *s, int kind, ud_catalog_key *key,
                           ud_string *document) {
    const char *id = (const char *)sqlite3_column_text(s, 0);
    const char *revision = (const char *)sqlite3_column_text(s, 1);
    const char *json = (const char *)sqlite3_column_text(s, 2);
    int id_length = sqlite3_column_bytes(s, 0), n = sqlite3_column_bytes(s, 2);
    if (!id || !revision || !json || id_length <= 0 || id_length > 255 || n <= 0 ||
        sqlite3_column_bytes(s, 1) != 20 || sqlite3_column_bytes(s, 3) != 32 ||
        sqlite3_column_int64(s, 4) < 0)
        return UD_CORRUPT;
    uint64_t value = 0;
    for (int i = 0; i < 20; i++) {
        unsigned digit = (unsigned char)revision[i] - '0';
        if (digit > 9 || value > (UINT64_MAX - digit) / 10)
            return UD_CORRUPT;
        value = value * 10 + digit;
    }
    *key = (ud_catalog_key){sizeof(*key), UD_ABI_VERSION, kind, {id, (uint32_t)id_length}, value};
    if (key_check(key))
        return UD_CORRUPT;
    *document = (ud_string){json, (uint32_t)n};
    uint8_t digest[32];
    UD_TRY(digest_document(c, key, *document, (uint64_t)sqlite3_column_int64(s, 4), digest));
    if (memcmp(digest, sqlite3_column_blob(s, 3), 32))
        return UD_CORRUPT;
    ud_json *doc = NULL;
    ud_status status = ud_json_parse(c->ctx, json, (uint32_t)n, &doc, NULL);
    if (!status && ud_json_kind(doc) != UD_JSON_OBJECT)
        status = UD_CORRUPT;
    ud_json_destroy(doc);
    return status == UD_PARSE || status == UD_SCHEMA ? UD_CORRUPT : status;
}
static ud_status pragma_integer(ud_catalog *c, const char *query, int *value) {
    sqlite3_stmt *s = NULL;
    ud_status status = prepare(c, query, &s);
    if (!status) {
        int rc = sqlite3_step(s);
        status = rc == SQLITE_ROW ? UD_OK : sql_status(rc);
        if (!status && rc != SQLITE_ROW)
            status = UD_CORRUPT;
        if (!status)
            *value = sqlite3_column_int(s, 0);
    }
    sqlite3_finalize(s);
    return status;
}
static ud_status normalize(ud_catalog *c, const ud_catalog_key *key, ud_string input, char **json,
                           uint32_t *length) {
    ud_json *document = NULL;
    UD_TRY(ud_json_parse(c->ctx, input.data, input.length, &document, NULL));
    ud_status s = ud_json_kind(document) == UD_JSON_OBJECT ? UD_OK : UD_SCHEMA;
    if (!s && key->kind == UD_CATALOG_PACKAGE) {
        ud_package *p = NULL;
        s = ud_package_create(c->ctx, input, &p);
        ud_package_destroy(p);
        const ud_json *id = ud_json_find(document, "id", 2);
        const char *text = NULL;
        uint32_t n = 0;
        if (!s)
            s = ud_json_text(id, &text, &n);
        if (!s && (n != key->id.length || memcmp(text, key->id.data, n)))
            s = UD_SCHEMA;
    }
    if (!s)
        s = ud_catalog_definition_check(c, key, document);
    if (!s)
        s = ud_json_serialize(document, json, length);
    ud_json_destroy(document);
    return s;
}
ud_status ud_catalog_open(ud_context *ctx, const ud_catalog_options *o, ud_catalog **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !o)
        return UD_INVALID;
    if (o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!string_valid(o->directory, 1) || !string_valid(o->tenant, 1) ||
        o->directory.length > 32767 || o->tenant.length > 255 || o->create_directory < 0 ||
        o->create_directory > 1)
        return UD_INVALID;
#ifdef _WIN32
    if (o->directory.length < 3 || o->directory.data[1] != ':')
        return UD_INVALID;
#else
    if (o->directory.data[0] != '/')
        return UD_INVALID;
#endif
    ud_catalog *c = ud_alloc(ctx, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    c->ctx = ctx;
    c->options = *o;
    c->directory = ud_strdup_n(ctx, o->directory.data, o->directory.length);
    c->tenant = ud_strdup_n(ctx, o->tenant.data, o->tenant.length);
    if (!c->directory || !c->tenant) {
        ud_catalog_close(c);
        return UD_RESOURCE;
    }
    c->options.directory = (ud_string){c->directory, o->directory.length};
    c->options.tenant = (ud_string){c->tenant, o->tenant.length};
    ud_status status = grant(c, UD_ACCESS_OPEN);
    if (!status)
        status = grant(c, UD_ACCESS_WRITE);
    if (!status && o->create_directory)
        status = ud_mkdir(ctx, c->directory);
    char *file = NULL;
    if (!status)
        status = ud_join_path(ctx, c->directory, "catalog.sqlite3", &file);
    if (!status)
        status = ud_path_no_symlinks(ctx, file);
    if (!status) {
        int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX;
        if (o->create_directory)
            flags |= SQLITE_OPEN_CREATE;
        status = sql_status(sqlite3_open_v2(file, &c->database, flags, NULL));
    }
    ud_free(file);
    if (!status)
        status = sql_status(sqlite3_busy_timeout(c->database, 5000));
    if (!status)
        status = sql(c, "BEGIN IMMEDIATE");
    int application = 0, version = 0;
    if (!status)
        status = pragma_integer(c, "PRAGMA application_id", &application);
    if (!status)
        status = pragma_integer(c, "PRAGMA user_version", &version);
    if (!status && (application || version) &&
        (application != CATALOG_APPLICATION_ID || version != UD_CONFIG_VERSION))
        status = UD_INCOMPATIBLE;
    if (!status && !application && !version) {
        int tables = 0;
        status = pragma_integer(
            c, "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'", &tables);
        if (!status && tables)
            status = UD_INCOMPATIBLE;
        if (!status)
            status = sql(
                c,
                "CREATE TABLE documents(tenant TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT "
                "NULL,"
                "revision TEXT NOT NULL CHECK(length(revision)=20),document TEXT NOT NULL,digest "
                "BLOB NOT NULL CHECK(length(digest)=32),"
                "timestamp_ms INTEGER NOT NULL CHECK(timestamp_ms>=0),PRIMARY "
                "KEY(tenant,kind,id,revision)) WITHOUT ROWID;"
                "CREATE INDEX document_time ON documents(tenant,kind,timestamp_ms,id,revision);"
                "PRAGMA application_id=1430537025;PRAGMA user_version=" UD_PROTOCOL_VERSION_TEXT);
    }
    if (!status)
        status = sql(c, "COMMIT");
    else if (c->database)
        (void)sql(c, "ROLLBACK");
    if (!status)
        status = sql(c, "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA "
                        "foreign_keys=ON;PRAGMA trusted_schema=OFF");
    if (status) {
        ud_catalog_close(c);
        return status;
    }
    *out = c;
    return UD_OK;
}
static ud_status get_document(ud_catalog *c, const ud_catalog_key *key, char **json,
                              uint32_t *length) {
    if (!c || !json || !length)
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(key_check(key));
    sqlite3_stmt *s = NULL;
    ud_status status = prepare(c,
                               "SELECT id,revision,document,digest,timestamp_ms FROM documents "
                               "WHERE tenant=?1 AND kind=?2 AND id=?3 AND revision=?4",
                               &s);
    if (!status)
        status = bind_key(c, s, key);
    if (!status) {
        int rc = sqlite3_step(s);
        if (rc == SQLITE_DONE)
            status = UD_END;
        else if (rc != SQLITE_ROW)
            status = sql_status(rc);
        else {
            ud_catalog_key actual;
            ud_string document;
            status = row_check(c, s, key->kind, &actual, &document);
            if (!status && (actual.revision != key->revision || !ud_same(actual.id, key->id)))
                status = UD_CORRUPT;
            if (!status) {
                *json = ud_strdup_n(c->ctx, document.data, document.length);
                if (!*json)
                    status = UD_RESOURCE;
                else
                    *length = document.length;
            }
        }
    }
    sqlite3_finalize(s);
    return status;
}
ud_status ud_catalog_get(ud_catalog *c, const ud_catalog_key *key, char **json, uint32_t *length) {
    if (!c || !json || !length)
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(grant(c, UD_ACCESS_READ));
    return get_document(c, key, json, length);
}
ud_status ud_catalog_put(ud_catalog *c, const ud_catalog_key *key, ud_string input) {
    if (!c)
        return UD_INVALID;
    UD_TRY(key_check(key));
    UD_TRY(grant(c, UD_ACCESS_WRITE));
    char *json = NULL;
    uint32_t length = 0;
    ud_status status = normalize(c, key, input, &json, &length);
    ud_json *doc = NULL;
    uint64_t timestamp = 0;
    if (!status)
        status = ud_json_parse(c->ctx, json, length, &doc, NULL);
    if (!status && ud_member(doc, "timestampMs"))
        status = ud_json_uint(ud_member(doc, "timestampMs"), &timestamp);
    if (!status && timestamp > INT64_MAX)
        status = UD_OVERFLOW;
    ud_json_destroy(doc);
    uint8_t digest[32];
    if (!status)
        status = digest_document(c, key, (ud_string){json, length}, timestamp, digest);
    int transaction = 0;
    if (!status) {
        status = sql(c, "BEGIN IMMEDIATE");
        transaction = !status;
    }
    char *previous = NULL;
    uint32_t previous_length = 0;
    if (!status) {
        ud_status found = get_document(c, key, &previous, &previous_length);
        if (found == UD_OK) {
            if (length != previous_length || memcmp(previous, json, length))
                status = UD_CORRUPT;
        } else if (found == UD_END) {
            sqlite3_stmt *s = NULL;
            status = prepare(
                c,
                "INSERT INTO documents(tenant,kind,id,revision,document,digest,timestamp_ms) "
                "VALUES(?1,?2,?3,?4,?5,?6,?7)",
                &s);
            if (!status)
                status = bind_key(c, s, key);
            if (!status)
                status = bind_text(s, 5, (ud_string){json, length});
            if (!status)
                status = sql_status(sqlite3_bind_blob(s, 6, digest, 32, SQLITE_TRANSIENT));
            if (!status)
                status = sql_status(sqlite3_bind_int64(s, 7, (sqlite3_int64)timestamp));
            if (!status)
                status = sql_status(sqlite3_step(s));
            sqlite3_finalize(s);
        } else
            status = found;
    }
    if (transaction) {
        if (!status)
            status = sql(c, "COMMIT");
        if (status)
            (void)sql(c, "ROLLBACK");
    }
    ud_free(previous);
    ud_free(json);
    return status;
}
static ud_status collect(ud_catalog *c, int kind, sqlite3_stmt *s, int documents, char **json,
                         uint32_t *length) {
    ud_bytes bytes = {.ctx = c->ctx, .limit = 64u * 1024u * 1024u};
    ud_status status = ud_bytes_put(&bytes, "[", 1);
    uint32_t count = 0;
    while (!status) {
        int rc = sqlite3_step(s);
        if (rc == SQLITE_DONE)
            break;
        if (rc != SQLITE_ROW) {
            status = sql_status(rc);
            break;
        }
        ud_catalog_key key = {0};
        ud_string document = {0};
        status = row_check(c, s, kind, &key, &document);
        if (!status && count++)
            status = ud_bytes_put(&bytes, ",", 1);
        if (!status)
            status = ud_bytes_put(&bytes, "{\"id\":", 6);
        if (!status)
            status = ud_json_quote(&bytes, key.id.data, key.id.length);
        char revision[64];
        int n = snprintf(revision, sizeof(revision), ",\"revision\":%llu",
                         (unsigned long long)key.revision);
        if (!status)
            status = ud_bytes_put(&bytes, revision, (size_t)n);
        if (!status && documents)
            status = ud_bytes_put(&bytes, ",\"document\":", 12);
        if (!status && documents)
            status = ud_bytes_put(&bytes, document.data, document.length);
        if (!status)
            status = ud_bytes_put(&bytes, "}", 1);
    }
    if (!status)
        status = ud_bytes_put(&bytes, "]", 1);
    if (!status)
        status = ud_bytes_put(&bytes, "", 1);
    if (!status) {
        *json = (char *)bytes.data;
        *length = (uint32_t)bytes.size - 1;
        bytes.data = NULL;
    }
    ud_bytes_clear(&bytes);
    return status;
}
ud_status ud_catalog_list(ud_catalog *c, int32_t kind, char **json, uint32_t *length) {
    if (!c || !json || !length || !kind_valid(kind))
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(grant(c, UD_ACCESS_READ));
    sqlite3_stmt *s = NULL;
    ud_status status = prepare(c,
                               "SELECT id,revision,document,digest,timestamp_ms FROM documents "
                               "WHERE tenant=?1 AND kind=?2 ORDER BY id,revision",
                               &s);
    if (!status)
        status = bind_text(s, 1, c->options.tenant);
    if (!status)
        status = sql_status(sqlite3_bind_int(s, 2, kind));
    if (!status)
        status = collect(c, kind, s, 0, json, length);
    sqlite3_finalize(s);
    return status;
}
ud_status ud_catalog_query(ud_catalog *c, ud_string input, char **json, uint32_t *length) {
    if (!c || !json || !length)
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(grant(c, UD_ACCESS_READ));
    ud_json *query = NULL;
    ud_status status = ud_json_parse(c->ctx, input.data, input.length, &query, NULL);
    const char *keys[] = {"kind", "id", "afterId", "afterRevision", "sinceMs", "untilMs", "limit"};
    if (!status)
        status = ud_json_keys(query, keys, 7);
    uint64_t kind = 0, revision = 0, since = 0, until = INT64_MAX, limit = 100;
    ud_string id = {0}, after = {0};
    if (!status)
        status = ud_json_uint(ud_member(query, "kind"), &kind);
    if (!status && (kind > UD_CATALOG_AUDIT || !kind_valid((int32_t)kind)))
        status = UD_SCHEMA;
    if (!status && ud_member(query, "id"))
        status = ud_json_string(ud_member(query, "id"), &id);
    if (!status && ud_member(query, "afterId"))
        status = ud_json_string(ud_member(query, "afterId"), &after);
    if (!status && ud_member(query, "afterRevision"))
        status = ud_json_uint(ud_member(query, "afterRevision"), &revision);
    if (!status && ud_member(query, "sinceMs"))
        status = ud_json_uint(ud_member(query, "sinceMs"), &since);
    if (!status && ud_member(query, "untilMs"))
        status = ud_json_uint(ud_member(query, "untilMs"), &until);
    if (!status && ud_member(query, "limit"))
        status = ud_json_uint(ud_member(query, "limit"), &limit);
    if (!status && (!string_valid(id, 0) || !string_valid(after, 0) || id.length > 255 ||
                    after.length > 255 || !limit || limit > 1000 || until > INT64_MAX ||
                    since > until || (id.length && after.length && !ud_same(id, after)) ||
                    (revision && !id.length && !after.length)))
        status = UD_SCHEMA;
    if (!after.length && revision)
        after = id;
    sqlite3_stmt *s = NULL;
    if (!status)
        status = prepare(
            c,
            id.length ? "SELECT id,revision,document,digest,timestamp_ms FROM documents WHERE "
                        "tenant=?1 AND kind=?2 AND id=?3 AND (id,revision)>(?4,?5) AND "
                        "timestamp_ms>=?6 AND timestamp_ms<=?7 ORDER BY id,revision LIMIT ?8"
                      : "SELECT id,revision,document,digest,timestamp_ms FROM documents WHERE "
                        "tenant=?1 AND kind=?2 AND ?3='' AND (id,revision)>(?4,?5) AND "
                        "timestamp_ms>=?6 AND timestamp_ms<=?7 ORDER BY id,revision LIMIT ?8",
            &s);
    char number[21];
    revision_text(revision, number);
    if (!status)
        status = bind_text(s, 1, c->options.tenant);
    if (!status)
        status = sql_status(sqlite3_bind_int(s, 2, (int)kind));
    if (!status)
        status = bind_text(s, 3, id);
    if (!status)
        status = bind_text(s, 4, after);
    if (!status)
        status = bind_text(s, 5, (ud_string){number, 20});
    if (!status)
        status = sql_status(sqlite3_bind_int64(s, 6, (sqlite3_int64)since));
    if (!status)
        status = sql_status(sqlite3_bind_int64(s, 7, (sqlite3_int64)until));
    if (!status)
        status = sql_status(sqlite3_bind_int(s, 8, (int)limit));
    if (!status)
        status = collect(c, (int)kind, s, 1, json, length);
    sqlite3_finalize(s);
    ud_json_destroy(query);
    return status;
}
ud_status ud_catalog_record_event(ud_catalog *c, const ud_execution_event *event,
                                  uint64_t sequence) {
    if (!c || !event || !sequence)
        return UD_INVALID;
    if (event->struct_size != sizeof(*event) || event->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!string_valid(event->execution_id, 1) || !string_valid(event->package_id, 1))
        return UD_INVALID;
    if (event->state < UD_EXEC_CREATED || event->state > UD_EXEC_RECOVERY_REQUIRED ||
        event->kind < UD_EXEC_EVENT_STATE || event->kind > UD_EXEC_EVENT_ERROR)
        return UD_INVALID;
    ud_bytes json = {.ctx = c->ctx};
    ud_status s = ud_bytes_put(&json, "{\"version\":" UD_PROTOCOL_VERSION_TEXT,
                               sizeof("{\"version\":" UD_PROTOCOL_VERSION_TEXT) - 1);
    const char *keys[] = {"packageId", "executionId", "taskId", "pipelineId", "componentId"};
    ud_string values[] = {event->package_id, event->execution_id, event->task_id,
                          event->pipeline_id, event->component_id};
    for (int i = 0; !s && i < 5; i++) {
        if (!string_valid(values[i], 0)) {
            s = UD_INVALID;
            break;
        }
        s = ud_bytes_put(&json, ",", 1);
        if (!s)
            s = ud_json_quote(&json, keys[i], (uint32_t)strlen(keys[i]));
        if (!s)
            s = ud_bytes_put(&json, ":", 1);
        if (!s)
            s = ud_json_quote(&json, values[i].data ? values[i].data : "", values[i].length);
    }
    char numbers[160];
    int n = snprintf(numbers, sizeof(numbers),
                     ",\"state\":%d,\"result\":%d,\"timestampMs\":%llu,\"kind\":%d}", event->state,
                     event->result, (unsigned long long)event->timestamp_ms, event->kind);
    if (!s)
        s = ud_bytes_put(&json, numbers, (size_t)n);
    ud_catalog_key key = {sizeof(key), UD_ABI_VERSION, UD_CATALOG_EVENT, event->execution_id,
                          sequence};
    if (!s && json.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s)
        s = ud_catalog_put(c, &key, (ud_string){(char *)json.data, (uint32_t)json.size});
    ud_bytes_clear(&json);
    return s;
}
ud_status ud_catalog_latest_revision(ud_catalog *catalog, int32_t kind, ud_string id,
                                     uint64_t *revision) {
    if (!catalog || !revision || !kind_valid(kind) || !string_valid(id, 1))
        return UD_INVALID;
    *revision = 0;
    UD_TRY(grant(catalog, UD_ACCESS_READ));
    sqlite3_stmt *statement = NULL;
    ud_status status = prepare(catalog,
                               "SELECT revision FROM documents WHERE tenant=?1 AND kind=?2 AND "
                               "id=?3 ORDER BY revision DESC LIMIT 1",
                               &statement);
    if (!status)
        status = bind_text(statement, 1, catalog->options.tenant);
    if (!status)
        status = sql_status(sqlite3_bind_int(statement, 2, kind));
    if (!status)
        status = bind_text(statement, 3, id);
    if (!status) {
        int result = sqlite3_step(statement);
        if (result == SQLITE_DONE)
            status = UD_END;
        else if (result != SQLITE_ROW)
            status = sql_status(result);
        else {
            const char *text = (const char *)sqlite3_column_text(statement, 0);
            if (!text || sqlite3_column_bytes(statement, 0) != 20)
                status = UD_CORRUPT;
            for (uint32_t at = 0; !status && at < 20; at++) {
                unsigned digit = (unsigned char)text[at] - '0';
                if (digit > 9 || *revision > (UINT64_MAX - digit) / 10)
                    status = UD_CORRUPT;
                else
                    *revision = *revision * 10 + digit;
            }
        }
    }
    sqlite3_finalize(statement);
    if (!status) {
        char *document = NULL; uint32_t length = 0;
        ud_catalog_key key = {sizeof(key), UD_ABI_VERSION, kind, id, *revision};
        status = get_document(catalog, &key, &document, &length);
        ud_free(document);
    }
    return status;
}
void ud_catalog_close(ud_catalog *c) {
    if (c) {
        if (c->database)
            sqlite3_close_v2(c->database);
        ud_free(c->directory);
        ud_free(c->tenant);
        ud_free(c);
    }
}
