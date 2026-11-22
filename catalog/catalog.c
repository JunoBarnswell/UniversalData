#include "internal.h"
#include "ud/json.h"
/* Native Catalog owns document and lease semantics. Journals remain execution
 * recovery owners; a Catalog document is never a substitute for a commit fact. */
#define CATALOG_APPLICATION_ID 1430537025
/* Protocol family 10, storage revision 2. Older binaries reject this version;
 * Native upgrades existing documents intact under the SQLite writer lock. */
#define CATALOG_STORAGE_VERSION ((UD_CONFIG_VERSION << 16) | UD_CATALOG_FORMAT_REVISION)
#define CATALOG_STORAGE_PREVIOUS ((UD_CONFIG_VERSION << 16) | 2u)
#define CATALOG_ABI14_STORAGE ((14u << 16) | 1u)
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
static ud_status key_address_check(const ud_catalog_key *key) {
    if (!key)
        return UD_INVALID;
    if (key->struct_size != sizeof(*key) || key->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    return kind_valid(key->kind) && string_valid(key->id, 1) && key->id.length <= 255
               ? UD_OK
               : UD_INVALID;
}
static ud_status key_check(const ud_catalog_key *key) {
    UD_TRY(key_address_check(key));
    return key->revision ? UD_OK : UD_INVALID;
}
static void revision_text(uint64_t revision, char out[21]) {
    snprintf(out, 21, "%020llu", (unsigned long long)revision);
}
static ud_status bind_address(ud_catalog *c, catalog_statement *s, const ud_catalog_key *key) {
    UD_TRY(catalog_text(s, 1, c->options.tenant));
    UD_TRY(catalog_int(s, 2, key->kind));
    return catalog_text(s, 3, key->id);
}
static ud_status bind_key(ud_catalog *c, catalog_statement *s, const ud_catalog_key *key) {
    UD_TRY(bind_address(c, s, key));
    char revision[21];
    revision_text(key->revision, revision);
    return catalog_text(s, 4, (ud_string){revision, 20});
}
static ud_status digest_document_version(ud_catalog *c, const ud_catalog_key *key, ud_string json,
                                         uint64_t timestamp, uint32_t version, uint8_t out[32]) {
    ud_bytes bytes = {.ctx = c->ctx};
    ud_status s = ud_put_u64(&bytes, version);
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
static ud_status digest_document(ud_catalog *c, const ud_catalog_key *key, ud_string json,
                                 uint64_t timestamp, uint8_t out[32]) {
    return digest_document_version(c, key, json, timestamp, UD_CONFIG_VERSION, out);
}
/* SQL columns: id, revision, document, digest, timestamp_ms. Every returned row
 * is validated against its physical key, not just its JSON body. */
static ud_status row_check(ud_catalog *c, catalog_statement *s, int kind, ud_catalog_key *key,
                           ud_string *document) {
    const char *id = (const char *)catalog_column_text(s, 0);
    const char *revision = (const char *)catalog_column_text(s, 1);
    const char *json = (const char *)catalog_column_text(s, 2);
    int id_length = catalog_column_bytes(s, 0), n = catalog_column_bytes(s, 2);
    if (!id || !revision || !json || id_length <= 0 || id_length > 255 || n <= 0 ||
        catalog_column_bytes(s, 1) != 20 || catalog_column_bytes(s, 3) != 32 ||
        catalog_column_int(s, 4) < 0)
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
    UD_TRY(digest_document(c, key, *document, (uint64_t)catalog_column_int(s, 4), digest));
    if (memcmp(digest, catalog_column_blob(s, 3), 32)) {
        /* v14 history is immutable. Its domain-separated hash still proves the
         * exact stored bytes after the catalog advances to the next revision. */
        UD_TRY(digest_document_version(c, key, *document, (uint64_t)catalog_column_int(s, 4),
                                       14, digest));
        if (memcmp(digest, catalog_column_blob(s, 3), 32))
            return UD_CORRUPT;
    }
    ud_json *doc = NULL;
    ud_status status = ud_json_parse(c->ctx, json, (uint32_t)n, &doc, NULL);
    if (!status && ud_json_kind(doc) != UD_JSON_OBJECT)
        status = UD_CORRUPT;
    ud_json_destroy(doc);
    return status == UD_PARSE || status == UD_SCHEMA ? UD_CORRUPT : status;
}
static ud_status pragma_integer(ud_catalog *c, const char *query, int *value) {
    catalog_statement *s = NULL;
    ud_status status = catalog_prepare(c, query, &s);
    if (!status) {
        status = catalog_step(s);
        if (status == UD_END)
            status = UD_UNSUPPORTED;
        if (!status)
            *value = catalog_column_int(s, 0);
    }
    catalog_finalize(s);
    return status;
}
static ud_status confirm_sqlite_configuration(ud_catalog *catalog) {
    catalog_statement *statement = NULL;
    ud_status status = catalog_prepare(catalog, "PRAGMA journal_mode=WAL", &statement);
    if (!status)
        status = catalog_step(statement);
    if (!status) {
        const char *mode = catalog_column_text(statement, 0);
        if (!mode || strcmp(mode, "wal"))
            status = UD_UNSUPPORTED;
    }
    catalog_finalize(statement);
    if (status == UD_END)
        status = UD_UNSUPPORTED;
    UD_TRY(status);
    int synchronous = 0, foreign_keys = 0;
    UD_TRY(pragma_integer(catalog, "PRAGMA synchronous", &synchronous));
    UD_TRY(pragma_integer(catalog, "PRAGMA foreign_keys", &foreign_keys));
    return synchronous >= 2 && foreign_keys == 1 ? UD_OK : UD_UNSUPPORTED;
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
static ud_status open_sqlite(ud_catalog *c) {
    ud_status status = UD_OK;
    if (!status && c->options.create_directory)
        status = ud_mkdir(c->ctx, c->directory);
    char *file = NULL;
    if (!status)
        status = ud_join_path(c->ctx, c->directory, "catalog.sqlite3", &file);
    if (!status)
        status = ud_path_no_symlinks(c->ctx, file);
    if (!status) {
        int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX;
        if (c->options.create_directory)
            flags |= SQLITE_OPEN_CREATE;
        status = catalog_sqlite_status(sqlite3_open_v2(file, &c->database, flags, NULL));
    }
    ud_free(file);
    if (!status)
        status = catalog_sqlite_status(sqlite3_busy_timeout(c->database, 5000));
    if (!status)
        status = ud_catalog_sql(c, "PRAGMA synchronous=FULL;PRAGMA foreign_keys=ON;"
                                   "PRAGMA trusted_schema=OFF");
    int application = 0, version = 0;
    if (!status)
        status = pragma_integer(c, "PRAGMA application_id", &application);
    if (!status)
        status = pragma_integer(c, "PRAGMA user_version", &version);
    if (!status && application &&
        (application != CATALOG_APPLICATION_ID ||
         (version != UD_CONFIG_VERSION && version != CATALOG_STORAGE_PREVIOUS &&
          version != CATALOG_STORAGE_VERSION &&
          version != CATALOG_ABI14_STORAGE)))
        status = UD_INCOMPATIBLE;
    if (!status && !application) {
        status = ud_catalog_sql(c, "BEGIN IMMEDIATE");
        int transaction = !status;
        /* A competing first opener may have initialized the file while this
         * handle waited or between the two autocommit PRAGMA reads. Only
         * retry the initially unclaimed header under the writer lock. */
        if (!status)
            status = pragma_integer(c, "PRAGMA application_id", &application);
        if (!status)
            status = pragma_integer(c, "PRAGMA user_version", &version);
        if (!status && (application || version) &&
            (application != CATALOG_APPLICATION_ID ||
             (version != UD_CONFIG_VERSION && version != CATALOG_STORAGE_PREVIOUS &&
          version != CATALOG_STORAGE_VERSION &&
              version != CATALOG_ABI14_STORAGE)))
            status = UD_INCOMPATIBLE;
        int tables = 0;
        if (!status && !application)
            status = pragma_integer(
                c, "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'", &tables);
        if (!status && !application && tables)
            status = UD_INCOMPATIBLE;
        if (!status && !application)
            status = ud_catalog_sql(
                c,
                "CREATE TABLE documents(tenant TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT "
                "NULL,"
                "revision TEXT NOT NULL CHECK(length(revision)=20),document TEXT NOT NULL,digest "
                "BLOB NOT NULL CHECK(length(digest)=32),"
                "timestamp_ms INTEGER NOT NULL CHECK(timestamp_ms>=0),PRIMARY "
                "KEY(tenant,kind,id,revision)) WITHOUT ROWID;"
                "CREATE INDEX document_time ON documents(tenant,kind,timestamp_ms,id,revision);"
                "CREATE INDEX event_task ON "
                "documents(tenant,kind,(document ->> 'taskId'),timestamp_ms,id,revision);"
                "CREATE INDEX event_component ON "
                "documents(tenant,kind,(document ->> 'componentId'),timestamp_ms,id,"
                "revision);"
                "PRAGMA application_id=1430537025;PRAGMA user_version=" UD_PROTOCOL_VERSION_TEXT);
        if (transaction)
            status = ud_catalog_finish(c, status);
    }
    if (!status && version != CATALOG_STORAGE_VERSION) {
        status = ud_catalog_sql(c, "BEGIN IMMEDIATE");
        int transaction = !status;
        if (!status && version == CATALOG_ABI14_STORAGE) {
            int active = 0;
            status = pragma_integer(c,
                "SELECT count(*) FROM leases WHERE until_ms>CAST(unixepoch('subsec')*1000 AS INTEGER)",
                &active);
            if (!status && active)
                status = UD_BUSY;
        }
        if (!status)
            status = ud_catalog_sql(c,
                "CREATE TABLE IF NOT EXISTS catalog_clock(id INTEGER PRIMARY KEY CHECK(id=1),"
                "milliseconds INTEGER NOT NULL CHECK(milliseconds>=0));"
                "INSERT OR IGNORE INTO catalog_clock VALUES(1,0);"
                "CREATE TABLE IF NOT EXISTS lease_pools(tenant TEXT NOT NULL,pool TEXT NOT NULL,"
                "capacity INTEGER NOT NULL CHECK(capacity>0),PRIMARY KEY(tenant,pool)) WITHOUT ROWID;"
                "CREATE TABLE IF NOT EXISTS leases(tenant TEXT NOT NULL,kind INTEGER NOT NULL,"
                "id TEXT NOT NULL,owner TEXT NOT NULL,pool TEXT NOT NULL,generation INTEGER NOT NULL "
                "CHECK(generation>0),until_ms INTEGER NOT NULL CHECK(until_ms>=0),"
                "PRIMARY KEY(tenant,kind,id)) WITHOUT ROWID;"
                "CREATE INDEX IF NOT EXISTS lease_capacity ON leases(tenant,pool,until_ms);"
                "CREATE TABLE IF NOT EXISTS run_heads(tenant TEXT NOT NULL,id TEXT NOT NULL,"
                "revision TEXT NOT NULL,state TEXT NOT NULL,timestamp_ms INTEGER NOT NULL,"
                "not_before_ms INTEGER NOT NULL,root INTEGER NOT NULL,PRIMARY KEY(tenant,id)) WITHOUT ROWID;"
                "CREATE INDEX IF NOT EXISTS run_dispatch ON run_heads(tenant,state,root,not_before_ms,timestamp_ms,id);"
                "INSERT OR IGNORE INTO run_heads SELECT tenant,id,revision,"
                "coalesce(document ->> 'state',''),timestamp_ms,"
                "coalesce(CAST(document ->> 'notBeforeMs' AS BIGINT),0),"
                "CASE WHEN document ->> 'parentId' IS NULL THEN 1 ELSE 0 END "
                "FROM documents WHERE kind=7 AND id LIKE 'run/%' ORDER BY revision DESC");
        if (!status) {
            char version_sql[64];
            snprintf(version_sql,sizeof(version_sql),"PRAGMA user_version=%u",CATALOG_STORAGE_VERSION);
            status=ud_catalog_sql(c,version_sql);
        }
        if (transaction)
            status = ud_catalog_finish(c, status);
    }
    if (!status)
        status = confirm_sqlite_configuration(c);
    return status;
}
static ud_status open_catalog(ud_context *ctx, const ud_catalog_options *o, ud_string connection, ud_catalog **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !o)
        return UD_INVALID;
    if (o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!string_valid(connection, 0) || connection.length > 32767 ||
        !string_valid(o->directory, 1) || !string_valid(o->tenant, 1) ||
        o->directory.length > 32767 || o->tenant.length > 255 || o->create_directory < 0 ||
        o->create_directory > 1)
        return UD_INVALID;
#ifdef _WIN32
    if (!connection.length && (o->directory.length < 3 || o->directory.data[1] != ':'))
        return UD_INVALID;
#else
    if (!connection.length && o->directory.data[0] != '/')
        return UD_INVALID;
#endif
    ud_catalog *c = ud_alloc(ctx, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    c->ctx = ctx;
    c->remote = connection.length != 0;
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
    if (!status)
        status = c->remote ? catalog_postgresql_open(c, connection) : open_sqlite(c);
    if (status) {
        ud_catalog_close(c);
        return status;
    }
    *out = c;
    return UD_OK;
}
ud_status ud_catalog_open(ud_context *ctx, const ud_catalog_options *options, ud_catalog **out) {
    return open_catalog(ctx, options, (ud_string){0}, out);
}

ud_status ud_catalog_open_postgresql(ud_context *ctx, const ud_catalog_options *options, ud_string connection,
                                     ud_catalog **out) {
    if (!connection.length)
        return UD_INVALID;
    return open_catalog(ctx, options, connection, out);
}

static ud_status get_document(ud_catalog *c, const ud_catalog_key *key, char **json,
                              uint32_t *length) {
    if (!c || !json || !length)
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(key_address_check(key));
    catalog_statement *s = NULL;
    const char *query = key->revision
        ? "SELECT id,revision,document,digest,timestamp_ms FROM documents "
          "WHERE tenant=?1 AND kind=?2 AND id=?3 AND revision=?4"
        : "SELECT id,revision,document,digest,timestamp_ms FROM documents "
          "WHERE tenant=?1 AND kind=?2 AND id=?3 ORDER BY revision DESC LIMIT 1";
    ud_status status = catalog_prepare(c, query, &s);
    if (!status)
        status = key->revision ? bind_key(c, s, key) : bind_address(c, s, key);
    if (!status) {
        ud_status rc = catalog_step(s);
        if (rc == UD_END)
            status = UD_END;
        else if (rc != UD_OK)
            status = rc;
        else {
            ud_catalog_key actual;
            ud_string document;
            status = row_check(c, s, key->kind, &actual, &document);
            if (!status && ((key->revision && actual.revision != key->revision) ||
                            !ud_same(actual.id, key->id)))
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
    catalog_finalize(s);
    return status;
}
ud_status ud_catalog_get(ud_catalog *c, const ud_catalog_key *key, char **json, uint32_t *length) {
    if (!c || !json || !length)
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(grant(c, UD_ACCESS_READ));
    UD_TRY(key_check(key));
    return get_document(c, key, json, length);
}
ud_status ud_catalog_get_confirmed(ud_catalog *catalog, const ud_catalog_key *key, char **json,
                                  uint32_t *length) {
    if (!catalog || !json || !length)
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(grant(catalog, UD_ACCESS_READ));
    UD_TRY(key_address_check(key));
    if (!catalog->remote)
        return get_document(catalog, key, json, length);
    UD_TRY(ud_catalog_begin(catalog, 0));
    ud_status status = get_document(catalog, key, json, length);
    return ud_catalog_finish_confirmation(catalog, status, json, length);
}
static ud_status lease_guard(ud_catalog *,const ud_catalog_lease *,int32_t,ud_string);
static ud_status lease_check(const ud_catalog_lease *);
static ud_status lease_end(ud_catalog *, ud_status);

static ud_status update_run_head(ud_catalog *catalog, const ud_catalog_key *key) {
    if (key->kind != UD_CATALOG_AUDIT || key->id.length < 5 || memcmp(key->id.data,"run/",4))
        return UD_OK;
    catalog_statement *statement = NULL;
    ud_status status = catalog_prepare(catalog,
        "INSERT INTO run_heads(tenant,id,revision,state,timestamp_ms,not_before_ms,root) "
        "SELECT tenant,id,revision,coalesce(document ->> 'state',''),timestamp_ms,"
        "coalesce(CAST(document ->> 'notBeforeMs' AS BIGINT),0),"
        "CASE WHEN document ->> 'parentId' IS NULL THEN 1 ELSE 0 END FROM documents "
        "WHERE tenant=?1 AND kind=?2 AND id=?3 AND revision=?4 "
        "ON CONFLICT(tenant,id) DO UPDATE SET revision=excluded.revision,state=excluded.state,"
        "timestamp_ms=excluded.timestamp_ms,not_before_ms=excluded.not_before_ms,root=excluded.root "
        "WHERE run_heads.revision<excluded.revision", &statement);
    if (!status) status = bind_key(catalog, statement, key);
    if (!status) status = catalog_execute(statement);
    catalog_finalize(statement);
    return status;
}
ud_status ud_catalog_append_document(ud_catalog *c, const ud_catalog_key *key, ud_string input,
                                     int conditional, uint64_t expected,
                                     const ud_catalog_lease *lease, int own_transaction) {
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
    int observation = !status && key->kind == UD_CATALOG_EXECUTION &&
                      ud_json_kind(ud_member(doc,"progress")) == UD_JSON_OBJECT;
    ud_json_destroy(doc);
    uint8_t digest[32];
    if (!status)
        status = digest_document(c, key, (ud_string){json, length}, timestamp, digest);
    int transaction = 0;
    if (!status && own_transaction) {
        status = ud_catalog_begin(c, 0);
        transaction = !status;
    }
    if (!status && conditional) {
        uint64_t latest = 0;
        ud_status found = ud_catalog_latest_revision(c,key->kind,key->id,&latest);
        if (found != UD_OK && found != UD_END)
            status = found;
        else if (latest != expected)
            status = UD_FENCED;
    }
    if (!status)
        status = lease_guard(c,lease,key->kind,key->id);
    char *previous = NULL;
    uint32_t previous_length = 0;
    if (!status) {
        ud_status found = get_document(c, key, &previous, &previous_length);
        if (found == UD_OK) {
            if (length != previous_length || memcmp(previous, json, length))
                status = UD_CORRUPT;
        } else if (found == UD_END) {
            catalog_statement *s = NULL;
            status = catalog_prepare(
                c,
                "INSERT INTO documents(tenant,kind,id,revision,document,digest,timestamp_ms) "
                "VALUES(?1,?2,?3,?4,?5,?6,?7)",
                &s);
            if (!status)
                status = bind_key(c, s, key);
            if (!status)
                status = catalog_text(s, 5, (ud_string){json, length});
            if (!status)
                status = catalog_blob(s, 6, digest, 32);
            if (!status)
                status = catalog_int(s, 7, (int64_t)timestamp);
            if (!status)
                status = catalog_execute(s);
            catalog_finalize(s);
            if (!status) status = update_run_head(c, key);
        } else
            status = found;
    }
    if (!status && observation) {
        // Retain eight recent observation revisions; immutable Ready/terminal,
        // Native events, Source positions and Journal snapshots are untouched.
        catalog_statement *prune = NULL;
        status = catalog_prepare(c,"DELETE FROM documents WHERE tenant=?1 AND kind=?2 AND id=?3 "
            "AND substr(CAST(document -> 'progress' AS TEXT),1,1)='{' AND revision NOT IN "
            "(SELECT revision FROM documents WHERE tenant=?1 AND kind=?2 AND id=?3 "
            "AND substr(CAST(document -> 'progress' AS TEXT),1,1)='{' ORDER BY revision DESC LIMIT 8)",&prune);
        if (!status) status=catalog_text(prune,1,c->options.tenant);
        if (!status) status=catalog_int(prune,2,key->kind);
        if (!status) status=catalog_text(prune,3,key->id);
        if (!status) status=catalog_execute(prune);
        catalog_finalize(prune);
    }
    if (transaction) {
        /* A rejected fencing check has not appended data. Persist its clock
         * observation so a later wall-clock correction cannot revive expiry. */
        status = status == UD_FENCED ? lease_end(c, status) : ud_catalog_finish(c, status);
    }
    ud_free(previous);
    ud_free(json);
    return status;
}
ud_status ud_catalog_put(ud_catalog *c, const ud_catalog_key *key, ud_string input) {
    return ud_catalog_append_document(c, key, input, 0, 0, NULL, 1);
}
ud_status ud_catalog_compare_exchange(ud_catalog *c,const ud_catalog_key *key,ud_string input,
                                       uint64_t expected,const ud_catalog_lease *lease) {
    if (!key || expected == UINT64_MAX || key->revision != expected + 1)
        return UD_INVALID;
    return ud_catalog_append_document(c, key, input, 1, expected, lease, 1);
}
static ud_status collect(ud_catalog *c, int kind, catalog_statement *s, int documents, char **json,
                         uint32_t *length) {
    ud_bytes bytes = {.ctx = c->ctx, .limit = 64u * 1024u * 1024u};
    ud_status status = ud_bytes_put(&bytes, "[", 1);
    uint32_t count = 0;
    while (!status) {
        ud_status rc = catalog_step(s);
        if (rc == UD_END)
            break;
        if (rc != UD_OK) {
            status = rc;
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
    catalog_statement *s = NULL;
    ud_status status = catalog_prepare(c,
                               "SELECT id,revision,document,digest,timestamp_ms FROM documents "
                               "WHERE tenant=?1 AND kind=?2 ORDER BY id,revision",
                               &s);
    if (!status)
        status = catalog_text(s, 1, c->options.tenant);
    if (!status)
        status = catalog_int(s, 2, kind);
    if (!status)
        status = collect(c, kind, s, 0, json, length);
    catalog_finalize(s);
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
    const char *keys[] = {"kind",  "id",     "afterId",    "afterRevision", "sinceMs", "untilMs",
                          "limit", "taskId", "pipelineId", "componentId",   "result",  "eventKind", "latestOnly"};
    if (!status)
        status = ud_json_keys(query, keys, 13);
    int latest_only=0;
    const ud_json *latest=ud_member(query,"latestOnly");
    if (!status && latest) {
        ud_string value={0};
        status=ud_json_kind(latest)==UD_JSON_BOOL ? ud_json_text(latest,&value.data,&value.length) : UD_SCHEMA;
        if (!status) latest_only=ud_named(value,"true");
    }
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
    ud_string task = {0}, pipeline = {0}, component = {0};
    uint64_t result_filter = UINT64_MAX, event_filter = UINT64_MAX;
    const char *names[] = {"taskId", "pipelineId", "componentId"};
    ud_string *values[] = {&task, &pipeline, &component};
    for (uint32_t at = 0; !status && at < 3; at++)
        if (ud_member(query, names[at]))
            status = ud_json_string(ud_member(query, names[at]), values[at]);
    if (!status && ud_member(query, "result"))
        status = ud_json_uint(ud_member(query, "result"), &result_filter);
    if (!status && ud_member(query, "eventKind"))
        status = ud_json_uint(ud_member(query, "eventKind"), &event_filter);
    if (!status &&
        ((result_filter != UINT64_MAX && result_filter > UD_STATUS_LAST) ||
         (event_filter != UINT64_MAX && (!event_filter || event_filter > UD_EXEC_EVENT_ERROR)) ||
         (kind != UD_CATALOG_EVENT && (task.length || pipeline.length || component.length ||
                                       result_filter != UINT64_MAX || event_filter != UINT64_MAX))))
        status = UD_SCHEMA;
    catalog_statement *s = NULL;
    if (!status)
        status = catalog_prepare(
            c,
            id.length ? "SELECT id,revision,document,digest,timestamp_ms FROM documents WHERE "
                        "tenant=?1 AND kind=?2 AND id=?3 AND (id,revision)>(?4,?5) AND "
                        "timestamp_ms>=?6 AND timestamp_ms<=?7 AND (?9='' OR "
                        "(document ->> 'taskId')=?9) AND "
                        "(?10='' OR (document ->> 'pipelineId')=?10) AND (?11='' OR "
                        "(document ->> 'componentId')=?11) AND "
                        "(?12=-1 OR CAST(document ->> 'result' AS TEXT)=CAST(?12 AS TEXT)) AND (?13=-1 OR "
                        "CAST(document ->> 'kind' AS TEXT)=CAST(?13 AS TEXT)) AND (?14=0 OR NOT EXISTS(SELECT 1 "
                        "FROM documents newer WHERE newer.tenant=documents.tenant AND newer.kind=documents.kind "
                        "AND newer.id=documents.id AND newer.revision>documents.revision)) ORDER BY id,revision LIMIT ?8"
                      : "SELECT id,revision,document,digest,timestamp_ms FROM documents WHERE "
                        "tenant=?1 AND kind=?2 AND ?3='' AND (id,revision)>(?4,?5) AND "
                        "timestamp_ms>=?6 AND timestamp_ms<=?7 AND (?9='' OR "
                        "(document ->> 'taskId')=?9) AND "
                        "(?10='' OR (document ->> 'pipelineId')=?10) AND (?11='' OR "
                        "(document ->> 'componentId')=?11) AND "
                        "(?12=-1 OR CAST(document ->> 'result' AS TEXT)=CAST(?12 AS TEXT)) AND (?13=-1 OR "
                        "CAST(document ->> 'kind' AS TEXT)=CAST(?13 AS TEXT)) AND (?14=0 OR NOT EXISTS(SELECT 1 "
                        "FROM documents newer WHERE newer.tenant=documents.tenant AND newer.kind=documents.kind "
                        "AND newer.id=documents.id AND newer.revision>documents.revision)) ORDER BY id,revision LIMIT ?8",
            &s);
    char number[21];
    revision_text(revision, number);
    if (!status)
        status = catalog_text(s, 1, c->options.tenant);
    if (!status)
        status = catalog_int(s, 2, (int)kind);
    if (!status)
        status = catalog_text(s, 3, id);
    if (!status)
        status = catalog_text(s, 4, after);
    if (!status)
        status = catalog_text(s, 5, (ud_string){number, 20});
    if (!status)
        status = catalog_int(s, 6, (int64_t)since);
    if (!status)
        status = catalog_int(s, 7, (int64_t)until);
    if (!status)
        status = catalog_int(s, 8, (int)limit);
    if (!status)
        status = catalog_text(s, 9, task);
    if (!status)
        status = catalog_text(s, 10, pipeline);
    if (!status)
        status = catalog_text(s, 11, component);
    if (!status)
        status = catalog_int(
            s, 12, result_filter == UINT64_MAX ? -1 : (int64_t)result_filter);
    if (!status)
        status = catalog_int(
            s, 13, event_filter == UINT64_MAX ? -1 : (int64_t)event_filter);
    if (!status)
        status=catalog_int(s,14,latest_only);
    if (!status)
        status = collect(c, (int)kind, s, 1, json, length);
    catalog_finalize(s);
    ud_json_destroy(query);
    return status;
}
ud_status ud_catalog_query_runs(ud_catalog *catalog, ud_string state, int32_t roots_only,
                                uint64_t due_ms, uint32_t max_rows, uint64_t after_due, uint64_t after_timestamp,
                                ud_string after_id, char **json, uint32_t *length) {
    if (!catalog || !json || !length || !string_valid(state,1) || state.length > 32 ||
        roots_only < 0 || roots_only > 1 || due_ms > INT64_MAX || !max_rows || max_rows > 1000 ||
        after_due > INT64_MAX || after_timestamp > INT64_MAX || !string_valid(after_id,0) || after_id.length > 255)
        return UD_INVALID;
    *json = NULL;
    *length = 0;
    UD_TRY(grant(catalog, UD_ACCESS_READ));
    catalog_statement *statement = NULL;
    ud_status status = catalog_prepare(catalog,
        "SELECT d.id,d.revision,d.document,d.digest,d.timestamp_ms FROM "
        "(SELECT tenant,id,revision,not_before_ms,timestamp_ms FROM run_heads WHERE tenant=?1 AND state=?2 "
        "AND (?3=0 OR root=1) AND not_before_ms<=?4 AND (not_before_ms,timestamp_ms,id)>(?6,?7,?8) "
        "ORDER BY not_before_ms,timestamp_ms,id LIMIT ?5) h "
        "JOIN documents d ON d.tenant=h.tenant AND d.kind=7 AND d.id=h.id AND d.revision=h.revision "
        "ORDER BY h.not_before_ms,h.timestamp_ms,h.id", &statement);
    if (!status) status = catalog_text(statement, 1, catalog->options.tenant);
    if (!status) status = catalog_text(statement, 2, state);
    if (!status) status = catalog_int(statement, 3, roots_only);
    if (!status) status = catalog_int(statement, 4, (int64_t)due_ms);
    if (!status) status = catalog_int(statement, 5, max_rows);
    if (!status) status = catalog_int(statement, 6, (int64_t)after_due);
    if (!status) status = catalog_int(statement, 7, (int64_t)after_timestamp);
    if (!status) status = catalog_text(statement, 8, after_id);
    if (!status) status = collect(catalog, UD_CATALOG_AUDIT, statement, 1, json, length);
    catalog_finalize(statement);
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
    if (event->state < UD_EXEC_CREATED || event->state > UD_EXEC_STOPPED ||
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
    if (!s && event->error_chain.length) {
        ud_json *chain = NULL;
        s = ud_json_parse(c->ctx, event->error_chain.data, event->error_chain.length, &chain, NULL);
        if (!s && (ud_json_kind(chain) != UD_JSON_ARRAY || ud_json_size(chain) > 32))
            s = UD_INVALID;
        ud_json_destroy(chain);
        if (!s) {
            json.size--;
            s = ud_bytes_put(&json, ",\"errorChain\":", 14);
        }
        if (!s)
            s = ud_bytes_put(&json, event->error_chain.data, event->error_chain.length);
        if (!s)
            s = ud_bytes_put(&json, "}", 1);
    }
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
    catalog_statement *statement = NULL;
    ud_status status = catalog_prepare(catalog,
                               "SELECT revision FROM documents WHERE tenant=?1 AND kind=?2 AND "
                               "id=?3 ORDER BY revision DESC LIMIT 1",
                               &statement);
    if (!status)
        status = catalog_text(statement, 1, catalog->options.tenant);
    if (!status)
        status = catalog_int(statement, 2, kind);
    if (!status)
        status = catalog_text(statement, 3, id);
    if (!status) {
        ud_status result = catalog_step(statement);
        if (result == UD_END)
            status = UD_END;
        else if (result != UD_OK)
            status = result;
        else {
            const char *text = (const char *)catalog_column_text(statement, 0);
            if (!text || catalog_column_bytes(statement, 0) != 20)
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
    catalog_finalize(statement);
    if (!status) {
        char *document = NULL;
        uint32_t length = 0;
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
#ifdef UD_WITH_POSTGRESQL_CATALOG
        if (c->postgresql)
            PQfinish(c->postgresql);
#endif
        ud_free(c->leader_owner);
        ud_free(c->directory);
        ud_free(c->tenant);
        ud_free(c);
    }
}
ud_status ud_catalog_prune_events(ud_catalog *catalog, uint64_t before_ms, uint32_t limit,
                                  uint64_t *removed) {
    if (!catalog || !removed || before_ms > INT64_MAX || !limit || limit > 10000)
        return UD_INVALID;
    *removed = 0;
    UD_TRY(grant(catalog, UD_ACCESS_DELETE));
    UD_TRY(ud_catalog_begin(catalog, 0));
    catalog_statement *statement = NULL;
    ud_status status = catalog_prepare(catalog,
                               "DELETE FROM documents WHERE (tenant,kind,id,revision) IN ("
                               "SELECT d.tenant,d.kind,d.id,d.revision FROM documents d WHERE "
                               "d.tenant=?1 AND d.kind=6 AND d.timestamp_ms<?2 "
                               "AND d.revision<(SELECT max(e.revision) FROM documents e WHERE "
                               "e.tenant=d.tenant AND e.kind=d.kind AND e.id=d.id) "
                               "ORDER BY d.timestamp_ms,d.id,d.revision LIMIT ?3)",
                               &statement);
    if (!status)
        status = catalog_text(statement, 1, catalog->options.tenant);
    if (!status)
        status = catalog_int(statement, 2, (int64_t)before_ms);
    if (!status)
        status = catalog_int(statement, 3, (int)limit);
    if (!status)
        status = catalog_execute(statement);
    uint64_t changed = !status ? catalog->changed : 0;
    catalog_finalize(statement);
    status = ud_catalog_finish(catalog, status);
    if (!status)
        *removed = changed;
    return status;
}

ud_status ud_catalog_protect_parameters(ud_catalog *catalog, const ud_catalog_key *key,
                                        ud_string reference) {
    if (!catalog || !key || key->kind != UD_CATALOG_AUDIT || !string_valid(reference, 1) ||
        reference.length > 255) return UD_INVALID;
    UD_TRY(key_check(key));
    if (key->id.length < 5 || (memcmp(key->id.data,"run/",4) && memcmp(key->id.data,"job/",4))) return UD_INVALID;
    UD_TRY(grant(catalog, UD_ACCESS_DELETE));
    UD_TRY(grant(catalog, UD_ACCESS_WRITE));
    if (catalog->remote) return UD_UNSUPPORTED;
    UD_TRY(ud_catalog_sql(catalog, "PRAGMA secure_delete=ON"));
    UD_TRY(ud_catalog_begin(catalog, 0));
    char *original = NULL;
    uint32_t original_length = 0;
    ud_status status = get_document(catalog, key, &original, &original_length);
    ud_json *document = NULL;
    if (!status) status = ud_json_parse(catalog->ctx, original, original_length, &document, NULL);
    const ud_json *parameters = ud_member(document, "parameters");
    if (!status && !parameters) {
        ud_string existing;
        status = ud_json_string(ud_member(document, "parametersReference"), &existing);
        if (!status && !ud_same(existing, reference)) status = UD_CORRUPT;
    }
    ud_bytes bytes = {.ctx = catalog->ctx};
    if (!status && parameters) status = ud_bytes_put(&bytes, "{", 1);
    uint32_t copied = 0;
    for (uint32_t at = 0; !status && parameters && at < ud_json_size(document); at++) {
        const char *name; uint32_t size;
        status = ud_json_key(document, at, &name, &size);
        if (!status && ((size == 10 && !memcmp(name,"parameters",10)) ||
                        (size == 19 && !memcmp(name,"parametersReference",19)))) continue;
        if (!status && copied++) status = ud_bytes_put(&bytes, ",", 1);
        if (!status) status = ud_json_quote(&bytes, name, size);
        if (!status) status = ud_bytes_put(&bytes, ":", 1);
        char *value = NULL; uint32_t length = 0;
        if (!status) status = ud_json_serialize(ud_json_at(document,at), &value, &length);
        if (!status) status = ud_bytes_put(&bytes, value, length);
        ud_free(value);
    }
    if (!status && parameters) status = ud_bytes_put(&bytes, copied ? ",\"parametersReference\":" : "\"parametersReference\":", copied ? 23 : 22);
    if (!status && parameters) status = ud_json_quote(&bytes, reference.data, reference.length);
    if (!status && parameters) status = ud_bytes_put(&bytes, "}", 1);
    uint64_t timestamp = 0;
    if (!status && ud_member(document,"timestampMs")) status = ud_json_uint(ud_member(document,"timestampMs"), &timestamp);
    uint8_t digest[32];
    if (!status && parameters) status = digest_document(catalog,key,(ud_string){(char *)bytes.data,(uint32_t)bytes.size},timestamp,digest);
    catalog_statement *statement = NULL;
    if (!status && parameters) status = catalog_prepare(catalog,
        "UPDATE documents SET document=?5,digest=?6 WHERE tenant=?1 AND kind=?2 AND id=?3 AND revision=?4", &statement);
    if (!status && parameters) status = bind_key(catalog,statement,key);
    if (!status && parameters) status = catalog_text(statement,5,(ud_string){(char *)bytes.data,(uint32_t)bytes.size});
    if (!status && parameters) status = catalog_blob(statement,6,digest,32);
    if (!status && parameters) status = catalog_execute(statement);
    catalog_finalize(statement);
    status = ud_catalog_finish(catalog, status);
    ud_free(original); ud_json_destroy(document); ud_bytes_clear(&bytes);
    return status;
}
ud_status ud_catalog_secure_compact(ud_catalog *catalog) {
    if (!catalog) return UD_INVALID;
    UD_TRY(grant(catalog,UD_ACCESS_DELETE)); UD_TRY(grant(catalog,UD_ACCESS_WRITE));
    if (catalog->remote) return UD_UNSUPPORTED;
    /* Exclusive maintenance is a host operation. A busy WAL reader blocks the
     * proof; it cannot silently leave a plaintext historical WAL behind. */
    int log = 0, checkpointed = 0;
    UD_TRY(catalog_sqlite_status(sqlite3_wal_checkpoint_v2(catalog->database,NULL,SQLITE_CHECKPOINT_TRUNCATE,&log,&checkpointed)));
    if (log != checkpointed) return UD_BUSY;
    UD_TRY(ud_catalog_sql(catalog,"PRAGMA secure_delete=ON;VACUUM"));
    UD_TRY(catalog_sqlite_status(sqlite3_wal_checkpoint_v2(catalog->database,NULL,SQLITE_CHECKPOINT_TRUNCATE,&log,&checkpointed)));
    return log == checkpointed ? UD_OK : UD_BUSY;
}

/* Lease decisions and conditional document appends share a serialized Native transaction. */
#define LEADERSHIP_TENANT "__ud_cluster"
#define LEADERSHIP_ID "leadership"
#define PG_CLOCK "CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)"
static int election_key(const ud_catalog *catalog, int32_t kind, ud_string id) {
    return catalog->remote && kind == UD_CATALOG_AUDIT &&
        ud_named(catalog->options.tenant, LEADERSHIP_TENANT) && ud_named(id, LEADERSHIP_ID);
}
static ud_status catalog_clock_read(ud_catalog *c,uint64_t *now) {
    catalog_statement *statement = NULL;
    ud_status status = catalog_prepare(c,"SELECT milliseconds FROM catalog_clock WHERE id=1",&statement);
    if (!status) {
        ud_status rc=catalog_step(statement);
        if (rc != UD_OK || catalog_column_int(statement,0)<0)
            status=rc != UD_END && rc != UD_OK ? rc : UD_CORRUPT;
        else
            *now=(uint64_t)catalog_column_int(statement,0);
    }
    catalog_finalize(statement);
    return status;
}
static ud_status lease_clock(ud_catalog *c,uint64_t *now) {
    uint64_t wall = ud_wall_ms();
    if (wall > INT64_MAX) return UD_OVERFLOW;
    catalog_statement *statement = NULL;
    ud_status status = catalog_prepare(c,c->remote
        ? "UPDATE catalog_clock SET milliseconds=greatest(milliseconds," PG_CLOCK ") WHERE id=1"
        : "UPDATE catalog_clock SET milliseconds=max(milliseconds,?1) WHERE id=1",&statement);
    if (!status && !c->remote) status = catalog_int(statement,1,(int64_t)wall);
    if (!status) status = catalog_execute(statement);
    if (!status && c->changed != 1) status = UD_CORRUPT;
    catalog_finalize(statement); statement = NULL;
    if (!status) status = catalog_clock_read(c,now);
    return status;
}
static ud_status lease_key(ud_catalog *c,catalog_statement *statement,int32_t kind,ud_string id) {
    UD_TRY(catalog_text(statement,1,c->options.tenant));
    UD_TRY(catalog_int(statement,2,kind));
    return catalog_text(statement,3,id);
}
static ud_status lease_check(const ud_catalog_lease *lease) {
    if (!lease) return UD_INVALID;
    if (lease->struct_size != sizeof(*lease) || lease->abi_version != UD_ABI_VERSION) return UD_INCOMPATIBLE;
    return kind_valid(lease->kind) && string_valid(lease->id,1) && lease->id.length<=255 &&
           string_valid(lease->owner,1) && lease->owner.length<=255 && lease->generation &&
           lease->generation<=INT64_MAX ? UD_OK : UD_INVALID;
}
static ud_status lease_guard(ud_catalog *c,const ud_catalog_lease *lease,int32_t kind,ud_string id) {
    if (lease) {
        UD_TRY(lease_check(lease));
        if (lease->kind!=kind || !ud_same(lease->id,id)) return UD_FENCED;
    }
    catalog_statement *statement=NULL;
    ud_status status=catalog_prepare(c,"SELECT owner,generation,until_ms FROM leases WHERE tenant=?1 AND kind=?2 AND id=?3",&statement);
    if (!status) status=lease_key(c,statement,kind,id);
    uint64_t now=0;
    if (!status) status=lease_clock(c,&now);
    if (!status) {
        ud_status rc=catalog_step(statement);
        if (rc==UD_OK) {
            const char *owner=(const char *)catalog_column_text(statement,0);
            int n=catalog_column_bytes(statement,0);
            if (!lease || !owner || n!=(int)lease->owner.length || memcmp(owner,lease->owner.data,(size_t)n) ||
                catalog_column_int(statement,1)!=(int64_t)lease->generation ||
                catalog_column_int(statement,2)<=(int64_t)now) status=UD_FENCED;
        } else if (rc==UD_END) status=lease ? UD_FENCED : UD_OK;
        else status=rc;
    }
    catalog_finalize(statement); return status;
}
ud_status ud_catalog_set_leadership(ud_catalog *catalog, ud_string owner, uint64_t generation) {
    if (!catalog || !catalog->remote || catalog->leader_owner || !string_valid(owner, 1) ||
        owner.length > 255 || !generation || generation > INT64_MAX)
        return UD_INVALID;
    catalog->leader_owner = ud_strdup_n(catalog->ctx,owner.data,owner.length);
    if (!catalog->leader_owner) return UD_RESOURCE;
    catalog->leader_generation = generation;
    return UD_OK;
}

ud_status ud_catalog_begin(ud_catalog *catalog, int election) {
    if (!catalog->remote)
        return ud_catalog_sql(catalog, "BEGIN IMMEDIATE");
    if (!election && !catalog->leader_generation)
        return UD_FENCED;
#ifdef UD_WITH_POSTGRESQL_CATALOG
    UD_TRY(catalog_postgresql_begin(catalog, "BEGIN"));
    uint64_t now = 0;
    ud_status status = catalog_postgresql_admit(catalog);
    if (!status)
        status = lease_clock(catalog, &now);
    catalog_statement *statement = NULL;
    if (!status && !election) {
        status = catalog_prepare(catalog,
            "SELECT 1 FROM leases WHERE tenant='" LEADERSHIP_TENANT "' AND kind=7 AND id='" LEADERSHIP_ID
            "' AND owner=?1 AND generation=?2 AND until_ms>?3", &statement);
        if (!status) status = catalog_text(statement, 1,
            (ud_string){catalog->leader_owner, (uint32_t)strlen(catalog->leader_owner)});
        if (!status) status = catalog_int(statement, 2, (int64_t)catalog->leader_generation);
        if (!status) status = catalog_int(statement, 3, (int64_t)now);
        if (!status) status = catalog_step(statement);
    }
    catalog_finalize(statement);
    if (status == UD_END)
        return lease_end(catalog, UD_FENCED);
    return status ? ud_catalog_finish(catalog, status) : UD_OK;
#else
    return UD_UNSUPPORTED;
#endif
}

ud_status ud_catalog_finish(ud_catalog *catalog, ud_status status) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
    if (catalog->remote)
        return catalog_postgresql_finish(catalog, status);
#endif
    if (!status) {
        status = ud_catalog_sql(catalog, "COMMIT");
        if (status == UD_IO)
            status = UD_COMMIT_UNKNOWN;
    }
    if (status) (void)ud_catalog_sql(catalog, "ROLLBACK");
    return status;
}

ud_status ud_catalog_finish_confirmation(ud_catalog *catalog, ud_status observed, char **json,
                                         uint32_t *length) {
    ud_status status = ud_catalog_finish(catalog,
        observed == UD_END || observed == UD_FENCED ? UD_OK : observed);
    if (!status)
        return observed;
    ud_free(*json);
    *json = NULL;
    *length = 0;
    return status == UD_COMMIT_UNKNOWN ? UD_TRANSIENT : status;
}

ud_status ud_catalog_put_leased(ud_catalog *catalog, const ud_catalog_key *key,
                                ud_string document, const ud_catalog_lease *lease) {
    if (!catalog || !key || (key->kind != UD_CATALOG_EXECUTION && key->kind != UD_CATALOG_EVENT &&
        key->kind != UD_CATALOG_AUDIT))
        return UD_INVALID;
    UD_TRY(lease_check(lease));
    UD_TRY(ud_catalog_begin(catalog, 0));
    ud_status status = lease_guard(catalog, lease, lease->kind, lease->id);
    if (!status)
        status = ud_catalog_append_document(catalog,key,document,0,0,NULL,0);
    return ud_catalog_finish(catalog, status);
}

static ud_status lease_end(ud_catalog *c,ud_status status) {
    ud_status confirmed = ud_catalog_finish(c, status == UD_FENCED || status == UD_BUSY ? UD_OK : status);
    return confirmed ? confirmed : status;
}
ud_status ud_catalog_lease_acquire(ud_catalog *c,const ud_catalog_lease_request *request,
                                   uint64_t *generation,uint64_t *until) {
    if (!c || !request || !generation || !until) return UD_INVALID;
    *generation=*until=0;
    if (request->struct_size!=sizeof(*request) || request->abi_version!=UD_ABI_VERSION) return UD_INCOMPATIBLE;
    if (!kind_valid(request->kind) || !string_valid(request->id,1) || request->id.length>255 ||
        !string_valid(request->owner,1) || request->owner.length>255 || !string_valid(request->pool,0) ||
        request->pool.length>255 || request->duration_ms<100 || request->duration_ms>300000 ||
        (!!request->pool.length != !!request->capacity) || request->capacity>65536) return UD_INVALID;
    UD_TRY(grant(c,UD_ACCESS_WRITE));
    UD_TRY(ud_catalog_begin(c,election_key(c,request->kind,request->id)));
    uint64_t now=0,previous=0;
    ud_status status=lease_clock(c,&now);
    catalog_statement *statement=NULL;
    if (!status) status=catalog_prepare(c,"SELECT generation,until_ms FROM leases WHERE tenant=?1 AND kind=?2 AND id=?3",&statement);
    if (!status) status=lease_key(c,statement,request->kind,request->id);
    if (!status) {
        ud_status rc=catalog_step(statement);
        if (rc==UD_OK) {
            int64_t value=catalog_column_int(statement,0);
            if (value<=0 || value==INT64_MAX) status=UD_OVERFLOW;
            else if (catalog_column_int(statement,1)>(int64_t)now) status=UD_BUSY;
            else previous=(uint64_t)value;
        } else if (rc!=UD_END) status=rc;
    }
    catalog_finalize(statement); statement=NULL;
    if (!status && request->pool.length) {
        status=catalog_prepare(c,"INSERT INTO lease_pools(tenant,pool,capacity) VALUES(?1,?2,?3) ON CONFLICT(tenant,pool) DO NOTHING",&statement);
        if (!status) status=catalog_text(statement,1,c->options.tenant);
        if (!status) status=catalog_text(statement,2,request->pool);
        if (!status) status=catalog_int(statement,3,(int)request->capacity);
        if (!status) status=catalog_execute(statement);
        catalog_finalize(statement); statement=NULL;
        if (!status) status=catalog_prepare(c,"SELECT capacity,(SELECT count(*) FROM leases WHERE tenant=?1 AND pool=?2 AND until_ms>?3) "
                                      "FROM lease_pools WHERE tenant=?1 AND pool=?2",&statement);
        if (!status) status=catalog_text(statement,1,c->options.tenant);
        if (!status) status=catalog_text(statement,2,request->pool);
        if (!status) status=catalog_int(statement,3,(int64_t)now);
        if (!status) {
            ud_status rc=catalog_step(statement);
            if (rc!=UD_OK) status=UD_CORRUPT;
            else if (catalog_column_int(statement,0)!=(int)request->capacity) status=UD_FENCED;
            else if (catalog_column_int(statement,1)>=(int64_t)request->capacity) status=UD_BUSY;
        }
        catalog_finalize(statement); statement=NULL;
    }
    uint64_t expiry=now+request->duration_ms;
    if (!status && expiry>INT64_MAX) status=UD_OVERFLOW;
    if (!status) status=catalog_prepare(c,"INSERT INTO leases(tenant,kind,id,owner,pool,generation,until_ms) VALUES(?1,?2,?3,?4,?5,?6,?7) "
                                  "ON CONFLICT(tenant,kind,id) DO UPDATE SET owner=excluded.owner,pool=excluded.pool,"
                                  "generation=excluded.generation,until_ms=excluded.until_ms",&statement);
    if (!status) status=lease_key(c,statement,request->kind,request->id);
    if (!status) status=catalog_text(statement,4,request->owner);
    if (!status) status=catalog_text(statement,5,request->pool);
    if (!status) status=catalog_int(statement,6,(int64_t)(previous+1));
    if (!status) status=catalog_int(statement,7,(int64_t)expiry);
    if (!status) status=catalog_execute(statement);
    catalog_finalize(statement);
    status=lease_end(c,status);
    if (!status) { *generation=previous+1;*until=expiry; }
    return status;
}
ud_status ud_catalog_lease_renew(ud_catalog *c,const ud_catalog_lease *lease,uint32_t duration,uint64_t *until) {
    if (!c || !until || duration<100 || duration>300000) return UD_INVALID;
    *until=0;
    UD_TRY(lease_check(lease)); UD_TRY(grant(c,UD_ACCESS_WRITE)); UD_TRY(ud_catalog_begin(c,election_key(c,lease->kind,lease->id)));
    ud_status status=lease_guard(c,lease,lease->kind,lease->id);
    uint64_t now=0;
    if (!status) status=lease_clock(c,&now);
    if (!status && now>(uint64_t)INT64_MAX-duration) status=UD_OVERFLOW;
    catalog_statement *statement=NULL;
    if (!status) status=catalog_prepare(c,"UPDATE leases SET until_ms=?4 WHERE tenant=?1 AND kind=?2 AND id=?3",&statement);
    if (!status) status=lease_key(c,statement,lease->kind,lease->id);
    if (!status) status=catalog_int(statement,4,(int64_t)(now+duration));
    if (!status) status=catalog_execute(statement);
    catalog_finalize(statement); status=lease_end(c,status);
    if (!status) *until=now+duration;
    return status;
}
ud_status ud_catalog_lease_release(ud_catalog *c,const ud_catalog_lease *lease) {
    if (!c) return UD_INVALID;
    UD_TRY(lease_check(lease)); UD_TRY(grant(c,UD_ACCESS_WRITE)); UD_TRY(ud_catalog_begin(c,election_key(c,lease->kind,lease->id)));
    ud_status status=lease_guard(c,lease,lease->kind,lease->id);
    catalog_statement *statement=NULL;
    if (!status) status=catalog_prepare(c,"UPDATE leases SET owner='',until_ms=0 WHERE tenant=?1 AND kind=?2 AND id=?3",&statement);
    if (!status) status=lease_key(c,statement,lease->kind,lease->id);
    if (!status) status=catalog_execute(statement);
    catalog_finalize(statement); return lease_end(c,status);
}
ud_status ud_catalog_lease_adopt(ud_catalog *c, const ud_catalog_lease *lease, ud_string owner,
                                 uint32_t duration, uint64_t *until) {
    if (!c || !until || !string_valid(owner, 1) || owner.length > 255 ||
        duration < 100 || duration > 300000)
        return UD_INVALID;
    *until = 0;
    UD_TRY(lease_check(lease));
    if (election_key(c, lease->kind, lease->id))
        return UD_INVALID;
    UD_TRY(grant(c, UD_ACCESS_WRITE));
    UD_TRY(ud_catalog_begin(c, 0));
    uint64_t now = 0;
    ud_status status = lease_guard(c, lease, lease->kind, lease->id);
    if (!status) status = lease_clock(c, &now);
    if (!status && now > (uint64_t)INT64_MAX - duration) status = UD_OVERFLOW;
    catalog_statement *statement = NULL;
    if (!status) status = catalog_prepare(c, "UPDATE leases SET owner=?4,until_ms=?5 "
        "WHERE tenant=?1 AND kind=?2 AND id=?3", &statement);
    if (!status) status = lease_key(c, statement, lease->kind, lease->id);
    if (!status) status = catalog_text(statement, 4, owner);
    if (!status) status = catalog_int(statement, 5, (int64_t)(now + duration));
    if (!status) status = catalog_execute(statement);
    catalog_finalize(statement);
    status = lease_end(c, status);
    if (!status) *until = now + duration;
    return status;
}
ud_status ud_catalog_lease_active(ud_catalog *c,int32_t kind,ud_string id,int32_t *active) {
    if (!c || !active || !kind_valid(kind) || !string_valid(id,1) || id.length>255) return UD_INVALID;
    *active=0;
    UD_TRY(grant(c,UD_ACCESS_READ));
    if (!c->remote) UD_TRY(ud_catalog_begin(c,0));
    uint64_t now=0;
    ud_status status=c->remote ? UD_OK : lease_clock(c,&now);
    catalog_statement *statement=NULL;
    if (!status) status=catalog_prepare(c,c->remote ?
        "SELECT CAST(until_ms>greatest((SELECT milliseconds FROM catalog_clock WHERE id=1)," PG_CLOCK ") AS INTEGER) "
        "FROM leases WHERE tenant=?1 AND kind=?2 AND id=?3" :
        "SELECT until_ms FROM leases WHERE tenant=?1 AND kind=?2 AND id=?3",&statement);
    if (!status) status=lease_key(c,statement,kind,id);
    if (!status) {
        ud_status rc=catalog_step(statement);
        if (rc==UD_OK) *active=catalog_column_int(statement,0)>(int64_t)now;
        else if (rc!=UD_END) status=rc;
    }
    catalog_finalize(statement); if (!c->remote) status=lease_end(c,status);
    if (status) *active=0;
    return status;
}

ud_status ud_catalog_worker_clock(ud_catalog *c,uint64_t *milliseconds) {
    if (!c || !milliseconds)
        return UD_INVALID;
    *milliseconds=0;
    UD_TRY(grant(c,UD_ACCESS_READ));
    UD_TRY(ud_catalog_begin(c,0));
    ud_status status = c->remote ? catalog_clock_read(c,milliseconds) : lease_clock(c,milliseconds);
    return ud_catalog_finish(c,status);
}

ud_status ud_catalog_copy_snapshot(ud_catalog *source, ud_catalog *target, int32_t imports,
                                   uint64_t *rows, uint8_t digest[32]) {
    if (!source || source->remote || !rows || !digest || (imports != 0 && imports != 1) ||
        (imports && !target) || (target && (!target->remote || !ud_same(source->options.tenant, target->options.tenant))))
        return UD_INVALID;
    *rows = 0;
    memset(digest, 0, 32);
    UD_TRY(grant(source, UD_ACCESS_READ));
    if (target) UD_TRY(grant(target, imports ? UD_ACCESS_WRITE : UD_ACCESS_READ));
    UD_TRY(ud_catalog_sql(source, imports ? "BEGIN IMMEDIATE" : "BEGIN"));
    int target_open = 0;
    uint64_t count = 0;
    ud_sha256 hash;
    ud_sha256_init(&hash);
    catalog_statement *active = NULL;
    ud_status status = catalog_prepare(source, "SELECT 1 FROM leases WHERE tenant=?1 "
        "AND until_ms>max(?2,(SELECT milliseconds FROM catalog_clock WHERE id=1)) LIMIT 1", &active);
    if (!status) status = catalog_text(active, 1, source->options.tenant);
    if (!status) status = catalog_int(active, 2, (int64_t)ud_wall_ms());
    if (!status) {
        status = catalog_step(active);
        if (!status) status = UD_BUSY;
        else if (status == UD_END) status = UD_OK;
    }
    catalog_finalize(active);
    if (!status && target) {
#ifdef UD_WITH_POSTGRESQL_CATALOG
        status = imports ? ud_catalog_begin(target, 0) : catalog_postgresql_begin(target,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
        target_open = !status;
        /* Offline snapshot transfer uses a deadline for each I/O and final
         * commit, so its duration can scale with the number of documents. */
        target->postgresql_deadline = 0;
#else
        status = UD_UNSUPPORTED;
#endif
    }
    catalog_statement *records = NULL;
    if (!status) status = catalog_prepare(source,
        "SELECT id,revision,document,digest,timestamp_ms,kind FROM documents WHERE tenant=?1 "
        "ORDER BY kind,id,revision", &records);
    if (!status) status = catalog_text(records,1,source->options.tenant);
    while (!status) {
        status = catalog_step(records);
        if (status == UD_END) {
            status = UD_OK;
            break;
        }
        if (status) break;
        ud_catalog_key key;
        ud_string document;
        status = row_check(source,records,(int)catalog_column_int(records,5),&key,&document);
        if (status) break;
        const uint8_t *proof = catalog_column_blob(records,3);
        int64_t timestamp = catalog_column_int(records,4);
        ud_sha256_update(&hash,proof,32);
        count++;
        if (!target) continue;
        catalog_statement *entry = NULL;
        status = catalog_prepare(target,
            "SELECT id,revision,document,digest,timestamp_ms FROM documents "
            "WHERE tenant=?1 AND kind=?2 AND id=?3 AND revision=?4", &entry);
        if (!status) status = bind_key(target,entry,&key);
        if (!status) {
            status = catalog_step(entry);
            if (!status) {
                ud_catalog_key stored_key;
                ud_string stored;
                status = row_check(target,entry,key.kind,&stored_key,&stored);
                if (!status && (!ud_same(document,stored) || memcmp(proof,catalog_column_blob(entry,3),32) ||
                    timestamp != catalog_column_int(entry,4)))
                    status = UD_CORRUPT;
            }
        }
        catalog_finalize(entry);
        if (status != UD_END) continue;
        if (!imports) {
            status = UD_CORRUPT;
            break;
        }
        status = catalog_prepare(target,
            "INSERT INTO documents(tenant,kind,id,revision,document,digest,timestamp_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            &entry);
        if (!status) status = bind_key(target,entry,&key);
        if (!status) status = catalog_text(entry,5,document);
        if (!status) status = catalog_blob(entry,6,proof,32);
        if (!status) status = catalog_int(entry,7,timestamp);
        if (!status) status = catalog_execute(entry);
        catalog_finalize(entry);
        if (!status) status = update_run_head(target,&key);
    }
    catalog_finalize(records);
    if (!status && target) {
        catalog_statement *total = NULL;
        status = catalog_prepare(target, "SELECT count(*) FROM documents WHERE tenant=?1 "
            "AND NOT(kind=7 AND id LIKE 'protected/parameters-%')", &total);
        if (!status) status = catalog_text(total,1,target->options.tenant);
        if (!status) {
            status = catalog_step(total);
            if (status == UD_END || (!status && (uint64_t)catalog_column_int(total,0) != count))
                status = UD_CORRUPT;
        }
        catalog_finalize(total);
    }
    if (target_open) status = ud_catalog_finish(target,status);
    status = ud_catalog_finish(source,status);
    if (!status) {
        *rows = count;
        ud_sha256_finish(&hash,digest);
    }
    return status;
}
