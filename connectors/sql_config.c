#include "sql_config.h"
#include "../components/foundation/internal.h"
#include "ud/data.h"
#include <ctype.h>

typedef struct connection_setting {
    char key[48];
    ud_string value;
} connection_setting;

static ud_string trim(ud_string input) {
    while (input.length && isspace((unsigned char)*input.data)) {
        input.data++;
        input.length--;
    }
    while (input.length && isspace((unsigned char)input.data[input.length - 1]))
        input.length--;
    return input;
}

static void clear_secret(void *bytes, size_t size) {
    volatile unsigned char *ptr = bytes;
    while (size--)
        *ptr++ = 0;
}

static ud_status put(ud_bytes *output, const char *text) {
    return ud_bytes_put(output, text, strlen(text));
}

static int word(ud_string text, const char *expected) {
    if (text.length != strlen(expected))
        return 0;
    for (uint32_t i = 0; i < text.length; i++)
        if (tolower((unsigned char)text.data[i]) != tolower((unsigned char)expected[i]))
            return 0;
    return 1;
}

static int prefix(ud_string text, const char *value) {
    uint32_t length = (uint32_t)strlen(value);
    return text.length >= length && word((ud_string){text.data, length}, value);
}

static ud_status setting_key(ud_string input, char out[48]) {
    uint32_t written = 0;
    for (uint32_t i = 0; i < input.length; i++) {
        unsigned char c = (unsigned char)input.data[i];
        if (isspace(c) || c == '_')
            continue;
        if (!isalnum(c) || written == 47)
            return UD_SCHEMA;
        out[written++] = (char)tolower(c);
    }
    out[written] = 0;
    return written ? UD_OK : UD_SCHEMA;
}

static ud_status setting_value(ud_string input, uint32_t *position, char **storage,
                               ud_string *out) {
    uint32_t at = *position;
    while (at < input.length && isspace((unsigned char)input.data[at]))
        at++;
    const char *start = *storage;
    char quote = at < input.length ? input.data[at] : 0;
    if (quote == '"' || quote == '\'' || quote == '{') {
        quote = quote == '{' ? '}' : quote;
        at++;
        int closed = 0;
        while (at < input.length) {
            char c = input.data[at++];
            if (c == quote) {
                if (at == input.length || input.data[at] != quote) {
                    closed = 1;
                    break;
                }
                at++;
            }
            *(*storage)++ = c;
        }
        if (!closed)
            return UD_SCHEMA;
        while (at < input.length && isspace((unsigned char)input.data[at]))
            at++;
        if (at < input.length && input.data[at] != ';')
            return UD_SCHEMA;
    } else {
        uint32_t begin = at;
        while (at < input.length && input.data[at] != ';')
            at++;
        ud_string value = trim((ud_string){input.data + begin, at - begin});
        memcpy(*storage, value.data, value.length);
        *storage += value.length;
    }
    *out = (ud_string){start, (uint32_t)(*storage - start)};
    *position = at < input.length ? at + 1 : at;
    return UD_OK;
}

static ud_status parse_settings(ud_string input, char *storage, connection_setting *settings,
                                uint32_t capacity, uint32_t *count) {
    uint32_t at = 0;
    *count = 0;
    while (at < input.length) {
        if (isspace((unsigned char)input.data[at]) || input.data[at] == ';') {
            at++;
            continue;
        }
        if (*count == capacity)
            return UD_RESOURCE;
        connection_setting *setting = settings + *count;
        uint32_t begin = at;
        while (at < input.length && input.data[at] != '=' && input.data[at] != ';')
            at++;
        if (at == input.length || input.data[at] != '=')
            return UD_SCHEMA;
        UD_TRY(setting_key((ud_string){input.data + begin, at - begin}, setting->key));
        at++;
        UD_TRY(setting_value(input, &at, &storage, &setting->value));
        for (uint32_t i = 0; i < *count; i++)
            if (!strcmp(settings[i].key, setting->key))
                return UD_SCHEMA;
        (*count)++;
    }
    return *count ? UD_OK : UD_SCHEMA;
}

static const char *provider_key(int32_t dialect, const char *input) {
    if (!strcmp(input, "host") || !strcmp(input, "server"))
        return dialect == UD_SQL_SQLSERVER ? "Server" : "host";
    if (!strcmp(input, "userid") || !strcmp(input, "uid") || !strcmp(input, "username") ||
        !strcmp(input, "user"))
        return dialect == UD_SQL_SQLSERVER ? "UID" : "user";
    if (!strcmp(input, "password") || !strcmp(input, "pwd"))
        return dialect == UD_SQL_SQLSERVER ? "PWD" : "password";
    if (!strcmp(input, "database") || !strcmp(input, "initialcatalog") || !strcmp(input, "dbname"))
        return dialect == UD_SQL_SQLSERVER ? "Database"
               : dialect == UD_SQL_POSTGRESQL ? "dbname" : "database";
    if (!strcmp(input, "datasource")) {
        switch (dialect) {
        case UD_SQL_SQLITE: return "path";
        case UD_SQL_ORACLE: return "connectString";
        case UD_SQL_SQLSERVER: return "Server";
        default: return "host";
        }
    }
    if (dialect == UD_SQL_POSTGRESQL) {
        if (!strcmp(input, "port") || !strcmp(input, "sslmode") || !strcmp(input, "applicationname"))
            return !strcmp(input, "applicationname") ? "application_name" : input;
        if (!strcmp(input, "timeout") || !strcmp(input, "connecttimeout") ||
            !strcmp(input, "connectiontimeout"))
            return "connect_timeout";
        if (!strcmp(input, "sslrootcert") || !strcmp(input, "sslcert") || !strcmp(input, "sslkey"))
            return input;
    }
    if (dialect == UD_SQL_MYSQL || dialect == UD_SQL_MARIADB) {
        if (!strcmp(input, "port") || !strcmp(input, "ca") || !strcmp(input, "cert") ||
            !strcmp(input, "key"))
            return input;
        if (!strcmp(input, "sslca") || !strcmp(input, "sslcert") || !strcmp(input, "sslkey"))
            return input + 3;
        if (!strcmp(input, "connectiontimeout") || !strcmp(input, "connecttimeout") ||
            !strcmp(input, "timeoutseconds"))
            return "timeoutSeconds";
        if (!strcmp(input, "timestamptimezone"))
            return "timestampTimezone";
    }
    if (dialect == UD_SQL_ORACLE) {
        if (!strcmp(input, "connectstring"))
            return "connectString";
        if (!strcmp(input, "calltimeoutms"))
            return "callTimeoutMs";
        if (!strcmp(input, "timestamptimezone"))
            return "timestampTimezone";
    }
    if (dialect == UD_SQL_SQLSERVER) {
        if (!strcmp(input, "encrypt"))
            return "Encrypt";
        if (!strcmp(input, "trustservercertificate"))
            return "TrustServerCertificate";
        if (!strcmp(input, "integratedsecurity") || !strcmp(input, "trustedconnection"))
            return "Trusted_Connection";
        if (!strcmp(input, "applicationname"))
            return "APP";
    }
    return NULL;
}

static ud_status string_literal(ud_bytes *output, ud_string text, char left, char right) {
    UD_TRY(ud_bytes_put(output, &left, 1));
    char escape = left == '{' ? '}' : '\\';
    for (uint32_t i = 0; i < text.length; i++) {
        char c = text.data[i];
        if (c == right || (left == '\'' && c == '\\'))
            UD_TRY(ud_bytes_put(output, &escape, 1));
        UD_TRY(ud_bytes_put(output, &c, 1));
    }
    return ud_bytes_put(output, &right, 1);
}

static ud_status postgres_value(ud_bytes *config, const char *name, ud_string value) {
    UD_TRY(put(config, name));
    UD_TRY(put(config, "="));
    if (!strcmp(name, "sslmode")) {
        const char *modes[] = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"};
        const char *aliases[] = {"disable", "allow", "prefer", "require", "verifyca", "verifyfull"};
        for (uint32_t i = 0; i < sizeof(modes) / sizeof(*modes); i++)
            if (word(value, modes[i]) || word(value, aliases[i]))
                value = (ud_string){modes[i], (uint32_t)strlen(modes[i])};
    }
    UD_TRY(string_literal(config, value, '\'', '\''));
    return put(config, " ");
}

static ud_status odbc_value(ud_bytes *config, const char *name, ud_string value) {
    UD_TRY(put(config, name));
    UD_TRY(put(config, "="));
    if (word(value, "true") || (!strcmp(name, "Trusted_Connection") && word(value, "SSPI")))
        value = (ud_string){"yes", 3};
    else if (word(value, "false"))
        value = (ud_string){"no", 2};
    UD_TRY(string_literal(config, value, '{', '}'));
    return put(config, ";");
}

static ud_status json_setting(ud_bytes *config, const char *name, ud_string value) {
    UD_TRY(ud_json_quote(config, name, (uint32_t)strlen(name)));
    UD_TRY(put(config, ":"));
    if (strcmp(name, "port") && strcmp(name, "timeoutSeconds"))
        return ud_json_quote(config, value.data, value.length);
    if (!value.length || value.length > 5)
        return UD_SCHEMA;
    unsigned number = 0;
    for (uint32_t i = 0; i < value.length; i++) {
        if (value.data[i] < '0' || value.data[i] > '9')
            return UD_SCHEMA;
        number = number * 10 + (unsigned)(value.data[i] - '0');
    }
    if (!number || number > 65535)
        return UD_SCHEMA;
    char digits[6];
    int length = snprintf(digits, sizeof(digits), "%u", number);
    return ud_bytes_put(config, digits, (size_t)length);
}

static ud_status encode_settings(ud_context *ctx, int32_t dialect, const connection_setting *pairs,
                                 uint32_t count, ud_bytes *config) {
    ud_bytes odbc = {.ctx = ctx, .limit = 65536};
    ud_status status = UD_OK;
    if (dialect == UD_SQL_SQLSERVER)
#ifdef _WIN32
        status = put(&odbc, "Driver={ODBC Driver 17 for SQL Server};");
#else
        status = put(&odbc, "Driver={ODBC Driver 18 for SQL Server};");
#endif
    else if (dialect != UD_SQL_POSTGRESQL && dialect != UD_SQL_SQLITE)
        status = put(config, "{");
    const char *seen[32] = {0};
    for (uint32_t i = 0; !status && i < count; i++) {
        const char *key = provider_key(dialect, pairs[i].key);
        if (!key || (dialect == UD_SQL_SQLITE && (count != 1 || strcmp(key, "path")))) {
            status = UD_UNSUPPORTED;
            break;
        }
        for (uint32_t j = 0; j < i; j++)
            if (!strcmp(key, seen[j]))
                status = UD_SCHEMA;
        if (status)
            break;
        seen[i] = key;
        if (dialect == UD_SQL_SQLSERVER)
            status = odbc_value(&odbc, key, pairs[i].value);
        else if (dialect == UD_SQL_POSTGRESQL)
            status = postgres_value(config, key, pairs[i].value);
        else if (dialect == UD_SQL_SQLITE)
            status = ud_bytes_put(config, pairs[i].value.data, pairs[i].value.length);
        else {
            if (i)
                status = put(config, ",");
            if (!status)
                status = json_setting(config, key, pairs[i].value);
        }
    }
    if (!status && dialect == UD_SQL_SQLSERVER) {
        status = put(config, "{\"connectionString\":");
        if (!status)
            status = ud_json_quote(config, (const char *)odbc.data, (uint32_t)odbc.size);
    }
    if (!status && dialect != UD_SQL_POSTGRESQL && dialect != UD_SQL_SQLITE)
        status = put(config, "}");
    clear_secret(odbc.data, odbc.size);
    ud_bytes_clear(&odbc);
    return status;
}

ud_status ud_sql_connection_config(ud_context *ctx, int32_t dialect, ud_string input,
                                   ud_string *out) {
    *out = (ud_string){0};
    if (!input.data || !input.length || input.length > 65535 ||
        memchr(input.data, 0, input.length) || !ud_utf8_valid(input.data, input.length))
        return UD_INVALID;
    input = trim(input);
    ud_bytes config = {.ctx = ctx, .limit = 131072};
    ud_status status = UD_OK;
    if (dialect == UD_SQL_SQLITE && !memchr(input.data, '=', input.length))
        status = ud_bytes_put(&config, input.data, input.length);
    else if (dialect == UD_SQL_POSTGRESQL &&
             (prefix(input, "postgresql://") || prefix(input, "postgres://") ||
              !memchr(input.data, ';', input.length)))
        status = ud_bytes_put(&config, input.data, input.length);
    else if (dialect != UD_SQL_SQLITE && dialect != UD_SQL_POSTGRESQL && prefix(input, "{"))
        status = ud_bytes_put(&config, input.data, input.length);
    else if (dialect == UD_SQL_SQLSERVER && (prefix(input, "Driver=") || prefix(input, "DSN="))) {
        status = put(&config, "{\"connectionString\":");
        if (!status)
            status = ud_json_quote(&config, input.data, input.length);
        if (!status)
            status = put(&config, "}");
    } else {
        connection_setting pairs[32];
        char *storage = ud_alloc(ctx, (size_t)input.length + 1);
        uint32_t count = 0;
        status = storage ? parse_settings(input, storage, pairs, 32, &count) : UD_RESOURCE;
        if (!status)
            status = encode_settings(ctx, dialect, pairs, count, &config);
        if (storage)
            clear_secret(storage, (size_t)input.length + 1);
        ud_free(storage);
    }
    uint32_t length = (uint32_t)config.size;
    if (!status && !length)
        status = UD_SCHEMA;
    if (!status)
        status = ud_bytes_put(&config, "", 1);
    if (!status) {
        *out = (ud_string){(const char *)config.data, length};
        config.data = NULL;
    } else
        clear_secret(config.data, config.size);
    ud_bytes_clear(&config);
    return status;
}
