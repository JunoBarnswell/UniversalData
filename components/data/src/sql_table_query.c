#include "../internal.h"
#include "../row_range.h"
#include "ud/sql.h"

typedef struct query_writer {
    ud_context *ctx;
    ud_bytes text;
    ud_sql_statement statement;
    const ud_batch *schema;
    const ud_batch *parameters;
    int32_t dialect;
    int32_t style;
    uint32_t capacity;
} query_writer;

typedef struct query_value {
    ud_type type;
    int nullable;
} query_value;

static const ud_json *query_member(const ud_json *object, const char *name) {
    return object ? ud_json_find(object, name, (uint32_t)strlen(name)) : NULL;
}

static int query_same(ud_string a, ud_string b) {
    return a.length == b.length && (!a.length || !memcmp(a.data, b.data, a.length));
}

static ud_status query_text(const ud_json *value, ud_string *out) {
    if (!value || ud_json_kind(value) != UD_JSON_STRING)
        return UD_SCHEMA;
    UD_TRY(ud_json_text(value, &out->data, &out->length));
    return out->length && !memchr(out->data, 0, out->length) ? UD_OK : UD_SCHEMA;
}

static ud_status query_u64(const ud_json *value, uint64_t *out) {
    const char *text = NULL;
    uint32_t length = 0;
    if (!value || ud_json_kind(value) != UD_JSON_NUMBER)
        return UD_SCHEMA;
    UD_TRY(ud_json_text(value, &text, &length));
    if (!length)
        return UD_SCHEMA;
    uint64_t result = 0;
    for (uint32_t at = 0; at < length; at++) {
        unsigned digit = (unsigned)(text[at] - '0');
        if (digit > 9 || result > (UINT64_MAX - digit) / 10)
            return UD_SCHEMA;
        result = result * 10 + digit;
    }
    *out = result;
    return UD_OK;
}

static ud_status query_bool(const ud_json *value, int *out) {
    const char *text = NULL;
    uint32_t length = 0;
    if (!value || ud_json_kind(value) != UD_JSON_BOOL)
        return UD_SCHEMA;
    UD_TRY(ud_json_text(value, &text, &length));
    if (length == 4 && !memcmp(text, "true", 4))
        *out = 1;
    else if (length == 5 && !memcmp(text, "false", 5))
        *out = 0;
    else
        return UD_SCHEMA;
    return UD_OK;
}

static ud_status query_put(query_writer *w, const char *text) {
    return ud_bytes_put(&w->text, text, strlen(text));
}

static ud_status query_quote(query_writer *w, ud_string name) {
    if (!name.data || !name.length || memchr(name.data, 0, name.length))
        return UD_SCHEMA;
    char quote = w->dialect == UD_SQL_MYSQL || w->dialect == UD_SQL_MARIADB ? '`' : '"';
    UD_TRY(ud_bytes_put(&w->text, &quote, 1));
    for (uint32_t at = 0; at < name.length; at++) {
        if (name.data[at] == quote)
            UD_TRY(ud_bytes_put(&w->text, &quote, 1));
        UD_TRY(ud_bytes_put(&w->text, name.data + at, 1));
    }
    return ud_bytes_put(&w->text, &quote, 1);
}

static ud_status query_table(query_writer *w, ud_string name) {
    const char *dot = memchr(name.data, '.', name.length);
    if (!dot)
        return query_quote(w, name);
    if (memchr(dot + 1, '.', name.length - (uint32_t)(dot - name.data) - 1))
        return UD_SCHEMA;
    UD_TRY(query_quote(w, (ud_string){name.data, (uint32_t)(dot - name.data)}));
    UD_TRY(query_put(w, "."));
    return query_quote(w, (ud_string){dot + 1,
                                      name.length - (uint32_t)(dot - name.data) - 1});
}

static ud_status query_column(query_writer *w, uint32_t index) {
    if (!w->schema || index >= w->schema->columns)
        return UD_SCHEMA;
    const ud_field *field = &w->schema->column[index].field;
    return query_quote(w, (ud_string){field->name, field->name_length});
}

static ud_status query_find_column(const query_writer *w, ud_string name, uint32_t *out) {
    for (uint32_t at = 0; at < w->schema->columns; at++) {
        const ud_field *field = &w->schema->column[at].field;
        if (query_same(name, (ud_string){field->name, field->name_length})) {
            *out = at;
            return UD_OK;
        }
    }
    return UD_SCHEMA;
}

static int query_scalar_type(ud_type type) {
    /* Floating-point NaN ordering/equality differs between the supported
     * providers, so it is projection-only until a non-NaN proof exists. */
    return type == UD_BOOL || type == UD_INT64 || type == UD_UINT64 ||
           type == UD_DECIMAL128 || type == UD_DATE32 || type == UD_TIME64 ||
           type == UD_TIMESTAMP || type == UD_DATETIME;
}

static int query_order_type(const ud_field *field) {
    /* Text ordering is deliberately not guessed: provider collations are not
     * part of the table-query wire contract.  Numeric/date domains have an
     * exact provider ordering across all supported SQL dialects. */
    return field && query_scalar_type(field->type);
}

static ud_status query_parameter(query_writer *w, uint32_t index) {
    if (!w->parameters || index >= w->parameters->columns)
        return UD_SCHEMA;
    ud_value value;
    UD_TRY(ud_batch_get(w->parameters, 0, index, &value));
    if (value.is_absent)
        return UD_INCOMPATIBLE;
    uint32_t maximum = ud_sql_parameter_limit(w->dialect);
    if (!maximum || w->statement.parameter_count >= maximum)
        return UD_RESOURCE;
    if (w->statement.parameter_count == w->capacity) {
        uint32_t capacity = w->capacity ? w->capacity * 2 : 32;
        if (capacity > maximum)
            capacity = maximum;
        ud_sql_parameter *parameters = ud_alloc(w->ctx, (size_t)capacity * sizeof(*parameters));
        if (!parameters)
            return UD_RESOURCE;
        if (w->statement.parameter_count)
            memcpy(parameters, w->statement.parameters,
                   (size_t)w->statement.parameter_count * sizeof(*parameters));
        ud_free(w->statement.parameters);
        w->statement.parameters = parameters;
        w->capacity = capacity;
    }
    w->statement.parameters[w->statement.parameter_count++] =
        (ud_sql_parameter){0, index, UD_SQL_AFTER};
    char marker[24];
    int length = w->style == UD_SQL_PARAMETER_QUESTION
                     ? snprintf(marker, sizeof(marker), "?")
                     : snprintf(marker, sizeof(marker),
                                w->style == UD_SQL_PARAMETER_NAMED ? ":p%u" : "$%u",
                                w->statement.parameter_count);
    return ud_bytes_put(&w->text, marker, (size_t)length);
}

static ud_status query_bound_parameters(query_writer *w, ud_batch **out) {
    *out = NULL;
    uint32_t count = w->statement.parameter_count;
    if (!count)
        return UD_OK;
    ud_field *fields = ud_alloc(w->ctx, (size_t)count * sizeof(*fields));
    ud_value *values = ud_alloc(w->ctx, (size_t)count * sizeof(*values));
    char (*names)[24] = ud_alloc(w->ctx, (size_t)count * sizeof(*names));
    ud_status status = fields && values && names ? UD_OK : UD_RESOURCE;
    for (uint32_t at = 0; !status && at < count; at++) {
        uint32_t source = w->statement.parameters[at].column;
        status = ud_batch_field(w->parameters, source, fields + at);
        if (!status)
            status = ud_batch_get(w->parameters, 0, source, values + at);
        int length = snprintf(names[at], sizeof(names[at]), "p%u", at + 1);
        fields[at].name = names[at];
        fields[at].name_length = (uint32_t)length;
        fields[at].identity = (ud_id){0};
        fields[at].lineage = NULL;
        fields[at].lineage_count = 0;
        fields[at].key_order = 0;
    }
    if (!status)
        status = ud_batch_create(w->ctx, fields, count, 1, out);
    if (!status)
        status = ud_batch_append(*out, values, count);
    if (!status)
        status = ud_batch_seal(*out);
    ud_free(names);
    ud_free(values);
    ud_free(fields);
    return status;
}

static ud_status query_finish(query_writer *w, ud_status status, ud_string *sql,
                              ud_batch **arguments) {
    if (!status)
        status = query_bound_parameters(w, arguments);
    uint32_t length = w->text.size > UINT32_MAX ? 0 : (uint32_t)w->text.size;
    if (!status && !length && w->text.size)
        status = UD_OVERFLOW;
    if (!status)
        status = ud_bytes_put(&w->text, "", 1);
    if (!status) {
        *sql = (ud_string){(const char *)w->text.data, length};
        w->text.data = NULL;
    } else {
        ud_batch_destroy(*arguments);
        *arguments = NULL;
    }
    ud_bytes_clear(&w->text);
    ud_sql_statement_clear(&w->statement);
    return status;
}

static ud_status query_operand(query_writer *w, const ud_json *node, uint32_t depth,
                               int boolean_context, query_value *out);
static ud_status query_is_null(query_writer *w, const ud_json *node, uint32_t depth);

/* The public SqlSugar shape predates tableQuery.  Keep its permissive operand
 * semantics (including text and binary values) isolated from the native
 * compiler's proof-oriented operand path below. */
static ud_status query_legacy_operand(query_writer *w, const ud_json *node, uint32_t depth) {
    if (!node || depth > 64 || ud_json_kind(node) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    const ud_json *column = query_member(node, "col");
    const ud_json *parameter = query_member(node, "param");
    if (column || parameter) {
        if (column && parameter)
            return UD_SCHEMA;
        if (column) {
            ud_string name;
            uint32_t index;
            UD_TRY(query_text(column, &name));
            UD_TRY(query_find_column(w, name, &index));
            return query_column(w, index);
        }
        uint64_t ordinal;
        UD_TRY(query_u64(parameter, &ordinal));
        if (ordinal > UINT32_MAX || !w->parameters || ordinal >= w->parameters->columns)
            return UD_SCHEMA;
        return query_parameter(w, (uint32_t)ordinal);
    }
    ud_string operation;
    UD_TRY(query_text(query_member(node, "op"), &operation));
    const ud_json *args = query_member(node, "args");
    if (!args || ud_json_kind(args) != UD_JSON_ARRAY)
        return UD_SCHEMA;
    static const char *names[] = {"eq", "ne", "lt", "le", "gt", "ge",
                                  "and", "or", "not", "isnull"};
    static const char *operators[] = {" = ", " <> ", " < ", " <= ", " > ", " >= ",
                                      " AND ", " OR "};
    uint32_t op = 0;
    for (; op < sizeof(names) / sizeof(*names); op++)
        if (query_same(operation, (ud_string){names[op], (uint32_t)strlen(names[op])}))
            break;
    if (op >= sizeof(names) / sizeof(*names) || ud_json_size(args) != (op >= 8 ? 1u : 2u))
        return UD_SCHEMA;
    UD_TRY(query_put(w, op == 8 ? "(NOT " : "("));
    UD_TRY(query_legacy_operand(w, ud_json_at(args, 0), depth + 1));
    if (op == 9)
        UD_TRY(query_put(w, " IS NULL"));
    else if (op < 8) {
        UD_TRY(query_put(w, operators[op]));
        UD_TRY(query_legacy_operand(w, ud_json_at(args, 1), depth + 1));
    }
    return query_put(w, ")");
}

static ud_status query_bool_literal(query_writer *w) {
    return query_put(w, w->dialect == UD_SQL_POSTGRESQL || w->dialect == UD_SQL_DUCKDB ? "TRUE" : "1");
}

static ud_status query_bool_operand(query_writer *w, const ud_json *node, uint32_t depth) {
    query_value value;
    UD_TRY(query_operand(w, node, depth, 1, &value));
    if (value.type != UD_BOOL)
        return UD_SCHEMA;
    return UD_OK;
}

static ud_status query_operand(query_writer *w, const ud_json *node, uint32_t depth,
                               int boolean_context, query_value *out) {
    if (!node || depth > 64 || ud_json_kind(node) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    const ud_json *column = query_member(node, "col");
    const ud_json *parameter = query_member(node, "param");
    if (column || parameter) {
        if (column && parameter)
            return UD_SCHEMA;
        if (column) {
            ud_string name;
            uint32_t index;
            UD_TRY(query_text(column, &name));
            UD_TRY(query_find_column(w, name, &index));
            const ud_field *field = &w->schema->column[index].field;
            out->type = field->type;
            out->nullable = field->nullable;
            if (boolean_context && field->type == UD_BOOL) {
                UD_TRY(query_put(w, "("));
                UD_TRY(query_column(w, index));
                UD_TRY(query_put(w, " IS NOT NULL AND "));
                UD_TRY(query_column(w, index));
                UD_TRY(query_put(w, "="));
                UD_TRY(query_bool_literal(w));
                return query_put(w, ")");
            }
            return query_column(w, index);
        }
        uint64_t ordinal;
        UD_TRY(query_u64(parameter, &ordinal));
        if (ordinal > UINT32_MAX || !w->parameters || ordinal >= w->parameters->columns)
            return UD_SCHEMA;
        const ud_field *field = &w->parameters->column[(uint32_t)ordinal].field;
        out->type = field->type;
        out->nullable = field->nullable;
        if (boolean_context && field->type == UD_BOOL) {
            UD_TRY(query_put(w, "("));
            UD_TRY(query_parameter(w, (uint32_t)ordinal));
            UD_TRY(query_put(w, " IS NOT NULL AND "));
            UD_TRY(query_parameter(w, (uint32_t)ordinal));
            UD_TRY(query_put(w, "="));
            UD_TRY(query_bool_literal(w));
            return query_put(w, ")");
        }
        return query_parameter(w, (uint32_t)ordinal);
    }
    ud_string operation;
    UD_TRY(query_text(query_member(node, "op"), &operation));
    const ud_json *args = query_member(node, "args");
    if (!args || ud_json_kind(args) != UD_JSON_ARRAY)
        return UD_SCHEMA;
    static const char *names[] = {"eq", "ne", "lt", "le", "gt", "ge",
                                  "and", "or", "not", "isnull"};
    uint32_t op = 0;
    for (; op < sizeof(names) / sizeof(*names); op++)
        if (query_same(operation, (ud_string){names[op], (uint32_t)strlen(names[op])}))
            break;
    if (op >= sizeof(names) / sizeof(*names))
        return UD_SCHEMA;
    uint32_t expected = op == 8 || op == 9 ? 1 : 2;
    if (ud_json_size(args) != expected)
        return UD_SCHEMA;
    query_value left, right;
    if (op == 9) {
        out->type = UD_BOOL;
        out->nullable = 0;
        return query_is_null(w, node, depth);
    }
    if (op == 8) {
        UD_TRY(query_put(w, "(NOT "));
        UD_TRY(query_bool_operand(w, ud_json_at(args, 0), depth + 1));
        UD_TRY(query_put(w, ")"));
        out->type = UD_BOOL;
        out->nullable = 0;
        return UD_OK;
    }
    int boolean = op == 6 || op == 7;
    if (boolean) {
        UD_TRY(query_put(w, "("));
        UD_TRY(query_bool_operand(w, ud_json_at(args, 0), depth + 1));
        UD_TRY(query_put(w, op == 6 ? " AND " : " OR "));
        UD_TRY(query_bool_operand(w, ud_json_at(args, 1), depth + 1));
        UD_TRY(query_put(w, ")"));
        out->type = UD_BOOL;
        out->nullable = 0;
        return UD_OK;
    }
    /* Resolve scalar types before recording actual placeholders. Comparisons
     * are two-valued even inside NOT and across nullable operands. */
    static const char *operators[] = {"=", "<>", "<", "<=", ">", ">="};
    const ud_json *lhs = ud_json_at(args, 0), *rhs = ud_json_at(args, 1);
    query_writer proof = *w;
    proof.text = (ud_bytes){.ctx = w->ctx, .limit = 16777216};
    proof.statement = (ud_sql_statement){0};
    proof.capacity = 0;
    ud_status status = query_operand(&proof, lhs, depth + 1, 0, &left);
    if (!status)
        status = query_operand(&proof, rhs, depth + 1, 0, &right);
    ud_bytes_clear(&proof.text);
    ud_sql_statement_clear(&proof.statement);
    if (status)
        return status;
    if (left.type != right.type || !query_scalar_type(left.type) ||
        (op >= 2 && left.type == UD_BOOL))
        return UD_UNSUPPORTED;
    int nullable = left.nullable || right.nullable;
    int negate = op == 1 && nullable;
    UD_TRY(query_put(w, negate ? "(NOT (" : "("));
    UD_TRY(query_put(w, "("));
    UD_TRY(query_operand(w, lhs, depth + 1, 0, &left));
    UD_TRY(query_put(w, operators[negate ? 0 : op]));
    UD_TRY(query_operand(w, rhs, depth + 1, 0, &right));
    if (left.nullable) {
        UD_TRY(query_put(w, " AND "));
        UD_TRY(query_operand(w, lhs, depth + 1, 0, &left));
        UD_TRY(query_put(w, " IS NOT NULL"));
    }
    if (right.nullable) {
        UD_TRY(query_put(w, " AND "));
        UD_TRY(query_operand(w, rhs, depth + 1, 0, &right));
        UD_TRY(query_put(w, " IS NOT NULL"));
    }
    UD_TRY(query_put(w, ")"));
    if (op <= 1 && left.nullable && right.nullable) {
        UD_TRY(query_put(w, " OR ("));
        UD_TRY(query_operand(w, lhs, depth + 1, 0, &left));
        UD_TRY(query_put(w, " IS NULL AND "));
        UD_TRY(query_operand(w, rhs, depth + 1, 0, &right));
        UD_TRY(query_put(w, " IS NULL)"));
    }
    out->type = UD_BOOL;
    out->nullable = 0;
    return query_put(w, negate ? "))" : ")");
}

static ud_status query_is_null(query_writer *w, const ud_json *node, uint32_t depth) {
    /* Emit isnull in one pass; the normal operand function is intentionally
     * side-effecting because it records parameter occurrences. */
    const ud_json *args = query_member(node, "args");
    if (!args || ud_json_kind(args) != UD_JSON_ARRAY || ud_json_size(args) != 1)
        return UD_SCHEMA;
    UD_TRY(query_put(w, "("));
    query_value value;
    UD_TRY(query_operand(w, ud_json_at(args, 0), depth + 1, 0, &value));
    UD_TRY(query_put(w, " IS NULL)"));
    return UD_OK;
}

static ud_status query_predicate(query_writer *w, const ud_json *node, uint32_t depth) {
    if (!node || ud_json_kind(node) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    const ud_json *op = query_member(node, "op");
    if (!op) {
        query_value value;
        UD_TRY(query_operand(w, node, depth, 1, &value));
        return value.type == UD_BOOL ? UD_OK : UD_SCHEMA;
    }
    ud_string text;
    UD_TRY(query_text(op, &text));
    if (query_same(text, (ud_string){"isnull", 6}))
        return query_is_null(w, node, depth);
    query_value value;
    UD_TRY(query_operand(w, node, depth, 1, &value));
    return value.type == UD_BOOL ? UD_OK : UD_SCHEMA;
}

static ud_status query_emit_order(query_writer *w, const ud_json *order, int native,
                                  uint8_t *seen, uint32_t *emitted) {
    for (uint32_t at = 0; at < ud_json_size(order); at++) {
        const ud_json *entry = ud_json_at(order, at);
        if (!entry || ud_json_kind(entry) != UD_JSON_OBJECT)
            return UD_SCHEMA;
        ud_string name;
        UD_TRY(query_text(query_member(entry, "column"), &name));
        uint32_t index;
        UD_TRY(query_find_column(w, name, &index));
        if (seen[index])
            return UD_SCHEMA;
        const ud_field *field = &w->schema->column[index].field;
        if (native && !query_order_type(field))
            return UD_UNSUPPORTED;
        int descending = 0;
        if (native && query_member(entry, "descending"))
            UD_TRY(query_bool(query_member(entry, "descending"), &descending));
        if (*emitted)
            UD_TRY(query_put(w, ","));
        if (native && field->nullable) {
            UD_TRY(query_put(w, "CASE WHEN "));
            UD_TRY(query_column(w, index));
            UD_TRY(query_put(w, descending ? " IS NULL THEN 1 ELSE 0 END," :
                                           " IS NULL THEN 0 ELSE 1 END,"));
        }
        UD_TRY(query_column(w, index));
        if (descending)
            UD_TRY(query_put(w, " DESC"));
        seen[index] = 1;
        (*emitted)++;
    }
    return UD_OK;
}

static ud_status query_find_primary_keys(const query_writer *w, uint32_t **out, uint32_t *count) {
    uint32_t maximum = 0;
    for (uint32_t at = 0; at < w->schema->columns; at++)
        if (w->schema->column[at].field.key_order > maximum)
            maximum = w->schema->column[at].field.key_order;
    if (!maximum || maximum > w->schema->columns)
        return UD_SCHEMA;
    uint32_t *keys = ud_alloc(w->ctx, (size_t)maximum * sizeof(*keys));
    if (!keys)
        return UD_RESOURCE;
    for (uint32_t expected = 1; expected <= maximum; expected++) {
        uint32_t found = UINT32_MAX;
        for (uint32_t at = 0; at < w->schema->columns; at++)
            if (w->schema->column[at].field.key_order == expected) {
                if (found != UINT32_MAX || w->schema->column[at].field.nullable) {
                    ud_free(keys);
                    return UD_SCHEMA;
                }
                found = at;
            }
        if (found == UINT32_MAX) {
            ud_free(keys);
            return UD_SCHEMA;
        }
        keys[expected - 1] = found;
    }
    *out = keys;
    *count = maximum;
    return UD_OK;
}

static ud_status query_projection(query_writer *w, const ud_json *projection) {
    if (!projection) {
        for (uint32_t at = 0; at < w->schema->columns; at++) {
            if (at)
                UD_TRY(query_put(w, ","));
            UD_TRY(query_column(w, at));
            UD_TRY(query_put(w, " AS "));
            UD_TRY(query_column(w, at));
        }
        return UD_OK;
    }
    if (ud_json_kind(projection) != UD_JSON_ARRAY || !ud_json_size(projection))
        return UD_SCHEMA;
    for (uint32_t at = 0; at < ud_json_size(projection); at++) {
        const ud_json *entry = ud_json_at(projection, at);
        if (!entry || ud_json_kind(entry) != UD_JSON_OBJECT)
            return UD_SCHEMA;
        ud_string column, alias;
        UD_TRY(query_text(query_member(entry, "column"), &column));
        UD_TRY(query_text(query_member(entry, "name"), &alias));
        uint32_t index;
        UD_TRY(query_find_column(w, column, &index));
        if (w->schema->column[index].field.child_count ||
            w->schema->column[index].field.type >= UD_STRUCT)
            return UD_UNSUPPORTED;
        for (uint32_t prior = 0; prior < at; prior++) {
            const ud_json *old = ud_json_at(projection, prior);
            ud_string old_alias;
            UD_TRY(query_text(query_member(old, "name"), &old_alias));
            if (query_same(alias, old_alias))
                return UD_SCHEMA;
        }
        if (at)
            UD_TRY(query_put(w, ","));
        UD_TRY(query_column(w, index));
        UD_TRY(query_put(w, " AS "));
        UD_TRY(query_quote(w, alias));
    }
    return UD_OK;
}

static ud_status query_legacy_order(query_writer *w, const ud_json *order, const ud_json *keys,
                                    int paging) {
    if ((order && ud_json_kind(order) != UD_JSON_ARRAY) ||
        (paging && (!keys || ud_json_kind(keys) != UD_JSON_ARRAY || !ud_json_size(keys))))
        return UD_SCHEMA;
    uint32_t orders = order ? ud_json_size(order) : 0;
    uint32_t key_count = paging ? ud_json_size(keys) : 0;
    if (orders + key_count > w->schema->columns)
        return UD_SCHEMA;
    if (!orders && !key_count)
        return UD_OK;
    uint8_t *seen = ud_alloc(w->ctx, w->schema->columns);
    if (!seen)
        return UD_RESOURCE;
    memset(seen, 0, w->schema->columns);
    ud_status status = query_put(w, " ORDER BY ");
    uint32_t emitted = 0;
    for (uint32_t at = 0; !status && at < orders + key_count; at++) {
        const ud_json *entry = at < orders ? ud_json_at(order, at) : NULL;
        const ud_json *name_node = entry ? query_member(entry, "column") : ud_json_at(keys, at - orders);
        ud_string name;
        status = query_text(name_node, &name);
        uint32_t index = 0;
        if (!status)
            status = query_find_column(w, name, &index);
        /* Preserve the legacy compiler's historical duplicate-key behavior:
         * a repeated order key was silently ignored. Native tableQuery
         * performs its own ambiguity rejection. */
        if (!status && seen[index])
            continue;
        int descending = 0;
        if (!status && entry && query_member(entry, "descending"))
            status = query_bool(query_member(entry, "descending"), &descending);
        if (!status) {
            if (emitted++)
                status = query_put(w, ",");
            if (!status)
                status = query_column(w, index);
            if (!status && descending)
                status = query_put(w, " DESC");
            seen[index] = 1;
        }
    }
    ud_free(seen);
    return status;
}

static ud_status query_range_sql(query_writer *w, uint64_t offset, uint64_t limit) {
    char tail[96];
    int length;
    if (w->dialect == UD_SQL_ORACLE || w->dialect == UD_SQL_SQLSERVER)
        length = snprintf(tail, sizeof(tail), " OFFSET %llu ROWS FETCH NEXT %llu ROWS ONLY",
                          (unsigned long long)offset, (unsigned long long)limit);
    else
        length = snprintf(tail, sizeof(tail), " LIMIT %llu OFFSET %llu",
                          (unsigned long long)limit, (unsigned long long)offset);
    if (length < 0 || (size_t)length >= sizeof(tail))
        return UD_OVERFLOW;
    return query_put(w, tail);
}

static ud_status query_native_order(query_writer *w, const ud_json *order) {
    uint32_t *keys = NULL, key_count = 0;
    UD_TRY(query_find_primary_keys(w, &keys, &key_count));
    uint8_t *ordered = ud_alloc(w->ctx, w->schema->columns);
    ud_status status = ordered ? UD_OK : UD_RESOURCE;
    if (!status) {
        memset(ordered, 0, w->schema->columns);
        status = query_put(w, " ORDER BY ");
    }
    uint32_t emitted = 0;
    if (!status && order)
        status = query_emit_order(w, order, 1, ordered, &emitted);
    for (uint32_t at = 0; !status && at < key_count; at++) {
        uint32_t index = keys[at];
        if (ordered[index])
            continue;
        if (emitted++)
            status = query_put(w, ",");
        if (!status)
            status = query_column(w, index);
    }
    ud_free(ordered);
    ud_free(keys);
    return status;
}

static ud_status query_page(const ud_json *spec, int native, uint64_t *offset, uint64_t *limit) {
    *offset = 0;
    *limit = UINT64_MAX;
    if (native) {
        UD_TRY(ud_row_range_bind(query_member(spec, "ranges"), offset, limit));
        if (*offset > (uint64_t)INT64_MAX)
            return UD_OVERFLOW;
        return *limit <= 65536 ? UD_OK : UD_INVALID;
    }
    const ud_json *page = query_member(spec, "page"), *size = query_member(spec, "size");
    if (!page && !size)
        return UD_OK;
    uint64_t number;
    UD_TRY(query_u64(page, &number));
    UD_TRY(query_u64(size, limit));
    if (!number || number > UINT32_MAX || !*limit || *limit > 65536)
        return UD_SCHEMA;
    *offset = (number - 1) * *limit;
    return UD_OK;
}

static ud_status query_compile(query_writer *w, const ud_json *spec, ud_string table, int native) {
    const ud_json *projection = native ? query_member(spec, "projection") : NULL;
    if (native && !projection)
        return UD_SCHEMA;
    uint64_t offset, limit;
    UD_TRY(query_page(spec, native, &offset, &limit));
    UD_TRY(query_put(w, "SELECT "));
    UD_TRY(query_projection(w, projection));
    UD_TRY(query_put(w, " FROM "));
    UD_TRY(query_table(w, table));
    const ud_json *predicate = query_member(spec, "where");
    if (predicate) {
        UD_TRY(query_put(w, " WHERE "));
        if (native)
            UD_TRY(query_predicate(w, predicate, 0));
        else
            UD_TRY(query_legacy_operand(w, predicate, 0));
    }
    /* SQL Server requires a positive FETCH count. An empty page is a false predicate. */
    if (!limit)
        UD_TRY(query_put(w, predicate ? " AND 1=0" : " WHERE 1=0"));
    const ud_json *order = query_member(spec, "order");
    if (native)
        UD_TRY(query_native_order(w, order));
    else
        UD_TRY(query_legacy_order(w, order, query_member(spec, "keys"), limit != UINT64_MAX));
    return limit && limit != UINT64_MAX ? query_range_sql(w, offset, limit) : UD_OK;
}

ud_status ud_sql_compile_table_query(ud_context *ctx, int32_t dialect, int32_t style,
                                     ud_string document, const ud_batch *schema,
                                     const ud_batch *parameters, ud_string *sql,
                                     ud_batch **arguments) {
    if (!sql || !arguments)
        return UD_INVALID;
    *sql = (ud_string){0};
    *arguments = NULL;
    if (!ctx || !document.data || !document.length || document.length > 16777216 ||
        !schema || schema->ctx != ctx || !schema->columns ||
        (parameters && (parameters->ctx != ctx || !parameters->sealed || parameters->rows != 1)) ||
        dialect < UD_SQL_SQLITE || dialect > UD_SQL_ORACLE || style < UD_SQL_PARAMETER_NUMBERED ||
        style > UD_SQL_PARAMETER_NAMED)
        return UD_INVALID;
    ud_json *query = NULL;
    UD_TRY(ud_json_parse(ctx, document.data, document.length, &query, NULL));
    ud_string table = {0};
    ud_status status = query_text(query_member(query, "table"), &table);
    if (status) {
        ud_json_destroy(query);
        return status;
    }
    const ud_json *spec = query_member(query, "tableQuery");
    int native = spec || query_member(query, "ranges") || query_member(query, "projection");
    if (spec && ud_json_kind(spec) != UD_JSON_OBJECT) {
        ud_json_destroy(query);
        return UD_SCHEMA;
    }
    if (!spec)
        spec = query;
    query_writer writer = {.ctx = ctx,
                           .text = {.ctx = ctx, .limit = 16777216},
                           .schema = schema,
                           .parameters = parameters,
                           .dialect = dialect,
                           .style = style};
    status = query_compile(&writer, spec, table, native);
    status = query_finish(&writer, status, sql, arguments);
    ud_json_destroy(query);
    return status;
}
