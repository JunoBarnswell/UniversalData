#include "ud/expression.h"
#include "ud/json.h"
#include "../../data/internal.h"
#include <math.h>
typedef struct expression_node {
    int kind, op, constant;
    ud_type type;
    uint32_t column;
    ud_string name;
    ud_value value;
    struct expression_node *args[2];
} expression_node;
struct ud_expression {
    ud_context *ctx;
    ud_json *document;
    expression_node *root;
    ud_expression_resolver resolve;
    void *user;
    ud_batch *schema;
    uint32_t nodes;
};
enum { LITERAL, REFERENCE, COLUMN, OPERATION };
enum { ADD, SUB, MUL, EQ, NE, LT, LE, GT, GE, AND, OR, NOT, ISNULL };
static const ud_json *member(const ud_json *v, const char *key) {
    return ud_json_find(v, key, (uint32_t)strlen(key));
}
static int text_is(ud_string v, const char *s) {
    return v.length == strlen(s) && !memcmp(v.data, s, v.length);
}
static ud_status string(const ud_json *v, ud_string *out) {
    if (ud_json_kind(v) != UD_JSON_STRING)
        return UD_SCHEMA;
    return ud_json_text(v, &out->data, &out->length);
}
static ud_status eval(const ud_expression *e, const expression_node *n, const ud_batch *b,
                      uint32_t row, ud_value *out) {
    if (n->constant) {
        *out = n->value;
        return UD_OK;
    }
    if (n->kind == REFERENCE) {
        UD_TRY(e->resolve(e->user, n->name, out));
        return out->type == n->type ? UD_OK : UD_SCHEMA;
    }
    if (n->kind == COLUMN)
        return ud_batch_get(b, row, n->column, out);
    ud_value a = {0}, z = {0};
    UD_TRY(eval(e, n->args[0], b, row, &a));
    if (n->args[1])
        UD_TRY(eval(e, n->args[1], b, row, &z));
    memset(out, 0, sizeof(*out));
    out->type = n->type;
    if (n->op == ISNULL) {
        out->integer = a.is_null;
        return UD_OK;
    }
    if (n->op == AND || n->op == OR || n->op == NOT) {
        int32_t x = a.is_null ? -1 : (int32_t)a.integer, y = z.is_null ? -1 : (int32_t)z.integer;
        int32_t result = n->op == AND  ? ud_truth_and(x, y)
                         : n->op == OR ? ud_truth_or(x, y)
                                       : ud_truth_not(x);
        out->is_null = result < 0;
        out->integer = result > 0;
        return UD_OK;
    }
    if (a.is_null || z.is_null) {
        out->is_null = 1;
        return UD_OK;
    }
    if (n->op >= EQ && n->op <= GE) {
        int32_t cmp = ud_value_compare(&a, &z);
        out->integer = n->op == EQ   ? cmp == 0
                       : n->op == NE ? cmp != 0
                       : n->op == LT ? cmp < 0
                       : n->op == LE ? cmp <= 0
                       : n->op == GT ? cmp > 0
                                     : cmp >= 0;
        return UD_OK;
    }
    if (n->type == UD_FLOAT64) {
        out->real = n->op == ADD   ? a.real + z.real
                    : n->op == SUB ? a.real - z.real
                                   : a.real * z.real;
        return isfinite(out->real) ? UD_OK : UD_OVERFLOW;
    }
    int64_t x = a.integer, y = z.integer;
    if (n->op == ADD) {
        if ((y > 0 && x > INT64_MAX - y) || (y < 0 && x < INT64_MIN - y))
            return UD_OVERFLOW;
        out->integer = x + y;
    } else if (n->op == SUB) {
        if ((y < 0 && x > INT64_MAX + y) || (y > 0 && x < INT64_MIN + y))
            return UD_OVERFLOW;
        out->integer = x - y;
    } else {
        if (x && y &&
            ((x == -1 && y == INT64_MIN) || (y == -1 && x == INT64_MIN) ||
             (x > 0 ? (y > 0 ? x > INT64_MAX / y : y < INT64_MIN / x)
                    : (y > 0 ? x < INT64_MIN / y : x < INT64_MAX / y))))
            return UD_OVERFLOW;
        out->integer = x * y;
    }
    return UD_OK;
}
static void node_free(expression_node *n) {
    if (!n)
        return;
    node_free(n->args[0]);
    node_free(n->args[1]);
    ud_free(n);
}
static ud_status bind(ud_expression *e, const ud_json *json, uint32_t depth,
                      expression_node **out) {
    *out = NULL;
    if (depth > 64 || ++e->nodes > 4096 || ud_json_kind(json) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    expression_node *n = ud_alloc(e->ctx, sizeof(*n));
    if (!n)
        return UD_RESOURCE;
    *out = n;
    const ud_json *literal = member(json, "literal"), *ref = member(json, "ref"),
                  *col = member(json, "column"), *op = member(json, "op");
    if ((!!literal + !!ref + !!col + !!op) != 1)
        return UD_SCHEMA;
    if (literal) {
        if (ud_json_size(json) != 2)
            return UD_SCHEMA;
        ud_string type;
        UD_TRY(string(member(json, "type"), &type));
        n->type = text_is(type, "bool")      ? UD_BOOL
                  : text_is(type, "int64")   ? UD_INT64
                  : text_is(type, "float64") ? UD_FLOAT64
                  : text_is(type, "utf8")    ? UD_UTF8
                                             : 0;
        if (!n->type)
            return UD_UNSUPPORTED;
        n->kind = LITERAL;
        n->constant = 1;
        n->value.type = n->type;
        int kind = ud_json_kind(literal);
        if (kind == UD_JSON_NULL) {
            n->value.is_null = 1;
            return UD_OK;
        }
        if ((n->type == UD_BOOL && kind != UD_JSON_BOOL) ||
            (n->type == UD_UTF8 && kind != UD_JSON_STRING) ||
            ((n->type == UD_INT64 || n->type == UD_FLOAT64) && kind != UD_JSON_NUMBER))
            return UD_SCHEMA;
        const char *text;
        uint32_t length;
        UD_TRY(ud_json_text(literal, &text, &length));
        return ud_value_parse(e->ctx, n->type, text, length, &n->value);
    }
    if (ref || col) {
        if (ud_json_size(json) != 1)
            return UD_SCHEMA;
        UD_TRY(string(ref ? ref : col, &n->name));
        if (!n->name.length || memchr(n->name.data, 0, n->name.length))
            return UD_SCHEMA;
        if (ref) {
            if (!e->resolve)
                return UD_SCHEMA;
            n->kind = REFERENCE;
            ud_value v;
            UD_TRY(e->resolve(e->user, n->name, &v));
            n->type = v.type;
            if (n->type < UD_BOOL || n->type > UD_UTF8)
                return UD_UNSUPPORTED;
        } else {
            if (!e->schema)
                return UD_SCHEMA;
            n->kind = COLUMN;
            int found = 0;
            for (uint32_t i = 0; i < ud_batch_columns(e->schema); i++) {
                ud_field f;
                UD_TRY(ud_batch_field(e->schema, i, &f));
                if (f.name_length == n->name.length &&
                    !memcmp(f.name, n->name.data, n->name.length)) {
                    n->column = i;
                    n->type = f.type;
                    found = 1;
                    break;
                }
            }
            if (!found)
                return UD_SCHEMA;
        }
        return UD_OK;
    }
    if (ud_json_size(json) != 2)
        return UD_SCHEMA;
    n->kind = OPERATION;
    ud_string name;
    UD_TRY(string(op, &name));
    static const char *names[] = {"add", "sub", "mul", "eq", "ne",  "lt",    "le",
                                  "gt",  "ge",  "and", "or", "not", "isnull"};
    n->op = -1;
    for (uint32_t i = 0; i < sizeof(names) / sizeof(*names); i++)
        if (text_is(name, names[i]))
            n->op = (int)i;
    if (n->op < 0)
        return UD_UNSUPPORTED;
    const ud_json *args = member(json, "args");
    uint32_t count = n->op >= NOT ? 1 : 2;
    if (ud_json_kind(args) != UD_JSON_ARRAY || ud_json_size(args) != count)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < count; i++)
        UD_TRY(bind(e, ud_json_at(args, i), depth + 1, n->args + i));
    ud_type a = n->args[0]->type, b = count == 2 ? n->args[1]->type : a;
    if (n->op <= MUL) {
        if (a != b || (a != UD_INT64 && a != UD_FLOAT64))
            return UD_SCHEMA;
        n->type = a;
    } else {
        if (n->op != ISNULL && (a != b || a >= UD_STRUCT))
            return UD_SCHEMA;
        if (n->op >= AND && n->op <= NOT && a != UD_BOOL)
            return UD_SCHEMA;
        /* Decimal comparisons require explicit bound scale in future AST metadata. */
        if (a == UD_DECIMAL128)
            return UD_UNSUPPORTED;
        n->type = UD_BOOL;
    }
    if (n->args[0]->constant && (count == 1 || n->args[1]->constant)) {
        UD_TRY(eval(e, n, NULL, 0, &n->value));
        n->constant = 1;
    }
    return UD_OK;
}
ud_status ud_expression_create(ud_context *ctx, ud_string json, const ud_expression_options *o,
                               ud_expression **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !o || (!json.data && json.length))
        return UD_INVALID;
    if (o->struct_size < sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    ud_expression *e = ud_alloc(ctx, sizeof(*e));
    if (!e)
        return UD_RESOURCE;
    e->ctx = ctx;
    e->resolve = o->resolve;
    e->user = o->user;
    ud_status s = ud_json_parse(ctx, json.data, json.length, &e->document, NULL);
    if (!s && o->schema) {
        uint32_t count = ud_batch_columns(o->schema);
        ud_field *fields = ud_alloc(ctx, count * sizeof(*fields));
        if (!fields)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < count; i++)
            s = ud_batch_field(o->schema, i, fields + i);
        if (!s)
            s = ud_batch_create(ctx, fields, count, 0, &e->schema);
        ud_free(fields);
    }
    if (!s)
        s = bind(e, e->document, 0, &e->root);
    if (s) {
        ud_expression_destroy(e);
        return ud_fail(ctx, s, "expression binding failed");
    }
    *out = e;
    return UD_OK;
}
ud_type ud_expression_type(const ud_expression *e) {
    return e ? e->root->type : 0;
}
ud_status ud_expression_evaluate(const ud_expression *e, const ud_batch *b, uint32_t row,
                                 ud_value *out) {
    if (!e || !out || (e->schema && (!b || row >= ud_batch_rows(b))))
        return UD_INVALID;
    if (e->schema) {
        if (ud_batch_columns(b) != ud_batch_columns(e->schema))
            return UD_SCHEMA;
        for (uint32_t i = 0; i < ud_batch_columns(b); i++) {
            ud_field x, y;
            ud_batch_field(b, i, &x);
            ud_batch_field(e->schema, i, &y);
            if (!ud_field_equal(&x, &y, 1))
                return UD_SCHEMA;
        }
    }
    if (ud_is_cancelled(e->ctx))
        return UD_CANCELLED;
    return eval(e, e->root, b, row, out);
}
ud_status ud_expression_evaluate_batch(const ud_expression *e, const ud_batch *b, ud_string name,
                                       ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!e || !b)
        return UD_INVALID;
    ud_field f = {
        .name = name.data, .name_length = name.length, .type = e->root->type, .nullable = 1};
    ud_status s = ud_batch_create(e->ctx, &f, 1, ud_batch_rows(b), out);
    for (uint32_t i = 0; !s && i < ud_batch_rows(b); i++) {
        ud_value v;
        s = ud_expression_evaluate(e, b, i, &v);
        if (!s)
            s = ud_batch_append(*out, &v, 1);
    }
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
void ud_expression_destroy(ud_expression *e) {
    if (!e)
        return;
    node_free(e->root);
    ud_batch_destroy(e->schema);
    ud_json_destroy(e->document);
    ud_free(e);
}
