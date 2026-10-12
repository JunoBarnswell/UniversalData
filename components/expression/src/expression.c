#include "ud/expression.h"
#include "ud/json.h"
#include "ud/format.h"
#include "../../data/internal.h"
#include "../../contracts/registry_internal.h"
#include <math.h>
typedef struct expression_node {
    int kind, op, constant;
    ud_type type;
    ud_field field;
    uint32_t column;
    ud_string name;
    ud_value value;
    ud_column_id *lineage_owned;
    ud_batch *literal_owner;
    int field_owned, value_owned;
    uint32_t argument_count;
    ud_component_descriptor function;
    struct expression_node *args[16];
} expression_node;
struct ud_expression {
    ud_context *ctx;
    ud_json *document;
    expression_node *root;
    ud_expression_resolver resolve;
    ud_expression_field_resolver resolve_field;
    void *user;
    ud_batch *schema;
    uint32_t nodes;
    ud_registry *registry;
    int registry_owned, registry_leased;
    uint64_t function_capabilities;
};
enum { LITERAL, REFERENCE, COLUMN, OPERATION, FUNCTION };
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
        if (out->type != n->type) return UD_SCHEMA;
        return n->type == UD_DECIMAL128 && !out->is_null &&
                       (out->precision != n->field.precision || out->scale != n->field.scale)
                   ? UD_SCHEMA : UD_OK;
    }
    if (n->kind == COLUMN)
        return ud_batch_get(b, row, n->column, out);
    if (n->kind == FUNCTION) {
        ud_value arguments[16] = {{0}};
        for (uint32_t i = 0; i < n->argument_count; i++)
            UD_TRY(eval(e, n->args[i], b, row, arguments + i));
        expression_node *state = (expression_node *)n;
        if (state->value_owned) ud_clear_value(&state->value);
        state->value_owned = 0;
        memset(&state->value, 0, sizeof(state->value));
        ud_status s = n->function.function_evaluate(n->function.user, e->ctx, arguments,
                                                    n->argument_count, &n->field, &state->value);
        if (!s && state->value.type != n->field.type) s = UD_SCHEMA;
        if (!s && !state->value.is_null && n->field.type == UD_DECIMAL128 &&
            (state->value.precision != n->field.precision || state->value.scale != n->field.scale))
            s = UD_SCHEMA;
        if (s) { ud_clear_value(&state->value); return s; }
        state->value_owned = 1;
        *out = state->value;
        return UD_OK;
    }
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
    return ud_value_numeric(n->op, &a, &z, &n->field, out);
}
static void node_free(expression_node *n) {
    if (!n)
        return;
    for (uint32_t i = 0; i < 16; i++) node_free(n->args[i]);
    if (n->value_owned) ud_clear_value(&n->value);
    ud_free(n->lineage_owned);
    ud_batch_destroy(n->literal_owner);
    if (n->field_owned) ud_field_clear(&n->field);
    ud_free(n);
}
static void literal_origins(ud_field *field) {
    ud_free((void *)field->lineage);field->lineage=NULL;field->lineage_count=0;
    field->identity=(ud_column_id){0};
    for(uint32_t j=0;j<field->child_count;j++)literal_origins((ud_field *)field->children+j);
}
static ud_status bind_literal(ud_expression *e,expression_node *n,const ud_json *json,const ud_json *literal) {
    static const char *keys[]={"literal","type","precision","scale","timezone","children"};
    for(uint32_t j=0;j<ud_json_size(json);j++) {
        const char *key=NULL;uint32_t length=0;UD_TRY(ud_json_key(json,j,&key,&length));int known=0;
        for(uint32_t k=0;k<6;k++)if(length==strlen(keys[k]) && !memcmp(key,keys[k],length))known=1;
        if(!known)return UD_SCHEMA;
    }
    ud_bytes schema={.ctx=e->ctx},rows={.ctx=e->ctx};
    const char *prefix="{\"version\":4,\"fields\":[{\"name\":\"literal\",\"nullable\":true";
    ud_status s=ud_bytes_put(&schema,prefix,strlen(prefix));
    for(uint32_t j=1;!s && j<6;j++) {
        const ud_json *value=member(json,keys[j]);if(!value)continue;
        char *text=NULL;uint32_t length=0;s=ud_json_serialize(value,&text,&length);
        if(!s)s=ud_bytes_put(&schema,",",1);
        if(!s)s=ud_json_quote(&schema,keys[j],(uint32_t)strlen(keys[j]));
        if(!s)s=ud_bytes_put(&schema,":",1);
        if(!s)s=ud_bytes_put(&schema,text,length);
        ud_free(text);
    }
    if(!s)s=ud_bytes_put(&schema,"}]}",3);
    char *value=NULL;uint32_t length=0;
    if(!s)s=ud_json_serialize(literal,&value,&length);
    if(!s)s=ud_bytes_put(&rows,"[{\"literal\":",12);
    if(!s)s=ud_bytes_put(&rows,value,length);
    if(!s)s=ud_bytes_put(&rows,"}]",2);
    if(!s)s=ud_batch_from_json_schema(e->ctx,(const char *)schema.data,(uint32_t)schema.size,
                                     (const char *)rows.data,(uint32_t)rows.size,UD_ROUND_EXACT,&n->literal_owner);
    ud_free(value);ud_bytes_clear(&schema);ud_bytes_clear(&rows);
    if(!s)s=ud_batch_seal(n->literal_owner);
    ud_field field={0};
    if(!s)s=ud_batch_field(n->literal_owner,0,&field);
    if(!s){s=ud_field_copy(e->ctx,&field,(ud_column_id){0},&n->field);n->field_owned=!s;}
    if(!s){literal_origins(&n->field);n->type=n->field.type;n->kind=LITERAL;n->constant=1;s=ud_batch_get(n->literal_owner,0,0,&n->value);}
    return s;
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
                  *col = member(json, "column"), *op = member(json, "op"),
                  *function = member(json, "function");
    if ((!!literal + !!ref + !!col + !!op + !!function) != 1)
        return UD_SCHEMA;
    if (literal) {
        return bind_literal(e,n,json,literal);
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
            if (e->resolve_field) {
                ud_field borrowed;
                UD_TRY(e->resolve_field(e->user, n->name, &borrowed));
                UD_TRY(ud_field_copy(e->ctx, &borrowed, (ud_column_id){0}, &n->field));
                n->field_owned = 1;
                if (n->field.type != n->type) return UD_SCHEMA;
            } else {
                if (n->type < UD_BOOL || n->type > UD_UTF8) return UD_UNSUPPORTED;
                n->field.type = n->type;
                n->field.nullable = 1;
            }
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
                    n->field = f;
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
    if (function) {
        n->kind = FUNCTION;
        ud_string id;
        UD_TRY(string(function, &id));
        n->function = (ud_component_descriptor){.struct_size = sizeof(n->function),
                                                .abi_version = UD_ABI_VERSION};
        UD_TRY(ud_registry_find(e->registry, id, &n->function));
        uint64_t required = UD_FUNCTION_PURE | e->function_capabilities;
        if (n->function.category != UD_COMPONENT_FUNCTION ||
            (n->function.capabilities & required) != required) return UD_AUTH;
        const ud_json *arguments = member(json, "args");
        n->argument_count = ud_json_size(arguments);
        if (ud_json_kind(arguments) != UD_JSON_ARRAY ||
            n->argument_count < n->function.min_arguments ||
            n->argument_count > n->function.max_arguments || n->argument_count > 16)
            return UD_SCHEMA;
        ud_field fields[16] = {{0}}, result = {0};
        int constant = !!(n->function.capabilities & UD_FUNCTION_DETERMINISTIC);
        for (uint32_t i = 0; i < n->argument_count; i++) {
            UD_TRY(bind(e, ud_json_at(arguments, i), depth + 1, n->args + i));
            fields[i] = n->args[i]->field;
            constant = constant && n->args[i]->constant;
        }
        UD_TRY(n->function.function_bind(n->function.user, fields, n->argument_count, &result));
        UD_TRY(ud_field_copy(e->ctx, &result, (ud_column_id){0}, &n->field));
        n->field_owned = 1;
        ud_free((void *)n->field.lineage);
        n->field.lineage = NULL; n->field.lineage_count = 0;
        ud_column_id *origins = NULL;
        UD_TRY(ud_lineage_merge(e->ctx, fields, n->argument_count, &origins, &n->field.lineage_count));
        n->field.lineage = origins;
        n->field.identity = (ud_column_id){0};
        n->type = n->field.type;
        if (constant) {
            ud_value value;
            UD_TRY(eval(e, n, NULL, 0, &value));
            n->constant = 1;
        }
        return UD_OK;
    }
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
    n->argument_count = count;
    if (ud_json_kind(args) != UD_JSON_ARRAY || ud_json_size(args) != count)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < count; i++)
        UD_TRY(bind(e, ud_json_at(args, i), depth + 1, n->args + i));
    ud_type a = n->args[0]->type, b = count == 2 ? n->args[1]->type : a;
    if (n->op <= MUL) {
        UD_TRY(ud_numeric_result_field(n->op,&n->args[0]->field,&n->args[1]->field,&n->field));
        n->type=n->field.type;
    } else {
        if (n->op != ISNULL && a != b)
            return UD_SCHEMA;
        if (n->op >= AND && n->op <= NOT && a != UD_BOOL)
            return UD_SCHEMA;
        if (n->op >= EQ && n->op <= GE) {
            ud_field left = n->args[0]->field, right = n->args[1]->field;
            left.nullable = right.nullable = 1;
            if (!ud_field_matches(&left, &right, 0))
                return UD_SCHEMA;
        }
        n->type = UD_BOOL;
        n->field = (ud_field){.type = UD_BOOL, .nullable = 1};
    }
    ud_field dependencies[2] = {n->args[0]->field, count == 2 ? n->args[1]->field : (ud_field){0}};
    UD_TRY(ud_lineage_merge(e->ctx, dependencies, count, &n->lineage_owned, &n->field.lineage_count));
    n->field.lineage = n->lineage_owned;
    n->field.identity = (ud_column_id){0};
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
    if (o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    ud_expression *e = ud_alloc(ctx, sizeof(*e));
    if (!e)
        return UD_RESOURCE;
    e->ctx = ctx;
    e->resolve = o->resolve;
    e->resolve_field = o->resolve_field;
    e->user = o->user;
    e->function_capabilities = o->function_capabilities;
    e->registry = o->registry;
    ud_status s = UD_OK;
    if (!e->registry) {
        s = ud_registry_create(ctx, &e->registry);
        e->registry_owned = !s;
        if (!s) s = ud_expression_register_functions(e->registry);
    }
    if (!s) { s = ud_registry_acquire(e->registry); e->registry_leased = !s; }
    if (!s) s = ud_json_parse(ctx, json.data, json.length, &e->document, NULL);
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
ud_status ud_expression_field(const ud_expression *e, ud_field *out) {
    if (!e || !out)
        return UD_INVALID;
    *out = e->root->field;
    out->nullable = 1;
    return UD_OK;
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
            if (!ud_field_matches(&x, &y, 1))
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
    ud_field f = {0};
    ud_status s = ud_field_derive(e->ctx, &e->root->field, (ud_string){"expression-result",17}, name, &f);
    f.nullable = 1;
    if (!s) s = ud_batch_create(e->ctx, &f, 1, ud_batch_rows(b), out);
    ud_field_clear(&f);
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
    if (e->registry_leased) ud_registry_release(e->registry);
    if (e->registry_owned) ud_registry_destroy(e->registry);
    ud_free(e);
}
