#include "../internal.h"
#include "ud/format.h"
#include "ud/sql.h"
#include <math.h>
static const ud_json *member(const ud_json *v, const char *key) {
    return ud_json_find(v, key, (uint32_t)strlen(key));
}
static int text_is(ud_string value, const char *name) {
    return value.length == strlen(name) && !memcmp(value.data, name, value.length);
}
static ud_status string(const ud_json *v, ud_string *out) {
    if (ud_json_kind(v) != UD_JSON_STRING)
        return UD_SCHEMA;
    return ud_json_text(v, &out->data, &out->length);
}
ud_status ud_expr_error(ud_expression *e, ud_status s, uint32_t start, uint32_t length,
                        const char *message) {
    if (!e->diagnostic.status) {
        e->diagnostic.status = s;
        e->diagnostic.start = start;
        e->diagnostic.length = length;
        snprintf(e->diagnostic.message, sizeof(e->diagnostic.message), "%s", message);
    }
    return s;
}
expression_node *ud_expr_node(ud_expression *e, int kind, uint32_t start, uint32_t length) {
    if (++e->nodes > 4096) {
        ud_expr_error(e, UD_SCHEMA, start, length, "Expression exceeds 4096 nodes");
        return NULL;
    }
    expression_node *n = ud_alloc(e->ctx, sizeof(*n));
    if (n) {
        n->kind = kind;
        n->span_start = start;
        n->span_length = length;
    }
    return n;
}
void ud_expr_node_free(expression_node *n) {
    if (!n)
        return;
    for (uint32_t j = 0; j < 16; j++)
        ud_expr_node_free(n->args[j]);
    if (n->value_owned)
        ud_clear_value(&n->value);
    if (n->field_owned)
        ud_field_clear(&n->field);
    if (n->name_owned)
        ud_free((void *)n->name.data);
    ud_batch_destroy(n->literal_owner);
    ud_free(n);
}
static ud_status own_field(ud_expression *e, expression_node *n, const ud_field *field) {
    if (!(field->identity.high || field->identity.low) && field->lineage_count) {
        char scope[96];
        int length = snprintf(scope, sizeof(scope), "expression.ir.%u.%u.%d", n->span_start,
                              n->span_length, n->op);
        UD_TRY(ud_field_derive(e->ctx, field, (ud_string){scope, (uint32_t)length},
                               (ud_string){field->name, field->name_length}, &n->field));
    } else
        UD_TRY(ud_field_copy(e->ctx, field, (ud_id){0}, &n->field));
    n->field_owned = 1;
    n->type = n->field.type;
    return UD_OK;
}
ud_status ud_expr_bind_literal(ud_expression *e, expression_node *n, const ud_field *field,
                               const ud_value *value) {
    ud_field spec = *field;
    spec.name = "literal";
    spec.name_length = 7;
    spec.nullable = value->is_null != 0;
    spec.identity = (ud_id){0};
    spec.physical_identity = (ud_id){0};
    spec.lineage = NULL;
    spec.lineage_count = 0;
    spec.key_order = 0;
    UD_TRY(own_field(e, n, &spec));
    ud_free((void *)n->field.lineage);
    n->field.lineage = NULL;
    n->field.lineage_count = 0;
    UD_TRY(ud_value_validate_field(e->ctx, &n->field, value));
    UD_TRY(ud_copy_value(e->ctx, value, &n->value, 0));
    n->value_owned = 1;
    n->constant = 1;
    return UD_OK;
}
ud_status ud_expr_bind_reference(ud_expression *e, expression_node *n, ud_string name, int kind) {
    if (!name.length || memchr(name.data, 0, name.length))
        return UD_SCHEMA;
    n->kind = kind;
    n->name.data = ud_strdup_n(e->ctx, name.data, name.length);
    n->name.length = name.length;
    n->name_owned = 1;
    if (!n->name.data)
        return UD_RESOURCE;
    ud_field field = {0};
    if (kind == COLUMN) {
        if (!e->schema)
            return UD_SCHEMA;
        int found = 0;
        for (uint32_t j = 0; j < e->schema->columns; j++) {
            field = e->schema->column[j].field;
            if (field.name_length == name.length && !memcmp(field.name, name.data, name.length)) {
                n->column = j;
                found = 1;
                break;
            }
        }
        if (!found) {
            uint32_t position = 0;
            const ud_field *fields = e->schema->schema->fields;
            uint32_t count = e->schema->columns;
            while (position < name.length) {
                uint32_t end = position;
                while (end < name.length && name.data[end] != '.')
                    end++;
                int at = -1;
                for (uint32_t j = 0; j < count; j++)
                    if (fields[j].name_length == end - position &&
                        !memcmp(fields[j].name, name.data + position, end - position)) {
                        at = (int)j;
                        break;
                    }
                if (at < 0)
                    return ud_expr_error(e, UD_SCHEMA, n->span_start, n->span_length,
                                         "Unknown input field path");
                field = fields[at];
                if (!position)
                    n->column = (uint32_t)at;
                else {
                    if (n->path_length == 32)
                        return UD_SCHEMA;
                    n->path[n->path_length++] = (uint32_t)at;
                }
                position = end + 1;
                if (end == name.length) {
                    found = 1;
                    break;
                }
                if (field.type != UD_STRUCT)
                    return UD_SCHEMA;
                fields = field.children;
                count = field.child_count;
            }
        }
        if (!found)
            return ud_expr_error(e, UD_SCHEMA, n->span_start, n->span_length,
                                 "Unknown input column");
        n->has_columns = 1;
    } else {
        if (!e->resolve)
            return UD_SCHEMA;
        ud_value value = {0};
        UD_TRY(e->resolve(e->user, name, &value));
        if (e->resolve_field) {
            UD_TRY(e->resolve_field(e->user, name, &field));
            if (field.type != value.type)
                return UD_SCHEMA;
        } else {
            if (value.type < UD_BOOL || value.type > UD_UTF8)
                return UD_SCHEMA;
            field = (ud_field){
                .name = "reference", .name_length = 9, .type = value.type, .nullable = 1};
        }
    }
    return own_field(e, n, &field);
}
static int numeric(const ud_field *f) {
    return !f->dictionary_kind && f->scalar.kind != UD_DT_DATE &&
           f->scalar.kind != UD_DT_FILETIME &&
           (f->type == UD_INT64 || f->type == UD_UINT64 || f->type == UD_FLOAT64 ||
            f->type == UD_DECIMAL128);
}
static unsigned bits(const ud_field *f) {
    uint32_t k = f->scalar.kind;
    return k >= UD_DT_I1 && k <= UD_DT_I4     ? 8u << (k - UD_DT_I1)
           : k >= UD_DT_UI1 && k <= UD_DT_UI4 ? 8u << (k - UD_DT_UI1)
                                              : 64;
}
static ud_status eval(const ud_expression *, const expression_node *, const ud_batch *, uint32_t,
                      ud_value *);
static ud_status calculate(ud_expression *e, expression_node *n, const ud_batch *batch,
                           uint32_t row, ud_value *out) {
    ud_value values[16] = {{0}};
    out->type = n->type;
    out->precision = n->field.precision;
    out->scale = n->field.scale;
    UD_TRY(eval(e, n->args[0], batch, row, values));
    if (n->kind == OPERATION && n->op == CONDITIONAL) {
        if (values[0].is_null) {
            out->is_null = 1;
            return UD_OK;
        }
        uint32_t selected = values[0].integer ? 1 : 2;
        UD_TRY(eval(e, n->args[selected], batch, row, values + selected));
        return ud_value_cast_field(e->ctx, &n->args[selected]->field, values + selected, &n->field,
                                   UD_ROUND_HALF_AWAY, 0, out);
    }
    for (uint32_t j = 1; j < n->argument_count; j++)
        UD_TRY(eval(e, n->args[j], batch, row, values + j));
    if (n->kind == FUNCTION) {
        ud_field fields[16];
        for (uint32_t j = 0; j < n->argument_count; j++)
            fields[j] = n->args[j]->field;
        return n->function.function_evaluate(n->function.user, e->ctx, fields, values,
                                             n->argument_count, &n->field, out);
    }
    ud_value *a = values, *b = values + 1;
    if (n->op == CAST)
        return ud_value_cast_field(e->ctx, &n->args[0]->field, a, &n->field, UD_ROUND_HALF_AWAY, 1,
                                   out);
    if (n->op == ISNULL) {
        out->integer = a->is_null;
        return UD_OK;
    }
    if (n->op == AND || n->op == OR || n->op == NOT) {
        int32_t x = a->is_null ? -1 : (int32_t)a->integer,
                y = b->is_null ? -1 : (int32_t)b->integer;
        int32_t v = n->op == AND  ? ud_truth_and(x, y)
                    : n->op == OR ? ud_truth_or(x, y)
                                  : ud_truth_not(x);
        out->is_null = v < 0;
        out->integer = v > 0;
        return UD_OK;
    }
    if (a->is_null || (n->argument_count > 1 && b->is_null)) {
        out->is_null = 1;
        return UD_OK;
    }
    ud_value left = {0}, right = {0};
    ud_status s = ud_value_cast_field(e->ctx, &n->args[0]->field, a, &n->operand_field,
                                      UD_ROUND_HALF_AWAY, 0, &left);
    if (!s && n->argument_count > 1)
        s = ud_value_cast_field(e->ctx, &n->args[1]->field, b, &n->operand_field,
                                UD_ROUND_HALF_AWAY, 0, &right);
    if (!s && n->op >= EQ && n->op <= GE) {
        int cmp = ud_value_compare(&left, &right);
        out->integer = n->op == EQ   ? cmp == 0
                       : n->op == NE ? cmp != 0
                       : n->op == LT ? cmp < 0
                       : n->op == LE ? cmp <= 0
                       : n->op == GT ? cmp > 0
                                     : cmp >= 0;
    } else if (!s && n->op == ADD && n->type == UD_UTF8) {
        uint64_t bytes = (uint64_t)left.text_length + right.text_length;
        if (bytes > UINT32_MAX)
            s = UD_RESOURCE;
        else {
            char *text = ud_alloc(e->ctx, (size_t)bytes + 1);
            if (!text)
                s = UD_RESOURCE;
            else {
                memcpy(text, left.text, left.text_length);
                memcpy(text + left.text_length, right.text, right.text_length);
                out->text = text;
                out->text_length = (uint32_t)bytes;
            }
        }
    } else if (!s && n->op == POS)
        s = ud_copy_value(e->ctx, &left, out, 0);
    else if (!s && (n->op == NEG)) {
        ud_value zero = {.type = left.type, .precision = left.precision, .scale = left.scale};
        s = ud_value_numeric(UD_NUMERIC_SUBTRACT, &zero, &left, &n->field, out);
    } else if (!s && (n->op >= BITAND && n->op <= BITNOT)) {
        uint64_t x = left.unsigned_integer, y = right.unsigned_integer,
                 result = n->op == BITAND   ? x & y
                          : n->op == BITOR  ? x | y
                          : n->op == BITXOR ? x ^ y
                                            : ~x;
        unsigned width = bits(&n->field);
        if (width < 64) {
            uint64_t mask = (UINT64_C(1) << width) - 1;
            result &= mask;
            if (n->type == UD_INT64 && (result & (UINT64_C(1) << (width - 1))))
                result |= ~mask;
        }
        out->unsigned_integer = result;
    } else if (!s)
        s = ud_value_numeric(n->op == DIV   ? UD_NUMERIC_DIVIDE
                             : n->op == MOD ? UD_NUMERIC_REMAINDER
                                            : n->op,
                             &left, &right, &n->field, out);
    ud_clear_value(&left);
    ud_clear_value(&right);
    return s;
}
static ud_status eval(const ud_expression *e, const expression_node *input, const ud_batch *batch,
                      uint32_t row, ud_value *out) {
    expression_node *n = (expression_node *)input;
    if (n->constant) {
        *out = n->value;
        return UD_OK;
    }
    if (n->kind == COLUMN) {
        UD_TRY(ud_batch_get(batch, row, n->column, out));
        for (uint32_t j = 0; j < n->path_length; j++) {
            if (out->is_null) {
                *out = (ud_value){.type = n->type,
                                  .is_null = 1,
                                  .is_absent = out->is_absent,
                                  .precision = n->field.precision,
                                  .scale = n->field.scale};
                break;
            }
            if (out->type != UD_STRUCT || n->path[j] >= out->child_count)
                return UD_SCHEMA;
            *out = out->children[n->path[j]];
        }
        return out->is_absent ? UD_INCOMPATIBLE : UD_OK;
    }
    if (n->kind == REFERENCE) {
        UD_TRY(e->resolve(e->user, n->name, out));
        if (out->is_absent)
            return UD_INCOMPATIBLE;
        return ud_value_validate_field(e->ctx, &n->field, out);
    }
    ud_value result = {0};
    ud_status s;
    if (n->kind == FUNCTION && !n->argument_count)
        s = n->function.function_evaluate(n->function.user, e->ctx, NULL, NULL, 0, &n->field,
                                          &result);
    else
        s = calculate((ud_expression *)e, n, batch, row, &result);
    if (!s)
        s = ud_value_validate_field(e->ctx, &n->field, &result);
    if (s) {
        ud_clear_value(&result);
        return ud_fail(e->ctx, s, "expression at %u:%u failed", n->span_start, n->span_length);
    }
    if (n->value_owned)
        ud_clear_value(&n->value);
    n->value = result;
    n->value_owned = 1;
    *out = n->value;
    return UD_OK;
}
ud_status ud_expr_finish(ud_expression *e, expression_node *n) {
    ud_field fields[16] = {{0}}, result = {.name = "value", .name_length = 5};
    int constant = 1;
    for (uint32_t j = 0; j < n->argument_count; j++) {
        fields[j] = n->args[j]->field;
        n->has_columns |= n->args[j]->has_columns;
        constant &= n->args[j]->constant;
    }
    if (n->kind == FUNCTION) {
        n->function = (ud_component_descriptor){.struct_size = sizeof(n->function),
                                                .abi_version = UD_ABI_VERSION};
        ud_status s = ud_registry_find(e->registry, n->name, &n->function);
        if (s) {
            char lower[128];
            if (n->name.length >= sizeof(lower))
                return s;
            for (uint32_t j = 0; j < n->name.length; j++)
                lower[j] =
                    (char)(n->name.data[j] >= 'A' && n->name.data[j] <= 'Z' ? n->name.data[j] + 32
                                                                            : n->name.data[j]);
            s = ud_registry_find(e->registry, (ud_string){lower, n->name.length}, &n->function);
        }
        UD_TRY(s);
        uint64_t required = e->function_capabilities;
        if (n->function.category != UD_COMPONENT_FUNCTION ||
            !(n->function.capabilities & (UD_FUNCTION_PURE | UD_FUNCTION_CONTEXTUAL)) ||
            (n->function.capabilities & required) != required)
            return UD_AUTH;
        if (n->argument_count < n->function.min_arguments ||
            n->argument_count > n->function.max_arguments)
            return UD_SCHEMA;
        ud_batch *bound = NULL;
        ud_status binding =
            n->function.function_bind(n->function.user, e->ctx, fields, n->argument_count, &bound);
        if (!binding && (!bound || bound->columns != 1 || bound->rows))
            binding = UD_SCHEMA;
        if (!binding)
            result = bound->schema->fields[0];
        n->literal_owner = bound;
        UD_TRY(binding);
        constant &= !!(n->function.capabilities & UD_FUNCTION_DETERMINISTIC);
        if (text_is(n->function.id, "dateadd") &&
            (n->args[0]->has_columns || n->args[1]->has_columns))
            return UD_SCHEMA;
        if ((text_is(n->function.id, "datepart") || text_is(n->function.id, "datediff")) &&
            n->args[0]->has_columns)
            return UD_SCHEMA;
    } else if (n->op == CAST) {
        result = n->field;
        result.nullable = fields[0].nullable;
    } else if (n->op == CONDITIONAL) {
        if (fields[0].type != UD_BOOL)
            return UD_SCHEMA;
        UD_TRY(ud_scalar_common(fields + 1, fields + 2, &result));
        result.nullable = fields[0].nullable || fields[1].nullable || fields[2].nullable;
    } else if (n->op == ISNULL || n->op == NOT || n->op == AND || n->op == OR) {
        if (n->op != ISNULL &&
            (fields[0].type != UD_BOOL || (n->argument_count == 2 && fields[1].type != UD_BOOL)))
            return UD_SCHEMA;
        result = (ud_field){.name = "value",
                            .name_length = 5,
                            .type = UD_BOOL,
                                .nullable = n->op != ISNULL &&
                                            (fields[0].nullable ||
                                             (n->argument_count == 2 && fields[1].nullable)),
                            .scalar = {.kind = UD_DT_BOOL}};
    } else {
        UD_TRY(ud_scalar_common(fields, n->argument_count == 1 ? fields : fields + 1,
                                &n->operand_field));
        result = n->operand_field;
        if (n->op >= EQ && n->op <= GE)
            result = (ud_field){.name = "value",
                                .name_length = 5,
                                .type = UD_BOOL,
                                .nullable = fields[0].nullable || fields[1].nullable,
                                .scalar = {.kind = UD_DT_BOOL}};
        else if (n->op == ADD && result.type == UD_UTF8) {
            if (result.scalar.kind) {
                uint64_t length = (uint64_t)fields[0].scalar.length + fields[1].scalar.length;
                if (length > INT32_MAX)
                    return UD_OVERFLOW;
                result.scalar.length = (uint32_t)length;
            }
        } else if (n->op >= BITAND && n->op <= BITNOT) {
            if (result.type != UD_INT64 && result.type != UD_UINT64)
                return UD_SCHEMA;
        } else {
            if (!numeric(&result) && result.type != UD_DURATION && result.type != UD_INTERVAL)
                return UD_SCHEMA;
            if (result.type == UD_DECIMAL128) {
                if (n->op == MUL) {
                    result.scale *= 2;
                    result.precision *= 2;
                    if (result.precision > 38)
                        result.precision = 38;
                    if (result.scale > 38)
                        return UD_OVERFLOW;
                } else if (n->op == DIV) {
                    result.scale = result.scale < 6 ? 6 : result.scale;
                    result.precision = 38;
                } else if ((n->op == ADD || n->op == SUB) && result.scalar.kind &&
                           result.precision < 38)
                    result.precision++;
            }
        }
    }
    result.name = "value";
    result.name_length = 5;
    result.identity = (ud_id){0};
    result.physical_identity = (ud_id){0};
    result.key_order = 0;
    result.lineage = NULL;
    result.lineage_count = 0;
    ud_id *origins = NULL;
    UD_TRY(ud_lineage_merge(e->ctx, fields, n->argument_count, &origins, &result.lineage_count));
    result.lineage = origins;
    ud_status s = own_field(e, n, &result);
    ud_free(origins);
    if (n->kind == FUNCTION) {
        ud_batch_destroy(n->literal_owner);
        n->literal_owner = NULL;
    }
    UD_TRY(s);
    if (constant) {
        ud_value value = {0};
        UD_TRY(eval(e, n, NULL, 0, &value));
        n->constant = 1;
    }
    return UD_OK;
}
static ud_status bind_json(ud_expression *e, const ud_json *json, uint32_t depth,
                           expression_node **out) {
    *out = NULL;
    if (depth > 64 || ud_json_kind(json) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    expression_node *n = ud_expr_node(e, 0, 0, e->source.length);
    if (!n)
        return UD_RESOURCE;
    *out = n;
    const ud_json *literal = member(json, "literal"), *ref = member(json, "ref"),
                  *col = member(json, "column"), *op = member(json, "op"),
                  *function = member(json, "function"), *cast = member(json, "cast");
    if (!!literal + !!ref + !!col + !!op + !!function + !!cast != 1)
        return UD_SCHEMA;
    if (literal) {
        static const char *keys[] = {"literal",  "type",     "precision",  "scale",
                                     "timezone", "children", "dictionary", "scalar"};
        UD_TRY(ud_json_keys(json, keys, 8));
        ud_bytes schema = {.ctx = e->ctx}, rows = {.ctx = e->ctx};
        const char *prefix = "{\"version\":" UD_PROTOCOL_VERSION_TEXT
                             ",\"fields\":[{\"name\":\"literal\",\"nullable\":true";
        ud_status s = ud_bytes_put(&schema, prefix, strlen(prefix));
        for (uint32_t j = 1; !s && j < 8; j++)
            if (member(json, keys[j])) {
                char *text = NULL;
                uint32_t len = 0;
                s = ud_json_serialize(member(json, keys[j]), &text, &len);
                if (!s)
                    s = ud_bytes_put(&schema, ",", 1);
                if (!s)
                    s = ud_json_quote(&schema, keys[j], (uint32_t)strlen(keys[j]));
                if (!s)
                    s = ud_bytes_put(&schema, ":", 1);
                if (!s)
                    s = ud_bytes_put(&schema, text, len);
                ud_free(text);
            }
        if (!s)
            s = ud_bytes_put(&schema, "}]}", 3);
        char *text = NULL;
        uint32_t len = 0;
        if (!s)
            s = ud_json_serialize(literal, &text, &len);
        if (!s)
            s = ud_bytes_put(&rows, "[{\"literal\":", 12);
        if (!s)
            s = ud_bytes_put(&rows, text, len);
        if (!s)
            s = ud_bytes_put(&rows, "}]", 2);
        ud_free(text);
        if (!s)
            s = ud_batch_from_json_schema(e->ctx, (char *)schema.data, (uint32_t)schema.size,
                                          (char *)rows.data, (uint32_t)rows.size, UD_ROUND_EXACT,
                                          &n->literal_owner);
        ud_bytes_clear(&schema);
        ud_bytes_clear(&rows);
        ud_value value = {0};
        ud_field field = {0};
        if (!s)
            s = ud_batch_get(n->literal_owner, 0, 0, &value);
        if (!s)
            s = ud_batch_field(n->literal_owner, 0, &field);
        if (!s) {
            n->kind = LITERAL;
            s = ud_expr_bind_literal(e, n, &field, &value);
        }
        return s;
    }
    if (ref || col) {
        if (ud_json_size(json) != 1)
            return UD_SCHEMA;
        ud_string name;
        UD_TRY(string(ref ? ref : col, &name));
        return ud_expr_bind_reference(e, n, name, ref ? REFERENCE : COLUMN);
    }
    if (cast) {
        static const char *keys[] = {"cast", "value"};
        UD_TRY(ud_json_keys(json, keys, 2));
        if (ud_json_kind(cast) != UD_JSON_OBJECT || !member(json, "value"))
            return UD_SCHEMA;
        char *spec = NULL;
        uint32_t length = 0;
        UD_TRY(ud_json_serialize(cast, &spec, &length));
        ud_bytes schema = {.ctx = e->ctx};
        const char *prefix = "{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"fields\":[";
        ud_status status = ud_bytes_put(&schema, prefix, strlen(prefix));
        if (!status)
            status = ud_bytes_put(&schema, spec, length);
        if (!status)
            status = ud_bytes_put(&schema, "]}", 2);
        ud_free(spec);
        if (!status)
            status = ud_schema_from_json(e->ctx, (char *)schema.data, (uint32_t)schema.size,
                                         (ud_string){"expression-cast", 15}, &n->literal_owner);
        ud_bytes_clear(&schema);
        UD_TRY(status);
        if (n->literal_owner->columns != 1)
            return UD_SCHEMA;
        n->field = n->literal_owner->schema->fields[0];
        n->kind = OPERATION;
        n->op = CAST;
        n->argument_count = 1;
        UD_TRY(bind_json(e, member(json, "value"), depth + 1, n->args));
        return ud_expr_finish(e, n);
    }
    if (ud_json_size(json) != 2)
        return UD_SCHEMA;
    ud_string name;
    UD_TRY(string(function ? function : op, &name));
    const ud_json *args = member(json, "args");
    if (ud_json_kind(args) != UD_JSON_ARRAY || ud_json_size(args) > 16)
        return UD_SCHEMA;
    n->argument_count = ud_json_size(args);
    for (uint32_t j = 0; j < n->argument_count; j++)
        UD_TRY(bind_json(e, ud_json_at(args, j), depth + 1, n->args + j));
    if (function) {
        n->kind = FUNCTION;
        n->name = name;
    } else {
        n->kind = OPERATION;
        static const char *names[] = {"add",    "sub",    "mul",        "eq",  "ne",     "lt",
                                      "le",     "gt",     "ge",         "and", "or",     "not",
                                      "isnull", "div",    "mod",        "neg", "bitand", "bitor",
                                      "bitxor", "bitnot", "conditional"};
        n->op = -1;
        for (uint32_t j = 0; j < 21; j++)
            if (text_is(name, names[j]))
                n->op = (int)j;
        if (text_is(name, "pos"))
            n->op = POS;
        if (n->op < 0)
            return UD_UNSUPPORTED;
        uint32_t count =
            n->op == NOT || n->op == ISNULL || n->op == NEG || n->op == BITNOT || n->op == POS ? 1
            : n->op == CONDITIONAL                                                             ? 3
                                                                                               : 2;
        if (n->argument_count != count)
            return UD_SCHEMA;
    }
    return ud_expr_finish(e, n);
}
ud_status ud_expression_compile(ud_context *ctx, ud_string input, const ud_expression_options *o,
                                ud_expression_diagnostic *diagnostic, ud_expression **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !o || !input.data || !input.length)
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
    e->registry = o->registry;
    e->function_capabilities = o->function_capabilities;
    e->source = input;
    e->diagnostic = (ud_expression_diagnostic){.struct_size = sizeof(e->diagnostic),
                                               .abi_version = UD_ABI_VERSION};
    ud_status s = UD_OK;
    if (!e->registry) {
        s = ud_registry_create(ctx, &e->registry);
        e->registry_owned = !s;
        if (!s)
            s = ud_expression_register_functions(e->registry);
    }
    if (!s) {
        s = ud_registry_acquire(e->registry);
        e->registry_leased = !s;
    }
    if (!s && o->schema)
        s = ud_batch_create(ctx, o->schema->schema->fields, o->schema->columns, 0, &e->schema);
    uint32_t first = 0;
    while (first < input.length && (input.data[first] == ' ' || input.data[first] == '\t' ||
                                    input.data[first] == '\r' || input.data[first] == '\n'))
        first++;
    if (!s && first < input.length && input.data[first] == '{') {
        s = ud_json_parse(ctx, input.data, input.length, &e->document, NULL);
        if (!s)
            s = bind_json(e, e->document, 0, &e->root);
    } else if (!s)
        s = ud_expr_parse(e, input, &e->root);
    if (diagnostic) {
        *diagnostic = e->diagnostic;
        if (s && !diagnostic->status) {
            diagnostic->status = s;
            snprintf(diagnostic->message, sizeof(diagnostic->message), "Expression binding failed");
        }
    }
    if (s) {
        ud_expression_destroy(e);
        return ud_fail(ctx, s, "expression compilation failed");
    }
    *out = e;
    return UD_OK;
}
ud_status ud_expression_create(ud_context *ctx, ud_string input,
                               const ud_expression_options *options, ud_expression **out) {
    /* Component/Package JSON string values use the formula frontend. */
    if (input.data && input.length && input.data[0] == '"') {
        ud_json *json = NULL;
        UD_TRY(ud_json_parse(ctx, input.data, input.length, &json, NULL));
        ud_string formula;
        ud_status s = string(json, &formula);
        if (!s)
            s = ud_expression_compile(ctx, formula, options, NULL, out);
        ud_json_destroy(json);
        return s;
    }
    return ud_expression_compile(ctx, input, options, NULL, out);
}
ud_type ud_expression_type(const ud_expression *e) {
    return e ? e->root->type : 0;
}
ud_status ud_expression_field(const ud_expression *e, ud_field *out) {
    if (!e || !out)
        return UD_INVALID;
    *out = e->root->field;
    return UD_OK;
}
ud_status ud_expression_evaluate(const ud_expression *e, const ud_batch *batch, uint32_t row,
                                 ud_value *out) {
    if (!e || !out || (e->schema && (!batch || row >= batch->rows)))
        return UD_INVALID;
    if (e->schema) {
        if (e->schema->columns != batch->columns)
            return UD_SCHEMA;
        for (uint32_t j = 0; j < batch->columns; j++)
            if (!ud_field_matches(e->schema->schema->fields + j, batch->schema->fields + j,
                                  UD_FIELD_PORT))
                return UD_SCHEMA;
    }
    if (ud_is_cancelled(e->ctx))
        return UD_CANCELLED;
    return eval(e, e->root, batch, row, out);
}
ud_status ud_expression_evaluate_batch(const ud_expression *e, const ud_batch *batch,
                                       ud_string name, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!e || !batch || !batch->sealed || batch->ctx != e->ctx)
        return UD_INVALID;
    ud_field field = {0};
    UD_TRY(ud_field_derive(e->ctx, &e->root->field, (ud_string){"expression-result", 17}, name,
                           &field));
    ud_status s = ud_batch_create(e->ctx, &field, 1, batch->rows, out);
    ud_field_clear(&field);
    for (uint32_t row = 0; !s && row < batch->rows; row++) {
        ud_value value = {0};
        s = ud_expression_evaluate(e, batch, row, &value);
        if (!s)
            s = ud_batch_append(*out, &value, 1);
        if (!s)
            s = ud_batch_copy_row_info(*out, row, batch, row);
    }
    if (!s)
        s = ud_batch_seal(*out);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_expression_describe(const ud_expression *e, char **out, uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    ud_field f = e->root->field;
    f.name = "result";
    f.name_length = 6;
    ud_batch *schema = NULL;
    UD_TRY(ud_batch_create(e->ctx, &f, 1, 0, &schema));
    ud_status s = ud_schema_to_json(schema, out, length);
    ud_batch_destroy(schema);
    return s;
}
void ud_expression_destroy(ud_expression *e) {
    if (!e)
        return;
    ud_expr_node_free(e->root);
    ud_batch_destroy(e->schema);
    ud_json_destroy(e->document);
    if (e->registry_leased)
        ud_registry_release(e->registry);
    if (e->registry_owned)
        ud_registry_destroy(e->registry);
    ud_free(e);
}
/* SQL lowering shares the bound AST and scalar semantics. Only exact primitive
 * domains are admitted; arithmetic, text/collation, temporal casts and mutable
 * references retain their Native vector execution. A caller must additionally
 * prove the provider's source columns have these actual domains. */
typedef struct sql_expression {
    ud_expression *expression;
    ud_bytes text;
    int32_t dialect;
    uint32_t count;
    ud_field fields[4096];
    ud_value values[4096];
} sql_expression;
static ud_status sql_put(sql_expression *s, const char *text) {
    return ud_bytes_put(&s->text, text, strlen(text));
}
static ud_status sql_node(sql_expression *s, const expression_node *n, int predicate) {
    if (n->type != UD_BOOL && n->type != UD_INT64 && n->type != UD_UINT64)
        return UD_UNSUPPORTED;
    int tsql = s->dialect == UD_SQL_SQLSERVER;
    if (n->type == UD_UINT64 || (tsql && !predicate && !n->constant && n->kind == OPERATION))
        return UD_UNSUPPORTED;
    int bit_test = tsql && predicate && (n->kind == COLUMN || n->constant);
    if (bit_test)
        UD_TRY(sql_put(s, "("));
    if (n->constant) {
        if (s->count == (tsql ? 2100u : 4096u))
            return UD_UNSUPPORTED;
        uint32_t at = s->count++;
        s->fields[at] = n->field;
        char name[24];
        int length = snprintf(name, sizeof(name), "p%u", at + 1);
        s->fields[at].name = ud_strdup_n(s->expression->ctx, name, (size_t)length);
        s->fields[at].name_length = (uint32_t)length;
        s->fields[at].identity = (ud_id){0};
        s->fields[at].lineage = NULL;
        s->fields[at].lineage_count = 0;
        s->fields[at].key_order = 0;
        s->values[at] = n->value;
        if (!s->fields[at].name)
            return UD_RESOURCE;
        char marker[24];
        int size = tsql ? snprintf(marker, sizeof(marker), "?")
                        : snprintf(marker, sizeof(marker), "$%u", at + 1);
        UD_TRY(ud_bytes_put(&s->text, marker, (size_t)size));
        return bit_test ? sql_put(s, " = 1)") : UD_OK;
    }
    if (n->kind == COLUMN) {
        if (n->path_length)
            return UD_UNSUPPORTED;
        const ud_field *field = &s->expression->schema->column[n->column].field;
        char closing = tsql ? ']' : '"';
        UD_TRY(sql_put(s, tsql ? "[" : "\""));
        for (uint32_t i = 0; i < field->name_length; i++) {
            if (field->name[i] == closing)
                UD_TRY(ud_bytes_put(&s->text, &closing, 1));
            UD_TRY(ud_bytes_put(&s->text, field->name + i, 1));
        }
        UD_TRY(ud_bytes_put(&s->text, &closing, 1));
        return bit_test ? sql_put(s, " = 1)") : UD_OK;
    }
    if (n->kind != OPERATION || n->op < EQ)
        return UD_UNSUPPORTED;
    UD_TRY(sql_put(s, "("));
    if (n->op == NOT)
        UD_TRY(sql_put(s, "NOT "));
    UD_TRY(sql_node(s, n->args[0], n->op == NOT || n->op == AND || n->op == OR));
    if (n->op == ISNULL)
        UD_TRY(sql_put(s, " IS NULL"));
    else if (n->op != NOT) {
        const char *operators[] = {" = ", " <> ", " < ", " <= ", " > ", " >= ", " AND ", " OR "};
        if (n->op > OR)
            return UD_UNSUPPORTED;
        UD_TRY(sql_put(s, operators[n->op - EQ]));
        UD_TRY(sql_node(s, n->args[1], n->op == AND || n->op == OR));
    }
    return sql_put(s, ")");
}
ud_status ud_expression_sql(ud_expression *expression, int32_t dialect, ud_string *text,
                            ud_batch **parameters) {
    if (!expression || !text || !parameters)
        return UD_INVALID;
    *text = (ud_string){0};
    *parameters = NULL;
    if ((dialect != UD_SQL_POSTGRESQL && dialect != UD_SQL_SQLSERVER) ||
        expression->root->type != UD_BOOL)
        return UD_UNSUPPORTED;
    sql_expression *sql = ud_alloc(expression->ctx, sizeof(*sql));
    if (!sql)
        return UD_RESOURCE;
    sql->dialect = dialect;
    sql->expression = expression;
    sql->text = (ud_bytes){.ctx = expression->ctx};
    ud_status status = sql_node(sql, expression->root, 1);
    if (!status && sql->count) {
        status = ud_batch_create(expression->ctx, sql->fields, sql->count, 1, parameters);
        if (!status)
            status = ud_batch_append(*parameters, sql->values, sql->count);
        if (!status)
            status = ud_batch_seal(*parameters);
    }
    if (!status) {
        text->length = (uint32_t)sql->text.size;
        status = ud_bytes_put(&sql->text, "", 1);
    }
    if (!status) {
        text->data = (char *)sql->text.data;
        sql->text.data = NULL;
    }
    for (uint32_t i = 0; i < sql->count; i++)
        ud_free((void *)sql->fields[i].name);
    ud_bytes_clear(&sql->text);
    ud_free(sql);
    if (status) {
        ud_batch_destroy(*parameters);
        *parameters = NULL;
    }
    return status;
}

ud_status ud_expression_language_catalog(ud_context *ctx, char **out, uint32_t *length) {
    if (!ctx || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    static const char *const names[] = {
        "add",    "sub",   "mul",    "eq",     "ne",          "lt",   "le",  "gt",
        "ge",     "and",   "or",     "not",    "isnull",      "div",  "mod", "neg",
        "bitand", "bitor", "bitxor", "bitnot", "conditional", "cast", "pos"};
    static const char *const symbols[] = {"+",  "-",  "*",  "==", "!=",     "<",      "<=", ">",
                                          ">=", "&&", "||", "!",  "ISNULL", "/",      "%",  "-",
                                          "&",  "|",  "^",  "~",  "?:",     "(type)", "+"};
    ud_bytes json = {.ctx = ctx};
    ud_status s = ud_bytes_put(&json, "{\"operators\":[", 14);
    for (uint32_t j = 0; !s && j < 23; j++) {
        if (j)
            s = ud_bytes_put(&json, ",", 1);
        char row[160];
        int n = snprintf(
            row, sizeof(row), "{\"id\":\"%s\",\"symbol\":\"%s\",\"arguments\":%u}", names[j],
            symbols[j],
            j == CONDITIONAL                                                              ? 3
            : j == NEG || j == BITNOT || j == NOT || j == ISNULL || j == CAST || j == POS ? 1
                                                                                          : 2);
        if (!s)
            s = ud_bytes_put(&json, row, (size_t)n);
    }
    if (!s)
        s = ud_bytes_put(&json, "],\"types\":[", sizeof("],\"types\":[") - 1);
    for (uint32_t j = 1; !s && j <= 29; j++) {
        if (j > 1)
            s = ud_bytes_put(&json, ",", 1);
        if (!s)
            s = ud_json_quote(&json, ud_scalar_type_name(j),
                              (uint32_t)strlen(ud_scalar_type_name(j)));
    }
    if (!s)
        s = ud_bytes_put(&json, "],\"typedNull\":\"NULL(typespec)\"}",
                         sizeof("],\"typedNull\":\"NULL(typespec)\"}") - 1);
    if (!s) {
        *length = (uint32_t)json.size;
        s = ud_bytes_put(&json, "", 1);
    }
    if (!s) {
        *out = (char *)json.data;
        json.data = NULL;
    }
    ud_bytes_clear(&json);
    return s;
}
