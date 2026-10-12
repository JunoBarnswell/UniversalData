#include "../internal.h"
#include <math.h>
#include <ctype.h>
typedef struct group {
    ud_value *values;
    struct group *next;
} group;
struct ud_accumulator {
    const ud_plan *plan;
    group *head, *tail;
    uint32_t groups;
    int failed;
};
int32_t ud_truth_and(int32_t a, int32_t b) {
    return !a || !b ? 0 : a < 0 || b < 0 ? -1 : 1;
}
int32_t ud_truth_or(int32_t a, int32_t b) {
    return a > 0 || b > 0 ? 1 : a < 0 || b < 0 ? -1 : 0;
}
int32_t ud_truth_not(int32_t a) {
    return a < 0 ? -1 : !a;
}
int ud_value_compare(const ud_value *a, const ud_value *b) {
    if (a->is_null || b->is_null)
        return a->is_null == b->is_null ? 0 : a->is_null ? -1 : 1;
    if (a->type == UD_UTF8 || a->type == UD_BINARY) {
        uint32_t n = a->text_length < b->text_length ? a->text_length : b->text_length;
        int v = n ? memcmp(a->text, b->text, n) : 0;
        return v ? (v > 0 ? 1 : -1)
                 : (a->text_length > b->text_length) - (a->text_length < b->text_length);
    }
    if (a->type == UD_FLOAT64)
        return (a->real > b->real) - (a->real < b->real);
    if (a->type == UD_DECIMAL128)
        return ud_decimal_compare(a->decimal, b->decimal);
    if (a->type >= UD_STRUCT) {
        uint32_t count = a->child_count < b->child_count ? a->child_count : b->child_count;
        for (uint32_t j = 0; j < count; j++) {
            int cmp = ud_value_compare(a->children + j, b->children + j);
            if (cmp) return cmp;
        }
        return (a->child_count > b->child_count) - (a->child_count < b->child_count);
    }
    return (a->integer > b->integer) - (a->integer < b->integer);
}
static char *token(char **cursor) {
    char *p = *cursor;
    while (*p == ' ' || *p == '\t' || *p == '\r')
        p++;
    if (!*p) {
        *cursor = p;
        return NULL;
    }
    char *start = p;
    while (*p && *p != ' ' && *p != '\t' && *p != '\r')
        p++;
    if (*p)
        *p++ = 0;
    *cursor = p;
    return start;
}
static int identifier(const char *s) {
    if (!s || !(isalpha((unsigned char)*s) || *s == '_'))
        return 0;
    for (s++; *s; s++)
        if (!(isalnum((unsigned char)*s) || *s == '_'))
            return 0;
    return 1;
}
static int field_index(const ud_plan *p, const char *name) {
    for (uint32_t i = 0; i < p->n; i++)
        if (!strcmp(p->fields[i].name, name))
            return (int)i;
    return -1;
}
static ud_status grow(ud_context *c, void **ptr, uint32_t count, size_t width) {
    if (count == UINT32_MAX || (size_t)(count + 1) > SIZE_MAX / width)
        return UD_RESOURCE;
    void *v = ud_alloc(c, (size_t)(count + 1) * width);
    if (!v)
        return UD_RESOURCE;
    if (count)
        memcpy(v, *ptr, (size_t)count * width);
    ud_free(*ptr);
    *ptr = v;
    return UD_OK;
}
static ud_status parse_schema(ud_plan *p, char *text) {
    for (char *line = text; line && *line;) {
        char *end = strchr(line, '\n');
        if (end)
            *end++ = 0;
        char *cursor = line, *name = token(&cursor), *type = token(&cursor),
             *nullable = token(&cursor);
        if (!name) {
            line = end;
            continue;
        }
        if (!identifier(name) || !type || !nullable || token(&cursor) ||
            field_index(p, name) >= 0 || p->n == 65536)
            return UD_SCHEMA;
        ud_type t = !strcmp(type, "bool")      ? UD_BOOL
                    : !strcmp(type, "int64")   ? UD_INT64
                    : !strcmp(type, "float64") ? UD_FLOAT64
                    : !strcmp(type, "utf8")    ? UD_UTF8
                    : !strcmp(type, "date32")  ? UD_DATE32
                    : !strcmp(type, "time64")  ? UD_TIME64
                    : !strcmp(type, "binary")  ? UD_BINARY
                    : !strncmp(type, "decimal(", 8) ? UD_DECIMAL128
                    : !strncmp(type, "timestamp(", 10) ? UD_TIMESTAMP
                                               : 0;
        if (!t || (strcmp(nullable, "nullable") && strcmp(nullable, "required")))
            return UD_SCHEMA;
        UD_TRY(grow(p->ctx, (void **)&p->fields, p->n, sizeof(*p->fields)));
        ud_field *f = p->fields + p->n;
        f->name = ud_strdup_n(p->ctx, name, strlen(name));
        if (!f->name)
            return UD_RESOURCE;
        f->name_length = (uint32_t)strlen(name);
        f->type = t;
        f->nullable = !strcmp(nullable, "nullable");
        p->n++;
        if (t == UD_DECIMAL128) {
            int pos = 0;
            if (sscanf(type, "decimal(%d,%d)%n", &f->precision, &f->scale, &pos) != 2 ||
                type[pos] || f->precision < 1 || f->precision > 38 || f->scale < -38 || f->scale > 38)
                return UD_SCHEMA;
        } else if (t == UD_TIMESTAMP) {
            size_t len = strlen(type);
            if (len < 12 || type[len - 1] != ')') return UD_SCHEMA;
            f->timezone_length = (uint32_t)(len - 11);
            f->timezone = ud_strdup_n(p->ctx, type + 10, f->timezone_length);
            if (!f->timezone) return UD_RESOURCE;
        }
        line = end;
    }
    return p->n ? UD_OK : UD_SCHEMA;
}
static ud_status parse_commands(ud_plan *p, char *text) {
    for (char *line = text; line && *line;) {
        char *end = strchr(line, '\n');
        if (end)
            *end++ = 0;
        char *cursor = line, *cmd = token(&cursor);
        if (!cmd) {
            line = end;
            continue;
        }
        if (!strcmp(cmd, "project") || !strcmp(cmd, "group")) {
            uint32_t **list = !strcmp(cmd, "project") ? &p->projection : &p->keys;
            uint32_t *n = !strcmp(cmd, "project") ? &p->project_n : &p->key_n;
            if (*n)
                return UD_INVALID;
            char *name;
            while ((name = token(&cursor))) {
                int index = field_index(p, name);
                if (index < 0)
                    return UD_SCHEMA;
                for (uint32_t i = 0; i < *n; i++)
                    if ((*list)[i] == (uint32_t)index)
                        return UD_SCHEMA;
                UD_TRY(grow(p->ctx, (void **)list, *n, sizeof(uint32_t)));
                (*list)[(*n)++] = (uint32_t)index;
            }
            if (!*n)
                return UD_SCHEMA;
        } else if (!strcmp(cmd, "filter")) {
            char *name = token(&cursor), *op = token(&cursor);
            if (!name || !op)
                return UD_PARSE;
            int col = field_index(p, name);
            if (col < 0)
                return UD_SCHEMA;
            const char *ops[] = {"eq", "ne", "lt", "le", "gt", "ge", "isnull", "notnull"};
            int kind = -1;
            for (int i = 0; i < 8; i++)
                if (!strcmp(op, ops[i]))
                    kind = i;
            if (kind < 0)
                return UD_PARSE;
            while (*cursor == ' ' || *cursor == '\t')
                cursor++;
            size_t len = strlen(cursor);
            if (len && cursor[len - 1] == '\r')
                cursor[--len] = 0;
            ud_predicate pred = {.column = (uint32_t)col, .op = kind};
            pred.value.type = p->fields[col].type;
            if (kind < 6) {
                if (!len)
                    return UD_PARSE;
                if (!strcmp(cursor, "\\N"))
                    pred.value.is_null = 1;
                else
                    UD_TRY(ud_value_parse_field(p->ctx, p->fields + col, cursor, (uint32_t)len,
                                                UD_ROUND_EXACT, &pred.value));
            } else if (len)
                return UD_PARSE;
            UD_TRY(grow(p->ctx, (void **)&p->predicates, p->predicate_n, sizeof(pred)));
            if ((pred.value.type == UD_UTF8 || pred.value.type == UD_BINARY) && !pred.value.is_null && kind < 6) {
                pred.value.text = ud_strdup_n(p->ctx, cursor, len);
                if (!pred.value.text)
                    return UD_RESOURCE;
            }
            p->predicates[p->predicate_n++] = pred;
        } else {
            int kind = !strcmp(cmd, "count") ? 0
                       : !strcmp(cmd, "sum") ? 1
                       : !strcmp(cmd, "min") ? 2
                       : !strcmp(cmd, "max") ? 3
                                             : -1;
            char *name = token(&cursor), *alias = token(&cursor);
            if (kind < 0 || !name || !identifier(alias) || token(&cursor))
                return UD_PARSE;
            int index = field_index(p, name);
            if (kind == 0 && !strcmp(name, "*"))
                index = -1;
            else if (index < 0)
                return UD_SCHEMA;
            if (kind == 1 && p->fields[index].type != UD_INT64 &&
                p->fields[index].type != UD_FLOAT64 && p->fields[index].type != UD_DECIMAL128)
                return UD_UNSUPPORTED;
            for (uint32_t i = 0; i < p->agg_n; i++)
                if (!strcmp(p->aggs[i].name, alias))
                    return UD_SCHEMA;
            UD_TRY(grow(p->ctx, (void **)&p->aggs, p->agg_n, sizeof(*p->aggs)));
            ud_aggregate_def *a = p->aggs + p->agg_n;
            a->column = index < 0 ? UINT32_MAX : (uint32_t)index;
            a->kind = kind;
            a->name = ud_strdup_n(p->ctx, alias, strlen(alias));
            if (!a->name)
                return UD_RESOURCE;
            p->agg_n++;
        }
        line = end;
    }
    if (p->key_n && !p->agg_n)
        return UD_INVALID;
    if (p->agg_n) {
        for (uint32_t k = 0; k < p->key_n; k++)
            for (uint32_t i = 0; i < p->agg_n; i++)
                if (!strcmp(p->fields[p->keys[k]].name, p->aggs[i].name))
                    return UD_SCHEMA;
        if (p->project_n) {
            for (uint32_t k = 0; k < p->key_n; k++) {
                int found = 0;
                for (uint32_t i = 0; i < p->project_n; i++)
                    found |= p->projection[i] == p->keys[k];
                if (!found)
                    return UD_SCHEMA;
            }
            for (uint32_t k = 0; k < p->agg_n; k++)
                if (p->aggs[k].column != UINT32_MAX) {
                    int found = 0;
                    for (uint32_t i = 0; i < p->project_n; i++)
                        found |= p->projection[i] == p->aggs[k].column;
                    if (!found)
                        return UD_SCHEMA;
                }
        }
    }
    return UD_OK;
}
ud_status ud_plan_create(ud_context *ctx, const char *schema, uint32_t sn, const char *commands,
                         uint32_t cn, ud_plan **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !schema || (!commands && cn) || memchr(schema, 0, sn) ||
        (cn && memchr(commands, 0, cn)) || !ud_utf8_valid(schema, sn) ||
        !ud_utf8_valid(commands, cn))
        return UD_INVALID;
    size_t schema_header = sizeof(UD_SCHEMA_HEADER) - 1, plan_header = sizeof(UD_PLAN_HEADER) - 1;
    if (sn < schema_header || memcmp(schema, UD_SCHEMA_HEADER, schema_header) ||
        cn < plan_header || memcmp(commands, UD_PLAN_HEADER, plan_header))
        return ud_fail(ctx, UD_INCOMPATIBLE, "schema / plan document requires explicit version 2 header");
    ud_plan *p = ud_alloc(ctx, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->ctx = ctx;
    p->schema_text = ud_strdup_n(ctx, schema, sn);
    p->commands = ud_strdup_n(ctx, commands ? commands : "", cn);
    char *s = ud_strdup_n(ctx, schema, sn), *c = ud_strdup_n(ctx, commands ? commands : "", cn);
    ud_status code = UD_RESOURCE;
    if (s && c && p->schema_text && p->commands) {
        code = parse_schema(p, s + schema_header);
        if (!code)
            code = parse_commands(p, c + plan_header);
    }
    ud_free(s);
    ud_free(c);
    if (!code) {
        ud_bytes bytes = {.ctx = ctx};
        code = ud_put_string(&bytes, schema, sn);
        if (!code)
            code = ud_put_string(&bytes, commands, cn);
        if (!code)
            ud_hash(bytes.data, bytes.size, p->fingerprint);
        ud_bytes_clear(&bytes);
    }
    if (code) {
        ud_plan_destroy(p);
        return ud_fail(ctx, code, "invalid schema or plan commands (%s)", ud_status_name(code));
    }
    *out = p;
    return UD_OK;
}
void ud_plan_destroy(ud_plan *p) {
    if (!p)
        return;
    for (uint32_t i = 0; !p->schema_owner && i < p->n; i++) {
        ud_free((void *)p->fields[i].name);
        ud_free((void *)p->fields[i].timezone);
    }
    for (uint32_t i = 0; i < p->predicate_n; i++)
        if ((p->predicates[i].value.type == UD_UTF8 || p->predicates[i].value.type == UD_BINARY))
            ud_free((void *)p->predicates[i].value.text);
    for (uint32_t i = 0; i < p->agg_n; i++)
        ud_free(p->aggs[i].name);
    ud_free(p->fields);
    ud_free(p->projection);
    ud_free(p->keys);
    ud_free(p->predicates);
    ud_free(p->aggs);
    ud_free(p->schema_text);
    ud_free(p->commands);
    ud_batch_destroy(p->schema_owner);
    ud_free(p);
}
ud_status ud_plan_create_fields(ud_context *ctx, uint32_t version, const ud_field *fields,
                                 uint32_t columns, const char *commands, uint32_t length,
                                 ud_plan **out) {
    if (!out) return UD_INVALID;
    *out = NULL;
    if (version != UD_PLAN_VERSION) return UD_INCOMPATIBLE;
    if (!ctx || (!commands && length) || (length && memchr(commands, 0, length)) ||
        !ud_utf8_valid(commands, length)) return UD_INVALID;
    ud_plan *p = ud_alloc(ctx, sizeof(*p));
    if (!p) return UD_RESOURCE;
    p->ctx = ctx;
    ud_status s = ud_batch_create(ctx, fields, columns, 0, &p->schema_owner);
    if (!s) {
        p->fields = ud_alloc(ctx, columns * sizeof(*p->fields));
        if (!p->fields) s = UD_RESOURCE;
        else {
            p->n = columns;
            for (uint32_t j = 0; j < columns; j++) ud_batch_field(p->schema_owner, j, p->fields + j);
        }
    }
    p->commands = ud_strdup_n(ctx, commands ? commands : "", length);
    char *copy = ud_strdup_n(ctx, commands ? commands : "", length);
    if (!s && (!p->commands || !copy)) s = UD_RESOURCE;
    if (!s) s = parse_commands(p, copy);
    ud_free(copy);
    ud_bytes wire = {.ctx=ctx};
    if (!s) s = ud_put_u64(&wire, UD_PLAN_VERSION);
    if (!s) s = ud_encode_batch(&wire, p->schema_owner);
    if (!s) s = ud_put_string(&wire, commands, length);
    if (!s) ud_hash(wire.data, wire.size, p->fingerprint);
    ud_bytes_clear(&wire);
    if (s) { ud_plan_destroy(p); return s; }
    *out = p; return UD_OK;
}
ud_status ud_plan_schema(const ud_plan *p, const ud_field **f, uint32_t *n) {
    if (!p || !f || !n)
        return UD_INVALID;
    *f = p->fields;
    *n = p->n;
    return UD_OK;
}
void ud_plan_fingerprint(const ud_plan *p, uint8_t out[32]) {
    if (p && out)
        memcpy(out, p->fingerprint, 32);
}
ud_status ud_plan_check_batch(const ud_plan *p, const ud_batch *b) {
    if (!p || !b || ud_batch_columns(b) != p->n)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < p->n; i++) {
        ud_field f;
        ud_batch_field(b, i, &f);
        if (!ud_field_equal(&f, p->fields + i, 1))
            return ud_fail(p->ctx, UD_SCHEMA, "input schema mismatch at column %u", i);
    }
    return UD_OK;
}
ud_status ud_plan_accept(const ud_plan *p, const ud_batch *b, uint32_t row, int *accept) {
    if (ud_is_cancelled(p->ctx))
        return UD_CANCELLED;
    int result = 1;
    for (uint32_t i = 0; i < p->predicate_n; i++) {
        const ud_predicate *pred = p->predicates + i;
        ud_value v;
        UD_TRY(ud_batch_get(b, row, pred->column, &v));
        int t;
        if (pred->op == 6)
            t = v.is_null;
        else if (pred->op == 7)
            t = !v.is_null;
        else if (v.is_null || pred->value.is_null)
            t = -1;
        else {
            int cmp = ud_value_compare(&v, &pred->value);
            switch (pred->op) {
            case 0:
                t = cmp == 0;
                break;
            case 1:
                t = cmp != 0;
                break;
            case 2:
                t = cmp < 0;
                break;
            case 3:
                t = cmp <= 0;
                break;
            case 4:
                t = cmp > 0;
                break;
            default:
                t = cmp >= 0;
                break;
            }
        }
        result = ud_truth_and(result, t);
        if (!result)
            break;
    }
    *accept = result == 1;
    return UD_OK;
}
static ud_status own_value(ud_context *ctx, ud_value *target, const ud_value *source) {
    ud_status s = ud_copy_value(ctx, source, target, 0);
    if (s) ud_clear_value(target);
    return s;
}
static void clear_values(ud_value *v, uint32_t n) {
    if (!v)
        return;
    for (uint32_t i = 0; i < n; i++) ud_clear_value(v + i);
    ud_free(v);
}
static ud_status new_group(ud_accumulator *a, const ud_batch *b, uint32_t row, group **out) {
    const ud_plan *p = a->plan;
    uint32_t n = p->key_n + p->agg_n;
    if (a->groups == INT32_MAX)
        return UD_RESOURCE;
    group *g = ud_alloc(p->ctx, sizeof(*g));
    if (!g)
        return UD_RESOURCE;
    g->values = ud_alloc(p->ctx, n * sizeof(ud_value));
    if (!g->values) {
        ud_free(g);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < p->key_n; i++) {
        ud_value v;
        ud_batch_get(b, row, p->keys[i], &v);
        ud_status s = own_value(p->ctx, g->values + i, &v);
        if (s) {
            clear_values(g->values, n);
            ud_free(g);
            return s;
        }
    }
    for (uint32_t i = 0; i < p->agg_n; i++) {
        ud_value *v = g->values + p->key_n + i;
        v->type = p->aggs[i].kind == 0 ? UD_INT64 : p->fields[p->aggs[i].column].type;
        v->is_null = p->aggs[i].kind != 0;
        if (p->aggs[i].kind != 0) {
            v->precision = p->fields[p->aggs[i].column].precision;
            v->scale = p->fields[p->aggs[i].column].scale;
        }
    }
    if (a->tail)
        a->tail->next = g;
    else
        a->head = g;
    a->tail = g;
    a->groups++;
    *out = g;
    return UD_OK;
}
ud_status ud_accumulator_create(const ud_plan *p, ud_accumulator **out) {
    if (!p || !out || !p->agg_n)
        return UD_INVALID;
    *out = ud_alloc(p->ctx, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->plan = p;
    if (!p->key_n) {
        group *g;
        ud_status s = new_group(*out, NULL, 0, &g);
        if (s) {
            ud_accumulator_destroy(*out);
            *out = NULL;
            return s;
        }
    }
    return UD_OK;
}
void ud_accumulator_destroy(ud_accumulator *a) {
    if (!a)
        return;
    group *g = a->head;
    while (g) {
        group *next = g->next;
        clear_values(g->values, a->plan->key_n + a->plan->agg_n);
        ud_free(g);
        g = next;
    }
    ud_free(a);
}
static ud_status add_rows(ud_accumulator *a, const ud_batch *b) {
    const ud_plan *p = a->plan;
    UD_TRY(ud_plan_check_batch(p, b));
    for (uint32_t r = 0; r < ud_batch_rows(b); r++) {
        int accept;
        UD_TRY(ud_plan_accept(p, b, r, &accept));
        if (!accept)
            continue;
        group *g;
        for (g = a->head; g; g = g->next) {
            uint32_t k = 0;
            for (; k < p->key_n; k++) {
                ud_value v;
                ud_batch_get(b, r, p->keys[k], &v);
                if (ud_value_compare(&v, g->values + k))
                    break;
            }
            if (k == p->key_n)
                break;
        }
        if (!g)
            UD_TRY(new_group(a, b, r, &g));
        for (uint32_t i = 0; i < p->agg_n; i++) {
            const ud_aggregate_def *def = p->aggs + i;
            ud_value input = {.type = UD_INT64}, *v = g->values + p->key_n + i;
            if (def->column != UINT32_MAX)
                ud_batch_get(b, r, def->column, &input);
            if (input.is_null)
                continue;
            if (def->kind == 0) {
                if (v->integer == INT64_MAX)
                    return UD_OVERFLOW;
                v->integer++;
            } else if (v->is_null) {
                ud_clear_value(v);
                UD_TRY(own_value(p->ctx, v, &input));
            } else if (def->kind == 1) {
                if (v->type == UD_DECIMAL128) {
                    UD_TRY(ud_decimal_add(v->decimal, input.decimal, v->precision, &v->decimal));
                } else if (v->type == UD_INT64) {
                    if ((input.integer > 0 && v->integer > INT64_MAX - input.integer) ||
                        (input.integer < 0 && v->integer < INT64_MIN - input.integer))
                        return ud_fail(p->ctx, UD_OVERFLOW, "SUM int64 overflow");
                    v->integer += input.integer;
                } else {
                    double sum = v->real + input.real;
                    if (!isfinite(sum))
                        return UD_OVERFLOW;
                    v->real = sum;
                }
            } else {
                int cmp = ud_value_compare(&input, v);
                if ((def->kind == 2 && cmp < 0) || (def->kind == 3 && cmp > 0)) {
                    ud_value replacement;
                    UD_TRY(own_value(p->ctx, &replacement, &input));
                    ud_clear_value(v);
                    *v = replacement;
                }
            }
        }
    }
    return UD_OK;
}
ud_status ud_accumulator_add(ud_accumulator *a, const ud_batch *b) {
    if (!a || a->failed)
        return UD_INVALID;
    ud_status s = add_rows(a, b);
    if (s)
        a->failed = 1;
    return s;
}
static ud_status output_fields(const ud_plan *p, ud_field **out, uint32_t *n) {
    *n = p->key_n + p->agg_n;
    *out = ud_alloc(p->ctx, *n * sizeof(ud_field));
    if (!*out)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < p->key_n; i++)
        (*out)[i] = p->fields[p->keys[i]];
    for (uint32_t i = 0; i < p->agg_n; i++) {
        const ud_aggregate_def *a = p->aggs + i;
        (*out)[p->key_n + i] =
            (ud_field){a->name, (uint32_t)strlen(a->name),
                       a->kind == 0 ? UD_INT64 : p->fields[a->column].type, a->kind != 0};
        if (a->kind != 0) {
            (*out)[p->key_n + i].precision = p->fields[a->column].precision;
            (*out)[p->key_n + i].scale = p->fields[a->column].scale;
            (*out)[p->key_n + i].timezone = p->fields[a->column].timezone;
            (*out)[p->key_n + i].timezone_length = p->fields[a->column].timezone_length;
            (*out)[p->key_n + i].children = p->fields[a->column].children;
            (*out)[p->key_n + i].child_count = p->fields[a->column].child_count;
        }
    }
    return UD_OK;
}
ud_status ud_accumulator_snapshot(const ud_accumulator *a, ud_batch **out) {
    if (!a || !out || a->failed)
        return UD_INVALID;
    *out = NULL;
    ud_field *fields;
    uint32_t n;
    UD_TRY(output_fields(a->plan, &fields, &n));
    ud_status s = ud_batch_create(a->plan->ctx, fields, n, a->groups, out);
    ud_free(fields);
    for (group *g = a->head; g && !s; g = g->next)
        s = ud_batch_append(*out, g->values, n);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_accumulator_restore(ud_accumulator *a, const ud_batch *b) {
    if (!a || a->failed || a->groups > (a->plan->key_n ? 0u : 1u))
        return UD_INVALID;
    ud_field *fields;
    uint32_t n;
    UD_TRY(output_fields(a->plan, &fields, &n));
    ud_status s = UD_OK;
    if (ud_batch_columns(b) != n || (!a->plan->key_n && ud_batch_rows(b) != 1))
        s = UD_CORRUPT;
    for (uint32_t i = 0; !s && i < n; i++) {
        ud_field f;
        ud_batch_field(b, i, &f);
        if (!ud_field_equal(&f, fields + i, 1))
            s = UD_CORRUPT;
    }
    ud_free(fields);
    if (s)
        return s;
    if (a->head) {
        clear_values(a->head->values, n);
        ud_free(a->head);
        a->head = a->tail = NULL;
        a->groups = 0;
    }
    for (uint32_t r = 0; r < ud_batch_rows(b); r++) {
        group *g = ud_alloc(a->plan->ctx, sizeof(*g));
        if (!g) {
            a->failed = 1;
            return UD_RESOURCE;
        }
        g->values = ud_alloc(a->plan->ctx, n * sizeof(ud_value));
        if (!g->values) {
            ud_free(g);
            a->failed = 1;
            return UD_RESOURCE;
        }
        for (uint32_t i = 0; i < n; i++) {
            ud_value v;
            ud_batch_get(b, r, i, &v);
            s = own_value(a->plan->ctx, g->values + i, &v);
            if (s)
                break;
        }
        if (s) {
            clear_values(g->values, n);
            ud_free(g);
            a->failed = 1;
            return s;
        }
        if (a->tail)
            a->tail->next = g;
        else
            a->head = g;
        a->tail = g;
        a->groups++;
    }
    return UD_OK;
}
ud_status ud_compute_apply(const ud_plan *p, const ud_batch *b, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    UD_TRY(ud_plan_check_batch(p, b));
    if (p->agg_n) {
        ud_accumulator *a;
        UD_TRY(ud_accumulator_create(p, &a));
        ud_status s = ud_accumulator_add(a, b);
        if (!s)
            s = ud_accumulator_snapshot(a, out);
        ud_accumulator_destroy(a);
        return s;
    }
    uint32_t n = p->project_n ? p->project_n : p->n;
    ud_field *f = ud_alloc(p->ctx, n * sizeof(*f));
    ud_value *v = ud_alloc(p->ctx, n * sizeof(*v));
    if (!f || !v) {
        ud_free(f);
        ud_free(v);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < n; i++)
        f[i] = p->fields[p->project_n ? p->projection[i] : i];
    ud_status s = ud_batch_create(p->ctx, f, n, ud_batch_rows(b), out);
    for (uint32_t r = 0; !s && r < ud_batch_rows(b); r++) {
        int accept;
        s = ud_plan_accept(p, b, r, &accept);
        if (s || !accept)
            continue;
        for (uint32_t i = 0; i < n; i++)
            ud_batch_get(b, r, p->project_n ? p->projection[i] : i, v + i);
        s = ud_batch_append(*out, v, n);
    }
    ud_free(f);
    ud_free(v);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
