#include "../internal.h"
#include <math.h>
#include <unicode/ustring.h>
enum { END = 256, IDENT, NUMBER, TEXT, COL, REF, GE_T, LE_T, EQ_T, NE_T, AND_T, OR_T };
typedef struct parser {
    ud_expression *e;
    ud_string source;
    uint32_t at, start, length, depth, expression_depth;
    int token;
    ud_string text;
    ud_bytes decoded;
} parser;
static int alpha(unsigned char c) {
    return c == '_' || c > 127 || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}
static int digit(unsigned char c) {
    return c >= '0' && c <= '9';
}
static int hex(unsigned char c) {
    return digit(c)               ? c - '0'
           : c >= 'a' && c <= 'f' ? c - 'a' + 10
           : c >= 'A' && c <= 'F' ? c - 'A' + 10
                                  : -1;
}
static ud_status error(parser *p, const char *message) {
    return ud_expr_error(p->e, UD_PARSE, p->start, p->length ? p->length : 1, message);
}
static int equal(ud_string s, const char *name) {
    if (s.length != strlen(name))
        return 0;
    for (uint32_t j = 0; j < s.length; j++) {
        char c = s.data[j];
        if (c >= 'a' && c <= 'z')
            c -= 32;
        if (c != name[j])
            return 0;
    }
    return 1;
}
static ud_status unicode_escape(parser *p, uint32_t cp) {
    if (cp == 0)
        return error(p, "NUL is not permitted in SSIS string literals");
    char bytes[4];
    uint32_t n = 0;
    if (cp < 0x80)
        bytes[n++] = (char)cp;
    else if (cp < 0x800) {
        bytes[n++] = (char)(0xc0 | (cp >> 6));
        bytes[n++] = (char)(0x80 | (cp & 63));
    } else if (cp < 0x10000) {
        bytes[n++] = (char)(0xe0 | (cp >> 12));
        bytes[n++] = (char)(0x80 | ((cp >> 6) & 63));
        bytes[n++] = (char)(0x80 | (cp & 63));
    } else {
        bytes[n++] = (char)(0xf0 | (cp >> 18));
        bytes[n++] = (char)(0x80 | ((cp >> 12) & 63));
        bytes[n++] = (char)(0x80 | ((cp >> 6) & 63));
        bytes[n++] = (char)(0x80 | (cp & 63));
    }
    return ud_bytes_put(&p->decoded, bytes, n);
}
static ud_status next(parser *p) {
    p->decoded.size = 0;
    const char *s = p->source.data;
    uint32_t n = p->source.length;
    while (p->at < n &&
           (s[p->at] == ' ' || s[p->at] == '\t' || s[p->at] == '\r' || s[p->at] == '\n'))
        p->at++;
    p->start = p->at;
    p->length = 0;
    p->text = (ud_string){0};
    if (p->at == n) {
        p->token = END;
        return UD_OK;
    }
    unsigned char c = (unsigned char)s[p->at++];
    p->token = c;
    if (c == '"') {
        int closed = 0;
        while (p->at < n) {
            c = (unsigned char)s[p->at++];
            if (c == '"') {
                closed = 1;
                break;
            }
            if (c < 32)
                return error(p, "Unescaped control character in string literal");
            if (c == '\\') {
                if (p->at == n)
                    return error(p, "Unterminated string escape");
                c = (unsigned char)s[p->at++];
                const char *codes = "abfnrtv\"\\", *values = "\a\b\f\n\r\t\v\"\\";
                const char *found = strchr(codes, c);
                if (found)
                    c = (unsigned char)values[found - codes];
                else if (c == 'x') {
                    uint32_t cp = 0;
                    if (n - p->at < 4)
                        return error(p, "Unicode escape requires four hex digits");
                    for (uint32_t j = 0; j < 4; j++) {
                        int h = hex((unsigned char)s[p->at++]);
                        if (h < 0)
                            return error(p, "Invalid Unicode escape");
                        cp = cp * 16 + (uint32_t)h;
                    }
                    if (cp >= 0xd800 && cp <= 0xdbff) {
                        if (n - p->at < 6 || s[p->at] != '\\' || s[p->at + 1] != 'x')
                            return error(p, "Unpaired UTF-16 surrogate");
                        p->at += 2;
                        uint32_t low = 0;
                        for (uint32_t j = 0; j < 4; j++) {
                            int h = hex((unsigned char)s[p->at++]);
                            if (h < 0)
                                return error(p, "Invalid Unicode escape");
                            low = low * 16 + (uint32_t)h;
                        }
                        if (low < 0xdc00 || low > 0xdfff)
                            return error(p, "Unpaired UTF-16 surrogate");
                        cp = 0x10000 + (cp - 0xd800) * 1024 + low - 0xdc00;
                    } else if (cp >= 0xdc00 && cp <= 0xdfff)
                        return error(p, "Unpaired UTF-16 surrogate");
                    UD_TRY(unicode_escape(p, cp));
                    continue;
                } else
                    return error(p, "Unknown string escape");
            }
            UD_TRY(ud_bytes_put(&p->decoded, &c, 1));
        }
        if (!closed)
            return error(p, "Unterminated string literal");
        p->token = TEXT;
        p->text = (ud_string){(const char *)p->decoded.data, (uint32_t)p->decoded.size};
    } else if (c == '[' || c == '@') {
        int reference = c == '@';
        if (reference && p->at < n && s[p->at] == '[')
            p->at++;
        else if (reference) {
            uint32_t begin = p->at;
            while (p->at < n && (alpha((unsigned char)s[p->at]) || digit((unsigned char)s[p->at]) ||
                                 s[p->at] == ':'))
                p->at++;
            if (begin == p->at)
                return error(p, "Expected variable name");
            p->token = REF;
            p->text = (ud_string){s + begin, p->at - begin};
            p->length = p->at - p->start;
            return UD_OK;
        }
        int closed = 0;
        while (p->at < n) {
            c = (unsigned char)s[p->at++];
            if (c == ']') {
                if (p->at < n && s[p->at] == ']') {
                    p->at++;
                    UD_TRY(ud_bytes_put(&p->decoded, "]", 1));
                    continue;
                }
                closed = 1;
                break;
            }
            if (!c)
                return error(p, "Invalid identifier");
            UD_TRY(ud_bytes_put(&p->decoded, &c, 1));
        }
        if (!closed || !p->decoded.size)
            return error(p, "Unterminated bracketed identifier");
        p->token = reference ? REF : COL;
        p->text = (ud_string){(const char *)p->decoded.data, (uint32_t)p->decoded.size};
    } else if (alpha(c)) {
        while (p->at < n && (alpha((unsigned char)s[p->at]) || digit((unsigned char)s[p->at]) ||
                             s[p->at] == '.'))
            p->at++;
        p->token = IDENT;
        p->text = (ud_string){s + p->start, p->at - p->start};
    } else if (digit(c) || (c == '.' && p->at < n && digit((unsigned char)s[p->at]))) {
        if (c == '0' && p->at < n && (s[p->at] == 'x' || s[p->at] == 'X')) {
            p->at++;
            uint32_t begin = p->at;
            while (p->at < n && hex((unsigned char)s[p->at]) >= 0)
                p->at++;
            if (p->at == begin)
                return error(p, "Hexadecimal literal has no digits");
        } else {
            while (p->at < n && digit((unsigned char)s[p->at]))
                p->at++;
            if (p->at < n && s[p->at] == '.') {
                p->at++;
                while (p->at < n && digit((unsigned char)s[p->at]))
                    p->at++;
            }
            if (p->at < n && (s[p->at] == 'e' || s[p->at] == 'E')) {
                p->at++;
                if (p->at < n && (s[p->at] == '+' || s[p->at] == '-'))
                    p->at++;
                uint32_t begin = p->at;
                while (p->at < n && digit((unsigned char)s[p->at]))
                    p->at++;
                if (p->at == begin)
                    return error(p, "Exponent has no digits");
            }
        }
        while (p->at < n && (s[p->at] == 'u' || s[p->at] == 'U' || s[p->at] == 'l' ||
                             s[p->at] == 'L' || s[p->at] == 'f' || s[p->at] == 'F'))
            p->at++;
        p->token = NUMBER;
        p->text = (ud_string){s + p->start, p->at - p->start};
    } else if (p->at < n) {
        char z = s[p->at];
        if ((c == '=' && z == '=') || (c == '!' && z == '=') || (c == '>' && z == '=') ||
            (c == '<' && z == '=') || (c == '&' && z == '&') || (c == '|' && z == '|')) {
            p->at++;
            p->token = c == '='   ? EQ_T
                       : c == '!' ? NE_T
                       : c == '>' ? GE_T
                       : c == '<' ? LE_T
                       : c == '&' ? AND_T
                                  : OR_T;
        }
    }
    p->length = p->at - p->start;
    return UD_OK;
}
static ud_status expect(parser *p, int token) {
    if (p->token != token)
        return error(p, "Unexpected token");
    return next(p);
}
static ud_status typespec(parser *p, ud_field *field) {
    if (p->token != IDENT)
        return error(p, "Expected SSIS type name");
    ud_string name = p->text;
    uint32_t kind = 0;
    UD_TRY(ud_scalar_type_parse(name, &kind));
    UD_TRY(next(p));
    int64_t params[3] = {0};
    uint32_t count = 0;
    while (p->token == ',') {
        if (count == 3)
            return error(p, "Too many type parameters");
        UD_TRY(next(p));
        if (p->token != NUMBER)
            return error(p, "Type parameters must be integer constants");
        ud_value value;
        UD_TRY(ud_value_parse(p->e->ctx, UD_INT64, p->text.data, p->text.length, &value));
        if (value.integer < 0 || value.integer > INT32_MAX)
            return error(p, "Invalid type parameter");
        params[count++] = value.integer;
        UD_TRY(next(p));
    }
    uint32_t required = kind == UD_DT_STR || kind == UD_DT_NUMERIC ? 2
                        : kind == UD_DT_WSTR || kind == UD_DT_BYTES || kind == UD_DT_DECIMAL ||
                                kind == UD_DT_TEXT || kind == UD_DT_DBTIME2 ||
                                kind == UD_DT_DBTIMESTAMP2 || kind == UD_DT_DBTIMESTAMPOFFSET
                            ? 1
                            : 0;
    if (count != required)
        return error(p, "Wrong number of SSIS type parameters");
    uint32_t length =
        kind == UD_DT_STR || kind == UD_DT_WSTR || kind == UD_DT_BYTES ? (uint32_t)params[0] : 0;
    int32_t precision = kind == UD_DT_NUMERIC ? (int32_t)params[0] : 0,
            scale = kind == UD_DT_NUMERIC ? (int32_t)params[1]
                    : kind == UD_DT_DECIMAL || kind == UD_DT_DBTIME2 ||
                            kind == UD_DT_DBTIMESTAMP2 || kind == UD_DT_DBTIMESTAMPOFFSET
                        ? (int32_t)params[0]
                        : 0;
    uint32_t cp = kind == UD_DT_STR    ? (uint32_t)params[1]
                  : kind == UD_DT_TEXT ? (uint32_t)params[0]
                                       : 0;
    return ud_scalar_field(name, length, precision, scale, cp, 0, field);
}
static ud_status literal_number(parser *p, int negative, expression_node **out) {
    ud_string text = p->text;
    uint32_t end = text.length;
    int u = 0, l = 0, f = 0;
    int ishex = end > 2 && text.data[0] == '0' && (text.data[1] == 'x' || text.data[1] == 'X');
    while (end) {
        char c = text.data[end - 1];
        if (c == 'u' || c == 'U') {
            if (u++)
                return error(p, "Duplicate numeric suffix");
            end--;
        } else if (c == 'l' || c == 'L') {
            if (l++)
                return error(p, "Duplicate numeric suffix");
            end--;
        } else if (!ishex && (c == 'f' || c == 'F')) {
            if (f++)
                return error(p, "Duplicate numeric suffix");
            end--;
        } else
            break;
    }
    int real = !ishex && (memchr(text.data, '.', end) || memchr(text.data, 'e', end) ||
                          memchr(text.data, 'E', end));
    if (u && (negative || real || f))
        return error(p, "Invalid unsigned literal");
    uint32_t kind = real ? (f   ? UD_DT_R4
                            : l ? UD_DT_R8
                                : UD_DT_NUMERIC)
                         : (u ? (l ? UD_DT_UI8 : UD_DT_UI4) : (l ? UD_DT_I8 : UD_DT_I4));
    if (f && !real)
        kind = UD_DT_R4;
    if (kind == UD_DT_NUMERIC && (memchr(text.data, 'e', end) || memchr(text.data, 'E', end)))
        return error(p, "Scientific notation requires an F or L floating point suffix");
    ud_field field = {.name = "literal", .name_length = 7, .nullable = 0};
    ud_value value = {0};
    if (kind == UD_DT_NUMERIC) {
        uint32_t precision = 0, scale = 0;
        int fractional = 0;
        for (uint32_t j = 0; j < end; j++) {
            if (text.data[j] == '.')
                fractional = 1;
            else if (digit((unsigned char)text.data[j])) {
                precision++;
                scale += (uint32_t)fractional;
            }
        }
        if (!precision)
            precision = 1;
        UD_TRY(ud_scalar_field((ud_string){"DT_NUMERIC", 10}, 0, (int32_t)precision, (int32_t)scale,
                               0, 0, &field));
    } else
        UD_TRY(ud_scalar_field(
            (ud_string){ud_scalar_type_name(kind), (uint32_t)strlen(ud_scalar_type_name(kind))}, 0,
            0, 0, 0, 0, &field));
    field.nullable = 0;
    char number[160];
    if (end + 1 >= sizeof(number))
        return error(p, "Numeric literal is too long");
    uint32_t offset = negative ? 1 : 0;
    if (negative)
        number[0] = '-';
    memcpy(number + offset, text.data, end);
    if (ishex) {
        uint64_t n = 0;
        for (uint32_t j = 2; j < end; j++) {
            int h = hex((unsigned char)text.data[j]);
            if (h < 0 || n > (UINT64_MAX - (unsigned)h) / 16)
                return UD_OVERFLOW;
            n = n * 16 + (unsigned)h;
        }
        value.type = field.type;
        if (negative) {
            if (n > UINT64_C(0x8000000000000000))
                return UD_OVERFLOW;
            value.unsigned_integer = UINT64_C(0) - n;
        } else
            value.unsigned_integer = n;
    } else
        UD_TRY(ud_value_parse_field(p->e->ctx, &field, number, end + offset, UD_ROUND_HALF_AWAY,
                                    &value));
    expression_node *n = ud_expr_node(p->e, LITERAL, p->start, p->length + (negative ? 1 : 0));
    if (!n)
        return UD_RESOURCE;
    *out = n;
    UD_TRY(ud_expr_bind_literal(p->e, n, &field, &value));
    return next(p);
}
static ud_status expression(parser *, int, expression_node **);
static ud_status prefix(parser *p, expression_node **out) {
    *out = NULL;
    if (++p->depth > 64)
        return error(p, "Expression nesting exceeds 64");
    uint32_t start = p->start;
    ud_status s = UD_OK;
    if (p->token == NUMBER)
        s = literal_number(p, 0, out);
    else if (p->token == '+' || p->token == '-' || p->token == '!' || p->token == '~') {
        int op = p->token == '+' ? POS : p->token == '-' ? NEG : p->token == '!' ? NOT : BITNOT;
        s = next(p);
        if (!s && op == NEG && p->token == NUMBER)
            s = literal_number(p, 1, out);
        else if (!s) {
            expression_node *n = ud_expr_node(p->e, OPERATION, start, 1);
            *out = n;
            if (!n)
                s = UD_RESOURCE;
            else {
                n->op = op;
                n->argument_count = 1;
                s = prefix(p, n->args);
                if (!s) {
                    n->span_length = p->start - start;
                    s = ud_expr_finish(p->e, n);
                }
            }
        }
    } else if (p->token == '(') {
        s = next(p);
        uint32_t kind = 0;
        if (!s && p->token == IDENT && !ud_scalar_type_parse(p->text, &kind)) {
            expression_node *n = ud_expr_node(p->e, OPERATION, start, 1);
            *out = n;
            if (!n)
                s = UD_RESOURCE;
            else {
                n->op = CAST;
                n->argument_count = 1;
                s = typespec(p, &n->field);
                if (!s)
                    s = expect(p, ')');
                if (!s)
                    s = prefix(p, n->args);
                if (!s) {
                    n->span_length = p->start - start;
                    s = ud_expr_finish(p->e, n);
                }
            }
        } else if (!s) {
            s = expression(p, 1, out);
            if (!s)
                s = expect(p, ')');
        }
    } else if (p->token == TEXT) {
        ud_field f = {.type = UD_UTF8, .nullable = 0, .scalar = {.kind = UD_DT_WSTR}};
        UErrorCode error_code = U_ZERO_ERROR;
        int32_t units = 0;
        u_strFromUTF8(NULL, 0, &units, p->text.data, (int32_t)p->text.length, &error_code);
        f.scalar.length = (uint32_t)(units ? units : 1);
        ud_value value = {.type = UD_UTF8, .text = p->text.data, .text_length = p->text.length};
        expression_node *n = ud_expr_node(p->e, LITERAL, start, p->length);
        *out = n;
        s = n ? ud_expr_bind_literal(p->e, n, &f, &value) : UD_RESOURCE;
        if (!s)
            s = next(p);
    } else if (p->token == COL || p->token == REF) {
        expression_node *n =
            ud_expr_node(p->e, p->token == COL ? COLUMN : REFERENCE, start, p->length);
        *out = n;
        s = n ? ud_expr_bind_reference(p->e, n, p->text, n->kind) : UD_RESOURCE;
        if (!s)
            s = next(p);
    } else if (p->token == IDENT) {
        if (equal(p->text, "TRUE") || equal(p->text, "FALSE")) {
            ud_field f = {.type = UD_BOOL, .scalar = {.kind = UD_DT_BOOL}};
            ud_value v = {.type = UD_BOOL, .integer = equal(p->text, "TRUE")};
            expression_node *n = ud_expr_node(p->e, LITERAL, start, p->length);
            *out = n;
            s = n ? ud_expr_bind_literal(p->e, n, &f, &v) : UD_RESOURCE;
            if (!s)
                s = next(p);
        } else {
            ud_string name = {ud_strdup_n(p->e->ctx, p->text.data, p->text.length), p->text.length};
            if (!name.data)
                s = UD_RESOURCE;
            else
                s = next(p);
            if (!s && p->token == '(') {
                s = next(p);
                if (!s && equal(name, "NULL")) {
                    ud_field f = {0};
                    s = typespec(p, &f);
                    if (!s)
                        s = expect(p, ')');
                    if (!s) {
                        expression_node *n = ud_expr_node(p->e, LITERAL, start, p->start - start);
                        *out = n;
                        ud_value v = {.type = f.type,
                                      .is_null = 1,
                                      .precision = f.precision,
                                      .scale = f.scale};
                        s = n ? ud_expr_bind_literal(p->e, n, &f, &v) : UD_RESOURCE;
                    }
                } else if (!s) {
                    expression_node *n = ud_expr_node(p->e, FUNCTION, start, 1);
                    *out = n;
                    if (!n)
                        s = UD_RESOURCE;
                    else {
                        n->name = name;
                        n->name_owned = 1;
                        name.data = NULL;
                        if (p->token != ')')
                            for (;;) {
                                if (n->argument_count == 16) {
                                    s = error(p, "At most 16 function arguments");
                                    break;
                                }
                                s = expression(p, 1, n->args + n->argument_count++);
                                if (s || p->token != ',')
                                    break;
                                s = next(p);
                                if (s)
                                    break;
                            }
                        if (!s)
                            s = expect(p, ')');
                        if (!s) {
                            n->span_length = p->start - start;
                            s = ud_expr_finish(p->e, n);
                        }
                    }
                }
            } else if (!s) {
                expression_node *n = ud_expr_node(p->e, COLUMN, start, name.length);
                *out = n;
                s = n ? ud_expr_bind_reference(p->e, n, name, COLUMN) : UD_RESOURCE;
            }
            ud_free((void *)name.data);
        }
    } else
        s = error(p, "Expected literal, input, variable, function or parenthesized expression");
    p->depth--;
    return s;
}
static int precedence(int t, int *op) {
    switch (t) {
    case '*':
        *op = MUL;
        return 12;
    case '/':
        *op = DIV;
        return 12;
    case '%':
        *op = MOD;
        return 12;
    case '+':
        *op = ADD;
        return 11;
    case '-':
        *op = SUB;
        return 11;
    case '<':
        *op = LT;
        return 10;
    case '>':
        *op = GT;
        return 10;
    case LE_T:
        *op = LE;
        return 10;
    case GE_T:
        *op = GE;
        return 10;
    case EQ_T:
        *op = EQ;
        return 9;
    case NE_T:
        *op = NE;
        return 9;
    case '&':
        *op = BITAND;
        return 8;
    case '^':
        *op = BITXOR;
        return 7;
    case '|':
        *op = BITOR;
        return 6;
    case AND_T:
        *op = AND;
        return 5;
    case OR_T:
        *op = OR;
        return 4;
    case '?':
        *op = CONDITIONAL;
        return 3;
    default:
        return 0;
    }
}
static ud_status expression_inner(parser *p, int minimum, expression_node **out) {
    UD_TRY(prefix(p, out));
    for (;;) {
        int op = 0, level = precedence(p->token, &op);
        if (level < minimum || !level)
            break;
        expression_node *n = ud_expr_node(p->e, OPERATION, (*out)->span_start, 1);
        if (!n)
            return UD_RESOURCE;
        n->op = op;
        n->args[0] = *out;
        *out = n;
        n->argument_count = op == CONDITIONAL ? 3 : 2;
        UD_TRY(next(p));
        UD_TRY(expression(p, op == CONDITIONAL ? 1 : level + 1, n->args + 1));
        if (op == CONDITIONAL) {
            UD_TRY(expect(p, ':'));
            UD_TRY(expression(p, level, n->args + 2));
        }
        n->span_length = p->start - n->span_start;
        UD_TRY(ud_expr_finish(p->e, n));
    }
    return UD_OK;
}
static ud_status expression(parser *p, int minimum, expression_node **out) {
    if (++p->expression_depth > 64) {
        p->expression_depth--;
        return error(p, "Expression nesting exceeds 64");
    }
    ud_status status = expression_inner(p, minimum, out);
    p->expression_depth--;
    return status;
}
ud_status ud_expr_parse(ud_expression *e, ud_string source, expression_node **out) {
    parser p = {.e = e, .source = source, .decoded = {.ctx = e->ctx}};
    ud_status s = next(&p);
    if (!s)
        s = expression(&p, 1, out);
    if (!s && p.token != END)
        s = error(&p, "Unexpected trailing token");
    if (s && !e->diagnostic.status)
        ud_expr_error(e, s, p.start, p.length, "Formula static binding failed");
    ud_bytes_clear(&p.decoded);
    return s;
}
