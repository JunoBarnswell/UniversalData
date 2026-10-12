#include "ud/regex.h"
#include "../internal.h"
enum {
    RX_EPS = 1,
    RX_CHAR,
    RX_DOT,
    RX_CLASS,
    RX_BEGIN,
    RX_END,
    RX_CAT,
    RX_ALT,
    RX_STAR,
    RX_PLUS,
    RX_OPT,
    RX_SPLIT,
    RX_MATCH
};
typedef struct rx_node {
    int kind, left, right;
    uint32_t cp, range[64], ranges;
    int inverted;
} rx_node;
typedef struct rx_instruction {
    int kind, next, other;
    uint32_t cp, range[64], ranges;
    int inverted;
} rx_instruction;
struct ud_regex {
    ud_context *ctx;
    rx_instruction *code;
    uint32_t count, capacity;
    int start;
};
typedef struct rx_parser {
    const char *text;
    uint32_t size, pos, count, capacity;
    rx_node *nodes;
    ud_status status;
} rx_parser;
/* Called only after strict UTF8 validation. */
static uint32_t next_cp(const char *text, uint32_t *pos) {
    unsigned char c = (unsigned char)text[(*pos)++];
    if (c < 128)
        return c;
    uint32_t cp = c < 224 ? c & 31 : c < 240 ? c & 15 : c & 7;
    uint32_t n = c < 224 ? 1 : c < 240 ? 2 : 3;
    while (n--)
        cp = (cp << 6) | ((unsigned char)text[(*pos)++] & 63);
    return cp;
}
static int new_node(rx_parser *p, int kind, int left, int right) {
    if (p->count == p->capacity) {
        p->status = UD_RESOURCE;
        return -1;
    }
    int at = (int)p->count++;
    p->nodes[at].kind = kind;
    p->nodes[at].left = left;
    p->nodes[at].right = right;
    return at;
}
static void range(rx_parser *p, rx_node *n, uint32_t first, uint32_t last) {
    if (first > last) {
        p->status = UD_PARSE;
        return;
    }
    if (n->ranges == 32) {
        p->status = UD_RESOURCE;
        return;
    }
    n->range[2 * n->ranges] = first;
    n->range[2 * n->ranges + 1] = last;
    n->ranges++;
}
static int shorthand(rx_parser *p, rx_node *n, uint32_t c, int in_class) {
    if (c != 'd' && c != 'D' && c != 'w' && c != 'W' && c != 's' && c != 'S')
        return 0;
    int inverse = c == 'D' || c == 'W' || c == 'S';
    if (in_class && inverse) {
        p->status = UD_UNSUPPORTED;
        return 1;
    }
    n->kind = RX_CLASS;
    if (!in_class)
        n->inverted = inverse;
    if (c == 'd' || c == 'D' || c == 'w' || c == 'W')
        range(p, n, '0', '9');
    if (c == 'w' || c == 'W') {
        range(p, n, 'A', 'Z');
        range(p, n, 'a', 'z');
        range(p, n, '_', '_');
    }
    if (c == 's' || c == 'S') {
        range(p, n, 9, 13);
        range(p, n, ' ', ' ');
    }
    return 1;
}
static uint32_t escaped(rx_parser *p) {
    if (p->pos == p->size) {
        p->status = UD_PARSE;
        return 0;
    }
    uint32_t c = next_cp(p->text, &p->pos);
    if (c == 'n')
        return '\n';
    if (c == 'r')
        return '\r';
    if (c == 't')
        return '\t';
    if (c == 'f')
        return '\f';
    if (c == 'v')
        return '\v';
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
        p->status = UD_UNSUPPORTED;
    return c;
}
static int expression(rx_parser *, uint32_t);
static int atom(rx_parser *p, uint32_t depth) {
    if (p->pos == p->size) {
        p->status = UD_PARSE;
        return -1;
    }
    uint32_t c = next_cp(p->text, &p->pos);
    int at = -1;
    if (c == '(') {
        if (depth == 64) {
            p->status = UD_RESOURCE;
            return -1;
        }
        if (p->pos < p->size && p->text[p->pos] == '?') {
            p->status = UD_UNSUPPORTED;
            return -1;
        }
        at = expression(p, depth + 1);
        if (!p->status && (p->pos == p->size || p->text[p->pos++] != ')'))
            p->status = UD_PARSE;
        return at;
    }
    if (c == '*' || c == '+' || c == '?' || c == ')') {
        p->status = UD_PARSE;
        return -1;
    }
    if (c == '{' || c == '}') {
        p->status = UD_UNSUPPORTED;
        return -1;
    }
    at = new_node(p,
                  c == '.'   ? RX_DOT
                  : c == '^' ? RX_BEGIN
                  : c == '$' ? RX_END
                  : c == '[' ? RX_CLASS
                             : RX_CHAR,
                  -1, -1);
    if (at < 0)
        return at;
    rx_node *n = p->nodes + at;
    if (c == '[') {
        if (p->pos < p->size && p->text[p->pos] == '^') {
            n->inverted = 1;
            p->pos++;
        }
        int complete = 0;
        while (!p->status && p->pos < p->size) {
            uint32_t start = next_cp(p->text, &p->pos);
            if (start == ']') {
                complete = 1;
                break;
            }
            if (start == '\\') {
                if (p->pos == p->size) {
                    p->status = UD_PARSE;
                    break;
                }
                uint32_t save = p->pos, code = next_cp(p->text, &p->pos);
                if (shorthand(p, n, code, 1)) {
                    if (p->pos + 1 < p->size && p->text[p->pos] == '-' &&
                        p->text[p->pos + 1] != ']')
                        p->status = UD_PARSE;
                    continue;
                }
                p->pos = save;
                start = escaped(p);
            }
            uint32_t end = start;
            if (!p->status && p->pos + 1 < p->size && p->text[p->pos] == '-' &&
                p->text[p->pos + 1] != ']') {
                p->pos++;
                end = next_cp(p->text, &p->pos);
                if (end == '\\') {
                    if (p->pos < p->size && p->text[p->pos] && strchr("dDwWsS", p->text[p->pos])) {
                        p->status = UD_PARSE;
                        break;
                    }
                    end = escaped(p);
                }
            }
            if (!p->status)
                range(p, n, start, end);
        }
        if (!p->status && (!complete || !n->ranges))
            p->status = UD_PARSE;
    } else if (c == '\\') {
        if (p->pos == p->size)
            p->status = UD_PARSE;
        else {
            uint32_t save = p->pos, code = next_cp(p->text, &p->pos);
            if (!shorthand(p, n, code, 0)) {
                p->pos = save;
                n->cp = escaped(p);
            }
        }
    } else
        n->cp = c;
    return at;
}
static int sequence(rx_parser *p, uint32_t depth) {
    int result = -1;
    while (!p->status && p->pos < p->size && p->text[p->pos] != '|' && p->text[p->pos] != ')') {
        int part = atom(p, depth);
        if (p->status)
            break;
        if (p->pos < p->size && p->text[p->pos] && strchr("*+?", p->text[p->pos])) {
            char c = p->text[p->pos++];
            part = new_node(p, c == '*' ? RX_STAR : c == '+' ? RX_PLUS : RX_OPT, part, -1);
            if (p->pos < p->size && p->text[p->pos] && strchr("*+?", p->text[p->pos]))
                p->status = UD_UNSUPPORTED;
        }
        if (!p->status)
            result = result < 0 ? part : new_node(p, RX_CAT, result, part);
    }
    return !p->status && result < 0 ? new_node(p, RX_EPS, -1, -1) : result;
}
static int expression(rx_parser *p, uint32_t depth) {
    int result = sequence(p, depth);
    while (!p->status && p->pos < p->size && p->text[p->pos] == '|') {
        p->pos++;
        int right = sequence(p, depth);
        if (!p->status)
            result = new_node(p, RX_ALT, result, right);
    }
    return result;
}
static int instruction(ud_regex *r, int kind, int next, int other) {
    if (r->count == r->capacity)
        return -1;
    int at = (int)r->count++;
    r->code[at].kind = kind;
    r->code[at].next = next;
    r->code[at].other = other;
    return at;
}
/* Compilation walks the AST iteratively to bound C stack independent of input. */
typedef struct rx_frame {
    int node, continuation, stage, first, instruction;
} rx_frame;
static ud_status compile_ast(ud_regex *r, const rx_node *nodes, uint32_t count, int root) {
    rx_frame *stack = ud_alloc(r->ctx, (count + 1) * sizeof(*stack));
    if (!stack)
        return UD_RESOURCE;
    uint32_t size = 1;
    stack[0] = (rx_frame){root, 0, 0, 0, 0};
    int result = 0;
    ud_status s = UD_OK;
    while (size && !s) {
        rx_frame *f = stack + size - 1;
        const rx_node *n = nodes + f->node;
        if (f->stage == 0) {
            if (n->kind == RX_CAT) {
                f->stage = 1;
                stack[size++] = (rx_frame){n->right, f->continuation, 0, 0, 0};
            } else if (n->kind == RX_ALT) {
                f->stage = 1;
                stack[size++] = (rx_frame){n->left, f->continuation, 0, 0, 0};
            } else if (n->kind == RX_STAR || n->kind == RX_PLUS) {
                f->instruction = instruction(r, RX_SPLIT, -1, f->continuation);
                if (f->instruction < 0)
                    s = UD_RESOURCE;
                else {
                    f->stage = 1;
                    stack[size++] = (rx_frame){n->left, f->instruction, 0, 0, 0};
                }
            } else if (n->kind == RX_OPT) {
                f->stage = 1;
                stack[size++] = (rx_frame){n->left, f->continuation, 0, 0, 0};
            } else {
                result = n->kind == RX_EPS ? f->continuation
                                           : instruction(r, n->kind, f->continuation, -1);
                if (result < 0)
                    s = UD_RESOURCE;
                else if (n->kind != RX_EPS) {
                    r->code[result].cp = n->cp;
                    r->code[result].ranges = n->ranges;
                    r->code[result].inverted = n->inverted;
                    memcpy(r->code[result].range, n->range, sizeof(n->range));
                }
                size--;
            }
        } else if (n->kind == RX_CAT) {
            if (f->stage == 1) {
                f->stage = 2;
                stack[size++] = (rx_frame){n->left, result, 0, 0, 0};
            } else
                size--;
        } else if (n->kind == RX_ALT) {
            if (f->stage == 1) {
                f->stage = 2;
                f->first = result;
                stack[size++] = (rx_frame){n->right, f->continuation, 0, 0, 0};
            } else {
                result = instruction(r, RX_SPLIT, f->first, result);
                if (result < 0)
                    s = UD_RESOURCE;
                size--;
            }
        } else if (n->kind == RX_STAR || n->kind == RX_PLUS) {
            r->code[f->instruction].next = result;
            if (n->kind == RX_STAR)
                result = f->instruction;
            size--;
        } else {
            result = instruction(r, RX_SPLIT, result, f->continuation);
            if (result < 0)
                s = UD_RESOURCE;
            size--;
        }
    }
    if (!s)
        r->start = result;
    ud_free(stack);
    return s;
}
void ud_regex_destroy(ud_regex *r) {
    if (r) {
        ud_free(r->code);
        ud_free(r);
    }
}
ud_status ud_regex_compile(ud_context *ctx, const char *text, uint32_t length, ud_regex **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || (!text && length))
        return UD_INVALID;
    if (ud_is_cancelled(ctx))
        return UD_CANCELLED;
    if (length > 4096)
        return UD_RESOURCE;
    if (!ud_utf8_valid(text, length))
        return UD_PARSE;
    uint32_t capacity = length * 2 + 1;
    if (capacity > 8192)
        capacity = 8192;
    rx_node *nodes = ud_alloc(ctx, capacity * sizeof(*nodes));
    if (!nodes)
        return UD_RESOURCE;
    rx_parser p = {text, length, 0, 0, capacity, nodes, UD_OK};
    int root = expression(&p, 0);
    if (!p.status && p.pos != p.size)
        p.status = UD_PARSE;
    ud_regex *r = NULL;
    if (!p.status) {
        r = ud_alloc(ctx, sizeof(*r));
        if (!r)
            p.status = UD_RESOURCE;
    }
    if (!p.status) {
        r->ctx = ctx;
        r->capacity = p.count + 1;
        r->code = ud_alloc(ctx, r->capacity * sizeof(*r->code));
        if (!r->code)
            p.status = UD_RESOURCE;
        else {
            instruction(r, RX_MATCH, -1, -1);
            p.status = compile_ast(r, nodes, p.count, root);
        }
    }
    ud_free(nodes);
    if (p.status)
        ud_regex_destroy(r);
    else
        *out = r;
    return p.status;
}
typedef struct rx_set {
    uint8_t *seen;
    int *active, *stack;
    uint32_t count;
} rx_set;
static void closure(const ud_regex *r, rx_set *set, int start, uint32_t pos, uint32_t length) {
    if (set->seen[start])
        return;
    uint32_t size = 0;
    set->seen[start] = 1;
    set->stack[size++] = start;
    while (size) {
        int at = set->stack[--size];
        const rx_instruction *code = r->code + at;
        if (code->kind != RX_SPLIT && code->kind != RX_BEGIN && code->kind != RX_END) {
            set->active[set->count++] = at;
            continue;
        }
        if ((code->kind == RX_BEGIN && pos != 0) || (code->kind == RX_END && pos != length))
            continue;
        if (!set->seen[code->next]) {
            set->seen[code->next] = 1;
            set->stack[size++] = code->next;
        }
        if (code->kind == RX_SPLIT && !set->seen[code->other]) {
            set->seen[code->other] = 1;
            set->stack[size++] = code->other;
        }
    }
}
ud_status ud_regex_test(const ud_regex *r, const char *text, uint32_t length, int32_t *matched) {
    if (!r || !matched || (!text && length))
        return UD_INVALID;
    *matched = 0;
    if (!ud_utf8_valid(text, length))
        return UD_PARSE;
    size_t bytes = (size_t)r->count;
    uint8_t *seen = ud_alloc(r->ctx, bytes * 2);
    int *indices = ud_alloc(r->ctx, bytes * 4 * sizeof(*indices));
    if (!seen || !indices) {
        ud_free(seen);
        ud_free(indices);
        return UD_RESOURCE;
    }
    rx_set current = {seen, indices, indices + 2 * r->count, 0},
           next = {seen + r->count, indices + r->count, indices + 3 * r->count, 0};
    uint32_t pos = 0;
    ud_status s = UD_OK;
    for (;;) {
        if (ud_is_cancelled(r->ctx)) {
            s = UD_CANCELLED;
            break;
        }
        closure(r, &current, r->start, pos, length);
        for (uint32_t i = 0; i < current.count; i++)
            if (r->code[current.active[i]].kind == RX_MATCH) {
                *matched = 1;
                break;
            }
        if (*matched || pos == length)
            break;
        uint32_t cp = next_cp(text, &pos);
        next.count = 0;
        memset(next.seen, 0, bytes);
        for (uint32_t i = 0; i < current.count; i++) {
            const rx_instruction *code = r->code + current.active[i];
            int accepts = code->kind == RX_CHAR  ? code->cp == cp
                          : code->kind == RX_DOT ? cp != '\n'
                                                 : 0;
            if (code->kind == RX_CLASS) {
                for (uint32_t j = 0; j < code->ranges && !accepts; j++)
                    accepts = cp >= code->range[2 * j] && cp <= code->range[2 * j + 1];
                if (code->inverted)
                    accepts = !accepts;
            }
            if (accepts)
                closure(r, &next, code->next, pos, length);
        }
        rx_set swap = current;
        current = next;
        next = swap;
    }
    ud_free(seen);
    ud_free(indices);
    return s;
}