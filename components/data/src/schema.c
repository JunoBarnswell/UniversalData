#include "../internal.h"
#define MAX_LINEAGE 4096u
static int id_empty(ud_column_id id) {
    return !id.high && !id.low;
}
int32_t ud_column_id_compare(ud_column_id a, ud_column_id b) {
    if (a.high != b.high)
        return a.high > b.high ? 1 : -1;
    return (a.low > b.low) - (a.low < b.low);
}
void ud_column_id_format(ud_column_id id, char out[33]) {
    static const char hex[] = "0123456789abcdef";
    for (uint32_t j = 0; j < 16; j++) {
        uint64_t value = j < 8 ? id.high : id.low;
        uint32_t shift = (7 - (j & 7)) * 8;
        uint8_t byte = (uint8_t)(value >> shift);
        out[j * 2] = hex[byte >> 4];
        out[j * 2 + 1] = hex[byte & 15];
    }
    out[32] = 0;
}
ud_status ud_column_id_parse(ud_string text, ud_column_id *out) {
    if (!out)
        return UD_INVALID;
    *out = (ud_column_id){0};
    if (!text.data || text.length != 32)
        return UD_SCHEMA;
    for (uint32_t j = 0; j < 32; j++) {
        unsigned char ch = (unsigned char)text.data[j];
        unsigned digit = ch >= '0' && ch <= '9'   ? ch - '0'
                         : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
                                                  : 16;
        if (digit == 16) {
            *out = (ud_column_id){0};
            return UD_SCHEMA;
        }
        uint64_t *word = j < 16 ? &out->high : &out->low;
        *word = (*word << 4) | digit;
    }
    return id_empty(*out) ? UD_SCHEMA : UD_OK;
}
ud_status ud_column_id_make(ud_context *ctx, ud_string scope, ud_string name, ud_column_id *out) {
    if (!out)
        return UD_INVALID;
    *out = (ud_column_id){0};
    if (!ctx || !scope.data || !scope.length || scope.length > 1048576 || !name.data ||
        !name.length || name.length > 65536 || memchr(scope.data, 0, scope.length) ||
        memchr(name.data, 0, name.length) || !ud_utf8_valid(scope.data, scope.length) ||
        !ud_utf8_valid(name.data, name.length))
        return UD_SCHEMA;
    ud_bytes bytes = {.ctx = ctx};
    ud_status s = ud_bytes_put(&bytes, "UD.ColumnIdentity.4", 19);
    if (!s)
        s = ud_put_string(&bytes, scope.data, scope.length);
    if (!s)
        s = ud_put_string(&bytes, name.data, name.length);
    uint8_t digest[32];
    if (!s) {
        ud_hash(bytes.data, bytes.size, digest);
        for (uint32_t j = 0; j < 8; j++) {
            out->high = (out->high << 8) | digest[j];
            out->low = (out->low << 8) | digest[8 + j];
        }
        if (id_empty(*out))
            s = UD_CORRUPT;
    }
    ud_bytes_clear(&bytes);
    return s;
}
static int order_id(const void *a, const void *b) {
    return ud_column_id_compare(*(const ud_column_id *)a, *(const ud_column_id *)b);
}
static ud_status lineage_copy(ud_context *ctx, const ud_column_id *input, uint32_t count,
                              ud_column_id **out, uint32_t *size) {
    *out = NULL;
    *size = 0;
    if (count > MAX_LINEAGE || (count && !input))
        return UD_SCHEMA;
    if (!count)
        return UD_OK;
    ud_column_id *ids = ud_alloc(ctx, count * sizeof(*ids));
    if (!ids)
        return UD_RESOURCE;
    memcpy(ids, input, count * sizeof(*ids));
    qsort(ids, count, sizeof(*ids), order_id);
    for (uint32_t j = 0; j < count; j++) {
        if (id_empty(ids[j])) {
            ud_free(ids);
            return UD_SCHEMA;
        }
        if (!*size || ud_column_id_compare(ids[j], ids[*size - 1]))
            ids[(*size)++] = ids[j];
    }
    *out = ids;
    return UD_OK;
}
ud_status ud_lineage_merge(ud_context *ctx, const ud_field *fields, uint32_t count,
                           ud_column_id **out, uint32_t *size) {
    if (!out || !size || !ctx || (count && !fields))
        return UD_INVALID;
    *out = NULL;
    *size = 0;
    uint64_t total = 0;
    for (uint32_t j = 0; j < count; j++) {
        if (fields[j].lineage_count > MAX_LINEAGE ||
            (fields[j].lineage_count && !fields[j].lineage))
            return UD_SCHEMA;
        total += fields[j].lineage_count;
    }
    if (!total)
        return UD_OK;
    uint32_t capacity = (uint32_t)(total > MAX_LINEAGE ? MAX_LINEAGE : total);
    ud_column_id *all = ud_alloc(ctx, (size_t)capacity * sizeof(*all));
    if (!all)
        return UD_RESOURCE;
    uint32_t at = 0;
    ud_status s = UD_OK;
    for (uint32_t j = 0; j < count; j++) {
        for (uint32_t k = 0; k < fields[j].lineage_count; k++) {
            ud_column_id id = fields[j].lineage[k];
            if (id_empty(id)) {
                s = UD_SCHEMA;
                break;
            }
            uint32_t lo = 0, hi = at;
            while (lo < hi) {
                uint32_t mid = lo + (hi - lo) / 2;
                if (ud_column_id_compare(all[mid], id) < 0)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            if (lo < at && !ud_column_id_compare(all[lo], id))
                continue;
            if (at == capacity) {
                s = UD_RESOURCE;
                break;
            }
            memmove(all + lo + 1, all + lo, (at - lo) * sizeof(*all));
            all[lo] = id;
            at++;
        }
        if (s)
            break;
    }
    if (s) {
        ud_free(all);
        return s;
    }
    *out = all;
    *size = at;
    return UD_OK;
}
void ud_field_clear(ud_field *f) {
    if (!f)
        return;
    ud_free((void *)f->name);
    ud_free((void *)f->timezone);
    ud_free((void *)f->lineage);
    for (uint32_t j = 0; j < f->child_count; j++)
        ud_field_clear((ud_field *)f->children + j);
    ud_free((void *)f->children);
    memset(f, 0, sizeof(*f));
}
static ud_status copy_field(ud_context *ctx, const ud_field *f, ud_column_id parent, ud_field *out,
                            uint32_t depth) {
    memset(out, 0, sizeof(*out));
    if (depth > 32 || !f || !f->name || !f->name_length || f->name_length > 65536 ||
        memchr(f->name, 0, f->name_length) || !ud_utf8_valid(f->name, f->name_length) ||
        f->type < UD_BOOL || f->type > UD_MAP || (f->nullable != 0 && f->nullable != 1) ||
        (f->type == UD_DECIMAL128 &&
         (f->precision < 1 || f->precision > 38 || f->scale < -38 || f->scale > 38)) ||
        (f->type != UD_DECIMAL128 && (f->precision || f->scale)) ||
        (f->type == UD_TIMESTAMP &&
         (!f->timezone || !f->timezone_length || f->timezone_length > 65536 ||
          memchr(f->timezone, 0, f->timezone_length) ||
          !ud_utf8_valid(f->timezone, f->timezone_length))) ||
        (f->type != UD_TIMESTAMP && f->timezone_length) ||
        (f->type < UD_STRUCT && f->child_count) ||
        (f->type >= UD_STRUCT && (!f->children || !f->child_count || f->child_count > 65536)) ||
        (f->type >= UD_LIST && f->child_count != 1) ||
        (f->type == UD_MAP && (f->children[0].type != UD_STRUCT || f->children[0].nullable ||
                               f->children[0].child_count != 2 || !f->children[0].children ||
                               f->children[0].children[0].nullable)))
        return UD_SCHEMA;
    *out = *f;
    out->name = NULL;
    out->timezone = NULL;
    out->children = NULL;
    out->lineage = NULL;
    out->child_count = 0;
    out->lineage_count = 0;
    out->name = ud_strdup_n(ctx, f->name, f->name_length);
    if (!out->name)
        return UD_RESOURCE;
    if (f->timezone_length) {
        out->timezone = ud_strdup_n(ctx, f->timezone, f->timezone_length);
        if (!out->timezone)
            return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    if (id_empty(f->identity)) {
        if (f->lineage_count)
            return UD_SCHEMA; /* Origins cannot bind an anonymous result. */
        char scope[33];
        ud_column_id_format(parent, scope);
        s = ud_column_id_make(ctx, (ud_string){scope, 32}, (ud_string){f->name, f->name_length},
                              &out->identity);
        if (!s)
            s = lineage_copy(ctx, &out->identity, 1, (ud_column_id **)&out->lineage,
                             &out->lineage_count);
    } else
        s = lineage_copy(ctx, f->lineage, f->lineage_count, (ud_column_id **)&out->lineage,
                         &out->lineage_count);
    if (s)
        return s;
    if (f->child_count) {
        ud_field *children = ud_alloc(ctx, f->child_count * sizeof(*children));
        if (!children)
            return UD_RESOURCE;
        out->children = children;
        out->child_count = f->child_count;
        for (uint32_t j = 0; j < f->child_count; j++) {
            s = copy_field(ctx, f->children + j, out->identity, children + j, depth + 1);
            if (s)
                return s;
            for (uint32_t prior = 0; prior < j; prior++)
                if (!ud_column_id_compare(children[j].identity, children[prior].identity) ||
                    (children[j].name_length == children[prior].name_length &&
                     !memcmp(children[j].name, children[prior].name, children[j].name_length)))
                    return UD_SCHEMA;
        }
    }
    return UD_OK;
}
ud_status ud_field_copy(ud_context *ctx, const ud_field *f, ud_column_id parent, ud_field *out) {
    if (!ctx || !f || !out)
        return UD_INVALID;
    ud_status s = copy_field(ctx, f, parent, out, 0);
    if (s)
        ud_field_clear(out);
    return s;
}
static ud_status derive_field(ud_context *ctx, const ud_field *source, ud_string scope,
                              ud_string name, ud_field *out, uint32_t depth) {
    memset(out, 0, sizeof(*out));
    if (depth > 32)
        return UD_SCHEMA;
    ud_field spec = *source;
    spec.name = name.data;
    spec.name_length = name.length;
    UD_TRY(ud_column_id_make(ctx, scope, name, &spec.identity));
    UD_TRY(ud_field_copy(ctx, &spec, (ud_column_id){0}, out));
    char child_scope[33];
    ud_column_id_format(out->identity, child_scope);
    for (uint32_t j = 0; j < source->child_count; j++) {
        ud_field replacement = {0};
        const ud_field *child = source->children + j;
        ud_status s =
            derive_field(ctx, child, (ud_string){child_scope, 32},
                         (ud_string){child->name, child->name_length}, &replacement, depth + 1);
        if (s) {
            ud_field_clear(&replacement);
            return s;
        }
        ud_field_clear((ud_field *)out->children + j);
        ((ud_field *)out->children)[j] = replacement;
    }
    return UD_OK;
}
ud_status ud_field_derive(ud_context *ctx, const ud_field *source, ud_string scope, ud_string name,
                          ud_field *out) {
    if (!ctx || !source || !out)
        return UD_INVALID;
    ud_status s = derive_field(ctx, source, scope, name, out, 0);
    if (s)
        ud_field_clear(out);
    return s;
}
int32_t ud_field_matches(const ud_field *a, const ud_field *b, int32_t mode) {
    if (!a || !b || mode < UD_FIELD_VALUE_TYPE || mode > UD_FIELD_LAYOUT || a->type != b->type ||
        a->precision != b->precision || a->scale != b->scale || a->child_count != b->child_count)
        return 0;
    if (mode != UD_FIELD_VALUE_TYPE &&
        (a->timezone_length != b->timezone_length ||
         (a->timezone_length && memcmp(a->timezone, b->timezone, a->timezone_length))))
        return 0;
    if (mode != UD_FIELD_VALUE_TYPE &&
        (a->nullable != b->nullable || a->name_length != b->name_length ||
         (a->name_length && memcmp(a->name, b->name, a->name_length))))
        return 0;
    if (mode == UD_FIELD_PORT) {
        if (ud_column_id_compare(a->identity, b->identity) || a->lineage_count != b->lineage_count)
            return 0;
        for (uint32_t j = 0; j < a->lineage_count; j++)
            if (ud_column_id_compare(a->lineage[j], b->lineage[j]))
                return 0;
    }
    for (uint32_t j = 0; j < a->child_count; j++) {
        if (mode == UD_FIELD_VALUE_TYPE &&
            (a->children[j].name_length != b->children[j].name_length ||
             memcmp(a->children[j].name, b->children[j].name, a->children[j].name_length)))
            return 0;
        if (!ud_field_matches(a->children + j, b->children + j, mode))
            return 0;
    }
    return 1;
}
static ud_status cast_metadata(ud_context *ctx, const ud_field *source, const ud_field *target,
                               ud_field *out, uint32_t depth) {
    if (depth > 32 || !target->name || !target->name_length)
        return UD_SCHEMA;
    ud_field spec = *target;
    spec.lineage = source->lineage;
    spec.lineage_count = source->lineage_count;
    ud_status s = UD_OK;
    if (!id_empty(target->identity))
        s = ud_field_copy(ctx, &spec, (ud_column_id){0}, out);
    else if (ud_field_matches(source, target, UD_FIELD_VALUE_TYPE)) {
        spec.identity = source->identity;
        s = ud_field_copy(ctx, &spec, (ud_column_id){0}, out);
    } else {
        char id[33], scope[40];
        ud_column_id_format(source->identity, id);
        snprintf(scope, sizeof(scope), "cast.%s", id);
        s = ud_field_derive(ctx, &spec, (ud_string){scope, (uint32_t)strlen(scope)},
                            (ud_string){target->name, target->name_length}, out);
    }
    if (!s && target->type >= UD_STRUCT) {
        if (source->type != target->type || source->child_count != target->child_count)
            s = UD_SCHEMA;
        for (uint32_t j = 0; !s && j < target->child_count; j++) {
            ud_field child = {0};
            s = cast_metadata(ctx, source->children + j, target->children + j, &child, depth + 1);
            if (!s) {
                ud_field_clear((ud_field *)out->children + j);
                ((ud_field *)out->children)[j] = child;
            } else
                ud_field_clear(&child);
        }
    }
    return s;
}
ud_status ud_field_cast_metadata(ud_context *ctx, const ud_field *source, const ud_field *target,
                                 ud_field *out) {
    if (!ctx || !source || !target || !out)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    ud_status s = cast_metadata(ctx, source, target, out, 0);
    if (s)
        ud_field_clear(out);
    return s;
}
ud_status ud_schema_owner_create(ud_context *ctx, const ud_field *fields, uint32_t columns,
                                 ud_string scope, ud_schema_owner **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !fields || !columns || columns > 65536)
        return UD_INVALID;
    if (!scope.length)
        scope = (ud_string){"UD.Schema.4", 11};
    ud_column_id parent;
    UD_TRY(ud_column_id_make(ctx, scope, (ud_string){"root", 4}, &parent));
    ud_schema_owner *schema = ud_alloc(ctx, sizeof(*schema));
    if (!schema)
        return UD_RESOURCE;
    schema->ctx = ctx;
    schema->columns = columns;
    atomic_init(&schema->references, 1);
    schema->fields = ud_alloc(ctx, columns * sizeof(*schema->fields));
    ud_status s = schema->fields ? UD_OK : UD_RESOURCE;
    for (uint32_t j = 0; !s && j < columns; j++) {
        s = ud_field_copy(ctx, fields + j, parent, schema->fields + j);
        for (uint32_t prior = 0; !s && prior < j; prior++) {
            ud_field *a = schema->fields + j, *b = schema->fields + prior;
            if (!ud_column_id_compare(a->identity, b->identity) ||
                (a->name_length == b->name_length && !memcmp(a->name, b->name, a->name_length)))
                s = UD_SCHEMA;
        }
    }
    if (s) {
        ud_schema_owner_release(schema);
        return s;
    }
    *out = schema;
    return UD_OK;
}
ud_status ud_schema_owner_retain(ud_schema_owner *s) {
    if (!s)
        return UD_INVALID;
    unsigned refs = atomic_load_explicit(&s->references, memory_order_relaxed);
    do {
        if (!refs || refs == UINT_MAX)
            return UD_RESOURCE;
    } while (!atomic_compare_exchange_weak_explicit(&s->references, &refs, refs + 1,
                                                    memory_order_relaxed, memory_order_relaxed));
    return UD_OK;
}
void ud_schema_owner_release(ud_schema_owner *s) {
    if (!s || atomic_fetch_sub_explicit(&s->references, 1, memory_order_acq_rel) != 1)
        return;
    if (s->fields)
        for (uint32_t j = 0; j < s->columns; j++)
            ud_field_clear(s->fields + j);
    ud_free(s->fields);
    ud_free(s);
}