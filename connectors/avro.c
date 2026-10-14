#include "ud/avro.h"
#include "../components/data/internal.h"
#include <zlib.h>
/* OCF framing and physical value encoding are format concerns. Validation,
 * conversion, dictionaries, row identities and errors use Native Data. */
typedef struct avro_type {
    int kind, nullable, null_branch, width, multiplier;
    struct avro_type *children;
    uint32_t count;
} avro_type;
enum {
    AV_BOOLEAN = 1,
    AV_INT,
    AV_LONG,
    AV_FLOAT,
    AV_DOUBLE,
    AV_STRING,
    AV_BYTES,
    AV_FIXED,
    AV_RECORD,
    AV_ARRAY,
    AV_MAP, AV_ENUM, AV_UNION, AV_NULL
};
typedef struct avro_default {
    const ud_json *schema, *value;
    avro_type type;
    ud_bytes wire;
    struct avro_default *next;
} avro_default;
typedef struct avro_file {
    ud_context *memory;
    FILE *file;
    ud_avro_options options;
    ud_batch *schema;
    avro_type *types;
    ud_batch *projection;
    ud_json *writer_schema, *reader_schema;
    avro_default *defaults;
    uint8_t sync[16];
    int codec, poisoned;
    ud_bytes block;
    uint64_t remaining, ordinal;
    ud_record_options records;
} avro_file;
struct ud_avro_reader {
    avro_file state;
};
struct ud_avro_writer {
    avro_file state;
};
static voidpf avro_z_allocate(voidpf context, uInt count, uInt width) {
    if (width && count > SIZE_MAX / width)
        return NULL;
    return ud_alloc(context, (size_t)count * width);
}
static void avro_z_free(voidpf context, voidpf allocation) {
    (void)context;
    ud_free(allocation);
}
static ud_status check(avro_file *file) {
    if (ud_is_cancelled(file->memory))
        return UD_CANCELLED;
    return file->options.check ? file->options.check(file->options.user) : UD_OK;
}
static ud_status options_read(const ud_avro_options *options, ud_avro_options *out) {
    *out = options ? *options
                   : (ud_avro_options){.struct_size = sizeof(*out),
                                       .abi_version = UD_ABI_VERSION,
                                       .batch_rows = 4096,
                                       .max_block_bytes = 16777216};
    if (out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!out->batch_rows || out->batch_rows > 65536 || !out->max_block_bytes ||
        out->max_block_bytes > 67108864 || out->codec < 0 || out->codec > 1)
        return UD_INVALID;
    return UD_OK;
}
static ud_status put_long(ud_bytes *bytes, int64_t value) {
    uint64_t zigzag = ((uint64_t)value << 1) ^ (UINT64_C(0) - (value < 0));
    do {
        uint8_t byte = (uint8_t)(zigzag & 127);
        zigzag >>= 7;
        if (zigzag)
            byte |= 128;
        UD_TRY(ud_bytes_put(bytes, &byte, 1));
    } while (zigzag);
    return UD_OK;
}
static ud_status get_long(ud_bytes *bytes, int64_t *out) {
    uint64_t value = 0;
    for (uint32_t at = 0; at < 10; at++) {
        uint8_t byte;
        UD_TRY(ud_bytes_get(bytes, &byte, 1));
        if (at == 9 && byte > 1)
            return UD_CORRUPT;
        value |= (uint64_t)(byte & 127) << (at * 7);
        if (!(byte & 128)) {
            uint64_t decoded = (value >> 1) ^ (UINT64_C(0) - (value & 1));
            memcpy(out, &decoded, 8);
            return UD_OK;
        }
    }
    return UD_CORRUPT;
}
static ud_status file_long(avro_file *file, int64_t *out, int allow_end) {
    uint8_t data[10];
    uint32_t count = 0;
    do {
        int byte = fgetc(file->file);
        if (byte == EOF)
            return ferror(file->file) ? UD_IO : !count && allow_end ? UD_END : UD_CORRUPT;
        if (count == 10)
            return UD_CORRUPT;
        data[count++] = (uint8_t)byte;
    } while (data[count - 1] & 128);
    ud_bytes bytes = {.data = data, .size = count};
    return get_long(&bytes, out);
}
static ud_status put_string(ud_bytes *bytes, const void *text, uint32_t length) {
    UD_TRY(put_long(bytes, length));
    return ud_bytes_put(bytes, text, length);
}
static ud_status file_string(avro_file *file, ud_bytes *bytes, uint32_t maximum) {
    int64_t length;
    UD_TRY(file_long(file, &length, 0));
    if (length < 0 || length > maximum)
        return UD_RESOURCE;
    bytes->size = 0;
    uint8_t buffer[16384];
    while (length) {
        UD_TRY(check(file));
        size_t count = length > (int64_t)sizeof(buffer) ? sizeof(buffer) : (size_t)length;
        if (fread(buffer, 1, count, file->file) != count)
            return ferror(file->file) ? UD_IO : UD_CORRUPT;
        UD_TRY(ud_bytes_put(bytes, buffer, count));
        length -= (int64_t)count;
    }
    return UD_OK;
}
static ud_status json_literal(ud_bytes *bytes, const char *text) {
    return ud_bytes_put(bytes, text, strlen(text));
}
static int valid_name(ud_string name) {
    if (!name.length || !((name.data[0] >= 'A' && name.data[0] <= 'Z') ||
                          (name.data[0] >= 'a' && name.data[0] <= 'z') || name.data[0] == '_'))
        return 0;
    for (uint32_t at = 1; at < name.length; at++)
        if (!((name.data[at] >= 'A' && name.data[at] <= 'Z') ||
              (name.data[at] >= 'a' && name.data[at] <= 'z') ||
              (name.data[at] >= '0' && name.data[at] <= '9') || name.data[at] == '_'))
            return 0;
    return 1;
}
static ud_status schema_field(ud_bytes *, const ud_field *, uint32_t, uint32_t *);
static ud_status schema_record(ud_bytes *bytes, const ud_field *fields, uint32_t count,
                               uint32_t depth, uint32_t *serial) {
    char prefix[96];
    snprintf(prefix, sizeof(prefix), "{\"type\":\"record\",\"name\":\"ud_record_%u\",\"fields\":[",
             (*serial)++);
    UD_TRY(json_literal(bytes, prefix));
    for (uint32_t at = 0; at < count; at++) {
        if (at)
            UD_TRY(json_literal(bytes, ","));
        UD_TRY(json_literal(bytes, "{\"name\":"));
        ud_string name = {fields[at].name, fields[at].name_length};
        if (!valid_name(name))
            return UD_UNSUPPORTED;
        UD_TRY(ud_json_quote(bytes, name.data, name.length));
        UD_TRY(json_literal(bytes, ",\"type\":"));
        UD_TRY(schema_field(bytes, fields + at, depth + 1, serial));
        UD_TRY(json_literal(bytes, "}"));
    }
    return json_literal(bytes, "]}");
}
static ud_status schema_field(ud_bytes *bytes, const ud_field *field, uint32_t depth,
                              uint32_t *serial) {
    if (depth > 32)
        return UD_RESOURCE;
    if (field->nullable)
        UD_TRY(json_literal(bytes, "[\"null\","));
    char text[256];
    const char *literal = NULL;
    switch (field->type) {
    case UD_BOOL:
        literal = "\"boolean\"";
        break;
    case UD_INT64:
        literal = "\"long\"";
        break;
    case UD_FLOAT64:
        literal = "\"double\"";
        break;
    case UD_UTF8:
        literal = "\"string\"";
        break;
    case UD_BINARY:
        literal = "\"bytes\"";
        break;
    case UD_DATE32:
        literal = "{\"type\":\"int\",\"logicalType\":\"date\"}";
        break;
    case UD_TIME64:
        literal = "{\"type\":\"long\",\"logicalType\":\"time-micros\"}";
        break;
    case UD_TIMESTAMP:
        literal = "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}";
        break;
    case UD_DURATION:
        literal = "{\"type\":\"long\",\"logicalType\":\"ud-duration-micros\",\"udProtocol\":10}";
        break;
    case UD_UINT64:
    case UD_INTERVAL:
        snprintf(text, sizeof(text),
                 "{\"type\":\"fixed\",\"name\":\"ud_fixed_%u\",\"size\":%u,"
                 "\"logicalType\":\"%s\",\"udProtocol\":10}",
                 (*serial)++, field->type == UD_UINT64 ? 8 : 16,
                 field->type == UD_UINT64 ? "ud-uint64" : "ud-interval");
        literal = text;
        break;
    case UD_DECIMAL128:
        snprintf(text, sizeof(text),
                 "{\"type\":\"fixed\",\"name\":\"ud_decimal_%u\",\"size\":16,"
                 "\"logicalType\":\"%s\",\"precision\":%d,\"scale\":%d,\"udProtocol\":10}",
                 (*serial)++, field->scale >= 0 ? "decimal" : "ud-decimal128", field->precision,
                 field->scale);
        literal = text;
        break;
    case UD_STRUCT:
        UD_TRY(schema_record(bytes, field->children, field->child_count, depth + 1, serial));
        break;
    case UD_LIST:
        UD_TRY(json_literal(bytes, "{\"type\":\"array\",\"items\":"));
        UD_TRY(schema_field(bytes, field->children, depth + 1, serial));
        UD_TRY(json_literal(bytes, "}"));
        break;
    case UD_MAP:
        if (field->children[0].children[0].type != UD_UTF8 ||
            field->children[0].children[0].nullable)
            return UD_UNSUPPORTED;
        UD_TRY(json_literal(bytes, "{\"type\":\"map\",\"values\":"));
        UD_TRY(schema_field(bytes, field->children[0].children + 1, depth + 1, serial));
        UD_TRY(json_literal(bytes, "}"));
        break;
    default:
        return UD_UNSUPPORTED;
    }
    if (literal)
        UD_TRY(json_literal(bytes, literal));
    return field->nullable ? json_literal(bytes, "]") : UD_OK;
}
static int named(const ud_json *value, const char *name) {
    const char *text;
    uint32_t length;
    return !ud_json_text(value, &text, &length) && length == strlen(name) &&
           !memcmp(text, name, length);
}
static const ud_json *member(const ud_json *value, const char *name) {
    return ud_json_find(value, name, (uint32_t)strlen(name));
}
static int null_schema(const ud_json *schema) {
    return named(ud_json_kind(schema) == UD_JSON_OBJECT ? member(schema,"type") : schema,"null");
}
static ud_status number(const ud_json *value, int64_t *out) {
    const char *text;
    uint32_t length;
    ud_value parsed;
    UD_TRY(ud_json_text(value, &text, &length));
    UD_TRY(ud_value_parse(NULL, UD_INT64, text, length, &parsed));
    *out = parsed.integer;
    return UD_OK;
}

typedef struct avro_symbol {
    char *full, *space;
    const ud_json *definition;
    int seen;
} avro_symbol;
typedef struct avro_symbols {
    ud_context *memory;
    avro_symbol *items;
    uint32_t count, capacity;
} avro_symbols;
static int primitive_name(const ud_json *value) {
    static const char *names[] = {"null","boolean","int","long","float","double","bytes","string"};
    for (uint32_t at = 0; at < sizeof(names)/sizeof(*names); at++) if (named(value,names[at])) return 1;
    return 0;
}
static ud_status full_name(ud_context *memory, const ud_json *name, const char *space, char **out) {
    ud_string text;
    UD_TRY(ud_json_text(name,&text.data,&text.length));
    if (!text.length || text.length > 65536 || memchr(text.data,0,text.length)) return UD_SCHEMA;
    ud_bytes bytes = {.ctx = memory,.limit = 131072};
    ud_status status = UD_OK;
    if (!memchr(text.data,'.',text.length) && space && *space) {
        status = json_literal(&bytes,space);
        if (!status) status = json_literal(&bytes,".");
    }
    if (!status) status = ud_bytes_put(&bytes,text.data,text.length);
    if (!status) status = ud_bytes_put(&bytes,"",1);
    if (!status) { *out = (char *)bytes.data; bytes.data = NULL; }
    ud_bytes_clear(&bytes); return status;
}
static ud_status collect_names(avro_symbols *symbols,const ud_json *schema,const char *space,uint32_t depth) {
    if (depth > 32) return UD_RESOURCE;
    if (ud_json_kind(schema) == UD_JSON_STRING) return UD_OK;
    if (ud_json_kind(schema) == UD_JSON_ARRAY) {
        for (uint32_t at = 0; at < ud_json_size(schema); at++) UD_TRY(collect_names(symbols,ud_json_at(schema,at),space,depth+1));
        return UD_OK;
    }
    if (ud_json_kind(schema) != UD_JSON_OBJECT) return UD_SCHEMA;
    const ud_json *type = member(schema,"type");
    const char *child_space = space;
    if (named(type,"record") || named(type,"enum") || named(type,"fixed")) {
        char *declared_space = NULL,*full = NULL;
        ud_status status = UD_OK;
        if (member(schema,"namespace")) {
            ud_string text; status = ud_json_text(member(schema,"namespace"),&text.data,&text.length);
            if (!status) { declared_space = ud_strdup_n(symbols->memory,text.data,text.length); if (!declared_space) status = UD_RESOURCE; }
        }
        if (!status) status = full_name(symbols->memory,member(schema,"name"),declared_space ? declared_space : space,&full);
        ud_free(declared_space);
        if (status) return status;
        for (uint32_t at = 0; at < symbols->count; at++) if (!strcmp(full,symbols->items[at].full)) { ud_free(full); return UD_SCHEMA; }
        if (symbols->count == 4096) { ud_free(full); return UD_RESOURCE; }
        if (symbols->count == symbols->capacity) {
            uint32_t capacity = symbols->capacity ? symbols->capacity * 2 : 16;
            avro_symbol *items = ud_alloc(symbols->memory,capacity*sizeof(*items));
            if (!items) { ud_free(full); return UD_RESOURCE; }
            if (symbols->count) memcpy(items,symbols->items,symbols->count*sizeof(*items));
            ud_free(symbols->items); symbols->items = items; symbols->capacity = capacity;
        }
        const char *last = strrchr(full,'.');
        char *owned_space = ud_strdup_n(symbols->memory,full,last ? (size_t)(last-full) : 0);
        if (!owned_space) { ud_free(full); return UD_RESOURCE; }
        symbols->items[symbols->count++] = (avro_symbol){full,owned_space,schema,0}; child_space = owned_space;
    }
    if (named(type,"record")) {
        const ud_json *fields = member(schema,"fields");
        if (ud_json_kind(fields) != UD_JSON_ARRAY) return UD_SCHEMA;
        for (uint32_t at = 0; at < ud_json_size(fields); at++) UD_TRY(collect_names(symbols,member(ud_json_at(fields,at),"type"),child_space,depth+1));
    } else if (named(type,"array") || named(type,"map"))
        return collect_names(symbols,member(schema,named(type,"array") ? "items" : "values"),child_space,depth+1);
    else if (ud_json_kind(type) == UD_JSON_OBJECT || ud_json_kind(type) == UD_JSON_ARRAY)
        return collect_names(symbols,type,child_space,depth+1);
    return UD_OK;
}
static ud_status copy_json(ud_bytes *bytes,const ud_json *json) {
    char *text = NULL; uint32_t length = 0;
    UD_TRY(ud_json_serialize(json,&text,&length));
    ud_status status = ud_bytes_put(bytes,text,length); ud_free(text); return status;
}
static ud_status expand_schema(avro_symbols *symbols,const ud_json *schema,const char *space,
                                const ud_json **stack,uint32_t depth,ud_bytes *output) {
    if (depth >= 32) return UD_UNSUPPORTED; /* An infinite recursive type has no finite Native field shape. */
    for (uint32_t at = 0; at < depth; at++) if (stack[at] == schema) return UD_UNSUPPORTED;
    stack[depth] = schema;
    if (ud_json_kind(schema) == UD_JSON_STRING) {
        if (primitive_name(schema)) return copy_json(output,schema);
        char *full = NULL; UD_TRY(full_name(symbols->memory,schema,space,&full));
        avro_symbol *found = NULL;
        for (uint32_t at = 0; at < symbols->count; at++) if (!strcmp(full,symbols->items[at].full)) { found = symbols->items+at; break; }
        ud_free(full);
        if (!found || !found->seen) return UD_SCHEMA;
        return expand_schema(symbols,found->definition,found->space,stack,depth+1,output);
    }
    if (ud_json_kind(schema) == UD_JSON_ARRAY) {
        uint32_t count = ud_json_size(schema); if (!count || count > 64) return UD_RESOURCE;
        UD_TRY(json_literal(output,"["));
        for (uint32_t at = 0; at < count; at++) {
            if (at) UD_TRY(json_literal(output,","));
            if (ud_json_kind(ud_json_at(schema,at)) == UD_JSON_ARRAY) return UD_SCHEMA;
            UD_TRY(expand_schema(symbols,ud_json_at(schema,at),space,stack,depth+1,output));
        }
        return json_literal(output,"]");
    }
    if (ud_json_kind(schema) != UD_JSON_OBJECT) return UD_SCHEMA;
    const ud_json *wrapped_type = member(schema,"type");
    if (ud_json_kind(wrapped_type) == UD_JSON_STRING && !primitive_name(wrapped_type) &&
        !named(wrapped_type,"record") && !named(wrapped_type,"enum") && !named(wrapped_type,"fixed") &&
        !named(wrapped_type,"array") && !named(wrapped_type,"map"))
        return expand_schema(symbols,wrapped_type,space,stack,depth+1,output);
    const char *definition_name = NULL;
    for (uint32_t at = 0; at < symbols->count; at++) if (symbols->items[at].definition == schema) {
        symbols->items[at].seen = 1; space = symbols->items[at].space; definition_name = symbols->items[at].full; break;
    }
    UD_TRY(json_literal(output,"{"));
    for (uint32_t at = 0; at < ud_json_size(schema); at++) {
        const char *key; uint32_t length;
        UD_TRY(ud_json_key(schema,at,&key,&length));
        if (at) UD_TRY(json_literal(output,","));
        UD_TRY(ud_json_quote(output,key,length)); UD_TRY(json_literal(output,":"));
        const ud_json *value = ud_json_at(schema,at);
        if (definition_name && length == 4 && !memcmp(key,"name",4)) UD_TRY(ud_json_quote(output,definition_name,(uint32_t)strlen(definition_name)));
        else if ((length == 5 && !memcmp(key,"items",5)) || (length == 6 && !memcmp(key,"values",6)) ||
            (length == 4 && !memcmp(key,"type",4) && ud_json_kind(value) != UD_JSON_STRING))
            UD_TRY(expand_schema(symbols,value,space,stack,depth+1,output));
        else if (length == 6 && !memcmp(key,"fields",6)) {
            UD_TRY(json_literal(output,"["));
            for (uint32_t field = 0; field < ud_json_size(value); field++) {
                if (field) UD_TRY(json_literal(output,","));
                const ud_json *entry = ud_json_at(value,field); UD_TRY(json_literal(output,"{"));
                for (uint32_t item = 0; item < ud_json_size(entry); item++) {
                    const char *name; uint32_t n; UD_TRY(ud_json_key(entry,item,&name,&n));
                    if (item) UD_TRY(json_literal(output,","));
                    UD_TRY(ud_json_quote(output,name,n)); UD_TRY(json_literal(output,":"));
                    if (n == 4 && !memcmp(name,"type",4)) UD_TRY(expand_schema(symbols,ud_json_at(entry,item),space,stack,depth+1,output));
                    else UD_TRY(copy_json(output,ud_json_at(entry,item)));
                }
                UD_TRY(json_literal(output,"}"));
            }
            UD_TRY(json_literal(output,"]"));
        } else UD_TRY(copy_json(output,value));
    }
    return json_literal(output,"}");
}
static ud_status resolve_names(ud_context *memory,const ud_json *input,ud_json **out) {
    *out = NULL; avro_symbols symbols = {.memory = memory};
    ud_status status = collect_names(&symbols,input,"",0);
    ud_bytes bytes = {.ctx = memory,.limit = 16u*1024u*1024u};
    const ud_json *stack[32] = {0};
    if (!status) status = expand_schema(&symbols,input,"",stack,0,&bytes);
    if (!status) status = ud_json_parse(memory,(const char *)bytes.data,(uint32_t)bytes.size,out,NULL);
    for (uint32_t at = 0; at < symbols.count; at++) { ud_free(symbols.items[at].full); ud_free(symbols.items[at].space); }
    ud_free(symbols.items); ud_bytes_clear(&bytes); return status;
}
static void type_clear(avro_type *type) {
    if (!type)
        return;
    for (uint32_t at = 0; type->children && at < type->count; at++)
        type_clear(type->children + at);
    ud_free(type->children);
    memset(type, 0, sizeof(*type));
}
static ud_status type_bind(ud_context *memory, const ud_json *json, const ud_field *field,
                           avro_type *out, uint32_t depth) {
    if (depth > 32)
        return UD_RESOURCE;
    out->multiplier = 1;
    if (ud_json_kind(json) == UD_JSON_ARRAY) {
        uint32_t count = ud_json_size(json);
        int first = count == 2 && null_schema(ud_json_at(json,0)), second = count == 2 && null_schema(ud_json_at(json,1));
        if (count != 2 || first == second) {
            if (!count || count > 64 || field->type != UD_STRUCT || field->child_count != count+1 || field->children[0].type != UD_INT64) return UD_INCOMPATIBLE;
            out->kind = AV_UNION; out->count = count; out->children = ud_alloc(memory,count*sizeof(*out->children));
            if (!out->children) return UD_RESOURCE;
            for (uint32_t at = 0; at < count; at++) {
                ud_field branch = field->children[at+1];
                if (null_schema(ud_json_at(json,at))) { out->children[at].kind = AV_NULL; continue; }
                branch.nullable = 0; UD_TRY(type_bind(memory,ud_json_at(json,at),&branch,out->children+at,depth+1));
            }
            return UD_OK;
        }
        if (!field->nullable) return UD_INCOMPATIBLE;
        out->nullable = 1;
        out->null_branch = first ? 0 : 1;
        json = ud_json_at(json, first ? 1 : 0);
    } else if (null_schema(json) && field->nullable) { out->kind = AV_NULL; return UD_OK; }
    else if (field->nullable) return UD_INCOMPATIBLE;
    const ud_json *type = ud_json_kind(json) == UD_JSON_STRING ? json : member(json, "type");
    const ud_json *logical = member(json, "logicalType");
    const struct {
        const char *name;
        int kind;
    } names[] = {{"boolean", AV_BOOLEAN}, {"int", AV_INT},       {"long", AV_LONG},
                 {"float", AV_FLOAT},     {"double", AV_DOUBLE}, {"string", AV_STRING},
                 {"bytes", AV_BYTES},     {"fixed", AV_FIXED},   {"record", AV_RECORD},
                 {"array", AV_ARRAY},     {"map", AV_MAP}, {"enum", AV_ENUM}};
    for (uint32_t at = 0; at < sizeof(names) / sizeof(*names); at++)
        if (named(type, names[at].name))
            out->kind = names[at].kind;
    if (!out->kind)
        return UD_UNSUPPORTED;
    int match =
        field->type == UD_BOOL      ? out->kind == AV_BOOLEAN && !logical
        : field->type == UD_INT64   ? (out->kind == AV_INT || out->kind == AV_LONG) && !logical
        : field->type == UD_FLOAT64 ? (out->kind == AV_FLOAT || out->kind == AV_DOUBLE) && !logical
        : field->type == UD_UTF8    ? out->kind == AV_STRING && !logical
        : field->type == UD_BINARY  ? (out->kind == AV_BYTES || out->kind == AV_FIXED) && !logical
        : field->type == UD_DATE32  ? out->kind == AV_INT && named(logical, "date")
        : field->type == UD_TIME64  ? (out->kind == AV_LONG && named(logical, "time-micros")) ||
                                         (out->kind == AV_INT && named(logical, "time-millis"))
        : field->type == UD_TIMESTAMP
            ? out->kind == AV_LONG &&
                  (named(logical, "timestamp-micros") || named(logical, "timestamp-millis"))
        : field->type == UD_DURATION ? out->kind == AV_LONG && named(logical, "ud-duration-micros")
        : field->type == UD_UINT64   ? (out->kind == AV_FIXED && named(logical, "ud-uint64")) || (out->kind == AV_ENUM && field->dictionary_kind == UD_DICTIONARY_ENUM)
        : field->type == UD_INTERVAL ? out->kind == AV_FIXED && named(logical, "ud-interval")
        : field->type == UD_DECIMAL128
            ? (out->kind == AV_FIXED || out->kind == AV_BYTES) &&
                  (named(logical, "decimal") || named(logical, "ud-decimal128"))
        : field->type == UD_STRUCT ? out->kind == AV_RECORD
        : field->type == UD_LIST   ? out->kind == AV_ARRAY
        : field->type == UD_MAP    ? out->kind == AV_MAP
                                   : 0;
    if (!match)
        return UD_INCOMPATIBLE;
    if ((field->type == UD_UINT64 && out->kind != AV_ENUM) || field->type == UD_INTERVAL || field->type == UD_DURATION ||
        named(logical, "ud-decimal128")) {
        int64_t version;
        UD_TRY(number(member(json, "udProtocol"), &version));
        if (version != UD_WIRE_VERSION)
            return UD_INCOMPATIBLE;
    }
    if (named(logical, "time-millis") || named(logical, "timestamp-millis"))
        out->multiplier = 1000;
    if (out->kind == AV_ENUM) {
        const ud_json *symbols = member(json,"symbols"); out->count = ud_json_size(symbols);
        if (!out->count || out->count != field->dictionary_count) return UD_INCOMPATIBLE;
        for (uint32_t at = 0; at < out->count; at++) {
            ud_string symbol; UD_TRY(ud_json_text(ud_json_at(symbols,at),&symbol.data,&symbol.length));
            if (symbol.length != field->dictionary[at].length || memcmp(symbol.data,field->dictionary[at].data,symbol.length)) return UD_INCOMPATIBLE;
        }
    }
    if (out->kind == AV_FIXED) {
        int64_t width;
        UD_TRY(number(member(json, "size"), &width));
        if (width < 1 || width > 67108864)
            return UD_RESOURCE;
        out->width = (int)width;
        if ((field->type == UD_UINT64 && width != 8) ||
            (field->type == UD_INTERVAL && width != 16) ||
            (field->type == UD_DECIMAL128 && width > 16))
            return UD_INCOMPATIBLE;
    }
    if (field->type == UD_DECIMAL128) {
        int64_t precision, scale = 0;
        UD_TRY(number(member(json, "precision"), &precision));
        if (member(json, "scale"))
            UD_TRY(number(member(json, "scale"), &scale));
        if (precision != field->precision || scale != field->scale)
            return UD_INCOMPATIBLE;
    }
    if (out->kind == AV_RECORD || out->kind == AV_ARRAY || out->kind == AV_MAP) {
        const ud_json *fields = member(json, "fields");
        out->count = out->kind == AV_RECORD ? field->child_count : 1;
        if (out->kind == AV_RECORD && ud_json_size(fields) != field->child_count)
            return UD_INCOMPATIBLE;
        out->children = ud_alloc(memory, out->count * sizeof(*out->children));
        if (!out->children)
            return UD_RESOURCE;
        for (uint32_t at = 0; at < out->count; at++) {
            const ud_field *child =
                out->kind == AV_MAP ? field->children[0].children + 1 : field->children + at;
            const ud_json *child_type =
                out->kind == AV_RECORD ? member(ud_json_at(fields, at), "type")
                                       : member(json, out->kind == AV_MAP ? "values" : "items");
            if (out->kind == AV_RECORD) {
                const char *name;
                uint32_t length;
                UD_TRY(ud_json_text(member(ud_json_at(fields, at), "name"), &name, &length));
                if (length != child->name_length || memcmp(name, child->name, length))
                    return UD_INCOMPATIBLE;
            }
            UD_TRY(type_bind(memory, child_type, child, out->children + at, depth + 1));
        }
    }
    return UD_OK;
}
static void value_release(ud_value *value) {
    for (uint32_t at = 0; at < value->child_count; at++)
        value_release((ud_value *)value->children + at);
    ud_free((void *)value->children);
    value->children = NULL;
    value->child_count = 0;
}
static ud_status little_put(ud_bytes *bytes, uint64_t value, uint32_t width) {
    uint8_t data[8];
    for (uint32_t at = 0; at < width; at++)
        data[at] = (uint8_t)(value >> (at * 8));
    return ud_bytes_put(bytes, data, width);
}
static ud_status little_get(ud_bytes *bytes, uint32_t width, uint64_t *out) {
    uint8_t data[8];
    UD_TRY(ud_bytes_get(bytes, data, width));
    *out = 0;
    for (uint32_t at = 0; at < width; at++)
        *out |= (uint64_t)data[at] << (at * 8);
    return UD_OK;
}
static ud_status slice_get(ud_bytes *bytes, uint32_t width, const uint8_t **out) {
    if (width > bytes->size - bytes->pos)
        return UD_CORRUPT;
    *out = bytes->data + bytes->pos;
    bytes->pos += width;
    return UD_OK;
}
static ud_status string_get(ud_bytes *bytes, const uint8_t **out, uint32_t *width) {
    int64_t length;
    UD_TRY(get_long(bytes, &length));
    if (length < 0 || length > UINT32_MAX)
        return UD_CORRUPT;
    *width = (uint32_t)length;
    return slice_get(bytes, *width, out);
}
static ud_status encode_value(ud_bytes *bytes, const ud_field *field, const avro_type *type,
                              const ud_value *value, uint32_t depth) {
    if (depth > 32 || value->is_absent)
        return UD_INCOMPATIBLE;
    if (type->kind == AV_NULL) return value->is_null ? UD_OK : UD_QUALITY;
    if (type->nullable)
        UD_TRY(put_long(bytes, value->is_null ? type->null_branch : 1 - type->null_branch));
    if (value->is_null)
        return type->nullable ? UD_OK : UD_QUALITY;
    switch (type->kind) {
    case AV_ENUM:
        if (!value->unsigned_integer || value->unsigned_integer > type->count) return UD_QUALITY;
        return put_long(bytes,(int64_t)value->unsigned_integer-1);
    case AV_UNION: {
        if (value->child_count != type->count+1 || value->children[0].is_null || value->children[0].type != UD_INT64) return UD_SCHEMA;
        int64_t branch = value->children[0].integer;
        if (branch < 0 || (uint64_t)branch >= type->count) return UD_QUALITY;
        for (uint32_t at = 0; at < type->count; at++) if (at != (uint32_t)branch && !value->children[at+1].is_null) return UD_QUALITY;
        UD_TRY(put_long(bytes,branch));
        return encode_value(bytes,field->children+branch+1,type->children+branch,value->children+branch+1,depth+1);
    }
    case AV_BOOLEAN: {
        uint8_t flag = value->integer != 0;
        return ud_bytes_put(bytes, &flag, 1);
    }
    case AV_INT:
        if (value->integer / type->multiplier < INT32_MIN || value->integer / type->multiplier > INT32_MAX)
            return UD_OVERFLOW;
        /* fall through */
    case AV_LONG:
        if (type->multiplier != 1 && value->integer % type->multiplier) return UD_TRUNCATED;
        return put_long(bytes, value->integer / type->multiplier);
    case AV_FLOAT: {
        float real = (float)value->real;
        if ((double)real != value->real) return UD_TRUNCATED;
        uint32_t bits;
        memcpy(&bits, &real, 4);
        return little_put(bytes, bits, 4);
    }
    case AV_DOUBLE: {
        uint64_t bits;
        memcpy(&bits, &value->real, 8);
        return little_put(bytes, bits, 8);
    }
    case AV_STRING:
        return put_string(bytes, value->text, value->text_length);
    case AV_BYTES:
        if (field->type != UD_DECIMAL128)
            return put_string(bytes, value->text, value->text_length);
        UD_TRY(put_long(bytes, 16));
        /* fall through */
    case AV_FIXED: {
        uint8_t data[16];
        if (field->type == UD_UINT64) {
            for (uint32_t at = 0; at < 8; at++)
                data[at] = (uint8_t)(value->unsigned_integer >> ((7 - at) * 8));
        } else if (field->type == UD_INTERVAL) {
            uint64_t micros;
            memcpy(&micros, &value->interval.microseconds, 8);
            for (uint32_t at = 0; at < 4; at++) {
                data[at] = (uint8_t)((uint32_t)value->interval.months >> ((3 - at) * 8));
                data[at + 4] = (uint8_t)((uint32_t)value->interval.days >> ((3 - at) * 8));
            }
            for (uint32_t at = 0; at < 8; at++)
                data[at + 8] = (uint8_t)(micros >> ((7 - at) * 8));
        } else if (field->type == UD_DECIMAL128) {
            for (uint32_t at = 0; at < 8; at++) {
                data[at] = (uint8_t)((uint64_t)value->decimal.high >> ((7 - at) * 8));
                data[at + 8] = (uint8_t)(value->decimal.low >> ((7 - at) * 8));
            }
        } else {
            if (value->text_length != (uint32_t)type->width)
                return UD_TRUNCATED;
            return ud_bytes_put(bytes, value->text, value->text_length);
        }
        if (field->type == UD_DECIMAL128 && type->kind == AV_FIXED && type->width < 16) {
            uint32_t skip = 16-(uint32_t)type->width; uint8_t sign = data[skip] & 128 ? 255 : 0;
            for (uint32_t at = 0; at < skip; at++) if (data[at] != sign) return UD_OVERFLOW;
            return ud_bytes_put(bytes,data+skip,(size_t)type->width);
        }
        return ud_bytes_put(bytes, data, field->type == UD_UINT64 ? 8 : 16);
    }
    case AV_RECORD:
        if (value->child_count != type->count)
            return UD_SCHEMA;
        for (uint32_t at = 0; at < type->count; at++)
            UD_TRY(encode_value(bytes, field->children + at, type->children + at,
                                value->children + at, depth + 1));
        return UD_OK;
    case AV_ARRAY:
    case AV_MAP:
        if (value->child_count)
            UD_TRY(put_long(bytes, value->child_count));
        for (uint32_t at = 0; at < value->child_count; at++) {
            const ud_value *child = value->children + at;
            const ud_field *child_field = field->children;
            if (type->kind == AV_MAP) {
                if (child->child_count != 2 || child->children[0].is_null)
                    return UD_SCHEMA;
                UD_TRY(put_string(bytes, child->children[0].text, child->children[0].text_length));
                child = child->children + 1;
                child_field = field->children[0].children + 1;
            }
            UD_TRY(encode_value(bytes, child_field, type->children, child, depth + 1));
        }
        return put_long(bytes, 0);
    default:
        return UD_UNSUPPORTED;
    }
}
static ud_status decode_value(avro_file *file, ud_bytes *bytes, const ud_field *field,
                              const avro_type *type, ud_value *value, uint32_t depth) {
    if (depth > 32)
        return UD_RESOURCE;
    value->is_null = 0; value->is_absent = 0;
    value->type = field->type;
    value->precision = field->precision;
    value->scale = field->scale;
    if (type->nullable) {
        int64_t branch;
        UD_TRY(get_long(bytes, &branch));
        if (branch < 0 || branch > 1)
            return UD_CORRUPT;
        if (branch == type->null_branch) {
            value->is_null = 1;
            return UD_OK;
        }
    }
    uint64_t bits = 0;
    int64_t integer = 0;
    const uint8_t *data = NULL;
    uint32_t width = 0;
    switch (type->kind) {
    case AV_NULL: value->is_null = 1; return UD_OK;
    case AV_ENUM:
        UD_TRY(get_long(bytes,&integer));
        if (integer < 0 || (uint64_t)integer >= type->count) return UD_CORRUPT;
        value->unsigned_integer = (uint64_t)integer+1; break;
    case AV_UNION: {
        UD_TRY(get_long(bytes,&integer));
        if (integer < 0 || (uint64_t)integer >= type->count) return UD_CORRUPT;
        ud_value *children = ud_alloc(file->memory,(type->count+1)*sizeof(*children));
        if (!children) return UD_RESOURCE;
        value->children = children; value->child_count = type->count+1;
        for (uint32_t at = 0; at <= type->count; at++) children[at] = (ud_value){.type = field->children[at].type,.precision = field->children[at].precision,.scale = field->children[at].scale,.is_null = 1};
        children[0] = (ud_value){.type = UD_INT64,.integer = integer};
        return decode_value(file,bytes,field->children+integer+1,type->children+integer,children+integer+1,depth+1);
    }
    case AV_BOOLEAN: {
        uint8_t flag;
        UD_TRY(ud_bytes_get(bytes, &flag, 1));
        if (flag > 1)
            return UD_CORRUPT;
        value->integer = flag;
        break;
    }
    case AV_INT:
    case AV_LONG:
        UD_TRY(get_long(bytes, &integer));
        if (type->kind == AV_INT && (integer < INT32_MIN || integer > INT32_MAX))
            return UD_CORRUPT;
        if (type->multiplier == 1000 && (integer > INT64_MAX / 1000 || integer < INT64_MIN / 1000))
            return UD_OVERFLOW;
        value->integer = integer * type->multiplier;
        break;
    case AV_FLOAT: {
        UD_TRY(little_get(bytes, 4, &bits));
        uint32_t small = (uint32_t)bits;
        float real;
        memcpy(&real, &small, 4);
        value->real = real;
        break;
    }
    case AV_DOUBLE:
        UD_TRY(little_get(bytes, 8, &bits));
        memcpy(&value->real, &bits, 8);
        break;
    case AV_STRING:
    case AV_BYTES:
        UD_TRY(string_get(bytes, &data, &width));
        if (type->kind == AV_STRING)
            if (!ud_utf8_valid((const char *)data, width))
                return UD_CORRUPT;
        if (field->type != UD_DECIMAL128) {
            value->text = (const char *)data;
            value->text_length = width;
            break;
        }
        /* fall through */
    case AV_FIXED:
        if (type->kind == AV_FIXED) {
            width = (uint32_t)type->width;
            UD_TRY(slice_get(bytes, width, &data));
        }
        if (field->type == UD_UINT64) {
            for (uint32_t at = 0; at < 8; at++)
                value->unsigned_integer = (value->unsigned_integer << 8) | data[at];
        } else if (field->type == UD_INTERVAL) {
            uint32_t months = 0, days = 0;
            uint64_t micros = 0;
            for (uint32_t at = 0; at < 4; at++) {
                months = (months << 8) | data[at];
                days = (days << 8) | data[at + 4];
            }
            for (uint32_t at = 0; at < 8; at++)
                micros = (micros << 8) | data[at + 8];
            memcpy(&value->interval.months, &months, 4);
            memcpy(&value->interval.days, &days, 4);
            memcpy(&value->interval.microseconds, &micros, 8);
        } else if (field->type == UD_DECIMAL128) {
            if (!width || width > 16)
                return UD_CORRUPT;
            uint8_t expanded[16];
            memset(expanded, data[0] & 128 ? 255 : 0, 16);
            memcpy(expanded + 16 - width, data, width);
            uint64_t high = 0;
            for (uint32_t at = 0; at < 8; at++) {
                high = (high << 8) | expanded[at];
                value->decimal.low = (value->decimal.low << 8) | expanded[at + 8];
            }
            memcpy(&value->decimal.high, &high, 8);
        } else {
            value->text = (const char *)data;
            value->text_length = width;
        }
        break;
    case AV_RECORD: {
        ud_value *children = ud_alloc(file->memory, type->count * sizeof(*children));
        if (!children)
            return UD_RESOURCE;
        value->children = children;
        value->child_count = type->count;
        for (uint32_t at = 0; at < type->count; at++)
            UD_TRY(decode_value(file, bytes, field->children + at, type->children + at,
                                children + at, depth + 1));
        break;
    }
    case AV_ARRAY:
    case AV_MAP: {
        ud_value *children = NULL;
        uint32_t count = 0;
        for (;;) {
            int64_t items, block_size = 0;
            UD_TRY(get_long(bytes, &items));
            if (!items)
                break;
            if (items == INT64_MIN)
                return UD_CORRUPT;
            int block = items < 0;
            if (block) {
                items = -items;
                UD_TRY(get_long(bytes, &block_size));
                if (block_size < 0 || (uint64_t)block_size > bytes->size - bytes->pos)
                    return UD_CORRUPT;
            }
            if (items > 1048576 - count)
                return UD_RESOURCE;
            size_t end = block ? bytes->pos + (size_t)block_size : 0;
            ud_value *grown = ud_alloc(file->memory, (count + (uint32_t)items) * sizeof(*grown));
            if (!grown)
                return UD_RESOURCE;
            if (count)
                memcpy(grown, children, count * sizeof(*grown));
            ud_free(children);
            children = grown;
            value->children = children;
            value->child_count = count + (uint32_t)items;
            for (int64_t at = 0; at < items; at++) {
                UD_TRY(check(file));
                ud_value *child = children + count + (uint32_t)at;
                const ud_field *child_field = field->children;
                if (type->kind == AV_MAP) {
                    ud_value *entry = ud_alloc(file->memory, 2 * sizeof(*entry));
                    if (!entry)
                        return UD_RESOURCE;
                    child->type = UD_STRUCT;
                    child->children = entry;
                    child->child_count = 2;
                    UD_TRY(string_get(bytes, &data, &width));
                    if (!ud_utf8_valid((const char *)data, width))
                        return UD_CORRUPT;
                    entry[0] = (ud_value){
                        .type = UD_UTF8, .text = (const char *)data, .text_length = width};
                    child = entry + 1;
                    child_field = field->children[0].children + 1;
                }
                UD_TRY(decode_value(file, bytes, child_field, type->children, child, depth + 1));
            }
            count += (uint32_t)items;
            if (block && bytes->pos != end)
                return UD_CORRUPT;
        }
        break;
    }
    default:
        return UD_UNSUPPORTED;
    }
    return UD_OK;
}
static void close_file(avro_file *file) {
    if (file->file)
        fclose(file->file);
    if (file->types)
        for (uint32_t at = 0; at < ud_batch_columns(file->schema); at++)
            type_clear(file->types + at);
    ud_free(file->types);
    while (file->defaults) { avro_default *next = file->defaults->next; type_clear(&file->defaults->type); ud_bytes_clear(&file->defaults->wire); ud_free(file->defaults); file->defaults = next; }
    ud_json_destroy(file->writer_schema); ud_json_destroy(file->reader_schema); ud_batch_destroy(file->projection);
    ud_batch_destroy(file->schema);
    ud_bytes_clear(&file->block);
}
static ud_status schema_bind(avro_file *file, const ud_json *json) {
    const ud_json *fields = member(json, "fields");
    if (!named(member(json, "type"), "record") || ud_json_kind(fields) != UD_JSON_ARRAY ||
        ud_json_size(fields) != ud_batch_columns(file->schema))
        return UD_INCOMPATIBLE;
    file->types = ud_alloc(file->memory, ud_json_size(fields) * sizeof(*file->types));
    if (!file->types)
        return UD_RESOURCE;
    for (uint32_t at = 0; at < ud_json_size(fields); at++) {
        ud_field field;
        UD_TRY(ud_batch_field(file->schema, at, &field));
        const char *name;
        uint32_t size;
        UD_TRY(ud_json_text(member(ud_json_at(fields, at), "name"), &name, &size));
        if (size != field.name_length || memcmp(name, field.name, size))
            return UD_INCOMPATIBLE;
        UD_TRY(type_bind(file->memory, member(ud_json_at(fields, at), "type"), &field,
                         file->types + at, 0));
    }
    return UD_OK;
}
/* Foreign schemas are projected into canonical Data, then bound by the same
 * physical decoder. Named references are resolved before binding; multi-member
 * unions preserve their branch in a typed tagged Struct. */
static ud_status infer_field(ud_context *memory, const ud_json *json, ud_field *field,
                             uint32_t depth) {
    if (depth > 32)
        return UD_RESOURCE;
    if (ud_json_kind(json) == UD_JSON_ARRAY) {
        uint32_t count = ud_json_size(json);
        int first = count == 2 && null_schema(ud_json_at(json,0)), second = count == 2 && null_schema(ud_json_at(json,1));
        if (count != 2 || first == second) {
            static const char *names[] = {"variant0", "variant1", "variant2", "variant3", "variant4", "variant5", "variant6", "variant7", "variant8", "variant9", "variant10", "variant11", "variant12", "variant13", "variant14", "variant15", "variant16", "variant17", "variant18", "variant19", "variant20", "variant21", "variant22", "variant23", "variant24", "variant25", "variant26", "variant27", "variant28", "variant29", "variant30", "variant31", "variant32", "variant33", "variant34", "variant35", "variant36", "variant37", "variant38", "variant39", "variant40", "variant41", "variant42", "variant43", "variant44", "variant45", "variant46", "variant47", "variant48", "variant49", "variant50", "variant51", "variant52", "variant53", "variant54", "variant55", "variant56", "variant57", "variant58", "variant59", "variant60", "variant61", "variant62", "variant63"};
            if (!count || count > 64) return UD_RESOURCE;
            ud_field *children = ud_alloc(memory,(count+1)*sizeof(*children)); if (!children) return UD_RESOURCE;
            field->type = UD_STRUCT; field->children = children; field->child_count = count+1;
            children[0] = (ud_field){.name = "branch",.name_length = 6,.type = UD_INT64};
            for (uint32_t at = 0; at < count; at++) {
                children[at+1].name = names[at]; children[at+1].name_length = (uint32_t)strlen(names[at]);
                UD_TRY(infer_field(memory,ud_json_at(json,at),children+at+1,depth+1)); children[at+1].nullable = 1;
            }
            return UD_OK;
        }
        field->nullable = 1; json = ud_json_at(json,first ? 1 : 0);
    }
    const ud_json *type = ud_json_kind(json) == UD_JSON_STRING ? json : member(json, "type");
    const ud_json *logical = member(json, "logicalType");
    if (named(type,"null")) { field->type = UD_BOOL; field->nullable = 1; }
    else if (named(type,"enum")) {
        ud_bytes bytes = {.ctx = memory};
        ud_status status = json_literal(&bytes,"{\"kind\":\"enum\",\"values\":");
        if (!status) status = copy_json(&bytes,member(json,"symbols"));
        if (!status) status = json_literal(&bytes,"}");
        ud_json *dictionary = NULL;
        if (!status) status = ud_json_parse(memory,(const char *)bytes.data,(uint32_t)bytes.size,&dictionary,NULL);
        field->type = UD_UINT64;
        if (!status) status = ud_dictionary_parse(memory,dictionary,field);
        ud_json_destroy(dictionary); ud_bytes_clear(&bytes); return status;
    }
    else if (named(type, "boolean")) field->type = UD_BOOL;
    else if (named(type, "int") || named(type, "long"))
        field->type = UD_INT64;
    else if (named(type, "float") || named(type, "double"))
        field->type = UD_FLOAT64;
    else if (named(type, "string"))
        field->type = UD_UTF8;
    else if (named(type, "bytes") || named(type, "fixed"))
        field->type = UD_BINARY;
    else if (named(type, "record"))
        field->type = UD_STRUCT;
    else if (named(type, "array"))
        field->type = UD_LIST;
    else if (named(type, "map"))
        field->type = UD_MAP;
    else
        return UD_UNSUPPORTED;
    if (logical) {
        if (named(logical, "date"))
            field->type = UD_DATE32;
        else if (named(logical, "time-millis") || named(logical, "time-micros"))
            field->type = UD_TIME64;
        else if (named(logical, "timestamp-millis") || named(logical, "timestamp-micros")) {
            field->type = UD_TIMESTAMP;
            field->timezone = "UTC";
            field->timezone_length = 3;
        } else if (named(logical, "decimal") || named(logical, "ud-decimal128")) {
            field->type = UD_DECIMAL128;
            int64_t precision, scale = 0;
            UD_TRY(number(member(json, "precision"), &precision));
            if (member(json, "scale"))
                UD_TRY(number(member(json, "scale"), &scale));
            if (precision < 1 || precision > 38 || scale < -38 || scale > 38)
                return UD_UNSUPPORTED;
            field->precision = (int32_t)precision;
            field->scale = (int32_t)scale;
        } else if (named(logical, "ud-uint64"))
            field->type = UD_UINT64;
        else if (named(logical, "ud-duration-micros"))
            field->type = UD_DURATION;
        else if (named(logical, "ud-interval"))
            field->type = UD_INTERVAL;
        else
            return UD_UNSUPPORTED;
    }
    if (field->type >= UD_STRUCT) {
        const ud_json *fields = member(json, "fields");
        uint32_t count = field->type == UD_STRUCT ? ud_json_size(fields) : 1;
        if (!count || count > 65536 ||
            (field->type == UD_STRUCT && ud_json_kind(fields) != UD_JSON_ARRAY))
            return UD_SCHEMA;
        ud_field *children = ud_alloc(memory, count * sizeof(*children));
        if (!children)
            return UD_RESOURCE;
        field->children = children;
        field->child_count = count;
        if (field->type == UD_MAP) {
            children[0].name = "entries";
            children[0].name_length = 7;
            children[0].type = UD_STRUCT;
            ud_field *pair = ud_alloc(memory, 2 * sizeof(*pair));
            if (!pair)
                return UD_RESOURCE;
            children[0].children = pair;
            children[0].child_count = 2;
            pair[0] = (ud_field){.name = "key", .name_length = 3, .type = UD_UTF8};
            pair[1].name = "value";
            pair[1].name_length = 5;
            UD_TRY(infer_field(memory, member(json, "values"), pair + 1, depth + 1));
        } else
            for (uint32_t at = 0; at < count; at++) {
                const ud_json *entry = field->type == UD_STRUCT ? ud_json_at(fields, at) : NULL;
                if (entry)
                    UD_TRY(ud_json_text(member(entry, "name"), &children[at].name,
                                        &children[at].name_length));
                else {
                    children[at].name = "item";
                    children[at].name_length = 4;
                }
                UD_TRY(infer_field(memory, entry ? member(entry, "type") : member(json, "items"),
                                   children + at, depth + 1));
            }
    }
    return UD_OK;
}
static void infer_clear(ud_field *field) {
    ud_dictionary_clear(field);
    for (uint32_t at = 0; at < field->child_count; at++)
        infer_clear((ud_field *)field->children + at);
    ud_free((void *)field->children);
}
#include "avro_resolution.h"

static ud_status header_read(avro_file *file, const ud_batch *expected, ud_string scope, ud_string reader_schema) {
    uint8_t magic[4];
    if (fread(magic, 1, 4, file->file) != 4 || memcmp(magic, "Obj\1", 4))
        return UD_CORRUPT;
    ud_bytes key = {.ctx = file->memory, .limit = 65536},
             value = {.ctx = file->memory, .limit = file->options.max_block_bytes};
    ud_json *schema = NULL;
    ud_batch *embedded = NULL;
    ud_status status = UD_OK;
    int got_schema = 0, got_codec = 0, got_native = 0;
    uint32_t entries = 0;
    for (;;) {
        int64_t count = 0, size = 0;
        status = file_long(file, &count, 0);
        if (status || !count)
            break;
        if (count == INT64_MIN) {
            status = UD_CORRUPT;
            break;
        }
        int block = count < 0;
        if (block) {
            count = -count;
            status = file_long(file, &size, 0);
        }
        int64_t begin = ud_tell(file->file);
        if (!status && (count > 128 - entries ||
                        (block && (size < 0 || size > file->options.max_block_bytes))))
            status = UD_RESOURCE;
        for (int64_t at = 0; !status && at < count; at++) {
            status = file_string(file, &key, 65536);
            if (!status)
                status = file_string(file, &value, file->options.max_block_bytes);
            if (!status && key.size == 11 && !memcmp(key.data, "avro.schema", 11)) {
                if (got_schema++)
                    status = UD_CORRUPT;
                else
                    status = ud_json_parse(file->memory, (const char *)value.data,
                                           (uint32_t)value.size, &schema, NULL);
            } else if (!status && key.size == 10 && !memcmp(key.data, "avro.codec", 10)) {
                if (got_codec++)
                    status = UD_CORRUPT;
                else if (value.size == 4 && !memcmp(value.data, "null", 4))
                    file->codec = UD_AVRO_NULL;
                else if (value.size == 7 && !memcmp(value.data, "deflate", 7))
                    file->codec = UD_AVRO_DEFLATE;
                else
                    status = UD_UNSUPPORTED;
            } else if (!status && key.size == 9 && !memcmp(key.data, "ud.schema", 9)) {
                if (got_native++)
                    status = UD_CORRUPT;
                else
                    status = ud_schema_from_json(file->memory, (const char *)value.data,
                                                 (uint32_t)value.size, scope, &embedded);
            }
        }
        if (!status && block && (begin < 0 || ud_tell(file->file) - begin != size))
            status = UD_CORRUPT;
        entries += (uint32_t)count;
        if (status)
            break;
    }
    if (!status && (!got_schema || fread(file->sync, 1, 16, file->file) != 16))
        status = UD_CORRUPT;
    if (!status) {
        ud_json *resolved = NULL; status = resolve_names(file->memory,schema,&resolved);
        if (!status) { ud_json_destroy(schema); schema = resolved; }
    }
    if (!status && expected && !reader_schema.length) {
        if (embedded && ud_batch_columns(expected) != ud_batch_columns(embedded))
            status = UD_INCOMPATIBLE;
        for (uint32_t at = 0; !status && embedded && at < ud_batch_columns(expected); at++) {
            ud_field a, b;
            status = ud_batch_field(expected, at, &a);
            if (!status)
                status = ud_batch_field(embedded, at, &b);
            if (!status && !ud_field_matches(&a, &b, UD_FIELD_LAYOUT))
                status = UD_INCOMPATIBLE;
        }
        if (!status)
            status = ud_batch_create(file->memory, expected->schema->fields, expected->columns, 0,
                                     &file->schema);
    } else if (!status && embedded) {
        file->schema = embedded;
        embedded = NULL;
    } else if (!status) {
        ud_field root = {0};
        status = infer_field(file->memory, schema, &root, 0);
        if (!status && root.type != UD_STRUCT)
            status = UD_SCHEMA;
        if (!status)
            status = ud_batch_create_named(file->memory, root.children, root.child_count, 0, scope,
                                           &file->schema);
        infer_clear(&root);
    }
    if (!status) status = schema_bind(file,schema);
    if (!status && reader_schema.length) status = bind_resolution(file,schema,reader_schema,expected,scope);
    if (!status && reader_schema.length) { file->writer_schema = schema; schema = NULL; }
    ud_batch_destroy(embedded);
    ud_json_destroy(schema);
    ud_bytes_clear(&key);
    ud_bytes_clear(&value);
    return status;
}
ud_status ud_avro_reader_open_schema(ud_context *memory, const char *path, const ud_batch *expected,
                              ud_string reader_schema, const ud_avro_options *options, ud_avro_reader **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!memory || !path || (!expected && !reader_schema.length))
        return UD_INVALID;
    ud_avro_reader *reader = ud_alloc(memory, sizeof(*reader));
    if (!reader)
        return UD_RESOURCE;
    reader->state.memory = memory;
    ud_status status = options_read(options, &reader->state.options);
    if (!status && options && options->records)
        status = ud_record_options_validate(memory, options->records);
    if (!status && options && options->records)
        reader->state.records = *options->records;
    else if (!status)
        reader->state.records =
            (ud_record_options){.struct_size = sizeof(ud_record_options),
                                .abi_version = UD_ABI_VERSION,
                                .errors = {.struct_size = sizeof(ud_error_context),
                                           .abi_version = UD_ABI_VERSION,
                                           .memory = memory,
                                           .component_id = {"avro", 4},
                                           .field_scope = {1, 1}}};
    if (!status && !reader->state.records.source_scope.high &&
        !reader->state.records.source_scope.low)
        status = ud_id_make(memory, (ud_string){"avro-file", 9},
                            (ud_string){path, (uint32_t)strlen(path)},
                            &reader->state.records.source_scope);
    reader->state.block = (ud_bytes){.ctx = memory, .limit = reader->state.options.max_block_bytes};
    if (!status)
        status = ud_open_regular(memory, path, &reader->state.file);
    if (!status)
        status = header_read(&reader->state, expected, (ud_string){"avro", 4}, reader_schema);
    if (status) {
        close_file(&reader->state);
        ud_free(reader);
    } else
        *out = reader;
    return status;
}
static ud_status block_read(avro_file *file) {
    if (file->block.pos != file->block.size)
        return UD_CORRUPT;
    int64_t count, size;
    UD_TRY(file_long(file, &count, 1));
    UD_TRY(file_long(file, &size, 0));
    if (count < 1 || count > UINT32_MAX || size < 0 || size > file->options.max_block_bytes)
        return UD_CORRUPT;
    ud_bytes packed = {.ctx = file->memory, .limit = file->options.max_block_bytes};
    uint8_t sync[16];
    ud_status status = UD_OK;
    uint8_t data[16384];
    int64_t left = size;
    while (!status && left) {
        status = check(file);
        if (status)
            break;
        size_t chunk = left > (int64_t)sizeof(data) ? sizeof(data) : (size_t)left;
        if (fread(data, 1, chunk, file->file) != chunk)
            status = ferror(file->file) ? UD_IO : UD_CORRUPT;
        if (!status)
            status = ud_bytes_put(&packed, data, chunk);
        left -= (int64_t)chunk;
    }
    if (!status && (fread(sync, 1, 16, file->file) != 16 || memcmp(sync, file->sync, 16)))
        status = UD_CORRUPT;
    file->block.size = file->block.pos = 0;
    if (!status && file->codec == UD_AVRO_NULL)
        status = ud_bytes_put(&file->block, packed.data, packed.size);
    else if (!status) {
        z_stream stream = {.zalloc = avro_z_allocate, .zfree = avro_z_free, .opaque = file->memory};
        if (inflateInit2(&stream, -15) != Z_OK)
            status = UD_RESOURCE;
        else {
            stream.next_in = packed.data;
            stream.avail_in = (uInt)packed.size;
            int result = Z_OK;
            do {
                status = check(file);
                stream.next_out = data;
                stream.avail_out = sizeof(data);
                if (!status) {
                    result = inflate(&stream, Z_NO_FLUSH);
                    if (result != Z_OK && result != Z_STREAM_END)
                        status = UD_CORRUPT;
                    if (!status)
                        status = ud_bytes_put(&file->block, data, sizeof(data) - stream.avail_out);
                    if (!status && result == Z_OK && !stream.avail_in &&
                        stream.avail_out == sizeof(data))
                        status = UD_CORRUPT;
                }
            } while (!status && result != Z_STREAM_END);
            if (!status && stream.avail_in)
                status = UD_CORRUPT;
            inflateEnd(&stream);
        }
    }
    ud_bytes_clear(&packed);
    if (!status)
        file->remaining = (uint64_t)count;
    return status;
}
ud_status ud_avro_reader_next(ud_avro_reader *reader, ud_batch **accepted, ud_batch **errors) {
    if (!reader || !accepted || !errors)
        return UD_INVALID;
    *accepted = *errors = NULL;
    avro_file *file = &reader->state;
    if (file->poisoned)
        return UD_CORRUPT;
    uint32_t columns = file->schema->columns;
    const ud_batch *output_schema = file->projection ? file->projection : file->schema;
    ud_batch *rows = NULL, *bad = NULL;
    ud_status status = ud_batch_create(file->memory, output_schema->schema->fields, output_schema->columns,
                                       file->options.batch_rows, &rows);
    if (!status)
        status = ud_error_batch(&file->records.errors, file->records.include_input,
                                 file->options.batch_rows, &bad);
    ud_value *values = ud_alloc(file->memory, columns * sizeof(*values));
    if (!values)
        status = UD_RESOURCE;
    int end = 0;
    uint32_t failed_column = UINT32_MAX;
    for (uint32_t at = 0; !status && at < file->options.batch_rows; at++) {
        status = check(file);
        if (!status && !file->remaining) {
            status = block_read(file);
            if (status == UD_END) {
                status = UD_OK;
                end = 1;
                break;
            }
        }
        size_t begin = file->block.pos;
        ud_status conversion = UD_OK;
        uint32_t column = UINT32_MAX;
        for (uint32_t c = 0; !status && c < columns; c++) {
            ud_status decoded = decode_value(file, &file->block, &file->schema->column[c].field,
                                             file->types + c, values + c, 0);
            /* Decoder errors can lose record boundaries. Only validations after
             * successful physical decoding are eligible for redirection. */
            if (decoded) {
                status = decoded; failed_column = c;
                break;
            }
        }
        ud_batch *single = NULL;
        if (!status)
            status =
                ud_batch_create(file->memory, file->schema->schema->fields, columns, 1, &single);
        if (!status) {
            for (uint32_t c = 0; !conversion && c < columns; c++) {
                conversion = ud_dictionary_validate(&file->schema->column[c].field, values + c);
                if (conversion)
                    column = c;
            }
            if (!conversion)
                conversion = ud_batch_append(single, values, columns);
        }
        ud_row_info identity = {0};
        if (!status)
            status = ud_row_info_source(file->records.source_scope, file->ordinal, &identity);
        ud_value resolved = {0};
        if (!status && !conversion && file->projection) {
            ud_field writer = {.type = UD_STRUCT,.children = file->schema->schema->fields,.child_count = columns};
            ud_field reader_field = {.type = UD_STRUCT,.children = output_schema->schema->fields,.child_count = output_schema->columns};
            ud_value input = {.type = UD_STRUCT,.children = values,.child_count = columns};
            conversion = resolve_value(file,file->writer_schema,file->reader_schema,&writer,&reader_field,&input,&resolved,0);
        }
        if (!status && !conversion) {
            status = ud_batch_append(rows,file->projection ? resolved.children : values,output_schema->columns);
            if (!status) status = ud_batch_set_row_info(rows,ud_batch_rows(rows)-1,&identity);
        } else if (!status && (file->records.policy == UD_ERROR_FAIL ||
                               !ud_record_error_recoverable(conversion)))
            status = conversion;
        else if (!status && file->records.policy == UD_ERROR_REDIRECT)
            status = ud_error_append(
                &file->records.errors, bad,
                &(ud_record_error){.struct_size = sizeof(ud_record_error),
                                   .abi_version = UD_ABI_VERSION,
                                   .source = identity,
                                   .rule = {"avro.convert", 12},
                                   .rule_version = 1,
                                   .column = column < columns
                                                 ? file->schema->column[column].field.identity
                                                 : (ud_id){0},
                                   .code = conversion,
                                   .policy = file->records.policy,
                                   .input_format = {"Avro", 4},
                                   .raw = file->block.data + begin,
                                   .raw_length = (uint32_t)(file->block.pos - begin)});
        value_release(&resolved);
        ud_batch_destroy(single);
        for (uint32_t c = 0; values && c < columns; c++) {
            value_release(values + c);
            memset(values + c, 0, sizeof(*values));
        }
        if (!status) {
            file->remaining--;
            file->ordinal++;
        }
    }
    ud_free(values);
    if (!status && !file->remaining && file->block.pos != file->block.size)
        status = UD_CORRUPT;
    if (!status && ud_batch_rows(rows))
        status = ud_batch_seal(rows);
    if (!status && ud_batch_rows(bad))
        status = ud_batch_seal(bad);
    if (status) {
        (void)ud_fail(file->memory,status,
            "Avro record boundary failed: row=%llu field=%u byte=%llu blockBytes=%llu remaining=%llu",
            (unsigned long long)file->ordinal,failed_column,(unsigned long long)file->block.pos,
            (unsigned long long)file->block.size,(unsigned long long)file->remaining);
        file->poisoned = 1;
        ud_batch_destroy(rows);
        ud_batch_destroy(bad);
        return status;
    }
    if (ud_batch_rows(rows))
        *accepted = rows;
    else
        ud_batch_destroy(rows);
    if (ud_batch_rows(bad))
        *errors = bad;
    else
        ud_batch_destroy(bad);
    return end && !*accepted && !*errors ? UD_END : UD_OK;
}
void ud_avro_reader_close(ud_avro_reader *reader) {
    if (reader) {
        close_file(&reader->state);
        ud_free(reader);
    }
}
ud_status ud_avro_discover(ud_context *memory, const char *path, ud_string scope, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    avro_file file = {.memory = memory};
    ud_status status = options_read(NULL, &file.options);
    if (!status)
        status = ud_open_regular(memory, path, &file.file);
    if (!status)
        status = header_read(&file, NULL, scope, (ud_string){0});
    if (!status) {
        *out = file.schema;
        file.schema = NULL;
    }
    if (file.types) {
        for (uint32_t at = 0; at < ud_batch_columns(file.schema ? file.schema : *out); at++)
            type_clear(file.types + at);
        ud_free(file.types);
        file.types = NULL;
    }
    close_file(&file);
    return status;
}
ud_status ud_avro_writer_open_schema(ud_context *memory, const char *path, const ud_batch *schema,
                              ud_string writer_schema, const ud_avro_options *options, ud_avro_writer **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!memory || !path || !schema)
        return UD_INVALID;
    ud_avro_writer *writer = ud_alloc(memory, sizeof(*writer));
    if (!writer)
        return UD_RESOURCE;
    avro_file *file = &writer->state;
    file->memory = memory;
    ud_status status = options_read(options, &file->options);
    file->codec = file->options.codec;
    file->block = (ud_bytes){.ctx = memory, .limit = file->options.max_block_bytes};
    ud_bytes avro = {.ctx = memory, .limit = file->options.max_block_bytes},
             header = {.ctx = memory, .limit = file->options.max_block_bytes};
    uint32_t serial = 0;
    char *native = NULL;
    uint32_t native_size = 0;
    ud_json *document = NULL;
    if (!status)
        status = ud_batch_create(memory, schema->schema->fields, schema->columns, 0, &file->schema);
    if (!status) status = writer_schema.length ? ud_bytes_put(&avro,writer_schema.data,writer_schema.length)
        : schema_record(&avro,schema->schema->fields,schema->columns,0,&serial);
    if (!status)
        status =
            ud_json_parse(memory, (const char *)avro.data, (uint32_t)avro.size, &document, NULL);
    if (!status) {
        ud_json *resolved = NULL; status = resolve_names(memory,document,&resolved);
        if (!status) status = schema_bind(file,resolved);
        ud_json_destroy(resolved);
    }
    if (!status)
        status = ud_schema_to_json(schema, &native, &native_size);
    if (!status)
        status = ud_bytes_put(&header, "Obj\1", 4);
    if (!status)
        status = put_long(&header, 3);
    if (!status)
        status = put_string(&header, "avro.schema", 11);
    if (!status)
        status = put_string(&header, avro.data, (uint32_t)avro.size);
    if (!status)
        status = put_string(&header, "avro.codec", 10);
    if (!status)
        status = put_string(&header, file->codec ? "deflate" : "null", file->codec ? 7 : 4);
    if (!status)
        status = put_string(&header, "ud.schema", 9);
    if (!status)
        status = put_string(&header, native, native_size);
    if (!status)
        status = put_long(&header, 0);
    if (!status)
        status = ud_random_bytes(memory, file->sync, 16);
    if (!status)
        status = ud_bytes_put(&header, file->sync, 16);
    if (!status)
        status = ud_path_no_symlinks(memory, path);
    if (!status) {
        file->file = ud_fopen(path, "wb");
        if (!file->file)
            status = UD_IO;
    }
    if (!status && fwrite(header.data, 1, header.size, file->file) != header.size)
        status = UD_IO;
    ud_json_destroy(document);
    ud_free(native);
    ud_bytes_clear(&avro);
    ud_bytes_clear(&header);
    if (status) {
        close_file(file);
        ud_free(writer);
    } else
        *out = writer;
    return status;
}
static ud_status write_block(avro_file *file, uint32_t rows) {
    ud_status status = check(file);
    ud_bytes packed = {.ctx = file->memory, .limit = file->options.max_block_bytes},
             frame = {.ctx = file->memory};
    if (!status && file->codec == UD_AVRO_NULL)
        status = ud_bytes_put(&packed, file->block.data, file->block.size);
    else if (!status) {
        z_stream stream = {.zalloc = avro_z_allocate, .zfree = avro_z_free, .opaque = file->memory};
        if (deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY) !=
            Z_OK)
            status = UD_RESOURCE;
        else {
            stream.next_in = file->block.data;
            stream.avail_in = (uInt)file->block.size;
            uint8_t output[16384];
            int result = Z_OK;
            do {
                status = check(file);
                stream.next_out = output;
                stream.avail_out = sizeof(output);
                if (!status) {
                    result = deflate(&stream, Z_FINISH);
                    if (result != Z_OK && result != Z_STREAM_END)
                        status = UD_IO;
                    if (!status)
                        status = ud_bytes_put(&packed, output, sizeof(output) - stream.avail_out);
                }
            } while (!status && result != Z_STREAM_END);
            deflateEnd(&stream);
        }
    }
    if (!status)
        status = put_long(&frame, rows);
    if (!status)
        status = put_long(&frame, (int64_t)packed.size);
    if (!status && (fwrite(frame.data, 1, frame.size, file->file) != frame.size ||
                    fwrite(packed.data, 1, packed.size, file->file) != packed.size ||
                    fwrite(file->sync, 1, 16, file->file) != 16))
        status = UD_IO;
    ud_bytes_clear(&packed);
    ud_bytes_clear(&frame);
    return status;
}

ud_status ud_avro_writer_append(ud_avro_writer *writer, const ud_batch *batch) {
    if (!writer || !batch)
        return UD_INVALID;
    avro_file *file = &writer->state;
    if (file->poisoned)
        return UD_IO;
    if (batch->columns != file->schema->columns)
        return UD_INCOMPATIBLE;
    for (uint32_t at = 0; at < batch->columns; at++)
        if (!ud_field_matches(&batch->column[at].field, &file->schema->column[at].field,
                              UD_FIELD_PORT))
            return UD_INCOMPATIBLE;
    ud_bytes record = {.ctx = file->memory, .limit = file->options.max_block_bytes};
    file->block.size = file->block.pos = 0;
    ud_status status = check(file);
    uint32_t count = 0;
    for (uint32_t row = 0; !status && row < batch->rows; row++) {
        record.size = record.pos = 0;
        for (uint32_t column = 0; !status && column < batch->columns; column++) {
            ud_value value;
            status = ud_batch_get(batch, row, column, &value);
            if (!status)
                status = encode_value(&record, &file->schema->column[column].field,
                                      file->types + column, &value, 0);
        }
        if (!status && count && record.size > file->options.max_block_bytes - file->block.size) {
            status = write_block(file, count);
            file->block.size = file->block.pos = 0;
            count = 0;
        }
        if (!status)
            status = ud_bytes_put(&file->block, record.data, record.size);
        if (!status)
            count++;
    }
    if (!status && count)
        status = write_block(file, count);
    ud_bytes_clear(&record);
    if (status)
        file->poisoned = 1;
    return status;
}
ud_status ud_avro_writer_close(ud_avro_writer *writer) {
    if (!writer)
        return UD_OK;
    avro_file *file = &writer->state;
    ud_status status = file->poisoned ? UD_IO : ud_flush(file->memory, file->file);
    close_file(file);
    ud_free(writer);
    return status;
}

ud_status ud_avro_reader_open(ud_context *memory,const char *path,const ud_batch *schema,const ud_avro_options *options,ud_avro_reader **out) {
    return ud_avro_reader_open_schema(memory,path,schema,(ud_string){0},options,out);
}
ud_status ud_avro_writer_open(ud_context *memory,const char *path,const ud_batch *schema,const ud_avro_options *options,ud_avro_writer **out) {
    return ud_avro_writer_open_schema(memory,path,schema,(ud_string){0},options,out);
}
