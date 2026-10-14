/* Private Avro schema resolution inside the single Native codec. No execution,
 * data model, checkpoint or conversion implementation is duplicated here. */
static const ud_json *avro_kind(const ud_json *schema) {
    return ud_json_kind(schema) == UD_JSON_STRING ? schema : member(schema,"type");
}
static int avro_text_same(const ud_json *left,const ud_json *right) {
    ud_string a,b;
    return !ud_json_text(left,&a.data,&a.length) && !ud_json_text(right,&b.data,&b.length) && a.length == b.length && !memcmp(a.data,b.data,a.length);
}
static int nullable_union(const ud_json *schema,int *null_index) {
    if (ud_json_kind(schema) != UD_JSON_ARRAY || ud_json_size(schema) != 2) return 0;
    int a = null_schema(ud_json_at(schema,0)), b = null_schema(ud_json_at(schema,1));
    if (a == b) return 0;
    *null_index = a ? 0 : 1; return 1;
}
static int avro_alias(const ud_json *writer_name,const ud_json *reader,const char *name_key,int qualified) {
    const ud_json *reader_name = member(reader,name_key);
    if (avro_text_same(writer_name,reader_name)) return 1;
    ud_string wanted,current;
    if (ud_json_text(writer_name,&wanted.data,&wanted.length) || ud_json_text(reader_name,&current.data,&current.length)) return 0;
    const ud_json *aliases = member(reader,"aliases");
    for (uint32_t at = 0; at < ud_json_size(aliases); at++) {
        ud_string alias;
        if (ud_json_text(ud_json_at(aliases,at),&alias.data,&alias.length)) continue;
        if (alias.length == wanted.length && !memcmp(alias.data,wanted.data,wanted.length)) return 1;
        if (qualified && !memchr(alias.data,'.',alias.length)) {
            uint32_t prefix = current.length;
            while (prefix && current.data[prefix-1] != '.') prefix--;
            if (prefix+alias.length == wanted.length && !memcmp(current.data,wanted.data,prefix) && !memcmp(alias.data,wanted.data+prefix,alias.length)) return 1;
        }
    }
    return 0;
}
static int writer_field(const ud_json *writer,const ud_json *reader_field) {
    const ud_json *fields = member(writer,"fields");
    for (uint32_t at = 0; at < ud_json_size(fields); at++)
        if (avro_alias(member(ud_json_at(fields,at),"name"),reader_field,"name",0)) return (int)at;
    return -1;
}
static int schema_compatible(const ud_json *writer,const ud_json *reader,uint32_t depth) {
    if (depth > 32) return 0;
    if (ud_json_kind(writer) == UD_JSON_ARRAY) {
        for (uint32_t at = 0; at < ud_json_size(writer); at++) if (!schema_compatible(ud_json_at(writer,at),reader,depth+1)) return 0;
        return 1;
    }
    if (ud_json_kind(reader) == UD_JSON_ARRAY) {
        for (uint32_t at = 0; at < ud_json_size(reader); at++) if (schema_compatible(writer,ud_json_at(reader,at),depth+1)) return 1;
        return 0;
    }
    const ud_json *w = avro_kind(writer), *r = avro_kind(reader);
    const ud_json *wl = member(writer,"logicalType"), *rl = member(reader,"logicalType");
    if (wl || rl) {
        int timestamp = (named(wl,"timestamp-micros") || named(wl,"timestamp-millis")) && (named(rl,"timestamp-micros") || named(rl,"timestamp-millis"));
        int time = (named(wl,"time-micros") || named(wl,"time-millis")) && (named(rl,"time-micros") || named(rl,"time-millis"));
        if (!timestamp && !time && !avro_text_same(wl,rl)) return 0;
    }
    if (named(w,"record") && named(r,"record")) {
        if (!avro_alias(member(writer,"name"),reader,"name",1)) return 0;
        const ud_json *fields = member(reader,"fields");
        for (uint32_t at = 0; at < ud_json_size(fields); at++) {
            const ud_json *field = ud_json_at(fields,at); int index = writer_field(writer,field);
            if (index < 0) { if (!member(field,"default")) return 0; }
            else if (!schema_compatible(member(ud_json_at(member(writer,"fields"),(uint32_t)index),"type"),member(field,"type"),depth+1)) return 0;
        }
        return 1;
    }
    if (named(w,"array") && named(r,"array")) return schema_compatible(member(writer,"items"),member(reader,"items"),depth+1);
    if (named(w,"map") && named(r,"map")) return schema_compatible(member(writer,"values"),member(reader,"values"),depth+1);
    if (named(w,"enum") && named(r,"enum")) return avro_alias(member(writer,"name"),reader,"name",1);
    if (named(w,"fixed") && named(r,"fixed")) return avro_alias(member(writer,"name"),reader,"name",1) && avro_text_same(member(writer,"size"),member(reader,"size"));
    if (avro_text_same(w,r)) return 1;
    if (named(w,"int") && (named(r,"long") || named(r,"float") || named(r,"double"))) return 1;
    if (named(w,"long") && (named(r,"float") || named(r,"double"))) return 1;
    if (named(w,"float") && named(r,"double")) return 1;
    return (named(w,"string") && named(r,"bytes")) || (named(w,"bytes") && named(r,"string"));
}
static ud_status default_bytes(ud_bytes *output,const ud_json *value,int32_t fixed) {
    ud_string string; UD_TRY(ud_json_text(value,&string.data,&string.length));
    if (ud_json_kind(value) != UD_JSON_STRING) return UD_SCHEMA;
    ud_bytes bytes = {.ctx = output->ctx}; ud_status status = UD_OK;
    for (uint32_t at = 0; !status && at < string.length;) {
        uint32_t point = (uint8_t)string.data[at++];
        if (point >= 0x80) {
            uint32_t count = point < 0xE0 ? 1 : point < 0xF0 ? 2 : 3;
            point &= count == 1 ? 31u : count == 2 ? 15u : 7u;
            if (count > string.length-at) { status = UD_SCHEMA; break; }
            while (count--) point = (point << 6) | ((uint8_t)string.data[at++] & 63u);
        }
        if (point > 255) { status = UD_SCHEMA; break; }
        uint8_t byte = (uint8_t)point; status = ud_bytes_put(&bytes,&byte,1);
    }
    if (!status && fixed >= 0 && bytes.size != (size_t)fixed) status = UD_SCHEMA;
    if (!status && fixed < 0) status = put_long(output,(int64_t)bytes.size);
    if (!status) status = ud_bytes_put(output,bytes.data,bytes.size);
    ud_bytes_clear(&bytes); return status;
}
static ud_status encode_default(ud_bytes *output,const ud_json *schema,const ud_field *field,const avro_type *type,const ud_json *value,uint32_t depth) {
    if (depth > 32 || !value) return UD_SCHEMA;
    if (type->kind == AV_UNION) {
        UD_TRY(put_long(output,0)); return encode_default(output,ud_json_at(schema,0),field->children+1,type->children,value,depth+1);
    }
    if (type->nullable) {
        UD_TRY(put_long(output,0));
        if (type->null_branch == 0) return ud_json_kind(value) == UD_JSON_NULL ? UD_OK : UD_SCHEMA;
        avro_type nonnull = *type; nonnull.nullable = 0;
        return encode_default(output,ud_json_at(schema,0),field,&nonnull,value,depth+1);
    }
    switch(type->kind) {
    case AV_NULL: return ud_json_kind(value) == UD_JSON_NULL ? UD_OK : UD_SCHEMA;
    case AV_BOOLEAN: {
        if (ud_json_kind(value) != UD_JSON_BOOL) return UD_SCHEMA;
        uint8_t boolean = named(value,"true"); return ud_bytes_put(output,&boolean,1);
    }
    case AV_INT:
    case AV_LONG: {
        if (ud_json_kind(value) != UD_JSON_NUMBER) return UD_SCHEMA;
        int64_t integer; UD_TRY(number(value,&integer));
        if (type->kind == AV_INT && (integer < INT32_MIN || integer > INT32_MAX)) return UD_OVERFLOW;
        return put_long(output,integer);
    }
    case AV_FLOAT:
    case AV_DOUBLE: {
        ud_string text; ud_value real;
        if (ud_json_kind(value) != UD_JSON_NUMBER) return UD_SCHEMA;
        UD_TRY(ud_json_text(value,&text.data,&text.length)); UD_TRY(ud_value_parse(output->ctx,UD_FLOAT64,text.data,text.length,&real));
        uint64_t bits;
        if (type->kind == AV_FLOAT) { float small = (float)real.real; if ((double)small != real.real) return UD_TRUNCATED; uint32_t word; memcpy(&word,&small,4); bits = word; }
        else memcpy(&bits,&real.real,8);
        return little_put(output,bits,type->kind == AV_FLOAT ? 4 : 8);
    }
    case AV_STRING: {
        ud_string text; if (ud_json_kind(value) != UD_JSON_STRING) return UD_SCHEMA;
        UD_TRY(ud_json_text(value,&text.data,&text.length)); return put_string(output,text.data,text.length);
    }
    case AV_BYTES: return default_bytes(output,value,-1);
    case AV_FIXED: return default_bytes(output,value,type->width);
    case AV_ENUM: {
        const ud_json *symbols = member(schema,"symbols");
        for (uint32_t at = 0; at < ud_json_size(symbols); at++) if (avro_text_same(value,ud_json_at(symbols,at))) return put_long(output,at);
        return UD_SCHEMA;
    }
    case AV_RECORD: {
        if (ud_json_kind(value) != UD_JSON_OBJECT) return UD_SCHEMA;
        const ud_json *fields = member(schema,"fields");
        for (uint32_t at = 0; at < type->count; at++) {
            const ud_json *declaration = ud_json_at(fields,at); ud_string name;
            UD_TRY(ud_json_text(member(declaration,"name"),&name.data,&name.length));
            const ud_json *item = ud_json_find(value,name.data,name.length); if (!item) item = member(declaration,"default");
            UD_TRY(encode_default(output,member(declaration,"type"),field->children+at,type->children+at,item,depth+1));
        }
        return UD_OK;
    }
    case AV_ARRAY:
    case AV_MAP: {
        if (ud_json_kind(value) != (type->kind == AV_ARRAY ? UD_JSON_ARRAY : UD_JSON_OBJECT)) return UD_SCHEMA;
        uint32_t count = ud_json_size(value); if (count) UD_TRY(put_long(output,count));
        for (uint32_t at = 0; at < count; at++) {
            const ud_field *child = field->children;
            if (type->kind == AV_MAP) {
                const char *key; uint32_t size; UD_TRY(ud_json_key(value,at,&key,&size)); UD_TRY(put_string(output,key,size)); child = child->children+1;
            }
            UD_TRY(encode_default(output,member(schema,type->kind == AV_ARRAY ? "items" : "values"),child,type->children,ud_json_at(value,at),depth+1));
        }
        return put_long(output,0);
    }
    default: return UD_UNSUPPORTED;
    }
}
static ud_status resolve_default(avro_file *file,const ud_json *schema,const ud_field *field,const ud_json *value,ud_value *out) {
    avro_default *cached = file->defaults;
    while (cached && (cached->schema != schema || cached->value != value)) cached = cached->next;
    if (!cached) {
        cached = ud_alloc(file->memory,sizeof(*cached)); if (!cached) return UD_RESOURCE;
        cached->schema = schema; cached->value = value; cached->wire = (ud_bytes){.ctx = file->memory,.limit = file->options.max_block_bytes};
        cached->next = file->defaults; file->defaults = cached;
        UD_TRY(type_bind(file->memory,schema,field,&cached->type,0));
        UD_TRY(encode_default(&cached->wire,schema,field,&cached->type,value,0));
    }
    cached->wire.pos = 0;
    UD_TRY(decode_value(file,&cached->wire,field,&cached->type,out,0));
    return cached->wire.pos == cached->wire.size ? UD_OK : UD_CORRUPT;
}
static ud_status resolve_value(avro_file *file,const ud_json *writer,const ud_json *reader,
    const ud_field *source,const ud_field *target,const ud_value *value,ud_value *out,uint32_t depth) {
    if (depth > 32) return UD_RESOURCE;
    *out = (ud_value){.type = target->type,.precision = target->precision,.scale = target->scale};
    if (ud_json_kind(writer) == UD_JSON_ARRAY) {
        int null_branch = 0; uint32_t branch;
        if (nullable_union(writer,&null_branch)) branch = value->is_null ? (uint32_t)null_branch : 1u-(uint32_t)null_branch;
        else {
            if (value->child_count != ud_json_size(writer)+1 || value->children[0].type != UD_INT64 || value->children[0].integer < 0 || (uint64_t)value->children[0].integer >= ud_json_size(writer)) return UD_CORRUPT;
            branch = (uint32_t)value->children[0].integer; source = source->children+branch+1; value = value->children+branch+1;
        }
        return resolve_value(file,ud_json_at(writer,branch),reader,source,target,value,out,depth+1);
    }
    if (ud_json_kind(reader) == UD_JSON_ARRAY) {
        uint32_t branch = 0; for (; branch < ud_json_size(reader); branch++) if (schema_compatible(writer,ud_json_at(reader,branch),depth+1)) break;
        if (branch == ud_json_size(reader)) return UD_INCOMPATIBLE;
        int null_branch;
        if (nullable_union(reader,&null_branch)) return resolve_value(file,writer,ud_json_at(reader,branch),source,target,value,out,depth+1);
        uint32_t count = ud_json_size(reader)+1;
        ud_value *children = ud_alloc(file->memory,count*sizeof(*children)); if (!children) return UD_RESOURCE;
        out->children = children; out->child_count = count;
        for (uint32_t at = 0; at < count; at++) children[at] = (ud_value){.type = target->children[at].type,.precision = target->children[at].precision,.scale = target->children[at].scale,.is_null = 1};
        children[0] = (ud_value){.type = UD_INT64,.integer = branch};
        return resolve_value(file,writer,ud_json_at(reader,branch),source,target->children+branch+1,value,children+branch+1,depth+1);
    }
    if (!schema_compatible(writer,reader,depth)) return UD_INCOMPATIBLE;
    if (value->is_null) { out->is_null = 1; return target->nullable ? UD_OK : UD_QUALITY; }
    const ud_json *kind = avro_kind(reader);
    if (named(kind,"record")) {
        uint32_t count = target->child_count; ud_value *children = ud_alloc(file->memory,count*sizeof(*children));
        if (!children) return UD_RESOURCE;
        out->children = children; out->child_count = count;
        const ud_json *fields = member(reader,"fields");
        for (uint32_t at = 0; at < count; at++) {
            const ud_json *field = ud_json_at(fields,at); int index = writer_field(writer,field);
            if (index < 0) UD_TRY(resolve_default(file,member(field,"type"),target->children+at,member(field,"default"),children+at));
            else UD_TRY(resolve_value(file,member(ud_json_at(member(writer,"fields"),(uint32_t)index),"type"),member(field,"type"),
                source->children+index,target->children+at,value->children+index,children+at,depth+1));
        }
        return UD_OK;
    }
    if (named(kind,"array") || named(kind,"map")) {
        uint32_t count = value->child_count; ud_value *children = count ? ud_alloc(file->memory,count*sizeof(*children)) : NULL;
        if (count && !children) return UD_RESOURCE;
        out->children = children; out->child_count = count;
        for (uint32_t at = 0; at < count; at++) {
            const ud_field *input = source->children,*output = target->children; const ud_value *item = value->children+at; ud_value *result = children+at;
            if (named(kind,"map")) {
                ud_value *pair = ud_alloc(file->memory,2*sizeof(*pair)); if (!pair) return UD_RESOURCE;
                children[at] = (ud_value){.type = UD_STRUCT,.children = pair,.child_count = 2}; pair[0] = item->children[0];
                item = item->children+1; input = input->children+1; output = output->children+1; result = pair+1;
            }
            UD_TRY(resolve_value(file,member(writer,named(kind,"map") ? "values" : "items"),member(reader,named(kind,"map") ? "values" : "items"),input,output,item,result,depth+1));
        }
        return UD_OK;
    }
    if (named(kind,"enum")) {
        uint64_t ordinal = value->unsigned_integer; if (!ordinal || ordinal > source->dictionary_count) return UD_CORRUPT;
        ud_string symbol = source->dictionary[ordinal-1];
        for (uint32_t at = 0; at < target->dictionary_count; at++) if (symbol.length == target->dictionary[at].length && !memcmp(symbol.data,target->dictionary[at].data,symbol.length)) { out->unsigned_integer = (uint64_t)at+1; return UD_OK; }
        const ud_json *fallback = member(reader,"default");
        for (uint32_t at = 0; at < ud_json_size(member(reader,"symbols")); at++) if (avro_text_same(fallback,ud_json_at(member(reader,"symbols"),at))) { out->unsigned_integer = (uint64_t)at+1; return UD_OK; }
        return UD_QUALITY;
    }
    if ((source->type == UD_UTF8 && target->type == UD_BINARY) || (source->type == UD_BINARY && target->type == UD_UTF8)) {
        if (target->type == UD_UTF8 && !ud_utf8_valid(value->text,value->text_length)) return UD_PARSE;
        *out = *value; out->type = target->type; return UD_OK;
    }
    if (source->type == target->type && source->precision == target->precision && source->scale == target->scale) { *out = *value; return UD_OK; }
    return ud_cast_value(file->memory,source,value,target,UD_ROUND_EXACT,out,depth);
}
static ud_status bind_resolution(avro_file *file,const ud_json *writer,ud_string reader_schema,const ud_batch *expected,ud_string scope) {
    ud_json *document = NULL; ud_status status = ud_json_parse(file->memory,reader_schema.data,reader_schema.length,&document,NULL);
    if (!status) status = resolve_names(file->memory,document,&file->reader_schema);
    ud_json_destroy(document);
    if (!status && !schema_compatible(writer,file->reader_schema,0)) status = UD_INCOMPATIBLE;
    ud_field root = {0}; ud_batch *inferred = NULL;
    if (!status) status = infer_field(file->memory,file->reader_schema,&root,0);
    if (!status && root.type != UD_STRUCT) status = UD_SCHEMA;
    if (!status) status = ud_batch_create_named(file->memory,root.children,root.child_count,0,scope,&inferred);
    infer_clear(&root);
    if (!status && expected) {
        if (expected->columns != inferred->columns) status = UD_INCOMPATIBLE;
        for (uint32_t at = 0; !status && at < inferred->columns; at++) if (!ud_field_matches(&expected->column[at].field,&inferred->column[at].field,UD_FIELD_LAYOUT)) status = UD_INCOMPATIBLE;
        if (!status) status = ud_batch_create(file->memory,expected->schema->fields,expected->columns,0,&file->projection);
    } else if (!status) { file->projection = inferred; inferred = NULL; }
    ud_batch_destroy(inferred); return status;
}
