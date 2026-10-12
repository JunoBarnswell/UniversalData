#include "../package/internal.h"
#include "ud/connection_manager.h"
enum { COLLECTION=1, VARIABLE, FILES, QUERY, SCHEMA };
typedef struct enumerator {
    ud_component_context context;
    ud_json *config;
    ud_batch *schema, *page;
    ud_connection_lease *lease;
    ud_value list;
    char **files;
    uint32_t file_count, page_row;
    uint64_t position, metadata_offset;
    int kind, ended;
} enumerator;
static ud_status definition(const ud_component_context *c,ud_string text,ud_json **config,ud_field *field) {
    UD_TRY(ud_json_parse(c->memory,text.data,text.length,config,NULL));
    static const char *keys[]={"variable","items","enumerator","fromVariable","connection","query","table","suffix","kind"};
    UD_TRY(ud_json_keys(*config,keys,9));
    ud_string variable; UD_TRY(ud_json_string(ud_member(*config,"variable"),&variable));
    UD_TRY(ud_activation_variable_field(c->activation,variable,field));
    ud_variable *writable; return ud_variable_writable(c->activation,variable,&writable);
}
static ud_status configure(void *user,const ud_component_context *c,ud_string text,
                           const ud_batch *const *inputs,ud_batch **out) {
    (void)inputs;
    int kind=(int)(intptr_t)user;
    ud_json *config=NULL;ud_field field={0};
    ud_status status=definition(c,text,&config,&field);
    const ud_json *items=ud_member(config,"items");
    if (!status && kind==COLLECTION && ud_json_kind(items)!=UD_JSON_ARRAY) status=UD_SCHEMA;
    for (uint32_t i=0;!status && kind==COLLECTION && i<ud_json_size(items);i++) {
        ud_value value={0};status=ud_json_value_decode(c->memory,&field,ud_json_at(items,i),UD_ROUND_EXACT,&value);
        ud_clear_value(&value);
    }
    if (!status && kind==VARIABLE) {
        ud_string name;ud_value list;
        status=ud_json_string(ud_member(config,"fromVariable"),&name);
        ud_field source={0};
        if (!status) status=ud_activation_variable_field(c->activation,name,&source);
        if (!status) status=ud_activation_variable(c->activation,name,&list);
        /* The collection's element definition may have a distinct name and
         * nullable policy from the loop variable. Enumeration yields values;
         * validate value semantics here and let foreach assignment enforce
         * the destination variable's declared contract. */
        if (!status && (source.type!=UD_LIST || source.child_count!=1 || list.is_null ||
            !ud_field_matches(source.children,&field,UD_FIELD_VALUE_TYPE))) status=UD_SCHEMA;
    }
    if (!status && (kind==FILES || kind==SCHEMA) && field.type!=UD_UTF8) status=UD_SCHEMA;
    if (!status && kind>=FILES) {
        ud_string id; status=ud_json_string(ud_member(config,"connection"),&id);
        const ud_json *conn;int project;
        if (!status) status=ud_connection_definition(c->activation,id,&conn,&project);
    }
    if (!status && kind==QUERY) {
        ud_string sql;status=ud_json_string(ud_member(config,"query"),&sql);
    }
    if (!status) status=ud_batch_create(c->memory,&field,1,0,out);
    ud_json_destroy(config);return status;
}
static void dispose(void *state,ud_status outcome) {
    (void)outcome;enumerator *e=state;if (!e) return;
    if (e->lease) {
        if (e->kind==QUERY) ud_connection_lease_read_close(e->lease);
        ud_connection_lease_release(e->lease);
    }
    ud_batch_destroy(e->schema);ud_batch_destroy(e->page);
    ud_list_free(e->files,e->file_count);ud_json_destroy(e->config);ud_free(e);
}
static ud_status prepare(void *user,const ud_component_context *c,ud_string text,
                         const ud_batch *const *in,const ud_batch *const *out,void **state) {
    (void)in;*state=NULL;
    enumerator *e=ud_alloc(c->memory,sizeof(*e));if (!e) return UD_RESOURCE;
    e->context=*c;e->kind=(int)(intptr_t)user;
    ud_field field={0};ud_status status=definition(c,text,&e->config,&field);
    if (!status) status=ud_batch_create(c->memory,&field,1,0,&e->schema);
    (void)out;
    if (!status && e->kind==VARIABLE) {
        ud_string name;status=ud_json_string(ud_member(e->config,"fromVariable"),&name);
        if (!status) status=ud_activation_variable(c->activation,name,&e->list);
    }
    ud_string id={0};
    if (!status && e->kind>=FILES) status=ud_json_string(ud_member(e->config,"connection"),&id);
    if (!status && e->kind==FILES) {
        status=ud_acquire_file_connection(c->activation,id,&e->lease);
        ud_string resource={0},suffix={"",0};
        if (!status) status=ud_connection_lease_authorize(e->lease,UD_ACCESS_DISCOVER);
        if (!status) status=ud_connection_lease_path(e->lease,&resource);
        if (!status && ud_member(e->config,"suffix")) status=ud_json_string(ud_member(e->config,"suffix"),&suffix);
        char *directory=!status ? ud_strdup_n(c->memory,resource.data,resource.length) : NULL;
        char *ending=!status ? ud_strdup_n(c->memory,suffix.data,suffix.length) : NULL;
        if (!status && (!directory || !ending)) status=UD_RESOURCE;
        if (!status) status=ud_path_no_symlinks(c->memory,directory);
        if (!status) status=ud_list(c->memory,directory,ending,&e->files,&e->file_count);
        ud_bytes snapshot={.ctx=c->memory};
        for (uint32_t i=0;!status && i<e->file_count;i++) {
            char *path=NULL;status=ud_join_path(c->memory,directory,e->files[i],&path);
            int kind=0;
            if (!status) status=ud_path_no_symlinks(c->memory,path);
            if (!status) status=ud_path_kind(c->memory,path,&kind);
            ud_free(e->files[i]);e->files[i]=path;
            if (!status && kind==1) status=ud_put_string(&snapshot,path,(uint32_t)strlen(path));
            else if (!status) { ud_free(path);e->files[i]=NULL; }
        }
        if (!status) {
            uint8_t digest[32];ud_hash(snapshot.data,snapshot.size,digest);
            status=ud_checkpoint_enumerator(c->activation,digest);
        }
        ud_bytes_clear(&snapshot);ud_free(directory);ud_free(ending);
    } else if (!status && e->kind==QUERY) {
        status=ud_activation_acquire_connection(c->activation,id,UD_CONNECT_READ,&e->lease);
        ud_string sql; if (!status) status=ud_json_string(ud_member(e->config,"query"),&sql);
        ud_read_options options={sizeof(options),UD_ABI_VERSION,c->max_batch_rows,sql};
        if (!status) status=ud_connection_lease_read_open(e->lease,&options,e->schema);
    } else if (!status && e->kind==SCHEMA)
        status=ud_activation_acquire_connection(c->activation,id,UD_CONNECT_METADATA,&e->lease);
    if (status) { dispose(e,status);return status; }
    *state=e;return UD_OK;
}
static ud_status next(void *state,ud_batch **out) {
    enumerator *e=state;
    if (!out || !e) return UD_INVALID;
    *out=NULL;
    UD_TRY(ud_activation_check(e->context.activation));
    if (e->ended) return UD_END;
    ud_field field=e->schema->column[0].field;
    ud_value value={0};ud_status status=UD_OK;
    int decoded=0;
    if (e->kind==COLLECTION) {
        const ud_json *items=ud_member(e->config,"items");
        if (e->position>=ud_json_size(items)) { e->ended=1;return UD_END; }
        status=ud_json_value_decode(e->context.memory,&field,ud_json_at(items,(uint32_t)e->position),UD_ROUND_EXACT,&value);
        decoded=1;
    } else if (e->kind==VARIABLE) {
        if (e->position>=e->list.child_count) { e->ended=1;return UD_END; }
        value=e->list.children[e->position];
        /* The list element has its own schema name and nullability. Emit the
         * enumerator's declared output field while retaining the element value. */
        value.type=field.type;
        value.precision=field.precision;
        value.scale=field.scale;
    } else if (e->kind==FILES) {
        while (e->position<e->file_count && !e->files[e->position]) e->position++;
        if (e->position==e->file_count) { e->ended=1;return UD_END; }
        UD_TRY(ud_connection_lease_authorize(e->lease,UD_ACCESS_READ));
        const char *path=e->files[e->position];
        value=(ud_value){.type=UD_UTF8,.text=path,.text_length=(uint32_t)strlen(path)};
    } else {
        while (!e->page || e->page_row==e->page->rows) {
            ud_batch_destroy(e->page);e->page=NULL;e->page_row=0;
            if (e->kind==QUERY) status=ud_connection_lease_read_next(e->lease,&e->page);
            else {
                ud_string table={0};status=ud_json_string(ud_member(e->config,"table"),&table);
                ud_metadata_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,
                    .kind=UD_METADATA_COLUMNS,.parent=table,.offset=e->metadata_offset,.limit=e->context.max_batch_rows};
                if (!status) status=ud_connection_lease_metadata(e->lease,&options,&e->page);
                if (!status && !e->page->rows) status=UD_END;
                if (!status) e->metadata_offset+=e->page->rows;
            }
            if (status==UD_END) { e->ended=1;return UD_END; }
            if (status) return status;
        }
        status=ud_batch_get(e->page,e->page_row++,0,&value);
    }
    if (!status) status=ud_batch_create(e->context.memory,&field,1,1,out);
    if (!status) status=ud_batch_append(*out,&value,1);
    if (!status) status=ud_batch_seal(*out);
    if (decoded) ud_clear_value(&value);
    if (status) { ud_batch_destroy(*out);*out=NULL;return status; }
    e->position++;return UD_OK;
}
static ud_status position(void *state,uint64_t *out) {
    if (!state || !out) return UD_INVALID;
    *out=((enumerator *)state)->position;return UD_OK;
}
ud_status ud_enumerators_register(ud_registry *r) {
    const char *ids[]={NULL,"core.collection","core.variable","core.files","core.query","core.schema"};
    for (int i=COLLECTION;i<=SCHEMA;i++) {
        ud_component_descriptor d={.struct_size=sizeof(d),.abi_version=UD_ABI_VERSION,
            .id={ids[i],(uint32_t)strlen(ids[i])},.name={ids[i],(uint32_t)strlen(ids[i])},
            .version={UD_SDK_VERSION,sizeof(UD_SDK_VERSION)-1},.configuration_schema={"{\"type\":\"object\"}",17},
            .category=UD_COMPONENT_ENUMERATOR,.synchronous_input=-1,.error_output=-1,
            .capabilities=UD_COMPONENT_THREAD_SAFE,.user=(void *)(intptr_t)i,
            .configure=configure,.prepare=prepare,.dispose=dispose,.enumerate_next=next,.enumerate_position=position};
        if (i<=FILES) d.capabilities|=UD_ENUMERATOR_DETERMINISTIC|UD_ENUMERATOR_SNAPSHOT|UD_ENUMERATOR_RESUMABLE;
        UD_TRY(ud_registry_register(r,&d));
    }
    return UD_OK;
}
ud_status ud_foreach(ud_activation *a,const ud_json *config,const ud_json *children,int execute,uint32_t depth) {
    ud_string id={"core.collection",15},variable={0};
    if (ud_member(config,"enumerator")) UD_TRY(ud_json_string(ud_member(config,"enumerator"),&id));
    ud_component_descriptor d={.struct_size=sizeof(d),.abi_version=UD_ABI_VERSION};
    UD_TRY(ud_component_lookup(a,id,&d));
    if (d.category!=UD_COMPONENT_ENUMERATOR) return UD_SCHEMA;
    if (a->execution->options.state_directory.length &&
        (d.capabilities & (UD_ENUMERATOR_DETERMINISTIC|UD_ENUMERATOR_SNAPSHOT|UD_ENUMERATOR_RESUMABLE))!=
        (UD_ENUMERATOR_DETERMINISTIC|UD_ENUMERATOR_SNAPSHOT|UD_ENUMERATOR_RESUMABLE)) return UD_UNSUPPORTED;
    ud_json *bound=NULL;char *text=NULL;uint32_t length=0;
    ud_status status=ud_bound_config(a,config,&bound);
    if (!status) status=ud_json_string(ud_member(bound,"variable"),&variable);
    if (!status) status=ud_json_serialize(bound,&text,&length);
    ud_component_context c={.struct_size=sizeof(c),.abi_version=UD_ABI_VERSION,.memory=a->execution->ctx,
        .execution=a->execution,.activation=a,.task_id=a->task_id,.max_batch_rows=a->execution->options.max_batch_rows,
        .resolve=ud_resolve_value,.resolve_user=a,.resolve_field=ud_resolve_field};
    ud_batch *schema=NULL;
    if (!status) status=d.configure(d.user,&c,(ud_string){text,length},NULL,&schema);
    if (!status && (!schema || schema->columns!=1 || schema->rows)) status=UD_SCHEMA;
    void *instance=NULL;int opened=0;
    if (!status && execute) {
        const ud_batch *outputs[]={schema};
        status=d.prepare(d.user,&c,(ud_string){text,length},NULL,outputs,&instance);opened=!status;
    }
    uint64_t iteration=0;
    while (!status && execute) {
        ud_batch *values=NULL;status=d.enumerate_next(instance,&values);
        if (status==UD_END) { ud_batch_destroy(values);status=UD_OK;break; }
        if (!status && (!values || values->rows!=1 || values->columns!=1 || !values->sealed ||
            !ud_field_matches(&values->column[0].field,&schema->column[0].field,UD_FIELD_PORT))) status=UD_SCHEMA;
        ud_value value={0};
        if (!status) status=ud_batch_get(values,0,0,&value);
        if (!status) status=ud_activation_set_variable(a,variable,&value,&schema->column[0].field);
        if (!status) status=ud_control_iteration(a,children,iteration++);
        ud_batch_destroy(values);
    }
    if (!status && !execute) status=ud_control_validate(a,children,depth+1);
    if (!status && opened && d.finish) status=d.finish(instance);
    if (opened) d.dispose(instance,status);
    ud_batch_destroy(schema);ud_json_destroy(bound);ud_free(text);
    return status;
}
