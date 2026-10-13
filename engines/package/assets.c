#include "internal.h"
#include "ud/cache.h"
#include "ud/connection_manager.h"
#include "ud/format.h"
struct ud_cache_asset {
    ud_string id, source_connection,source_table;
    uint32_t mode,max_entries;uint64_t ttl_ms;
    ud_execution *execution;
    ud_connection_lease *grant;
    ud_batch *schema;
    uint32_t *keys, key_count;
    int duplicates, error_policy;
    ud_cache *cache;
    ud_cache_metrics metrics;
    uint8_t fingerprint[32];
    int building;
};
/* Bound resource grants belong to the Root, so a sealed asset cannot rebind a
 * mutable variable or outlive the source activation's authorization context. */
struct ud_source_grant {
    ud_string task, pipeline, connection;
    ud_connection_lease *lease;
    struct ud_source_grant *next;
};
static struct ud_source_grant *source_grant(ud_activation *a, ud_string connection) {
    for (struct ud_source_grant *g = a->execution->source_grants; g; g = g->next)
        if (ud_same(g->task, a->task_id) && ud_same(g->pipeline, a->pipeline_id) &&
            ud_same(g->connection, connection))
            return g;
    return NULL;
}
ud_status ud_assets_bind_source(ud_activation *a, ud_string connection,
                                ud_connection_lease *lease) {
    if (!a || !lease)
        return UD_INVALID;
    ud_execution *e = a->execution;
    int declared = 0;
    for (uint32_t i = 0; i < e->asset_count; i++)
        if (ud_same(e->assets[i].source_connection, connection))
            declared = 1;
    if (!declared)
        return UD_OK;
    UD_TRY(ud_connection_lease_authorize(lease, UD_ACCESS_READ));
    ud_mutex_lock(&e->asset_lock);
    struct ud_source_grant *g = source_grant(a, connection);
    ud_status status = UD_OK;
    if (g) {
        ud_string old_resource = {0}, resource = {0};
        status = ud_connection_lease_resource(g->lease, &old_resource);
        if (!status)
            status = ud_connection_lease_resource(lease, &resource);
        if (!status && !ud_same(old_resource, resource))
            status = UD_INCOMPATIBLE;
    } else {
        g = ud_alloc(e->ctx, sizeof(*g));
        if (!g)
            status = UD_RESOURCE;
        if (!status) {
            g->task = (ud_string){ud_strdup_n(e->ctx, a->task_id.data, a->task_id.length),
                                  a->task_id.length};
            g->pipeline =
                (ud_string){ud_strdup_n(e->ctx, a->pipeline_id.data, a->pipeline_id.length),
                            a->pipeline_id.length};
            g->connection = (ud_string){ud_strdup_n(e->ctx, connection.data, connection.length),
                                        connection.length};
            if (!g->task.data || !g->pipeline.data || !g->connection.data)
                status = UD_RESOURCE;
        }
        if (!status)
            status = ud_connection_lease_snapshot_grant(lease, &e->root, &g->lease);
        if (!status) {
            g->next = e->source_grants;
            e->source_grants = g;
        } else if (g) {
            ud_free((void *)g->task.data);
            ud_free((void *)g->pipeline.data);
            ud_free((void *)g->connection.data);
            ud_free(g);
        }
    }
    ud_mutex_unlock(&e->asset_lock);
    return status;
}
static ud_status root_check(void *user) {
    struct ud_cache_asset *asset = user;
    UD_TRY(ud_activation_check(&asset->execution->root));
    return ud_connection_lease_authorize(asset->grant, UD_ACCESS_READ);
}
ud_status ud_assets_create(ud_execution *e) {
    const ud_json *definitions = ud_member(e->package->document, "caches");
    if (!definitions)
        return UD_OK;
    if (ud_json_kind(definitions) != UD_JSON_ARRAY || ud_json_size(definitions) > 1024)
        return UD_SCHEMA;
    e->asset_count = ud_json_size(definitions);
    e->assets = ud_alloc(e->ctx, (size_t)e->asset_count * sizeof(*e->assets));
    if (!e->assets)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < e->asset_count; i++) {
        const ud_json *definition = ud_json_at(definitions, i);
        static const char *keys[] = {
            "id",          "schema",           "keys",       "duplicates", "scope",
            "errorPolicy", "sourceConnection", "sourceTable","mode","maxEntries","ttlMs"};
        UD_TRY(ud_json_keys(definition, keys, 11));
        struct ud_cache_asset *asset = e->assets + i;
        asset->execution = e;
        ud_string mode={"full",4};if(ud_member(definition,"mode"))UD_TRY(ud_json_string(ud_member(definition,"mode"),&mode));
        if(ud_named(mode,"partial"))asset->mode=UD_CACHE_PARTIAL;else if(!ud_named(mode,"full"))return UD_SCHEMA;
        uint64_t maximum=4096,ttl=60000;if(ud_member(definition,"maxEntries"))UD_TRY(ud_json_uint(ud_member(definition,"maxEntries"),&maximum));if(ud_member(definition,"ttlMs"))UD_TRY(ud_json_uint(ud_member(definition,"ttlMs"),&ttl));if(!maximum||maximum>65536||!ttl)return UD_SCHEMA;asset->max_entries=(uint32_t)maximum;asset->ttl_ms=ttl;

        UD_TRY(ud_json_string(ud_member(definition, "id"), &asset->id));
        UD_TRY(
            ud_json_string(ud_member(definition, "sourceConnection"), &asset->source_connection));
        const ud_json *source = NULL;
        int project = 0;
        UD_TRY(ud_connection_definition(&e->root, asset->source_connection, &source, &project));
        for (uint32_t j = 0; j < i; j++)
            if (ud_same(asset->id, e->assets[j].id))
                return UD_SCHEMA;
        ud_string scope = {"execution", 9}, duplicates = {"first", 5}, policy = {"fail", 4};
        if (ud_member(definition, "scope"))
            UD_TRY(ud_json_string(ud_member(definition, "scope"), &scope));
        if (!ud_named(scope, "execution"))
            return UD_UNSUPPORTED;
        if (ud_member(definition, "duplicates"))
            UD_TRY(ud_json_string(ud_member(definition, "duplicates"), &duplicates));
        if (ud_member(definition, "errorPolicy"))
            UD_TRY(ud_json_string(ud_member(definition, "errorPolicy"), &policy));
        asset->duplicates = ud_named(duplicates, "first")   ? UD_CACHE_FIRST
                            : ud_named(duplicates, "all")   ? UD_CACHE_ALL
                            : ud_named(duplicates, "error") ? UD_CACHE_ERROR
                                                            : -1;
        asset->error_policy = ud_named(policy, "fail")       ? UD_ERROR_FAIL
                              : ud_named(policy, "redirect") ? UD_ERROR_REDIRECT
                              : ud_named(policy, "ignore")   ? UD_ERROR_IGNORE
                                                             : -1;
        if (asset->duplicates < 0 || asset->error_policy < 0)
            return UD_SCHEMA;
        char *json = NULL;
        uint32_t length = 0;
        UD_TRY(ud_json_serialize(ud_member(definition, "schema"), &json, &length));
        ud_string table = {0};
        ud_status status = UD_OK;
        if (ud_member(definition, "sourceTable"))
            status = ud_json_string(ud_member(definition, "sourceTable"), &table);
        char name_space[33];
        if (!status)
            status = ud_connection_field_namespace(&e->root, asset->source_connection, table,
                                                   name_space);
        if(!status){asset->source_table=table;if(asset->mode==UD_CACHE_PARTIAL&&!table.length)status=UD_SCHEMA;}
        if (!status)
            status = ud_schema_from_json(e->ctx, json, length, (ud_string){name_space, 32},
                                         &asset->schema);
        ud_free(json);
        UD_TRY(status);
        const ud_json *columns = ud_member(definition, "keys");
        asset->key_count = ud_json_size(columns);
        if (ud_json_kind(columns) != UD_JSON_ARRAY || !asset->key_count ||
            asset->key_count > asset->schema->columns)
            return UD_SCHEMA;
        asset->keys = ud_alloc(e->ctx, (size_t)asset->key_count * sizeof(*asset->keys));
        if (!asset->keys)
            return UD_RESOURCE;
        for (uint32_t j = 0; j < asset->key_count; j++) {
            uint64_t column = 0;
            UD_TRY(ud_json_uint(ud_json_at(columns, j), &column));
            if (column >= asset->schema->columns)
                return UD_SCHEMA;
            asset->keys[j] = (uint32_t)column;
        }
    }
    return UD_OK;
}
static struct ud_cache_asset *find_asset(ud_activation *a, ud_string id) {
    for (uint32_t i = 0; i < a->execution->asset_count; i++)
        if (ud_same(a->execution->assets[i].id, id))
            return a->execution->assets + i;
    return NULL;
}
ud_status ud_asset_schema(ud_activation *a, ud_string id, ud_batch **out, int *duplicates,
                          int *policy) {
    if (!a || !out)
        return UD_INVALID;
    struct ud_cache_asset *asset = find_asset(a, id);
    if (!asset)
        return UD_SCHEMA;
    if (duplicates)
        *duplicates = asset->duplicates;
    if (policy)
        *policy = asset->error_policy;
    return ud_batch_create(a->execution->ctx, asset->schema->schema->fields, asset->schema->columns,
                           0, out);
}
ud_status ud_asset_source(ud_activation *a, ud_string id, ud_string *out) {
    struct ud_cache_asset *asset = find_asset(a, id);
    if (!asset)
        return UD_SCHEMA;
    *out = asset->source_connection;
    return UD_OK;
}
ud_status ud_asset_keys(ud_activation *a, ud_string id, const uint32_t **keys, uint32_t *count) {
    struct ud_cache_asset *asset = find_asset(a, id);
    if (!asset)
        return UD_SCHEMA;
    *keys = asset->keys;
    *count = asset->key_count;
    return UD_OK;
}
ud_status ud_asset_build(ud_activation *a, ud_string id, ud_cache **out) {
    if (!a || !out)
        return UD_INVALID;
    *out = NULL;
    struct ud_cache_asset *asset = find_asset(a, id);
    if (!asset || asset->mode!=UD_CACHE_FULL)
        return UD_SCHEMA;
    ud_mutex_lock(&a->execution->asset_lock);
    int busy = asset->building || asset->cache;
    asset->building = !busy;
    ud_mutex_unlock(&a->execution->asset_lock);
    if (busy)
        return UD_SCHEMA; /* A declared execution asset has one builder. */
    ud_mutex_lock(&a->execution->asset_lock);
    struct ud_source_grant *source = source_grant(a, asset->source_connection);
    ud_status status =
        source
            ? ud_connection_lease_snapshot_grant(source->lease, &a->execution->root, &asset->grant)
            : UD_SCHEMA;
    ud_mutex_unlock(&a->execution->asset_lock);
    ud_cache_options options = {.struct_size = sizeof(options),
                                .abi_version = UD_ABI_VERSION,
                                .max_batch_rows = a->execution->options.max_batch_rows,
                                .keys = asset->keys,
                                .key_count = asset->key_count,
                                .duplicates = asset->duplicates,
                                .error_policy = asset->error_policy,
                                .buffers = a->execution->buffers,
                                .executor = a->execution->executor,
                                .check = root_check,
                                .user = asset,.record_state=a->execution->journal!=NULL};
    if (!status)
        status = ud_cache_create(a->execution->ctx, asset->schema, &options, out);
    if (status) {
        ud_mutex_lock(&a->execution->asset_lock);
        asset->building = 0;
        ud_mutex_unlock(&a->execution->asset_lock);
    }
    return status;
}
static ud_status asset_checkpoint_key(struct ud_cache_asset*asset,char out[65]){ud_bytes bytes={.ctx=asset->execution->ctx};ud_status status=ud_put_string(&bytes,"cache.asset",11);if(!status)status=ud_put_string(&bytes,asset->id.data,asset->id.length);uint8_t digest[32];if(!status){ud_hash(bytes.data,bytes.size,digest);for(uint32_t i=0;i<32;i++)snprintf(out+2*i,3,"%02x",digest[i]);}ud_bytes_clear(&bytes);return status;}
ud_status ud_asset_publish(ud_activation *a, ud_string id, ud_cache *cache) {
    struct ud_cache_asset *asset = find_asset(a, id);
    if (!asset || !cache)
        return UD_SCHEMA;
    uint8_t fingerprint[32];
    if(asset->execution->journal){ud_batch*state=NULL;char key[65];ud_status status=asset_checkpoint_key(asset,key);if(!status)status=ud_cache_checkpoint(cache,&state);if(!status)status=ud_checkpoint_snapshot_write(&asset->execution->root,(ud_string){key,64},0,state);ud_batch_destroy(state);UD_TRY(status);}
    UD_TRY(ud_cache_fingerprint(cache, fingerprint));
    UD_TRY(ud_cache_retain(cache));
    ud_mutex_lock(&a->execution->asset_lock);
    ud_status status = asset->cache ? UD_SCHEMA : UD_OK;
    if (!status) {
        asset->cache = cache;
        asset->building = 0;
        memcpy(asset->fingerprint, fingerprint, 32);
        ud_cache_get_metrics(cache, &asset->metrics);
    }
    ud_mutex_unlock(&a->execution->asset_lock);
    if (status)
        ud_cache_release(cache);
    return status;
}
ud_status ud_asset_acquire(ud_activation *a, ud_string id, ud_cache **out) {
    if (!a || !out)
        return UD_INVALID;
    *out = NULL;
    struct ud_cache_asset *asset = find_asset(a, id);
    if (!asset)
        return UD_SCHEMA;
    ud_mutex_lock(&a->execution->asset_lock);
    ud_status status=UD_OK;
    if(!asset->cache&&asset->mode==UD_CACHE_FULL&&asset->execution->journal){char key[65];ud_batch*state=NULL;status=asset_checkpoint_key(asset,key);if(!status)status=ud_checkpoint_snapshot_read(&asset->execution->root,(ud_string){key,64},0,&state);
        if(!status&&!asset->grant)status=ud_activation_acquire_grant(&asset->execution->root,asset->source_connection,&asset->grant);
        ud_cache_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,.max_batch_rows=a->execution->options.max_batch_rows,.keys=asset->keys,.key_count=asset->key_count,.duplicates=asset->duplicates,.error_policy=asset->error_policy,.buffers=a->execution->buffers,.executor=a->execution->executor,.check=root_check,.user=asset,.record_state=1};
        if(!status)status=ud_cache_create(a->execution->ctx,asset->schema,&options,&asset->cache);if(!status)status=ud_cache_restore(asset->cache,state);if(!status)status=ud_cache_fingerprint(asset->cache,asset->fingerprint);ud_batch_destroy(state);
        if(status&&asset->cache){ud_cache_release(asset->cache);asset->cache=NULL;}
    }
    if(!status)status=asset->cache?ud_cache_retain(asset->cache):UD_SCHEMA;
    if (!status)
        *out = asset->cache;
    ud_mutex_unlock(&a->execution->asset_lock);
    return status;
}
ud_status ud_assets_release(ud_execution *e) {
    ud_status status = UD_OK;
    for (uint32_t i = 0; i < e->asset_count; i++)
        if (e->assets[i].cache) {
            ud_cache_get_metrics(e->assets[i].cache, &e->assets[i].metrics);
            ud_status closed = ud_cache_release(e->assets[i].cache);
            e->assets[i].cache = NULL;
            if (!status)
                status = closed;
        }
    for (uint32_t i = 0; i < e->asset_count; i++)
        if (e->assets[i].grant) {
            ud_status closed = ud_connection_lease_release(e->assets[i].grant);
            if (!closed)
                e->assets[i].grant = NULL;
            if (!status)
                status = closed;
        }
    while (e->source_grants) {
        struct ud_source_grant *g = e->source_grants;
        ud_status closed = ud_connection_lease_release(g->lease);
        if (closed) {
            if (!status)
                status = closed;
            break;
        }
        e->source_grants = g->next;
        ud_free((void *)g->task.data);
        ud_free((void *)g->pipeline.data);
        ud_free((void *)g->connection.data);
        ud_free(g);
    }
    return status;
}
void ud_assets_destroy(ud_execution *e) {
    ud_assets_release(e);
    for (uint32_t i = 0; i < e->asset_count; i++) {
        ud_batch_destroy(e->assets[i].schema);
        ud_free(e->assets[i].keys);
    }
    ud_free(e->assets);
    e->assets = NULL;
    e->asset_count = 0;
}
ud_status ud_assets_diagnostics(ud_execution *e, ud_bytes *out) {
    UD_TRY(ud_bytes_put(out, "[", 1));
    for (uint32_t i = 0; i < e->asset_count; i++) {
        struct ud_cache_asset *asset = e->assets + i;
        if (asset->cache)
            ud_cache_get_metrics(asset->cache, &asset->metrics);
        if (i)
            UD_TRY(ud_bytes_put(out, ",", 1));
        UD_TRY(ud_bytes_put(out, "{\"id\":", 6));
        UD_TRY(ud_json_quote(out, asset->id.data, asset->id.length));
        char fingerprint[65];
        static const char hex[] = "0123456789abcdef";
        for (uint32_t j = 0; j < 32; j++) {
            fingerprint[2 * j] = hex[asset->fingerprint[j] >> 4];
            fingerprint[2 * j + 1] = hex[asset->fingerprint[j] & 15];
        }
        fingerprint[64] = 0;
        char text[384];
        int n = snprintf(text, sizeof(text),
                         ",\"version\":\"%s\",\"loaded\":%llu,\"hits\":%llu,\"misses\":%llu,"
                         "\"duplicates\":%llu,\"pages\":%llu}",
                         fingerprint, (unsigned long long)asset->metrics.loaded,
                         (unsigned long long)asset->metrics.hits,
                         (unsigned long long)asset->metrics.misses,
                         (unsigned long long)asset->metrics.duplicates,
                         (unsigned long long)asset->metrics.pages);
        UD_TRY(ud_bytes_put(out, text, (size_t)n));
    }
    return ud_bytes_put(out, "]", 1);
}
struct ud_source_family {
    uint8_t key[32], fingerprint[32];
    struct ud_source_family *next;
};
ud_status ud_source_version(ud_activation *a, ud_id scope, const uint8_t fingerprint[32]) {
    ud_bytes identity = {.ctx = a->execution->ctx};
    ud_status status = ud_put_string(&identity, a->task_id.data, a->task_id.length);
    if (!status)
        status = ud_put_string(&identity, a->pipeline_id.data, a->pipeline_id.length);
    if (!status)
        status = ud_put_u64(&identity, scope.high);
    if (!status)
        status = ud_put_u64(&identity, scope.low);
    uint8_t key[32];
    if (!status)
        ud_hash(identity.data, identity.size, key);
    ud_bytes_clear(&identity);
    if (status)
        return status;
    ud_execution *e = a->execution;
    ud_mutex_lock(&e->asset_lock);
    struct ud_source_family *family = e->source_families;
    while (family && memcmp(family->key, key, 32))
        family = family->next;
    if (family)
        status = memcmp(family->fingerprint, fingerprint, 32) ? UD_INCOMPATIBLE : UD_OK;
    else {
        family = ud_alloc(e->ctx, sizeof(*family));
        if (!family)
            status = UD_RESOURCE;
        else {
            memcpy(family->key, key, 32);
            memcpy(family->fingerprint, fingerprint, 32);
            family->next = e->source_families;
            e->source_families = family;
        }
    }
    ud_mutex_unlock(&e->asset_lock);
    return status;
}
void ud_source_versions_destroy(ud_execution *e) {
    while (e->source_families) {
        struct ud_source_family *next = e->source_families->next;
        ud_free(e->source_families);
        e->source_families = next;
    }
}

ud_status ud_asset_mode(ud_activation*a,ud_string id,uint32_t*mode,ud_string*table){struct ud_cache_asset*asset=find_asset(a,id);if(!asset)return UD_SCHEMA;if(mode)*mode=asset->mode;if(table)*table=asset->source_table;return UD_OK;}
ud_status ud_asset_partial(ud_activation*a,ud_string id,ud_connection_lease*lease,ud_cache**out){
    if(!a||!lease||!out)return UD_INVALID;*out=NULL;struct ud_cache_asset*asset=find_asset(a,id);if(!asset||asset->mode!=UD_CACHE_PARTIAL)return UD_SCHEMA;
    UD_TRY(ud_connection_lease_authorize(lease,UD_ACCESS_READ));ud_mutex_lock(&a->execution->asset_lock);ud_status status=UD_OK;
    if(!asset->cache){status=ud_connection_lease_snapshot_grant(lease,&a->execution->root,&asset->grant);
      ud_cache_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,.max_batch_rows=a->execution->options.max_batch_rows,.keys=asset->keys,.key_count=asset->key_count,.duplicates=asset->duplicates,.error_policy=asset->error_policy,.buffers=a->execution->buffers,.executor=a->execution->executor,.check=root_check,.user=asset,.mode=UD_CACHE_PARTIAL,.max_entries=asset->max_entries,.ttl_ms=asset->ttl_ms};
      if(!status)status=ud_cache_create(a->execution->ctx,asset->schema,&options,&asset->cache);if(!status)status=ud_cache_fingerprint(asset->cache,asset->fingerprint);
      if(status&&asset->grant){ud_connection_lease_release(asset->grant);asset->grant=NULL;}
    }else{ud_string first={0},next={0};status=ud_connection_lease_resource(asset->grant,&first);if(!status)status=ud_connection_lease_resource(lease,&next);if(!status&&!ud_same(first,next))status=UD_INCOMPATIBLE;}
    if(!status)status=ud_cache_retain(asset->cache);if(!status)*out=asset->cache;ud_mutex_unlock(&a->execution->asset_lock);return status;
}
