#ifndef UD_CONNECTOR_COMMON_H
#define UD_CONNECTOR_COMMON_H
#include "common.h"
#include <stdio.h>
/* Only SDK-generated identifiers and numbered parameters enter the statement. */
static ud_status connector_write_sql(const ud_host_api *h, const ud_write_options *options,
                                      const ud_batch *batch, int postgres, char **out) {
    *out = NULL;
    uint32_t columns = h->columns(batch);
    char **names = h->allocate(h->context, columns * sizeof(*names));
    uint8_t *key = h->allocate(h->context, columns);
    char *table = plugin_identifier(h, options->table);
    if (!names || !key || !table) { h->deallocate(names); h->deallocate(key); h->deallocate(table); return UD_RESOURCE; }
    ud_status s = UD_OK; size_t capacity = strlen(table) + 1024;
    for (uint32_t j = 0; j < options->key_count; j++) key[options->keys[j]] = 1;
    for (uint32_t j = 0; !s && j < columns; j++) {
        ud_field f; h->field(batch, j, &f);
        if (f.type >= UD_STRUCT) { s = UD_UNSUPPORTED; break; }
        names[j] = plugin_identifier(h, (ud_string){f.name,f.name_length});
        if (!names[j]) { s = UD_RESOURCE; break; }
        size_t length = strlen(names[j]);
        if (length > (SIZE_MAX - capacity - 80) / 8) { s = UD_OVERFLOW; break; }
        capacity += length * 8 + 80;
    }
    char *sql = !s ? h->allocate(h->context, capacity) : NULL;
    if (!s && !sql) s = UD_RESOURCE;
    size_t used = 0;
    if (!s && options->mode == UD_WRITE_DELETE) {
        used += (size_t)snprintf(sql + used, capacity - used, "DELETE FROM %s WHERE ", table);
        for (uint32_t j = 0; j < options->key_count; j++) used += (size_t)snprintf(sql + used, capacity - used,
            "%s%s=%c%u", j ? " AND " : "", names[options->keys[j]], postgres ? '$' : '?', j + 1);
    } else if (!s) {
        used += (size_t)snprintf(sql + used, capacity - used, "INSERT INTO %s(", table);
        for (uint32_t j = 0; j < columns; j++) used += (size_t)snprintf(sql + used, capacity - used, "%s%s", j ? "," : "", names[j]);
        used += (size_t)snprintf(sql + used, capacity - used, ") VALUES(");
        for (uint32_t j = 0; j < columns; j++) used += (size_t)snprintf(sql + used, capacity - used, "%s%c%u", j ? "," : "", postgres ? '$' : '?', j + 1);
        used += (size_t)snprintf(sql + used, capacity - used, ")");
        if (options->mode == UD_WRITE_UPSERT) {
            used += (size_t)snprintf(sql + used, capacity - used, " ON CONFLICT(");
            for (uint32_t j = 0; j < options->key_count; j++) used += (size_t)snprintf(sql + used, capacity - used, "%s%s", j ? "," : "", names[options->keys[j]]);
            int any = 0;
            for (uint32_t j = 0; j < columns; j++) if (!key[j]) any = 1;
            used += (size_t)snprintf(sql + used, capacity - used, any ? ") DO UPDATE SET " : ") DO NOTHING");
            int written = 0;
            for (uint32_t j = 0; any && j < columns; j++) if (!key[j]) used += (size_t)snprintf(sql + used, capacity - used,
                "%s%s=excluded.%s", written++ ? "," : "", names[j], names[j]);
        }
    }
    for (uint32_t j = 0; j < columns; j++) h->deallocate(names[j]);
    h->deallocate(names); h->deallocate(key); h->deallocate(table);
    if (s) h->deallocate(sql); else *out = sql;
    return s;
}
/* A typed changeset stays inside the provider's existing receipt transaction.
 * UPDATE predicates include the complete preimage, with NULL-safe equality. */
static ud_status connector_change_sql(const ud_host_api *h,const ud_write_options *o,
                                      const ud_batch *b,int postgres,int action,char **out) {
    uint32_t n=h->columns(b);char *table=plugin_identifier(h,o->table);char **names=h->allocate(h->context,n*sizeof(*names));
    if(!table||!names){h->deallocate(table);h->deallocate(names);return UD_RESOURCE;}
    size_t capacity=strlen(table)+1024;ud_status s=UD_OK;
    for(uint32_t i=0;!s&&i<n;i++){ud_field f;h->field(b,i,&f);names[i]=plugin_identifier(h,(ud_string){f.name,f.name_length});if(!names[i])s=UD_RESOURCE;else if(strlen(names[i])>(SIZE_MAX-capacity-128)/8)s=UD_OVERFLOW;else capacity+=strlen(names[i])*8+128;}
    char *sql=!s?h->allocate(h->context,capacity):NULL;if(!s&&!sql)s=UD_RESOURCE;size_t used=0;char parameter=postgres?'$':'?';
    if(!s&&action==UD_CHANGE_INSERT){used+=(size_t)snprintf(sql+used,capacity-used,"INSERT INTO %s(",table);for(uint32_t i=0;i<n;i++)used+=(size_t)snprintf(sql+used,capacity-used,"%s%s",i?",":"",names[i]);used+=(size_t)snprintf(sql+used,capacity-used,") SELECT ");for(uint32_t i=0;i<n;i++)used+=(size_t)snprintf(sql+used,capacity-used,"%s%c%u",i?",":"",parameter,i+1);used+=(size_t)snprintf(sql+used,capacity-used," WHERE NOT EXISTS(SELECT 1 FROM %s WHERE %s=%s",table,names[o->current_column],postgres?"TRUE":"1");for(uint32_t k=0;k<o->key_count;k++)used+=(size_t)snprintf(sql+used,capacity-used," AND %s=%c%u",names[o->keys[k]],parameter,o->keys[k]+1);used+=(size_t)snprintf(sql+used,capacity-used,")");}
    else if(!s){used+=(size_t)snprintf(sql+used,capacity-used,action==UD_CHANGE_UPDATE?"UPDATE %s SET ":"DELETE FROM %s",table);if(action==UD_CHANGE_UPDATE)for(uint32_t i=0;i<n;i++)used+=(size_t)snprintf(sql+used,capacity-used,"%s%s=%c%u",i?",":"",names[i],parameter,i+1);used+=(size_t)snprintf(sql+used,capacity-used," WHERE ");for(uint32_t i=0;i<n;i++)used+=(size_t)snprintf(sql+used,capacity-used,"%s%s %s %c%u",i?" AND ":"",names[i],postgres?"IS NOT DISTINCT FROM":"IS",parameter,(action==UD_CHANGE_UPDATE?n:0)+i+1);}
    for(uint32_t i=0;i<n;i++)h->deallocate(names[i]);h->deallocate(names);h->deallocate(table);if(s)h->deallocate(sql);else *out=sql;return s;
}

#endif
