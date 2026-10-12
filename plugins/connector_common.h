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
#endif
