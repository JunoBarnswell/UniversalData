#include "postgresql_metadata.h"
const char *ud_pg_metadata_query(void) {
    return "SELECT a.attname,format_type(a.atttypid,a.atttypmod),NOT a.attnotnull,"
           "COALESCE(array_position(i.indkey::smallint[],a.attnum)-array_lower(i.indkey::smallint[]"
           ",1)"
           "+1,0),"
           "has_column_privilege(a.attrelid,a.attname,'SELECT'),has_column_privilege(a.attrelid,a."
           "attname,'INSERT'),"
           "has_column_privilege(a.attrelid,a.attname,'UPDATE'),has_table_privilege(a.attrelid,'"
           "DELETE'),'postgresql/'||(SELECT oid::text FROM pg_database WHERE datname=current_database())||'/'||a.attrelid::text||'/'||a.attnum::text "
           "FROM pg_attribute a LEFT JOIN pg_index i ON i.indrelid=a.attrelid AND i.indisprimary "
           "WHERE a.attrelid=$1::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum";
}
ud_status ud_pg_metadata_decode(ud_host_api *h, PGresult *r, ud_batch **out) {
    *out = NULL;
    if (!r || PQresultStatus(r) != PGRES_TUPLES_OK)
        return UD_IO;
    int count = PQntuples(r);
    if (count < 1 || count > 65536) {
        return UD_SCHEMA;
    }
    ud_field fields[] = {{.name = "name", .name_length = 4, .type = UD_UTF8},
                         {.name = "declared_type", .name_length = 13, .type = UD_UTF8},
                         {.name = "nullable", .name_length = 8, .type = UD_BOOL},
                         {.name = "key_order", .name_length = 9, .type = UD_INT64},
                         {.name = "readable", .name_length = 8, .type = UD_BOOL},
                         {.name = "appendable", .name_length = 10, .type = UD_BOOL},
                         {.name = "updatable", .name_length = 9, .type = UD_BOOL},
                         {.name = "deletable", .name_length = 9, .type = UD_BOOL},
                         {.name = "physical_identity", .name_length = 17, .type = UD_UTF8}};
    ud_status s = h->batch_create(h->context, fields, 9, (uint32_t)count, out);
    for (int row = 0; !s && row < count; row++) {
        ud_value v[9] = {{.type = UD_UTF8},  {.type = UD_UTF8}, {.type = UD_BOOL},
                         {.type = UD_INT64}, {.type = UD_BOOL}, {.type = UD_BOOL},
                         {.type = UD_BOOL},  {.type = UD_BOOL}, {.type = UD_UTF8}};
        for (int j = 0; j < 2; j++) {
            v[j].text = PQgetvalue(r, row, j);
            v[j].text_length = (uint32_t)PQgetlength(r, row, j);
        }
        v[2].integer = !strcmp(PQgetvalue(r, row, 2), "t");
        v[3].integer = strtoll(PQgetvalue(r, row, 3), NULL, 10);
        for (int j = 4; j < 8; j++)
            v[j].integer = !strcmp(PQgetvalue(r, row, j), "t");
        v[8].text=PQgetvalue(r,row,8);v[8].text_length=(uint32_t)PQgetlength(r,row,8);
        s = h->append(*out, v, 9);
    }
    return s;
}
