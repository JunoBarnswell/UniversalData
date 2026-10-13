#ifndef UD_SQL_H
#define UD_SQL_H
#include "connector.h"
#ifdef __cplusplus
extern "C" {
#endif
enum { UD_SQL_PARAMETER_NUMBERED = 1, UD_SQL_PARAMETER_QUESTION, UD_SQL_PARAMETER_NAMED };
enum { UD_SQL_AFTER = 1, UD_SQL_BEFORE };
typedef struct ud_sql_parameter { uint32_t row, column, source; } ud_sql_parameter;
typedef struct ud_sql_statement {
    ud_string sql; /* Owned SQL contains identifiers and placeholders only. */
    ud_sql_parameter *parameters;
    uint32_t parameter_count;
} ud_sql_statement;
/* Single Native write grammar for every provider. A Changeset statement covers
 * exactly one row; ordinary writes may compile bounded multi-row statements.
 * Providers bind the returned parameter references without formatting values. */
UD_API ud_status ud_sql_compile_write(ud_context *,int32_t dialect,int32_t parameter_style,
                                      const ud_write_options *,const ud_batch *,uint32_t first,
                                      uint32_t rows,ud_sql_statement *);
UD_API ud_status ud_sql_compile_preimage(ud_context *,int32_t,int32_t,const ud_write_options *,const ud_batch *,uint32_t,ud_sql_statement *);
UD_API ud_status ud_sql_compile_scan(ud_context *,int32_t,ud_string,const ud_batch *,ud_string *);
UD_API ud_status ud_sql_compile_lookup(ud_context *,int32_t,int32_t,ud_string,const ud_batch *,const uint32_t *,const ud_batch *,const uint32_t *,uint32_t,uint32_t,uint32_t,ud_sql_statement *);
UD_API ud_status ud_write_validate(const ud_write_options *,const ud_batch *);
UD_API ud_status ud_write_digest(ud_context *,ud_string tenant,ud_string resource,const ud_write_options *,const ud_batch *,uint8_t [32]);
UD_API void ud_sql_statement_clear(ud_sql_statement *);
#ifdef __cplusplus
}
#endif
#endif
