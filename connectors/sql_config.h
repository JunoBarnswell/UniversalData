#ifndef UD_SQL_CONFIG_H
#define UD_SQL_CONFIG_H
#include "ud/memory.h"

ud_status ud_sql_connection_config(ud_context *, int32_t dialect, ud_string, ud_string *);

#endif
