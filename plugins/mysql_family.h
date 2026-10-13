#ifndef UD_MYSQL_FAMILY_H
#define UD_MYSQL_FAMILY_H
#include "common.h"
#if defined(UD_MYSQL_CLIENT)
#include <mysql.h>
#define UD_MYSQL_ID "org.universaldata.connector.mysql"
#define UD_MYSQL_DIALECT UD_SQL_MYSQL
#elif defined(UD_MARIADB_CLIENT)
#include <mysql.h>
#define UD_MYSQL_ID "org.universaldata.connector.mariadb"
#define UD_MYSQL_DIALECT UD_SQL_MARIADB
#else
#error A real vendor client SDK is required.
#endif
/* Common prepared-statement ownership protocol compiled against each vendor's
 * own headers and client library. No connector adapts or loads another plugin. */
ud_status ud_mysql_initialize(const ud_host_api *, const char *, uint32_t, void **);
void ud_mysql_destroy(void *);
extern const ud_connector_api ud_mysql_connector;
#endif
