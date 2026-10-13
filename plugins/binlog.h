#ifndef UD_BINLOG_H
#define UD_BINLOG_H
#include "common.h"
typedef struct ud_binlog ud_binlog;
ud_status ud_binlog_create(const ud_host_api*,const ud_cdc_options*,int mariadb,ud_binlog**);
ud_status ud_binlog_decode(ud_binlog*,const void*,size_t,ud_string file,ud_cdc_event*);
void ud_binlog_destroy(ud_binlog*);
#endif
