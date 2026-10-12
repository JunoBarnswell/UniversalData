#ifndef UD_CONNECTION_INTERNAL_H
#define UD_CONNECTION_INTERNAL_H
#include "ud/plugin.h"
/* Physical session creation shares the complete typed driver contract. The
 * manager supplies already resolved credentials and keeps their bytes private. */
ud_status ud_connection_open_resolved(ud_plugin *, const ud_connection_options *, ud_string,
                                      ud_connection **);
ud_status ud_connection_bind_lease(ud_connection *, void *, ud_authorize_fn,
                                   ud_status (*check)(void *));
ud_status ud_connection_reset_session(ud_connection *);
int ud_connection_reusable(const ud_connection *);
#endif
