#ifndef UD_WINDOW_INTERNAL_H
#define UD_WINDOW_INTERNAL_H
#include "state.h"
#include "ud/operator.h"
typedef struct ud_window ud_window;
ud_status ud_window_create(ud_context *, const ud_operator_options *, const ud_batch *,
                           ud_window **);
ud_status ud_window_schema(const ud_window *, ud_batch **);
ud_status ud_window_order(const ud_window *, ud_sort_contract *);
ud_status ud_window_push(ud_window *, const ud_batch *);
ud_status ud_window_end(ud_window *);
ud_status ud_window_pull(ud_window *, ud_batch **);
uint64_t ud_window_demand(const ud_window *);
void ud_window_destroy(ud_window *);
#endif
