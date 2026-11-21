#ifndef UD_ROW_RANGE_H
#define UD_ROW_RANGE_H

#include "ud/json.h"

/* Bind the ordered Skip/Take operations to one interval. UINT64_MAX denotes
 * an open upper bound. Counts are exact, non-negative signed integers. */
ud_status ud_row_range_bind(const ud_json *ranges, uint64_t *offset, uint64_t *limit);

#endif
