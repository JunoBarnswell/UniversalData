#include "ud/query.h"
#include "../../../components/compute/internal.h"
ud_status ud_query_execute(const ud_plan *p, const ud_batch *b, ud_batch **out) {
    return ud_compute_apply(p, b, out);
}
