#ifndef UD_PIPELINE_LANES_H
#define UD_PIPELINE_LANES_H

#include "ud/activation.h"
#include "ud/json.h"

ud_status ud_pipeline_run_lanes(ud_activation *, const ud_json *definition, uint32_t lanes,
                               ud_status (*run_one)(ud_activation *, const ud_json *));

#endif
