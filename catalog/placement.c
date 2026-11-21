#include "../include/ud/scheduler.h"

#include <limits.h>
#include <string.h>

static int bool_value(int32_t value) { return value == 0 || value == 1; }

#define PLACEMENT_TRY(call)                                                                         \
    do {                                                                                             \
        ud_status placement_status = (call);                                                        \
        if (placement_status)                                                                        \
            return placement_status;                                                                \
    } while (0)

static int string_value(ud_string value) {
    return value.data && value.length && value.length <= 255 && !memchr(value.data, 0, value.length);
}

static int same_string(ud_string left, ud_string right) {
    return left.length == right.length && left.length &&
           !memcmp(left.data, right.data, left.length);
}

static ud_status budget_check(const ud_scheduler_budget *budget) {
    if (!budget)
        return UD_INVALID;
    if (budget->struct_size != sizeof(*budget) || budget->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    return budget->cpu_slots && budget->consumer_lanes && budget->native_memory_bytes &&
                   budget->process_memory_bytes
               ? UD_OK
               : UD_INVALID;
}

static ud_status node_check(const ud_scheduler_node *node) {
    if (!node)
        return UD_INVALID;
    if (node->struct_size != sizeof(*node) || node->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!string_value(node->id) || !bool_value(node->remote) || !bool_value(node->online) ||
        !bool_value(node->eligible) || !bool_value(node->supports_package) || !node->max_cpu ||
        !node->max_processes || !node->max_memory_bytes || node->used_cpu > node->max_cpu ||
        node->used_processes > node->max_processes || node->used_memory_bytes > node->max_memory_bytes ||
        node->available_cpu > node->max_cpu || node->available_processes > node->max_processes ||
        node->available_memory_bytes > node->max_memory_bytes)
        return UD_INVALID;
    return UD_OK;
}

static ud_status placement_check(const ud_scheduler_placement *placement) {
    if (!placement)
        return UD_INVALID;
    if (placement->struct_size != sizeof(*placement) || placement->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!string_value(placement->node_id) || !bool_value(placement->remote))
        return UD_INVALID;
    return budget_check(&placement->budget);
}

static int less_id(ud_string left, ud_string right) {
    size_t length = left.length < right.length ? left.length : right.length;
    int comparison = memcmp(left.data, right.data, length);
    return comparison < 0 || (comparison == 0 && left.length < right.length);
}

static int placement_match(const ud_scheduler_placement *placement, const ud_scheduler_node *node) {
    return placement && placement->node_id.length && same_string(placement->node_id, node->id) &&
           placement->remote == node->remote;
}

static int fits(const ud_scheduler_node *node, uint32_t free_processes, uint32_t free_cpu,
                uint64_t free_memory, const ud_scheduler_budget *budget) {
    if (!node->online || !node->eligible || !node->supports_package || !free_processes ||
        budget->cpu_slots > free_cpu || budget->process_memory_bytes > free_memory)
        return 0;
    return 1;
}

static int choose_node(const ud_scheduler_placement_request *request, uint32_t slot,
                       const uint32_t *free_processes, const uint32_t *free_cpu,
                       const uint64_t *free_memory, const uint32_t *reserved,
                       const ud_scheduler_budget *budget) {
    const ud_scheduler_placement *pin = request->pins ? &request->pins[slot] : NULL;
    const ud_scheduler_placement *excluded = request->excluded;
    int selected = -1;
    for (uint32_t at = 0; at < request->node_count; at++) {
        const ud_scheduler_node *node = &request->nodes[at];
        if (pin && pin->node_id.length && !placement_match(pin, node))
            continue;
        if (excluded && excluded->node_id.length && placement_match(excluded, node))
            continue;
        if (!fits(node, free_processes[at], free_cpu[at], free_memory[at], budget))
            continue;
        if (selected < 0 || reserved[at] < reserved[selected] ||
            (reserved[at] == reserved[selected] &&
             (node->remote < request->nodes[selected].remote ||
              (node->remote == request->nodes[selected].remote &&
               less_id(node->id, request->nodes[selected].id)))))
            selected = (int)at;
    }
    return selected;
}

ud_status ud_scheduler_place(const ud_scheduler_placement_request *request,
                             ud_scheduler_placement *placements) {
    if (!request || !placements)
        return UD_INVALID;
    if (request->struct_size != sizeof(*request) || request->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!request->count || request->count > 64 || !request->node_count || request->node_count > 256 ||
        !request->nodes)
        return UD_INVALID;
    PLACEMENT_TRY(budget_check(&request->default_budget));
    if (request->max_native_memory_bytes &&
        request->default_budget.native_memory_bytes > request->max_native_memory_bytes)
        return UD_RESOURCE;
    if (request->pins && request->count > UINT32_MAX / sizeof(*request->pins))
        return UD_OVERFLOW;
    if (request->budgets && request->count > UINT32_MAX / sizeof(*request->budgets))
        return UD_OVERFLOW;
    if (request->budgets)
        for (uint32_t at = 0; at < request->count; at++) {
            PLACEMENT_TRY(budget_check(&request->budgets[at]));
            if (request->max_native_memory_bytes &&
                request->budgets[at].native_memory_bytes > request->max_native_memory_bytes)
                return UD_RESOURCE;
        }
    if (request->pins)
        for (uint32_t at = 0; at < request->count; at++)
            if (request->pins[at].node_id.length)
                PLACEMENT_TRY(placement_check(&request->pins[at]));
    if (request->excluded && request->excluded->node_id.length)
        PLACEMENT_TRY(placement_check(request->excluded));
    uint32_t free_processes[256], free_cpu[256], reserved[256];
    uint64_t free_memory[256];
    for (uint32_t at = 0; at < request->node_count; at++) {
        PLACEMENT_TRY(node_check(&request->nodes[at]));
        for (uint32_t prior = 0; prior < at; prior++)
            if (same_string(request->nodes[at].id, request->nodes[prior].id) &&
                request->nodes[at].remote == request->nodes[prior].remote)
                return UD_INVALID;
        reserved[at] = request->nodes[at].used_processes;
        uint32_t local_processes = request->nodes[at].max_processes - request->nodes[at].used_processes;
        uint32_t local_cpu = request->nodes[at].max_cpu - request->nodes[at].used_cpu;
        uint64_t local_memory = request->nodes[at].max_memory_bytes - request->nodes[at].used_memory_bytes;
        free_processes[at] = local_processes < request->nodes[at].available_processes
                                 ? local_processes
                                 : request->nodes[at].available_processes;
        free_cpu[at] = local_cpu < request->nodes[at].available_cpu ? local_cpu
                                                                      : request->nodes[at].available_cpu;
        free_memory[at] = local_memory < request->nodes[at].available_memory_bytes
                              ? local_memory
                              : request->nodes[at].available_memory_bytes;
    }

    /* Pinned members are admitted first, matching the durable shard handoff
     * rule.  Unpinned members then use least-loaded deterministic placement. */
    for (int pass = 0; pass < 2; pass++) {
        for (uint32_t slot = 0; slot < request->count; slot++) {
            const ud_scheduler_placement *pin = request->pins ? &request->pins[slot] : NULL;
            int pinned = pin && pin->node_id.length;
            if ((pass == 0) != pinned)
                continue;
            ud_scheduler_budget budget = pinned ? pin->budget
                : request->budgets ? request->budgets[slot]
                : request->excluded && request->excluded->node_id.length
                    ? request->excluded->budget : request->default_budget;
            PLACEMENT_TRY(budget_check(&budget));
            if (request->max_native_memory_bytes &&
                budget.native_memory_bytes > request->max_native_memory_bytes)
                return UD_RESOURCE;
            int selected = choose_node(request, slot, free_processes, free_cpu, free_memory, reserved, &budget);
            if (selected < 0)
                return UD_RESOURCE;
            const ud_scheduler_node *node = &request->nodes[selected];
            placements[slot] = (ud_scheduler_placement){sizeof(*placements), UD_ABI_VERSION, node->id,
                                                        node->remote, budget};
            free_processes[selected]--;
            reserved[selected]++;
            free_cpu[selected] -= budget.cpu_slots;
            free_memory[selected] -= budget.process_memory_bytes;
        }
    }
    return UD_OK;
}

#undef PLACEMENT_TRY
