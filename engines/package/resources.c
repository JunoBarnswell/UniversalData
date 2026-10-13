#include "internal.h"
#include "ud/connection_manager.h"
static ud_status check_buffers(void *user) {
    ud_execution *e = user;
    UD_TRY(ud_activation_check(&e->root));
    if (!e->buffer_lease)
        return UD_OK;
    UD_TRY(ud_connection_lease_authorize(e->buffer_lease, UD_ACCESS_READ));
    UD_TRY(ud_connection_lease_authorize(e->buffer_lease, UD_ACCESS_WRITE));
    return ud_connection_lease_authorize(e->buffer_lease, UD_ACCESS_DELETE);
}
ud_status ud_execution_buffer_options(ud_execution *e,ud_buffer_options *out) {
    const ud_json *policy = ud_member(ud_member(e->package->document, "policy"), "buffers");
    ud_buffer_options options = {.struct_size = sizeof(options),
                                 .abi_version = UD_ABI_VERSION,
                                 .max_blocks = 16384,
                                 .resident_limit = UINT64_C(67108864),
                                 .working_limit = UINT64_C(67108864),
                                 .check = check_buffers,
                                 .user = e};
    if (e->options.context.memory_limit) {
        options.resident_limit = e->options.context.memory_limit / 4;
        options.working_limit = e->options.context.memory_limit / 4;
    }
    if (!options.resident_limit)
        return UD_RESOURCE;
    uint64_t value = 0;
    ud_status s = UD_OK;
    if (ud_member(policy, "residentBytes")) {
        s = ud_json_uint(ud_member(policy, "residentBytes"), &value);
        options.resident_limit = value;
    }
    if (!s && ud_member(policy, "spillBytes"))
        s = ud_json_uint(ud_member(policy, "spillBytes"), &options.spill_limit);
    if (!s && ud_member(policy, "workingBytes"))
        s = ud_json_uint(ud_member(policy, "workingBytes"), &options.working_limit);
    if (!s && ud_member(policy, "maxBlocks")) {
        s = ud_json_uint(ud_member(policy, "maxBlocks"), &value);
        options.max_blocks = (uint32_t)value;
    }
    if (!s) *out = options;
    return s;
}
ud_status ud_execution_prepare_buffers(ud_execution *e) {
    const ud_json *policy = ud_member(ud_member(e->package->document, "policy"), "buffers");
    ud_buffer_options options = {0};
    ud_status s = ud_execution_buffer_options(e, &options);
    if (!s && ud_member(policy, "connection")) {
        ud_string id;
        s = ud_json_string(ud_member(policy, "connection"), &id);
        if (!s)
            s = ud_acquire_file_connection(&e->root, id, &e->buffer_lease);
        if (!s)
            s = ud_connection_lease_path(e->buffer_lease, &options.directory);
        if (!s)
            s = check_buffers(e);
    }
    if (!s)
        s = ud_buffer_manager_create(e->ctx, &options, &e->buffers);
    if (s) {
        ud_status cleanup = ud_connection_lease_release(e->buffer_lease);
        e->buffer_lease = NULL;
        return cleanup ? cleanup : s;
    }
    return UD_OK;
}
ud_status ud_execution_release_buffers(ud_execution *e) {
    if (e->buffers) {
        ud_buffer_metrics metrics;
        UD_TRY(ud_buffer_manager_metrics(e->buffers, &metrics));
        atomic_store(&e->spill_bytes, metrics.total_spill_bytes);
        e->buffer_metrics = metrics;
        UD_TRY(ud_buffer_manager_destroy(e->buffers));
        e->buffers = NULL;
    }
    ud_status s = ud_connection_lease_release(e->buffer_lease);
    if (!s)
        e->buffer_lease = NULL;
    return s;
}
