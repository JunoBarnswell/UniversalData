#include "internal.h"
static ud_status quoted_member(ud_bytes *b, const char *name, ud_string value) {
    UD_TRY(ud_bytes_put(b, ",", 1));
    UD_TRY(ud_json_quote(b, name, (uint32_t)strlen(name)));
    UD_TRY(ud_bytes_put(b, ":", 1));
    return ud_json_quote(b, value.data ? value.data : "", value.length);
}
static void hex_digest(const uint8_t digest[32], char out[65]) {
    static const char digits[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) {
        out[i * 2] = digits[digest[i] >> 4];
        out[i * 2 + 1] = digits[digest[i] & 15];
    }
    out[64] = 0;
}
static ud_status record_execution(ud_catalog *c, const ud_execution *e, uint64_t revision,
                                  int progress) {
    if (!c || !e || !revision)
        return UD_INVALID;
    ud_execution_metrics m = {.struct_size = sizeof(m), .abi_version = UD_ABI_VERSION};
    UD_TRY(ud_execution_get_metrics(e, &m));
    if (!progress && m.state != UD_EXEC_READY && !ud_execution_terminal(m.state))
        return UD_BUSY;
    if (!ud_same(c->options.tenant, e->options.tenant))
        return UD_AUTH;
    if (!e->options.execution_id.length)
        return UD_INVALID;
    char definition[65], parameter_hash[65];
    hex_digest(e->package->fingerprint, definition);
    hex_digest(e->parameters_fingerprint, parameter_hash);
    ud_bytes b = {.ctx = c->ctx};
    ud_status s = ud_bytes_put(&b, "{\"version\":" UD_PROTOCOL_VERSION_TEXT,
                               sizeof("{\"version\":" UD_PROTOCOL_VERSION_TEXT) - 1);
    if (!s)
        s = quoted_member(&b, "id", e->options.execution_id);
    if (!s)
        s = quoted_member(&b, "packageId", e->package->id);
    const ud_json *project = ud_member(e->package->document, "project");
    if (!s && project) {
        ud_string id;
        s = ud_json_string(ud_member(project, "id"), &id);
        if (!s)
            s = quoted_member(&b, "projectId", id);
    }
    if (!s)
        s = quoted_member(&b, "definitionHash", (ud_string){definition, 64});
    if (!s)
        s = quoted_member(&b, "parametersHash", (ud_string){parameter_hash, 64});
    if (!s && e->deployment_metadata.length) {
        s = ud_bytes_put(&b, ",\"deployment\":", 14);
        if (!s)
            s = ud_bytes_put(&b, e->deployment_metadata.data, e->deployment_metadata.length);
    }
    char values[768];
    int count =
        snprintf(values, sizeof(values),
                 ",\"state\":%d,\"result\":%d,\"inputRows\":%llu,\"outputRows\":%llu,"
                 "\"rejectedRows\":%llu,\"batches\":%llu,\"queuedBatches\":%llu,\"queueHighWater\":"
                 "%llu,\"memoryUsed\":%llu,\"executedTasks\":%llu,\"durableTasks\":%llu,"
                 "\"recoveryTasks\":%llu,\"resumedTasks\":%llu,\"spillBytes\":%llu,"
                 "\"pipelineTrees\":%llu,\"treeHighWater\":%llu,\"workerHighWater\":%llu}",
                 m.state, m.result, (unsigned long long)m.input_rows,
                 (unsigned long long)m.output_rows, (unsigned long long)m.rejected_rows,
                 (unsigned long long)m.batches, (unsigned long long)m.queued_batches,
                 (unsigned long long)m.queue_high_water, (unsigned long long)m.memory_used,
                 (unsigned long long)m.executed_tasks, (unsigned long long)m.durable_tasks,
                 (unsigned long long)m.recovery_tasks, (unsigned long long)m.resumed_tasks,
                 (unsigned long long)m.spill_bytes, (unsigned long long)m.pipeline_trees,
                 (unsigned long long)m.tree_high_water, (unsigned long long)m.worker_high_water);
    if (!s && (count < 0 || (size_t)count >= sizeof(values)))
        s = UD_OVERFLOW;
    if (!s)
        s = ud_bytes_put(&b, values, (size_t)count);
    if (!s && b.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s && b.size) {
        b.size--;
        char facts[512];
        int n = snprintf(
            facts, sizeof(facts),
            ",\"committedRows\":%llu,\"acknowledgedRows\":%llu,\"errorRows\":%llu,"
            "\"committedErrorRows\":%llu,\"acknowledgedErrorRows\":%llu,\"errorCount\":%llu,"
            "\"diagnostics\":",
            (unsigned long long)m.committed_rows, (unsigned long long)m.acknowledged_rows,
            (unsigned long long)m.error_rows, (unsigned long long)m.committed_error_rows,
            (unsigned long long)m.acknowledged_error_rows, (unsigned long long)m.error_count);
        s = ud_bytes_put(&b, facts, (size_t)n);
        char *diagnostics = NULL;
        uint32_t length = 0;
        if (!s && !progress && m.state != UD_EXEC_RUNNING && m.state != UD_EXEC_DRAINING)
            s = ud_execution_diagnostics((ud_execution *)e, &diagnostics, &length);
        if (!s)
            s = diagnostics ? ud_bytes_put(&b, diagnostics, length) : ud_bytes_put(&b, "null", 4);
        ud_free(diagnostics);
        if (!s && progress) {
            char observed[80];
            int observed_length = snprintf(observed, sizeof(observed), ",\"observedAtMs\":%llu",
                                           (unsigned long long)ud_wall_ms());
            s = ud_bytes_put(&b, observed, (size_t)observed_length);
            char *value = NULL;
            uint32_t size = 0;
            if (!s)
                s = ud_execution_progress((ud_execution *)e, &value, &size);
            if (!s)
                s = ud_bytes_put(&b, ",\"progress\":", 12);
            if (!s)
                s = ud_bytes_put(&b, value, size);
            ud_free(value);
        }
        if (!s)
            s = ud_bytes_put(&b, "}", 1);
    }
    ud_catalog_key key = {sizeof(key), UD_ABI_VERSION, UD_CATALOG_EXECUTION,
                          e->options.execution_id, revision};
    if (!s)
        s = ud_catalog_put(c, &key, (ud_string){(char *)b.data, (uint32_t)b.size});
    ud_bytes_clear(&b);
    return s;
}

ud_status ud_catalog_record_execution(ud_catalog *c, const ud_execution *e, uint64_t revision) {
    return record_execution(c, e, revision, 0);
}
ud_status ud_catalog_record_progress(ud_catalog *c, const ud_execution *e, uint64_t revision) {
    return record_execution(c, e, revision, 1);
}
