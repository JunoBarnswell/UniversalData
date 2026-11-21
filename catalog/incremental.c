#include "internal.h"
#include "../engines/package/incremental.h"

static ud_catalog_key audit_key(ud_string id, uint64_t revision) {
    return (ud_catalog_key){sizeof(ud_catalog_key), UD_ABI_VERSION, UD_CATALOG_AUDIT, id, revision};
}

static int named_member(const ud_json *document, const char *member, const char *value) {
    ud_string text;
    return !ud_json_string(ud_member(document, member), &text) && ud_named(text, value);
}

static int true_member(const ud_json *document, const char *member) {
    const ud_json *entry = ud_member(document, member);
    ud_string value;
    return ud_json_kind(entry) == UD_JSON_BOOL &&
           !ud_json_text(entry, &value.data, &value.length) && ud_named(value, "true");
}

static ud_status read_record(ud_catalog *catalog, ud_catalog_key key, ud_json **document) {
    char *text = NULL;
    uint32_t length = 0;
    ud_status status = ud_catalog_get(catalog, &key, &text, &length);
    if (!status)
        status = ud_json_parse(catalog->ctx, text, length, document, NULL);
    ud_free(text);
    return status;
}

static ud_status checkpoint_key(ud_catalog *catalog, const ud_json *run, char key[256],
                                 ud_catalog_key *out) {
    ud_string job;
    UD_TRY(ud_json_string(ud_member(run, "jobId"), &job));
    int length = snprintf(key, 256, "incremental/%.*s", (int)job.length, job.data);
    if (!job.length || length < 0 || length >= 256)
        return UD_SCHEMA;
    *out = audit_key((ud_string){key, (uint32_t)length}, 0);
    ud_status status = ud_catalog_latest_revision(catalog, out->kind, out->id, &out->revision);
    return status == UD_END ? UD_OK : status;
}

static ud_status write_position(ud_catalog *catalog, ud_catalog_key key, ud_string owner,
                                 ud_string fingerprint, const ud_json *after) {
    const char header[] = "{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"id\":";
    ud_bytes bytes = {.ctx = catalog->ctx};
    ud_status status = ud_bytes_put(&bytes, header, sizeof(header) - 1);
    if (!status)
        status = ud_json_quote(&bytes, key.id.data, key.id.length);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"ownerRun\":", sizeof(",\"ownerRun\":") - 1);
    if (!status)
        status = owner.length ? ud_json_quote(&bytes, owner.data, owner.length) :
                                ud_bytes_put(&bytes, "null", 4);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"fingerprint\":", sizeof(",\"fingerprint\":") - 1);
    if (!status)
        status = ud_json_quote(&bytes, fingerprint.data, fingerprint.length);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"after\":", sizeof(",\"after\":") - 1);
    char *values = NULL;
    uint32_t length = 0;
    if (!status && after)
        status = ud_json_serialize(after, &values, &length);
    if (!status)
        status = ud_bytes_put(&bytes, values ? values : "null", values ? length : 4);
    ud_free(values);
    if (!status)
        status = ud_bytes_put(&bytes, "}", 1);
    if (!status && key.revision == UINT64_MAX)
        status = UD_OVERFLOW;
    if (!status) {
        key.revision++;
        status = ud_catalog_append_document(catalog, &key,
            (ud_string){(const char *)bytes.data, (uint32_t)bytes.size}, 1, key.revision - 1, NULL, 0);
    }
    ud_bytes_clear(&bytes);
    return status;
}

static ud_status parse_run(ud_catalog *catalog, const ud_catalog_lease *lease, ud_string document,
                            ud_json **run) {
    if (!lease || lease->struct_size != sizeof(*lease) || lease->abi_version != UD_ABI_VERSION ||
        lease->kind != UD_CATALOG_AUDIT || !lease->id.data || lease->id.length <= 4 ||
        memcmp(lease->id.data, "run/", 4))
        return UD_INVALID;
    UD_TRY(ud_json_parse(catalog->ctx, document.data, document.length, run, NULL));
    ud_string id;
    UD_TRY(ud_json_string(ud_member(*run, "id"), &id));
    return ud_same(id, lease->id) ? UD_OK : UD_FENCED;
}

ud_status ud_catalog_incremental_claim(ud_catalog *catalog, const ud_catalog_lease *lease,
                                      uint64_t revision, ud_string run_text, ud_string package_text,
                                      ud_string parameter_text, char **binding, uint32_t *length) {
    if (!catalog || !binding || !length || revision < 2)
        return UD_INVALID;
    *binding = NULL;
    *length = 0;
    ud_json *package = NULL, *parameters = NULL, *run = NULL, *current = NULL;
    UD_TRY(ud_json_parse(catalog->ctx, package_text.data, package_text.length, &package, NULL));
    ud_incremental_plan plan;
    ud_status status = ud_json_parse(catalog->ctx, parameter_text.data, parameter_text.length, &parameters, NULL);
    if (!status)
        status = ud_incremental_describe(catalog->ctx, package, parameters, &plan);
    if (!status)
        status = parse_run(catalog, lease, run_text, &run);
    ud_string package_id = {0}, run_package = {0};
    if (!status)
        status = ud_json_string(ud_member(package, "id"), &package_id);
    if (!status)
        status = ud_json_string(ud_member(run, "packageId"), &run_package);
    if (!status && (!ud_same(package_id, run_package) || !named_member(run, "state", "Claimed") ||
                    !true_member(run, "incremental")))
        status = UD_INVALID;
    if (!status)
        status = ud_catalog_begin(catalog, 0);
    int transaction = !status;
    char key_text[256];
    ud_catalog_key key;
    if (!status)
        status = checkpoint_key(catalog, run, key_text, &key);
    if (!status && key.revision)
        status = read_record(catalog, key, &current);
    ud_string prior_owner = {0}, prior_fingerprint = {0};
    ud_string fingerprint = {plan.fingerprint, 64};
    const ud_json *after = plan.initial;
    if (!status && current) {
        const ud_json *owner = ud_member(current, "ownerRun");
        if (ud_json_kind(owner) != UD_JSON_NULL)
            status = ud_json_string(owner, &prior_owner);
        if (!status && prior_owner.length && !ud_same(prior_owner, lease->id))
            status = UD_BUSY;
        if (!status)
            status = ud_json_string(ud_member(current, "fingerprint"), &prior_fingerprint);
        if (!status && ud_same(prior_fingerprint, fingerprint))
            after = ud_member(current, "after");
        else if (!status && (prior_owner.length || ud_json_kind(after) != UD_JSON_ARRAY))
            status = UD_INCOMPATIBLE;
    }
    if (!status && ud_json_kind(after) != UD_JSON_NULL && ud_json_kind(after) != UD_JSON_ARRAY)
        status = UD_SCHEMA;
    if (!status && !prior_owner.length)
        status = write_position(catalog, key, lease->id, fingerprint, after);
    if (!status) {
        ud_catalog_key run_key = audit_key(lease->id, revision);
        status = ud_catalog_append_document(catalog, &run_key, run_text, 1, revision - 1, lease, 0);
    }
    ud_bytes response = {.ctx = catalog->ctx};
    if (!status)
        status = ud_incremental_binding_json(&response, &plan, after);
    if (!status) {
        *binding = ud_strdup_n(catalog->ctx, (const char *)response.data, response.size);
        if (!*binding)
            status = UD_RESOURCE;
        else
            *length = (uint32_t)response.size;
    }
    ud_bytes_clear(&response);
    if (transaction)
        status = ud_catalog_finish(catalog, status);
    if (status) {
        ud_free(*binding);
        *binding = NULL;
        *length = 0;
    }
    ud_json_destroy(current);
    ud_json_destroy(run);
    ud_json_destroy(parameters);
    ud_json_destroy(package);
    return status;
}

ud_status ud_catalog_incremental_settle(ud_catalog *catalog, const ud_catalog_lease *lease,
                                       uint64_t revision, ud_string run_text, ud_string proof_text) {
    if (!catalog || revision < 2)
        return UD_INVALID;
    ud_json *run = NULL, *current = NULL, *proof = NULL, *previous = NULL;
    ud_status status = parse_run(catalog, lease, run_text, &run);
    int success = !status && named_member(run, "state", "Succeeded");
    if (!status && (success != (proof_text.length != 0) || !true_member(run, "incremental")))
        status = success ? UD_COMMIT_UNKNOWN : UD_INVALID;
    if (!status && !success && !named_member(run, "state", "Cancelled") &&
        !named_member(run, "state", "Failed") && !named_member(run, "state", "Stopped"))
        status = UD_INVALID;
    if (!status)
        status = ud_catalog_begin(catalog, 0);
    int transaction = !status;
    char key_text[256];
    ud_catalog_key key;
    if (!status)
        status = checkpoint_key(catalog, run, key_text, &key);
    if (!status)
        status = key.revision ? read_record(catalog, key, &current) : UD_FENCED;
    ud_string owner = {0}, fingerprint = {0};
    if (!status)
        status = ud_json_string(ud_member(current, "ownerRun"), &owner);
    if (!status && !ud_same(owner, lease->id))
        status = UD_FENCED;
    if (!status)
        status = ud_json_string(ud_member(current, "fingerprint"), &fingerprint);
    const ud_json *after = ud_member(current, "after");
    if (!status && success) {
        status = ud_json_parse(catalog->ctx, proof_text.data, proof_text.length, &proof, NULL);
        ud_string proof_fingerprint, execution, executed;
        if (!status)
            status = ud_json_string(ud_member(proof, "fingerprint"), &proof_fingerprint);
        if (!status && !ud_same(proof_fingerprint, fingerprint))
            status = UD_FENCED;
        if (!status)
            status = ud_json_string(ud_member(run, "executionId"), &execution);
        if (!status)
            status = ud_json_string(ud_member(proof, "executionId"), &executed);
        if (!status && (!execution.length || !ud_same(execution, executed)))
            status = UD_FENCED;
        const ud_json *confirmed = ud_member(proof, "after");
        if (!status && ud_json_kind(confirmed) == UD_JSON_ARRAY && ud_json_size(confirmed))
            after = confirmed;
        else if (!status && (!confirmed || ud_json_kind(confirmed) != UD_JSON_NULL))
            status = UD_CORRUPT;
    }
    if (!status && !success) {
        status = read_record(catalog, audit_key(lease->id, revision - 1), &previous);
        if (!status && (true_member(previous, "startAuthorized") || true_member(previous, "resume")))
            status = UD_COMMIT_UNKNOWN;
    }
    if (!status) {
        ud_catalog_key run_key = audit_key(lease->id, revision);
        status = ud_catalog_append_document(catalog, &run_key, run_text, 1, revision - 1, lease, 0);
    }
    if (!status)
        status = write_position(catalog, key, (ud_string){0}, fingerprint, after);
    if (transaction)
        status = ud_catalog_finish(catalog, status);
    ud_json_destroy(previous);
    ud_json_destroy(proof);
    ud_json_destroy(current);
    ud_json_destroy(run);
    return status;
}
