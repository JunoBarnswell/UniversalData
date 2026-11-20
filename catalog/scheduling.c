#include "internal.h"

static int audit_key(const ud_catalog_key *key, const char *prefix) {
    size_t prefix_length = strlen(prefix);
    return key->kind == UD_CATALOG_AUDIT && key->revision && key->id.data &&
           key->id.length > prefix_length && !memcmp(key->id.data, prefix, prefix_length);
}

ud_status ud_catalog_publish_occurrence(ud_catalog *catalog, const ud_catalog_job_occurrence *request,
                                        const ud_catalog_lease *job_lease) {
    if (!catalog || !request || !job_lease)
        return UD_INVALID;
    if (request->struct_size != sizeof(*request) || request->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!audit_key(&request->job, "job/") || request->job.revision < 2 ||
        !audit_key(&request->occurrence, "occurrence/") ||
        (request->run_document.length &&
         (!audit_key(&request->run, "run/") || request->run.revision != 1)))
        return UD_INVALID;

    UD_TRY(ud_catalog_begin(catalog, 0));
    ud_status status = ud_catalog_append_document(catalog, &request->job, request->job_document, 1,
                                                  request->job.revision - 1, job_lease, 0);
    if (!status)
        status = ud_catalog_append_document(catalog, &request->occurrence,
                                            request->occurrence_document, 1,
                                            request->occurrence.revision - 1, NULL, 0);
    if (!status && request->run_document.length)
        status = ud_catalog_append_document(catalog, &request->run, request->run_document, 1,
                                            0, NULL, 0);
    return ud_catalog_finish(catalog, status);
}
