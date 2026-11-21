#include "internal.h"
#include "ud/json.h"

/* Publication is deliberately owned by the Catalog. The management layer
 * prepares opaque, already-authorized documents; this function is the only
 * write path for the package/deployment/job bundle. */

static int publication_string(ud_string value, int required, uint32_t maximum) {
    return (!required || value.length) && (!value.length ||
        (value.data && value.length <= maximum && !memchr(value.data, 0, value.length) &&
         ud_utf8_valid(value.data, value.length)));
}

static ud_status publication_grant(ud_catalog *catalog, int action) {
    return catalog->options.authorize &&
                   catalog->options.authorize(catalog->options.authorize_user,
                                               catalog->options.tenant,
                                               catalog->options.directory, action) == UD_OK
               ? UD_OK
               : UD_AUTH;
}

static ud_catalog_key publication_key(int kind, ud_string id, uint64_t revision) {
    return (ud_catalog_key){sizeof(ud_catalog_key), UD_ABI_VERSION, kind, id, revision};
}

static void publication_hex(const uint8_t digest[32], char text[65]) {
    static const char digits[] = "0123456789abcdef";
    for (uint32_t at = 0; at < 32; at++) {
        text[at * 2] = digits[digest[at] >> 4];
        text[at * 2 + 1] = digits[digest[at] & 15];
    }
    text[64] = 0;
}

/* The subject is intentionally hashed. A Catalog row must be tenant/subject
 * scoped for idempotency, but it must not become another subject directory. */
static ud_status publication_identity(ud_catalog *catalog,
                                       const ud_catalog_publication_request *request,
                                       ud_catalog_key *key, char id[256]) {
    ud_bytes identity = {.ctx = catalog->ctx};
    uint8_t digest[32];
    ud_status status = ud_bytes_put(&identity, catalog->tenant, strlen(catalog->tenant));
    if (!status)
        status = ud_bytes_put(&identity, "\0", 1);
    if (!status)
        status = ud_bytes_put(&identity, request->subject.data, request->subject.length);
    if (!status)
        status = ud_bytes_put(&identity, "\0", 1);
    if (!status)
        status = ud_bytes_put(&identity, request->request_id.data, request->request_id.length);
    if (!status)
        ud_hash(identity.data, identity.size, digest);
    char hex[65];
    if (!status)
        publication_hex(digest, hex);
    int count = !status ? snprintf(id, 256, "publication/%s/%.*s", hex,
                                    (int)request->request_id.length,
                                    request->request_id.data) : 0;
    if (!status && (count < 1 || count >= 256))
        status = UD_OVERFLOW;
    if (!status)
        *key = publication_key(UD_CATALOG_AUDIT,
                               (ud_string){id, (uint32_t)count}, 1);
    ud_bytes_clear(&identity);
    return status;
}

static ud_status append_member(ud_bytes *bytes, const char *name, ud_string value) {
    UD_TRY(ud_bytes_put(bytes, ",", 1));
    UD_TRY(ud_json_quote(bytes, name, (uint32_t)strlen(name)));
    UD_TRY(ud_bytes_put(bytes, ":", 1));
    return ud_json_quote(bytes, value.data, value.length);
}

static ud_status publication_receipt(ud_catalog *catalog,
                                     const ud_catalog_publication_request *request,
                                     const ud_catalog_key *deployment_state,
                                     const ud_catalog_key *occurrence,
                                     char **receipt, uint32_t *length) {
    *receipt = NULL;
    *length = 0;
    char *deployment = NULL, *occurrence_text = NULL;
    uint32_t deployment_length = 0, occurrence_length = 0;
    ud_status status = ud_catalog_get(catalog, deployment_state, &deployment, &deployment_length);
    if (!status && occurrence)
        status = ud_catalog_get(catalog, occurrence, &occurrence_text, &occurrence_length);
    ud_bytes bytes = {.ctx = catalog->ctx};
    char revision[32];
    int count = snprintf(revision, sizeof(revision), "%llu",
                         (unsigned long long)(request->expected_package_revision + 1));
    if (!status && (count < 1 || (size_t)count >= sizeof(revision)))
        status = UD_OVERFLOW;
    if (!status)
        status = ud_bytes_put(&bytes, "{\"id\":", sizeof("{\"id\":") - 1);
    if (!status)
        status = ud_json_quote(&bytes, request->package_id.data, request->package_id.length);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"revision\":", sizeof(",\"revision\":") - 1);
    if (!status)
        status = ud_json_quote(&bytes, revision, (uint32_t)count);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"deployment\":", sizeof(",\"deployment\":") - 1);
    if (!status)
        status = ud_bytes_put(&bytes, deployment, deployment_length);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"occurrence\":", sizeof(",\"occurrence\":") - 1);
    if (!status)
        status = occurrence_text ? ud_bytes_put(&bytes, occurrence_text, occurrence_length)
                                 : ud_bytes_put(&bytes, "null", 4);
    if (!status)
        status = ud_bytes_put(&bytes, "}", 1);
    if (!status && bytes.size > UINT32_MAX)
        status = UD_OVERFLOW;
    if (!status) {
        *receipt = (char *)bytes.data;
        *length = (uint32_t)bytes.size;
        bytes.data = NULL;
    }
    ud_bytes_clear(&bytes);
    ud_free(deployment);
    ud_free(occurrence_text);
    return status;
}

static ud_status publication_record(ud_catalog *catalog,
                                    const ud_catalog_publication_request *request,
                                    const ud_catalog_key *identity,
                                    ud_string receipt, char **record, uint32_t *length) {
    *record = NULL;
    *length = 0;
    ud_bytes bytes = {.ctx = catalog->ctx};
    ud_status status = ud_bytes_put(&bytes, "{\"version\":" UD_PROTOCOL_VERSION_TEXT,
                                    sizeof("{\"version\":" UD_PROTOCOL_VERSION_TEXT) - 1);
    if (!status)
        status = append_member(&bytes, "id", identity->id);
    if (!status)
        status = append_member(&bytes, "fingerprint", request->fingerprint);
    if (!status) {
        status = ud_bytes_put(&bytes, ",\"receipt\":", sizeof(",\"receipt\":") - 1);
        if (!status)
            status = ud_bytes_put(&bytes, receipt.data, receipt.length);
    }
    if (!status)
        status = ud_bytes_put(&bytes, "}", 1);
    if (!status && bytes.size > UINT32_MAX)
        status = UD_OVERFLOW;
    if (!status) {
        *record = (char *)bytes.data;
        *length = (uint32_t)bytes.size;
        bytes.data = NULL;
    }
    ud_bytes_clear(&bytes);
    return status;
}

static ud_status publication_existing(ud_catalog *catalog,
                                      const ud_catalog_publication_request *request,
                                      const ud_catalog_key *identity,
                                      char **receipt, uint32_t *length, int *found) {
    *receipt = NULL;
    *length = 0;
    *found = 0;
    char *record_text = NULL;
    uint32_t record_length = 0;
    ud_status status = ud_catalog_get(catalog, identity, &record_text, &record_length);
    if (status == UD_END)
        return UD_OK;
    if (status)
        return status;
    ud_json *record = NULL;
    status = ud_json_parse(catalog->ctx, record_text, record_length, &record, NULL);
    ud_string fingerprint = {0};
    if (!status)
        status = ud_json_string(ud_member(record, "fingerprint"), &fingerprint);
    if (!status && !ud_same(fingerprint, request->fingerprint))
        status = UD_FENCED;
    const ud_json *stored = !status ? ud_member(record, "receipt") : NULL;
    if (!status && (!stored || ud_json_kind(stored) != UD_JSON_OBJECT))
        status = UD_CORRUPT;
    if (!status)
        status = ud_json_serialize(stored, receipt, length);
    if (!status) {
        *found = 1;
        if (*length > UINT32_MAX)
            status = UD_OVERFLOW;
    }
    ud_json_destroy(record);
    ud_free(record_text);
    if (status) {
        ud_free(*receipt);
        *receipt = NULL;
        *length = 0;
    }
    return status;
}

static ud_status publication_validate(const ud_catalog_publication_request *request) {
    if (!request)
        return UD_INVALID;
    if (request->struct_size != sizeof(*request) || request->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!publication_string(request->request_id, 1, 255) ||
        !publication_string(request->subject, 1, 255) ||
        !publication_string(request->fingerprint, 1, 255) ||
        !publication_string(request->package_id, 1, 255) ||
        !publication_string(request->package_document, 1, UINT32_MAX) ||
        !publication_string(request->project_id, 1, 255) ||
        !publication_string(request->project_document, 1, UINT32_MAX) ||
        !publication_string(request->deployment_id, 1, 255) ||
        !publication_string(request->deployment_document, 1, UINT32_MAX) ||
        !publication_string(request->deployment_state_id, 1, 255) ||
        !publication_string(request->deployment_state_document, 1, UINT32_MAX) ||
        !publication_string(request->job_id, 1, 255) ||
        !publication_string(request->job_document, 1, UINT32_MAX))
        return UD_INVALID;
    if (request->expected_package_revision == UINT64_MAX ||
        !request->project_revision || !request->deployment_revision ||
        !request->deployment_state_revision || !request->job_revision)
        return UD_INVALID;
    int occurrence = request->occurrence_document.length != 0;
    int run = request->run_document.length != 0;
    if (occurrence != (request->occurrence_id.length != 0) ||
        occurrence && !request->occurrence_revision || run != (request->run_id.length != 0) ||
        run && !request->run_revision || run && !occurrence ||
        !publication_string(request->occurrence_id, 0, 255) ||
        !publication_string(request->occurrence_document, 0, UINT32_MAX) ||
        !publication_string(request->run_id, 0, 255) ||
        !publication_string(request->run_document, 0, UINT32_MAX))
        return UD_INVALID;
    return UD_OK;
}

ud_status ud_catalog_find_publication(ud_catalog *catalog, ud_string request_id, ud_string subject,
                                      ud_string fingerprint, char **receipt, uint32_t *length) {
    if (!catalog || !receipt || !length || !publication_string(request_id, 1, 255) ||
        !publication_string(subject, 1, 255) || !publication_string(fingerprint, 1, 255))
        return UD_INVALID;
    ud_catalog_publication_request request = {.request_id = request_id, .subject = subject,
                                               .fingerprint = fingerprint};
    ud_catalog_key identity;
    char identity_id[256];
    UD_TRY(publication_identity(catalog, &request, &identity, identity_id));
    int found = 0;
    ud_status status = publication_existing(catalog, &request, &identity, receipt, length, &found);
    return status ? status : found ? UD_OK : UD_END;
}

static ud_status publication_append(ud_catalog *catalog, int kind, ud_string id,
                                    uint64_t revision, ud_string document) {
    ud_catalog_key key = publication_key(kind, id, revision);
    return ud_catalog_append_document(catalog, &key, document, 1, revision - 1, NULL, 0);
}

ud_status ud_catalog_publish_job(ud_catalog *catalog,
                                 const ud_catalog_publication_request *request,
                                 char **receipt, uint32_t *length) {
    if (!catalog || !receipt || !length)
        return UD_INVALID;
    *receipt = NULL;
    *length = 0;
    UD_TRY(publication_validate(request));
    UD_TRY(publication_grant(catalog, UD_ACCESS_WRITE));

    ud_catalog_key identity;
    char identity_id[256];
    UD_TRY(publication_identity(catalog, request, &identity, identity_id));
    UD_TRY(ud_catalog_begin(catalog, 0));

    int found = 0;
    ud_status status = publication_existing(catalog, request, &identity, receipt, length, &found);
    if (!status && found) {
        status = ud_catalog_finish(catalog, UD_OK);
        if (status) {
            ud_free(*receipt);
            *receipt = NULL;
            *length = 0;
        }
        return status;
    }

    uint64_t package_revision = request->expected_package_revision + 1;
    ud_catalog_key deployment_state = publication_key(UD_CATALOG_AUDIT,
                                                      request->deployment_state_id,
                                                      request->deployment_state_revision);
    ud_catalog_key occurrence = {0};
    if (!status && request->occurrence_document.length)
        occurrence = publication_key(UD_CATALOG_AUDIT, request->occurrence_id,
                                     request->occurrence_revision);

    if (!status)
        status = publication_append(catalog, UD_CATALOG_PACKAGE, request->package_id,
                                    package_revision, request->package_document);
    if (!status)
        status = publication_append(catalog, UD_CATALOG_PROJECT, request->project_id,
                                    request->project_revision, request->project_document);
    if (!status)
        status = publication_append(catalog, UD_CATALOG_DEPLOYMENT, request->deployment_id,
                                    request->deployment_revision, request->deployment_document);
    if (!status)
        status = publication_append(catalog, UD_CATALOG_AUDIT, request->deployment_state_id,
                                    request->deployment_state_revision,
                                    request->deployment_state_document);
    if (!status)
        status = publication_append(catalog, UD_CATALOG_AUDIT, request->job_id,
                                    request->job_revision, request->job_document);
    if (!status && request->occurrence_document.length)
        status = publication_append(catalog, UD_CATALOG_AUDIT, request->occurrence_id,
                                    request->occurrence_revision, request->occurrence_document);
    if (!status && request->run_document.length)
        status = publication_append(catalog, UD_CATALOG_AUDIT, request->run_id,
                                    request->run_revision, request->run_document);

    if (!status)
        status = publication_receipt(catalog, request, &deployment_state,
                                     request->occurrence_document.length ? &occurrence : NULL,
                                     receipt, length);
    char *record = NULL;
    uint32_t record_length = 0;
    if (!status)
        status = publication_record(catalog, request, &identity,
                                    (ud_string){*receipt, *length}, &record, &record_length);
    if (!status)
        status = ud_catalog_append_document(catalog, &identity,
                                            (ud_string){record, record_length}, 1, 0, NULL, 0);
    ud_free(record);
    if (status) {
        ud_free(*receipt);
        *receipt = NULL;
        *length = 0;
    }
    status = ud_catalog_finish(catalog, status);
    if (status) {
        ud_free(*receipt);
        *receipt = NULL;
        *length = 0;
    }
    return status;
}

static int occurrence_audit_key(const ud_catalog_key *key, const char *prefix) {
    size_t prefix_length = strlen(prefix);
    return key->kind == UD_CATALOG_AUDIT && key->revision && key->id.data &&
           key->id.length > prefix_length && !memcmp(key->id.data, prefix, prefix_length);
}

ud_status ud_catalog_publish_occurrence(ud_catalog *catalog,
                                        const ud_catalog_job_occurrence *request,
                                        const ud_catalog_lease *job_lease) {
    if (!catalog || !request || !job_lease)
        return UD_INVALID;
    if (request->struct_size != sizeof(*request) || request->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!occurrence_audit_key(&request->job, "job/") || request->job.revision < 2 ||
        !occurrence_audit_key(&request->occurrence, "occurrence/") ||
        (request->run_document.length &&
         (!occurrence_audit_key(&request->run, "run/") || request->run.revision != 1)))
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
