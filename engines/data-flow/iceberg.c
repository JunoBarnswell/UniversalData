#include "builtins_internal.h"

#include "../../connectors/iceberg_table.h"
#include "../../connectors/network.h"
#include "../../connectors/s3_object.h"
#include "ud/http.h"

#include <limits.h>

/* Added by the native Iceberg connector: read_open resolves CURRENT to the
 * immutable snapshot used by its scan, and this getter exposes that choice
 * for the component checkpoint. */

/* The C++ connector owns Parquet and REST details; this file owns Package
 * grants, source projection, lifecycle, changeset projection and Journal
 * effect fencing. */
typedef struct iceberg_component {
    ud_component_context context;
    ud_json *config;
    ud_batch *schema;
    ud_connection_lease *catalog;
    ud_connection_lease *storage;
    ud_string s3_root;
    ud_iceberg_table *table;
    uint32_t *keys;
    uint32_t key_count;
    uint32_t before_column;
    int32_t mode;
    int source;
    int read_open;
    uint64_t skip_batches;
    uint64_t number;
    ud_string publishing;
    int submission_recorded;
    int64_t snapshot_id;
    int source_metadata_loaded;
    char source_policy[65];
    ud_iceberg_read_options read_options;
} iceberg_component;

static int named(ud_string value, const char *literal) {
    size_t length = strlen(literal);
    return value.length == length && !memcmp(value.data, literal, length);
}

static int field_named(const ud_field *field, ud_string name) {
    return field && field->name_length == name.length &&
           !memcmp(field->name, name.data, name.length);
}

static int path_has_parent(ud_string path) {
    uint32_t at = 0;
    while (at < path.length) {
        while (at < path.length && (path.data[at] == '/' || path.data[at] == '\\'))
            at++;
        uint32_t end = at;
        while (end < path.length && path.data[end] != '/' && path.data[end] != '\\')
            end++;
        if (end - at == 2 && path.data[at] == '.' && path.data[at + 1] == '.')
            return 1;
        at = end;
    }
    return 0;
}

static ud_status mode_value(const ud_json *config, int32_t *mode) {
    ud_string value = {"append", 6};
    if (ud_member(config, "mode"))
        UD_TRY(ud_json_string(ud_member(config, "mode"), &value));
    *mode = named(value, "append")      ? UD_WRITE_APPEND
           : named(value, "upsert")    ? UD_WRITE_UPSERT
           : named(value, "changeset") ? UD_WRITE_CHANGESET
                                        : 0;
    return *mode ? UD_OK : UD_SCHEMA;
}

static ud_status before_index(const ud_json *config, const ud_batch *input, uint32_t *out) {
    *out = UINT32_MAX;
    const ud_json *value = ud_member(config, "beforeField");
    if (!value)
        return UD_OK;
    ud_string name;
    UD_TRY(ud_json_string(value, &name));
    for (uint32_t i = 0; i < input->columns; i++)
        if (field_named(&input->column[i].field, name)) {
            *out = i;
            break;
        }
    if (*out == UINT32_MAX || input->column[*out].field.type != UD_STRUCT)
        return UD_SCHEMA;
    return UD_OK;
}

static ud_status validate_before(const ud_batch *input, uint32_t before) {
    if (before == UINT32_MAX)
        return UD_OK;
    const ud_field *field = &input->column[before].field;
    if (field->child_count != input->columns - 1)
        return UD_SCHEMA;
    uint32_t child = 0;
    for (uint32_t column = 0; column < input->columns; column++) {
        if (column == before)
            continue;
        if (!ud_field_matches(field->children + child, &input->column[column].field,
                              UD_FIELD_LAYOUT))
            return UD_SCHEMA;
        child++;
    }
    return UD_OK;
}

static ud_status validate_config(const ud_json *config, const ud_batch *input,
                                 int32_t *mode, uint32_t *before, uint32_t *key_count) {
    static const char *keys[] = {"connection", "storageConnection", "warehouse", "namespace",
                                 "table", "mode", "keys", "beforeField"};
    UD_TRY(ud_json_keys(config, keys, sizeof(keys) / sizeof(*keys)));
    ud_string text;
    UD_TRY(ud_json_string(ud_member(config, "connection"), &text));
    UD_TRY(ud_json_string(ud_member(config, "storageConnection"), &text));
    UD_TRY(ud_json_string(ud_member(config, "namespace"), &text));
    UD_TRY(ud_json_string(ud_member(config, "table"), &text));
    UD_TRY(mode_value(config, mode));
    UD_TRY(before_index(config, input, before));
    UD_TRY(validate_before(input, *before));
    const ud_json *key_values = ud_member(config, "keys");
    if (key_values && ud_json_kind(key_values) != UD_JSON_ARRAY)
        return UD_SCHEMA;
    *key_count = key_values ? ud_json_size(key_values) : 0;
    if (*key_count > 16 || (*mode != UD_WRITE_APPEND && !*key_count))
        return UD_SCHEMA;
    if (*mode == UD_WRITE_APPEND && *before != UINT32_MAX)
        return UD_SCHEMA;
    uint32_t seen[16] = {0};
    for (uint32_t i = 0; i < *key_count; i++) {
        uint64_t index;
        UD_TRY(ud_json_uint(ud_json_at(key_values, i), &index));
        if (index >= input->columns || index == *before || index > UINT32_MAX)
            return UD_SCHEMA;
        for (uint32_t j = 0; j < i; j++)
            if (seen[j] == index)
                return UD_SCHEMA;
        seen[i] = (uint32_t)index;
    }
    return UD_OK;
}

static ud_status validate_source_config(const ud_json *config) {
    static const char *keys[] = {"connection", "storageConnection", "warehouse", "namespace",
                                 "table", "schema", "snapshotId", "batchSize"};
    UD_TRY(ud_json_keys(config, keys, sizeof(keys) / sizeof(*keys)));
    ud_string text;
    UD_TRY(ud_json_string(ud_member(config, "connection"), &text));
    UD_TRY(ud_json_string(ud_member(config, "storageConnection"), &text));
    UD_TRY(ud_json_string(ud_member(config, "namespace"), &text));
    UD_TRY(ud_json_string(ud_member(config, "table"), &text));
    if (ud_json_kind(ud_member(config, "schema")) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    if (ud_member(config, "snapshotId")) {
        uint64_t snapshot;
        UD_TRY(ud_json_uint(ud_member(config, "snapshotId"), &snapshot));
        if (snapshot > INT64_MAX)
            return UD_SCHEMA;
    }
    if (ud_member(config, "batchSize")) {
        uint64_t batch;
        UD_TRY(ud_json_uint(ud_member(config, "batchSize"), &batch));
        if (!batch || batch > 65536)
            return UD_SCHEMA;
    }
    return UD_OK;
}

static ud_status validate_connections(const ud_component_context *context, const ud_json *config) {
    ud_string connection = {0}, storage = {0};
    UD_TRY(ud_json_string(ud_member(config, "connection"), &connection));
    UD_TRY(ud_json_string(ud_member(config, "storageConnection"), &storage));
    UD_TRY(ud_builtin_connection_defined(context, connection));
    return ud_builtin_connection_defined(context, storage);
}

static ud_status request(void *user, const ud_http_options *options, ud_http_response *response) {
    iceberg_component *component = user;
    if (!component || !options || !response)
        return UD_INVALID;
    return ud_http_call(&component->context, component->catalog, options, response);
}

static ud_status authorize_file(void *user, ud_string location, int32_t action) {
    iceberg_component *component = user;
    if (location.length >= 5 && !memcmp(location.data, "s3://", 5)) {
        if (!component || !component->storage)
            return UD_AUTH;
        return ud_s3_object_authorize(&component->context, component->storage, location, action);
    }
    ud_string root = {0};
    if (!component || !component->storage || !location.length ||
        ud_connection_lease_path(component->storage, &root) != UD_OK || !root.length)
        return UD_AUTH;
    ud_string path = location;
    if (path.length >= 7 && !memcmp(path.data, "file://", 7)) {
        path.data += 7;
        path.length -= 7;
    }
    if (memchr(path.data, 0, path.length) || path_has_parent(path) || path.length < root.length ||
        memcmp(path.data, root.data, root.length) ||
        (path.length > root.length && path.data[root.length] != '/' &&
         path.data[root.length] != '\\'))
        return UD_AUTH;
    UD_TRY(ud_connection_lease_authorize(component->storage, action));
    ud_execution *execution = component->context.execution;
    return execution->options.authorize && execution->options.tenant.length &&
                   execution->options.authorize(execution->options.authorize_user,
                                                execution->options.tenant, path, action) == UD_OK
               ? UD_OK : UD_AUTH;
}

static ud_status check(void *user) {
    iceberg_component *component = user;
    return component ? ud_activation_check(component->context.activation) : UD_INVALID;
}

static ud_status before_commit(void *user) {
    iceberg_component *component = user;
    if (!component || !component->publishing.length)
        return UD_INVALID;
    UD_TRY(ud_connection_lease_authorize(component->catalog, UD_ACCESS_NETWORK));
    UD_TRY(ud_connection_lease_authorize(component->storage, UD_ACCESS_WRITE));
    if (!component->submission_recorded) {
        const ud_string state = {"{\"submitted\":true}", sizeof("{\"submitted\":true}") - 1};
        UD_TRY(ud_checkpoint_snapshot_seal(component->context.activation, component->publishing,
                                          state));
        component->submission_recorded = 1;
    }
    return UD_OK;
}

static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    if (!context || !outputs)
        return UD_SCHEMA;
    ud_json *config = NULL;
    ud_status status = ud_builtin_parse(context, text, &config);
    if (!status)
        status = validate_connections(context, config);
    if ((intptr_t)user) {
        if (!status)
            status = validate_source_config(config);
        if (!status)
            status = ud_builtin_schema_config(context, config, outputs);
        ud_json_destroy(config);
        return status;
    }
    if (!inputs || !inputs[0])
        status = UD_SCHEMA;
    int32_t mode = 0;
    uint32_t before = UINT32_MAX, key_count = 0;
    if (!status)
        status = validate_config(config, inputs[0], &mode, &before, &key_count);
    ud_json_destroy(config);
    return status;
}

static ud_status read_lease_resources(iceberg_component *component, ud_string *catalog_url,
                                      ud_string *storage_root);

static ud_status open_table(iceberg_component *component, const ud_batch *schema_batch) {
    ud_string catalog_connection = {0}, storage_connection = {0};
    ud_string namespace_name = {0}, table_name = {0}, warehouse = {0};
    ud_status status = ud_json_string(ud_member(component->config, "connection"),
                                      &catalog_connection);
    if (!status)
        status = ud_json_string(ud_member(component->config, "storageConnection"),
                                &storage_connection);
    if (status)
        return status;
    status = ud_json_string(ud_member(component->config, "namespace"), &namespace_name);
    if (status)
        return status;
    status = ud_json_string(ud_member(component->config, "table"), &table_name);
    if (status)
        return status;
    if (ud_member(component->config, "warehouse")) {
        status = ud_json_string(ud_member(component->config, "warehouse"), &warehouse);
        if (status)
            return status;
    }
    status = ud_activation_acquire_connection(component->context.activation, catalog_connection,
                                               UD_CONNECT_HTTP, &component->catalog);
    if (status)
        return status;
    uint64_t storage_caps = component->source
                                ? UD_CONNECT_READ
                                : UD_CONNECT_READ | UD_CONNECT_APPEND | UD_CONNECT_DELETE;
    status = ud_activation_acquire_connection(component->context.activation, storage_connection,
                                              storage_caps, &component->storage);
    if (status)
        goto cleanup;
    ud_string catalog_url = {0}, storage_root = {0};
    status = read_lease_resources(component, &catalog_url, &storage_root);
    if (status)
        goto cleanup;
    if (!warehouse.length)
        warehouse = storage_root;
    ud_iceberg_options options = {
        .struct_size = sizeof(options),
        .abi_version = UD_ABI_VERSION,
        .memory = component->context.memory,
        .context = &component->context,
        .buffers = component->context.execution->buffers,
        .storage_lease = component->storage,
        .user = component,
        .catalog_url = catalog_url,
        .catalog_warehouse = warehouse,
        .storage_root = storage_root,
        .namespace_name = namespace_name,
        .table_name = table_name,
        .request = request,
        .authorize_file = authorize_file,
        .check = check,
        .before_commit = before_commit};
    status = ud_iceberg_table_open(&options, schema_batch, &component->table);
    if (status)
        goto cleanup;
    return UD_OK;

cleanup:
    ud_free((void *)component->s3_root.data);
    component->s3_root = (ud_string){0};
    if (component->storage) {
        ud_connection_lease_release(component->storage);
        component->storage = NULL;
    }
    if (component->catalog) {
        ud_connection_lease_release(component->catalog);
        component->catalog = NULL;
    }
    return status;
}

static ud_status output_schema(iceberg_component *component, const ud_batch *input) {
    uint32_t count = input->columns - (component->before_column != UINT32_MAX);
    if (!count)
        return UD_SCHEMA;
    ud_field *fields = ud_alloc(component->context.memory, count * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    uint32_t at = 0;
    for (uint32_t i = 0; i < input->columns; i++)
        if (i != component->before_column) {
            fields[at] = input->column[i].field;
            for (uint32_t key = 0; key < component->key_count; key++)
                if (component->keys[key] == i)
                    fields[at].key_order = key + 1;
            at++;
        }
    ud_status status = ud_batch_create(component->context.memory, fields, count, 0,
                                       &component->schema);
    ud_free(fields);
    return status;
}

static ud_status read_lease_resources(iceberg_component *component, ud_string *catalog_url,
                                      ud_string *storage_root) {
    UD_TRY(ud_http_connection_resource(component->catalog, catalog_url));
    ud_status status = ud_connection_lease_path(component->storage, storage_root);
    if (!status)
        return UD_OK;
    UD_TRY(ud_s3_object_root(component->context.memory, component->storage, &component->s3_root));
    *storage_root = component->s3_root;
    return UD_OK;
}

static ud_status open_source_scan(iceberg_component *component, int64_t expected,
                                  int require_expected) {
    ud_batch *actual = NULL;
    ud_status status = ud_iceberg_table_read_open(component->table, &component->read_options,
                                                  &actual);
    ud_batch_destroy(actual);
    if (status)
        return status;
    int64_t selected = 0;
    status = ud_iceberg_table_snapshot_id(component->table, &selected);
    if (!status && require_expected && selected != expected)
        status = UD_INCOMPATIBLE;
    if (status) {
        ud_iceberg_table_read_close(component->table);
        return status;
    }
    component->snapshot_id = selected;
    component->read_open = 1;
    return UD_OK;
}

static ud_status source_policy(iceberg_component *component) {
    char *json = NULL;
    uint32_t length = 0;
    UD_TRY(ud_json_serialize(component->config, &json, &length));
    uint8_t digest[32];
    ud_hash(json, length, digest);
    ud_free(json);
    for (uint32_t i = 0; i < 32; i++)
        snprintf(component->source_policy + i * 2, 3, "%02x", digest[i]);
    component->source_policy[64] = 0;
    return UD_OK;
}

static ud_status load_source_snapshot(iceberg_component *component) {
    char *info = NULL;
    uint32_t length = 0;
    ud_status status = ud_checkpoint_snapshot_info(
        component->context.activation, component->context.component_id,
        (ud_string){component->source_policy, 64}, &info, &length);
    if (status == UD_END)
        return UD_OK;
    if (status)
        return status;
    ud_json *entry = NULL;
    status = ud_json_parse(component->context.memory, info, length, &entry, NULL);
    ud_free(info);
    uint64_t snapshot = 0;
    if (!status)
        status = ud_json_uint(ud_member(entry, "snapshotId"), &snapshot);
    if (!status && snapshot > INT64_MAX)
        status = UD_CORRUPT;
    if (!status && component->read_options.snapshot_id > 0 &&
        component->read_options.snapshot_id != (int64_t)snapshot)
        status = UD_INCOMPATIBLE;
    if (!status) {
        component->snapshot_id = (int64_t)snapshot;
        component->read_options.snapshot_id = component->snapshot_id;
        component->source_metadata_loaded = 1;
    }
    ud_json_destroy(entry);
    return status;
}

static ud_status seal_source_snapshot(iceberg_component *component) {
    char payload[160];
    int length = snprintf(payload, sizeof(payload), "{\"policy\":\"%s\",\"snapshotId\":%lld}",
                          component->source_policy, (long long)component->snapshot_id);
    if (length < 0 || (size_t)length >= sizeof(payload))
        return UD_OVERFLOW;
    return ud_checkpoint_snapshot_seal(
        component->context.activation, component->context.component_id,
        (ud_string){payload, (uint32_t)length});
}

static ud_status prepare(void *user, const ud_component_context *context, ud_string text,
                         const ud_batch *const *inputs, const ud_batch *const *outputs,
                         void **instance) {
    (void)user;
    *instance = NULL;
    iceberg_component *component = ud_alloc(context->memory, sizeof(*component));
    if (!component)
        return UD_RESOURCE;
    memset(component, 0, sizeof(*component));
    component->context = *context;
    component->before_column = UINT32_MAX;
    component->source = (int)(intptr_t)user;
    ud_status status = ud_builtin_parse(context, text, &component->config);
    if (!status && component->source) {
        status = validate_source_config(component->config);
        if (!status)
            status = source_policy(component);
        if (!status)
            status = ud_builtin_clone_schema(context->memory, outputs[0], &component->schema);
        if (!status) {
            uint64_t batch = context->max_batch_rows;
            if (ud_member(component->config, "batchSize"))
                status = ud_json_uint(ud_member(component->config, "batchSize"), &batch);
            if (!status && (!batch || batch > context->max_batch_rows))
                status = UD_SCHEMA;
            component->read_options = (ud_iceberg_read_options){
                .struct_size = sizeof(component->read_options),
                .abi_version = UD_ABI_VERSION,
                .batch_rows = (uint32_t)batch};
            if (!status && ud_member(component->config, "snapshotId")) {
                uint64_t snapshot;
                status = ud_json_uint(ud_member(component->config, "snapshotId"), &snapshot);
                component->read_options.snapshot_id = (int64_t)snapshot;
            }
        }
        if (!status)
            status = load_source_snapshot(component);
        if (!status)
            status = open_table(component, NULL);
        if (!status && !(component->source_metadata_loaded && !component->snapshot_id))
            status = open_source_scan(component, component->read_options.snapshot_id,
                                      component->source_metadata_loaded ||
                                          component->read_options.snapshot_id > 0);
        if (!status && !component->source_metadata_loaded)
            status = seal_source_snapshot(component);
        if (status) {
            ud_iceberg_table_close(component->table);
            ud_connection_lease_release(component->storage);
            ud_connection_lease_release(component->catalog);
            ud_batch_destroy(component->schema);
            ud_json_destroy(component->config);
            ud_free(component);
            return status;
        }
        *instance = component;
        return UD_OK;
    }
    uint32_t key_count = 0;
    if (!status)
        status = validate_config(component->config, inputs[0], &component->mode,
                                 &component->before_column, &key_count);
    if (!status) {
        component->key_count = key_count;
        component->keys = ud_alloc(context->memory, key_count * sizeof(*component->keys));
        if (key_count && !component->keys)
            status = UD_RESOURCE;
    }
    const ud_json *key_values = ud_member(component->config, "keys");
    for (uint32_t i = 0; !status && i < key_count; i++) {
        uint64_t key;
        status = ud_json_uint(ud_json_at(key_values, i), &key);
        if (!status)
            component->keys[i] = (uint32_t)key;
    }
    if (!status)
        status = output_schema(component, inputs[0]);
    if (!status)
        status = open_table(component, component->schema);
    if (status) {
        if (component->table)
            ud_iceberg_table_close(component->table);
        ud_connection_lease_release(component->storage);
        ud_connection_lease_release(component->catalog);
        ud_batch_destroy(component->schema);
        ud_free(component->keys);
        ud_json_destroy(component->config);
        ud_free(component);
        return status;
    }
    *instance = component;
    return UD_OK;
}

static ud_status project_rows(iceberg_component *component, const ud_batch *input,
                              ud_batch **rows, ud_batch **before, int32_t **operations) {
    *rows = NULL;
    *before = NULL;
    *operations = NULL;
    const uint32_t count = input->rows;
    const uint32_t columns = component->schema->columns;
    ud_batch *selected = NULL, *old = NULL;
    int32_t *actions = NULL;
    uint32_t *indices = ud_alloc(component->context.memory, columns * sizeof(*indices));
    if (!indices)
        return UD_RESOURCE;
    uint32_t index = 0;
    for (uint32_t column = 0; column < input->columns; column++)
        if (column != component->before_column)
            indices[index++] = column;
    ud_status status = ud_batch_create(component->context.memory, component->schema->schema->fields,
                                       columns, count, &selected);
    int need_before = component->mode != UD_WRITE_APPEND;
    if (!status && need_before)
        status = ud_batch_create(component->context.memory, component->schema->schema->fields,
                                 columns, count, &old);
    if (!status && need_before)
        actions = ud_alloc(component->context.memory, count * sizeof(*actions));
    if (!status && need_before && !actions)
        status = UD_RESOURCE;
    for (uint32_t row = 0; !status && row < count; row++) {
        ud_row_info info = {0};
        status = ud_batch_row_info(input, row, &info);
        int32_t action = component->mode == UD_WRITE_UPSERT ? UD_CHANGE_UPSERT : info.operation;
        if (!status && component->mode == UD_WRITE_CHANGESET &&
            (action < UD_CHANGE_INSERT || action > UD_CHANGE_DELETE))
            status = UD_SCHEMA;
        if (!status && component->mode == UD_WRITE_UPSERT)
            info.operation = (uint32_t)action;
        if (!status)
            status = ud_batch_append_projected(selected, input, row, indices);
        if (!status)
            status = ud_batch_set_row_info(selected, selected->rows - 1, &info);
        if (!status && need_before) {
            ud_value prior = {0};
            if (component->before_column != UINT32_MAX)
                status = ud_batch_get(input, row, component->before_column, &prior);
            if (!status && (component->before_column == UINT32_MAX || prior.is_null)) {
                if (action != UD_CHANGE_INSERT && action != UD_CHANGE_UPSERT)
                    status = UD_SCHEMA;
                if (!status)
                    status = ud_batch_append_projected(old, input, row, indices);
            } else if (!status) {
                if (prior.type != UD_STRUCT || prior.child_count != columns)
                    status = UD_SCHEMA;
                if (!status)
                    status = ud_batch_append(old, prior.children, columns);
            }
            if (!status)
                status = ud_batch_set_row_info(old, old->rows - 1, &info);
            actions[row] = action;
        }
    }
    if (!status)
        status = ud_batch_seal(selected);
    if (!status && old)
        status = ud_batch_seal(old);
    if (status) {
        ud_batch_destroy(selected);
        ud_batch_destroy(old);
        ud_free(actions);
        ud_free(indices);
        return status;
    }
    ud_free(indices);
    *rows = selected;
    *before = old;
    *operations = actions;
    return UD_OK;
}

/* A source schema is part of the component contract, while the physical
 * Iceberg schema is discovered from the table.  Keep that distinction at the
 * boundary: scan batches are projected into the configured output schema
 * before they are emitted.  Names are the stable user-facing match; an
 * explicit identity remains useful for a renamed column when it is available
 * in a provider-produced schema. */
static ud_status project_source_batch(iceberg_component *component, const ud_batch *input,
                                      ud_batch **out) {
    *out = NULL;
    const uint32_t columns = component->schema->columns;
    if (!columns || input->columns < columns)
        return UD_SCHEMA;
    uint32_t *indices = ud_alloc(component->context.memory, columns * sizeof(*indices));
    unsigned char *used = ud_alloc(component->context.memory, input->columns);
    ud_field *fields = ud_alloc(component->context.memory, columns * sizeof(*fields));
    ud_status status = indices && used && fields ? UD_OK : UD_RESOURCE;
    if (!status)
        memset(used, 0, input->columns);
    for (uint32_t target = 0; !status && target < columns; target++) {
        status = ud_batch_field(component->schema, target, fields + target);
        uint32_t found = UINT32_MAX;
        if (!status) {
            for (uint32_t source = 0; source < input->columns; source++) {
                ud_field actual;
                if (used[source] || ud_batch_field(input, source, &actual) != UD_OK)
                    continue;
                if (field_named(&actual, (ud_string){fields[target].name,
                                                     fields[target].name_length}) &&
                    ud_field_matches(&actual, fields + target, UD_FIELD_VALUE_TYPE)) {
                    found = source;
                    break;
                }
            }
        }
        if (!status && found == UINT32_MAX &&
            (fields[target].identity.high || fields[target].identity.low)) {
            for (uint32_t source = 0; source < input->columns; source++) {
                ud_field actual;
                if (used[source] || ud_batch_field(input, source, &actual) != UD_OK)
                    continue;
                if (!ud_id_compare(actual.identity, fields[target].identity) &&
                    ud_field_matches(&actual, fields + target, UD_FIELD_VALUE_TYPE)) {
                    found = source;
                    break;
                }
            }
        }
        if (!status && found == UINT32_MAX)
            status = UD_INCOMPATIBLE;
        if (!status) {
            indices[target] = found;
            used[found] = 1;
        }
    }
    if (!status)
        status = ud_batch_project(component->context.memory, input, indices, fields, columns, out);
    ud_free(fields);
    ud_free(used);
    ud_free(indices);
    return status;
}

static ud_status digest_rows(iceberg_component *component, const ud_batch *rows,
                             const ud_batch *before, const int32_t *actions, uint8_t digest[32]) {
    ud_sha256 hash;
    ud_sha256_init(&hash);
    UD_TRY(ud_batch_hash_tagged_rows(rows, &hash, 0));
    if (before)
        UD_TRY(ud_batch_hash_tagged_rows(before, &hash, 1));
    ud_sha256_update(&hash, &component->mode, sizeof(component->mode));
    if (actions)
        ud_sha256_update(&hash, actions, rows->rows * sizeof(*actions));
    if (component->key_count)
        ud_sha256_update(&hash, component->keys,
                         component->key_count * sizeof(*component->keys));
    ud_sha256_finish(&hash, digest);
    return UD_OK;
}

static ud_status effect_identity(iceberg_component *component, char identity[65]) {
    ud_bytes bytes = {.ctx = component->context.memory};
    ud_activation *activation = component->context.activation;
    ud_status status = ud_put_string(&bytes, activation->execution->options.execution_id.data,
                                      activation->execution->options.execution_id.length);
    if (!status)
        status = ud_put_string(&bytes, component->context.task_id.data,
                               component->context.task_id.length);
    if (!status)
        status = ud_put_string(&bytes, component->context.pipeline_id.data,
                               component->context.pipeline_id.length);
    if (!status)
        status = ud_put_string(&bytes, component->context.component_id.data,
                               component->context.component_id.length);
    if (!status)
        status = ud_put_u64(&bytes, component->number);
    uint8_t digest[32];
    if (!status)
        ud_hash(bytes.data, bytes.size, digest);
    for (uint32_t i = 0; !status && i < 32; i++)
        snprintf(identity + i * 2, 3, "%02x", digest[i]);
    identity[64] = 0;
    ud_bytes_clear(&bytes);
    return status;
}

static ud_status write_rows(iceberg_component *component, const ud_batch *rows,
                            const ud_batch *before, const int32_t *actions) {
    uint8_t digest[32];
    UD_TRY(digest_rows(component, rows, before, actions, digest));
    char identity_text[65], digest_text[65];
    UD_TRY(effect_identity(component, identity_text));
    for (uint32_t i = 0; i < 32; i++)
        snprintf(digest_text + i * 2, 3, "%02x", digest[i]);
    digest_text[64] = 0;
    ud_string identity = {identity_text, 64}, encoded_digest = {digest_text, 64};
    ud_command_result prior = {.struct_size = sizeof(prior), .abi_version = UD_ABI_VERSION};
    ud_status status = ud_checkpoint_effect_read(component->context.activation, identity, digest,
                                                 &prior);
    int completed = !status && (prior.state == UD_COMMAND_APPLIED ||
                                prior.state == UD_COMMAND_COMMITTED);
    int replay = !status && prior.state == UD_COMMAND_PENDING;
    if (!status && prior.state == UD_COMMAND_REJECTED)
        status = prior.result ? prior.result : UD_CORRUPT;
    ud_command_result_clear(&prior);
    int submitted = 0;
    if (!status && replay) {
        char *record = NULL;
        uint32_t record_length = 0;
        status = ud_checkpoint_snapshot_info(component->context.activation, identity,
                                             (ud_string){0}, &record, &record_length);
        if (status == UD_END)
            status = UD_OK;
        else if (!status) {
            ud_json *submission = NULL;
            status = ud_json_parse(component->context.memory, record, record_length, &submission, NULL);
            ud_string value = {0};
            if (!status)
                status = ud_json_text(ud_member(submission, "submitted"), &value.data, &value.length);
            if (!status)
                submitted = named(value, "true");
            ud_json_destroy(submission);
        }
        ud_free(record);
    }
    if (!status && !completed && !replay)
        status = ud_checkpoint_effect_intent(component->context.activation, identity, digest);
    if (!status && !completed) {
        component->publishing = identity;
        component->submission_recorded = submitted;
        status = component->mode == UD_WRITE_APPEND
                     ? ud_iceberg_table_append(component->table, rows, identity, encoded_digest,
                                                replay, submitted)
                     : ud_iceberg_table_apply(component->table, rows, before, actions,
                                              rows->rows, identity, encoded_digest, replay, submitted);
        component->publishing = (ud_string){0};
        if (status == UD_FENCED && component->submission_recorded) {
            const ud_string state = {"{\"submitted\":false}", sizeof("{\"submitted\":false}") - 1};
            if (!ud_checkpoint_snapshot_seal(component->context.activation, identity, state))
                ud_fail(component->context.memory, UD_COMMIT_UNKNOWN,
                        "Iceberg Catalog rejected the conflicting snapshot. Resume to rebind the table.");
            return UD_COMMIT_UNKNOWN;
        }
        if (status && (component->submission_recorded || status == UD_COMMIT_UNKNOWN))
            return UD_COMMIT_UNKNOWN;
        if (component->s3_root.length && (status == UD_IO || status == UD_TRANSIENT || status == UD_TIMEOUT))
            return UD_COMMIT_UNKNOWN;
        ud_command_result outcome = {.struct_size = sizeof(outcome),
                                     .abi_version = UD_ABI_VERSION,
                                     .state = status ? UD_COMMAND_REJECTED : UD_COMMAND_COMMITTED,
                                     .result = status,
                                     .affected_rows = rows->rows};
        ud_status recorded = ud_checkpoint_effect_complete(component->context.activation, identity,
                                                            digest, &outcome);
        if (!status)
            status = recorded ? UD_COMMIT_UNKNOWN : UD_OK;
        else if (recorded)
            status = recorded;
    }
    if (!status) {
        ud_target_rows(component->context.activation, rows->rows, 0, 0);
        if (component->context.execution->journal)
            ud_target_rows(component->context.activation, rows->rows, 0, 1);
    }
    return status;
}

static ud_status step(void *instance, const ud_component_event *event) {
    iceberg_component *component = instance;
    if (!component || !event)
        return UD_INVALID;
    UD_TRY(ud_activation_check(component->context.activation));
    if (component->source) {
        if (event->kind == UD_COMPONENT_INPUT_END)
            return UD_OK;
        if (event->kind != UD_COMPONENT_POLL)
            return UD_OK;
        if (!component->read_open)
            return UD_END;
        UD_TRY(ud_connection_lease_authorize(component->catalog, UD_ACCESS_NETWORK));
        UD_TRY(ud_connection_lease_authorize(component->storage, UD_ACCESS_READ));
        while (component->skip_batches) {
            ud_batch *discarded = NULL;
            ud_status status = ud_iceberg_table_read_next(component->table, &discarded);
            ud_batch_destroy(discarded);
            if (status == UD_END)
                return UD_CORRUPT;
            if (status)
                return status;
            component->skip_batches--;
        }
        ud_batch *batch = NULL;
        ud_status status = ud_iceberg_table_read_next(component->table, &batch);
        if (status == UD_END) {
            ud_batch_destroy(batch);
            return UD_END;
        }
        if (!status && !batch)
            status = UD_CORRUPT;
        ud_batch *projected = NULL;
        if (!status)
            status = project_source_batch(component, batch, &projected);
        ud_batch_destroy(batch);
        batch = projected;
        if (!status)
            status = component->context.emit(component->context.emit_user, 0, batch);
        ud_batch_destroy(batch);
        if (!status)
            component->number++;
        return status ? status : UD_MORE;
    }
    if (event->kind == UD_COMPONENT_POLL)
        return UD_END;
    if (event->kind == UD_COMPONENT_INPUT_END)
        return UD_OK;
    if (event->kind != UD_COMPONENT_DATA)
        return UD_OK;
    if (!event->batch || !event->batch->rows)
        return UD_OK;
    UD_TRY(ud_connection_lease_authorize(component->catalog, UD_ACCESS_NETWORK));
    UD_TRY(ud_connection_lease_authorize(component->storage, UD_ACCESS_WRITE));
    ud_batch *rows = NULL, *before = NULL;
    int32_t *actions = NULL;
    ud_status status = project_rows(component, event->batch, &rows, &before, &actions);
    if (!status)
        status = write_rows(component, rows, before, actions);
    if (status == UD_COMMIT_UNKNOWN)
        component->context.activation->pipeline_result = UD_COMMIT_UNKNOWN;
    ud_batch_destroy(rows);
    ud_batch_destroy(before);
    ud_free(actions);
    if (!status)
        component->number++;
    return status;
}

static ud_status finish(void *instance) {
    iceberg_component *component = instance;
    if (!component || !component->table)
        return UD_INVALID;
    if (component->read_open) {
        ud_status status = ud_iceberg_table_read_close(component->table);
        component->read_open = 0;
        return status;
    }
    return UD_OK;
}

static void dispose(void *instance, ud_status outcome) {
    (void)outcome;
    iceberg_component *component = instance;
    if (!component)
        return;
    if (component->read_open)
        ud_iceberg_table_read_close(component->table);
    ud_iceberg_table_close(component->table);
    ud_free((void *)component->s3_root.data);
    ud_connection_lease_release(component->storage);
    ud_connection_lease_release(component->catalog);
    ud_batch_destroy(component->schema);
    ud_json_destroy(component->config);
    ud_free(component->keys);
    ud_free(component);
}

static ud_status checkpoint(void *instance, ud_batch **out) {
    iceberg_component *component = instance;
    if (!component || !out)
        return UD_INVALID;
    if (component->source && component->number && component->snapshot_id <= 0)
        return UD_CORRUPT;
    ud_field fields[] = {
        {.name = "nextBatch", .name_length = 9, .type = UD_UINT64},
        {.name = "snapshotId", .name_length = 10, .type = UD_INT64}};
    ud_value values[] = {{.type = UD_UINT64, .unsigned_integer = component->number},
                         {.type = UD_INT64, .integer = component->snapshot_id}};
    uint32_t columns = component->source ? 2 : 1;
    UD_TRY(ud_batch_create(component->context.memory, fields, columns, 1, out));
    ud_status status = ud_batch_append(*out, values, columns);
    if (!status)
        status = ud_batch_seal(*out);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}

static ud_status restore(void *instance, const ud_batch *state) {
    iceberg_component *component = instance;
    uint32_t expected_columns = component && component->source ? 2 : 1;
    if (!component || !state || state->columns != expected_columns || state->rows != 1)
        return UD_CORRUPT;
    ud_value value, snapshot;
    UD_TRY(ud_batch_get(state, 0, 0, &value));
    if (value.type != UD_UINT64 || value.is_null || value.is_absent)
        return UD_CORRUPT;
    component->number = value.unsigned_integer;
    if (!component->source)
        return UD_OK;
    UD_TRY(ud_batch_get(state, 0, 1, &snapshot));
    if (snapshot.type != UD_INT64 || snapshot.is_null || snapshot.is_absent ||
        snapshot.integer < 0 || (component->number && !snapshot.integer))
        return UD_CORRUPT;
    component->snapshot_id = snapshot.integer;
    component->skip_batches = component->number;
    if (!component->snapshot_id) {
        if (component->read_open) {
            UD_TRY(ud_iceberg_table_read_close(component->table));
            component->read_open = 0;
        }
        return UD_OK;
    }
    UD_TRY(ud_connection_lease_authorize(component->catalog, UD_ACCESS_NETWORK));
    UD_TRY(ud_connection_lease_authorize(component->storage, UD_ACCESS_READ));
    if (component->read_open) {
        UD_TRY(ud_iceberg_table_read_close(component->table));
        component->read_open = 0;
    }
    component->read_options.snapshot_id = component->snapshot_id;
    UD_TRY(open_source_scan(component, component->snapshot_id, 1));
    return UD_OK;
}

ud_status ud_iceberg_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":["
        "\"connection\",\"storageConnection\",\"namespace\",\"table\"],"
        "\"properties\":{\"connection\":{\"type\":\"string\",\"minLength\":1,"
        "\"x-role\":\"resource-reference\"},\"storageConnection\":{\"type\":\"string\","
        "\"minLength\":1,\"x-role\":\"resource-reference\"},\"warehouse\":{\"type\":"
        "\"string\",\"minLength\":1},\"namespace\":{\"type\":\"string\",\"minLength\":1},"
        "\"table\":{\"type\":\"string\",\"minLength\":1},\"mode\":{\"enum\":["
        "\"append\",\"upsert\",\"changeset\"],\"default\":\"append\"},\"keys\":{"
        "\"type\":\"array\",\"minItems\":1,\"maxItems\":16,\"items\":{\"type\":"
        "\"integer\",\"minimum\":0},\"uniqueItems\":true},\"beforeField\":{\"type\":"
        "\"string\",\"minLength\":1}}}";
    static const char source_schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":["
        "\"connection\",\"storageConnection\",\"namespace\",\"table\",\"schema\"],"
        "\"properties\":{\"connection\":{\"type\":\"string\",\"minLength\":1,"
        "\"x-role\":\"resource-reference\"},\"storageConnection\":{\"type\":\"string\","
        "\"minLength\":1,\"x-role\":\"resource-reference\"},\"warehouse\":{\"type\":"
        "\"string\",\"minLength\":1},\"namespace\":{\"type\":\"string\",\"minLength\":1},"
        "\"table\":{\"type\":\"string\",\"minLength\":1},\"schema\":{\"type\":"
        "\"object\"},\"snapshotId\":{\"type\":\"integer\",\"minimum\":0},\"batchSize\":{"
        "\"type\":\"integer\",\"minimum\":1,\"maximum\":65536}}}";
    const ud_component_descriptor destination = {
        .struct_size = sizeof(destination),
        .abi_version = UD_ABI_VERSION,
        .id = {"iceberg.destination", 19},
        .name = {"Iceberg Destination", 19},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
        .configuration_schema = {schema, sizeof(schema) - 1},
        .category = UD_COMPONENT_DESTINATION,
        .inputs = 1,
        .outputs = 0,
        .synchronous_input = -1,
        .error_output = -1,
        .capabilities = UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING,
        .effects = UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_COMMIT_UNKNOWN | UD_EFFECT_IDEMPOTENT,
        .configure = configure,
        .prepare = prepare,
        .step = step,
        .finish = finish,
        .dispose = dispose,
        .state_version = 1,
        .checkpoint = checkpoint,
        .restore = restore};
    UD_TRY(ud_registry_register(registry, &destination));
    const ud_component_descriptor source = {
        .struct_size = sizeof(source),
        .abi_version = UD_ABI_VERSION,
        .id = {"iceberg.source", 14},
        .name = {"Iceberg Source", 14},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
        .configuration_schema = {source_schema, sizeof(source_schema) - 1},
        .category = UD_COMPONENT_SOURCE,
        .inputs = 0,
        .outputs = 1,
        .synchronous_input = -1,
        .error_output = -1,
        .capabilities = UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING,
        .effects = UD_EFFECT_EXTERNAL_READ,
        .user = (void *)(intptr_t)1,
        .configure = configure,
        .prepare = prepare,
        .step = step,
        .finish = finish,
        .dispose = dispose,
        .state_version = 2,
        .checkpoint = checkpoint,
        .restore = restore};
    return ud_registry_register(registry, &source);
}
