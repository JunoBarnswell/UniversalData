#include "../package/internal.h"
#include "ud/connection_manager.h"
#include <errno.h>
static ud_status config_read(const ud_component_context *c, ud_string text, ud_json **out, int *op,
                             ud_string *source, ud_string *target) {
    *out = NULL;
    *source = (ud_string){0};
    *target = (ud_string){0};
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, out, NULL));
    static const char *keys[] = {"operation", "source", "target"};
    ud_status s = ud_json_keys(*out, keys, 3);
    ud_string operation;
    if (!s)
        s = ud_json_string(ud_member(*out, "operation"), &operation);
    if (!s) {
        *op = ud_named(operation, "copy")     ? 1
              : ud_named(operation, "move")   ? 2
              : ud_named(operation, "delete") ? 3
              : ud_named(operation, "mkdir")  ? 4
                                              : 0;
        if (!*op)
            s = UD_UNSUPPORTED;
    }
    if (!s && *op != 4)
        s = ud_json_string(ud_member(*out, "source"), source);
    if (!s && *op != 3)
        s = ud_json_string(ud_member(*out, "target"), target);
    if (!s && ((*op == 3 && ud_member(*out, "target")) || (*op == 4 && ud_member(*out, "source"))))
        s = UD_SCHEMA;
    if (!s && source->length && target->length && ud_same(*source, *target))
        s = UD_SCHEMA;
    ud_string ids[] = {*source, *target};
    for (int i = 0; !s && i < 2; i++)
        if (ids[i].length) {
            const ud_json *def = NULL;
            int owner;
            s = ud_connection_definition(c->execution, ids[i], &def, &owner);
            const ud_json *driver = ud_member(def, "driver");
            ud_string value;
            if (!s && ud_json_kind(driver) == UD_JSON_STRING) {
                s = ud_json_string(driver, &value);
                if (!s && !ud_named(value, "file"))
                    s = UD_UNSUPPORTED;
            }
        }
    return s;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    (void)user;
    (void)inputs;
    (void)outputs;
    ud_json *config = NULL;
    int op;
    ud_string source, target;
    ud_status s = config_read(c, text, &config, &op, &source, &target);
    ud_json_destroy(config);
    return s;
}
static ud_status path_copy(ud_context *ctx, ud_connection_lease *lease, char **out) {
    ud_string path;
    UD_TRY(ud_connection_lease_path(lease, &path));
    size_t start = 0;
    for (size_t i = 0; i <= path.length; i++)
        if (i == path.length || path.data[i] == '/'
#ifdef _WIN32
            || path.data[i] == '\\'
#endif
        ) {
            size_t n = i - start;
            if ((n == 1 && path.data[start] == '.') ||
                (n == 2 && path.data[start] == '.' && path.data[start + 1] == '.'))
                return UD_INVALID;
            start = i + 1;
        }
    *out = ud_strdup_n(ctx, path.data, path.length);
    return *out ? UD_OK : UD_RESOURCE;
}
static ud_status copy_file(const ud_component_context *c, ud_connection_lease *source,
                           ud_connection_lease *target, const char *from, const char *to) {
    size_t n = strlen(to) + 20;
    char *temporary = ud_alloc(c->memory, n);
    if (!temporary)
        return UD_RESOURCE;
    snprintf(temporary, n, "%s.ud-task.part", to);
    ud_execution *e = c->execution;
    ud_status s = e->options.authorize &&
                          e->options.authorize(e->options.authorize_user, e->options.tenant,
                                               (ud_string){temporary, (uint32_t)strlen(temporary)},
                                               UD_ACCESS_WRITE) == UD_OK
                      ? UD_OK
                      : UD_AUTH;
    if (!s)
        s = ud_execution_check(e);
    FILE *input = NULL, *output = NULL;
    int owned = 0;
    uint8_t *buffer = NULL;
    if (!s)
        s = ud_open_regular(c->memory, from, &input);
    if (!s) {
        output = ud_fopen(temporary, "wbx");
        if (!output)
            s = errno == EEXIST ? UD_BUSY : UD_IO;
        else
            owned = 1;
    }
    if (!s) {
        buffer = ud_alloc(c->memory, 65536);
        if (!buffer)
            s = UD_RESOURCE;
    }
    while (!s) {
        s = ud_connection_lease_authorize(source, UD_ACCESS_READ);
        if (!s)
            s = ud_connection_lease_authorize(target, UD_ACCESS_WRITE);
        if (!s)
            s = ud_execution_check(e);
        if (s)
            break;
        size_t count = fread(buffer, 1, 65536, input);
        if (count && fwrite(buffer, 1, count, output) != count)
            s = UD_IO;
        if (count < 65536) {
            if (ferror(input))
                s = UD_IO;
            break;
        }
    }
    if (!s)
        s = ud_flush(c->memory, output);
    if (input && fclose(input) && !s)
        s = UD_IO;
    if (output && fclose(output) && !s)
        s = UD_IO;
    if (!s)
        s = ud_execution_check(e);
    if (!s)
        s = ud_connection_lease_authorize(target, UD_ACCESS_WRITE);
    if (!s) {
        s = ud_publish(c->memory, temporary, to);
        if (s) {
            int kind = 0;
            if (!ud_path_kind(c->memory, to, &kind) && kind)
                s = UD_COMMIT_UNKNOWN;
        }
    }
    if (owned)
        (void)ud_remove_file(c->memory, temporary);
    ud_free(buffer);
    ud_free(temporary);
    return s;
}
static ud_status invoke(void *user, const ud_component_context *c, ud_string text) {
    (void)user;
    ud_json *config = NULL;
    int op = 0;
    ud_string source_id, target_id;
    ud_status s = config_read(c, text, &config, &op, &source_id, &target_id);
    ud_connection_lease *source = NULL, *target = NULL;
    char *from = NULL, *to = NULL;
    if (!s && source_id.length)
        s = ud_acquire_file_connection(c->execution, source_id, &source);
    if (!s && target_id.length)
        s = ud_acquire_file_connection(c->execution, target_id, &target);
    if (!s && source)
        s = ud_connection_lease_authorize(source, op == 3 ? UD_ACCESS_DELETE : UD_ACCESS_READ);
    if (!s && op == 2)
        s = ud_connection_lease_authorize(source, UD_ACCESS_DELETE);
    if (!s && target)
        s = ud_connection_lease_authorize(target, UD_ACCESS_WRITE);
    if (!s && source)
        s = path_copy(c->memory, source, &from);
    if (!s && target)
        s = path_copy(c->memory, target, &to);
    if (!s && from)
        s = ud_path_no_symlinks(c->memory, from);
    if (!s && to)
        s = ud_path_no_symlinks(c->memory, to);
    int source_kind = 0, target_kind = 0;
    if (!s && from)
        s = ud_path_kind(c->memory, from, &source_kind);
    if (!s && to)
        s = ud_path_kind(c->memory, to, &target_kind);
    if (!s && from && source_kind != 1)
        s = source_kind == 0 && op == 3 ? UD_OK : UD_IO;
    if (!s && to && target_kind && op != 4)
        s = UD_BUSY;
    if (!s && op == 4 && target_kind && target_kind != 2)
        s = UD_SCHEMA;
    if (!s)
        s = ud_execution_check(c->execution);
    if (!s && op == 1)
        s = copy_file(c, source, target, from, to);
    else if (!s && op == 2)
        s = ud_move_noreplace(c->memory, from, to);
    else if (!s && op == 3 && source_kind) {
        s = ud_remove_file(c->memory, from);
        if (!s && ud_sync_parent(c->memory, from))
            s = UD_COMMIT_UNKNOWN;
    } else if (!s && op == 4 && !target_kind) {
        s = ud_mkdir(c->memory, to);
        if (!s && ud_sync_parent(c->memory, to))
            s = UD_COMMIT_UNKNOWN;
    }
    ud_status clean = ud_connection_lease_release(target);
    if (!s)
        s = clean;
    clean = ud_connection_lease_release(source);
    if (!s)
        s = clean;
    ud_free(from);
    ud_free(to);
    ud_json_destroy(config);
    return s;
}
ud_status ud_filesystem_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"operation\"],"
        "\"properties\":{\"operation\":{\"enum\":[\"copy\",\"move\",\"delete\",\"mkdir\"]},"
        "\"source\":{\"type\":\"string\"},\"target\":{\"type\":\"string\"}}}";
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = {"file.system", 11},
                                 .name = {"File System", 11},
                                 .version = {"1.0.0", 5},
                                 .configuration_schema = {schema, sizeof(schema) - 1},
                                 .category = UD_COMPONENT_TASK,
                                 .synchronous_input = -1,
                                 .error_output = -1,
                                 .configure = configure,
                                 .invoke = invoke};
    return ud_registry_register(registry, &d);
}
