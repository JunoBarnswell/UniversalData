#include "../package/internal.h"
#include "ud/connection_manager.h"
#include "ud/compute.h"
#include "ud/operator.h"
#include "ud/format.h"
#include "ud/csv.h"
#include <errno.h>
#include <sys/stat.h>
enum {
    CSV_SOURCE = 1,
    JSON_SOURCE,
    SQL_SOURCE,
    CSV_SINK,
    JSON_SINK,
    SQL_SINK,
    PASS,
    CAST,
    DERIVE,
    SPLIT,
    MULTICAST,
    UNION_ALL,
    SORT,
    JOIN,
    AGGREGATE,
    LOOKUP,
    WINDOW
};
typedef struct builtin {
    int kind;
    ud_component_context context;
    ud_json *config;
    ud_batch *schema, *inputs[2];
    ud_operator *operator;
    ud_field *fields;
    uint32_t columns;
    ud_expression **expressions;
    uint32_t expression_count;
    ud_connection_lease *lease;
    ud_csv_reader *csv;
    ud_csv_writer *csv_writer;
    ud_json_reader *reader;
    ud_json_writer *writer;
    char *path, *temporary, *job;
    void *lock;
    int published, existing, temporary_owned;
    uint64_t number;
    uint32_t *keys, key_count;
    int mode;
} builtin;
static ud_status clone_schema(ud_context *ctx, const ud_batch *in, ud_batch **out) {
    uint32_t count = ud_batch_columns(in);
    ud_field *fields = ud_alloc(ctx, count * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < count; i++)
        s = ud_batch_field(in, i, fields + i);
    if (!s)
        s = ud_batch_create(ctx, fields, count, 0, out);
    ud_free(fields);
    return s;
}
static ud_status schema_config(const ud_component_context *c, const ud_json *config,
                               ud_batch **out) {
    const ud_json *s = ud_member(config, "schema");
    char *json = NULL;
    uint32_t n = 0;
    UD_TRY(ud_json_serialize(s, &json, &n));
    char scope[33];
    ud_status status = ud_pipeline_field_namespace(c, scope);
    if (!status)
        status = ud_schema_from_json(c->memory, json, n, (ud_string){scope, 32}, out);
    ud_free(json);
    return status;
}
static ud_status parse(const ud_component_context *c, ud_string config, ud_json **out) {
    return ud_json_parse(c->memory, config.data, config.length, out, NULL);
}
static ud_status bind_expr(const ud_component_context *c, const ud_json *value,
                           const ud_batch *schema, ud_expression **out) {
    char *json;
    uint32_t n = 0;
    UD_TRY(ud_json_serialize(value, &json, &n));
    ud_expression_options o = {sizeof(o),
                               UD_ABI_VERSION,
                               schema,
                               c->resolve,
                               c->resolve_user,
                               c->resolve_field,
                               c->execution->registry,
                               c->activation->parallel ? UD_COMPONENT_THREAD_SAFE : 0};
    ud_status s = ud_expression_create(c->memory, (ud_string){json, n}, &o, out);
    ud_free(json);
    return s;
}
static ud_status connection_defined(const ud_component_context *ctx, ud_string id) {
    const ud_json *definition = NULL;
    int project_scope = 0;
    return ud_connection_definition(ctx->activation, id, &definition, &project_scope);
}
static ud_status check_config(int kind, const ud_json *config) {
    static const char *source[] = {"connection", "schema", "batchSize", "sql", "nullMarker"};
    static const char *sink[] = {"connection", "table", "mode", "keys"};
    static const char *cast[] = {"schema", "rounding"}, *derive[] = {"columns"},
                      *split[] = {"condition"}, *sort[] = {"key", "descending"},
                      *join[] = {"leftKey", "rightKey"}, *aggregate[] = {"plan"};
    if (kind == CSV_SOURCE) {
        static const char *keys[] = {"connection", "schema", "batchSize", "nullMarker"};
        return ud_json_keys(config, keys, 4);
    }
    if (kind == JSON_SOURCE)
        return ud_json_keys(config, source, 3);
    if (kind == SQL_SOURCE)
        return ud_json_keys(config, source, 4);
    if (kind == CSV_SINK || kind == JSON_SINK)
        return ud_json_keys(config, sink, 1);
    if (kind == SQL_SINK)
        return ud_json_keys(config, sink, 4);
    if (kind == CAST)
        return ud_json_keys(config, cast, 2);
    if (kind == DERIVE)
        return ud_json_keys(config, derive, 1);
    if (kind == SPLIT)
        return ud_json_keys(config, split, 1);
    if (kind == SORT)
        return ud_json_keys(config, sort, 2);
    if (kind == JOIN || kind == LOOKUP)
        return ud_json_keys(config, join, 2);
    if (kind == AGGREGATE)
        return ud_json_keys(config, aggregate, 1);
    if (kind == WINDOW) {
        static const char *keys[] = {"keys", "partitionKeys", "function", "column", "name"};
        return ud_json_keys(config, keys, 5);
    }
    return ud_json_size(config) == 0 ? UD_OK : UD_SCHEMA;
}
static ud_status operator_check(void *user) {
    return ud_activation_check(user);
}
static ud_status create_operator(const ud_component_context *c, int kind, const ud_json *config,
                                 const ud_batch *const *inputs, ud_operator **out) {
    uint64_t left = 0, right = 0;
    ud_operator_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .max_batch_rows = c->max_batch_rows,
                                   .buffers = c->execution->buffers,
                                   .check = operator_check,
                                   .user = c->activation};
    ud_status s = UD_OK;
    uint32_t key = 0;
    ud_plan *plan = NULL;
    uint32_t *window_keys = NULL;
    if (kind == WINDOW) {
        const ud_json *keys = ud_member(config, "keys");
        options.key_count = ud_json_size(keys);
        if (ud_json_kind(keys) != UD_JSON_ARRAY || options.key_count > inputs[0]->columns)
            s = UD_SCHEMA;
        window_keys =
            !s ? ud_alloc(c->memory, (options.key_count + 1) * sizeof(*window_keys)) : NULL;
        if (!s && !window_keys)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < options.key_count; i++) {
            uint64_t index = 0;
            s = ud_json_uint(ud_json_at(keys, i), &index);
            if (!s && index >= inputs[0]->columns)
                s = UD_SCHEMA;
            if (!s)
                window_keys[i] = (uint32_t)index;
        }
        uint64_t partition = 0, column = 0;
        ud_string function = {0};
        if (!s)
            s = ud_json_uint(ud_member(config, "partitionKeys"), &partition);
        if (!s && partition > options.key_count)
            s = UD_SCHEMA;
        if (!s)
            s = ud_json_string(ud_member(config, "function"), &function);
        if (!s)
            s = ud_json_string(ud_member(config, "name"), &options.window_name);
        options.window_function = ud_named(function, "rowNumber")    ? UD_WINDOW_ROW_NUMBER
                                  : ud_named(function, "runningSum") ? UD_WINDOW_RUNNING_SUM
                                                                     : 0;
        if (!s && !options.window_function)
            s = UD_UNSUPPORTED;
        if (!s && options.window_function == UD_WINDOW_RUNNING_SUM)
            s = ud_json_uint(ud_member(config, "column"), &column);
        if (!s && column >= inputs[0]->columns)
            s = UD_SCHEMA;
        options.kind = UD_OPERATOR_WINDOW;
        options.keys = window_keys;
        options.partition_key_count = (uint32_t)partition;
        options.window_column = (uint32_t)column;
    } else if (kind == AGGREGATE) {
        char *json = NULL;
        uint32_t length = 0;
        s = ud_json_serialize(ud_member(config, "plan"), &json, &length);
        ud_field *fields = ud_alloc(c->memory, inputs[0]->columns * sizeof(*fields));
        if (!fields)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < inputs[0]->columns; i++)
            fields[i] = inputs[0]->column[i].field;
        ud_expression_options bindings = {sizeof(bindings),
                                          UD_ABI_VERSION,
                                          inputs[0],
                                          c->resolve,
                                          c->resolve_user,
                                          c->resolve_field,
                                          c->execution->registry,
                                          c->activation->parallel ? UD_COMPONENT_THREAD_SAFE : 0};
        if (!s)
            s = ud_plan_create_fields(c->memory, UD_PLAN_VERSION, fields, inputs[0]->columns,
                                      (ud_string){json, length}, &bindings, &plan);
        ud_free(fields);
        ud_free(json);
        options.kind = UD_OPERATOR_AGGREGATE;
        options.aggregation = plan;
    } else {
        s = ud_json_uint(ud_member(config, kind == SORT ? "key" : "leftKey"), &left);
        if (!s && left >= inputs[0]->columns)
            s = UD_SCHEMA;
        if (!s && kind != SORT)
            s = ud_json_uint(ud_member(config, "rightKey"), &right);
        if (!s && kind != SORT && right >= inputs[1]->columns)
            s = UD_SCHEMA;
        if (!s && kind == SORT && ud_member(config, "descending")) {
            const ud_json *v = ud_member(config, "descending");
            ud_string text = {0};
            if (ud_json_kind(v) != UD_JSON_BOOL)
                s = UD_SCHEMA;
            else {
                s = ud_json_text(v, &text.data, &text.length);
                options.descending = ud_named(text, "true");
            }
        }
        key = (uint32_t)left;
        options.keys = &key;
        options.key_count = 1;
        options.right_key = (uint32_t)right;
        options.kind = kind == SORT     ? UD_OPERATOR_SORT
                       : kind == LOOKUP ? UD_OPERATOR_LOOKUP
                                        : UD_OPERATOR_INNER_JOIN;
    }
    if (!s)
        s = ud_operator_create(c->memory, &options, inputs, out);
    ud_free(window_keys);
    ud_plan_destroy(plan);
    return s;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    ud_status s = parse(c, text, &config);
    if (!s)
        s = check_config(kind, config);
    if (!s && (kind <= SQL_SOURCE))
        s = schema_config(c, config, out);
    else if (!s &&
             (kind == CSV_SINK || kind == JSON_SINK || kind == SQL_SINK)) { /* No output schemas. */
    } else if (!s && kind == CAST) {
        char *schema = NULL;
        uint32_t length = 0;
        uint64_t rounding = 0;
        s = ud_json_serialize(ud_member(config, "schema"), &schema, &length);
        if (!s && ud_member(config, "rounding"))
            s = ud_json_uint(ud_member(config, "rounding"), &rounding);
        if (!s && rounding > UD_ROUND_CEILING)
            s = UD_SCHEMA;
        if (!s)
            s = ud_batch_cast_schema(c->memory, in[0], schema, length, (int32_t)rounding, out);
        ud_free(schema);
    } else if (!s && kind == DERIVE) {
        const ud_json *columns = ud_member(config, "columns");
        uint32_t count = ud_json_size(columns), original = ud_batch_columns(in[0]);
        if (ud_json_kind(columns) != UD_JSON_ARRAY || !count || count > 65536 - original)
            s = UD_SCHEMA;
        ud_field *fields = !s ? ud_alloc(c->memory, (original + count) * sizeof(*fields)) : NULL;
        ud_expression **bound = !s ? ud_alloc(c->memory, count * sizeof(*bound)) : NULL;
        ud_field *derived = !s ? ud_alloc(c->memory, count * sizeof(*derived)) : NULL;
        if (!s && (!fields || !bound || !derived))
            s = UD_RESOURCE;
        char scope[33];
        if (!s)
            s = ud_pipeline_field_namespace(c, scope);
        for (uint32_t i = 0; !s && i < original; i++)
            s = ud_batch_field(in[0], i, fields + i);
        for (uint32_t i = 0; !s && i < count; i++) {
            const ud_json *col = ud_json_at(columns, i);
            static const char *keys[] = {"name", "expression"};
            s = ud_json_keys(col, keys, 2);
            ud_string name = {0};
            if (!s)
                s = ud_json_string(ud_member(col, "name"), &name);
            if (!s)
                s = bind_expr(c, ud_member(col, "expression"), in[0], bound + i);
            if (!s)
                s = ud_expression_field(bound[i], fields + original + i);
            if (!s) {
                s = ud_field_derive(c->memory, fields + original + i, (ud_string){scope, 32}, name,
                                    derived + i);
                if (!s)
                    fields[original + i] = derived[i];
            }
        }
        if (!s)
            s = ud_batch_create(c->memory, fields, original + count, 0, out);
        for (uint32_t i = 0; bound && i < count; i++)
            ud_expression_destroy(bound[i]);
        ud_free(bound);
        if (derived)
            for (uint32_t i = 0; i < count; i++)
                ud_field_clear(derived + i);
        ud_free(derived);
        ud_free(fields);
    } else if (!s && (kind == SORT || kind == JOIN || kind == LOOKUP || kind == WINDOW ||
                      kind == AGGREGATE)) {
        ud_operator *operator = NULL;
        s = create_operator(c, kind, config, in, &operator);
        if (!s)
            s = ud_operator_schema(operator, out);
        ud_status cleanup = ud_operator_destroy(operator);
        if (!s)
            s = cleanup;
    } else if (!s && kind == UNION_ALL)
        s = ud_compute_union_schema(c->memory, in[0], in[1], out);
    else if (!s)
        s = clone_schema(c->memory, in[0], out);
    if (!s && (kind == SPLIT || kind == MULTICAST))
        s = clone_schema(c->memory, in[0], out + 1);
    if (!s && kind == SPLIT) {
        ud_expression *expr = NULL;
        s = bind_expr(c, ud_member(config, "condition"), in[0], &expr);
        if (!s && ud_expression_type(expr) != UD_BOOL)
            s = UD_SCHEMA;
        ud_expression_destroy(expr);
    }
    if (!s && kind <= SQL_SOURCE) {
        ud_string name = {0};
        s = ud_json_string(ud_member(config, "connection"), &name);
        if (!s)
            s = connection_defined(c, name);
    }
    if (!s && (kind == CSV_SINK || kind == JSON_SINK || kind == SQL_SINK)) {
        ud_string name = {0};
        s = ud_json_string(ud_member(config, "connection"), &name);
        if (!s)
            s = connection_defined(c, name);
    }
    ud_json_destroy(config);
    return s;
}
static void dispose(void *ptr, ud_status result) {
    (void)result;
    builtin *b = ptr;
    if (!b)
        return;
    ud_csv_close(b->csv);
    ud_json_reader_close(b->reader);
    if (b->writer)
        (void)ud_json_writer_close(b->writer);
    if (b->csv_writer)
        (void)ud_csv_writer_close(b->csv_writer);
    if (b->temporary && b->temporary_owned && !b->published)
        (void)ud_remove_file(b->context.memory, b->temporary);
    ud_unlock(b->lock);
    (void)ud_connection_lease_release(b->lease);
    for (uint32_t i = 0; i < b->expression_count; i++)
        ud_expression_destroy(b->expressions[i]);
    ud_free(b->expressions);
    ud_batch_destroy(b->schema);
    for (int i = 0; i < 2; i++) {
        ud_batch_destroy(b->inputs[i]);
    }
    (void)ud_operator_destroy(b->operator);
    ud_free(b->fields);
    ud_free(b->path);
    ud_free(b->temporary);
    ud_free(b->job);
    ud_free(b->keys);
    ud_json_destroy(b->config);
    ud_free(b);
}
static ud_status prepare(void *user, const ud_component_context *c, ud_string text,
                         const ud_batch *const *in, const ud_batch *const *out, void **instance) {
    *instance = NULL;
    builtin *b = ud_alloc(c->memory, sizeof(*b));
    if (!b)
        return UD_RESOURCE;
    b->kind = (int)(intptr_t)user;
    b->context = *c;
    ud_status s = parse(c, text, &b->config);
    int kind = b->kind;
    uint32_t inputs = kind <= SQL_SOURCE                                    ? 0
                      : kind == JOIN || kind == LOOKUP || kind == UNION_ALL ? 2
                                                                            : 1;
    for (uint32_t i = 0; !s && i < inputs; i++)
        s = clone_schema(c->memory, in[i], b->inputs + i);
    if (!s)
        s = clone_schema(c->memory,
                         kind == CSV_SINK || kind == JSON_SINK || kind == SQL_SINK ? in[0] : out[0],
                         &b->schema);
    if (!s) {
        b->columns = b->schema->columns;
        b->fields = ud_alloc(c->memory, b->columns * sizeof(*b->fields));
        if (!b->fields)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < b->columns; i++)
            s = ud_batch_field(b->schema, i, b->fields + i);
    }
    if (!s && (kind <= SQL_SOURCE || kind == CSV_SINK || kind == JSON_SINK || kind == SQL_SINK)) {
        ud_string connection = {0};
        s = ud_json_string(ud_member(b->config, "connection"), &connection);
        uint64_t caps = kind <= SQL_SOURCE ? UD_CONNECT_READ : UD_CONNECT_APPEND;
        if (kind == SQL_SINK) {
            ud_string mode = {0};
            s = ud_json_string(ud_member(b->config, "mode"), &mode);
            b->mode = ud_named(mode, "append")   ? UD_WRITE_APPEND
                      : ud_named(mode, "upsert") ? UD_WRITE_UPSERT
                      : ud_named(mode, "delete") ? UD_WRITE_DELETE
                                                 : 0;
            if (!b->mode)
                s = UD_UNSUPPORTED;
            caps = b->mode == UD_WRITE_APPEND   ? UD_CONNECT_APPEND
                   : b->mode == UD_WRITE_UPSERT ? UD_CONNECT_UPSERT
                                                : UD_CONNECT_DELETE;
        }
        if (!s)
            s = ud_activation_acquire_connection(c->activation, connection, caps, &b->lease);
    }
    uint64_t batch = c->max_batch_rows;
    if (!s && ud_member(b->config, "batchSize"))
        s = ud_json_uint(ud_member(b->config, "batchSize"), &batch);
    if (!s && (!batch || batch > c->max_batch_rows))
        s = UD_SCHEMA;
    if (!s &&
        (kind == CSV_SOURCE || kind == JSON_SOURCE || kind == CSV_SINK || kind == JSON_SINK)) {
        ud_string path = {0};
        s = ud_connection_lease_path(b->lease, &path);
        if (!s) {
            b->path = ud_strdup_n(c->memory, path.data, path.length);
            if (!b->path)
                s = UD_RESOURCE;
        }
    }
    if (!s && (kind == CSV_SOURCE || kind == JSON_SOURCE))
        s = ud_checkpoint_source(c->activation, b->path);
    if (!s && kind == CSV_SOURCE) {
        ud_string null = {"\\N", 2};
        if (ud_member(b->config, "nullMarker"))
            s = ud_json_string(ud_member(b->config, "nullMarker"), &null);
        ud_csv_options o = {sizeof(o), UD_ABI_VERSION, (uint32_t)batch, null.data, null.length};
        if (!s)
            s = ud_csv_open(c->memory, b->path, b->fields, b->columns, &o, &b->csv);
    }
    if (!s && kind == JSON_SOURCE) {
        ud_json_options o = {sizeof(o), UD_CONFIG_VERSION, (uint32_t)batch, 1048576,
                             UD_ROUND_EXACT};
        s = ud_json_reader_open(c->memory, b->path, b->fields, b->columns, &o, &b->reader);
    }
    if (!s && kind == SQL_SOURCE) {
        ud_string sql = {0};
        s = ud_json_string(ud_member(b->config, "sql"), &sql);
        ud_read_options o = {sizeof(o), UD_ABI_VERSION, (uint32_t)batch, sql};
        if (!s)
            s = ud_connection_lease_read_open(b->lease, &o, b->schema);
    }
    if (!s && (kind == CSV_SINK || kind == JSON_SINK)) {
        size_t len = strlen(b->path);
        char *lock = ud_alloc(c->memory, len + 32);
        if (!lock)
            s = UD_RESOURCE;
        else {
            snprintf(lock, len + 32, "%s.ud-package.lock", b->path);
            s = ud_lock(c->memory, lock, &b->lock);
            ud_free(lock);
        }
        int replaying = 0;
        if (!s) {
            b->temporary = ud_alloc(c->memory, len + 32);
            if (!b->temporary)
                s = UD_RESOURCE;
            else
                snprintf(b->temporary, len + 32, "%s.ud-package.part", b->path);
        }
        if (!s)
            s = ud_checkpoint_stage(c->activation, b->path, b->temporary, &replaying);
        struct stat info;
        if (!s) {
            b->existing = !stat(b->path, &info);
            if (b->existing && !replaying)
                s = UD_BUSY;
            else if (!b->existing && errno != ENOENT)
                s = UD_IO;
        }
        if (!s && replaying)
            s = ud_remove_file(c->memory, b->temporary);
        if (!s) {
            s = ud_reserve_file(c->memory, b->temporary);
            if (!s)
                b->temporary_owned = 1;
        }
        if (!s)
            s = kind == CSV_SINK ? ud_csv_writer_open(c->memory, b->temporary, b->fields,
                                                      b->columns, "\\N", &b->csv_writer)
                                 : ud_json_writer_open(c->memory, b->temporary, b->fields,
                                                       b->columns, 1048576, &b->writer);
    }
    if (!s && kind == SQL_SINK) {
        const ud_json *keys = ud_member(b->config, "keys");
        if (keys && ud_json_kind(keys) != UD_JSON_ARRAY)
            s = UD_SCHEMA;
        b->key_count = ud_json_size(keys);
        b->keys = ud_alloc(c->memory, (b->key_count + 1) * sizeof(*b->keys));
        if (!b->keys)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < b->key_count; i++) {
            uint64_t key;
            s = ud_json_uint(ud_json_at(keys, i), &key);
            if (!s && key >= b->columns)
                s = UD_SCHEMA;
            if (!s)
                b->keys[i] = (uint32_t)key;
        }
        ud_activation *e = c->activation;
        if (!s && !e->execution->options.execution_id.length)
            s = UD_INVALID;
        size_t len = e->execution->options.execution_id.length + c->task_id.length +
                     c->component_id.length + 64;
        if (!s) {
            b->job = ud_alloc(c->memory, len);
            if (!b->job)
                s = UD_RESOURCE;
            else
                snprintf(b->job, len, "%.*s/%.*s/%.*s",
                         (int)e->execution->options.execution_id.length,
                         e->execution->options.execution_id.data, (int)c->task_id.length,
                         c->task_id.data, (int)c->component_id.length, c->component_id.data);
        }
    }
    if (!s && (kind == DERIVE || kind == SPLIT)) {
        const ud_json *columns = ud_member(b->config, "columns");
        b->expression_count = kind == SPLIT ? 1 : ud_json_size(columns);
        b->expressions = ud_alloc(c->memory, b->expression_count * sizeof(*b->expressions));
        if (!b->expressions)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < b->expression_count; i++)
            s = bind_expr(c,
                          kind == SPLIT ? ud_member(b->config, "condition")
                                        : ud_member(ud_json_at(columns, i), "expression"),
                          in[0], b->expressions + i);
    }
    if (!s &&
        (kind == SORT || kind == JOIN || kind == LOOKUP || kind == WINDOW || kind == AGGREGATE))
        s = create_operator(c, kind, b->config, in, &b->operator);
    if (s) {
        dispose(b, s);
        return s;
    }
    *instance = b;
    return UD_OK;
}
static ud_status transformed(builtin *b, const ud_batch *in) {
    int kind = b->kind;
    ud_status s = UD_OK;
    ud_batch *output[2] = {0};
    if (kind == PASS || kind == MULTICAST) {
        s = b->context.emit(b->context.emit_user, 0, (ud_batch *)in);
        if (!s && kind == MULTICAST)
            s = b->context.emit(b->context.emit_user, 1, (ud_batch *)in);
        return s;
    }
    if (kind == UNION_ALL) {
        uint32_t *indices = ud_alloc(b->context.memory, b->columns * sizeof(*indices));
        if (!indices)
            return UD_RESOURCE;
        for (uint32_t j = 0; j < b->columns; j++)
            indices[j] = j;
        s = ud_batch_project(b->context.memory, in, indices, b->fields, b->columns, output);
        ud_free(indices);
        if (!s)
            s = b->context.emit(b->context.emit_user, 0, output[0]);
        ud_batch_destroy(output[0]);
        return s;
    }
    if (kind == CAST) {
        uint64_t rounding = 0;
        if (ud_member(b->config, "rounding"))
            s = ud_json_uint(ud_member(b->config, "rounding"), &rounding);
        if (!s && rounding > UD_ROUND_CEILING)
            s = UD_SCHEMA;
        if (!s)
            s = ud_batch_cast(b->context.memory, in, b->fields, b->columns, (int32_t)rounding,
                              output);
    } else {
        s = ud_batch_create(b->context.memory, b->fields, b->columns, in->rows, output);
        if (!s && kind == SPLIT)
            s = ud_batch_create(b->context.memory, b->fields, b->columns, in->rows, output + 1);
        ud_value *values = ud_alloc(b->context.memory, b->columns * sizeof(*values));
        if (!values && !s)
            s = UD_RESOURCE;
        for (uint32_t row = 0; !s && row < in->rows; row++) {
            s = ud_activation_check(b->context.activation);
            for (uint32_t j = 0; !s && j < in->columns; j++)
                s = ud_batch_get(in, row, j, values + j);
            uint32_t port = 0;
            if (kind == SPLIT) {
                ud_value value = {0};
                if (!s)
                    s = ud_expression_evaluate(b->expressions[0], in, row, &value);
                if (!s)
                    port = value.is_null || !value.integer ? 1 : 0;
            } else
                for (uint32_t j = 0; !s && j < b->expression_count; j++)
                    s = ud_expression_evaluate(b->expressions[j], in, row,
                                               values + in->columns + j);
            if (!s)
                s = ud_batch_append(output[port], values, b->columns);
        }
        ud_free(values);
    }
    for (uint32_t i = 0; !s && i < 2; i++)
        if (output[i] && output[i]->rows)
            s = b->context.emit(b->context.emit_user, i, output[i]);
    ud_batch_destroy(output[0]);
    ud_batch_destroy(output[1]);
    return s;
}
static ud_status blocking(builtin *b) {
    ud_batch *output = NULL;
    ud_status s = ud_operator_pull(b->operator, &output);
    if (!s)
        s = b->context.emit(b->context.emit_user, 0, output);
    ud_batch_destroy(output);
    return s;
}
static ud_status step(void *ptr, const ud_component_event *event) {
    builtin *b = ptr;
    int kind = b->kind;
    UD_TRY(ud_activation_check(b->context.activation));
    if (kind <= SQL_SOURCE) {
        UD_TRY(ud_connection_lease_authorize(b->lease, UD_ACCESS_READ));
        ud_batch *batch = NULL;
        ud_status s = kind == CSV_SOURCE    ? ud_csv_next(b->csv, &batch)
                      : kind == JSON_SOURCE ? ud_json_reader_next(b->reader, &batch)
                                            : ud_connection_lease_read_next(b->lease, &batch);
        if (!s)
            s = b->context.emit(b->context.emit_user, 0, batch);
        ud_batch_destroy(batch);
        return s;
    }
    if (event->kind == UD_COMPONENT_INPUT_END)
        return b->operator ? ud_operator_finish_input(b->operator, event->input) : UD_OK;
    if (event->kind == UD_COMPONENT_POLL)
        return kind == SORT || kind == JOIN || kind == LOOKUP || kind == WINDOW || kind == AGGREGATE
                   ? blocking(b)
                   : UD_END;
    if (kind == CSV_SINK || kind == JSON_SINK) {
        UD_TRY(ud_connection_lease_authorize(b->lease, UD_ACCESS_WRITE));
        return kind == CSV_SINK ? ud_csv_writer_append(b->csv_writer, event->batch)
                                : ud_json_writer_append(b->writer, event->batch);
    }
    if (kind == SQL_SINK) {
        ud_string table = {0};
        UD_TRY(ud_json_string(ud_member(b->config, "table"), &table));
        char id[32], position[32];
        snprintf(id, sizeof(id), "%llu", (unsigned long long)b->number);
        snprintf(position, sizeof(position), "batch:%llu", (unsigned long long)b->number);
        ud_write_options o = {sizeof(o),
                              UD_ABI_VERSION,
                              b->mode,
                              table,
                              {b->job, (uint32_t)strlen(b->job)},
                              {id, (uint32_t)strlen(id)},
                              {position, (uint32_t)strlen(position)},
                              b->keys,
                              b->key_count};
        uint8_t digest[32];
        ud_status s = ud_connection_lease_digest(b->lease, &o, event->batch, digest);
        size_t length = strlen(b->job) + strlen(id) + 8;
        char *identity = ud_alloc(b->context.memory, length);
        if (!identity)
            return UD_RESOURCE;
        snprintf(identity, length, "%s/%s", b->job, id);
        ud_string key = {identity, (uint32_t)strlen(identity)};
        int replaying = ud_checkpoint_batch_seen(b->context.activation, key);
        if (!s)
            s = ud_checkpoint_batch(b->context.activation, key, digest, 0);
        if (!s && replaying) {
            ud_commit_receipt receipt = {0};
            s = ud_connection_lease_receipt(b->lease, o.job, o.batch_id, &receipt);
            if (!s && receipt.committed &&
                (memcmp(receipt.digest, digest, 32) ||
                 !ud_same(receipt.source_position, o.source_position)))
                s = UD_CORRUPT;
            ud_free((void *)receipt.source_position.data);
        }
        if (!s)
            s = ud_connection_lease_write(b->lease, &o, event->batch);
        if (!s) {
            ud_activation_notify_kind(b->context.activation, UD_EXEC_EVENT_TARGET_COMMITTED,
                                      UD_EXEC_RUNNING, UD_OK);
            s = ud_checkpoint_batch(b->context.activation, key, digest, 1);
            if (s)
                s = UD_COMMIT_UNKNOWN;
        }
        if (!s)
            b->number++;
        ud_free(identity);
        return s;
    }
    if (kind == SORT || kind == JOIN || kind == LOOKUP || kind == WINDOW || kind == AGGREGATE)
        return ud_operator_push(b->operator, event->input, event->batch);
    return transformed(b, event->batch);
}
static ud_status finish(void *ptr) {
    builtin *b = ptr;
    if (b->kind == CSV_SOURCE || b->kind == JSON_SOURCE)
        return ud_checkpoint_source(b->context.activation, b->path);
    if (b->kind != CSV_SINK && b->kind != JSON_SINK)
        return UD_OK;
    ud_status s =
        b->kind == CSV_SINK ? ud_csv_writer_close(b->csv_writer) : ud_json_writer_close(b->writer);
    b->csv_writer = NULL;
    b->writer = NULL;
    if (!s)
        s = ud_connection_lease_authorize(b->lease, UD_ACCESS_WRITE);
    if (!s && b->existing) {
        uint8_t expected[32], actual[32];
        s = ud_file_hash(b->context.memory, b->temporary, expected);
        if (!s)
            s = ud_file_hash(b->context.memory, b->path, actual);
        if (!s && memcmp(expected, actual, 32))
            s = UD_CORRUPT;
        if (!s)
            s = ud_remove_file(b->context.memory, b->temporary);
    } else if (!s)
        s = ud_publish(b->context.memory, b->temporary, b->path);
    if (!s) {
        b->published = 1;
        ud_activation_notify_kind(b->context.activation, UD_EXEC_EVENT_TARGET_COMMITTED,
                                  UD_EXEC_RUNNING, UD_OK);
        s = ud_checkpoint_file_done(b->context.activation, b->path);
        if (s)
            s = UD_COMMIT_UNKNOWN;
    }
    return s;
}
static ud_status sql_task(void *user, const ud_component_context *c, ud_string text) {
    (void)user;
    ud_json *config = NULL;
    UD_TRY(parse(c, text, &config));
    static const char *keys[] = {"connection", "statement"};
    ud_status s = ud_json_keys(config, keys, 2);
    ud_string id = {0}, statement = {0};
    if (!s)
        s = ud_json_string(ud_member(config, "connection"), &id);
    if (!s)
        s = ud_json_string(ud_member(config, "statement"), &statement);
    ud_connection_lease *lease = NULL;
    if (!s)
        s = ud_activation_acquire_connection(c->activation, id, UD_CONNECT_EXECUTE_SQL, &lease);
    if (!s)
        s = ud_connection_lease_execute_sql(lease, statement);
    ud_status clean = ud_connection_lease_release(lease);
    ud_json_destroy(config);
    return s ? s : clean;
}
static const char *configuration_schema(int kind) {
    switch (kind) {
    case CSV_SOURCE:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"schema\":{\"type\":\"object\"},\"batchSize\":{\"type\":\"integer\"},"
               "\"nullMarker\":{\"type\":\"string\"}},\"required\":[\"connection\",\"schema\"],"
               "\"additionalProperties\":false}";
    case JSON_SOURCE:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"schema\":{\"type\":\"object\"},\"batchSize\":{\"type\":\"integer\"}},"
               "\"required\":[\"connection\",\"schema\"],\"additionalProperties\":false}";
    case SQL_SOURCE:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"schema\":{\"type\":\"object\"},\"batchSize\":{\"type\":\"integer\"},\"sql\":{"
               "\"type\":\"string\"}},\"required\":[\"connection\",\"schema\",\"sql\"],"
               "\"additionalProperties\":false}";
    case CSV_SINK:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"}},"
               "\"required\":[\"connection\"],\"additionalProperties\":false}";
    case JSON_SINK:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"}},"
               "\"required\":[\"connection\"],\"additionalProperties\":false}";
    case SQL_SINK:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"table\":{\"type\":\"string\"},\"mode\":{\"type\":\"string\"},\"keys\":{\"type\":"
               "\"array\"}},\"required\":[\"connection\",\"table\",\"mode\",\"keys\"],"
               "\"additionalProperties\":false}";
    case CAST:
        return "{\"type\":\"object\",\"properties\":{\"schema\":{\"type\":\"object\"},\"rounding\":"
               "{\"type\":\"integer\"}},\"required\":[\"schema\"],\"additionalProperties\":false}";
    case DERIVE:
        return "{\"type\":\"object\",\"properties\":{\"columns\":{\"type\":\"array\"}},"
               "\"required\":[\"columns\"],\"additionalProperties\":false}";
    case SPLIT:
        return "{\"type\":\"object\",\"properties\":{\"condition\":{\"type\":\"object\"}},"
               "\"required\":[\"condition\"],\"additionalProperties\":false}";
    case SORT:
        return "{\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"integer\"},\"descending\":"
               "{\"type\":\"boolean\"}},\"required\":[\"key\"],\"additionalProperties\":false}";
    case JOIN:
        return "{\"type\":\"object\",\"properties\":{\"leftKey\":{\"type\":\"integer\"},"
               "\"rightKey\":{\"type\":\"integer\"}},\"required\":[\"leftKey\",\"rightKey\"],"
               "\"additionalProperties\":false}";
    case LOOKUP:
        return "{\"type\":\"object\",\"properties\":{\"leftKey\":{\"type\":\"integer\"},"
               "\"rightKey\":{\"type\":\"integer\"}},\"required\":[\"leftKey\",\"rightKey\"],"
               "\"additionalProperties\":false}";
    case AGGREGATE:
        return "{\"type\":\"object\",\"properties\":{\"plan\":{\"type\":\"object\"}},\"required\":["
               "\"plan\"],\"additionalProperties\":false}";
    case WINDOW:
        return "{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"array\"},"
               "\"partitionKeys\":{\"type\":\"integer\"},\"function\":{\"type\":\"string\"},"
               "\"column\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"}},\"required\":["
               "\"keys\",\"partitionKeys\",\"function\",\"name\"],\"additionalProperties\":false}";
    default:
        return "{\"type\":\"object\",\"additionalProperties\":false}";
    }
}
ud_status ud_builtin_register(ud_registry *registry) {
    static const struct {
        const char *id;
        int kind;
        uint32_t inputs, outputs;
        int sync, error;
    } entries[] = {{"file.csv.source", CSV_SOURCE, 0, 1, -1, -1},
                   {"file.ndjson.source", JSON_SOURCE, 0, 1, -1, -1},
                   {"sql.source", SQL_SOURCE, 0, 1, -1, -1},
                   {"file.csv.destination", CSV_SINK, 1, 0, -1, -1},
                   {"file.ndjson.destination", JSON_SINK, 1, 0, -1, -1},
                   {"sql.destination", SQL_SINK, 1, 0, -1, -1},
                   {"pass", PASS, 1, 1, 0, -1},
                   {"cast", CAST, 1, 1, -1, -1},
                   {"derive", DERIVE, 1, 1, -1, -1},
                   {"split", SPLIT, 1, 2, -1, -1},
                   {"multicast", MULTICAST, 1, 2, 0, -1},
                   {"unionAll", UNION_ALL, 2, 1, -1, -1},
                   {"sort", SORT, 1, 1, -1, -1},
                   {"join", JOIN, 2, 1, -1, -1},
                   {"aggregate", AGGREGATE, 1, 1, -1, -1},
                   {"lookup", LOOKUP, 2, 1, -1, -1},
                   {"window", WINDOW, 1, 1, -1, -1}};
    for (uint32_t i = 0; i < sizeof(entries) / sizeof(*entries); i++) {
        int kind = entries[i].kind;
        int category = kind <= SQL_SOURCE ? UD_COMPONENT_SOURCE
                       : kind <= SQL_SINK ? UD_COMPONENT_DESTINATION
                                          : UD_COMPONENT_TRANSFORM;
        ud_component_descriptor d = {
            .struct_size = sizeof(d),
            .abi_version = UD_ABI_VERSION,
            .id = {entries[i].id, (uint32_t)strlen(entries[i].id)},
            .name = {entries[i].id, (uint32_t)strlen(entries[i].id)},
            .version = {"1.0.0", 5},
            .configuration_schema = {configuration_schema(kind),
                                     (uint32_t)strlen(configuration_schema(kind))},
            .category = category,
            .capabilities =
                UD_COMPONENT_THREAD_SAFE | (kind == SQL_SOURCE ? 0 : UD_COMPONENT_REPLAY_SAFE),
            .inputs = entries[i].inputs,
            .outputs = entries[i].outputs,
            .synchronous_input = entries[i].sync,
            .error_output = entries[i].error,
            .user = (void *)(intptr_t)kind,
            .configure = configure,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .dispose = dispose};
        UD_TRY(ud_registry_register(registry, &d));
    }
    ud_component_descriptor task = {.struct_size = sizeof(task),
                                    .abi_version = UD_ABI_VERSION,
                                    .id = {"sql.execute", 11},
                                    .name = {"Execute SQL", 11},
                                    .version = {"1.0.0", 5},
                                    .configuration_schema = {"{\"type\":\"object\"}", 17},
                                    .category = UD_COMPONENT_TASK,
                                    .synchronous_input = -1,
                                    .error_output = -1,
                                    .invoke = sql_task};
    UD_TRY(ud_registry_register(registry, &task));
    UD_TRY(ud_transforms_register(registry));
    UD_TRY(ud_filesystem_register(registry));
    UD_TRY(ud_process_register(registry));
    UD_TRY(ud_http_register(registry));
    return ud_profile_register(registry);
}