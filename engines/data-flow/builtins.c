#include "../package/internal.h"
#include "ud/connection_manager.h"
#include "ud/compute.h"
#include "ud/format.h"
#include "ud/csv.h"
#include <errno.h>
#include <sys/stat.h>
enum {
    CSV_SOURCE = 1,
    JSON_SOURCE,
    SQL_SOURCE,
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
    AGGREGATE
};
typedef struct builtin {
    int kind;
    ud_component_context context;
    ud_json *config;
    ud_batch *schema, *inputs[2], *buffer[2], *result;
    ud_field *fields;
    uint32_t columns, offset;
    ud_expression **expressions;
    uint32_t expression_count;
    ud_connection_lease *lease;
    ud_csv_reader *csv;
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
static ud_status schema_config(ud_context *ctx, const ud_json *config, ud_batch **out) {
    const ud_json *s = ud_member(config, "schema");
    char *json = NULL;
    uint32_t n;
    UD_TRY(ud_json_serialize(s, &json, &n));
    ud_status status = ud_schema_from_json(ctx, json, n, out);
    ud_free(json);
    return status;
}
static ud_status parse(const ud_component_context *c, ud_string config, ud_json **out) {
    return ud_json_parse(c->memory, config.data, config.length, out, NULL);
}
static ud_status bind_expr(const ud_component_context *c, const ud_json *value,
                           const ud_batch *schema, ud_expression **out) {
    char *json;
    uint32_t n;
    UD_TRY(ud_json_serialize(value, &json, &n));
    ud_expression_options o = {sizeof(o), UD_ABI_VERSION, schema, c->resolve, c->resolve_user};
    ud_status s = ud_expression_create(c->memory, (ud_string){json, n}, &o, out);
    ud_free(json);
    return s;
}
static ud_status connection_defined(const ud_component_context *ctx, ud_string id) {
    const ud_json *definition = NULL;
    int project_scope = 0;
    return ud_connection_definition(ctx->execution, id, &definition, &project_scope);
}
static ud_status check_config(int kind, const ud_json *config) {
    static const char *source[] = {"connection", "schema", "batchSize", "sql", "nullMarker"};
    static const char *sink[] = {"connection", "table", "mode", "keys"};
    static const char *cast[] = {"schema", "rounding"}, *derive[] = {"columns"},
                      *split[] = {"condition"}, *sort[] = {"key", "descending"},
                      *join[] = {"leftKey", "rightKey"}, *aggregate[] = {"commands"};
    if (kind <= SQL_SOURCE)
        return ud_json_keys(config, source, 5);
    if (kind == JSON_SINK || kind == SQL_SINK)
        return ud_json_keys(config, sink, 4);
    if (kind == CAST)
        return ud_json_keys(config, cast, 2);
    if (kind == DERIVE)
        return ud_json_keys(config, derive, 1);
    if (kind == SPLIT)
        return ud_json_keys(config, split, 1);
    if (kind == SORT)
        return ud_json_keys(config, sort, 2);
    if (kind == JOIN)
        return ud_json_keys(config, join, 2);
    if (kind == AGGREGATE)
        return ud_json_keys(config, aggregate, 1);
    return ud_json_size(config) == 0 ? UD_OK : UD_SCHEMA;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    ud_status s = parse(c, text, &config);
    if (!s)
        s = check_config(kind, config);
    if (!s && (kind <= SQL_SOURCE))
        s = schema_config(c->memory, config, out);
    else if (!s && (kind == JSON_SINK || kind == SQL_SINK)) { /* No output schemas. */
    } else if (!s && kind == CAST)
        s = schema_config(c->memory, config, out);
    else if (!s && kind == DERIVE) {
        const ud_json *columns = ud_member(config, "columns");
        uint32_t count = ud_json_size(columns), original = ud_batch_columns(in[0]);
        if (ud_json_kind(columns) != UD_JSON_ARRAY || !count || count > 65536 - original)
            s = UD_SCHEMA;
        ud_field *fields = !s ? ud_alloc(c->memory, (original + count) * sizeof(*fields)) : NULL;
        if (!s && !fields)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < original; i++)
            s = ud_batch_field(in[0], i, fields + i);
        for (uint32_t i = 0; !s && i < count; i++) {
            const ud_json *col = ud_json_at(columns, i);
            static const char *keys[] = {"name", "expression"};
            s = ud_json_keys(col, keys, 2);
            ud_string name;
            if (!s)
                s = ud_json_string(ud_member(col, "name"), &name);
            ud_expression *e = NULL;
            if (!s)
                s = bind_expr(c, ud_member(col, "expression"), in[0], &e);
            if (!s)
                fields[original + i] = (ud_field){.name = name.data,
                                                  .name_length = name.length,
                                                  .type = ud_expression_type(e),
                                                  .nullable = 1};
            ud_expression_destroy(e);
        }
        if (!s)
            s = ud_batch_create(c->memory, fields, original + count, 0, out);
        ud_free(fields);
    } else if (!s && kind == JOIN) {
        uint32_t a = ud_batch_columns(in[0]), b = ud_batch_columns(in[1]);
        uint64_t lk = 0, rk = 0;
        s = ud_json_uint(ud_member(config, "leftKey"), &lk);
        if (!s)
            s = ud_json_uint(ud_member(config, "rightKey"), &rk);
        if (!s && (lk >= a || rk >= b))
            s = UD_SCHEMA;
        ud_field x, y;
        if (!s) {
            ud_batch_field(in[0], (uint32_t)lk, &x);
            ud_batch_field(in[1], (uint32_t)rk, &y);
            y.nullable = x.nullable;
            if (!ud_field_equal(&x, &y, 0))
                s = UD_SCHEMA;
        }
        ud_field *fields = !s ? ud_alloc(c->memory, (a + b) * sizeof(*fields)) : NULL;
        if (!s && !fields)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < a; i++)
            s = ud_batch_field(in[0], i, fields + i);
        for (uint32_t i = 0; !s && i < b; i++)
            s = ud_batch_field(in[1], i, fields + a + i);
        if (!s)
            s = ud_batch_create(c->memory, fields, a + b, 0, out);
        ud_free(fields);
    } else if (!s && kind == AGGREGATE) {
        ud_string commands;
        s = ud_json_string(ud_member(config, "commands"), &commands);
        uint32_t n = ud_batch_columns(in[0]);
        ud_field *fields = ud_alloc(c->memory, n * sizeof(*fields));
        if (!fields)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < n; i++)
            s = ud_batch_field(in[0], i, fields + i);
        ud_plan *plan = NULL;
        if (!s)
            s = ud_plan_create_fields(c->memory, UD_PLAN_VERSION, fields, n, commands.data,
                                      commands.length, &plan);
        ud_accumulator *acc = NULL;
        if (!s)
            s = ud_accumulator_create(plan, &acc);
        if (!s)
            s = ud_accumulator_snapshot(acc, out);
        ud_accumulator_destroy(acc);
        ud_plan_destroy(plan);
        ud_free(fields);
    } else if (!s)
        s = clone_schema(c->memory, in[0], out);
    if (!s && (kind == SPLIT || kind == MULTICAST))
        s = clone_schema(c->memory, in[0], out + 1);
    if (!s && kind == UNION_ALL) {
        if (in[0]->columns != in[1]->columns)
            s = UD_SCHEMA;
        for (uint32_t i = 0; !s && i < in[0]->columns; i++)
            if (!ud_field_equal(&in[0]->column[i].field, &in[1]->column[i].field, 1))
                s = UD_SCHEMA;
    }
    if (!s && kind == SPLIT) {
        ud_expression *expr = NULL;
        s = bind_expr(c, ud_member(config, "condition"), in[0], &expr);
        if (!s && ud_expression_type(expr) != UD_BOOL)
            s = UD_SCHEMA;
        ud_expression_destroy(expr);
    }
    if (!s && kind <= SQL_SOURCE) {
        ud_string name;
        s = ud_json_string(ud_member(config, "connection"), &name);
        if (!s)
            s = connection_defined(c, name);
    }
    if (!s && (kind == JSON_SINK || kind == SQL_SINK)) {
        ud_string name;
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
        ud_batch_destroy(b->buffer[i]);
    }
    ud_batch_destroy(b->result);
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
    uint32_t inputs = kind <= SQL_SOURCE ? 0 : kind == JOIN || kind == UNION_ALL ? 2 : 1;
    for (uint32_t i = 0; !s && i < inputs; i++)
        s = clone_schema(c->memory, in[i], b->inputs + i);
    if (!s)
        s = clone_schema(c->memory, kind == JSON_SINK || kind == SQL_SINK ? in[0] : out[0],
                         &b->schema);
    if (!s) {
        b->columns = b->schema->columns;
        b->fields = ud_alloc(c->memory, b->columns * sizeof(*b->fields));
        if (!b->fields)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < b->columns; i++)
            s = ud_batch_field(b->schema, i, b->fields + i);
    }
    if (!s && (kind <= SQL_SOURCE || kind == JSON_SINK || kind == SQL_SINK)) {
        ud_string connection;
        s = ud_json_string(ud_member(b->config, "connection"), &connection);
        uint64_t caps = kind <= SQL_SOURCE ? UD_CONNECT_READ : UD_CONNECT_APPEND;
        if (kind == SQL_SINK) {
            ud_string mode;
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
            s = ud_execution_acquire_connection(c->execution, connection, caps, &b->lease);
    }
    uint64_t batch = c->max_batch_rows;
    if (!s && ud_member(b->config, "batchSize"))
        s = ud_json_uint(ud_member(b->config, "batchSize"), &batch);
    if (!s && (!batch || batch > c->max_batch_rows))
        s = UD_SCHEMA;
    if (!s && (kind == CSV_SOURCE || kind == JSON_SOURCE || kind == JSON_SINK)) {
        ud_string path;
        s = ud_connection_lease_path(b->lease, &path);
        if (!s) {
            b->path = ud_strdup_n(c->memory, path.data, path.length);
            if (!b->path)
                s = UD_RESOURCE;
        }
    }
    if (!s && (kind == CSV_SOURCE || kind == JSON_SOURCE))
        s = ud_checkpoint_source(c->execution, b->path);
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
        ud_string sql;
        s = ud_json_string(ud_member(b->config, "sql"), &sql);
        ud_read_options o = {sizeof(o), UD_ABI_VERSION, (uint32_t)batch, sql};
        if (!s)
            s = ud_connection_lease_read_open(b->lease, &o, b->schema);
    }
    if (!s && kind == JSON_SINK) {
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
            s = ud_checkpoint_stage(c->execution, b->path, b->temporary, &replaying);
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
            s = ud_json_writer_open(c->memory, b->temporary, b->fields, b->columns, 1048576,
                                    &b->writer);
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
        ud_execution *e = c->execution;
        if (!s && !e->options.execution_id.length)
            s = UD_INVALID;
        size_t len =
            e->options.execution_id.length + c->task_id.length + c->component_id.length + 64;
        if (!s) {
            b->job = ud_alloc(c->memory, len);
            if (!b->job)
                s = UD_RESOURCE;
            else
                snprintf(b->job, len, "%.*s/%.*s/%.*s", (int)e->options.execution_id.length,
                         e->options.execution_id.data, (int)c->task_id.length, c->task_id.data,
                         (int)c->component_id.length, c->component_id.data);
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
    if (s) {
        dispose(b, s);
        return s;
    }
    *instance = b;
    return UD_OK;
}
static ud_status append_input(builtin *b, const ud_batch *in, uint32_t port) {
    if (port > 1)
        return UD_SCHEMA;
    if (!b->buffer[port])
        UD_TRY(clone_schema(b->context.memory, b->inputs[port], b->buffer + port));
    ud_value *values = ud_alloc(b->context.memory, in->columns * sizeof(*values));
    if (!values)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t row = 0; !s && row < in->rows; row++) {
        s = ud_execution_check(b->context.execution);
        for (uint32_t j = 0; !s && j < in->columns; j++)
            s = ud_batch_get(in, row, j, values + j);
        if (!s)
            s = ud_batch_append(b->buffer[port], values, in->columns);
    }
    ud_free(values);
    return s;
}
static ud_status transformed(builtin *b, const ud_batch *in) {
    int kind = b->kind;
    ud_status s = UD_OK;
    ud_batch *output[2] = {0};
    if (kind == PASS || kind == MULTICAST || kind == UNION_ALL) {
        s = b->context.emit(b->context.emit_user, 0, (ud_batch *)in);
        if (!s && kind == MULTICAST)
            s = b->context.emit(b->context.emit_user, 1, (ud_batch *)in);
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
            s = ud_execution_check(b->context.execution);
            for (uint32_t j = 0; !s && j < in->columns; j++)
                s = ud_batch_get(in, row, j, values + j);
            uint32_t port = 0;
            if (kind == SPLIT) {
                ud_value value;
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
    if (!b->result) {
        for (int i = 0; i < (b->kind == JOIN ? 2 : 1); i++)
            if (!b->buffer[i])
                UD_TRY(clone_schema(b->context.memory, b->inputs[i], b->buffer + i));
        ud_status s = UD_OK;
        if (b->kind == SORT) {
            uint64_t key = 0;
            s = ud_json_uint(ud_member(b->config, "key"), &key);
            if (!s && key >= b->buffer[0]->columns)
                s = UD_SCHEMA;
            const ud_json *desc = ud_member(b->config, "descending");
            ud_string text = {0};
            int descending = 0;
            if (!s && desc) {
                if (ud_json_kind(desc) != UD_JSON_BOOL)
                    s = UD_SCHEMA;
                else {
                    s = ud_json_text(desc, &text.data, &text.length);
                    descending = ud_named(text, "true");
                }
            }
            if (!s)
                s = ud_compute_sort(b->context.memory, b->buffer[0], (uint32_t)key, descending,
                                    &b->result);
        } else if (b->kind == JOIN) {
            uint64_t lk, rk;
            s = ud_json_uint(ud_member(b->config, "leftKey"), &lk);
            if (!s)
                s = ud_json_uint(ud_member(b->config, "rightKey"), &rk);
            if (!s)
                s = ud_compute_inner_join(b->context.memory, b->buffer[0], (uint32_t)lk,
                                          b->buffer[1], (uint32_t)rk, &b->result);
        } else {
            ud_string commands;
            s = ud_json_string(ud_member(b->config, "commands"), &commands);
            uint32_t n = b->inputs[0]->columns;
            ud_field *fields = ud_alloc(b->context.memory, n * sizeof(*fields));
            if (!fields)
                s = UD_RESOURCE;
            for (uint32_t i = 0; !s && i < n; i++)
                s = ud_batch_field(b->inputs[0], i, fields + i);
            ud_plan *plan = NULL;
            ud_accumulator *acc = NULL;
            if (!s)
                s = ud_plan_create_fields(b->context.memory, UD_PLAN_VERSION, fields, n,
                                          commands.data, commands.length, &plan);
            if (!s)
                s = ud_accumulator_create(plan, &acc);
            if (!s)
                s = ud_accumulator_add(acc, b->buffer[0]);
            if (!s)
                s = ud_accumulator_snapshot(acc, &b->result);
            ud_accumulator_destroy(acc);
            ud_plan_destroy(plan);
            ud_free(fields);
        }
        if (s)
            return s;
    }
    if (b->offset >= b->result->rows)
        return UD_END;
    ud_batch *slice = NULL;
    UD_TRY(ud_compute_slice(b->context.memory, b->result, b->offset, b->context.max_batch_rows,
                            &slice));
    ud_status s = b->context.emit(b->context.emit_user, 0, slice);
    if (!s)
        b->offset += slice->rows;
    ud_batch_destroy(slice);
    return s;
}
static ud_status step(void *ptr, const ud_component_event *event) {
    builtin *b = ptr;
    int kind = b->kind;
    UD_TRY(ud_execution_check(b->context.execution));
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
        return UD_OK;
    if (event->kind == UD_COMPONENT_POLL)
        return kind == SORT || kind == JOIN || kind == AGGREGATE ? blocking(b) : UD_END;
    if (kind == JSON_SINK) {
        UD_TRY(ud_connection_lease_authorize(b->lease, UD_ACCESS_WRITE));
        return ud_json_writer_append(b->writer, event->batch);
    }
    if (kind == SQL_SINK) {
        ud_string table;
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
        int replaying = ud_checkpoint_batch_seen(b->context.execution, key);
        if (!s)
            s = ud_checkpoint_batch(b->context.execution, key, digest, 0);
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
            ud_execution_notify_kind(b->context.execution, UD_EXEC_EVENT_TARGET_COMMITTED,
                                     UD_EXEC_RUNNING, UD_OK);
            s = ud_checkpoint_batch(b->context.execution, key, digest, 1);
            if (s)
                s = UD_COMMIT_UNKNOWN;
        }
        if (!s)
            b->number++;
        ud_free(identity);
        return s;
    }
    if (kind == SORT || kind == JOIN || kind == AGGREGATE)
        return append_input(b, event->batch, event->input);
    return transformed(b, event->batch);
}
static ud_status finish(void *ptr) {
    builtin *b = ptr;
    if (b->kind == CSV_SOURCE || b->kind == JSON_SOURCE)
        return ud_checkpoint_source(b->context.execution, b->path);
    if (b->kind != JSON_SINK)
        return UD_OK;
    ud_status s = ud_json_writer_close(b->writer);
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
        ud_execution_notify_kind(b->context.execution, UD_EXEC_EVENT_TARGET_COMMITTED,
                                 UD_EXEC_RUNNING, UD_OK);
        s = ud_checkpoint_file_done(b->context.execution, b->path);
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
    ud_string id, statement;
    if (!s)
        s = ud_json_string(ud_member(config, "connection"), &id);
    if (!s)
        s = ud_json_string(ud_member(config, "statement"), &statement);
    ud_connection_lease *lease = NULL;
    if (!s)
        s = ud_execution_acquire_connection(c->execution, id, UD_CONNECT_EXECUTE_SQL, &lease);
    if (!s)
        s = ud_connection_lease_execute_sql(lease, statement);
    ud_status clean = ud_connection_lease_release(lease);
    ud_json_destroy(config);
    return s ? s : clean;
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
                   {"aggregate", AGGREGATE, 1, 1, -1, -1}};
    for (uint32_t i = 0; i < sizeof(entries) / sizeof(*entries); i++) {
        int kind = entries[i].kind;
        int category = kind <= SQL_SOURCE ? UD_COMPONENT_SOURCE
                       : kind <= SQL_SINK ? UD_COMPONENT_DESTINATION
                                          : UD_COMPONENT_TRANSFORM;
        ud_component_descriptor d = {.struct_size = sizeof(d),
                                     .abi_version = UD_ABI_VERSION,
                                     .id = {entries[i].id, (uint32_t)strlen(entries[i].id)},
                                     .name = {entries[i].id, (uint32_t)strlen(entries[i].id)},
                                     .version = {"1.0.0", 5},
                                     .configuration_schema = {"{\"type\":\"object\"}", 17},
                                     .category = category,
                                     .capabilities =
                                         kind == SQL_SOURCE ? 0 : UD_COMPONENT_REPLAY_SAFE,
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
    return ud_registry_register(registry, &task);
}
