#include "../package/internal.h"
#include "ud/connection_manager.h"
#include "ud/compute.h"
#include "ud/operator.h"
#include "ud/format.h"
#include "ud/csv.h"
#include "ud/xlsx.h"
#include "../../connectors/rest.h"
#ifdef UD_WITH_ARROW_FILES
#include "ud/columnar_file.h"
#endif
#include "../../components/data/cursor_internal.h"
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
    WINDOW,
    XLSX_SOURCE,
    XLSX_SINK,REST_SOURCE,REST_SINK,
    PARQUET_SOURCE,PARQUET_SINK,IPC_SOURCE,IPC_SINK,IPC_STREAM_SOURCE,IPC_STREAM_SINK
};
static int file_source(int k);
static int source_kind(int k) {
    return k <= SQL_SOURCE || file_source(k) || k==REST_SOURCE;
}
static int file_source(int k) {
    return k == CSV_SOURCE || k == JSON_SOURCE || k == XLSX_SOURCE || k==PARQUET_SOURCE || k==IPC_SOURCE || k==IPC_STREAM_SOURCE;
}
static int file_sink(int k) {
    return k == CSV_SINK || k == JSON_SINK || k == XLSX_SINK || k==PARQUET_SINK || k==IPC_SINK || k==IPC_STREAM_SINK;
}
static int sink_kind(int k) {
    return file_sink(k) || k == SQL_SINK || k==REST_SINK;
}
typedef struct builtin {
    int kind;
    ud_component_context context;
    ud_json *config;
    ud_batch *schema, **inputs, *errors;
    uint32_t input_count;
    ud_operator *operator;
    ud_rest_reader *rest_reader;
    ud_field *fields;
    uint32_t columns;
    ud_expression **expressions;
    uint32_t expression_count;
    ud_connection_lease *lease;
    ud_csv_reader *csv;
    ud_csv_writer *csv_writer;
    ud_json_reader *reader;
    ud_json_writer *writer;
    ud_xlsx_reader *xlsx;
    ud_xlsx_writer *xlsx_writer;
#ifdef UD_WITH_ARROW_FILES
    ud_columnar_reader *columnar_reader;
    ud_columnar_writer *columnar_writer;
#endif
    uint64_t stage_bytes;
    int32_t xlsx_rejected;
    char *path, *temporary, *job;
    void *lock;
    int published, existing, temporary_owned;
    uint64_t number, source_position, source_begin, source_end, written_rows, written_errors;
    uint32_t source_partitions, source_partition;
    uint32_t *keys, key_count, *write_indices, before_column;
    int mode;
    struct ud_rule_delivery *delivery;
    ud_cursor *cursor;
    ud_batch *actual_schema, *write_schema, *parameters;
    ud_operator *source_sort;
    ud_buffer_block **snapshot;
    uint32_t snapshot_capacity;
    uint64_t snapshot_count, snapshot_at, snapshot_rows, source_identified, snapshot_read_ms;
    int materialized;
    uint8_t file_fingerprint[32];
    uint64_t file_hash_ms;
    ud_sha256 snapshot_hash;
    uint8_t snapshot_fingerprint[32];
    ud_id source_scope;
    char source_policy[65];
    char *schema_diff;
    ud_string schema_policy;
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
    ud_string connection = {0}, table = {0};
    ud_status status = ud_json_string(ud_member(config, "connection"), &connection);
    if (!status && ud_member(config, "table"))
        status = ud_json_string(ud_member(config, "table"), &table);
    if (!status)
        status = ud_connection_field_namespace(c->activation, connection, table, scope);
    if (!status)
        status = ud_schema_from_json(c->memory, json, n, (ud_string){scope, 32}, out);
    ud_free(json);
    return status;
}
static ud_status parameters(const ud_component_context *c, const ud_json *config, ud_batch **out) {
    *out = NULL;
    const ud_json *p = ud_member(config, "parameters");
    if (!p)
        return UD_OK;
    static const char *keys[] = {"schema", "rows"};
    UD_TRY(ud_json_keys(p, keys, 2));
    char *schema = NULL, *rows = NULL;
    uint32_t a = 0, b = 0;
    ud_status s = ud_json_serialize(ud_member(p, "schema"), &schema, &a);
    if (!s)
        s = ud_json_serialize(ud_member(p, "rows"), &rows, &b);
    if (!s)
        s = ud_batch_from_json_schema(c->memory, schema, a, rows, b, UD_ROUND_EXACT, out);
    if (!s && (*out)->rows > c->max_batch_rows)
        s = UD_RESOURCE;
    if (!s)
        s = ud_batch_seal(*out);
    ud_free(schema);
    ud_free(rows);
    return s;
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
static ud_status operator_check(void *user) {
    return ud_activation_check(user);
}
static ud_status create_operator(const ud_component_context *c, int kind, const ud_json *config,
                                 const ud_batch *const *inputs, ud_operator **out) {
    ud_operator_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .max_batch_rows = c->max_batch_rows,
                                   .buffers = c->execution->buffers,
                                   .check = operator_check,
                                   .user = c->activation};
    UD_TRY(ud_component_operator_options(c, &options));
    if (kind == JOIN) {
        char *json = NULL;
        uint32_t length = 0;
        ud_status status = ud_json_serialize(config, &json, &length);
        ud_sort_contract orders[2] = {{0}};
        if (c->input_plans) {
            orders[0] = c->input_plans[0].sort;
            orders[1] = c->input_plans[1].sort;
        }
        options.kind = UD_OPERATOR_JOIN;
        options.configuration = (ud_string){json, length};
        options.input_order = orders;
        if (!status)
            status = ud_operator_create(c->memory, &options, inputs, out);
        ud_free(json);
        return status;
    }
    ud_status s = UD_OK;
    uint64_t partitions = 1, maximum = 64;
    if (ud_member(config, "partitions"))
        s = ud_json_uint(ud_member(config, "partitions"), &partitions);
    const ud_json *policy = ud_member(c->activation->package->document, "policy");
    if (!s && ud_member(policy, "maxPartitions"))
        s = ud_json_uint(ud_member(policy, "maxPartitions"), &maximum);
    if (!s && (!partitions || partitions > 64 || partitions > maximum))
        s = UD_RESOURCE;
    options.partition_count = (uint32_t)partitions;
    options.executor = c->execution->executor;
    uint32_t *left_keys = NULL, *right_keys = NULL;
    ud_plan *plan = NULL;
    uint32_t *window_keys = NULL;
    if (!s && kind == WINDOW) {
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
            uint32_t selected = 0;
            s = ud_compute_field_index(inputs[0], ud_json_at(keys, i), &selected);
            index = selected;
            if (!s) window_keys[i] = (uint32_t)index;
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
        static const char *functions[] = {"rowNumber", "runningSum", "rank", "denseRank", "lag", "lead", "first", "last", "movingSum", "movingAverage"};
        for (uint32_t i = 0; i < sizeof(functions) / sizeof(*functions); i++)
            if (ud_named(function, functions[i])) options.window_function = (int32_t)i + 1;
        if (!s && !options.window_function) s = UD_UNSUPPORTED;
        if (!s && (options.window_function == UD_WINDOW_RUNNING_SUM || options.window_function >= UD_WINDOW_LAG)) {
            uint32_t selected = 0;
            s = ud_compute_field_index(inputs[0], ud_member(config, "column"), &selected);
            column = selected;
        }
        uint64_t offset = 1;
        if (!s && ud_member(config, "offset")) s = ud_json_uint(ud_member(config, "offset"), &offset);
        if (!s && offset > 65536) s = UD_SCHEMA;
        options.window_offset = (uint32_t)offset;
        const ud_json *frame = ud_member(config, "frame");
        if (!s && frame) {
            ud_string type = {0};
            s = ud_json_string(ud_member(frame, "type"), &type);
            if (!s) s = ud_json_uint(ud_member(frame, "preceding"), &options.frame_preceding);
            if (!s) s = ud_json_uint(ud_member(frame, "following"), &options.frame_following);
            options.frame_kind = ud_named(type, "rows") ? UD_FRAME_ROWS : ud_named(type, "range") ? UD_FRAME_RANGE : 0;
            if (!options.frame_kind) s = UD_SCHEMA;
        }
        if (!s && !frame && options.window_function >= UD_WINDOW_MOVING_SUM) s = UD_SCHEMA;
        options.average_rounding = UD_ROUND_HALF_EVEN;
        if (!s && ud_member(config, "averageRounding")) {
            ud_string mode = {0}; s = ud_json_string(ud_member(config, "averageRounding"), &mode);
            if (!s) options.average_rounding = ud_named(mode, "exact") ? UD_ROUND_EXACT : UD_ROUND_HALF_EVEN;
        }
        options.kind = UD_OPERATOR_WINDOW;
        options.keys = window_keys;
        options.partition_key_count = (uint32_t)partition;
        options.window_column = (uint32_t)column;
    } else if (!s && kind == AGGREGATE) {
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
    } else if (!s) {
        const ud_json *keys = ud_member(config, kind == SORT ? "keys" : "leftKeys");
        options.key_count = ud_json_size(keys);
        if (ud_json_kind(keys) != UD_JSON_ARRAY || !options.key_count ||
            options.key_count > inputs[0]->columns)
            s = UD_SCHEMA;
        left_keys = !s ? ud_alloc(c->memory, (size_t)options.key_count * sizeof(*left_keys)) : NULL;
        if (!s && !left_keys)
            s = UD_RESOURCE;
        if (!s && kind != SORT) {
            const ud_json *right = ud_member(config, "rightKeys");
            if (ud_json_kind(right) != UD_JSON_ARRAY || ud_json_size(right) != options.key_count)
                s = UD_SCHEMA;
            right_keys =
                !s ? ud_alloc(c->memory, (size_t)options.key_count * sizeof(*right_keys)) : NULL;
            if (!s && !right_keys)
                s = UD_RESOURCE;
            for (uint32_t i = 0; !s && i < options.key_count; i++) {
                uint64_t index;
                s = ud_json_uint(ud_json_at(right, i), &index);
                if (!s && index >= inputs[1]->columns)
                    s = UD_SCHEMA;
                if (!s)
                    right_keys[i] = (uint32_t)index;
            }
        }
        for (uint32_t i = 0; !s && i < options.key_count; i++) {
            uint64_t index;
            s = ud_json_uint(ud_json_at(keys, i), &index);
            if (!s && index >= inputs[0]->columns)
                s = UD_SCHEMA;
            if (!s)
                left_keys[i] = (uint32_t)index;
        }
        if (!s && kind == SORT && ud_member(config, "descending")) {
            const ud_json *v = ud_member(config, "descending");
            const char *p = NULL;
            uint32_t n = 0;
            if (ud_json_kind(v) != UD_JSON_BOOL)
                s = UD_SCHEMA;
            else {
                s = ud_json_text(v, &p, &n);
                options.descending = n == 4 && !memcmp(p, "true", 4);
            }
        }
        options.keys = left_keys;
        options.right_keys = right_keys;
        options.kind = kind == SORT ? UD_OPERATOR_SORT : UD_OPERATOR_JOIN;
    }
    if (!s)
        s = ud_operator_create(c->memory, &options, inputs, out);
    ud_free(window_keys);
    ud_free(left_keys);
    ud_free(right_keys);
    ud_plan_destroy(plan);
    return s;
}
static ud_status describe_operator(void *user, const ud_component_context *c, ud_string text,
                                   const ud_batch *const *inputs, ud_component_plan *out) {
    if (!out || out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    ud_operator *op = NULL;
    ud_status status = parse(c, text, &config);
    if (!status)
        status = create_operator(c, kind, config, inputs, &op);
    if (!status) {
        status = ud_component_operator_plan(op, out);
    }
    ud_status released = ud_operator_destroy(op);
    if (!status)
        status = released;
    ud_json_destroy(config);
    return status;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    ud_status s = parse(c, text, &config);
    if (!s && source_kind(kind)) {
        uint64_t count = 1, id = 0, begin = 0, end = UINT64_MAX;
        if (ud_member(config, "partitionCount"))
            s = ud_json_uint(ud_member(config, "partitionCount"), &count);
        if (!s && ud_member(config, "partitionId"))
            s = ud_json_uint(ud_member(config, "partitionId"), &id);
        if (!s && ud_member(config, "partitionBegin"))
            s = ud_json_uint(ud_member(config, "partitionBegin"), &begin);
        if (!s && ud_member(config, "partitionEnd"))
            s = ud_json_uint(ud_member(config, "partitionEnd"), &end);
        uint64_t maximum = 64;
        const ud_json *policy = ud_member(c->activation->package->document, "policy");
        if (!s && ud_member(policy, "maxPartitions"))
            s = ud_json_uint(ud_member(policy, "maxPartitions"), &maximum);
        if (!s && (!count || count > 64 || count > maximum || id >= count || begin > end))
            s = UD_SCHEMA;
    }
    if (!s && (source_kind(kind)))
        s = schema_config(c, config, out);
    else if (!s && (sink_kind(kind))) { /* No output schemas. */
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
        ud_string policy = {"fail", 4};
        if (!s && ud_member(config, "errorPolicy"))
            s = ud_json_string(ud_member(config, "errorPolicy"), &policy);
        if (!s && !ud_named(policy, "fail") && !ud_named(policy, "ignore") &&
            !ud_named(policy, "redirect"))
            s = UD_SCHEMA;
        int include = 0;
        const ud_json *flag = ud_member(config, "includeInput");
        if (!s && flag) {
            const char *p = NULL;
            uint32_t n = 0;
            if (ud_json_kind(flag) != UD_JSON_BOOL)
                s = UD_SCHEMA;
            else {
                s = ud_json_text(flag, &p, &n);
                include = n == 4 && !memcmp(p, "true", 4);
            }
        }
        if (!s)
            s = ud_error_schema(&c->errors, in[0], include, out + 1);
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
    } else if (!s && (kind == SORT || kind == JOIN || kind == WINDOW || kind == AGGREGATE)) {
        ud_operator *operator= NULL;
        s = create_operator(c, kind, config, in, &operator);
        if (!s)
            s = ud_operator_schema(operator, 0, out);
        ud_status cleanup = ud_operator_destroy(operator);
        if (!s)
            s = cleanup;
    } else if (!s && kind == UNION_ALL)
        s = ud_compute_union_schema(c->memory, in[0], in[1], out);
    else if (!s)
        s = clone_schema(c->memory, in[0], out);
    if (!s && kind == XLSX_SOURCE) {
        ud_string formula = {0}, policy = {0};
        if (ud_member(config, "formulaPolicy"))
            s = ud_json_string(ud_member(config, "formulaPolicy"), &formula);
        if (!s && ud_member(config, "errorPolicy"))
            s = ud_json_string(ud_member(config, "errorPolicy"), &policy);
        if (!s && (ud_named(formula, "redirect") || ud_named(policy, "redirect")) &&
            !(c->connected_outputs & UINT64_C(2)))
            s = UD_SCHEMA;
        ud_field *nullable = ud_alloc(c->memory, out[0]->columns * sizeof(*nullable));
        ud_batch *payload = NULL;
        if (!nullable)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < out[0]->columns; i++) {
            nullable[i] = out[0]->column[i].field;
            nullable[i].nullable = 1;
        }
        if (!s)
            s = ud_batch_create(c->memory, nullable, out[0]->columns, 0, &payload);
        int include = 0;
        const ud_json *flag = ud_member(config, "includeInput");
        if (flag) {
            const char *p = NULL;
            uint32_t n = 0;
            if (ud_json_kind(flag) != UD_JSON_BOOL)
                s = UD_SCHEMA;
            else {
                s = ud_json_text(flag, &p, &n);
                include = n == 4;
            }
        }
        if (!s)
            s = ud_error_schema(&c->errors, payload, include, out + 1);
        ud_batch_destroy(payload);
        ud_free(nullable);
    }
    if (!s && kind == SQL_SOURCE) {
        ud_batch *params = NULL;
        s = parameters(c, config, &params);
        if (!s && params && params->rows != 1)
            s = UD_SCHEMA;
        ud_batch_destroy(params);
    }
    if (!s && source_kind(kind) && ud_member(config, "incremental")) {
        const ud_json *policy = ud_member(config, "incremental");
        ud_string mode;
        s = ud_json_string(ud_member(policy, "mode"), &mode);
        if (!s) {
            char *text = NULL;
            uint32_t length = 0;
            ud_cursor *cursor = NULL;
            s = ud_json_serialize(policy, &text, &length);
            if (!s)
                s = ud_cursor_create(c->memory, out[0], (ud_string){text, length}, &cursor);
            ud_cursor_destroy(cursor);
            ud_free(text);
        }
    }
    if (!s && (kind == SPLIT || kind == MULTICAST))
        for (uint32_t p = 1; !s && p < c->output_count; p++)
            s = clone_schema(c->memory, in[0], out + p);
    if (!s && kind == SPLIT) {
        const ud_json *branches = ud_member(config, "branches");
        uint32_t count = branches ? ud_json_size(branches) : 1;
        for (uint32_t p = 0; !s && p < count; p++) {
            ud_expression *expr = NULL;
            s = bind_expr(c,
                          branches ? ud_member(ud_json_at(branches, p), "condition")
                                   : ud_member(config, "condition"),
                          in[0], &expr);
            if (!s && ud_expression_type(expr) != UD_BOOL)
                s = UD_SCHEMA;
            ud_expression_destroy(expr);
        }
    }
    if (!s && source_kind(kind)) {
        ud_string name = {0};
        s = ud_json_string(ud_member(config, "connection"), &name);
        if (!s)
            s = connection_defined(c, name);
    }
    if (!s && (sink_kind(kind))) {
        ud_string name = {0};
        s = ud_json_string(ud_member(config, "connection"), &name);
        if (!s)
            s = connection_defined(c, name);
    }
    ud_json_destroy(config);
    return s;
}
static ud_status source_raw(builtin *b, ud_batch **out) {
    ud_batch *raw = NULL;
    ud_status status;
    if(b->rest_reader)status=ud_rest_reader_next(b->rest_reader,&raw);
    else
#ifdef UD_WITH_ARROW_FILES
    if(b->columnar_reader)status=ud_columnar_reader_next(b->columnar_reader,&raw);
    else
#endif
    status = b->kind == CSV_SOURCE    ? ud_csv_next(b->csv, &raw)
                       : b->kind == JSON_SOURCE ? ud_json_reader_next(b->reader, &raw)
                       : b->kind == XLSX_SOURCE
                           ? ud_xlsx_reader_next(b->xlsx, &raw, &b->xlsx_rejected)
                           : ud_connection_lease_read_next(b->lease, &raw);
    if (!status && b->actual_schema) {
        status = ud_schema_map(b->context.memory, raw, b->schema, b->schema_policy, out);
        ud_batch_destroy(raw);
    } else
        *out = raw;
    if (!status && b->kind != SQL_SOURCE && b->kind != REST_SOURCE && b->kind != XLSX_SOURCE) {
        ud_batch *identified = NULL;
        status = ud_batch_seal(*out);
        if (!status)
            status = ud_batch_identify(b->context.memory, *out, b->source_scope,
                                       b->source_identified, &identified);
        if (!status)
            b->source_identified += (*out)->rows;
        ud_batch_destroy(*out);
        *out = identified;
    }
    return status;
}
static ud_status source_identity(builtin *b, ud_batch *input, ud_batch **out) {
    ud_status status = ud_batch_seal(input);
    if (!status)
        status = b->cursor && b->cursor->count
                     ? ud_batch_identify_keys(b->context.memory, input, b->source_scope,
                                              b->cursor->keys, b->cursor->count, out)
                     : ud_batch_identify(b->context.memory, input, b->source_scope,
                                         b->source_identified, out);
    if (!status)
        b->source_identified += input->rows;
    ud_batch_destroy(input);
    return status;
}
static ud_status snapshot_store(builtin *b, ud_batch *batch) {
    ud_status status = b->cursor ? ud_cursor_validate(b->cursor, batch) : UD_OK;
    ud_batch *identified = batch;
    if (!status)
        status = ud_batch_seal(identified);
    if (!status)
        status = ud_batch_hash_rows(identified, &b->snapshot_hash);
    if (!status && b->context.execution->journal)
        status = ud_checkpoint_snapshot_write(b->context.activation, b->context.component_id,
                                              b->snapshot_count, identified);
    else if (!status) {
        if (b->snapshot_count == b->snapshot_capacity) {
            uint32_t cap = b->snapshot_capacity ? b->snapshot_capacity * 2 : 32;
            if (cap < b->snapshot_capacity)
                status = UD_RESOURCE;
            ud_buffer_block **items =
                !status ? ud_alloc(b->context.memory, (size_t)cap * sizeof(*items)) : NULL;
            if (!status && !items)
                status = UD_RESOURCE;
            if (!status) {
                if (b->snapshot_count)
                    memcpy(items, b->snapshot, (size_t)b->snapshot_count * sizeof(*items));
                ud_free(b->snapshot);
                b->snapshot = items;
                b->snapshot_capacity = cap;
            }
        }
        if (!status)
            status = ud_buffer_store(b->context.execution->buffers, identified,
                                     b->snapshot + b->snapshot_count);
    }
    if (!status) {
        b->snapshot_count++;
        b->snapshot_rows += identified->rows;
    }
    ud_batch_destroy(identified);
    return status;
}
static ud_status source_sealed(builtin *b, const uint8_t digest[32]) {
    memcpy(b->snapshot_fingerprint, digest, 32);
    if (b->source_partitions > 1)
        UD_TRY(ud_source_version(b->context.activation, b->source_scope, digest));
    if (b->kind == SQL_SOURCE && (!b->cursor || !b->cursor->count)) {
        ud_bytes bytes = {.ctx = b->context.memory};
        ud_status status = ud_put_u64(&bytes, b->source_scope.high);
        if (!status)
            status = ud_put_u64(&bytes, b->source_scope.low);
        if (!status)
            status = ud_bytes_put(&bytes, digest, 32);
        if (!status) {
            uint8_t hash[32];
            ud_hash(bytes.data, bytes.size, hash);
            b->source_scope = (ud_id){0};
            for (uint32_t i = 0; i < 8; i++) {
                b->source_scope.high = (b->source_scope.high << 8) | hash[i];
                b->source_scope.low = (b->source_scope.low << 8) | hash[i + 8];
            }
        }
        ud_bytes_clear(&bytes);
        UD_TRY(status);
    }
    if (b->kind == SQL_SOURCE) {
        ud_connection_lease *grant = NULL;
        UD_TRY(ud_connection_lease_snapshot_grant(b->lease, b->context.activation, &grant));
        ud_status status = ud_connection_lease_release(b->lease);
        if (status) {
            ud_connection_lease_release(grant);
            return status;
        }
        b->lease = grant;
    }
    return UD_OK;
}
static ud_status source_prepare(builtin *b, uint32_t batch_rows) {
    const ud_component_context *c = &b->context;
    ud_status status = UD_OK;
    char namespace_id[33];
    ud_string logical_connection = {0}, logical_table = {0};
    status = ud_json_string(ud_member(b->config, "connection"), &logical_connection);
    if (!status)
        status = ud_assets_bind_source(c->activation, logical_connection, b->lease);
    if (!status && ud_member(b->config, "table"))
        status = ud_json_string(ud_member(b->config, "table"), &logical_table);
    if (!status)
        status = ud_connection_field_namespace(c->activation, logical_connection, logical_table,
                                               namespace_id);
    if (!status && b->kind == SQL_SOURCE && ud_member(b->config, "table")) {
        ud_string table;
        status = ud_json_string(ud_member(b->config, "table"), &table);
        if (!status)
            status = ud_connection_lease_schema(b->lease, table, (ud_string){namespace_id, 32},
                                                &b->actual_schema);
        char *policy = NULL;
        uint32_t length = 0;
        if (!status && ud_member(b->config, "schemaPolicy"))
            status = ud_json_serialize(ud_member(b->config, "schemaPolicy"), &policy, &length);
        else if (!status) {
            policy = ud_strdup_n(c->memory, "{\"policy\":\"strict\"}", 19);
            length = 19;
            if (!policy)
                status = UD_RESOURCE;
        }
        b->schema_policy = (ud_string){policy, length};
        uint32_t diff_length = 0;
        if (!status)
            status = ud_schema_compare(c->memory, b->schema, b->actual_schema, b->schema_policy,
                                       &b->schema_diff, &diff_length);
        ud_json *diff = NULL;
        if (!status)
            status = ud_json_parse(c->memory, b->schema_diff, diff_length, &diff, NULL);
        if (!status) {
            const char *p = NULL;
            uint32_t n = 0;
            status = ud_json_text(ud_member(diff, "compatible"), &p, &n);
            if (!status && (n != 4 || memcmp(p, "true", 4)))
                status = UD_INCOMPATIBLE;
        }
        ud_json_destroy(diff);
        if (b->schema_diff) {
            ud_status recorded =
                ud_schema_report(c->activation, (ud_string){b->schema_diff, diff_length}, status);
            if (!status)
                status = recorded;
        }
    } else if (!status && ud_member(b->config, "schemaPolicy"))
        status = UD_SCHEMA;
    if (!status)
        status = ud_checkpoint_schema(c->activation, c->component_id,
                                      b->actual_schema ? b->actual_schema : b->schema);
    const ud_json *incremental = ud_member(b->config, "incremental");
    if (!status && incremental) {
        ud_string mode;
        status = ud_json_string(ud_member(incremental, "mode"), &mode);
        if (!status) {
            char *json = NULL;
            uint32_t length = 0;
            status = ud_json_serialize(incremental, &json, &length);
            if (!status)
                status =
                    ud_cursor_create(c->memory, b->schema, (ud_string){json, length}, &b->cursor);
            ud_free(json);
        }
        if (!status && b->cursor && (ud_transaction_active(c->activation)))
            status = UD_UNSUPPORTED;
    }
    /* The immutable snapshot and physical batching are part of the replay policy. */
    char *configuration = NULL;
    uint32_t configuration_length = 0;
    if (!status)
        status = ud_json_serialize(b->config, &configuration, &configuration_length);
    if (!status) {
        uint8_t hash[32];
        ud_hash(configuration, configuration_length, hash);
        static const char hex[] = "0123456789abcdef";
        for (uint32_t i = 0; i < 32; i++) {
            b->source_policy[2 * i] = hex[hash[i] >> 4];
            b->source_policy[2 * i + 1] = hex[hash[i] & 15];
        }
        b->source_policy[64] = 0;
    }
    ud_free(configuration);
    ud_bytes identity = {.ctx = c->memory};
    ud_string connection = {0};
    if (!status)
        status = ud_json_string(ud_member(b->config, "connection"), &connection);
    if (!status)
        status = ud_put_string(&identity, c->execution->options.tenant.data,
                               c->execution->options.tenant.length);
    ud_string resource = {0};
    if (!status)
        status = ud_connection_lease_resource(b->lease, &resource);
    if (!status)
        status = ud_put_string(&identity, resource.data, resource.length);
    if (!status && b->kind == SQL_SOURCE) {
        ud_string sql;
        status = ud_json_string(ud_member(b->config, "sql"), &sql);
        if (!status)
            status = ud_put_string(&identity, logical_table.length ? logical_table.data : sql.data,
                                   logical_table.length ? logical_table.length : sql.length);
    }
    if (!status && b->path)
        status = ud_connection_lease_authorize(b->lease, UD_ACCESS_READ);
    if (!status && b->path) {
        uint64_t started = ud_now_ms();
        status = ud_file_hash(c->memory, b->path, b->file_fingerprint);
        b->file_hash_ms += ud_now_ms() - started;
        if (!status)
            status = ud_bytes_put(&identity, b->file_fingerprint, 32);
    }
    if (!status && b->cursor && b->cursor->count)
        status = ud_bytes_put(&identity, b->cursor->fingerprint, 32);
    if (!status) {
        uint8_t hash[32];
        ud_hash(identity.data, identity.size, hash);
        for (uint32_t i = 0; i < 8; i++) {
            b->source_scope.high = (b->source_scope.high << 8) | hash[i];
            b->source_scope.low = (b->source_scope.low << 8) | hash[i + 8];
        }
    }
    ud_bytes_clear(&identity);
    b->materialized = b->cursor != NULL || b->kind == SQL_SOURCE || b->kind==REST_SOURCE;
    ud_sha256_init(&b->snapshot_hash);
    if (!status) {
        uint8_t schema[32];
        status = ud_schema_fingerprint(b->schema, schema);
        if (!status)
            ud_sha256_update(&b->snapshot_hash, schema, 32);
    }
    if (!status && b->materialized) {
        char *info = NULL;
        uint32_t length = 0;
        ud_status saved = ud_checkpoint_snapshot_info(
            c->activation, c->component_id, (ud_string){b->source_policy, 64}, &info, &length);
        if (saved == UD_OK) {
            ud_json *header = NULL;
            status = ud_json_parse(c->memory, info, length, &header, NULL);
            if (!status)
                status = ud_json_uint(ud_member(header, "batches"), &b->snapshot_count);
            if (!status)
                status = ud_json_uint(ud_member(header, "rows"), &b->snapshot_rows);
            if (!status) {
                ud_string hash;
                status = ud_json_string(ud_member(header, "fingerprint"), &hash);
                uint8_t fingerprint[32];
                if (!status && hash.length != 64)
                    status = UD_CORRUPT;
                for (uint32_t j = 0; !status && j < 32; j++) {
                    unsigned value = 0;
                    for (uint32_t k = 0; k < 2; k++) {
                        char ch = hash.data[j * 2 + k];
                        unsigned digit = ch >= '0' && ch <= '9'   ? ch - '0'
                                         : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
                                                                  : 16;
                        if (digit > 15)
                            status = UD_CORRUPT;
                        value = value * 16 + digit;
                    }
                    fingerprint[j] = (uint8_t)value;
                }
                if (!status)
                    status = source_sealed(b, fingerprint);
            }
            ud_json_destroy(header);
            ud_free(info);
            return status;
        }
        if (saved != UD_END)
            return saved;
    }
    if (!status && b->kind == SQL_SOURCE) {
        ud_string sql;
        status = ud_json_string(ud_member(b->config, "sql"), &sql);
        ud_read_options options = {
            sizeof(options), UD_ABI_VERSION, batch_rows, sql, b->parameters, 0};
        if (!status)
            status = ud_connection_lease_read_open(b->lease, &options,
                                                   b->actual_schema ? b->actual_schema : b->schema);
    }
    if (!status && b->cursor && b->cursor->count) {
        ud_operator_options options = {.struct_size = sizeof(options),
                                       .abi_version = UD_ABI_VERSION,
                                       .kind = UD_OPERATOR_SORT,
                                       .max_batch_rows = batch_rows,
                                       .keys = b->cursor->keys,
                                       .key_count = b->cursor->count,
                                       .buffers = c->execution->buffers,
                                       .executor = c->execution->executor,
                                       .partition_count = 1,
                                       .check = operator_check,
                                       .user = c->activation};
        const ud_batch *inputs[] = {b->schema};
        status = ud_operator_create(c->memory, &options, inputs, &b->source_sort);
    }
    if (!status && b->materialized) {
        for (;;) {
            ud_batch *input = NULL;
            status = ud_activation_check(c->activation);
            if (!status)
                status = ud_connection_lease_authorize(b->lease, UD_ACCESS_READ);
            if (!status)
                status = source_raw(b, &input);
            if (status == UD_END) {
                status = UD_OK;
                break;
            }
            if (!status && b->source_sort) {
                status = ud_operator_push(b->source_sort, 0, input);
                ud_batch_destroy(input);
            } else if (!status)
                status = snapshot_store(b, input);
            else
                ud_batch_destroy(input);
            if (status)
                break;
        }
        if (!status && b->kind == SQL_SOURCE)
            status = ud_connection_lease_read_close(b->lease);
        if (!status && b->source_sort) {
            status = ud_operator_finish_input(b->source_sort, 0);
            while (!status) {
                ud_batch *input = NULL;
                status = ud_operator_pull(b->source_sort, &input, NULL);
                if (status == UD_END) {
                    status = UD_OK;
                    break;
                }
                if (!status)
                    status = snapshot_store(b, input);
                else
                    ud_batch_destroy(input);
            }
        }
        if (!status && b->path) {
            uint8_t hash[32];
            uint64_t started = ud_now_ms();
            status = ud_connection_lease_authorize(b->lease, UD_ACCESS_READ);
            if (!status)
                status = ud_file_hash(c->memory, b->path, hash);
            b->file_hash_ms += ud_now_ms() - started;
            if (!status && memcmp(hash, b->file_fingerprint, 32))
                status = UD_INCOMPATIBLE;
            if (!status)
                status = ud_checkpoint_source(c->activation, b->path);
        }
        if (!status && c->execution->journal) {
            ud_batch *trailing = NULL;
            ud_status saved = ud_checkpoint_snapshot_read(c->activation, c->component_id,
                                                          b->snapshot_count, &trailing);
            ud_batch_destroy(trailing);
            if (saved != UD_END)
                status = saved ? saved : UD_INCOMPATIBLE;
        }
        uint8_t digest[32];
        char fingerprint[65];
        if (!status) {
            ud_sha256_finish(&b->snapshot_hash, digest);
            static const char hex[] = "0123456789abcdef";
            for (uint32_t j = 0; j < 32; j++) {
                fingerprint[2 * j] = hex[digest[j] >> 4];
                fingerprint[2 * j + 1] = hex[digest[j] & 15];
            }
            fingerprint[64] = 0;
            status = source_sealed(b, digest);
        }
        if (!status) {
            char header[384];
            int n = snprintf(
                header, sizeof(header),
                "{\"policy\":\"%s\",\"fingerprint\":\"%s\",\"batches\":%llu,\"rows\":%llu}",
                b->source_policy, fingerprint, (unsigned long long)b->snapshot_count,
                (unsigned long long)b->snapshot_rows);
            status = ud_checkpoint_snapshot_seal(c->activation, c->component_id,
                                                 (ud_string){header, (uint32_t)n});
        }
    }
    if (!status && b->cursor)
        ud_cursor_reset(b->cursor);
    return status;
}
static ud_status source_partition(builtin *, ud_batch *, ud_batch **);
static ud_status source_next(builtin *b, ud_batch **out) {
    *out = NULL;
    ud_batch *input = NULL;
    ud_status status = UD_OK;
    uint64_t started = ud_now_ms();
    if (b->materialized) {
        if (b->snapshot_at == b->snapshot_count)
            return UD_END;
        if (b->context.execution->journal) {
            status = ud_checkpoint_snapshot_read(b->context.activation, b->context.component_id,
                                                 b->snapshot_at, &input);
            if (status == UD_END)
                status = UD_CORRUPT;
        } else {
            ud_buffer_view *view = NULL;
            ud_buffer_block *block = b->snapshot[b->snapshot_at];
            status = ud_buffer_read(block, &view);
            if (!status) {
                input = (ud_batch *)ud_buffer_view_batch(view);
                status = ud_batch_retain(input);
            }
            ud_buffer_view_release(view);
            ud_status closed = ud_buffer_release(block);
            b->snapshot[b->snapshot_at] = NULL;
            if (!status)
                status = closed;
        }
        if (!status)
            b->snapshot_at++;
    } else
        status = source_raw(b, &input);
    if (!status && (b->kind == SQL_SOURCE || b->kind==REST_SOURCE)) {
        ud_batch *identified = NULL;
        status = source_identity(b, input, &identified);
        input = identified;
    }
    b->snapshot_read_ms += ud_now_ms() - started;
    if (!status) {
        ud_batch *selected = NULL;
        status = source_partition(b, input, &selected);
        input = selected;
    }
    if (!status && b->cursor) {
        status = ud_cursor_filter(b->cursor, input, out);
        ud_batch_destroy(input);
    } else
        *out = input;
    return status;
}
static ud_status confirm(void *ptr) {
    builtin *b = ptr;
    if (!b->cursor)
        return UD_OK;
    uint64_t before = b->cursor->confirmed;
    UD_TRY(ud_cursor_confirm(b->cursor));
    char *json = NULL;
    uint32_t length = 0;
    ud_status status = ud_cursor_serialize(b->cursor, &json, &length);
    ud_bytes record = {.ctx = b->context.memory};
    char source[33], snapshot[65], schema[65], head[512];
    ud_id_format(b->source_scope, source);
    uint8_t schema_hash[32];
    if (!status)
        status = ud_schema_fingerprint(b->schema, schema_hash);
    if (!status) {
        static const char hex[] = "0123456789abcdef";
        for (uint32_t i = 0; i < 32; i++) {
            snapshot[i * 2] = hex[b->snapshot_fingerprint[i] >> 4];
            snapshot[i * 2 + 1] = hex[b->snapshot_fingerprint[i] & 15];
            schema[i * 2] = hex[schema_hash[i] >> 4];
            schema[i * 2 + 1] = hex[schema_hash[i] & 15];
        }
        snapshot[64] = schema[64] = 0;
        int n = snprintf(head, sizeof(head),
                         "{\"version\":%u,\"sourceIdentity\":\"%s\","
                         "\"snapshotFingerprint\":\"%s\",\"schemaFingerprint\":\"%s\","
                         "\"partition\":{\"id\":%u,\"count\":%u,\"begin\":%llu,\"end\":%llu},"
                         "\"cursor\":",
                         UD_CONFIG_VERSION, source, snapshot, schema, b->source_partition,
                         b->source_partitions, (unsigned long long)b->source_begin,
                         (unsigned long long)b->source_end);
        status = ud_bytes_put(&record, head, (size_t)n);
    }
    if (!status)
        status = ud_bytes_put(&record, json, length);
    if (!status)
        status = ud_bytes_put(&record, "}", 1);
    if (!status)
        status = ud_checkpoint_cursor(b->context.activation, b->context.component_id,
                                      (ud_string){(char *)record.data, (uint32_t)record.size});
    ud_bytes_clear(&record);
    ud_free(json);
    if (status) {
        b->cursor->confirmed = before;
        return UD_COMMIT_UNKNOWN;
    }
    return UD_OK;
}
static ud_status diagnostics(void *ptr, char **out, uint32_t *length) {
    builtin *b = ptr;
    if (b->kind == XLSX_SOURCE)
        return ud_xlsx_reader_metadata(b->xlsx, out, length);
    ud_bytes json = {.ctx = b->context.memory};
    char *cursor = NULL;
    uint32_t cursor_length = 0;
    if (b->cursor)
        b->cursor->commit_unknown = b->context.activation->pipeline_result == UD_COMMIT_UNKNOWN;
    ud_status status =
        b->cursor ? ud_cursor_diagnostics(b->cursor, &cursor, &cursor_length) : UD_OK;
    char head[256];
    int n = snprintf(head, sizeof(head),
                     "{\"snapshotRows\":%llu,\"snapshotBatches\":%llu,\"snapshotReadMs\":%llu,"
                     "\"sourceHashMs\":%llu,\"materialized\":%s",
                     (unsigned long long)b->snapshot_rows, (unsigned long long)b->snapshot_count,
                     (unsigned long long)b->snapshot_read_ms, (unsigned long long)b->file_hash_ms,
                     b->materialized ? "true" : "false");
    if (!status)
        status = ud_bytes_put(&json, head, (size_t)n);
    if (!status && cursor) {
        status = ud_bytes_put(&json, ",\"cursor\":", 10);
        if (!status)
            status = ud_bytes_put(&json, cursor, cursor_length);
    }
    if (!status && b->schema_diff) {
        status = ud_bytes_put(&json, ",\"schema\":", 10);
        if (!status)
            status = ud_bytes_put(&json, b->schema_diff, strlen(b->schema_diff));
    }
    if (!status)
        status = ud_bytes_put(&json, "}", 1);
    if (!status) {
        *out = ud_strdup_n(b->context.memory, (char *)json.data, json.size);
        *length = (uint32_t)json.size;
        if (!*out)
            status = UD_RESOURCE;
    }
    ud_free(cursor);
    ud_bytes_clear(&json);
    return status;
}
static void dispose(void *ptr, ud_status result) {
    (void)result;
    builtin *b = ptr;
    if (!b)
        return;
    ud_xlsx_reader_close(b->xlsx);
#ifdef UD_WITH_ARROW_FILES
    if(b->columnar_reader)ud_columnar_reader_close(b->columnar_reader);
    if(b->columnar_writer)ud_columnar_writer_close(b->columnar_writer);
#endif
    ud_rest_reader_close(b->rest_reader);
    if (b->xlsx_writer)
        ud_xlsx_writer_close(b->xlsx_writer);
    ud_csv_close(b->csv);
    ud_json_reader_close(b->reader);
    if (b->writer)
        (void)ud_json_writer_close(b->writer);
    if (b->csv_writer)
        (void)ud_csv_writer_close(b->csv_writer);
    if (b->temporary && b->temporary_owned && !b->published)
        (void)ud_remove_file(b->context.memory, b->temporary);
    if (b->stage_bytes)
        ud_buffer_release_external(b->context.execution->buffers, b->stage_bytes);
    ud_unlock(b->lock);
    (void)ud_connection_lease_release(b->lease);
    for (uint32_t i = 0; i < b->expression_count; i++)
        ud_expression_destroy(b->expressions[i]);
    ud_free(b->expressions);
    ud_batch_destroy(b->schema);
    ud_batch_destroy(b->errors);
    for (uint32_t i = 0; i < b->input_count; i++)
        ud_batch_destroy(b->inputs[i]);
    ud_free(b->inputs);
    (void)ud_operator_destroy(b->operator);
    ud_free(b->fields);
    ud_free(b->path);
    ud_free(b->temporary);
    ud_free(b->job);
    ud_free(b->keys);
    ud_free(b->write_indices);
    ud_rule_delivery_destroy(b->delivery);
    ud_cursor_destroy(b->cursor);
    ud_batch_destroy(b->actual_schema);
    ud_batch_destroy(b->write_schema);
    ud_batch_destroy(b->parameters);
    ud_operator_destroy(b->source_sort);
    for (uint64_t i = b->snapshot_at; b->snapshot && i < b->snapshot_count; i++)
        ud_buffer_release(b->snapshot[i]);
    ud_free(b->snapshot);
    ud_free(b->schema_diff);
    ud_free((void *)b->schema_policy.data);
    ud_json_destroy(b->config);
    ud_free(b);
}
/* Destination binding uses the same native comparison and cast as Source.
 * Storage fields belong to the target namespace; row identity remains the source's. */
static ud_status sink_schema(builtin *b) {
    ud_string connection = {0}, table = {0};
    UD_TRY(ud_json_string(ud_member(b->config, "connection"), &connection));
    UD_TRY(ud_json_string(ud_member(b->config, "table"), &table));
    char scope[33];
    UD_TRY(ud_connection_field_namespace(b->context.activation, connection, table, scope));
    ud_batch *actual = NULL;
    ud_status status = ud_connection_lease_schema(b->lease, table, (ud_string){scope, 32}, &actual);
    if (!status)
        status = ud_checkpoint_schema(b->context.activation, b->context.component_id, actual);
    char *policy = NULL;
    uint32_t length = 0;
    ud_json *options = NULL;
    if (!status && ud_member(b->config, "schemaPolicy"))
        status = ud_json_serialize(ud_member(b->config, "schemaPolicy"), &policy, &length);
    else if (!status) {
        const char *default_policy = "{\"policy\":\"compatible\"}";
        length = (uint32_t)strlen(default_policy);
        policy = ud_strdup_n(b->context.memory, default_policy, length);
        if (!policy)
            status = UD_RESOURCE;
    }
    b->schema_policy = (ud_string){policy, length};
    if (!status)
        status = ud_json_parse(b->context.memory, policy, length, &options, NULL);
    ud_field *target =
                 !status ? ud_alloc(b->context.memory, (size_t)b->columns * sizeof(*target)) : NULL,
             *input =
                 !status ? ud_alloc(b->context.memory, (size_t)b->columns * sizeof(*input)) : NULL;
    if (!status && (!target || !input))
        status = UD_RESOURCE;
    const ud_json *mapping = ud_member(options, "mapping");
    for (uint32_t i = 0; !status && i < b->columns; i++) {
        ud_string name = {b->fields[i].name, b->fields[i].name_length};
        for (uint32_t m = 0; !status && m < ud_json_size(mapping); m++) {
            ud_string from = {0}, to = {0};
            status = ud_json_string(ud_member(ud_json_at(mapping, m), "actual"), &from);
            if (!status)
                status = ud_json_string(ud_member(ud_json_at(mapping, m), "expected"), &to);
            if (!status && ud_same(from, name))
                name = to;
        }
        int at = -1;
        for (uint32_t j = 0; !status && j < actual->columns; j++) {
            const ud_field *f = &actual->column[j].field;
            char id[33];
            ud_id_format(f->identity, id);
            if ((name.length == f->name_length && !memcmp(name.data, f->name, name.length)) ||
                (name.length == 32 && !memcmp(name.data, id, 32)))
                at = (int)j;
        }
        if (!status && at < 0)
            status = UD_INCOMPATIBLE;
        if (!status) {
            target[i] = actual->column[at].field;
            input[i] = b->fields[i];
            input[i].identity = target[i].identity;
            input[i].lineage = NULL;
            input[i].lineage_count = 0;
            input[i].key_order = target[i].key_order;
        }
    }
    if (!status)
        status = ud_batch_create(b->context.memory, target, b->columns, 0, &b->actual_schema);
    if (!status)
        status = ud_batch_create(b->context.memory, input, b->columns, 0, &b->write_schema);
    uint32_t diff_length = 0;
    if (!status)
        status = ud_schema_compare(b->context.memory, b->actual_schema, b->write_schema,
                                   b->schema_policy, &b->schema_diff, &diff_length);
    ud_json *diff = NULL;
    if (!status)
        status = ud_json_parse(b->context.memory, b->schema_diff, diff_length, &diff, NULL);
    if (!status) {
        const char *p = NULL;
        uint32_t n = 0;
        status = ud_json_text(ud_member(diff, "compatible"), &p, &n);
        if (!status && (n != 4 || memcmp(p, "true", 4)))
            status = UD_INCOMPATIBLE;
    }
    if (b->schema_diff) {
        ud_status recorded = ud_schema_report(b->context.activation,
                                              (ud_string){b->schema_diff, diff_length}, status);
        if (!status)
            status = recorded;
    }
    ud_free(target);
    ud_free(input);
    ud_json_destroy(diff);
    ud_json_destroy(options);
    ud_batch_destroy(actual);
    return status;
}
static ud_status sink_batch(builtin *b, const ud_batch *input, ud_batch **out, ud_batch **before) {
    *out = NULL; *before = NULL;
    ud_context *ctx = b->context.memory;
    ud_batch *selected = NULL, *old = NULL, *bound = NULL;
    ud_status status = ud_batch_create(ctx, b->schema->schema->fields, b->columns, input->rows, &selected);
    if (!status && b->before_column != UINT32_MAX)
        status = ud_batch_create(ctx, b->schema->schema->fields, b->columns, input->rows, &old);
    ud_value *values = ud_alloc(ctx, b->columns * sizeof(*values));
    uint32_t *indices = ud_alloc(ctx, b->columns * sizeof(*indices));
    if (!values || !indices) status = UD_RESOURCE;
    for (uint32_t col = 0; !status && col < b->columns; col++) indices[col] = col;
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        ud_row_info info; status = ud_batch_row_info(input, row, &info);
        if (!status && b->mode == UD_WRITE_CHANGESET && !info.operation) continue;
        if (!status && b->mode == UD_WRITE_CHANGESET && info.operation > UD_CHANGE_UPSERT) status = UD_SCHEMA;
        if (!status) status = ud_batch_append_projected(selected, input, row, b->write_indices);
        if (!status && old) {
            ud_value previous; status = ud_batch_get(input, row, b->before_column, &previous);
            if (!status && previous.is_null && info.operation != UD_CHANGE_INSERT && info.operation != UD_CHANGE_UPSERT) status = UD_SCHEMA;
            for (uint32_t col = 0; !status && col < b->columns; col++) {
                if (previous.is_null) status = ud_batch_get(input, row, b->write_indices[col], values + col);
                else values[col] = previous.children[col];
            }
            if (!status) status = ud_batch_append(old, values, b->columns);
            if (!status) status = ud_batch_set_row_info(old, old->rows - 1, &info);
        }
    }
    if (!status) status = ud_batch_seal(selected);
    if (!status && old) status = ud_batch_seal(old);
    if (!status) status = ud_batch_project(ctx, selected, indices, b->write_schema->schema->fields, b->columns, &bound);
    if (!status) status = ud_schema_map(ctx, bound, b->actual_schema, b->schema_policy, out);
    ud_batch_destroy(bound); bound = NULL;
    if (!status && old) status = ud_batch_project(ctx, old, indices, b->write_schema->schema->fields, b->columns, &bound);
    if (!status && old) status = ud_schema_map(ctx, bound, b->actual_schema, b->schema_policy, before);
    ud_batch_destroy(bound); ud_batch_destroy(selected); ud_batch_destroy(old); ud_free(values); ud_free(indices);
    if (status) { ud_batch_destroy(*out); *out = NULL; ud_batch_destroy(*before); *before = NULL; }
    return status;
}
static ud_status prepare(void *user, const ud_component_context *c, ud_string text,
                         const ud_batch *const *in, const ud_batch *const *out, void **instance) {
    *instance = NULL;
    builtin *b = ud_alloc(c->memory, sizeof(*b));
    if (!b)
        return UD_RESOURCE;
    b->kind = (int)(intptr_t)user;
    b->context = *c;
    b->before_column = UINT32_MAX;
    ud_status s = parse(c, text, &b->config);
    int kind = b->kind;
    uint32_t inputs = c->input_count;
    b->input_count = inputs;
    b->inputs = ud_alloc(c->memory, (inputs + 1) * sizeof(*b->inputs));
    if (!b->inputs)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < inputs; i++)
        s = clone_schema(c->memory, in[i], b->inputs + i);
    if (!s)
        s = clone_schema(c->memory, sink_kind(kind) ? in[0] : out[0], &b->schema);
    if (!s && (kind == CAST || kind == XLSX_SOURCE))
        s = clone_schema(c->memory, out[1], &b->errors);
    if (!s && (kind == SQL_SINK || kind==REST_SINK)) {
        ud_string mode = {0}; s = ud_json_string(ud_member(b->config, "mode"), &mode);
        b->mode = ud_named(mode, "append") ? UD_WRITE_APPEND : ud_named(mode, "upsert") ? UD_WRITE_UPSERT
                : ud_named(mode, "delete") ? UD_WRITE_DELETE : ud_named(mode, "changeset") ? UD_WRITE_CHANGESET : 0;
        if (!s && !b->mode) s = UD_SCHEMA;
        ud_string before = {"before", 6};
        if (!s && ud_member(b->config, "beforeField")) s = ud_json_string(ud_member(b->config, "beforeField"), &before);
        uint32_t n = in[0]->columns;
        b->write_indices = ud_alloc(c->memory, n * sizeof(*b->write_indices));
        ud_field *fields = ud_alloc(c->memory, n * sizeof(*fields));
        if (!b->write_indices || !fields) s = UD_RESOURCE;
        uint32_t count = 0;
        for (uint32_t i = 0; !s && i < n; i++) {
            ud_field f = in[0]->column[i].field;
            if (b->mode == UD_WRITE_CHANGESET && f.name_length == before.length && !memcmp(f.name, before.data, before.length)) {
                if (f.type != UD_STRUCT) s = UD_SCHEMA; else b->before_column = i;
            } else { b->write_indices[count] = i; fields[count++] = f; }
        }
        if (!s && b->before_column != UINT32_MAX) {
            const ud_field *f = &in[0]->column[b->before_column].field;
            if (f->child_count != count) s = UD_SCHEMA;
            for (uint32_t i = 0; !s && i < count; i++)
                if (!ud_field_matches(f->children + i, fields + i, UD_FIELD_LAYOUT)) s = UD_SCHEMA;
        }
        if (!s) { ud_batch_destroy(b->schema); b->schema = NULL; s = ud_batch_create(c->memory, fields, count, 0, &b->schema); }
        ud_free(fields);
    }
    if (!s) {
        b->columns = b->schema->columns;
        b->fields = ud_alloc(c->memory, b->columns * sizeof(*b->fields));
        if (!b->fields)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < b->columns; i++)
            s = ud_batch_field(b->schema, i, b->fields + i);
    }
    if (!s && (source_kind(kind) || sink_kind(kind))) {
        ud_string connection = {0};
        s = ud_json_string(ud_member(b->config, "connection"), &connection);
        uint64_t caps = source_kind(kind) ? UD_CONNECT_READ : UD_CONNECT_APPEND;
        if (kind == SQL_SINK || kind==REST_SINK) caps = b->mode == UD_WRITE_APPEND ? UD_CONNECT_APPEND
                : b->mode == UD_WRITE_UPSERT ? UD_CONNECT_UPSERT : b->mode == UD_WRITE_DELETE ? UD_CONNECT_DELETE : UD_CONNECT_CHANGESET;
        if (kind == SQL_SINK)
            caps |= UD_CONNECT_METADATA|UD_CONNECT_RECEIPTS;
        if(!s)s=kind==REST_SOURCE||kind==REST_SINK?ud_acquire_http_connection(c->activation,connection,&b->lease):ud_activation_acquire_connection(c->activation,connection,caps,&b->lease);
    }
    uint64_t batch = c->max_batch_rows;
    if (!s && ud_member(b->config, "batchSize"))
        s = ud_json_uint(ud_member(b->config, "batchSize"), &batch);
    if (!s && (!batch || batch > c->max_batch_rows))
        s = UD_SCHEMA;
    if (!s && (file_source(kind) || file_sink(kind))) {
        ud_string path = {0};
        s = ud_connection_lease_path(b->lease, &path);
        if (!s) {
            b->path = ud_strdup_n(c->memory, path.data, path.length);
            if (!b->path)
                s = UD_RESOURCE;
        }
    }
    if (!s && source_kind(kind)) {
        uint64_t count = 1, id = 0, begin = 0, end = UINT64_MAX;
        if (ud_member(b->config, "partitionCount"))
            s = ud_json_uint(ud_member(b->config, "partitionCount"), &count);
        if (!s && ud_member(b->config, "partitionId"))
            s = ud_json_uint(ud_member(b->config, "partitionId"), &id);
        if (!s && ud_member(b->config, "partitionBegin"))
            s = ud_json_uint(ud_member(b->config, "partitionBegin"), &begin);
        if (!s && ud_member(b->config, "partitionEnd"))
            s = ud_json_uint(ud_member(b->config, "partitionEnd"), &end);
        if (!s && (!count || count > 64 || id >= count || begin > end))
            s = UD_SCHEMA;
        b->source_partitions = (uint32_t)count;
        b->source_partition = (uint32_t)id;
        b->source_begin = begin;
        b->source_end = end;
    }
    if(!s&&kind==REST_SOURCE)s=ud_rest_reader_open(c,b->lease,b->config,b->schema,&b->rest_reader);
    if(!s&&kind==REST_SINK)s=ud_connection_lease_bind_rest(b->lease,c,b->config);
    if (!s && (file_source(kind)))
        s = ud_checkpoint_source(c->activation, b->path);
    if (!s && (file_source(kind)))
        s = ud_connection_lease_authorize(b->lease, UD_ACCESS_READ);
    if (!s && kind == CSV_SOURCE) {
        ud_string null = {"\\N", 2};
        if (ud_member(b->config, "nullMarker"))
            s = ud_json_string(ud_member(b->config, "nullMarker"), &null);
        ud_csv_options o = {sizeof(o), UD_ABI_VERSION, (uint32_t)batch, null.data, null.length};
        if (!s)
            s = ud_csv_open(c->memory, b->path, b->fields, b->columns, &o, &b->csv);
    }
    if (!s && kind == XLSX_SOURCE) {
        char *configuration = NULL;
        uint32_t length = 0;
        s = ud_json_serialize(b->config, &configuration, &length);
        ud_xlsx_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .batch_size = (uint32_t)batch,
                                   .configuration = {configuration, length},
                                   .buffers = c->execution->buffers,
                                   .check = operator_check,
                                   .user = c->activation};
        if (!s)
            s = ud_xlsx_reader_open(c->memory, b->path, b->fields, b->columns, &options, &b->xlsx);
        ud_free(configuration);
    }
#ifdef UD_WITH_ARROW_FILES
    if(!s&&(kind==PARQUET_SOURCE||kind==IPC_SOURCE||kind==IPC_STREAM_SOURCE)){
        ud_columnar_file_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,.batch_rows=(uint32_t)batch,.format=kind==PARQUET_SOURCE?UD_FILE_PARQUET:kind==IPC_SOURCE?UD_FILE_ARROW_IPC:UD_FILE_ARROW_STREAM,.buffers=c->execution->buffers,.check=operator_check,.user=c->activation};
        s=ud_columnar_reader_open(c->memory,b->path,b->schema,&options,&b->columnar_reader);
    }
#endif
    if (!s && kind == JSON_SOURCE) {
        ud_json_options o = {sizeof(o), UD_CONFIG_VERSION, (uint32_t)batch, 1048576,
                             UD_ROUND_EXACT};
        s = ud_json_reader_open(c->memory, b->path, b->fields, b->columns, &o, &b->reader);
    }
    if (!s && (file_sink(kind))) {
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
        if (!s && (kind==CSV_SINK||kind==JSON_SINK))
            s = kind == CSV_SINK ? ud_csv_writer_open(c->memory, b->temporary, b->fields,
                                                      b->columns, "\\N", &b->csv_writer)
                                 : ud_json_writer_open(c->memory, b->temporary, b->fields,
                                                       b->columns, 1048576, &b->writer);
        if (!s && kind == XLSX_SINK) {
            char *configuration = NULL;
            uint32_t length = 0;
            s = ud_json_serialize(b->config, &configuration, &length);
            ud_xlsx_options options = {.struct_size = sizeof(options),
                                       .abi_version = UD_ABI_VERSION,
                                       .batch_size = c->max_batch_rows,
                                       .configuration = {configuration, length},
                                       .buffers = c->execution->buffers,
                                       .check = operator_check,
                                       .user = c->activation,
                                       .stage_bytes = &b->stage_bytes};
            if (!s)
                s = ud_xlsx_writer_open(c->memory, b->temporary, b->fields, b->columns, &options,
                                        &b->xlsx_writer);
            ud_free(configuration);
        }
    }
#ifdef UD_WITH_ARROW_FILES
    if(!s&&(kind==PARQUET_SINK||kind==IPC_SINK||kind==IPC_STREAM_SINK)){
        ud_columnar_file_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,.batch_rows=c->max_batch_rows,.format=kind==PARQUET_SINK?UD_FILE_PARQUET:kind==IPC_SINK?UD_FILE_ARROW_IPC:UD_FILE_ARROW_STREAM,.buffers=c->execution->buffers,.check=operator_check,.user=c->activation,.stage_bytes=&b->stage_bytes};
        s=ud_columnar_writer_open(c->memory,b->temporary,b->schema,&options,&b->columnar_writer);
    }
#endif
    if (!s && kind == SQL_SINK && ud_member(b->config, "provisionReceipts")) {
        const char *flag = NULL;
        uint32_t n = 0;
        s = ud_json_text(ud_member(b->config, "provisionReceipts"), &flag, &n);
        if (!s && ud_json_kind(ud_member(b->config, "provisionReceipts")) != UD_JSON_BOOL)
            s = UD_SCHEMA;
        if (!s && n == 4)
            s = ud_connection_lease_provision(b->lease);
    }
    if (!s && (kind == SQL_SINK || kind==REST_SINK)) {
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
        if(!s&&kind==SQL_SINK)s=sink_schema(b);
        else if(!s&&kind==REST_SINK){s=clone_schema(c->memory,b->schema,&b->actual_schema);if(!s)s=clone_schema(c->memory,b->schema,&b->write_schema);b->schema_policy=(ud_string){ud_strdup_n(c->memory,"{\"policy\":\"strict\"}",19),19};if(!b->schema_policy.data)s=UD_RESOURCE;}
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
        const ud_json *branches = ud_member(b->config, "branches");
        b->expression_count =
            kind == SPLIT ? (branches ? ud_json_size(branches) : 1) : ud_json_size(columns);
        b->expressions = ud_alloc(c->memory, b->expression_count * sizeof(*b->expressions));
        if (!b->expressions)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < b->expression_count; i++)
            s = bind_expr(c,
                          kind == SPLIT
                              ? (branches ? ud_member(ud_json_at(branches, i), "condition")
                                          : ud_member(b->config, "condition"))
                              : ud_member(ud_json_at(columns, i), "expression"),
                          in[0], b->expressions + i);
    }
    if (!s && (kind == SORT || kind == JOIN || kind == WINDOW || kind == AGGREGATE))
        s = create_operator(c, kind, b->config, in, &b->operator);
    if (!s && kind == SQL_SOURCE)
        s = parameters(c, b->config, &b->parameters);
    if (!s && source_kind(kind))
        s = source_prepare(b, (uint32_t)batch);
    if (s) {
        dispose(b, s);
        return s;
    }
    *instance = b;
    return UD_OK;
}
static ud_status cast_rows(builtin *b, const ud_batch *input) {
    int policy;
    UD_TRY(ud_error_policy(b->config, "errorPolicy", UD_ERROR_FAIL, &policy));
    uint64_t rounding = 0;
    if (ud_member(b->config, "rounding"))
        UD_TRY(ud_json_uint(ud_member(b->config, "rounding"), &rounding));
    ud_batch *accepted = NULL, *errors = NULL;
    ud_status status =
        ud_batch_create(b->context.memory, b->fields, b->columns, input->rows, &accepted);
    if (!status && policy == UD_ERROR_REDIRECT)
        status = ud_batch_create(b->context.memory, b->errors->schema->fields, b->errors->columns,
                                 input->rows, &errors);
    ud_value *values = ud_alloc(b->context.memory, (size_t)b->columns * sizeof(*values));
    if (!status && !values)
        status = UD_RESOURCE;
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        ud_status conversion = UD_OK;
        uint32_t bad = 0;
        status = ud_activation_check(b->context.activation);
        for (uint32_t col = 0; !status && !conversion && col < b->columns; col++) {
            ud_value original;
            status = ud_batch_get(input, row, col, &original);
            if (!status) {
                conversion = ud_cast_value(b->context.memory, &input->column[col].field, &original,
                                           b->fields + col, (int32_t)rounding, values + col, 0);
                bad = col;
            }
        }
        if (!status && !conversion) {
            status = ud_batch_append(accepted, values, b->columns);
            if (!status)
                status = ud_batch_copy_row_info(accepted, accepted->rows - 1, input, row);
        } else if (!status && (policy == UD_ERROR_FAIL || conversion == UD_RESOURCE ||
                               conversion == UD_CANCELLED || conversion == UD_TIMEOUT ||
                               conversion == UD_AUTH))
            status = conversion;
        else if (!status && policy == UD_ERROR_REDIRECT) {
            status = ud_error_append(&b->context.errors, errors, input, row, (ud_string){"cast", 4}, 1,
                                     input->column[bad].field.identity, conversion, policy);
            if (!status)
                ud_task_rows(b->context.activation, 0, 0, 1);
        }
        for (uint32_t col = 0; col < b->columns; col++)
            ud_clear_value(values + col);
    }
    if (!status && accepted->rows)
        status = b->context.emit(b->context.emit_user, 0, accepted);
    if (!status && errors && errors->rows)
        status = b->context.emit(b->context.emit_user, 1, errors);
    ud_free(values);
    ud_batch_destroy(accepted);
    ud_batch_destroy(errors);
    return status;
}
static ud_status source_partition(builtin *b, ud_batch *input, ud_batch **out) {
    if (b->source_partitions == 1 && !b->source_begin && b->source_end == UINT64_MAX) {
        b->source_position += input->rows;
        *out = input;
        return UD_OK;
    }
    ud_status status = ud_batch_create(b->context.memory, b->fields, b->columns, input->rows, out);
    for (uint32_t row = 0; !status && row < input->rows; row++, b->source_position++) {
        uint64_t position = b->source_position;
        if (position < b->source_begin || position >= b->source_end ||
            position % b->source_partitions != b->source_partition)
            continue;
        status = ud_batch_append_from(*out, input, row);
    }
    ud_batch_destroy(input);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
static ud_status transformed(builtin *b, const ud_batch *in) {
    int kind = b->kind;
    ud_status s = UD_OK;
    uint32_t output_count = b->context.output_count;
    ud_batch **output = ud_alloc(b->context.memory, (output_count + 1) * sizeof(*output));
    if (!output)
        return UD_RESOURCE;
    if (kind == PASS || kind == MULTICAST) {
        for (uint32_t p = 0; !s && p < output_count; p++)
            s = b->context.emit(b->context.emit_user, p, (ud_batch *)in);
        ud_free(output);
        return s;
    }
    if (kind == UNION_ALL) {
        uint32_t *indices = ud_alloc(b->context.memory, b->columns * sizeof(*indices));
        if (!indices) {
            ud_free(output);
            return UD_RESOURCE;
        }
        for (uint32_t j = 0; j < b->columns; j++)
            indices[j] = j;
        s = ud_batch_project(b->context.memory, in, indices, b->fields, b->columns, output);
        ud_free(indices);
        if (!s)
            s = b->context.emit(b->context.emit_user, 0, output[0]);
        ud_batch_destroy(output[0]);
        ud_free(output);
        return s;
    }
    if (kind == CAST) {
        ud_free(output);
        return cast_rows(b, in);
    }
    {
        for (uint32_t p = 0; !s && p < output_count; p++)
            s = ud_batch_create(b->context.memory, b->fields, b->columns, in->rows, output + p);
        ud_value *values = ud_alloc(b->context.memory, b->columns * sizeof(*values));
        if (!values && !s)
            s = UD_RESOURCE;
        for (uint32_t row = 0; !s && row < in->rows; row++) {
            s = ud_activation_check(b->context.activation);
            for (uint32_t j = 0; !s && j < in->columns; j++)
                s = ud_batch_get(in, row, j, values + j);
            uint32_t port = 0;
            if (kind == SPLIT) {
                port = b->expression_count;
                for (uint32_t p = 0; !s && p < b->expression_count; p++) {
                    ud_value value = {0};
                    s = ud_expression_evaluate(b->expressions[p], in, row, &value);
                    if (!s && !value.is_null && value.integer) {
                        port = p;
                        break;
                    }
                }
            } else
                for (uint32_t j = 0; !s && j < b->expression_count; j++)
                    s = ud_expression_evaluate(b->expressions[j], in, row,
                                               values + in->columns + j);
            if (!s)
                s = ud_batch_append(output[port], values, b->columns);
            if (!s)
                s = ud_batch_copy_row_info(output[port], output[port]->rows - 1, in, row);
        }
        ud_free(values);
    }
    for (uint32_t i = 0; !s && i < output_count; i++)
        if (output[i] && output[i]->rows)
            s = b->context.emit(b->context.emit_user, i, output[i]);
    for (uint32_t p = 0; p < output_count; p++)
        ud_batch_destroy(output[p]);
    ud_free(output);
    return s;
}
static ud_status blocking(builtin *b) {
    ud_batch *output = NULL;
    ud_status s = ud_operator_pull(b->operator, & output, NULL);
    if (!s)
        s = b->context.emit(b->context.emit_user, 0, output);
    ud_batch_destroy(output);
    return s == UD_BUSY ? UD_OK : s ? s : UD_MORE;
}
ud_status ud_pipeline_write(const ud_component_context *c, ud_connection_lease *lease,
                            const ud_write_options *options, const ud_batch *input) {
    ud_write_options o = *options;
    uint8_t digest[32];
    ud_status s = ud_connection_lease_digest(lease, &o, input, digest);
    size_t length = o.job.length + o.batch_id.length + 8;
    char *identity = ud_alloc(c->memory, length);
    if (!identity)
        return UD_RESOURCE;
    snprintf(identity, length, "%.*s/%.*s", (int)o.job.length, o.job.data, (int)o.batch_id.length,
             o.batch_id.data);
    ud_string key = {identity, (uint32_t)strlen(identity)};
    int replaying = ud_checkpoint_batch_seen(c->activation, key);
    if (!s)
        s = ud_checkpoint_batch(c->activation, key, digest, 0);
    if (!s && replaying) {
        ud_commit_receipt receipt = {0};
        s = ud_connection_lease_receipt(lease, o.job, o.batch_id, &receipt);
        if (!s && receipt.committed &&
            (memcmp(receipt.digest, digest, 32) ||
             !ud_same(receipt.source_position, o.source_position)))
            s = UD_CORRUPT;
        ud_free((void *)receipt.source_position.data);
    }
    struct ud_rule_delivery *delivery = NULL;
    if (!s)
        s = ud_rule_stage(c->activation, input, &delivery);
    if (!s)
        s = ud_connection_lease_write(lease, &o, input);
    if (!s) {
        if (!ud_transaction_rows(c->activation, input->rows, ud_batch_error_rows(input)))
            ud_target_rows(c->activation, input->rows, ud_batch_error_rows(input), 0);
        s = ud_transaction_active(c->activation) ? ud_transaction_delivery(c->activation, input)
                                                 : ud_rule_delivered(c->activation, delivery, 0);
        if (!s)
            s = ud_checkpoint_batch(c->activation, key, digest, 1);
        if (s)
            s = UD_COMMIT_UNKNOWN;
        else if (c->execution->journal) {
            ud_target_rows(c->activation, input->rows, ud_batch_error_rows(input), 1);
            s = ud_rule_delivered(c->activation, delivery, 1);
        }
    }
    if (s == UD_COMMIT_UNKNOWN)
        ud_rule_delivered(c->activation, delivery, 2);
    ud_rule_delivery_destroy(delivery);
    ud_free(identity);
    return s;
}
static ud_status sink_write(builtin *b, const ud_batch *input, const ud_batch *before) {
    if (!input->rows) return UD_OK;
    ud_string table = {0};
    UD_TRY(ud_json_string(ud_member(b->config, "table"), &table));
    char id[32], position[96];
    snprintf(id, sizeof(id), "%llu", (unsigned long long)b->number);
    ud_row_info last = {0};
    if (input->rows)
        UD_TRY(ud_batch_row_info(input, input->rows - 1, &last));
    if (last.id.high || last.id.low) {
        char row_id[33];
        ud_id_format(last.id, row_id);
        snprintf(position, sizeof(position), "row:%s/count:%u", row_id, input->rows);
    } else
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
    o.current_column = UINT32_MAX; o.before = before;
    int32_t *operations = b->mode == UD_WRITE_CHANGESET ? ud_alloc(b->context.memory, input->rows * sizeof(*operations)) : NULL;
    ud_status status = b->mode == UD_WRITE_CHANGESET && !operations ? UD_RESOURCE : UD_OK;
    for (uint32_t row = 0; !status && operations && row < input->rows; row++) {
        ud_row_info info; status = ud_batch_row_info(input, row, &info); operations[row] = (int32_t)info.operation;
    }
    o.operations = operations;
    if (!status) status = ud_pipeline_write(&b->context, b->lease, &o, input);
    ud_free(operations);
    if (!status)
        b->number++;
    return status;
}

static ud_status step(void *ptr, const ud_component_event *event) {
    builtin *b = ptr;
    int kind = b->kind;
    UD_TRY(ud_activation_check(b->context.activation));
    if (source_kind(kind)) {
        UD_TRY(ud_connection_lease_authorize(b->lease, UD_ACCESS_READ));
        ud_batch *batch = NULL;
        ud_status s = source_next(b, &batch);
        if (!s) {
            if (!s && kind == XLSX_SOURCE && b->xlsx_rejected) {
                ud_batch *errors = NULL;
                s = ud_batch_create(b->context.memory, b->errors->schema->fields,
                                    b->errors->columns, batch->rows, &errors);
                for (uint32_t row = 0; !s && row < batch->rows; row++)
                    s = ud_error_append(&b->context.errors, errors, batch, row,
                                        (ud_string){"xlsx.cell", 9}, 1, (ud_id){0}, UD_PARSE,
                                        UD_ERROR_REDIRECT);
                if (!s)
                    s = ud_batch_seal(errors);
                if (!s)
                    s = b->context.emit(b->context.emit_user, 1, errors);
                ud_batch_destroy(errors);
            } else if (!s && batch->rows)
                s = b->context.emit(b->context.emit_user, 0, batch);
            else if (!s)
                s = UD_MORE;
        }
        ud_batch_destroy(batch);
        return s;
    }
    if (kind == JOIN) {
        if (event->kind == UD_COMPONENT_DATA)
            UD_TRY(ud_operator_push(b->operator, event->input, event->batch));
        else if (event->kind == UD_COMPONENT_INPUT_END)
            UD_TRY(ud_operator_finish_input(b->operator, event->input));
        return blocking(b);
    }
    if (event->kind == UD_COMPONENT_INPUT_END)
        return b->operator? ud_operator_finish_input(b->operator, event->input) : UD_OK;
    if (event->kind == UD_COMPONENT_POLL)
        return kind == SORT || kind == JOIN || kind == WINDOW || kind == AGGREGATE ? blocking(b)
                                                                                   : UD_END;
    if (file_sink(kind)) {
        UD_TRY(ud_connection_lease_authorize(b->lease, UD_ACCESS_WRITE));
        ud_status status;
#ifdef UD_WITH_ARROW_FILES
        if(b->columnar_writer)status=ud_columnar_writer_append(b->columnar_writer,event->batch);
        else
#endif
        status = kind == CSV_SINK    ? ud_csv_writer_append(b->csv_writer, event->batch)
                           : kind == XLSX_SINK ? ud_xlsx_writer_append(b->xlsx_writer, event->batch)
                                               : ud_json_writer_append(b->writer, event->batch);
        if (!status)
            status = ud_rule_stage(b->context.activation, event->batch, &b->delivery);
        if (!status) {
            b->written_rows += event->batch->rows;
            b->written_errors += ud_batch_error_rows(event->batch);
        }
        return status;
    }
    if (kind == SQL_SINK || kind==REST_SINK) {
        ud_batch *mapped = NULL, *before = NULL;
        ud_status status = sink_batch(b, event->batch, &mapped, &before);
        if (!status) status = sink_write(b, mapped, before);
        ud_batch_destroy(mapped); ud_batch_destroy(before);
        return status;
    }
    if (kind == SORT || kind == JOIN || kind == WINDOW || kind == AGGREGATE)
        return ud_operator_push(b->operator, event->input, event->batch);
    return transformed(b, event->batch);
}
static ud_status checkpoint_state(void*ptr,ud_batch**out){builtin*b=ptr;if(b->operator)return ud_operator_checkpoint(b->operator,out);if(b->kind!=SQL_SINK&&b->kind!=REST_SINK)return UD_UNSUPPORTED;ud_field field={.name="nextBatch",.name_length=9,.type=UD_INT64};ud_value value={.type=UD_INT64,.integer=(int64_t)b->number};if(b->number>INT64_MAX)return UD_RESOURCE;UD_TRY(ud_batch_create(b->context.memory,&field,1,1,out));ud_status status=ud_batch_append(*out,&value,1);if(!status)status=ud_batch_seal(*out);if(status){ud_batch_destroy(*out);*out=NULL;}return status;}
static ud_status restore_state(void*ptr,const ud_batch*state){builtin*b=ptr;if(b->operator)return ud_operator_restore(&b->operator,state);if((b->kind!=SQL_SINK&&b->kind!=REST_SINK)||state->columns!=1||state->rows!=1)return UD_CORRUPT;ud_value value;UD_TRY(ud_batch_get(state,0,0,&value));if(value.type!=UD_INT64||value.is_null||value.integer<0)return UD_CORRUPT;b->number=(uint64_t)value.integer;return UD_OK;}
static ud_status finish(void *ptr) {
    builtin *b = ptr;
    if (file_source(b->kind)) {
        uint8_t hash[32];
        uint64_t started = ud_now_ms();
        ud_status status = ud_file_hash(b->context.memory, b->path, hash);
        b->file_hash_ms += ud_now_ms() - started;
        if (!status && memcmp(hash, b->file_fingerprint, 32))
            status = UD_INCOMPATIBLE;
        return status ? status : ud_checkpoint_source(b->context.activation, b->path);
    }
    if (!file_sink(b->kind))
        return UD_OK;
    ud_status s;
#ifdef UD_WITH_ARROW_FILES
    if(b->columnar_writer){s=ud_columnar_writer_close(b->columnar_writer);b->columnar_writer=NULL;}
    else
#endif
    s = b->kind == CSV_SINK    ? ud_csv_writer_close(b->csv_writer)
                  : b->kind == XLSX_SINK ? ud_xlsx_writer_close(b->xlsx_writer)
                                         : ud_json_writer_close(b->writer);
    b->csv_writer = NULL;
    b->writer = NULL;
    b->xlsx_writer = NULL;
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
        ud_target_rows(b->context.activation, b->written_rows, b->written_errors, 0);
        s = ud_rule_delivered(b->context.activation, b->delivery, 0);
        if (!s)
            s = ud_checkpoint_file_done(b->context.activation, b->path);
        if (s)
            s = UD_COMMIT_UNKNOWN;
        else if (b->context.execution->journal) {
            ud_target_rows(b->context.activation, b->written_rows, b->written_errors, 1);
            s = ud_rule_delivered(b->context.activation, b->delivery, 1);
        }
    }
    if (s == UD_COMMIT_UNKNOWN)
        ud_rule_delivered(b->context.activation, b->delivery, 2);
    return s;
}
static ud_status sql_task(void *user, const ud_component_context *c, ud_string text) {
    (void)user;
    ud_json *config = NULL;
    UD_TRY(parse(c, text, &config));
    static const char *keys[] = {"connection", "statement", "parameters"};
    ud_status s = ud_json_keys(config, keys, 3);
    ud_string id = {0}, statement = {0};
    if (!s)
        s = ud_json_string(ud_member(config, "connection"), &id);
    if (!s)
        s = ud_json_string(ud_member(config, "statement"), &statement);
    ud_connection_lease *lease = NULL;
    if (!s)
        s = ud_activation_acquire_connection(c->activation, id, UD_CONNECT_EXECUTE_SQL, &lease);
    ud_batch *params = NULL;
    if (!s)
        s = parameters(c, config, &params);
    if (!s)
        s = ud_connection_lease_execute_command(
            lease,
            &(ud_read_options){.struct_size = sizeof(ud_read_options),
                               .abi_version = UD_ABI_VERSION,
                               .sql = statement,
                               .parameters = params},
            NULL);
    ud_batch_destroy(params);
    ud_status clean = ud_connection_lease_release(lease);
    ud_json_destroy(config);
    return s ? s : clean;
}
static ud_status sql_task_configure(void *user, const ud_component_context *c, ud_string text,
                                    const ud_batch *const *inputs, ud_batch **outputs) {
    (void)user;
    (void)inputs;
    (void)outputs;
    ud_json *config = NULL;
    ud_status status = parse(c, text, &config);
    static const char *keys[] = {"connection", "statement", "parameters"};
    if (!status)
        status = ud_json_keys(config, keys, 3);
    ud_string id = {0}, statement = {0};
    if (!status)
        status = ud_json_string(ud_member(config, "connection"), &id);
    /* A loop-variable string is typed at definition binding and materialized at
     * Activation binding. Nonempty SQL is checked by invoke before any lease. */
    if (!status && ud_json_kind(ud_member(config, "statement")) != UD_JSON_STRING)
        status = UD_SCHEMA;
    if (!status)
        status = ud_json_text(ud_member(config, "statement"), &statement.data, &statement.length);
    if (!status)
        status = connection_defined(c, id);
    ud_batch *params = NULL;
    if (!status)
        status = parameters(c, config, &params);
    ud_batch_destroy(params);
    ud_json_destroy(config);
    return status;
}
static const char *configuration_schema(int kind) {
    switch (kind) {
    case REST_SOURCE:return "{ \"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},\"schema\":{\"type\":\"object\"},\"table\":{\"type\":\"string\"},\"batchSize\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":65536},\"caConnection\":{\"type\":\"string\"},\"url\":{\"type\":\"string\"},\"rowsField\":{\"type\":\"string\",\"default\":\"rows\"},\"cursorField\":{\"type\":\"string\",\"default\":\"nextCursor\"},\"cursorParameter\":{\"type\":\"string\",\"default\":\"cursor\"},\"maxPages\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":1000000,\"default\":10000},\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000},\"maxResponseBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864},\"maxAttempts\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":8},\"retryDelayMs\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":60000},\"incremental\":{\"type\":\"object\"},\"ordering\":{\"type\":\"object\"}},\"required\":[\"connection\",\"schema\"],\"additionalProperties\":false}";
    case REST_SINK:return "{ \"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},\"table\":{\"type\":\"string\",\"default\":\"rest\"},\"mode\":{\"type\":\"string\",\"enum\":[\"append\",\"upsert\",\"delete\",\"changeset\"],\"default\":\"append\"},\"keys\":{\"type\":\"array\",\"items\":{\"type\":\"integer\",\"minimum\":0},\"default\":[]},\"beforeField\":{\"type\":\"string\"},\"receiptUrl\":{\"type\":\"string\",\"minLength\":1},\"caConnection\":{\"type\":\"string\"},\"url\":{\"type\":\"string\"},\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000},\"maxResponseBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864},\"maxAttempts\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":8},\"retryDelayMs\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":60000}},\"required\":[\"connection\",\"receiptUrl\"],\"additionalProperties\":false}";
    case PARQUET_SOURCE:
    case IPC_SOURCE:
    case IPC_STREAM_SOURCE:
    case CSV_SOURCE:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"schema\":{\"type\":\"object\"},\"batchSize\":{\"type\":\"integer\"},"
               "\"partitionCount\":{\"type\":\"integer\"},\"partitionId\":{\"type\":\"integer\"},"
               "\"partitionBegin\":{\"type\":\"integer\"},\"partitionEnd\":{\"type\":\"integer\"},"
               "\"incremental\":{\"type\":\"object\"},\"schemaPolicy\":{\"type\":\"object\"},"
               "\"table\":{\"type\":\"string\"},\"nullMarker\":{\"type\":\"string\"}},\"required\":"
               "[\"connection\",\"schema\"],\"additionalProperties\":false}";
    case JSON_SOURCE:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"schema\":{\"type\":\"object\"},\"batchSize\":{\"type\":\"integer\"},"
               "\"partitionCount\":{\"type\":\"integer\"},\"partitionId\":{\"type\":\"integer\"},"
               "\"partitionBegin\":{\"type\":\"integer\"},\"partitionEnd\":{\"type\":\"integer\"},"
               "\"incremental\":{\"type\":\"object\"},\"schemaPolicy\":{\"type\":\"object\"},"
               "\"table\":{\"type\":\"string\"}},\"required\":[\"connection\",\"schema\"],"
               "\"additionalProperties\":false}";
    case SQL_SOURCE:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"schema\":{\"type\":\"object\"},\"batchSize\":{\"type\":\"integer\"},"
               "\"partitionCount\":{\"type\":\"integer\"},\"partitionId\":{\"type\":\"integer\"},"
               "\"partitionBegin\":{\"type\":\"integer\"},\"partitionEnd\":{\"type\":\"integer\"},"
               "\"incremental\":{\"type\":\"object\"},\"schemaPolicy\":{\"type\":\"object\"},"
               "\"table\":{\"type\":\"string\"},\"sql\":{\"type\":\"string\"},\"parameters\":{\"type\":\"object\"},\"ordering\":{\"type\":\"object\"}},\"required\":["
               "\"connection\",\"schema\",\"sql\"],\"additionalProperties\":false}";
    case PARQUET_SINK:
    case IPC_SINK:
    case IPC_STREAM_SINK:
    case CSV_SINK:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"}},"
               "\"required\":[\"connection\"],\"additionalProperties\":false}";
    case JSON_SINK:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"}},"
               "\"required\":[\"connection\"],\"additionalProperties\":false}";
    case SQL_SINK:
        return "{\"type\":\"object\",\"properties\":{\"connection\":{\"type\":\"string\"},"
               "\"table\":{\"type\":\"string\"},\"mode\":{\"type\":\"string\",\"enum\":[\"append\",\"upsert\",\"delete\",\"changeset\"]},\"beforeField\":{\"type\":\"string\"},\"keys\":{\"type\":"
               "\"array\"},\"schemaPolicy\":{\"type\":\"object\"},\"provisionReceipts\":{\"type\":\"boolean\"}},\"required\":[\"connection\","
               "\"table\",\"mode\"],\"additionalProperties\":false}";
    case CAST:
        return "{\"type\":\"object\",\"properties\":{\"schema\":{\"type\":\"object\"},\"rounding\":"
               "{\"type\":\"integer\"},\"errorPolicy\":{\"type\":\"string\"},\"includeInput\":{"
               "\"type\":\"boolean\"}},\"required\":[\"schema\"],\"additionalProperties\":false}";
    case DERIVE:
        return "{\"type\":\"object\",\"properties\":{\"columns\":{\"type\":\"array\"}},"
               "\"required\":[\"columns\"],\"additionalProperties\":false}";
    case SPLIT:
        return "{\"type\":\"object\",\"properties\":{\"condition\":{\"type\":\"object\"},"
               "\"branches\":{\"type\":\"array\"},\"default\":{\"type\":\"string\"}},"
               "\"additionalProperties\":false}";
    case SORT: case JOIN: case AGGREGATE: case WINDOW: {
        ud_string schema = {0};
        int k = kind == SORT ? UD_OPERATOR_SORT : kind == JOIN ? UD_OPERATOR_JOIN :
                kind == AGGREGATE ? UD_OPERATOR_AGGREGATE : UD_OPERATOR_WINDOW;
        return ud_operator_configuration_schema(k, &schema) ? NULL : schema.data;
    }
    case MULTICAST:
        return "{\"type\":\"object\",\"properties\":{\"outputs\":{\"type\":\"array\"}},"
               "\"additionalProperties\":false}";
    case UNION_ALL:
        return "{\"type\":\"object\",\"properties\":{\"inputs\":{\"type\":\"array\"}},"
               "\"additionalProperties\":false}";
    case XLSX_SOURCE:
        return "{\"type\":\"object\",\"required\":[\"connection\",\"schema\"],\"properties\":{"
               "\"connection\":{\"type\":\"string\"},\"schema\":{\"type\":\"object\"},\"sheet\":{"
               "\"type\":\"string\"},\"range\":{\"type\":\"string\"},\"header\":{\"type\":"
               "\"boolean\"},\"datePolicy\":{\"type\":\"string\"},\"formulaPolicy\":{\"type\":"
               "\"string\"},\"errorPolicy\":{\"type\":\"string\"},\"includeInput\":{\"type\":"
               "\"boolean\"},\"batchSize\":{\"type\":\"integer\"},\"maxCompressedBytes\":{\"type\":"
               "\"integer\"},\"maxDecompressedBytes\":{\"type\":\"integer\"},\"maxEntries\":{"
               "\"type\":\"integer\"},\"maxCells\":{\"type\":\"integer\"},\"maxStringBytes\":{"
               "\"type\":\"integer\"},\"ordering\":{\"type\":\"object\"}},\"additionalProperties\":"
               "false}";
    case XLSX_SINK:
        return "{\"type\":\"object\",\"required\":[\"connection\"],\"properties\":{\"connection\":{"
               "\"type\":\"string\"},\"sheet\":{\"type\":\"string\"},\"header\":{\"type\":"
               "\"boolean\"},\"datePolicy\":{\"type\":\"string\"},\"maxCompressedBytes\":{\"type\":"
               "\"integer\"}},\"additionalProperties\":false}";
    default:
        return "{\"type\":\"object\",\"additionalProperties\":false}";
    }
}
static uint64_t builtin_demand(void *ptr) {
    return ud_operator_input_demand(((builtin *)ptr)->operator);
}
static ud_status builtin_ports(void *user, const ud_component_context *c, ud_string text,
                               ud_component_ports *ports) {
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    UD_TRY(parse(c, text, &config));
    const ud_json *names = ud_member(config, kind == MULTICAST   ? "outputs"
                                             : kind == UNION_ALL ? "inputs"
                                                                 : "branches");
    uint32_t n = names ? ud_json_size(names) : 2;
    ud_status s = UD_OK;
    if (kind == SPLIT)
        n = names ? ud_json_size(names) + 1 : 2;
    if (n < 2 || n > 64 || (names && ud_json_kind(names) != UD_JSON_ARRAY))
        s = UD_SCHEMA;
    ports->input_count = kind == UNION_ALL ? n : 1;
    ports->output_count = kind == UNION_ALL ? 1 : n;
    ports->inputs =
        !s ? ud_alloc(c->memory, (ports->input_count + 1) * sizeof(*ports->inputs)) : NULL;
    ports->outputs =
        !s ? ud_alloc(c->memory, (ports->output_count + 1) * sizeof(*ports->outputs)) : NULL;
    if (!s && (!ports->inputs || !ports->outputs))
        s = UD_RESOURCE;
    for (uint32_t direction = 0; !s && direction < 2; direction++)
        for (uint32_t i = 0; !s && i < (direction ? ports->output_count : ports->input_count);
             i++) {
            ud_component_port *port = (direction ? ports->outputs : ports->inputs) + i;
            ud_string name = {0};
            char fallback[32];
            const ud_json *value = NULL;
            if (names && ((kind == UNION_ALL && !direction) || (kind == MULTICAST && direction)))
                value = ud_json_at(names, i);
            if (names && kind == SPLIT && direction) {
                if (i + 1 < ports->output_count) {
                    const ud_json *branch = ud_json_at(names, i);
                    static const char *allow[] = {"id", "condition"};
                    s = ud_json_keys(branch, allow, 2);
                    value = ud_member(branch, "id");
                } else
                    value = ud_member(config, "default");
            }
            if (!s && value)
                s = ud_json_string(value, &name);
            else if (!s) {
                int length =
                    snprintf(fallback, sizeof(fallback), "%s%u", direction ? "output" : "input", i);
                name = (ud_string){fallback, (uint32_t)length};
            }
            if (!s) {
                port->name =
                    (ud_string){ud_strdup_n(c->memory, name.data, name.length), name.length};
                port->flags = UD_PORT_REQUIRED;
                if (!port->name.data)
                    s = UD_RESOURCE;
            }
        }
    ud_json_destroy(config);
    return s;
}
static ud_status describe_source(void *user, const ud_component_context *c, ud_string text,
                                 const ud_batch *const *in, ud_component_plan *plan) {
    (void)user;
    (void)in;
    ud_json *config = NULL;
    UD_TRY(parse(c, text, &config));
    const ud_json *order = ud_member(config, "ordering");
    ud_status s = UD_OK;
    if (order) {
        static const char *allow[] = {"keys", "descending", "stable"};
        s = ud_json_keys(order, allow, 3);
        const ud_json *keys = ud_member(order, "keys");
        uint32_t n = ud_json_size(keys);
        if (!s && (ud_json_kind(keys) != UD_JSON_ARRAY || !n || n > 16))
            s = UD_SCHEMA;
        int descending = 0, stable = 0;
        const ud_json *d = ud_member(order, "descending"), *st = ud_member(order, "stable");
        const char *p = NULL;
        uint32_t length = 0;
        if (!s && d) {
            if (ud_json_kind(d) != UD_JSON_BOOL)
                s = UD_SCHEMA;
            else {
                s = ud_json_text(d, &p, &length);
                descending = length == 4;
            }
        }
        if (!s && st) {
            if (ud_json_kind(st) != UD_JSON_BOOL)
                s = UD_SCHEMA;
            else {
                s = ud_json_text(st, &p, &length);
                stable = length == 4;
            }
        }
        const ud_batch *schema = c->output_schemas[0];
        for (uint32_t k = 0; !s && k < n; k++) {
            uint64_t index = 0;
            s = ud_json_uint(ud_json_at(keys, k), &index);
            if (!s && index >= schema->columns)
                s = UD_SCHEMA;
            if (!s)
                plan->sort.keys[k] =
                    (ud_sort_key){schema->column[index].field.identity, descending, !descending};
        }
        if (!s) {
            plan->ordering = UD_ORDER_GLOBAL;
            plan->sort.count = n;
            plan->sort.stable = stable;
        }
    }
    ud_json_destroy(config);
    return s;
}
ud_status ud_builtin_register(ud_registry *registry) {
    UD_TRY(ud_relational_register(registry));
    UD_TRY(ud_cdc_register(registry));
    UD_TRY(ud_advanced_register(registry));
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
                   {"cast", CAST, 1, 2, -1, 1},
                   {"derive", DERIVE, 1, 1, -1, -1},
                   {"split", SPLIT, 1, 2, -1, -1},
                   {"multicast", MULTICAST, 1, 2, 0, -1},
                   {"unionAll", UNION_ALL, 2, 1, -1, -1},
                   {"sort", SORT, 1, 1, -1, -1},
                   {"join", JOIN, 2, 1, -1, -1},
                   {"aggregate", AGGREGATE, 1, 1, -1, -1},
                   {"window", WINDOW, 1, 1, -1, -1},
                   {"file.xlsx.source", XLSX_SOURCE, 0, 2, -1, 1},
                   {"file.xlsx.destination", XLSX_SINK, 1, 0, -1, -1}
#ifdef UD_WITH_HTTP
                   ,{"rest.source",REST_SOURCE,0,1,-1,-1},{"rest.destination",REST_SINK,1,0,-1,-1}
#endif
#ifdef UD_WITH_ARROW_FILES
                   ,{"file.parquet.source",PARQUET_SOURCE,0,1,-1,-1},{"file.parquet.destination",PARQUET_SINK,1,0,-1,-1},
                   {"file.arrowIpc.source",IPC_SOURCE,0,1,-1,-1},{"file.arrowIpc.destination",IPC_SINK,1,0,-1,-1},
                   {"file.arrowStream.source",IPC_STREAM_SOURCE,0,1,-1,-1},{"file.arrowStream.destination",IPC_STREAM_SINK,1,0,-1,-1}
#endif
                   };
    for (uint32_t i = 0; i < sizeof(entries) / sizeof(*entries); i++) {
        int kind = entries[i].kind;
        int category = source_kind(kind) ? UD_COMPONENT_SOURCE
                       : sink_kind(kind) ? UD_COMPONENT_DESTINATION
                                         : UD_COMPONENT_TRANSFORM;
        ud_component_descriptor d = {
            .struct_size = sizeof(d),
            .abi_version = UD_ABI_VERSION,
            .id = {entries[i].id, (uint32_t)strlen(entries[i].id)},
            .name = {entries[i].id, (uint32_t)strlen(entries[i].id)},
            .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
            .configuration_schema = {configuration_schema(kind),
                                     (uint32_t)strlen(configuration_schema(kind))},
            .category = category,
            .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_REPLAY_SAFE,
            .inputs = entries[i].inputs,
            .outputs = entries[i].outputs,
            .synchronous_input = entries[i].sync,
            .error_output = entries[i].error,
            .optional_outputs = kind == CAST || kind == XLSX_SOURCE ? UINT64_C(2) : 0,
            .user = (void *)(intptr_t)kind,
            .configure = configure,
            .prepare = prepare,
            .step = step,
            .finish = finish,
            .confirm = source_kind(kind) ? confirm : NULL,
            .diagnostics = source_kind(kind) || kind == SQL_SINK ? diagnostics : NULL,
            .dispose = dispose};
        if (kind == JOIN)
            d.input_demand = builtin_demand;
        if (kind == SPLIT || kind == MULTICAST || kind == UNION_ALL)
            d.bind_ports = builtin_ports;
        if (source_kind(kind))
            d.describe_plan = describe_source;
        if (kind == SQL_SOURCE)
            d.capabilities |= UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING;
        if (kind == SORT || kind == JOIN || kind == AGGREGATE || kind == WINDOW)
            d.capabilities |= UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING |
                              UD_COMPONENT_CLONEABLE | UD_COMPONENT_MERGEABLE;
        if (kind == SORT || kind == JOIN || kind == AGGREGATE || kind == WINDOW)
            d.describe_plan = describe_operator;
        if(kind==SQL_SINK||kind==REST_SINK){d.effects=UD_EFFECT_EXTERNAL_WRITE|UD_EFFECT_TRANSACTIONAL|UD_EFFECT_RECEIPTED|UD_EFFECT_COMMIT_UNKNOWN;d.state_version=1;d.checkpoint=checkpoint_state;d.restore=restore_state;d.capabilities|=UD_COMPONENT_STATEFUL;}
        else if(source_kind(kind))d.effects=UD_EFFECT_EXTERNAL_READ;
        else if(file_sink(kind))d.effects=UD_EFFECT_EXTERNAL_WRITE|UD_EFFECT_COMMIT_UNKNOWN;
        if(kind==SORT||kind==JOIN||kind==AGGREGATE||kind==WINDOW){d.state_version=1;d.checkpoint=checkpoint_state;d.restore=restore_state;}
        UD_TRY(ud_registry_register(registry, &d));
    }
    ud_component_descriptor task = {.struct_size = sizeof(task),
                                    .abi_version = UD_ABI_VERSION,
                                    .id = {"sql.execute", 11},
                                    .name = {"Execute SQL", 11},
                                    .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
                                    .configuration_schema = {"{\"type\":\"object\"}", 17},
                                    .category = UD_COMPONENT_TASK,
                                    .synchronous_input = -1,
                                    .error_output = -1,
                                    .configure = sql_task_configure,
                                    .capabilities = UD_COMPONENT_THREAD_SAFE,
                                    .invoke = sql_task};
    UD_TRY(ud_registry_register(registry, &task));
    UD_TRY(ud_transforms_register(registry));
    UD_TRY(ud_quality_component_register(registry));
    UD_TRY(ud_cache_components_register(registry));
    UD_TRY(ud_enumerators_register(registry));
    UD_TRY(ud_filesystem_register(registry));
    UD_TRY(ud_process_register(registry));
    UD_TRY(ud_http_register(registry));
    return ud_profile_register(registry);
}
