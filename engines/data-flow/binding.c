#include "builtins_internal.h"
static ud_status error_schema(const ud_component_context *context, const ud_json *config,
                              const ud_batch *input, uint32_t port, ud_batch **output) {
    int policy, include = 0;
    UD_TRY(ud_error_policy(config, "errorPolicy", UD_ERROR_FAIL, &policy));
    const ud_json *flag = ud_member(config, "includeInput");
    if (flag) {
        const char *value;
        uint32_t length;
        if (ud_json_kind(flag) != UD_JSON_BOOL)
            return UD_SCHEMA;
        UD_TRY(ud_json_text(flag, &value, &length));
        include = length == 4;
    }
    if (policy == UD_ERROR_REDIRECT && !(context->connected_outputs & (UINT64_C(1) << port)))
        return UD_SCHEMA;
    return ud_error_schema(&context->errors, input, include, output);
}
ud_status ud_builtin_clone_schema(ud_context *ctx, const ud_batch *in, ud_batch **out) {
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

ud_status ud_builtin_schema_config(const ud_component_context *c, const ud_json *config,
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

ud_status ud_builtin_parameters(const ud_component_context *c, const ud_json *config,
                                ud_batch **out) {
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

ud_status ud_builtin_parse(const ud_component_context *c, ud_string config, ud_json **out) {
    return ud_json_parse(c->memory, config.data, config.length, out, NULL);
}

ud_status ud_builtin_bind_expr(const ud_component_context *c, const ud_json *value,
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

ud_status ud_builtin_connection_defined(const ud_component_context *ctx, ud_string id) {
    const ud_json *definition = NULL;
    int project_scope = 0;
    return ud_connection_definition(ctx->activation, id, &definition, &project_scope);
}

ud_status ud_builtin_operator_check(void *user) {
    return ud_activation_check(user);
}

ud_status ud_builtin_create_operator(const ud_component_context *c, int kind, const ud_json *config,
                                     const ud_batch *const *inputs, ud_operator **out) {
    ud_operator_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .kind = kind == JOIN     ? UD_OPERATOR_JOIN
                                           : kind == SORT   ? UD_OPERATOR_SORT
                                           : kind == WINDOW ? UD_OPERATOR_WINDOW
                                                            : UD_OPERATOR_AGGREGATE,
                                   .max_batch_rows = c->max_batch_rows,
                                   .buffers = c->execution->buffers,
                                   .check = ud_builtin_operator_check,
                                   .user = c->activation,
                                   .executor = c->execution->executor};
    UD_TRY(ud_component_operator_options(c, &options));
    ud_sort_contract orders[2] = {{0}};
    if (c->input_plans) {
        orders[0] = c->input_plans[0].sort;
        if (kind == JOIN)
            orders[1] = c->input_plans[1].sort;
    }
    options.input_order = orders;
    ud_expression_options bindings = {.struct_size = sizeof(bindings),
                                      .abi_version = UD_ABI_VERSION,
                                      .schema = inputs[0],
                                      .resolve = c->resolve,
                                      .user = c->resolve_user,
                                      .resolve_field = c->resolve_field,
                                      .registry = c->execution->registry,
                                      .function_capabilities =
                                          c->activation->parallel ? UD_COMPONENT_THREAD_SAFE : 0};
    options.expression_bindings = &bindings;
    char *json = NULL;
    uint32_t length = 0;
    ud_status status = ud_json_serialize(config, &json, &length);
    options.configuration = (ud_string){json, length};
    if (!status)
        status = ud_operator_create(c->memory, &options, inputs, out);
    ud_free(json);
    return status;
}

ud_status ud_builtin_sink_delivery(int kind, const ud_json *config, int32_t *delivery) {
    if (kind == REST_SINK)
        return ud_rest_delivery(config, delivery);
    if (kind != SQL_SINK)
        return UD_INVALID;
    ud_string value = {"receipt", 7};
    if (ud_member(config, "delivery"))
        UD_TRY(ud_json_string(ud_member(config, "delivery"), &value));
    if (ud_named(value, "receipt"))
        *delivery = UD_DELIVERY_RECEIPT;
    else if (ud_named(value, "response"))
        *delivery = UD_DELIVERY_RESPONSE;
    else
        return UD_SCHEMA;
    return UD_OK;
}

ud_status ud_builtin_describe_sink(void *user, const ud_component_context *c, ud_string text,
                                   const ud_batch *const *inputs, ud_component_plan *out) {
    (void)inputs;
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    UD_TRY(ud_builtin_parse(c, text, &config));
    ud_string mode = {"append", 6};
    ud_status status = UD_OK;
    if (ud_member(config, "mode"))
        status = ud_json_string(ud_member(config, "mode"), &mode);
    int32_t delivery = UD_DELIVERY_RECEIPT;
    if (!status)
        status = ud_builtin_sink_delivery(kind, config, &delivery);
    out->effects = UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_COMMIT_UNKNOWN;
    if (kind == SQL_SINK)
        out->effects |= UD_EFFECT_TRANSACTIONAL;
    if (delivery == UD_DELIVERY_RECEIPT)
        out->effects |= UD_EFFECT_RECEIPTED;
    if (delivery == UD_DELIVERY_IDEMPOTENT)
        out->effects |= UD_EFFECT_IDEMPOTENT;
    int keyed = kind == SQL_SINK;
    if (!status && kind == REST_SINK && ud_member(config, "keyedReplace")) {
        const ud_json *value = ud_member(config, "keyedReplace");
        const char *raw = NULL;
        uint32_t length = 0;
        status =
            ud_json_kind(value) == UD_JSON_BOOL ? ud_json_text(value, &raw, &length) : UD_SCHEMA;
        keyed = !status && length == 4 && !memcmp(raw, "true", 4);
    }
    if (!status && keyed && ud_named(mode, "upsert") &&
        ud_json_kind(ud_member(config, "keys")) == UD_JSON_ARRAY &&
        ud_json_size(ud_member(config, "keys")) && delivery != UD_DELIVERY_RESPONSE)
        out->effects |= UD_EFFECT_KEYED_REPLACE;
    ud_json_destroy(config);
    return status;
}
ud_status ud_builtin_describe_operator(void *user, const ud_component_context *c, ud_string text,
                                       const ud_batch *const *inputs, ud_component_plan *out) {
    if (!out || out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    ud_operator *op = NULL;
    ud_status status = ud_builtin_parse(c, text, &config);
    if (!status)
        status = ud_builtin_create_operator(c, kind, config, inputs, &op);
    if (!status) {
        status = ud_component_operator_plan(op, out);
    }
    ud_status released = ud_operator_destroy(op);
    if (!status)
        status = released;
    ud_json_destroy(config);
    return status;
}

ud_status ud_builtin_configure(void *user, const ud_component_context *c, ud_string text,
                               const ud_batch *const *in, ud_batch **out) {
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    ud_status s = ud_builtin_parse(c, text, &config);
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
        s = ud_builtin_schema_config(c, config, out);
    else if (!s && kind == SQL_SINK) {
        int32_t delivery = UD_DELIVERY_RECEIPT;
        int policy;
        s = ud_builtin_sink_delivery(kind, config, &delivery);
        if (!s)
            s = ud_error_policy(config, "errorPolicy", UD_ERROR_FAIL, &policy);
        if (!s && policy != UD_ERROR_FAIL && !c->activation->transaction_serial)
            s = UD_UNSUPPORTED;
        if (!s)
            s = error_schema(c, config, in[0], 0, out);
    } else if (!s && (sink_kind(kind))) { /* No output schemas. */
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
        if (!s)
            s = error_schema(c, config, in[0], 1, out + 1);
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
                s = ud_builtin_bind_expr(c, ud_member(col, "expression"), in[0], bound + i);
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
        s = ud_builtin_create_operator(c, kind, config, in, &operator);
        if (!s)
            s = ud_operator_schema(operator, 0, out);
        ud_status cleanup = ud_operator_destroy(operator);
        if (!s)
            s = cleanup;
    } else if (!s && kind == UNION_ALL)
        s = ud_compute_union_schema(c->memory, in[0], in[1], out);
    else if (!s)
        s = ud_builtin_clone_schema(c->memory, in[0], out);
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
        s = ud_builtin_parameters(c, config, &params);
        if (!s && params && params->rows != 1)
            s = UD_SCHEMA;
        ud_batch_destroy(params);
    }
    if (!s && (kind == CSV_SOURCE || kind == JSON_SOURCE || kind == SQL_SOURCE))
        s = error_schema(c, config, out[0], 1, out + 1);
    if (!s && kind == SQL_SOURCE && ud_member(config, "where")) {
        ud_expression *predicate = NULL;
        s = ud_builtin_bind_expr(c, ud_member(config, "where"), out[0], &predicate);
        if (!s && ud_expression_type(predicate) != UD_BOOL)
            s = UD_SCHEMA;
        ud_expression_destroy(predicate);
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
    if (!s && (kind == SPLIT || kind == MULTICAST || kind == BALANCED))
        for (uint32_t p = 1; !s && p < c->output_count - (kind == SPLIT); p++)
            s = ud_builtin_clone_schema(c->memory, in[0], out + p);
    if (!s && (kind == DERIVE || kind == SPLIT || kind == SQL_COMMAND)) {
        uint32_t port = kind == SPLIT ? c->output_count - 1 : 1;
        s = error_schema(c, config, in[0], port, out + port);
        if (!s && kind == SQL_COMMAND) {
            int policy;
            s = ud_error_policy(config, "errorPolicy", UD_ERROR_FAIL, &policy);
            if (!s && policy != UD_ERROR_FAIL && !c->activation->transaction_serial)
                s = UD_UNSUPPORTED;
            const ud_json *parameters = ud_member(config, "parameters");
            for (uint32_t at = 0; !s && at < ud_json_size(parameters); at++) {
                uint64_t column;
                s = ud_json_uint(ud_json_at(parameters, at), &column);
                if (!s && column >= in[0]->columns)
                    s = UD_SCHEMA;
                for (uint32_t previous = 0; !s && previous < at; previous++) {
                    uint64_t before;
                    s = ud_json_uint(ud_json_at(parameters, previous), &before);
                    if (!s && before == column)
                        s = UD_SCHEMA;
                }
            }
        }
    }
    if (!s && kind == SPLIT) {
        const ud_json *branches = ud_member(config, "branches");
        uint32_t count = branches ? ud_json_size(branches) : 1;
        for (uint32_t p = 0; !s && p < count; p++) {
            ud_expression *expr = NULL;
            s = ud_builtin_bind_expr(c,
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
            s = ud_builtin_connection_defined(c, name);
    }
    if (!s && (sink_kind(kind))) {
        ud_string name = {0};
        s = ud_json_string(ud_member(config, "connection"), &name);
        if (!s)
            s = ud_builtin_connection_defined(c, name);
    }
    ud_json_destroy(config);
    return s;
}

ud_status ud_builtin_describe_source(void *user, const ud_component_context *c, ud_string text,
                                     const ud_batch *const *in, ud_component_plan *plan) {
    (void)user;
    (void)in;
    ud_json *config = NULL;
    UD_TRY(ud_builtin_parse(c, text, &config));
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

ud_status ud_builtin_builtin_ports(void *user, const ud_component_context *c, ud_string text,
                                   ud_component_ports *ports) {
    int kind = (int)(intptr_t)user;
    ud_json *config = NULL;
    UD_TRY(ud_builtin_parse(c, text, &config));
    const ud_json *names = ud_member(config, kind == MULTICAST || kind == BALANCED ? "outputs"
                                             : kind == UNION_ALL                   ? "inputs"
                                                                                   : "branches");
    uint32_t n = names ? ud_json_size(names) : 2;
    ud_status s = UD_OK;
    if (kind == SPLIT)
        n = names ? ud_json_size(names) + 2 : 3;
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
            if (names && ((kind == UNION_ALL && !direction) ||
                          ((kind == MULTICAST || kind == BALANCED) && direction)))
                value = ud_json_at(names, i);
            if (names && kind == SPLIT && direction) {
                if (i + 2 < ports->output_count) {
                    const ud_json *branch = ud_json_at(names, i);
                    static const char *allow[] = {"id", "condition"};
                    s = ud_json_keys(branch, allow, 2);
                    value = ud_member(branch, "id");
                } else if (i + 2 == ports->output_count)
                    value = ud_member(config, "default");
            }
            if (!s && kind == SPLIT && direction && i + 1 == ports->output_count)
                name = (ud_string){"error", 5};
            else if (!s && value)
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
                if (kind == SPLIT && direction && i + 1 == ports->output_count)
                    port->flags = UD_PORT_ERROR;
                if (!port->name.data)
                    s = UD_RESOURCE;
            }
        }
    ud_json_destroy(config);
    return s;
}

uint64_t ud_builtin_builtin_demand(void *ptr) {
    return ud_operator_input_demand(((builtin *)ptr)->operator);
}
