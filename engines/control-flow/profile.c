#include "../package/internal.h"
#include "ud/profile.h"
typedef struct profile_sink {
    ud_component_context context;
    ud_json *config;
    ud_profile_statistic *statistics;
    ud_string *names;
    uint32_t count;
    ud_profile *profile;
} profile_sink;
static ud_status read_statistics(const ud_component_context *c, ud_string text,
                                 const ud_batch *schema, profile_sink *p, uint64_t *maximum) {
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, &p->config, NULL));
    static const char *keys[] = {"statistics", "maximumRows"};
    UD_TRY(ud_json_keys(p->config, keys, 2));
    const ud_json *stats = ud_member(p->config, "statistics");
    p->count = ud_json_size(stats);
    if (ud_json_kind(stats) != UD_JSON_ARRAY || !p->count || p->count > 256)
        return UD_SCHEMA;
    p->statistics = ud_alloc(c->memory, p->count * sizeof(*p->statistics));
    p->names = ud_alloc(c->memory, p->count * sizeof(*p->names));
    if (!p->statistics || !p->names)
        return UD_RESOURCE;
    *maximum = INT64_MAX;
    if (ud_member(p->config, "maximumRows"))
        UD_TRY(ud_json_uint(ud_member(p->config, "maximumRows"), maximum));
    if (!*maximum || *maximum > INT64_MAX)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < p->count; i++) {
        const ud_json *def = ud_json_at(stats, i);
        static const char *sk[] = {"kind", "column", "variable"};
        UD_TRY(ud_json_keys(def, sk, 3));
        ud_string kind;
        UD_TRY(ud_json_string(ud_member(def, "kind"), &kind));
        ud_profile_statistic *stat = p->statistics + i;
        *stat = (ud_profile_statistic){.struct_size = sizeof(*stat),
                                       .abi_version = UD_ABI_VERSION,
                                       .kind = ud_named(kind, "rows")    ? UD_PROFILE_ROWS
                                               : ud_named(kind, "nulls") ? UD_PROFILE_NULLS
                                               : ud_named(kind, "min")   ? UD_PROFILE_MINIMUM
                                                : ud_named(kind, "max") ? UD_PROFILE_MAXIMUM
                                               : ud_named(kind, "distinct") ? UD_PROFILE_DISTINCT
                                               : ud_named(kind, "topK") ? UD_PROFILE_TOPK
                                               : ud_named(kind, "histogram") ? UD_PROFILE_HISTOGRAM
                                               : ud_named(kind, "lengthDistribution") ? UD_PROFILE_LENGTH_DISTRIBUTION
                                               : ud_named(kind, "approximateCardinality") ? UD_PROFILE_APPROXIMATE_CARDINALITY
                                               : ud_named(kind, "anomalies") ? UD_PROFILE_ANOMALIES
                                               : ud_named(kind, "report") ? UD_PROFILE_REPORT : 0};
        if (!stat->kind)
            return UD_UNSUPPORTED;
        ud_type type = stat->kind >= UD_PROFILE_TOPK ? UD_UTF8 : UD_INT64;
        if (stat->kind != UD_PROFILE_ROWS) {
            uint64_t column;
            UD_TRY(ud_json_uint(ud_member(def, "column"), &column));
            if (column >= ud_batch_columns(schema))
                return UD_SCHEMA;
            stat->column = (uint32_t)column;
            ud_field field;
            UD_TRY(ud_batch_field(schema, stat->column, &field));
            if (stat->kind == UD_PROFILE_MINIMUM || stat->kind == UD_PROFILE_MAXIMUM)
                type = field.type;
        } else if (ud_member(def, "column"))
            return UD_SCHEMA;
        if (type < UD_BOOL || type > UD_MAP)
            return UD_UNSUPPORTED;
        UD_TRY(ud_json_string(ud_member(def, "variable"), p->names + i));
        for (uint32_t j = 0; j < i; j++)
            if (ud_same(p->names[i], p->names[j]))
                return UD_SCHEMA;
        ud_variable *target = NULL;
        UD_TRY(ud_variable_writable(c->activation, p->names[i], &target));
        if (target->value.type != type)
            return UD_SCHEMA;
    }
    return UD_OK;
}
static void clear_statistics(profile_sink *p) {
    ud_profile_destroy(p->profile);
    ud_free(p->statistics);
    ud_free(p->names);
    ud_json_destroy(p->config);
}
static ud_status stat_configure(void *user, const ud_component_context *c, ud_string text,
                                const ud_batch *const *inputs, ud_batch **outputs) {
    (void)user;
    (void)outputs;
    profile_sink p = {0};
    uint64_t maximum;
    ud_status s = read_statistics(c, text, inputs[0], &p, &maximum);
    if (!s)
        s = ud_profile_create(c->memory, inputs[0], p.statistics, p.count, maximum, NULL, NULL,
                              &p.profile);
    clear_statistics(&p);
    return s;
}
static ud_status check_execution(void *user) {
    return ud_activation_check(user);
}
static ud_status stat_prepare(void *user, const ud_component_context *c, ud_string text,
                              const ud_batch *const *inputs, const ud_batch *const *outputs,
                              void **instance) {
    (void)user;
    (void)outputs;
    *instance = NULL;
    profile_sink *p = ud_alloc(c->memory, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->context = *c;
    uint64_t maximum;
    ud_status s = read_statistics(c, text, inputs[0], p, &maximum);
    if (!s)
        s = ud_profile_create(c->memory, inputs[0], p->statistics, p->count, maximum,
                              check_execution, c->activation, &p->profile);
    if (s) {
        clear_statistics(p);
        ud_free(p);
    } else
        *instance = p;
    return s;
}
static ud_status stat_step(void *instance, const ud_component_event *event) {
    profile_sink *p = instance;
    UD_TRY(ud_activation_check(p->context.activation));
    return event->kind == UD_COMPONENT_DATA   ? ud_profile_add(p->profile, event->batch)
           : event->kind == UD_COMPONENT_POLL ? UD_END
                                              : UD_OK;
}
static ud_status stat_finish(void *instance) {
    profile_sink *p = instance;
    ud_batch **boxes = ud_alloc(p->context.memory, p->count * sizeof(*boxes));
    ud_variable **targets = ud_alloc(p->context.memory, p->count * sizeof(*targets));
    ud_status s = !boxes || !targets ? UD_RESOURCE : UD_OK;
    for (uint32_t i = 0; !s && i < p->count; i++) {
        ud_value value = {0};
        ud_field field = {0};
        s = ud_profile_get(p->profile, i, &value);
        if (!s)
            s = ud_variable_writable(p->context.activation, p->names[i], targets + i);
        if (!s)
            s = ud_batch_field(targets[i]->box, 0, &field);
        if (!s && field.type != value.type)
            s = UD_SCHEMA;
        if (!s)
            s = ud_batch_create(p->context.memory, &field, 1, 1, boxes + i);
        if (!s)
            s = ud_batch_append(boxes[i], &value, 1);
        if (!s)
            s = ud_batch_seal(boxes[i]);
    }
    if (!s)
        s = ud_activation_check(p->context.activation);
    if (!s)
        for (uint32_t i = 0; i < p->count; i++) {
            ud_batch *previous = targets[i]->box;
            targets[i]->box = boxes[i];
            boxes[i] = NULL;
            ud_batch_get(targets[i]->box, 0, 0, &targets[i]->value);
            ud_batch_destroy(previous);
        }
    if (boxes)
        for (uint32_t i = 0; i < p->count; i++)
            ud_batch_destroy(boxes[i]);
    ud_free(boxes);
    ud_free(targets);
    return s;
}
static void stat_dispose(void *instance, ud_status outcome) {
    (void)outcome;
    profile_sink *p = instance;
    clear_statistics(p);
    ud_free(p);
}
static ud_status read_task(const ud_component_context *c, ud_string text, ud_json **out,
                           const ud_json **pipeline, ud_string *passed, ud_string *failed) {
    *out = NULL;
    *pipeline = NULL;
    *passed = *failed = (ud_string){0};
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, out, NULL));
    static const char *keys[] = {"pipeline", "checks", "passedVariable", "failedChecksVariable"};
    UD_TRY(ud_json_keys(*out, keys, 4));
    ud_string id;
    UD_TRY(ud_json_string(ud_member(*out, "pipeline"), &id));
    const ud_json *flows = ud_member(c->activation->package->document, "dataFlows");
    for (uint32_t i = 0; i < ud_json_size(flows); i++) {
        const ud_json *flow = ud_json_at(flows, i);
        ud_string name;
        UD_TRY(ud_json_string(ud_member(flow, "id"), &name));
        if (ud_same(id, name))
            *pipeline = flow;
    }
    if (!*pipeline)
        return UD_SCHEMA;
    const ud_json *nodes = ud_member(*pipeline, "components"), *statistics = NULL;
    uint32_t sinks = 0;
    for (uint32_t i = 0; i < ud_json_size(nodes); i++) {
        const ud_json *node = ud_json_at(nodes, i);
        ud_string component;
        UD_TRY(ud_json_string(ud_member(node, "component"), &component));
        ud_component_descriptor d = {.struct_size = sizeof(d), .abi_version = UD_ABI_VERSION};
        UD_TRY(ud_component_lookup(c->activation, component, &d));
        if (d.category == UD_COMPONENT_DESTINATION) {
            if (!ud_named(component, "profile.statistics"))
                return UD_SCHEMA;
            statistics = ud_member(ud_member(node, "config"), "statistics");
            sinks++;
        }
    }
    if (sinks != 1)
        return UD_SCHEMA;
    const ud_json *checks = ud_member(*out, "checks");
    uint32_t count = ud_json_size(checks);
    if (ud_json_kind(checks) != UD_JSON_ARRAY || !count || count > 256)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < count; i++) {
        const ud_json *def = ud_json_at(checks, i);
        static const char *ck[] = {"id", "expression"};
        UD_TRY(ud_json_keys(def, ck, 2));
        ud_string name;
        UD_TRY(ud_json_string(ud_member(def, "id"), &name));
        for (uint32_t j = 0; j < i; j++) {
            ud_string other;
            UD_TRY(ud_json_string(ud_member(ud_json_at(checks, j), "id"), &other));
            if (ud_same(name, other))
                return UD_SCHEMA;
        }
        ud_expression *expr = NULL;
        ud_status s = ud_expression_json(c->activation, ud_member(def, "expression"), NULL, &expr);
        if (!s && ud_expression_type(expr) != UD_BOOL)
            s = UD_SCHEMA;
        ud_expression_destroy(expr);
        UD_TRY(s);
    }
    const char *names[] = {"passedVariable", "failedChecksVariable"};
    ud_string *variables[] = {passed, failed};
    for (int i = 0; i < 2; i++)
        if (ud_member(*out, names[i])) {
            UD_TRY(ud_json_string(ud_member(*out, names[i]), variables[i]));
            ud_variable *v;
            UD_TRY(ud_variable_writable(c->activation, *variables[i], &v));
            if (v->value.type != (i ? UD_INT64 : UD_BOOL))
                return UD_SCHEMA;
            for (uint32_t j = 0; j < ud_json_size(statistics); j++) {
                ud_string stat;
                UD_TRY(ud_json_string(ud_member(ud_json_at(statistics, j), "variable"), &stat));
                if (ud_same(stat, *variables[i]))
                    return UD_SCHEMA;
            }
        }
    return ud_pipeline_bind_validate(c->activation, *pipeline);
}
static ud_status task_configure(void *user, const ud_component_context *c, ud_string text,
                                const ud_batch *const *in, ud_batch **out) {
    (void)user;
    (void)in;
    (void)out;
    ud_json *doc = NULL;
    const ud_json *pipeline;
    ud_string passed, failed;
    ud_status s = read_task(c, text, &doc, &pipeline, &passed, &failed);
    ud_json_destroy(doc);
    return s;
}
static ud_status task_invoke(void *user, const ud_component_context *c, ud_string text) {
    (void)user;
    ud_json *doc = NULL;
    const ud_json *pipeline;
    ud_string passed, failed;
    ud_status s = read_task(c, text, &doc, &pipeline, &passed, &failed);
    if (!s)
        s = ud_pipeline_execute(c->activation, pipeline);
    uint32_t failures = 0;
    const ud_json *checks = ud_member(doc, "checks");
    for (uint32_t i = 0; !s && i < ud_json_size(checks); i++) {
        ud_expression *expr = NULL;
        s = ud_activation_check(c->activation);
        if (!s)
            s = ud_expression_json(c->activation, ud_member(ud_json_at(checks, i), "expression"),
                                   NULL, &expr);
        ud_value value = {0};
        if (!s)
            s = ud_expression_evaluate(expr, NULL, 0, &value);
        if (!s && (value.is_null || !value.integer))
            failures++;
        ud_expression_destroy(expr);
    }
    if (!s && passed.length) {
        ud_value v = {.type = UD_BOOL, .integer = !failures};
        s = ud_activation_set_variable(c->activation, passed, &v, NULL);
    }
    if (!s && failed.length) {
        ud_value v = {.type = UD_INT64, .integer = failures};
        s = ud_activation_set_variable(c->activation, failed, &v, NULL);
    }
    if (!s && failures)
        s = UD_QUALITY;
    ud_json_destroy(doc);
    return s;
}
ud_status ud_profile_register(ud_registry *registry) {
    static const char statistics_schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"statistics\"],"
        "\"properties\":{\"maximumRows\":{\"type\":\"integer\",\"minimum\":1},"
        "\"statistics\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":256,\"items\":{"
        "\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"kind\",\"variable\"],"
        "\"properties\":{\"kind\":{\"enum\":[\"rows\",\"nulls\",\"min\",\"max\",\"distinct\",\"topK\",\"histogram\",\"lengthDistribution\",\"approximateCardinality\",\"anomalies\",\"report\"]},"
        "\"column\":{\"type\":\"integer\",\"minimum\":0},\"variable\":{\"type\":\"string\"}}}}}}";
    ud_component_descriptor d = {
        .struct_size = sizeof(d),
        .abi_version = UD_ABI_VERSION,
        .id = {"profile.statistics", 18},
        .name = {"Profile Statistics", 18},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION)-1},
        .configuration_schema = {statistics_schema, sizeof(statistics_schema) - 1},
        .category = UD_COMPONENT_DESTINATION,
        .inputs = 1,
        .synchronous_input = -1,
        .error_output = -1,
        .capabilities = UD_COMPONENT_REPLAY_SAFE,
        .configure = stat_configure,
        .prepare = stat_prepare,
        .step = stat_step,
        .finish = stat_finish,
        .dispose = stat_dispose};
    UD_TRY(ud_registry_register(registry, &d));
    static const char task_schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"pipeline\",\"checks\"]"
        ","
        "\"properties\":{\"pipeline\":{\"type\":\"string\"},\"passedVariable\":{\"type\":"
        "\"string\"},"
        "\"failedChecksVariable\":{\"type\":\"string\"},\"checks\":{\"type\":\"array\","
        "\"minItems\":1,\"maxItems\":256,\"items\":{\"type\":\"object\","
        "\"additionalProperties\":false,\"required\":[\"id\",\"expression\"],"
        "\"properties\":{\"id\":{\"type\":\"string\"},\"expression\":{\"type\":\"object\"}}}}}}";
    d = (ud_component_descriptor){.struct_size = sizeof(d),
                                  .abi_version = UD_ABI_VERSION,
                                  .id = {"data.profile", 12},
                                  .name = {"Data Profiling Check", 20},
                                  .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION)-1},
                                  .configuration_schema = {task_schema, sizeof(task_schema) - 1},
                                  .category = UD_COMPONENT_TASK,
                                  .synchronous_input = -1,
                                  .error_output = -1,
                                  .capabilities = UD_COMPONENT_REPLAY_SAFE,
                                  .configure = task_configure,
                                  .invoke = task_invoke};
    return ud_registry_register(registry, &d);
}