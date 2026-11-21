#include "ud/package.h"
#include "ud/format.h"
#include "ud/xlsx.h"
#include "ud/json.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x)                                                                                   \
    do {                                                                                           \
        if (!(x)) {                                                                                \
            fprintf(stderr, "line %d: %s\n", __LINE__, #x);                                        \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)
#define OK(x)                                                                                      \
    do {                                                                                           \
        ud_status tested_status = (x);                                                             \
        if (tested_status)                                                                         \
            fprintf(stderr, "Native status %d (%s) at line %d\n", tested_status,                   \
                    ud_status_name(tested_status), __LINE__);                                      \
        CHECK(tested_status == UD_OK);                                                             \
    } while (0)
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static int writes;
static int broker_accesses;
static ud_status authorize(void *user, ud_string tenant, ud_string resource, int32_t access) {
    (void)user;
    (void)tenant;
    const ud_string broker = str("preview-broker");
    if (resource.length == broker.length && !memcmp(resource.data, broker.data, broker.length)) {
        broker_accesses++;
        return UD_AUTH;
    }
    if (access == UD_ACCESS_WRITE || access == UD_ACCESS_RECEIPT || access == UD_ACCESS_EXECUTE) {
        writes++;
        return UD_AUTH;
    }
    return UD_OK;
}
static const ud_json *member(const ud_json *j, const char *key) {
    return ud_json_find(j, key, (uint32_t)strlen(key));
}
static long integer(const ud_json *j) {
    const char *p = NULL;
    uint32_t n = 0;
    if (ud_json_text(j, &p, &n))
        return -1;
    char b[64];
    if (n >= sizeof(b))
        return -1;
    memcpy(b, p, n);
    b[n] = 0;
    return strtol(b, NULL, 10);
}
static long port_rows(const ud_json *root, const char *component, int port) {
    const ud_json *edges = member(member(root, "snapshot"), "preview");
    for (uint32_t i = 0; i < ud_json_size(edges); i++) {
        const ud_json *e = ud_json_at(edges, i);
        const char *id = NULL;
        uint32_t n = 0;
        if (!ud_json_text(member(e, "component"), &id, &n) && n == strlen(component) &&
            !memcmp(id, component, n) && integer(member(e, "port")) == port)
            return integer(member(e, "totalRows"));
    }
    return 0;
}
static ud_status execution_scope(const char *path, const char *project, const char *tenant,
                                 ud_execution **out) {
    char package[4096];
    snprintf(
        package, sizeof(package),
        "{\"version\":" UD_PROTOCOL_VERSION_TEXT
        ",\"id\":\"designer-proof\"%s,\"connections\":[{\"id\":\"input\",\"driver\":\"file\","
        "\"resource\":\"%s\"},{\"id\":\"messages\",\"driver\":\"kafka\",\"resource\":\"preview-broker\"}],"
        "\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}",
        project, path);
    ud_execution_options options = {
        .struct_size = sizeof(options),
        .abi_version = UD_ABI_VERSION,
        .context = {sizeof(ud_context_options), UD_ABI_VERSION, 134217728},
        .edge_capacity = 2,
        .max_batch_rows = 16,
        .max_depth = 32,
        .authorize = authorize,
        .tenant = str(tenant),
        .timeout_ms = 10000,
        .max_concurrency = 2};
    return ud_execution_create_json(str(package), &options, out);
}
static ud_status execution(const char *path, ud_execution **out) {
    return execution_scope(path, "", "test", out);
}
int main(int argc, char **argv) {
    CHECK(argc == 3);
    ud_context *ctx = NULL;
    ud_context_options co = {sizeof(co), UD_ABI_VERSION, 134217728};
    OK(ud_context_create(&co, &ctx));
    ud_execution *e = NULL;
    OK(execution(argv[1], &e));
    char *inspection = NULL;
    uint32_t length = 0;
    OK(ud_execution_inspect_file(e, str("input"),
                                 str("{\"format\":\"csv\",\"options\":{\"header\":true}}"),
                                 &inspection, &length));
    ud_json *inspected = NULL;
    OK(ud_json_parse(ctx, inspection, length, &inspected, NULL));
    CHECK(ud_json_size(member(member(inspected, "schema"), "fields")) == 3);
    char *schema = NULL;
    uint32_t schema_n = 0;
    OK(ud_json_serialize(member(inspected, "schema"), &schema, &schema_n));
    ud_json_destroy(inspected);
    ud_free(inspection);
    char pipeline[16384];
    snprintf(
        pipeline, sizeof(pipeline),
        "{\"id\":\"main\",\"components\":[{\"id\":\"source\",\"component\":\"file.csv.source\","
        "\"config\":{\"connection\":\"input\",\"batchSize\":16,\"schema\":%s}},{\"id\":\"derive\","
        "\"component\":"
        "\"derive\",\"config\":{\"columns\":[{\"name\":\"scaled\",\"expression\":\"100 / "
        "[Amount]\",\"errorDisposition\":\"redirect\"}],\"includeInput\":true}},{\"id\":\"split\","
        "\"component\":\"split\",\"config\":{\"branches\":[{\"id\":\"vip-stable\",\"name\":\"VIP\","
        "\"condition\":\"[scaled] >= "
        "25\"}],\"default\":\"Other\"}},{\"id\":\"sink\",\"component\":\"sql.destination\","
        "\"config\":{\"connection\":\"unavailable-production\",\"table\":\"never_written\","
        "\"mode\":\"append\"}}],\"paths\":[{\"from\":\"source\",\"output\":0,\"to\":\"derive\","
        "\"input\":0},{\"from\":\"derive\",\"output\":0,\"to\":\"split\",\"input\":0},{\"from\":"
        "\"split\",\"output\":0,\"to\":\"sink\",\"input\":0}]}",
        schema);
    char *ports1 = NULL, *ports2 = NULL;
    uint32_t n1 = 0, n2 = 0;
    OK(ud_execution_component_ports(
        e, str("main"), str("split"), str("split"),
        str("{\"branches\":[{\"id\":\"stable-a\",\"name\":\"VIP\",\"condition\":\"true\"},{\"id\":"
            "\"stable-b\",\"name\":\"Refund\",\"condition\":\"true\"}],\"default\":\"Other\"}"),
        &ports1, &n1));
    OK(ud_execution_component_ports(
        e, str("main"), str("split"), str("split"),
        str("{\"branches\":[{\"id\":\"stable-b\",\"name\":\"Renamed\",\"condition\":\"true\"},{"
            "\"id\":\"stable-a\",\"name\":\"VIP "
            "new\",\"condition\":\"true\"}],\"default\":\"Other\"}"),
        &ports2, &n2));
    ud_json *p1 = NULL, *p2 = NULL;
    OK(ud_json_parse(ctx, ports1, n1, &p1, NULL));
    OK(ud_json_parse(ctx, ports2, n2, &p2, NULL));
    const char *first = NULL, *second = NULL;
    uint32_t a = 0, b = 0;
    OK(ud_json_text(member(ud_json_at(p1, 1), "id"), &first, &a));
    OK(ud_json_text(member(ud_json_at(p2, 2), "id"), &second, &b));
    CHECK(a == b && !memcmp(first, second, a));
    ud_execution *deployed = NULL;
    OK(execution_scope(argv[1], ",\"project\":{\"id\":\"published-project\"}", "isolated-tenant",
                       &deployed));
    char *deployed_ports = NULL;
    uint32_t deployed_length = 0;
    OK(ud_execution_component_ports(deployed, str("main"), str("split"), str("split"),
                                    str("{\"branches\":[{\"id\":\"stable-a\",\"name\":\"VIP\","
                                        "\"condition\":\"true\"}],\"default\":\"Other\"}"),
                                    &deployed_ports, &deployed_length));
    ud_json *p3 = NULL;
    OK(ud_json_parse(ctx, deployed_ports, deployed_length, &p3, NULL));
    const char *third = NULL;
    uint32_t n3 = 0;
    OK(ud_json_text(member(ud_json_at(p3, 1), "id"), &third, &n3));
    CHECK(a == n3 && !memcmp(first, third, a));
    ud_json_destroy(p3);
    ud_free(deployed_ports);
    OK(ud_execution_destroy(deployed));
    ud_json_destroy(p1);
    ud_json_destroy(p2);
    ud_free(ports1);
    ud_free(ports2);
    char *result = NULL;
    OK(ud_execution_preview(e, str(pipeline), str("sink"), 10, 8, &result, &length));
    ud_json *value = NULL;
    OK(ud_json_parse(ctx, result, length, &value, NULL));
    CHECK(integer(member(value, "result")) == 0);
    CHECK(port_rows(value, "source", 0) == 10);
    CHECK(port_rows(value, "derive", 0) == 9);
    CHECK(port_rows(value, "derive", 1) == 1);
    CHECK(port_rows(value, "split", 0) == 4);
    CHECK(port_rows(value, "split", 1) == 5);
    CHECK(writes == 0);
    ud_json_destroy(value);
    ud_free(result);
    OK(ud_execution_destroy(e));
    OK(execution(argv[1], &e));
    OK(ud_execution_preview(e, str(pipeline), str("derive"), 3, 3, &result, &length));
    OK(ud_json_parse(ctx, result, length, &value, NULL));
    CHECK(integer(member(value, "result")) == 0);
    CHECK(port_rows(value, "source", 0) == 3);
    CHECK(port_rows(value, "derive", 0) == 2);
    CHECK(port_rows(value, "derive", 1) == 1);
    ud_json_destroy(value);
    ud_free(result);
    OK(ud_execution_destroy(e));
    OK(execution(argv[1], &e));
    char source_only[8192];
    snprintf(source_only, sizeof(source_only),
             "{\"id\":\"main\",\"components\":[{\"id\":\"source\",\"component\":\"file.csv."
             "source\",\"config\":{\"connection\":\"input\",\"schema\":%s}}],\"paths\":[]}",
             schema);
    OK(ud_execution_preview(e, str(source_only), str("source"), 4, 2, &result, &length));
    OK(ud_json_parse(ctx, result, length, &value, NULL));
    CHECK(port_rows(value, "source", 0) == 4);
    ud_json_destroy(value);
    ud_free(result);
    OK(ud_execution_destroy(e));
    OK(execution(argv[1], &e));
    char message_pipeline[8192];
    snprintf(message_pipeline, sizeof(message_pipeline),
             "{\"id\":\"messages\",\"components\":[{\"id\":\"source\",\"component\":\"mq.source\","
             "\"config\":{\"connection\":\"messages\",\"targetType\":\"topic\",\"address\":\"orders\","
             "\"group\":\"preview\",\"format\":\"json\",\"includeMetadata\":false,\"schema\":%s}},"
             "{\"id\":\"derive\",\"component\":\"derive\",\"config\":{\"columns\":[{\"name\":\"adjusted\","
             "\"expression\":\"[Amount] + 1L\"}]}}],\"paths\":[{\"from\":\"source\",\"output\":0,"
             "\"to\":\"derive\",\"input\":0}]}", schema);
    OK(ud_execution_preview(e, str(message_pipeline), str("derive"), 10, 8, &result, &length));
    OK(ud_json_parse(ctx, result, length, &value, NULL));
    CHECK(integer(member(value, "result")) == UD_OK);
    CHECK(integer(member(value, "sourceLimit")) == 0);
    const char *sampled = NULL;
    uint32_t sampled_length = 0;
    OK(ud_json_text(member(value, "sampled"), &sampled, &sampled_length));
    CHECK(sampled_length == 5 && !memcmp(sampled, "false", 5));
    const ud_json *message_edges = member(member(value, "snapshot"), "preview");
    const ud_json *message_output = ud_json_at(message_edges, 0);
    const ud_json *derived_output = ud_json_at(message_edges, 2);
    CHECK(ud_json_size(member(member(message_output, "schema"), "fields")) == 3);
    CHECK(ud_json_size(member(member(derived_output, "schema"), "fields")) == 4);
    CHECK(integer(member(message_output, "totalRows")) == 0);
    CHECK(integer(member(derived_output, "totalRows")) == 0);
    CHECK(ud_json_size(member(member(value, "diagnostics"), "pipelines")) == 1);
    CHECK(broker_accesses == 0);
    ud_json_destroy(value);
    ud_free(result);
    OK(ud_execution_destroy(e));
    OK(execution(argv[1], &e));
    char unsafe[16384];
    snprintf(
        unsafe, sizeof(unsafe),
        "{\"id\":\"main\",\"components\":[{\"id\":\"source\",\"component\":\"file.csv.source\","
        "\"config\":{\"connection\":\"input\",\"schema\":%s}},{\"id\":\"write\",\"component\":"
        "\"sql.command\",\"config\":{\"connection\":\"production\",\"statement\":\"DELETE FROM "
        "business\",\"parameters\":[]}}],\"paths\":[{\"from\":\"source\",\"output\":0,\"to\":"
        "\"write\",\"input\":0}]}",
        schema);
    CHECK(ud_execution_preview(e, str(unsafe), str("write"), 10, 8, &result, &length) ==
          UD_UNSUPPORTED);
    CHECK(writes == 0);
    OK(ud_execution_destroy(e));
    ud_free(schema);
    /* XLSX generation and discovery use the same real native codec. */
    ud_buffer_manager *buffers = NULL;
    ud_buffer_options bo = {.struct_size = sizeof(bo),
                            .abi_version = UD_ABI_VERSION,
                            .max_blocks = 256,
                            .resident_limit = 16777216,
                            .working_limit = 16777216,
                            .spill_limit = 67108864};
    OK(ud_buffer_manager_create(ctx, &bo, &buffers));
    ud_field fields[] = {{.name = "Amount", .name_length = 6, .type = UD_INT64},
                         {.name = "Customer", .name_length = 8, .type = UD_UTF8},
                         {.name = "Date", .name_length = 4, .type = UD_DATE32}};
    ud_batch *batch = NULL;
    OK(ud_batch_create(ctx, fields, 3, 1, &batch));
    ud_value values[] = {{.type = UD_INT64, .integer = 42},
                         {.type = UD_UTF8, .text = "客户", .text_length = 6},
                         {.type = UD_DATE32, .integer = 20000}};
    OK(ud_batch_append(batch, values, 3));
    OK(ud_batch_seal(batch));
    ud_xlsx_options xo = {.struct_size = sizeof(xo),
                          .abi_version = UD_ABI_VERSION,
                          .batch_size = 8,
                          .configuration = {"{\"sheet\":\"Sales\",\"header\":true}", 31},
                          .buffers = buffers};
    xo.configuration = str("{\"sheet\":\"Sales\",\"header\":true}");
    ud_xlsx_writer *writer = NULL;
    OK(ud_xlsx_writer_open(ctx, argv[2], fields, 3, &xo, &writer));
    OK(ud_xlsx_writer_append(writer, batch));
    OK(ud_xlsx_writer_close(writer));
    ud_batch_destroy(batch);
    OK(ud_buffer_manager_destroy(buffers));
    OK(execution(argv[2], &e));
    OK(ud_execution_inspect_file(e, str("input"),
                                 str("{\"format\":\"xlsx\",\"options\":{\"header\":true}}"),
                                 &inspection, &length));
    OK(ud_json_parse(ctx, inspection, length, &inspected, NULL));
    CHECK(ud_json_size(member(member(inspected, "schema"), "fields")) == 3);
    CHECK(ud_json_size(member(member(inspected, "metadata"), "sheets")) == 1);
    ud_json_destroy(inspected);
    ud_free(inspection);
    OK(ud_execution_destroy(e));
    CHECK(writes == 0);
    OK(ud_context_destroy(ctx));
    puts("Native designer preview: real CSV/XLSX, atomic errors, source bounds, stable ports and "
         "no writes passed");
    return 0;
}
