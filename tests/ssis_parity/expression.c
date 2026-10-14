#include "../check.h"
#include "ud/expression.h"
#include "ud/component.h"
#include "ud/format.h"
#include "../../components/data/internal.h"
#include <math.h>
#include <string.h>
static ud_context *ctx;
static ud_string str(const char *s) {
    return (ud_string){s, (uint32_t)strlen(s)};
}
static int expression(const char *source, const char *expected, ud_type type) {
    ud_expression_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION};
    ud_expression *e = NULL;
    ud_expression_diagnostic d = {0};
    ud_status s = ud_expression_compile(ctx, str(source), &options, &d, &e);
    if (s) {
        fprintf(stderr, "%s: status %d at %u:%u %s\n", source, s, d.start, d.length, d.message);
        return 1;
    }
    ud_value value = {0};
    OK(ud_expression_evaluate(e, NULL, 0, &value));
    CHECK(value.type == type);
    if (!expected)
        CHECK(value.is_null);
    else {
        char buffer[512];
        uint32_t n = 0;
        OK(ud_value_format(&value, buffer, sizeof(buffer), &n));
        if (strcmp(buffer, expected)) {
            fprintf(stderr, "%s: expected %s, got %s\n", source, expected, buffer);
            return 1;
        }
    }
    ud_expression_destroy(e);
    return 0;
}
static int reject(const char *source) {
    ud_expression_options o = {.struct_size = sizeof(o), .abi_version = UD_ABI_VERSION};
    ud_expression *e = NULL;
    ud_expression_diagnostic d = {0};
    CHECK(ud_expression_compile(ctx, str(source), &o, &d, &e) != UD_OK);
    CHECK(!e && d.status);
    return 0;
}
static int roundtrip(const char *source) {
    ud_expression_options o = {.struct_size = sizeof(o), .abi_version = UD_ABI_VERSION};
    ud_expression *e = NULL;
    OK(ud_expression_compile(ctx, str(source), &o, NULL, &e));
    ud_field f = {0};
    ud_value v = {0};
    OK(ud_expression_field(e, &f));
    f.name = "value";
    f.name_length = 5;
    OK(ud_expression_evaluate(e, NULL, 0, &v));
    ud_batch *a = NULL, *b = NULL;
    OK(ud_batch_create(ctx, &f, 1, 1, &a));
    OK(ud_batch_append(a, &v, 1));
    OK(ud_batch_seal(a));
    uint8_t *wire = NULL;
    uint64_t size = 0;
    OK(ud_batch_serialize(a, &wire, &size));
    OK(ud_batch_deserialize(ctx, wire, size, &b));
    ud_free(wire);
    ud_field got = {0};
    ud_value vv = {0};
    OK(ud_batch_field(b, 0, &got));
    CHECK(ud_field_matches(&f, &got, UD_FIELD_PORT));
    OK(ud_batch_get(b, 0, 0, &vv));
    CHECK(ud_value_compare(&v, &vv) == 0 && ud_value_hash(&v, 7) == ud_value_hash(&vv, 7));
    ud_batch_destroy(b);
    b = NULL;
    char *schema = NULL, *rows = NULL;
    uint32_t sn = 0, rn = 0;
    OK(ud_schema_to_json(a, &schema, &sn));
    OK(ud_batch_to_json(a, &rows, &rn));
    OK(ud_batch_from_json_schema(ctx, schema, sn, rows, rn, UD_ROUND_EXACT, &b));
    ud_free(schema);
    ud_free(rows);
    OK(ud_batch_field(b, 0, &got));
    CHECK(ud_field_matches(&f, &got, UD_FIELD_PORT));
    OK(ud_batch_get(b, 0, 0, &vv));
    CHECK(ud_value_compare(&v, &vv) == 0);
    ud_batch_destroy(b);
    b = NULL;
    struct ArrowArray array = {0};
    struct ArrowSchema arrow = {0};
    OK(ud_batch_export_arrow(a, &array, &arrow));
    OK(ud_batch_import_arrow(ctx, &array, &arrow, &b));
    OK(ud_batch_field(b, 0, &got));
    CHECK(ud_field_matches(&f, &got, UD_FIELD_PORT));
    OK(ud_batch_get(b, 0, 0, &vv));
    CHECK(ud_value_compare(&v, &vv) == 0);
    ud_batch_destroy(b);
    if (array.release)
        array.release(&array);
    if (arrow.release)
        arrow.release(&arrow);
    ud_batch_destroy(a);
    ud_expression_destroy(e);
    return 0;
}
int main(void) {
    OK(ud_context_create(NULL, &ctx));
    struct {
        const char *source, *value;
        ud_type type;
    } cases[] = {
        {"+1+2*3", "7", UD_INT64},
        {"(1+2)*3", "9", UD_INT64},
        {"17/3", "5", UD_INT64},
        {"-17%3", "-2", UD_INT64},
        {"1.00 / 8.00", "0.125000", UD_DECIMAL128},
        {"10.00 % 3.00", "1.00", UD_DECIMAL128},
        {"(DT_NUMERIC,18,2)ROUND((DT_NUMERIC,18,2)123.45*(DT_NUMERIC,3,2)0.13,2)", "16.05",
         UD_DECIMAL128},
        {"18446744073709551615UL", "18446744073709551615", UD_UINT64},
        {"-2147483648", "-2147483648", UD_INT64},
        {"(DT_I4)TRUE", "-1", UD_INT64},
        {"(DT_I1)127+0", "127", UD_INT64},
        {"5&3", "1", UD_INT64},
        {"5|2", "7", UD_INT64},
        {"5^3", "6", UD_INT64},
        {"~(DT_UI1)0", "255", UD_UINT64},
        {"5>=3 && !(1==2)", "true", UD_BOOL},
        {"1!=2 && 2<=2 && 3>2 && 1<2", "true", UD_BOOL},
        {"NULL(DT_BOOL) || TRUE", "true", UD_BOOL},
        {"NULL(DT_BOOL) && FALSE", "false", UD_BOOL},
        {"NULL(DT_BOOL) ? 1 : 2", NULL, UD_INT64},
        {"ABS(-17)", "17", UD_INT64},
        {"EXP(0)", "1", UD_FLOAT64},
        {"CEILING(1.25)", "2.00", UD_DECIMAL128},
        {"FLOOR(-1.25)", "-2.00", UD_DECIMAL128},
        {"LN(1)", "0", UD_FLOAT64},
        {"LOG(100)", "2", UD_FLOAT64},
        {"POWER(2,3)", "8", UD_FLOAT64},
        {"ROUND(2.345,2)", "2.350", UD_DECIMAL128},
        {"ROUND(-2.345,2)", "-2.350", UD_DECIMAL128},
        {"SIGN(-3)", "-1", UD_INT64},
        {"SQUARE(3)", "9", UD_FLOAT64},
        {"SQRT(4)", "2", UD_FLOAT64},
        {"CODEPOINT(\"A\")", "65", UD_UINT64},
        {"FINDSTRING(\"a-b-a\",\"a\",2)", "5", UD_INT64},
        {"HEX((DT_I1)-1)", "FF", UD_UTF8},
        {"LEN(\" a  \")", "4", UD_INT64},
        {"LEN(\"\\xD83D\\xDE80\")", "2", UD_INT64},
        {"LEFT(\"abcd\",2)", "ab", UD_UTF8},
        {"LOWER(\"AbC\")", "abc", UD_UTF8},
        {"LTRIM(\"  x  \")", "x  ", UD_UTF8},
        {"REPLACE(\"aba\",\"a\",\"XY\")", "XYbXY", UD_UTF8},
        {"REPLICATE(\"xy\",3)", "xyxyxy", UD_UTF8},
        {"REVERSE(\"abc\")", "cba", UD_UTF8},
        {"RIGHT(\"abcd\",2)", "cd", UD_UTF8},
        {"RTRIM(\"  x  \")", "  x", UD_UTF8},
        {"SUBSTRING(\"abcd\",2,2)", "bc", UD_UTF8},
        {"SUBSTRING(\"abcd\",9,2)", "", UD_UTF8},
        {"TRIM(\"  x  \")", "x", UD_UTF8},
        {"TRIM(\"\\t x \\t\")", "\t x \t", UD_UTF8},
        {"UPPER(\"aBc\")", "ABC", UD_UTF8},
        {"DATEADD(\"month\",1,(DT_DBTIMESTAMP)\"2024-01-31 00:00:00\")",
         "2024-02-29T00:00:00.0000000", UD_DATETIME},
        {"DATEDIFF(\"day\",(DT_DBDATE)\"2024-02-28\",(DT_DBDATE)\"2024-03-01\")", "2", UD_INT64},
        {"DATEPART(\"hour\",(DT_DBTIMESTAMP)\"2026-10-14 12:34:56\")", "12", UD_INT64},
        {"DAY((DT_DBDATE)\"2024-02-29\")", "29", UD_INT64},
        {"MONTH((DT_DBDATE)\"2024-02-29\")", "2", UD_INT64},
        {"YEAR((DT_DBDATE)\"2024-02-29\")", "2024", UD_INT64},
        {"ISNULL(NULL(DT_I4))", "true", UD_BOOL},
        {"REPLACENULL(NULL(DT_WSTR,10),\"x\")", "x", UD_UTF8},
        {"TOKEN(\"|a||b|\",\"|\",2)", "b", UD_UTF8},
        {"TOKENCOUNT(\"|a||b|\",\"|\")", "2", UD_INT64},
        {"(DT_WSTR,4)\"abcdef\"", "abcd", UD_UTF8},
        {"(DT_STR,4,936)\"中文\"", "中文", UD_UTF8},
        {"(DT_WSTR,36)(DT_GUID)\"00112233-4455-6677-8899-AABBCCDDEEFF\"",
         "00112233-4455-6677-8899-AABBCCDDEEFF", UD_UTF8},
        {"\"a\"+\"b\"", "ab", UD_UTF8},
        {"\"a\"==\"a \"", "false", UD_BOOL},
        {"{\"cast\":{\"name\":\"target\",\"nullable\":false,\"type\":\"int64\",\"scalar\":{"
         "\"type\":\"DT_I2\"}},"
         "\"value\":{\"literal\":\"42\",\"type\":\"utf8\"}}",
         "42", UD_INT64}};
    for (uint32_t j = 0; j < sizeof(cases) / sizeof(*cases); j++)
        CHECK(!expression(cases[j].source, cases[j].value, cases[j].type));
    for (unsigned j = 0; j < 500; j++) {
        char source[180], expected[48];
        snprintf(source, sizeof(source), "((DT_I8)%u + 2L*3L) == %uL ? (DT_I8)%u+6L : -1L", j,
                 j + 6, j);
        snprintf(expected, sizeof(expected), "%u", j + 6);
        CHECK(!expression(source, expected, UD_INT64));
    }
    static const char *invalid[] = {"2147483648",
                                    "18446744073709551616UL",
                                    "18446744073709551615UL+1UL",
                                    "1UL+-1L",
                                    "(DT_I1)128",
                                    "1/0",
                                    "1%0",
                                    "SQRT(-1)",
                                    "ROUND(1.2,-1)",
                                    "SUBSTRING(\"a\",0,1)",
                                    "(DT_STR,8,1252)\"中文\"",
                                    "unknown(1)",
                                    "1+",
                                    "\"\\xD800\"",
                                    "(DT_NUMERIC,39,2)1",
                                    "1e40f", "REPLACENULL(NULL(DT_I4),2147483648L)",
                                    "(DT_CY)\"922337203685477.5808\"",
                                    "(DT_DECIMAL,0)\"79228162514264337593543950336\""};
    for (uint32_t j = 0; j < sizeof(invalid) / sizeof(*invalid); j++)
        CHECK(!reject(invalid[j]));
    static const char *types[] = {"DT_BOOL",
                                  "DT_I1",
                                  "DT_I2",
                                  "DT_I4",
                                  "DT_I8",
                                  "DT_UI1",
                                  "DT_UI2",
                                  "DT_UI4",
                                  "DT_UI8",
                                  "DT_R4",
                                  "DT_R8",
                                  "DT_DECIMAL,4",
                                  "DT_NUMERIC,38,7",
                                  "DT_CY",
                                  "DT_DATE",
                                  "DT_DBDATE",
                                  "DT_DBTIME",
                                  "DT_DBTIME2,7",
                                  "DT_DBTIMESTAMP",
                                  "DT_DBTIMESTAMP2,7",
                                  "DT_DBTIMESTAMPOFFSET,7",
                                  "DT_FILETIME",
                                  "DT_STR,20,65001",
                                  "DT_WSTR,20",
                                  "DT_TEXT,65001",
                                  "DT_NTEXT",
                                  "DT_BYTES,16",
                                  "DT_IMAGE",
                                  "DT_GUID"};
    for (uint32_t j = 0; j < 29; j++) {
        char source[96];
        snprintf(source, sizeof(source), "NULL(%s)", types[j]);
        CHECK(!roundtrip(source));
    }
    const char *values[] = {"(DT_DBTIMESTAMP2,7)\"0001-01-01 00:00:00.0000001\"",
                            "(DT_DBTIMESTAMPOFFSET,7)\"9999-12-31 23:59:59.9999999 +0:00\"",
                            "(DT_UI8)18446744073709551615UL",
                            "(DT_STR,10,936)\"中文\"",
                            "(DT_GUID)\"00112233-4455-6677-8899-AABBCCDDEEFF\"",
                            "(DT_NUMERIC,38,7)123456789012345678901234567890.1234567"};
    for (uint32_t j = 0; j < sizeof(values) / sizeof(*values); j++)
        CHECK(!roundtrip(values[j]));
    CHECK(!expression("REPLACENULL((DT_I4)1,2147483648L)", "1", UD_INT64));
    CHECK(!expression("LEN((DT_NTEXT)\"中🚀\")", "6", UD_INT64));
    CHECK(!expression("LEN((DT_TEXT,936)\"中文\")", "4", UD_INT64));
    for (uint32_t j = 0; j < 2; j++) {
        ud_expression *e = NULL;
        ud_expression_options o = {.struct_size = sizeof(o), .abi_version = UD_ABI_VERSION};
        OK(ud_expression_compile(ctx, str(j ? "GETUTCDATE()" : "GETDATE()"), &o, NULL, &e));
        ud_value v;
        OK(ud_expression_evaluate(e, NULL, 0, &v));
        CHECK(v.type == UD_DATETIME && !v.is_null && v.datetime.ticks > 0);
        ud_expression_destroy(e);
    }
    char *catalog = NULL;
    uint32_t length;
    OK(ud_expression_language_catalog(ctx, &catalog, &length));
    ud_json *json = NULL;
    OK(ud_json_parse(ctx, catalog, length, &json, NULL));
    CHECK(ud_json_size(ud_json_find(json, "types", 5)) == 29);
    ud_json_destroy(json);
    ud_free(catalog);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    puts("Native formula/AST, 38 functions plus typed NULL, scalar domains, 100ns dates, "
         "Wire/Arrow/JSON passed; SSIS Golden not executed");
    return 0;
}
