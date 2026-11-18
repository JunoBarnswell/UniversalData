#include "../check.h"
#include "ud/expression.h"
#include "ud/component.h"
#include "ud/format.h"
#include "ud/sql.h"
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
static const ud_json *property(const ud_json *object, const char *name) {
    return ud_json_find(object, name, (uint32_t)strlen(name));
}
static ud_string fixture_text(const ud_json *object, const char *name) {
    ud_string result = {0};
    ud_json_text(property(object, name), &result.data, &result.length);
    return result;
}
static int64_t fixture_number(const ud_json *object, const char *name) {
    ud_string text = fixture_text(object, name);
    return text.data ? strtoll(text.data, NULL, 10) : 0;
}
static ud_status fixture_status(const ud_json *object, const char *name) {
    ud_string text = fixture_text(object, name);
    if (!text.data || !strcmp(text.data, "ok"))
        return UD_OK;
    if (!strcmp(text.data, "schema"))
        return UD_SCHEMA;
    if (!strcmp(text.data, "parse"))
        return UD_PARSE;
    if (!strcmp(text.data, "truncated"))
        return UD_TRUNCATED;
    if (!strcmp(text.data, "overflow"))
        return UD_OVERFLOW;
    fprintf(stderr, "Unknown documented status %s\n", text.data);
    abort();
}
static int documented_domains(const ud_json *domains) {
    uint32_t seen = 0;
    for (uint32_t i = 0; i < ud_json_size(domains); i++) {
        const ud_json *item = ud_json_at(domains, i);
        ud_string kind = fixture_text(item, "type");
        uint32_t parsed_kind;
        OK(ud_scalar_type_parse(kind, &parsed_kind));
        seen |= UINT32_C(1) << (parsed_kind - 1);
        ud_field field;
        ud_status s = ud_scalar_field(kind, (uint32_t)fixture_number(item, "length"),
                                     (int32_t)fixture_number(item, "precision"),
                                     (int32_t)fixture_number(item, "scale"),
                                     (uint32_t)fixture_number(item, "codePage"), 0, &field);
        ud_status expected = fixture_status(item, "schemaStatus");
        if (s != expected)
            fprintf(stderr, "Domain %u (%s) schema status %d, expected %d\n", i, kind.data, s,
                    expected);
        CHECK(s == expected);
        if (s)
            continue;
        ud_string input = fixture_text(item, "input");
        CHECK(input.data);
        uint32_t repeat = (uint32_t)fixture_number(item, "repeat");
        char *owned = NULL;
        if (repeat) {
            CHECK(input.length == 1);
            owned = malloc(repeat);
            CHECK(owned);
            memset(owned, input.data[0], repeat);
            input = (ud_string){owned, repeat};
        }
        ud_value value;
        expected = fixture_status(item, "status");
        s = ud_value_parse_field(ctx, &field, input.data, input.length, UD_ROUND_EXACT, &value);
        if (s != expected)
            fprintf(stderr, "Domain %u (%s) value status %d, expected %d\n", i, kind.data, s,
                    expected);
        CHECK(s == expected);
        if (property(item, "storedLength")) {
            uint32_t length = (uint32_t)fixture_number(item, "storedLength");
            CHECK(value.text_length == length);
            field.name = "value";
            field.name_length = 5;
            ud_batch *batch = NULL;
            OK(ud_batch_create(ctx, &field, 1, 1, &batch));
            value.text = input.data;
            value.text_length = input.length;
            OK(ud_batch_append(batch, &value, 1));
            OK(ud_batch_get(batch, 0, 0, &value));
            CHECK(value.text_length == length && !memcmp(value.text, input.data, length));
            ud_batch_destroy(batch);
        }
        free(owned);
    }
    CHECK(seen == (UINT32_C(1) << 29) - 1);
    return 0;
}
static int documented_expressions(const ud_json *expressions) {
    CHECK(ud_json_size(expressions) >= 10);
    for (uint32_t i = 0; i < ud_json_size(expressions); i++) {
        const ud_json *item = ud_json_at(expressions, i);
        ud_string source = fixture_text(item, "source");
        CHECK(source.data);
        if (property(item, "reject")) {
            CHECK(!reject(source.data));
            continue;
        }
        ud_expression_options options = {.struct_size = sizeof(options),
                                         .abi_version = UD_ABI_VERSION};
        ud_expression *e = NULL;
        OK(ud_expression_compile(ctx, source, &options, NULL, &e));
        ud_field field;
        ud_value value;
        OK(ud_expression_field(e, &field));
        OK(ud_expression_evaluate(e, NULL, 0, &value));
        ud_string expected = fixture_text(item, "expected");
        ud_string kind = fixture_text(item, "type");
        CHECK(kind.data && ud_scalar_type_name(field.scalar.kind));
        CHECK(!strcmp(kind.data, ud_scalar_type_name(field.scalar.kind)));
        if (!expected.data)
            CHECK(value.is_null);
        else {
            char text[128];
            uint32_t n;
            OK(ud_value_format(&value, text, sizeof(text), &n));
            CHECK(n == expected.length + 1 && !memcmp(text, expected.data, expected.length));
        }
        ud_expression_destroy(e);
    }
    return 0;
}
static int documented_comparisons(const ud_json *comparisons) {
    CHECK(ud_json_size(comparisons) >= 15);
    for (uint32_t i = 0; i < ud_json_size(comparisons); i++) {
        const ud_json *item = ud_json_at(comparisons, i);
        ud_string left = fixture_text(item, "left"), right = fixture_text(item, "right");
        uint32_t flags = (uint32_t)fixture_number(item, "flags");
        uint32_t locale = (uint32_t)fixture_number(item, "locale");
        uint32_t prefix = (uint32_t)fixture_number(item, "repeat");
        ud_field field;
        OK(ud_scalar_field(str(prefix ? "DT_NTEXT" : "DT_WSTR"), prefix ? 0 : 64, 0, 0, 0,
                           locale, &field));
        field.scalar.comparison_flags = flags;
        OK(ud_scalar_field_validate(&field));
        ud_value a = {.type = UD_UTF8, .text = left.data, .text_length = left.length};
        ud_value b = {.type = UD_UTF8, .text = right.data, .text_length = right.length};
        char *x = NULL, *y = NULL;
        if (prefix) {
            x = malloc(prefix + left.length);
            y = malloc(prefix + right.length);
            CHECK(x && y);
            memset(x, 'x', prefix);
            memset(y, 'x', prefix);
            memcpy(x + prefix, left.data, left.length);
            memcpy(y + prefix, right.data, right.length);
            a.text = x;
            a.text_length += prefix;
            b.text = y;
            b.text_length += prefix;
        }
        int expected = (int)fixture_number(item, "result");
        int order = ud_field_value_compare(&field, &a, &field, &b);
        if (expected == 2 ? !order : order != expected)
            fprintf(stderr, "Comparison %s yielded %d, expected %d\n",
                    fixture_text(item, "name").data, order, expected);
        CHECK(expected == 2 ? order != 0 : order == expected);
        CHECK(order == -ud_field_value_compare(&field, &b, &field, &a));
        if (!expected) {
            CHECK(ud_field_value_hash(&field, &a, 123) == ud_field_value_hash(&field, &b, 123));
            CHECK(ud_value_hash(&a, 123) != ud_value_hash(&b, 123));
        }
        free(x);
        free(y);
    }
    ud_field left, right;
    OK(ud_scalar_field(str("DT_WSTR"), 10, 0, 0, 0, 1033, &left));
    right = left;
    left.scalar.comparison_flags = UD_COMPARE_IGNORE_CASE;
    ud_value x = {.type = UD_UTF8, .text = "a", .text_length = 1};
    ud_value y = {.type = UD_UTF8, .text = "A", .text_length = 1};
    CHECK(ud_field_value_compare(&left, &x, &right, &y) > 0);
    CHECK(ud_field_value_compare(&right, &y, &left, &x) < 0);
    left.scalar.comparison_flags = 8;
    CHECK(ud_scalar_field_validate(&left) == UD_SCHEMA);
    left.scalar.comparison_flags = 0;
    left.scalar.locale_id = UINT32_MAX;
    CHECK(ud_scalar_field_validate(&left) == UD_SCHEMA);
    return 0;
}
static int documented_cases(const char *path) {
    FILE *input = fopen(path, "rb");
    CHECK(input);
    char bytes[65536];
    size_t n = fread(bytes, 1, sizeof(bytes), input);
    CHECK(!ferror(input) && feof(input));
    fclose(input);
    ud_json *root = NULL;
    OK(ud_json_parse(ctx, bytes, (uint32_t)n, &root, NULL));
    CHECK(!documented_domains(property(root, "domains")));
    CHECK(!documented_expressions(property(root, "expressions")));
    CHECK(!documented_comparisons(property(root, "comparisons")));
    ud_json_destroy(root);
    return 0;
}
static int native_predicate(void) {
    const ud_field fields[] = {{.name="enabled",.name_length=7,.type=UD_BOOL,.nullable=1},
                               {.name="id",.name_length=2,.type=UD_INT64}};
    ud_batch *schema=NULL, *params=NULL;
    OK(ud_batch_create(ctx,fields,2,0,&schema));
    ud_expression_options options = {.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,
                                     .schema=schema};
    ud_expression *parsed=NULL;
    OK(ud_expression_compile(ctx,str("[enabled] && [id] > (DT_I8)5"),&options,NULL,&parsed));
    ud_string sql = {0};
    OK(ud_expression_sql(parsed,UD_SQL_SQLSERVER,&sql,&params));
    CHECK(!strcmp(sql.data,"(([enabled] = 1) AND ([id] > ?))"));
    ud_value threshold;
    CHECK(ud_batch_rows(params)==1 && ud_batch_columns(params)==1);
    OK(ud_batch_get(params,0,0,&threshold));
    CHECK(threshold.type==UD_INT64 && threshold.integer==5);
    ud_free((void*)sql.data); ud_batch_destroy(params);
    OK(ud_expression_sql(parsed,UD_SQL_POSTGRESQL,&sql,&params));
    CHECK(!strcmp(sql.data,"(\"enabled\" AND (\"id\" > $1))"));
    ud_free((void*)sql.data); ud_batch_destroy(params);
    ud_expression_destroy(parsed); ud_batch_destroy(schema);
    return 0;
}

int main(int argc, char **argv) {
    OK(ud_context_create(NULL, &ctx));
    CHECK(argc == 2);
    CHECK(!native_predicate());
    CHECK(!documented_cases(argv[1]));
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
