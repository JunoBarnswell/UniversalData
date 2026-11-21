#include "ud/operator.h"
#include "../../foundation/internal.h"
/* The operator declaration is queried by direct C binding and by the component
 * descriptor. Runtime field/type proofs add no second configuration grammar. */
ud_status ud_operator_configuration_schema(int32_t kind, ud_string *out) {
    if (!out)
        return UD_INVALID;
    const char *schema = NULL;
    switch (kind) {
    case UD_OPERATOR_EVENT_DEDUPLICATE:
    case UD_OPERATOR_EVENT_JOIN:
#define EVENT_FIELD "{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"}"
#define EVENT_KEYS "{\"type\":\"array\",\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true,\"items\":" EVENT_FIELD "}"
#define EVENT_BUDGET "{\"type\":\"integer\",\"minimum\":4096,\"default\":1048576}"
#define EVENT_INTERVAL "{\"type\":\"integer\",\"minimum\":1,\"maximum\":9223372036854775807}"
        schema = kind == UD_OPERATOR_EVENT_JOIN
                 ? "{\"type\":\"object\",\"properties\":{\"leftKeys\":" EVENT_KEYS
                   ",\"rightKeys\":" EVENT_KEYS ",\"leftTimeField\":" EVENT_FIELD
                   ",\"rightTimeField\":" EVENT_FIELD ",\"withinMicros\":" EVENT_INTERVAL
                   ",\"stateBytes\":" EVENT_BUDGET "},\"required\":[\"leftKeys\",\"rightKeys\","
                   "\"leftTimeField\",\"rightTimeField\",\"withinMicros\"],\"additionalProperties\":false}"
                 : "{\"type\":\"object\",\"properties\":{\"keys\":" EVENT_KEYS
                   ",\"timeField\":" EVENT_FIELD ",\"ttlMicros\":" EVENT_INTERVAL
                   ",\"stateBytes\":" EVENT_BUDGET "},\"required\":[\"keys\",\"timeField\","
                   "\"ttlMicros\"],\"additionalProperties\":false}";
#undef EVENT_INTERVAL
#undef EVENT_BUDGET
#undef EVENT_KEYS
#undef EVENT_FIELD
        break;
    case UD_OPERATOR_EVENT_WINDOW:
        schema = "{\"type\":\"object\",\"properties\":{\"mode\":{\"type\":\"string\",\"enum\":[\"tumbling\",\"sliding\",\"session\"],\"default\":\"tumbling\"},\"timeField\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},\"sizeMicros\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":9223372036854775807,\"default\":60000000},\"slideMicros\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":9223372036854775807,\"default\":60000000},\"gapMicros\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":9223372036854775807,\"default\":30000000},\"allowedLatenessMicros\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":9223372036854775807,\"default\":0},\"maxWindows\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":1000000,\"default\":4096},\"stateBytes\":{\"type\":\"integer\",\"minimum\":4096,\"default\":1048576},\"latePolicy\":{\"enum\":[\"fail\",\"drop\",\"redirect\"],\"default\":\"fail\"},\"emit\":{\"type\":\"string\",\"enum\":[\"final\",\"changes\"],\"default\":\"final\"},\"beforeField\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},\"plan\":{\"type\":\"object\"}},\"required\":[\"timeField\",\"plan\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_CACHE_LOOKUP:
        schema =
            "{\"type\":\"object\",\"properties\":{\"cache\":{\"type\":\"string\",\"minLength\":1,"
            "\"x-role\":\"cache-resource\"},\"keys\":{\"type\":\"array\",\"minItems\":1,"
            "\"maxItems\":16,\"uniqueItems\":true,\"items\":{\"type\":\"integer\",\"minimum\":0}},"
            "\"errorPolicy\":{\"enum\":[\"fail\",\"redirect\",\"ignore\"],\"default\":\"ignore\"},"
            "\"noMatch\":{\"enum\":[\"ignore\",\"output\"],\"default\":\"ignore\"},"
            "\"includeInput\":{\"type\":\"boolean\",\"default\":false}},\"required\":[\"cache\","
            "\"keys\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_SORT:
        schema = "{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"array\",\"items\":{"
                 "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
                 "\"minLength\":1}],\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":16,"
                 "\"uniqueItems\":true},\"descending\":{\"type\":\"boolean\"},"
                 "\"keyDirections\":{\"type\":\"array\",\"items\":{\"type\":\"boolean\"},\"minItems\":1,\"maxItems\":16},"
                 "\"partitions\":{"
                 "\"type\":\"integer\",\"minimum\":1,\"maximum\":64}},\"required\":[\"keys\"],"
                 "\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_AGGREGATE:
        schema = "{\"type\":\"object\",\"properties\":{\"plan\":{\"type\":\"object\"},"
                 "\"partitions\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":64}},\"required\":"
                 "[\"plan\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_JOIN:
        schema =
            "{\"type\":\"object\",\"properties\":{\"leftKeys\":{\"type\":\"array\",\"items\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},"
            "\"rightKeys\":{\"type\":\"array\",\"items\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},\"joinType\":{\"type\":"
            "\"string\",\"enum\":[\"inner\",\"left\",\"right\",\"full\",\"semi\",\"anti\"],"
            "\"default\":\"inner\"},\"rightAliases\":{\"type\":\"object\",\"additionalProperties\":"
            "{\"type\":\"string\",\"minLength\":1}},\"strategy\":{\"type\":\"string\",\"enum\":["
            "\"auto\",\"hash\",\"graceHash\",\"merge\"],\"default\":\"auto\"},\"partitions\":{"
            "\"type\":\"integer\",\"minimum\":1,\"maximum\":64}},\"required\":[\"leftKeys\","
            "\"rightKeys\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_WINDOW:
        schema =
            "{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"array\",\"items\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},"
            "\"partitionKeys\":{\"type\":\"integer\",\"minimum\":0},\"function\":{\"type\":"
            "\"string\",\"enum\":[\"rowNumber\",\"rank\",\"denseRank\",\"lag\",\"lead\",\"first\","
            "\"last\",\"runningSum\",\"movingSum\",\"movingAverage\"]},\"name\":{\"type\":"
            "\"string\",\"minLength\":1},\"column\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":"
            "0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},\"offset\":{"
            "\"type\":\"integer\",\"minimum\":0,\"maximum\":65536,\"default\":1},\"frame\":{"
            "\"type\":\"object\",\"properties\":{\"type\":{\"type\":\"string\",\"enum\":[\"rows\","
            "\"range\"],\"default\":\"rows\"},\"preceding\":{\"type\":\"integer\",\"minimum\":0},"
            "\"following\":{\"type\":\"integer\",\"minimum\":0}},\"required\":[\"preceding\","
            "\"following\"],\"additionalProperties\":false},\"averageRounding\":{\"type\":"
            "\"string\",\"enum\":[\"exact\",\"halfEven\"],\"default\":\"halfEven\"}},\"required\":["
            "\"keys\",\"partitionKeys\",\"function\",\"name\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_MERGE:
        schema = "{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"array\",\"items\":{"
                 "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
                 "\"minLength\":1}],\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":16,"
                 "\"uniqueItems\":true}},\"required\":[\"keys\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_MERGE_JOIN:
        schema =
            "{\"type\":\"object\",\"properties\":{\"leftKeys\":{\"type\":\"array\",\"items\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},"
            "\"rightKeys\":{\"type\":\"array\",\"items\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},\"joinType\":{\"type\":"
            "\"string\",\"enum\":[\"inner\",\"left\",\"right\",\"full\",\"semi\",\"anti\"],"
            "\"default\":\"inner\"},\"rightAliases\":{\"type\":\"object\",\"additionalProperties\":"
            "{\"type\":\"string\",\"minLength\":1}},\"strategy\":{\"type\":\"string\",\"enum\":["
            "\"auto\",\"hash\",\"graceHash\",\"merge\"],\"default\":\"auto\"},\"partitions\":{"
            "\"type\":\"integer\",\"minimum\":1,\"maximum\":64}},\"required\":[\"leftKeys\","
            "\"rightKeys\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_PIVOT:
        schema =
            "{\"type\":\"object\",\"properties\":{\"setKeys\":{\"type\":\"array\",\"items\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},"
            "\"pivotKey\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
            "\"minLength\":1}],\"x-role\":\"field-selector\"},\"valueColumn\":{\"anyOf\":[{"
            "\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":"
            "\"field-selector\"},\"values\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":4096,"
            "\"items\":{\"type\":\"object\",\"properties\":{\"value\":{},\"name\":{\"type\":"
            "\"string\",\"minLength\":1}},\"required\":[\"value\",\"name\"],"
            "\"additionalProperties\":false}},\"errorPolicy\":{\"type\":\"string\",\"enum\":["
            "\"fail\",\"redirect\",\"ignore\"],\"default\":\"fail\"},\"includeInput\":{\"type\":"
            "\"boolean\",\"default\":false},\"duplicate\":{\"type\":\"string\",\"enum\":["
            "\"reject\",\"first\",\"last\",\"sum\",\"count\",\"min\",\"max\"],\"default\":"
            "\"reject\"}},\"required\":[\"setKeys\",\"pivotKey\",\"valueColumn\",\"values\"],"
            "\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_UNPIVOT:
        schema =
            "{\"type\":\"object\",\"properties\":{\"columns\":{\"type\":\"array\",\"minItems\":1,"
            "\"maxItems\":65536,\"items\":{\"type\":\"object\",\"required\":[\"column\",\"key\"],"
            "\"properties\":{\"column\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":"
            "\"string\",\"minLength\":1}]},\"key\":{\"type\":\"string\",\"minLength\":1}},"
            "\"additionalProperties\":false}},\"retain\":{\"type\":\"array\",\"items\":{\"anyOf\":["
            "{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-"
            "role\":\"field-selector\"},\"uniqueItems\":true},\"keyName\":{\"type\":\"string\","
            "\"minLength\":1},\"valueName\":{\"type\":\"string\",\"minLength\":1},\"nulls\":{"
            "\"type\":\"string\",\"enum\":[\"include\",\"skip\"],\"default\":\"include\"}},"
            "\"required\":[\"columns\",\"keyName\",\"valueName\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_ROW_SAMPLING:
        schema = "{\"type\":\"object\",\"properties\":{\"count\":{\"type\":\"integer\",\"minimum\":"
                 "0},\"seed\":{\"type\":\"integer\",\"minimum\":0},\"whenShort\":{\"type\":"
                 "\"string\",\"enum\":[\"all\",\"fail\"],\"default\":\"all\"}},\"required\":["
                 "\"count\",\"seed\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_PERCENTAGE_SAMPLING:
        schema = "{\"type\":\"object\",\"properties\":{\"percentage\":{\"type\":\"number\","
                 "\"minimum\":0,\"maximum\":100},\"seed\":{\"type\":\"integer\",\"minimum\":0}},"
                 "\"required\":[\"percentage\",\"seed\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_SCD:
        schema =
            "{\"type\":\"object\",\"properties\":{\"cache\":{\"type\":\"string\",\"x-role\":"
            "\"cache-resource\"},\"businessKeys\":{\"type\":\"array\",\"items\":{\"anyOf\":[{"
            "\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":"
            "\"field-selector\"},\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},\"mapping\":{"
            "\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"source\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"target\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"change\":{\"type\":\"string\",\"enum\":[\"key\",\"type1\",\"type2\",\"fixed\"]}},"
            "\"required\":[\"source\",\"target\",\"change\"],\"additionalProperties\":false}},"
            "\"current\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
            "\"minLength\":1}],\"x-role\":\"field-selector\"},\"validFrom\":{\"anyOf\":[{\"type\":"
            "\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-"
            "selector\"},\"validTo\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":"
            "\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},\"version\":{\"anyOf\":[{"
            "\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":"
            "\"field-selector\"},\"effectiveTime\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0}"
            ",{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],"
            "\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"},\"lateEvents\":{"
            "\"type\":\"string\",\"enum\":[\"reject\"],\"default\":\"reject\"},\"surrogate\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"dimensionKeys\":{\"type\":\"array\",\"items\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},"
            "\"inferredMember\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":"
            "\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"surrogateSource\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":"
            "\"string\",\"minLength\":1}]}},\"required\":[\"cache\",\"businessKeys\","
            "\"dimensionKeys\",\"mapping\",\"current\",\"validFrom\",\"validTo\",\"surrogate\","
            "\"effectiveTime\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_DEDUPLICATE:
        schema =
            "{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"array\",\"items\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":15,\"uniqueItems\":true},"
            "\"keep\":{\"type\":\"string\",\"enum\":[\"first\",\"last\"],\"default\":\"first\"},"
            "\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],"
            "\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"},\"mode\":{\"type\":"
            "\"string\",\"enum\":[\"key\",\"fullRow\",\"window\"],\"default\":\"key\"},"
            "\"timeColumn\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
            "\"minLength\":1}],\"x-role\":\"field-selector\"},\"windowMicroseconds\":{\"type\":"
            "\"integer\",\"minimum\":1,\"maximum\":9223372036854775807}},\"required\":[],"
            "\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_TOP_N:
        schema =
            "{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"array\",\"items\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":15,\"uniqueItems\":true},"
            "\"partitionKeys\":{\"type\":\"integer\",\"minimum\":0},\"count\":{\"type\":"
            "\"integer\",\"minimum\":0},\"descending\":{\"type\":\"boolean\"},\"errorPolicy\":{"
            "\"type\":\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],\"default\":\"fail\"},"
            "\"includeInput\":{\"type\":\"boolean\"}},\"required\":[\"keys\",\"partitionKeys\","
            "\"count\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_RANK:
        schema =
            "{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"array\",\"items\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":15,\"uniqueItems\":true},"
            "\"partitionKeys\":{\"type\":\"integer\",\"minimum\":0},\"name\":{\"type\":\"string\","
            "\"minLength\":1},\"method\":{\"type\":\"string\",\"enum\":[\"rank\",\"dense\"],"
            "\"default\":\"rank\"},\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\","
            "\"redirect\",\"ignore\"],\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}"
            "},\"required\":[\"keys\",\"partitionKeys\",\"name\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_SURROGATE_KEY:
        schema =
            "{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"array\",\"items\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"minItems\":1,\"maxItems\":15,\"uniqueItems\":true},"
            "\"namespace\":{\"type\":\"string\",\"minLength\":1},\"name\":{\"type\":\"string\","
            "\"minLength\":1},\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\",\"redirect\","
            "\"ignore\"],\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}},"
            "\"required\":[\"keys\",\"namespace\",\"name\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_ALTER_ROW:
        schema = "{\"type\":\"object\",\"properties\":{\"rules\":{\"type\":\"array\",\"minItems\":"
                 "1,\"maxItems\":64,\"items\":{\"type\":\"object\",\"properties\":{\"condition\":{"
                 "\"type\":\"object\"},\"operation\":{\"type\":\"string\",\"enum\":[\"insert\","
                 "\"update\",\"delete\",\"upsert\"]}},\"required\":[\"condition\",\"operation\"],"
                 "\"additionalProperties\":false}},\"errorPolicy\":{\"type\":\"string\",\"enum\":["
                 "\"fail\",\"redirect\",\"ignore\"],\"default\":\"fail\"},\"includeInput\":{"
                 "\"type\":\"boolean\"}},\"required\":[\"rules\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_FLATTEN:
        schema =
            "{\"type\":\"object\",\"properties\":{\"errorPolicy\":{\"type\":\"string\",\"enum\":["
            "\"fail\",\"redirect\",\"ignore\"],\"default\":\"fail\"},\"includeInput\":{\"type\":"
            "\"boolean\"}},\"required\":[],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_EXPLODE:
        schema =
            "{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"name\":{\"type\":\"string\",\"minLength\":1},\"outer\":{\"type\":\"boolean\","
            "\"default\":false},\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\","
            "\"redirect\",\"ignore\"],\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}"
            "},\"required\":[\"column\",\"name\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_PARSE_JSON:
        schema =
            "{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"name\":{\"type\":\"string\",\"minLength\":1},\"schema\":{\"type\":\"object\"},"
            "\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],"
            "\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}},\"required\":["
            "\"column\",\"name\",\"schema\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_JSON_PATH:
        schema =
            "{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"name\":{\"type\":\"string\",\"minLength\":1},\"path\":{\"type\":\"string\","
            "\"minLength\":1},\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\",\"redirect\","
            "\"ignore\"],\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}},"
            "\"required\":[\"column\",\"name\",\"path\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_STANDARDIZE:
        schema =
            "{\"type\":\"object\",\"properties\":{\"columns\":{\"type\":\"array\",\"minItems\":1,"
            "\"items\":{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":"
            "\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-"
            "selector\"},\"name\":{\"type\":\"string\",\"minLength\":1},\"trim\":{\"type\":"
            "\"boolean\"},\"normalization\":{\"type\":\"string\",\"enum\":[\"none\",\"NFC\","
            "\"NFKC\"],\"default\":\"none\"},\"case\":{\"type\":\"string\",\"enum\":[\"none\","
            "\"upper\",\"lower\",\"fold\"],\"default\":\"none\"},\"schema\":{\"type\":\"object\","
            "\"x-role\":\"columnar-schema\"},\"expression\":{\"type\":\"object\",\"x-role\":"
            "\"native-expression\"},\"rounding\":{\"type\":\"string\",\"enum\":[\"exact\","
            "\"halfEven\"],\"default\":\"exact\"},\"dateFormat\":{\"type\":\"string\",\"enum\":["
            "\"iso\",\"yyyy/MM/dd\",\"dd/MM/"
            "yyyy\"],\"default\":\"iso\"},\"decimalSeparator\":{\"type\":\"string\",\"enum\":[\"."
            "\",\",\"],\"default\":\".\"}},\"required\":[\"column\"],\"additionalProperties\":"
            "false}},\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\",\"redirect\","
            "\"ignore\"],\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}},"
            "\"required\":[\"columns\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_REGEX_EXTRACT:
        schema =
            "{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"pattern\":{\"type\":\"string\",\"minLength\":1},\"groups\":{\"type\":\"array\","
            "\"minItems\":1,\"maxItems\":64,\"items\":{\"type\":\"object\",\"properties\":{"
            "\"group\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
            "\"minLength\":1}],\"x-role\":\"field-selector\"},\"name\":{\"type\":\"string\","
            "\"minLength\":1}},\"required\":[\"group\",\"name\"],\"additionalProperties\":false}},"
            "\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],"
            "\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}},\"required\":["
            "\"column\",\"pattern\",\"groups\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_REGEX_REPLACE:
        schema =
            "{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"pattern\":{\"type\":\"string\",\"minLength\":1},\"replacement\":{\"type\":"
            "\"string\"},\"name\":{\"type\":\"string\",\"minLength\":1},\"errorPolicy\":{\"type\":"
            "\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],\"default\":\"fail\"},"
            "\"includeInput\":{\"type\":\"boolean\"}},\"required\":[\"column\",\"pattern\","
            "\"replacement\",\"name\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_TERM_EXTRACT:
        schema =
            "{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"name\":{\"type\":\"string\",\"minLength\":1},\"countName\":{\"type\":\"string\","
            "\"minLength\":1},\"maxTokens\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":65536,"
            "\"default\":1024},\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\","
            "\"redirect\",\"ignore\"],\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}"
            "},\"required\":[\"column\",\"name\",\"countName\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_ASOF_JOIN:
        schema =
            "{\"type\":\"object\",\"properties\":{\"leftKeys\":{\"type\":\"array\",\"minItems\":1,"
            "\"maxItems\":14,\"items\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":"
            "\"string\",\"minLength\":1}]},\"uniqueItems\":true},\"rightKeys\":{\"type\":\"array\","
            "\"minItems\":1,\"maxItems\":14,\"items\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]},\"uniqueItems\":true},"
            "\"leftTime\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
            "\"minLength\":1}]},\"rightTime\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{"
            "\"type\":\"string\",\"minLength\":1}]},\"joinType\":{\"type\":\"string\",\"enum\":["
            "\"inner\",\"left\"],\"default\":\"left\"},\"rightAliases\":{\"type\":\"object\","
            "\"additionalProperties\":{\"type\":\"string\",\"minLength\":1}},\"errorPolicy\":{"
            "\"type\":\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],\"default\":\"fail\"},"
            "\"includeInput\":{\"type\":\"boolean\"},\"tieBreak\":{\"anyOf\":[{\"type\":"
            "\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]}},\"required\":["
            "\"leftKeys\",\"rightKeys\",\"leftTime\",\"rightTime\"],\"additionalProperties\":"
            "false}";
        break;
    case UD_OPERATOR_TEMPORAL_JOIN:
        schema =
            "{\"type\":\"object\",\"properties\":{\"leftKeys\":{\"type\":\"array\",\"minItems\":1,"
            "\"maxItems\":14,\"items\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":"
            "\"string\",\"minLength\":1}]},\"uniqueItems\":true},\"rightKeys\":{\"type\":\"array\","
            "\"minItems\":1,\"maxItems\":14,\"items\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]},\"uniqueItems\":true},"
            "\"leftTime\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
            "\"minLength\":1}]},\"rightTime\":{\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{"
            "\"type\":\"string\",\"minLength\":1}]},\"joinType\":{\"type\":\"string\",\"enum\":["
            "\"inner\",\"left\"],\"default\":\"left\"},\"rightAliases\":{\"type\":\"object\","
            "\"additionalProperties\":{\"type\":\"string\",\"minLength\":1}},\"errorPolicy\":{"
            "\"type\":\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],\"default\":\"fail\"},"
            "\"includeInput\":{\"type\":\"boolean\"},\"rightEnd\":{\"anyOf\":[{\"type\":"
            "\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]}},\"required\":["
            "\"leftKeys\",\"rightKeys\",\"leftTime\",\"rightTime\",\"rightEnd\"],"
            "\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_DIFF_COMPARE:
        schema = "{\"type\":\"object\",\"properties\":{\"leftKeys\":{\"type\":\"array\","
                 "\"minItems\":1,\"maxItems\":14,\"items\":{\"anyOf\":[{\"type\":\"integer\","
                 "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]},\"uniqueItems\":true},"
                 "\"rightKeys\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":14,\"items\":{"
                 "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\","
                 "\"minLength\":1}]},\"uniqueItems\":true},\"errorPolicy\":{\"type\":\"string\","
                 "\"enum\":[\"fail\"],\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}"
                 "},\"required\":[\"leftKeys\",\"rightKeys\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_MULTIWAY_JOIN:
        schema =
            "{\"type\":\"object\",\"properties\":{\"inputs\":{\"type\":\"array\",\"minItems\":2,"
            "\"maxItems\":64,\"uniqueItems\":true,\"items\":{\"type\":\"string\",\"minLength\":1}},"
            "\"joins\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":63,\"items\":{\"type\":"
            "\"object\",\"properties\":{\"leftKeys\":{\"type\":\"array\",\"items\":{\"anyOf\":[{"
            "\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":"
            "\"field-selector\"},\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},"
            "\"rightKeys\":{\"type\":\"array\",\"items\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"minItems\":1,\"maxItems\":16,\"uniqueItems\":true},\"joinType\":{\"type\":"
            "\"string\",\"enum\":[\"inner\",\"left\",\"right\",\"full\",\"semi\",\"anti\"],"
            "\"default\":\"inner\"},\"rightAliases\":{\"type\":\"object\",\"additionalProperties\":"
            "{\"type\":\"string\",\"minLength\":1}},\"strategy\":{\"type\":\"string\",\"enum\":["
            "\"auto\",\"hash\",\"graceHash\",\"merge\"],\"default\":\"auto\"},\"partitions\":{"
            "\"type\":\"integer\",\"minimum\":1,\"maximum\":64},\"input\":{\"type\":\"string\","
            "\"minLength\":1}},\"required\":[\"leftKeys\",\"rightKeys\",\"input\"],"
            "\"additionalProperties\":false}},\"errorPolicy\":{\"type\":\"string\",\"enum\":["
            "\"fail\"],\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\"}},\"required\":"
            "[\"inputs\",\"joins\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_DATA_PROFILE:
        schema =
            "{\"type\":\"object\",\"properties\":{\"maxRows\":{\"type\":\"integer\",\"minimum\":1,"
            "\"maximum\":9223372036854775807,\"default\":9223372036854775807},\"errorPolicy\":{"
            "\"type\":\"string\",\"enum\":[\"fail\"],\"default\":\"fail\"},\"includeInput\":{"
            "\"type\":\"boolean\",\"default\":false}},\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_FUZZY_LOOKUP:
        schema =
            "{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"threshold\":{\"type\":\"number\",\"exclusiveMinimum\":0,\"maximum\":1,\"default\":0."
            "8},\"method\":{\"type\":\"string\",\"enum\":[\"editDistance\",\"jaroWinkler\"],"
            "\"default\":\"jaroWinkler\"},\"maxRows\":{\"type\":\"integer\",\"minimum\":1,"
            "\"maximum\":1000000,\"default\":65536},\"maxIndexEntries\":{\"type\":\"integer\","
            "\"minimum\":1,\"maximum\":16000000},\"maxCandidates\":{\"type\":\"integer\","
            "\"minimum\":1,\"maximum\":16384,\"default\":4096},\"errorPolicy\":{\"type\":"
            "\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],\"default\":\"fail\"},"
            "\"includeInput\":{\"type\":\"boolean\",\"default\":false},\"cache\":{\"type\":"
            "\"string\",\"minLength\":1,\"x-role\":\"cache-resource\"},\"referenceColumn\":{"
            "\"anyOf\":[{\"type\":\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}]"
            ",\"x-role\":\"field-selector\"},\"topK\":{\"type\":\"integer\",\"minimum\":1,"
            "\"maximum\":64,\"default\":1}},\"required\":[\"column\",\"cache\",\"referenceColumn\"]"
            ",\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_FUZZY_GROUPING:
        schema = "{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":"
                 "\"integer\",\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":"
                 "\"field-selector\"},\"threshold\":{\"type\":\"number\",\"exclusiveMinimum\":0,"
                 "\"maximum\":1,\"default\":0.8},\"method\":{\"type\":\"string\",\"enum\":["
                 "\"editDistance\",\"jaroWinkler\"],\"default\":\"jaroWinkler\"},\"maxRows\":{"
                 "\"type\":\"integer\",\"minimum\":1,\"maximum\":1000000,\"default\":65536},"
                 "\"maxIndexEntries\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":16000000},"
                 "\"maxCandidates\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":16384,"
                 "\"default\":4096},\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\"],"
                 "\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\",\"default\":false}},"
                 "\"required\":[\"column\"],\"additionalProperties\":false}";
        break;
    case UD_OPERATOR_TERM_LOOKUP:
        schema =
            "{\"type\":\"object\",\"properties\":{\"column\":{\"anyOf\":[{\"type\":\"integer\","
            "\"minimum\":0},{\"type\":\"string\",\"minLength\":1}],\"x-role\":\"field-selector\"},"
            "\"cache\":{\"type\":\"string\",\"minLength\":1,\"x-role\":\"cache-resource\"},"
            "\"name\":{\"type\":\"string\",\"minLength\":1,\"default\":\"term\"},\"countName\":{"
            "\"type\":\"string\",\"minLength\":1,\"default\":\"frequency\"},\"maxTokens\":{"
            "\"type\":\"integer\",\"minimum\":1,\"maximum\":65536,\"default\":1024},"
            "\"errorPolicy\":{\"type\":\"string\",\"enum\":[\"fail\",\"redirect\",\"ignore\"],"
            "\"default\":\"fail\"},\"includeInput\":{\"type\":\"boolean\",\"default\":false}},"
            "\"required\":[\"column\",\"cache\"],\"additionalProperties\":false}";
        break;
    default:
        return UD_UNSUPPORTED;
    }
    *out = (ud_string){schema, (uint32_t)strlen(schema)};
    return UD_OK;
}
