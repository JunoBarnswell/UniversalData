/* Full domains through real MySQL/MariaDB protocol and metadata. */
#define main advanced_fixture_main
#include "advanced_sql.c"
#undef main
int main(int argc, char **argv) {
    CHECK(argc == 3);
    OK(ud_context_create(NULL, &ctx));
    ud_plugin *plugin = NULL;
    OK(ud_plugin_load(ctx, argv[1], 0, &plugin));
    int allowed = 1;
    ud_connection_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION,
                                      .tenant = {"domains", 7}, .resource = {"fixture", 7},
                                      .credential_reference = {"fixture", 7},
                                      .authorize_user = &allowed, .authorize = authorize,
                                      .resolve_user = argv[2], .resolve = resolve};
    ud_connection *connection = NULL;
    OK(ud_connection_open(plugin, &options, &connection));
    OK(command(connection, "DROP TABLE IF EXISTS ud_advanced_domains"));
    OK(command(connection, "CREATE TABLE ud_advanced_domains("
                           "id BIGINT UNSIGNED NOT NULL PRIMARY KEY,elapsed TIME(6) NOT NULL,"
                           "payload JSON NOT NULL,enumeration ENUM('','1','汉字') NOT NULL,"
                           "flags SET('','a','b') NOT NULL)"));
    OK(command(connection, "INSERT INTO ud_advanced_domains VALUES(18446744073709551615,"
                           "'-123:45:56.123456','{\"u\":18446744073709551615,"
                           "\"n\":-9223372036854775808}',3,7)"));
    ud_batch *schema = NULL;
    OK(ud_connection_schema(connection, str("ud_advanced_domains"), str("domains"), &schema));
    CHECK(ud_batch_columns(schema) == 5);
    ud_field fields[5];
    for (uint32_t at = 0; at < 5; at++) OK(ud_batch_field(schema, at, fields + at));
    CHECK(fields[0].type == UD_UINT64 && fields[1].type == UD_DURATION);
    CHECK(fields[3].type == UD_UINT64 && fields[3].dictionary_kind == UD_DICTIONARY_ENUM &&
          fields[3].dictionary_count == 3 && fields[3].dictionary[0].length == 0);
    CHECK(fields[4].type == UD_UINT64 && fields[4].dictionary_kind == UD_DICTIONARY_SET &&
          fields[4].dictionary_count == 3);
    ud_read_options read = {.struct_size = sizeof(read), .abi_version = UD_ABI_VERSION,
                             .batch_size = 8, .sql = {0}};
    read.sql = str("SELECT id,elapsed,payload,CAST(enumeration AS UNSIGNED) AS enumeration,"
                   "CAST(flags AS UNSIGNED) AS flags FROM ud_advanced_domains");
    OK(ud_connection_read_open(connection, &read, schema));
    ud_batch *batch = NULL;
    OK(ud_connection_read_next(connection, &batch));
    CHECK(ud_batch_rows(batch) == 1);
    ud_value value;
    OK(ud_batch_get(batch, 0, 0, &value)); CHECK(value.unsigned_integer == UINT64_MAX);
    OK(ud_batch_get(batch, 0, 1, &value)); CHECK(value.integer == -INT64_C(445556123456));
    OK(ud_batch_get(batch, 0, 3, &value)); CHECK(value.unsigned_integer == 3);
    OK(ud_batch_get(batch, 0, 4, &value)); CHECK(value.unsigned_integer == 7);
    OK(ud_batch_get(batch, 0, 2, &value));
    ud_json *json = NULL;
    OK(ud_json_parse(ctx, value.text, value.text_length, &json, NULL));
    const char *number; uint32_t length;
    OK(ud_json_text(ud_json_find(json, "u", 1), &number, &length));
    CHECK(length == 20 && !memcmp(number, "18446744073709551615", 20));
    ud_json_destroy(json); ud_batch_destroy(batch); ud_batch_destroy(schema);
    OK(ud_connection_read_close(connection));
    OK(ud_connection_close(connection)); OK(ud_plugin_unload(plugin)); OK(ud_context_destroy(ctx));
    puts("Actual unsigned boundary, negative long TIME, JSON numeric range and ENUM/SET dictionaries passed");
    return 0;
}
