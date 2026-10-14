/* Real participant recovery: each phase runs in a fresh operating-system process. */
#include "check.h"
#include "ud/plugin.h"
#include "ud/sql.h"
#include "ud/format.h"
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
static ud_context *context;
static ud_string string(const char *value) {
    return (ud_string){value, (uint32_t)strlen(value)};
}
static ud_status grant(void *user, ud_string tenant, ud_string resource, int32_t action) {
    (void)user;
    (void)tenant;
    (void)resource;
    (void)action;
    return UD_OK;
}
static ud_status configuration(void *user, ud_context *ctx, ud_string tenant, ud_string resource,
                               ud_string reference, ud_string *out) {
    (void)tenant;
    (void)resource;
    (void)reference;
    FILE *file = fopen(user, "rb");
    if (!file)
        return UD_IO;
    if (fseek(file, 0, SEEK_END)) {
        fclose(file);
        return UD_IO;
    }
    long size = ftell(file);
    rewind(file);
    if (size < 1 || size > 65536) {
        fclose(file);
        return UD_SCHEMA;
    }
    char *data = ud_alloc(ctx, (size_t)size + 1);
    if (!data) {
        fclose(file);
        return UD_RESOURCE;
    }
    size_t count = fread(data, 1, (size_t)size, file);
    fclose(file);
    if (count != (size_t)size) {
        ud_free(data);
        return UD_IO;
    }
    *out = (ud_string){data, (uint32_t)size};
    return UD_OK;
}
static ud_status command(ud_connection *connection, const char *sql) {
    ud_read_options options = {
        .struct_size = sizeof(options), .abi_version = UD_ABI_VERSION, .sql = string(sql)};
    return ud_connection_execute_command(connection, &options, NULL);
}
#undef OK
#define OK(call)                                                                                   \
    do {                                                                                           \
        ud_status status_ = (call);                                                                \
        if (status_) {                                                                             \
            fprintf(stderr, "participant status=%d line=%d: %s\n", status_, __LINE__,              \
                    ud_error(context));                                                            \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)
int main(int argc,char **argv) {
    CHECK(argc==3);OK(ud_context_create(NULL,&context));
    ud_plugin *plugin=NULL;OK(ud_plugin_load(context,argv[1],0,&plugin));
    ud_connection_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,
        .tenant={"schema-fixture",14},.resource={"postgresql/schema-fixture",25},
        .credential_reference={"fixture",7},.authorize=grant,.resolve_user=argv[2],.resolve=configuration};
    ud_connection *connection=NULL;OK(ud_connection_open(plugin,&options,&connection));
    const char *table="ud_schema_identity_fixture";
    (void)command(connection,"DROP TABLE ud_schema_identity_fixture");
    OK(command(connection,"CREATE TABLE ud_schema_identity_fixture(id BIGINT PRIMARY KEY,old_label TEXT NOT NULL)"));
    OK(command(connection,"INSERT INTO ud_schema_identity_fixture VALUES(1,'original')"));
    ud_batch *before=NULL,*actual=NULL,*bound=NULL,*mapped=NULL;
    OK(ud_connection_schema(connection,string(table),string("package-a"),&before));
    ud_field original={0},renamed={0};OK(ud_batch_field(before,1,&original));
    CHECK(original.physical_identity.high || original.physical_identity.low);
    OK(command(connection,"ALTER TABLE ud_schema_identity_fixture RENAME COLUMN old_label TO new_label"));
    OK(ud_connection_schema(connection,string(table),string("package-b"),&actual));
    OK(ud_batch_field(actual,1,&renamed));
    CHECK(!ud_id_compare(original.physical_identity,renamed.physical_identity));
    CHECK(ud_id_compare(original.identity,renamed.identity));
    OK(ud_schema_bind_identity(context,before,actual,&bound));
    OK(ud_batch_field(bound,1,&renamed));CHECK(!ud_id_compare(original.identity,renamed.identity));
    ud_read_options read={.struct_size=sizeof(read),.abi_version=UD_ABI_VERSION,.batch_size=32,
        .sql=string("SELECT id,new_label FROM ud_schema_identity_fixture")};
    OK(ud_connection_read_open(connection,&read,bound));
    ud_batch *rows=NULL;OK(ud_connection_read_next(connection,&rows));
    OK(ud_schema_map(context,rows,before,string("{\"policy\":\"compatible\"}"),&mapped));
    ud_value value={0};OK(ud_batch_get(mapped,0,1,&value));CHECK(value.text_length==8 && !memcmp(value.text,"original",8));
    ud_batch_destroy(rows);ud_batch_destroy(mapped);mapped=NULL;OK(ud_connection_read_close(connection));
    ud_batch_destroy(actual);actual=NULL;ud_batch_destroy(bound);bound=NULL;
    OK(command(connection,"ALTER TABLE ud_schema_identity_fixture DROP COLUMN new_label"));
    OK(command(connection,"ALTER TABLE ud_schema_identity_fixture ADD COLUMN old_label TEXT NOT NULL DEFAULT 'replacement'"));
    OK(ud_connection_schema(connection,string(table),string("package-a"),&actual));
    OK(ud_schema_bind_identity(context,before,actual,&bound));OK(ud_batch_field(bound,1,&renamed));
    CHECK(ud_id_compare(original.physical_identity,renamed.physical_identity) && ud_id_compare(original.identity,renamed.identity));
    CHECK(ud_schema_map(context,bound,before,string("{\"policy\":\"compatible\"}"),&mapped)==UD_SCHEMA && !mapped);
    OK(command(connection,"DROP TABLE ud_schema_identity_fixture"));
    ud_batch_destroy(bound);ud_batch_destroy(actual);ud_batch_destroy(before);
    OK(ud_connection_close(connection));OK(ud_plugin_unload(plugin));OK(ud_context_destroy(context));
    puts("Real PostgreSQL DDL: rename and cross-Package physical identity, correct row projection, drop/re-add incarnation refusal passed");return 0;
}
