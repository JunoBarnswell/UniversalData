#include "check.h"
#include "ud/catalog.h"
#include <stdio.h>
#include <string.h>
static ud_status authorize(void *user,ud_string tenant,ud_string resource,int32_t access) {
    (void)tenant;(void)resource;return access == UD_ACCESS_DELETE && *(int *)user ? UD_AUTH : UD_OK;
}
int main(int argc,char **argv) {
    CHECK(argc == 2); ud_context *memory = NULL; OK(ud_context_create(NULL,&memory));
    int denied = 0; ud_catalog *catalog = NULL;
    ud_catalog_options options = {sizeof(options),UD_ABI_VERSION,{argv[1],(uint32_t)strlen(argv[1])},{"tenant-a",8},authorize,&denied,1};
    OK(ud_catalog_open(memory,&options,&catalog));
    const char *document = "{\"version\":10,\"id\":\"run/example\",\"parameters\":\"{\\\"password\\\":\\\"historical-secret-928157\\\"}\",\"state\":\"Failed\",\"timestampMs\":37}";
    ud_catalog_key key = {sizeof(key),UD_ABI_VERSION,UD_CATALOG_AUDIT,{"run/example",11},1};
    OK(ud_catalog_put(catalog,&key,(ud_string){document,(uint32_t)strlen(document)}));
    denied = 1;
    CHECK(ud_catalog_protect_parameters(catalog,&key,(ud_string){"parameters-0123456789abcdef0123456789abcdef",43}) == UD_AUTH);
    denied = 0;
    ud_catalog_key invalid = key; invalid.id = (ud_string){NULL,11};
    CHECK(ud_catalog_protect_parameters(catalog,&invalid,(ud_string){"reference",9}) == UD_INVALID);
    OK(ud_catalog_protect_parameters(catalog,&key,(ud_string){"parameters-0123456789abcdef0123456789abcdef",43}));
    char *json = NULL; uint32_t length = 0;
    OK(ud_catalog_get(catalog,&key,&json,&length));
    CHECK(!strstr(json,"historical-secret") && !strstr(json,"password") && strstr(json,"parametersReference") && strstr(json,"Failed"));
    ud_free(json);
    OK(ud_catalog_protect_parameters(catalog,&key,(ud_string){"parameters-0123456789abcdef0123456789abcdef",43}));
    CHECK(ud_catalog_protect_parameters(catalog,&key,(ud_string){"another-reference",17}) == UD_CORRUPT);
    OK(ud_catalog_secure_compact(catalog)); ud_catalog_close(catalog);
    CHECK(ud_memory_used(memory) == 0); OK(ud_context_destroy(memory)); return 0;
}
