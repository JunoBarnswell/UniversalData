#include "check.h"
#include "ud/package.h"
#include "../components/execution/journal.h"
#include <string.h>
static ud_status authorize(void *user,ud_string tenant,ud_string resource,int32_t action) {
    (void)user; (void)tenant; (void)resource; (void)action; return UD_OK;
}
int main(int argc,char **argv) {
    CHECK(argc == 2);
    ud_context *memory = NULL; OK(ud_context_create(NULL,&memory));
    ud_string directory = {argv[1],(uint32_t)strlen(argv[1])};
    ud_journal *journal = NULL; OK(ud_journal_open(memory,directory,0,&journal));
    /* A real former header representation in a checksummed Native Journal.
     * No source or destination is run to discover the incompatible contract. */
    const char old[] = "\"0000000000000000000000000000000000000000000000000000000000000000\"";
    ud_journal_record header = {1,{"identity",8},{0},{old,sizeof(old)-1}};
    OK(ud_journal_append(journal,&header,1)); ud_journal_close(journal);
    const char package[] = "{\"version\":12,\"id\":\"contract\",\"controlFlow\":{\"tasks\":[],"
                           "\"constraints\":[]},\"dataFlows\":[]}";
    ud_execution_options options = {.struct_size = sizeof(options),.abi_version = UD_ABI_VERSION,
        .context = {.struct_size = sizeof(ud_context_options),.abi_version = UD_ABI_VERSION,.memory_limit = 16777216},
        .edge_capacity = 1,.max_batch_rows = 1,.max_depth = 32,
        .authorize = authorize,.tenant = {"test",4},.execution_id = {"identity",8},.state_directory = directory,.resume = 1};
    ud_execution *execution = NULL;
    OK(ud_execution_create_json((ud_string){package,sizeof(package)-1},&options,&execution));
    CHECK(ud_execution_execute(execution) == UD_INCOMPATIBLE); OK(ud_execution_destroy(execution));
    CHECK(!ud_memory_used(memory)); OK(ud_context_destroy(memory)); return 0;
}
