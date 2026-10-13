#include "check.h"
#include "../components/execution/journal.h"
#include <string.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
static ud_string text(const char *value) {
    return (ud_string){value, (uint32_t)strlen(value)};
}
int main(int argc, char **argv) {
    CHECK(argc == 3);
    int resume = strcmp(argv[2], "stage") != 0;
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_journal *journal = NULL;
    OK(ud_journal_open(ctx, text(argv[1]), resume, &journal));
    if (!resume) {
        /* Reverse insertion order proves publication follows logical execution order. */
        ud_journal_record facts[] = {
            {18, text("tx/2"), text("tx"),
             text("{\"order\":2,\"records\":[{\"kind\":3,\"key\":\"task\",\"owner\":\"\","
                  "\"payload\":{\"done\":true}},{\"kind\":0,\"key\":\"\",\"owner\":\"task\","
                  "\"payload\":null}]}")},
            {18, text("tx/1"), text("tx"),
             text("{\"order\":1,\"records\":[{\"kind\":2,\"key\":\"task\",\"owner\":\"task\","
                  "\"payload\":{\"begin\":true}}]}")}};
        OK(ud_journal_append(journal, facts, 2));
        CHECK(!ud_journal_find(journal, 3, text("task")));
        _exit(86);
    }
    if (!strcmp(argv[2], "discard")) {
        OK(ud_journal_retire_owner(journal, text("tx")));
        CHECK(!ud_journal_find(journal, 3, text("task")));
        CHECK(!ud_journal_find(journal, 18, text("tx/1")));
    } else {
        CHECK(!strcmp(argv[2], "publish") || !strcmp(argv[2], "complete"));
        OK(ud_journal_publish_owner(journal, 18, text("tx")));
        CHECK(ud_journal_find(journal, 3, text("task")));
        CHECK(!ud_journal_find(journal, 2, text("task")));
        CHECK(ud_journal_find(journal, 18, text("tx/1")));
        if (!strcmp(argv[2], "publish"))
            _exit(86);
        /* Replaying after death between publication and coordinator completion is stable. */
        OK(ud_journal_publish_owner(journal, 18, text("tx")));
        CHECK(!ud_journal_find(journal, 2, text("task")));
        OK(ud_journal_retire_owner(journal, text("tx")));
        CHECK(ud_journal_find(journal, 3, text("task")));
    }
    ud_journal_close(journal);
    OK(ud_context_destroy(ctx));
    return 0;
}
