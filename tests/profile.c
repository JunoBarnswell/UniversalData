#include "check.h"
#include "ud/profile.h"
#include <string.h>
static ud_status stop(void *user) {
    return ++*(int *)user == 2 ? UD_TRANSIENT : UD_OK;
}
int main(void) {
    ud_context *ctx = NULL;
    OK(ud_context_create(NULL, &ctx));
    ud_field fields[] = {{.name = "x", .name_length = 1, .type = UD_INT64, .nullable = 1},
                         {.name = "text", .name_length = 4, .type = UD_UTF8, .nullable = 1}};
    ud_batch *batch = NULL;
    OK(ud_batch_create(ctx, fields, 2, 4, &batch));
    ud_profile_statistic stats[7];
    int kinds[] = {UD_PROFILE_ROWS,    UD_PROFILE_NULLS,   UD_PROFILE_MINIMUM, UD_PROFILE_MAXIMUM,
                   UD_PROFILE_MINIMUM, UD_PROFILE_MAXIMUM, UD_PROFILE_DISTINCT};
    for (int i = 0; i < 7; i++)
        stats[i] = (ud_profile_statistic){sizeof(stats[i]), UD_ABI_VERSION, kinds[i],
                                          i < 4 || i == 6 ? 0 : 1};
    ud_profile *p = NULL;
    OK(ud_profile_create(ctx, batch, stats, 7, 10, NULL, NULL, &p));
    ud_value value;
    OK(ud_profile_get(p, 0, &value));
    CHECK(value.integer == 0 && !value.is_null);
    OK(ud_profile_get(p, 2, &value));
    CHECK(value.is_null && value.type == UD_INT64);
    ud_value rows[][2] = {
        {{.type = UD_INT64, .integer = 5}, {.type = UD_UTF8, .text = "a", .text_length = 1}},
        {{.type = UD_INT64, .is_null = 1}, {.type = UD_UTF8, .text = "中文", .text_length = 6}},
        {{.type = UD_INT64, .integer = -1}, {.type = UD_UTF8, .text = "z", .text_length = 1}},
        {{.type = UD_INT64, .integer = 9}, {.type = UD_UTF8, .is_null = 1}}};
    for (int i = 0; i < 4; i++)
        OK(ud_batch_append(batch, rows[i], 2));
    OK(ud_profile_add(p, batch));
    OK(ud_profile_add(p, batch));
    ud_batch_destroy(batch);
    batch = NULL;
    OK(ud_profile_get(p, 0, &value));
    CHECK(value.integer == 8);
    OK(ud_profile_get(p, 1, &value));
    CHECK(value.integer == 2);
    OK(ud_profile_get(p, 2, &value));
    CHECK(value.integer == -1);
    OK(ud_profile_get(p, 3, &value));
    CHECK(value.integer == 9);
    OK(ud_profile_get(p, 4, &value));
    CHECK(value.text_length == 1 && value.text[0] == 'a');
    OK(ud_profile_get(p, 5, &value));
    CHECK(value.text_length == 6 && !memcmp(value.text, "中文", 6));
    OK(ud_profile_get(p, 6, &value));
    CHECK(value.integer == 3 && !value.is_null);
    char *report = NULL; uint32_t report_size = 0;
    OK(ud_profile_report(p,0,&report,&report_size));
    CHECK(strstr(report,"HLL1024") && strstr(report,"relativeStandardError") && strstr(report,"SpaceSaving32") && strstr(report,"ExactNativeValueSet"));
    ud_free(report);
    ud_profile_destroy(p);
    OK(ud_batch_create(ctx, fields, 2, 4, &batch));
    for (int i = 0; i < 4; i++)
        OK(ud_batch_append(batch, rows[i], 2));
    OK(ud_profile_create(ctx, batch, stats, 6, 3, NULL, NULL, &p));
    CHECK(ud_profile_add(p, batch) == UD_RESOURCE);
    CHECK(ud_profile_get(p, 0, &value) == UD_INVALID);
    ud_profile_destroy(p);
    int checks = 0;
    OK(ud_profile_create(ctx, batch, stats, 6, 10, stop, &checks, &p));
    CHECK(ud_profile_add(p, batch) == UD_TRANSIENT && checks == 2);
    CHECK(ud_profile_get(p, 0, &value) == UD_INVALID);
    ud_profile_destroy(p);
    ud_profile_statistic reports[6];
    for (uint32_t at = 0; at < 6; at++) reports[at] = (ud_profile_statistic){sizeof(*reports),UD_ABI_VERSION,(int32_t)(UD_PROFILE_TOPK+at),0};
    OK(ud_profile_create(ctx,batch,reports,6,100,NULL,NULL,&p));
    for (uint32_t at = 0; at < 6; at++) { OK(ud_profile_get(p,at,&value)); CHECK(value.type == UD_UTF8 && value.text_length > 0); }
    OK(ud_profile_add(p,batch));
    for (uint32_t at = 0; at < 6; at++) { OK(ud_profile_get(p,at,&value)); CHECK(value.type == UD_UTF8 && value.text_length > 0); }
    ud_profile_destroy(p);
    stats[0].abi_version = 2;
    CHECK(ud_profile_create(ctx, batch, stats, 6, 10, NULL, NULL, &p) == UD_INCOMPATIBLE && !p);
    ud_batch_destroy(batch);
    CHECK(ud_memory_used(ctx) == 0);
    OK(ud_context_destroy(ctx));
    return 0;
}
