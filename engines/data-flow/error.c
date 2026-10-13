#include "../package/internal.h"
ud_status ud_error_policy(const ud_json *config, const char *key, int fallback, int *out) {
    *out = fallback;
    const ud_json *value = ud_member(config, key);
    if (!value)
        return UD_OK;
    ud_string text;
    UD_TRY(ud_json_string(value, &text));
    *out = ud_named(text, "fail")       ? UD_ERROR_FAIL
           : ud_named(text, "redirect") ? UD_ERROR_REDIRECT
           : ud_named(text, "ignore")   ? UD_ERROR_IGNORE
                                        : -1;
    return *out < 0 ? UD_SCHEMA : UD_OK;
}
