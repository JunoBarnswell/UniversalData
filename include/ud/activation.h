#ifndef UD_ACTIVATION_H
#define UD_ACTIVATION_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_activation ud_activation;
/* Borrowed task frame, valid for its callback/lifecycle only. Root cancellation
 * and parent deadlines are observed without mutating sibling activations. */
UD_API ud_status ud_activation_check(const ud_activation *);
/* Graceful stop: a continuous Source finishes its accepted transaction, then END. */
UD_API int ud_activation_stopping(const ud_activation *);
UD_API ud_status ud_activation_variable(const ud_activation *, ud_string, ud_value *);
UD_API ud_status ud_activation_variable_field(const ud_activation *, ud_string, ud_field *);
UD_API ud_status ud_activation_set_variable(ud_activation *, ud_string, const ud_value *,
                                            const ud_field *provenance);
#ifdef __cplusplus
}
#endif
#endif
