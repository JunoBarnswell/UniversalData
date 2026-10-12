#ifndef UD_EXPRESSION_H
#define UD_EXPRESSION_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_expression ud_expression;
/* Returns a borrowed scalar. Called during binding and evaluation. Missing
 * names return SCHEMA; callback is serial on the caller's execution thread. */
typedef ud_status (*ud_expression_resolver)(void *, ud_string name, ud_value *);
typedef struct ud_expression_options {
    uint32_t struct_size, abi_version;
    const ud_batch *schema; /* NULL prohibits column expressions. */
    ud_expression_resolver resolve;
    void *user;
} ud_expression_options;
/* Strict JSON AST: {literal: value, type: bool|int64|float64|utf8}, {ref:name},
 * {column:name}, {op:add|sub|mul|eq|ne|lt|le|gt|ge|and|or|not|isnull,args:[...]}.
 * No implicit type coercion. Typed NULL has the explicit type. AST depth <= 64,
 * nodes <= 4096. Constants fold once, including overflow diagnostics. */
UD_API ud_status ud_expression_create(ud_context *, ud_string json, const ud_expression_options *,
                                      ud_expression **);
UD_API ud_type ud_expression_type(const ud_expression *);
UD_API ud_status ud_expression_evaluate(const ud_expression *, const ud_batch *, uint32_t row,
                                        ud_value *);
UD_API ud_status ud_expression_evaluate_batch(const ud_expression *, const ud_batch *,
                                              ud_string output_name, ud_batch **);
UD_API void ud_expression_destroy(ud_expression *);
#ifdef __cplusplus
}
#endif
#endif
