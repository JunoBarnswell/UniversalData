#ifndef UD_EXPRESSION_H
#define UD_EXPRESSION_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_expression ud_expression;
typedef struct ud_registry ud_registry;
/* Returns a borrowed scalar. Called during binding and evaluation. Missing
 * names return SCHEMA; callback is serial on the caller's execution thread. */
typedef ud_status (*ud_expression_resolver)(void *, ud_string name, ud_value *);
typedef ud_status (*ud_expression_field_resolver)(void *, ud_string name, ud_field *);
typedef struct ud_expression_options {
    uint32_t struct_size, abi_version;
    const ud_batch *schema; /* NULL prohibits column expressions. */
    ud_expression_resolver resolve;
    void *user;
    ud_expression_field_resolver resolve_field; /* Required for nonprimitive references. */
    ud_registry *registry; /* Optional pure functions; leased for expression lifetime. */
    uint64_t function_capabilities; /* Additional required function capabilities. */
} ud_expression_options;
/* Strict JSON AST: {literal: value, type: bool|int64|float64|utf8}, {ref:name},
 * {column:name}, {op:add|sub|mul|eq|ne|lt|le|gt|ge|and|or|not|isnull,args:[...]},
 * {function:id,args:[...]}. Functions are pure native Registry descriptors.
 * No implicit type coercion. Typed NULL has the explicit type. AST depth <= 64,
 * nodes <= 4096. Constants fold once, including overflow diagnostics. Float64
 * nonfinite literals use the exact typed JSON strings from ud/format.h. Column
 * comparisons require matching bound metadata, including Decimal scale and
 * recursive shape; vector results preserve the bound field's metadata. */
UD_API ud_status ud_expression_create(ud_context *, ud_string json, const ud_expression_options *,
                                      ud_expression **);
UD_API ud_type ud_expression_type(const ud_expression *);
/* Borrowed bound result metadata, valid until expression destruction. Caller
 * selects the output name. Nullability is conservative. */
UD_API ud_status ud_expression_field(const ud_expression *, ud_field *);
UD_API ud_status ud_expression_evaluate(const ud_expression *, const ud_batch *, uint32_t row,
                                        ud_value *);
UD_API ud_status ud_expression_evaluate_batch(const ud_expression *, const ud_batch *,
                                              ud_string output_name, ud_batch **);
UD_API void ud_expression_destroy(ud_expression *);
/* Registers the native pure scalar catalog atomically in the same Registry. */
UD_API ud_status ud_expression_register_functions(ud_registry *);
#ifdef __cplusplus
}
#endif
#endif
