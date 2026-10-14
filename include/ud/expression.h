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
    ud_registry *registry;          /* Optional pure functions; leased for expression lifetime. */
    uint64_t function_capabilities; /* Additional required function capabilities. */
} ud_expression_options;
typedef struct ud_expression_diagnostic {
    uint32_t struct_size, abi_version, start, length;
    int32_t status;
    char message[256];
} ud_expression_diagnostic;
/* A string JSON value or direct formula is parsed as SSIS syntax; an object
 * is the JSON AST frontend. Both bind the identical typed Native IR. */
UD_API ud_status ud_expression_compile(ud_context *, ud_string, const ud_expression_options *,
                                       ud_expression_diagnostic *, ud_expression **);
UD_API ud_status ud_expression_language_catalog(ud_context *, char **, uint32_t *);
UD_API ud_status ud_expression_describe(const ud_expression *, char **, uint32_t *);
/* Strict JSON AST: {literal: value, type: bool|int64|float64|utf8}, {ref:name},
 * {column:name}, {op:add|sub|mul|eq|ne|lt|le|gt|ge|and|or|not|isnull,args:[...]},
 * {function:id,args:[...]}. Functions are pure native Registry descriptors.
 * No implicit type coercion. Typed NULL has the explicit type. AST depth <= 64,
 * nodes <= 4096. Constants fold once, including overflow diagnostics. Float64
 * nonfinite literals use the exact typed JSON strings from ud/format.h. Column
 * One controller per expression; use separate instances for parallel evaluation.
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
/* Exact PostgreSQL Bool/Int64 predicate lowering from the bound Native AST.
 * Caller proves actual source column domains; Unsupported retains Native AST.
 * SQL string and typed parameters are owned by the caller. */
UD_API ud_status ud_expression_sql(ud_expression *,int32_t,ud_string *,ud_batch **);
UD_API void ud_expression_destroy(ud_expression *);
/* Language projections use owned Native batches for argument metadata/data.
 * No JSON or language-specific value interpreter participates in evaluation. */
UD_API ud_status ud_function_argument_schema(ud_context *, const ud_field *, uint32_t, ud_batch **);
UD_API ud_status ud_function_arguments(ud_context *, const ud_field *, const ud_value *, uint32_t,
                                       ud_batch **);
UD_API ud_status ud_function_result(ud_context *, const ud_batch *, const ud_field *, ud_value *);
/* Registers the native pure scalar catalog atomically in the same Registry. */
UD_API ud_status ud_expression_register_functions(ud_registry *);
#ifdef __cplusplus
}
#endif
#endif
