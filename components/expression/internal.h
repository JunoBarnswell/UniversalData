#ifndef UD_EXPRESSION_INTERNAL_H
#define UD_EXPRESSION_INTERNAL_H
#include "ud/expression.h"
#include "../data/internal.h"
#include "../contracts/registry_internal.h"
enum { LITERAL, REFERENCE, COLUMN, OPERATION, FUNCTION };
enum {
    ADD,
    SUB,
    MUL,
    EQ,
    NE,
    LT,
    LE,
    GT,
    GE,
    AND,
    OR,
    NOT,
    ISNULL,
    DIV,
    MOD,
    NEG,
    BITAND,
    BITOR,
    BITXOR,
    BITNOT,
    CONDITIONAL,
    CAST,
    POS
};
typedef struct expression_node {
    int kind, op, constant, has_columns;
    ud_type type;
    ud_field field, operand_field;
    uint32_t path[32], path_length, column, span_start, span_length, argument_count;
    ud_string name;
    int name_owned, field_owned, value_owned;
    ud_value value;
    ud_batch *literal_owner;
    ud_component_descriptor function;
    struct expression_node *args[16];
} expression_node;
struct ud_expression {
    ud_context *ctx;
    ud_json *document;
    expression_node *root;
    ud_expression_resolver resolve;
    ud_expression_field_resolver resolve_field;
    void *user;
    ud_batch *schema;
    uint32_t nodes;
    ud_registry *registry;
    int registry_owned, registry_leased;
    uint64_t function_capabilities;
    ud_expression_diagnostic diagnostic;
    ud_string source;
};
expression_node *ud_expr_node(ud_expression *, int, uint32_t, uint32_t);
void ud_expr_node_free(expression_node *);
ud_status ud_expr_bind_reference(ud_expression *, expression_node *, ud_string, int);
ud_status ud_expr_bind_literal(ud_expression *, expression_node *, const ud_field *,
                               const ud_value *);
ud_status ud_expr_finish(ud_expression *, expression_node *);
ud_status ud_expr_parse(ud_expression *, ud_string, expression_node **);
ud_status ud_expr_error(ud_expression *, ud_status, uint32_t, uint32_t, const char *);
#endif
