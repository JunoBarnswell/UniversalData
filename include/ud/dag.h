#ifndef UD_DAG_H
#define UD_DAG_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_dag ud_dag;
enum { UD_NODE_SOURCE=1, UD_NODE_TRANSFORM=2, UD_NODE_QUALITY=3, UD_NODE_SINK=4 };
typedef struct ud_dag_node {
    uint32_t struct_size, version;
    ud_string id;
    int32_t kind;
    ud_string operation, configuration; /* Configuration is a strict JSON object. */
} ud_dag_node;
typedef struct ud_dag_edge {
    ud_string from, to;
    uint32_t input_port; /* Ports of each consumer are distinct and contiguous 0..N-1. */
} ud_dag_edge;
/* Versioned structured graph contract. Strict JSON root keys: version, nodes,
 * edges, parameters. Invalid dependencies, cycles, duplicate IDs/ports, incoming
 * Source edges, outgoing Sink edges and unreachable paths are rejected.
 * Every path starts at Source and reaches Sink. Maximum 65,536 nodes / edges.
 * Node configs may use {"$parameter":"name"}; declared parameters are typed JSON.
 * This constructs and validates a plan; execution requires the DAG runtime. */
UD_API ud_status ud_dag_create(ud_context *, const char *, uint32_t length, ud_dag **);
/* C builder and JSON use the same validator. Strings/configs are copied. */
UD_API ud_status ud_dag_build(ud_context *, uint32_t version, const ud_dag_node *, uint32_t nodes,
                              const ud_dag_edge *, uint32_t edges, ud_string parameters_json, ud_dag **);
UD_API void ud_dag_destroy(ud_dag *);
UD_API uint32_t ud_dag_nodes(const ud_dag *);
UD_API uint32_t ud_dag_edges(const ud_dag *);
UD_API ud_status ud_dag_node_get(const ud_dag *, uint32_t index, ud_dag_node *);
UD_API ud_status ud_dag_edge_get(const ud_dag *, uint32_t index, ud_dag_edge *);
/* Borrowed topological node indices, length = ud_dag_nodes. */
UD_API ud_status ud_dag_topology(const ud_dag *, const uint32_t **);
UD_API void ud_dag_fingerprint(const ud_dag *, uint8_t out[32]);
/* Owned compact versioned document, released with ud_free. */
UD_API ud_status ud_dag_serialize(const ud_dag *, char **, uint32_t *length);
/* Resolve typed params structurally, without text substitution. Unknown, absent
 * required or wrong-kind values return SCHEMA. Default values are used only for
 * absent parameters. Result has literal configs and no parameter declarations. */
UD_API ud_status ud_dag_bind(const ud_dag *, ud_string values_json, ud_dag **);
#ifdef __cplusplus
}
#endif
#endif
