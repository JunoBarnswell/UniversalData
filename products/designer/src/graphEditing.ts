import { PROTOCOL_VERSION } from "./version";
import {
  controlGraph,
  isControlFlow,
  updateControl,
  type Configuration,
  type FlowComponent,
  type PackageDocument,
  type NativePort,
  portIndex,
} from "./model";
export function removeGraphNodes(
  document: PackageDocument,
  flow: string,
  ids: ReadonlySet<string>,
): PackageDocument {
  const control = isControlFlow(flow),
    scope: string[] = flow.startsWith("control:")
      ? JSON.parse(flow.slice(8))
      : [];
  const retained = (candidate: string) =>
    !control ||
    !candidate.startsWith("control:") ||
    (() => {
      const path: string[] = JSON.parse(candidate.slice(8));
      return (
        !scope.every((part, index) => path[index] === part) ||
        !ids.has(path[scope.length])
      );
    })();
  const next = control
    ? updateControl(document, flow, (graph) => ({
        ...graph,
        tasks: graph.tasks.filter((task) => !ids.has(task.id)),
        constraints: graph.constraints.filter(
          (edge) => !ids.has(edge.from) && !ids.has(edge.to),
        ),
      }))
    : {
        ...document,
        dataFlows: document.dataFlows.map((graph) =>
          graph.id === flow
            ? {
                ...graph,
                components: graph.components.filter(
                  (node) => !ids.has(node.id),
                ),
                paths: graph.paths.filter(
                  (path) => !ids.has(path.from) && !ids.has(path.to),
                ),
              }
            : graph,
        ),
      };
  return {
    ...next,
    layout: document.layout
      ? {
          ...document.layout,
          nodes: document.layout.nodes.filter(
            (node) =>
              retained(node.flow) && (node.flow !== flow || !ids.has(node.id)),
          ),
          viewports: document.layout.viewports.filter((view) =>
            retained(view.flow),
          ),
        }
      : undefined,
  };
}
export function duplicateGraphNodes(
  document: PackageDocument,
  flow: string,
  ids: ReadonlySet<string>,
  bound: Map<string, NativePort[]>,
): { document: PackageDocument; ids: string[] } {
  const source = isControlFlow(flow)
    ? controlGraph(document, flow).tasks
    : (document.dataFlows.find((graph) => graph.id === flow)?.components ?? []);
  const names = new Map(
    source
      .filter((node) => ids.has(node.id))
      .map((node) => [node.id, `node-${crypto.randomUUID()}`]),
  );
  if (isControlFlow(flow)) return { document, ids: [] };
  const graph = document.dataFlows.find((graph) => graph.id === flow)!;
  const nodes = graph.components
    .filter((node) => names.has(node.id))
    .map((node) => ({ ...structuredClone(node), id: names.get(node.id)! }));
  const internal = graph.paths.filter(
    (path) => names.has(path.from) && names.has(path.to),
  );
  if (
    internal.some(
      (path) =>
        portIndex(bound.get(path.from) ?? [], path.output, 2) < 0 ||
        portIndex(bound.get(path.to) ?? [], path.input, 1) < 0,
    )
  )
    return { document, ids: [] };
  const paths = internal.map((path) => ({
    ...path,
    output: portIndex(bound.get(path.from) ?? [], path.output, 2),
    input: portIndex(bound.get(path.to) ?? [], path.input, 1),
    from: names.get(path.from)!,
    to: names.get(path.to)!,
  }));
  const layout = document.layout ?? {
    version: PROTOCOL_VERSION,
    nodes: [],
    viewports: [],
  };
  return {
    document: {
      ...document,
      dataFlows: document.dataFlows.map((value) =>
        value.id === flow
          ? {
              ...value,
              components: [...value.components, ...nodes],
              paths: [...value.paths, ...paths],
            }
          : value,
      ),
      layout: {
        ...layout,
        nodes: [
          ...layout.nodes,
          ...nodes.map((node, index) => {
            const original = [...names].find(
                ([, next]) => next === node.id,
              )![0],
              position = layout.nodes.find(
                (node) => node.id === original && node.flow === flow,
              );
            return {
              flow,
              id: node.id,
              x: (position?.x ?? index * 300) + 60,
              y: (position?.y ?? 140) + 70,
              label: position?.label,
            };
          }),
        ],
      },
    },
    ids: nodes.map((node) => node.id),
  };
}
export function templateNodes(kind: "derive-split" | "dimensions"): {
  nodes: FlowComponent[];
  paths: { from: string; output: number; to: string; input: number }[];
  labels: Record<string, string>;
} {
  const node = (component: string, config: Configuration): FlowComponent => ({
    id: `node-${crypto.randomUUID()}`,
    component,
    config,
  });
  const aggregate = () =>
    node("aggregate", {
      plan: {
        version: PROTOCOL_VERSION,
        group: [],
        aggregates: [{ name: "row_count", function: "count", column: "*" }],
      },
    });
  if (kind === "derive-split") {
    const derive = node("derive", { columns: [] }),
      split = node("split", {
        branches: [
          { id: crypto.randomUUID(), name: "业务分支", condition: "" },
        ],
        default: "其他",
        errorPolicy: "fail",
      });
    return {
      nodes: [derive, split],
      paths: [{ from: derive.id, output: 0, to: split.id, input: 0 }],
      labels: { [derive.id]: "字段处理", [split.id]: "业务分类" },
    };
  }
  const multicast = node("multicast", { outputs: ["维度一", "维度二"] }),
    a = aggregate(),
    b = aggregate();
  return {
    nodes: [multicast, a, b],
    paths: [
      { from: multicast.id, output: 0, to: a.id, input: 0 },
      { from: multicast.id, output: 1, to: b.id, input: 0 },
    ],
    labels: {
      [multicast.id]: "维度数据复制",
      [a.id]: "维度一统计",
      [b.id]: "维度二统计",
    },
  };
}
