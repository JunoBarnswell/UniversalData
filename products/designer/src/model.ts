import { PROTOCOL_VERSION } from './version';
export type Configuration = Record<string, unknown>;
export interface PropertySchema { type?: string | string[]; title?: string; description?: string; enum?: unknown[]; default?: unknown; properties?: Record<string, PropertySchema>; required?: string[]; items?: PropertySchema; additionalProperties?: boolean | PropertySchema; oneOf?: PropertySchema[]; anyOf?: PropertySchema[]; minimum?: number; maximum?: number; minItems?: number; maxItems?: number; }
export interface Component { id: string; name: string; category: number; inputs: number; outputs: number; dynamicPorts: boolean; structuralControl?: boolean; children?: boolean; errorOutput: number; configurationSchema: PropertySchema; }
export interface FlowComponent { id: string; component: string; config: Configuration; }
export interface FlowPath { from: string; output: number; to: string; input: number; }
export interface TaskDefinition { id: string; kind: string; config: Configuration; children?: ControlGraph; }
export interface ControlGraph { tasks: TaskDefinition[]; constraints: Constraint[]; }
export interface Constraint { from: string; to: string; on: 'success' | 'failure' | 'completion'; }
export interface Connection { id: string; driver: string; resource: string; credentialReference?: string; }
export interface PackageDocument { version: number; id: string; connections?: Connection[]; controlFlow: ControlGraph; dataFlows: { id: string; components: FlowComponent[]; paths: FlowPath[] }[];
  layout?: { version: number; nodes: { flow: string; id: string; x: number; y: number }[]; viewports: { flow: string; x: number; y: number; zoom: number }[] }; }
export interface RunState { id: string; packageId: string; packageRevision: string; state: string; executionId: string; result: number; timestampMs: number; debug?: { previewRows: number; breakpoints: string[] }; }
export interface CatalogDocument<T> { id: string; revision: number | bigint; document: T; }
export const emptyPackage = (): PackageDocument => ({ version: PROTOCOL_VERSION, id: 'new-package', connections: [], controlFlow: { tasks: [{ id: 'run-main', kind: 'dataFlow', config: { pipeline: 'main' } }], constraints: [] }, dataFlows: [{ id: 'main', components: [], paths: [] }], layout: { version: PROTOCOL_VERSION, nodes: [], viewports: [] } });
export const stateName = (state: string) => ({ Queued: '已入队', Claimed: '准备中', Running: '运行中', Succeeded: '已完成', Failed: '失败', Cancelled: '已取消', RecoveryRequired: '需要恢复' }[state] ?? state);
export function defaults(schema: PropertySchema): Configuration { return Object.fromEntries(Object.entries(schema.properties ?? {}).filter(([, value]) => value.default !== undefined).map(([key, value]) => [key, structuredClone(value.default)])); }
export function ports(component: Component) {
  return { inputs: component.inputs, outputs: component.outputs, error: component.errorOutput };
}
export interface DataField { name: string; type: string; nullable: boolean; identity?: string; physicalIdentity?: string; lineage?: string[]; children?: DataField[]; precision?: number; scale?: number; timezone?: string; keyOrder?: number; }
export interface DataSchema { version: number; fields: DataField[]; }
export interface NativePort { id: string; name: string; direction: number; index: number; flags: number; }
export interface BoundNode { id: string; inputCount: number; outputCount: number; ports: NativePort[]; inputs: (DataSchema | null)[]; outputs: DataSchema[]; }
export interface NativePlan { pipelines: { id: string; trees: { nodes: BoundNode[] }[] }[]; }
export const isControlFlow = (flow: string) => flow === 'control' || flow.startsWith('control:');
export function controlGraph(document: PackageDocument, flow: string): ControlGraph {
  let graph = document.controlFlow;
  if (flow.startsWith('control:')) for (const id of JSON.parse(flow.slice(8)) as string[]) {
    const task = graph.tasks.find(task => task.id === id);
    if (!task?.children) throw new Error('控制流不存在');
    graph = task.children;
  }
  return graph;
}
export function updateControl(document: PackageDocument, flow: string, update: (graph: ControlGraph) => ControlGraph): PackageDocument {
  const path: string[] = flow.startsWith('control:') ? JSON.parse(flow.slice(8)) : [];
  const apply = (graph: ControlGraph, depth: number): ControlGraph => depth === path.length ? update(graph) : {
    ...graph, tasks: graph.tasks.map(task => task.id === path[depth] && task.children ? { ...task, children: apply(task.children, depth + 1) } : task),
  };
  return { ...document, controlFlow: apply(document.controlFlow, 0) };
}
export function controlScopes(document: PackageDocument): { id: string; label: string }[] {
  const scopes = [{ id: 'control', label: '控制流' }];
  function walk(graph: ControlGraph, path: string[]) {
    for (const task of graph.tasks) if (task.children) {
      const ids = [...path, task.id]; scopes.push({ id: `control:${JSON.stringify(ids)}`, label: ids.join(' / ') });
      walk(task.children, ids);
    }
  }
  walk(document.controlFlow, []); return scopes;
}
