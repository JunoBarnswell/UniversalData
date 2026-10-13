import { PROTOCOL_VERSION } from './version';
export type Configuration = Record<string, unknown>;
export interface PropertySchema { type?: string; title?: string; enum?: unknown[]; default?: unknown; properties?: Record<string, PropertySchema>; required?: string[]; }
export interface Component { id: string; name: string; category: number; inputs: number; outputs: number; dynamicPorts: boolean; errorOutput: number; configurationSchema: PropertySchema; }
export interface FlowComponent { id: string; component: string; config: Configuration; }
export interface FlowPath { from: string; output: number; to: string; input: number; }
export interface TaskDefinition { id: string; kind: string; config: Configuration; }
export interface Constraint { from: string; to: string; on: 'success' | 'failure' | 'completion'; }
export interface Connection { id: string; driver: string; resource: string; credentialReference?: string; }
export interface PackageDocument { version: number; id: string; connections?: Connection[]; controlFlow: { tasks: TaskDefinition[]; constraints: Constraint[] }; dataFlows: { id: string; components: FlowComponent[]; paths: FlowPath[] }[];
  layout?: { version: number; nodes: { flow: string; id: string; x: number; y: number }[]; viewports: { flow: string; x: number; y: number; zoom: number }[] }; }
export interface RunState { id: string; packageId: string; packageRevision: string; state: string; executionId: string; result: number; timestampMs: number; debug?: { previewRows: number; breakpoints: string[] }; }
export interface CatalogDocument<T> { id: string; revision: number | bigint; document: T; }
export const emptyPackage = (): PackageDocument => ({ version: PROTOCOL_VERSION, id: 'new-package', connections: [], controlFlow: { tasks: [{ id: 'run-main', kind: 'dataFlow', config: { pipeline: 'main' } }], constraints: [] }, dataFlows: [{ id: 'main', components: [], paths: [] }], layout: { version: PROTOCOL_VERSION, nodes: [], viewports: [] } });
export const stateName = (state: string) => ({ Queued: '已入队', Claimed: '准备中', Running: '运行中', Succeeded: '已完成', Failed: '失败', Cancelled: '已取消', RecoveryRequired: '需要恢复' }[state] ?? state);
export function defaults(schema: PropertySchema): Configuration { return Object.fromEntries(Object.entries(schema.properties ?? {}).filter(([, value]) => value.default !== undefined).map(([key, value]) => [key, structuredClone(value.default)])); }
export function ports(component: Component, config: Configuration) {
  let inputs = component.inputs, outputs = component.outputs;
  if (component.dynamicPorts) {
    if (Array.isArray(config.outputs)) outputs = config.outputs.length;
    if (Array.isArray(config.inputs)) inputs = config.inputs.length;
    if (component.id === 'split') outputs = Array.isArray(config.branches) ? config.branches.length + 2 : 3;
  }
  return { inputs, outputs, error: component.id === 'split' ? outputs - 1 : component.errorOutput };
}
