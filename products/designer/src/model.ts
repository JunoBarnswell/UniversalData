import { PROTOCOL_VERSION } from "./version";
export type Configuration = Record<string, unknown>;
export interface PropertySchema {
  "x-role"?:string;
  type?: string | string[];
  title?: string;
  description?: string;
  enum?: unknown[];
  default?: unknown;
  properties?: Record<string, PropertySchema>;
  required?: string[];
  items?: PropertySchema;
  additionalProperties?: boolean | PropertySchema;
  oneOf?: PropertySchema[];
  anyOf?: PropertySchema[];
  minimum?: number;
  maximum?: number;
  minItems?: number;
  maxItems?: number;
}
export interface Component {
  minimumArguments?: number;
  maximumArguments?: number;
  id: string;
  name: string;
  category: number;
  inputs: number;
  outputs: number;
  dynamicPorts: boolean;
  previewSafe?: boolean;
  continuous?:boolean;
  streamSafe?:boolean;
  structuralControl?: boolean;
  children?: boolean;
  errorOutput: number;
  configurationSchema: PropertySchema;
}
export interface FlowComponent {
  id: string;
  component: string;
  config: Configuration;
}
export interface FlowPath {
  from: string;
  output: number | string;
  to: string;
  input: number | string;
}
export interface DataFlowParallelism {
  workerProcesses?: number;
  consumerLanesPerWorker?: number;
  threadsPerWorker?: number;
}
export type DataFlowDelivery =
  | { mode: "atLeastOnce" }
  | { mode: "kafkaTransaction"; checkpointTopic: string; checkpointPartition?: number };
export interface DataFlow {
  id: string;
  components: FlowComponent[];
  paths: FlowPath[];
  parallelism?: DataFlowParallelism;
  delivery?: DataFlowDelivery;
}
export interface TaskDefinition {
  id: string;
  kind: string;
  config: Configuration;
  children?: ControlGraph;
}
export interface ControlGraph {
  tasks: TaskDefinition[];
  constraints: Constraint[];
}
export interface Constraint {
  from: string;
  to: string;
  on: "success" | "failure" | "completion";
}
export interface Connection {
  id: string;
  driver: string;
  resource: string;
  credentialReference?: string;
  oauth2?: { tokenEndpoint: string; clientId: string; credentialReference: string; scope?: string;
    authentication: "basic" | "post" };
}
export interface PackageDocument {
  caches?: Configuration[];
  variables?: Record<string, unknown>;
  parameters?: Record<string, unknown>;
  version: number;
  id: string;
  connections?: Connection[];
  controlFlow: ControlGraph;
  dataFlows: DataFlow[];
  layout?: {
    label?: string;
    version: number;
    nodes?: { flow: string; id: string; x: number; y: number; label?: string }[];
    viewports?: { flow: string; x: number; y: number; zoom: number }[];
  };
}
export interface RunState {
  attempt?:number;
  id: string;
  packageId: string;
  packageRevision: string;
  state: string;
  requiresJournal?: boolean;
  queueReason?: string;
  queueWaitMs?: number;
  executionId: string;
  result: number;
  failure?: { result: number; phase: string; errorType: string; detail?: {
    source: number; cause: number; nativeCode?: number; sqlState?: string; httpStatus?: number; exitCode?: number;
  } };
  timestampMs: number;
  debug?: {
    previewRows: number;
    breakpoints: string[];
    definitionBreakpoints?: string[][];
  };
}
export const failurePhase = (phase: string) =>
  ({
    "worker.catalog": "打开执行记录",
    "package.bind": "绑定发布版本",
    "catalog.replay": "读取恢复记录",
    "native.create": "创建执行",
    "native.debug": "设置调试",
    "control.bind": "建立执行控制",
    "catalog.ready": "保存就绪状态",
    "native.execute": "处理数据",
    dispatch: "派发任务",
  })[phase] ?? phase;
export interface CatalogDocument<T> {
  id: string;
  revision: number | bigint;
  document: T;
}
export const emptyPackage = (): PackageDocument => ({
  version: PROTOCOL_VERSION,
  id: "new-package",
  connections: [],
  controlFlow: {
    tasks: [{ id: "run-main", kind: "dataFlow", config: { pipeline: "main" } }],
    constraints: [],
  },
  dataFlows: [{ id: "main", components: [], paths: [] }],
  layout: { version: PROTOCOL_VERSION, nodes: [], viewports: [] },
});
export const stateName = (state: string) =>
  ({
    Queued: "已入队",
    Claimed: "准备中",
    Running: "运行中",
    Succeeded: "已完成",
    Failed: "失败",
    Cancelled: "已取消",
    Stopped: "已停止（排空完成）",
    RecoveryRequired: "需要恢复",
  })[state] ?? state;
export const executionStateName = (
  run: Pick<RunState, "state" | "requiresJournal">,
) =>
  run.state === "RecoveryRequired" && run.requiresJournal === false
    ? "流式执行已停止（不可恢复）"
    : stateName(run.state);
export function defaults(schema: PropertySchema): Configuration {
  return Object.fromEntries(
    Object.entries(schema.properties ?? {})
      .filter(([, value]) => value.default !== undefined)
      .map(([key, value]) => [key, structuredClone(value.default)]),
  );
}
export function ports(component: Component) {
  return {
    inputs: component.inputs,
    outputs: component.outputs,
    error: component.errorOutput,
  };
}
export interface DataField {
  scalar?: {
    type: string;
    length: number;
    codePage: number;
    localeId: number;
    comparisonFlags: number;
    fractionalDigits: number;
  };
  name: string;
  type: string;
  nullable: boolean;
  identity?: string;
  physicalIdentity?: string;
  lineage?: string[];
  children?: DataField[];
  precision?: number;
  scale?: number;
  timezone?: string;
  keyOrder?: number;
}
export interface DataSchema {
  version: number;
  fields: DataField[];
}
export interface NativePort {
  rows?: number;
  id: string;
  name: string;
  direction: number;
  index: number;
  flags: number;
}
export interface BoundNode {
  result?: number;
  processedRows?: number;
  producedRows?: number;
  rejectedRows?: number;
  callbackMs?: number;
  state?: number;
  id: string;
  inputCount: number;
  outputCount: number;
  ports: NativePort[];
  inputs: (DataSchema | null)[];
  outputs: DataSchema[];
}
export interface NativePlan {
  pipelines: { id: string; trees: { nodes: BoundNode[] }[] }[];
  tasks?: { activation: string; httpStatus?: number; responseHeaders?: string; responseHeadersOmitted?: boolean }[];
}
export const isControlFlow = (flow: string) =>
  flow === "control" || flow.startsWith("control:");
export function controlGraph(
  document: PackageDocument,
  flow: string,
): ControlGraph {
  let graph = document.controlFlow;
  if (flow.startsWith("control:"))
    for (const id of JSON.parse(flow.slice(8)) as string[]) {
      const task = graph.tasks.find((task) => task.id === id);
      if (!task?.children) throw new Error("控制流不存在");
      graph = task.children;
    }
  return graph;
}
export function updateControl(
  document: PackageDocument,
  flow: string,
  update: (graph: ControlGraph) => ControlGraph,
): PackageDocument {
  const path: string[] = flow.startsWith("control:")
    ? JSON.parse(flow.slice(8))
    : [];
  const apply = (graph: ControlGraph, depth: number): ControlGraph =>
    depth === path.length
      ? update(graph)
      : {
          ...graph,
          tasks: graph.tasks.map((task) =>
            task.id === path[depth] && task.children
              ? { ...task, children: apply(task.children, depth + 1) }
              : task,
          ),
        };
  return { ...document, controlFlow: apply(document.controlFlow, 0) };
}
export function controlScopes(
  document: PackageDocument,
): { id: string; label: string }[] {
  const scopes = [{ id: "control", label: "控制流" }];
  function walk(graph: ControlGraph, path: string[]) {
    for (const task of graph.tasks)
      if (task.children) {
        const ids = [...path, task.id];
        scopes.push({
          id: `control:${JSON.stringify(ids)}`,
          label: ids.join(" / "),
        });
        walk(task.children, ids);
      }
  }
  walk(document.controlFlow, []);
  return scopes;
}

export interface PreviewRow {
  identity: {
    id?: string;
    source?: string;
    ordinal?: number;
    [key: string]: unknown;
  };
  fields: { present: boolean; value?: unknown }[];
}
export interface PreviewEdge {
  portId?: string;
  portName?: string;
  pipeline: string;
  component: string;
  port: number;
  totalRows: number;
  schema?: DataSchema;
  rows?: PreviewRow[];
  truncated?: boolean;
}
export interface PreviewResult {
  result: number;
  sourceLimit: number;
  sampled: boolean;
  snapshot: { preview: PreviewEdge[] };
  diagnostics: {
    pipelines?: {
      id: string;
      trees: { nodes: BoundNode[] }[];
      nodes?: BoundNode[];
    }[];
    [key: string]: unknown;
  };
}
export interface Deployment {
  id: string;
  packageId: string;
  packageRevision: string;
  deploymentRevision: string;
  enabled: boolean;
}
export interface Resources {
  fileRoots: string[];
  readOnlyRoots: string[];
  credentialReferences: string[];
  resources: string[];
  testResources: string[];
}
export interface ConnectorDescriptor {
  path: string;
  id: string;
  version: string;
  result: number;
  capabilities: number;
}
export const MESSAGE_SUBSCRIBE = 2 ** 26;
export const MESSAGE_PUBLISH = 2 ** 27;
export const MESSAGE_TRANSACTION = 2 ** 28;
export interface ConnectionTestResult {
  capabilities: number;
  elapsedMs: number;
}
export function nativeConnector(
  connection: Connection,
  descriptors: ConnectorDescriptor[],
) {
  return descriptors.find(
    (descriptor) =>
      descriptor.path === connection.driver || descriptor.id === connection.driver,
  );
}
export function messagePreviewBlock(flow?: DataFlow, selected?: string) {
  if (!flow || !selected) return undefined;
  const upstream = new Map<string, string[]>();
  for (const path of flow.paths) {
    const incoming = upstream.get(path.to) ?? [];
    incoming.push(path.from);
    upstream.set(path.to, incoming);
  }
  const sources = new Set(
    flow.components.filter((node) => node.component === "mq.source").map((node) => node.id),
  );
  const queue = [selected];
  const visited = new Set(queue);
  for (const node of queue) {
    if (sources.has(node))
      return "此路径会订阅并读取消息。请在正式执行中确认消费与位点影响。";
    for (const input of upstream.get(node) ?? []) {
      if (!visited.has(input)) {
        visited.add(input);
        queue.push(input);
      }
    }
  }
  return undefined;
}
export interface FileInspection {
  schema: DataSchema;
  metadata: {
    format: string;
    sheets?: string[];
    selectedSheet?: string;
    delimiterAmbiguous?: boolean;
    options?: Configuration;
    headerConfirmed: boolean;
  };
}
export interface DraftRecord {
  packageId: string;
  revision: string;
  baseRevision: string;
  document: PackageDocument;
  timestampMs: number;
  subject: string;
}
export const portIndex = (
  ports: NativePort[],
  reference: number | string,
  direction: number,
) =>
  typeof reference === "number"
    ? reference
    : (ports.find(
        (port) =>
          port.direction === direction &&
          (port.id === reference || port.name === reference),
      )?.index ?? -1);
