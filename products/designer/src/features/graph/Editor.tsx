import { ComponentPalette } from "./ComponentPalette";
import { CommandMenu } from "../workspace/CommandMenu";
import { PROTOCOL_VERSION } from "../../version";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import {
  Background,
  Controls,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Connection as GraphConnection,
  type Edge,
  type NodeChange,
  type Viewport,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  Undo2,
  Redo2,
  Save,
  CircleCheck,
  Database,
  Trash2,
  Plus,
  Settings2,
  X,
  Eye,
  Upload,
  Play,
  PanelRightClose,
  PanelRightOpen,
  WandSparkles,
  FileInput,
} from "lucide-react";
import { api, post, encode, decode, ApiError } from "../../api";
import {
  defaults,
  emptyPackage,
  ports,
  portIndex,
  isControlFlow,
  controlGraph,
  controlScopes,
  updateControl,
  messagePreviewBlock,
  type CatalogDocument,
  type Component,
  type Configuration,
  type PackageDocument,
  type NativePlan,
  type NativePort,
  type PreviewResult,
  type FlowComponent,
  type Connection,
  type Deployment,
  type DraftRecord,
} from "../../model";
import { useHistory } from "../../useHistory";
import { FlowNode, type FlowNodeValue } from "../../FlowNode";
import { ComponentInspector } from "./ComponentInspector";
import { DataFlowSettings } from "./DataFlowSettings";
import { Button } from "../../shared/design-system/Controls";
import { PreviewPanel } from "../../PreviewPanel";
import { ConnectionCenter } from "../../ConnectionCenter";
import { SourceWizard } from "../../SourceWizard";
import { ResizeHandle } from "../../ResizeHandle";
import { ExecutionConfirmation } from "../../ReleaseDialog";
import { TestDialog } from "../../TestDialog";
import { VersionDiff } from "../../VersionDiff";
import {
  RunMonitor,
  type RunFacts,
  type ProgressNode,
} from "../observability/RunMonitor";
import {
  removeGraphNodes,
  duplicateGraphNodes,
  templateNodes,
} from "../../graphEditing";
const nodeTypes = { component: FlowNode };
const constraintId = (edge: { from: string; to: string; on: string }) =>
  `constraint/${encode([edge.from, edge.to, edge.on])}`;
const pathId = (edge: {
  from: string;
  to: string;
  output: number | string;
  input: number | string;
}) => `path/${encode([edge.from, edge.to, edge.output, edge.input])}`;
export interface OpenWorkspace {
  token: string;
  document?: PackageDocument;
  revision?: string;
  import?: boolean;
}
interface Props {
  components: Component[];
  request?: OpenWorkspace;
  onExecution: () => void;
  active?: boolean;
  onStatus?: (
    token: string,
    label: string,
    dirty: boolean,
    packageId: string,
    revision?: string,
  ) => void;
}
export function Editor(props: Props) {
  return (
    <ReactFlowProvider key={props.request?.token ?? "initial"}>
      <EditorCanvas {...props} />
    </ReactFlowProvider>
  );
}
const errorText = (error: unknown) =>
  error instanceof ApiError && error.status === 409
    ? "版本已被其他会话更新。请重载并比较草稿，避免覆盖。"
    : error instanceof Error
      ? error.message
      : String(error);
const executionText = (document: PackageDocument) =>
  encode({ ...document, layout: undefined });
const portScope = (document: PackageDocument) =>
  encode({
    ...document,
    layout: undefined,
    dataFlows: [],
    controlFlow: { tasks: [], constraints: [] },
  });
function EditorCanvas({
  components,
  request,
  onExecution,
  active = true,
  onStatus,
}: Props) {
  const history = useHistory(
      () =>
        request?.document ?? {
          ...emptyPackage(),
          id: `task-${crypto.randomUUID()}`,
        },
    ),
    document = history.value,
    text = encode(document),
    dataText = executionText(document),
    bindingScope = portScope(document),
    documentRef = useRef(document);
  const { edit: editHistory, reset: resetHistory, undo, redo } = history;
  const graphApi = useReactFlow();
  const [selectedNodes, setSelectedNodes] = useState<string[]>([]);
  const selected = selectedNodes.at(-1);
  const [flowId, setFlow] = useState(
      request?.document?.dataFlows[0]?.id ?? "main",
    ),
    [selectedEdge, setSelectedEdge] = useState<string>();
  const [search, setSearch] = useState(""),
    [message, setMessage] = useState("从数据源开始创建任务"),
    [busy, setBusy] = useState(false),
    [previewBusy, setPreviewBusy] = useState(false);
  const [monitor, setMonitor] = useState<string>(),
    [monitorText, setMonitorText] = useState<string>(),
    [monitorCaption, setMonitorCaption] = useState<string>(),
    [liveFacts, setLiveFacts] = useState<RunFacts>(),
    [showMonitor, setShowMonitor] = useState(false);
  const [plan, setPlan] = useState<{ text: string; value: NativePlan }>(),
    [preview, setPreview] = useState<{
      text: string;
      flow: string;
      value: PreviewResult;
    }>();
  const [binding, setBinding] = useState<
    Record<string, { fingerprint: string; ports: NativePort[] }>
  >({});
  const [connectionsOpen, setConnectionsOpen] = useState(false),
    [importOpen, setImportOpen] = useState(Boolean(request?.import)),
    [documentOpen, setDocumentOpen] = useState(false),
    [versionsOpen, setVersionsOpen] = useState(false),
    [testOpen, setTestOpen] = useState(false),
    [commandsOpen, setCommandsOpen] = useState(false);
  const [versions, setVersions] = useState<CatalogDocument<PackageDocument>[]>(
      [],
    ),
    [comparison, setComparison] = useState<PackageDocument>();
  const [saved, setSaved] = useState<
      | {
          id: string;
          revision: string;
          text: string;
          document: PackageDocument;
        }
      | undefined
    >(() =>
      request?.document && BigInt(request.revision ?? "0") > 0n
        ? {
            id: request.document.id,
            revision: request.revision ?? "0",
            text: encode(request.document),
            document: request.document,
          }
        : undefined,
    ),
    [published, setPublished] = useState<{
      deployment: Deployment;
      document: PackageDocument;
    }>(),
    [confirmation, setConfirmation] = useState(false);
  const [draftSavedText, setDraftSavedText] = useState<string>();
  const [remoteDraft, setRemoteDraft] = useState<DraftRecord>();
  const [draftMessage, setDraftMessage] = useState("草稿尚未保存"),
    [draftReady, setDraftReady] = useState(!request?.document),
    [draftConflict, setDraftConflict] = useState(false);
  const draftRevisions = useRef(
      new Map<string, string>(request?.document ? [] : [[document.id, "0"]]),
    ),
    baseRevisions = useRef(
      new Map<string, string>([[document.id, request?.revision ?? "0"]]),
    ),
    draftQueue = useRef(Promise.resolve()),
    generation = useRef(0);
  const [rail, setRail] = useState(224),
    [inspector, setInspector] = useState(370),
    [previewHeight, setPreviewHeight] = useState(300),
    [inspectorOpen, setInspectorOpen] = useState(true),
    [catalogOpen, setCatalogOpen] = useState(false),
    [sourceRows, setSourceRows] = useState(1000);
  const [dragPositions, setDragPositions] = useState<
      Record<string, { x: number; y: number }>
    >({}),
    [draftViewport, setDraftViewport] = useState<Viewport>();
  const scopes = useMemo(() => controlScopes(document), [document]),
    flow =
      scopes.some((scope) => scope.id === flowId) ||
      document.dataFlows.some((flow) => flow.id === flowId)
        ? flowId
        : (document.dataFlows[0]?.id ?? "control");
  const control = isControlFlow(flow),
    graph = document.dataFlows.find((value) => value.id === flow),
    tasks = control ? controlGraph(document, flow) : undefined;
  const items = useMemo(
    () =>
      control
        ? tasks!.tasks.map((task) => ({
            id: task.id,
            component: task.kind,
            config: task.config,
          }))
        : (graph?.components ?? []),
    [control, tasks, graph],
  );
  const catalog = useMemo(
    () => new Map(components.map((component) => [component.id, component])),
    [components],
  );
  const item = items.find((node) => node.id === selected),
    descriptor = item && catalog.get(item.component),
    previewBlock = messagePreviewBlock(graph, item?.id);
  const effectivePlan =
    preview?.text === dataText && preview.flow === flow
      ? (preview.value.diagnostics as unknown as NativePlan)
      : plan?.value;
  const boundNodes =
    effectivePlan?.pipelines
      ?.find((value) => value.id === flow)
      ?.trees?.flatMap((tree) => tree.nodes) ?? [];
  const boundNode = boundNodes.find((node) => node.id === selected);
  const boundById = new Map(boundNodes.map((node) => [node.id, node]));
  const nativePorts = useCallback(
    (id: string, config: Configuration): NativePort[] =>
      binding[`${flow}/${id}`]?.fingerprint ===
      bindingScope + "\0" + encode(config)
        ? binding[`${flow}/${id}`].ports
        : [],
    [binding, flow, bindingScope],
  );
  function count(node: FlowComponent) {
    const bound = nativePorts(node.id, node.config);
    return control
      ? { inputs: 1, outputs: 1, error: -1 }
      : bound.length
        ? {
            inputs: bound.filter((port) => port.direction === 1).length,
            outputs: bound.filter((port) => port.direction === 2).length,
            error: bound.find((port) => port.flags & 2)?.index ?? -1,
          }
        : catalog.get(node.component)
          ? ports(catalog.get(node.component)!)
          : { inputs: 0, outputs: 0, error: -1 };
  }
  function referenceIndex(
    id: string,
    reference: number | string,
    direction: number,
  ) {
    const node = items.find((node) => node.id === id);
    return node
      ? portIndex(nativePorts(node.id, node.config), reference, direction)
      : -1;
  }
  const inputPath = graph?.paths.find(
    (edge) =>
      edge.to === selected && referenceIndex(edge.to, edge.input, 1) === 0,
  );
  const inputIndex = inputPath
    ? referenceIndex(inputPath.from, inputPath.output, 2)
    : 0;
  const inputSchema =
    boundNode?.inputs[0] ??
    (inputPath
      ? boundById.get(inputPath.from)?.outputs[inputIndex]
      : undefined) ??
    (inputPath
      ? preview?.value.snapshot.preview.find(
          (edge) =>
            edge.component === inputPath.from && edge.port === inputIndex,
        )?.schema
      : undefined);
  function label(id: string) {
    const node = items.find((node) => node.id === id);
    return (
      document.layout?.nodes?.find(
        (node) => node.flow === flow && node.id === id,
      )?.label ??
      (node?.component === "sql.destination" && node.config.table
        ? String(node.config.table)
        : (catalog.get(node?.component ?? "")?.name ?? id))
    );
  }
  const stale = Boolean(preview && preview.text !== dataText);
  const liveNodes =
    monitorText !== dataText
      ? []
      : (liveFacts?.progress?.pipelines.find((pipeline) => pipeline.id === flow)
          ?.nodes ??
        liveFacts?.diagnostics?.pipelines
          .find((pipeline) => pipeline.id === flow)
          ?.trees.flatMap((tree) => tree.nodes) ??
        []);
  const nodes: FlowNodeValue[] = items.map((node, index) => {
    const layout = document.layout?.nodes?.find(
        (value) => value.flow === flow && value.id === node.id,
      ),
      bound = boundById.get(node.id),
      portRows = Object.fromEntries(
        (preview?.flow === flow ? preview.value.snapshot.preview : [])
          .filter((edge) => edge.component === node.id)
          .map((edge) => [edge.port, edge.totalRows]),
      );
    const live = liveNodes.find((value) => value.id === node.id) as
      | ProgressNode
      | undefined;
    if (preview?.text === dataText && bound?.ports)
      for (const port of bound.ports)
        if (port.direction === 2 && port.rows !== undefined)
          portRows[port.index] = port.rows;
    if (live?.ports)
      for (const port of live.ports)
        if (port.rows !== undefined) portRows[port.index] = port.rows;
    return {
      id: node.id,
      type: "component",
      selected: selectedNodes.includes(node.id),
      // Reuse the dimensions measured by the canvas across controlled updates.
      measured: graphApi.getInternalNode(node.id)?.measured,
      position: dragPositions[node.id] ??
        layout ?? { x: 70 + index * 300, y: 140 },
      data: {
        label: label(node.id),
        componentId: node.component,
        ...count(node),
        ports: nativePorts(node.id, node.config),
        inputFields: bound?.inputs.reduce(
          (sum, schema) => sum + (schema?.fields.length ?? 0),
          0,
        ),
        outputFields: bound?.outputs[0]?.fields.length,
        portRows,
        runtime: live
          ? `${live.boundedness ? "持续" : "有界"}${live.idle ? " · 空闲" : ""} · ${live.rateRowsPerSecond?.toFixed(1) ?? "—"} 行/秒`
          : undefined,
        stale,
        failed: Boolean((bound?.result ?? 0) || (live?.result ?? 0)),
      },
    };
  });
  const edges: Edge[] = control
    ? tasks!.constraints.map((edge) => ({
        id: constraintId(edge),
        source: edge.from,
        target: edge.to,
        sourceHandle: "out-0",
        targetHandle: "in-0",
        label: edge.on,
        selected: selectedEdge === constraintId(edge),
      }))
    : (graph?.paths ?? []).map((path) => {
        const producer = items.find((node) => node.id === path.from),
          consumer = items.find((node) => node.id === path.to),
          out = producer
            ? nativePorts(producer.id, producer.config).find(
                (port) =>
                  port.direction === 2 &&
                  (port.id === path.output ||
                    port.name === path.output ||
                    port.index === path.output),
              )
            : undefined,
          into = consumer
            ? nativePorts(consumer.id, consumer.config).find(
                (port) =>
                  port.direction === 1 &&
                  (port.id === path.input ||
                    port.name === path.input ||
                    port.index === path.input),
              )
            : undefined;
        return {
          id: pathId(path),
          source: path.from,
          target: path.to,
          sourceHandle: `out-${out?.id ?? path.output}`,
          targetHandle: `in-${into?.id ?? path.input}`,
          label: out?.name,
          selected: selectedEdge === pathId(path),
          style: {
            stroke: out?.flags && out.flags & 2 ? "#d94a52" : "#718294",
            strokeWidth: 1.6,
          },
        };
      });
  function edit(update: (value: PackageDocument) => PackageDocument) {
    editHistory(update);
    setMessage("草稿有更改，校验与样本将重新计算");
  }
  function selectFlow(next: string) {
    setFlow(next);
    setSelectedNodes([]);
    setSelectedEdge(undefined);
    setDraftViewport(undefined);
    setDragPositions({});
  }
  const registerPlan = useCallback(
    (value: NativePlan, snapshot: PackageDocument) => {
      const scope = portScope(snapshot);
      setBinding((current) => {
        const updated = { ...current };
        for (const pipeline of value.pipelines ?? [])
          for (const node of pipeline.trees?.flatMap((tree) => tree.nodes) ??
            []) {
            const config = snapshot.dataFlows
              .find((flow) => flow.id === pipeline.id)
              ?.components.find(
                (component) => component.id === node.id,
              )?.config;
            if (config)
              updated[`${pipeline.id}/${node.id}`] = {
                fingerprint: scope + "\0" + encode(config),
                ports: node.ports,
              };
          }
        return updated;
      });
    },
    [],
  );
  useEffect(() => {
    documentRef.current = document;
  }, [document]);
  useEffect(() => {
    if (!request?.document) return;
    const token = generation.current;
    const initial = request.document;
    const controller = new AbortController();
    api<DraftRecord>(`/drafts/${encodeURIComponent(initial.id)}`, {
      signal: controller.signal,
    })
      .then((draft) => {
        if (generation.current !== token) return;
        draftRevisions.current.set(initial.id, draft.revision);
        if (BigInt(draft.baseRevision) >= BigInt(request.revision ?? "0")) {
          resetHistory(draft.document);
          setDraftSavedText(encode(draft.document));
          setDraftMessage(`已恢复 ${draft.subject} 的工作草稿`);
        } else setDraftMessage("已打开发布后版本；旧草稿可在版本比较中查看");
      })
      .catch((error) => {
        if (!controller.signal.aborted) {
          if (error instanceof ApiError && error.status === 404)
            draftRevisions.current.set(initial.id, "0");
          else {
            setMessage(errorText(error));
            setDraftConflict(true);
          }
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setDraftReady(true);
      });
    return () => controller.abort();
  }, [request, resetHistory]);
  useEffect(
    () => () => {
      generation.current++;
    },
    [],
  );
  useEffect(() => {
    if (!draftReady || draftConflict) return;
    const snapshot = document,
      content = text,
      token = generation.current;
    const timer = window.setTimeout(() => {
      setDraftMessage("正在保存工作草稿…");
      draftQueue.current = draftQueue.current
        .then(async () => {
          if (token !== generation.current) return;
          let expected = draftRevisions.current.get(snapshot.id);
          if (expected === undefined) {
            try {
              const existing = await api<DraftRecord>(
                `/drafts/${encodeURIComponent(snapshot.id)}`,
              );
              if (encode(existing.document) !== content)
                throw new ApiError("同名工作草稿已存在，请先打开并比较", 409);
              expected = existing.revision;
            } catch (error) {
              if (error instanceof ApiError && error.status === 404)
                expected = "0";
              else throw error;
            }
            draftRevisions.current.set(snapshot.id, expected);
          }
          if (token !== generation.current) return;
          const value = await api<DraftRecord>(
            `/drafts/${encodeURIComponent(snapshot.id)}`,
            {
              method: "PUT",
              body: encode({
                document: snapshot,
                expectedRevision: expected,
                baseRevision: baseRevisions.current.get(snapshot.id) ?? "0",
              }),
            },
          );
          draftRevisions.current.set(snapshot.id, value.revision);
          if (
            token === generation.current &&
            encode(documentRef.current) === content
          ) {
            setDraftSavedText(content);
            setDraftMessage("工作草稿已自动保存");
          }
        })
        .catch((error) => {
          if (token === generation.current) {
            setDraftMessage("草稿未保存");
            setMessage(errorText(error));
            setDraftConflict(true);
          }
        });
    }, 1200);
    return () => window.clearTimeout(timer);
  }, [text, document, draftReady, draftConflict]);
  useEffect(() => {
    if (!item || control) return;
    const snapshot = document,
      config = portScope(document) + "\0" + encode(item.config),
      key = `${flow}/${item.id}`,
      controller = new AbortController();
    const timer = window.setTimeout(() => {
      post<NativePort[]>("/ports", {
        package: snapshot,
        pipelineId: flow,
        componentId: item.id,
        kind: item.component,
        configuration: item.config,
      })
        .then((value) => {
          if (!controller.signal.aborted)
            setBinding((current) => ({
              ...current,
              [key]: { fingerprint: config, ports: value },
            }));
        })
        .catch(() => {
          /* Invalid partial configuration remains editable; validation reports it. */
        });
    }, 200);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [item, control, flow, document]);
  const [initialText] = useState(text);
  const dirty =
    text !== initialText && draftSavedText !== text && saved?.text !== text;
  useEffect(() => {
    if (request)
      onStatus?.(
        request.token,
        document.layout?.label ?? document.id,
        dirty,
        document.id,
        saved?.id === document.id ? saved.revision : undefined,
      );
  }, [
    request,
    onStatus,
    document.id,
    document.layout?.label,
    dirty,
    saved?.id,
    saved?.revision,
  ]);
  const runAction = useCallback(
    async (operation: "save" | "validate" | "publish") => {
      const snapshot = document,
        snapshotText = encode(snapshot);
      setBusy(true);
      try {
        if (operation === "validate") {
          const value = await post<NativePlan>("/validate", snapshot);
          setPlan({ text: snapshotText, value });
          registerPlan(value, snapshot);
          setMessage("Native 校验通过，字段与端口已绑定");
          return;
        }
        const expected = baseRevisions.current.get(snapshot.id) ?? "0";
        if (operation === "save") {
          const result = await post<{
            id: string;
            revision: string;
            document: PackageDocument;
          }>("/versions", {
            document: snapshot,
            expectedPackageRevision: expected,
          });
          baseRevisions.current.set(snapshot.id, result.revision);
          setSaved({
            id: snapshot.id,
            revision: result.revision,
            text: snapshotText,
            document: snapshot,
          });
          setMessage(`已保存版本 v${result.revision}`);
        } else {
          const result = await post<{
            deployment: Deployment;
            revision: string;
          }>("/publications", {
            document: snapshot,
            expectedPackageRevision: expected,
          });
          baseRevisions.current.set(snapshot.id, result.revision);
          setSaved({
            id: snapshot.id,
            revision: result.revision,
            text: snapshotText,
            document: snapshot,
          });
          setPublished({ deployment: result.deployment, document: snapshot });
          setMessage(`已发布 v${result.revision}，等待正式执行`);
        }
      } catch (error) {
        setMessage(errorText(error));
      } finally {
        setBusy(false);
      }
    },
    [registerPlan, document],
  );
  useEffect(() => {
    const key = (event: KeyboardEvent) => {
      if (!active) return;
      const target = event.target as HTMLElement;
      if (event.key.toLowerCase() === "k" && (event.ctrlKey || event.metaKey)) {
        event.preventDefault();
        setCommandsOpen(true);
        return;
      }
      if (event.key.toLowerCase() === "s" && (event.ctrlKey || event.metaKey)) {
        event.preventDefault();
        void runAction("save");
      }
      if (target.closest("input,textarea,[contenteditable=true]")) return;
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "z") {
        event.preventDefault();
        if (event.shiftKey) redo();
        else undo();
      }
    };
    window.addEventListener("keydown", key);
    return () => window.removeEventListener("keydown", key);
  }, [runAction, undo, redo, active]);
  function withPosition(
    current: PackageDocument,
    id: string,
    position: { x: number; y: number },
    name?: string,
  ) {
    const layout = current.layout ?? {
      version: PROTOCOL_VERSION,
      nodes: [],
      viewports: [],
    };
    return {
      ...current,
      layout: {
        ...layout,
        nodes: [
          ...(layout.nodes ?? []).filter(
            (node) => node.id !== id || node.flow !== flow,
          ),
          { flow, id, ...position, ...(name ? { label: name } : {}) },
        ],
      },
    };
  }
  function add(
    component: Component,
    position?: { x: number; y: number },
    edge?: Edge,
  ) {
    const id = `node-${crypto.randomUUID()}`;
    let placement = position;
    if (!placement) {
      const anchor = nodes.find((node) => node.id === selected);
      placement = anchor
        ? {
            x:
              anchor.position.x +
              Math.max(330, (anchor.measured?.width ?? 260) + 70),
            y: anchor.position.y,
          }
        : graphApi.screenToFlowPosition({ x: window.innerWidth / 2, y: 300 });
      // Catalog clicks create a reachable node without covering an existing
      // port. Explicit canvas drops retain the user's chosen position.
      while (
        nodes.some(
          (node) =>
            placement!.x <
              node.position.x + (node.measured?.width ?? 260) + 32 &&
            placement!.x + 292 > node.position.x &&
            placement!.y <
              node.position.y + (node.measured?.height ?? 180) + 32 &&
            placement!.y + 212 > node.position.y,
        )
      )
        placement = { ...placement, y: placement.y + 230 };
    }
    let config = defaults(component.configurationSchema);
    if (component.id === "derive") config = { columns: [] };
    if (component.id === "split")
      config = {
        branches: [{ id: crypto.randomUUID(), name: "分支 1", condition: "" }],
        default: "其他",
        errorPolicy: "fail",
      };
    if (component.id === "multicast")
      config = { outputs: ["业务明细", "维度统计"] };
    if (component.id === "aggregate" || component.id === "eventWindow")
      config = {
        ...config,
        plan: {
          version: PROTOCOL_VERSION,
          group: [],
          aggregates: [{ name: "row_count", function: "count", column: "*" }],
        },
      };
    if (component.id === "sql.destination")
      config = { mode: "append", loadMode: "auto" };
    edit((current) => {
      let next = control
        ? updateControl(current, flow, (graph) => ({
            ...graph,
            tasks: [
              ...graph.tasks,
              {
                id,
                kind: component.id,
                config,
                ...(component.children
                  ? { children: { tasks: [], constraints: [] } }
                  : {}),
              },
            ],
          }))
        : {
            ...current,
            dataFlows: current.dataFlows.map((graph) =>
              graph.id === flow
                ? {
                    ...graph,
                    components: [
                      ...graph.components,
                      { id, component: component.id, config },
                    ],
                    paths: edge
                      ? graph.paths.flatMap((path) =>
                          pathId(path) === edge.id
                            ? [
                                { ...path, to: id, input: 0 },
                                {
                                  from: id,
                                  output: 0,
                                  to: path.to,
                                  input: path.input,
                                },
                              ]
                            : [path],
                        )
                      : graph.paths,
                  }
                : graph,
            ),
          };
      next = withPosition(next, id, placement);
      return next;
    });
    setSelectedNodes([id]);
    setSelectedEdge(undefined);
    setInspectorOpen(true);
    setCatalogOpen(false);
  }
  function imported(
    connection: Connection,
    component: FlowComponent,
    name?: string,
    sample?: PreviewResult,
  ) {
    const next = withPosition(
      {
        ...document,
        connections: [...(document.connections ?? []), connection],
        dataFlows: document.dataFlows.map((graph) =>
          graph.id === flow
            ? { ...graph, components: [...graph.components, component] }
            : graph,
        ),
      },
      component.id,
      { x: 70, y: 140 },
      name,
    );
    edit(() => next);
    if (sample) {
      setPreview({ text: executionText(next), flow, value: sample });
      setPlan({
        text: encode(next),
        value: sample.diagnostics as unknown as NativePlan,
      });
      registerPlan(sample.diagnostics as unknown as NativePlan, next);
    }
    setSelectedNodes([component.id]);
    setImportOpen(false);
    setInspectorOpen(true);
  }
  function validConnection(connection: GraphConnection) {
    if (
      !connection.source ||
      !connection.target ||
      connection.source === connection.target
    )
      return false;
    if (control)
      return !tasks?.constraints.some(
        (edge) =>
          edge.from === connection.source && edge.to === connection.target,
      );
    const output = connection.sourceHandle?.slice(4),
      input = connection.targetHandle?.slice(3);
    if (!output || !input) return false;
    const incoming = graph?.paths.some(
      (path) =>
        path.to === connection.target &&
        (String(path.input) === input ||
          referenceIndex(path.to, path.input, 1) ===
            referenceIndex(path.to, input, 1)),
    );
    if (incoming) return false;
    const visited = new Set<string>();
    const queue = [connection.target];
    while (queue.length) {
      const id = queue.pop()!;
      if (id === connection.source) return false;
      if (visited.has(id)) continue;
      visited.add(id);
      for (const path of graph?.paths ?? [])
        if (path.from === id) queue.push(path.to);
    }
    return true;
  }
  function connect(connection: GraphConnection) {
    if (!validConnection(connection)) {
      setMessage("连线无效：请检查方向、重复输入或环路");
      return;
    }
    const numeric = (value: string | undefined) =>
      value && /^\d+$/.test(value) ? Number(value) : (value ?? 0);
    edit((current) =>
      control
        ? updateControl(current, flow, (graph) => ({
            ...graph,
            constraints: [
              ...graph.constraints,
              { from: connection.source, to: connection.target, on: "success" },
            ],
          }))
        : {
            ...current,
            dataFlows: current.dataFlows.map((graph) =>
              graph.id === flow
                ? {
                    ...graph,
                    paths: [
                      ...graph.paths,
                      {
                        from: connection.source,
                        to: connection.target,
                        output: numeric(connection.sourceHandle?.slice(4)),
                        input: numeric(connection.targetHandle?.slice(3)),
                      },
                    ],
                  }
                : graph,
            ),
          },
    );
  }
  function configure(config: Configuration, outputRemap?: number[]) {
    edit((current) =>
      control
        ? updateControl(current, flow, (graph) => ({
            ...graph,
            tasks: graph.tasks.map((task) =>
              task.id === selected ? { ...task, config } : task,
            ),
          }))
        : {
            ...current,
            dataFlows: current.dataFlows.map((graph) =>
              graph.id === flow
                ? {
                    ...graph,
                    components: graph.components.map((node) =>
                      node.id === selected ? { ...node, config } : node,
                    ),
                    paths: graph.paths.flatMap((path) => {
                      if (path.from !== selected || !outputRemap) return [path];
                      if (typeof path.output === "string") {
                        const index = referenceIndex(path.from, path.output, 2);
                        return outputRemap[index] === -1 ? [] : [path];
                      }
                      const output = outputRemap[path.output];
                      return output === undefined || output < 0
                        ? []
                        : [{ ...path, output }];
                    }),
                  }
                : graph,
            ),
          },
    );
  }
  function remove() {
    const ids = new Set(selectedNodes);
    edit((current) => removeGraphNodes(current, flow, ids));
    setSelectedNodes([]);
  }
  function duplicate() {
    const ids = new Set(selectedNodes);
    const result = duplicateGraphNodes(
      document,
      flow,
      ids,
      new Map(
        items.map((node) => [node.id, nativePorts(node.id, node.config)]),
      ),
    );
    if (!result.ids.length) {
      setMessage("先绑定所选节点的 Native 端口，再复制连线");
      return;
    }
    edit(() => result.document);
    setSelectedNodes(result.ids);
  }
  function insertTemplate(kind: "derive-split" | "dimensions") {
    const template = templateNodes(kind);
    if (template.nodes.some((node) => !catalog.has(node.component))) {
      setMessage("当前宿主没有模板所需的 Native 组件");
      return;
    }
    edit((current) => {
      let next = {
        ...current,
        dataFlows: current.dataFlows.map((graph) =>
          graph.id === flow
            ? {
                ...graph,
                components: [...graph.components, ...template.nodes],
                paths: [
                  ...graph.paths,
                  ...template.paths,
                  ...(selected &&
                  !graph.paths.some((path) => path.from === selected)
                    ? [
                        {
                          from: selected,
                          output: 0,
                          to: template.nodes[0].id,
                          input: 0,
                        },
                      ]
                    : []),
                ],
              }
            : graph,
        ),
      };
      template.nodes.forEach((node, index) => {
        next = withPosition(
          next,
          node.id,
          { x: 100 + (index % 2) * 330, y: 170 + Math.floor(index / 2) * 230 },
          template.labels[node.id],
        );
      });
      return next;
    });
    setSelectedNodes([template.nodes[0].id]);
    setInspectorOpen(true);
  }
  const changes = useCallback(
    (updates: NodeChange[]) => {
      for (const update of updates)
        if (update.type === "select")
          setSelectedNodes((current) =>
            update.selected
              ? [...new Set([...current, update.id])]
              : current.filter((id) => id !== update.id),
          );
      for (const update of updates)
        if (update.type === "position" && update.position && update.dragging)
          setDragPositions((current) => ({
            ...current,
            [update.id]: update.position!,
          }));
      const positions = updates.filter(
        (update) =>
          update.type === "position" && update.position && !update.dragging,
      );
      if (!positions.length) return;
      editHistory((current) => {
        const layout = current.layout ?? {
          version: PROTOCOL_VERSION,
          nodes: [],
          viewports: [],
        };
        let nodes = layout.nodes ?? [];
        for (const update of positions)
          if (update.type === "position" && update.position) {
            const old = nodes.find(
              (node) => node.flow === flow && node.id === update.id,
            );
            nodes = [
              ...nodes.filter(
                (node) => node.flow !== flow || node.id !== update.id,
              ),
              { ...old, flow, id: update.id, ...update.position },
            ];
          }
        return { ...current, layout: { ...layout, nodes } };
      });
      setDragPositions({});
    },
    [flow, editHistory],
  );
  function layout() {
    const rank = new Map<string, number>();
    for (let pass = 0; pass < items.length; pass++)
      for (const node of items) {
        const parents =
          graph?.paths.filter((path) => path.to === node.id) ?? [];
        const depth = parents.length
          ? Math.max(...parents.map((path) => (rank.get(path.from) ?? 0) + 1))
          : 0;
        rank.set(node.id, Math.min(depth, items.length));
      }
    const lanes = new Map<number, number>();
    edit((current) => {
      let next = current;
      for (const node of items) {
        const depth = rank.get(node.id) ?? 0,
          slot = lanes.get(depth) ?? 0;
        lanes.set(depth, slot + 1);
        next = withPosition(
          next,
          node.id,
          { x: 60 + depth * 330, y: 70 + slot * 220 },
          label(node.id),
        );
      }
      return next;
    });
    requestAnimationFrame(
      () => void graphApi.fitView({ padding: 0.15, maxZoom: 1 }),
    );
  }
  async function readPreview() {
    if (!item || control) return;
    if (previewBlock) {
      setMessage(previewBlock);
      return;
    }
    const snapshot = document,
      snapshotText = executionText(snapshot),
      selectedId = item.id;
    setPreviewBusy(true);
    try {
      const value = await post<PreviewResult>("/preview", {
        package: snapshot,
        pipelineId: flow,
        componentId: selectedId,
        sourceRows,
        sampleRows: 32,
      });
      setPreview({ text: snapshotText, flow, value });
      registerPlan(value.diagnostics as unknown as NativePlan, snapshot);
      setMessage(
        value.result
          ? `预览失败 · Native 状态 ${value.result}`
          : "真实样本已就绪，目标未执行",
      );
    } catch (error) {
      setMessage(errorText(error));
    } finally {
      setPreviewBusy(false);
    }
  }
  async function openVersions(after = false) {
    try {
      const last = after ? versions.at(-1) : undefined;
      const page = await api<CatalogDocument<PackageDocument>[]>(
        `/packages${last ? `?afterId=${encodeURIComponent(last.id)}&afterRevision=${last.revision}` : ""}`,
      );
      setVersions((current) => (after ? [...current, ...page] : page));
      setVersionsOpen(true);
    } catch (error) {
      setMessage(errorText(error));
    }
  }
  function load(version: CatalogDocument<PackageDocument>) {
    generation.current++;
    resetHistory(version.document);
    baseRevisions.current.set(version.id, String(version.revision));
    setSaved({
      id: version.id,
      revision: String(version.revision),
      text: encode(version.document),
      document: version.document,
    });
    selectFlow(version.document.dataFlows[0]?.id ?? "control");
    setPlan(undefined);
    setPreview(undefined);
    setBinding({});
    setVersionsOpen(false);
    setDraftReady(true);
    setDraftConflict(false);
    setPublished(undefined);
  }
  function newFlow() {
    const id = `flow-${crypto.randomUUID()}`;
    edit((current) => ({
      ...current,
      dataFlows: [...current.dataFlows, { id, components: [], paths: [] }],
      controlFlow: {
        ...current.controlFlow,
        tasks: [
          ...current.controlFlow.tasks,
          { id: `run-${id}`, kind: "dataFlow", config: { pipeline: id } },
        ],
      },
    }));
    selectFlow(id);
  }
  const activeConstraint =
      control &&
      tasks!.constraints.find((edge) => constraintId(edge) === selectedEdge),
    viewport = draftViewport ??
      document.layout?.viewports?.find((value) => value.flow === flow) ?? {
        x: 0,
        y: 0,
        zoom: 1,
      };
  const before = !stale
      ? preview?.value.snapshot.preview.find(
          (edge) =>
            edge.component === inputPath?.from && edge.port === inputIndex,
        )
      : undefined,
    after = !stale
      ? preview?.value.snapshot.preview.find(
          (edge) => edge.component === selected && edge.port === 0,
        )
      : undefined;
  const splitDestinations = Object.fromEntries(
    (graph?.paths ?? [])
      .filter((path) => path.from === selected)
      .map((path) => [
        referenceIndex(path.from, path.output, 2),
        label(path.to),
      ]),
  );
  const bodyStyle = {
    "--rail-width": `${rail}px`,
    "--inspector-width": inspectorOpen ? `${inspector}px` : "0px",
    "--preview-height": `${previewHeight}px`,
  } as CSSProperties;
  return (
    <>
      <CommandMenu
        open={commandsOpen}
        close={() => setCommandsOpen(false)}
        actions={[
          {
            id: "save",
            label: "保存版本",
            run: () => void runAction("save"),
            disabled: busy,
          },
          {
            id: "validate",
            label: "Native 校验",
            run: () => void runAction("validate"),
            disabled: busy,
          },
          {
            id: "publish",
            label: "发布版本",
            run: () => void runAction("publish"),
            disabled: busy,
          },
          {
            id: "connections",
            label: "连接中心",
            run: () => setConnectionsOpen(true),
          },
          { id: "layout", label: "自动布局", run: layout },
          { id: "undo", label: "撤销", run: undo, disabled: !history.canUndo },
          { id: "redo", label: "重做", run: redo, disabled: !history.canRedo },
          { id: "catalog", label: "搜索组件", run: () => setCatalogOpen(true) },
          {
            id: "json",
            label: "查看 Package 文档",
            run: () => setDocumentOpen(true),
          },
        ]}
      />
      <div className="workspace-heading">
        <div>
          <small>项目 / 数据集成</small>
          <input
            className="workspace-title"
            aria-label="作业包名称"
            value={document.layout?.label ?? document.id}
            onChange={(event) =>
              edit((current) => ({
                ...current,
                layout: {
                  ...current.layout,
                  version: PROTOCOL_VERSION,
                  nodes: current.layout?.nodes ?? [],
                  viewports: current.layout?.viewports ?? [],
                  label: event.target.value,
                },
              }))
            }
          />
        </div>
        <div className="workspace-state">
          <span className={draftConflict ? "error" : ""}>
            {draftSavedText === text || draftConflict
              ? draftMessage
              : "草稿有未保存更改"}
          </span>
          {draftConflict ? (
            <button
              onClick={() =>
                void api<DraftRecord>(
                  `/drafts/${encodeURIComponent(document.id)}`,
                )
                  .then((remote) => {
                    setRemoteDraft(remote);
                    setComparison(remote.document);
                    setVersionsOpen(true);
                  })
                  .catch((error) => setMessage(errorText(error)))
              }
            >
              比较远程草稿
            </button>
          ) : null}
          <small>
            {saved?.id === document.id
              ? `已保存 v${saved.revision}${saved.text === text ? "" : " · 草稿有更改"}`
              : "尚无保存版本"}
            {published
              ? ` · 已发布 v${published.deployment.packageRevision}`
              : ""}
          </small>
        </div>
        <div className="workspace-actions">
          <button disabled={busy} onClick={() => void runAction("validate")}>
            <CircleCheck size={16} />
            校验
          </button>
          <Button
            disabled={
              !item ||
              control ||
              previewBusy ||
              !!previewBlock ||
              (descriptor?.category !== 4 && descriptor?.previewSafe === false)
            }
            title={
              previewBlock ?? (descriptor?.previewSafe === false && descriptor.category !== 4
                ? "此 Native 组件未声明只读预览能力"
                : undefined)
            }
            onClick={() => void readPreview()}
          >
            <Eye size={16} />
            预览
          </Button>
          <button disabled={busy} onClick={() => setTestOpen(true)}>
            完整测试
          </button>
          <button
            className="primary"
            disabled={busy}
            onClick={() => void runAction("publish")}
          >
            <Upload size={16} />
            发布版本
          </button>
          <button
            disabled={!published || busy}
            onClick={() => setConfirmation(true)}
          >
            <Play size={16} />
            正式执行
          </button>
        </div>
      </div>
      <div className="editor-toolbar">
        <div className="flow-tabs">
          <select
            aria-label="选择图"
            value={flow}
            onChange={(event) => selectFlow(event.target.value)}
          >
            {scopes.map((scope) => (
              <option key={scope.id} value={scope.id}>
                {scope.label}
              </option>
            ))}
            {document.dataFlows.map((flow) => (
              <option key={flow.id} value={flow.id}>
                数据流 · {flow.id}
              </option>
            ))}
          </select>
          <button aria-label="新建数据流" onClick={newFlow}>
            <Plus size={15} />
          </button>
        </div>
        <button
          className="compact-catalog-button"
          onClick={() => setCatalogOpen((value) => !value)}
        >
          组件目录
        </button>
        <button onClick={() => setImportOpen(true)} disabled={control}>
          <FileInput size={15} />
          导入文件
        </button>
        <button onClick={() => setConnectionsOpen(true)}>
          <Database size={15} />
          连接中心
        </button>
        <span role="status" className="graph-count">{items.length} 个组件 · {edges.length} 条路径</span>
        {items.length>100?<select aria-label="定位组件" value={selected??""} onChange={event=>{const id=event.target.value;setSelectedNodes(id?[id]:[]);setInspectorOpen(true);if(id)void graphApi.fitView({nodes:[{id}],maxZoom:1,padding:0.3});}}><option value="">定位组件…</option>{items.map(node=><option key={node.id} value={node.id}>{label(node.id)}</option>)}</select>:null}
        <div className="toolbar-actions">
          <button
            onClick={() => setCommandsOpen(true)}
            aria-keyshortcuts="Control+k Meta+k"
          >
            命令 ⌘/Ctrl K
          </button>
          <button aria-label="撤销" disabled={!history.canUndo} onClick={undo}>
            <Undo2 size={15} />
          </button>
          <button aria-label="重做" disabled={!history.canRedo} onClick={redo}>
            <Redo2 size={15} />
          </button>
          <button disabled={busy} onClick={() => void runAction("save")}>
            <Save size={15} />
            保存版本
          </button>
          <button onClick={() => void openVersions()}>版本与比较</button>
          <button
            disabled={control || (!selected && !selectedNodes.length)}
            onClick={duplicate}
          >
            复制选中
          </button>
          <button
            disabled={!selected && !selectedNodes.length}
            aria-label="删除选中节点"
            onClick={remove}
          >
            <Trash2 size={14} />
          </button>
          <button aria-label="自动布局" onClick={layout}>
            <WandSparkles size={15} />
          </button>
          <button
            aria-label={inspectorOpen ? "收起属性" : "展开属性"}
            onClick={() => setInspectorOpen((value) => !value)}
          >
            {inspectorOpen ? (
              <PanelRightClose size={16} />
            ) : (
              <PanelRightOpen size={16} />
            )}
          </button>
        </div>
      </div>
      <div
        className={`editor-body workbench${catalogOpen ? " catalog-open" : ""}${inspectorOpen ? "" : " inspector-collapsed"}`}
        style={bodyStyle}
      >
        <ComponentPalette
          components={components}
          control={control}
          search={search}
          setSearch={setSearch}
          add={add}
          template={insertTemplate}
        />
        <ResizeHandle
          label="调整组件目录宽度"
          value={rail}
          min={180}
          max={340}
          change={setRail}
        />
        <main className="editor-center">
          <div
            className="flow-canvas"
            onDragOver={(event) => {
              event.preventDefault();
              event.dataTransfer.dropEffect = "copy";
            }}
            onDrop={(event) => {
              event.preventDefault();
              const component = catalog.get(
                event.dataTransfer.getData(
                  "application/universaldata-component",
                ),
              );
              if (!component) return;
              const edgeTarget = (event.target as Element)
                .closest(".react-flow__edge")
                ?.getAttribute("data-id");
              const edge = edges.find((edge) => edge.id === edgeTarget);
              if (
                edge &&
                (component.inputs !== 1 ||
                  component.outputs < 1 ||
                  component.category !== 3)
              ) {
                setMessage("仅单输入转换可直接插入连线。");
                return;
              }
              add(
                component,
                graphApi.screenToFlowPosition({
                  x: event.clientX,
                  y: event.clientY,
                }),
                edge,
              );
            }}
          >
            <ReactFlow
              key={flow}
              nodes={nodes}
              edges={edges}
              nodeTypes={nodeTypes}
              onlyRenderVisibleElements={items.length > 100}
              onNodesChange={changes}
              onNodesDelete={(deleted) => {
                edit((current) =>
                  removeGraphNodes(
                    current,
                    flow,
                    new Set(deleted.map((node) => node.id)),
                  ),
                );
                setSelectedNodes([]);
              }}
              onNodeClick={(event, node) => {
                if (!event.shiftKey && !event.ctrlKey && !event.metaKey)
                  setSelectedNodes([node.id]);
                setSelectedEdge(undefined);
                setInspectorOpen(true);
              }}
              onEdgeClick={(_, edge) => {
                setSelectedEdge(edge.id);
                setSelectedNodes([]);
              }}
              onPaneClick={() => {
                setSelectedNodes([]);
                setSelectedEdge(undefined);
              }}
              isValidConnection={(connection) =>
                validConnection(connection as GraphConnection)
              }
              onConnect={connect}
              onEdgesDelete={(deleted) =>
                edit((current) =>
                  control
                    ? updateControl(current, flow, (graph) => ({
                        ...graph,
                        constraints: graph.constraints.filter(
                          (edge) =>
                            !deleted.some(
                              (value) => value.id === constraintId(edge),
                            ),
                        ),
                      }))
                    : {
                        ...current,
                        dataFlows: current.dataFlows.map((graph) =>
                          graph.id === flow
                            ? {
                                ...graph,
                                paths: graph.paths.filter(
                                  (path) =>
                                    !deleted.some(
                                      (value) => value.id === pathId(path),
                                    ),
                                ),
                              }
                            : graph,
                        ),
                      },
                )
              }
              viewport={viewport}
              onViewportChange={setDraftViewport}
              onMoveEnd={(_, viewport) => {
                editHistory((current) => ({
                  ...current,
                  layout: {
                    ...current.layout,
                    version: PROTOCOL_VERSION,
                    nodes: current.layout?.nodes ?? [],
                    viewports: [
                      ...(current.layout?.viewports ?? []).filter(
                        (value) => value.flow !== flow,
                      ),
                      { flow, ...viewport },
                    ],
                  },
                }));
                setDraftViewport(undefined);
              }}
            >
              <Background gap={20} color="#d9e2e9" />
              <Controls />
            </ReactFlow>
            {!items.length ? (
              <div className="canvas-start">
                <FileInput size={34} />
                <h2>让数据流动起来</h2>
                <p>导入 CSV / Excel，或从左侧拖入一个数据源。</p>
                <button className="primary" onClick={() => setImportOpen(true)}>
                  从文件开始
                </button>
              </div>
            ) : null}
            <div className="canvas-legend">
              <span>条件分流：首个命中</span>
              <span>Multicast：复制到各分支</span>
              <span>Aggregate：归并统计</span>
            </div>
          </div>
          <ResizeHandle
            label="调整数据预览高度"
            orientation="horizontal"
            value={previewHeight}
            min={170}
            max={650}
            change={setPreviewHeight}
            reverse
          />
          <div className="results-panel">
            {monitor ? (
              <div className="monitor-tabs">
                <button
                  className={!showMonitor ? "active" : ""}
                  onClick={() => setShowMonitor(false)}
                >
                  设计数据预览
                </button>
                <button
                  className={showMonitor ? "active" : ""}
                  onClick={() => setShowMonitor(true)}
                >
                  正式执行 / 核账
                </button>
                <button onClick={onExecution}>打开执行管理</button>
              </div>
            ) : null}
            <div hidden={showMonitor}>
              <PreviewPanel
                value={previewBlock ? undefined : preview?.value}
                node={boundNode}
                item={item}
                input={
                  inputPath
                    ? { from: inputPath.from, output: inputIndex }
                    : undefined
                }
                busy={previewBusy}
                stale={stale}
                blockedReason={previewBlock}
                refresh={() => void readPreview()}
                sourceRows={sourceRows}
                setSourceRows={setSourceRows}
              />
            </div>
            {monitor && active && showMonitor ? (
              <div>
                <RunMonitor
                  key={monitor}
                  ticket={monitor}
                  compact
                  caption={monitorCaption}
                  onFacts={setLiveFacts}
                  select={(pipeline, id) => {
                    selectFlow(pipeline);
                    setSelectedNodes([id]);
                    setInspectorOpen(true);
                  }}
                />
              </div>
            ) : null}
          </div>
        </main>
        <ResizeHandle
          label="调整组件属性宽度"
          value={inspector}
          min={300}
          max={760}
          change={setInspector}
          reverse
        />
        <aside className="inspector">
          <h2>
            组件属性
            <button
              aria-label="关闭属性"
              onClick={() => setInspectorOpen(false)}
            >
              <X size={16} />
            </button>
          </h2>
          {graph && (graph.parallelism || graph.delivery ||
            graph.components.some((node) => node.component === "mq.source" || node.component === "mq.destination")) ? (
            <DataFlowSettings graph={graph} change={(settings) => edit((current) => ({
              ...current,
              dataFlows: current.dataFlows.map((value) => value.id === flow ? { ...value, ...settings } : value),
            }))} />
          ) : null}
          {item && descriptor ? (
            <ComponentInspector
              item={item}
              descriptor={descriptor}
              document={document}
              components={components}
              label={label(item.id)}
              rename={(value) =>
                edit((current) =>
                  withPosition(
                    current,
                    item.id,
                    document.layout?.nodes?.find(
                      (node) => node.flow === flow && node.id === item.id,
                    ) ?? { x: 70, y: 140 },
                    value,
                  ),
                )
              }
              configure={configure}
              remove={remove}
              boundNode={boundNode}
              inputSchema={inputSchema}
              before={before}
              after={after}
              preview={() => void readPreview()}
              previewBusy={previewBusy}
              previewBlocked={previewBlock}
              kafkaTransaction={graph?.delivery?.mode === "kafkaTransaction"}
              stale={stale}
              samples={preview?.value.snapshot.preview.filter((edge) => edge.component === item.id)}
              splitDestinations={splitDestinations}
            />
          ) : activeConstraint ? (
            <label>
              前置条件
              <select
                value={activeConstraint.on}
                onChange={(event) =>
                  edit((current) =>
                    updateControl(current, flow, (graph) => ({
                      ...graph,
                      constraints: graph.constraints.map((edge) =>
                        constraintId(edge) === selectedEdge
                          ? {
                              ...edge,
                              on: event.target.value as
                                | "success"
                                | "failure"
                                | "completion",
                            }
                          : edge,
                      ),
                    })),
                  )
                }
              >
                <option value="success">成功</option>
                <option value="failure">失败</option>
                <option value="completion">完成</option>
              </select>
            </label>
          ) : (
            <div className="empty-state compact">
              <Settings2 size={24} />
              <p>选择节点查看业务配置、输入字段和真实数据。</p>
            </div>
          )}
        </aside>
      </div>
      <footer className="status-bar">
        <span role="status">{message}</span>
        <span>
          {plan?.text === text ? "✓ Native 校验通过" : "草稿待校验"} ·{" "}
          {items.length} 个组件{" "}
          <button onClick={() => setDocumentOpen(true)}>专业包配置</button>
        </span>
      </footer>
      {connectionsOpen ? (
        <ConnectionCenter
          document={document}
          change={(connections) =>
            edit((current) => ({ ...current, connections }))
          }
          changeCaches={(caches) => edit((current) => ({ ...current, caches }))}
          close={() => setConnectionsOpen(false)}
        />
      ) : null}
      {importOpen ? (
        <SourceWizard
          document={document}
          flow={flow}
          close={() => setImportOpen(false)}
          complete={imported}
        />
      ) : null}
      {testOpen ? (
        <TestDialog
          document={document}
          close={() => setTestOpen(false)}
          complete={(run) => {
            setTestOpen(false);
            setMonitorText(dataText);
            setMonitorCaption(
              `隔离测试 · ${document.layout?.label ?? document.id}`,
            );
            setMonitor(run.id);
            setShowMonitor(true);
          }}
        />
      ) : null}
      {confirmation && published ? (
        <ExecutionConfirmation
          document={published.document}
          deployment={published.deployment}
          close={() => setConfirmation(false)}
          complete={(run) => {
            setConfirmation(false);
            setMonitorCaption(undefined);
            setMonitorText(executionText(published.document));
            setMonitor(run.id);
            setShowMonitor(true);
          }}
        />
      ) : null}
      {versionsOpen ? (
        <div className="modal-backdrop">
          <section
            className="modal version-dialog"
            role="dialog"
            aria-modal="true"
            aria-label="版本与比较"
          >
            <header className="dialog-heading">
              <h2>工作草稿 / 保存版本 / 发布版本</h2>
              <button onClick={() => setVersionsOpen(false)}>关闭</button>
            </header>
            {versions.map((version) => (
              <div
                className="version-row"
                key={`${version.id}/${version.revision}`}
              >
                <span>
                  {version.id} · v{String(version.revision)}
                </span>
                <button onClick={() => load(version)}>打开</button>
                <button onClick={() => setComparison(version.document)}>
                  与当前草稿比较
                </button>
              </div>
            ))}
            <button
              disabled={!versions.length || versions.length % 100 !== 0}
              onClick={() => void openVersions(true)}
            >
              更多版本
            </button>
            {remoteDraft ? (
              <div className="inline-actions">
                <p>
                  远程草稿 r{String(remoteDraft.revision)}
                  ；本地编辑仍保留。请选择合并后的保存基准。
                </p>
                <button
                  onClick={() => {
                    draftRevisions.current.set(
                      document.id,
                      remoteDraft.revision,
                    );
                    setDraftConflict(false);
                    setRemoteDraft(undefined);
                    setVersionsOpen(false);
                    setDraftSavedText(undefined);
                  }}
                >
                  保留本地并重新保存
                </button>
                <button
                  onClick={() => {
                    draftRevisions.current.set(
                      document.id,
                      remoteDraft.revision,
                    );
                    resetHistory(remoteDraft.document);
                    setDraftSavedText(encode(remoteDraft.document));
                    setDraftConflict(false);
                    setRemoteDraft(undefined);
                    setVersionsOpen(false);
                  }}
                >
                  采用远程草稿
                </button>
              </div>
            ) : null}
            {comparison ? (
              <VersionDiff previous={comparison} current={document} />
            ) : null}
          </section>
        </div>
      ) : null}
      {documentOpen ? (
        <JsonEditor
          title="专业包配置"
          value={document}
          close={() => setDocumentOpen(false)}
          save={(value) => {
            const next = value as PackageDocument;
            if (
              next.version !== PROTOCOL_VERSION ||
              !Array.isArray(next.dataFlows) ||
              !next.controlFlow
            )
              throw new Error("包格式无效");
            edit(() => next);
            selectFlow(next.dataFlows[0]?.id ?? "control");
            setDocumentOpen(false);
            setPlan(undefined);
            setBinding({});
          }}
        />
      ) : null}
    </>
  );
}
function JsonEditor({
  title,
  value,
  save,
  close,
}: {
  title: string;
  value: unknown;
  save: (value: unknown) => void;
  close: () => void;
}) {
  const [draft, setDraft] = useState(encode(value, 2)),
    [error, setError] = useState("");
  return (
    <div className="modal-backdrop">
      <section
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-label={title}
      >
        <h2>{title}</h2>
        <textarea
          aria-label={title}
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
        />
        {error ? <p role="alert">{error}</p> : null}
        <footer>
          <button onClick={close}>取消</button>
          <button
            className="primary"
            onClick={() => {
              try {
                save(decode(draft));
              } catch (error) {
                setError(errorText(error));
              }
            }}
          >
            应用
          </button>
        </footer>
      </section>
    </div>
  );
}
