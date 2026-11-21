import { useEffect, useRef, useState } from "react";
import { Pause, Play, SkipForward } from "lucide-react";
import { executionFeed, type FeedMode } from "./executionFeed";
import { api, post } from "../../api";
import { DataGrid } from "../../DataGrid";
import { Button } from "../../shared/design-system/Controls";
import {
  executionStateName,
  failurePhase,
  type NativePlan,
  type PreviewEdge,
  type RunState,
} from "../../model";
export interface ProgressNode {
  id: string;
  state?: number;
  result?: number;
  processedRows?: number;
  producedRows?: number;
  rejectedRows?: number;
  callbackMs?: number;
  boundedness?: number;
  idle?: boolean;
  idleMs?: number;
  watermark?: number | bigint;
  rateRowsPerSecond?: number;
  stagedEpoch?: number | bigint;
  ports?: { index: number; rows?: number }[];
}
export interface RunFacts {
  metrics: Record<string, unknown>;
  progress?: {
    pipelines: {
      id: string;
      nodes: ProgressNode[];
      paths?: {
        from: number;
        to: number;
        queueBatches: number;
        queueRows: number;
        referencedBytes: number;
        capacity: number;
      }[];
      confirmedEpoch?: number | bigint;
    }[];
  };
  diagnostics?: NativePlan;
}
export function RunMonitor({
  ticket,
  select,
  onFacts,
  compact = false,
  caption,
}: {
  ticket: string;
  select?: (pipeline: string, component: string) => void;
  onFacts?: (facts: RunFacts) => void;
  compact?: boolean;
  caption?: string;
}) {
  const [run, setRun] = useState<RunState>(),
    [facts, setFacts] = useState<RunFacts>(),
    [snapshot, setSnapshot] = useState<{
      paused: boolean;
      pauseRequested: boolean;
      preview: PreviewEdge[];
    }>(),
    [port, setPort] = useState(""),
    [error, setError] = useState(""),
    [feedMode, setFeedMode] = useState<FeedMode>("连接中"),
    [events, setEvents] = useState<unknown[]>([]),
    [gap, setGap] = useState(false),
    [sampleTime, setSampleTime] = useState<string>();
  const callback = useRef(onFacts);
  useEffect(() => {
    callback.current = onFacts;
  }, [onFacts]);
  useEffect(() => {
    const controller = new AbortController();
    let readingDebug = false;
    void executionFeed(
      ticket,
      controller.signal,
      (snapshot) => {
        setRun(snapshot.run);
        setFacts(snapshot.facts);
        callback.current?.(snapshot.facts);
        if (snapshot.gap) setGap(true);
        if (snapshot.events.length)
          setEvents((current) => [...current, ...snapshot.events].slice(-100));
        if (
          snapshot.run.debug &&
          !readingDebug &&
          !["Queued", "Claimed"].includes(snapshot.run.state)
        ) {
          readingDebug = true;
          void api<{
            paused: boolean;
            pauseRequested: boolean;
            preview: PreviewEdge[];
          }>(`/executions/${ticket}/debug`, { signal: controller.signal })
            .then((value) => {
              if (!controller.signal.aborted) {
                setSnapshot(value);
                setSampleTime(new Date().toLocaleTimeString());
              }
            })
            .catch(() => {})
            .finally(() => {
              readingDebug = false;
            });
        }
      },
      setFeedMode,
    ).catch((error) => {
      if (!controller.signal.aborted) setError(String(error));
    });
    return () => controller.abort();
  }, [ticket]);
  async function control(action: string) {
    try {
      await post(`/executions/${ticket}/debug`, { action });
    } catch (error) {
      setError(String(error));
    }
  }
  async function runAction(action: "stop" | "cancel" | "resume") {
    try {
      if (action === "resume") await post(`/executions/${ticket}/resume`);
      else await post(`/executions/${ticket}/${action}`);
    } catch (failure) {
      setError(String(failure));
    }
  }
  const rows: (ProgressNode & { pipeline: string })[] =
    facts?.progress?.pipelines.flatMap((pipeline) =>
      pipeline.nodes.map((node) => ({ ...node, pipeline: pipeline.id })),
    ) ??
    facts?.diagnostics?.pipelines.flatMap((pipeline) =>
      pipeline.trees.flatMap((tree) =>
        tree.nodes.map((node) => ({ ...node, pipeline: pipeline.id })),
      ),
    ) ??
    [];
  const edges = snapshot?.preview ?? [],
    edge =
      edges.find(
        (edge) => `${edge.pipeline}/${edge.component}/${edge.port}` === port,
      ) ?? edges[0];
  const metrics = facts?.metrics ?? {};
  const allowed = new Set(run?.allowedActions ?? (
    run?.state === "Running" ? ["stop", "cancel"] :
      run?.state === "RecoveryRequired" ? ["resume", "cancel"] :
        ["Queued", "Claimed"].includes(run?.state ?? "") ? ["cancel"] : []
  ));
  if (run?.canResume === true) allowed.add("resume");
  if (run?.canResume === false) allowed.delete("resume");
  const diagnosticNodes = facts?.diagnostics?.pipelines.flatMap((pipeline) =>
    pipeline.trees.flatMap((tree) => tree.nodes.map((node) => ({ ...node, pipeline: pipeline.id }))),
  ) ?? [];
  return (
    <section className={`run-monitor${compact ? " compact" : ""}`}>
      <p className="feed-status" role="status">
        {feedMode} · Native Catalog{facts?.metrics.observedAtMs ? ` · 快照 ${new Date(Number(facts.metrics.observedAtMs)).toLocaleTimeString()}` : ""}{gap ? " · 事件有缺口，已补齐最新快照" : ""}
      </p>
      <header>
        <h3>
          {run
            ? `${caption ?? `${run.packageId} v${run.packageRevision}`} · ${executionStateName(run)}`
            : "读取 Native 执行事实…"}
        </h3>
        {allowed.has("stop") ? (
          <Button onClick={() => void runAction("stop")}>
            停止并排空
          </Button>
        ) : null}
        {allowed.has("cancel") ? <Button onClick={() => void runAction("cancel")}>取消执行</Button> : null}
        {allowed.has("resume") ? <Button className="primary" onClick={() => void runAction("resume")}>恢复同一执行</Button> : null}
        {run?.debug ? (
          <div className="inline-actions">
            <button
              disabled={run.state !== "Running"}
              onClick={() => void control("pause")}
            >
              <Pause size={14} />
              暂停
            </button>
            <button
              disabled={!snapshot?.paused || run.state !== "Running"}
              onClick={() => void control("continue")}
            >
              <Play size={14} />
              继续
            </button>
            <button
              disabled={!snapshot?.paused || run.state !== "Running"}
              onClick={() => void control("step")}
            >
              <SkipForward size={14} />
              单批次
            </button>
          </div>
        ) : null}
      </header>
      {error ? <p role="alert">{error}</p> : null}
      {run?.state === "Queued" ? (
        <p role="status">
          {run.queueReason ?? "等待派发"} · 已等待 {Math.floor((run.queueWaitMs ?? 0) / 1000)} 秒
        </p>
      ) : null}
      {run?.failure ? (
        <p role="alert">
          {failurePhase(run.failure.phase)}失败 · 状态码 {run.failure.result}
          {run.failure.detail?.sqlState ? ` · SQLSTATE ${run.failure.detail.sqlState}` : ""}
          {run.failure.detail?.httpStatus ? ` · HTTP ${run.failure.detail.httpStatus}` : ""}
          {run.failure.detail?.exitCode != null ? ` · Worker 退出码 ${run.failure.detail.exitCode}` : ""}
          {run.failure.detail?.message ? ` · ${run.failure.detail.message}` : ""}
        </p>
      ) : null}
      {facts?.diagnostics?.schemas?.map((schema, index) => (
        <details key={`${schema.task}/${schema.component}/${index}`} open={!schema.diff.compatible}>
          <summary>{schema.task} / {schema.component} · {schema.diff.compatible ? "结构变更已接受" : "结构变更待处理"}</summary>
          <ul>{schema.diff.changes.map((change, position) => (
            <li key={`${change.columnId}/${position}`}>{change.column ?? change.columnId} · {change.kind}</li>
          ))}</ul>
          {!schema.diff.compatible ? <p>核对源表字段、主键和最后确认位点。需要修改作业契约时，发布新版本并明确起点。</p> : null}
        </details>
      ))}
      {facts?.diagnostics?.tasks?.filter(task => task.httpStatus !== undefined).map((task, index) => (
        <details key={`${task.activation}/${index}`}>
          <summary>{task.activation} · HTTP {task.httpStatus}</summary>
          {task.responseHeaders ? <pre>{task.responseHeaders.trimEnd()}</pre> : null}
          {task.responseHeadersOmitted ? <p>响应头超出运行历史记录上限。</p> : null}
        </details>
      ))}
      <div className="native-metrics">
        {[
          ["inputRows", "读取行"],
          ["outputRows", "目标处理行"],
          ["committedRows", "已提交行"],
          ["acknowledgedRows", "已确认行"],
          ["errorRows", "异常行"],
          ["memoryUsed", "内存字节"],
          ["spillBytes", "Spill 字节"],
          ["queuedBatches", "排队批次"],
        ].map(([key, label]) => (
          <dl key={key}>
            <dt>{label}</dt>
            <dd>{metrics[key] === undefined ? "—" : String(metrics[key])}</dd>
          </dl>
        ))}
      </div>
      {run?.state === "RecoveryRequired" ? (
        <p className="warning">
          {run.requiresJournal === false
            ? "这是普通流式执行，未建立 Native Journal；已知提交量保留在历史中，不能恢复或重放。"
            : "提交状态未确认。等待 Native 回执与 Journal 恢复，不能将重试视为已提交。"}
        </p>
      ) : null}
      {diagnosticNodes.length ? (
        <details className="native-diagnostics">
          <summary>Native 诊断与目标承诺</summary>
          <div className="data-grid-scroll">
            <table>
              <thead><tr><th>Pipeline / 节点</th><th>结果</th><th>源重放</th><th>可见性</th><th>确认</th></tr></thead>
              <tbody>{diagnosticNodes.map((node) => <tr key={`${node.pipeline}/${node.id}`}>
                <td>{node.pipeline} / {node.id}</td>
                <td>{node.result ?? 0}</td>
                <td>{node.sourceReplay === undefined ? "—" : node.sourceReplay ? "支持" : "不支持"}</td>
                <td>{node.delivery?.visibility ?? "—"}</td>
                <td>{node.delivery?.confirmation ?? "—"}</td>
              </tr>)}</tbody>
            </table>
          </div>
        </details>
      ) : null}
      <div className="data-grid-scroll">
        <table>
          <thead>
            <tr>
              <th>组件</th>
              <th>输入行</th>
              <th>输出行</th>
              <th>异常行</th>
              <th>处理耗时</th>
              <th>输入语义</th>
              <th>行/秒</th>
              <th>Watermark (µs)</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((node) => (
              <tr
                key={node.pipeline + "/" + node.id}
                onClick={() => select?.(node.pipeline, node.id)}
              >
                <td>
                  <button className="text-button">{node.id}</button>
                </td>
                <td>{String(node.processedRows ?? 0)}</td>
                <td>{String(node.producedRows ?? 0)}</td>
                <td>{String(node.rejectedRows ?? 0)}</td>
                <td>{String(node.callbackMs ?? 0)} ms</td>
                <td>
                  {node.boundedness
                    ? node.idle
                      ? "持续 · 空闲"
                      : "持续"
                    : "有界"}
                </td>
                <td>{node.rateRowsPerSecond?.toFixed(1) ?? "—"}</td>
                <td>
                  {node.watermark === undefined ||
                  String(node.watermark) === "-9223372036854775808"
                    ? "—"
                    : String(node.watermark)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {facts?.progress?.pipelines.map((pipeline) => (
        <div key={pipeline.id} className="path-metrics">
          <strong>
            {pipeline.id} · 已确认 Epoch {String(pipeline.confirmedEpoch ?? 0)}
          </strong>
          {pipeline.paths?.map((path, index) => (
            <p key={index}>
              {pipeline.nodes[path.from]?.id} → {pipeline.nodes[path.to]?.id} ·
              排队 {path.queueBatches}/{path.capacity} 批 · {path.queueRows} 行
              · 引用 {path.referencedBytes} 字节
              {path.queueBatches >= path.capacity ? " · 背压" : ""}
            </p>
          ))}
        </div>
      ))}
      <details>
        <summary>Native 事件时间轴（保留最近 100 条）</summary>
        <pre>
          {events
            .map((event) =>
              JSON.stringify(event, (_, value) =>
                typeof value === "bigint" ? String(value) : value,
              ),
            )
            .join("\n")}
        </pre>
      </details>
      <p className="muted">
        分流输出按首个命中核账；复制与聚合组件的输出不能直接相加。提交与确认计数来自
        Native。
      </p>
      {edges.length ? (
        <>
          <label>
            查看真实端口数据 · 首批有界样本 · 最近读取 {sampleTime ?? "—"}
            （非滚动全量数据）
            <select
              value={
                edge ? `${edge.pipeline}/${edge.component}/${edge.port}` : ""
              }
              onChange={(event) => setPort(event.target.value)}
            >
              {edges.map((edge) => (
                <option
                  key={`${edge.pipeline}/${edge.component}/${edge.port}`}
                  value={`${edge.pipeline}/${edge.component}/${edge.port}`}
                >
                  {edge.component} · 输出 {edge.port} · {edge.totalRows} 行
                </option>
              ))}
            </select>
          </label>
          <DataGrid schema={edge?.schema} rows={edge?.rows} />
        </>
      ) : null}
    </section>
  );
}
