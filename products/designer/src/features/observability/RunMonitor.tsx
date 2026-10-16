import { useEffect, useRef, useState } from "react";
import { Pause, Play, SkipForward } from "lucide-react";
import { executionFeed, type FeedMode } from "./executionFeed";
import { api, post } from "../../api";
import { DataGrid } from "../../DataGrid";
import {
  stateName,
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
  return (
    <section className={`run-monitor${compact ? " compact" : ""}`}>
      <p className="feed-status" role="status">
        {feedMode} · Native Catalog{facts?.metrics.observedAtMs ? ` · 快照 ${new Date(Number(facts.metrics.observedAtMs)).toLocaleTimeString()}` : ""}{gap ? " · 事件有缺口，已补齐最新快照" : ""}
      </p>
      <header>
        <h3>
          {run
            ? `${caption ?? `${run.packageId} v${run.packageRevision}`} · ${stateName(run.state)}`
            : "读取 Native 执行事实…"}
        </h3>
        {run?.state === "Running" ? (
          <button
            onClick={() =>
              void post(`/executions/${ticket}/stop`).catch((error) =>
                setError(String(error)),
              )
            }
          >
            停止并排空
          </button>
        ) : null}
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
      {run?.failure ? (
        <p role="alert">
          {failurePhase(run.failure.phase)}失败 · 状态码 {run.failure.result}
        </p>
      ) : null}
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
          提交状态未确认。等待 Native 回执与 Journal
          恢复，不能将重试视为已提交。
        </p>
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
