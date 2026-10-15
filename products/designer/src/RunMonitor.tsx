import { useEffect, useRef, useState } from "react";
import { Pause, Play, SkipForward } from "lucide-react";
import { api, post } from "./api";
import { DataGrid } from "./DataGrid";
import {
  stateName,
  failurePhase,
  type NativePlan,
  type PreviewEdge,
  type RunState,
} from "./model";
export interface ProgressNode {
  id: string;
  state?: number;
  result?: number;
  processedRows?: number;
  producedRows?: number;
  rejectedRows?: number;
  callbackMs?: number;
  ports?: { index: number; rows: number }[];
}
export interface RunFacts {
  metrics: Record<string, unknown>;
  progress?: { pipelines: { id: string; nodes: ProgressNode[] }[] };
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
    [error, setError] = useState("");
  const callback = useRef(onFacts);
  useEffect(() => {
    callback.current = onFacts;
  }, [onFacts]);
  useEffect(() => {
    const controller = new AbortController();
    let polling = false;
    async function refresh() {
      if (polling) return;
      polling = true;
      try {
        const [run, facts] = await Promise.all([
          api<RunState>(`/executions/${ticket}`, { signal: controller.signal }),
          api<RunFacts>(`/executions/${ticket}/metrics`, {
            signal: controller.signal,
          }),
        ]);
        if (controller.signal.aborted) return;
        setRun(run);
        setFacts(facts);
        callback.current?.(facts);
        if (run.debug && run.state !== "Queued" && run.state !== "Claimed") {
          try {
            const debug = await api<{
              paused: boolean;
              pauseRequested: boolean;
              preview: PreviewEdge[];
            }>(`/executions/${ticket}/debug`, { signal: controller.signal });
            if (!controller.signal.aborted) setSnapshot(debug);
          } catch {
            /* State may finish between a control request and the next poll. */
          }
        }
      } catch (error) {
        if (!controller.signal.aborted) setError(String(error));
      } finally {
        polling = false;
      }
    }
    void refresh();
    const timer = window.setInterval(refresh, 1200);
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [ticket]);
  async function control(action: string) {
    try {
      await post(`/executions/${ticket}/debug`, { action });
    } catch (error) {
      setError(String(error));
    }
  }
  const rows =
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
      <header>
        <h3>
          {run
            ? `${caption ?? `${run.packageId} v${run.packageRevision}`} · ${stateName(run.state)}`
            : "读取 Native 执行事实…"}
        </h3>
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
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="muted">
        分流输出按首个命中核账；复制与聚合组件的输出不能直接相加。提交与确认计数来自
        Native。
      </p>
      {edges.length ? (
        <>
          <label>
            查看真实端口数据
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
