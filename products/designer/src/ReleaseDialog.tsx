import { useState } from "react";
import { post } from "./api";
import type {
  ControlGraph,
  Deployment,
  PackageDocument,
  RunState,
} from "./model";
export function ExecutionConfirmation({
  document,
  deployment,
  close,
  complete,
}: {
  document: PackageDocument;
  deployment: Deployment;
  close: () => void;
  complete: (run: RunState) => void;
}) {
  const [confirmed, setConfirmed] = useState(false),
    [debug, setDebug] = useState(false),
    [breakpoints, setBreakpoints] = useState<string[][]>([]),
    [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const targets = document.dataFlows.flatMap((flow) =>
    flow.components
      .filter(
        (node) =>
          node.component.includes("destination") ||
          node.component === "sql.command",
      )
      .map((node) => ({
        flow: flow.id,
        node,
        connection: document.connections?.find(
          (connection) => connection.id === node.config.connection,
        ),
      })),
  );
  async function run() {
    setBusy(true);
    try {
      const result = await post<RunState>("/executions", {
        packageId: deployment.packageId,
        packageRevision: deployment.packageRevision,
        deploymentId: deployment.id,
        deploymentRevision: deployment.deploymentRevision,
        debug: debug
          ? {
              previewRows: 16,
              breakpoints: [],
              definitionBreakpoints: breakpoints,
            }
          : undefined,
      });
      complete(result);
    } catch (error) {
      setError(String(error));
    } finally {
      setBusy(false);
    }
  }
  const tasks: { path: string[]; label: string }[] = [];
  function taskOptions(graph: ControlGraph, parent: string[] = []) {
    for (const task of graph.tasks) {
      const path = [...parent, task.id];
      tasks.push({ path, label: path.join(" / ") });
      if (task.children) taskOptions(task.children, path);
    }
  }
  taskOptions(document.controlFlow);
  return (
    <div className="modal-backdrop">
      <section
        className="modal execution-confirmation"
        role="dialog"
        aria-modal="true"
        aria-label="确认正式执行"
      >
        <h2>正式执行</h2>
        <p>
          包 <strong>{document.layout?.label ?? deployment.packageId}</strong> ·
          固定版本 <strong>v{deployment.packageRevision}</strong> · 发布 v
          {deployment.deploymentRevision}
        </p>
        <table>
          <thead>
            <tr>
              <th>目标环境 / 资源</th>
              <th>目标表 / 组件</th>
              <th>写入方式</th>
            </tr>
          </thead>
          <tbody>
            {targets.map(({ flow, node, connection }) => (
              <tr key={flow + "/" + node.id}>
                <td>
                  {connection?.id}
                  <small>{connection?.resource ?? "组件内部资源"}</small>
                </td>
                <td>{String(node.config.table ?? node.component)}</td>
                <td>
                  {String(
                    node.config.mode ?? node.config.fileMode ?? "外部写入",
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <p>执行将使用这一已发布版本；后续草稿更改将进入下一个版本。</p>
        <label className="checkbox">
          <input
            type="checkbox"
            checked={debug}
            onChange={(event) => setDebug(event.target.checked)}
          />
          开启 Native 调试与有界数据查看
        </label>
        {debug ? (
          <fieldset>
            <legend>任务断点</legend>
            {tasks.map((task) => (
              <label className="checkbox" key={JSON.stringify(task.path)}>
                <input
                  type="checkbox"
                  checked={breakpoints.some(
                    (path) =>
                      JSON.stringify(path) === JSON.stringify(task.path),
                  )}
                  onChange={(event) =>
                    setBreakpoints((current) =>
                      event.target.checked
                        ? [...current, task.path]
                        : current.filter(
                            (path) =>
                              JSON.stringify(path) !==
                              JSON.stringify(task.path),
                          ),
                    )
                  }
                />
                {task.label}
              </label>
            ))}
            <p className="muted">
              任务定义由 Native
              匹配。断点仅在可暂停的边界生效，活动事务内不中断。
            </p>
          </fieldset>
        ) : null}
        <label className="checkbox">
          <input
            aria-label="确认正式业务写入"
            type="checkbox"
            checked={confirmed}
            onChange={(event) => setConfirmed(event.target.checked)}
          />
          我已核对版本、环境、目标表及写入模式，确认执行
        </label>
        {error ? <p role="alert">{error}</p> : null}
        <footer>
          <button onClick={close}>取消</button>
          <button
            className="primary"
            disabled={!confirmed || busy}
            onClick={() => void run()}
          >
            {busy ? "提交中…" : "确认正式执行"}
          </button>
        </footer>
      </section>
    </div>
  );
}
