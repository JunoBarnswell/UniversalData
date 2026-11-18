import { Button, Input } from "./shared/design-system/Controls";
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
          node.component === "mq.source" ||
          node.component === "sql.command",
      )
      .map((node) => ({
        flow: flow.id,
        delivery: flow.delivery,
        node,
        connection: document.connections?.find(
          (connection) => connection.id === node.config.connection,
        ),
      })),
  );
  const consumesMessages = targets.some(({ node }) => node.component === "mq.source");
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
              <th>连接 / 资源</th>
              <th>表 / 消息地址</th>
              <th>执行影响</th>
            </tr>
          </thead>
          <tbody>
            {targets.map(({ flow, node, connection, delivery }) => (
              <tr key={flow + "/" + node.id}>
                <td>
                  {connection?.id}
                  <small>{connection?.resource ?? "组件内部资源"}</small>
                </td>
                <td>
                  {node.component === "mq.source" || node.component === "mq.destination" ? (
                    <>
                      {node.config.targetType === "queue" ? "队列" :
                        node.config.targetType === "topic" ? "主题" :
                        node.config.targetType === "exchange" ? "交换机" : "消息地址"}
                      {" · "}{String(node.config.address ?? "未配置")}
                      {typeof node.config.group === "string" && node.config.group ? (
                        <small>消费组 {node.config.group}</small>
                      ) : null}
                      {typeof node.config.subscription === "string" && node.config.subscription ? (
                        <small>订阅 {node.config.subscription}</small>
                      ) : null}
                      {typeof node.config.routingKey === "string" && node.config.routingKey ? (
                        <small>路由键 {node.config.routingKey}</small>
                      ) : null}
                    </>
                  ) : String(node.config.table ?? node.component)}
                </td>
                <td>
                  {node.component === "mq.source" ? (
                    delivery?.mode === "kafkaTransaction" ? (
                      <>
                        事务提交时推进消费组位点
                        <small>检查点 {delivery.checkpointTopic} / 分区 {delivery.checkpointPartition ?? 0}</small>
                      </>
                    ) : "检查点完成后确认消费，可能移除队列消息或推进消费组位点"
                  ) : node.component === "mq.destination" ? (
                    delivery?.mode === "kafkaTransaction" ? "在 Kafka 事务中发布消息" : "向 broker 发布消息"
                  ) : String(node.config.mode ?? node.config.fileMode ?? "外部写入")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <p>执行将使用这一已发布版本；后续草稿更改将进入下一个版本。</p>
        <label className="checkbox">
          <Input
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
                <Input
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
          <Input
            aria-label="确认正式业务写入"
            type="checkbox"
            checked={confirmed}
            onChange={(event) => setConfirmed(event.target.checked)}
          />
          {consumesMessages
            ? "我已核对版本、目标资源和消费确认影响，确认执行"
            : "我已核对版本、环境和写入目标，确认执行"}
        </label>
        {error ? <p role="alert">{error}</p> : null}
        <footer>
          <Button onClick={close}>取消</Button>
          <Button
            className="primary"
            disabled={!confirmed || busy}
            onClick={() => void run()}
          >
            {busy ? "提交中…" : "确认正式执行"}
          </Button>
        </footer>
      </section>
    </div>
  );
}
