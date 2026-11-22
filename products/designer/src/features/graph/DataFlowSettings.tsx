import { Input, Select } from "../../shared/design-system/Controls";
import { transactionLabel, type BoundPipeline, type DataFlow, type DataFlowParallelism } from "../../model";

const limits: { key: keyof DataFlowParallelism; label: string }[] = [
  { key: "workerProcesses", label: "Worker 进程数" },
  { key: "consumerLanesPerWorker", label: "每个 Worker 的消费实例数" },
  { key: "threadsPerWorker", label: "每个 Worker 的线程数" },
];

export function DataFlowSettings({
  graph, bound, change,
}: {
  graph: DataFlow;
  bound?: BoundPipeline;
  change: (settings: Partial<Pick<DataFlow, "parallelism" | "delivery">>) => void;
}) {
  const { parallelism, delivery } = graph;
  const transaction = delivery?.mode === "kafkaTransaction" ? delivery : undefined;
  return (
    <details>
      <summary>消息流运行设置</summary>
      <fieldset className="property-group">
        <legend>并行</legend>
        <label className="checkbox">
          <Input type="checkbox" checked={parallelism !== undefined}
            onChange={(event) => change({ parallelism: event.target.checked ? {} : undefined })} />
          配置 Worker 与消费实例
        </label>
        {parallelism ? (
          <>
            {limits.map(({ key, label }) => (
              <label key={key}>
                {label}
                <Input type="number" min={1} max={64} step={1}
                  value={parallelism[key] ?? ""} placeholder="默认 1"
                  onChange={(event) => change({
                    parallelism: { ...parallelism, [key]: event.target.value === "" ? undefined : event.target.valueAsNumber },
                  })} />
              </label>
            ))}
            <p className="property-help">消费实例数不能超过线程数。实际并行度受宿主限制。</p>
          </>
        ) : null}
      </fieldset>
      <fieldset className="property-group">
        <legend>投递</legend>
        <label>
          投递模式
          <Select value={delivery?.mode ?? "atLeastOnce"} onChange={(event) => {
            const mode = event.target.value;
            if (mode === "kafkaTransaction")
              change({ delivery: { mode, checkpointTopic: "", checkpointPartition: 0 } });
            else if (mode === "eventReceipt" || mode === "atLeastOnce")
              change({ delivery: { mode } });
          }}>
            <option value="atLeastOnce">至少一次</option>
            <option value="kafkaTransaction">Kafka 事务</option>
            <option value="eventReceipt">RabbitMQ 到 SQL 消息回执</option>
          </Select>
        </label>
        {transaction ? (
          <>
            <label>
              检查点主题
              <Input required value={transaction.checkpointTopic}
                onChange={(event) => change({ delivery: { ...transaction, checkpointTopic: event.target.value } })} />
            </label>
            <label>
              检查点分区
              <Input type="number" min={0} max={2147483647} step={1}
                value={transaction.checkpointPartition ?? ""} placeholder="默认 0"
                onChange={(event) => change({
                  delivery: { ...transaction, checkpointPartition: event.target.value === "" ? undefined : event.target.valueAsNumber },
                })} />
            </label>
            <p className="property-help">源、目标与检查点使用同一个 Kafka 连接。检查点主题须独立于业务主题。并行实例仅支持无状态流，每个实例使用一个检查点分区。</p>
          </>
        ) : null}
        {delivery?.mode === "eventReceipt" ? (
          <p className="property-help">
            一条 RabbitMQ 队列直接连到一个支持回执的 SQL 目标。关闭消息元数据和流并行，生产者须提供不可变消息 ID。
          </p>
        ) : null}
        {bound?.deliveryMode === "eventReceipt" ? (
          <p className="property-help">Native 已绑定逐消息 SQL 回执，目标提交之后才确认消费。</p>
        ) : null}
        {bound?.transaction ? (
          <p className="property-help">Native 事务声明 · {transactionLabel(bound.transaction)}</p>
        ) : null}
      </fieldset>
    </details>
  );
}
