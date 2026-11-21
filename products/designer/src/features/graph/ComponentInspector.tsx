import { Trash2 } from "lucide-react";
import { useEffect, useState } from "react";
import { api, encode } from "../../api";
import { Button, Input, Select } from "../../shared/design-system/Controls";
import { PropertyForm } from "../../PropertyForm";
import { FileSourceEditor } from "../../FileSourceEditor";
import { FieldMapping } from "../../FieldMapping";
import { SSISColumnEditor, SSISSplitEditor } from "../../SSISColumnEditor";
import { SqlEditor, ConnectionSelect } from "../../SqlEditors";
import { AggregateEditor, JoinEditor, LookupEditor } from "../../RelationalEditors";
import {
  MESSAGE_PUBLISH, MESSAGE_SUBSCRIBE, MESSAGE_TRANSACTION, nativeConnector,
  type BoundNode, type Component, type Configuration, type ConnectorDescriptor,
  type DataSchema, type FlowComponent, type PackageDocument, type PreviewEdge,
} from "../../model";

export function ComponentInspector({
  item, descriptor, document, components, label, rename, configure, remove, boundNode,
  inputSchema, before, after, preview, previewBusy, previewBlocked, stale, samples,
  splitDestinations, kafkaTransaction,
}: {
  item: FlowComponent;
  descriptor: Component;
  document: PackageDocument;
  components: Component[];
  label: string;
  rename: (value: string) => void;
  configure: (value: Configuration) => void;
  remove: () => void;
  boundNode?: BoundNode;
  inputSchema?: DataSchema | null;
  before?: PreviewEdge;
  after?: PreviewEdge;
  preview: () => void;
  previewBusy: boolean;
  previewBlocked?: string;
  stale: boolean;
  samples?: PreviewEdge[];
  splitDestinations: Record<number, string>;
  kafkaTransaction: boolean;
}) {
  const messageSource = item.component === "mq.source";
  const messageDirection = messageSource ? MESSAGE_SUBSCRIBE :
    item.component === "mq.destination" ? MESSAGE_PUBLISH : 0;
  const [connectors, setConnectors] = useState<ConnectorDescriptor[]>();
  useEffect(() => {
    if (!messageDirection) return;
    const controller = new AbortController();
    api<ConnectorDescriptor[]>("/connectors", { signal: controller.signal })
      .then(setConnectors)
      .catch(() => {
        if (!controller.signal.aborted) setConnectors(undefined);
      });
    return () => controller.abort();
  }, [messageDirection]);
  const messageConnections = (document.connections ?? []).filter((connection) => {
    if (connection.driver === "file" || connection.driver === "excel") return false;
    if (!connectors) return true;
    const connector = nativeConnector(connection, connectors);
    if (!connector || connector.result !== 0 || !(connector.capabilities & messageDirection))
      return false;
    if (kafkaTransaction && !(connector.capabilities & MESSAGE_TRANSACTION)) return false;
    const kind = item.config.targetType;
    if (connector.id === "org.universaldata.connector.kafka" ||
        connector.id === "org.universaldata.connector.rocketmq")
      return kind === undefined || kind === "topic";
    if (connector.id === "org.universaldata.connector.rabbitmq")
      return kind === undefined || kind === "queue" || (!messageSource && kind === "exchange");
    if (connector.id === "org.universaldata.connector.activemq") return kind !== "exchange";
    return true;
  });
  const currentConnection = String(item.config.connection ?? "");
  return (
    <>
      <label>
        显示名称
        <Input aria-label="组件显示名称" value={label} onChange={(event) => rename(event.target.value)} />
      </label>
      <div className="inspector-actions">
        <strong>{descriptor.name}</strong>
        <Button aria-label="删除组件" onClick={remove}>
          <Trash2 size={15} />
        </Button>
      </div>
      {item.component === "derive" ? (
        <SSISColumnEditor
          value={item.config}
          change={configure}
          schema={inputSchema ?? undefined}
          functions={components.filter(
            (component) => component.category === 5,
          )}
          document={document}
          before={before}
          after={after}
          preview={previewBlocked ? undefined : preview}
          busy={previewBusy}
        />
      ) : item.component === "split" &&
        item.config.condition === undefined ? (
        <SSISSplitEditor
          value={item.config}
          change={configure}
          schema={inputSchema ?? undefined}
          functions={components.filter(
            (component) => component.category === 5,
          )}
          document={document}
          samples={stale ? undefined : samples}
          destinations={splitDestinations}
          preview={previewBlocked ? undefined : preview}
        />
      ) : item.component === "eventWindow" ? (
        <>
          <p className="muted">
            事件时间采用微秒；窗口 [start, end) 在 Watermark ≥ end +
            允许迟到后输出最终结果。迟到输出必须连接端口 1。
          </p>
          <PropertyForm
            schema={{
              ...descriptor.configurationSchema,
              properties: Object.fromEntries(
                Object.entries(
                  descriptor.configurationSchema.properties ?? {},
                ).filter(([key]) => key !== "plan"),
              ),
            }}
            value={Object.fromEntries(
              Object.entries(item.config).filter(
                ([key]) => key !== "plan",
              ),
            )}
            change={(value) =>
              configure({ ...value, plan: item.config.plan })
            }
            fields={inputSchema?.fields}
          />
          <AggregateEditor
            value={item.config}
            change={configure}
            fields={inputSchema?.fields ?? []}
          />
        </>
      ) : item.component === "aggregate" ? (
        <AggregateEditor
          value={item.config}
          change={configure}
          fields={inputSchema?.fields ?? []}
        />
      ) : item.component === "join" ? (
        <JoinEditor
          value={item.config}
          change={configure}
          inputs={
            boundNode?.inputs.map((schema) => schema?.fields ?? []) ?? [
              inputSchema?.fields ?? [],
            ]
          }
        />
      ) : item.component === "file.csv.source" ||
        item.component === "file.xlsx.source" ? (
        <FileSourceEditor
          document={document}
          value={item.config}
          change={configure}
          xlsx={item.component === "file.xlsx.source"}
          schema={descriptor.configurationSchema}
        />
      ) : item.component === "cacheLookup" ||
        item.component === "dbLookup" ? (
        <LookupEditor
          value={item.config}
          change={configure}
          fields={inputSchema?.fields ?? []}
          schema={descriptor.configurationSchema}
          document={document}
        />
      ) : item.component === "sql.destination" ||
        item.component === "sql.source" ? (
        <SqlEditor
          key={item.id}
          document={document}
          value={item.config}
          change={configure}
          input={inputSchema ?? undefined}
          source={item.component === "sql.source"}
          configurationSchema={descriptor.configurationSchema}
        />
      ) : (
        <>
          {Object.hasOwn(
            descriptor.configurationSchema.properties ?? {},
            "connection",
          ) ? (
            messageDirection ? (
              <label>
                连接
                <Select aria-label="组件连接" value={currentConnection}
                  onChange={(event) => configure({ ...item.config, connection: event.target.value })}>
                  <option value="">选择连接</option>
                  {currentConnection && !messageConnections.some((value) => value.id === currentConnection) ? (
                    <option value={currentConnection} disabled>{currentConnection} · 当前能力不匹配</option>
                  ) : null}
                  {messageConnections.map((connection) => (
                    <option key={connection.id} value={connection.id}>{connection.id} · {connection.driver}</option>
                  ))}
                </Select>
                <small className="property-help">
                  {connectors ? (messageConnections.length ? null : "没有匹配当前消息类型的连接，请在连接中心添加。") :
                    "连接能力由 Native 校验。"}
                </small>
              </label>
            ) : (
              <ConnectionSelect
                document={document}
                value={currentConnection}
                change={(connection) => configure({ ...item.config, connection })}
              />
            )
          ) : null}
          <PropertyForm
            key={item.id}
            schema={{
              ...descriptor.configurationSchema,
              properties: Object.fromEntries(
                Object.entries(
                  descriptor.configurationSchema.properties ?? {},
                ).filter(([key]) => key !== "connection"),
              ),
            }}
            value={Object.fromEntries(
              Object.entries(item.config).filter(
                ([key]) => key !== "connection",
              ),
            )}
            change={(value) =>
              configure({
                ...value,
                ...(item.config.connection !== undefined
                  ? { connection: item.config.connection }
                  : {}),
              })
            }
            fields={inputSchema?.fields}
            inputs={boundNode?.inputs.map(
              (schema) => schema?.fields ?? [],
            )}
          />
        </>
      )}
      {item.component === "copyColumn" && inputSchema ? (
        <FieldMapping
          fields={inputSchema.fields}
          value={item.config}
          change={configure}
        />
      ) : null}
      {descriptor.continuous ? (
        <p className="muted">
          Native 持续源 ·
          结束由源模式和停止策略决定；空闲不等于完成。位点与确认沿用
          Native Journal。
        </p>
      ) : null}
      {boundNode?.sourceReplay !== undefined || boundNode?.delivery ? (
        <section className="native-contract" aria-label="Native 绑定契约">
          <h3>Native 绑定事实</h3>
          {boundNode.sourceReplay !== undefined ? (
            <p>源重放：{boundNode.sourceReplay ? "支持" : "不支持"}</p>
          ) : null}
          {boundNode.delivery ? (
            <p>目标确认：{boundNode.delivery.visibility} / {boundNode.delivery.confirmation}</p>
          ) : null}
          {boundNode.ports.filter((port) => port.changeMode).length ? (
            <ul>
              {boundNode.ports.filter((port) => port.changeMode).map((port) => (
                <li key={`${port.direction}/${port.index}`}>端口 {port.name || port.index}：{port.changeMode}</li>
              ))}
            </ul>
          ) : null}
        </section>
      ) : null}
      <details>
        <summary>组件身份与专业信息</summary>
        <code>{item.id}</code>
        {boundNode ? <pre>{encode(boundNode, 2)}</pre> : null}
      </details>
    </>
  );
}
