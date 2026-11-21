import { useCallback, useMemo, useState } from "react";
import { FileClock, GitBranch, LineChart, X } from "lucide-react";
import { decode } from "./api";
import { Button, Input, Select, TextArea } from "./shared/design-system/Controls";
import {
  buildJobTemplate,
  templateNames,
  type BuiltTemplate,
  type JobTemplateKind,
  type TemplateConnectionInput,
  type TemplateOptions,
  type TemplateSchedule,
} from "./examples/templates";
import type { PackageDocument } from "./model";

interface Props {
  document: PackageDocument;
  close: () => void;
  complete: (template: BuiltTemplate) => void;
}

const defaultConnection = (
  id: string,
  driver: string,
  resource: string,
  credentialReference = "",
): TemplateConnectionInput => ({ id, driver, resource, credentialReference });

const defaultSchedule = (): TemplateSchedule => ({
  id: "default",
  frequency: "Interval",
  startAt: new Date().toISOString(),
  enabled: true,
  intervalSeconds: 3600,
  timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
});

function isPathStyleTlsUrl(value: string) {
  try {
    const url = new URL(value);
    return url.protocol === "https:" && Boolean(url.hostname) &&
      url.pathname.split("/").filter(Boolean).length >= 2;
  } catch {
    return false;
  }
}

function initialOptions(document: PackageDocument): TemplateOptions {
  const existing = document.connections ?? [];
  const find = (id: string, fallback: TemplateConnectionInput) => {
    const value = existing.find((connection) => connection.id === id);
    return value
      ? {
          id: value.id,
          driver: value.driver,
          resource: value.resource,
          credentialReference: value.credentialReference ?? "",
        }
      : fallback;
  };
  return {
    kind: "batch-incremental",
    packageId: document.id.startsWith("new-package") ? "orders-daily" : document.id,
    label: document.layout?.label ?? "订单数据任务",
    sourceConnection: find("orders", defaultConnection("orders", "postgresql", "")),
    targetConnection: find("finance", defaultConnection("finance", "postgresql", "")),
    sourceTable: "orders",
    targetTable: "orders",
    changeSequence: "change_sequence",
    keyColumn: "id",
    cdcProvider: "postgresql",
    cdcSlot: "ud_orders_slot",
    cdcPublication: "ud_orders_publication",
    cdcServerId: 64029,
    appendOnly: false,
    paymentsConnection: find("payments", defaultConnection("payments", "kafka", "")),
    paymentsTopic: "payments",
    shopsConnection: find("shops", defaultConnection("shops", "mysql", "")),
    shopsTable: "shops",
    icebergCatalog: find("catalog", defaultConnection("catalog", "http", "https://catalog.example/v1", "host-catalog")),
    icebergStorage: find("warehouse", defaultConnection("warehouse", "s3", "https://s3.example/bucket/prefix", "host-aws-sigv4")),
    icebergNamespace: "metrics",
    icebergTable: "net_metric",
    schedule: defaultSchedule(),
  };
}

function connectionEditor(
  label: string,
  value: TemplateConnectionInput,
  change: (value: TemplateConnectionInput) => void,
) {
  return (
    <fieldset className="template-connection">
      <legend>{label}</legend>
      <label>
        连接名称
        <Input value={value.id} onChange={(event) => change({ ...value, id: event.target.value })} />
      </label>
      <label>
        Native 驱动
        <Input value={value.driver} onChange={(event) => change({ ...value, driver: event.target.value })} />
      </label>
      <label>
        资源
        <Input value={value.resource} onChange={(event) => change({ ...value, resource: event.target.value })} placeholder="宿主授权资源或绝对 URL" />
      </label>
      <label>
        宿主凭据引用
        <Input value={value.credentialReference ?? ""} onChange={(event) => change({ ...value, credentialReference: event.target.value })} placeholder="由宿主解析；不写入密钥" />
      </label>
    </fieldset>
  );
}

const iconFor = (kind: JobTemplateKind) =>
  kind === "batch-incremental" ? FileClock : kind === "cdc-mirror" ? GitBranch : LineChart;

export function JobTemplateDialog({ document, close, complete }: Props) {
  const [options, setOptions] = useState(() => initialOptions(document));
  const [scheduleEnabled, setScheduleEnabled] = useState(true);
  const [environmentId, setEnvironmentId] = useState("");
  const [environmentRevision, setEnvironmentRevision] = useState("1");
  const [parameterText, setParameterText] = useState("{}");
  const [error, setError] = useState("");
  const update = useCallback(
    <K extends keyof TemplateOptions>(key: K, value: TemplateOptions[K]) =>
      setOptions((current) => ({ ...current, [key]: value })),
    [],
  );
  const connectionFields = useMemo(() => {
    if (options.kind === "orders-payments-shops-iceberg")
      return (
        <>
          {connectionEditor("订单 CDC 连接", options.sourceConnection, (value) => update("sourceConnection", value))}
          {connectionEditor("指标 SQL 目标", options.targetConnection, (value) => update("targetConnection", value))}
          {connectionEditor("支付 Kafka 连接", options.paymentsConnection!, (value) => update("paymentsConnection", value))}
          {connectionEditor("门店 MySQL 连接", options.shopsConnection!, (value) => update("shopsConnection", value))}
          {connectionEditor("Iceberg REST Catalog", options.icebergCatalog, (value) => update("icebergCatalog", value))}
          {connectionEditor("Iceberg S3 存储", options.icebergStorage, (value) => update("icebergStorage", value))}
        </>
      );
    return (
      <>
        {connectionEditor("源连接", options.sourceConnection, (value) => update("sourceConnection", value))}
        {connectionEditor("目标连接", options.targetConnection, (value) => update("targetConnection", value))}
      </>
    );
  }, [options, update]);
  const create = () => {
    setError("");
    let parameters: Record<string, unknown> = {};
    try {
      const parsed = decode(parameterText);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
        throw new Error("环境参数必须是 JSON 对象");
      parameters = parsed as Record<string, unknown>;
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : String(failure));
      return;
    }
    const schedule = scheduleEnabled ? options.schedule : undefined;
    if (schedule?.frequency === "Interval" && (!schedule.intervalSeconds || schedule.intervalSeconds < 1)) {
      setError("间隔必须是正整数秒");
      return;
    }
    const connectionInputs = options.kind === "orders-payments-shops-iceberg"
      ? [options.sourceConnection, options.targetConnection, options.paymentsConnection!, options.shopsConnection!, options.icebergCatalog, options.icebergStorage]
      : [options.sourceConnection, options.targetConnection];
    if (connectionInputs.some((connection) => !connection.id.trim() || !connection.driver.trim())) {
      setError("每个 Native 连接都需要名称和驱动");
      return;
    }
    if (new Set(connectionInputs.map((connection) => connection.id.trim())).size !== connectionInputs.length) {
      setError("Native 连接名称必须唯一");
      return;
    }
    if (options.kind === "orders-payments-shops-iceberg" &&
      (options.icebergStorage.driver.trim().toLowerCase() !== "s3" ||
        !isPathStyleTlsUrl(options.icebergStorage.resource.trim()) ||
        !options.icebergStorage.credentialReference?.trim())) {
      setError("Iceberg 存储必须使用 Native s3、绝对 HTTPS path-style URL（endpoint/bucket/prefix）和宿主 AWS SigV4 credential reference");
      return;
    }
    const built = buildJobTemplate({
      ...options,
      schedule,
      environment: environmentId.trim()
        ? { id: environmentId.trim(), revision: environmentRevision.trim() || "1" }
        : undefined,
      parameters,
    });
    complete(built);
  };
  return (
    <div className="modal-backdrop">
      <section className="modal job-template-dialog" role="dialog" aria-modal="true" aria-label="新建作业模板">
        <header className="dialog-heading">
          <div>
            <small>现有编辑器 · Native v15 Package</small>
            <h2>从模板建立数据任务</h2>
          </div>
          <Button aria-label="关闭模板" onClick={close}><X size={18} /></Button>
        </header>
        <div className="template-kind-grid">
          {(Object.keys(templateNames) as JobTemplateKind[]).map((kind) => {
            const TemplateIcon = iconFor(kind);
            return (
              <Button key={kind} className={options.kind === kind ? "selected" : ""} onClick={() => update("kind", kind)}>
                <TemplateIcon size={17} />
                <span>{templateNames[kind]}</span>
              </Button>
            );
          })}
        </div>
        <div className="template-form-grid">
          <label>
            作业名称
            <Input value={options.label} onChange={(event) => update("label", event.target.value)} />
          </label>
          <label>
            Package ID
            <Input value={options.packageId} onChange={(event) => update("packageId", event.target.value)} />
          </label>
          <label>
            源表
            <Input value={options.sourceTable} onChange={(event) => update("sourceTable", event.target.value)} />
          </label>
          <label>
            目标表
            <Input value={options.targetTable} onChange={(event) => update("targetTable", event.target.value)} />
          </label>
        </div>
        {options.kind === "batch-incremental" ? (
          <fieldset className="property-group">
            <legend>复合增量位点</legend>
            <div className="template-form-grid">
              <label>变更序列列<Input value={options.changeSequence} onChange={(event) => update("changeSequence", event.target.value)} /></label>
              <label>业务主键列<Input value={options.keyColumn} onChange={(event) => update("keyColumn", event.target.value)} /></label>
            </div>
            <p className="property-help">Native 将以 composite / unique / monotonic 位点读取；目标使用 receipt-backed upsert。</p>
          </fieldset>
        ) : null}
        {options.kind === "cdc-mirror" ? (
          <fieldset className="property-group">
            <legend>CDC 源与变更目标</legend>
            <div className="template-form-grid">
              <label>数据库类型<Select value={options.cdcProvider} onChange={(event) => {
                const provider = event.target.value as TemplateOptions["cdcProvider"];
                setOptions((current) => ({
                  ...current,
                  cdcProvider: provider,
                  sourceConnection: ["postgresql", "mysql", "mariadb"].includes(current.sourceConnection.driver.trim().toLowerCase())
                    ? { ...current.sourceConnection, driver: provider }
                    : current.sourceConnection,
                }));
              }}><option value="postgresql">PostgreSQL</option><option value="mysql">MySQL</option><option value="mariadb">MariaDB</option></Select></label>
              <label>主键列<Input value={options.keyColumn} onChange={(event) => update("keyColumn", event.target.value)} /></label>
              {options.cdcProvider === "postgresql" ? <><label>Replication slot<Input value={options.cdcSlot} onChange={(event) => update("cdcSlot", event.target.value)} /></label><label>Publication<Input value={options.cdcPublication} onChange={(event) => update("cdcPublication", event.target.value)} /></label><label className="checkbox"><Input type="checkbox" checked={options.appendOnly} onChange={(event) => update("appendOnly", event.target.checked)} />只读 append-only outbox（Native 会拒绝 UPDATE / DELETE / TRUNCATE）</label></> : <label>Replication server ID<Input type="number" min={1} value={options.cdcServerId} onChange={(event) => update("cdcServerId", event.target.valueAsNumber || 1)} /></label>}
            </div>
            <p className="property-help">目标固定使用 changeset、beforeField=before、主键和回执，保留新增、更新、删除及键变化。</p>
          </fieldset>
        ) : null}
        {options.kind === "orders-payments-shops-iceberg" ? (
          <fieldset className="property-group">
            <legend>订单 · 支付 · 门店 · Iceberg</legend>
            <div className="template-form-grid">
              <label>支付主题<Input value={options.paymentsTopic} onChange={(event) => update("paymentsTopic", event.target.value)} /></label>
              <label>门店表<Input value={options.shopsTable} onChange={(event) => update("shopsTable", event.target.value)} /></label>
              <label>Iceberg Namespace<Input value={options.icebergNamespace} onChange={(event) => update("icebergNamespace", event.target.value)} /></label>
              <label>Iceberg 表<Input value={options.icebergTable} onChange={(event) => update("icebergTable", event.target.value)} /></label>
            </div>
            <p className="property-help">使用 MySQL 当前门店快照 / cache、有界 eventJoin、最终 eventWindow；SQL 与 Iceberg 两个目标按实际图键输出，示例期望净额 70。</p>
            <p className="property-help">S3 资源必须是绝对 TLS path-style URL；AWS SigV4 凭据由现有宿主 credential reference 解析。</p>
          </fieldset>
        ) : null}
        <div className="template-connections">{connectionFields}</div>
        <fieldset className="property-group">
          <legend>发布绑定</legend>
          <div className="template-form-grid">
            <label>环境 ID<Input value={environmentId} onChange={(event) => setEnvironmentId(event.target.value)} placeholder="Dev / Test / Prod 的不可变环境 ID" /></label>
            <label>环境修订<Input value={environmentRevision} onChange={(event) => setEnvironmentRevision(event.target.value)} /></label>
          </div>
          <label>环境参数（JSON 对象）<TextArea rows={3} value={parameterText} onChange={(event) => setParameterText(event.target.value)} /></label>
          <label className="checkbox"><Input type="checkbox" checked={scheduleEnabled} onChange={(event) => setScheduleEnabled(event.target.checked)} />发布时附带调度</label>
          {scheduleEnabled && options.schedule ? <div className="template-form-grid">
            <label>调度类型<Select value={options.schedule.frequency} onChange={(event) => update("schedule", { ...options.schedule!, frequency: event.target.value as TemplateSchedule["frequency"] })}><option value="Interval">固定间隔</option><option value="Daily">每天</option><option value="Weekly">每周</option><option value="Monthly">每月</option><option value="Once">一次</option></Select></label>
            {options.schedule.frequency === "Interval" ? <label>间隔秒数<Input type="number" min={1} value={options.schedule.intervalSeconds ?? 3600} onChange={(event) => update("schedule", { ...options.schedule!, intervalSeconds: event.target.valueAsNumber })} /></label> : null}
            <label>时区<Input value={options.schedule.timeZone ?? "UTC"} onChange={(event) => update("schedule", { ...options.schedule!, timeZone: event.target.value })} /></label>
          </div> : null}
        </fieldset>
        {error ? <p role="alert">{error}</p> : null}
        <footer>
          <span className="muted">
            {options.kind === "batch-incremental" ? <FileClock size={14} /> : options.kind === "cdc-mirror" ? <GitBranch size={14} /> : <LineChart size={14} />}
            创建后仍在当前编辑器中预检、预览、保存和发布
          </span>
          <Button onClick={close}>取消</Button>
          <Button className="primary" onClick={create}>应用模板</Button>
        </footer>
      </section>
    </div>
  );
}
