import { Button, Select } from "./shared/design-system/Controls";
import { useState } from "react";
import { Eye, RefreshCw } from "lucide-react";
import { encode } from "./api";
import { DataGrid, RowComparison, SchemaTable } from "./DataGrid";
import type { BoundNode, FlowComponent, PreviewResult } from "./model";
export function PreviewPanel({
  value,
  node,
  item,
  input,
  busy,
  stale,
  blockedReason,
  refresh,
  sourceRows,
  setSourceRows,
}: {
  value?: PreviewResult;
  node?: BoundNode;
  item?: FlowComponent;
  input?: { from: string; output: number };
  busy: boolean;
  stale: boolean;
  blockedReason?: string;
  refresh: () => void;
  sourceRows: number;
  setSourceRows: (value: number) => void;
}) {
  const [tab, setTab] = useState("output"),
    [port, setPort] = useState(0);
  const edges = value?.snapshot.preview ?? [];
  const incoming = edges.find(
    (edge) => edge.component === input?.from && edge.port === input.output,
  );
  const outputs =
    value && node
      ? node.ports
          .filter((port) => port.direction === 2)
          .map(
            (port) =>
              edges.find(
                (edge) =>
                  edge.component === item?.id && edge.port === port.index,
              ) ?? {
                pipeline: "",
                component: item?.id ?? "",
                port: port.index,
                portId: port.id,
                portName: port.name,
                totalRows: port.rows ?? 0,
                schema: node.outputs[port.index] ?? undefined,
                rows: [],
              },
          )
      : edges.filter((edge) => edge.component === item?.id);
  if (!outputs.length && incoming && item?.component.includes("destination"))
    outputs.push(incoming);
  const selected = outputs.find((edge) => edge.port === port) ?? outputs[0];
  const error = outputs.find(
    (edge) =>
      node?.ports.some(
        (port) =>
          port.direction === 2 && port.index === edge.port && port.flags & 2,
      ) || edge.schema?.fields.some((field) => field.name === "errorCode"),
  );
  const active =
    tab === "input" ? incoming : tab === "errors" ? error : selected;
  const schema =
    active?.schema ??
    (tab === "input" ? node?.inputs[0] : node?.outputs[selected?.port ?? port]);
  return (
    <section className="preview-workspace">
      <header className="panel-tabs">
        <div className="preview-heading">
          <Eye size={16} />
          <strong>
            {item ? "节点数据 · " + item.id.slice(0, 16) : "数据观察"}
          </strong>
        </div>
        {[
          ["output", "输出数据"],
          ["input", "输入数据"],
          ["compare", "前后对照"],
          ["errors", "异常行"],
          ["schema", "Schema"],
          ["plan", "执行计划"],
        ].map(([id, label]) => (
          <Button
            key={id}
            className={tab === id ? "active" : ""}
            onClick={() => setTab(id)}
          >
            {label}
          </Button>
        ))}
        <Button
          className="preview-refresh"
          disabled={busy || !item || !!blockedReason}
          title={blockedReason}
          onClick={refresh}
        >
          <RefreshCw size={14} />
          {busy ? "读取中…" : "只读预览"}
        </Button>
      </header>
      <div className="preview-status">
        <span className={stale ? "warning" : value?.result ? "error" : ""}>
          {blockedReason ?? (stale
            ? "草稿已更改，样本已过期"
            : value
              ? value.result
                ? `Native 预览失败 · 状态码 ${value.result}`
                : "Native 真实样本 · 目标未执行"
              : "预览会限制源读取并跳过所有目标写入")}
        </span>
        <label>
          每源读取上限
          <Select
            aria-label="预览读取上限"
            value={sourceRows}
            onChange={(event) => setSourceRows(Number(event.target.value))}
          >
            {[32, 256, 1000, 10000].map((count) => (
              <option key={count} value={count}>
                {count.toLocaleString()} 行
              </option>
            ))}
          </Select>
        </label>
        {outputs.length > 0 ? (
          <label>
            输出端口
            <Select
              aria-label="预览输出端口"
              value={selected?.port ?? 0}
              onChange={(event) => setPort(Number(event.target.value))}
            >
              {outputs.map((edge) => (
                <option key={edge.port} value={edge.port}>
                  {edge.portName ??
                    node?.ports.find(
                      (port) =>
                        port.direction === 2 && port.index === edge.port,
                    )?.name ??
                    `输出 ${edge.port + 1}`}{" "}
                  · {edge.totalRows.toLocaleString()} 行
                </option>
              ))}
            </Select>
          </label>
        ) : null}
      </div>
      {tab === "plan" ? (
        <details open>
          <summary>Native 计划与诊断</summary>
          <pre className="diagnostic-json">
            {value ? encode(value.diagnostics, 2) : "校验或预览后显示真实计划"}
          </pre>
        </details>
      ) : tab === "compare" ? (
        <RowComparison before={incoming} after={selected} />
      ) : tab === "schema" ? (
        <SchemaTable schema={schema ?? undefined} />
      ) : (
        <DataGrid schema={schema ?? undefined} rows={active?.rows} />
      )}
      {active?.truncated ? (
        <p role="status" className="warning">
          大字段或资源限制导致样本截断；没有更改实际数据。
        </p>
      ) : null}
    </section>
  );
}
