import { useState } from "react";
import { encode } from "./api";
import type { DataSchema, PreviewEdge, PreviewRow } from "./model";
export function displayValue(value: unknown): string {
  return value === null
    ? "NULL"
    : typeof value === "string"
      ? value
      : encode(value);
}
export function DataGrid({
  schema,
  rows = [],
  onRow,
  selected,
}: {
  schema?: DataSchema;
  rows?: PreviewRow[];
  onRow?: (row: PreviewRow) => void;
  selected?: PreviewRow;
}) {
  const [query, setQuery] = useState(""),
    [detail, setDetail] = useState<PreviewRow>();
  if (!schema)
    return (
      <div className="empty-state compact">
        <strong>尚无数据样本</strong>
        <span>选择节点，运行只读预览。</span>
      </div>
    );
  return (
    <div className="data-grid">
      <div className="data-grid-tools">
        <input
          aria-label="筛选样本"
          placeholder="查找样本中的值或来源行"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <span>
          {rows.length} 条已捕获样本 · {schema.fields.length} 个字段
        </span>
      </div>
      <div className="data-grid-scroll">
        <table>
          <thead>
            <tr>
              <th>来源行 / Row Identity</th>
              {schema.fields.map((field) => (
                <th key={field.identity ?? field.name} title={field.identity}>
                  <strong>{field.name}</strong>
                  <small>
                    {field.scalar?.type ?? field.type}
                    {field.precision
                      ? ` (${field.precision}, ${field.scale ?? 0})`
                      : ""}
                    {field.nullable ? " · 可空" : ""}
                  </small>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows
              .filter(
                (row) =>
                  !query ||
                  encode(row).toLowerCase().includes(query.toLowerCase()),
              )
              .map((row, index) => (
                <tr
                  key={encode(row.identity) + index}
                  tabIndex={0}
                  className={selected === row ? "selected" : ""}
                  onClick={() => {
                    setDetail(row);
                    onRow?.(row);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      setDetail(row);
                      onRow?.(row);
                    }
                  }}
                >
                  <td title={encode(row.identity)}>
                    <span className="row-ordinal">
                      {String(
                        row.identity.ordinal ??
                          row.identity.position ??
                          index + 1,
                      )}
                    </span>
                    <code>{row.identity.id ?? encode(row.identity)}</code>
                  </td>
                  {schema.fields.map((field, column) => {
                    const cell = row.fields[column];
                    return (
                      <td
                        key={field.identity ?? field.name}
                        title={
                          !cell?.present
                            ? "字段不存在（Absent）"
                            : displayValue(cell.value)
                        }
                        className={
                          !cell?.present || cell.value === null
                            ? "null-value"
                            : ""
                        }
                      >
                        {!cell?.present ? "ABSENT" : displayValue(cell.value)}
                      </td>
                    );
                  })}
                </tr>
              ))}
          </tbody>
        </table>
        {!rows.length ? (
          <p className="empty-state compact">
            此端口在本次样本范围内没有输出行。
          </p>
        ) : null}
      </div>
      {detail && rows.includes(detail) ? (
        <section className="row-detail">
          <header>
            <h3>来源与字段详情</h3>
            <button onClick={() => setDetail(undefined)}>关闭行详情</button>
          </header>
          <dl>
            <dt>Row Identity</dt>
            <dd>{encode(detail.identity)}</dd>
            {schema.fields.map((field, index) => (
              <div key={field.identity ?? field.name}>
                <dt>
                  {field.name}
                  <small>
                    {field.scalar?.type ?? field.type} · {field.identity}
                  </small>
                </dt>
                <dd>
                  {detail.fields[index]?.present
                    ? displayValue(detail.fields[index].value)
                    : "ABSENT"}
                </dd>
              </div>
            ))}
          </dl>
        </section>
      ) : null}
    </div>
  );
}
export function SchemaTable({ schema }: { schema?: DataSchema }) {
  return (
    <div className="data-grid-scroll">
      <table>
        <thead>
          <tr>
            <th>字段</th>
            <th>类型</th>
            <th>长度 / 精度 / Scale</th>
            <th>代码页</th>
            <th>字段身份</th>
          </tr>
        </thead>
        <tbody>
          {schema?.fields.map((field) => (
            <tr key={field.identity ?? field.name}>
              <td>
                {field.name}
                {field.nullable ? " · 可空" : ""}
              </td>
              <td>{field.scalar?.type ?? field.type}</td>
              <td>
                {field.scalar?.length || "—"} / {field.precision ?? "—"} /{" "}
                {field.scale ?? "—"}
              </td>
              <td>{field.scalar?.codePage || "Unicode"}</td>
              <td>
                <code title={field.identity}>{field.identity ?? "待绑定"}</code>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
export function RowComparison({
  before,
  after,
}: {
  before?: PreviewEdge;
  after?: PreviewEdge;
}) {
  const [selection, setSelection] = useState<string>();
  const rows = before?.rows ?? [],
    input = rows.find((row) => encode(row.identity) === selection) ?? rows[0];
  const output = after?.rows?.find(
    (row) => row.identity.id === input?.identity.id,
  );
  return (
    <section className="row-comparison">
      <header>
        <h3>原始输入 → 本组件输出</h3>
        <select
          aria-label="选择真实样本行"
          value={input ? encode(input.identity) : ""}
          onChange={(event) => setSelection(event.target.value)}
        >
          {rows.map((row, index) => (
            <option key={encode(row.identity)} value={encode(row.identity)}>
              样本 {index + 1} · {row.identity.id}
            </option>
          ))}
        </select>
      </header>
      <p className="muted">
        同一组件内的所有公式始终读取原始输入。预览结果仅对应本次读取范围。
      </p>
      <div className="data-grid-scroll">
        <table>
          <thead>
            <tr>
              <th>字段</th>
              <th>原值</th>
              <th>计算值</th>
            </tr>
          </thead>
          <tbody>
            {after?.schema?.fields.map((field, column) => {
              const source =
                before?.schema?.fields.findIndex(
                  (candidate) =>
                    candidate.identity === field.identity ||
                    candidate.name === field.name,
                ) ?? -1;
              const previous = input?.fields[source];
              const next = output?.fields[column];
              return (
                <tr
                  key={field.identity ?? field.name}
                  className={
                    encode(previous) !== encode(next) ? "value-changed" : ""
                  }
                >
                  <td>{field.name}</td>
                  <td>
                    {source < 0
                      ? "新增字段"
                      : previous?.present
                        ? displayValue(previous.value)
                        : "ABSENT"}
                  </td>
                  <td>
                    {!output
                      ? "本行未进入此端口；请查看异常行或其他分支"
                      : next?.present
                        ? displayValue(next.value)
                        : "ABSENT"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
