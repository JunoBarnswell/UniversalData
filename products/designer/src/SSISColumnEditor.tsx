import { useCallback, useRef, useState } from "react";
import {
  FormulaSurface,
  utf8Position,
  type FormulaHandle,
} from "./FormulaSurface";
import { RowComparison } from "./DataGrid";
import type { PreviewEdge } from "./model";
import { post } from "./api";
import type {
  Component,
  Configuration,
  DataField,
  DataSchema,
  PackageDocument,
} from "./model";
export interface ExpressionLanguage {
  operators: { id: string; symbol: string; arguments: number }[];
  types: string[];
  typedNull: string;
}
interface Compilation {
  result: number;
  start: number;
  length: number;
  message: string;
  schema?: DataSchema;
}
const dispositions = [
  { id: "fail", label: "组件失败" },
  { id: "redirect", label: "重定向行" },
  { id: "ignore", label: "忽略失败" },
];
export function FormulaEditor({
  value,
  change,
  fields,
  functions,
  document,
  schema,
}: {
  value: string;
  change: (source: string) => void;
  fields: DataField[];
  functions: Component[];
  document: PackageDocument;
  schema?: DataSchema;
}) {
  const input = useRef<FormulaHandle>(null);
  const [check, setCheck] = useState<{
      source: string;
      compilation?: Compilation;
      error?: string;
    }>(),
    [busy, setBusy] = useState(false);
  const compileSource = useCallback(
    async (source: string) => {
      if (!schema) return { result: 0, start: 0, length: 0, message: "" };
      const compilation = await post<Compilation>("/expression", {
        package: document,
        schema,
        source,
      });
      setCheck({ source, compilation });
      return compilation;
    },
    [schema, document],
  );
  async function compile() {
    const source = value;
    setBusy(true);
    try {
      await compileSource(source);
    } catch (error) {
      setCheck({
        source,
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      setBusy(false);
    }
  }
  function insert(text: string) {
    input.current?.insert(text);
  }
  const current = check?.source === value ? check : undefined,
    metadata = current?.compilation?.schema?.fields[0];
  return (
    <div className="ssis-formula">
      <label>SSIS 公式</label>
      <FormulaSurface
        value={value}
        change={change}
        fields={fields}
        functions={functions}
        variables={Object.keys(document.variables ?? {})}
        compile={schema ? compileSource : undefined}
        editorRef={input}
      />
      <div className="ssis-formula-tools">
        <label>
          插入输入列
          <select
            value=""
            onChange={(event) => {
              if (event.target.value)
                insert(`[${event.target.value.replaceAll("]", "]]")}]`);
            }}
          >
            <option value="">选择字段</option>
            {fields.map((field) => (
              <option key={field.identity ?? field.name} value={field.name}>
                {field.name} · {field.scalar?.type ?? field.type}
              </option>
            ))}
          </select>
        </label>
        <label>
          插入注册函数
          <select
            value=""
            onChange={(event) => {
              if (event.target.value)
                insert(`${event.target.value.toUpperCase()}()`);
            }}
          >
            <option value="">选择函数</option>
            {functions.map((fn) => (
              <option key={fn.id} value={fn.id}>
                {fn.name} · {fn.minimumArguments ?? 0}–
                {fn.maximumArguments ?? 0} 参数
              </option>
            ))}
          </select>
        </label>
        <label>
          插入变量
          <select
            value=""
            onChange={(event) => {
              if (event.target.value) insert(`@[${event.target.value}]`);
            }}
          >
            <option value="">选择变量</option>
            {Object.keys(document.variables ?? {}).map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          disabled={busy || !schema || !value}
          onClick={() => void compile()}
        >
          {busy ? "编译中…" : "Native 编译"}
        </button>
      </div>
      <small>
        {functions
          .filter((fn) =>
            value.toLowerCase().includes(fn.id.toLowerCase() + "("),
          )
          .map(
            (fn) =>
              `${fn.id}(${fn.minimumArguments ?? 0}–${fn.maximumArguments ?? 0} 参数)`,
          )
          .join(" · ")}
      </small>
      {!schema ? <small>先绑定输入字段以查看公式类型。</small> : null}
      {current?.error ? (
        <p role="alert">{current.error}</p>
      ) : current?.compilation?.result ? (
        <p role="alert">
          {current.compilation.message}{" "}
          <button
            type="button"
            onClick={() => {
              const diagnostic = current.compilation!;
              input.current?.select(
                utf8Position(value, diagnostic.start),
                utf8Position(value, diagnostic.start + diagnostic.length),
              );
            }}
          >
            定位 {current.compilation.start}:{current.compilation.length}
          </button>
        </p>
      ) : metadata ? (
        <output className="ssis-inferred">
          {metadata.scalar?.type ?? metadata.type} · 长度{" "}
          {metadata.type === "utf8" || metadata.type === "binary"
            ? metadata.scalar?.length || "未限定"
            : "—"}{" "}
          · 精度 {metadata.precision ?? "—"} · Scale {metadata.scale ?? "—"} ·
          代码页{" "}
          {metadata.type === "utf8"
            ? metadata.scalar?.codePage || "Unicode"
            : "—"}{" "}
          · {metadata.nullable ? "可空" : "非空"}
        </output>
      ) : null}
    </div>
  );
}
export function SSISColumnEditor({
  value,
  change,
  schema,
  functions,
  document,
  before,
  after,
  preview,
  busy,
}: {
  value: Configuration;
  change: (value: Configuration) => void;
  schema?: DataSchema;
  functions: Component[];
  document: PackageDocument;
  before?: PreviewEdge;
  after?: PreviewEdge;
  preview?: () => void;
  busy?: boolean;
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  const rows: Configuration[] = Array.isArray(value.columns)
    ? (value.columns as Configuration[])
    : [];
  const fields = schema?.fields ?? [];
  function replacement(item: Configuration): DataField | undefined {
    const reference = String(
      item.inputIdentity ?? item.input ?? item.name ?? "",
    );
    return fields.find(
      (field) => field.identity === reference || field.name === reference,
    );
  }
  function row(index: number, update: Configuration) {
    change({
      ...value,
      columns: rows.map((item, at) =>
        at === index ? { ...item, ...update } : item,
      ),
    });
  }
  return (
    <section className="ssis-column-editor">
      <p>{rows.length} 项列写入 · 全部公式读取原始输入</p>
      <button type="button" onClick={() => dialog.current?.showModal()}>
        编辑派生列
      </button>
      <dialog ref={dialog} className="ssis-column-dialog">
        <header>
          <h2>Derived Column</h2>
          <button type="button" onClick={() => dialog.current?.close()}>
            完成
          </button>
        </header>
        <div className="ssis-column-table">
          <table>
            <thead>
              <tr>
                <th>输出列</th>
                <th>操作与输入身份</th>
                <th>表达式与 Native 类型</th>
                <th>Error</th>
                <th>Truncation</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((item, index) => (
                <tr key={index}>
                  <td>
                    <input
                      aria-label={`输出列 ${index + 1}`}
                      value={String(item.name ?? "")}
                      readOnly={item.action === "replace"}
                      onChange={(event) =>
                        row(index, { name: event.target.value })
                      }
                    />
                  </td>
                  <td>
                    <select
                      aria-label={`列操作 ${index + 1}`}
                      value={String(item.action ?? "add")}
                      onChange={(event) =>
                        row(index, {
                          action: event.target.value,
                          inputIdentity: undefined,
                          input: undefined,
                        })
                      }
                    >
                      <option value="add">新增列</option>
                      <option value="replace">替换原列</option>
                    </select>
                    {item.action === "replace" ? (
                      <>
                        <select
                          aria-label={`替换字段 ${index + 1}`}
                          value={
                            replacement(item)?.identity ??
                            replacement(item)?.name ??
                            ""
                          }
                          onChange={(event) => {
                            const field = fields.find(
                              (field) =>
                                (field.identity ?? field.name) ===
                                event.target.value,
                            );
                            if (field)
                              row(index, {
                                name: field.name,
                                inputIdentity: field.identity,
                                input: field.identity ? undefined : field.name,
                              });
                          }}
                        >
                          <option value="">选择原始输入列</option>
                          {fields.map((field) => (
                            <option
                              key={field.identity ?? field.name}
                              value={field.identity ?? field.name}
                            >
                              {field.name} · {field.scalar?.type ?? field.type}
                            </option>
                          ))}
                        </select>
                        <small>
                          {replacement(item)?.identity ?? "先绑定输入字段"}
                        </small>
                      </>
                    ) : null}
                  </td>
                  <td>
                    {typeof item.expression === "object" &&
                    item.expression !== null ? (
                      <p>
                        已保存 JSON AST。
                        <button
                          type="button"
                          onClick={() => row(index, { expression: "" })}
                        >
                          改用公式文本
                        </button>
                      </p>
                    ) : (
                      <FormulaEditor
                        value={String(item.expression ?? "")}
                        change={(expression) =>
                          row(index, {
                            expression,
                            friendlyExpression: expression,
                          })
                        }
                        fields={fields}
                        functions={functions}
                        document={document}
                        schema={schema}
                      />
                    )}
                  </td>
                  <td>
                    <select
                      aria-label={`Error 处置 ${index + 1}`}
                      value={String(
                        item.errorDisposition ?? value.errorPolicy ?? "fail",
                      )}
                      onChange={(event) =>
                        row(index, { errorDisposition: event.target.value })
                      }
                    >
                      {dispositions.map((item) => (
                        <option key={item.id} value={item.id}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <select
                      aria-label={`Truncation 处置 ${index + 1}`}
                      value={String(
                        item.truncationDisposition ??
                          value.errorPolicy ??
                          "fail",
                      )}
                      onChange={(event) =>
                        row(index, {
                          truncationDisposition: event.target.value,
                        })
                      }
                    >
                      {dispositions.map((item) => (
                        <option key={item.id} value={item.id}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <button
                      type="button"
                      onClick={() =>
                        change({
                          ...value,
                          columns: rows.filter((_, at) => at !== index),
                        })
                      }
                    >
                      移除
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <footer>
          <button type="button" disabled={busy} onClick={preview}>
            计算真实样本
          </button>
          <button
            type="button"
            onClick={() =>
              change({
                ...value,
                columns: [
                  ...rows,
                  {
                    name: "",
                    action: "add",
                    expression: "",
                    errorDisposition: "fail",
                    truncationDisposition: "fail",
                  },
                ],
              })
            }
          >
            新增列写入
          </button>
          <label>
            <input
              type="checkbox"
              checked={value.includeInput === true}
              onChange={(event) =>
                change({ ...value, includeInput: event.target.checked })
              }
            />
            错误输出保留原始记录
          </label>
        </footer>
        <RowComparison before={before} after={after} />
      </dialog>
    </section>
  );
}
export function SSISSplitEditor({
  value,
  change,
  schema,
  functions,
  document,
  samples,
  destinations,
  preview,
}: {
  value: Configuration;
  change: (value: Configuration, outputRemap?: number[]) => void;
  schema?: DataSchema;
  functions: Component[];
  document: PackageDocument;
  samples?: PreviewEdge[];
  destinations?: Record<number, string>;
  preview?: () => void;
}) {
  const rows: Configuration[] = Array.isArray(value.branches)
    ? (value.branches as Configuration[])
    : [];
  const [editing, setEditing] = useState<number>(),
    [drag, setDrag] = useState<number>();
  const dialog = useRef<HTMLDialogElement>(null);
  function update(index: number, item: Configuration) {
    change({
      ...value,
      branches: rows.map((row, at) =>
        at === index ? { ...row, ...item } : row,
      ),
    });
  }
  function move(from: number, to: number) {
    if (from === to || to < 0 || to >= rows.length) return;
    const next = [...rows],
      order = rows.map((_, index) => index);
    next.splice(to, 0, next.splice(from, 1)[0]);
    order.splice(to, 0, order.splice(from, 1)[0]);
    const remap = Array.from({ length: rows.length + 2 }, (_, index) =>
      index < rows.length ? order.indexOf(index) : index,
    );
    change({ ...value, branches: next }, remap);
    setEditing(undefined);
  }
  function remove(index: number) {
    change(
      { ...value, branches: rows.filter((_, at) => at !== index) },
      Array.from({ length: rows.length + 2 }, (_, at) =>
        at === index ? -1 : at > index ? at - 1 : at,
      ),
    );
  }
  const editor = (
    <section className="ssis-split-editor">
      <header>
        <h3>按序匹配首个 TRUE</h3>
        <button onClick={preview}>计算样本命中</button>
      </header>
      <p className="muted">
        每行只进入一个正常分支。调整优先级后重新预览，以核对样本归属。
      </p>
      <div className="split-rules-scroll">
        <table>
          <thead>
            <tr>
              <th>顺序</th>
              <th>分支与规则</th>
              <th>样本命中</th>
              <th>去向</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr
                key={String(row.id) || index}
                draggable
                onDragStart={() => setDrag(index)}
                onDragOver={(event) => event.preventDefault()}
                onDrop={() => {
                  if (drag !== undefined) move(drag, index);
                  setDrag(undefined);
                }}
              >
                <td>{index + 1}</td>
                <td>
                  <input
                    aria-label={`分支名称 ${index + 1}`}
                    value={String(row.name ?? row.id ?? "")}
                    onChange={(event) =>
                      update(index, { name: event.target.value })
                    }
                  />
                  <button
                    className="rule-expression"
                    onClick={() => setEditing(index)}
                  >
                    {typeof row.condition === "string"
                      ? row.condition || "编辑条件"
                      : "已保存 Native AST"}
                  </button>
                </td>
                <td>
                  {samples
                    ? (samples.find((edge) => {
                        const portId = String(row.id ?? "");
                        return (
                          edge.portName === String(row.name ?? row.id) ||
                          edge.portName === portId
                        );
                      })?.totalRows ?? 0)
                    : "待预览"}
                </td>
                <td>{destinations?.[index] ?? "尚未连接"}</td>
                <td>
                  <button
                    aria-label={`上移分支 ${index + 1}`}
                    disabled={!index}
                    onClick={() => move(index, index - 1)}
                  >
                    ↑
                  </button>
                  <button
                    aria-label={`下移分支 ${index + 1}`}
                    disabled={index === rows.length - 1}
                    onClick={() => move(index, index + 1)}
                  >
                    ↓
                  </button>
                  <button onClick={() => remove(index)}>移除</button>
                </td>
              </tr>
            ))}
            <tr>
              <td>默认</td>
              <td>
                <input
                  value={String(value.default ?? "default")}
                  onChange={(event) =>
                    change({ ...value, default: event.target.value })
                  }
                />
                <small>以上均未命中</small>
              </td>
              <td>
                {samples
                  ? (samples.find((edge) => edge.port === rows.length)
                      ?.totalRows ?? 0)
                  : "待预览"}
              </td>
              <td>{destinations?.[rows.length] ?? "尚未连接"}</td>
              <td />
            </tr>
            <tr className="error-row">
              <td>错误</td>
              <td>条件求值失败</td>
              <td>
                {samples
                  ? (samples.find((edge) => edge.port === rows.length + 1)
                      ?.totalRows ?? 0)
                  : "待预览"}
              </td>
              <td>{destinations?.[rows.length + 1] ?? "尚未连接"}</td>
              <td />
            </tr>
          </tbody>
        </table>
      </div>
      <button
        onClick={() =>
          change(
            {
              ...value,
              branches: [
                ...rows,
                {
                  id: crypto.randomUUID(),
                  name: `分支 ${rows.length + 1}`,
                  condition: "",
                },
              ],
            },
            Array.from({ length: rows.length + 2 }, (_, at) =>
              at >= rows.length ? at + 1 : at,
            ),
          )
        }
      >
        新增分支
      </button>
      <label>
        条件错误
        <select
          value={String(value.errorPolicy ?? "fail")}
          onChange={(event) =>
            change({ ...value, errorPolicy: event.target.value })
          }
        >
          {dispositions.map((item) => (
            <option key={item.id} value={item.id}>
              {item.label}
            </option>
          ))}
        </select>
      </label>
      {editing !== undefined && rows[editing] ? (
        <section className="split-formula">
          <h3>{String(rows[editing].name ?? rows[editing].id)}</h3>
          <FormulaEditor
            value={String(rows[editing].condition ?? "")}
            change={(condition) => update(editing, { condition })}
            fields={schema?.fields ?? []}
            functions={functions}
            document={document}
            schema={schema}
          />
          <button onClick={() => setEditing(undefined)}>完成条件</button>
        </section>
      ) : null}
    </section>
  );
  return (
    <section>
      <p>{rows.length} 个正常分支 · 按序匹配首个 TRUE</p>
      <button onClick={() => dialog.current?.showModal()}>编辑条件分流</button>
      <dialog ref={dialog} className="ssis-split-dialog">
        <header>
          <h2>Conditional Split</h2>
          <button onClick={() => dialog.current?.close()}>完成分流</button>
        </header>
        {editor}
      </dialog>
    </section>
  );
}
