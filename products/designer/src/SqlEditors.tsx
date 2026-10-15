import { useState } from "react";
import { post, encode } from "./api";
import { SchemaTable } from "./DataGrid";
import { PropertyForm } from "./PropertyForm";
import { DataSchemaEditor } from "./DataSchemaEditor";
import type {
  Configuration,
  DataSchema,
  PackageDocument,
  PropertySchema,
} from "./model";
type SchemaComparison = {
  compatible: boolean;
  result: string;
  changes: {
    kind: string;
    columnId: string;
    expectedIndex: number;
    actualIndex: number;
  }[];
};
export function ConnectionSelect({
  document,
  value,
  change,
}: {
  document: PackageDocument;
  value: string;
  change: (value: string) => void;
}) {
  return (
    <label>
      连接
      <select
        aria-label="组件连接"
        value={value}
        onChange={(event) => change(event.target.value)}
      >
        <option value="">选择连接</option>
        {document.connections?.map((connection) => (
          <option key={connection.id} value={connection.id}>
            {connection.id} · {connection.driver}
          </option>
        ))}
      </select>
    </label>
  );
}
export function SqlEditor({
  document,
  value,
  change,
  input,
  source = false,
  configurationSchema,
}: {
  document: PackageDocument;
  value: Configuration;
  change: (value: Configuration) => void;
  input?: DataSchema;
  source?: boolean;
  configurationSchema?: PropertySchema;
}) {
  const [catalogs, setCatalogs] = useState<{ name: string }[]>([]),
    [database, setDatabase] = useState(""),
    [tables, setTables] = useState<{ name: string; parent: string }[]>([]);
  const [schema, setSchema] = useState<DataSchema>(),
    [message, setMessage] = useState(""),
    [busy, setBusy] = useState(false),
    [comparison, setComparison] = useState<SchemaComparison>();
  const mapping = Array.isArray(value.mapping)
    ? (value.mapping as { source: string; target: string }[])
    : [];
  const fields = input?.fields ?? [];
  function updateMapping(rows: { source: string; target: string }[]) {
    const previous = mapping.length
      ? mapping
      : fields.map((field) => ({ source: field.name, target: field.name }));
    const keys = Array.isArray(value.keys) ? (value.keys as number[]) : [];
    const targets = keys.map((index) => previous[index]?.target);
    change({
      ...value,
      mapping: rows,
      ...(Array.isArray(value.keys)
        ? {
            keys: rows.flatMap((row, index) =>
              targets.includes(row.target) ? [index] : [],
            ),
          }
        : {}),
    });
    setComparison(undefined);
  }
  async function metadata(kind: number, parent = "") {
    setBusy(true);
    try {
      const rows = await post<{ name: string; parent: string }[]>("/metadata", {
        package: document,
        connection: value.connection,
        kind,
        parent,
      });
      if (kind === 1) setCatalogs(rows);
      else setTables(rows);
      setMessage(`连接正常，读取 ${rows.length} 项。`);
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }
  async function discover() {
    setBusy(true);
    try {
      const result = await post<DataSchema>("/schema", {
        package: {
          ...document,
          controlFlow: { tasks: [], constraints: [] },
          dataFlows: [],
        },
        connection: value.connection,
        table: value.table,
      });
      setSchema(result);
      setComparison(undefined);
      if (source) change({ ...value, schema: result });
      setMessage("已读取目标表结构。");
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }
  function setMapping(target: string, selected: string) {
    updateMapping([
      ...mapping.filter((row) => row.target !== target),
      ...(selected ? [{ source: selected, target }] : []),
    ]);
  }
  async function check() {
    if (!schema || !input) return;
    setBusy(true);
    try {
      const targets = schema.fields.filter((field) =>
          mapping.some((row) => row.target === field.name),
        ),
        actual = input.fields.filter((field) =>
          mapping.some((row) => row.source === field.name),
        );
      const result = await post<SchemaComparison>("/schema/compare", {
        package: document,
        expected: { ...schema, fields: targets },
        actual: { ...input, fields: actual },
        policy: {
          policy: "evolutionMapping",
          mapping: mapping.map((row) => ({
            expected: row.target,
            actual: row.source,
          })),
        },
      });
      setComparison(result);
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <section className="sql-editor">
      <ConnectionSelect
        document={document}
        value={String(value.connection ?? "")}
        change={(connection) => {
          change({ ...value, connection });
          setSchema(undefined);
          setTables([]);
        }}
      />
      <button
        disabled={busy || !value.connection}
        onClick={() => void metadata(1)}
      >
        测试连接 / 发现数据库
      </button>
      <label>
        数据库
        <select
          value={database}
          onChange={(event) => {
            setDatabase(event.target.value);
            void metadata(3, event.target.value);
          }}
        >
          <option value="">选择数据库</option>
          {catalogs.map((catalog) => (
            <option key={catalog.name} value={catalog.name}>
              {catalog.name}
            </option>
          ))}
        </select>
      </label>
      <label>
        {source ? "源表" : "目标表"}
        <input
          aria-label="目标表名称"
          list="database-tables"
          value={String(value.table ?? "")}
          onChange={(event) => {
            change({ ...value, table: event.target.value });
            setSchema(undefined);
          }}
          placeholder="选择表或输入 schema.table"
        />
        <datalist id="database-tables">
          {tables.map((table) => (
            <option
              key={table.parent + "/" + table.name}
              value={
                table.parent ? `${table.parent}.${table.name}` : table.name
              }
            />
          ))}
        </datalist>
      </label>
      <button
        disabled={busy || !value.connection || !value.table}
        onClick={() => void discover()}
      >
        发现{source ? "输出" : "目标"}字段
      </button>
      {!source ? (
        <>
          <label>
            写入模式
            <select
              value={String(value.mode ?? "append")}
              onChange={(event) =>
                change({ ...value, mode: event.target.value })
              }
            >
              <option value="append">追加</option>
              <option value="upsert">按业务键更新 / 插入</option>
              <option value="delete">按业务键删除</option>
            </select>
          </label>
          <label>
            批量传输
            <select
              value={String(value.loadMode ?? "auto")}
              onChange={(event) =>
                change({ ...value, loadMode: event.target.value })
              }
            >
              <option value="auto">Native 自动选择</option>
              <option value="native">原生批量装载</option>
              <option value="parameters">参数化批量</option>
            </select>
          </label>
          {value.mode === "upsert" || value.mode === "delete" ? (
            <fieldset>
              <legend>业务键</legend>
              {(mapping.length
                ? mapping.map((row) => ({ name: row.target }))
                : fields
              ).map((field, index) => (
                <label key={field.name} className="checkbox">
                  <input
                    type="checkbox"
                    checked={
                      Array.isArray(value.keys) && value.keys.includes(index)
                    }
                    onChange={(event) =>
                      change({
                        ...value,
                        keys: event.target.checked
                          ? [
                              ...(Array.isArray(value.keys) ? value.keys : []),
                              index,
                            ]
                          : (value.keys as number[]).filter(
                              (key) => key !== index,
                            ),
                      })
                    }
                  />
                  {field.name}
                </label>
              ))}
            </fieldset>
          ) : null}
          <label className="checkbox">
            <input
              type="checkbox"
              checked={value.provisionReceipts === true}
              onChange={(event) =>
                change({ ...value, provisionReceipts: event.target.checked })
              }
            />
            允许建立目标执行回执以支持恢复
          </label>
          <label>
            记录错误
            <select
              value={String(value.errorPolicy ?? "fail")}
              onChange={(event) =>
                change({ ...value, errorPolicy: event.target.value })
              }
            >
              <option value="fail">失败并回滚</option>
              <option value="redirect">隔离错误行</option>
              <option value="ignore">忽略可恢复错误</option>
            </select>
          </label>
        </>
      ) : null}
      {message ? <p role="status">{message}</p> : null}
      {source ? (
        <details>
          <summary>输出字段与 SQL 查询</summary>
          <DataSchemaEditor
            value={value.schema}
            change={(schema) => change({ ...value, schema })}
          />
          <label>
            SQL 查询（留空使用表扫描）
            <textarea
              value={String(value.sql ?? "")}
              onChange={(event) => {
                const next = { ...value };
                if (event.target.value) next.sql = event.target.value;
                else delete next.sql;
                change(next);
              }}
              spellCheck={false}
            />
          </label>
          <p className="muted">
            表扫描支持只读预览。自定义 SQL 需要使用隔离测试资源验证。
          </p>
        </details>
      ) : null}
      {configurationSchema ? (
        <details>
          <summary>专业连接与提交选项</summary>
          <PropertyForm
            schema={{
              ...configurationSchema,
              required: [],
              properties: Object.fromEntries(
                Object.entries(configurationSchema.properties ?? {}).filter(
                  ([key]) =>
                    ![
                      "connection",
                      "table",
                      "schema",
                      "sql",
                      "mode",
                      "loadMode",
                      "keys",
                      "mapping",
                      "errorPolicy",
                      "provisionReceipts",
                    ].includes(key),
                ),
              ),
            }}
            value={Object.fromEntries(
              Object.entries(value).filter(
                ([key]) =>
                  ![
                    "connection",
                    "table",
                    "schema",
                    "sql",
                    "mode",
                    "loadMode",
                    "keys",
                    "mapping",
                    "errorPolicy",
                    "provisionReceipts",
                  ].includes(key),
              ),
            )}
            change={(next) =>
              change({
                ...Object.fromEntries(
                  Object.entries(value).filter(([key]) =>
                    [
                      "connection",
                      "table",
                      "schema",
                      "sql",
                      "mode",
                      "loadMode",
                      "keys",
                      "mapping",
                      "errorPolicy",
                      "provisionReceipts",
                    ].includes(key),
                  ),
                ),
                ...next,
              })
            }
          />
        </details>
      ) : null}
      {schema && !source ? (
        <section className="target-mapping">
          <header>
            <h3>输入 → 目标字段</h3>
            <button
              onClick={() =>
                updateMapping(
                  schema.fields
                    .filter((target) =>
                      fields.some((field) => field.name === target.name),
                    )
                    .map((field) => ({
                      source: field.name,
                      target: field.name,
                    })),
                )
              }
            >
              同名匹配
            </button>
          </header>
          <table>
            <thead>
              <tr>
                <th>输入字段</th>
                <th>目标字段 / 类型</th>
              </tr>
            </thead>
            <tbody>
              {schema.fields.map((target) => (
                <tr key={target.identity ?? target.name}>
                  <td>
                    <select
                      aria-label={`映射 ${target.name}`}
                      value={
                        mapping.find((row) => row.target === target.name)
                          ?.source ?? ""
                      }
                      onChange={(event) =>
                        setMapping(target.name, event.target.value)
                      }
                    >
                      <option value="">未映射</option>
                      {fields.map((field) => (
                        <option
                          key={field.identity ?? field.name}
                          value={field.name}
                        >
                          {field.name} · {field.scalar?.type ?? field.type}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td>
                    {target.name}
                    <small>
                      {target.scalar?.type ?? target.type}{" "}
                      {target.nullable ? "可空" : "非空"}
                    </small>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <p>
            {
              schema.fields.filter(
                (field) => !mapping.some((row) => row.target === field.name),
              ).length
            }{" "}
            个目标字段未映射 ·{" "}
            {
              fields.filter(
                (field) => !mapping.some((row) => row.source === field.name),
              ).length
            }{" "}
            个输入字段未使用
          </p>
          <button
            disabled={busy || !mapping.length}
            onClick={() => void check()}
          >
            Native 检查类型与精度
          </button>
          {comparison ? (
            <div className="schema-comparison">
              <p role="status" className={comparison.compatible ? "" : "error"}>
                {comparison.compatible
                  ? "Native 确认映射兼容"
                  : "Native 判定映射需要调整"}{" "}
                · {comparison.result}
              </p>
              {comparison.changes.length ? (
                <table>
                  <thead>
                    <tr>
                      <th>字段</th>
                      <th>Native 检查结果</th>
                    </tr>
                  </thead>
                  <tbody>
                    {comparison.changes.map((change, index) => (
                      <tr key={index}>
                        <td>
                          {schema.fields.find(
                            (field) => field.identity === change.columnId,
                          )?.name ?? change.columnId}
                        </td>
                        <td>
                          {(
                            {
                              TypeChanged: "类型变化",
                              NullabilityChanged: "可空性变化",
                              PrecisionChanged: "精度变化",
                              Removed: "缺少字段",
                              Added: "新增字段",
                              Renamed: "字段名称映射",
                              Reordered: "字段顺序映射",
                            } as Record<string, string>
                          )[change.kind] ?? change.kind}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : null}
              <details>
                <summary>专业诊断</summary>
                <pre>{encode(comparison, 2)}</pre>
              </details>
            </div>
          ) : null}
        </section>
      ) : schema ? (
        <SchemaTable schema={schema} />
      ) : null}
    </section>
  );
}
