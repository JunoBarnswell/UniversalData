import { useState } from "react";
import { post } from "./api";
import { SchemaTable } from "./DataGrid";
import { DataSchemaEditor } from "./DataSchemaEditor";
import { ConnectionSelect } from "./SqlEditors";
import { PropertyForm } from "./PropertyForm";
import type {
  Configuration,
  DataSchema,
  FileInspection,
  PackageDocument,
  PropertySchema,
} from "./model";

export function FileSourceEditor({
  document,
  value,
  change,
  xlsx,
  schema: descriptor,
}: {
  document: PackageDocument;
  value: Configuration;
  change: (value: Configuration) => void;
  xlsx: boolean;
  schema: PropertySchema;
}) {
  const [busy, setBusy] = useState(false),
    [message, setMessage] = useState("");
  const format = (value.fileFormat ?? {}) as Configuration;
  const schema = value.schema as DataSchema | undefined;
  const options = xlsx
    ? {
        header: value.header ?? true,
        ...(value.sheet ? { sheet: value.sheet } : {}),
      }
    : format;
  function option(name: string, next: unknown) {
    change(
      xlsx
        ? { ...value, [name]: next }
        : { ...value, fileFormat: { ...format, [name]: next } },
    );
  }
  async function discover() {
    setBusy(true);
    try {
      const found = await post<FileInspection>("/files/inspect", {
        package: document,
        connection: value.connection,
        format: xlsx ? "xlsx" : "csv",
        options,
      });
      change({
        ...value,
        schema: found.schema,
        ...(xlsx
          ? found.metadata.options
          : { fileFormat: found.metadata.options }),
      });
      setMessage(
        found.metadata.delimiterAmbiguous
          ? "分隔符存在歧义，请明确选择并重新发现。"
          : "字段已由 Native 重新发现；预览将按当前配置读取。",
      );
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }
  const business = new Set([
    "connection",
    "schema",
    "fileFormat",
    "sheet",
    "header",
    "errorPolicy",
    "includeInput",
  ]);
  return (
    <section className="business-editor">
      <ConnectionSelect
        document={document}
        value={String(value.connection ?? "")}
        change={(connection) => change({ ...value, connection })}
      />
      {xlsx ? (
        <label>
          工作表
          <input
            value={String(value.sheet ?? "")}
            placeholder="第一张工作表"
            onChange={(event) => option("sheet", event.target.value)}
          />
        </label>
      ) : (
        <>
          <label>
            文件编码
            <select
              aria-label="源文件编码"
              value={String(format.encoding ?? "UTF-8")}
              onChange={(event) => option("encoding", event.target.value)}
            >
              {["UTF-8", "UTF-16LE", "UTF-16BE", "GB18030", "windows-1252"].map(
                (encoding) => (
                  <option key={encoding}>{encoding}</option>
                ),
              )}
            </select>
          </label>
          <label>
            分隔符
            <select
              aria-label="源文件分隔符"
              value={String(format.delimiter ?? ",")}
              onChange={(event) => option("delimiter", event.target.value)}
            >
              {[
                [",", "逗号"],
                ["\t", "制表符"],
                [";", "分号"],
                ["|", "竖线"],
              ].map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </label>
        </>
      )}
      <label className="checkbox">
        <input
          type="checkbox"
          checked={options.header !== false}
          onChange={(event) => option("header", event.target.checked)}
        />
        首行是字段名称
      </label>
      <button
        disabled={busy || !value.connection}
        onClick={() => void discover()}
      >
        {busy ? "读取中…" : "重新发现字段"}
      </button>
      {message ? <p role="status">{message}</p> : null}
      <SchemaTable schema={schema} />
      <details>
        <summary>调整字段类型与名称</summary>
        <DataSchemaEditor
          value={schema}
          change={(schema) => change({ ...value, schema })}
        />
      </details>
      <label>
        解析错误
        <select
          aria-label="源解析错误处置"
          value={String(value.errorPolicy ?? "fail")}
          onChange={(event) =>
            change({ ...value, errorPolicy: event.target.value })
          }
        >
          <option value="fail">组件失败</option>
          <option value="redirect">隔离异常行至错误端口</option>
          <option value="ignore">忽略可恢复的记录错误</option>
        </select>
      </label>
      <label className="checkbox">
        <input
          type="checkbox"
          checked={value.includeInput === true}
          onChange={(event) =>
            change({ ...value, includeInput: event.target.checked })
          }
        />
        在异常行中保留原始输入
      </label>
      <details>
        <summary>专业读取选项</summary>
        <PropertyForm
          schema={{
            ...descriptor,
            required: descriptor.required?.filter((key) => !business.has(key)),
            properties: Object.fromEntries(
              Object.entries(descriptor.properties ?? {}).filter(
                ([key]) => !business.has(key),
              ),
            ),
          }}
          value={Object.fromEntries(
            Object.entries(value).filter(([key]) => !business.has(key)),
          )}
          change={(next) =>
            change({
              ...Object.fromEntries(
                Object.entries(value).filter(([key]) => business.has(key)),
              ),
              ...next,
            })
          }
        />
      </details>
    </section>
  );
}
