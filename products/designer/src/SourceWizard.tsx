import { useState } from "react";
import { FileSpreadsheet, FileText, Upload, X } from "lucide-react";
import { api, post } from "./api";
import { DataGrid } from "./DataGrid";
import { DataSchemaEditor } from "./DataSchemaEditor";
import type {
  Configuration,
  Connection,
  DataSchema,
  FileInspection,
  FlowComponent,
  PackageDocument,
  PreviewResult,
} from "./model";
export function SourceWizard({
  document,
  flow,
  close,
  complete,
}: {
  document: PackageDocument;
  flow: string;
  close: () => void;
  complete: (
    connection: Connection,
    component: FlowComponent,
    label?: string,
    sample?: PreviewResult,
  ) => void;
}) {
  const [format, setFormat] = useState("csv"),
    [resource, setResource] = useState(""),
    [name, setName] = useState(""),
    [sheet, setSheet] = useState("");
  const [encoding, setEncoding] = useState("auto"),
    [delimiter, setDelimiter] = useState("auto"),
    [header, setHeader] = useState(true),
    [confirmed, setConfirmed] = useState(false);
  const [inspection, setInspection] = useState<FileInspection>(),
    [schema, setSchema] = useState<DataSchema>(),
    [preview, setPreview] = useState<PreviewResult>(),
    [message, setMessage] = useState(""),
    [busy, setBusy] = useState(false);
  const [sampleCurrent, setSampleCurrent] = useState(false);
  const [ids] = useState(() => ({
    connection: `import-${crypto.randomUUID()}`,
    component: `source-${crypto.randomUUID()}`,
  }));
  const connection: Connection = {
    id: ids.connection,
    driver: "file",
    resource,
  };
  function options(): Configuration {
    return format === "xlsx"
      ? { header, ...(sheet ? { sheet } : {}) }
      : {
          header,
          ...(encoding !== "auto" ? { encoding } : {}),
          ...(delimiter !== "auto" ? { delimiter } : {}),
        };
  }
  function component(
    selectedSchema: DataSchema,
    fileOptions: Configuration,
  ): FlowComponent {
    return {
      id: ids.component,
      component: format === "csv" ? "file.csv.source" : "file.xlsx.source",
      config: {
        connection: connection.id,
        schema: selectedSchema,
        ...(format === "csv" ? { fileFormat: fileOptions } : fileOptions),
        errorPolicy: "fail",
      },
    };
  }
  async function upload(file?: File) {
    if (!file) return;
    setBusy(true);
    setInspection(undefined);
    setConfirmed(false);
    try {
      const result = await api<{
        resource: string;
        name: string;
        format: string;
      }>("/files/upload", {
        method: "POST",
        body: file,
        headers: {
          "Content-Type": "application/octet-stream",
          "X-File-Name": encodeURIComponent(file.name),
        },
      });
      setResource(result.resource);
      setFormat(result.format);
      setName(result.name);
      setMessage("上传完成，点击发现字段。");
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }
  async function discover() {
    setBusy(true);
    setConfirmed(false);
    setPreview(undefined);
    setSampleCurrent(false);
    try {
      const candidate = {
        ...document,
        connections: [...(document.connections ?? []), connection],
      };
      const result = await post<FileInspection>("/files/inspect", {
        package: candidate,
        connection: connection.id,
        format,
        options: options(),
      });
      setInspection(result);
      setSchema(result.schema);
      if (result.metadata.options?.encoding)
        setEncoding(String(result.metadata.options.encoding));
      if (result.metadata.options?.delimiter)
        setDelimiter(String(result.metadata.options.delimiter));
      if (result.metadata.selectedSheet)
        setSheet(result.metadata.selectedSheet);
      const source = component(
        result.schema,
        result.metadata.options ?? options(),
      );
      const sample = await post<PreviewResult>("/preview", {
        package: {
          ...candidate,
          dataFlows: [{ id: flow, components: [source], paths: [] }],
        },
        pipelineId: flow,
        componentId: source.id,
        sourceRows: 32,
        sampleRows: 16,
      });
      setPreview(sample);
      setSampleCurrent(sample.result === 0);
      setMessage(
        result.metadata.delimiterAmbiguous
          ? "分隔符无法唯一确定，请手动选择并重新发现。"
          : "已读取真实文件。请核对格式、字段和样本。",
      );
    } catch (error) {
      setInspection(undefined);
      setSchema(undefined);
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }
  function invalidate() {
    setConfirmed(false);
    setInspection(undefined);
    setPreview(undefined);
    setSchema(undefined);
    setSampleCurrent(false);
  }
  async function refreshSample() {
    if (!schema || !inspection) return;
    setBusy(true);
    setConfirmed(false);
    setSampleCurrent(false);
    try {
      const source = component(schema, options());
      const sample = await post<PreviewResult>("/preview", {
        package: {
          ...document,
          connections: [...(document.connections ?? []), connection],
          dataFlows: [{ id: flow, components: [source], paths: [] }],
        },
        pipelineId: flow,
        componentId: source.id,
        sourceRows: 32,
        sampleRows: 16,
      });
      setPreview(sample);
      setSampleCurrent(sample.result === 0);
      setMessage(
        sample.result === 0
          ? "已按调整后的类型重新读取真实样本。"
          : "当前字段类型无法读取样本，请检查类型和错误记录。",
      );
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="modal-backdrop">
      <section
        className="modal source-wizard"
        role="dialog"
        aria-modal="true"
        aria-label="文件导入向导"
      >
        <header className="dialog-heading">
          <div>
            <h2>从文件开始</h2>
            <p>选择文件 → 发现字段 → 核对真实样本</p>
          </div>
          <button aria-label="关闭导入向导" onClick={close}>
            <X size={18} />
          </button>
        </header>
        <div className="source-format-choices">
          {[
            ["csv", "CSV 文件", FileText],
            ["xlsx", "Excel 工作簿", FileSpreadsheet],
          ].map(([id, label, Icon]) => {
            const Glyph = Icon as typeof FileText;
            return (
              <button
                key={String(id)}
                className={format === id ? "selected" : ""}
                onClick={() => {
                  setFormat(String(id));
                  invalidate();
                }}
              >
                <Glyph size={22} />
                {String(label)}
              </button>
            );
          })}
        </div>
        <div className="source-options">
          <label className="upload-button">
            <Upload size={18} />
            上传 CSV / XLSX
            <input
              aria-label="上传数据文件"
              type="file"
              accept=".csv,.xlsx"
              disabled={busy}
              onChange={(event) => void upload(event.target.files?.[0])}
            />
          </label>
          <label>
            或填写授权文件路径
            <input
              aria-label="授权文件路径"
              value={resource}
              onChange={(event) => {
                setResource(event.target.value);
                invalidate();
              }}
              placeholder="已授权的绝对路径"
            />
          </label>
          {name ? <small>{name}</small> : null}
          {format === "xlsx" ? (
            <label>
              工作表
              <input
                aria-label="工作表"
                list="workbook-sheets"
                value={sheet}
                placeholder="留空读取第一张工作表"
                onChange={(event) => {
                  setSheet(event.target.value);
                  invalidate();
                }}
              />
              <datalist id="workbook-sheets">
                {inspection?.metadata.sheets?.map((name) => (
                  <option key={name} value={name} />
                ))}
              </datalist>
            </label>
          ) : (
            <>
              <label>
                文件编码
                <select
                  value={encoding}
                  onChange={(event) => {
                    setEncoding(event.target.value);
                    invalidate();
                  }}
                >
                  {["auto", "UTF-8", "UTF-16LE", "UTF-16BE", "ISO-8859-1"].map(
                    (id) => (
                      <option key={id} value={id}>
                        {id === "auto" ? "检测 BOM / UTF-8，导入前确认" : id}
                      </option>
                    ),
                  )}
                </select>
              </label>
              <label>
                分隔符
                <select
                  aria-label="CSV 分隔符"
                  value={delimiter}
                  onChange={(event) => {
                    setDelimiter(event.target.value);
                    invalidate();
                  }}
                >
                  {[
                    ["auto", "自动检测"],
                    [",", "逗号"],
                    ["\t", "Tab"],
                    [";", "分号"],
                    ["|", "竖线"],
                  ].map(([id, label]) => (
                    <option key={id} value={id}>
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
              checked={header}
              onChange={(event) => {
                setHeader(event.target.checked);
                invalidate();
              }}
            />
            首行是字段名称
          </label>
          <button
            className="primary"
            disabled={!resource || busy}
            onClick={() => void discover()}
          >
            {busy ? "Native 读取中…" : "发现字段与样本"}
          </button>
        </div>
        {message ? <p role="status">{message}</p> : null}
        {schema ? (
          <div className="discovery-results">
            <details>
              <summary>
                {schema.fields.length} 个字段 · 点击调整已发现类型
              </summary>
              <DataSchemaEditor
                value={schema}
                change={(value) => {
                  setSchema(value as DataSchema);
                  setConfirmed(false);
                  setPreview(undefined);
                  setSampleCurrent(false);
                }}
              />
              <button disabled={busy} onClick={() => void refreshSample()}>
                按当前类型重新读取样本
              </button>
            </details>
            <button
              disabled={busy}
              onClick={() => {
                setSchema({
                  ...schema,
                  fields: schema.fields.map((field) => ({
                    ...field,
                    type: "utf8",
                    scalar: undefined,
                    precision: undefined,
                    scale: undefined,
                    timezone: undefined,
                    children: undefined,
                  })),
                });
                setConfirmed(false);
                setSampleCurrent(false);
                setPreview(undefined);
                setMessage(
                  "已明确选择全部按文本读取，请重新读取样本；可在派生列中转换并隔离异常值。",
                );
              }}
            >
              全部按文本读取
            </button>
            {!sampleCurrent ? (
              <button disabled={busy} onClick={() => void refreshSample()}>
                按当前类型读取样本
              </button>
            ) : null}
            <DataGrid
              schema={schema}
              rows={
                preview?.snapshot.preview.find(
                  (edge) => edge.component === ids.component && edge.port === 0,
                )?.rows
              }
            />
          </div>
        ) : null}
        <footer>
          <label className="checkbox">
            <input
              aria-label="确认文件格式与字段"
              type="checkbox"
              checked={confirmed}
              disabled={!inspection || !sampleCurrent || busy}
              onChange={(event) => setConfirmed(event.target.checked)}
            />
            已确认编码、分隔符 / 工作表、首行与字段类型
          </label>
          <button
            disabled={
              busy ||
              !schema ||
              !inspection ||
              !confirmed ||
              !sampleCurrent ||
              inspection.metadata.delimiterAmbiguous
            }
            className="primary"
            onClick={() => {
              if (schema)
                complete(
                  connection,
                  component(schema, options()),
                  name || resource.split(/[\\/]/).at(-1),
                  preview,
                );
            }}
          >
            添加数据源
          </button>
        </footer>
      </section>
    </div>
  );
}
