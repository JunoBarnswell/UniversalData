import { useEffect, useRef, useState } from "react";
import { ArrowRight, Database, FileSpreadsheet, FolderOpen, Plus, Upload, X } from "lucide-react";
import { api, encode } from "../../api";
import { AnalysisReport } from "./AnalysisReport";
import { ValuesTable } from "./AnalysisTable";
import { datasetPath, roles, type Dataset, type Model, type Preview, type Publication, type PublishRequest, type Release, type Rule, type Source } from "./model";
import "./analysis.css";

function replaceAddress(dataset?: string, draft = false) {
  window.history.replaceState(null, "", dataset ? `/analysis/${dataset}${draft ? "?draft=1" : ""}` : "/analysis");
}

export function Analysis() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [dataset, setDataset] = useState<Dataset>();
  const [stage, setStage] = useState<"source" | "model" | "report">("source");
  const [replacement, setReplacement] = useState<Dataset>();
  const [release, setRelease] = useState<Release>();
  const [error, setError] = useState("");
  const [busy, setBusy] = useState("");
  const navigation = useRef<AbortController | null>(null);
  const focus = useRef<HTMLElement>(null);

  useEffect(() => {
    const controller = new AbortController();
    const saved = window.location.pathname.split("/")[2];
    api<Dataset[]>("/analysis/datasets", { signal: controller.signal }).then(setDatasets).catch(fail);
    api<Source[]>("/analysis/sources", { signal: controller.signal }).then(setSources).catch(fail);
    if (saved) {
      api<Dataset>(datasetPath(saved), { signal: controller.signal }).then(async (value) => {
        setDataset(value);
        const version = value.report?.query.releaseId ?? value.publishedReleaseId;
        if (version && !new URLSearchParams(window.location.search).has("draft")) {
          const open = await api<Release>(`${datasetPath(saved)}/releases/${version}`, { signal: controller.signal });
          setRelease(open);
          setStage("report");
        } else setStage("model");
      }).catch(fail);
    }
    function fail(reason: unknown) { if (!controller.signal.aborted) setError(String(reason)); }
    return () => controller.abort();
  }, []);

  function remember(value: Dataset, draft = false) {
    setDataset(value);
    setDatasets((saved) => [value, ...saved.filter((item) => item.id !== value.id)]);
    replaceAddress(value.id, draft);
  }

  async function open(value: Dataset, preferred?: string | null) {
    navigation.current?.abort();
    const controller = new AbortController();
    navigation.current = controller;
    setError("");
    setBusy("正在打开分析…");
    try {
      const current = await api<Dataset>(datasetPath(value.id), { signal: controller.signal });
      const version = preferred ?? current.report?.query.releaseId ?? current.publishedReleaseId;
      if (version) {
        const result = await api<Release>(`${datasetPath(value.id)}/releases/${version}`, { signal: controller.signal });
        setRelease(result);
        setStage("report");
      } else setStage("model");
      remember(current, !version);
      setReplacement(undefined);
      focus.current?.focus();
    } catch (reason) {
      if (!controller.signal.aborted) setError(String(reason));
    } finally { if (!controller.signal.aborted) setBusy(""); }
  }

  async function choose(action: (signal: AbortSignal) => Promise<Dataset>) {
    navigation.current?.abort();
    const controller = new AbortController();
    navigation.current = controller;
    setError("");
    setBusy("正在准备数据…");
    try {
      remember(await action(controller.signal), true);
      setStage("model");
      setReplacement(undefined);
      focus.current?.focus();
    } catch (reason) {
      if (!controller.signal.aborted) setError(String(reason));
    } finally { if (!controller.signal.aborted) setBusy(""); }
  }

  function start(replace?: Dataset) {
    setError("");
    setReplacement(replace);
    setDataset(undefined);
    setStage("source");
    replaceAddress();
    focus.current?.focus();
  }

  function editModel() {
    setStage("model");
    replaceAddress(dataset?.id, true);
  }

  const sourceBody = replacement ? { datasetId: replacement.id, expectedRevision: replacement.revision } : {};
  const uploadPath = replacement ? `?datasetId=${replacement.id}&expectedRevision=${replacement.revision}` : "";
  return <main className="analysis-page" ref={focus} tabIndex={-1}>
    <aside className="analysis-sidebar" aria-label="已保存的分析">
      <div className="analysis-sidebar-title"><FolderOpen size={19} /><strong>我的分析</strong></div>
      <button type="button" className="analysis-new" onClick={() => start()} disabled={!!busy}><Plus size={16} />新建分析</button>
      <div className="analysis-library">
        {datasets.map((item) => <button type="button" key={item.id} className={item.id === dataset?.id ? "selected" : ""}
          disabled={!!busy} onClick={() => void open(item)}>
          <strong>{item.name}</strong><small>{item.publishedReleaseId ? "已保存报表" : "草稿"} · {item.sourceName}</small>
        </button>)}
        {!datasets.length && <small>保存的报表和草稿会出现在这里。</small>}
      </div>
    </aside>
    <section className="analysis-main">
      <nav className="analysis-steps" aria-label="分析进度">
        <button type="button" aria-current={stage === "source" ? "step" : undefined} onClick={() => start(dataset)} disabled={!!busy || stage === "source"}>选数据</button>
        <ArrowRight size={14} aria-hidden="true" />
        <button type="button" aria-current={stage === "model" ? "step" : undefined} onClick={editModel} disabled={!dataset || !!busy || stage !== "report"}>确认建议</button>
        <ArrowRight size={14} aria-hidden="true" />
        <button type="button" aria-current={stage === "report" ? "step" : undefined} onClick={() => dataset && void open(dataset)}
          disabled={!dataset?.publishedReleaseId || !!busy || stage === "report"}>看结果</button>
      </nav>
      {error && <div className="analysis-alert" role="alert">{error}</div>}
      {busy && <div className="analysis-progress" role="status">{busy}<button type="button" onClick={() => {
        navigation.current?.abort();
        setBusy("");
      }}><X size={14} />取消</button></div>}
      {stage === "source" && <div className="analysis-source">
        <h1>{replacement ? "更新分析数据" : "用一张表生成报表"}</h1>
        <p>{replacement ? `为「${replacement.name}」选择新数据。已保存报表仍可打开。` : "选择已有数据或上传一张明细表。"}</p>
        <div className="analysis-source-options">
          <section>
            <Database size={23} aria-hidden="true" />
            <h2>选已有数据</h2>
            {sources.length ? sources.map((source) => <button type="button" key={source.id} disabled={!!busy}
                onClick={() => void choose((signal) => api<Dataset>("/analysis/datasets", { method: "POST", signal, body: encode({ sourceId: source.id, ...sourceBody }) }))}>
                {source.name}<ArrowRight size={16} />
              </button>) : <p>当前没有可用的已发布数据。</p>}
          </section>
          <section>
            <FileSpreadsheet size={23} aria-hidden="true" />
            <h2>上传表格</h2>
            <p>CSV 或 Excel，最多 50 MiB。表格需有一行唯一表头。</p>
            <label className="analysis-upload">
              <Upload size={17} aria-hidden="true" />选择文件
              <input aria-label="选择要上传的表格" disabled={!!busy} type="file" accept=".csv,.xlsx" onChange={(event) => {
                const file = event.currentTarget.files?.[0];
                event.currentTarget.value = "";
                if (file) void choose((signal) => api<Dataset>(`/analysis/datasets/upload${uploadPath}`, {
                  method: "POST", signal, body: file,
                  headers: { "Content-Type": "application/octet-stream", "X-File-Name": encodeURIComponent(file.name) },
                }));
              }} />
            </label>
          </section>
        </div>
        <button type="button" className="analysis-sample" disabled={!!busy}
          onClick={() => void choose((signal) => api<Dataset>("/analysis/datasets", { method: "POST", signal, body: encode({ sourceId: "sample", ...sourceBody }) }))}>
          使用订单样例<ArrowRight size={16} />
        </button>
      </div>}
      {stage === "model" && dataset && <ModelReview key={dataset.id} dataset={dataset}
        onSave={(value) => remember(value, true)} onPublished={(value) => open(dataset, value.id)} />}
      {stage === "report" && dataset && release && <AnalysisReport key={`${dataset.id}/${release.id}`} dataset={dataset} release={release}
        onSaved={remember} onEdit={editModel} onLatest={() => void open(dataset, dataset.publishedReleaseId)}
        onReplace={() => start(dataset)} />}
    </section>
  </main>;
}

function ModelReview({ dataset, onSave, onPublished }: {
  dataset: Dataset;
  onSave(dataset: Dataset): void;
  onPublished(release: Release): Promise<void>;
}) {
  const path = datasetPath(dataset.id);
  const [sheet, setSheet] = useState(dataset.sheet ?? "");
  const [preview, setPreview] = useState<Preview>();
  const [model, setModel] = useState<Model | null>(dataset.draft);
  const [name, setName] = useState(dataset.name);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");
  const [publication, setPublication] = useState<Publication>();
  const [operation, setOperation] = useState<PublishRequest | undefined>(() => {
    const stored = sessionStorage.getItem(`ud-analysis-operation-${dataset.id}`);
    return stored ? JSON.parse(stored) as PublishRequest : undefined;
  });
  const [attempt, setAttempt] = useState(0);
  const action = useRef<AbortController | null>(null);
  const saved = useRef(dataset);
  const key = `ud-analysis-operation-${dataset.id}`;

  useEffect(() => {
    const controller = new AbortController();
    api<Preview>(`${path}/preview${sheet ? `?sheet=${encodeURIComponent(sheet)}` : ""}`, { signal: controller.signal })
      .then((result) => {
        setPreview(result);
        setError("");
        if (!result.suggestion) { setModel(null); return; }
        const current = saved.current;
        setModel(current.sheet === result.sheet && current.draft ? current.draft : result.suggestion);
      }).catch((failure) => { if (!controller.signal.aborted) setError(String(failure)); });
    return () => controller.abort();
  }, [path, sheet, attempt]);
  useEffect(() => () => action.current?.abort(), []);

  function setRule(rule: Rule, role: Rule["role"]) {
    setModel((current) => current && ({
      ...current,
      fields: current.fields.map((field) => field.fieldId === rule.fieldId ? { ...field, role }
        : field.role === role && ["date", "amount", "identifier", "currency"].includes(role) ? { ...field, role: "text" } : field),
    }));
  }

  async function save(generate: boolean) {
    if (!model) return;
    const controller = new AbortController();
    action.current = controller;
    setError("");
    setPublication(undefined);
    setStatus(generate ? "正在核对整张表…" : "正在保存草稿…");
    try {
      const before = saved.current;
      let current = before;
      let request = generate && operation && before.revision === operation.expectedRevision && JSON.stringify(before.draft) === JSON.stringify(model)
        ? operation : undefined;
      if (!request) {
        current = await api<Dataset>(`${path}/draft`, { method: "PUT", signal: controller.signal, body: encode({
          expectedRevision: before.revision, name, model, sheet: preview?.sheet ?? null,
        }) });
        saved.current = current;
        onSave(current);
        if (generate) {
          request = { operationId: crypto.randomUUID().replaceAll("-", ""), expectedRevision: current.revision, expectedReleaseId: current.publishedReleaseId };
          setOperation(request);
          sessionStorage.setItem(key, JSON.stringify(request));
        }
      }
      if (!generate || !request) { setStatus("草稿已保存。"); return; }
      const result = await api<Publication>(`${path}/publish`, { method: "POST", signal: controller.signal, body: encode(request) });
      setPublication(result);
      if (result.state !== "checking") {
        sessionStorage.removeItem(key);
        setOperation(undefined);
      }
      if (result.release) await onPublished(result.release);
      setStatus("");
    } catch (failure) {
      if (!controller.signal.aborted) setError(String(failure));
      setStatus("");
    }
  }

  const date = model?.fields.find((rule) => rule.role === "date");
  const money = model?.fields.find((rule) => rule.role === "amount");
  const unit = model?.fields.find((rule) => rule.role === "currency");
  const identifier = model?.fields.find((rule) => rule.role === "identifier");
  const saving = status.startsWith("正在");
  return <div className="analysis-review">
    <header className="analysis-heading">
      <div><h1>确认建模建议</h1><p>{dataset.sourceName}{preview?.sheet ? ` · ${preview.sheet}` : ""}</p></div>
      {model && <label>报表名称<input value={name} onChange={(event) => setName(event.target.value)} maxLength={128} /></label>}
    </header>
    {error && <div className="analysis-alert" role="alert">{error}<button type="button" onClick={() => setAttempt(attempt + 1)}>重试预览</button></div>}
    {preview && preview.sheets.length > 1 && <label className="analysis-field">工作表
      <select value={sheet || preview.sheet || ""} disabled={saving} onChange={(event) => { setPreview(undefined); setModel(null); setSheet(event.target.value); }}>
        <option value="" disabled>选择一张工作表</option>
        {preview.sheets.map((value) => <option key={value}>{value}</option>)}
      </select>
    </label>}
    {!preview && !error && <p role="status">正在读取表头和样本…</p>}
    {preview?.suggestion && model && <>
      <div className="analysis-assumptions">
        <strong>每行是{model.grain}</strong>
        {date && <span>按「{date.label}」看趋势</span>}
        {money && <span>对「{money.label}」求和</span>}
        {identifier && <span>按「{identifier.label}」计算订单数</span>}
      </div>
      <section className="analysis-panel">
        <h2>样本预览 <small>前 {preview.rows.length} 条可读记录</small></h2>
        <ValuesTable fields={preview.fields.map((field) => ({ fieldId: field.id, label: field.label }))} rows={preview.rows} />
      </section>
      <section className="analysis-panel">
        <h2>字段用途</h2>
        <div className="analysis-rules">
          {preview.fields.map((field) => {
            const rule = model.fields.find((item) => item.fieldId === field.id);
            return rule ? <div key={field.id}>
              <label>{field.label}<small>{field.reason}</small></label>
              <select aria-label={`${field.label}用途`} value={rule.role} onChange={(event) => setRule(rule, event.target.value as Rule["role"])} disabled={saving}>
                {roles.map(({value, label}) => <option key={value} value={value}>{label}</option>)}
              </select>
            </div> : null;
          })}
        </div>
        <details className="analysis-settings">
          <summary>编辑粒度、显示名称和解析规则</summary>
          <div className="analysis-settings-fields">
            <label>每行代表什么<input value={model.grain} onChange={(event) => setModel({ ...model, grain: event.target.value })} /></label>
            {date && <label>日期格式<select value={model.dateFormat} onChange={(event) => setModel({ ...model, dateFormat: event.target.value })}>
              <option value="">请选择日期格式</option><option value="iso">年-月-日（2026-04-03）</option>
              <option value="dmy">日/月/年（03/04/2026）</option><option value="mdy">月/日/年（04/03/2026）</option>
              <option value="timestamp">含时区的 ISO 时间戳</option>
            </select></label>}
            {date && model.dateFormat === "timestamp" && <label>报表时区与 UTC 的分钟差<input type="number" min={-840} max={840} value={model.offsetMinutes}
              onChange={(event) => setModel({ ...model, offsetMinutes: Number(event.target.value) })} /></label>}
            {money && <label>金额格式<select value={model.amountFormat} onChange={(event) => setModel({ ...model, amountFormat: event.target.value })}>
              <option value="point">1234.50</option><option value="pointGrouped">1,234.50</option>
              <option value="comma">1234,50</option><option value="commaGrouped">1.234,50</option>
            </select></label>}
            {money && <label className="analysis-checkbox"><input type="checkbox" checked={model.allowEmptyAmount}
              onChange={(event) => setModel({ ...model, allowEmptyAmount: event.target.checked })} />空金额不参与求和</label>}
            {model.fields.filter((field) => field.role !== "omit").map((field) => <label key={field.fieldId}>
              {preview.fields.find((column) => column.id === field.fieldId)?.label}的显示名称
              <input value={field.label} maxLength={128}
                onChange={(event) => setModel({ ...model, fields: model.fields.map((value) => value.fieldId === field.fieldId ? { ...value, label: event.target.value } : value) })} />
            </label>)}
          </div>
        </details>
        {(money || unit || (date && !model.dateFormat)) && <div className="analysis-decisions">
          {date && !model.dateFormat && <label>确认「{date.label}」的日期顺序
            <select value="" onChange={(event) => setModel({ ...model, dateFormat: event.target.value })}>
              <option value="">选择日期格式</option><option value="iso">年-月-日</option>
              <option value="dmy">日/月/年</option><option value="mdy">月/日/年</option>
              <option value="timestamp">含时区的 ISO 时间戳</option>
            </select>
          </label>}
          {(money || unit) && <label>币种代码<input aria-label="币种代码" placeholder="CNY" value={model.currency ?? ""} maxLength={3}
            onChange={(event) => setModel({ ...model, currency: event.target.value.toUpperCase() })} /></label>}
          {money && <label className="analysis-checkbox"><input type="checkbox" checked={model.confirmLineAmount}
            onChange={(event) => setModel({ ...model, confirmLineAmount: event.target.checked })} />「{money.label}」是每一行的金额</label>}
          {money && !unit && <label className="analysis-checkbox"><input type="checkbox" checked={model.confirmSingleCurrency}
            onChange={(event) => setModel({ ...model, confirmSingleCurrency: event.target.checked })} />确认这张表使用同一币种</label>}
        </div>}
      </section>
      {publication?.state === "rejected" && <div className="analysis-alert" role="alert">
        <strong>{publication.error ?? `核对了 ${publication.scannedRows} 条记录，发现 ${publication.affectedRows} 条需要修改`}</strong>
        {publication.issues && <ul>{publication.issues.map((issue) => <li key={`${issue.row}/${issue.fieldId}`}>
          第 {issue.row} 条记录 · {model.fields.find((field) => field.fieldId === issue.fieldId)?.label} · {issue.message}
        </li>)}</ul>}
      </div>}
      <footer className="analysis-actions">
        <span role="status">{status}</span>
        {saving && <button type="button" onClick={() => { action.current?.abort(); setStatus(""); }}>取消</button>}
        <button type="button" disabled={saving} onClick={() => void save(false)}>保存草稿</button>
        <button type="button" className="analysis-primary" disabled={saving} onClick={() => void save(true)}>
          {operation ? "继续核对" : "生成报表"}<ArrowRight size={16} />
        </button>
      </footer>
    </>}
  </div>;
}
