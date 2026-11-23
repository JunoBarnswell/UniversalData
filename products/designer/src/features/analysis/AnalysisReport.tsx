import { useEffect, useRef, useState } from "react";
import { ArrowLeft, ChevronRight, Download, Info, RefreshCw, Save, X } from "lucide-react";
import { api, encode, request } from "../../api";
import { Button, Input, Select } from "../../shared/design-system/Controls";
import { ValuesTable } from "./AnalysisTable";
import { datasetPath, type Cell, type Chart, type Dataset, type Details, type Dimension, type Filter, type Group,
  type Query, type Release, type ReportResult, type SavedReport,
} from "./model";

function defaultReport(dataset: Dataset, release: Release): SavedReport {
  if (dataset.report?.query.releaseId === release.id) return dataset.report;
  return release.startingReport;
}

const setKey = (value: (string | null)[]) => JSON.stringify(value);
function label(value: string | null): string {
  return value === null ? "未填写" : value === "未填写" ? "未填写（原文）" : value;
}
function number(value: string | null | undefined, missing = "—") {
  if (value == null) return missing;
  return value.replace(/^-?\d+/, (whole) => whole.replace(/\B(?=(\d{3})+(?!\d))/g, ","));
}

export function AnalysisReport({ dataset, release, onSaved, onEdit, onLatest, onReplace }: {
  dataset: Dataset; release: Release; onSaved(dataset: Dataset): void;
  onEdit(): void; onLatest(): void; onReplace(): void;
}) {
  const path = datasetPath(dataset.id);
  const [settings, setSettings] = useState(() => defaultReport(dataset, release));
  const [applied, setApplied] = useState(settings);
  const [view, setView] = useState<{ settings: SavedReport; data: ReportResult }>();
  const data = view?.data;
  const visible = view?.settings ?? applied;
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [saving, setSaving] = useState(false);
  const [details, setDetails] = useState<Details>();
  const [selected, setSelected] = useState<(string | null)[] | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const detailRequest = useRef<AbortController | null>(null);
  const date = release.model.fields.find((field) => field.role === "date");
  const categories = release.model.fields.filter((field) => field.role === "category");
  const filters = settings.query.filters ?? [];
  const category = filters.find((filter) => filter.operator === "eq" || filter.operator === "isNull");
  const labels = new Map(release.model.fields.map((field) => [field.fieldId, field.label]));

  useEffect(() => {
    const controller = new AbortController();
    api<ReportResult>(`${path}/query`, { method: "POST", body: encode(applied.query), signal: controller.signal })
      .then((result) => {
        setView({ settings: applied, data: result });
        setLoading(false);
        setError("");
        if (result.groups.length === 0) {
          detailRequest.current?.abort();
          const detailController = new AbortController();
          detailRequest.current = detailController;
          api<Details>(`${path}/drill`, { method: "POST", signal: detailController.signal,
            body: encode({ context: result.context }) }).then(setDetails).catch((failure) => {
            if (!detailController.signal.aborted) setDetailError(String(failure));
          });
        }
      }).catch((failure) => {
        if (!controller.signal.aborted) {
          setError(String(failure));
          setLoading(false);
        }
      });
    return () => { controller.abort(); detailRequest.current?.abort(); };
  }, [path, applied]);

  function updateQuery(next: Partial<Query>) {
    setSettings((value) => ({ ...value, query: { ...value.query, ...next } }));
  }

  function setDimension(index: number, value: string) {
    const dimensions = settings.query.dimensions.slice();
    if (!value) dimensions.splice(index, 1);
    else {
      const [fieldId, level] = value.split("/");
      dimensions[index] = { fieldId, level: level as Dimension["level"] };
    }
    const level = dimensions[0]?.level;
    setSettings({ ...settings, query: { ...settings.query, dimensions },
      chart: !level ? "table" : level === "value" && settings.chart === "line" ? "bar" : settings.chart,
    });
  }

  function dateBound(operator: "from" | "before", value: string) {
    const rest = filters.filter((filter) => filter.operator !== operator);
    updateQuery({ filters: date && value ? [...rest, { fieldId: date.fieldId, operator, value }] : rest });
  }

  function categoryFilter(fieldId: string, value?: string, empty?: boolean) {
    const rest = filters.filter((filter) => filter.operator === "from" || filter.operator === "before");
    const filter: Filter = empty ? { fieldId, operator: "isNull" } : { fieldId, operator: "eq", value: value ?? "" };
    updateQuery({ filters: fieldId ? [...rest, filter] : rest });
  }

  function apply() {
    detailRequest.current?.abort();
    setDetails(undefined);
    setDetailLoading(false);
    setDetailError("");
    setSelected(null);
    setError("");
    setNotice("");
    setLoading(true);
    setApplied({ ...settings });
  }

  async function saveReport() {
    setSaving(true);
    setError("");
    setNotice("");
    try {
      onSaved(await api<Dataset>(`${path}/report`, { method: "PUT",
        body: encode({ expectedRevision: dataset.revision, report: { ...applied, name: settings.name } }) }));
      setView((current) => current && ({ ...current, settings: { ...current.settings, name: settings.name } }));
      setNotice("报表已保存。");
    } catch (failure) { setError(String(failure)); }
    finally { setSaving(false); }
  }

  async function drill(cell: (string | null)[] | null, cursor?: string | null) {
    if (!data || loading) return;
    detailRequest.current?.abort();
    const controller = new AbortController();
    detailRequest.current = controller;
    setSelected(cell);
    if (!cursor) setDetails(undefined);
    setDetailLoading(true);
    setDetailError("");
    try {
      setDetails(await api<Details>(`${path}/drill`, {
        method: "POST", signal: controller.signal, body: encode({ context: data.context, cell, cursor: cursor ?? null }),
      }));
    } catch (failure) {
      if (!controller.signal.aborted) setDetailError(String(failure));
    } finally { if (!controller.signal.aborted) setDetailLoading(false); }
  }

  async function download(detail = false) {
    if (!data) return;
    setError("");
    try {
      const response = await request(`${path}/export`, {
        method: "POST", body: encode({ context: data.context, cell: detail ? selected : null, details: detail }),
      });
      const url = URL.createObjectURL(await response.blob());
      const link = document.createElement("a");
      link.href = url;
      link.download = `${settings.name || "分析"}${detail ? "-明细" : ""}.csv`;
      link.click();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (failure) { setError(String(failure)); }
  }

  const measure = data?.measures.find((item) => item.id === visible.chartMeasure) ?? data?.measures[0];
  const primary = data?.groups[0];
  const display = data?.groups.at(-1);
  const editing = JSON.stringify(settings.query) !== JSON.stringify(applied.query) || settings.chart !== applied.chart ||
    settings.chartMeasure !== applied.chartMeasure;
  const dimensions: Dimension[] = [
    ...(date ? [{ fieldId: date.fieldId, level: "day" as const }, { fieldId: date.fieldId, level: "month" as const }] : []),
    ...categories.map((field) => ({ fieldId: field.fieldId, level: "value" as const })),
  ];
  const dimensionLabel = (item: Dimension) => `${labels.get(item.fieldId)}${item.level === "month" ? " · 月" : item.level === "day" ? " · 日" : ""}`;
  const asOf = new Date(release.dataAsOf).toLocaleString("zh-CN");
  const detailOpen = !!data && (detailLoading || !!details || !!detailError);
  const standaloneDetails = display == null;
  return <article className="analysis-report">
    <header className="analysis-heading">
      <div><h1>{visible.name}</h1><p>{release.sourceName} · 版本 {release.id} · 数据截至 {asOf}</p></div>
      <div className="analysis-heading-actions">
        <Button onClick={onEdit}><ArrowLeft size={15} />编辑建模规则</Button>
        <Button onClick={onReplace}><RefreshCw size={15} />更新数据</Button>
        <Button disabled={!data || loading} onClick={() => void download(data?.groups.length === 0)}><Download size={15} />导出当前视图</Button>
      </div>
    </header>
    {dataset.publishedReleaseId && release.id !== dataset.publishedReleaseId && <div className="analysis-progress">
      正在查看已保存的版本 {release.id}。<Button onClick={onLatest}>打开版本 {dataset.publishedReleaseId}</Button>
    </div>}
    {error && <div className="analysis-alert" role="alert">{data ? "正在查看上次成功的数据。 " : ""}{error}</div>}
    {notice && <p role="status">{notice}</p>}
    <form className="analysis-filters" onSubmit={(event) => { event.preventDefault(); apply(); }}>
      <label>看什么<Select value={settings.chartMeasure ?? "records"} onChange={(event) => setSettings({ ...settings, chartMeasure: event.target.value })}>
        {release.measures.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}
      </Select></label>
      {[0, 1].map((index) => (index === 0 || settings.query.dimensions.length > 0) && <label key={index}>{index === 0 ? "按什么看" : "透视列"}
        <Select value={settings.query.dimensions[index] ? `${settings.query.dimensions[index].fieldId}/${settings.query.dimensions[index].level}` : ""}
          onChange={(event) => setDimension(index, event.target.value)}>
          <option value="">{index === 0 ? "查看明细" : "无"}</option>
          {dimensions.filter((item) => !settings.query.dimensions.some((group, at) => at !== index && group.fieldId === item.fieldId))
            .map((item) => <option key={`${item.fieldId}/${item.level}`} value={`${item.fieldId}/${item.level}`}>{dimensionLabel(item)}</option>)}
        </Select></label>)}
      <label>视图<Select value={settings.chart} onChange={(event) => setSettings({ ...settings, chart: event.target.value as Chart })}>
        <option value="table">表格</option>
        {settings.query.dimensions.length > 0 && <option value="bar">横条</option>}
        {settings.query.dimensions[0]?.level !== "value" && settings.query.dimensions.length > 0 && <option value="line">折线</option>}
      </Select></label>
      {date && <>
        <label>开始日期<Input type="date" value={filters.find((filter) => filter.operator === "from")?.value ?? ""}
          onChange={(event) => dateBound("from", event.target.value)} /></label>
        <label>结束日期（不含）<Input type="date" value={filters.find((filter) => filter.operator === "before")?.value ?? ""}
          onChange={(event) => dateBound("before", event.target.value)} /></label>
      </>}
      {categories.length > 0 && <label>分类筛选<Select value={category?.fieldId ?? ""} onChange={(event) => categoryFilter(event.target.value)}>
        <option value="">所有分类</option>{categories.map((item) => <option value={item.fieldId} key={item.fieldId}>{item.label}</option>)}
      </Select></label>}
      {category && <>
        <label>分类值<Input value={category.value ?? ""} placeholder="输入完整类别名称" disabled={category.operator === "isNull"}
          onChange={(event) => categoryFilter(category.fieldId, event.target.value)} /></label>
        <label className="analysis-checkbox"><Input type="checkbox" checked={category.operator === "isNull"}
          onChange={(event) => categoryFilter(category.fieldId, category.value, event.target.checked)} />仅未填写</label>
      </>}
      <Button type="submit" className="analysis-primary">应用筛选</Button>
    </form>
    {loading && <p className="analysis-progress" role="status">正在计算版本 {release.id} 的数据…</p>}
    {data && <>
      <div className="analysis-metrics">
        {data.measures.map((item) => <section key={item.id}>
          <span>{item.label}</span>
          <strong>{data.empty ? "无数据" : number(data.total[item.id], item.id === "amount" ? "无有效金额" : "不适用")}</strong>
          {item.unit && <small>{item.unit}</small>}
        </section>)}
      </div>
      <details className="analysis-explanation">
        <summary><Info size={15} />数字怎么算</summary>
        <p>每行是{release.model.grain}。使用版本 {release.id}，核对时间 {new Date(release.validatedAt).toLocaleString("zh-CN")}。</p>
        <ul>{data.measures.map((item) => <li key={item.id}>{item.label} · {item.calculation}{item.unit ? ` · ${item.unit}` : ""}</li>)}</ul>
        {(visible.query.filters ?? []).length > 0 && <p>当前筛选 · {(visible.query.filters ?? []).map((filter) =>
          `${labels.get(filter.fieldId)} ${filter.operator === "from" ? "自" : filter.operator === "before" ? "早于" : ""} ${filter.value ?? "未填写"}`).join("、")}</p>}
        {data.measures.some((item) => item.id === "orders" || item.id === "average") && <p>总计按完整筛选范围重新计算。同一订单可出现在多个类别。</p>}
      </details>
      {data.empty && <p className="analysis-empty" role="status">当前筛选范围没有记录。</p>}
      {!data.empty && primary && measure && visible.chart !== "table" && <section className="analysis-panel">
        <h2>{measure.label} <small>按{dimensionLabel(primary.dimensions[0])}</small></h2>
        <ChartView kind={visible.chart} cells={primary.cells} measure={measure.id} onCell={(cell) => void drill(cell.keys)} />
      </section>}
      {!data.empty && display && measure && <section className="analysis-panel">
        <div className="analysis-section-heading"><h2>数据透视表</h2><Button onClick={() => void drill(null)}>查看全部明细<ChevronRight size={15} /></Button></div>
        {display.dimensions.length === 2 ? <Pivot result={data} group={display} measure={measure.id} labelFor={dimensionLabel}
          onCell={(cell) => void drill(cell.keys)} /> : <div className="analysis-scroll"><table className="analysis-table">
          <thead><tr><th scope="col">{dimensionLabel(display.dimensions[0])}</th>{data.measures.map((item) => <th key={item.id} scope="col">{item.label}</th>)}</tr></thead>
          <tbody>{display.cells.map((cell) => <tr key={setKey(cell.keys)}><th scope="row">{label(cell.keys[0])}</th>
            {data.measures.map((item) => <td key={item.id}><Button className="text-button" onClick={() => void drill(cell.keys)}>{number(cell.values[item.id])}</Button></td>)}
          </tr>)}</tbody>
          <tfoot><tr><th scope="row">总计</th>{data.measures.map((item) => <td key={item.id}>{number(data.total[item.id])}</td>)}</tr></tfoot>
        </table></div>}
      </section>}
      {detailOpen && <section className="analysis-panel" aria-label="明细">
        <div className="analysis-section-heading">
          <h2>{standaloneDetails ? "明细表" : "明细"} <small>{selected?.map(label).join(" / ") || "当前筛选范围"}</small></h2>
          <Button disabled={!details} onClick={() => void download(true)}><Download size={15} />导出明细</Button>
          {!standaloneDetails && <Button aria-label="关闭明细" onClick={() => {
            detailRequest.current?.abort(); setDetails(undefined); setDetailLoading(false); setDetailError("");
          }}><X size={16} /></Button>}
        </div>
        {detailLoading && <p role="status">正在读取明细…</p>}
        {detailError && <p role="alert">{detailError}</p>}
        {details && <ValuesTable fields={details.fields} rows={details.rows} />}
        {details?.nextCursor && <Button disabled={detailLoading} onClick={() => void drill(selected, details.nextCursor)}>下一页<ChevronRight size={15} /></Button>}
      </section>}
    </>}
    <footer className="analysis-actions">
      <label>报表名称<Input value={settings.name} maxLength={128} onChange={(event) => setSettings({ ...settings, name: event.target.value })} /></label>
      {editing && <span>应用当前设置后可保存。</span>}
      <Button className="analysis-primary" disabled={!data || loading || saving || editing || !!error}
        onClick={() => void saveReport()}><Save size={15} />保存报表</Button>
    </footer>
  </article>;
}

function Pivot({ result, group, measure, labelFor, onCell }: {
  result: ReportResult; group: Group; measure: string; labelFor(value: Dimension): string; onCell(cell: Cell): void;
}) {
  const [rows, columns] = result.groups;
  const lookup = new Map(group.cells.map((cell) => [setKey(cell.keys), cell]));
  return <div className="analysis-scroll"><table className="analysis-table">
    <thead><tr><th scope="col">{labelFor(group.dimensions[0])} / {labelFor(group.dimensions[1])}</th>
      {columns.cells.map((cell) => <th scope="col" key={setKey(cell.keys)}>{label(cell.keys[0])}</th>)}
      <th scope="col">小计</th></tr></thead>
    <tbody>{rows.cells.map((row) => <tr key={setKey(row.keys)}><th scope="row">{label(row.keys[0])}</th>
      {columns.cells.map((column) => {
        const cell = lookup.get(setKey([row.keys[0], column.keys[0]]));
        return <td key={setKey(column.keys)}>{cell ? <Button className="text-button" onClick={() => onCell(cell)}>{number(cell.values[measure])}</Button> : "—"}</td>;
      })}<td><Button className="text-button" onClick={() => onCell(row)}>{number(row.values[measure])}</Button></td>
    </tr>)}</tbody>
    <tfoot><tr><th scope="row">总计</th>{columns.cells.map((cell) => <td key={setKey(cell.keys)}>{number(cell.values[measure])}</td>)}
      <td>{number(result.total[measure])}</td></tr></tfoot>
  </table></div>;
}

function ChartView({ kind, cells, measure, onCell }: {
  kind: Chart; cells: Cell[]; measure: string; onCell(cell: Cell): void;
}) {
  const ordered = kind === "line" ? [...cells].sort((a, b) => (a.keys[0] ?? "").localeCompare(b.keys[0] ?? "")) : cells;
  const values = ordered.map((cell) => cell.values[measure] == null ? null : Number(cell.values[measure]));
  const numeric = values.filter((value): value is number => value !== null && Number.isFinite(value));
  if (!numeric.length) return <p className="analysis-empty">当前指标没有有效数值。</p>;
  const max = Math.max(0, ...numeric), min = Math.min(0, ...numeric), range = max - min || 1;
  if (kind === "bar") return <div className="analysis-bars" aria-label="分类横条">
    {ordered.map((cell, index) => {
      const value = values[index];
      const zero = -min / range * 100;
      const position = value === null ? zero : (value - min) / range * 100;
      return <Button key={setKey(cell.keys)} onClick={() => onCell(cell)}>
        <span>{label(cell.keys[0])}</span>
        <span className="analysis-bar-track" aria-hidden="true"><i style={{ left: `${Math.min(zero, position)}%`, width: `${Math.abs(position - zero)}%` }} /></span>
        <strong>{number(cell.values[measure])}</strong>
      </Button>;
    })}
  </div>;
  const width = 720, height = 190, inset = 32;
  const xValue = (index: number) => ordered.length === 1 ? width / 2 : inset + (width - 2 * inset) * index / (ordered.length - 1);
  const yValue = (value: number) => height - inset - (height - 2 * inset) * (value - min) / range;
  const path = values.map((value, index) => value === null ? "" : `${index === 0 || values[index - 1] === null ? "M" : "L"}${xValue(index)} ${yValue(value)}`).join(" ");
  return <div className="analysis-line-chart">
    <svg viewBox={`0 0 ${width} ${height}`} role="group" aria-label="日期折线">
      <line x1={inset} x2={width - inset} y1={yValue(0)} y2={yValue(0)} />
      <path d={path} />
      {ordered.map((cell, index) => values[index] === null ? null : <circle key={setKey(cell.keys)}
        cx={xValue(index)} cy={yValue(values[index])} r="4" tabIndex={0} role="button"
        aria-label={`${label(cell.keys[0])}，${number(cell.values[measure])}，查看明细`}
        onClick={() => onCell(cell)}
        onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onCell(cell); } }}>
        <title>{label(cell.keys[0])} · {number(cell.values[measure])}</title>
      </circle>)}
    </svg>
    <div className="analysis-axis"><span>{label(ordered[0].keys[0])}</span><span>{label(ordered.at(-1)!.keys[0])}</span></div>
  </div>;
}
