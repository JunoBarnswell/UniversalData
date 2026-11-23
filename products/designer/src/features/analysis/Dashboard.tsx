import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import {
  BarChart3,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Download,
  ExternalLink,
  Filter,
  Gauge,
  LoaderCircle,
  Plus,
  RefreshCw,
  Share2,
  UserRound,
  UsersRound,
  X,
} from "lucide-react";
import { api, encode, request } from "../../api";
import { Button, Input, Select } from "../../shared/design-system/Controls";
import { ValuesTable } from "./AnalysisTable";
import { DashboardCardView } from "./DashboardCards";
import { DashboardEditor } from "./DashboardEditor";
import { DashboardRefresh } from "./DashboardRefresh";
import { DashboardSharing } from "./DashboardSharing";
import { apiTime, cellText } from "./dashboard-utils";
import {
  dashboardPath,
  datasetPath,
  type Cell,
  type Dashboard,
  type DashboardCard,
  type DashboardFilterState,
  type DashboardMember,
  type DashboardPage,
  type DashboardQueryResult,
  type Dataset,
  type Details,
  type Dimension,
  type Measure,
  type Filter as QueryFilter,
  type Release,
  type Rule,
  type Source,
} from "./model";

interface DashboardExperienceProps {
  canManage: boolean;
  onOpenLegacy(datasetId?: string): void;
}

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}

function monthFromApi(value: string | null | undefined): string {
  if (value && /^\d{4}-\d{2}/.test(value)) return value.slice(0, 7);
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

function nextMonth(value: string): string {
  const [year, month] = value.split("-").map(Number);
  if (!year || !month) return value;
  const next = new Date(Date.UTC(year, month, 1));
  return `${next.getUTCFullYear()}-${String(next.getUTCMonth() + 1).padStart(2, "0")}-01`;
}

function monthStart(value: string): string {
  return /^\d{4}-\d{2}$/.test(value) ? `${value}-01` : "";
}


function dashboardPathFor(id: string, view = true): string {
  return `/analysis/dashboard/${encodeURIComponent(id)}${view ? "" : "?edit=1"}`;
}

function dashboardDimensions(fields: Rule[]): Dimension[] {
  const dimensions: Dimension[] = [];
  for (const field of fields) {
    if (field.role === "date") {
      dimensions.push({ fieldId: field.fieldId, level: "month" }, { fieldId: field.fieldId, level: "day" });
    } else if (field.role === "category") {
      dimensions.push({ fieldId: field.fieldId, level: "value" });
    }
  }
  return dimensions;
}

function dashboardFilters(fields: Rule[], state: DashboardFilterState, crossFilters: QueryFilter[]): QueryFilter[] {
  const date = fields.find((field) => field.role === "date");
  const filters: QueryFilter[] = [];
  if (date && state.month) {
    filters.push({ fieldId: date.fieldId, operator: "from", value: monthStart(state.month) });
    filters.push({ fieldId: date.fieldId, operator: "before", value: nextMonth(state.month) });
  }
  if (state.categoryFieldId && state.categoryValue) {
    filters.push({ fieldId: state.categoryFieldId, operator: "eq", value: state.categoryValue });
  }
  return mergeFilterPrecedence(filters, crossFilters);
}

function mergeFilterPrecedence(base: QueryFilter[], overrides: QueryFilter[]): QueryFilter[] {
  const result = base.slice();
  const fields = [...new Set(overrides.map((filter) => filter.fieldId))];
  for (const fieldId of fields) {
    const group = overrides.filter((filter) => filter.fieldId === fieldId);
    const range = group.some((filter) => filter.operator === "from" || filter.operator === "before");
    for (const current of result.filter((item) => item.fieldId === fieldId)) {
      const currentRange = current.operator === "from" || current.operator === "before";
      if (range === currentRange) result.splice(result.indexOf(current), 1);
    }
    result.push(...group);
  }
  return uniqueFilters(result);
}

function starterCards(fields: Rule[], measures: Measure[]): DashboardCard[] {
  const dimensions = dashboardDimensions(fields);
  const date = dimensions.find((dimension) => dimension.level === "month");
  const category = dimensions.find((dimension) => dimension.level === "value");
  const first = measures[0]?.id ?? "";
  const second = measures[1]?.id ?? first;
  return [
    { id: crypto.randomUUID(), title: measures[0]?.label ?? "核心指标", chart: "kpi", measureId: first, dimensions: [], comparison: "none" },
    ...(date && first ? [{ id: crypto.randomUUID(), title: `${measures[0]?.label ?? "指标"}趋势`, chart: "line" as const, measureId: first, dimensions: [date], comparison: "mom" as const }] : []),
    ...(category && second ? [{ id: crypto.randomUUID(), title: `${measures[1]?.label ?? measures[0]?.label ?? "指标"}分类`, chart: "bar" as const, measureId: second, dimensions: [category], comparison: "none" as const }] : []),
  ];
}

function releaseMeasures(release: Release): Measure[] {
  if (release.measures.length) return release.measures;
  return (release.model.metrics ?? []).map((metric) => ({
    id: metric.id,
    label: metric.label,
    operation: metric.operation,
    fieldId: metric.fieldId ?? null,
    left: metric.left ?? null,
    right: metric.right ?? null,
    unit: metric.unit ?? null,
    calculation: metric.operation,
  }));
}

function cellFilters(fields: Rule[], dimensions: Dimension[], cell: Cell): QueryFilter[] {
  const filters: QueryFilter[] = [];
  dimensions.forEach((dimension, index) => {
    const value = cell.keys[index];
    const field = fields.find((item) => item.fieldId === dimension.fieldId);
    if (value === null || value === "未填写") {
      if (field?.role === "category") filters.push({ fieldId: dimension.fieldId, operator: "isNull" });
      return;
    }
    if (dimension.level === "value") {
      filters.push({ fieldId: dimension.fieldId, operator: "eq", value });
      return;
    }
    if (dimension.level === "month" || dimension.level === "day") {
      const month = String(value).slice(0, 7);
      if (dimension.level === "month" && monthStart(month)) {
        filters.push(
          { fieldId: dimension.fieldId, operator: "from", value: monthStart(month) },
          { fieldId: dimension.fieldId, operator: "before", value: nextMonth(month) },
        );
      } else if (dimension.level === "day") {
        const day = String(value).slice(0, 10);
        if (/^\d{4}-\d{2}-\d{2}$/.test(day)) {
          const end = new Date(`${day}T00:00:00Z`);
          end.setUTCDate(end.getUTCDate() + 1);
          filters.push({ fieldId: dimension.fieldId, operator: "from", value: day }, { fieldId: dimension.fieldId, operator: "before", value: end.toISOString().slice(0, 10) });
        }
      }
      return;
    }
    filters.push({ fieldId: dimension.fieldId, operator: "eq", value });
  });
  return filters.filter((filter) => fields.some((field) => field.fieldId === filter.fieldId));
}

function uniqueFilters(filters: QueryFilter[]): QueryFilter[] {
  const seen = new Set<string>();
  return filters.filter((filter) => {
    const key = `${filter.fieldId}/${filter.operator}/${filter.value ?? ""}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function DashboardExperience({ canManage, onOpenLegacy }: DashboardExperienceProps) {
  const [dashboards, setDashboards] = useState<Dashboard[]>([]);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [active, setActive] = useState<Dashboard>();
  const [creating, setCreating] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [tab, setTab] = useState<"mine" | "shared">(canManage ? "mine" : "shared");
  const navigation = useRef<AbortController | null>(null);

  const loadLibraries = useCallback(async (signal?: AbortSignal) => {
    const page = await api<DashboardPage>("/analysis/dashboards", { signal });
    setDashboards(page.items);
    setCursor(page.nextCursor);
    if (!canManage) return;
    const [availableDatasets, availableSources] = await Promise.all([
      api<Dataset[]>("/analysis/datasets", { signal }).catch(() => []),
      api<Source[]>("/analysis/sources", { signal }).catch(() => []),
    ]);
    setDatasets(availableDatasets);
    setSources(availableSources);
  }, [canManage]);

  useEffect(() => {
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      void loadLibraries(controller.signal).catch((reason) => {
        if (!controller.signal.aborted) setError(errorMessage(reason));
      }).finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    }, 0);
    const parts = window.location.pathname.split("/").filter(Boolean);
    const id = parts[1] === "dashboard" ? parts[2] : undefined;
    if (id) void openDashboard(id, controller.signal);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
      navigation.current?.abort();
    };
  }, [loadLibraries]);

  async function openDashboard(id: string, signal?: AbortSignal) {
    navigation.current?.abort();
    const controller = signal ? undefined : new AbortController();
    if (controller) navigation.current = controller;
    const activeSignal = signal ?? controller?.signal;
    setError("");
    setBusy("正在打开仪表盘…");
    try {
      const value = await api<Dashboard>(dashboardPath(id), { signal: activeSignal });
      setActive(value);
      setCreating(false);
      window.history.replaceState(null, "", dashboardPathFor(value.id));
    } catch (reason) {
      if (!activeSignal?.aborted) setError(errorMessage(reason));
    } finally {
      if (!activeSignal?.aborted) setBusy("");
    }
  }

  async function loadMore() {
    if (!cursor || busy) return;
    setBusy("正在读取更多仪表盘…");
    try {
      const page = await api<DashboardPage>(`/analysis/dashboards?after=${encodeURIComponent(cursor)}`);
      setDashboards((current) => [...current, ...page.items]);
      setCursor(page.nextCursor);
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setBusy("");
    }
  }

  function closeDashboard() {
    setActive(undefined);
    setCreating(false);
    window.history.replaceState(null, "", "/analysis");
  }

  function rememberDashboard(value: Dashboard) {
    setActive(value);
    setDashboards((current) => [value, ...current.filter((item) => item.id !== value.id)]);
  }

  async function createDashboard(name: string, datasetId: string) {
    setBusy("正在创建仪表盘…");
    setError("");
    try {
      const dataset = await api<Dataset>(datasetPath(datasetId));
      if (!dataset.publishedReleaseId) throw new Error("该数据集还没有已发布版本，请先完成数据建模和发布。");
      const release = await api<Release>(`${datasetPath(datasetId)}/releases/${encodeURIComponent(dataset.publishedReleaseId)}`);
      const value = await api<Dashboard>("/analysis/dashboards", {
        method: "POST",
        body: encode({ name: name.trim(), datasetId, cards: starterCards(release.model.fields, releaseMeasures(release)) }),
      });
      rememberDashboard(value);
      setCreating(false);
      window.history.replaceState(null, "", dashboardPathFor(value.id, false));
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setBusy("");
    }
  }

  const visibleDashboards = dashboards.filter((item) => tab === "mine" ? item.owner : !item.owner);
  const sqlSources = sources.filter((source) => source.kind.toLowerCase() === "sql");
  return <main className="analysis-page dashboard-page">
    {active ? <DashboardWorkspace dashboard={active} onChanged={rememberDashboard} onBack={closeDashboard} onOpenLegacy={onOpenLegacy} /> :
      <DashboardHome
        dashboards={visibleDashboards}
        datasets={datasets}
        sqlSources={sqlSources.length ? sqlSources : sources}
        canManage={canManage}
        tab={tab}
        loading={loading}
        busy={busy}
        error={error}
        creating={creating}
        onTab={setTab}
        onCreate={() => setCreating(true)}
        onCancelCreate={() => setCreating(false)}
        onOpen={(id) => void openDashboard(id)}
        onOpenLegacy={onOpenLegacy}
        onCreateDashboard={(name, datasetId) => void createDashboard(name, datasetId)}
        onLoadMore={() => void loadMore()}
        hasMore={!!cursor}
      />}
  </main>;
}

function DashboardHome({
  dashboards, datasets, sqlSources, canManage, tab, loading, busy, error, creating, onTab, onCreate, onCancelCreate,
  onOpen, onOpenLegacy, onCreateDashboard, onLoadMore, hasMore,
}: {
  dashboards: Dashboard[]; datasets: Dataset[]; sqlSources: Source[]; canManage: boolean; tab: "mine" | "shared";
  loading: boolean; busy: string; error: string; creating: boolean;
  onTab(tab: "mine" | "shared"): void; onCreate(): void; onCancelCreate(): void; onOpen(id: string): void;
  onOpenLegacy(datasetId?: string): void; onCreateDashboard(name: string, datasetId: string): void;
  onLoadMore(): void; hasMore: boolean;
}) {
  return <div className="dashboard-home">
    <header className="dashboard-home-heading">
      <div>
        <p className="dashboard-eyebrow">BI 工作台</p>
        <h1>数据分析</h1>
        <p className="dashboard-lede">把已发布的数据集组合成团队都能读懂的仪表盘，指标、趋势和明细在同一个版本里保持一致。</p>
      </div>
      {canManage && <Button className="analysis-primary" onClick={onCreate} disabled={creating}><Plus size={16} />新建仪表盘</Button>}
    </header>
    {error && <div className="analysis-alert" role="alert">{error}</div>}
    {busy && <div className="analysis-progress" role="status"><LoaderCircle className="analysis-spin" size={15} />{busy}</div>}
    {creating && <CreateDashboard datasets={datasets} onCancel={onCancelCreate} onCreate={onCreateDashboard} />}
    <section className="dashboard-library" aria-label="仪表盘库">
      <div className="dashboard-tabs" role="tablist" aria-label="仪表盘分类">
        <Button role="tab" aria-selected={tab === "mine"} className={tab === "mine" ? "active" : ""} onClick={() => onTab("mine")}>我的仪表盘</Button>
        <Button role="tab" aria-selected={tab === "shared"} className={tab === "shared" ? "active" : ""} onClick={() => onTab("shared")}>与我共享</Button>
      </div>
      {loading ? <div className="dashboard-empty"><LoaderCircle className="analysis-spin" size={18} />正在加载仪表盘…</div> : dashboards.length ?
        <div className="dashboard-grid">
          {dashboards.map((item) => <DashboardLibraryCard key={item.id} dashboard={item} onOpen={() => onOpen(item.id)} />)}
        </div> : <div className="dashboard-empty">
          <Gauge size={28} />
          <strong>{tab === "mine" ? "还没有仪表盘" : "还没有共享给你的仪表盘"}</strong>
          <span>{tab === "mine" ? "从已发布数据集开始，创建第一张团队报表。" : "拥有者共享仪表盘后，会显示在这里。"}</span>
          {tab === "mine" && canManage && <Button onClick={onCreate}><Plus size={15} />创建第一张仪表盘</Button>}
        </div>}
      {hasMore && <div className="dashboard-load-more"><Button onClick={onLoadMore}>加载更多<ChevronDown size={15} /></Button></div>}
    </section>
    {canManage && <section className="dashboard-source-summary">
      <div className="dashboard-section-title"><div><h2>已发布数据集</h2><p>仪表盘只能基于已发布版本，数据刷新不会改变当前已打开的版本。</p></div><Button onClick={() => onOpenLegacy()}><ExternalLink size={15} />管理数据集</Button></div>
      <div className="dashboard-source-list">
        {datasets.length ? datasets.map((dataset) => <div className="dashboard-source-row" key={dataset.id}>
          <div><strong>{dataset.name}</strong><small>{dataset.sourceName} · {dataset.kind === "sql" ? "SQL 数据源" : dataset.kind.toUpperCase()} · {dataset.publishedReleaseId ? `版本 ${dataset.publishedReleaseId}` : "未发布"}</small></div>
          {dataset.publishedReleaseId ? <Button onClick={() => onCreateDashboard(dataset.name, dataset.id)} disabled={creating}><Plus size={14} />以此创建</Button> : <span className="dashboard-muted">先发布数据</span>}
        </div>) : <div className="dashboard-empty compact"><span>还没有数据集。</span><Button onClick={() => onOpenLegacy()}><Plus size={14} />新建数据集</Button></div>}
      </div>
      {sqlSources.length > 0 && <p className="dashboard-source-note">当前可用 SQL 来源：{sqlSources.map((source) => source.name).join("、")}</p>}
    </section>}
  </div>;
}

function DashboardLibraryCard({ dashboard, onOpen }: { dashboard: Dashboard; onOpen(): void }) {
  return <article className="dashboard-library-card">
    <div className="dashboard-card-icon"><BarChart3 size={20} /></div>
    <div className="dashboard-card-copy"><h3>{dashboard.name}</h3><p>{dashboard.owner ? "我创建的" : "与我共享"} · {dashboard.cards.length} 个组件</p>
      <small>{dashboard.dataAsOf ? `数据截至 ${apiTime(dashboard.dataAsOf)}` : "尚未发布数据"}{dashboard.owner && dashboard.publishedRevision ? ` · 发布版本 ${dashboard.publishedRevision}` : ""}</small></div>
    <Button aria-label={`打开${dashboard.name}`} onClick={onOpen}><ChevronRight size={17} /></Button>
  </article>;
}

function CreateDashboard({ datasets, onCancel, onCreate }: { datasets: Dataset[]; onCancel(): void; onCreate(name: string, datasetId: string): void }) {
  const published = datasets.filter((dataset) => !!dataset.publishedReleaseId);
  const [name, setName] = useState("");
  const [datasetId, setDatasetId] = useState(published[0]?.id ?? "");
  const [error, setError] = useState("");
  function submit(event: FormEvent) {
    event.preventDefault();
    if (!name.trim()) { setError("请填写仪表盘名称。"); return; }
    if (!datasetId) { setError("请选择一个已发布数据集。"); return; }
    onCreate(name.trim(), datasetId);
  }
  return <section className="dashboard-create-card" aria-label="新建仪表盘">
    <div className="dashboard-section-title"><div><h2>新建仪表盘</h2><p>选择一个已发布数据集，系统会先放入可编辑的指标卡、趋势和分类组件。</p></div><Button aria-label="关闭新建仪表盘" onClick={onCancel}><X size={17} /></Button></div>
    {error && <p className="analysis-alert" role="alert">{error}</p>}
    <form className="dashboard-create-form" onSubmit={submit}>
      <label>仪表盘名称<Input value={name} maxLength={128} placeholder="例如：经营周报" onChange={(event) => setName(event.target.value)} /></label>
      <label>数据集<Select value={datasetId} onChange={(event) => setDatasetId(event.target.value)}>
        <option value="">选择已发布数据集</option>{published.map((dataset) => <option key={dataset.id} value={dataset.id}>{dataset.name} · {dataset.sourceName}</option>)}
      </Select></label>
      <div className="dashboard-form-actions"><Button type="button" onClick={onCancel}>取消</Button><Button className="analysis-primary" type="submit"><Plus size={15} />创建并编辑</Button></div>
    </form>
    {!published.length && <p className="dashboard-muted">暂无已发布数据集。请先在“管理数据集”中完成建模和发布。</p>}
  </section>;
}

function DashboardWorkspace({ dashboard: initial, onChanged, onBack, onOpenLegacy }: {
  dashboard: Dashboard; onChanged(value: Dashboard): void; onBack(): void; onOpenLegacy(datasetId?: string): void;
}) {
  const [dashboard, setDashboard] = useState(initial);
  const [editing, setEditing] = useState(!initial.publishedRevision && initial.owner);
  const [sharing, setSharing] = useState(false);
  const [query, setQuery] = useState<DashboardQueryResult>();
  const [queryRevision, setQueryRevision] = useState(initial.publishedRevision ?? initial.revision);
  const [queryRelease, setQueryRelease] = useState(initial.releaseId);
  const [previewingDraft, setPreviewingDraft] = useState(false);
  const [filters, setFilters] = useState<DashboardFilterState>(() => ({ month: monthFromApi(initial.dataAsOf), categoryFieldId: "", categoryValue: "" }));
  const [appliedFilters, setAppliedFilters] = useState<DashboardFilterState>(filters);
  const [crossFilters, setCrossFilters] = useState<QueryFilter[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [pendingVersion, setPendingVersion] = useState<string | null>(null);
  const [pendingDashboard, setPendingDashboard] = useState<Dashboard>();
  const [details, setDetails] = useState<Details>();
  const [selectedCell, setSelectedCell] = useState<(string | null)[] | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const requestRef = useRef<AbortController | null>(null);

  const refreshDashboard = useCallback((value: Dashboard) => {
    setDashboard(value);
    onChanged(value);
  }, [onChanged]);

  function pinUpdatedRelease(value: Dashboard): boolean {
    const changed = !!queryRelease && !!value.releaseId && value.releaseId !== queryRelease;
    if (changed) setPendingDashboard(value);
    return changed;
  }

  const loadQuery = useCallback(async (state: DashboardFilterState, cross: QueryFilter[], revision: string, releaseId: string | null, fields = dashboard.fields) => {
    if (!revision || !releaseId) return;
    requestRef.current?.abort();
    const controller = new AbortController();
    requestRef.current = controller;
    setLoading(true);
    setError("");
    try {
      const result = await api<DashboardQueryResult>(`${dashboardPath(dashboard.id)}/query`, {
        method: "POST", signal: controller.signal,
        body: encode({ revision, releaseId, filters: uniqueFilters(dashboardFilters(fields, state, cross)) }),
      });
      setQuery(result);
      setQueryRevision(result.revision);
      setQueryRelease(result.releaseId);
    } catch (reason) {
      if (!controller.signal.aborted) setError(errorMessage(reason));
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [dashboard.id, dashboard.fields]);

  useEffect(() => {
    if (dashboard.publishedRevision && dashboard.releaseId) {
      const timer = window.setTimeout(() => void loadQuery(appliedFilters, crossFilters, queryRevision, queryRelease), 0);
      return () => window.clearTimeout(timer);
    }
    return () => requestRef.current?.abort();
    // The initial dashboard is intentionally pinned. Draft/release changes need an explicit version switch.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dashboard.id]);

  async function saveDraft(name: string, cards: DashboardCard[]): Promise<Dashboard | undefined> {
    setSaving(true); setError(""); setNotice("");
    try {
      const value = await api<Dashboard>(`${dashboardPath(dashboard.id)}/draft`, {
        method: "PUT", body: encode({ expectedRevision: dashboard.revision, name, cards }),
      });
      const releaseChanged = pinUpdatedRelease(value);
      refreshDashboard(value);
      setNotice(releaseChanged
        ? "草稿已保存。数据集已有新版本，当前查询仍固定在旧版本。"
        : "草稿已保存。预览数据仍固定在当前发布版本，点击“切换到新版本”后才会重新计算。 ");
      return value;
    } catch (reason) { setError(errorMessage(reason)); }
    finally { setSaving(false); }
    return undefined;
  }

  async function publish() {
    setSaving(true); setError(""); setNotice("");
    try {
      const value = await api<Dashboard>(`${dashboardPath(dashboard.id)}/publish`, {
        method: "POST", body: encode({ expectedRevision: dashboard.revision }),
      });
      const oldRelease = dashboard.publishedRevision;
      const releaseChanged = pinUpdatedRelease(value);
      refreshDashboard(value);
      setPendingDashboard(undefined);
      if (releaseChanged) setPendingDashboard(value);
      setPreviewingDraft(false);
      if (value.publishedRevision && value.publishedRevision !== oldRelease) {
        setPendingVersion(value.publishedRevision);
        setNotice(oldRelease ? "已发布新版本。当前页面仍显示旧版本数据。" : "已发布。点击“加载已发布版本”查看数据。 ");
      }
    } catch (reason) { setError(errorMessage(reason)); }
    finally { setSaving(false); }
  }

  async function saveMembers(members: DashboardMember[]) {
    setSaving(true); setError("");
    try {
      const value = await api<Dashboard>(`${dashboardPath(dashboard.id)}/members`, {
        method: "PUT", body: encode({ expectedRevision: dashboard.revision, members }),
      });
      const releaseChanged = pinUpdatedRelease(value);
      refreshDashboard(value);
      setSharing(false);
      setNotice(releaseChanged
        ? "共享权限已更新。数据集已有新版本，当前查询仍固定在旧版本。"
        : "共享权限已更新。已打开的查看会话不会被切换版本。 ");
    } catch (reason) { setError(errorMessage(reason)); }
    finally { setSaving(false); }
  }

  function switchVersion() {
    const latest = pendingDashboard ?? dashboard;
    if (!latest.publishedRevision || !latest.releaseId) return;
    if (pendingDashboard) refreshDashboard(latest);
    setPendingVersion(null);
    setPendingDashboard(undefined);
    setPreviewingDraft(false);
    setCrossFilters([]);
    setSelectedCell(null);
    setDetails(undefined);
    setQueryRevision(latest.publishedRevision);
    setQueryRelease(latest.releaseId);
    void loadQuery(appliedFilters, [], latest.publishedRevision, latest.releaseId, latest.fields);
  }

  async function checkDashboardVersion() {
    try {
      const latest = await api<Dashboard>(dashboardPath(dashboard.id));
      if (latest.releaseId !== dashboard.releaseId || latest.publishedRevision !== dashboard.publishedRevision) {
        setPendingDashboard(latest);
        setNotice(`检测到新的数据版本 ${latest.releaseId ?? "未知"}。当前页面仍固定显示 ${queryRelease ?? "当前"}。`);
      } else {
        setNotice("当前已是最新的已发布数据版本。 ");
      }
    } catch (reason) {
      setError(errorMessage(reason));
    }
  }

  async function previewDraft(name: string, cards: DashboardCard[]) {
    if (!dashboard.owner || !dashboard.releaseId) return;
    const value = await saveDraft(name, cards);
    if (!value?.releaseId) return;
    setPreviewingDraft(true);
    setPendingVersion(null);
    setCrossFilters([]);
    setDetails(undefined);
    setNotice("正在预览草稿。保存或发布不会自动替换已打开的发布版本。 ");
    setQueryRevision(value.revision);
    setQueryRelease(value.releaseId);
    void loadQuery(appliedFilters, [], value.revision, value.releaseId, value.fields);
  }

  function applyFilters(event?: FormEvent) {
    event?.preventDefault();
    setAppliedFilters(filters);
    setCrossFilters([]);
    setSelectedCell(null);
    setDetails(undefined);
    void loadQuery(filters, [], queryRevision, queryRelease);
  }

  function crossFilter(card: DashboardCard, cell: Cell) {
    const result = query?.cards.find((item) => item.id === card.id);
    const group = result?.current.groups.at(-1);
    if (!group) return;
    const next = mergeFilterPrecedence(crossFilters, cellFilters(dashboard.fields, group.dimensions, cell));
    setCrossFilters(next);
    void loadQuery(appliedFilters, next, queryRevision, queryRelease);
  }

  async function drill(context: string, cell: (string | null)[] | null, cursor?: string | null) {
    if (!dashboard.details) return;
    setSelectedCell(cell);
    setDetailError("");
    if (!cursor) setDetails(undefined);
    setDetailLoading(true);
    try {
      setDetails(await api<Details>(`${dashboardPath(dashboard.id)}/drill`, {
        method: "POST", body: encode({ context, cell, cursor: cursor ?? null }),
      }));
    } catch (reason) { setDetailError(errorMessage(reason)); }
    finally { setDetailLoading(false); }
  }

  async function download(detailsOnly = false) {
    if (!query || !dashboard.export) return;
    const card = query.cards[0];
    if (!card) return;
    try {
      const response = await request(`${dashboardPath(dashboard.id)}/export`, {
        method: "POST", body: encode({ context: card.current.context, cell: detailsOnly ? selectedCell : null, details: detailsOnly }),
      });
      const url = URL.createObjectURL(await response.blob());
      const link = document.createElement("a");
      link.href = url;
      link.download = `${dashboard.name}${detailsOnly ? "-明细" : "-汇总"}.csv`;
      link.click();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (reason) { setError(errorMessage(reason)); }
  }

  function clearCrossFilters() {
    setCrossFilters([]);
    void loadQuery(appliedFilters, [], queryRevision, queryRelease);
  }

  const queryCards = useMemo(() => new Map((query?.cards ?? []).map((card) => [card.id, card])), [query]);
  const displayedDataAsOf = query?.dataAsOf ?? dashboard.dataAsOf;
  return <div className="dashboard-workspace">
    <header className="dashboard-workspace-heading">
      <Button className="dashboard-back" onClick={onBack}><ChevronLeft size={16} />仪表盘</Button>
      <div className="dashboard-title"><h1>{dashboard.name}</h1><p>{dashboard.owner ? <><UserRound size={13} />我创建的</> : <><UsersRound size={13} />与我共享</>} · {displayedDataAsOf ? `数据截至 ${apiTime(displayedDataAsOf)}` : "尚未发布"}</p></div>
      <div className="dashboard-heading-actions">
        {dashboard.owner && <Button onClick={() => setEditing((value) => !value)} className={editing ? "active" : ""}><Gauge size={15} />{editing ? "关闭编辑" : "编辑仪表盘"}</Button>}
        {dashboard.owner && <Button onClick={() => setSharing((value) => !value)} className={sharing ? "active" : ""}><Share2 size={15} />共享设置</Button>}
        <Button onClick={() => void checkDashboardVersion()}><RefreshCw size={15} />检查版本</Button>
        {dashboard.owner && <DashboardRefresh datasetId={dashboard.datasetId} onRefresh={() => void checkDashboardVersion()} />}
        {dashboard.owner && <Button onClick={() => onOpenLegacy(dashboard.datasetId)}><ExternalLink size={15} />数据集</Button>}
      </div>
    </header>
    {error && <div className="analysis-alert" role="alert">{error}</div>}
    {notice && <div className="analysis-progress" role="status">{notice}</div>}
    {pendingVersion && <div className="dashboard-version-banner" role="status"><span>有新的已发布版本（{pendingVersion}），当前仍固定显示版本 {queryRevision}。</span><Button onClick={switchVersion}>加载已发布版本<RefreshCw size={14} /></Button></div>}
    {pendingDashboard && <div className="dashboard-version-banner" role="status"><span>检测到数据版本更新为 {pendingDashboard.releaseId ?? "未知"}，当前仍固定显示版本 {queryRelease ?? "当前"}。</span><Button onClick={switchVersion}>加载最新数据<RefreshCw size={14} /></Button></div>}
    {previewingDraft && <div className="dashboard-version-banner" role="status"><span>当前正在预览草稿版本 {queryRevision}，团队成员仍只能看到已发布版本。</span><Button onClick={switchVersion}>返回已发布版本<RefreshCw size={14} /></Button></div>}
    {editing && dashboard.owner && <DashboardEditor key={`${dashboard.id}/${dashboard.revision}`} dashboard={dashboard} saving={saving} onSave={saveDraft} onPublish={publish} onPreview={previewDraft} />}
    {sharing && dashboard.owner && <DashboardSharing key={`${dashboard.id}/${dashboard.revision}`} dashboard={dashboard} saving={saving} onSave={saveMembers} />}
    {(!dashboard.releaseId || (!dashboard.publishedRevision && !previewingDraft)) ? <div className="dashboard-empty dashboard-no-release"><Gauge size={30} /><strong>先发布仪表盘，再查看数据</strong><span>编辑卡片并保存草稿后，发布一个版本，团队成员才能看到稳定的数据快照。</span>{dashboard.owner && !editing && <Button className="analysis-primary" onClick={() => setEditing(true)}>打开编辑器</Button>}</div> : <>
      <DashboardFilters fields={dashboard.fields} values={filters} crossFilters={crossFilters} onChange={setFilters} onSubmit={applyFilters} onClearCross={clearCrossFilters} />
      {loading && <div className="analysis-progress" role="status"><LoaderCircle className="analysis-spin" size={15} />正在计算版本 {queryRevision}…</div>}
      {query && <section className="dashboard-cards" aria-label="仪表盘内容" aria-busy={loading} data-query-context={query.cards[0]?.current.context}>
        {dashboard.cards.map((card) => <DashboardCardView key={card.id} card={card} result={queryCards.get(card.id)} fields={dashboard.fields} allowDetails={dashboard.details} onCell={(cell) => { crossFilter(card, cell); const result = queryCards.get(card.id); const group = result?.current.groups.at(-1); if (dashboard.details && result && group) void drill(result.current.context, cell.keys); }} onDrill={(cell) => { const result = queryCards.get(card.id); if (dashboard.details && result) void drill(result.current.context, cell); }} />)}
      </section>}
      {!query && !loading && <div className="dashboard-empty compact"><span>当前版本还没有可显示的数据。</span></div>}
      {query && <section className="dashboard-result-footer"><span>查询版本 {query.revision} · 数据截至 {apiTime(query.dataAsOf)}</span><div><Button disabled={!dashboard.export} onClick={() => void download(false)}><Download size={14} />导出汇总</Button>{!dashboard.export && <small>当前账号没有导出权限</small>}</div></section>}
      {details && <DetailPanel details={details} selectedCell={selectedCell} loading={detailLoading} error={detailError} allowExport={dashboard.export} onClose={() => { setDetails(undefined); setDetailError(""); }} onNext={(cursor) => { const card = query?.cards[0]; if (card) void drill(card.current.context, selectedCell, cursor); }} onExport={() => void download(true)} />}
      {!details && detailLoading && <div className="analysis-panel dashboard-detail-loading" role="status"><LoaderCircle className="analysis-spin" size={16} />正在读取明细…</div>}
      {detailError && !details && <div className="analysis-alert" role="alert">{detailError}</div>}
    </>}
  </div>;
}

function DashboardFilters({ fields, values, crossFilters, onChange, onSubmit, onClearCross }: {
  fields: Rule[]; values: DashboardFilterState; crossFilters: QueryFilter[];
  onChange(value: DashboardFilterState): void; onSubmit(event: FormEvent): void; onClearCross(): void;
}) {
  const date = fields.find((field) => field.role === "date");
  const categories = fields.filter((field) => field.role === "category");
  return <form className="dashboard-filters" onSubmit={onSubmit}>
    <div className="dashboard-filter-intro"><Filter size={16} /><div><strong>筛选数据</strong><small>{date ? "按完整自然月计算，系统会同时提交开始和结束日期。" : "选择分类后应用到所有组件。"}</small></div></div>
    {date && <label>月份<Input type="month" value={values.month} onChange={(event) => onChange({ ...values, month: event.target.value })} /></label>}
    {categories.length > 0 && <label>分类字段<Select value={values.categoryFieldId} onChange={(event) => onChange({ ...values, categoryFieldId: event.target.value, categoryValue: "" })}>
      <option value="">所有分类</option>{categories.map((field) => <option key={field.fieldId} value={field.fieldId}>{field.label}</option>)}
    </Select></label>}
    {values.categoryFieldId && <label>分类值<Input value={values.categoryValue} placeholder="输入完整分类名称" onChange={(event) => onChange({ ...values, categoryValue: event.target.value })} /></label>}
    <Button className="analysis-primary" type="submit"><Filter size={14} />应用筛选</Button>
    {crossFilters.length > 0 && <div className="dashboard-cross-filter"><span>已交叉筛选 {crossFilters.length} 项</span><Button type="button" onClick={onClearCross}>清除</Button></div>}
  </form>;
}

function DetailPanel({ details, selectedCell, loading, error, allowExport, onClose, onNext, onExport }: {
  details: Details; selectedCell: (string | null)[] | null; loading: boolean; error: string; allowExport: boolean;
  onClose(): void; onNext(cursor: string): void; onExport(): void;
}) {
  return <section className="analysis-panel dashboard-detail-panel" aria-label="明细"><div className="analysis-section-heading"><div><h2>明细记录</h2><small>{selectedCell?.map(cellText).join(" / ") || "当前筛选范围"}</small></div><div className="dashboard-heading-actions"><Button disabled={!allowExport} onClick={onExport}><Download size={14} />导出明细</Button><Button aria-label="关闭明细" onClick={onClose}><X size={15} /></Button></div></div>{error && <p className="analysis-alert" role="alert">{error}</p>}{loading && <p role="status">正在读取明细…</p>}<ValuesTable fields={details.fields.map((field) => ({ fieldId: field.fieldId, label: field.label }))} rows={details.rows} />{details.nextCursor && <Button disabled={loading} onClick={() => onNext(details.nextCursor!)}>下一页<ChevronRight size={14} /></Button>}</section>;
}
