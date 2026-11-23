import { useCallback, useEffect, useState } from "react";
import { CalendarClock, LoaderCircle, RefreshCw, Save, X } from "lucide-react";
import { api, encode } from "../../api";
import { Button, Input } from "../../shared/design-system/Controls";
import { apiTime } from "./dashboard-utils";
import { datasetPath, type Dataset, type RefreshJob, type RefreshSchedule } from "./model";

export function DashboardRefresh({ datasetId, onRefresh }: { datasetId: string; onRefresh(): void }) {
  const [available, setAvailable] = useState<boolean>();
  const [open, setOpen] = useState(false);
  const [schedule, setSchedule] = useState<RefreshSchedule>();
  const [history, setHistory] = useState<RefreshJob[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [job, setJob] = useState<RefreshJob>();
  const [enabled, setEnabled] = useState(false);
  const [interval, setInterval] = useState(60);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void api<Dataset>(datasetPath(datasetId)).then((dataset) => setAvailable(dataset.kind.toLowerCase() === "sql")).catch(() => setAvailable(false));
    }, 0);
    return () => window.clearTimeout(timer);
  }, [datasetId]);

  const load = useCallback(async () => {
    setBusy(true); setError("");
    try {
      const [current, page] = await Promise.all([
        api<RefreshSchedule>(`${datasetPath(datasetId)}/refresh-schedule`),
        api<{ items: RefreshJob[]; nextCursor: string | null }>(`${datasetPath(datasetId)}/refreshes`),
      ]);
      setSchedule(current); setEnabled(current.enabled); setInterval(current.intervalMinutes); setHistory(page.items); setCursor(page.nextCursor);
    } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  }, [datasetId]);

  if (available !== true) return null;

  function toggle() {
    const next = !open;
    setOpen(next);
    if (next && !schedule) void load();
  }

  async function saveSchedule() {
    if (!schedule) return;
    setBusy(true); setError("");
    try {
      const value = await api<RefreshSchedule>(`${datasetPath(datasetId)}/refresh-schedule`, {
        method: "PUT", body: encode({ expectedRevision: schedule.revision, enabled, intervalMinutes: interval }),
      });
      setSchedule(value); setEnabled(value.enabled); setInterval(value.intervalMinutes);
    } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  }

  async function run() {
    setBusy(true); setError("");
    try {
      const value = await api<RefreshJob>(`${datasetPath(datasetId)}/refresh`, {
        method: "POST", body: encode({ operationId: crypto.randomUUID().replaceAll("-", "") }),
      });
      setJob(value); setHistory((current) => [value, ...current.filter((item) => item.operationId !== value.operationId)]); onRefresh();
    } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  }

  async function more() {
    if (!cursor) return;
    try {
      const page = await api<{ items: RefreshJob[]; nextCursor: string | null }>(`${datasetPath(datasetId)}/refreshes?after=${encodeURIComponent(cursor)}`);
      setHistory((current) => [...current, ...page.items]); setCursor(page.nextCursor);
    } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
  }

  return <div className="refresh-panel">
    <Button className={open ? "active" : ""} onClick={toggle}><CalendarClock size={15} />数据更新</Button>
    {open && <div className="refresh-popover">
      <div className="dashboard-section-title"><h2>数据更新</h2><Button aria-label="关闭数据更新" onClick={() => setOpen(false)}><X size={15} /></Button></div>
      {error && <p className="analysis-alert" role="alert">{error}</p>}
      {busy && <p className="dashboard-muted"><LoaderCircle className="analysis-spin" size={14} />正在更新…</p>}
      <div className="refresh-form"><label className="dashboard-check"><Input type="checkbox" checked={enabled} onChange={(event) => setEnabled(event.target.checked)} />启用定时刷新</label><label>间隔（分钟）<Input type="number" min={1} max={1440} value={interval} onChange={(event) => setInterval(event.target.valueAsNumber || 1)} /></label><small>下次执行：{apiTime(schedule?.nextDueAt)}</small></div>
      <div className="dashboard-form-actions"><Button onClick={() => void run()} disabled={busy}>立即刷新<RefreshCw size={14} /></Button><Button className="analysis-primary" onClick={() => void saveSchedule()} disabled={busy || !schedule}><Save size={14} />保存计划</Button></div>
      {job && <div className="refresh-job"><strong>本次刷新：{job.state}</strong><small>开始 {apiTime(job.startedAt)}{job.finishedAt ? ` · 完成 ${apiTime(job.finishedAt)}` : ""}{job.releaseId ? ` · 版本 ${job.releaseId}` : ""}</small>{job.error && <span className="error">{job.error}</span>}</div>}
      <h3>最近刷新</h3>{history.length ? <div className="refresh-history">{history.map((item) => <div key={item.operationId}><span>{item.state}</span><small>{apiTime(item.startedAt)}{item.releaseId ? ` · ${item.releaseId}` : ""}</small></div>)}</div> : <p className="dashboard-muted">暂无刷新记录。</p>}
      {cursor && <Button onClick={() => void more()}>加载更多</Button>}
    </div>}
  </div>;
}
