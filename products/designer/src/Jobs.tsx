import { useEffect, useState } from "react";
import { Plus, Save } from "lucide-react";
import { api } from "./api";
import { PROTOCOL_VERSION } from "./version";
import type { Deployment } from "./model";
import { Deployments } from "./Deployments";

const frequencies = { Once: "一次", Interval: "固定间隔", Daily: "每天", Weekly: "每周", Monthly: "每月" };
type Frequency = keyof typeof frequencies;
interface Schedule {
  id: string;
  frequency: Frequency;
  startAt: string;
  enabled: boolean;
  intervalSeconds?: number;
  timeZone?: string;
  localTime?: string;
  weekdays?: number[];
  dayOfMonth?: number;
}
interface Cursor { schedule: Schedule; nextDueMs?: number | null }
interface Job {
  version: number;
  id: string;
  packageId: string;
  packageRevision: string;
  parameters?: string;
  parametersReference?: string;
  enabled: boolean;
  schedules: Cursor[];
  maxConcurrency: number;
  maxAttempts: number;
  retryDelaySeconds: number;
  deploymentId?: string;
  deploymentRevision?: string;
}
interface Occurrence {
  id: string;
  timestampMs: number;
  state: string;
  packageRevision: string;
  runId?: string;
}
const newSchedule = (): Schedule => ({
  id: crypto.randomUUID(), frequency: "Interval", startAt: new Date().toISOString(),
  enabled: true, intervalSeconds: 3600, timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  localTime: "09:00:00", weekdays: [1], dayOfMonth: 1,
});
const path = (job: Job) => `/jobs/${encodeURIComponent(job.id)}`;
const instant = (ms: number) => new Date(ms).toLocaleString();

export function Jobs() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [deployments, setDeployments] = useState<Deployment[]>([]);
  const [editing, setEditing] = useState<Job>();
  const [error, setError] = useState("");
  const [history, setHistory] = useState<{ job: Job; items: Occurrence[] }>();
  const [preview, setPreview] = useState<{ id: string; times: number[] }>();

  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    Promise.all([api<Job[]>("/jobs", { signal }), api<Deployment[]>("/deployments", { signal })])
      .then(([loaded, published]) => { setJobs(loaded); setDeployments(published); })
      .catch((failure) => { if (!signal.aborted) setError(String(failure)); });
    return () => controller.abort();
  }, []);

  async function save(job: Job) {
    try {
      const stored = await api<Job>(path(job), { method: "PUT",
        body: JSON.stringify({ ...job, schedules: job.schedules.map((item) => item.schedule) }),
      });
      setJobs((all) => [...all.filter((item) => item.id !== stored.id), stored]);
      setEditing(undefined);
      setError("");
    } catch (failure) { setError(String(failure)); }
  }

  async function loadHistory(job: Job) {
    try { setHistory({ job, items: await api<Occurrence[]>(path(job) + "/occurrences") }); }
    catch (failure) { setError(String(failure)); }
  }

  async function run(job: Job, occurrenceId?: string) {
    try {
      await api<Occurrence>(path(job) + "/runs", { method: "POST",
        body: JSON.stringify({ requestId: crypto.randomUUID(), occurrenceId }),
      });
      await loadHistory(job);
    } catch (failure) { setError(String(failure)); }
  }

  async function showPreview(job: Job, schedule: Schedule) {
    try {
      const times = await api<number[]>(path(job) + "/schedule-preview",
        { method: "POST", body: JSON.stringify(schedule) });
      setPreview({ id: schedule.id, times });
    } catch (failure) { setError(String(failure)); }
  }

  function changeSchedule(id: string, update: Partial<Schedule>) {
    setEditing((job) => job && ({ ...job, schedules: job.schedules.map((item) =>
      item.schedule.id === id ? { ...item, schedule: { ...item.schedule, ...update } } : item) }));
  }

  return (
    <section className="page">
      <div className="page-heading">
        <h1>作业管理</h1>
        <button className="primary" onClick={() => setEditing({
          version: PROTOCOL_VERSION, id: crypto.randomUUID(), packageId: "", packageRevision: "1",
          enabled: true, schedules: [{ schedule: newSchedule() }], maxConcurrency: 1,
          maxAttempts: 1, retryDelaySeconds: 30,
        })}><Plus size={16} />新建作业</button>
      </div>
      {error && <p role="alert" className="error">{error}</p>}
      <div className="table-panel">
        <table><thead><tr><th>作业</th><th>包版本</th><th>下次触发</th><th>状态</th><th>操作</th></tr></thead>
          <tbody>{jobs.map((job) => {
            const times = job.schedules.flatMap((item) =>
              item.schedule.enabled && item.nextDueMs != null ? [item.nextDueMs] : []);
            return <tr key={job.id}><td>{job.id}</td><td>{job.packageId} v{job.packageRevision}</td>
              <td>{job.enabled && times.length ? instant(Math.min(...times)) : "—"}</td>
              <td>{job.enabled ? "已启用" : "已暂停"}</td><td>
                <button onClick={() => setEditing(job)}>编辑</button>
                <button onClick={() => void save({ ...job, enabled: !job.enabled })}>{job.enabled ? "暂停" : "启用"}</button>
                <button onClick={() => void run(job)}>立即运行</button>
                <button onClick={() => void loadHistory(job)}>历史</button>
              </td></tr>;
          })}</tbody>
        </table>
        {!jobs.length && <div className="empty-state">暂无作业</div>}
      </div>
      {editing && <div className="modal-backdrop"><section className="modal" aria-label="作业配置">
        <h2>作业配置</h2>
        <label>作业名称<input value={editing.id} onChange={(event) =>
          setEditing({ ...editing, id: event.target.value })} /></label>
        <label>已发布版本<select aria-label="作业发布版本" value={editing.deploymentId ?? ""}
          onChange={(event) => {
            const selected = deployments.find((item) => item.id === event.target.value);
            if (selected) setEditing({ ...editing, deploymentId: selected.id,
              packageId: selected.packageId, packageRevision: selected.packageRevision,
              deploymentRevision: selected.deploymentRevision,
            });
          }}>
          <option value="">选择已发布任务</option>
          {deployments.filter((item) => item.enabled).map((item) =>
            <option key={item.id} value={item.id}>{item.packageId} · v{item.packageRevision}</option>)}
        </select></label>
        <label>并发上限<input type="number" min="1" value={editing.maxConcurrency}
          onChange={(event) => setEditing({ ...editing, maxConcurrency: Number(event.target.value) })} /></label>
        {editing.schedules.map(({ schedule }) =>
          <fieldset key={schedule.id}>
            <label>日程类型<select value={schedule.frequency} onChange={(event) =>
              changeSchedule(schedule.id, { frequency: event.target.value as Frequency })}>
              {Object.entries(frequencies).map(([key, value]) => <option key={key} value={key}>{value}</option>)}
            </select></label>
            <label>起点<input type="datetime-local" value={new Date(Date.parse(schedule.startAt) -
              new Date(schedule.startAt).getTimezoneOffset() * 60000).toISOString().slice(0, 16)}
              onChange={(event) => { if (event.target.value) changeSchedule(schedule.id,
                { startAt: new Date(event.target.value).toISOString() }); }} /></label>
            {schedule.frequency === "Interval" && <label>间隔（秒）<input type="number" min="1"
              value={schedule.intervalSeconds ?? 3600} onChange={(event) =>
                changeSchedule(schedule.id, { intervalSeconds: Number(event.target.value) })} /></label>}
            {schedule.frequency !== "Once" && schedule.frequency !== "Interval" && <>
              <label>时区<input value={schedule.timeZone ?? "UTC"} onChange={(event) =>
                changeSchedule(schedule.id, { timeZone: event.target.value })} /></label>
              <label>当地时间<input type="time" value={schedule.localTime?.slice(0, 5) ?? "09:00"}
                onChange={(event) => changeSchedule(schedule.id, { localTime: event.target.value + ":00" })} /></label>
            </>}
            {schedule.frequency === "Weekly" && <div>{["日", "一", "二", "三", "四", "五", "六"].map((day, index) =>
              <label key={day}><input type="checkbox" checked={schedule.weekdays?.includes(index) ?? false}
                onChange={(event) => changeSchedule(schedule.id, { weekdays: event.target.checked
                  ? [...(schedule.weekdays ?? []), index] : (schedule.weekdays ?? []).filter((value) => value !== index) })} />{day}</label>)}</div>}
            {schedule.frequency === "Monthly" && <label>日期<input type="number" min="1" max="31"
              value={schedule.dayOfMonth ?? 1} onChange={(event) =>
                changeSchedule(schedule.id, { dayOfMonth: Number(event.target.value) })} /></label>}
            <label>启用<input type="checkbox" checked={schedule.enabled}
              onChange={(event) => changeSchedule(schedule.id, { enabled: event.target.checked })} /></label>
            <button onClick={() => void showPreview(editing, schedule)}>预览触发时间</button>
            <button onClick={() => setEditing({ ...editing,
              schedules: editing.schedules.filter((item) => item.schedule.id !== schedule.id) })}>删除日程</button>
            {preview?.id === schedule.id && (preview.times.length
              ? <ul>{preview.times.map((time) => <li key={time}>{instant(time)}</li>)}</ul> : <p>没有后续触发</p>)}
          </fieldset>)}
        <div className="actions">
          <button onClick={() => setEditing({ ...editing,
            schedules: [...editing.schedules, { schedule: newSchedule() }] })}>添加日程</button>
          <button onClick={() => setEditing(undefined)}>取消</button>
          <button className="primary" onClick={() => void save(editing)} disabled={!editing.packageId}><Save size={16} />保存</button>
        </div>
      </section></div>}
      {history && <div className="modal-backdrop"><section className="modal" aria-label="触发历史">
        <h2>触发历史</h2>
        <table><thead><tr><th>计划时间</th><th>包版本</th><th>状态</th><th>运行</th></tr></thead>
          <tbody>{history.items.map((item) => <tr key={item.id}><td>{instant(item.timestampMs)}</td>
            <td>{item.packageRevision}</td><td>{item.state === "Missed" ? "已错过" : item.state === "Blocked" ? "发布修订不可执行" : "已入队"}</td>
            <td>{item.state === "Missed" ? <button onClick={() => void run(history.job, item.id)}>补跑</button> : item.runId}</td>
          </tr>)}</tbody>
        </table>
        <button onClick={() => setHistory(undefined)}>关闭</button>
      </section></div>}
      <Deployments />
    </section>
  );
}
