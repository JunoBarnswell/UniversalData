import { PROTOCOL_VERSION } from "./version";
import { useEffect, useState } from "react";
import { Plus, Save } from "lucide-react";
import { api } from "./api";
import type { Deployment } from "./model";
import { Deployments } from "./Deployments";
interface Job {
  version: number;
  id: string;
  packageId: string;
  packageRevision: string;
  parameters?: string;
  parametersReference?: string;
  enabled: boolean;
  everySeconds: number;
  nextDueMs: number;
  maxConcurrency: number;
  maxAttempts: number;
  retryDelaySeconds: number;
  deploymentId?: string;
  deploymentRevision?: string;
}
export function Jobs() {
  const [jobs, setJobs] = useState<Job[]>([]),
    [error, setError] = useState(""),
    [editing, setEditing] = useState<Job>();
  const [deployments, setDeployments] = useState<Deployment[]>([]);
  useEffect(() => {
    const c = new AbortController();
    api<Deployment[]>("/deployments", { signal: c.signal })
      .then(setDeployments)
      .catch((error) => {
        if (!c.signal.aborted) setError(String(error));
      });
    return () => c.abort();
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    api<Job[]>("/jobs", { signal: controller.signal })
      .then(setJobs)
      .catch((error) => {
        if (!controller.signal.aborted) setError(String(error));
      });
    return () => controller.abort();
  }, []);
  async function save(job: Job) {
    try {
      const value = await api<Job>(`/jobs/${encodeURIComponent(job.id)}`, {
        method: "PUT",
        body: JSON.stringify(job),
      });
      setJobs((current) => [
        ...current.filter((job) => job.id !== value.id),
        value,
      ]);
      setEditing(undefined);
    } catch (error) {
      setError(String(error));
    }
  }
  return (
    <section className="page">
      <div className="page-heading">
        <h1>作业管理</h1>
        <button
          className="primary"
          onClick={() =>
            setEditing({
              version: PROTOCOL_VERSION,
              id: crypto.randomUUID(),
              packageId: "",
              packageRevision: "1",
              parameters: "{}",
              enabled: true,
              everySeconds: 3600,
              nextDueMs: 0,
              maxConcurrency: 1,
              maxAttempts: 1,
              retryDelaySeconds: 30,
            })
          }
        >
          <Plus size={16} />
          新建作业
        </button>
      </div>
      {error ? (
        <p role="alert" className="error">
          {error}
        </p>
      ) : null}
      <div className="table-panel">
        <table>
          <thead>
            <tr>
              <th>作业</th>
              <th>包版本</th>
              <th>间隔</th>
              <th>下次触发</th>
              <th>状态</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr key={job.id}>
                <td>{job.id}</td>
                <td>
                  {job.packageId} v{job.packageRevision}
                </td>
                <td>{job.everySeconds} 秒</td>
                <td>{new Date(job.nextDueMs).toLocaleString()}</td>
                <td>{job.enabled ? "已启用" : "已停用"}</td>
                <td>
                  <button onClick={() => setEditing(job)}>编辑</button>
                  <button
                    onClick={() => void save({ ...job, enabled: !job.enabled })}
                  >
                    {job.enabled ? "停用" : "启用"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!jobs.length ? <div className="empty-state">暂无作业</div> : null}
      </div>
      {editing ? (
        <div className="modal-backdrop">
          <section className="modal">
            <h2>作业配置</h2>
            <label>
              作业名称
              <input
                value={editing.id}
                onChange={(event) =>
                  setEditing({ ...editing, id: event.target.value })
                }
              />
            </label>
            <label>
              已发布版本
              <select
                aria-label="作业发布版本"
                value={editing.deploymentId ?? ""}
                onChange={(event) => {
                  const selected = deployments.find(
                    (item) => item.id === event.target.value,
                  );
                  if (selected)
                    setEditing({
                      ...editing,
                      packageId: selected.packageId,
                      packageRevision: selected.packageRevision,
                      deploymentId: selected.id,
                      deploymentRevision: selected.deploymentRevision,
                    });
                }}
              >
                <option value="">选择已发布任务</option>
                {deployments
                  .filter((item) => item.enabled)
                  .map((item) => (
                    <option key={item.id} value={item.id}>
                      {item.packageId} · v{item.packageRevision} · 发布 v
                      {item.deploymentRevision}
                    </option>
                  ))}
              </select>
            </label>
            {(
              [
                "everySeconds",
                "maxConcurrency",
                "maxAttempts",
                "retryDelaySeconds",
              ] as const
            ).map((key) => (
              <label key={key}>
                {
                  {
                    everySeconds: "触发间隔（秒）",
                    maxConcurrency: "并发上限",
                    maxAttempts: "最大尝试次数",
                    retryDelaySeconds: "失败恢复间隔（秒）",
                  }[key]
                }
                <input
                  type="number"
                  min="1"
                  value={editing[key]}
                  onChange={(event) =>
                    setEditing({
                      ...editing,
                      [key]: Number(event.target.value),
                    })
                  }
                />
              </label>
            ))}
            <label>
              运行参数（保存后不回显原值）
              <textarea
                value={editing.parameters ?? ""}
                placeholder={
                  editing.parametersReference
                    ? "留空保留已保护的参数；输入新值可替换"
                    : "{}"
                }
                onChange={(event) =>
                  setEditing({
                    ...editing,
                    parameters: event.target.value || undefined,
                  })
                }
              />
            </label>
            <label className="checkbox">
              <input
                type="checkbox"
                checked={editing.enabled}
                onChange={(event) =>
                  setEditing({ ...editing, enabled: event.target.checked })
                }
              />
              启用作业
            </label>
            <div>
              <button onClick={() => setEditing(undefined)}>取消</button>
              <button
                className="primary"
                disabled={!editing.deploymentId}
                onClick={() => void save(editing)}
              >
                <Save size={15} />
                保存作业
              </button>
            </div>
          </section>
        </div>
      ) : null}
      <Deployments />
    </section>
  );
}
