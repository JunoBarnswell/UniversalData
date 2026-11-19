import { useEffect, useState } from "react";
import { FileInput, FolderOpen, Plus, Search } from "lucide-react";
import { api } from "./api";
import {
  executionStateName,
  type CatalogDocument,
  type PackageDocument,
  type RunState,
  type DraftRecord,
} from "./model";
export function Projects({
  open,
  create,
}: {
  open: (version: CatalogDocument<PackageDocument>) => void;
  create: (importFile: boolean) => void;
}) {
  const [versions, setVersions] = useState<CatalogDocument<PackageDocument>[]>(
      [],
    ),
    [runs, setRuns] = useState<RunState[]>([]),
    [drafts, setDrafts] = useState<DraftRecord[]>([]),
    [query, setQuery] = useState(""),
    [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    Promise.all([
      api<CatalogDocument<PackageDocument>[]>("/packages", {
        signal: controller.signal,
      }),
      api<RunState[]>("/executions", { signal: controller.signal }),
      api<DraftRecord[]>("/drafts", { signal: controller.signal }),
    ])
      .then(([versions, runs, drafts]) => {
        setVersions(versions);
        setRuns(runs);
        setDrafts(drafts);
      })
      .catch((error) => {
        if (!controller.signal.aborted) setError(String(error));
      });
    return () => controller.abort();
  }, []);
  const latest = new Map<string, CatalogDocument<PackageDocument>>();
  for (const version of versions)
    if (
      !latest.has(version.id) ||
      BigInt(latest.get(version.id)!.revision) < BigInt(version.revision)
    )
      latest.set(version.id, version);
  for (const draft of drafts)
    if (!latest.has(draft.packageId))
      latest.set(draft.packageId, {
        id: draft.packageId,
        revision: BigInt(draft.baseRevision),
        document: draft.document,
      });
  return (
    <section className="page projects">
      <header className="page-heading">
        <div>
          <small>UniversalData 工作台</small>
          <h1>项目与数据任务</h1>
          <p className="muted">
            从文件建立数据流，观察每个转换，再固定版本并执行。
          </p>
        </div>
        <div className="inline-actions">
          <button onClick={() => create(false)}>
            <Plus size={16} />
            空白任务
          </button>
          <button className="primary" onClick={() => create(true)}>
            <FileInput size={16} />从 CSV / Excel 导入
          </button>
        </div>
      </header>
      {error ? <p role="alert">{error}</p> : null}
      <div className="project-list-heading">
        <h2>已保存的数据任务</h2>
        <label className="search">
          <Search size={16} />
          <input
            aria-label="搜索任务"
            placeholder="搜索任务名称"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </label>
      </div>
      <div className="table-panel">
        <table>
          <thead>
            <tr>
              <th>任务</th>
              <th>保存版本</th>
              <th>数据流 / 组件</th>
              <th>上次运行</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {[...latest.values()]
              .filter(
                (version) =>
                  !version.id.startsWith("test-") &&
                  `${version.id} ${version.document.layout?.label ?? ""}`.includes(
                    query,
                  ),
              )
              .map((version) => {
                const run = runs
                  .filter((run) => run.packageId === version.id)
                  .sort((a, b) => b.timestampMs - a.timestampMs)[0];
                return (
                  <tr key={version.id}>
                    <td>
                      <FolderOpen size={16} />
                      {version.document.layout?.label ?? version.id}
                    </td>
                    <td>
                      {BigInt(version.revision) === 0n
                        ? "仅工作草稿"
                        : `v${String(version.revision)}`}
                      {drafts.some((draft) => draft.packageId === version.id)
                        ? " · 有工作草稿"
                        : ""}
                    </td>
                    <td>
                      {version.document.dataFlows.length} /{" "}
                      {version.document.dataFlows.reduce(
                        (sum, flow) => sum + flow.components.length,
                        0,
                      )}
                    </td>
                    <td>{run ? executionStateName(run) : "尚未运行"}</td>
                    <td>
                      <button
                        className="text-button"
                        onClick={() => open(version)}
                      >
                        打开工作台
                      </button>
                    </td>
                  </tr>
                );
              })}
          </tbody>
        </table>
        {!latest.size ? (
          <div className="empty-state">
            <FileInput size={32} />
            <strong>开始第一个数据任务</strong>
            <span>导入文件后，系统会发现字段并展示真实样本。</span>
            <button onClick={() => create(true)}>从文件开始</button>
          </div>
        ) : null}
      </div>
      <p className="muted">
        预览仅读取有界样本。完整测试需要授权的隔离资源；发布与正式执行分别确认。
      </p>
    </section>
  );
}
