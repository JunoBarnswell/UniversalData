import { useEffect, useState } from "react";
import { api, post } from "./api";
import type { PackageDocument, Resources, RunState } from "./model";
export function TestDialog({
  document,
  close,
  complete,
}: {
  document: PackageDocument;
  close: () => void;
  complete: (run: RunState) => void;
}) {
  const [resources, setResources] = useState<Resources>(),
    [connection, setConnection] = useState(""),
    [tables, setTables] = useState<Record<string, Record<string, string>>>({}),
    [confirm, setConfirm] = useState(false),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  useEffect(() => {
    const c = new AbortController();
    api<Resources>("/resources", { signal: c.signal })
      .then(setResources)
      .catch((error) => {
        if (!c.signal.aborted) setError(String(error));
      });
    return () => c.abort();
  }, []);
  const targets = document.dataFlows.flatMap((flow) =>
    flow.components
      .filter((node) => node.component === "sql.destination")
      .map((node) => ({ flow: flow.id, node })),
  );
  const allowed =
    document.connections?.filter((connection) =>
      resources?.testResources.includes(connection.resource),
    ) ?? [];
  async function run() {
    setBusy(true);
    try {
      complete(
        await post<RunState>("/tests", {
          package: document,
          targetConnection: connection,
          tables,
        }),
      );
    } catch (error) {
      setError(String(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="modal-backdrop">
      <section
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-label="隔离完整测试"
      >
        <h2>隔离完整测试</h2>
        <p>
          使用完整源数据和原生执行链路。每个目标必须绑定宿主批准的测试资源与明确的测试表。
        </p>
        {!allowed.length ? (
          <p className="warning">
            宿主尚未配置隔离测试资源。请先在连接中心添加已授权的测试连接。
          </p>
        ) : null}
        <label>
          测试数据库连接
          <select
            value={connection}
            onChange={(event) => setConnection(event.target.value)}
          >
            <option value="">选择隔离测试连接</option>
            {allowed.map((connection) => (
              <option key={connection.id} value={connection.id}>
                {connection.id} · {connection.resource}
              </option>
            ))}
          </select>
        </label>
        {targets.map(({ flow, node: target }) => (
          <label key={JSON.stringify([flow, target.id])}>
            {flow} / {String(target.config.table ?? target.id)} → 测试表
            <input
              value={tables[flow]?.[target.id] ?? ""}
              onChange={(event) =>
                setTables({
                  ...tables,
                  [flow]: { ...tables[flow], [target.id]: event.target.value },
                })
              }
              placeholder="预先准备的隔离测试表"
            />
          </label>
        ))}
        <label className="checkbox">
          <input
            type="checkbox"
            checked={confirm}
            onChange={(event) => setConfirm(event.target.checked)}
          />
          已核对全部隔离测试目标
        </label>
        {error ? <p role="alert">{error}</p> : null}
        <footer>
          <button onClick={close}>取消</button>
          <button
            className="primary"
            disabled={
              busy ||
              !confirm ||
              !connection ||
              !targets.length ||
              targets.some(({ flow, node }) => !tables[flow]?.[node.id]?.trim())
            }
            onClick={() => void run()}
          >
            运行完整测试
          </button>
        </footer>
      </section>
    </div>
  );
}
