import { useEffect, useState } from "react";
import { Database, FileSpreadsheet, Plus, Trash2, X } from "lucide-react";
import { CacheEditor } from "./CacheEditor";
import type { Configuration } from "./model";
import { api, post, encode } from "./api";
import type {
  Connection,
  ConnectorDescriptor,
  PackageDocument,
  Resources,
} from "./model";
export function ConnectionCenter({
  document,
  change,
  changeCaches,
  close,
}: {
  document: PackageDocument;
  change: (connections: Connection[]) => void;
  changeCaches: (caches: Configuration[]) => void;
  close: () => void;
}) {
  const [selected, select] = useState(document.connections?.[0]?.id ?? ""),
    [resources, setResources] = useState<Resources>(),
    [connectors, setConnectors] = useState<ConnectorDescriptor[]>([]),
    [message, setMessage] = useState(""),
    [busy, setBusy] = useState(false);
  useEffect(() => {
    const c = new AbortController();
    Promise.all([
      api<Resources>("/resources", { signal: c.signal }),
      api<ConnectorDescriptor[]>("/connectors", { signal: c.signal }),
    ])
      .then(([resources, connectors]) => {
        setResources(resources);
        setConnectors(connectors);
      })
      .catch((error) => {
        if (!c.signal.aborted) setMessage(String(error));
      });
    return () => c.abort();
  }, []);
  const connections = document.connections ?? [],
    current = connections.find((value) => value.id === selected);
  function edit(update: Partial<Connection>) {
    change(
      connections.map((value) =>
        value.id === selected ? { ...value, ...update } : value,
      ),
    );
    if (update.id) select(update.id);
  }
  async function test() {
    if (!current) return;
    setBusy(true);
    try {
      if (current.driver === "file")
        throw new Error("请在文件向导中发现字段与预览内容");
      const rows = await post<unknown[]>("/metadata", {
        package: document,
        connection: current.id,
        kind: 1,
      });
      setMessage(`连接成功，Native 返回 ${rows.length} 项数据库元数据`);
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="modal-backdrop">
      <section
        className="modal connection-center"
        role="dialog"
        aria-modal="true"
        aria-label="连接中心"
      >
        <header className="dialog-heading">
          <h2>连接中心</h2>
          <button aria-label="关闭连接中心" onClick={close}>
            <X size={18} />
          </button>
        </header>
        <p>凭据保留在宿主中，包只保存授权引用。</p>
        <div className="connection-layout">
          <aside>
            {connections.map((connection) => (
              <button
                className={selected === connection.id ? "selected" : ""}
                key={connection.id}
                onClick={() => select(connection.id)}
              >
                {connection.driver === "file" ? (
                  <FileSpreadsheet size={16} />
                ) : (
                  <Database size={16} />
                )}
                <span>
                  {connection.id}
                  <small>{connection.driver}</small>
                </span>
              </button>
            ))}
            <button
              onClick={() => {
                const id = `connection-${crypto.randomUUID()}`;
                change([
                  ...connections,
                  {
                    id,
                    driver:
                      connectors.find((value) => value.result === 0)?.path ??
                      "file",
                    resource: resources?.resources?.[0] ?? "",
                  },
                ]);
                select(id);
              }}
            >
              <Plus size={15} />
              新建连接
            </button>
          </aside>
          <div>
            {current ? (
              <>
                <label>
                  连接名称
                  <input
                    aria-label="连接名称"
                    value={current.id}
                    readOnly
                    title="创建后身份固定，组件引用保持稳定"
                  />
                </label>
                <label>
                  数据源类型
                  <select
                    aria-label="数据源类型"
                    value={current.driver}
                    onChange={(event) => edit({ driver: event.target.value })}
                  >
                    <option value="file">文件</option>
                    {current.driver === "excel" ? (
                      <option value="excel">Excel</option>
                    ) : null}
                    {current.driver !== "file" &&
                    current.driver !== "excel" &&
                    !connectors.some(
                      (value) => value.path === current.driver,
                    ) ? (
                      <option value={current.driver}>
                        当前驱动（宿主目录未列出）
                      </option>
                    ) : null}
                    {connectors.map((value) => (
                      <option
                        key={value.path}
                        value={value.path}
                        disabled={value.result !== 0}
                      >
                        {value.id || value.path.split(/[\\/]/).at(-1)}{" "}
                        {value.result === 0
                          ? value.version
                          : `· 不可用 (${value.result})`}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  授权资源
                  <input
                    aria-label="授权资源"
                    list="authorized-resources"
                    value={current.resource}
                    onChange={(event) => edit({ resource: event.target.value })}
                    placeholder="宿主授权的路径或数据库资源"
                  />
                  <datalist id="authorized-resources">
                    {resources?.resources?.map((resource) => (
                      <option key={resource} value={resource} />
                    ))}
                  </datalist>
                </label>
                <label>
                  凭据引用
                  <select
                    aria-label="凭据引用"
                    value={current.credentialReference ?? ""}
                    onChange={(event) =>
                      edit({
                        credentialReference: event.target.value || undefined,
                      })
                    }
                  >
                    <option value="">无需凭据</option>
                    {resources?.credentialReferences?.map((reference) => (
                      <option key={reference} value={reference}>
                        {reference}
                      </option>
                    ))}
                  </select>
                </label>
                <div className="inline-actions">
                  <button
                    disabled={busy || current.driver === "file"}
                    onClick={() => void test()}
                  >
                    测试连接 / 列出表
                  </button>
                  <button
                    onClick={() => {
                      const used = document.dataFlows.some((flow) =>
                        flow.components.some(
                          (node) => node.config.connection === current.id,
                        ),
                      );
                      if (used) {
                        setMessage("此连接仍被组件使用，请先更改组件连接。");
                        return;
                      }
                      change(
                        connections.filter((value) => value.id !== selected),
                      );
                      select("");
                    }}
                  >
                    <Trash2 size={15} />
                    移除
                  </button>
                </div>
                <small>
                  支持的驱动及资源权限由宿主决定。测试只请求 Native 元数据。
                </small>
              </>
            ) : (
              <p>选择或创建一个连接。</p>
            )}
            {message ? <p role="status">{message}</p> : null}
          </div>
        </div>
        <CacheEditor document={document} change={changeCaches} />
        <details>
          <summary>专业调试：连接定义</summary>
          <pre>{encode(connections, 2)}</pre>
        </details>
        <footer>
          <button className="primary" onClick={close}>
            完成
          </button>
        </footer>
      </section>
    </div>
  );
}
