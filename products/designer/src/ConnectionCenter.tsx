import { Button, Input, Select } from "./shared/design-system/Controls";
import { useEffect, useState } from "react";
import { Database, FileSpreadsheet, Plus, Trash2, X } from "lucide-react";
import { CacheEditor } from "./CacheEditor";
import type { Configuration } from "./model";
import { api, post, encode } from "./api";
import { MESSAGE_PUBLISH, MESSAGE_SUBSCRIBE, nativeConnector } from "./model";
import type {
  Connection,
  ConnectionTestResult,
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
    [loadedConnectors, setConnectors] = useState<ConnectorDescriptor[]>(),
    [message, setMessage] = useState(""),
    [busy, setBusy] = useState(false);
  useEffect(() => {
    const c = new AbortController();
    void Promise.allSettled([
      api<Resources>("/resources", { signal: c.signal }),
      api<ConnectorDescriptor[]>("/connectors", { signal: c.signal }),
    ]).then(([resources, connectors]) => {
      if (c.signal.aborted) return;
      if (resources.status === "fulfilled") setResources(resources.value);
      else setMessage(String(resources.reason));
      setConnectors(connectors.status === "fulfilled" ? connectors.value : []);
      if (connectors.status === "rejected") setMessage(String(connectors.reason));
    });
    return () => c.abort();
  }, []);
  const connectors = loadedConnectors ?? [],
    connections = document.connections ?? [],
    current = connections.find((value) => value.id === selected),
    descriptor = current && nativeConnector(current, connectors),
    messageConnector = !!(descriptor?.result === 0 &&
      (descriptor.capabilities & (MESSAGE_SUBSCRIBE | MESSAGE_PUBLISH)));
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
      if (messageConnector) {
        const { capabilities, elapsedMs } = await post<ConnectionTestResult>("/connections/test", {
          package: document,
          connection: current.id,
        });
        const operations = [
          capabilities & MESSAGE_SUBSCRIBE ? "订阅" : "",
          capabilities & MESSAGE_PUBLISH ? "发布" : "",
        ].filter(Boolean);
        setMessage(`${current.id} 连接成功 · ${elapsedMs} ms${operations.length ? ` · ${operations.join(" / ")}` : ""}`);
        return;
      }
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
          <Button aria-label="关闭连接中心" onClick={close}>
            <X size={18} />
          </Button>
        </header>
        <p>凭据保留在宿主中，包只保存授权引用。</p>
        <div className="connection-layout">
          <aside>
            {connections.map((connection) => (
              <Button
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
              </Button>
            ))}
            <Button
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
            </Button>
          </aside>
          <div>
            {current ? (
              <>
                <label>
                  连接名称
                  <Input
                    aria-label="连接名称"
                    value={current.id}
                    readOnly
                    title="创建后身份固定，组件引用保持稳定"
                  />
                </label>
                <label>
                  数据源类型
                  <Select
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
                  </Select>
                </label>
                <label>
                  授权资源
                  <Input
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
                  <Select
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
                  </Select>
                </label>
                {current.driver === "http" && <fieldset>
                  <label><input type="checkbox" checked={current.oauth2 !== undefined}
                    onChange={(event) => edit({ oauth2: event.target.checked ? {
                      tokenEndpoint: "", clientId: "", credentialReference: "", authentication: "basic",
                    } : undefined })} />OAuth2 客户端凭据</label>
                  {current.oauth2 && <>
                    <label>令牌端点<input type="url" value={current.oauth2.tokenEndpoint}
                      onChange={(event) => edit({ oauth2: { ...current.oauth2!, tokenEndpoint: event.target.value } })} /></label>
                    <label>客户端 ID<input value={current.oauth2.clientId}
                      onChange={(event) => edit({ oauth2: { ...current.oauth2!, clientId: event.target.value } })} /></label>
                    <label>客户端密钥引用<Select value={current.oauth2.credentialReference}
                      onChange={(event) => edit({ oauth2: { ...current.oauth2!, credentialReference: event.target.value } })}>
                      <option value="">选择凭据</option>{resources?.credentialReferences?.map((reference) =>
                        <option key={reference} value={reference}>{reference}</option>)}
                    </Select></label>
                    <label>Scope<input value={current.oauth2.scope ?? ""}
                      onChange={(event) => edit({ oauth2: { ...current.oauth2!, scope: event.target.value || undefined } })} /></label>
                    <label>客户端认证<Select value={current.oauth2.authentication}
                      onChange={(event) => edit({ oauth2: { ...current.oauth2!, authentication:
                        event.target.value === "post" ? "post" : "basic" } })}>
                      <option value="basic">HTTP Basic</option><option value="post">表单</option>
                    </Select></label>
                  </>}
                </fieldset>}
                <div className="inline-actions">
                  <Button
                    disabled={busy || !loadedConnectors || current.driver === "file" ||
                      (descriptor !== undefined && descriptor.result !== 0)}
                    onClick={() => void test()}
                  >
                    {messageConnector ? "测试消息连接" : "测试连接 / 列出表"}
                  </Button>
                  <Button
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
                  </Button>
                </div>
                <small>
                  {messageConnector
                    ? "Native 验证 broker 连接。测试期间不订阅或发布消息。"
                    : "支持的驱动及资源权限由宿主决定。测试只请求 Native 元数据。"}
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
          <Button className="primary" onClick={close}>
            完成
          </Button>
        </footer>
      </section>
    </div>
  );
}
