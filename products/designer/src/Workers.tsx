import { useEffect, useState } from "react";
import { api } from "./api";

interface WorkerNode {
  nodeId: string;
  remote: boolean;
  online: boolean;
  lastHeartbeatMs: number;
  maxProcesses: number;
  activeProcesses: number;
  maxCpu: number;
  usedCpu: number;
  maxMemoryBytes: number | bigint;
  usedMemoryBytes: number | bigint;
}

export function Workers() {
  const [nodes, setNodes] = useState<WorkerNode[]>([]);
  const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function update() {
      try { setNodes(await api<WorkerNode[]>("/workers", { signal: controller.signal })); setError(""); }
      catch (failure) { if (!controller.signal.aborted) setError(String(failure)); }
      if (!controller.signal.aborted) timer = setTimeout(() => void update(), 5000);
    }
    void update();
    return () => { clearTimeout(timer); controller.abort(); };
  }, []);
  const memory = (bytes: number | bigint) => (Number(bytes) / 2 ** 20).toFixed(0);
  return <section className="page">
    <h1>Worker 节点</h1>
    {error && <p className="error" role="alert">{error}</p>}
    <div className="table-panel"><table>
      <thead><tr><th>节点</th><th>状态</th><th>最近心跳</th><th>进程</th><th>CPU 配额</th><th>内存（MiB）</th></tr></thead>
      <tbody>{nodes.map((node) => <tr key={node.nodeId}>
        <td>{node.nodeId}{node.remote ? "" : "（本机）"}</td><td>{node.online ? "在线" : "失联"}</td>
        <td>{new Date(node.lastHeartbeatMs).toLocaleString()}</td>
        <td>{node.activeProcesses} / {node.maxProcesses}</td><td>{node.usedCpu} / {node.maxCpu}</td>
        <td>{memory(node.usedMemoryBytes)} / {memory(node.maxMemoryBytes)}</td>
      </tr>)}</tbody>
    </table></div>
  </section>;
}
