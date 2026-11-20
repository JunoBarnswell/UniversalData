import { useEffect, useState } from "react";
import { api } from "./api";
import { workerReason } from "./model";

interface WorkerNode {
  nodeId: string;
  remote: boolean;
  state: "Online" | "Suspect" | "Offline";
  capacity?: {
    starting: number;
    prewarmedPids: number[];
    running: number;
    draining: number;
    availableProcesses: number;
    availableCpu: number;
    availableMemoryBytes: number | bigint;
    reason?: string;
  };
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
      <thead><tr><th>节点</th><th>状态</th><th>最近心跳</th><th>运行 / 上限</th><th>预热 PID</th><th>启动 / 排空</th><th>可用进程</th><th>CPU 配额</th><th>内存（MiB）</th></tr></thead>
      <tbody>{nodes.map((node) => <tr key={node.nodeId}>
        <td>{node.nodeId}{node.remote ? "" : "（本机）"}</td><td>{({ Online: "在线", Suspect: "心跳延迟", Offline: "离线" })[node.state]}
          {node.capacity?.reason && <small>{workerReason(node.capacity.reason)}</small>}</td>
        <td>{new Date(node.lastHeartbeatMs).toLocaleString()}</td>
        <td>{node.capacity?.running ?? node.activeProcesses} / {node.maxProcesses}</td>
        <td>{node.capacity?.prewarmedPids.join("、") || "—"}</td>
        <td>{node.capacity ? `${node.capacity.starting} / ${node.capacity.draining}` : "—"}</td>
        <td>{node.capacity?.availableProcesses ?? "—"}</td><td>{node.usedCpu} / {node.maxCpu}</td>
        <td>{memory(node.usedMemoryBytes)} / {memory(node.maxMemoryBytes)}</td>
      </tr>)}</tbody>
    </table></div>
  </section>;
}
