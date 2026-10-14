import { useEffect, useState } from 'react';
import { api, post } from './api';
interface Deployment { id: string; packageId: string; packageRevision: string; deploymentRevision: string; enabled: boolean; }
export function Deployments() {
  const [items, setItems] = useState<Deployment[]>([]), [error, setError] = useState('');
  useEffect(() => { const controller = new AbortController(); api<Deployment[]>('/deployments', { signal: controller.signal }).then(setItems).catch(error => { if (!controller.signal.aborted) setError(String(error)); }); return () => controller.abort(); }, []);
  async function activate(item: Deployment) {
    try { const changed = await post<Deployment>(`/deployments/${encodeURIComponent(item.id)}/${item.enabled ? 'stop' : 'start'}`); setItems(current => current.map(value => value.id === item.id ? changed : value)); }
    catch (error) { setError(String(error)); }
  }
  return <section className="deployment-panel"><h2>已部署版本</h2>{error ? <p role="alert">{error}</p> : null}<table><thead><tr><th>部署</th><th>包版本</th><th>部署版本</th><th>状态</th><th>操作</th></tr></thead><tbody>{items.map(item => <tr key={item.id}><td>{item.id}</td><td>{item.packageId} v{item.packageRevision}</td><td>v{item.deploymentRevision}</td><td>{item.enabled ? '已启用' : '已停止'}</td><td><button onClick={() => void activate(item)}>{item.enabled ? '停止并取消执行' : '启用'}</button></td></tr>)}</tbody></table>{!items.length ? <p className="muted">暂无部署</p> : null}</section>;
}
