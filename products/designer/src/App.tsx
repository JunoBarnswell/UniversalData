import { useEffect, useState } from 'react';
import { Box, Play, List, CalendarDays, Upload } from 'lucide-react';
import { api, post, encode } from './api';
import { Editor } from './Editor';
import { Executions } from './Executions';
import { Jobs } from './Jobs';
import type { Component, PackageDocument } from './model';
type Page = 'designer' | 'runs' | 'logs' | 'jobs';
export default function App() {
  const [page, setPage] = useState<Page>('designer');
  const [components, setComponents] = useState<Component[]>([]);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState<{ id: string; revision: string; content: string }>();
  useEffect(() => { const controller = new AbortController(); api<Component[]>('/components', { signal: controller.signal }).then(setComponents).catch(error => { if (!controller.signal.aborted) setError(String(error)); }); return () => controller.abort(); }, []);
  async function deploy(document?: PackageDocument) {
    try {
      if (!saved || (document && encode(document) !== saved.content)) throw new Error('请先保存包版本');
      const deployment = await post<{ id: string; deploymentRevision: string }>('/deployments', { id: saved.id, packageId: saved.id, packageRevision: saved.revision });
      await post('/executions', { packageId: saved.id, packageRevision: saved.revision, deploymentId: deployment.id, deploymentRevision: deployment.deploymentRevision }); setPage('runs');
    } catch (error) { setError(String(error)); }
  }
  const items: { key: Page; label: string; icon: typeof Box }[] = [{ key: 'designer', label: '设计器', icon: Box }, { key: 'runs', label: '执行', icon: Play }, { key: 'logs', label: '日志', icon: List }, { key: 'jobs', label: '作业', icon: CalendarDays }];
  return <div className="application"><header className="main-header"><a className="brand" href="#" onClick={() => setPage('designer')}><Box size={28} fill="#087f8c" color="#087f8c"/><strong>UniversalData</strong></a><nav aria-label="主导航">{items.map(({ key, label, icon: Icon }) => <button key={key} className={page === key ? 'active' : ''} onClick={() => setPage(key)}><Icon size={16}/>{label}</button>)}</nav><button className="primary deploy" disabled={!saved} onClick={() => void deploy()}><Upload size={16}/>部署</button></header>
    {error ? <div role="alert" className="banner">{error}<button aria-label="关闭提示" onClick={() => setError('')}>×</button></div> : null}
    {page === 'designer' ? <Editor components={components} saved={setSaved} deploy={deploy}/> : page === 'runs' || page === 'logs' ? <Executions logsOnly={page === 'logs'}/> : <Jobs/>}
  </div>;
}
