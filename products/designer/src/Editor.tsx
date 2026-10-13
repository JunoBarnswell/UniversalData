import { PROTOCOL_VERSION } from './version';
import { useCallback, useMemo, useState } from 'react';
import { Background, Controls, ReactFlow, ReactFlowProvider, type Connection as GraphConnection, type Node, type Edge, type NodeChange } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Search, ChevronDown, Undo2, Redo2, Save, CircleCheck, FileText, Database, Trash2, Plus, Settings2 } from 'lucide-react';
import { api, post, encode, decode } from './api';
import { defaults, emptyPackage, ports, type CatalogDocument, type Component, type Configuration, type PackageDocument, type Connection } from './model';
import { useHistory } from './useHistory';
import { FlowNode } from './FlowNode';
import { PropertyForm } from './PropertyForm';
const nodeTypes = { component: FlowNode };
export function Editor({ components, saved, deploy }: { components: Component[]; saved: (value: { id: string; revision: string; content: string }) => void; deploy: (document?: PackageDocument) => Promise<void> }) {
  return <ReactFlowProvider><EditorCanvas components={components} saved={saved} deploy={deploy}/></ReactFlowProvider>;
}
function EditorCanvas({ components, saved, deploy }: { components: Component[]; saved: (value: { id: string; revision: string; content: string }) => void; deploy: (document?: PackageDocument) => Promise<void> }) {
  const history = useHistory(emptyPackage), document = history.value;
  const [flow, setFlow] = useState('main'); const [selected, setSelected] = useState<string>();
  const [search, setSearch] = useState(''); const [message, setMessage] = useState('尚未校验');
  const [result, setResult] = useState<unknown>(); const [busy, setBusy] = useState(false);
  const [connectionsOpen, setConnectionsOpen] = useState(false); const [versions, setVersions] = useState<CatalogDocument<PackageDocument>[]>([]);
  const [dragPositions, setDragPositions] = useState<Record<string, { x: number; y: number }>>({});
  const catalog = useMemo(() => new Map(components.map(component => [component.id, component])), [components]);
  const graph = document.dataFlows.find(value => value.id === flow);
  const nodes: Node[] = useMemo(() => {
    const items = flow === 'control' ? document.controlFlow.tasks.map(task => ({ id: task.id, component: task.kind, config: task.config })) : graph?.components ?? [];
    return items.map((item, index) => {
      const descriptor = catalog.get(item.component); const counts = descriptor ? ports(descriptor, item.config) : { inputs: 1, outputs: 1, error: -1 };
      const position = document.layout?.nodes.find(node => node.flow === flow && node.id === item.id);
      return { id: item.id, type: 'component', selected: selected === item.id, position: dragPositions[item.id] ?? position ?? { x: 60 + index * 230, y: 180 }, data: { label: descriptor?.name ?? item.component, componentId: item.component, ...counts } };
    });
  }, [catalog, document, dragPositions, flow, graph, selected]);
  const edges: Edge[] = useMemo(() => flow === 'control' ? document.controlFlow.constraints.map((edge, index) => ({ id: `constraint-${index}`, source: edge.from, target: edge.to, sourceHandle: 'out-0', targetHandle: 'in-0', label: edge.on, style: { stroke: edge.on === 'failure' ? '#ef4444' : '#4b5563' } })) : (graph?.paths ?? []).map((edge, index) => {
    const producer = graph?.components.find(node => node.id === edge.from); const descriptor = producer && catalog.get(producer.component); const error = descriptor && edge.output === ports(descriptor, producer!.config).error;
    return { id: `path-${index}`, source: edge.from, target: edge.to, sourceHandle: `out-${edge.output}`, targetHandle: `in-${edge.input}`, style: { stroke: error ? '#ef4444' : '#4b5563', strokeWidth: 1.8 } };
  }), [catalog, document.controlFlow.constraints, flow, graph]);
  const item = flow === 'control' ? document.controlFlow.tasks.find(node => node.id === selected) : graph?.components.find(node => node.id === selected);
  const descriptor = item ? catalog.get('component' in item ? item.component : item.kind) : undefined;
  function modifyGraph(update: (value: PackageDocument) => PackageDocument) { history.edit(update); setMessage('未校验的更改'); }
  function add(component: Component) {
    const id = `node-${crypto.randomUUID()}`;
    modifyGraph(current => flow === 'control' ? { ...current, controlFlow: { ...current.controlFlow, tasks: [...current.controlFlow.tasks, { id, kind: component.id, config: defaults(component.configurationSchema) }] } } : { ...current, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, components: [...value.components, { id, component: component.id, config: defaults(component.configurationSchema) }] } : value) }); setSelected(id);
  }
  const changes = useCallback((updates: NodeChange[]) => {
    for (const update of updates) if (update.type === 'position' && update.position && update.dragging)
      setDragPositions(current => ({ ...current, [update.id]: update.position! }));
    const positions = updates.filter(update => update.type === 'position' && update.position && !update.dragging);
    if (!positions.length) return;
    history.edit(current => {
      const layout = current.layout ?? { version: PROTOCOL_VERSION, nodes: [], viewports: [] };
      let nodes = layout.nodes;
      for (const update of positions) if (update.type === 'position' && update.position) nodes = [...nodes.filter(node => node.flow !== flow || node.id !== update.id), { flow, id: update.id, ...update.position }];
      return { ...current, layout: { ...layout, nodes } };
    });
    setDragPositions({});
  }, [flow, history.edit]);
  function connect(connection: GraphConnection) {
    if (!connection.source || !connection.target) return;
    modifyGraph(current => flow === 'control' ? { ...current, controlFlow: { ...current.controlFlow, constraints: [...current.controlFlow.constraints, { from: connection.source, to: connection.target, on: 'success' }] } } : { ...current, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, paths: [...value.paths, { from: connection.source, to: connection.target, output: Number(connection.sourceHandle?.slice(4) ?? 0), input: Number(connection.targetHandle?.slice(3) ?? 0) }] } : value) });
  }
  function configure(config: Configuration) { modifyGraph(current => flow === 'control' ? { ...current, controlFlow: { ...current.controlFlow, tasks: current.controlFlow.tasks.map(task => task.id === selected ? { ...task, config } : task) } } : { ...current, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, components: value.components.map(node => node.id === selected ? { ...node, config } : node) } : value) }); }
  function remove() {
    modifyGraph(current => ({ ...current, controlFlow: flow === 'control' ? { tasks: current.controlFlow.tasks.filter(task => task.id !== selected), constraints: current.controlFlow.constraints.filter(edge => edge.from !== selected && edge.to !== selected) } : current.controlFlow, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, components: value.components.filter(node => node.id !== selected), paths: value.paths.filter(edge => edge.from !== selected && edge.to !== selected) } : value), layout: current.layout ? { ...current.layout, nodes: current.layout.nodes.filter(node => node.id !== selected || node.flow !== flow) } : undefined })); setSelected(undefined);
  }
  async function action(operation: 'save' | 'validate' | 'load') {
    setBusy(true);
    try {
      if (operation === 'validate') { const value = await post('/validate', document); setResult(value); setMessage('Native 校验通过'); }
      if (operation === 'load') setVersions(await api('/packages'));
      if (operation === 'save') {
        const latest = await api<{ revision: string }>(`/packages/${encodeURIComponent(document.id)}/latest`);
        const revision = String(BigInt(latest.revision) + 1n); await api(`/packages/${encodeURIComponent(document.id)}/versions/${revision}`, { method: 'PUT', body: encode(document) }); saved({ id: document.id, revision, content: encode(document) }); setMessage(`已保存 v${revision}`);
      }
    } catch (error) { setMessage(String(error)); } finally { setBusy(false); }
  }
  const groups = flow === 'control' ? [{ category: 1, label: '任务' }] : [{ category: 2, label: '数据源' }, { category: 3, label: '转换' }, { category: 4, label: '目标' }];
  return <><div className="editor-toolbar"><label>作业包名称<input aria-label="作业包名称" value={document.id} onChange={event => modifyGraph(current => ({ ...current, id: event.target.value }))}/></label><div className="tabs"><button className={flow === 'control' ? 'active' : ''} onClick={() => { setFlow('control'); setSelected(undefined); }}>控制流</button>{document.dataFlows.map(value => <button key={value.id} className={flow === value.id ? 'active' : ''} onClick={() => { setFlow(value.id); setSelected(undefined); }}>数据流 · {value.id}</button>)}</div><div className="toolbar-actions"><button disabled={!history.canUndo} onClick={history.undo}><Undo2 size={15}/>撤销</button><button disabled={!history.canRedo} onClick={history.redo}><Redo2 size={15}/>重做</button><button disabled={busy} onClick={() => void action('load')}>重载</button><button disabled={busy} onClick={() => void action('save')}><Save size={15}/>保存</button><button disabled={busy} onClick={() => void action('validate')}><CircleCheck size={15}/>校验</button></div></div>
    <div className="editor-body"><aside className="component-rail"><label className="search"><Search size={15}/><input aria-label="搜索组件" placeholder="搜索组件" value={search} onChange={event => setSearch(event.target.value)}/></label>{groups.map(group => <section key={group.category}><h3><ChevronDown size={15}/>{group.label}</h3>{components.filter(component => component.category === group.category && `${component.id} ${component.name}`.toLowerCase().includes(search.toLowerCase())).map(component => <button className="catalog-row" key={component.id} onClick={() => add(component)}><Database size={23}/><span><strong>{component.name}</strong><small>{component.id}</small></span><Plus size={14}/></button>)}</section>)}</aside>
    <main className="editor-center"><div className="flow-canvas"><ReactFlow nodes={nodes} edges={edges} nodeTypes={nodeTypes} onNodesChange={changes} onNodeClick={(_, node) => setSelected(node.id)} onPaneClick={() => setSelected(undefined)} onConnect={connect} onEdgesDelete={deleted => modifyGraph(current => flow === 'control' ? { ...current, controlFlow: { ...current.controlFlow, constraints: current.controlFlow.constraints.filter((_, index) => !deleted.some(edge => edge.id === `constraint-${index}`)) } } : { ...current, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, paths: value.paths.filter((_, index) => !deleted.some(edge => edge.id === `path-${index}`)) } : value) })} defaultViewport={document.layout?.viewports.find(value => value.flow === flow) ?? { x: 0, y: 0, zoom: 1 }} onMoveEnd={(_, viewport) => history.edit(current => ({ ...current, layout: { version: PROTOCOL_VERSION, nodes: current.layout?.nodes ?? [], viewports: [...(current.layout?.viewports ?? []).filter(value => value.flow !== flow), { flow, ...viewport }] } }))}><Background gap={20} color="#d8e0e5"/><Controls/></ReactFlow></div><section className="results-panel"><div className="panel-tabs"><button className="active">校验结果</button><button onClick={() => setResult(document)}>包文档</button><button onClick={() => setConnectionsOpen(true)}><Settings2 size={15}/>连接配置</button></div>{result ? <pre>{encode(result, 2)}</pre> : <div className="empty-state"><FileText size={34}/><strong>{message}</strong><span>点击上方「校验」检查包的配置。</span></div>}</section></main>
    <aside className="inspector"><h2>组件属性</h2>{item && descriptor ? <><label>组件 ID<input value={item.id} readOnly/></label><div className="inspector-actions"><strong>{descriptor.name}</strong><button aria-label="删除组件" onClick={remove}><Trash2 size={15}/></button></div><PropertyForm key={item.id} schema={descriptor.configurationSchema} value={item.config} change={configure}/></> : <p className="muted">选择画布中的组件以编辑属性。</p>}</aside></div><footer className="status-bar"><span>{message}</span><button onClick={() => void deploy(document)}>部署并执行</button></footer>
    {versions.length ? <div className="modal-backdrop"><section className="modal"><h2>打开包版本</h2>{versions.map(value => <button key={`${value.id}/${value.revision}`} onClick={() => { history.reset(value.document); setFlow(value.document.dataFlows[0]?.id ?? 'control'); setVersions([]); saved({ id: value.id, revision: String(value.revision), content: encode(value.document) }); }}>{value.id} · v{value.revision}</button>)}<button onClick={() => setVersions([])}>关闭</button></section></div> : null}
    {connectionsOpen ? <ConnectionEditor value={document.connections ?? []} close={() => setConnectionsOpen(false)} save={connections => { modifyGraph(current => ({ ...current, connections })); setConnectionsOpen(false); }}/>: null}
  </>;
}
function ConnectionEditor({ value, save, close }: { value: Connection[]; save: (value: Connection[]) => void; close: () => void }) {
  const [draft, setDraft] = useState(encode(value, 2)), [error, setError] = useState('');
  return <div className="modal-backdrop"><section className="modal"><h2>连接配置</h2><p>只保存连接资源和凭据引用。</p><textarea aria-label="连接定义" value={draft} onChange={event => setDraft(event.target.value)}/>{error ? <p role="alert">{error}</p> : null}<div><button onClick={close}>取消</button><button className="primary" onClick={() => { try { const value = decode(draft); if (!Array.isArray(value)) throw new Error('连接定义必须是数组'); save(value as Connection[]); } catch (error) { setError(String(error)); } }}>保存连接</button></div></section></div>;
}
