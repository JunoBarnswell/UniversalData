import { PROTOCOL_VERSION } from './version';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Background, Controls, ReactFlow, ReactFlowProvider, type Connection as GraphConnection, type Edge, type NodeChange, type Viewport } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Search, ChevronDown, Undo2, Redo2, Save, CircleCheck, FileText, Database, Trash2, Plus, Settings2, X } from 'lucide-react';
import { api, post, encode, decode } from './api';
import { defaults, emptyPackage, ports, isControlFlow, controlGraph, controlScopes, updateControl, type CatalogDocument, type Component, type Configuration, type PackageDocument, type Connection, type NativePlan, type NativePort } from './model';
import { useHistory } from './useHistory';
import { FlowNode, type FlowNodeValue } from './FlowNode';
import { PropertyForm } from './PropertyForm';
import { FieldMapping } from './FieldMapping';
import { SSISColumnEditor, SSISSplitEditor } from './SSISColumnEditor';
const nodeTypes = { component: FlowNode };
const constraintId = (edge: { from: string; to: string; on: string }) => `constraint/${encode([edge.from, edge.to, edge.on])}`;
const pathId = (edge: { from: string; to: string; output: number; input: number }) => `path/${encode([edge.from, edge.to, edge.output, edge.input])}`;
export function Editor({ components, saved, deploy }: { components: Component[]; saved: (value: { id: string; revision: string; content: string }) => void; deploy: (document?: PackageDocument) => Promise<void> }) {
  return <ReactFlowProvider><EditorCanvas components={components} saved={saved} deploy={deploy}/></ReactFlowProvider>;
}
function EditorCanvas({ components, saved, deploy }: { components: Component[]; saved: (value: { id: string; revision: string; content: string }) => void; deploy: (document?: PackageDocument) => Promise<void> }) {
  const history = useHistory(emptyPackage), document = history.value;
  const editHistory = history.edit;
  const [requestedFlow, setFlow] = useState('main'), [selected, setSelected] = useState<string>(), [selectedEdge, setSelectedEdge] = useState<string>();
  const scopes = useMemo(() => controlScopes(document), [document]);
  const flow = scopes.some(scope => scope.id === requestedFlow) || document.dataFlows.some(value => value.id === requestedFlow)
    ? requestedFlow : document.dataFlows[0]?.id ?? 'control';
  const [search, setSearch] = useState(''), [message, setMessage] = useState('尚未校验'), [result, setResult] = useState<NativePlan>(), [plan, setPlan] = useState<NativePlan>();
  const [busy, setBusy] = useState(false), [connectionsOpen, setConnectionsOpen] = useState(false), [documentOpen, setDocumentOpen] = useState(false), [versionsOpen, setVersionsOpen] = useState(false);
  const [catalogOpen, setCatalogOpen] = useState(false);
  const [versions, setVersions] = useState<CatalogDocument<PackageDocument>[]>([]), [binding, setBinding] = useState<Record<string, { fingerprint: string; ports: NativePort[] }>>({});
  const [dragPositions, setDragPositions] = useState<Record<string, { x: number; y: number }>>({}), [draftViewport, setDraftViewport] = useState<Viewport>();
  const catalog = useMemo(() => new Map(components.map(component => [component.id, component])), [components]);
  const control = isControlFlow(flow), graph = document.dataFlows.find(value => value.id === flow), tasks = control ? controlGraph(document, flow) : undefined;
  const items = useMemo(() => control ? tasks!.tasks.map(task => ({ id: task.id, component: task.kind, config: task.config })) : graph?.components ?? [], [control, graph, tasks]);
  function counts(id: string, descriptor: Component | undefined, config: Configuration) {
    if (control) return { inputs: 1, outputs: 1, error: -1 };
    const bound = binding[`${flow}/${id}`];
    if (bound?.fingerprint === encode(config)) return { inputs: bound.ports.filter(port => port.direction === 1).length, outputs: bound.ports.filter(port => port.direction === 2).length, error: bound.ports.find(port => port.direction === 2 && (port.flags & 2))?.index ?? -1 };
    return descriptor ? ports(descriptor) : { inputs: 0, outputs: 0, error: -1 };
  }
  const item = items.find(node => node.id === selected), descriptor = item && catalog.get(item.component);
  const boundNodes = plan?.pipelines.find(value => value.id === flow)?.trees.flatMap(tree => tree.nodes) ?? [];
  const boundNode = boundNodes.find(node => node.id === selected);
  const inputPath = graph?.paths.find(edge => edge.to === selected && edge.input === 0);
  const inputSchema = boundNode?.inputs[0] ?? (inputPath ? boundNodes.find(node => node.id === inputPath.from)?.outputs[inputPath.output] : undefined);
  const nodes: FlowNodeValue[] = items.map((item, index) => {
    const descriptor = catalog.get(item.component), position = document.layout?.nodes.find(node => node.flow === flow && node.id === item.id);
    return { id: item.id, type: 'component', selected: selected === item.id, position: dragPositions[item.id] ?? position ?? { x: 60 + index * 230, y: 180 }, data: { label: descriptor?.name ?? item.component, componentId: item.component, ...counts(item.id, descriptor, item.config) } };
  });
  const edges: Edge[] = control ? tasks!.constraints.map(edge => ({ id: constraintId(edge), source: edge.from, target: edge.to, sourceHandle: 'out-0', targetHandle: 'in-0', label: edge.on, selected: selectedEdge === constraintId(edge), style: { stroke: edge.on === 'failure' ? '#ef4444' : '#4b5563' } })) : (graph?.paths ?? []).map(edge => {
    const producer = graph?.components.find(node => node.id === edge.from), descriptor = producer && catalog.get(producer.component);
    const error = producer && edge.output === counts(producer.id, descriptor, producer.config).error;
    return { id: pathId(edge), source: edge.from, target: edge.to, sourceHandle: `out-${edge.output}`, targetHandle: `in-${edge.input}`, selected: selectedEdge === pathId(edge), style: { stroke: error ? '#ef4444' : '#4b5563', strokeWidth: 1.8 } };
  });
  function modifyGraph(update: (value: PackageDocument) => PackageDocument) { history.edit(update); setResult(undefined); setMessage('未校验的更改'); }
  function selectFlow(next: string) { setFlow(next); setSelected(undefined); setSelectedEdge(undefined); setDraftViewport(undefined); setDragPositions({}); }
  function add(component: Component) {
    setCatalogOpen(false);
    const id = `node-${crypto.randomUUID()}`, config = defaults(component.configurationSchema);
    modifyGraph(current => control ? updateControl(current, flow, graph => ({ ...graph, tasks: [...graph.tasks, { id, kind: component.id, config, ...(component.children ? { children: { tasks: [], constraints: [] } } : {}) }] })) : { ...current, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, components: [...value.components, { id, component: component.id, config }] } : value) }); setSelected(id);
  }
  function newFlow() {
    const id = `flow-${crypto.randomUUID()}`;
    modifyGraph(current => ({ ...current, dataFlows: [...current.dataFlows, { id, components: [], paths: [] }], controlFlow: { ...current.controlFlow, tasks: [...current.controlFlow.tasks, { id: `run-${id}`, kind: 'dataFlow', config: { pipeline: id } }] } })); selectFlow(id);
  }
  const changes = useCallback((updates: NodeChange[]) => {
    for (const update of updates) if (update.type === 'position' && update.position && update.dragging) setDragPositions(current => ({ ...current, [update.id]: update.position! }));
    const positions = updates.filter(update => update.type === 'position' && update.position && !update.dragging);
    if (!positions.length) return;
    editHistory(current => {
      const layout = current.layout ?? { version: PROTOCOL_VERSION, nodes: [], viewports: [] }; let nodes = layout.nodes;
      for (const update of positions) if (update.type === 'position' && update.position) nodes = [...nodes.filter(node => node.flow !== flow || node.id !== update.id), { flow, id: update.id, ...update.position }];
      return { ...current, layout: { ...layout, nodes } };
    }); setDragPositions({});
  }, [flow, editHistory]);
  function connect(connection: GraphConnection) {
    if (!connection.source || !connection.target || connection.source === connection.target) return;
    modifyGraph(current => control ? updateControl(current, flow, graph => ({ ...graph, constraints: [...graph.constraints, { from: connection.source, to: connection.target, on: 'success' }] })) : { ...current, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, paths: [...value.paths, { from: connection.source, to: connection.target, output: Number(connection.sourceHandle?.slice(4) ?? 0), input: Number(connection.targetHandle?.slice(3) ?? 0) }] } : value) });
  }
  function configure(config: Configuration, outputRemap?: number[]) {
    modifyGraph(current => control ? updateControl(current, flow, graph => ({ ...graph, tasks: graph.tasks.map(task => task.id === selected ? { ...task, config } : task) })) : { ...current, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, components: value.components.map(node => node.id === selected ? { ...node, config } : node), paths: value.paths.flatMap(path => {
      if (path.from !== selected || !outputRemap) return [path];
      const output = outputRemap[path.output];
      return output === undefined || output < 0 ? [] : [{ ...path, output }];
    }) } : value) });
    if (outputRemap) setSelectedEdge(undefined);
  }
  function remove() {
    const removedScope = control ? [...(flow.startsWith('control:') ? JSON.parse(flow.slice(8)) as string[] : []), selected] : [];
    const retainedFlow = (id: string) => {
      if (!control || !id.startsWith('control:')) return true;
      const path = JSON.parse(id.slice(8)) as string[];
      return !removedScope.every((part, index) => path[index] === part);
    };
    modifyGraph(current => ({ ...(control ? updateControl(current, flow, graph => ({ ...graph, tasks: graph.tasks.filter(task => task.id !== selected), constraints: graph.constraints.filter(edge => edge.from !== selected && edge.to !== selected) })) : { ...current, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, components: value.components.filter(node => node.id !== selected), paths: value.paths.filter(edge => edge.from !== selected && edge.to !== selected) } : value) }), layout: current.layout ? { ...current.layout, nodes: current.layout.nodes.filter(node => retainedFlow(node.flow) && (node.id !== selected || node.flow !== flow)), viewports: current.layout.viewports.filter(value => retainedFlow(value.flow)) } : undefined })); setSelected(undefined);
  }
  async function bindPorts() {
    if (!item || control) return;
    try { const value = await post<NativePort[]>('/ports', { package: document, pipelineId: flow, componentId: item.id, kind: item.component, configuration: item.config });
      setBinding(current => ({ ...current, [`${flow}/${item.id}`]: { fingerprint: encode(item.config), ports: value } })); setMessage('端口已绑定');
    } catch (error) { setMessage(String(error)); }
  }
  async function action(operation: 'save' | 'validate' | 'load') {
    setBusy(true);
    try {
      if (operation === 'validate') { const value = await post<NativePlan>('/validate', document); setPlan(value); setResult(value); setMessage('校验通过');
        const updated: typeof binding = {}; for (const pipeline of value.pipelines) for (const node of pipeline.trees.flatMap(tree => tree.nodes)) {
          const config = document.dataFlows.find(flow => flow.id === pipeline.id)?.components.find(item => item.id === node.id)?.config;
          if (config) updated[`${pipeline.id}/${node.id}`] = { fingerprint: encode(config), ports: node.ports };
        } setBinding(updated);
      }
      if (operation === 'load') { setVersions(await api('/packages')); setVersionsOpen(true); }
      if (operation === 'save') { const latest = await api<{ revision: string }>(`/packages/${encodeURIComponent(document.id)}/latest`); const revision = String(BigInt(latest.revision) + 1n);
        await api(`/packages/${encodeURIComponent(document.id)}/versions/${revision}`, { method: 'PUT', body: encode(document) }); saved({ id: document.id, revision, content: encode(document) }); setMessage(`已保存 v${revision}`); }
    } catch (error) { setMessage(String(error)); } finally { setBusy(false); }
  }
  async function nextVersions() { const last = versions.at(-1); if (!last) return;
    try { const page = await api<CatalogDocument<PackageDocument>[]>(`/packages?afterId=${encodeURIComponent(last.id)}&afterRevision=${last.revision}`); setVersions(current => [...current, ...page]); }
    catch (error) { setMessage(String(error)); }
  }
  const groups = control ? [{ category: 1, label: '任务' }] : [{ category: 2, label: '数据源' }, { category: 3, label: '转换' }, { category: 4, label: '目标' }];
  const activeConstraint = control && tasks!.constraints.find(edge => constraintId(edge) === selectedEdge);
  const viewport = draftViewport ?? document.layout?.viewports.find(value => value.flow === flow) ?? { x: 0, y: 0, zoom: 1 };
  return <><div className="editor-toolbar"><label>作业包名称<input aria-label="作业包名称" value={document.id} onChange={event => modifyGraph(current => ({ ...current, id: event.target.value }))}/></label><select aria-label="选择图" value={flow} onChange={event => selectFlow(event.target.value)}>{scopes.map(scope => <option key={scope.id} value={scope.id}>{scope.label}</option>)}{document.dataFlows.map(value => <option key={value.id} value={value.id}>数据流 · {value.id}</option>)}</select><button aria-label="新建数据流" onClick={newFlow}><Plus size={15}/></button><button className="compact-catalog-button" aria-expanded={catalogOpen} onClick={() => setCatalogOpen(value => !value)}>组件目录</button><div className="toolbar-actions"><button disabled={!history.canUndo} onClick={history.undo}><Undo2 size={15}/>撤销</button><button disabled={!history.canRedo} onClick={history.redo}><Redo2 size={15}/>重做</button><button disabled={busy} onClick={() => void action('load')}>重载</button><button disabled={busy} onClick={() => void action('save')}><Save size={15}/>保存</button><button disabled={busy} onClick={() => void action('validate')}><CircleCheck size={15}/>校验</button></div></div>
    <div className={`editor-body${catalogOpen ? " catalog-open" : ""}`}><aside className="component-rail"><label className="search"><Search size={15}/><input aria-label="搜索组件" placeholder="搜索组件" value={search} onChange={event => setSearch(event.target.value)}/></label>{groups.map(group => <section key={group.category}><h3><ChevronDown size={15}/>{group.label}</h3>{components.filter(component => component.category === group.category && `${component.id} ${component.name}`.toLowerCase().includes(search.toLowerCase())).map(component => <button className="catalog-row" key={component.id} onClick={() => add(component)}><Database size={23}/><span><strong>{component.name}</strong><small>{component.id}</small></span><Plus size={14}/></button>)}</section>)}</aside>
    <main className="editor-center"><div className="flow-canvas"><ReactFlow key={flow} nodes={nodes} edges={edges} nodeTypes={nodeTypes} onNodesChange={changes} onNodeClick={(_, node) => { setSelected(node.id); setSelectedEdge(undefined); }} onEdgeClick={(_, edge) => { setSelectedEdge(edge.id); setSelected(undefined); }} onPaneClick={() => { setSelected(undefined); setSelectedEdge(undefined); }} onConnect={connect} onEdgesDelete={deleted => modifyGraph(current => control ? updateControl(current, flow, graph => ({ ...graph, constraints: graph.constraints.filter(edge => !deleted.some(removed => removed.id === constraintId(edge))) })) : { ...current, dataFlows: current.dataFlows.map(value => value.id === flow ? { ...value, paths: value.paths.filter(edge => !deleted.some(removed => removed.id === pathId(edge))) } : value) })} viewport={viewport} onViewportChange={setDraftViewport} onMoveEnd={(_, viewport) => { history.edit(current => ({ ...current, layout: { version: PROTOCOL_VERSION, nodes: current.layout?.nodes ?? [], viewports: [...(current.layout?.viewports ?? []).filter(value => value.flow !== flow), { flow, ...viewport }] } })); setDraftViewport(undefined); }}><Background gap={20} color="#d8e0e5"/><Controls/></ReactFlow></div><section className="results-panel"><div className="panel-tabs"><button className="active">校验结果</button><button onClick={() => setDocumentOpen(true)}>包配置</button><button onClick={() => setConnectionsOpen(true)}><Settings2 size={15}/>连接配置</button></div>{result ? <div className="validation-summary"><p className="validation-success">校验通过，组件与字段已绑定。</p><table><thead><tr><th>数据流 / 组件</th><th>输入字段</th><th>输出字段</th></tr></thead><tbody>{result.pipelines.flatMap(pipeline => pipeline.trees.flatMap(tree => tree.nodes.map(node => <tr key={`${pipeline.id}/${node.id}`}><td>{pipeline.id} / {node.id}</td><td>{node.inputs.map(schema => schema?.fields.map(field => field.name).join(", ")).filter(Boolean).join(" · ") || "—"}</td><td>{node.outputs.map(schema => schema.fields.map(field => `${field.name}: ${field.type}`).join(", ")).join(" · ") || "—"}</td></tr>)))}</tbody></table></div> : <div className="empty-state"><FileText size={34}/><strong>{message}</strong><span>配置组件并连接数据流后，点击「校验」。</span></div>}</section></main>
    <aside className="inspector"><h2>组件属性<button aria-label="关闭属性" onClick={() => { setSelected(undefined); setSelectedEdge(undefined); }}><X size={16}/></button></h2>{item && descriptor ? <><label>组件 ID<input value={item.id} readOnly/></label><div className="inspector-actions"><strong>{descriptor.name}</strong><button aria-label="删除组件" onClick={remove}><Trash2 size={15}/></button></div>{item.component === 'derive' ? <SSISColumnEditor value={item.config} change={configure} schema={inputSchema ?? undefined} functions={components.filter(component => component.category === 5)} document={document}/> : item.component === 'split' && item.config.condition === undefined ? <SSISSplitEditor value={item.config} change={configure} schema={inputSchema ?? undefined} functions={components.filter(component => component.category === 5)} document={document}/> : <PropertyForm key={item.id} schema={descriptor.configurationSchema} value={item.config} change={configure} fields={inputSchema?.fields} inputs={boundNode?.inputs.map(schema => schema?.fields ?? [])}/>} {!control && descriptor.dynamicPorts ? <button onClick={() => void bindPorts()}>绑定端口</button> : null}{item.component === 'copyColumn' && inputSchema ? <FieldMapping key={item.id} fields={inputSchema.fields} value={item.config} change={configure}/> : null}{boundNode ? <section><h3>已绑定字段</h3>{boundNode.outputs.map((schema, index) => <div key={index}><h4>输出 {index}</h4>{schema.fields.map(field => <p key={field.identity ?? field.name}>{field.name} · {field.type}{field.nullable ? ' · 可空' : ''}</p>)}</div>)}</section> : null}</> : activeConstraint ? <label>前置条件<select value={activeConstraint.on} onChange={event => modifyGraph(current => updateControl(current, flow, graph => ({ ...graph, constraints: graph.constraints.map(edge => constraintId(edge) === selectedEdge ? { ...edge, on: event.target.value as 'success' | 'failure' | 'completion' } : edge) })))}><option value="success">成功</option><option value="failure">失败</option><option value="completion">完成</option></select></label> : <p className="muted">选择组件或连线以编辑属性。</p>}</aside></div><footer className="status-bar"><span>{message}</span><button onClick={() => void deploy(document)}>部署并执行</button></footer>
    {versionsOpen ? <div className="modal-backdrop"><section className="modal"><h2>打开包版本</h2>{versions.map(value => <button key={`${value.id}/${value.revision}`} onClick={() => { history.reset(value.document); selectFlow(value.document.dataFlows[0]?.id ?? 'control'); setVersionsOpen(false); setPlan(undefined); setBinding({}); saved({ id: value.id, revision: String(value.revision), content: encode(value.document) }); }}>{value.id} · v{value.revision}</button>)}{!versions.length ? <p>暂无保存的版本</p> : null}<button onClick={() => void nextVersions()} disabled={!versions.length || versions.length % 100 !== 0}>更多版本</button><button onClick={() => setVersionsOpen(false)}>关闭</button></section></div> : null}
    {connectionsOpen ? <JsonEditor title="连接配置" value={document.connections ?? []} close={() => setConnectionsOpen(false)} save={value => { if (!Array.isArray(value)) throw new Error('连接定义必须是数组'); modifyGraph(current => ({ ...current, connections: value as Connection[] })); setConnectionsOpen(false); }}/>: null}
    {documentOpen ? <JsonEditor title="包配置" value={document} close={() => setDocumentOpen(false)} save={value => { const next = value as PackageDocument; if (next.version !== PROTOCOL_VERSION || !Array.isArray(next.dataFlows) || !next.controlFlow) throw new Error('包格式无效'); history.edit(() => next); selectFlow(next.dataFlows[0]?.id ?? 'control'); setDocumentOpen(false); setPlan(undefined); setBinding({}); }}/>: null}
  </>;
}
function JsonEditor({ title, value, save, close }: { title: string; value: unknown; save: (value: unknown) => void; close: () => void }) {
  const [draft, setDraft] = useState(encode(value, 2)), [error, setError] = useState('');
  useEffect(() => { const key = (event: KeyboardEvent) => { if (event.key === 'Escape') close(); }; window.addEventListener('keydown', key); return () => window.removeEventListener('keydown', key); }, [close]);
  return <div className="modal-backdrop"><section className="modal" role="dialog" aria-modal="true" aria-label={title}><h2>{title}</h2><textarea aria-label={title} value={draft} onChange={event => setDraft(event.target.value)}/>{error ? <p role="alert">{error}</p> : null}<div><button onClick={close}>取消</button><button className="primary" onClick={() => { try { save(decode(draft)); } catch (error) { setError(String(error)); } }}>应用</button></div></section></div>;
}
