import { useId, useRef, useState } from 'react';
import { post } from './api';
import type { Component, Configuration, DataField, DataSchema, PackageDocument } from './model';
export interface ExpressionLanguage { operators: { id: string; symbol: string; arguments: number }[]; types: string[]; typedNull: string; }
interface Compilation { result: number; start: number; length: number; message: string; schema?: DataSchema; }
function utf8Offset(source: string, bytes: number): number {
  return new TextDecoder().decode(new TextEncoder().encode(source).subarray(0, bytes)).length;
}
const dispositions = [{ id: 'fail', label: '组件失败' }, { id: 'redirect', label: '重定向行' }, { id: 'ignore', label: '忽略失败' }];
export function FormulaEditor({ value, change, fields, functions, document, schema }: { value: string; change: (source: string) => void; fields: DataField[]; functions: Component[]; document: PackageDocument; schema?: DataSchema }) {
  const id = useId(), input = useRef<HTMLTextAreaElement>(null);
  const [check, setCheck] = useState<{ source: string; compilation?: Compilation; error?: string }>(), [busy, setBusy] = useState(false);
  function insert(text: string) {
    const at = input.current?.selectionStart ?? value.length, end = input.current?.selectionEnd ?? at;
    change(value.slice(0, at) + text + value.slice(end));
    requestAnimationFrame(() => { input.current?.focus(); input.current?.setSelectionRange(at + text.length, at + text.length); });
  }
  async function compile() {
    if (!schema) return;
    const source = value; setBusy(true);
    try { const compilation = await post<Compilation>('/expression', { package: document, schema, source }); setCheck({ source, compilation }); }
    catch (error) { setCheck({ source, error: error instanceof Error ? error.message : String(error) }); }
    finally { setBusy(false); }
  }
  const tokens = value.split(/("(?:\\.|[^"\\])*"|@\[[^\]]+\]|\[[^\]]+\]|\b[A-Za-z_][\w.]*(?=\s*\()|\bDT_[A-Z0-9]+\b|\b\d+(?:\.\d+)?[ULF]*\b|&&|\|\||==|!=|>=|<=|[+*/%&|^~!?:()-])/gi);
  const current = check?.source === value ? check : undefined, metadata = current?.compilation?.schema?.fields[0];
  return <div className="ssis-formula"><label htmlFor={id}>SSIS 公式<textarea id={id} ref={input} spellCheck={false} value={value} onChange={event => change(event.target.value)} /></label>
    <pre className="ssis-formula-highlight" aria-label="公式语法预览"><code>{tokens.map((token,index) => <span key={index} className={token.startsWith('"') ? 'formula-string' : token.startsWith('@') ? 'formula-variable' : token.startsWith('[') ? 'formula-column' : /^DT_/i.test(token) ? 'formula-type' : /^[A-Za-z_]/.test(token) ? 'formula-function' : 'formula-token'}>{token}</span>)}</code></pre>
    <div className="ssis-formula-tools"><label>插入输入列<select value="" onChange={event => { if (event.target.value) insert(`[${event.target.value.replaceAll(']', ']]')}]`); }}><option value="">选择字段</option>{fields.map(field => <option key={field.identity ?? field.name} value={field.name}>{field.name} · {field.scalar?.type ?? field.type}</option>)}</select></label>
      <label>插入注册函数<select value="" onChange={event => { if (event.target.value) insert(`${event.target.value.toUpperCase()}()`); }}><option value="">选择函数</option>{functions.map(fn => <option key={fn.id} value={fn.id}>{fn.name} · {fn.minimumArguments ?? 0}–{fn.maximumArguments ?? 0} 参数</option>)}</select></label>
      <label>插入变量<select value="" onChange={event => { if(event.target.value)insert(`@[${event.target.value}]`); }}><option value="">选择变量</option>{Object.keys(document.variables ?? {}).map(name => <option key={name} value={name}>{name}</option>)}</select></label>
      <button type="button" disabled={busy || !schema || !value} onClick={() => void compile()}>{busy ? '编译中…' : 'Native 编译'}</button>
    </div><small>{functions.filter(fn => value.toLowerCase().includes(fn.id.toLowerCase()+'(')).map(fn => `${fn.id}(${fn.minimumArguments ?? 0}–${fn.maximumArguments ?? 0} 参数)`).join(' · ')}</small>{!schema ? <small>先绑定输入字段以查看公式类型。</small> : null}
    {current?.error ? <p role="alert">{current.error}</p> : current?.compilation?.result ? <p role="alert">{current.compilation.message} <button type="button" onClick={() => { const diagnostic = current.compilation!; input.current?.focus(); input.current?.setSelectionRange(utf8Offset(value, diagnostic.start), utf8Offset(value, diagnostic.start + diagnostic.length)); }}>定位 {current.compilation.start}:{current.compilation.length}</button></p> : metadata ? <output className="ssis-inferred">{metadata.scalar?.type ?? metadata.type} · 长度 {metadata.type==='utf8'||metadata.type==='binary' ? metadata.scalar?.length || '未限定' : '—'} · 精度 {metadata.precision ?? '—'} · Scale {metadata.scale ?? '—'} · 代码页 {metadata.type==='utf8' ? metadata.scalar?.codePage || 'Unicode' : '—'} · {metadata.nullable ? '可空' : '非空'}</output> : null}
  </div>;
}
export function SSISColumnEditor({ value, change, schema, functions, document }: { value: Configuration; change: (value: Configuration) => void; schema?: DataSchema; functions: Component[]; document: PackageDocument }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const rows: Configuration[] = Array.isArray(value.columns) ? value.columns as Configuration[] : [];
  const fields = schema?.fields ?? [];
  function replacement(item: Configuration): DataField | undefined {
    const reference=String(item.inputIdentity ?? item.input ?? item.name ?? '');
    return fields.find(field => field.identity===reference || field.name===reference);
  }
  function row(index: number, update: Configuration) { change({ ...value, columns: rows.map((item, at) => at === index ? { ...item, ...update } : item) }); }
  return <section className="ssis-column-editor"><p>{rows.length} 项列写入 · 全部公式读取原始输入</p><button type="button" onClick={() => dialog.current?.showModal()}>编辑派生列</button>
    <dialog ref={dialog} className="ssis-column-dialog"><header><h2>Derived Column</h2><button type="button" onClick={() => dialog.current?.close()}>完成</button></header>
      <div className="ssis-column-table"><table><thead><tr><th>输出列</th><th>操作与输入身份</th><th>表达式与 Native 类型</th><th>Error</th><th>Truncation</th><th>操作</th></tr></thead><tbody>{rows.map((item, index) => <tr key={index}><td><input aria-label={`输出列 ${index + 1}`} value={String(item.name ?? '')} readOnly={item.action === 'replace'} onChange={event => row(index, { name: event.target.value })} /></td>
        <td><select aria-label={`列操作 ${index + 1}`} value={String(item.action ?? 'add')} onChange={event => row(index, { action: event.target.value, inputIdentity: undefined, input: undefined })}><option value="add">新增列</option><option value="replace">替换原列</option></select>
          {item.action === 'replace' ? <><select aria-label={`替换字段 ${index + 1}`} value={replacement(item)?.identity ?? replacement(item)?.name ?? ''} onChange={event => { const field = fields.find(field => (field.identity ?? field.name) === event.target.value); if (field) row(index, { name: field.name, inputIdentity: field.identity, input: field.identity ? undefined : field.name }); }}><option value="">选择原始输入列</option>{fields.map(field => <option key={field.identity ?? field.name} value={field.identity ?? field.name}>{field.name} · {field.scalar?.type ?? field.type}</option>)}</select><small>{replacement(item)?.identity ?? '先绑定输入字段'}</small></> : null}</td>
        <td>{typeof item.expression === 'object' && item.expression !== null ? <p>已保存 JSON AST。<button type="button" onClick={() => row(index, { expression: '' })}>改用公式文本</button></p> : <FormulaEditor value={String(item.expression ?? '')} change={expression => row(index, { expression, friendlyExpression: expression })} fields={fields} functions={functions} document={document} schema={schema} />}</td>
        <td><select aria-label={`Error 处置 ${index + 1}`} value={String(item.errorDisposition ?? value.errorPolicy ?? 'fail')} onChange={event => row(index, { errorDisposition: event.target.value })}>{dispositions.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}</select></td>
        <td><select aria-label={`Truncation 处置 ${index + 1}`} value={String(item.truncationDisposition ?? value.errorPolicy ?? 'fail')} onChange={event => row(index, { truncationDisposition: event.target.value })}>{dispositions.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}</select></td>
        <td><button type="button" onClick={() => change({ ...value, columns: rows.filter((_, at) => at !== index) })}>移除</button></td></tr>)}</tbody></table></div>
      <footer><button type="button" onClick={() => change({ ...value, columns: [...rows, { name: '', action: 'add', expression: '', errorDisposition: 'fail', truncationDisposition: 'fail' }] })}>新增列写入</button><label><input type="checkbox" checked={value.includeInput === true} onChange={event => change({ ...value, includeInput: event.target.checked })} />错误输出保留原始记录</label></footer>
    </dialog></section>;
}
export function SSISSplitEditor({ value, change, schema, functions, document }: { value: Configuration; change: (value: Configuration, outputRemap?: number[]) => void; schema?: DataSchema; functions: Component[]; document: PackageDocument }) {
  const rows: Configuration[] = Array.isArray(value.branches) ? value.branches as Configuration[] : [];
  function update(index: number, item: Configuration) { change({ ...value, branches: rows.map((row, at) => at === index ? { ...row, ...item } : row) }); }
  function move(index: number, delta: number) { const next = [...rows], ports = Array.from({length: rows.length + 2}, (_, at) => at); [next[index], next[index + delta]] = [next[index + delta], next[index]]; [ports[index], ports[index + delta]] = [ports[index + delta], ports[index]]; change({ ...value, branches: next }, ports); }
  function remove(index: number) { change({ ...value, branches: rows.filter((_, at) => at !== index) }, Array.from({length: rows.length + 2}, (_, at) => at === index ? -1 : at > index ? at - 1 : at)); }
  function add() { change({ ...value, branches: [...rows, { id: '', condition: '' }] }, Array.from({length: rows.length + 2}, (_, at) => at >= rows.length ? at + 1 : at)); }
  return <section className="ssis-split-editor"><h3>按序匹配首个 TRUE</h3>{rows.map((row, index) => <fieldset key={index}><legend>分支 {index + 1}</legend><label>输出名称<input value={String(row.id ?? '')} onChange={event => update(index, { id: event.target.value })} /></label>
    {typeof row.condition === 'object' && row.condition !== null ? <p>JSON AST 已保存。<button type="button" onClick={() => update(index, { condition: '' })}>改用公式文本</button></p> : <FormulaEditor value={String(row.condition ?? '')} change={condition => update(index, { condition })} fields={schema?.fields ?? []} functions={functions} document={document} schema={schema} />}
    <button type="button" disabled={index === 0} onClick={() => move(index, -1)}>上移</button><button type="button" disabled={index === rows.length - 1} onClick={() => move(index, 1)}>下移</button><button type="button" onClick={() => remove(index)}>移除</button></fieldset>)}
    <button type="button" onClick={add}>新增分支</button><label>默认出口<input value={String(value.default ?? 'default')} onChange={event => change({ ...value, default: event.target.value })} /></label>
    <label>条件错误<select value={String(value.errorPolicy ?? 'fail')} onChange={event => change({ ...value, errorPolicy: event.target.value })}>{dispositions.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}</select></label></section>;
}
