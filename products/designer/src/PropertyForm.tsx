import { encode, decode } from './api';
import { useState } from 'react';
import type { Configuration, PropertySchema } from './model';
export function PropertyForm({ schema, value, change }: { schema: PropertySchema; value: Configuration; change: (value: Configuration) => void }) {
  return <div className="property-form">{Object.entries(schema.properties ?? {}).map(([name, definition]) => <Property key={name} name={name} schema={definition} value={value[name]} required={schema.required?.includes(name) ?? false} change={next => change({ ...value, [name]: next })}/>)}</div>;
}
function Property({ name, schema, value, required, change }: { name: string; schema: PropertySchema; value: unknown; required: boolean; change: (value: unknown) => void }) {
  const id = `property-${name}`;
  const captions: Record<string, string> = { connection: '连接', schema: '数据 Schema', errorPolicy: '错误策略', includeInput: '保留原始输入', keys: '键字段', columns: '字段映射', table: '目标表', batchSize: '批次行数' };
  const label = `${captions[name] ?? schema.title ?? name}${required ? ' *' : ''}`;
  if (schema.type === 'boolean') return <label className="checkbox" htmlFor={id}><input id={id} type="checkbox" checked={value === true} onChange={event => change(event.target.checked)}/>{label}</label>;
  if (schema.enum) return <label htmlFor={id}>{label}<select id={id} value={String(value ?? '')} onChange={event => change(event.target.value)}><option value="">请选择</option>{schema.enum.map(option => <option key={String(option)} value={String(option)}>{String(option)}</option>)}</select></label>;
  if (schema.type === 'object' || schema.type === 'array') return <JsonProperty key={encode(value)} label={label} id={id} value={value} change={change}/>;
  return <label htmlFor={id}>{label}<input id={id} type={schema.type === 'integer' || schema.type === 'number' ? 'number' : 'text'} value={typeof value === 'string' || typeof value === 'number' || typeof value === 'bigint' ? String(value) : ''} onChange={event => change(schema.type === 'integer' || schema.type === 'number' ? event.target.value ? decode(event.target.value) : undefined : event.target.value)}/></label>;
}
function JsonProperty({ label, id, value, change }: { label: string; id: string; value: unknown; change: (value: unknown) => void }) {
  const [draft, setDraft] = useState(() => value === undefined ? '' : encode(value, 2));
  const [error, setError] = useState('');
  return <label htmlFor={id}>{label}<textarea id={id} value={draft} aria-invalid={Boolean(error)} onChange={event => setDraft(event.target.value)} onBlur={() => { try { change(draft ? decode(draft) : undefined); setError(''); } catch { setError('请输入有效 JSON'); } }}/>{error ? <small role="alert">{error}</small> : null}</label>;
}
