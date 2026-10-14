import { useState } from 'react';
import type { Configuration, DataField } from './model';
export function FieldMapping({ fields, value, change }: { fields: DataField[]; value: Configuration; change: (value: Configuration) => void }) {
  const [source, setSource] = useState(fields[0]?.name ?? ''), [name, setName] = useState('');
  const columns = Array.isArray(value.columns) ? value.columns as { column: string; name: string }[] : [];
  return <section className="field-mapping"><h3>字段复制映射</h3><table><thead><tr><th>源字段</th><th>输出名称</th><th>操作</th></tr></thead><tbody>{columns.map((column, index) => <tr key={`${column.column}/${column.name}`}><td>{column.column}</td><td>{column.name}</td><td><button aria-label={`删除映射 ${column.name}`} onClick={() => change({ ...value, columns: columns.filter((_, at) => at !== index) })}>删除</button></td></tr>)}</tbody></table><label>源字段<select value={source} onChange={event => setSource(event.target.value)}><option value="">选择字段</option>{fields.map(field => <option key={field.identity ?? field.name} value={field.name}>{field.name} · {field.type}</option>)}</select></label><label>输出名称<input value={name} onChange={event => setName(event.target.value)}/></label><button disabled={!source || !name} onClick={() => { change({ ...value, columns: [...columns, { column: source, name }] }); setName(''); }}>添加映射</button></section>;
}
