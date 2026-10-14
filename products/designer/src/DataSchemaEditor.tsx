import { PROTOCOL_VERSION } from './version';
import { DATA_TYPES } from './dataTypes';
import type { DataField, DataSchema } from './model';

export function DataSchemaEditor({ value, change }: { value: unknown; change: (value: unknown) => void }) {
  const schema: DataSchema = value && typeof value === 'object' && 'fields' in value ? value as DataSchema : { version: PROTOCOL_VERSION, fields: [] };
  return <Fields value={schema.fields} change={fields => change({ ...schema, version: PROTOCOL_VERSION, fields })}/>;
}
function childrenFor(type: string): DataField[] | undefined {
  return type === 'struct' ? [] : type === 'list' ? [{ name: 'item', type: 'utf8', nullable: true }] : type === 'map' ? [{ name: 'entries', type: 'struct', nullable: false, children: [{ name: 'key', type: 'utf8', nullable: false }, { name: 'value', type: 'utf8', nullable: true }] }] : undefined;
}
function Fields({ value, change, depth = 0 }: { value: DataField[]; change: (value: DataField[]) => void; depth?: number }) {
  function update(index: number, value: DataField) { change(valueAt(index, value)); }
  function valueAt(index: number, next: DataField) { return value.map((field, at) => at === index ? next : field); }
  return <div className="property-list">{value.map((field, index) => <fieldset className="property-group" key={field.identity ?? index}><legend>字段 {index + 1}</legend><label>名称<input value={field.name} onChange={event => update(index, { ...field, name: event.target.value })}/></label><label>数据类型<select value={field.type} onChange={event => update(index, { ...field, type: event.target.value, children: childrenFor(event.target.value), precision: event.target.value === 'decimal128' ? 38 : undefined, scale: event.target.value === 'decimal128' ? 0 : undefined, timezone: event.target.value === 'timestamp' ? 'UTC' : undefined })}>{DATA_TYPES.map(type => <option key={type}>{type}</option>)}</select></label><label className="checkbox"><input type="checkbox" checked={field.nullable} onChange={event => update(index, { ...field, nullable: event.target.checked })}/>允许为空</label>
    {field.type === 'decimal128' ? <><label>精度<input type="number" min={1} max={38} value={field.precision ?? 38} onChange={event => update(index, { ...field, precision: Number(event.target.value) })}/></label><label>小数位数<input type="number" min={-38} max={38} value={field.scale ?? 0} onChange={event => update(index, { ...field, scale: Number(event.target.value) })}/></label></> : null}
    {field.type === 'timestamp' ? <label>时区<input value={field.timezone ?? 'UTC'} onChange={event => update(index, { ...field, timezone: event.target.value })}/></label> : null}
    {field.identity ? <small className="property-help">字段身份：{field.identity}</small> : null}
    {field.children && depth < 16 ? <Fields depth={depth + 1} value={field.children} change={children => update(index, { ...field, children })}/> : null}
    <div><button type="button" disabled={index === 0} onClick={() => { const next = [...value]; [next[index - 1], next[index]] = [next[index], next[index - 1]]; change(next); }}>上移</button><button type="button" onClick={() => change(value.filter((_, at) => at !== index))}>移除字段</button></div>
  </fieldset>)}<button type="button" disabled={depth >= 16} onClick={() => change([...value, { name: `field${value.length + 1}`, type: 'utf8', nullable: true }])}>添加字段</button></div>;
}
