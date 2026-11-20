import { Button, Input, Select, TextArea } from "./shared/design-system/Controls";
import { useId, useState } from 'react';
import { decode, encode } from './api';
import { DataSchemaEditor } from './DataSchemaEditor';
import type { Configuration, DataField, PropertySchema } from './model';

type Context = { fields: DataField[]; inputs: DataField[][] };
const empty: DataField[] = [];
const captions: Record<string, string> = {mode:"运行模式",timeField:"事件时间字段",leftTimeField:"左侧事件时间字段",rightTimeField:"右侧事件时间字段",ttlMicros:"去重有效期（微秒）",withinMicros:"最大时间差（微秒）",sizeMicros:"窗口大小（微秒）",slideMicros:"滑动步长（微秒）",gapMicros:"会话间隔（微秒）",allowedLatenessMicros:"允许迟到（微秒）",latePolicy:"迟到处理",maxWindows:"最大活动窗口数",stateBytes:"状态预算（字节）",lagMicros:"乱序容忍（微秒）", connection: '连接', schema: '数据结构', errorPolicy: '异常行处理', includeInput: '保留原始输入', keys: '键字段', columns: '输出字段', table: '表', batchSize: '批次行数',
  leftKeys: '左侧连接键', rightKeys: '右侧连接键', joinType: '连接类型', groups: '分组字段', aggregates: '聚合计算', groupBy: '分组字段', expression: '表达式', condition: '条件', branches: '分流规则',
  businessKeys: '业务键', valueColumn: '值字段', referenceKeys: '引用字段', inputKeys: '输入字段', noMatch: '未命中处理', statement: 'SQL 命令', parameters: '参数映射', rules: '质量规则',
  url: '地址', method: '请求方法', headers: '请求头', body: '请求内容', pagination: '分页', fields: '字段', nullable: '允许为空', name: '名称', type: '类型', source: '源字段', target: '目标字段',
  contentType: '内容类型', expectedStatus: '预期 HTTP 状态码', responseHeaders: '记录的响应头',
  statusVariable: '响应状态变量', bodyVariable: '响应内容变量', headersVariable: '响应头变量',
  variable: '运行变量', credentialReference: '凭据引用', timeoutMs: '超时（毫秒）',
  function: '函数', column: '字段', precision: '精度', scale: '小数位数', readerSchema: '读取结构', maxRows: '最大行数', partitions: '分区数', topic: '主题', offset: '开始位置', format: '格式',
  targetType: '消息目标类型', address: '地址', group: '组', subscription: '订阅',
  routingKey: '路由键', filter: '订阅筛选', initialPosition: '起始位点', prefetch: '预取消息数',
  maxMessageBytes: '单条消息上限（字节）', maxInFlightBytes: '未确认消息上限（字节）',
  registryConnection: 'Schema Registry 连接', schemaSubject: 'Schema Subject', schemaVersion: 'Schema 版本', readMode: '读取方式', delivery: '交付方式' };
export function PropertyForm({ schema, value, change, fields = empty, inputs = [] }: { schema: PropertySchema; value: Configuration; change: (value: Configuration) => void; fields?: DataField[]; inputs?: DataField[][] }) {
  return <ObjectProperties schema={schema} value={value} change={change} context={{ fields, inputs }} path=""/>;
}
function typeOf(value: unknown): string {
  return Array.isArray(value) ? 'array' : value === null ? 'null' : typeof value === 'object' ? 'object' : typeof value === 'number' || typeof value === 'bigint' ? 'number' : typeof value === 'boolean' ? 'boolean' : 'string';
}
function initial(schema: PropertySchema): unknown {
  if (schema.default !== undefined) return structuredClone(schema.default);
  if (schema.enum?.length) return schema.enum[0];
  const type = Array.isArray(schema.type) ? schema.type.find(type => type !== 'null') : schema.type;
  return type === 'object' ? {} : type === 'array' ? [] : type === 'null' ? null : type === 'boolean' ? false : type === 'integer' || type === 'number' ? 0 : '';
}
function ObjectProperties({ schema, value, change, context, path }: { schema: PropertySchema; value: Configuration; change: (value: Configuration) => void; context: Context; path: string }) {
  const [newName, setName] = useState(''), [newType, setType] = useState('string');
  const names = [...new Set([...Object.keys(schema.properties ?? {}), ...Object.keys(value)])];
  const dynamic = schema.additionalProperties !== false;
  return <div className="property-form">{names.map(name => {
    const definition = schema.properties?.[name] ?? (typeof schema.additionalProperties === 'object' ? schema.additionalProperties : { type: typeOf(value[name]) });
    return <div className="property-entry" key={name}><Property name={name} path={`${path}/${name}`} schema={definition} value={value[name]} required={schema.required?.includes(name) ?? false} context={context} change={next => { const result = { ...value }; if (next === undefined) delete result[name]; else result[name] = next; change(result); }}/>
      {!schema.properties?.[name] || !(schema.required ?? []).includes(name) && value[name] !== undefined ? <Button type="button" className="text-button" onClick={() => { const next = { ...value }; delete next[name]; change(next); }}>移除 {captions[name] ?? name}</Button> : null}</div>;
  })}{dynamic ? <div className="property-entry"><label>添加属性<Input value={newName} onChange={event => setName(event.target.value)} placeholder="属性名称"/></label><label>值类型<Select value={newType} onChange={event => setType(event.target.value)}><option value="string">文本</option><option value="number">数值</option><option value="boolean">开关</option><option value="object">对象</option><option value="array">列表</option></Select></label><Button type="button" disabled={!newName || Object.hasOwn(value, newName)} onClick={() => { change({ ...value, [newName]: initial({ type: newType }) }); setName(''); }}>添加属性</Button></div> : null}</div>;
}
function Property({ name, path, schema, value, required, change, context }: { name: string; path: string; schema: PropertySchema; value: unknown; required: boolean; change: (value: unknown) => void; context: Context }) {
  const id = useId();
  const label = `${captions[name] ?? schema.title ?? name}${required ? ' *' : ''}`;
  const type = Array.isArray(schema.type) ? schema.type.find(type => type !== 'null') : schema.type ?? typeOf(value);
  const available = /right|reference/i.test(path) ? context.inputs[1] ?? context.fields : context.fields;
  if(schema['x-role']==='field-selector'&&available.length) return <label htmlFor={id}>{label}<Select id={id} value={typeof value==='number'?(available[value]?.identity??available[value]?.name??''):String(value??'')} onChange={event=>change(event.target.value||undefined)}><option value="">选择字段</option>{available.map((field)=><option key={field.identity??field.name} value={field.identity??field.name}>{field.name} · {field.type}</option>)}</Select></label>;
  const variants = schema.oneOf ?? schema.anyOf;
  if (variants?.length) return <fieldset className="property-group"><legend>{label}</legend><Variant name={name} path={path} choices={variants} value={value} change={change} context={context}/></fieldset>;
  if (schema.enum) {
    const selected = value === undefined ? schema.default : value;
    return <label htmlFor={id}>{label}<Select id={id}
      value={selected === undefined ? '' : encode(selected)}
      onChange={event => change(event.target.value ? decode(event.target.value) : undefined)}>
      <option value="">{name === 'delivery' && schema.enum.includes('receipt') ? '随作业模式' : '请选择'}</option>
      {schema.enum.map(option => <option key={encode(option)} value={encode(option)}>{String(option)}</option>)}
    </Select></label>;
  }
  if (type === 'boolean') return <label className="checkbox" htmlFor={id}><Input id={id} type="checkbox" checked={value === true} onChange={event => change(event.target.checked)}/>{label}</label>;
  if (['expression', 'condition', 'predicate'].includes(name) || value && typeof value === 'object' && !Array.isArray(value) && ['op', 'literal', 'ref', 'column', 'function'].some(key => Object.hasOwn(value, key)))
    return <fieldset className="property-group"><legend>{label}</legend><Expression value={value as Configuration | undefined} change={change} context={context}/></fieldset>;
  if (name === 'schema' && type === 'object') return <fieldset className="property-group"><legend>{label}</legend><DataSchemaEditor value={value} change={change}/></fieldset>;
  if (schema.type === undefined && schema.properties === undefined) return <fieldset className="property-group"><legend>{label}</legend><label>值类型<Select value={typeOf(value)} onChange={event => change(initial({ type: event.target.value }))}>{['string', 'number', 'boolean', 'object', 'array', 'null'].map(kind => <option key={kind} value={kind}>{({ string: '文本', number: '数值', boolean: '开关', object: '对象', array: '列表', null: '空值' })[kind]}</option>)}</Select></label>{value === null ? null : <Property name={name} path={path} schema={{ ...schema, type: typeOf(value) }} value={value} required={required} context={context} change={change}/>}</fieldset>;
  if (type === 'object') return <fieldset className="property-group"><legend>{label}</legend><ObjectProperties schema={schema} value={value && typeof value === 'object' && !Array.isArray(value) ? value as Configuration : {}} change={change} context={context} path={path}/></fieldset>;
  if (type === 'array') {
    const entries = Array.isArray(value) ? value : [], itemSchema = schema.items ?? {};
    return <fieldset className="property-group"><legend>{label}</legend><div className="property-list">{entries.map((entry, index) => <div className="property-entry" key={index}><header><strong>{index + 1}</strong><div><Button type="button" disabled={index === 0} onClick={() => { const next = [...entries]; [next[index - 1], next[index]] = [next[index], next[index - 1]]; change(next); }}>上移</Button><Button type="button" onClick={() => change(entries.filter((_, at) => at !== index))}>移除</Button></div></header><Property name={name} path={`${path}/${index}`} schema={itemSchema} value={entry} required context={context} change={next => change(entries.map((entry, at) => at === index ? next : entry))}/></div>)}</div><Button type="button" disabled={schema.maxItems !== undefined && entries.length >= schema.maxItems} onClick={() => change([...entries, initial(itemSchema)])}>添加{captions[name] ?? name}</Button></fieldset>;
  }
  if ((type === 'integer' || type === 'number') && /column|keys|groups|parameters|partition/i.test(path) && available.length)
    return <label htmlFor={id}>{label}<Select id={id} value={value === undefined ? '' : String(value)} onChange={event => change(event.target.value ? Number(event.target.value) : undefined)}><option value="">选择字段</option>{available.map((field, index) => <option key={field.identity ?? `${field.name}/${index}`} value={index}>{field.name} · {field.type}</option>)}</Select></label>;
  if (name === 'statement' || name === 'sql' || name === 'xpath' || name === 'xslt' || name === 'xsd') return <label htmlFor={id}>{label}<TextArea id={id} value={typeof value === 'string' ? value : ''} onChange={event => change(event.target.value)} spellCheck={false}/></label>;
  return <label htmlFor={id}>{label}<Input id={id} required={required} type={type === 'integer' || type === 'number' ? 'number' : 'text'} min={schema.minimum} max={schema.maximum} step={type === 'integer' ? 1 : 'any'} value={typeof value === 'string' || typeof value === 'number' || typeof value === 'bigint' ? String(value) : ''} onChange={event => change(type === 'integer' || type === 'number' ? event.target.value ? decode(event.target.value) : undefined : event.target.value)}/>{schema.description ? <small className="property-help">{schema.description}</small> : null}</label>;
}
function Variant({ name, path, choices, value, change, context }: { name: string; path: string; choices: PropertySchema[]; value: unknown; change: (value: unknown) => void; context: Context }) {
  const [selected, setSelected] = useState(() => Math.max(0, choices.findIndex(schema => schema.type === typeOf(value))));
  return <><label>配置方式<Select value={selected} onChange={event => { const index = Number(event.target.value); setSelected(index); change(initial(choices[index])); }}>{choices.map((choice, index) => <option key={index} value={index}>{choice.title ?? choice.type ?? `选项 ${index + 1}`}</option>)}</Select></label><Property name={name} path={path} schema={choices[selected]} value={value} change={change} context={context} required/></>;
}
function Expression({ value, change }: { value?: unknown; change: (value: unknown) => void; context: Context; depth?: number }) {
  if(typeof value==='object'&&value!==null) return <><pre className="property-help">{encode(value)}</pre><Button type="button" onClick={()=>change('')}>使用 SSIS 公式文本</Button></>;
  return <label>SSIS 公式<TextArea spellCheck={false} value={typeof value==='string'?value:''} onChange={event=>change(event.target.value)}/></label>;
}
