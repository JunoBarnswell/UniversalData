import { Plus, Trash2 } from "lucide-react";
import { Button, Input, Select } from "../../shared/design-system/Controls";
import type { MetricDefinition, MetricOperation, Rule } from "./model";

const operationLabels: Record<MetricOperation, string> = {
  count: "计数",
  sum: "求和",
  distinct: "精确去重",
  add: "相加",
  subtract: "相减",
  divide: "相除",
};
const builtinMetrics = [
  { id: "records", label: "记录数" },
  { id: "amount", label: "金额" },
  { id: "orders", label: "订单数" },
  { id: "average", label: "平均值" },
];

function newMetric(fields: Rule[], metrics: MetricDefinition[]): MetricDefinition {
  const amount = fields.find((field) => field.role === "amount");
  const first = metrics[0]?.id ?? "records";
  const second = metrics[1]?.id ?? "amount";
  const metric: MetricDefinition = {
    id: `metric-${crypto.randomUUID().replaceAll("-", "")}`,
    label: "新指标",
    operation: amount ? "sum" : "count",
    unit: "",
  };
  if (amount) metric.fieldId = amount.fieldId;
  else {
    metric.left = first;
    metric.right = second;
  }
  return metric;
}

function operandOptions(metrics: MetricDefinition[], current: string): { id: string; label: string }[] {
  return [...builtinMetrics, ...metrics.filter((metric) => metric.id !== current).map((metric) => ({ id: metric.id, label: metric.label }))];
}

export function MetricEditor({ metrics, fields, disabled, onChange }: {
  metrics: MetricDefinition[] | undefined;
  fields: Rule[];
  disabled: boolean;
  onChange(metrics: MetricDefinition[]): void;
}) {
  const current = metrics ?? [];
  const amountFields = fields.filter((field) => field.role === "amount");
  const referenceFields = fields.filter((field) => field.role !== "omit");

  function update(index: number, value: Partial<MetricDefinition>) {
    onChange(current.map((metric, at) => at === index ? { ...metric, ...value } : metric));
  }
  function changeOperation(index: number, operation: MetricOperation) {
    const metric = current[index];
    if (operation === "count") update(index, { operation, fieldId: undefined, left: undefined, right: undefined });
    else if (operation === "sum") update(index, { operation, fieldId: metric.fieldId ?? amountFields[0]?.fieldId, left: undefined, right: undefined });
    else if (operation === "distinct") update(index, { operation, fieldId: metric.fieldId ?? referenceFields[0]?.fieldId, left: undefined, right: undefined });
    else update(index, { operation, fieldId: undefined, left: metric.left ?? operandOptions(current, metric.id)[0]?.id, right: metric.right ?? operandOptions(current, metric.id)[1]?.id });
  }
  function remove(index: number) {
    onChange(current.filter((_, at) => at !== index));
  }
  function add() {
    onChange([...current, newMetric(fields, current)]);
  }

  return <section className="analysis-panel analysis-metric-editor" aria-label="自定义指标">
    <div className="analysis-section-heading"><div><h2>自定义指标</h2><p>为多个金额字段分别建指标，或用已有指标组合比率。指标标识在保存后固定，改名不会破坏已发布卡片。</p></div><Button onClick={add} disabled={disabled}><Plus size={15} />新增指标</Button></div>
    {current.length === 0 ? <p className="analysis-empty compact">还没有自定义指标。默认记录数、金额、订单数等内置指标仍可用。</p> : <div className="analysis-metric-list">{current.map((metric, index) => <MetricRow key={metric.id} metric={metric} index={index} metrics={current} amountFields={amountFields} referenceFields={referenceFields} disabled={disabled} onUpdate={(value) => update(index, value)} onOperation={(operation) => changeOperation(index, operation)} onRemove={() => remove(index)} />)}</div>}
  </section>;
}

function MetricRow({ metric, index, metrics, amountFields, referenceFields, disabled, onUpdate, onOperation, onRemove }: {
  metric: MetricDefinition;
  index: number;
  metrics: MetricDefinition[];
  amountFields: Rule[];
  referenceFields: Rule[];
  disabled: boolean;
  onUpdate(value: Partial<MetricDefinition>): void;
  onOperation(operation: MetricOperation): void;
  onRemove(): void;
}) {
  const operands = operandOptions(metrics, metric.id);
  const needsField = metric.operation === "sum" || metric.operation === "distinct";
  const derived = metric.operation === "add" || metric.operation === "subtract" || metric.operation === "divide";
  const selectableFields = metric.operation === "sum" ? amountFields : referenceFields;
  return <article className="analysis-metric-row">
    <div className="analysis-metric-row-heading"><strong>指标 {index + 1}</strong><code>{metric.id}</code><Button aria-label={`删除${metric.label}`} onClick={onRemove} disabled={disabled}><Trash2 size={14} /></Button></div>
    <div className="analysis-metric-grid">
      <label>显示名称<Input value={metric.label} maxLength={128} disabled={disabled} onChange={(event) => onUpdate({ label: event.target.value })} /></label>
      <label>计算方式<Select value={metric.operation} disabled={disabled} onChange={(event) => onOperation(event.target.value as MetricOperation)}>{Object.entries(operationLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</Select></label>
      {needsField && <label>{metric.operation === "sum" ? "金额字段" : "去重字段"}<Select value={metric.fieldId ?? ""} disabled={disabled} onChange={(event) => onUpdate({ fieldId: event.target.value || undefined })}><option value="">请选择字段</option>{selectableFields.map((field) => <option key={field.fieldId} value={field.fieldId}>{field.label}</option>)}</Select></label>}
      {metric.operation === "count" && <label>计数字段（可选）<Select value={metric.fieldId ?? ""} disabled={disabled} onChange={(event) => onUpdate({ fieldId: event.target.value || undefined })}><option value="">每行计数</option>{referenceFields.map((field) => <option key={field.fieldId} value={field.fieldId}>{field.label}</option>)}</Select></label>}
      {derived && <><label>左操作数<Select value={metric.left ?? ""} disabled={disabled} onChange={(event) => onUpdate({ left: event.target.value || undefined })}><option value="">请选择指标</option>{operands.map((operand) => <option key={operand.id} value={operand.id}>{operand.label}</option>)}</Select></label><label>右操作数<Select value={metric.right ?? ""} disabled={disabled} onChange={(event) => onUpdate({ right: event.target.value || undefined })}><option value="">请选择指标</option>{operands.map((operand) => <option key={operand.id} value={operand.id}>{operand.label}</option>)}</Select></label></>}
      <label>单位（可选）<Input value={metric.unit ?? ""} maxLength={64} disabled={disabled} placeholder={metric.operation === "divide" ? "%" : "例如 CNY"} onChange={(event) => onUpdate({ unit: event.target.value || undefined })} /></label>
    </div>
  </article>;
}
