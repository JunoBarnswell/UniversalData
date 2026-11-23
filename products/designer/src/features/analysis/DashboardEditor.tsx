import { ChevronLeft, ChevronRight, Gauge, Plus, Save, Share2, Trash2 } from "lucide-react";
import { useState } from "react";
import { Button, Input, Select } from "../../shared/design-system/Controls";
import { chartLabels, comparisonLabels, dimensionLabel } from "./dashboard-utils";
import type { Dashboard, DashboardCard, DashboardChart, Comparison, Dimension, Measure, Rule } from "./model";

export function DashboardEditor({ dashboard, saving, onSave, onPublish, onPreview }: {
  dashboard: Dashboard;
  saving: boolean;
  onSave(name: string, cards: DashboardCard[]): Promise<Dashboard | undefined>;
  onPublish(): Promise<void>;
  onPreview(name: string, cards: DashboardCard[]): Promise<void>;
}) {
  const [name, setName] = useState(dashboard.name);
  const [cards, setCards] = useState(dashboard.cards);
  const dimensions = dashboardDimensions(dashboard.fields);

  function update(index: number, value: Partial<DashboardCard>) {
    setCards((current) => current.map((card, at) => at === index ? { ...card, ...value } : card));
  }
  function remove(index: number) {
    setCards((current) => current.filter((_, at) => at !== index));
  }
  function move(index: number, direction: -1 | 1) {
    setCards((current) => {
      const next = current.slice();
      const target = index + direction;
      if (target < 0 || target >= next.length) return next;
      [next[index], next[target]] = [next[target], next[index]];
      return next;
    });
  }
  function add() {
    const measureId = dashboard.measures[0]?.id ?? "";
    setCards((current) => [...current, {
      id: crypto.randomUUID(), title: "新指标", chart: "kpi", measureId, dimensions: [], comparison: "none",
    }]);
  }

  return <section className="dashboard-editor" aria-label="编辑仪表盘">
    <div className="dashboard-section-title"><div><h2>编辑仪表盘</h2><p>修改草稿后保存，再发布一个可供团队读取的稳定版本。</p></div><span className="dashboard-revision">草稿版本 {dashboard.revision}</span></div>
    <div className="dashboard-editor-name"><label>仪表盘名称<Input value={name} maxLength={128} onChange={(event) => setName(event.target.value)} /></label></div>
    <div className="dashboard-editor-cards">
      {cards.map((card, index) => <CardEditor key={card.id} card={card} index={index} total={cards.length} measures={dashboard.measures} fields={dashboard.fields} dimensions={dimensions} saving={saving} onChange={(value) => update(index, value)} onRemove={() => remove(index)} onMove={move} />)}
    </div>
    <div className="dashboard-editor-actions"><Button onClick={add} disabled={saving}><Plus size={15} />添加组件</Button><span className="dashboard-editor-spacer" /><Button disabled={saving || !cards.length || !name.trim()} onClick={() => void onPreview(name.trim(), cards)}><Gauge size={15} />预览草稿</Button><Button className="analysis-primary" disabled={saving || !cards.length || !name.trim()} onClick={() => void onSave(name.trim(), cards)}><Save size={15} />保存草稿</Button><Button disabled={saving || !cards.length || !name.trim()} onClick={() => void onPublish()}><Share2 size={15} />发布版本</Button></div>
  </section>;
}

function dashboardDimensions(fields: Rule[]): Dimension[] {
  const dimensions: Dimension[] = [];
  for (const field of fields) {
    if (field.role === "date") dimensions.push({ fieldId: field.fieldId, level: "month" }, { fieldId: field.fieldId, level: "day" });
    if (field.role === "category") dimensions.push({ fieldId: field.fieldId, level: "value" });
  }
  return dimensions;
}

function CardEditor({ card, index, total, measures, fields, dimensions, saving, onChange, onRemove, onMove }: {
  card: DashboardCard;
  index: number;
  total: number;
  measures: Measure[];
  fields: Rule[];
  dimensions: Dimension[];
  saving: boolean;
  onChange(value: Partial<DashboardCard>): void;
  onRemove(): void;
  onMove(index: number, direction: -1 | 1): void;
}) {
  const selectableDimensions = card.chart === "line" ? dimensions.filter((dimension) => dimension.level !== "value") : dimensions;
  return <article className="dashboard-card-editor">
    <div className="dashboard-card-editor-heading"><strong>组件 {index + 1}</strong><div><Button aria-label="上移组件" disabled={saving || index === 0} onClick={() => onMove(index, -1)}><ChevronLeft size={14} /></Button><Button aria-label="下移组件" disabled={saving || index === total - 1} onClick={() => onMove(index, 1)}><ChevronRight size={14} /></Button><Button aria-label="删除组件" disabled={saving} onClick={onRemove}><Trash2 size={14} /></Button></div></div>
    <div className="dashboard-card-editor-grid"><label>标题<Input value={card.title} maxLength={128} disabled={saving} onChange={(event) => onChange({ title: event.target.value })} /></label><label>指标<Select value={card.measureId} disabled={saving} onChange={(event) => onChange({ measureId: event.target.value })}>{measures.map((measure) => <option key={measure.id} value={measure.id}>{measure.label}</option>)}</Select></label><label>图表<Select value={card.chart} disabled={saving} onChange={(event) => onChange({ chart: event.target.value as DashboardChart, dimensions: event.target.value === "kpi" ? [] : card.dimensions.slice(0, 1) })}>{Object.entries(chartLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</Select></label><label>比较<Select value={card.comparison} disabled={saving} onChange={(event) => onChange({ comparison: event.target.value as Comparison })}>{Object.entries(comparisonLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</Select></label></div>
    {card.chart !== "kpi" && <div className="dashboard-card-dimensions"><label>第一维度<Select value={card.dimensions[0] ? `${card.dimensions[0].fieldId}/${card.dimensions[0].level}` : ""} disabled={saving} onChange={(event) => onChange({ dimensions: replaceDimension(card.dimensions, 0, event.target.value) })}><option value="">选择维度</option>{selectableDimensions.map((dimension) => <option key={`${dimension.fieldId}/${dimension.level}`} value={`${dimension.fieldId}/${dimension.level}`}>{dimensionLabel(fields, dimension)}</option>)}</Select></label>{card.chart === "table" && <label>第二维度（可选）<Select value={card.dimensions[1] ? `${card.dimensions[1].fieldId}/${card.dimensions[1].level}` : ""} disabled={saving} onChange={(event) => onChange({ dimensions: replaceDimension(card.dimensions, 1, event.target.value) })}><option value="">无</option>{selectableDimensions.filter((dimension) => dimension.fieldId !== card.dimensions[0]?.fieldId).map((dimension) => <option key={`${dimension.fieldId}/${dimension.level}`} value={`${dimension.fieldId}/${dimension.level}`}>{dimensionLabel(fields, dimension)}</option>)}</Select></label>}</div>}
  </article>;
}

function replaceDimension(current: Dimension[], index: number, value: string): Dimension[] {
  const next = current.slice();
  if (!value) next.splice(index, 1);
  else {
    const [fieldId, level] = value.split("/");
    next[index] = { fieldId, level: level as Dimension["level"] };
  }
  return next.filter(Boolean).slice(0, 2);
}
