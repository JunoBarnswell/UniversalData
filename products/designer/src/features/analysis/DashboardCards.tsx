import { ChevronRight } from "lucide-react";
import { Button } from "../../shared/design-system/Controls";
import {
  cellText,
  chartLabels,
  comparisonLabels,
  dimensionLabel,
  formatDecimal,
  formatPercent,
  metricText,
} from "./dashboard-utils";
import type {
  Cell,
  Comparison,
  DashboardCard,
  DashboardCardResult,
  Measure,
  Rule,
} from "./model";

function reportMeasure(result: DashboardCardResult, measureId: string): Measure | undefined {
  return result.current.measures.find((measure) => measure.id === measureId);
}

function reportValue(result: DashboardCardResult, measureId: string): string | null | undefined {
  return result.current.total[measureId];
}

export function DashboardCardView({ card, result, fields, allowDetails, onCell, onDrill }: {
  card: DashboardCard;
  result: DashboardCardResult | undefined;
  fields: Rule[];
  allowDetails: boolean;
  onCell(cell: Cell): void;
  onDrill(cell: (string | null)[] | null): void;
}) {
  const measure = result ? reportMeasure(result, card.measureId) : undefined;
  const value = result ? reportValue(result, card.measureId) : null;
  const group = result?.current.groups.at(-1);
  const empty = !result || result.current.empty;
  return <article className={`dashboard-card dashboard-card-${card.chart}`}>
    <header className="dashboard-card-heading">
      <div>
        <h2>{card.title}</h2>
        <small>{measure?.label ?? card.measureId} · {chartLabels[card.chart]}{card.comparison !== "none" ? ` · ${comparisonLabels[card.comparison]}` : ""}</small>
      </div>
      <Button className="text-button" aria-label={`查看${card.title}明细`} onClick={() => onDrill(null)} disabled={!result || !allowDetails}><ChevronRight size={16} /></Button>
    </header>
    {empty ? <p className="dashboard-empty compact">当前筛选没有数据。</p> : card.chart === "kpi" ? <KpiCard result={result!} measure={measure} value={value} comparison={card.comparison} /> :
      card.chart === "line" ? <LineCard group={group} measureId={card.measureId} fields={fields} onCell={onCell} /> :
        card.chart === "bar" ? <BarCard group={group} measureId={card.measureId} fields={fields} onCell={onCell} /> :
          <TableCard group={group} measureId={card.measureId} measureLabel={measure?.label ?? card.measureId} fields={fields} onCell={onCell} />}
    {result && card.comparison !== "none" && card.chart !== "kpi" && <ComparisonSummary result={result} measureId={card.measureId} comparison={card.comparison} />}
  </article>;
}

function KpiCard({ result, measure, value, comparison }: {
  result: DashboardCardResult;
  measure?: Measure;
  value: string | null | undefined;
  comparison: Comparison;
}) {
  return <div className="dashboard-kpi">
    <strong>{formatDecimal(value, "无数据")}</strong>
    {measure?.unit && <span>{measure.unit}</span>}
    {comparison !== "none" && <ComparisonSummary result={result} measureId={measure?.id ?? ""} comparison={comparison} />}
    <small>{measure ? metricText(measure) : "当前指标"}</small>
  </div>;
}

function ComparisonSummary({ result, measureId, comparison }: {
  result: DashboardCardResult;
  measureId: string;
  comparison: Comparison;
}) {
  if (comparison === "none") return null;
  const previous = result.previous?.total[measureId];
  return <div className="dashboard-comparison">
    <span>{comparisonLabels[comparison]}</span>
    <strong className={result.delta?.startsWith("-") ? "negative" : "positive"}>
      {result.delta ? `${result.delta.startsWith("-") || result.delta.startsWith("+") ? "" : "+"}${formatDecimal(result.delta)}` : "—"}
    </strong>
    <small>{previous == null ? "暂无可比较数据" : `基准 ${formatDecimal(previous)}`}{result.changeRate ? ` · ${formatPercent(result.changeRate)}` : ""}</small>
  </div>;
}

function LineCard({ group, measureId, fields, onCell }: {
  group: { dimensions: import("./model").Dimension[]; cells: Cell[] } | undefined;
  measureId: string;
  fields: Rule[];
  onCell(cell: Cell): void;
}) {
  if (!group || !group.cells.length) return <p className="dashboard-empty compact">没有趋势数据。</p>;
  const ordered = [...group.cells].sort((a, b) => String(a.keys[0] ?? "").localeCompare(String(b.keys[0] ?? "")));
  const values = ordered.map((cell) => cell.values[measureId] == null ? null : Number(cell.values[measureId]));
  const valid = values.filter((value): value is number => value !== null && Number.isFinite(value));
  if (!valid.length) return <p className="dashboard-empty compact">当前指标没有有效数值。</p>;
  const width = 720;
  const height = 180;
  const inset = 28;
  const max = Math.max(0, ...valid);
  const min = Math.min(0, ...valid);
  const range = max - min || 1;
  const x = (index: number) => ordered.length === 1 ? width / 2 : inset + (width - inset * 2) * index / (ordered.length - 1);
  const y = (value: number) => height - inset - (height - inset * 2) * (value - min) / range;
  const path = values.map((value, index) => value === null ? "" : `${index === 0 || values[index - 1] === null ? "M" : "L"}${x(index)} ${y(value)}`).join(" ");
  return <div className="dashboard-line">
    <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${dimensionLabel(fields, group.dimensions[0])}趋势图`}>
      <line x1={inset} x2={width - inset} y1={y(0)} y2={y(0)} />
      <path d={path} />
      {ordered.map((cell, index) => values[index] === null ? null : <circle key={JSON.stringify(cell.keys)} cx={x(index)} cy={y(values[index]!)} r="4" />)}
    </svg>
    <div className="dashboard-axis"><span>{cellText(ordered[0].keys[0])}</span><span>{cellText(ordered.at(-1)!.keys[0])}</span></div>
    <div className="dashboard-line-actions">{ordered.slice(0, 12).map((cell) => <Button key={JSON.stringify(cell.keys)} onClick={() => onCell(cell)}>{cellText(cell.keys[0])} · {formatDecimal(cell.values[measureId])}</Button>)}</div>
  </div>;
}

function BarCard({ group, measureId, fields, onCell }: {
  group: { dimensions: import("./model").Dimension[]; cells: Cell[] } | undefined;
  measureId: string;
  fields: Rule[];
  onCell(cell: Cell): void;
}) {
  if (!group || !group.cells.length) return <p className="dashboard-empty compact">没有分类数据。</p>;
  const numeric = group.cells.map((cell) => cell.values[measureId] == null ? null : Number(cell.values[measureId]));
  const finite = numeric.filter((value): value is number => value !== null && Number.isFinite(value));
  if (!finite.length) return <p className="dashboard-empty compact">当前指标没有有效数值。</p>;
  const max = Math.max(0, ...finite);
  const min = Math.min(0, ...finite);
  const range = max - min || 1;
  return <div className="dashboard-bars" aria-label={`${dimensionLabel(fields, group.dimensions[0])}横条图`}>
    {group.cells.slice(0, 24).map((cell, index) => {
      const value = numeric[index];
      const zero = -min / range * 100;
      const position = value == null ? zero : (value - min) / range * 100;
      return <Button className="dashboard-bar-row" key={JSON.stringify(cell.keys)} onClick={() => onCell(cell)}>
        <span>{cellText(cell.keys[0])}</span>
        <span className="dashboard-bar-track"><i style={{ left: `${Math.min(zero, position)}%`, width: `${Math.abs(position - zero)}%` }} /></span>
        <strong>{formatDecimal(cell.values[measureId])}</strong>
      </Button>;
    })}
  </div>;
}

function TableCard({ group, measureId, measureLabel, fields, onCell }: {
  group: { dimensions: import("./model").Dimension[]; cells: Cell[] } | undefined;
  measureId: string;
  measureLabel: string;
  fields: Rule[];
  onCell(cell: Cell): void;
}) {
  if (!group || !group.cells.length) return <p className="dashboard-empty compact">没有表格数据。</p>;
  return <div className="analysis-scroll dashboard-table-wrap">
    <table className="analysis-table dashboard-table">
      <thead><tr>{group.dimensions.map((dimension) => <th key={`${dimension.fieldId}/${dimension.level}`} scope="col">{dimensionLabel(fields, dimension)}</th>)}<th scope="col">{measureLabel}</th></tr></thead>
      <tbody>{group.cells.map((cell) => <tr key={JSON.stringify(cell.keys)}>{cell.keys.map((key, index) => <th key={index} scope="row">{cellText(key)}</th>)}<td><Button className="text-button" onClick={() => onCell(cell)}>{formatDecimal(cell.values[measureId])}</Button></td></tr>)}</tbody>
    </table>
  </div>;
}
