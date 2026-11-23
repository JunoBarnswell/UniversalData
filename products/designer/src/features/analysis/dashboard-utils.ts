import type { Comparison, DashboardChart, Dimension, Measure, Rule } from "./model";

export const chartLabels: Record<DashboardChart, string> = {
  kpi: "指标卡",
  line: "折线图",
  bar: "横条图",
  table: "明细表",
};

export const comparisonLabels: Record<Comparison, string> = {
  none: "不比较",
  mom: "环比上月",
  yoy: "同比去年同月",
};

export function apiTime(value: string | null | undefined): string {
  if (!value) return "—";
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? value : parsed.toLocaleString("zh-CN", { hour12: false });
}

export function formatDecimal(value: string | number | bigint | null | undefined, missing = "—"): string {
  if (value === null || value === undefined || value === "") return missing;
  const text = String(value);
  const match = text.match(/^(-?)(\d+)(\.\d+)?(.*)$/);
  if (!match) return text;
  const [, sign, integer, fraction, suffix] = match;
  return `${sign}${integer.replace(/\B(?=(\d{3})+(?!\d))/g, ",")}${fraction ?? ""}${suffix ?? ""}`;
}

export function formatPercent(value: string | null | undefined): string {
  if (value === null || value === undefined || value === "") return "—";
  if (value.endsWith("%")) return formatDecimal(value);
  const match = value.match(/^(-?)(\d+)(?:\.(\d+))?$/);
  if (!match) return value;
  const [, sign, integer, fraction = ""] = match;
  const decimalPlaces = fraction.length;
  const digits = `${integer}${fraction}`.replace(/^0+(?=\d)/, "");
  const shifted = decimalPlaces < 2
    ? `${digits}${"0".repeat(2 - decimalPlaces)}`
    : digits;
  const split = decimalPlaces < 2 ? shifted.length : shifted.length - decimalPlaces + 2;
  const whole = shifted.slice(0, split).replace(/^0+(?=\d)/, "") || "0";
  const decimals = shifted.slice(split).replace(/0+$/, "");
  return `${sign}${whole}${decimals ? `.${decimals}` : ""}%`;
}

export function metricText(measure: Measure): string {
  if (measure.calculation) return measure.calculation;
  if (measure.operation) {
    const left = measure.left ? ` ${measure.left}` : "";
    const right = measure.right ? ` ${measure.right}` : "";
    return `${measure.operation}${left}${right}`.trim();
  }
  return measure.fieldId ? `字段 ${measure.fieldId}` : "由数据模型提供";
}

export function fieldLabel(fields: Rule[], fieldId: string): string {
  return fields.find((field) => field.fieldId === fieldId)?.label ?? fieldId;
}

export function dimensionLabel(fields: Rule[], dimension: Dimension): string {
  const suffix = dimension.level === "month" ? " · 月" : dimension.level === "day" ? " · 日" : "";
  return `${fieldLabel(fields, dimension.fieldId)}${suffix}`;
}

export function cellText(value: string | null | undefined): string {
  return value === null || value === undefined ? "未填写" : value === "未填写" ? "未填写（原文）" : value;
}
