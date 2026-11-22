export type Role = "text" | "category" | "date" | "amount" | "identifier" | "currency" | "omit";
export type Values = Record<string, string | null>;

export interface Source {
  id: string;
  name: string;
  kind: string;
  snapshotId: string | null;
}
export interface Field {
  id: string;
  label: string;
  physicalType: string;
  semanticRole: Role;
  confidenceBand: string;
  reason: string;
}
export interface Rule {
  fieldId: string;
  label: string;
  role: Role;
}
export interface Model {
  grain: string;
  fields: Rule[];
  dateFormat: string;
  offsetMinutes: number;
  amountFormat: string;
  currency: string | null;
  allowEmptyAmount: boolean;
  confirmSingleCurrency: boolean;
  confirmLineAmount: boolean;
}
export interface Dataset {
  id: string;
  name: string;
  revision: string;
  sourceName: string;
  kind: string;
  sheet: string | null;
  draft: Model | null;
  publishedReleaseId: string | null;
  report: SavedReport | null;
}
export interface Preview {
  fields: Field[];
  rows: Values[];
  sheets: string[];
  sheet: string | null;
  suggestion: Model | null;
}
export interface Measure {
  id: string;
  label: string;
  calculation: string;
  fieldId: string | null;
  unit: string | null;
}
export interface Release {
  id: string;
  modelRevisionId: string;
  model: Model;
  measures: Measure[];
  rows: number;
  validatedAt: string;
  dataAsOf: string;
  schemaFingerprint: string;
  sourceSnapshotId: string | null;
  sourceName: string;
  startingReport: SavedReport;
}
export interface PublishRequest {
  operationId: string;
  expectedRevision: string;
  expectedReleaseId: string | null;
}
export interface Publication {
  operationId: string;
  state: "checking" | "published" | "rejected";
  scannedRows: number;
  affectedRows: number;
  issues: { row: number; fieldId: string; message: string }[] | null;
  release: Release | null;
  error: string | null;
}
export interface Dimension {
  fieldId: string;
  level: "value" | "day" | "month";
}
export interface Filter {
  fieldId: string;
  operator: "isNull" | "eq" | "from" | "before";
  value?: string;
}
export interface Query {
  releaseId: string;
  dimensions: Dimension[];
  measures?: string[] | null;
  filters?: Filter[] | null;
  sortMeasure?: string | null;
  descending?: boolean;
  limit?: number;
}
export interface Cell {
  keys: (string | null)[];
  values: Values;
}
export interface Group {
  dimensions: Dimension[];
  cells: Cell[];
}
export interface ReportResult {
  context: string;
  releaseId: string;
  measures: Measure[];
  total: Values;
  groups: Group[];
  empty: boolean;
}
export interface Details {
  releaseId: string;
  fields: Rule[];
  rows: Values[];
  nextCursor: string | null;
}
export type Chart = "line" | "bar" | "table";
export interface SavedReport {
  name: string;
  query: Query;
  chart: Chart;
  chartMeasure: string | null;
}
export const roles: {value: Role; label: string}[] = [
  {value: "text", label: "明细文本"},
  {value: "category", label: "分类"},
  {value: "date", label: "分析日期"},
  {value: "amount", label: "行金额"},
  {value: "identifier", label: "订单号"},
  {value: "currency", label: "币种"},
  {value: "omit", label: "停用"},
];
export const datasetPath = (id: string) => `/analysis/datasets/${encodeURIComponent(id)}`;
