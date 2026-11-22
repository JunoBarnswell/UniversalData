import type { Values } from "./model";

export function ValuesTable({ fields, rows }: { fields: { fieldId: string; label: string }[]; rows: Values[] }) {
  return <div className="analysis-scroll"><table className="analysis-table">
    <thead><tr>{fields.map((field) => <th key={field.fieldId} scope="col">{field.label}</th>)}</tr></thead>
    <tbody>{rows.map((row, index) => <tr key={index}>{fields.map((field) => <td key={field.fieldId}
      title={row[field.fieldId] ?? undefined}>{row[field.fieldId] ?? "未填写"}</td>)}</tr>)}</tbody>
  </table>{!rows.length && <p className="analysis-empty">没有记录</p>}</div>;
}
