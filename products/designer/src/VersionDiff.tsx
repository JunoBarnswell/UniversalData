import { encode } from "./api";
import type { PackageDocument } from "./model";
export function VersionDiff({
  previous,
  current,
}: {
  previous: PackageDocument;
  current: PackageDocument;
}) {
  const changes: { path: string; before: string; after: string }[] = [];
  function visit(a: unknown, b: unknown, path: string) {
    if (encode(a) === encode(b)) return;
    if (a && b && typeof a === "object" && typeof b === "object") {
      for (const key of new Set([...Object.keys(a), ...Object.keys(b)]))
        visit(
          (a as Record<string, unknown>)[key],
          (b as Record<string, unknown>)[key],
          path + "/" + key,
        );
      return;
    }
    changes.push({ path, before: encode(a), after: encode(b) });
  }
  visit(previous, current, "");
  return (
    <section className="version-diff">
      <h3>{changes.length} 项差异</h3>
      <div className="data-grid-scroll">
        <table>
          <thead>
            <tr>
              <th>位置</th>
              <th>保存版本</th>
              <th>当前草稿</th>
            </tr>
          </thead>
          <tbody>
            {changes.map((row) => (
              <tr key={row.path}>
                <td title={row.path}>{row.path}</td>
                <td>{row.before}</td>
                <td>{row.after}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
