import { PROTOCOL_VERSION } from "./version";
import type { Configuration, DataField, PackageDocument } from "./model";
function FieldChoice({
  fields,
  value,
  change,
  all = false,
}: {
  fields: DataField[];
  value: unknown;
  change: (value: string) => void;
  all?: boolean;
}) {
  return (
    <select
      value={String(value ?? "")}
      onChange={(event) => change(event.target.value)}
    >
      <option value="">选择字段</option>
      {all ? <option value="*">所有行 *</option> : null}
      {fields.map((field) => (
        <option key={field.identity ?? field.name} value={field.name}>
          {field.name} · {field.scalar?.type ?? field.type}
        </option>
      ))}
    </select>
  );
}
export function AggregateEditor({
  value,
  change,
  fields,
}: {
  value: Configuration;
  change: (value: Configuration) => void;
  fields: DataField[];
}) {
  const plan =
    value.plan && typeof value.plan === "object"
      ? (value.plan as Configuration)
      : { version: PROTOCOL_VERSION, group: [], aggregates: [] };
  const groups = Array.isArray(plan.group) ? (plan.group as string[]) : [],
    rows = Array.isArray(plan.aggregates)
      ? (plan.aggregates as Configuration[])
      : [];
  function edit(update: Configuration) {
    change({
      ...value,
      plan: { ...plan, version: PROTOCOL_VERSION, ...update },
    });
  }
  function row(index: number, update: Configuration) {
    edit({
      aggregates: rows.map((row, at) =>
        at === index ? { ...row, ...update } : row,
      ),
    });
  }
  return (
    <section className="business-editor">
      <h3>维度分组</h3>
      <p className="muted">
        多个输入行归并为一个维度结果，结果行数不能与分流行数相加。
      </p>
      <div className="field-checks">
        {fields.map((field) => (
          <label className="checkbox" key={field.identity ?? field.name}>
            <input
              type="checkbox"
              checked={groups.includes(field.name)}
              onChange={(event) =>
                edit({
                  group: event.target.checked
                    ? [...groups, field.name]
                    : groups.filter((name) => name !== field.name),
                })
              }
            />
            {field.name}
          </label>
        ))}
      </div>
      <h3>统计字段</h3>
      {rows.map((item, index) => (
        <fieldset key={index}>
          <legend>统计 {index + 1}</legend>
          <label>
            输出名称
            <input
              value={String(item.name ?? "")}
              onChange={(event) => row(index, { name: event.target.value })}
            />
          </label>
          <label>
            统计函数
            <select
              value={String(item.function ?? "count")}
              onChange={(event) => row(index, { function: event.target.value })}
            >
              {[
                ["count", "计数"],
                ["sum", "合计"],
                ["min", "最小值"],
                ["max", "最大值"],
                ["avg", "平均值"],
                ["approxDistinct", "近似基数"],
              ].map(([id, label]) => (
                <option key={id} value={id}>
                  {label}
                </option>
              ))}
            </select>
          </label>
          <label>
            输入字段
            <FieldChoice
              fields={fields}
              value={item.column}
              all={item.function === "count"}
              change={(column) => row(index, { column })}
            />
          </label>
          <button
            onClick={() =>
              edit({ aggregates: rows.filter((_, at) => at !== index) })
            }
          >
            移除统计
          </button>
        </fieldset>
      ))}
      <button
        onClick={() =>
          edit({
            aggregates: [
              ...rows,
              {
                name: `count_${rows.length + 1}`,
                function: "count",
                column: "*",
              },
            ],
          })
        }
      >
        添加统计字段
      </button>
    </section>
  );
}
export function JoinEditor({
  value,
  change,
  inputs,
}: {
  value: Configuration;
  change: (value: Configuration) => void;
  inputs: DataField[][];
}) {
  const left = Array.isArray(value.leftKeys) ? value.leftKeys : [],
    right = Array.isArray(value.rightKeys) ? value.rightKeys : [];
  const aliases = value.rightAliases as Record<string, string> | undefined;
  function editKey(
    index: number,
    side: "leftKeys" | "rightKeys",
    next: string,
  ) {
    const keys = [...(side === "leftKeys" ? left : right)];
    keys[index] = next;
    change({ ...value, [side]: keys });
  }
  return (
    <section className="business-editor">
      <label>
        连接方式
        <select
          value={String(value.joinType ?? "inner")}
          onChange={(event) =>
            change({ ...value, joinType: event.target.value })
          }
        >
          {[
            ["inner", "匹配行"],
            ["left", "保留全部左侧行"],
            ["right", "保留全部右侧行"],
            ["full", "保留两侧全部行"],
            ["semi", "左侧匹配行"],
            ["anti", "左侧未匹配行"],
          ].map(([id, label]) => (
            <option key={id} value={id}>
              {label}
            </option>
          ))}
        </select>
      </label>
      <h3>左侧键 ↔ 右侧键</h3>
      {left.map((key, index) => (
        <div className="join-key" key={index}>
          <FieldChoice
            fields={inputs[0] ?? []}
            value={key}
            change={(next) => editKey(index, "leftKeys", next)}
          />
          <span>=</span>
          <FieldChoice
            fields={inputs[1] ?? []}
            value={right[index]}
            change={(next) => editKey(index, "rightKeys", next)}
          />
          <button
            onClick={() =>
              change({
                ...value,
                leftKeys: left.filter((_, at) => at !== index),
                rightKeys: right.filter((_, at) => at !== index),
              })
            }
          >
            移除
          </button>
        </div>
      ))}
      <button
        onClick={() =>
          change({
            ...value,
            leftKeys: [...left, ""],
            rightKeys: [...right, ""],
          })
        }
      >
        添加连接键
      </button>
      <h3>右侧输出名称</h3>
      {inputs[1]?.map((field) => (
        <label key={field.identity ?? field.name}>
          {field.name}
          <input
            value={aliases?.[field.name] ?? field.name}
            onChange={(event) =>
              change({
                ...value,
                rightAliases: { ...aliases, [field.name]: event.target.value },
              })
            }
          />
        </label>
      ))}
      <small>类型与键兼容性由 Native 校验。</small>
    </section>
  );
}
export function LookupEditor({
  value,
  change,
  fields,
  schema,
  document,
}: {
  document: PackageDocument;
  value: Configuration;
  change: (value: Configuration) => void;
  fields: DataField[];
  schema: { properties?: Record<string, unknown> };
}) {
  const keys = Array.isArray(value.keys) ? value.keys : [];
  return (
    <section className="business-editor">
      <label>
        引用缓存
        <select
          value={String(value.cache ?? "")}
          onChange={(event) => change({ ...value, cache: event.target.value })}
        >
          <option value="">选择连接中心中的引用缓存</option>
          {document.caches?.map((cache) => (
            <option key={String(cache.id)} value={String(cache.id)}>
              {String(cache.id)} · {String(cache.sourceTable ?? "")}
            </option>
          ))}
        </select>
      </label>
      <h3>匹配输入字段</h3>
      {fields.map((field, index) => (
        <label className="checkbox" key={field.identity ?? field.name}>
          <input
            type="checkbox"
            checked={keys.includes(index)}
            onChange={(event) =>
              change({
                ...value,
                keys: event.target.checked
                  ? [...keys, index]
                  : keys.filter((key) => key !== index),
              })
            }
          />
          {field.name}
        </label>
      ))}
      {schema.properties?.errorPolicy ? (
        <label>
          未命中 / 记录错误
          <select
            value={String(value.errorPolicy ?? "ignore")}
            onChange={(event) =>
              change({ ...value, errorPolicy: event.target.value })
            }
          >
            <option value="ignore">保留未命中行</option>
            <option value="redirect">输出异常行</option>
            <option value="fail">停止组件</option>
          </select>
        </label>
      ) : null}
      <p className="muted">
        缓存定义与生命周期由 Package 的 Native 资源配置控制。
      </p>
    </section>
  );
}
