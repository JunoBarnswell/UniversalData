import { Search, ChevronDown, Database, Plus } from "lucide-react";
import type { Component } from "../../model";
const categories = [
  { label: "读取", test: (component: Component) => component.category === 2 },
  {
    label: "清洗",
    test: (component: Component) =>
      component.category === 3 &&
      /quality|cast|clean|duplicate|copyColumn|importColumn|exportColumn/.test(
        component.id,
      ),
  },
  {
    label: "计算",
    test: (component: Component) =>
      component.category === 3 &&
      /derive|window|pivot|script|expression/.test(component.id),
  },
  {
    label: "关联",
    test: (component: Component) =>
      component.category === 3 && /join|lookup|scd/i.test(component.id),
  },
  {
    label: "分流",
    test: (component: Component) =>
      component.category === 3 &&
      /split|multicast|balanced|unionAll/.test(component.id),
  },
  {
    label: "聚合与排序",
    test: (component: Component) =>
      component.category === 3 &&
      /aggregate|sort|sampling|profile/i.test(component.id),
  },
  { label: "输出", test: (component: Component) => component.category === 4 },
];
export function ComponentPalette({
  components,
  control,
  search,
  setSearch,
  add,
  template,
}: {
  components: Component[];
  control: boolean;
  search: string;
  setSearch: (value: string) => void;
  add: (component: Component) => void;
  template: (kind: "derive-split" | "dimensions") => void;
}) {
  return (
    <aside className="component-rail">
      {!control ? (
        <section className="template-list">
          <h3>组件模板</h3>
          <button onClick={() => template("derive-split")}>
            字段处理 → 业务分流
          </button>
          <button onClick={() => template("dimensions")}>
            复制 → 双维度统计
          </button>
        </section>
      ) : null}
      <label className="search">
        <Search size={15} />
        <input
          aria-label="搜索组件"
          placeholder="搜索数据源、清洗、分流…"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
      </label>
      {(control
        ? [
            {
              label: "控制任务",
              test: (component: Component) => component.category === 1,
            },
          ]
        : [
            ...categories,
            {
              label: "其他转换",
              test: (component: Component) =>
                component.category === 3 &&
                !categories.some((group) => group.test(component)),
            },
          ]
      ).map((group) => (
        <section key={group.label}>
          <h3>
            <ChevronDown size={14} />
            {group.label}
          </h3>
          {components
            .filter(
              (component) =>
                group.test(component) &&
                `${component.id} ${component.name}`
                  .toLowerCase()
                  .includes(search.toLowerCase()),
            )
            .map((component) => (
              <button
                draggable
                onDragStart={(event) => {
                  event.dataTransfer.setData(
                    "application/universaldata-component",
                    component.id,
                  );
                  event.dataTransfer.effectAllowed = "copy";
                }}
                className="catalog-row"
                key={component.id}
                onClick={() => add(component)}
              >
                <Database size={18} />
                <span>
                  <strong>{component.name}</strong>
                  <small>{component.id}</small>
                </span>
                <Plus size={13} />
              </button>
            ))}
        </section>
      ))}
    </aside>
  );
}
