import { useState } from "react";
import { post } from "./api";
import { ConnectionSelect } from "./SqlEditors";
import type { Configuration, DataSchema, PackageDocument } from "./model";
export function CacheEditor({
  document,
  change,
}: {
  document: PackageDocument;
  change: (caches: Configuration[]) => void;
}) {
  const caches = document.caches ?? [],
    [selected, setSelected] = useState(0),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    cache = caches[selected];
  function update(value: Configuration) {
    change(
      caches.map((item, index) =>
        index === selected ? { ...item, ...value } : item,
      ),
    );
  }
  async function discover() {
    if (!cache) return;
    setBusy(true);
    try {
      const schema = await post<DataSchema>("/schema", {
        package: {
          ...document,
          caches: undefined,
          controlFlow: { tasks: [], constraints: [] },
          dataFlows: [],
        },
        connection: cache.sourceConnection,
        table: cache.sourceTable,
      });
      update({ schema });
    } catch (error) {
      setError(String(error));
    } finally {
      setBusy(false);
    }
  }
  const fields = (cache?.schema as DataSchema | undefined)?.fields ?? [],
    keys = Array.isArray(cache?.keys) ? cache.keys : [];
  return (
    <section className="cache-editor">
      <header>
        <h3>引用数据与缓存生命周期</h3>
        <button
          onClick={() => {
            change([
              ...caches,
              {
                id: `cache-${crypto.randomUUID().slice(0, 8)}`,
                sourceConnection: "",
                sourceTable: "",
                mode: "partial",
                scope: "execution",
                duplicates: "first",
                errorPolicy: "fail",
                keys: [],
                maxEntries: 4096,
                ttlMs: 60000,
              },
            ]);
            setSelected(caches.length);
          }}
        >
          新建引用缓存
        </button>
      </header>
      <label>
        引用定义
        <select
          value={selected}
          onChange={(event) => setSelected(Number(event.target.value))}
        >
          {caches.map((cache, index) => (
            <option key={String(cache.id)} value={index}>
              {String(cache.id)}
            </option>
          ))}
        </select>
      </label>
      {cache ? (
        <>
          <label>
            引用名称
            <input value={String(cache.id)} readOnly />
          </label>
          <ConnectionSelect
            document={document}
            value={String(cache.sourceConnection ?? "")}
            change={(sourceConnection) => update({ sourceConnection })}
          />
          <label>
            引用表
            <input
              value={String(cache.sourceTable ?? "")}
              onChange={(event) => update({ sourceTable: event.target.value })}
            />
          </label>
          <button
            disabled={busy || !cache.sourceConnection || !cache.sourceTable}
            onClick={() => void discover()}
          >
            发现引用字段
          </button>
          <fieldset>
            <legend>引用匹配键（按勾选顺序）</legend>
            {fields.map((field, index) => (
              <label key={field.identity ?? field.name} className="checkbox">
                <input
                  type="checkbox"
                  checked={keys.includes(index)}
                  onChange={(event) =>
                    update({
                      keys: event.target.checked
                        ? [...keys, index]
                        : keys.filter((key) => key !== index),
                    })
                  }
                />
                {field.name} · {field.type}
              </label>
            ))}
          </fieldset>
          <label>
            加载方式
            <select
              value={String(cache.mode ?? "partial")}
              onChange={(event) => update({ mode: event.target.value })}
            >
              <option value="partial">按需数据库 Lookup</option>
              <option value="full">完整缓存（配置缓存构建数据流）</option>
            </select>
          </label>
          <label>
            重复引用键
            <select
              value={String(cache.duplicates ?? "first")}
              onChange={(event) => update({ duplicates: event.target.value })}
            >
              <option value="first">首个结果</option>
              <option value="all">全部结果</option>
              <option value="error">报错</option>
            </select>
          </label>
          {cache.mode === "partial" ? (
            <>
              <label>
                最大缓存条目
                <input
                  type="number"
                  min={1}
                  max={65536}
                  value={Number(cache.maxEntries ?? 4096)}
                  onChange={(event) =>
                    update({ maxEntries: Number(event.target.value) })
                  }
                />
              </label>
              <label>
                有效期（毫秒）
                <input
                  type="number"
                  min={1}
                  value={Number(cache.ttlMs ?? 60000)}
                  onChange={(event) =>
                    update({ ttlMs: Number(event.target.value) })
                  }
                />
              </label>
            </>
          ) : null}
          <p className="muted">
            缓存由 Native Execution 创建、限制资源并释放。完整缓存需要由
            cacheBuild 组件在 Lookup 前完成构建。
          </p>
          <button
            onClick={() => {
              change(caches.filter((_, index) => index !== selected));
              setSelected(0);
            }}
          >
            移除引用缓存
          </button>
        </>
      ) : (
        <p>创建引用定义后，在 Lookup 组件中选择它。</p>
      )}
      {error ? <p role="alert">{error}</p> : null}
    </section>
  );
}
