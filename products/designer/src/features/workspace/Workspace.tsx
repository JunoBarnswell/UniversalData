import { useCallback, useEffect, useState } from "react";
import { Editor, type OpenWorkspace } from "../graph/Editor";
import type { Component } from "../../model";
type Tab = {
  request: OpenWorkspace;
  label: string;
  dirty: boolean;
  packageId?: string;
  revision?: string;
};
export function useWorkspaces() {
  const [tabs, setTabs] = useState<Tab[]>([]),
    [active, setActive] = useState("");
  function open(request: OpenWorkspace) {
    const existing = request.document
      ? tabs.find(
          (tab) =>
            tab.packageId === request.document?.id &&
            tab.revision === request.revision,
        )
      : undefined;
    if (existing) {
      setActive(existing.request.token);
      return;
    }
    setTabs((current) => [
      ...current,
      {
        request,
        label:
          request.document?.layout?.label ?? request.document?.id ?? "新建任务",
        dirty: false,
        packageId: request.document?.id,
        revision: request.revision,
      },
    ]);
    setActive(request.token);
  }
  const status = useCallback(
    (
      token: string,
      label: string,
      dirty: boolean,
      packageId: string,
      revision?: string,
    ) =>
      setTabs((current) =>
        current.map((tab) =>
          tab.request.token === token &&
          (tab.label !== label ||
            tab.dirty !== dirty ||
            tab.packageId !== packageId ||
            tab.revision !== revision)
            ? { ...tab, label, dirty, packageId, revision }
            : tab,
        ),
      ),
    [],
  );
  useEffect(() => {
    if (!tabs.some((tab) => tab.dirty)) return;
    const protect = (event: BeforeUnloadEvent) => {
      event.preventDefault();
    };
    window.addEventListener("beforeunload", protect);
    return () => window.removeEventListener("beforeunload", protect);
  }, [tabs]);
  function close(token: string) {
    const index = tabs.findIndex((tab) => tab.request.token === token),
      tab = tabs[index];
    if (
      tab?.dirty &&
      !window.confirm(`“${tab.label}”有尚未持久化的编辑，仍然关闭？`)
    )
      return;
    setTabs(tabs.filter((tab) => tab.request.token !== token));
    if (active === token)
      setActive(
        tabs[index - 1]?.request.token ?? tabs[index + 1]?.request.token ?? "",
      );
  }
  return { tabs, active, open, select: setActive, close, status };
}
export function Workspace({
  components,
  workspace,
  visible,
  onExecution,
}: {
  components: Component[];
  workspace: ReturnType<typeof useWorkspaces>;
  visible: boolean;
  onExecution: () => void;
}) {
  const { tabs, active, select, close, status } = workspace;
  return (
    <>
      <div className="workspace-tabs" role="tablist" aria-label="打开的作业包">
        {tabs.map((tab) => (
          <div
            key={tab.request.token}
            className={active === tab.request.token ? "active" : ""}
          >
            <button
              role="tab"
              id={`tab-${tab.request.token}`}
              aria-controls={`panel-${tab.request.token}`}
              aria-selected={active === tab.request.token}
              onClick={() => select(tab.request.token)}
            >
              {tab.dirty ? "● " : ""}
              {tab.label}
            </button>
            <button
              aria-label={`关闭 ${tab.label}`}
              onClick={() => close(tab.request.token)}
            >
              ×
            </button>
          </div>
        ))}
      </div>
      {!tabs.length ? (
        <p className="workspace-empty">
          从项目打开任务，或新建一个作业包。每个标签保留各自的草稿、撤销历史与画布。
        </p>
      ) : null}
      {tabs.map((tab) => (
        <section
          role="tabpanel"
          id={`panel-${tab.request.token}`}
          aria-labelledby={`tab-${tab.request.token}`}
          key={tab.request.token}
          className="workspace-panel"
          hidden={active !== tab.request.token}
        >
          <Editor
            components={components}
            request={tab.request}
            active={visible && active === tab.request.token}
            onStatus={status}
            onExecution={onExecution}
          />
        </section>
      ))}
    </>
  );
}
