import { useEffect, useRef, useState } from "react";
type Action = {
  id: string;
  label: string;
  run: () => void;
  disabled?: boolean;
};
export function CommandMenu({
  open,
  close,
  actions,
}: {
  open: boolean;
  close: () => void;
  actions: Action[];
}) {
  const dialog = useRef<HTMLDialogElement>(null),
    [query, setQuery] = useState("");
  useEffect(() => {
    const node = dialog.current;
    if (open && !node?.open) node?.showModal();
    else if (!open) node?.close();
  }, [open]);
  const found = actions.filter((action) =>
    action.label.toLowerCase().includes(query.toLowerCase()),
  );
  function invoke(action: Action) {
    if (action.disabled) return;
    close();
    action.run();
  }
  return (
    <dialog
      ref={dialog}
      className="command-menu"
      aria-label="工作台命令"
      onCancel={close}
      onClose={close}
    >
      <header>
        <h2>命令</h2>
        <button onClick={close} aria-label="关闭命令">
          ×
        </button>
      </header>
      <input
        aria-label="搜索命令"
        placeholder="搜索命令…"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter") {
            const action = found.find((action) => !action.disabled);
            if (action) invoke(action);
          }
        }}
      />
      <div className="command-results">
        {found.map((action) => (
          <button
            key={action.id}
            disabled={action.disabled}
            onClick={() => invoke(action)}
          >
            {action.label}
          </button>
        ))}
        {!found.length ? <p role="status">没有匹配的命令</p> : null}
      </div>
    </dialog>
  );
}
