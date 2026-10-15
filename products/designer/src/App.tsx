import { useEffect, useState } from "react";
import {
  Box,
  Play,
  List,
  CalendarDays,
  FolderOpen,
  Workflow,
} from "lucide-react";
import { api, type Session } from "./api";
import { Authentication, SessionControls } from "./Authentication";
import { Editor, type OpenWorkspace } from "./Editor";
import { Executions } from "./Executions";
import { Jobs } from "./Jobs";
import { Projects } from "./Projects";
import type { Component } from "./model";
type Page = "projects" | "designer" | "runs" | "logs" | "jobs";
export default function App() {
  return (
    <Authentication>
      {(session) => (
        <AuthenticatedApp
          key={`${session.tenant}/${session.subject}`}
          session={session}
        />
      )}
    </Authentication>
  );
}
function AuthenticatedApp({ session }: { session: Session }) {
  const [page, setPage] = useState<Page>("projects"),
    [components, setComponents] = useState<Component[]>([]),
    [error, setError] = useState(""),
    [workspace, setWorkspace] = useState<OpenWorkspace>();
  useEffect(() => {
    const controller = new AbortController();
    api<Component[]>("/components", { signal: controller.signal })
      .then(setComponents)
      .catch((error) => {
        if (!controller.signal.aborted) setError(String(error));
      });
    return () => controller.abort();
  }, []);
  const items: { key: Page; label: string; icon: typeof Box }[] = [
    { key: "projects", label: "项目", icon: FolderOpen },
    { key: "designer", label: "设计工作台", icon: Workflow },
    { key: "runs", label: "执行", icon: Play },
    { key: "logs", label: "日志", icon: List },
    { key: "jobs", label: "作业", icon: CalendarDays },
  ];
  return (
    <div className="application">
      <header className="main-header">
        <button className="brand" onClick={() => setPage("projects")}>
          <Box size={25} />
          <strong>UniversalData</strong>
        </button>
        <nav aria-label="主导航">
          {items.map(({ key, label, icon: Icon }) => (
            <button
              key={key}
              className={page === key ? "active" : ""}
              onClick={() => setPage(key)}
            >
              <Icon size={16} />
              {label}
            </button>
          ))}
        </nav>
        <SessionControls session={session} />
      </header>
      {error ? (
        <div role="alert" className="banner">
          {error}
          <button aria-label="关闭提示" onClick={() => setError("")}>
            ×
          </button>
        </div>
      ) : null}
      {page === "projects" ? (
        <Projects
          open={(version) => {
            setWorkspace({
              token: crypto.randomUUID(),
              document: version.document,
              revision: String(version.revision),
            });
            setPage("designer");
          }}
          create={(importFile) => {
            setWorkspace({ token: crypto.randomUUID(), import: importFile });
            setPage("designer");
          }}
        />
      ) : null}
      <div className="designer-page" hidden={page !== "designer"}>
        <Editor
          components={components}
          request={workspace}
          onExecution={() => setPage("runs")}
        />
      </div>
      {page === "runs" || page === "logs" ? (
        <Executions logsOnly={page === "logs"} />
      ) : page === "jobs" ? (
        <Jobs />
      ) : null}
    </div>
  );
}
