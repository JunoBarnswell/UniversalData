import { useEffect, useState } from "react";
import {
  Box,
  ChartNoAxesCombined,
  Play,
  List,
  CalendarDays,
  FolderOpen,
  Workflow,
  Server,
} from "lucide-react";
import { api, type Session } from "./api";
import { Authentication, SessionControls } from "./Authentication";
import { Workspace, useWorkspaces } from "./features/workspace/Workspace";
import { Appearance } from "./shared/design-system/Appearance";
import { Executions } from "./Executions";
import { Jobs } from "./Jobs";
import { Workers } from "./Workers";
import { Projects } from "./Projects";
import { Analysis } from "./features/analysis/Analysis";
import type { Component } from "./model";
type Page = "analysis" | "projects" | "designer" | "runs" | "logs" | "jobs" | "workers";
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
  const canAnalyze = session.grants.some(({ permission }) => permission === "*" || permission === "analysis.read");
  const canManageAnalysis = session.grants.some(({ permission }) => permission === "*" || permission === "analysis.write");
  const canReadComponents = session.grants.some(({ permission, resource }) =>
    (permission === "*" || permission === "component.read") && resource === "*");
  const analysisOnly = canAnalyze && !session.grants.some(({ permission }) =>
    permission === "*" || /^(component|package|deployment|execution|job|logs|worker)\./.test(permission));
  // A shared dashboard is authorized by its membership on the API. Keep a direct
  // analysis URL renderable even when the viewer has no author-side analysis grant.
  const analysisRoute = window.location.pathname === "/analysis" || window.location.pathname.startsWith("/analysis/");
  const [page, setPage] = useState<Page>(() =>
    analysisRoute || analysisOnly ? "analysis" : "projects");
  const [components, setComponents] = useState<Component[]>([]);
  const [error, setError] = useState("");
  const workspace = useWorkspaces();
  useEffect(() => {
    if (!canReadComponents) return;
    const controller = new AbortController();
    api<Component[]>("/components", { signal: controller.signal })
      .then(setComponents)
      .catch((error) => {
        if (!controller.signal.aborted) setError(String(error));
      });
    return () => controller.abort();
  }, [canReadComponents]);
  function navigate(next: Page) {
    if (next === "analysis" && page !== "analysis") window.history.replaceState(null, "", "/analysis");
    if (next !== "analysis" && page === "analysis") window.history.replaceState(null, "", "/");
    setPage(next);
  }
  const items: { key: Page; label: string; icon: typeof Box }[] = [
    { key: "analysis", label: "我的分析", icon: ChartNoAxesCombined },
    { key: "projects", label: "项目", icon: FolderOpen },
    { key: "designer", label: "设计工作台", icon: Workflow },
    { key: "runs", label: "执行", icon: Play },
    { key: "logs", label: "日志", icon: List },
    { key: "jobs", label: "作业", icon: CalendarDays },
    { key: "workers", label: "Worker", icon: Server },
  ];
  return (
    <div className="application">
      <header className="main-header">
        <button className="brand" onClick={() => navigate(analysisOnly ? "analysis" : "projects")}>
          <Box size={25} />
          <strong>UniversalData</strong>
        </button>
        <nav aria-label="主导航">
          {items.filter((item) => item.key === "analysis" ? canAnalyze : !analysisOnly).map(({ key, label, icon: Icon }) => (
            <button
              key={key}
              className={page === key ? "active" : ""}
              onClick={() => navigate(key)}
            >
              <Icon size={16} />
              {label}
            </button>
          ))}
        </nav>
        <Appearance />
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
      {page === "analysis" && <Analysis canManage={canManageAnalysis} />}
      {page === "projects" ? (
        <Projects
          open={(version) => {
            workspace.open({
              token: crypto.randomUUID(),
              document: version.document,
              revision: String(version.revision),
            });
            setPage("designer");
          }}
          create={(importFile) => {
            workspace.open({ token: crypto.randomUUID(), import: importFile });
            setPage("designer");
          }}
        />
      ) : null}
      {!analysisOnly && <div className="designer-page" hidden={page !== "designer"}>
        <Workspace
          visible={page === "designer"}
          components={components}
          workspace={workspace}
          onExecution={() => setPage("runs")}
        />
      </div>}
      {page === "runs" || page === "logs" ? (
        <Executions logsOnly={page === "logs"} />
      ) : page === "jobs" ? (
        <Jobs />
      ) : page === "workers" ? <Workers /> : null}
    </div>
  );
}
