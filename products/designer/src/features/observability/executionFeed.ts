import { api, request, decode, ApiError } from "../../api";
import type { RunState } from "../../model";
import type { RunFacts } from "./RunMonitor";
export type FeedMode = "连接中" | "实时连接" | "重连中" | "快照轮询" | "已结束";
export interface ExecutionSnapshot {
  run: RunState;
  facts: RunFacts;
  events: unknown[];
  gap: boolean;
  terminal: boolean;
  source: string;
}
const terminal = (state: string) =>
  ["Succeeded", "Failed", "Cancelled", "Stopped", "RecoveryRequired"].includes(
    state,
  );
function delay(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const done = () => {
      signal.removeEventListener("abort", abort);
      resolve();
    };
    const timer = window.setTimeout(done, ms);
    function abort() {
      clearTimeout(timer);
      reject(new DOMException("Aborted", "AbortError"));
    }
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, { once: true });
  });
}
/** One subscription owns reconnection/fallback. Frame and UI retention are bounded;
 * revisions remain opaque strings and payload integers use the common decoder. */
export async function executionFeed(
  ticket: string,
  signal: AbortSignal,
  accept: (snapshot: ExecutionSnapshot) => void,
  mode: (mode: FeedMode) => void,
) {
  let cursor = "",
    failures = 0;
  while (!signal.aborted) {
    mode(failures ? "重连中" : "连接中");
    try {
      const response = await request(
        `/executions/${encodeURIComponent(ticket)}/events`,
        {
          signal,
          headers: {
            Accept: "text/event-stream",
            ...(cursor ? { "Last-Event-ID": cursor } : {}),
          },
        },
      );
      if (
        !response.headers
          .get("content-type")
          ?.startsWith("text/event-stream") ||
        !response.body
      )
        throw new Error("服务未返回事件流");
      const reader = response.body.getReader(),
        decoder = new TextDecoder();
      let buffer = "",
        finished = false;
      mode("实时连接");
      try {
        while (!signal.aborted) {
          const part = await reader.read();
          if (part.done) break;
          buffer += decoder.decode(part.value, { stream: true });
          if (buffer.length > 8388608) throw new Error("事件帧超出观测配额");
          let end: number;
          while ((end = buffer.indexOf("\n\n")) >= 0) {
            const frame = buffer.slice(0, end);
            buffer = buffer.slice(end + 2);
            let id = "",
              data = "",
              kind = "";
            for (const line of frame.split("\n")) {
              if (line.startsWith("id: ")) id = line.slice(4);
              else if (line.startsWith("data: ")) data += line.slice(6);
              else if (line.startsWith("event: ")) kind = line.slice(7);
            }
            if (kind !== "snapshot" || !data) continue;
            const snapshot = decode(data) as ExecutionSnapshot;
            // Accept the frame before advancing its cursor; a failed consumer must not skip it.
            accept(snapshot);
            cursor = id;
            failures = 0;
            if (snapshot.terminal && snapshot.events.length < 100) {
              finished = true;
              break;
            }
          }
          if (finished) break;
        }
      } finally {
        await reader.cancel().catch(() => {});
        reader.releaseLock();
      }
      if (finished) {
        mode("已结束");
        return;
      }
      throw new Error("事件流已断开");
    } catch (error) {
      if (signal.aborted) throw error;
      if (
        error instanceof ApiError &&
        (error.status === 401 || error.status === 403 || error.status === 404)
      )
        throw error;
      failures++;
      if (failures >= 3) {
        // Same Catalog facts endpoint; polling does not synthesize timeline events.
        mode("快照轮询");
        try {
        const [run, facts] = await Promise.all([
          api<RunState>(`/executions/${ticket}`, { signal }),
          api<RunFacts>(`/executions/${ticket}/metrics`, { signal }),
        ]);
        const done = terminal(run.state);
        accept({
          run,
          facts,
          events: [],
          gap: true,
          source: "native-catalog",
          terminal: done,
        });
        if (done) {
          mode("已结束");
          return;
        }
        } catch(fallbackError) {
          if(signal.aborted || fallbackError instanceof ApiError && [401,403,404].includes(fallbackError.status))throw fallbackError;
        }
      }
      await delay(
        Math.min(10000, 1000 * 2 ** Math.min(failures - 1, 4)),
        signal,
      );
    }
  }
}
