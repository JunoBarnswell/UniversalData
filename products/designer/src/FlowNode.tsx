import { memo, useEffect } from "react";
import {
  Handle,
  Position,
  useUpdateNodeInternals,
  type NodeProps,
  type Node,
} from "@xyflow/react";
import {
  Database,
  FileSpreadsheet,
  Sigma,
  GitBranch,
  Copy,
  TriangleAlert,
  Braces,
} from "lucide-react";
import type { NativePort } from "./model";
export type FlowNodeValue = Node<
  {
    label: string;
    componentId: string;
    inputs: number;
    outputs: number;
    error: number;
    ports?: NativePort[];
    inputFields?: number;
    outputFields?: number;
    portRows?: Record<number, number>;
    stale?: boolean;
    failed?: boolean;
  },
  "component"
>;
export const FlowNode = memo(function FlowNode({
  id,
  data,
  selected,
}: NodeProps<FlowNodeValue>) {
  const updateNodeInternals = useUpdateNodeInternals();
  const handles = `${data.inputs}:${data.outputs}:${data.ports?.map((port) => `${port.direction}/${port.index}/${port.id}`).join(",") ?? ""}`;
  useEffect(() => {
    updateNodeInternals(id);
  }, [id, handles, updateNodeInternals]);
  const Icon = data.componentId.includes("sql")
    ? Database
    : data.componentId.includes("file")
      ? FileSpreadsheet
      : data.componentId === "aggregate"
        ? Sigma
        : data.componentId === "multicast"
          ? Copy
          : data.componentId === "derive"
            ? Braces
            : GitBranch;
  return (
    <div
      className={`flow-node ${selected ? "selected" : ""} ${data.failed ? "failed" : ""}`}
      style={{ minHeight: Math.max(108, 40 + data.outputs * 28) }}
    >
      {Array.from({ length: data.inputs }, (_, index) => {
        const port = data.ports?.find(
          (port) => port.direction === 1 && port.index === index,
        );
        return (
          <Handle
            key={`in-${index}`}
            id={`in-${port?.id ?? index}`}
            type="target"
            position={Position.Left}
            title={port?.name ?? `输入 ${index + 1}`}
            style={{ top: `${((index + 1) * 100) / (data.inputs + 1)}%` }}
          />
        );
      })}
      <div className="node-title">
        <Icon size={21} strokeWidth={1.7} />
        <div>
          <strong>{data.label}</strong>
          <span>{data.componentId}</span>
        </div>
        {data.failed ? <TriangleAlert size={16} /> : null}
      </div>
      <div className="node-fields">
        {data.inputFields === undefined
          ? "待绑定字段"
          : `${data.inputFields} 个输入字段`}
        {data.outputFields === undefined
          ? ""
          : ` → ${data.outputFields} 个输出字段`}
      </div>
      <div className="node-ports">
        {Array.from({ length: data.outputs }, (_, index) => {
          const port = data.ports?.find(
            (port) => port.direction === 2 && port.index === index,
          );
          const error = Boolean(port ? port.flags & 2 : index === data.error);
          return (
            <div
              className={error ? "node-port error-output" : "node-port"}
              key={port?.id ?? index}
            >
              <span>
                {port?.name ??
                  (error
                    ? "异常行"
                    : data.outputs === 1
                      ? "输出"
                      : `输出 ${index + 1}`)}
              </span>
              <b>
                {data.portRows?.[index] === undefined
                  ? "—"
                  : data.portRows[index].toLocaleString()}
                {data.stale && data.portRows?.[index] !== undefined ? "*" : ""}
              </b>
              <Handle
                id={`out-${port?.id ?? index}`}
                type="source"
                position={Position.Right}
                className={error ? "error-port" : ""}
                title={port?.name ?? `输出 ${index + 1}`}
              />
            </div>
          );
        })}
      </div>
    </div>
  );
});
