import { memo } from 'react';
import { Handle, Position, type NodeProps, type Node } from '@xyflow/react';
import { Database, FileText, ShieldCheck, GitBranch } from 'lucide-react';
export type FlowNodeValue = Node<{ label: string; componentId: string; inputs: number; outputs: number; error: number }, 'component'>;
export const FlowNode = memo(function FlowNode({ data, selected }: NodeProps<FlowNodeValue>) {
  const Icon = data.componentId === 'quality' ? ShieldCheck : data.componentId.includes('sql') ? Database : data.componentId.includes('file') ? FileText : GitBranch;
  return <div className={`flow-node ${selected ? 'selected' : ''}`}>
    {Array.from({ length: data.inputs }, (_, index) => <Handle key={`in-${index}`} id={`in-${index}`} type="target" position={Position.Left} style={{ top: `${(index + 1) * 100 / (data.inputs + 1)}%` }} />)}
    <Icon size={25} strokeWidth={1.7}/><div><strong>{data.label}</strong><span>{data.componentId}</span></div>
    {Array.from({ length: data.outputs }, (_, index) => <Handle key={`out-${index}`} id={`out-${index}`} type="source" position={Position.Right} className={index === data.error ? 'error-port' : ''} style={{ top: `${(index + 1) * 100 / (data.outputs + 1)}%` }} />)}
  </div>;
});
