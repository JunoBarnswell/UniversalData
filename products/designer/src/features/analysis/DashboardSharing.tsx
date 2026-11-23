import { useState } from "react";
import { Plus, Save, Trash2, UsersRound } from "lucide-react";
import { Button, Input } from "../../shared/design-system/Controls";
import type { Dashboard, DashboardMember } from "./model";

export function DashboardSharing({ dashboard, saving, onSave }: {
  dashboard: Dashboard;
  saving: boolean;
  onSave(members: DashboardMember[]): Promise<void>;
}) {
  const [members, setMembers] = useState(dashboard.members);
  const [subject, setSubject] = useState("");

  function add() {
    const value = subject.trim();
    if (!value || members.some((member) => member.subject === value)) return;
    setMembers((current) => [...current, { subject: value, details: true, export: false }]);
    setSubject("");
  }

  return <section className="dashboard-sharing" aria-label="共享设置">
    <div className="dashboard-section-title"><div><h2>共享设置</h2><p>权限在服务端按当前租户和账号重新校验。撤销后，成员无法打开下一次查询。</p></div><UsersRound size={20} /></div>
    <div className="dashboard-member-add"><label>成员账号<Input value={subject} placeholder="输入账号标识" onChange={(event) => setSubject(event.target.value)} /></label><Button onClick={add} disabled={saving || !subject.trim()}><Plus size={14} />添加</Button></div>
    <div className="dashboard-members">{members.length ? members.map((member, index) => <div className="dashboard-member-row" key={member.subject}><div><strong>{member.subject}</strong><small>可按当前发布版本查看</small></div><label className="dashboard-check"><Input type="checkbox" checked={member.details} onChange={(event) => setMembers((current) => current.map((item, at) => at === index ? { ...item, details: event.target.checked } : item))} />明细</label><label className="dashboard-check"><Input type="checkbox" checked={member.export} onChange={(event) => setMembers((current) => current.map((item, at) => at === index ? { ...item, export: event.target.checked } : item))} />导出</label><Button aria-label={`移除${member.subject}`} onClick={() => setMembers((current) => current.filter((_, at) => at !== index))}><Trash2 size={14} /></Button></div>) : <p className="dashboard-muted">还没有共享成员。</p>}</div>
    <div className="dashboard-editor-actions"><span className="dashboard-editor-spacer" /><Button className="analysis-primary" disabled={saving} onClick={() => void onSave(members)}><Save size={15} />保存权限</Button></div>
  </section>;
}
