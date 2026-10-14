import { useEffect, useState, type FormEvent, type ReactNode } from 'react';
import { Box, LogOut, KeyRound } from 'lucide-react';
import { api, post, setVerificationToken, type Session } from './api';

export function Authentication({ children }: { children: (session: Session) => ReactNode }) {
  const [session, setSession] = useState<Session>(), [pending, setPending] = useState(true), [error, setError] = useState('');
  useEffect(() => {
    const controller = new AbortController();
    api<Session>('/auth/session', { signal: controller.signal }).then(value => { setVerificationToken(value.csrf); setSession(value); })
      .catch(error => { if (!controller.signal.aborted && error.status !== 401) setError(String(error)); }).finally(() => { if (!controller.signal.aborted) setPending(false); });
    const expired = () => { setVerificationToken(''); setSession(undefined); };
    window.addEventListener('ud-session-expired', expired);
    return () => { controller.abort(); window.removeEventListener('ud-session-expired', expired); };
  }, []);
  if (pending) return <main className="login-page" aria-busy="true">正在检查会话…</main>;
  if (session) return children(session);
  return <Login error={error} authenticated={value => { setVerificationToken(value.csrf); setSession(value); setError(''); }}/>;
}
function Login({ error: initialError, authenticated }: { error: string; authenticated: (session: Session) => void }) {
  const [subject, setSubject] = useState(''), [password, setPassword] = useState(''), [error, setError] = useState(initialError), [busy, setBusy] = useState(false);
  async function login(event: FormEvent) {
    event.preventDefault(); if (busy) return; setBusy(true); setError('');
    try { authenticated(await post<Session>('/auth/login', { subject, password })); }
    catch (error) { setError(error instanceof Error ? error.message : '登录失败'); }
    finally { setPassword(''); setBusy(false); }
  }
  return <main className="login-page"><form className="login-card" onSubmit={event => void login(event)}><Box size={36} color="#087f8c"/><h1>登录 UniversalData</h1><p className="muted">使用管理员为你配置的账户。</p>
    <label>账户<input autoComplete="username" required maxLength={128} value={subject} onChange={event => setSubject(event.target.value)}/></label>
    <label>密码<input type="password" autoComplete="current-password" required maxLength={1024} value={password} onChange={event => setPassword(event.target.value)}/></label>
    {error ? <p className="error" role="alert">{error}</p> : null}<button className="primary" disabled={busy} type="submit">{busy ? '正在登录…' : '登录'}</button></form></main>;
}
export function SessionControls({ session }: { session: Session }) {
  const [changing, setChanging] = useState(false), [currentPassword, setCurrent] = useState(''), [password, setPassword] = useState(''), [confirmation, setConfirmation] = useState(''), [error, setError] = useState(''), [busy, setBusy] = useState(false);
  function end() { setVerificationToken(''); window.dispatchEvent(new Event('ud-session-expired')); }
  async function logout() { try { await post('/auth/logout'); end(); } catch(error) { setError(String(error)); } }
  async function change(event: FormEvent) {
    event.preventDefault(); if (password !== confirmation) { setError('两次新密码不一致'); return; }
    setBusy(true); setError('');
    try { await post('/auth/password', { currentPassword, password }); end(); }
    catch(error) { setError(String(error)); } finally { setCurrent(''); setPassword(''); setConfirmation(''); setBusy(false); }
  }
  return <div className="session-controls"><span title={`租户：${session.tenant}`}>{session.subject}</span><button aria-label="修改密码" onClick={() => { setError(''); setChanging(true); }}><KeyRound size={15}/></button><button aria-label="退出登录" onClick={() => void logout()}><LogOut size={15}/></button>
    {error && !changing ? <span role="alert" className="error">{error}</span> : null}
    {changing ? <div className="modal-backdrop"><form className="modal" onSubmit={event => void change(event)}><h2>修改密码</h2><p>修改后，所有已登录会话都会失效。</p>
      <label>当前密码<input required type="password" autoComplete="current-password" value={currentPassword} onChange={event => setCurrent(event.target.value)}/></label>
      <label>新密码<input required type="password" autoComplete="new-password" minLength={12} maxLength={1024} value={password} onChange={event => setPassword(event.target.value)}/></label>
      <label>确认新密码<input required type="password" autoComplete="new-password" value={confirmation} onChange={event => setConfirmation(event.target.value)}/></label>
      {error ? <p role="alert" className="error">{error}</p> : null}<div><button type="button" onClick={() => { setChanging(false); setCurrent(''); setPassword(''); setConfirmation(''); }}>取消</button><button className="primary" type="submit" disabled={busy}>修改并重新登录</button></div>
    </form></div> : null}</div>;
}
