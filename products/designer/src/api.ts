import { parse, stringify } from 'lossless-json';
export const decode = (text: string): unknown => parse(text, undefined, token => {
  if (/^-?\d+$/.test(token)) { const value = BigInt(token); if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) return value; }
  return Number(token);
});
export const encode = (value: unknown, space?: number): string => stringify(value, undefined, space) ?? 'null';
export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`/api${path}`, { ...options, headers: { 'Content-Type': 'application/json', ...options.headers } });
  const text = await response.text();
  if (!response.ok) {
    let message = `请求失败 (${response.status})`;
    try { const error = decode(text) as { error?: string }; message = error.error ?? message; } catch { /* Preserve HTTP status. */ }
    throw new Error(message);
  }
  return (text ? decode(text) : undefined) as T;
}
export const post = <T>(path: string, value?: unknown) => api<T>(path, { method: 'POST', body: value === undefined ? undefined : encode(value) });
