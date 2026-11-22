import { parse, stringify } from "lossless-json";
export const decode = (text: string): unknown =>
  parse(text, undefined, (token) => {
    if (/^-?\d+$/.test(token)) {
      const value = BigInt(token);
      if (
        value > BigInt(Number.MAX_SAFE_INTEGER) ||
        value < BigInt(Number.MIN_SAFE_INTEGER)
      )
        return value;
    }
    return Number(token);
  });
export const encode = (value: unknown, space?: number): string =>
  stringify(value, undefined, space) ?? "null";
export interface Session {
  subject: string;
  tenant: string;
  csrf: string;
  grants: { permission: string; resource: string }[];
}
let verificationToken = "";
export function setVerificationToken(value: string) {
  verificationToken = value;
}
export class ApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly code?: number,
  ) {
    super(message);
  }
}
export const commitUnknown = (error: unknown): boolean =>
  error instanceof ApiError
    ? error.code === 16 || error.status === 502 || error.status === 504
    : error instanceof TypeError || error instanceof SyntaxError;

export async function request(
  path: string,
  options: RequestInit = {},
): Promise<Response> {
  const response = await fetch(`/api${path}`, {
    ...options,
    credentials: "same-origin",
    headers: {
      "Content-Type": "application/json",
      ...(verificationToken ? { "X-UD-CSRF": verificationToken } : {}),
      ...options.headers,
    },
  });
  if (!response.ok) {
    const text = await response.text();
    let message = `请求失败 (${response.status})`;
    let code: number | undefined;
    try {
      const error = decode(text) as { code?: number; error?: string | { message: string } };
      code = error.code;
      message = typeof error.error === "string" ? error.error : error.error?.message ?? message;
    } catch {
      /* Preserve HTTP status. */
    }
    if (response.status === 401 && path !== "/auth/login")
      window.dispatchEvent(new Event("ud-session-expired"));
    throw new ApiError(
      response.status === 401 ? "账户或密码无效，或会话已失效" : message,
      response.status,
      code,
    );
  }
  return response;
}
export async function api<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const response = await request(path, options);
  const text = await response.text();
  return (text ? decode(text) : undefined) as T;
}
export const post = <T>(path: string, value?: unknown) =>
  api<T>(path, {
    method: "POST",
    body: value === undefined ? undefined : encode(value),
  });
