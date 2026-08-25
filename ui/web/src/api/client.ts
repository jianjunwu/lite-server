const ADMIN_KEY_PREFIX = 'lite-ui-admin-key:';

/** Browser-supplied admin key, isolated per instance and per browser session. */
export function getAdminKey(instanceId: string): string | null {
  return sessionStorage.getItem(ADMIN_KEY_PREFIX + instanceId);
}

export function setAdminKey(instanceId: string, key: string | null) {
  if (key) sessionStorage.setItem(ADMIN_KEY_PREFIX + instanceId, key);
  else sessionStorage.removeItem(ADMIN_KEY_PREFIX + instanceId);
}

export class ApiError extends Error {
  constructor(
    public status: number,
    public requestId: string | null,
    public body: unknown,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

/** Called when the BFF itself rejects the session (401 {error:'bff_unauthenticated'}).
 * Registered once by AuthProvider. Instance 401s never trigger this. */
let onBffUnauthorized: (() => void) | null = null;

export function setOnBffUnauthorized(handler: (() => void) | null) {
  onBffUnauthorized = handler;
}

/** Exported for raw fetch/XHR callers (playground, uploads) so every code
 * path drops an expired BFF session the same way. */
export function notifyBffUnauthorized(status: number, body: unknown) {
  if (
    status === 401 &&
    body !== null &&
    typeof body === 'object' &&
    (body as { error?: unknown }).error === 'bff_unauthenticated'
  ) {
    onBffUnauthorized?.();
  }
}

async function parseErrorBody(res: Response): Promise<unknown> {
  const text = await res.text().catch(() => '');
  try {
    return JSON.parse(text);
  } catch {
    return text || null;
  }
}

/** Human-readable message out of an upstream error envelope: `error` may be a
 * plain string (BFF) or an object `{code, message, ...}` (instance server). */
function errorMessage(body: unknown, status: number): string {
  if (body && typeof body === 'object' && 'error' in body) {
    const err = (body as { error: unknown }).error;
    if (err === 'forbidden' && (body as { reason?: unknown }).reason === 'model_denied') {
      const model = (body as { model?: unknown }).model;
      return `No access to model "${typeof model === 'string' ? model : '?'}"`;
    }
    if (typeof err === 'string') return err;
    if (err && typeof err === 'object' && typeof (err as { message?: unknown }).message === 'string') {
      return (err as { message: string }).message;
    }
  }
  return `HTTP ${status}`;
}

/** Fetch against the BFF; throws ApiError carrying the upstream x-request-id. */
export async function apiFetch<T>(instanceId: string, path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set('x-requested-with', 'lite-ui');
  const key = getAdminKey(instanceId);
  if (key && !headers.has('x-admin-key')) headers.set('x-admin-key', key);

  let res: Response;
  try {
    res = await fetch(`/api/i/${encodeURIComponent(instanceId)}${path}`, { ...init, headers });
  } catch (err) {
    throw new ApiError(0, null, null, err instanceof Error ? err.message : 'network error');
  }
  if (!res.ok) {
    const body = await parseErrorBody(res);
    notifyBffUnauthorized(res.status, body);
    const message = errorMessage(body, res.status);
    throw new ApiError(res.status, res.headers.get('x-request-id'), body, message);
  }
  return res.json() as Promise<T>;
}

/** Fetch against the BFF itself (not an instance). */
export async function bffFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { 'x-requested-with': 'lite-ui', ...init?.headers },
  });
  if (!res.ok) {
    const body = await parseErrorBody(res);
    notifyBffUnauthorized(res.status, body);
    throw new ApiError(res.status, res.headers.get('x-request-id'), body, `HTTP ${res.status}`);
  }
  return res.json() as Promise<T>;
}
