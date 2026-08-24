import { ApiError, getAdminKey } from './client';

const enc = encodeURIComponent;

// ---- SSE parsing ----------------------------------------------------------

/**
 * Incremental SSE frame parser: feed raw text chunks, get back complete
 * `data:` payloads. Frames are separated by a blank line; multi-line data
 * fields are joined with \n. The stream ends with a literal [DONE] payload.
 */
export class SseParser {
  private buffer = '';

  push(chunk: string): string[] {
    this.buffer += chunk;
    const events: string[] = [];
    // Frames are separated by a blank line; both LF and CRLF are legal.
    let match: RegExpExecArray | null;
    while ((match = /\r?\n\r?\n/.exec(this.buffer)) !== null) {
      const frame = this.buffer.slice(0, match.index);
      this.buffer = this.buffer.slice(match.index + match[0].length);
      const data = frame
        .split(/\r?\n/)
        .filter((line) => line.startsWith('data:'))
        .map((line) => line.slice(5).replace(/^ /, ''))
        .join('\n');
      if (data) events.push(data);
    }
    return events;
  }
}

export const SSE_DONE = '[DONE]';

// ---- endpoints ------------------------------------------------------------

function modelPath(model: string, version: string | null, tail: string): string {
  return version
    ? `/v2/models/${enc(model)}/versions/${enc(version)}/${tail}`
    : `/v2/models/${enc(model)}/${tail}`;
}

function authHeaders(instanceId: string): Record<string, string> {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    'x-requested-with': 'lite-ui',
  };
  const key = getAdminKey(instanceId);
  if (key) headers['x-admin-key'] = key;
  return headers;
}

export interface UnaryResult {
  status: number;
  durationMs: number;
  requestId: string | null;
  /** Pretty-printed JSON when parseable, raw text otherwise. */
  text: string;
}

export async function inferUnary(
  instanceId: string,
  model: string,
  version: string | null,
  body: string,
  signal?: AbortSignal,
): Promise<UnaryResult> {
  const started = performance.now();
  const res = await fetch(`/api/i/${enc(instanceId)}${modelPath(model, version, 'infer')}`, {
    method: 'POST',
    headers: authHeaders(instanceId),
    body,
    signal,
  });
  const raw = await res.text();
  const durationMs = performance.now() - started;
  let text = raw;
  try {
    text = JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    // Keep raw.
  }
  if (!res.ok) {
    let message = `HTTP ${res.status}`;
    try {
      const parsed = JSON.parse(raw) as { error?: unknown };
      if (parsed.error) message = String(parsed.error);
    } catch {
      // Keep default.
    }
    throw new ApiError(res.status, res.headers.get('x-request-id'), raw, message);
  }
  return { status: res.status, durationMs, requestId: res.headers.get('x-request-id'), text };
}

export interface StreamCallbacks {
  onEvent: (payload: string) => void;
  onDone: (durationMs: number) => void;
  onError: (err: Error) => void;
}

/** POST /events and consume the SSE stream; returns an abort function. */
export function streamEvents(
  instanceId: string,
  model: string,
  version: string | null,
  body: string,
  cb: StreamCallbacks,
): () => void {
  const controller = new AbortController();
  const started = performance.now();

  (async () => {
    const res = await fetch(`/api/i/${enc(instanceId)}${modelPath(model, version, 'events')}`, {
      method: 'POST',
      headers: authHeaders(instanceId),
      body,
      signal: controller.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new ApiError(res.status, res.headers.get('x-request-id'), text, `HTTP ${res.status}`);
    }
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    const parser = new SseParser();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      for (const payload of parser.push(decoder.decode(value, { stream: true }))) {
        if (payload === SSE_DONE) {
          // Release the connection instead of waiting for the upstream to
          // close it on its own.
          await reader.cancel().catch(() => {});
          cb.onDone(performance.now() - started);
          return;
        }
        cb.onEvent(payload);
      }
    }
    // Connection closed without [DONE] — treat as done (server finished early).
    cb.onDone(performance.now() - started);
  })().catch((err) => {
    if (controller.signal.aborted) {
      // Aborted (Stop button / page unmount): settle like a completed run so
      // callers awaiting send() are not left hanging.
      cb.onDone(performance.now() - started);
      return;
    }
    cb.onError(err instanceof Error ? err : new Error(String(err)));
  });

  return () => controller.abort();
}

// ---- request templates (localStorage, per model) ---------------------------

export interface RequestTemplate {
  name: string;
  body: string;
}

const tplKey = (model: string) => `lite-ui-tpl:${model}`;

export function listTemplates(model: string): RequestTemplate[] {
  try {
    const raw = localStorage.getItem(tplKey(model));
    const parsed = raw ? (JSON.parse(raw) as unknown) : [];
    return Array.isArray(parsed) ? (parsed as RequestTemplate[]) : [];
  } catch {
    return [];
  }
}

export function saveTemplate(model: string, name: string, body: string) {
  const list = listTemplates(model).filter((t) => t.name !== name);
  list.push({ name, body });
  localStorage.setItem(tplKey(model), JSON.stringify(list));
}

export function deleteTemplate(model: string, name: string) {
  localStorage.setItem(tplKey(model), JSON.stringify(listTemplates(model).filter((t) => t.name !== name)));
}

// ---- history (module memory; cleared on page refresh) ----------------------

export interface HistoryEntry {
  id: number;
  at: number;
  model: string;
  versionA: string | null;
  versionB: string | null;
  mode: 'unary' | 'stream';
  body: string;
  ok: boolean;
  durationMs: number | null;
}

let history: HistoryEntry[] = [];
let historySeq = 1;
const listeners = new Set<() => void>();

export function addHistory(entry: Omit<HistoryEntry, 'id' | 'at'>) {
  history = [{ ...entry, id: historySeq++, at: Date.now() }, ...history].slice(0, 50);
  listeners.forEach((fn) => fn());
}

export function getHistory(): HistoryEntry[] {
  return history;
}

export function subscribeHistory(fn: () => void): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}
