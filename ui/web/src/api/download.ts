//! Resumable model download (>1GiB files).
//!
//! With the File System Access API the download streams straight to disk
//! via Range requests: progress ({etag, offset}) lives in localStorage, so
//! an interrupted download resumes with `Range: bytes={offset}-` guarded by
//! `If-Range` (a changed representation answers 200 and the download simply
//! restarts from zero). Browsers without the API fall back to plain anchor
//! navigation — the old behavior, no resume.

import { ApiError, getAdminKey, notifyBffUnauthorized } from './client';

// Not yet in the TS dom lib version this project pins.
declare global {
  interface Window {
    showSaveFilePicker?: (opts?: {
      suggestedName?: string;
    }) => Promise<FileSystemFileHandle>;
  }
}

const enc = encodeURIComponent;

/** Persist resume state at most once per this many bytes. */
const PERSIST_EVERY_BYTES = 4 * 1024 * 1024;

export interface DownloadHandle {
  promise: Promise<{ fileName: string }>;
  abort: () => void;
}

export interface DownloadOptions {
  onProgress?: (percent: number, loaded: number, total: number) => void;
}

interface ResumeState {
  etag: string;
  offset: number;
  fileName: string;
}

function storageKey(inst: string, model: string, version: string): string {
  return `lite-ui-download:${inst}:${model}:${version}`;
}

function loadState(key: string): ResumeState | null {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as ResumeState) : null;
  } catch {
    return null;
  }
}

export function canResumeDownload(): boolean {
  return typeof window !== 'undefined' && typeof window.showSaveFilePicker === 'function';
}

function suggestedName(model: string, version: string): string {
  return `${model}_v${version}.lma`;
}

/** Total representation length from a 200 (Content-Length) or 206 (Content-Range). */
function totalFrom(res: Response): number {
  const contentRange = res.headers.get('content-range');
  const match = contentRange?.match(/\/(\d+)$/);
  if (match) return Number(match[1]);
  return Number(res.headers.get('content-length') ?? 0);
}

export function downloadModelPackage(
  instanceId: string,
  model: string,
  version: string,
  opts: DownloadOptions = {},
): DownloadHandle {
  const url = `/api/i/${enc(instanceId)}/v2/repository/models/${enc(model)}/versions/${enc(version)}/download`;

  if (!canResumeDownload()) {
    // Fallback: no resume possible — hand the URL to the browser wholesale.
    const promise = (async () => {
      const a = document.createElement('a');
      a.href = url;
      a.download = suggestedName(model, version);
      document.body.appendChild(a);
      a.click();
      a.remove();
      return { fileName: suggestedName(model, version) };
    })();
    return { promise, abort: () => {} };
  }

  const controller = new AbortController();
  const key = storageKey(instanceId, model, version);

  const promise = (async (): Promise<{ fileName: string }> => {
    const stored = loadState(key);
    // Plain object, not a Headers instance: undici's Headers classifies
    // `range` as a no-cors-forbidden name and silently drops it, which
    // would turn every resume into a full re-download.
    const headers: Record<string, string> = { 'x-requested-with': 'lite-ui' };
    const adminKey = getAdminKey(instanceId);
    if (adminKey) headers['x-admin-key'] = adminKey;
    const resume = stored && stored.offset > 0;
    if (resume) {
      headers['range'] = `bytes=${stored.offset}-`;
      headers['if-range'] = stored.etag;
    }

    let res: Response;
    try {
      res = await fetch(url, { headers, signal: controller.signal });
    } catch (err) {
      if (controller.signal.aborted) throw new ApiError(0, null, null, 'download cancelled');
      throw new ApiError(0, null, null, err instanceof Error ? err.message : 'network error');
    }
    if (!res.ok) {
      const body = await res.json().catch(() => null);
      notifyBffUnauthorized(res.status, body);
      throw new ApiError(res.status, res.headers.get('x-request-id'), body, `HTTP ${res.status}`);
    }

    const etag = res.headers.get('etag') ?? '';
    const total = totalFrom(res);
    // A 206 resumes at the stored offset; a 200 (no range support, or a
    // stale If-Range) restarts the file from byte 0.
    let offset = res.status === 206 && resume ? stored.offset : 0;
    const fileName = stored?.fileName ?? suggestedName(model, version);

    let writable: FileSystemWritableFileStream;
    try {
      const handle = await window.showSaveFilePicker!({ suggestedName: fileName });
      writable = await handle.createWritable({ keepExistingData: true });
    } catch (err) {
      // The response body is still streaming — abort and cancel it so the
      // connection frees immediately instead of relaying the whole pack.
      controller.abort();
      await res.body?.cancel().catch(() => {});
      if (err instanceof DOMException && err.name === 'AbortError') {
        throw new ApiError(0, null, null, 'download cancelled');
      }
      throw err;
    }

    const persist = () => {
      localStorage.setItem(key, JSON.stringify({ etag, offset, fileName } satisfies ResumeState));
    };

    let sincePersist = 0;
    const report = () => {
      if (total > 0) opts.onProgress?.(Math.round((offset / total) * 100), offset, total);
    };
    try {
      const reader = res.body!.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        await writable.write({ type: 'write', position: offset, data: value });
        offset += value.byteLength;
        sincePersist += value.byteLength;
        if (sincePersist >= PERSIST_EVERY_BYTES) {
          persist();
          sincePersist = 0;
        }
        report();
      }
      await writable.close();
    } catch (err) {
      // Keep the written bytes and the resume state — the next attempt
      // picks up at `offset`. Release the handle without closing.
      persist();
      await writable.abort().catch(() => {});
      if (controller.signal.aborted) throw new ApiError(0, null, null, 'download cancelled');
      throw err instanceof Error ? err : new Error(String(err));
    }

    localStorage.removeItem(key);
    report();
    return { fileName };
  })();

  return { promise, abort: () => controller.abort() };
}

/** Test-only surface. */
export const __test__ = { storageKey };
