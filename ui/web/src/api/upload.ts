//! Resumable chunked model upload (>1GiB files).
//!
//! Mirrors the server-side upload-session protocol: init once, PUT 16MiB
//! chunks (idempotent, retried with backoff), then complete. The session id
//! plus the file fingerprints live in localStorage, so a refreshed page
//! resumes from the server's received-bitmap instead of starting over.
//! Small selections still go through the legacy single multipart request —
//! one round trip beats four when there is nothing to resume.

import { ApiError, apiFetch } from './client';
import { uploadModelFiles, type UploadHandle, type UploadResult } from './mutations';

const enc = encodeURIComponent;

/** Below this total size the legacy multipart POST is the better path. */
const SMALL_TOTAL_BYTES = 32 * 1024 * 1024;
const CHUNK_CONCURRENCY = 3;
const MAX_CHUNK_ATTEMPTS = 5;

export interface ChunkedUploadOptions {
  load?: boolean;
  onProgress?: (percent: number, loaded: number, total: number) => void;
  /** Constant per-retry delay in ms (tests pass 0; default is exponential). */
  retryDelayMs?: number;
}

interface SessionRecord {
  sessionId: string;
  fingerprints: string[];
}

interface SessionInfo {
  session_id: string;
  chunk_size: number;
  files: { name: string; received_chunks: number[] }[];
}

function storageKey(inst: string, model: string, version: string): string {
  return `lite-ui-upload-sess:${inst}:${model}:${version}`;
}

function fingerprint(file: File): string {
  return `${file.name}:${file.size}:${file.lastModified}`;
}

function loadRecord(key: string): SessionRecord | null {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as SessionRecord) : null;
  } catch {
    return null;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function retryDelay(opts: ChunkedUploadOptions, attempt: number): number {
  return opts.retryDelayMs ?? 500 * 2 ** attempt;
}

/**
 * Chunked upload of every file in one session. The returned handle mirrors
 * the legacy multipart one: abort stops new chunks (the session stays
 * resumable server-side; re-submitting the same files picks up where it
 * left off).
 */
export function uploadModelChunked(
  instanceId: string,
  model: string,
  version: string,
  files: File[],
  opts: ChunkedUploadOptions = {},
): UploadHandle {
  const controller = new AbortController();
  const signal = controller.signal;
  const base = `/v2/repository/models/${enc(model)}/versions/${enc(version)}/upload-sessions`;
  const key = storageKey(instanceId, model, version);
  const totalBytes = files.reduce((sum, f) => sum + f.size, 0);
  let doneBytes = 0;

  const report = () => {
    opts.onProgress?.(
      totalBytes === 0 ? 100 : Math.round((doneBytes / totalBytes) * 100),
      doneBytes,
      totalBytes,
    );
  };

  const aborted = () => signal.aborted;
  const throwIfAborted = () => {
    if (aborted()) throw new ApiError(0, null, null, 'upload cancelled');
  };

  const promise = (async (): Promise<UploadResult> => {
    // 1. Resolve the session: reuse a stored one when the file selection is
    // unchanged and the server still has it; otherwise init fresh.
    const fingerprints = files.map(fingerprint);
    let sessionId: string | null = null;
    let chunkSize = 0;
    let received: number[][] = files.map(() => []);

    const stored = loadRecord(key);
    if (
      stored &&
      stored.fingerprints.length === fingerprints.length &&
      stored.fingerprints.every((f, i) => f === fingerprints[i])
    ) {
      try {
        const info = await apiFetch<SessionInfo>(instanceId, `${base}/${enc(stored.sessionId)}`, {
          signal,
        });
        sessionId = stored.sessionId;
        chunkSize = info.chunk_size;
        received = files.map((_, i) => info.files[i]?.received_chunks ?? []);
      } catch (err) {
        if (!(err instanceof ApiError && err.status === 404)) throw err;
        localStorage.removeItem(key);
      }
    }
    if (!sessionId) {
      const created = await apiFetch<{ session_id: string; chunk_size: number }>(
        instanceId,
        base,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            files: files.map((f) => ({ name: f.name, size: f.size })),
          }),
          signal,
        },
      );
      sessionId = created.session_id;
      chunkSize = created.chunk_size;
      localStorage.setItem(key, JSON.stringify({ sessionId, fingerprints } satisfies SessionRecord));
    }

    // Bytes already on the server count toward progress from the start.
    files.forEach((f, i) => {
      doneBytes += Math.min(received[i].length * chunkSize, f.size);
    });
    report();

    // 2. Upload the missing chunks of each file (bounded concurrency).
    for (let fi = 0; fi < files.length; fi++) {
      const file = files[fi];
      const chunkCount = Math.ceil(file.size / chunkSize);
      const have = new Set(received[fi]);
      const pending: number[] = [];
      for (let ci = 0; ci < chunkCount; ci++) {
        if (!have.has(ci)) pending.push(ci);
      }
      let cursor = 0;
      const worker = async () => {
        while (cursor < pending.length) {
          throwIfAborted();
          const ci = pending[cursor++];
          const blob = file.slice(ci * chunkSize, Math.min((ci + 1) * chunkSize, file.size));
          let attempt = 0;
          for (;;) {
            try {
              await apiFetch(
                instanceId,
                `${base}/${enc(sessionId!)}/files/${fi}/chunks/${ci}`,
                { method: 'PUT', body: blob, signal },
              );
              break;
            } catch (err) {
              if (err instanceof ApiError && err.status !== 0 && err.status < 500) throw err;
              if (aborted()) throw err;
              attempt += 1;
              if (attempt >= MAX_CHUNK_ATTEMPTS) throw err;
              await sleep(retryDelay(opts, attempt - 1));
            }
          }
          doneBytes += blob.size;
          report();
        }
      };
      await Promise.all(
        Array.from({ length: Math.min(CHUNK_CONCURRENCY, pending.length) }, worker),
      );
    }

    // 3. Finalize server-side (concat + hash check + atomic commit).
    throwIfAborted();
    const result = await apiFetch<UploadResult>(
      instanceId,
      `${base}/${enc(sessionId)}/complete?load=${opts.load ?? true}`,
      { method: 'POST', signal },
    );
    localStorage.removeItem(key);
    return result;
  })();

  return { promise, abort: () => controller.abort() };
}

/**
 * Smart entry point: small selections take the legacy one-shot multipart;
 * anything big enough to care about continuity goes chunked/resumable.
 */
export function uploadModelFilesResumable(
  instanceId: string,
  model: string,
  version: string,
  files: File[],
  opts: ChunkedUploadOptions = {},
): UploadHandle {
  const total = files.reduce((sum, f) => sum + f.size, 0);
  if (total < SMALL_TOTAL_BYTES) {
    return uploadModelFiles(instanceId, model, version, files, opts);
  }
  return uploadModelChunked(instanceId, model, version, files, opts);
}

/** Test-only surface (keeps the protocol details out of the public API). */
export const __test__ = { storageKey, SMALL_TOTAL_BYTES };
