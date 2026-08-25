import { ApiError, apiFetch, getAdminKey, notifyBffUnauthorized, setAdminKey } from './client';

const enc = encodeURIComponent;

function mutate<T>(instanceId: string, path: string, method: string, body?: unknown): Promise<T> {
  return apiFetch<T>(instanceId, path, {
    method,
    headers: body !== undefined ? { 'content-type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
}

export const modelOps = {
  loadVersion: (inst: string, model: string, version: string) =>
    mutate(inst, `/v2/repository/models/${enc(model)}/versions/${enc(version)}/load`, 'POST'),
  unloadModel: (inst: string, model: string) =>
    mutate(inst, `/v2/repository/models/${enc(model)}/unload`, 'POST'),
  unloadVersion: (inst: string, model: string, version: string) =>
    mutate(inst, `/v2/repository/models/${enc(model)}/versions/${enc(version)}/unload`, 'POST'),
  reloadModel: (inst: string, model: string) => mutate(inst, `/v2/models/${enc(model)}/reload`, 'POST'),
  reloadVersion: (inst: string, model: string, version: string) =>
    mutate(inst, `/v2/models/${enc(model)}/versions/${enc(version)}/reload`, 'POST'),
  activateVersion: (inst: string, model: string, version: string) =>
    mutate(inst, `/v2/models/${enc(model)}/versions/${enc(version)}/activate`, 'POST'),
  deleteVersion: (inst: string, model: string, version: string, force = false) =>
    mutate(
      inst,
      `/v2/models/${enc(model)}/versions/${enc(version)}${force ? '?force=true' : ''}`,
      'DELETE',
    ),
  deleteModel: (inst: string, model: string, force = false) =>
    mutate(inst, `/v2/models/${enc(model)}${force ? '?force=true' : ''}`, 'DELETE'),
  setRouting: (inst: string, model: string, weights: Record<string, number>) =>
    mutate<{ success: boolean; weights: Record<string, number> }>(
      inst,
      `/v2/models/${enc(model)}/routing`,
      'PUT',
      { weights },
    ),
};

export interface WeightValidation {
  ok: boolean;
  sum: number;
}

/** Routing weights must be non-negative integers summing to 100. */
export function validateWeights(weights: Record<string, number>): WeightValidation {
  const values = Object.values(weights);
  const sum = values.reduce((a, b) => a + b, 0);
  const ok = values.every((v) => Number.isInteger(v) && v >= 0) && sum === 100;
  return { ok, sum };
}

// ---- 401 → admin key prompt → retry -------------------------------------

export type KeyRequester = (instanceId: string) => Promise<string | null>;

let keyRequester: KeyRequester | null = null;

/** Registered once by AdminKeyProvider; null in tests unless stubbed. */
export function setKeyRequester(requester: KeyRequester | null) {
  keyRequester = requester;
}

/**
 * Run an admin mutation; on the first 401, prompt for the instance admin key
 * (sessionStorage), then retry exactly once.
 */
export async function withAdminKeyRetry<T>(instanceId: string, fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    if (!(err instanceof ApiError) || err.status !== 401 || !keyRequester) throw err;
    // A BFF-side 401 ({error:'bff_unauthenticated'}) means the login session
    // expired — the auth flow handles it; an instance-key prompt here would
    // sit on top of the login redirect and the retry would fail anyway.
    if (
      err.body !== null &&
      typeof err.body === 'object' &&
      (err.body as { error?: unknown }).error === 'bff_unauthenticated'
    ) {
      throw err;
    }
    const key = await keyRequester(instanceId);
    if (!key) throw err;
    setAdminKey(instanceId, key);
    return fn();
  }
}

// ---- Upload (XHR for real upload-progress events) ------------------------

export interface UploadHandle {
  promise: Promise<UploadResult>;
  abort: () => void;
}

export interface UploadResult {
  success: boolean;
  model: string;
  version: string;
  files: string[];
  loaded: boolean;
  load_error?: string;
}

export function uploadModelFiles(
  instanceId: string,
  model: string,
  version: string,
  files: File[],
  opts: { load?: boolean; force?: boolean; onProgress?: (percent: number, loaded: number, total: number) => void } = {},
): UploadHandle {
  const xhr = new XMLHttpRequest();
  const load = opts.load ?? true;
  const url = `/api/i/${enc(instanceId)}/v2/repository/models/${enc(model)}/versions/${enc(version)}/upload?load=${load}${opts.force ? '&force=true' : ''}`;

  const promise = new Promise<UploadResult>((resolve, reject) => {
    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable && opts.onProgress) {
        opts.onProgress(Math.round((e.loaded / e.total) * 100), e.loaded, e.total);
      }
    });
    xhr.addEventListener('load', () => {
      let body: unknown = null;
      try {
        body = JSON.parse(xhr.responseText);
      } catch {
        // Non-JSON error body.
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(body as UploadResult);
      } else {
        // XHR bypasses client.ts — report an expired BFF session here.
        notifyBffUnauthorized(xhr.status, body);
        const message =
          body && typeof body === 'object' && 'error' in body
            ? String((body as { error: unknown }).error)
            : `HTTP ${xhr.status}`;
        reject(new ApiError(xhr.status, xhr.getResponseHeader('x-request-id'), body, message));
      }
    });
    xhr.addEventListener('error', () => reject(new ApiError(0, null, null, 'network error')));
    xhr.addEventListener('abort', () => reject(new ApiError(0, null, null, 'upload cancelled')));

    const form = new FormData();
    for (const file of files) form.append('files', file, file.name);

    xhr.open('POST', url);
    xhr.setRequestHeader('x-requested-with', 'lite-ui');
    const key = getAdminKey(instanceId);
    if (key) xhr.setRequestHeader('x-admin-key', key);
    xhr.send(form);
  });

  return { promise, abort: () => xhr.abort() };
}
