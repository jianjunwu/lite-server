import { useQuery } from '@tanstack/react-query';
import { apiFetch } from './client';

/** M1 (admin-enhancement plan §3.3): model version config read — file values
 * as a generic JSON tree, secrets already redacted server-side. */
export interface ModelConfigResponse {
  model: string;
  version: string;
  config: Record<string, unknown>;
  has_file: boolean;
  redacted: string[];
  etag: string | null;
  loaded_at: number | null;
}

export function useModelConfig(
  instanceId: string | null,
  model: string,
  version: string,
  opts?: { pausePolling?: boolean },
) {
  return useQuery({
    queryKey: [instanceId, 'model-config', model, version],
    queryFn: () =>
      apiFetch<ModelConfigResponse>(
        instanceId!,
        `/v2/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(version)}/config`,
      ),
    enabled: instanceId !== null && model !== '' && version !== '',
    // Edit mode pauses polling so a background refresh can't clobber the
    // local draft (plan §4.0).
    refetchInterval: opts?.pausePolling ? false : 30_000,
    retry: 1,
  });
}

// ---- M2: PATCH (plan §3.3) -------------------------------------------------

export type ConfigPatchMode = 'apply_reload' | 'write_only' | 'dry_run';

export interface ModelConfigPatchRequest {
  /** RFC 7386 merge-patch against the on-disk config.yaml tree. */
  patch: Record<string, unknown>;
  /** etag from the last read; optimistic-concurrency precondition. */
  if_match?: string | null;
  /** Bypass the etag precondition (after a 409). */
  force?: boolean;
  mode?: ConfigPatchMode;
}

export interface ModelConfigPatchResponse {
  model: string;
  version: string;
  mode: string;
  valid: boolean;
  written: boolean;
  reloaded: boolean;
  etag: string | null;
  warnings: string[];
}

export function patchModelConfig(
  instanceId: string,
  model: string,
  version: string,
  req: ModelConfigPatchRequest,
): Promise<ModelConfigPatchResponse> {
  return apiFetch<ModelConfigPatchResponse>(
    instanceId,
    `/v2/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(version)}/config`,
    {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'apply_reload', ...req }),
    },
  );
}

// ---- M5: server (instance) config read (plan §3.6) -------------------------

export type ServerConfigSource = 'cli' | 'file' | 'default';

/** Effective server Config as a JSON tree with per-leaf source labels.
 * Sources are approximate (see plan §3.6): a value explicitly written into
 * the file that equals the built-in default reads as "default". Secrets
 * arrive already redacted server-side. */
export interface ServerConfigResponse {
  config: Record<string, unknown>;
  sources: Record<string, ServerConfigSource>;
  redacted: string[];
}

export function useServerConfig(instanceId: string | null) {
  return useQuery({
    queryKey: [instanceId, 'server-config'],
    queryFn: () => apiFetch<ServerConfigResponse>(instanceId!, '/v2/server/config'),
    enabled: instanceId !== null,
    refetchInterval: 30_000,
    retry: 1,
  });
}
