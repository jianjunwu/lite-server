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

export function useModelConfig(instanceId: string | null, model: string, version: string) {
  return useQuery({
    queryKey: [instanceId, 'model-config', model, version],
    queryFn: () =>
      apiFetch<ModelConfigResponse>(
        instanceId!,
        `/v2/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(version)}/config`,
      ),
    enabled: instanceId !== null && model !== '' && version !== '',
    refetchInterval: 30_000,
    retry: 1,
  });
}
