import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ApiError, apiFetch, apiFetchWithHeaders, bffFetch } from './client';
import { mergeModelList, mergeVersionList, type MergedModel, type MergedVersions } from './merge';
import type {
  AcceleratorReading,
  AlertsResponse,
  HealthSummary,
  InstanceInfo,
  ModelHealth,
  ModelList,
  ReadyResponse,
  RepoIndexResponse,
  ServerInfo,
  TimelineAllResponse,
  TimelineSnapshot,
  VersionsResponse,
} from './types';

export function useInstances() {
  return useQuery({
    queryKey: ['bff', 'instances'],
    queryFn: () => bffFetch<{ instances: InstanceInfo[] }>('/api/instances'),
    refetchInterval: 30_000,
  });
}

export function useServerInfo(instanceId: string | null) {
  return useQuery({
    queryKey: [instanceId, 'info'],
    queryFn: () => apiFetch<ServerInfo>(instanceId!, '/info'),
    enabled: instanceId !== null,
    retry: 1,
  });
}

export function useHealthSummary(instanceId: string | null) {
  return useQuery({
    queryKey: [instanceId, 'health'],
    queryFn: () => apiFetch<HealthSummary>(instanceId!, '/health'),
    enabled: instanceId !== null,
    refetchInterval: 10_000,
    retry: 1,
  });
}

export function useModels(instanceId: string | null) {
  return useQuery({
    queryKey: [instanceId, 'models'],
    queryFn: () => apiFetch<ModelList>(instanceId!, '/v2/models'),
    enabled: instanceId !== null,
    refetchInterval: 10_000,
  });
}

export function useVersions(instanceId: string | null, model: string) {
  return useQuery({
    queryKey: [instanceId, 'versions', model],
    queryFn: async (): Promise<VersionsResponse | null> => {
      try {
        return await apiFetch<VersionsResponse>(instanceId!, `/v2/models/${encodeURIComponent(model)}/versions`);
      } catch (err) {
        // Nothing loaded 404s here — that is a state (unloaded), not an error.
        if (err instanceof ApiError && err.status === 404) return null;
        throw err;
      }
    },
    // An empty model would poll /v2/models//versions (404) forever.
    enabled: instanceId !== null && model !== '',
    refetchInterval: 10_000,
  });
}

/** On-disk repository scan (POST is the KServe shape; the call is read-only). */
export function useRepoIndex(instanceId: string | null) {
  return useQuery({
    queryKey: [instanceId, 'repo-index'],
    queryFn: () => apiFetch<RepoIndexResponse>(instanceId!, '/v2/repository/index', { method: 'POST' }),
    enabled: instanceId !== null,
    refetchInterval: 10_000,
    retry: 1,
  });
}

export interface MergedModelsResult {
  data: MergedModel[];
  isLoading: boolean;
  /** True on older servers without /v2/repository/index — the list then
   * degrades to the loaded-only view. */
  repoUnavailable: boolean;
}

/** Models page data: every model in the repository plus runtime state. */
export function useMergedModels(instanceId: string | null): MergedModelsResult {
  const repoQuery = useRepoIndex(instanceId);
  const modelsQuery = useModels(instanceId);
  const data = useMemo(
    () => mergeModelList(repoQuery.data?.models, modelsQuery.data?.models),
    [repoQuery.data, modelsQuery.data],
  );
  return {
    data,
    isLoading: modelsQuery.isLoading || repoQuery.isLoading,
    repoUnavailable: repoQuery.isError,
  };
}

export interface MergedVersionsResult extends MergedVersions {
  isLoading: boolean;
  /** The model exists in the on-disk repository. */
  inRepo: boolean;
  /** At least one version is currently loaded. */
  hasLoaded: boolean;
}

/** One model's versions: registry state overlaid on the repository scan. */
export function useMergedVersions(instanceId: string | null, model: string): MergedVersionsResult {
  const repoQuery = useRepoIndex(instanceId);
  const versionsQuery = useVersions(instanceId, model);
  const merged = useMemo(
    () =>
      mergeVersionList(
        (repoQuery.data?.models ?? []).filter((e) => e.name === model),
        versionsQuery.data ?? null,
      ),
    [repoQuery.data, versionsQuery.data, model],
  );
  return {
    ...merged,
    isLoading: versionsQuery.isLoading || repoQuery.isLoading,
    inRepo: (repoQuery.data?.models ?? []).some((e) => e.name === model),
    hasLoaded: merged.versions.some((v) => v.loaded),
  };
}

export function useModelReady(instanceId: string | null, model: string, version?: string, active = true) {
  const path = version
    ? `/v2/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(version)}/ready`
    : `/v2/models/${encodeURIComponent(model)}/ready`;
  return useQuery({
    queryKey: [instanceId, 'ready', model, version ?? null],
    queryFn: () => apiFetch<ReadyResponse>(instanceId!, path),
    enabled: instanceId !== null && active,
    refetchInterval: 10_000,
  });
}

export function useModelHealth(instanceId: string | null, model: string, version?: string, active = true) {
  const path = version
    ? `/v2/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(version)}/health`
    : `/v2/models/${encodeURIComponent(model)}/health`;
  return useQuery({
    queryKey: [instanceId, 'model-health', model, version ?? null],
    queryFn: () => apiFetch<ModelHealth>(instanceId!, path),
    enabled: instanceId !== null && active,
    refetchInterval: 10_000,
  });
}

export interface TimelineAllResult extends TimelineAllResponse {
  /** X-Timeline-Coverage header: retention window in seconds. Undefined on
   * instances older than the M3 schema. */
  coverageSeconds?: number;
  /** X-Timeline-Interval header: timeline point spacing in seconds. */
  intervalSeconds?: number;
}

function positiveHeader(headers: Headers, name: string): number | undefined {
  const v = Number(headers.get(name));
  return Number.isFinite(v) && v > 0 ? v : undefined;
}

export function useTimelineAll(instanceId: string | null, refetchInterval: number | false = 5_000, step?: number) {
  return useQuery({
    queryKey: [instanceId, 'timeline', step ?? null],
    queryFn: async (): Promise<TimelineAllResult> => {
      const path = step && step > 1 ? `/metrics/timeline?step=${step}` : '/metrics/timeline';
      const { data, headers } = await apiFetchWithHeaders<TimelineAllResponse>(instanceId!, path);
      return {
        ...data,
        coverageSeconds: positiveHeader(headers, 'x-timeline-coverage'),
        intervalSeconds: positiveHeader(headers, 'x-timeline-interval'),
      };
    },
    enabled: instanceId !== null,
    refetchInterval,
  });
}

export function useTimeline(instanceId: string | null, model: string, version?: string, refetchInterval = 5_000, active = true) {
  const path = version
    ? `/metrics/timeline/${encodeURIComponent(model)}/versions/${encodeURIComponent(version)}`
    : `/metrics/timeline/${encodeURIComponent(model)}`;
  return useQuery({
    queryKey: [instanceId, 'timeline', model, version ?? null],
    queryFn: () => apiFetch<TimelineSnapshot>(instanceId!, path),
    enabled: instanceId !== null && active,
    refetchInterval,
  });
}

export function useAlerts(instanceId: string | null, refetchInterval: number | false = 10_000) {
  return useQuery({
    queryKey: [instanceId, 'alerts'],
    queryFn: () => apiFetch<AlertsResponse>(instanceId!, '/metrics/alerts'),
    enabled: instanceId !== null,
    refetchInterval,
  });
}

/** M4: per-device accelerator readings. `null` data on 404 — the instance
 * predates the endpoint or runs with features.accelerator_metrics off. */
export function useAcceleratorMetrics(instanceId: string | null, refetchInterval: number | false = 10_000, active = true) {
  return useQuery({
    queryKey: [instanceId, 'accelerator'],
    queryFn: async (): Promise<AcceleratorReading[] | null> => {
      try {
        return await apiFetch<AcceleratorReading[]>(instanceId!, '/metrics/accelerator');
      } catch (err) {
        if (err instanceof ApiError && err.status === 404) return null;
        throw err;
      }
    },
    enabled: instanceId !== null && active,
    refetchInterval,
  });
}
