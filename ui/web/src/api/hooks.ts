import { useQuery } from '@tanstack/react-query';
import { apiFetch, bffFetch } from './client';
import type {
  AlertsResponse,
  HealthSummary,
  InstanceInfo,
  ModelHealth,
  ModelList,
  ReadyResponse,
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
    queryFn: () => apiFetch<VersionsResponse>(instanceId!, `/v2/models/${encodeURIComponent(model)}/versions`),
    enabled: instanceId !== null,
    refetchInterval: 10_000,
  });
}

export function useModelReady(instanceId: string | null, model: string, version?: string) {
  const path = version
    ? `/v2/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(version)}/ready`
    : `/v2/models/${encodeURIComponent(model)}/ready`;
  return useQuery({
    queryKey: [instanceId, 'ready', model, version ?? null],
    queryFn: () => apiFetch<ReadyResponse>(instanceId!, path),
    enabled: instanceId !== null,
    refetchInterval: 10_000,
  });
}

export function useModelHealth(instanceId: string | null, model: string, version?: string) {
  const path = version
    ? `/v2/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(version)}/health`
    : `/v2/models/${encodeURIComponent(model)}/health`;
  return useQuery({
    queryKey: [instanceId, 'model-health', model, version ?? null],
    queryFn: () => apiFetch<ModelHealth>(instanceId!, path),
    enabled: instanceId !== null,
    refetchInterval: 10_000,
  });
}

export function useTimelineAll(instanceId: string | null, refetchInterval: number | false = 5_000) {
  return useQuery({
    queryKey: [instanceId, 'timeline'],
    queryFn: () => apiFetch<TimelineAllResponse>(instanceId!, '/metrics/timeline'),
    enabled: instanceId !== null,
    refetchInterval,
  });
}

export function useTimeline(instanceId: string | null, model: string, version?: string, refetchInterval = 5_000) {
  const path = version
    ? `/metrics/timeline/${encodeURIComponent(model)}/versions/${encodeURIComponent(version)}`
    : `/metrics/timeline/${encodeURIComponent(model)}`;
  return useQuery({
    queryKey: [instanceId, 'timeline', model, version ?? null],
    queryFn: () => apiFetch<TimelineSnapshot>(instanceId!, path),
    enabled: instanceId !== null,
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
