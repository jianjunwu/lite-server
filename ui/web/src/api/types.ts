// API response types, mirrored from lite-server handlers
// (src/http/handlers/admin.rs, health.rs; src/metrics/aggregator.rs).

export interface InstanceInfo {
  id: string;
  name: string;
  base_url: string;
  has_admin_key: boolean;
  readonly: boolean;
}

export interface ServerInfo {
  server: string;
  version: string;
  loaded_models: string[];
}

export interface HealthVersionEntry {
  version: string;
  status: string;
  workers: number;
  loaded_at: number | null;
  last_failure: string | null;
}

export interface HealthModelEntry {
  name: string;
  active_version: string | null;
  versions: HealthVersionEntry[];
}

export interface HealthSummary {
  status: 'ready' | 'not_ready';
  models: HealthModelEntry[];
}

export interface ModelListItem {
  name: string;
  version: string;
  status: string;
  model_type: string;
  workers: number;
}

export interface ModelList {
  models: ModelListItem[];
}

export interface VersionInfo {
  version: string;
  status: string;
  active: boolean;
  weight: number;
  workers: { ready: number; total: number };
  loaded_at: number | null;
}

export interface VersionsResponse {
  name: string;
  active_version: string | null;
  versions: VersionInfo[];
}

export interface ReadyResponse {
  name: string;
  version: string | null;
  ready: boolean;
  active_version: string | null;
}

export interface WorkerHealth {
  worker_id: number;
  healthy: boolean;
  ejected: boolean;
}

export interface ModelHealth {
  name: string;
  version: string;
  healthy_workers: number;
  total_workers: number;
  workers: WorkerHealth[];
}

export interface TimelineEntry {
  timestamp: number;
  qps: number;
  p99_ms: number;
  queue_depth: number;
  active_workers: number;
  active_streams: number;
}

export interface TimelineSnapshot {
  model: string;
  version: string;
  entries: TimelineEntry[];
}

export interface TimelineAllResponse {
  snapshots: TimelineSnapshot[];
}

export interface AlertItem {
  model: string;
  version: string;
  rule: string;
  message: string;
  severity: 'warning' | 'critical';
  timestamp: number;
  value: number;
  threshold: number;
}

export interface AlertsResponse {
  alerts: AlertItem[];
}
