/** Schema metadata driving the model config view (admin-enhancement §3.4).
 * Field semantics live HERE, not in the API: the GET endpoint returns a
 * generic tree, so a future config restructure only touches this table.
 * Keys not listed here fall into the "advanced" bucket untouched. */

export type ConfigFieldType = 'number' | 'string' | 'boolean' | 'list' | 'object';

export interface ConfigFieldMeta {
  key: string;
  type: ConfigFieldType;
  unit?: string;
  /** Dangerous at edit time (M2 asks for confirmation): device placement,
   * accelerator switch, lifecycle hooks. */
  danger?: boolean;
}

export type ConfigGroupKey =
  | 'batching'
  | 'resources'
  | 'queue'
  | 'lifecycle'
  | 'resilience'
  | 'health'
  | 'policies';

export interface ConfigGroupMeta {
  key: ConfigGroupKey;
  fields: ConfigFieldMeta[];
}

export const MODEL_CONFIG_GROUPS: ConfigGroupMeta[] = [
  {
    key: 'batching',
    fields: [
      { key: 'max_batch_size', type: 'number' },
      { key: 'batch_timeout', type: 'number', unit: 's' },
      { key: 'continuous_batching', type: 'boolean' },
      { key: 'adaptive_batching', type: 'boolean' },
      { key: 'min_batch_timeout', type: 'number', unit: 's' },
      { key: 'adaptive_queue_threshold', type: 'number' },
    ],
  },
  {
    key: 'resources',
    fields: [
      { key: 'accelerator', type: 'string', danger: true },
      { key: 'devices', type: 'list', danger: true },
      { key: 'workers_per_device', type: 'number' },
      { key: 'startup_concurrency', type: 'number' },
    ],
  },
  {
    key: 'queue',
    fields: [
      { key: 'max_queue_size', type: 'number' },
      { key: 'queue_timeout_secs', type: 'number', unit: 's' },
      { key: 'queue_timeout_action', type: 'string' },
      { key: 'request_timeout', type: 'number', unit: 's' },
      { key: 'max_concurrent_streams', type: 'number' },
    ],
  },
  {
    key: 'lifecycle',
    fields: [
      { key: 'hot_reload', type: 'boolean' },
      { key: 'hot_reload_patterns', type: 'list' },
      { key: 'max_requests', type: 'number' },
      { key: 'max_requests_jitter', type: 'number' },
      { key: 'recycle_max_percent', type: 'number', unit: '%' },
      { key: 'count_streams_toward_max_requests', type: 'boolean' },
      { key: 'recycle_stream_drain_timeout_secs', type: 'number', unit: 's' },
      { key: 'recycle_stream_grace_ms', type: 'number', unit: 'ms' },
      { key: 'stream', type: 'boolean' },
    ],
  },
  {
    key: 'resilience',
    fields: [
      { key: 'max_retries', type: 'number' },
      { key: 'ejection_error_threshold', type: 'number' },
      { key: 'ejection_timeout', type: 'number', unit: 's' },
      { key: 'ejection_max_percent', type: 'number', unit: '%' },
      { key: 'ejection_max_timeout', type: 'number', unit: 's' },
      { key: 'startup_timeout', type: 'number', unit: 's' },
    ],
  },
  {
    key: 'health',
    fields: [
      { key: 'health_check_interval', type: 'number', unit: 's' },
      { key: 'health_check_timeout', type: 'number', unit: 's' },
      { key: 'health_check_kill_threshold', type: 'number' },
      { key: 'worker_kill_timeout', type: 'number', unit: 's' },
    ],
  },
  {
    key: 'policies',
    fields: [
      { key: 'hooks', type: 'object', danger: true },
      { key: 'policies', type: 'object' },
    ],
  },
];

export interface GroupedConfig {
  groups: { meta: ConfigGroupMeta; entries: [string, unknown][] }[];
  advanced: [string, unknown][];
}

/** Split a config tree into schema-known groups plus an "advanced" bucket
 * for keys the schema doesn't know (custom / ensemble / future keys). */
export function groupModelConfig(config: Record<string, unknown>): GroupedConfig {
  const known = new Map<string, ConfigGroupMeta>();
  for (const g of MODEL_CONFIG_GROUPS) {
    for (const f of g.fields) known.set(f.key, g);
  }
  const byGroup = new Map<ConfigGroupKey, [string, unknown][]>();
  const advanced: [string, unknown][] = [];
  for (const [k, v] of Object.entries(config)) {
    const g = known.get(k);
    if (g) {
      const list = byGroup.get(g.key) ?? [];
      list.push([k, v]);
      byGroup.set(g.key, list);
    } else {
      advanced.push([k, v]);
    }
  }
  return {
    groups: MODEL_CONFIG_GROUPS.filter((g) => byGroup.has(g.key)).map((g) => ({
      meta: g,
      entries: byGroup.get(g.key)!,
    })),
    advanced,
  };
}
