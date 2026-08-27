/** Schema metadata driving the model config view (admin-enhancement §3.4).
 * Field semantics live HERE, not in the API: the GET endpoint returns a
 * generic tree, so a future config restructure only touches this table.
 * Keys not listed here fall into the "advanced" bucket untouched.
 *
 * defaultValue mirrors `impl Default for ModelConfig` in src/config.rs —
 * keep the two in sync when server defaults change; it is shown as the
 * placeholder of an unset field ("leave empty = server default"). */

export type ConfigFieldType = 'number' | 'string' | 'boolean' | 'list' | 'object';

export interface ConfigFieldMeta {
  key: string;
  type: ConfigFieldType;
  unit?: string;
  /** Server-side default (Rust `ModelConfig::default()`), shown as the
   * placeholder of unset fields. */
  defaultValue?: unknown;
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
      { key: 'max_batch_size', type: 'number', defaultValue: 1 },
      { key: 'batch_timeout', type: 'number', unit: 's', defaultValue: 0 },
      { key: 'continuous_batching', type: 'boolean', defaultValue: false },
      { key: 'adaptive_batching', type: 'boolean', defaultValue: false },
      { key: 'min_batch_timeout', type: 'number', unit: 's', defaultValue: 0.001 },
      { key: 'adaptive_queue_threshold', type: 'number', defaultValue: 10 },
    ],
  },
  {
    key: 'resources',
    fields: [
      { key: 'accelerator', type: 'string', danger: true, defaultValue: 'auto' },
      { key: 'devices', type: 'list', danger: true, defaultValue: 'auto' },
      { key: 'workers_per_device', type: 'number', defaultValue: 'auto' },
      { key: 'startup_concurrency', type: 'number', defaultValue: 1 },
    ],
  },
  {
    key: 'queue',
    fields: [
      { key: 'max_queue_size', type: 'number', defaultValue: 1000 },
      { key: 'queue_timeout_secs', type: 'number', unit: 's', defaultValue: 0 },
      { key: 'queue_timeout_action', type: 'string', defaultValue: 'delay' },
      { key: 'request_timeout', type: 'number', unit: 's', defaultValue: 0 },
      { key: 'max_concurrent_streams', type: 'number', defaultValue: 0 },
    ],
  },
  {
    key: 'lifecycle',
    fields: [
      { key: 'hot_reload', type: 'boolean', defaultValue: false },
      { key: 'hot_reload_patterns', type: 'list', defaultValue: ['*.py'] },
      { key: 'max_requests', type: 'number', defaultValue: 0 },
      { key: 'max_requests_jitter', type: 'number', defaultValue: 0 },
      { key: 'recycle_max_percent', type: 'number', unit: '%', defaultValue: 10 },
      { key: 'count_streams_toward_max_requests', type: 'boolean', defaultValue: true },
      { key: 'recycle_stream_drain_timeout_secs', type: 'number', unit: 's', defaultValue: 60 },
      { key: 'recycle_stream_grace_ms', type: 'number', unit: 'ms', defaultValue: 2000 },
      { key: 'stream', type: 'boolean', defaultValue: false },
    ],
  },
  {
    key: 'resilience',
    fields: [
      { key: 'max_retries', type: 'number', defaultValue: 3 },
      { key: 'ejection_error_threshold', type: 'number', defaultValue: 3 },
      { key: 'ejection_timeout', type: 'number', unit: 's', defaultValue: 30 },
      { key: 'ejection_max_percent', type: 'number', unit: '%', defaultValue: 50 },
      { key: 'ejection_max_timeout', type: 'number', unit: 's', defaultValue: 300 },
      { key: 'startup_timeout', type: 'number', unit: 's', defaultValue: 60 },
    ],
  },
  {
    key: 'health',
    fields: [
      { key: 'health_check_interval', type: 'number', unit: 's', defaultValue: 15 },
      { key: 'health_check_timeout', type: 'number', unit: 's', defaultValue: 5 },
      { key: 'health_check_kill_threshold', type: 'number', defaultValue: 0 },
      { key: 'worker_kill_timeout', type: 'number', unit: 's', defaultValue: 10 },
    ],
  },
  {
    key: 'policies',
    fields: [
      { key: 'hooks', type: 'object', danger: true, defaultValue: {} },
      { key: 'policies', type: 'object', defaultValue: {} },
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
