/** Schema metadata driving the instance (server) config view (M5,
 * admin-enhancement §3.6/§4.2). Mirrors modelConfigSchema: the API returns a
 * generic tree, so field semantics live HERE. Top-level sections not listed
 * fall into the "advanced" bucket untouched. */

import type { ServerConfigSource } from '../../api/config';

/** Top-level sections of the Rust `Config` struct, in display order. */
export const SERVER_CONFIG_SECTIONS = [
  'server',
  'grpc',
  'metrics',
  'alerts',
  'access_control',
  'openai_compact',
  'features',
  'model_defaults',
  'tunables',
  'orchestration',
  'model_repository',
  'rate_limit',
  'logging',
  'telemetry',
  'callbacks',
] as const;

export interface ServerConfigRow {
  /** Dot-joined leaf path, e.g. "server.http_port". */
  path: string;
  value: unknown;
  source: ServerConfigSource;
  redacted: boolean;
}

export interface ServerConfigGroup {
  /** Section key, or 'advanced' for sections the schema doesn't know. */
  key: string;
  rows: ServerConfigRow[];
}

/** Flatten a config tree into leaf rows: objects recurse, everything else
 * (scalars, arrays, nulls, empty objects) is a leaf compared wholesale. */
function flattenLeaves(value: unknown, path: string, out: [string, unknown][]): void {
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 0) {
      out.push([path, value]);
      return;
    }
    for (const [k, v] of entries) {
      flattenLeaves(v, path ? `${path}.${k}` : k, out);
    }
    return;
  }
  out.push([path, value]);
}

function isRedacted(path: string, redacted: string[]): boolean {
  return redacted.some(
    (r) => path === r || path.startsWith(`${r}.`) || path.startsWith(`${r}[`),
  );
}

/** Split the effective server config into per-section row groups with source
 * labels and redaction flags. Unknown top-level sections land in a single
 * trailing "advanced" group. */
export function groupServerConfig(
  config: Record<string, unknown>,
  sources: Record<string, ServerConfigSource>,
  redacted: string[],
): ServerConfigGroup[] {
  const known = new Set<string>(SERVER_CONFIG_SECTIONS);
  const bySection = new Map<string, ServerConfigRow[]>();
  const advanced: ServerConfigRow[] = [];
  for (const [section, value] of Object.entries(config)) {
    const leaves: [string, unknown][] = [];
    flattenLeaves(value, section, leaves);
    const rows = leaves.map(([path, v]) => ({
      path,
      value: v,
      source: sources[path] ?? 'default',
      redacted: isRedacted(path, redacted),
    }));
    if (known.has(section)) bySection.set(section, rows);
    else advanced.push(...rows);
  }
  const groups: ServerConfigGroup[] = SERVER_CONFIG_SECTIONS.filter((s) =>
    bySection.has(s),
  ).map((s) => ({ key: s, rows: bySection.get(s)! }));
  if (advanced.length > 0) groups.push({ key: 'advanced', rows: advanced });
  return groups;
}

/** antd Tag preset per source (plan §4.2): CLI orange, file blue, default gray. */
export function sourceTagColor(source: ServerConfigSource): string {
  switch (source) {
    case 'cli':
      return 'warning';
    case 'file':
      return 'processing';
    default:
      return 'default';
  }
}
