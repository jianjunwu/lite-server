export function formatNumber(n: number): string {
  if (!Number.isFinite(n)) return '-';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return Number.isInteger(n) ? String(n) : n.toFixed(1);
}

export function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n < 0) return '-';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = n;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(1)} ${units[unit]}`;
}

export function formatMs(ms: number): string {
  if (!Number.isFinite(ms)) return '-';
  if (ms < 1) return `${(ms * 1000).toFixed(0)}µs`;
  if (ms < 1000) return `${ms.toFixed(1)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

/** Unix seconds -> "3m2s ago"-style relative duration. */
export function formatAge(unixSecs: number | null | undefined, nowSecs = Date.now() / 1000): string {
  if (unixSecs == null) return '-';
  const delta = Math.max(0, nowSecs - unixSecs);
  if (delta < 60) return `${Math.floor(delta)}s`;
  if (delta < 3600) return `${Math.floor(delta / 60)}m${Math.floor(delta % 60)}s`;
  if (delta < 86400) return `${Math.floor(delta / 3600)}h${Math.floor((delta % 3600) / 60)}m`;
  return `${Math.floor(delta / 86400)}d${Math.floor((delta % 86400) / 3600)}h`;
}

export function formatTime(unixSecs: number): string {
  return new Date(unixSecs * 1000).toLocaleTimeString();
}
