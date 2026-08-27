import type { TimelineEntry } from '../api/types';
import type { MetricKey } from './timelineChart';

export interface ChartSpec {
  key: MetricKey;
  titleKey: string;
  /** Draws alert threshold lines on this chart (plan §5 — migrated from
   * MetricsPage's thresholdsFor). */
  rule?: 'p99_ms' | 'queue_depth';
  yAxisName?: string;
}

export type MetricGroupKey = 'throughput' | 'latency' | 'queue' | 'resources' | 'health';

/** Model-layer metric groups (plan §5): the single data source for the
 * model detail metrics tab, merged from MetricsPage's GROUP_CHARTS and the
 * old flat catalog. cpu_percent is deliberately absent — process-level, it
 * lives on the instance layer only (P1, one metric one home). */
export const METRIC_GROUPS: Record<MetricGroupKey, ChartSpec[]> = {
  throughput: [
    { key: 'qps', titleKey: 'metrics.qps' },
    { key: 'tokens_per_s', titleKey: 'metrics.tokensPerS' },
    { key: 'stream_bytes_per_s', titleKey: 'metrics.streamBytesPerS' },
  ],
  latency: [
    { key: 'p99_ms', titleKey: 'metrics.p99', rule: 'p99_ms', yAxisName: 'ms' },
    { key: 'ttft_p99_ms', titleKey: 'metrics.ttftP99', yAxisName: 'ms' },
    { key: 'tbt_p99_ms', titleKey: 'metrics.tbtP99', yAxisName: 'ms' },
  ],
  queue: [
    { key: 'queue_depth', titleKey: 'metrics.queueDepth', rule: 'queue_depth' },
    { key: 'in_flight', titleKey: 'metrics.inFlight' },
    { key: 'active_streams', titleKey: 'metrics.activeStreams' },
  ],
  resources: [
    { key: 'active_workers', titleKey: 'metrics.workers' },
    { key: 'worker_saturation', titleKey: 'metrics.saturation' },
    { key: 'rss_mb', titleKey: 'metrics.rss', yAxisName: 'MB' },
  ],
  health: [
    { key: 'retries_per_s', titleKey: 'metrics.retriesPerS' },
    { key: 'ejections_per_s', titleKey: 'metrics.ejectionsPerS' },
  ],
};

export const METRIC_GROUP_ORDER: MetricGroupKey[] = ['throughput', 'latency', 'queue', 'resources', 'health'];

export type RangeKey = '5m' | '15m' | '1h' | 'all';

/** null = the full retention window. */
export const RANGE_SECONDS: Record<RangeKey, number | null> = { '5m': 300, '15m': 900, '1h': 3600, all: null };
export const RANGE_LABEL: Record<RangeKey, string> = {
  '5m': 'metrics.range5m',
  '15m': 'metrics.range15m',
  '1h': 'metrics.range1h',
  all: 'metrics.rangeAll',
};

export const REFRESH_KEY = 'lite-ui-metrics-refresh-ms';

export function loadRefresh(): number {
  const v = Number(localStorage.getItem(REFRESH_KEY));
  return [2000, 5000, 10000, 30000].includes(v) ? v : 5000;
}

/** Sum a (possibly missing) field across latest entries; null when no
 * version reports it. */
export function sumField(
  latest: { entry: TimelineEntry }[],
  pick: (e: TimelineEntry) => number | null | undefined,
): number | null {
  let total = 0;
  let seen = false;
  for (const x of latest) {
    const v = pick(x.entry);
    if (v != null) {
      total += v;
      seen = true;
    }
  }
  return seen ? total : null;
}

export function maxField(
  latest: { entry: TimelineEntry }[],
  pick: (e: TimelineEntry) => number | null | undefined,
): number | null {
  let best: number | null = null;
  for (const x of latest) {
    const v = pick(x.entry);
    if (v != null && (best == null || v > best)) best = v;
  }
  return best;
}
