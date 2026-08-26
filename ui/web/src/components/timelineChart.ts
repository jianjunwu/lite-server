import type { EChartsOption, SeriesOption } from 'echarts';
import { SERIES_COLORS, STATUS_COLORS } from '../theme';
import type { TimelineEntry, TimelineSnapshot } from '../api/types';

export type MetricKey =
  | 'qps'
  | 'p99_ms'
  | 'queue_depth'
  | 'active_workers'
  | 'active_streams'
  | 'in_flight'
  | 'worker_saturation'
  | 'ttft_p99_ms'
  | 'tbt_p99_ms'
  | 'stream_bytes_per_s'
  | 'tokens_per_s'
  | 'rss_mb'
  | 'cpu_percent'
  | 'retries_per_s'
  | 'ejections_per_s';

/** Points per chart the time-range control aims for via ?step=. */
export const TIMELINE_TARGET_POINTS = 300;

/** Convert a time range (seconds) into the server-side ?step= downsample
 * factor so the returned series stays near TIMELINE_TARGET_POINTS. */
export function stepForRange(
  rangeSeconds: number,
  intervalSeconds: number,
  targetPoints: number = TIMELINE_TARGET_POINTS,
): number {
  if (rangeSeconds <= 0 || intervalSeconds <= 0 || targetPoints <= 0) return 1;
  return Math.max(1, Math.ceil(rangeSeconds / (intervalSeconds * targetPoints)));
}

/** Trim every snapshot to the last `rangeSeconds`, anchored at the newest
 * SERVER timestamp (not wall clock — browser/instance clocks can skew).
 * `null` keeps everything. */
export function trimToRange(snapshots: TimelineSnapshot[], rangeSeconds: number | null): TimelineSnapshot[] {
  if (rangeSeconds === null) return snapshots;
  const newest = Math.max(0, ...snapshots.flatMap((s) => s.entries.map((e) => e.timestamp)));
  if (newest <= 0) return snapshots;
  const cutoff = newest - rangeSeconds;
  return snapshots.map((s) => ({ ...s, entries: s.entries.filter((e) => e.timestamp >= cutoff) }));
}

export type MetricFieldState = 'ok' | 'unsupported' | 'not-reported';

/** Empty-state grading for a metric across the visible snapshots:
 * - unsupported: no entry carries the field (instance older than the schema)
 * - not-reported: the field is present but always null (e.g. tokens_per_s
 *   before the model reports token metrics)
 * - ok: at least one entry has a real value (0 counts) */
export function fieldState(snapshots: TimelineSnapshot[], key: MetricKey): MetricFieldState {
  let present = false;
  let valued = false;
  for (const s of snapshots) {
    for (const e of s.entries) {
      const v = e[key];
      if (v !== undefined) present = true;
      if (v !== undefined && v !== null) valued = true;
    }
  }
  if (!present) return 'unsupported';
  return valued ? 'ok' : 'not-reported';
}

export interface ThresholdLine {
  value: number;
  label: string;
  severity: 'warning' | 'critical';
}

export interface ChartPalette {
  axis: string;
  split: string;
  text: string;
}

const DEFAULT_PALETTE: ChartPalette = { axis: '#D1D5DB', split: '#E5E7EB', text: '#6B7280' };

/** Snapshots -> multi-series line chart option (one series per model/version). */
export function buildTimelineOption(
  snapshots: TimelineSnapshot[],
  key: MetricKey,
  opts: {
    title?: string;
    yAxisName?: string;
    thresholds?: ThresholdLine[];
    palette?: ChartPalette;
  } = {},
): EChartsOption {
  const palette = opts.palette ?? DEFAULT_PALETTE;
  const series: SeriesOption[] = snapshots.map((snap, idx) => {
    const color = SERIES_COLORS[idx % SERIES_COLORS.length];
    const markLine =
      opts.thresholds && opts.thresholds.length > 0 && idx === 0
        ? {
            silent: true,
            symbol: 'none',
            data: opts.thresholds.map((t) => ({
              yAxis: t.value,
              label: { formatter: t.label, position: 'insideEndTop' as const, fontSize: 10 },
              lineStyle: {
                color: t.severity === 'critical' ? STATUS_COLORS.error : STATUS_COLORS.warning,
                type: 'dashed' as const,
              },
            })),
          }
        : undefined;
    return {
      name: `${snap.model}/${snap.version}`,
      type: 'line',
      showSymbol: false,
      symbolSize: 5,
      lineStyle: { width: 1.5, color },
      itemStyle: { color },
      areaStyle: { color, opacity: 0.08 },
      emphasis: { focus: 'series' },
      markLine,
      data: snap.entries.map((e: TimelineEntry) => [e.timestamp * 1000, e[key] ?? null]),
    };
  });

  return {
    title: opts.title ? { text: opts.title, textStyle: { fontSize: 13, color: palette.text } } : undefined,
    grid: { left: 48, right: 16, top: 32, bottom: 28 },
    tooltip: { trigger: 'axis', order: 'valueDesc' },
    legend: { type: 'scroll', bottom: 0, textStyle: { fontSize: 11, color: palette.text } },
    xAxis: { type: 'time', axisLine: { lineStyle: { color: palette.axis } } },
    yAxis: {
      type: 'value',
      name: opts.yAxisName,
      splitLine: { lineStyle: { color: palette.split, type: 'dashed' } },
      axisLabel: {
        color: palette.text,
        formatter: (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(v)),
      },
    },
    series,
  };
}
