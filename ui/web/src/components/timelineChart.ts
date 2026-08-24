import type { EChartsOption, SeriesOption } from 'echarts';
import { SERIES_COLORS, STATUS_COLORS } from '../theme';
import type { TimelineEntry, TimelineSnapshot } from '../api/types';

export type MetricKey = 'qps' | 'p99_ms' | 'queue_depth' | 'active_workers' | 'active_streams';

export interface ThresholdLine {
  value: number;
  label: string;
  severity: 'warning' | 'critical';
}

/** Snapshots -> multi-series line chart option (one series per model/version). */
export function buildTimelineOption(
  snapshots: TimelineSnapshot[],
  key: MetricKey,
  opts: { title?: string; yAxisName?: string; thresholds?: ThresholdLine[]; group?: string } = {},
): EChartsOption {
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
      data: snap.entries.map((e: TimelineEntry) => [e.timestamp * 1000, e[key]]),
    };
  });

  return {
    title: opts.title ? { text: opts.title, textStyle: { fontSize: 13 } } : undefined,
    grid: { left: 48, right: 16, top: 32, bottom: 28 },
    tooltip: { trigger: 'axis', order: 'valueDesc' },
    legend: { type: 'scroll', bottom: 0, textStyle: { fontSize: 11 } },
    xAxis: { type: 'time', axisLine: { lineStyle: { color: '#D1D5DB' } } },
    yAxis: {
      type: 'value',
      name: opts.yAxisName,
      splitLine: { lineStyle: { color: '#E5E7EB', type: 'dashed' } },
      axisLabel: {
        formatter: (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(v)),
      },
    },
    series,
  };
}
