import { useEffect, useMemo, useState } from 'react';
import { Card, Col, Row, Segmented, Select, Space, Tabs, Tooltip } from 'antd';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useAlerts, useModels, useTimelineAll, useVersions } from '../api/hooks';
import type { TimelineEntry } from '../api/types';
import { ChartCard } from '../components/ChartCard';
import { PageHero, Reveal } from '../components/PageHero';
import { StatNum } from '../components/StatNum';
import { useChartColors } from '../context/ThemeModeContext';
import { EChart } from '../components/EChart';
import {
  buildTimelineOption,
  fieldState,
  stepForRange,
  trimToRange,
  type MetricKey,
  type ThresholdLine,
} from '../components/timelineChart';
import { formatBytes, formatMs, formatNumber } from '../components/format';
import { SPACE } from '../tokens';

const REFRESH_KEY = 'lite-ui-metrics-refresh-ms';
const CHART_GROUP = 'metrics-page';

type GroupKey = 'throughput' | 'latency' | 'queue' | 'resources' | 'health';
type RangeKey = '5m' | '15m' | '1h' | 'all';

/** null = the full retention window. */
const RANGE_SECONDS: Record<RangeKey, number | null> = { '5m': 300, '15m': 900, '1h': 3600, all: null };
const RANGE_LABEL: Record<RangeKey, string> = {
  '5m': 'metrics.range5m',
  '15m': 'metrics.range15m',
  '1h': 'metrics.range1h',
  all: 'metrics.rangeAll',
};

interface ChartSpec {
  key: MetricKey;
  titleKey: string;
  rule?: 'p99_ms' | 'queue_depth';
  yAxisName?: string;
}

/** Chart groups (plan §4.4): Tabs replace the flat 4-chart grid. */
const GROUP_CHARTS: Record<GroupKey, ChartSpec[]> = {
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
    { key: 'cpu_percent', titleKey: 'metrics.cpu', yAxisName: '%' },
  ],
  health: [
    { key: 'retries_per_s', titleKey: 'metrics.retriesPerS' },
    { key: 'ejections_per_s', titleKey: 'metrics.ejectionsPerS' },
  ],
};

function loadRefresh(): number {
  const v = Number(localStorage.getItem(REFRESH_KEY));
  return [2000, 5000, 10000, 30000].includes(v) ? v : 5000;
}

/** Sum a (possibly missing) field across the latest entries of the selected
 * versions; null when no version reports it. */
function sumField(latest: { entry: TimelineEntry }[], pick: (e: TimelineEntry) => number | null | undefined): number | null {
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

function maxField(latest: { entry: TimelineEntry }[], pick: (e: TimelineEntry) => number | null | undefined): number | null {
  let best: number | null = null;
  for (const x of latest) {
    const v = pick(x.entry);
    if (v != null && (best == null || v > best)) best = v;
  }
  return best;
}

export function MetricsPage() {
  const { t } = useTranslation();
  const { instanceId } = useInstance();

  const [model, setModel] = useState<string | null>(null);
  const [selectedVersions, setSelectedVersions] = useState<string[]>([]);
  const [refreshMs, setRefreshMs] = useState<number>(loadRefresh());
  const [paused, setPaused] = useState(false);
  const [group, setGroup] = useState<GroupKey>('throughput');
  const [range, setRange] = useState<RangeKey>('all');

  const chartColors = useChartColors();
  const modelsQuery = useModels(instanceId);
  const modelNames = useMemo(
    () => [...new Set((modelsQuery.data?.models ?? []).map((m) => m.name))],
    [modelsQuery.data],
  );
  const effectiveModel = model ?? modelNames[0] ?? null;

  const versionsQuery = useVersions(instanceId, effectiveModel ?? '');
  const versionNames = useMemo(
    () => (versionsQuery.data?.versions ?? []).map((v) => v.version),
    [versionsQuery.data],
  );
  const effectiveVersions = selectedVersions.length > 0 ? selectedVersions : versionNames;

  // Coverage arrives with the first response; until then (or on pre-M3
  // instances without the header) every range stays enabled and no step is
  // sent. A previously picked range that exceeds the coverage falls back to
  // 'all' (never disabled).
  const [coverageSeconds, setCoverageSeconds] = useState<number | undefined>(undefined);
  const [intervalSeconds, setIntervalSeconds] = useState<number | undefined>(undefined);
  const effectiveRange: RangeKey =
    RANGE_SECONDS[range] != null && coverageSeconds != null && RANGE_SECONDS[range]! > coverageSeconds
      ? 'all'
      : range;
  const step = useMemo(() => {
    if (intervalSeconds == null) return undefined;
    const seconds = RANGE_SECONDS[effectiveRange] ?? coverageSeconds;
    if (seconds == null) return undefined;
    return stepForRange(seconds, intervalSeconds);
  }, [effectiveRange, coverageSeconds, intervalSeconds]);

  const timelineQuery = useTimelineAll(instanceId, paused ? false : refreshMs, step);
  const alertsQuery = useAlerts(instanceId, paused ? false : 10_000);

  // Track the freshest coverage/interval the instance reported.
  const dataCoverage = timelineQuery.data?.coverageSeconds;
  const dataInterval = timelineQuery.data?.intervalSeconds;
  useEffect(() => {
    if (dataCoverage != null) setCoverageSeconds(dataCoverage);
    if (dataInterval != null) setIntervalSeconds(dataInterval);
  }, [dataCoverage, dataInterval]);

  const snapshots = useMemo(() => {
    const trimmed = trimToRange(timelineQuery.data?.snapshots ?? [], RANGE_SECONDS[effectiveRange]);
    if (!effectiveModel) return [];
    return trimmed.filter(
      (s) => s.model === effectiveModel && (effectiveVersions.length === 0 || effectiveVersions.includes(s.version)),
    );
  }, [timelineQuery.data, effectiveModel, effectiveVersions, effectiveRange]);

  const thresholdsFor = (rule: 'p99_ms' | 'queue_depth'): ThresholdLine[] => {
    const seen = new Map<string, ThresholdLine>();
    (alertsQuery.data?.alerts ?? [])
      .filter((a) => a.rule === rule && (!effectiveModel || a.model === effectiveModel))
      .forEach((a) => {
        seen.set(`${a.severity}:${a.threshold}`, {
          value: a.threshold,
          label: `${t(a.severity === 'critical' ? 'metrics.criticalThreshold' : 'metrics.warningThreshold')} ${a.threshold}`,
          severity: a.severity,
        });
      });
    return [...seen.values()];
  };

  const isEmpty = snapshots.every((s) => s.entries.length === 0);

  // Latest reading per selected version — drives the hero conclusion and
  // the big-number zone (plan §4.4).
  const latest = useMemo(
    () =>
      snapshots
        .map((s) => ({ version: s.version, entry: s.entries[s.entries.length - 1] }))
        .filter((x): x is { version: string; entry: NonNullable<typeof x.entry> } => Boolean(x.entry)),
    [snapshots],
  );

  const hero = useMemo(() => {
    if (latest.length === 0) {
      return { statement: t('metrics.stmtWaiting'), tone: 'ink' as const };
    }
    if (latest.length >= 2) {
      const sorted = [...latest].sort((a, b) => a.entry.p99_ms - b.entry.p99_ms);
      const fast = sorted[0];
      const slow = sorted[sorted.length - 1];
      const diff = slow.entry.p99_ms - fast.entry.p99_ms;
      if (diff > 0) {
        return {
          statement: t('metrics.stmtFaster', {
            fast: fast.version,
            slow: slow.version,
            ms: formatMs(diff),
          }),
          tone: 'ink' as const,
        };
      }
    }
    const first = latest[0];
    return {
      statement: t('metrics.stmtServing', {
        version: first.version,
        qps: formatNumber(first.entry.qps),
        p99: formatMs(first.entry.p99_ms),
      }),
      tone: 'ink' as const,
    };
  }, [latest, t]);

  // Big-number zone follows the active chart group (plan §4.4); a field no
  // selected version reports renders as '-'.
  const heroStats = useMemo((): { label: string; value: string }[] => {
    const num = (v: number | null) => (v == null ? '-' : formatNumber(v));
    const ms = (v: number | null) => (v == null || v <= 0 ? '-' : formatMs(v));
    const bytes = (v: number | null) => (v == null ? '-' : `${formatBytes(v)}/s`);
    switch (group) {
      case 'throughput':
        return [
          { label: t('metrics.currentQps'), value: num(sumField(latest, (e) => e.qps)) },
          { label: t('metrics.tokensPerS'), value: num(sumField(latest, (e) => e.tokens_per_s)) },
          { label: t('metrics.streamBytesPerS'), value: bytes(sumField(latest, (e) => e.stream_bytes_per_s)) },
        ];
      case 'latency':
        return [
          { label: t('metrics.currentP99'), value: ms(maxField(latest, (e) => e.p99_ms)) },
          { label: t('metrics.ttftP99'), value: ms(maxField(latest, (e) => e.ttft_p99_ms)) },
          { label: t('metrics.tbtP99'), value: ms(maxField(latest, (e) => e.tbt_p99_ms)) },
        ];
      case 'queue':
        return [
          { label: t('metrics.currentQueue'), value: num(sumField(latest, (e) => e.queue_depth)) },
          { label: t('metrics.inFlight'), value: num(sumField(latest, (e) => e.in_flight)) },
          { label: t('metrics.activeStreams'), value: num(sumField(latest, (e) => e.active_streams)) },
        ];
      case 'resources':
        return [
          { label: t('metrics.workers'), value: num(sumField(latest, (e) => e.active_workers)) },
          { label: t('metrics.saturation'), value: num(maxField(latest, (e) => e.worker_saturation)) },
          {
            label: t('metrics.rss'),
            value: (() => {
              const v = sumField(latest, (e) => e.rss_mb);
              return v == null ? '-' : `${formatNumber(Math.round(v))} MB`;
            })(),
          },
          {
            label: t('metrics.cpu'),
            value: (() => {
              const v = maxField(latest, (e) => e.cpu_percent);
              return v == null ? '-' : `${formatNumber(v)}%`;
            })(),
          },
        ];
      case 'health':
        return [
          { label: t('metrics.retriesPerS'), value: num(sumField(latest, (e) => e.retries_per_s)) },
          { label: t('metrics.ejectionsPerS'), value: num(sumField(latest, (e) => e.ejections_per_s)) },
        ];
    }
  }, [group, latest, t]);

  const rangeOptions = (Object.keys(RANGE_SECONDS) as RangeKey[]).map((rk) => {
    const seconds = RANGE_SECONDS[rk];
    const disabled = seconds != null && coverageSeconds != null && seconds > coverageSeconds;
    const text = t(RANGE_LABEL[rk]);
    return {
      value: rk,
      disabled,
      label: disabled ? (
        <Tooltip title={t('metrics.rangeDisabledHint')}>
          <span>{text}</span>
        </Tooltip>
      ) : (
        text
      ),
    };
  });

  return (
    <>
      <PageHero
        eyebrow={t('metrics.title')}
        live={!paused}
        statement={hero.statement}
        tone={hero.tone}
        subline={effectiveModel ? `${effectiveModel} · ${instanceId}` : instanceId}
      />
      <Reveal order={1}>
        <div
          style={{
            display: 'flex',
            gap: SPACE[7],
            rowGap: SPACE[5],
            flexWrap: 'wrap',
            marginBottom: SPACE[8],
          }}
        >
          {heroStats.map((s) => (
            <StatNum key={s.label} label={s.label} value={s.value} />
          ))}
        </div>
      </Reveal>
      <Reveal order={2}>
        {/* fit-content: a full-width card half-filled with controls reads as
            an empty, not-yet-loaded panel. */}
        <Card size="small" style={{ marginBottom: SPACE[5], width: 'fit-content', maxWidth: '100%' }}>
        <Space wrap size="middle">
          <Select
            style={{ minWidth: 200 }}
            placeholder={t('metrics.model')}
            value={effectiveModel ?? undefined}
            onChange={(v) => {
              setModel(v);
              setSelectedVersions([]);
            }}
            options={modelNames.map((m) => ({ value: m, label: m }))}
            loading={modelsQuery.isLoading}
          />
          <Select
            mode="multiple"
            style={{ minWidth: 260 }}
            placeholder={t('metrics.versionsOverlay')}
            value={effectiveVersions}
            onChange={setSelectedVersions}
            options={versionNames.map((v) => ({ value: v, label: v }))}
            disabled={!effectiveModel}
            maxTagCount={4}
          />
          <Segmented value={effectiveRange} onChange={(v) => setRange(v as RangeKey)} options={rangeOptions} />
          <Select
            style={{ width: 130 }}
            value={paused ? 0 : refreshMs}
            onChange={(v) => {
              if (v === 0) {
                setPaused(true);
              } else {
                setPaused(false);
                setRefreshMs(v);
                localStorage.setItem(REFRESH_KEY, String(v));
              }
            }}
            options={[
              { value: 2000, label: t('metrics.every2s') },
              { value: 5000, label: t('metrics.every5s') },
              { value: 10000, label: t('metrics.every10s') },
              { value: 30000, label: t('metrics.every30s') },
              { value: 0, label: t('metrics.pause') },
            ]}
          />
        </Space>
        </Card>
      </Reveal>

      <Reveal order={3}>
        <Tabs
          activeKey={group}
          onChange={(k) => setGroup(k as GroupKey)}
          items={(Object.keys(GROUP_CHARTS) as GroupKey[]).map((g) => ({
            key: g,
            label: t(`metrics.group${g.charAt(0).toUpperCase()}${g.slice(1)}`),
          }))}
        />
        <Row gutter={[SPACE[5], SPACE[5]]}>
          {GROUP_CHARTS[group].map((c) => {
            const state = isEmpty ? ('ok' as const) : fieldState(snapshots, c.key);
            const emptyText =
              state === 'unsupported'
                ? t('metrics.unsupported')
                : state === 'not-reported'
                  ? t('metrics.notReported')
                  : undefined;
            return (
              <Col xs={24} xl={12} key={c.key}>
                <ChartCard
                  title={t(c.titleKey)}
                  loading={timelineQuery.isLoading}
                  error={timelineQuery.error}
                  isEmpty={isEmpty || state !== 'ok'}
                  emptyText={emptyText}
                  onRetry={() => timelineQuery.refetch()}
                >
                  <EChart
                    option={buildTimelineOption(snapshots, c.key, {
                      yAxisName: c.yAxisName,
                      thresholds: c.rule ? thresholdsFor(c.rule) : undefined,
                      palette: chartColors,
                    })}
                    group={CHART_GROUP}
                  />
                </ChartCard>
              </Col>
            );
          })}
        </Row>
      </Reveal>
    </>
  );
}
