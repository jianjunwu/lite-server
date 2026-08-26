import { useMemo, useState } from 'react';
import { Card, Col, Row, Select, Space } from 'antd';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useAlerts, useModels, useTimelineAll, useVersions } from '../api/hooks';
import { ChartCard } from '../components/ChartCard';
import { PageHero, Reveal } from '../components/PageHero';
import { StatNum } from '../components/StatNum';
import { useChartColors } from '../context/ThemeModeContext';
import { EChart } from '../components/EChart';
import { buildTimelineOption, type MetricKey, type ThresholdLine } from '../components/timelineChart';
import { formatMs, formatNumber } from '../components/format';
import { SPACE } from '../tokens';

const REFRESH_KEY = 'lite-ui-metrics-refresh-ms';
const CHART_GROUP = 'metrics-page';

function loadRefresh(): number {
  const v = Number(localStorage.getItem(REFRESH_KEY));
  return [2000, 5000, 10000, 30000].includes(v) ? v : 5000;
}

export function MetricsPage() {
  const { t } = useTranslation();
  const { instanceId } = useInstance();

  const [model, setModel] = useState<string | null>(null);
  const [selectedVersions, setSelectedVersions] = useState<string[]>([]);
  const [refreshMs, setRefreshMs] = useState<number>(loadRefresh());
  const [paused, setPaused] = useState(false);

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

  const timelineQuery = useTimelineAll(instanceId, paused ? false : refreshMs);
  const alertsQuery = useAlerts(instanceId, paused ? false : 10_000);

  const snapshots = useMemo(() => {
    const all = timelineQuery.data?.snapshots ?? [];
    if (!effectiveModel) return [];
    return all.filter(
      (s) => s.model === effectiveModel && (effectiveVersions.length === 0 || effectiveVersions.includes(s.version)),
    );
  }, [timelineQuery.data, effectiveModel, effectiveVersions]);

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

  const charts: { key: MetricKey; title: string; rule?: 'p99_ms' | 'queue_depth'; yAxisName?: string }[] = [
    { key: 'qps', title: t('metrics.qps') },
    { key: 'p99_ms', title: t('metrics.p99'), rule: 'p99_ms', yAxisName: 'ms' },
    { key: 'queue_depth', title: t('metrics.queueDepth'), rule: 'queue_depth' },
    { key: 'active_workers', title: t('metrics.workers') },
  ];

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

  const currentQps = latest.reduce((sum, x) => sum + x.entry.qps, 0);
  const currentP99 = Math.max(0, ...latest.map((x) => x.entry.p99_ms));
  const currentQueue = latest.reduce((sum, x) => sum + x.entry.queue_depth, 0);

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
          <StatNum label={t('metrics.currentQps')} value={formatNumber(currentQps)} />
          <StatNum label={t('metrics.currentP99')} value={currentP99 > 0 ? formatMs(currentP99) : '-'} />
          <StatNum label={t('metrics.currentQueue')} value={formatNumber(currentQueue)} />
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
        <Row gutter={[SPACE[5], SPACE[5]]}>
          {charts.map((c) => (
            <Col xs={24} xl={12} key={c.key}>
              <ChartCard
                title={c.title}
                loading={timelineQuery.isLoading}
                error={timelineQuery.error}
                isEmpty={isEmpty}
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
          ))}
        </Row>
      </Reveal>
    </>
  );
}
