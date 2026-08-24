import { useMemo, useState } from 'react';
import { Card, Col, Row, Select, Space } from 'antd';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useAlerts, useModels, useTimelineAll, useVersions } from '../api/hooks';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
import { buildTimelineOption, type MetricKey, type ThresholdLine } from '../components/timelineChart';

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

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Card size="small">
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

      <Row gutter={[16, 16]}>
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
                })}
                group={CHART_GROUP}
              />
            </ChartCard>
          </Col>
        ))}
      </Row>
    </div>
  );
}
