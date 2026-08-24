import { useMemo } from 'react';
import { Card, Col, Empty, Row, Skeleton, Typography } from 'antd';
import { useQueries } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { apiFetch } from '../api/client';
import { useInstances } from '../api/hooks';
import type { AlertsResponse, HealthSummary, ServerInfo, TimelineAllResponse } from '../api/types';
import { StatusBadge, StatusDot, statusKind } from '../components/StatusBadge';
import { StatCard } from '../components/StatCard';
import { EChart } from '../components/EChart';
import { PageHeader } from '../components/PageHeader';
import { STATUS_COLORS, TYPE, dataTextStyle } from '../theme';
import { formatMs, formatNumber } from '../components/format';

const MAX_MODEL_ROWS = 6;

export function OverviewPage() {
  const { t } = useTranslation();
  const instancesQuery = useInstances();
  const instances = useMemo(() => instancesQuery.data?.instances ?? [], [instancesQuery.data]);

  const healthQueries = useQueries({
    queries: instances.map((i) => ({
      queryKey: [i.id, 'health'],
      queryFn: () => apiFetch<HealthSummary>(i.id, '/health'),
      retry: 0,
      refetchInterval: 10_000,
    })),
  });
  const infoQueries = useQueries({
    queries: instances.map((i) => ({
      queryKey: [i.id, 'info'],
      queryFn: () => apiFetch<ServerInfo>(i.id, '/info'),
      retry: 0,
    })),
  });
  const timelineQueries = useQueries({
    queries: instances.map((i) => ({
      queryKey: [i.id, 'timeline'],
      queryFn: () => apiFetch<TimelineAllResponse>(i.id, '/metrics/timeline'),
      retry: 0,
      refetchInterval: 10_000,
    })),
  });
  const alertsQueries = useQueries({
    queries: instances.map((i) => ({
      queryKey: [i.id, 'alerts'],
      queryFn: () => apiFetch<AlertsResponse>(i.id, '/metrics/alerts'),
      retry: 0,
      refetchInterval: 10_000,
    })),
  });

  const loading = instancesQuery.isLoading;

  const { statusCounts, totalVersions, fleetQps, sparkline, activeAlerts } = useMemo(() => {
    const counts: Record<string, number> = { ready: 0, loading: 0, warning: 0, error: 0, offline: 0 };
    let versions = 0;
    let qps = 0;
    let alerts = 0;
    const buckets = new Map<number, number>();

    healthQueries.forEach((q) => {
      q.data?.models.forEach((m) =>
        m.versions.forEach((v) => {
          counts[statusKind(v.status)] += 1;
          versions += 1;
        }),
      );
    });
    timelineQueries.forEach((q) => {
      q.data?.snapshots.forEach((s) => {
        const latest = s.entries[s.entries.length - 1];
        if (latest) qps += latest.qps;
        s.entries.forEach((e) => {
          const bucket = Math.round(e.timestamp / 10) * 10;
          buckets.set(bucket, (buckets.get(bucket) ?? 0) + e.qps);
        });
      });
    });
    alertsQueries.forEach((q) => {
      alerts += q.data?.alerts.length ?? 0;
    });
    const line = [...buckets.entries()].sort((a, b) => a[0] - b[0]).map(([ts, v]) => [ts * 1000, v]);
    return { statusCounts: counts, totalVersions: versions, fleetQps: qps, sparkline: line, activeAlerts: alerts };
  }, [healthQueries, timelineQueries, alertsQueries]);

  if (!loading && instances.length === 0) {
    return (
      <Empty
        description={
          <>
            <Typography.Title level={4}>{t('overview.noInstances')}</Typography.Title>
            <Typography.Text type="secondary">{t('overview.noInstancesHint')}</Typography.Text>
          </>
        }
      />
    );
  }

  const degraded = totalVersions - statusCounts.ready;

  const donutOption = {
    tooltip: { trigger: 'item' as const },
    legend: { bottom: 0, textStyle: { fontSize: 11 } },
    series: [
      {
        type: 'pie' as const,
        radius: ['55%', '80%'],
        label: { show: false },
        data: (['ready', 'loading', 'warning', 'error', 'offline'] as const)
          .filter((k) => statusCounts[k] > 0)
          .map((k) => ({
            name: k,
            value: statusCounts[k],
            itemStyle: {
              color: STATUS_COLORS[k],
              // Quiet rule: healthy slices recede, anomalies pop.
              opacity: k === 'ready' ? 0.35 : 1,
            },
          })),
      },
    ],
  };

  const sparkOption = {
    grid: { left: 0, right: 0, top: 4, bottom: 0 },
    xAxis: { type: 'time' as const, show: false },
    yAxis: { type: 'value' as const, show: false },
    series: [
      {
        type: 'line' as const,
        showSymbol: false,
        lineStyle: { width: 1.5, color: '#4F46E5' },
        areaStyle: { color: '#4F46E5', opacity: 0.08 },
        data: sparkline,
      },
    ],
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <PageHeader title={t('overview.title')} />
      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card title={t('overview.fleetStatus')} size="small" loading={loading}>
            {degraded === 0 && totalVersions > 0 ? (
              // All quiet: no chart, just a calm statement.
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, minHeight: 120 }}>
                <StatusDot kind="ready" size={10} />
                <span style={{ fontSize: TYPE.body, color: '#374151' }}>
                  {t('overview.allReady', { versions: totalVersions, instances: instances.length })}
                </span>
              </div>
            ) : (
              <EChart option={donutOption} height={180} />
            )}
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <StatCard title={t('overview.fleetQps')} value={formatNumber(fleetQps)}>
            <EChart option={sparkOption} height={60} />
          </StatCard>
        </Col>
        <Col xs={24} md={8}>
          <StatCard title={t('overview.fleetErrors')} value={activeAlerts} />
        </Col>
      </Row>

      <Typography.Title level={5} style={{ margin: 0 }}>{t('overview.instances')}</Typography.Title>
      <Row gutter={[16, 16]}>
        {instances.map((inst, idx) => {
          const health = healthQueries[idx];
          const info = infoQueries[idx];
          const timeline = timelineQueries[idx];
          const unreachable = health.isError;
          const models = health.data?.models ?? [];
          const snapshots = timeline.data?.snapshots ?? [];

          return (
            <Col xs={24} md={12} xl={8} key={inst.id}>
              <Card
                size="small"
                title={
                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                    {inst.name}
                    <Typography.Text type="secondary" style={{ fontSize: TYPE.eyebrow, fontWeight: 400 }}>
                      {info.data?.version ?? ''}
                    </Typography.Text>
                  </span>
                }
                extra={
                  unreachable ? (
                    <StatusBadge status="offline" text={t('common.unreachable')} />
                  ) : (
                    <StatusBadge status={health.data?.status} />
                  )
                }
              >
                {health.isLoading ? (
                  <Skeleton active paragraph={{ rows: 2 }} />
                ) : unreachable ? (
                  <Typography.Text type="secondary" style={dataTextStyle}>{inst.base_url}</Typography.Text>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                    {models.slice(0, MAX_MODEL_ROWS).map((m) => {
                      const modelSnaps = snapshots.filter((s) => s.model === m.name);
                      const latestP99 = Math.max(
                        0,
                        ...modelSnaps.map((s) => s.entries[s.entries.length - 1]?.p99_ms ?? 0),
                      );
                      return (
                        <div key={m.name} style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                          <Link
                            to={`/models/${encodeURIComponent(m.name)}?i=${encodeURIComponent(inst.id)}`}
                            style={{ flex: '0 0 40%', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                          >
                            {m.name}
                          </Link>
                          <span style={{ display: 'inline-flex', gap: 4 }}>
                            {m.versions.map((v) => (
                              <StatusDot key={v.version} kind={statusKind(v.status)} size={7} />
                            ))}
                          </span>
                          <span style={{ ...dataTextStyle, marginLeft: 'auto', fontSize: TYPE.secondary, color: '#6B7280' }}>
                            {latestP99 > 0 ? formatMs(latestP99) : '-'}
                          </span>
                        </div>
                      );
                    })}
                    {models.length > MAX_MODEL_ROWS && (
                      <Link to={`/models?i=${encodeURIComponent(inst.id)}`} style={{ fontSize: TYPE.secondary }}>
                        +{models.length - MAX_MODEL_ROWS} more
                      </Link>
                    )}
                    {models.length === 0 && (
                      <Typography.Text type="secondary" style={{ fontSize: TYPE.secondary }}>
                        {t('models.noModels')}
                      </Typography.Text>
                    )}
                  </div>
                )}
              </Card>
            </Col>
          );
        })}
      </Row>
    </div>
  );
}
