import { useMemo } from 'react';
import { Card, Col, Empty, Row, Skeleton, Typography } from 'antd';
import { useQueries } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { apiFetch } from '../api/client';
import { useInstances } from '../api/hooks';
import type { AlertsResponse, HealthSummary, ServerInfo, TimelineAllResponse } from '../api/types';
import { StatusBadge, statusKind } from '../components/StatusBadge';
import { StatCard } from '../components/StatCard';
import { EChart } from '../components/EChart';
import { STATUS_COLORS } from '../theme';
import { formatNumber } from '../components/format';

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

  // Fleet-wide aggregates.
  const { statusCounts, fleetQps, sparkline, activeAlerts } = useMemo(() => {
    const counts: Record<string, number> = { ready: 0, loading: 0, warning: 0, error: 0, offline: 0 };
    let qps = 0;
    let alerts = 0;
    const buckets = new Map<number, number>();

    healthQueries.forEach((q) => {
      q.data?.models.forEach((m) =>
        m.versions.forEach((v) => {
          counts[statusKind(v.status)] += 1;
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
    return { statusCounts: counts, fleetQps: qps, sparkline: line, activeAlerts: alerts };
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
          .map((k) => ({ name: k, value: statusCounts[k], itemStyle: { color: STATUS_COLORS[k] } })),
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
      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card title={t('overview.fleetStatus')} size="small" loading={loading}>
            <EChart option={donutOption} height={180} />
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
          const unreachable = health.isError;
          return (
            <Col xs={24} md={12} xl={8} key={inst.id}>
              <Card
                size="small"
                title={inst.name}
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
                  <Typography.Text type="secondary">{inst.base_url}</Typography.Text>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                    <Typography.Text type="secondary" style={{ fontSize: 12 }}>{inst.base_url}</Typography.Text>
                    <span>
                      {t('overview.serverVersion')}: <b>{info.data?.version ?? '-'}</b>
                    </span>
                    <span>
                      {t('overview.loadedModels')}: <b>{health.data?.models.length ?? 0}</b>
                    </span>
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
