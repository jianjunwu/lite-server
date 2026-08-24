import { Card, Tabs, Typography } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { apiFetch } from '../api/client';
import { useModelHealth, useTimeline, useVersions } from '../api/hooks';
import { WorkerMatrix } from '../components/WorkerMatrix';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
import { VersionsTable } from '../components/VersionsTable';
import { PageHeader } from '../components/PageHeader';
import { buildTimelineOption } from '../components/timelineChart';
import { dataTextStyle, MONO_FONT, TYPE } from '../theme';

export function ModelDetailPage() {
  const { t } = useTranslation();
  const { name = '' } = useParams();
  const { instanceId } = useInstance();

  const versionsQuery = useVersions(instanceId, name);
  const healthQuery = useModelHealth(instanceId, name);
  const timelineQuery = useTimeline(instanceId, name);
  const compareQuery = useQuery({
    queryKey: [instanceId, 'compare', name],
    queryFn: () => apiFetch<unknown>(instanceId!, `/v2/models/${encodeURIComponent(name)}/compare`),
    enabled: instanceId !== null,
    retry: 0,
  });

  const versions = versionsQuery.data?.versions ?? [];
  const snapshots = timelineQuery.data ? [timelineQuery.data] : [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <PageHeader
        title={
          <span>
            {name}
            {versionsQuery.data?.active_version && (
              <span style={{ ...dataTextStyle, fontSize: TYPE.secondary, color: '#4B5563', marginLeft: 12 }}>
                ● {versionsQuery.data.active_version}
              </span>
            )}
          </span>
        }
        subtitle={
          healthQuery.data
            ? `${t('models.healthyWorkers')}: ${healthQuery.data.healthy_workers}/${healthQuery.data.total_workers} · ${instanceId}`
            : instanceId
        }
      />

      <Tabs
        items={[
          {
            key: 'versions',
            label: t('models.tabs.versions'),
            children: (
              <Card size="small">
                <VersionsTable model={name} versions={versions} loading={versionsQuery.isLoading} />
              </Card>
            ),
          },
          {
            key: 'workers',
            label: t('models.tabs.workers'),
            children: (
              <Card size="small" loading={healthQuery.isLoading}>
                {healthQuery.data && <WorkerMatrix workers={healthQuery.data.workers} />}
              </Card>
            ),
          },
          {
            key: 'metrics',
            label: t('models.tabs.metrics'),
            children: (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                <ChartCard
                  title={t('metrics.qps')}
                  loading={timelineQuery.isLoading}
                  error={timelineQuery.error}
                  isEmpty={snapshots.every((s) => s.entries.length === 0)}
                  onRetry={() => timelineQuery.refetch()}
                >
                  <EChart option={buildTimelineOption(snapshots, 'qps')} group={`model-${name}`} />
                </ChartCard>
                <ChartCard
                  title={t('metrics.p99')}
                  loading={timelineQuery.isLoading}
                  error={timelineQuery.error}
                  isEmpty={snapshots.every((s) => s.entries.length === 0)}
                >
                  <EChart option={buildTimelineOption(snapshots, 'p99_ms', { yAxisName: 'ms' })} group={`model-${name}`} />
                </ChartCard>
              </div>
            ),
          },
          {
            key: 'compare',
            label: t('models.tabs.compare'),
            children: (
              <Card size="small" loading={compareQuery.isLoading}>
                {compareQuery.isError ? (
                  <Typography.Text type="secondary">{compareQuery.error.message}</Typography.Text>
                ) : (
                  <pre style={{ fontFamily: MONO_FONT, fontSize: 12, margin: 0 }}>
                    {JSON.stringify(compareQuery.data ?? {}, null, 2)}
                  </pre>
                )}
              </Card>
            ),
          },
        ]}
      />
    </div>
  );
}
