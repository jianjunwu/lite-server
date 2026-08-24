import { Card, Descriptions, Tabs, Tag, Typography } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { apiFetch } from '../api/client';
import { useModelHealth, useTimeline, useVersions } from '../api/hooks';
import { StatusBadge } from '../components/StatusBadge';
import { WorkerMatrix } from '../components/WorkerMatrix';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
import { buildTimelineOption } from '../components/timelineChart';
import { formatAge, formatMs } from '../components/format';
import { MONO_FONT } from '../theme';

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
      <Card size="small">
        <Descriptions
          title={
            <span>
              {name}{' '}
              {versionsQuery.data?.active_version && (
                <Tag color="#4F46E5">{t('models.activeVersion')}: {versionsQuery.data.active_version}</Tag>
              )}
            </span>
          }
          size="small"
          column={{ xs: 1, md: 3 }}
        >
          <Descriptions.Item label={t('models.versions')}>{versions.length}</Descriptions.Item>
          <Descriptions.Item label={t('models.healthyWorkers')}>
            {healthQuery.data ? `${healthQuery.data.healthy_workers}/${healthQuery.data.total_workers}` : '-'}
          </Descriptions.Item>
          <Descriptions.Item label={t('common.instance')}>{instanceId}</Descriptions.Item>
        </Descriptions>
      </Card>

      <Tabs
        items={[
          {
            key: 'versions',
            label: t('models.tabs.versions'),
            children: (
              <Card size="small">
                <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                  <tbody>
                    {versions.map((v) => (
                      <tr key={v.version} style={{ borderBottom: '1px solid #E5E7EB' }}>
                        <td style={{ padding: '8px 12px' }}>
                          <a href={`/models/${encodeURIComponent(name)}/versions/${encodeURIComponent(v.version)}`}>
                            {v.version}
                          </a>
                        </td>
                        <td><StatusBadge status={v.status} /></td>
                        <td>{v.active ? <Tag color="#4F46E5">{t('common.active')}</Tag> : null}</td>
                        <td>{t('common.weight')}: {v.weight}</td>
                        <td>{t('common.workers')}: {v.workers.ready}/{v.workers.total}</td>
                        <td>{v.loaded_at ? formatAge(v.loaded_at) : '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
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
