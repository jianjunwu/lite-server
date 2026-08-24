import { Card, Descriptions } from 'antd';
import { Link, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useModelHealth, useModelReady, useTimeline, useVersions } from '../api/hooks';
import { StatusBadge } from '../components/StatusBadge';
import { WorkerMatrix } from '../components/WorkerMatrix';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
import { buildTimelineOption } from '../components/timelineChart';
import { formatAge } from '../components/format';
import { dataTextStyle, TYPE } from '../theme';

export function VersionDetailPage() {
  const { t } = useTranslation();
  const { name = '', version = '' } = useParams();
  const { instanceId } = useInstance();

  const versionsQuery = useVersions(instanceId, name);
  const readyQuery = useModelReady(instanceId, name, version);
  const healthQuery = useModelHealth(instanceId, name, version);
  const timelineQuery = useTimeline(instanceId, name, version);

  const info = versionsQuery.data?.versions.find((v) => v.version === version);
  const snapshots = timelineQuery.data ? [timelineQuery.data] : [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Card size="small">
        <Descriptions
          title={
            <span>
              <Link to={`/models/${encodeURIComponent(name)}`}>{name}</Link>
              <span style={{ color: '#9CA3AF' }}> / </span>
              <span style={dataTextStyle}>{version}</span>
              {info?.active && (
                <span style={{ fontSize: TYPE.eyebrow, textTransform: 'uppercase', letterSpacing: '0.06em', color: '#111827', marginLeft: 10 }}>
                  ● {t('common.active')}
                </span>
              )}
            </span>
          }
          size="small"
          column={{ xs: 1, md: 4 }}
        >
          <Descriptions.Item label={t('common.status')}>
            <StatusBadge status={info?.status} />
          </Descriptions.Item>
          <Descriptions.Item label={t('models.ready')}>
            {readyQuery.data ? (readyQuery.data.ready ? t('models.ready') : t('models.notReady')) : '-'}
          </Descriptions.Item>
          <Descriptions.Item label={t('common.weight')}>{info?.weight ?? '-'}</Descriptions.Item>
          <Descriptions.Item label={t('common.loadedAt')}>
            {info?.loaded_at ? formatAge(info.loaded_at) : '-'}
          </Descriptions.Item>
        </Descriptions>
      </Card>

      <Card
        size="small"
        title={`${t('models.healthyWorkers')}: ${healthQuery.data ? `${healthQuery.data.healthy_workers}/${healthQuery.data.total_workers}` : '-'}`}
        loading={healthQuery.isLoading}
      >
        {healthQuery.data && <WorkerMatrix workers={healthQuery.data.workers} />}
      </Card>

      <ChartCard
        title={t('metrics.qps')}
        loading={timelineQuery.isLoading}
        error={timelineQuery.error}
        isEmpty={snapshots.every((s) => s.entries.length === 0)}
        onRetry={() => timelineQuery.refetch()}
      >
        <EChart option={buildTimelineOption(snapshots, 'qps')} group={`ver-${name}-${version}`} />
      </ChartCard>
      <ChartCard
        title={t('metrics.p99')}
        loading={timelineQuery.isLoading}
        error={timelineQuery.error}
        isEmpty={snapshots.every((s) => s.entries.length === 0)}
      >
        <EChart option={buildTimelineOption(snapshots, 'p99_ms', { yAxisName: 'ms' })} group={`ver-${name}-${version}`} />
      </ChartCard>
    </div>
  );
}
