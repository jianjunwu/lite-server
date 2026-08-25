import { Card, Descriptions, Empty } from 'antd';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useInstanceLink } from '../context/useInstanceLink';
import { useAuth } from '../context/AuthContext';
import { useMergedVersions, useModelHealth, useModelReady, useTimeline } from '../api/hooks';
import { StatusBadge } from '../components/StatusBadge';
import { WorkerMatrix } from '../components/WorkerMatrix';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
import { PageHeader } from '../components/PageHeader';
import { VersionActions } from '../components/VersionActions';
import { buildTimelineOption } from '../components/timelineChart';
import { useChartColors, useNeutrals } from '../context/ThemeModeContext';
import { formatAge } from '../components/format';
import { dataTextStyle, TYPE } from '../theme';

export function VersionDetailPage() {
  const { t } = useTranslation();
  const { name = '', version = '' } = useParams();
  const navigate = useNavigate();
  const { instanceId } = useInstance();
  const ilink = useInstanceLink();
  const { can } = useAuth();

  const chartColors = useChartColors();
  const neutrals = useNeutrals();
  const merged = useMergedVersions(instanceId, name);
  const info = merged.versions.find((v) => v.version === version);
  const isLoaded = info?.loaded !== false;
  const readyQuery = useModelReady(instanceId, name, version, isLoaded);
  const healthQuery = useModelHealth(instanceId, name, version, isLoaded);
  const timelineQuery = useTimeline(instanceId, name, version, 5_000, isLoaded);

  const snapshots = timelineQuery.data ? [timelineQuery.data] : [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <PageHeader
        breadcrumb={[
          { title: t('models.title'), href: ilink('/models') },
          { title: name, href: ilink(`/models/${encodeURIComponent(name)}`) },
          { title: version },
        ]}
        onBack={() => navigate(ilink(`/models/${encodeURIComponent(name)}`))}
        title={
          <span>
            <span style={dataTextStyle}>{version}</span>
            {info?.active && (
              <span style={{ fontSize: TYPE.eyebrow, textTransform: 'uppercase', letterSpacing: '0.06em', color: neutrals.textPrimary, marginLeft: 10 }}>
                ● {t('common.active')}
              </span>
            )}
          </span>
        }
        subtitle={`${name} · ${instanceId}`}
      />

      <Card size="small">
        <Descriptions size="small" column={{ xs: 1, md: 4 }}>
          <Descriptions.Item label={t('common.status')}>
            <StatusBadge
              status={info?.status}
              text={info?.status === 'unloaded' ? t('models.unloaded') : undefined}
            />
          </Descriptions.Item>
          {isLoaded && (
            <>
              <Descriptions.Item label={t('models.ready')}>
                {readyQuery.data ? (readyQuery.data.ready ? t('models.ready') : t('models.notReady')) : '-'}
              </Descriptions.Item>
              <Descriptions.Item label={t('common.weight')}>{info?.weight ?? '-'}</Descriptions.Item>
              <Descriptions.Item label={t('common.loadedAt')}>
                {info?.loaded_at ? formatAge(info.loaded_at) : '-'}
              </Descriptions.Item>
            </>
          )}
          <Descriptions.Item label="">
            {isLoaded ? (
              <Link to={ilink(`/playground?model=${encodeURIComponent(name)}&version=${encodeURIComponent(version)}`)}>
                {t('nav.playground')} →
              </Link>
            ) : can('operator') && info ? (
              <VersionActions model={name} version={info} />
            ) : null}
          </Descriptions.Item>
        </Descriptions>
      </Card>

      {isLoaded ? (
        <>
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
            <EChart option={buildTimelineOption(snapshots, 'qps', { palette: chartColors })} group={`ver-${name}-${version}`} />
          </ChartCard>
          <ChartCard
            title={t('metrics.p99')}
            loading={timelineQuery.isLoading}
            error={timelineQuery.error}
            isEmpty={snapshots.every((s) => s.entries.length === 0)}
          >
            <EChart option={buildTimelineOption(snapshots, 'p99_ms', { yAxisName: 'ms', palette: chartColors })} group={`ver-${name}-${version}`} />
          </ChartCard>
        </>
      ) : (
        <Card size="small">
          <Empty description={t('models.loadToView')} />
        </Card>
      )}
    </div>
  );
}
