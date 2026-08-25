import { Card, Empty, Tabs, Typography, Button, Input, Modal, App, Select } from 'antd';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useInstanceLink } from '../context/useInstanceLink';
import { useCanInstance } from '../context/useEffectiveRole';
import { apiFetch } from '../api/client';
import { modelOps, withAdminKeyRetry } from '../api/mutations';
import { useMergedVersions, useModelHealth, useTimeline } from '../api/hooks';
import { WorkerMatrix } from '../components/WorkerMatrix';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
import { ModelAccessPanel } from '../components/ModelAccessPanel';
import { StatusBadge } from '../components/StatusBadge';
import { VersionsTable } from '../components/VersionsTable';
import { PageHeader } from '../components/PageHeader';
import { RoutingEditor } from '../components/RoutingEditor';
import { buildTimelineOption } from '../components/timelineChart';
import { useChartColors, useNeutrals } from '../context/ThemeModeContext';
import { dataTextStyle, MONO_FONT, TYPE } from '../theme';

export function ModelDetailPage() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { name = '' } = useParams();
  const navigate = useNavigate();
  const { instanceId } = useInstance();
  const ilink = useInstanceLink();
  const can = useCanInstance();
  const queryClient = useQueryClient();
  const [editingRouting, setEditingRouting] = useState(false);
  const [loadOpen, setLoadOpen] = useState(false);
  const [loadVersion, setLoadVersion] = useState('');

  const chartColors = useChartColors();
  const neutrals = useNeutrals();
  const merged = useMergedVersions(instanceId, name);
  const versions = merged.versions;
  const unloadedVersions = versions.filter((v) => !v.loaded).map((v) => v.version);
  const healthQuery = useModelHealth(instanceId, name, undefined, merged.hasLoaded);
  const timelineQuery = useTimeline(instanceId, name, undefined, 5_000, merged.hasLoaded);
  const compareQuery = useQuery({
    queryKey: [instanceId, 'compare', name],
    queryFn: () => apiFetch<unknown>(instanceId!, `/v2/models/${encodeURIComponent(name)}/compare`),
    enabled: instanceId !== null && merged.hasLoaded,
    retry: 0,
  });

  const snapshots = timelineQuery.data ? [timelineQuery.data] : [];

  const submitLoad = async () => {
    if (!instanceId || !loadVersion.trim()) return;
    try {
      await withAdminKeyRetry(instanceId, () => modelOps.loadVersion(instanceId, name, loadVersion.trim()));
      message.success(t('ops.loadRequested', { version: loadVersion.trim() }));
      await queryClient.invalidateQueries({ queryKey: [instanceId] });
      setLoadOpen(false);
      setLoadVersion('');
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

  // Neither in the repository nor in the registry — a genuinely unknown model.
  if (!merged.isLoading && !merged.inRepo && !merged.hasLoaded) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <PageHeader
          title={name}
          breadcrumb={[{ title: t('models.title'), href: ilink('/models') }, { title: name }]}
          onBack={() => navigate(ilink('/models'))}
        />
        <Card size="small">
          <Empty description={t('models.notFound')} />
        </Card>
      </div>
    );
  }

  const loadFirstHint = <Empty description={t('models.loadToView')} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <PageHeader
        breadcrumb={[{ title: t('models.title'), href: ilink('/models') }, { title: name }]}
        onBack={() => navigate(ilink('/models'))}
        title={
          <span>
            {name}
            {merged.activeVersion && (
              <span style={{ ...dataTextStyle, fontSize: TYPE.secondary, color: neutrals.textSecondary, marginLeft: 12 }}>
                ● {merged.activeVersion}
              </span>
            )}
            {!merged.hasLoaded && !merged.isLoading && (
              <span style={{ marginLeft: 12 }}>
                <StatusBadge status="unloaded" text={t('models.unloaded')} />
              </span>
            )}
          </span>
        }
        subtitle={
          healthQuery.data
            ? `${t('models.healthyWorkers')}: ${healthQuery.data.healthy_workers}/${healthQuery.data.total_workers} · ${instanceId}`
            : instanceId
        }
        extra={
          can('operator') && (unloadedVersions.length > 0 || !merged.inRepo) ? (
            <Button size="small" onClick={() => setLoadOpen(true)}>
              {t('ops.loadVersion')}
            </Button>
          ) : undefined
        }
      />

      <Tabs
        items={[
          {
            key: 'versions',
            label: t('models.tabs.versions'),
            children: (
              <Card
                size="small"
                extra={
                  can('operator') && !editingRouting && merged.hasLoaded ? (
                    <Button size="small" onClick={() => setEditingRouting(true)}>
                      {t('routing.edit')}
                    </Button>
                  ) : undefined
                }
              >
                <VersionsTable model={name} versions={versions} loading={merged.isLoading} ops={can('operator')} />
                {editingRouting && (
                  <RoutingEditor model={name} versions={versions.filter((v) => v.loaded)} onClose={() => setEditingRouting(false)} />
                )}
              </Card>
            ),
          },
          {
            key: 'workers',
            label: t('models.tabs.workers'),
            children: (
              <Card size="small" loading={merged.hasLoaded && healthQuery.isLoading}>
                {merged.hasLoaded ? (
                  healthQuery.data && <WorkerMatrix workers={healthQuery.data.workers} />
                ) : (
                  loadFirstHint
                )}
              </Card>
            ),
          },
          {
            key: 'metrics',
            label: t('models.tabs.metrics'),
            children: merged.hasLoaded ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                <ChartCard
                  title={t('metrics.qps')}
                  loading={timelineQuery.isLoading}
                  error={timelineQuery.error}
                  isEmpty={snapshots.every((s) => s.entries.length === 0)}
                  onRetry={() => timelineQuery.refetch()}
                >
                  <EChart option={buildTimelineOption(snapshots, 'qps', { palette: chartColors })} group={`model-${name}`} />
                </ChartCard>
                <ChartCard
                  title={t('metrics.p99')}
                  loading={timelineQuery.isLoading}
                  error={timelineQuery.error}
                  isEmpty={snapshots.every((s) => s.entries.length === 0)}
                >
                  <EChart option={buildTimelineOption(snapshots, 'p99_ms', { yAxisName: 'ms', palette: chartColors })} group={`model-${name}`} />
                </ChartCard>
              </div>
            ) : (
              <Card size="small">{loadFirstHint}</Card>
            ),
          },
          {
            key: 'compare',
            label: t('models.tabs.compare'),
            children: (
              <Card size="small" loading={merged.hasLoaded && compareQuery.isLoading}>
                {!merged.hasLoaded ? (
                  loadFirstHint
                ) : compareQuery.isError ? (
                  <Typography.Text type="secondary">{compareQuery.error.message}</Typography.Text>
                ) : (
                  <pre style={{ fontFamily: MONO_FONT, fontSize: 12, margin: 0 }}>
                    {JSON.stringify(compareQuery.data ?? {}, null, 2)}
                  </pre>
                )}
              </Card>
            ),
          },
          // Per-model whitelist management — instance admins only.
          ...(can('admin') && instanceId
            ? [
                {
                  key: 'access',
                  label: t('models.tabs.access'),
                  children: (
                    <Card size="small">
                      <ModelAccessPanel instanceId={instanceId} model={name} />
                    </Card>
                  ),
                },
              ]
            : []),
        ]}
      />

      <Modal
        open={loadOpen}
        title={t('ops.loadVersion')}
        okText={t('ops.load')}
        onOk={submitLoad}
        onCancel={() => setLoadOpen(false)}
        okButtonProps={{ disabled: !loadVersion.trim() }}
      >
        <p style={{ fontSize: 13 }}>{t('ops.loadBody', { model: name })}</p>
        {unloadedVersions.length > 0 ? (
          <Select
            style={{ width: '100%', fontFamily: MONO_FONT }}
            value={loadVersion || undefined}
            onChange={setLoadVersion}
            placeholder="v2"
            options={unloadedVersions.map((v) => ({ value: v, label: v }))}
          />
        ) : (
          <Input
            value={loadVersion}
            onChange={(e) => setLoadVersion(e.target.value)}
            placeholder="v2"
            style={{ fontFamily: MONO_FONT }}
            onPressEnter={submitLoad}
          />
        )}
      </Modal>
    </div>
  );
}
