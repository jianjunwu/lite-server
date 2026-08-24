import { Card, Tabs, Typography, Button, Input, Modal, App } from 'antd';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useAuth } from '../context/AuthContext';
import { apiFetch } from '../api/client';
import { modelOps, withAdminKeyRetry } from '../api/mutations';
import { useModelHealth, useTimeline, useVersions } from '../api/hooks';
import { WorkerMatrix } from '../components/WorkerMatrix';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
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
  const { instanceId } = useInstance();
  const { can } = useAuth();
  const queryClient = useQueryClient();
  const [editingRouting, setEditingRouting] = useState(false);
  const [loadOpen, setLoadOpen] = useState(false);
  const [loadVersion, setLoadVersion] = useState('');

  const chartColors = useChartColors();
  const neutrals = useNeutrals();
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

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <PageHeader
        title={
          <span>
            {name}
            {versionsQuery.data?.active_version && (
              <span style={{ ...dataTextStyle, fontSize: TYPE.secondary, color: neutrals.textSecondary, marginLeft: 12 }}>
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
        extra={
          can('operator') ? (
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
                  can('operator') && !editingRouting && versions.length > 0 ? (
                    <Button size="small" onClick={() => setEditingRouting(true)}>
                      {t('routing.edit')}
                    </Button>
                  ) : undefined
                }
              >
                <VersionsTable model={name} versions={versions} loading={versionsQuery.isLoading} ops={can('operator')} />
                {editingRouting && (
                  <RoutingEditor model={name} versions={versions} onClose={() => setEditingRouting(false)} />
                )}
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

      <Modal
        open={loadOpen}
        title={t('ops.loadVersion')}
        okText={t('ops.load')}
        onOk={submitLoad}
        onCancel={() => setLoadOpen(false)}
        okButtonProps={{ disabled: !loadVersion.trim() }}
      >
        <p style={{ fontSize: 13 }}>{t('ops.loadBody', { model: name })}</p>
        <Input
          value={loadVersion}
          onChange={(e) => setLoadVersion(e.target.value)}
          placeholder="v2"
          style={{ fontFamily: MONO_FONT }}
          onPressEnter={submitLoad}
        />
      </Modal>
    </div>
  );
}
