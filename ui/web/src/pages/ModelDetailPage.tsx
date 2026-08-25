import { Card, Empty, Tabs, Typography, Button, Input, Modal, Select } from 'antd';
import {
  AimOutlined,
  ApiOutlined,
  BranchesOutlined,
  ClusterOutlined,
  DiffOutlined,
  LineChartOutlined,
  SafetyOutlined,
  TableOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useInstanceLink } from '../context/useInstanceLink';
import { useCanInstance } from '../context/useEffectiveRole';
import { apiFetch } from '../api/client';
import { useMergedModels, useMergedVersions, useModelHealth, useTimeline } from '../api/hooks';
import { WorkerMatrix } from '../components/WorkerMatrix';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
import { ModelAccessPanel } from '../components/ModelAccessPanel';
import { ModelGlyph } from '../components/ModelGlyph';
import { StatusBadge, statusKind } from '../components/StatusBadge';
import { VersionsTable } from '../components/VersionsTable';
import { PageHeader } from '../components/PageHeader';
import { Reveal } from '../components/PageHero';
import { TrafficRiver } from '../components/TrafficRiver';
import { RoutingEditor } from '../components/RoutingEditor';
import { useLifecycleOp } from '../components/useLifecycleOp';
import { buildTimelineOption } from '../components/timelineChart';
import { useChartColors, useNeutrals } from '../context/ThemeModeContext';
import { dataTextStyle, MONO_FONT, TYPE } from '../theme';
import { SPACE } from '../tokens';

/** Small inline icon for tab labels — aria-hidden so the tab's accessible
 * name stays pure text. */
function tabLabel(icon: React.ReactNode, text: string) {
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
      {icon}
      {text}
    </span>
  );
}

export function ModelDetailPage() {
  const { t } = useTranslation();
  const { name = '' } = useParams();
  const navigate = useNavigate();
  const { instanceId } = useInstance();
  const ilink = useInstanceLink();
  const can = useCanInstance();
  const [editingRouting, setEditingRouting] = useState(false);
  const [loadOpen, setLoadOpen] = useState(false);
  const [loadVersion, setLoadVersion] = useState('');
  const [tab, setTab] = useState('versions');
  const { runLifecycle } = useLifecycleOp();

  const chartColors = useChartColors();
  const neutrals = useNeutrals();
  const merged = useMergedVersions(instanceId, name);
  const modelsList = useMergedModels(instanceId);
  const modelType = modelsList.data.find((m) => m.name === name)?.modelType ?? 'unknown';
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
    if (!loadVersion.trim()) return;
    await runLifecycle('load', name, loadVersion.trim());
    setLoadOpen(false);
    setLoadVersion('');
  };

  // Neither in the repository nor in the registry — a genuinely unknown model.
  if (!merged.isLoading && !merged.inRepo && !merged.hasLoaded) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[5] }}>
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

  const loadedVersions = versions.filter((v) => v.loaded);
  const readyCount = loadedVersions.filter((v) => statusKind(v.status) === 'ready').length;
  const workerTotal = healthQuery.data?.total_workers;
  const activeWeight = versions.find((v) => v.version === merged.activeVersion)?.weight ?? 0;
  // Hero-layer statement under the title (plan §4.3).
  const statement = !merged.hasLoaded
    ? instanceId
    : merged.activeVersion
      ? t('models.detailStmtActive', {
          ready: readyCount,
          total: loadedVersions.length,
          version: merged.activeVersion,
          workers: workerTotal ?? '-',
        })
      : t('models.detailStmt', { ready: readyCount, total: loadedVersions.length, workers: workerTotal ?? '-' });

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[5] }}>
      <PageHeader
        breadcrumb={[{ title: t('models.title'), href: ilink('/models') }, { title: name }]}
        onBack={() => navigate(ilink('/models'))}
        title={
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: SPACE[3] }}>
            <ModelGlyph name={name} type={modelType} size={SPACE[7]} />
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
          </span>
        }
        subtitle={statement}
        extra={
          can('operator') && (unloadedVersions.length > 0 || !merged.inRepo) ? (
            <Button size="small" onClick={() => setLoadOpen(true)}>
              {t('ops.loadVersion')}
            </Button>
          ) : undefined
        }
      />

      {versions.length > 0 && (
        <div style={{ display: 'flex', gap: SPACE[2], flexWrap: 'wrap' }}>
          <button type="button" className="chip" onClick={() => setTab('versions')}>
            <BranchesOutlined aria-hidden />
            <span style={dataTextStyle}>
              {readyCount}/{loadedVersions.length}
            </span>
            {t('models.chipReady')}
          </button>
          {merged.hasLoaded && (
            <button type="button" className="chip" onClick={() => setTab('workers')}>
              <ApiOutlined aria-hidden />
              <span style={dataTextStyle}>{workerTotal ?? '-'}</span>
              {t('models.workerLabel', { count: workerTotal ?? 0 })}
            </button>
          )}
          {merged.activeVersion && (
            <Link
              className="chip"
              to={ilink(`/models/${encodeURIComponent(name)}/versions/${encodeURIComponent(merged.activeVersion)}`)}
            >
              <AimOutlined aria-hidden />
              <span style={dataTextStyle}>
                {merged.activeVersion} · {activeWeight}%
              </span>
            </Link>
          )}
        </div>
      )}

      {merged.hasLoaded && (
        <Reveal order={1}>
          <Card>
            <div
              style={{
                fontSize: TYPE.eyebrow,
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.08em',
                color: neutrals.textSecondary,
                marginBottom: SPACE[3],
              }}
            >
              {t('models.traffic')}
            </div>
            <TrafficRiver
              versions={loadedVersions}
              height={16}
              model={name}
              editable={can('operator')}
              onSelect={(v) =>
                navigate(ilink(`/models/${encodeURIComponent(name)}/versions/${encodeURIComponent(v)}`))
              }
            />
          </Card>
        </Reveal>
      )}

      <Tabs
        activeKey={tab}
        onChange={setTab}
        items={[
          {
            key: 'versions',
            label: tabLabel(<TableOutlined aria-hidden />, t('models.tabs.versions')),
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
            label: tabLabel(<ClusterOutlined aria-hidden />, t('models.tabs.workers')),
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
            label: tabLabel(<LineChartOutlined aria-hidden />, t('models.tabs.metrics')),
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
            label: tabLabel(<DiffOutlined aria-hidden />, t('models.tabs.compare')),
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
                  label: tabLabel(<SafetyOutlined aria-hidden />, t('models.tabs.access')),
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
