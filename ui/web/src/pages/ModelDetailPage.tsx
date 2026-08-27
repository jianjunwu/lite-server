import { Card, Col, Empty, Row, Select, Table, Tabs, Typography, Button, Input, Modal } from 'antd';
import {
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
import type { ModelCompare, TimelineEntry } from '../api/types';
import { WorkerMatrix } from '../components/WorkerMatrix';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
import { ModelAccessPanel } from '../components/ModelAccessPanel';
import { ModelGlyph } from '../components/ModelGlyph';
import { StatusBadge, statusKind } from '../components/StatusBadge';
import { StatNum } from '../components/StatNum';
import { VersionsTable } from '../components/VersionsTable';
import { PageHeader } from '../components/PageHeader';
import { Reveal } from '../components/PageHero';
import { TrafficRiver } from '../components/TrafficRiver';
import { RoutingEditor } from '../components/RoutingEditor';
import { useLifecycleOp } from '../components/useLifecycleOp';
import { buildTimelineOption, fieldState, type MetricKey } from '../components/timelineChart';
import { formatMs, formatNumber } from '../components/format';
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

/** Full model-level metric catalog (MetricKey in timelineChart.ts). Titles
 * reuse the MetricsPage i18n keys; MetricsPage keeps its own grouped copy. */
const METRIC_CATALOG: { key: MetricKey; titleKey: string; yAxisName?: string }[] = [
  { key: 'qps', titleKey: 'metrics.qps' },
  { key: 'p99_ms', titleKey: 'metrics.p99', yAxisName: 'ms' },
  { key: 'ttft_p99_ms', titleKey: 'metrics.ttftP99', yAxisName: 'ms' },
  { key: 'tbt_p99_ms', titleKey: 'metrics.tbtP99', yAxisName: 'ms' },
  { key: 'tokens_per_s', titleKey: 'metrics.tokensPerS' },
  { key: 'stream_bytes_per_s', titleKey: 'metrics.streamBytesPerS' },
  { key: 'queue_depth', titleKey: 'metrics.queueDepth' },
  { key: 'in_flight', titleKey: 'metrics.inFlight' },
  { key: 'active_streams', titleKey: 'metrics.activeStreams' },
  { key: 'active_workers', titleKey: 'metrics.workers' },
  { key: 'worker_saturation', titleKey: 'metrics.saturation' },
  { key: 'rss_mb', titleKey: 'metrics.rss', yAxisName: 'MB' },
  { key: 'cpu_percent', titleKey: 'metrics.cpu', yAxisName: '%' },
  { key: 'retries_per_s', titleKey: 'metrics.retriesPerS' },
  { key: 'ejections_per_s', titleKey: 'metrics.ejectionsPerS' },
];

const DEFAULT_METRICS: MetricKey[] = ['qps', 'p99_ms', 'ttft_p99_ms', 'queue_depth', 'in_flight', 'worker_saturation'];
const METRICS_STORAGE_KEY = 'lite-ui-model-metrics-v1';
const CATALOG_KEYS = new Set<string>(METRIC_CATALOG.map((m) => m.key));

function loadMetricSelection(): MetricKey[] {
  try {
    const raw = JSON.parse(localStorage.getItem(METRICS_STORAGE_KEY) ?? '[]') as unknown;
    if (Array.isArray(raw)) {
      const valid = raw.filter((k): k is MetricKey => typeof k === 'string' && CATALOG_KEYS.has(k));
      if (valid.length > 0) return valid;
    }
  } catch {
    // Corrupt payload → fall through to defaults.
  }
  return DEFAULT_METRICS;
}

/** Compact average for the compare table: keeps up to 3 decimals so small
 * rates (0.07 qps) don't round away. */
function formatAvg(v: number): string {
  if (!Number.isFinite(v)) return '-';
  return Number.isInteger(v) ? String(v) : String(+v.toFixed(3));
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
  const [selectedMetrics, setSelectedMetrics] = useState<MetricKey[]>(loadMetricSelection);
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
    queryFn: () => apiFetch<ModelCompare>(instanceId!, `/v2/models/${encodeURIComponent(name)}/compare`),
    enabled: instanceId !== null && merged.hasLoaded,
    retry: 0,
  });

  const snapshots = timelineQuery.data ? [timelineQuery.data] : [];

  const changeMetrics = (keys: MetricKey[]) => {
    const next = keys.length > 0 ? keys : DEFAULT_METRICS;
    setSelectedMetrics(next);
    localStorage.setItem(METRICS_STORAGE_KEY, JSON.stringify(next));
  };

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
  // Sum per-version workers: the model-level health endpoint describes only
  // one version, so its total undercounts multi-version models.
  const workerTotal = loadedVersions.reduce((sum, v) => sum + v.workers.total, 0);
  const readyWorkers = loadedVersions.reduce((sum, v) => sum + v.workers.ready, 0);
  // Latest timeline point drives the KPI strip under the header.
  const latestEntry: TimelineEntry | undefined =
    timelineQuery.data?.entries[timelineQuery.data.entries.length - 1];
  // Hero-layer statement under the title (plan §4.3), composed from atomic
  // plural-aware parts ("1 of 1 version ready · v2 active · 2 workers").
  const statement = !merged.hasLoaded
    ? instanceId
    : [
        t('models.detailReady', { ready: readyCount, count: loadedVersions.length }),
        merged.activeVersion ? t('models.detailActive', { version: merged.activeVersion }) : null,
        t('models.detailWorkers', { count: workerTotal }),
      ]
        .filter(Boolean)
        .join(' · ');

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[5] }}>
      <PageHeader
        breadcrumb={[{ title: t('models.title'), href: ilink('/models') }, { title: name }]}
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

      {merged.hasLoaded && (
        <Reveal order={1}>
          <div style={{ display: 'flex', gap: SPACE[5], flexWrap: 'wrap' }}>
            <StatNum
              label={t('metrics.currentQps')}
              value={latestEntry ? formatNumber(latestEntry.qps) : '-'}
            />
            <StatNum
              label={t('metrics.currentP99')}
              value={latestEntry && latestEntry.p99_ms > 0 ? formatMs(latestEntry.p99_ms) : '-'}
            />
            <StatNum
              label={t('metrics.currentQueue')}
              value={latestEntry ? formatNumber(latestEntry.queue_depth) : '-'}
            />
            <StatNum label={t('metrics.workers')} value={`${readyWorkers}/${workerTotal}`} />
          </div>
        </Reveal>
      )}

      {merged.hasLoaded && (
        <Reveal order={2}>
          <div style={{ display: 'flex', alignItems: 'center', gap: SPACE[3] }}>
            <span
              style={{
                fontSize: TYPE.eyebrow,
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.08em',
                color: neutrals.textSecondary,
                flexShrink: 0,
              }}
            >
              {t('models.traffic')}
            </span>
            <div style={{ flex: 1, minWidth: 0 }}>
              <TrafficRiver
                versions={loadedVersions}
                height={16}
                model={name}
                editable={can('operator')}
                onSelect={(v) =>
                  navigate(ilink(`/models/${encodeURIComponent(name)}/versions/${encodeURIComponent(v)}`))
                }
              />
            </div>
            {can('operator') && loadedVersions.length > 1 && (
              <span style={{ fontSize: TYPE.secondary, color: neutrals.textSecondary, flexShrink: 0 }}>
                {t('models.trafficDragHint')}
              </span>
            )}
          </div>
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
                <div style={{ display: 'flex', gap: SPACE[3], alignItems: 'center' }}>
                  <Select
                    mode="multiple"
                    style={{ flex: 1, minWidth: 280 }}
                    maxTagCount="responsive"
                    placeholder={t('models.selectMetrics')}
                    value={selectedMetrics}
                    onChange={(keys) => changeMetrics(keys as MetricKey[])}
                    options={METRIC_CATALOG.map((m) => ({ value: m.key, label: t(m.titleKey) }))}
                    // All 15 options inside the virtual-scroll window.
                    listHeight={480}
                  />
                  <Button size="small" onClick={() => changeMetrics(DEFAULT_METRICS)}>
                    {t('models.resetMetrics')}
                  </Button>
                </div>
                <Row gutter={[SPACE[5], SPACE[5]]}>
                  {METRIC_CATALOG.filter((m) => selectedMetrics.includes(m.key)).map((c) => {
                    const isEmpty = snapshots.every((s) => s.entries.length === 0);
                    const state = isEmpty ? ('ok' as const) : fieldState(snapshots, c.key);
                    const emptyText =
                      state === 'unsupported'
                        ? t('metrics.unsupported')
                        : state === 'not-reported'
                          ? t('metrics.notReported')
                          : undefined;
                    return (
                      <Col xs={24} xl={12} key={c.key}>
                        <ChartCard
                          title={t(c.titleKey)}
                          loading={timelineQuery.isLoading}
                          error={timelineQuery.error}
                          isEmpty={isEmpty || state !== 'ok'}
                          emptyText={emptyText}
                          onRetry={() => timelineQuery.refetch()}
                        >
                          <EChart
                            option={buildTimelineOption(snapshots, c.key, {
                              yAxisName: c.yAxisName,
                              palette: chartColors,
                            })}
                            group={`model-${name}`}
                          />
                        </ChartCard>
                      </Col>
                    );
                  })}
                </Row>
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
                ) : (compareQuery.data?.versions.length ?? 0) === 0 ? (
                  <Empty description={t('models.compareEmpty')} />
                ) : (
                  <Table
                    size="small"
                    rowKey="version"
                    pagination={false}
                    dataSource={compareQuery.data!.versions}
                    columns={[
                      {
                        title: t('models.activeVersion'),
                        dataIndex: 'version',
                        render: (v: string) => (
                          <Link to={ilink(`/models/${encodeURIComponent(name)}/versions/${encodeURIComponent(v)}`)}>
                            {v}
                          </Link>
                        ),
                      },
                      { title: t('models.compareAvgQps'), dataIndex: 'avg_qps', render: formatAvg },
                      {
                        title: t('models.compareAvgP99'),
                        dataIndex: 'avg_p99_ms',
                        render: (v: number) => formatMs(v),
                      },
                      { title: t('models.compareAvgQueue'), dataIndex: 'avg_queue_depth', render: formatAvg },
                      { title: t('models.compareAvgWorkers'), dataIndex: 'avg_active_workers', render: formatAvg },
                      { title: t('models.compareSamples'), dataIndex: 'sample_count' },
                    ]}
                  />
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
