import { Button, Card, Col, Empty, Input, Modal, Row, Segmented, Select, Space, Tabs, Tooltip, Typography } from 'antd';
import {
  LineChartOutlined,
  SafetyOutlined,
  TableOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useInstanceLink } from '../context/useInstanceLink';
import { useCanInstance } from '../context/useEffectiveRole';
import { useAlerts, useInstanceName, useMergedModels, useMergedVersions, useTimelineAll } from '../api/hooks';
import type { TimelineEntry } from '../api/types';
import { ChartCard } from '../components/ChartCard';
import { EChart } from '../components/EChart';
import { ModelAccessPanel } from '../components/ModelAccessPanel';
import { ModelGlyph } from '../components/ModelGlyph';
import { StatusBadge, statusKind } from '../components/StatusBadge';
import { StatNum } from '../components/StatNum';
import { VersionCard } from '../components/VersionCard';
import { PageHeader } from '../components/PageHeader';
import { Reveal } from '../components/PageHero';
import { TrafficRiver } from '../components/TrafficRiver';
import { RoutingEditor } from '../components/RoutingEditor';
import { UploadDrawer } from '../components/UploadDrawer';
import { useLifecycleOp } from '../components/useLifecycleOp';
import { buildTimelineOption, fieldState, stepForRange, trimToRange, type ThresholdLine } from '../components/timelineChart';
import {
  METRIC_GROUP_ORDER,
  METRIC_GROUPS,
  RANGE_LABEL,
  RANGE_SECONDS,
  REFRESH_KEY,
  loadRefresh,
  type MetricGroupKey,
  type RangeKey,
} from '../components/metricsGroups';
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

export function ModelDetailPage() {
  const { t } = useTranslation();
  const { name = '' } = useParams();
  const navigate = useNavigate();
  const { instanceId } = useInstance();
  const ilink = useInstanceLink();
  const instName = useInstanceName(instanceId);
  const can = useCanInstance();
  const [editingRouting, setEditingRouting] = useState(false);
  const [loadOpen, setLoadOpen] = useState(false);
  const [loadVersion, setLoadVersion] = useState('');
  const [tab, setTab] = useState('metrics');
  const [uploadOpen, setUploadOpen] = useState(false);
  // Metrics-tab controls (migrated from MetricsPage, plan §5): group tabs,
  // time range with coverage negotiation, version overlay, refresh/pause.
  const [group, setGroup] = useState<MetricGroupKey>('throughput');
  const [range, setRange] = useState<RangeKey>('all');
  const [refreshMs, setRefreshMs] = useState<number>(loadRefresh());
  const [paused, setPaused] = useState(false);
  const [selectedVersions, setSelectedVersions] = useState<string[]>([]);
  const { runLifecycle } = useLifecycleOp();

  const chartColors = useChartColors();
  const neutrals = useNeutrals();
  const merged = useMergedVersions(instanceId, name);
  const modelsList = useMergedModels(instanceId);
  const modelType = modelsList.data.find((m) => m.name === name)?.modelType ?? 'unknown';
  const versions = merged.versions;
  const unloadedVersions = versions.filter((v) => !v.loaded).map((v) => v.version);

  const [coverageSeconds, setCoverageSeconds] = useState<number | undefined>(undefined);
  const [intervalSeconds, setIntervalSeconds] = useState<number | undefined>(undefined);
  const effectiveRange: RangeKey =
    RANGE_SECONDS[range] != null && coverageSeconds != null && RANGE_SECONDS[range]! > coverageSeconds
      ? 'all'
      : range;
  const step = useMemo(() => {
    if (intervalSeconds == null) return undefined;
    const seconds = RANGE_SECONDS[effectiveRange] ?? coverageSeconds;
    if (seconds == null) return undefined;
    return stepForRange(seconds, intervalSeconds);
  }, [effectiveRange, coverageSeconds, intervalSeconds]);

  // Single timelineAll poll feeds the KPI strip, the version cards and the
  // metrics tab (plan §5) — the old model-level useTimeline is gone.
  const timelineAllQuery = useTimelineAll(instanceId, merged.hasLoaded && !paused ? refreshMs : false, step);
  const alertsQuery = useAlerts(instanceId, paused ? false : 10_000);

  // Coverage arrives with the first response; a picked range that exceeds it
  // falls back to 'all' (never disabled). Reset on instance change — a
  // pre-M3 instance (no headers) must not inherit the previous coverage.
  const dataCoverage = timelineAllQuery.data?.coverageSeconds;
  const dataInterval = timelineAllQuery.data?.intervalSeconds;
  useEffect(() => {
    setCoverageSeconds(dataCoverage);
    setIntervalSeconds(dataInterval);
  }, [instanceId, dataCoverage, dataInterval]);

  // Latest point per version — drives the version cards' current data and
  // the KPI strip (sum/max across versions, plan §4).
  const latestByVersion = useMemo(() => {
    const map = new Map<string, TimelineEntry>();
    (timelineAllQuery.data?.snapshots ?? [])
      .filter((s) => s.model === name)
      .forEach((s) => {
        const entry = s.entries[s.entries.length - 1];
        if (entry) map.set(s.version, entry);
      });
    return map;
  }, [timelineAllQuery.data, name]);

  const kpi = useMemo(() => {
    const entries = [...latestByVersion.values()];
    let qps = 0;
    let queue = 0;
    let p99 = 0;
    for (const p of entries) {
      qps += p.qps;
      queue += p.queue_depth;
      p99 = Math.max(p99, p.p99_ms);
    }
    return { hasData: entries.length > 0, qps, queue, p99 };
  }, [latestByVersion]);

  // Metrics-tab series: per-version overlay trimmed to the range and
  // filtered to the selected versions (default: all).
  const versionNames = versions.map((v) => v.version);
  const effectiveVersions = selectedVersions.length > 0 ? selectedVersions : versionNames;
  const snapshots = useMemo(() => {
    const trimmed = trimToRange(timelineAllQuery.data?.snapshots ?? [], RANGE_SECONDS[effectiveRange]);
    return trimmed.filter((s) => s.model === name && effectiveVersions.includes(s.version));
  }, [timelineAllQuery.data, name, effectiveVersions, effectiveRange]);

  // Latest reading per visible version — drives the comparison statement.
  const latest = useMemo(
    () =>
      snapshots
        .map((s) => ({ version: s.version, entry: s.entries[s.entries.length - 1] }))
        .filter((x): x is { version: string; entry: NonNullable<typeof x.entry> } => Boolean(x.entry)),
    [snapshots],
  );

  const isEmpty = snapshots.every((s) => s.entries.length === 0);

  // Comparison conclusion as a statement line inside the metrics tab
  // (plan §5): "v1 answers 12ms faster than v2" — PageHero stays unique to
  // the overview, so this is plain text, not a hero.
  const metricsStatement = useMemo(() => {
    if (latest.length === 0) return t('metrics.stmtWaiting');
    if (latest.length >= 2) {
      const sorted = [...latest].sort((a, b) => a.entry.p99_ms - b.entry.p99_ms);
      const fast = sorted[0];
      const slow = sorted[sorted.length - 1];
      const diff = slow.entry.p99_ms - fast.entry.p99_ms;
      if (diff > 0) {
        return t('metrics.stmtFaster', {
          fast: fast.version,
          slow: slow.version,
          ms: formatMs(diff),
        });
      }
    }
    const first = latest[0];
    return t('metrics.stmtServing', {
      version: first.version,
      qps: formatNumber(first.entry.qps),
      p99: formatMs(first.entry.p99_ms),
    });
  }, [latest, t]);

  // Alert threshold lines for the p99/queue charts (plan §5 — migrated).
  const thresholdsFor = (rule: 'p99_ms' | 'queue_depth'): ThresholdLine[] => {
    const seen = new Map<string, ThresholdLine>();
    (alertsQuery.data?.alerts ?? [])
      .filter((a) => a.rule === rule && a.model === name)
      .forEach((a) => {
        seen.set(`${a.severity}:${a.threshold}`, {
          value: a.threshold,
          label: `${t(a.severity === 'critical' ? 'metrics.criticalThreshold' : 'metrics.warningThreshold')} ${a.threshold}`,
          severity: a.severity,
        });
      });
    return [...seen.values()];
  };

  const rangeOptions = (Object.keys(RANGE_SECONDS) as RangeKey[]).map((rk) => {
    const seconds = RANGE_SECONDS[rk];
    const disabled = seconds != null && coverageSeconds != null && seconds > coverageSeconds;
    const text = t(RANGE_LABEL[rk]);
    return {
      value: rk,
      disabled,
      label: disabled ? (
        <Tooltip title={t('metrics.rangeDisabledHint')}>
          <span>{text}</span>
        </Tooltip>
      ) : (
        text
      ),
    };
  });

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
          breadcrumb={
            instanceId
              ? [
                  { title: instName ?? instanceId, href: ilink(`/instances/${encodeURIComponent(instanceId)}`) },
                  { title: t('models.title'), href: ilink('/models') },
                  { title: name },
                ]
              : undefined
          }
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
        breadcrumb={
          instanceId
            ? [
                { title: instName ?? instanceId, href: ilink(`/instances/${encodeURIComponent(instanceId)}`) },
                { title: t('models.title'), href: ilink('/models') },
                { title: name },
              ]
            : undefined
        }
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
          <span style={{ display: 'inline-flex', gap: SPACE[2] }}>
            {can('operator') && (
              <Button type="primary" icon={<UploadOutlined />} onClick={() => setUploadOpen(true)}>
                {t('upload.newVersion')}
              </Button>
            )}
            {can('operator') && (unloadedVersions.length > 0 || !merged.inRepo) ? (
              <Button size="small" onClick={() => setLoadOpen(true)}>
                {t('ops.loadVersion')}
              </Button>
            ) : undefined}
          </span>
        }
      />

      {merged.hasLoaded && (
        <Reveal order={1}>
          <div style={{ display: 'flex', gap: SPACE[5], flexWrap: 'wrap' }}>
            {/* KPI strip = latest points summed across versions, p99 maxed
                (plan §4 — same 口径 as the version cards). */}
            <StatNum
              label={t('metrics.currentQps')}
              value={kpi.hasData ? formatNumber(kpi.qps) : '-'}
            />
            <StatNum
              label={t('metrics.currentP99')}
              value={kpi.hasData && kpi.p99 > 0 ? formatMs(kpi.p99) : '-'}
            />
            <StatNum
              label={t('metrics.currentQueue')}
              value={kpi.hasData ? formatNumber(kpi.queue) : '-'}
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
              <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[3] }}>
                {can('operator') && !editingRouting && merged.hasLoaded && (
                  <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                    <Button size="small" onClick={() => setEditingRouting(true)}>
                      {t('routing.edit')}
                    </Button>
                  </div>
                )}
                <Row gutter={[SPACE[3], SPACE[3]]}>
                  {versions.map((v) => (
                    <Col xs={24} md={12} xl={8} key={v.version}>
                      <VersionCard
                        model={name}
                        version={v}
                        colorIndex={loadedVersions.findIndex((lv) => lv.version === v.version)}
                        latest={latestByVersion.get(v.version)}
                        ops={can('operator')}
                      />
                    </Col>
                  ))}
                </Row>
                {editingRouting && (
                  <Card size="small">
                    <RoutingEditor
                      model={name}
                      versions={versions.filter((v) => v.loaded)}
                      onClose={() => setEditingRouting(false)}
                    />
                  </Card>
                )}
              </div>
            ),
          },
          {
            key: 'metrics',
            label: tabLabel(<LineChartOutlined aria-hidden />, t('models.tabs.metrics')),
            children: merged.hasLoaded ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                {/* Comparison conclusion (plan §5): "v1 answers 12ms faster
                    than v2" as a statement line, not a hero — the model
                    detail header already owns the page title. */}
                <Typography.Text style={{ fontSize: TYPE.lead, color: neutrals.textSecondary }}>
                  {metricsStatement}
                </Typography.Text>
                <Card size="small" style={{ width: 'fit-content', maxWidth: '100%' }}>
                  <Space wrap size="middle">
                    <Segmented
                      value={effectiveRange}
                      onChange={(v) => setRange(v as RangeKey)}
                      options={rangeOptions}
                    />
                    <Select
                      mode="multiple"
                      style={{ minWidth: 240 }}
                      placeholder={t('metrics.versionsOverlay')}
                      value={effectiveVersions}
                      onChange={setSelectedVersions}
                      options={versionNames.map((v) => ({ value: v, label: v }))}
                      maxTagCount={4}
                    />
                    <Select
                      style={{ width: 130 }}
                      value={paused ? 0 : refreshMs}
                      onChange={(v) => {
                        if (v === 0) {
                          setPaused(true);
                        } else {
                          setPaused(false);
                          setRefreshMs(v);
                          localStorage.setItem(REFRESH_KEY, String(v));
                        }
                      }}
                      options={[
                        { value: 2000, label: t('metrics.every2s') },
                        { value: 5000, label: t('metrics.every5s') },
                        { value: 10000, label: t('metrics.every10s') },
                        { value: 30000, label: t('metrics.every30s') },
                        { value: 0, label: t('metrics.pause') },
                      ]}
                    />
                  </Space>
                </Card>
                <Tabs
                  activeKey={group}
                  onChange={(k) => setGroup(k as MetricGroupKey)}
                  items={METRIC_GROUP_ORDER.map((g) => ({
                    key: g,
                    label: t(`metrics.group${g.charAt(0).toUpperCase()}${g.slice(1)}`),
                  }))}
                />
                <Row gutter={[SPACE[5], SPACE[5]]}>
                  {METRIC_GROUPS[group].map((c) => {
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
                          loading={timelineAllQuery.isLoading}
                          error={timelineAllQuery.error}
                          isEmpty={isEmpty || state !== 'ok'}
                          emptyText={emptyText}
                          onRetry={() => timelineAllQuery.refetch()}
                        >
                          <EChart
                            option={buildTimelineOption(snapshots, c.key, {
                              yAxisName: c.yAxisName,
                              thresholds: c.rule ? thresholdsFor(c.rule) : undefined,
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

      <UploadDrawer
        open={uploadOpen}
        onClose={() => setUploadOpen(false)}
        existingModels={modelsList.data.map((m) => m.name)}
        model={name}
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
