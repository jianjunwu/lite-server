import { useMemo, useState } from 'react';
import { Button, Card, Checkbox, Col, Dropdown, Empty, Input, Modal, Popconfirm, Row, Segmented, Tooltip, Typography } from 'antd';
import {
  AimOutlined,
  ApiOutlined,
  BranchesOutlined,
  CopyOutlined,
  DownOutlined,
  MoreOutlined,
  UploadOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { Link, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { App } from 'antd';
import { useInstance } from '../context/InstanceContext';
import { useInstanceLink } from '../context/useInstanceLink';
import { useCanInstance } from '../context/useEffectiveRole';
import { useInstanceName, useMergedModels, useMergedVersions, useTimelineAll } from '../api/hooks';
import { modelOps, withAdminKeyRetry } from '../api/mutations';
import type { MergedModel, MergedModelStatus } from '../api/merge';
import type { TimelineEntry } from '../api/types';
import { StatusBadge } from '../components/StatusBadge';
import { VersionCard } from '../components/VersionCard';
import { PageHeader } from '../components/PageHeader';
import { Reveal } from '../components/PageHero';
import { TrafficRiver } from '../components/TrafficRiver';
import { UploadDrawer } from '../components/UploadDrawer';
import { ModelGlyph } from '../components/ModelGlyph';
import { lifecycleKey, useLifecycleOp } from '../components/useLifecycleOp';
import { formatMs, formatNumber } from '../components/format';
import { dataTextStyle, MONO_FONT, STATUS_COLORS, TYPE } from '../theme';
import { SPACE } from '../tokens';
import { useNeutrals } from '../context/ThemeModeContext';

const STATUS_FILTERS: MergedModelStatus[] = ['ready', 'loading', 'degraded', 'unloaded'];

/** Content column offset: glyph (SPACE[6]) + header gap (SPACE[3]). */
const GLYPH_OFFSET = SPACE[6] + SPACE[3];

/** Row-level action for an unloaded model: single repo version loads inline,
 * multi-version models jump to the detail page for version choice. The load
 * itself is tracked as a task until the version reports ready. */
function LoadAction({ model }: { model: MergedModel }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const ilink = useInstanceLink();
  const { runLifecycle, pending } = useLifecycleOp();

  if (model.repoVersions.length !== 1) {
    return (
      <Button type="text" size="small" onClick={() => navigate(ilink(`/models/${encodeURIComponent(model.name)}`))}>
        {t('ops.load')}
      </Button>
    );
  }
  const version = model.repoVersions[0];
  return (
    <Popconfirm title={t('ops.loadConfirm', { version })} onConfirm={() => runLifecycle('load', model.name, version)}>
      <Button type="text" size="small" loading={pending === lifecycleKey('load', model.name, version)}>
        {t('ops.load')}
      </Button>
    </Popconfirm>
  );
}

/** ⋯ menu with the destructive model-level op: delete the whole model from
 * the repository, gated by typing its name (force covers loaded versions). */
function DeleteModelAction({ model }: { model: MergedModel }) {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { instanceId } = useInstance();
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [confirmText, setConfirmText] = useState('');
  const [force, setForce] = useState(false);
  const [busy, setBusy] = useState(false);

  const close = () => {
    setOpen(false);
    setConfirmText('');
    setForce(false);
  };

  const submit = async () => {
    if (!instanceId) return;
    setBusy(true);
    try {
      await withAdminKeyRetry(instanceId, () => modelOps.deleteModel(instanceId, model.name, force));
      message.success(t('ops.modelDeleted'));
      await queryClient.invalidateQueries({ queryKey: [instanceId] });
      close();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <Dropdown
        menu={{
          items: [{ key: 'delete', danger: true, label: t('ops.deleteModel') }],
          // Stop the React-tree bubble: the menu lives in a portal, so the
          // card's onCardClick guard (closest a/button/input) can't see it and
          // would navigate away, unmounting the modal that just opened.
          onClick: ({ domEvent }) => {
            domEvent.stopPropagation();
            setOpen(true);
          },
        }}
        trigger={['click']}
      >
        <Button type="text" size="small" icon={<MoreOutlined />} aria-label={t('ops.actions')} />
      </Dropdown>
      <Modal
        open={open}
        title={t('ops.deleteModelTitle', { model: model.name })}
        okText={t('ops.delete')}
        okButtonProps={{ danger: true, disabled: confirmText !== model.name || busy }}
        onOk={submit}
        onCancel={close}
      >
        <p style={{ fontSize: TYPE.secondary }}>{t('ops.deleteModelBody', { model: model.name })}</p>
        <Input
          value={confirmText}
          onChange={(e) => setConfirmText(e.target.value)}
          placeholder={model.name}
          style={{ fontFamily: MONO_FONT }}
        />
        <Checkbox checked={force} onChange={(e) => setForce(e.target.checked)} style={{ marginTop: SPACE[3] }}>
          {t('ops.forceDeleteLoaded')}
        </Checkbox>
      </Modal>
    </>
  );
}

/**
 * One model = one card: glyph faceplate + name + status statement, the
 * traffic river as the card's main visual, a stat rail, and the versions
 * table behind a disclosure so the list stays scannable. The whole card
 * navigates to the detail page; nested controls keep their own behavior.
 */
function ModelCard({
  model,
  order,
  latestByVersion,
}: {
  model: MergedModel;
  order: number;
  /** Latest timeline point per version, fanned out from the page-level poll. */
  latestByVersion: Record<string, TimelineEntry>;
}) {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { instanceId } = useInstance();
  const can = useCanInstance();
  const ilink = useInstanceLink();
  const navigate = useNavigate();
  const neutrals = useNeutrals();
  const merged = useMergedVersions(instanceId, model.name);
  const [expanded, setExpanded] = useState(false);

  const loaded = merged.versions.filter((v) => v.loaded);
  const active = loaded.find((v) => v.active);
  const workersReady = loaded.reduce((sum, v) => sum + v.workers.ready, 0);
  const workersTotal = loaded.reduce((sum, v) => sum + v.workers.total, 0);

  // L2 live chips (plan §4): QPS/queue/RSS/streams summed across the model's
  // versions, p99 maxed — same sumField/maxField policy as MetricsPage.
  const live = useMemo(() => {
    const entries = Object.values(latestByVersion);
    if (entries.length === 0) return undefined;
    let qps = 0;
    let p99 = 0;
    let queue = 0;
    let streams = 0;
    let rss: number | undefined;
    for (const p of entries) {
      qps += p.qps;
      queue += p.queue_depth;
      streams += p.active_streams;
      if (p.rss_mb != null) rss = (rss ?? 0) + p.rss_mb;
      p99 = Math.max(p99, p.p99_ms);
    }
    return { qps, p99, queue, streams, rss };
  }, [latestByVersion]);
  const statement = !merged.hasLoaded
    ? t('models.stmtUnloaded')
    : active
      ? t('models.stmtServing', { version: active.version, weight: active.weight })
      : t('models.stmtNoActive');

  const detailUrl = ilink(`/models/${encodeURIComponent(model.name)}`);
  const onCardClick = (e: React.MouseEvent) => {
    const el = e.target as HTMLElement;
    if (el.closest('a, button, input, [role="slider"], .no-card-nav')) return;
    navigate(detailUrl);
  };

  const copyName = async () => {
    await navigator.clipboard.writeText(model.name);
    message.success(t('models.copied'));
  };

  return (
    <Reveal order={order}>
      <Card className="lift model-card" style={{ marginBottom: SPACE[5], cursor: 'pointer' }} onClick={onCardClick}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: SPACE[3] }}>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: SPACE[3], minWidth: 0 }}>
            <ModelGlyph name={model.name} type={model.modelType} />
            <Link to={detailUrl} style={{ fontSize: TYPE.cardTitle, fontWeight: 600, letterSpacing: '-0.01em' }}>
              {model.name}
            </Link>
            <Tooltip title={t('models.copyName')}>
              <Button
                type="text"
                size="small"
                className="hover-only"
                icon={<CopyOutlined aria-hidden />}
                aria-label={t('models.copyName')}
                onClick={copyName}
              />
            </Tooltip>
            {model.drifted && (
              <Tooltip title={t('models.drifted')}>
                <WarningOutlined aria-label="drift warning" style={{ color: STATUS_COLORS.warning }} />
              </Tooltip>
            )}
          </span>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: SPACE[2], flexShrink: 0 }}>
            <StatusBadge status={model.status} text={t(`models.filters.${model.status}`)} />
            {can('operator') && (
              <>
                {model.status === 'unloaded' && model.repoVersions.length > 0 && <LoadAction model={model} />}
                <DeleteModelAction model={model} />
              </>
            )}
          </span>
        </div>

        <div style={{ fontSize: TYPE.lead, color: neutrals.textSecondary, marginTop: SPACE[1], marginLeft: GLYPH_OFFSET }}>
          {statement}
        </div>

        {loaded.length > 0 && (
          <div className="no-card-nav" style={{ marginTop: SPACE[4], marginLeft: GLYPH_OFFSET }}>
            <TrafficRiver
              versions={loaded}
              height={16}
              onSelect={(v) =>
                navigate(ilink(`/models/${encodeURIComponent(model.name)}/versions/${encodeURIComponent(v)}`))
              }
            />
          </div>
        )}

        {live && (
          <div className="no-card-nav" style={{ display: 'flex', gap: SPACE[2], flexWrap: 'wrap', marginTop: SPACE[3], marginLeft: GLYPH_OFFSET }}>
            <span className="chip" style={{ ...dataTextStyle, fontSize: TYPE.secondary }}>{formatNumber(live.qps)} QPS</span>
            <span className="chip" style={{ ...dataTextStyle, fontSize: TYPE.secondary }}>
              {live.p99 > 0 ? formatMs(live.p99) : '-'} p99
            </span>
            <span className="chip" style={{ ...dataTextStyle, fontSize: TYPE.secondary }}>{formatNumber(live.queue)} queue</span>
            <span className="chip" style={{ ...dataTextStyle, fontSize: TYPE.secondary }}>
              {live.rss != null ? `${Math.round(live.rss)} MB` : '-'} RSS
            </span>
            <span className="chip" style={{ ...dataTextStyle, fontSize: TYPE.secondary }}>{live.streams} streams</span>
          </div>
        )}

        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: SPACE[3],
            marginTop: SPACE[4],
            borderTop: `1px solid ${neutrals.border}`,
            paddingTop: SPACE[3],
          }}
        >
          <span
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: SPACE[5],
              flexWrap: 'wrap',
              fontSize: TYPE.secondary,
              color: neutrals.textSecondary,
            }}
          >
            <span>
              <BranchesOutlined aria-hidden style={{ marginRight: SPACE[1] }} />
              <span style={dataTextStyle}>{merged.versions.length}</span>{' '}
              {t('models.versionLabel', { count: merged.versions.length })}
            </span>
            <span>
              <ApiOutlined aria-hidden style={{ marginRight: SPACE[1] }} />
              <span style={dataTextStyle}>
                {workersReady}/{workersTotal}
              </span>{' '}
              {t('models.workerLabel', { count: workersTotal })}
            </span>
            {active && (
              <span>
                <AimOutlined aria-hidden style={{ marginRight: SPACE[1] }} />
                <span style={dataTextStyle}>{active.version}</span> {t('common.active')}
              </span>
            )}
          </span>
          <Button
            type="text"
            size="small"
            aria-expanded={expanded}
            aria-label={t('models.expandVersions')}
            icon={<DownOutlined aria-hidden className={expanded ? 'chevron open' : 'chevron'} />}
            onClick={() => setExpanded((v) => !v)}
          />
        </div>

        {expanded && (
          <div className="expand-in no-card-nav" style={{ marginTop: SPACE[3] }}>
            <Row gutter={[SPACE[3], SPACE[3]]}>
              {merged.versions.map((v) => (
                <Col xs={24} md={12} xl={8} key={v.version}>
                  <VersionCard
                    model={model.name}
                    version={v}
                    colorIndex={loaded.findIndex((lv) => lv.version === v.version)}
                    latest={latestByVersion[v.version]}
                    ops={can('operator')}
                  />
                </Col>
              ))}
            </Row>
          </div>
        )}
      </Card>
    </Reveal>
  );
}

export function ModelsPage() {
  const { t } = useTranslation();
  const { instanceId } = useInstance();
  const ilink = useInstanceLink();
  const instName = useInstanceName(instanceId);
  const can = useCanInstance();
  const merged = useMergedModels(instanceId);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [statusFilter, setStatusFilter] = useState<'all' | MergedModelStatus>('all');

  // One page-level timeline poll feeds the L2 chips and the expanded
  // version-card grid (plan §8) — no per-card timeline requests.
  const timelineQuery = useTimelineAll(instanceId, 10_000);
  const latestByModel = useMemo(() => {
    const map: Record<string, Record<string, TimelineEntry>> = {};
    for (const s of timelineQuery.data?.snapshots ?? []) {
      const entry = s.entries[s.entries.length - 1];
      if (entry) (map[s.model] ??= {})[s.version] = entry;
    }
    return map;
  }, [timelineQuery.data]);

  const counts = useMemo(() => {
    const c: Record<string, number> = { all: merged.data.length };
    for (const m of merged.data) c[m.status] = (c[m.status] ?? 0) + 1;
    return c;
  }, [merged.data]);

  const rows = useMemo(
    () => (statusFilter === 'all' ? merged.data : merged.data.filter((m) => m.status === statusFilter)),
    [merged.data, statusFilter],
  );
  const modelNames = useMemo(() => merged.data.map((m) => m.name), [merged.data]);

  return (
    <>
      <PageHeader
        title={t('models.title')}
        subtitle={instanceId}
        breadcrumb={
          instanceId
            ? [
                { title: instName ?? instanceId, href: ilink(`/instances/${encodeURIComponent(instanceId)}`) },
                { title: t('models.title') },
              ]
            : undefined
        }
        extra={
          can('operator') ? (
            <Button type="primary" icon={<UploadOutlined />} onClick={() => setUploadOpen(true)}>
              {t('upload.title')}
            </Button>
          ) : undefined
        }
      />
      <Segmented
        style={{ marginBottom: SPACE[5] }}
        value={statusFilter}
        onChange={(v) => setStatusFilter(v as typeof statusFilter)}
        options={[
          { label: `${t('models.filters.all')} ${counts.all ?? 0}`, value: 'all' },
          ...STATUS_FILTERS.map((s) => ({
            label: `${t(`models.filters.${s}`)} ${counts[s] ?? 0}`,
            value: s,
          })),
        ]}
      />
      {rows.length > 0 && rows.every((m) => m.status === 'unloaded') && (
        <Typography.Text
          type="secondary"
          style={{ display: 'block', fontSize: TYPE.secondary, marginBottom: SPACE[3] }}
        >
          {t('models.noneLoaded', { count: rows.length })}
        </Typography.Text>
      )}
      {!merged.isLoading && rows.length === 0 ? (
        <Card>
          <Empty description={t('models.emptyGuide')}>
            {can('operator') && (
              <Button type="primary" icon={<UploadOutlined />} onClick={() => setUploadOpen(true)}>
                {t('upload.title')}
              </Button>
            )}
          </Empty>
        </Card>
      ) : (
        rows.map((m, idx) => (
          <ModelCard key={m.name} model={m} order={idx + 1} latestByVersion={latestByModel[m.name] ?? {}} />
        ))
      )}
      <UploadDrawer open={uploadOpen} onClose={() => setUploadOpen(false)} existingModels={modelNames} />
    </>
  );
}
