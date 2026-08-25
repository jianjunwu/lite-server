import { useMemo, useState } from 'react';
import { Button, Card, Checkbox, Dropdown, Empty, Input, Modal, Popconfirm, Segmented, Tooltip, Typography } from 'antd';
import { MoreOutlined, UploadOutlined, WarningOutlined } from '@ant-design/icons';
import { Link, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { App } from 'antd';
import { useInstance } from '../context/InstanceContext';
import { useInstanceLink } from '../context/useInstanceLink';
import { useCanInstance } from '../context/useEffectiveRole';
import { useMergedModels, useMergedVersions } from '../api/hooks';
import { modelOps, withAdminKeyRetry } from '../api/mutations';
import type { MergedModel, MergedModelStatus } from '../api/merge';
import { StatusBadge } from '../components/StatusBadge';
import { VersionsTable } from '../components/VersionsTable';
import { PageHeader } from '../components/PageHeader';
import { Reveal } from '../components/PageHero';
import { TrafficRiver } from '../components/TrafficRiver';
import { UploadDrawer } from '../components/UploadDrawer';
import { STATUS_COLORS, TYPE, MONO_FONT } from '../theme';
import { SPACE } from '../tokens';
import { useNeutrals } from '../context/ThemeModeContext';

const STATUS_FILTERS: MergedModelStatus[] = ['ready', 'loading', 'degraded', 'unloaded'];

/** Row-level action for an unloaded model: single repo version loads inline,
 * multi-version models jump to the detail page for version choice. */
function LoadAction({ model }: { model: MergedModel }) {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { instanceId } = useInstance();
  const navigate = useNavigate();
  const ilink = useInstanceLink();
  const queryClient = useQueryClient();
  const [busy, setBusy] = useState(false);

  if (model.repoVersions.length !== 1) {
    return (
      <Button type="text" size="small" onClick={() => navigate(ilink(`/models/${encodeURIComponent(model.name)}`))}>
        {t('ops.load')}
      </Button>
    );
  }
  const version = model.repoVersions[0];
  return (
    <Popconfirm
      title={t('ops.loadConfirm', { version })}
      onConfirm={async () => {
        if (!instanceId) return;
        setBusy(true);
        try {
          await withAdminKeyRetry(instanceId, () => modelOps.loadVersion(instanceId, model.name, version));
          message.success(t('ops.loadRequested', { version }));
          await queryClient.invalidateQueries({ queryKey: [instanceId] });
        } catch (err) {
          message.error(err instanceof Error ? err.message : String(err));
        } finally {
          setBusy(false);
        }
      }}
    >
      <Button type="text" size="small" loading={busy}>
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
          onClick: () => setOpen(true),
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
 * One model = one card (plan §4.2): name + status statement, the traffic
 * river full-width as the card's main visual, versions table beneath.
 */
function ModelCard({ model, order }: { model: MergedModel; order: number }) {
  const { t } = useTranslation();
  const { instanceId } = useInstance();
  const can = useCanInstance();
  const ilink = useInstanceLink();
  const navigate = useNavigate();
  const neutrals = useNeutrals();
  const merged = useMergedVersions(instanceId, model.name);

  const loaded = merged.versions.filter((v) => v.loaded);
  const active = loaded.find((v) => v.active);
  const statement = !merged.hasLoaded
    ? t('models.stmtUnloaded')
    : active
      ? t('models.stmtServing', { version: active.version, weight: active.weight })
      : t('models.stmtNoActive');

  return (
    <Reveal order={order}>
      <Card className="lift" style={{ marginBottom: SPACE[5] }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: SPACE[3] }}>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: SPACE[2], minWidth: 0 }}>
            <Link
              to={ilink(`/models/${encodeURIComponent(model.name)}`)}
              style={{ fontSize: TYPE.cardTitle, fontWeight: 600, letterSpacing: '-0.01em' }}
            >
              {model.name}
            </Link>
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

        <div style={{ fontSize: TYPE.lead, color: neutrals.textSecondary, marginTop: SPACE[1] }}>{statement}</div>

        {loaded.length > 0 && (
          <div style={{ marginTop: SPACE[4] }}>
            <TrafficRiver
              versions={loaded}
              height={16}
              onSelect={(v) =>
                navigate(ilink(`/models/${encodeURIComponent(model.name)}/versions/${encodeURIComponent(v)}`))
              }
            />
          </div>
        )}

        <div style={{ marginTop: SPACE[4], borderTop: `1px solid ${neutrals.border}`, paddingTop: SPACE[3] }}>
          <VersionsTable
            model={model.name}
            versions={merged.versions}
            loading={merged.isLoading}
            ops={can('operator')}
          />
        </div>
      </Card>
    </Reveal>
  );
}

export function ModelsPage() {
  const { t } = useTranslation();
  const { instanceId } = useInstance();
  const can = useCanInstance();
  const merged = useMergedModels(instanceId);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [statusFilter, setStatusFilter] = useState<'all' | MergedModelStatus>('all');

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
        rows.map((m, idx) => <ModelCard key={m.name} model={m} order={idx + 1} />)
      )}
      <UploadDrawer open={uploadOpen} onClose={() => setUploadOpen(false)} existingModels={modelNames} />
    </>
  );
}
