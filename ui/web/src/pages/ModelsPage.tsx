import { useMemo, useState } from 'react';
import { Button, Card, Checkbox, Dropdown, Empty, Input, Modal, Popconfirm, Segmented, Table, Tooltip, Typography } from 'antd';
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
import { UploadDrawer } from '../components/UploadDrawer';
import { dataTextStyle, MONO_FONT, STATUS_COLORS, TYPE } from '../theme';

const STATUS_FILTERS: MergedModelStatus[] = ['ready', 'loading', 'degraded', 'unloaded'];

function ModelExpandRow({ model }: { model: string }) {
  const { instanceId } = useInstance();
  const can = useCanInstance();
  const merged = useMergedVersions(instanceId, model);
  return (
    <VersionsTable
      model={model}
      versions={merged.versions}
      loading={merged.isLoading}
      ops={can('operator')}
    />
  );
}

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
        <p style={{ fontSize: 13 }}>{t('ops.deleteModelBody', { model: model.name })}</p>
        <Input
          value={confirmText}
          onChange={(e) => setConfirmText(e.target.value)}
          placeholder={model.name}
          style={{ fontFamily: MONO_FONT }}
        />
        <Checkbox checked={force} onChange={(e) => setForce(e.target.checked)} style={{ marginTop: 12 }}>
          {t('ops.forceDeleteLoaded')}
        </Checkbox>
      </Modal>
    </>
  );
}

export function ModelsPage() {
  const { t } = useTranslation();
  const { instanceId } = useInstance();
  const can = useCanInstance();
  const ilink = useInstanceLink();
  const merged = useMergedModels(instanceId);
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
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
      <Card size="small">
        <Segmented
          style={{ marginBottom: 12 }}
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
            style={{ display: 'block', fontSize: TYPE.secondary, marginBottom: 8 }}
          >
            {t('models.noneLoaded', { count: rows.length })}
          </Typography.Text>
        )}
        <Table<MergedModel>
          rowKey="name"
          loading={merged.isLoading}
          dataSource={rows}
          pagination={false}
          locale={{
            emptyText: (
              <Empty description={t('models.emptyGuide')}>
                {can('operator') && (
                  <Button type="primary" icon={<UploadOutlined />} onClick={() => setUploadOpen(true)}>
                    {t('upload.title')}
                  </Button>
                )}
              </Empty>
            ),
          }}
          expandable={{
            expandedRowKeys: expandedKeys,
            onExpandedRowsChange: (keys) => setExpandedKeys(keys as string[]),
            expandedRowRender: (record) => <ModelExpandRow model={record.name} />,
          }}
          columns={[
            {
              title: t('models.name'),
              dataIndex: 'name',
              render: (name: string, m: MergedModel) => (
                <span>
                  <Link to={ilink(`/models/${encodeURIComponent(name)}`)}>{name}</Link>
                  {m.drifted && (
                    <Tooltip title={t('models.drifted')}>
                      <WarningOutlined
                        aria-label="drift warning"
                        style={{ color: STATUS_COLORS.warning, marginLeft: 8 }}
                      />
                    </Tooltip>
                  )}
                </span>
              ),
            },
            {
              title: t('common.status'),
              dataIndex: 'status',
              width: 140,
              render: (s: MergedModelStatus) => (
                <StatusBadge status={s} text={t(`models.filters.${s}`)} />
              ),
            },
            {
              title: t('models.versions'),
              dataIndex: 'versionCount',
              width: 110,
              render: (n: number) => <span style={dataTextStyle}>{n}</span>,
            },
            { title: t('models.modelType'), dataIndex: 'modelType', width: 140 },
            {
              title: t('common.workers'),
              dataIndex: 'workers',
              width: 100,
              render: (w: number) => <span style={dataTextStyle}>{w}</span>,
            },
            ...(can('operator')
              ? [
                  {
                    title: t('ops.actions'),
                    key: 'actions',
                    width: 130,
                    render: (_: unknown, m: MergedModel) => (
                      <span style={{ display: 'inline-flex', gap: 4 }}>
                        {m.status === 'unloaded' && m.repoVersions.length > 0 ? <LoadAction model={m} /> : null}
                        <DeleteModelAction model={m} />
                      </span>
                    ),
                  },
                ]
              : []),
          ]}
        />
      </Card>
      <UploadDrawer open={uploadOpen} onClose={() => setUploadOpen(false)} existingModels={modelNames} />
    </>
  );
}
