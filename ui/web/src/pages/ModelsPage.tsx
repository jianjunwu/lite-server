import { useMemo, useState } from 'react';
import { Button, Card, Empty, Popconfirm, Segmented, Table } from 'antd';
import { UploadOutlined } from '@ant-design/icons';
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
import { dataTextStyle } from '../theme';

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
        <Table<MergedModel>
          rowKey="name"
          loading={merged.isLoading}
          dataSource={rows}
          pagination={false}
          locale={{ emptyText: <Empty description={t('models.noModels')} /> }}
          expandable={{
            expandedRowKeys: expandedKeys,
            onExpandedRowsChange: (keys) => setExpandedKeys(keys as string[]),
            expandedRowRender: (record) => <ModelExpandRow model={record.name} />,
          }}
          columns={[
            {
              title: t('models.name'),
              dataIndex: 'name',
              render: (name: string) => (
                <Link to={ilink(`/models/${encodeURIComponent(name)}`)}>{name}</Link>
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
                    width: 110,
                    render: (_: unknown, m: MergedModel) =>
                      m.status === 'unloaded' && m.repoVersions.length > 0 ? <LoadAction model={m} /> : null,
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
