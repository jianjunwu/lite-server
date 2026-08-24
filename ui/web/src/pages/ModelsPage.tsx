import { useMemo, useState } from 'react';
import { Card, Empty, Table, Tag, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useModels, useVersions } from '../api/hooks';
import type { ModelListItem, VersionInfo } from '../api/types';
import { StatusBadge } from '../components/StatusBadge';
import { formatAge } from '../components/format';

function VersionsSubTable({ model }: { model: string }) {
  const { t } = useTranslation();
  const { instanceId } = useInstance();
  const versionsQuery = useVersions(instanceId, model);
  const versions = versionsQuery.data?.versions ?? [];

  return (
    <Table<VersionInfo>
      size="small"
      rowKey="version"
      loading={versionsQuery.isLoading}
      dataSource={versions}
      pagination={false}
      columns={[
        {
          title: t('common.version'),
          dataIndex: 'version',
          render: (v: string) => (
            <Link to={`/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(v)}`}>{v}</Link>
          ),
        },
        { title: t('common.status'), dataIndex: 'status', render: (s: string) => <StatusBadge status={s} /> },
        {
          title: t('common.active'),
          dataIndex: 'active',
          width: 90,
          render: (a: boolean) => (a ? <Tag color="#4F46E5">{t('common.active')}</Tag> : null),
        },
        { title: t('common.weight'), dataIndex: 'weight', width: 90 },
        {
          title: t('common.workers'),
          width: 110,
          render: (_: unknown, v: VersionInfo) => `${v.workers.ready}/${v.workers.total}`,
        },
        {
          title: t('common.loadedAt'),
          dataIndex: 'loaded_at',
          render: (ts: number | null) => (ts ? formatAge(ts) : '-'),
        },
      ]}
    />
  );
}

export function ModelsPage() {
  const { t } = useTranslation();
  const { instanceId } = useInstance();
  const modelsQuery = useModels(instanceId);
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);

  const rows = useMemo(() => modelsQuery.data?.models ?? [], [modelsQuery.data]);

  if (!modelsQuery.isLoading && rows.length === 0) {
    return (
      <Card>
        <Empty description={t('models.noModels')} />
      </Card>
    );
  }

  return (
    <Card size="small">
      <Table<ModelListItem>
        rowKey={(r) => `${r.name}/${r.version}`}
        loading={modelsQuery.isLoading}
        dataSource={rows}
        pagination={false}
        expandable={{
          expandedRowKeys: expandedKeys,
          onExpandedRowsChange: (keys) => setExpandedKeys(keys as string[]),
          expandedRowRender: (record) => <VersionsSubTable model={record.name} />,
        }}
        columns={[
          {
            title: t('models.name'),
            dataIndex: 'name',
            render: (name: string) => <Link to={`/models/${encodeURIComponent(name)}`}>{name}</Link>,
          },
          {
            title: t('common.version'),
            dataIndex: 'version',
            render: (v: string, r) => (
              <Link to={`/models/${encodeURIComponent(r.name)}/versions/${encodeURIComponent(v)}`}>{v}</Link>
            ),
          },
          { title: t('models.modelType'), dataIndex: 'model_type', width: 140 },
          { title: t('common.status'), dataIndex: 'status', width: 130, render: (s: string) => <StatusBadge status={s} /> },
          { title: t('common.workers'), dataIndex: 'workers', width: 100 },
        ]}
      />
      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
        {instanceId}
      </Typography.Text>
    </Card>
  );
}
