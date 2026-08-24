import { useMemo, useState } from 'react';
import { Button, Card, Empty, Table } from 'antd';
import { UploadOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useModels, useVersions } from '../api/hooks';
import type { ModelListItem } from '../api/types';
import { StatusBadge } from '../components/StatusBadge';
import { VersionsTable } from '../components/VersionsTable';
import { PageHeader } from '../components/PageHeader';
import { UploadDrawer } from '../components/UploadDrawer';
import { dataTextStyle } from '../theme';

function ModelExpandRow({ model }: { model: string }) {
  const { instanceId } = useInstance();
  const versionsQuery = useVersions(instanceId, model);
  return (
    <VersionsTable model={model} versions={versionsQuery.data?.versions ?? []} loading={versionsQuery.isLoading} ops />
  );
}

export function ModelsPage() {
  const { t } = useTranslation();
  const { instanceId } = useInstance();
  const modelsQuery = useModels(instanceId);
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
  const [uploadOpen, setUploadOpen] = useState(false);

  const rows = useMemo(() => modelsQuery.data?.models ?? [], [modelsQuery.data]);
  const modelNames = useMemo(() => [...new Set(rows.map((m) => m.name))], [rows]);

  return (
    <>
      <PageHeader
        title={t('models.title')}
        subtitle={instanceId}
        extra={
          <Button type="primary" icon={<UploadOutlined />} onClick={() => setUploadOpen(true)}>
            {t('upload.title')}
          </Button>
        }
      />
      <Card size="small">
        <Table<ModelListItem>
          rowKey={(r) => `${r.name}/${r.version}`}
          loading={modelsQuery.isLoading}
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
              render: (name: string) => <Link to={`/models/${encodeURIComponent(name)}`}>{name}</Link>,
            },
            {
              title: t('common.version'),
              dataIndex: 'version',
              width: 120,
              render: (v: string, r) => (
                <Link to={`/models/${encodeURIComponent(r.name)}/versions/${encodeURIComponent(v)}`} style={dataTextStyle}>
                  {v}
                </Link>
              ),
            },
            { title: t('models.modelType'), dataIndex: 'model_type', width: 140 },
            { title: t('common.status'), dataIndex: 'status', width: 140, render: (s: string) => <StatusBadge status={s} /> },
            {
              title: t('common.workers'),
              dataIndex: 'workers',
              width: 100,
              render: (w: number) => <span style={dataTextStyle}>{w}</span>,
            },
          ]}
        />
      </Card>
      <UploadDrawer open={uploadOpen} onClose={() => setUploadOpen(false)} existingModels={modelNames} />
    </>
  );
}
