import { useState } from 'react';
import { App, Button, Col, Empty, Row, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useQueries, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { apiFetch, bffFetch } from '../api/client';
import { useInstances } from '../api/hooks';
import type { HealthSummary, InstanceInfo, ServerInfo } from '../api/types';
import { InstanceForm } from '../components/InstanceForm';
import { InstanceCard } from '../components/InstanceCard';
import { PageHeader } from '../components/PageHeader';
import { useAuth } from '../context/AuthContext';
import { SPACE } from '../tokens';

// Poll healthy instances briskly; back off when one is down so a dead
// backend does not spam a failed request every 10s (same rhythm as overview).
const pollMs = (query: { state: { status: string } }) => (query.state.status === 'error' ? 30_000 : 10_000);

/** Instances browse layer — "which instances do I have, in what state?"
 * (plan §3.1). Card grid + create/edit/delete + empty-state CTA. */
export function InstancesPage() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { can } = useAuth();
  const queryClient = useQueryClient();
  const instancesQuery = useInstances();
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<InstanceInfo | null>(null);

  const instances = instancesQuery.data?.instances ?? [];

  const healthQueries = useQueries({
    queries: instances.map((i) => ({
      queryKey: [i.id, 'health'],
      queryFn: () => apiFetch<HealthSummary>(i.id, '/health'),
      retry: 0,
      refetchInterval: pollMs,
    })),
  });
  const infoQueries = useQueries({
    queries: instances.map((i) => ({
      queryKey: [i.id, 'info'],
      queryFn: () => apiFetch<ServerInfo>(i.id, '/info'),
      retry: 0,
    })),
  });

  const refresh = () => queryClient.invalidateQueries({ queryKey: ['bff', 'instances'] });

  const remove = async (inst: InstanceInfo) => {
    try {
      await bffFetch(`/api/instances/${encodeURIComponent(inst.id)}`, { method: 'DELETE' });
      message.success(t('settings.instances.deleted'));
      await refresh();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

  const openAdd = () => {
    setEditing(null);
    setFormOpen(true);
  };
  const openEdit = (inst: InstanceInfo) => {
    setEditing(inst);
    setFormOpen(true);
  };
  const adminActions = (inst: InstanceInfo) =>
    can('admin') && !inst.readonly
      ? { onEdit: () => openEdit(inst), onDelete: () => void remove(inst) }
      : {};

  return (
    <div>
      <PageHeader
        title={t('nav.instances')}
        subtitle={instances.length > 0 ? t('instances.subtitle', { count: instances.length }) : undefined}
        extra={
          can('admin') ? (
            <Button type="primary" icon={<PlusOutlined />} onClick={openAdd}>
              {t('settings.instances.add')}
            </Button>
          ) : undefined
        }
      />

      {!instancesQuery.isLoading && instances.length === 0 ? (
        // Empty state is an action invitation, same button name as the page's.
        <Empty
          description={
            <>
              <Typography.Title level={4}>{t('instances.empty')}</Typography.Title>
              <Typography.Text type="secondary">{t('instances.emptyHint')}</Typography.Text>
              {can('admin') && (
                <div style={{ marginTop: SPACE[4] }}>
                  <Button type="primary" icon={<PlusOutlined />} onClick={openAdd}>
                    {t('settings.instances.add')}
                  </Button>
                </div>
              )}
            </>
          }
        />
      ) : (
        <Row gutter={[SPACE[5], SPACE[5]]}>
          {instances.map((inst, idx) => (
            <Col xs={24} sm={12} xl={8} key={inst.id}>
              <InstanceCard
                inst={inst}
                health={healthQueries[idx].data}
                healthLoading={healthQueries[idx].isLoading}
                unreachable={healthQueries[idx].isError}
                info={infoQueries[idx].data}
                {...adminActions(inst)}
              />
            </Col>
          ))}
        </Row>
      )}

      <InstanceForm
        open={formOpen}
        editing={editing}
        onClose={() => setFormOpen(false)}
        onSaved={refresh}
      />
    </div>
  );
}
