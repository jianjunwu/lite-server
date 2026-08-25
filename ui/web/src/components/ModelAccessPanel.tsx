import { useState } from 'react';
import { App, Button, Empty, Input, Select, Space, Table, Typography } from 'antd';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { modelGrantsApi, type ModelGrantUser } from '../api/auth';
import { dataTextStyle, MONO_FONT, TYPE } from '../theme';

/** Model-centric ACL panel (instance admin): which users hold per-model
 * grants on this model, with add/remove. Shares the same grant rows the
 * Settings > Users access drawer edits. */
export function ModelAccessPanel({ instanceId, model }: { instanceId: string; model: string }) {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const queryClient = useQueryClient();
  const [newUser, setNewUser] = useState('');
  const [newRole, setNewRole] = useState('viewer');

  const grantsQuery = useQuery({
    queryKey: ['bff', 'model-grants', 'by-model', instanceId, model],
    queryFn: () => modelGrantsApi.listForModel(instanceId, model),
  });
  const grants = grantsQuery.data?.grants ?? [];

  const setRole = async (username: string, role: string) => {
    try {
      await modelGrantsApi.set(username, instanceId, model, role);
      message.success(t('models.access.saved'));
      setNewUser('');
      await queryClient.invalidateQueries({ queryKey: ['bff', 'model-grants', 'by-model', instanceId, model] });
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <>
      <Typography.Paragraph type="secondary" style={{ fontSize: TYPE.secondary }}>
        {t('models.access.hint')}
      </Typography.Paragraph>
      <Table<ModelGrantUser>
        size="small"
        rowKey="username"
        pagination={false}
        loading={grantsQuery.isLoading}
        dataSource={grants}
        locale={{ emptyText: <Empty description={t('models.access.empty')} /> }}
        columns={[
          {
            title: t('settings.users.username'),
            dataIndex: 'username',
            render: (v: string) => <span style={dataTextStyle}>{v}</span>,
          },
          {
            title: t('settings.users.role'),
            dataIndex: 'role',
            width: 160,
            render: (_: string, g: ModelGrantUser) => (
              <Select
                style={{ width: 140 }}
                value={g.role}
                onChange={(role) => void setRole(g.username, role)}
                options={[
                  { value: 'viewer', label: 'viewer' },
                  { value: 'operator', label: 'operator' },
                ]}
              />
            ),
          },
          {
            title: '',
            width: 90,
            render: (_: unknown, g: ModelGrantUser) => (
              <Button type="text" size="small" danger onClick={() => void setRole(g.username, 'default')}>
                {t('settings.grants.remove')}
              </Button>
            ),
          },
        ]}
      />
      <Space.Compact block style={{ marginTop: 12, maxWidth: 480 }}>
        <Input
          value={newUser}
          onChange={(e) => setNewUser(e.target.value)}
          placeholder={t('models.access.userPlaceholder')}
          style={{ fontFamily: MONO_FONT }}
        />
        <Select
          style={{ width: 120 }}
          value={newRole}
          onChange={setNewRole}
          options={[
            { value: 'viewer', label: 'viewer' },
            { value: 'operator', label: 'operator' },
          ]}
        />
        <Button
          type="primary"
          disabled={!newUser.trim()}
          onClick={() => void setRole(newUser.trim(), newRole)}
        >
          {t('models.access.addUser')}
        </Button>
      </Space.Compact>
    </>
  );
}
