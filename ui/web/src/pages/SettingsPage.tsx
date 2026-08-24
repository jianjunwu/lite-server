import { useState } from 'react';
import { App, Button, Card, Checkbox, Drawer, Form, Input, Popconfirm, Select, Table, Tabs, Tag, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { bffFetch, setAdminKey } from '../api/client';
import { useInstances } from '../api/hooks';
import { useAuth } from '../context/AuthContext';
import type { InstanceInfo } from '../api/types';
import { PageHeader } from '../components/PageHeader';
import { MONO_FONT, dataTextStyle, TYPE } from '../theme';

interface InstanceFormValues {
  id: string;
  name: string;
  base_url: string;
  admin_key?: string;
  probe?: boolean;
}

function InstancesTab() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { can } = useAuth();
  const queryClient = useQueryClient();
  const instancesQuery = useInstances();
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editing, setEditing] = useState<InstanceInfo | null>(null);
  const [form] = Form.useForm<InstanceFormValues>();
  const [busy, setBusy] = useState(false);

  const instances = instancesQuery.data?.instances ?? [];

  const openCreate = () => {
    setEditing(null);
    form.resetFields();
    setDrawerOpen(true);
  };

  const openEdit = (inst: InstanceInfo) => {
    setEditing(inst);
    form.setFieldsValue({ id: inst.id, name: inst.name, base_url: inst.base_url, admin_key: '', probe: false });
    setDrawerOpen(true);
  };

  const submit = async (values: InstanceFormValues) => {
    setBusy(true);
    try {
      if (editing) {
        const patch: Record<string, unknown> = { name: values.name, base_url: values.base_url };
        // Empty key field on edit = keep the stored key; explicit space-free
        // non-empty value replaces it.
        if (values.admin_key && values.admin_key.trim().length > 0) {
          patch.admin_key = values.admin_key.trim();
        }
        await bffFetch(`/api/instances/${encodeURIComponent(editing.id)}`, {
          method: 'PUT',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(patch),
        });
        message.success(t('settings.instances.updated'));
      } else {
        const probe = values.probe ? '?probe=true' : '';
        await bffFetch(`/api/instances${probe}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            id: values.id.trim(),
            name: values.name.trim(),
            base_url: values.base_url.trim(),
            ...(values.admin_key?.trim() ? { admin_key: values.admin_key.trim() } : {}),
          }),
        });
        message.success(t('settings.instances.created'));
      }
      await queryClient.invalidateQueries({ queryKey: ['bff', 'instances'] });
      setDrawerOpen(false);
      form.resetFields();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const remove = async (inst: InstanceInfo) => {
    try {
      await bffFetch(`/api/instances/${encodeURIComponent(inst.id)}`, { method: 'DELETE' });
      message.success(t('settings.instances.deleted'));
      await queryClient.invalidateQueries({ queryKey: ['bff', 'instances'] });
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 12 }}>
        {can('admin') && (
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
            {t('settings.instances.add')}
          </Button>
        )}
      </div>
      <Table<InstanceInfo>
        size="small"
        rowKey="id"
        loading={instancesQuery.isLoading}
        dataSource={instances}
        pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'id', width: 130, render: (v: string) => <span style={dataTextStyle}>{v}</span> },
          { title: t('settings.instances.name'), dataIndex: 'name' },
          { title: 'URL', dataIndex: 'base_url', render: (v: string) => <span style={dataTextStyle}>{v}</span> },
          {
            title: t('settings.instances.adminKey'),
            width: 110,
            render: (_: unknown, i: InstanceInfo) =>
              i.has_admin_key ? <Tag color="#16A34A" style={{ border: 'none', color: '#fff' }}>server</Tag> : '-',
          },
          {
            title: '',
            width: 160,
            render: (_: unknown, i: InstanceInfo) =>
              i.readonly ? (
                <Typography.Text type="secondary" style={{ fontSize: TYPE.secondary }}>env · readonly</Typography.Text>
              ) : can('admin') ? (
                <span>
                  <Button type="text" size="small" onClick={() => openEdit(i)}>
                    {t('settings.instances.edit')}
                  </Button>
                  <Popconfirm title={t('settings.instances.deleteConfirm', { id: i.id })} onConfirm={() => remove(i)}>
                    <Button type="text" size="small" danger>
                      {t('settings.instances.delete')}
                    </Button>
                  </Popconfirm>
                </span>
              ) : null,
          },
        ]}
      />

      <Drawer
        title={editing ? t('settings.instances.editTitle', { id: editing.id }) : t('settings.instances.addTitle')}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={380}
      >
        <Form form={form} layout="vertical" onFinish={submit} initialValues={{ probe: true }}>
          <Form.Item
            name="id"
            label="ID"
            rules={[{ required: true, pattern: /^[a-z0-9][a-z0-9-]*$/, message: 'a-z, 0-9, -' }]}
          >
            <Input disabled={editing !== null} style={{ fontFamily: MONO_FONT }} placeholder="prod-gpu" />
          </Form.Item>
          <Form.Item name="name" label={t('settings.instances.name')} rules={[{ required: true }]}>
            <Input placeholder="Prod GPU cluster" />
          </Form.Item>
          <Form.Item name="base_url" label="URL" rules={[{ required: true, type: 'url' }]}>
            <Input style={{ fontFamily: MONO_FONT }} placeholder="http://10.0.0.11:8000" />
          </Form.Item>
          <Form.Item
            name="admin_key"
            label={t('settings.instances.adminKey')}
            extra={editing ? t('settings.instances.adminKeyKeepHint') : t('settings.instances.adminKeyHint')}
          >
            <Input.Password style={{ fontFamily: MONO_FONT }} autoComplete="new-password" />
          </Form.Item>
          {!editing && (
            <Form.Item name="probe" valuePropName="checked">
              <Checkbox>{t('settings.instances.probe')}</Checkbox>
            </Form.Item>
          )}
          <Button type="primary" htmlType="submit" block loading={busy}>
            {editing ? t('settings.instances.save') : t('settings.instances.add')}
          </Button>
        </Form>
      </Drawer>
    </>
  );
}

function AdminKeysTab() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const instancesQuery = useInstances();
  const instances = instancesQuery.data?.instances ?? [];
  const [values, setValues] = useState<Record<string, string>>({});

  return (
    <Card size="small">
      <Typography.Paragraph type="secondary" style={{ fontSize: TYPE.secondary }}>
        {t('settings.keys.hint')}
      </Typography.Paragraph>
      <Table<InstanceInfo>
        size="small"
        rowKey="id"
        dataSource={instances}
        pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'id', width: 140, render: (v: string) => <span style={dataTextStyle}>{v}</span> },
          {
            title: t('settings.keys.browserKey'),
            render: (_: unknown, i: InstanceInfo) => (
              <Input.Password
                size="small"
                style={{ maxWidth: 260, fontFamily: MONO_FONT }}
                placeholder={sessionStorage.getItem(`lite-ui-admin-key:${i.id}`) ? '••••••••' : 'x-admin-key'}
                value={values[i.id] ?? ''}
                onChange={(e) => setValues((prev) => ({ ...prev, [i.id]: e.target.value }))}
              />
            ),
          },
          {
            title: '',
            width: 180,
            render: (_: unknown, i: InstanceInfo) => (
              <span>
                <Button
                  type="text"
                  size="small"
                  disabled={!values[i.id]}
                  onClick={() => {
                    setAdminKey(i.id, values[i.id].trim());
                    setValues((prev) => ({ ...prev, [i.id]: '' }));
                    message.success(t('settings.keys.saved', { id: i.id }));
                  }}
                >
                  {t('settings.keys.save')}
                </Button>
                <Button
                  type="text"
                  size="small"
                  danger
                  onClick={() => {
                    setAdminKey(i.id, null);
                    message.success(t('settings.keys.cleared', { id: i.id }));
                  }}
                >
                  {t('settings.keys.clear')}
                </Button>
              </span>
            ),
          },
        ]}
      />
    </Card>
  );
}

interface UserRow {
  username: string;
  role: 'viewer' | 'operator' | 'admin';
  createdAt: string;
  mustChangePassword: boolean;
}

function UsersTab() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { user: me } = useAuth();
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editing, setEditing] = useState<UserRow | null>(null);
  const [form] = Form.useForm<{ username: string; password: string; role: string }>();
  const [busy, setBusy] = useState(false);

  const usersQuery = useQuery({
    queryKey: ['bff', 'users'],
    queryFn: () => bffFetch<{ users: UserRow[] }>('/api/users'),
  });
  const users = usersQuery.data?.users ?? [];

  const openCreate = () => {
    setEditing(null);
    form.resetFields();
    setDrawerOpen(true);
  };

  const openEdit = (u: UserRow) => {
    setEditing(u);
    form.setFieldsValue({ username: u.username, password: '', role: u.role });
    setDrawerOpen(true);
  };

  const submit = async (values: { username: string; password: string; role: string }) => {
    setBusy(true);
    try {
      if (editing) {
        const patch: Record<string, unknown> = { role: values.role };
        if (values.password && values.password.length >= 8) patch.password = values.password;
        await bffFetch(`/api/users/${encodeURIComponent(editing.username)}`, {
          method: 'PUT',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(patch),
        });
        message.success(t('settings.users.updated'));
      } else {
        await bffFetch('/api/users', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ username: values.username.trim(), password: values.password, role: values.role }),
        });
        message.success(t('settings.users.created'));
      }
      await usersQuery.refetch();
      setDrawerOpen(false);
      form.resetFields();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const remove = async (u: UserRow) => {
    try {
      await bffFetch(`/api/users/${encodeURIComponent(u.username)}`, { method: 'DELETE' });
      message.success(t('settings.users.deleted'));
      await usersQuery.refetch();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 12 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
          {t('settings.users.add')}
        </Button>
      </div>
      <Table<UserRow>
        size="small"
        rowKey="username"
        loading={usersQuery.isLoading}
        dataSource={users}
        pagination={false}
        columns={[
          { title: t('settings.users.username'), dataIndex: 'username', render: (v: string) => <span style={dataTextStyle}>{v}</span> },
          {
            title: t('settings.users.role'),
            dataIndex: 'role',
            width: 110,
            render: (r: string) => <Tag>{r}</Tag>,
          },
          {
            title: t('settings.users.status'),
            width: 160,
            render: (_: unknown, u: UserRow) =>
              u.mustChangePassword ? (
                <Typography.Text type="warning" style={{ fontSize: TYPE.secondary }}>
                  {t('settings.users.mustChange')}
                </Typography.Text>
              ) : null,
          },
          {
            title: '',
            width: 150,
            render: (_: unknown, u: UserRow) => (
              <span>
                <Button type="text" size="small" onClick={() => openEdit(u)}>
                  {t('settings.users.edit')}
                </Button>
                {u.username !== me?.username && (
                  <Popconfirm title={t('settings.users.deleteConfirm', { name: u.username })} onConfirm={() => remove(u)}>
                    <Button type="text" size="small" danger>
                      {t('settings.users.delete')}
                    </Button>
                  </Popconfirm>
                )}
              </span>
            ),
          },
        ]}
      />

      <Drawer
        title={editing ? t('settings.users.editTitle', { name: editing.username }) : t('settings.users.addTitle')}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={360}
      >
        <Form form={form} layout="vertical" onFinish={submit} initialValues={{ role: 'viewer' }} requiredMark={false}>
          <Form.Item
            name="username"
            label={t('settings.users.username')}
            rules={[{ required: true, pattern: /^[a-zA-Z0-9_.-]{2,32}$/ }]}
          >
            <Input disabled={editing !== null} style={{ fontFamily: MONO_FONT }} />
          </Form.Item>
          <Form.Item
            name="password"
            label={t('settings.users.password')}
            extra={editing ? t('settings.users.passwordKeepHint') : t('settings.users.passwordHint')}
            rules={editing ? [] : [{ required: true, min: 8 }]}
          >
            <Input.Password autoComplete="new-password" style={{ fontFamily: MONO_FONT }} />
          </Form.Item>
          <Form.Item name="role" label={t('settings.users.role')} rules={[{ required: true }]}>
            <Select
              options={[
                { value: 'viewer', label: 'viewer' },
                { value: 'operator', label: 'operator' },
                { value: 'admin', label: 'admin' },
              ]}
            />
          </Form.Item>
          <Button type="primary" htmlType="submit" block loading={busy}>
            {editing ? t('settings.instances.save') : t('settings.users.add')}
          </Button>
        </Form>
      </Drawer>
    </>
  );
}

export function SettingsPage() {
  const { t } = useTranslation();
  const { can } = useAuth();
  const tabs = [
    { key: 'instances', label: t('settings.tabs.instances'), children: <InstancesTab /> },
    { key: 'keys', label: t('settings.tabs.keys'), children: <AdminKeysTab /> },
  ];
  if (can('admin')) {
    tabs.push({ key: 'users', label: t('settings.tabs.users'), children: <UsersTab /> });
  }
  return (
    <>
      <PageHeader title={t('nav.settings')} />
      <Tabs items={tabs} />
    </>
  );
}
