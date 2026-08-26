import { useState } from 'react';
import { App, Button, Card, Checkbox, Divider, Drawer, Form, Input, Modal, Popconfirm, Select, Space, Table, Tabs, Tag, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { QRCodeSVG } from 'qrcode.react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useSearchParams } from 'react-router-dom';
import { bffFetch, setAdminKey } from '../api/client';
import { auditApi, authApi, grantsApi, modelGrantsApi, invitesApi, sessionsApi, type AuditEntry, type InviteInfo, type SessionInfo } from '../api/auth';
import { useInstances } from '../api/hooks';
import { useAuth } from '../context/AuthContext';
import type { InstanceInfo } from '../api/types';
import { PageHeader } from '../components/PageHeader';
import { MONO_FONT, STATUS_COLORS, dataTextStyle, TYPE } from '../theme';

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
          { title: t('settings.instances.name'), dataIndex: 'name', ellipsis: true },
          { title: 'URL', dataIndex: 'base_url', ellipsis: true, render: (v: string) => <span style={dataTextStyle}>{v}</span> },
          {
            title: t('settings.instances.adminKey'),
            width: 110,
            render: (_: unknown, i: InstanceInfo) =>
              i.has_admin_key ? <Tag color="#16A34A" style={{ border: 'none', color: '#fff' }}>server</Tag> : '-',
          },
          {
            title: t('settings.instances.actions'),
            width: 160,
            align: 'right',
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
            rules={[
              { required: true, message: t('settings.instances.idRequired') },
              { pattern: /^[a-z0-9][a-z0-9-]*$/, message: t('settings.instances.idPattern') },
            ]}
          >
            <Input disabled={editing !== null} style={{ fontFamily: MONO_FONT }} placeholder="prod-gpu" />
          </Form.Item>
          <Form.Item
            name="name"
            label={t('settings.instances.name')}
            rules={[{ required: true, message: t('settings.instances.nameRequired') }]}
          >
            <Input placeholder="Prod GPU cluster" />
          </Form.Item>
          <Form.Item
            name="base_url"
            label="URL"
            rules={[
              { required: true, message: t('settings.instances.urlRequired') },
              { type: 'url', message: t('settings.instances.urlInvalid') },
            ]}
          >
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
  totpEnabled?: boolean;
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

  const [sessionsFor, setSessionsFor] = useState<string | null>(null);
  const [accessFor, setAccessFor] = useState<UserRow | null>(null);
  const userSessionsQuery = useQuery({
    queryKey: ['bff', 'user-sessions', sessionsFor],
    queryFn: () => sessionsApi.listFor(sessionsFor as string),
    enabled: sessionsFor !== null,
  });

  const kick = async (id: string) => {
    if (!sessionsFor) return;
    try {
      await sessionsApi.revokeFor(sessionsFor, id);
      message.success(t('settings.sessions.revoked'));
      await userSessionsQuery.refetch();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

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
        // Empty means "keep the current password"; the form enforces min 8
        // for non-empty values, so anything here is safe to send.
        if (values.password) patch.password = values.password;
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

  const resetTotp = async (u: UserRow) => {
    try {
      await authApi.adminResetTotp(u.username);
      message.success(t('settings.users.totpReset'));
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
            width: 290,
            render: (_: unknown, u: UserRow) => (
              <span>
                <Button type="text" size="small" onClick={() => setAccessFor(u)}>
                  {t('settings.grants.access')}
                </Button>
                <Button type="text" size="small" onClick={() => setSessionsFor(u.username)}>
                  {t('settings.sessions.view')}
                </Button>
                <Button type="text" size="small" onClick={() => openEdit(u)}>
                  {t('settings.users.edit')}
                </Button>
                {u.totpEnabled && (
                  <Popconfirm title={t('settings.users.totpResetConfirm', { name: u.username })} onConfirm={() => void resetTotp(u)}>
                    <Button type="text" size="small">
                      {t('settings.users.totpResetAction')}
                    </Button>
                  </Popconfirm>
                )}
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
            rules={[{ required: editing === null, min: 12 }]}
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

      <Drawer
        title={t('settings.sessions.userTitle', { name: sessionsFor })}
        open={sessionsFor !== null}
        onClose={() => setSessionsFor(null)}
        width={640}
      >
        <SessionsTable
          sessions={userSessionsQuery.data?.sessions ?? []}
          loading={userSessionsQuery.isLoading}
          onRevoke={(id) => void kick(id)}
        />
      </Drawer>

      {accessFor && <GrantsDrawer user={accessFor} onClose={() => setAccessFor(null)} />}
    </>
  );
}

/** Per-instance role overrides for one user: a row per visible instance,
 * saved immediately on change (role "default" removes the grant row). */
function GrantsDrawer({ user, onClose }: { user: UserRow; onClose: () => void }) {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const instancesQuery = useInstances();
  const instances = instancesQuery.data?.instances ?? [];
  const grantsQuery = useQuery({
    queryKey: ['bff', 'grants', user.username],
    queryFn: () => grantsApi.list(user.username),
  });
  const grants = new Map((grantsQuery.data?.grants ?? []).map((g) => [g.instance_id, g.role]));

  const [modelInst, setModelInst] = useState<string | null>(null);
  const [newModel, setNewModel] = useState('');
  const [newModelRole, setNewModelRole] = useState('viewer');
  const effectiveInst = modelInst ?? instances[0]?.id ?? null;
  const modelGrantsQuery = useQuery({
    queryKey: ['bff', 'model-grants', user.username],
    queryFn: () => modelGrantsApi.listForUser(user.username),
  });
  const modelGrants = (modelGrantsQuery.data?.grants ?? []).filter(
    (g) => g.instance_id === effectiveInst,
  );

  const setRole = async (instanceId: string, role: string) => {
    try {
      await grantsApi.set(user.username, instanceId, role);
      message.success(t('settings.grants.saved'));
      await grantsQuery.refetch();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

  const setModelRole = async (model: string, role: string) => {
    if (!effectiveInst) return;
    try {
      await modelGrantsApi.set(user.username, effectiveInst, model, role);
      message.success(t('settings.grants.saved'));
      setNewModel('');
      await modelGrantsQuery.refetch();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <Drawer
      title={t('settings.grants.title', { name: user.username })}
      open
      onClose={onClose}
      width={560}
    >
      <Typography.Paragraph type="secondary" style={{ fontSize: TYPE.secondary }}>
        {t('settings.grants.hint')}
      </Typography.Paragraph>
      <Table<InstanceInfo>
        size="small"
        rowKey="id"
        pagination={false}
        loading={instancesQuery.isLoading || grantsQuery.isLoading}
        dataSource={instances}
        columns={[
          {
            title: t('settings.instances.name'),
            render: (_: unknown, i: InstanceInfo) => (
              <span>
                {i.name} <span style={dataTextStyle}>({i.id})</span>
              </span>
            ),
          },
          {
            title: t('settings.users.role'),
            width: 220,
            render: (_: unknown, i: InstanceInfo) => (
              <Select
                style={{ width: 200 }}
                value={grants.get(i.id) ?? 'default'}
                onChange={(role) => void setRole(i.id, role)}
                options={[
                  { value: 'default', label: t('settings.grants.default', { role: user.role }) },
                  { value: 'viewer', label: 'viewer' },
                  { value: 'operator', label: 'operator' },
                  { value: 'admin', label: 'admin' },
                  { value: 'none', label: t('settings.grants.hidden') },
                ]}
              />
            ),
          },
        ]}
      />

      <Divider style={{ margin: '20px 0 12px' }}>{t('settings.grants.modelSection')}</Divider>
      <Typography.Paragraph type="secondary" style={{ fontSize: TYPE.secondary }}>
        {t('settings.grants.modelHint')}
      </Typography.Paragraph>
      <Select
        style={{ width: 240, marginBottom: 12 }}
        value={effectiveInst ?? undefined}
        onChange={setModelInst}
        options={instances.map((i) => ({ value: i.id, label: `${i.name} (${i.id})` }))}
        placeholder={t('common.instance')}
      />
      {effectiveInst && (
        <>
          <Table
            size="small"
            rowKey="model"
            pagination={false}
            loading={modelGrantsQuery.isLoading}
            dataSource={modelGrants}
            columns={[
              {
                title: t('settings.grants.modelColumn'),
                dataIndex: 'model',
                render: (v: string) => <span style={dataTextStyle}>{v}</span>,
              },
              { title: t('settings.users.role'), dataIndex: 'role', width: 110, render: (r: string) => <Tag>{r}</Tag> },
              {
                title: '',
                width: 90,
                render: (_: unknown, g: { model: string }) => (
                  <Button type="text" size="small" danger onClick={() => void setModelRole(g.model, 'default')}>
                    {t('settings.grants.remove')}
                  </Button>
                ),
              },
            ]}
          />
          <Space.Compact block style={{ marginTop: 12 }}>
            <Input
              value={newModel}
              onChange={(e) => setNewModel(e.target.value)}
              placeholder={t('settings.grants.modelPlaceholder')}
              style={{ fontFamily: MONO_FONT }}
            />
            <Select
              style={{ width: 120 }}
              value={newModelRole}
              onChange={setNewModelRole}
              options={[
                { value: 'viewer', label: 'viewer' },
                { value: 'operator', label: 'operator' },
              ]}
            />
            <Button
              type="primary"
              disabled={!newModel.trim()}
              onClick={() => void setModelRole(newModel.trim(), newModelRole)}
            >
              {t('settings.grants.addModel')}
            </Button>
          </Space.Compact>
        </>
      )}
    </Drawer>
  );
}

function SessionsTable({ sessions, loading, onRevoke }: {
  sessions: SessionInfo[];
  loading?: boolean;
  onRevoke: (id: string) => void;
}) {
  const { t } = useTranslation();
  return (
    <Table<SessionInfo>
      size="small"
      rowKey="id"
      loading={loading}
      dataSource={sessions}
      pagination={false}
      columns={[
        { title: t('settings.sessions.created'), dataIndex: 'createdAt', render: (v: string) => <span style={dataTextStyle}>{v}</span> },
        { title: t('settings.sessions.lastSeen'), dataIndex: 'lastSeenAt', render: (v: string) => <span style={dataTextStyle}>{v}</span> },
        { title: t('settings.sessions.ip'), dataIndex: 'ip', render: (v: string | null) => <span style={dataTextStyle}>{v ?? '-'}</span> },
        { title: t('settings.sessions.userAgent'), dataIndex: 'userAgent', ellipsis: true },
        {
          title: '',
          width: 140,
          render: (_: unknown, s: SessionInfo) => (
            <span>
              {s.current && <Tag>{t('settings.sessions.current')}</Tag>}
              <Button type="text" size="small" danger onClick={() => onRevoke(s.id)}>
                {t('settings.sessions.revoke')}
              </Button>
            </span>
          ),
        },
      ]}
    />
  );
}

function TotpSection() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { user, refresh } = useAuth();
  const [enrollment, setEnrollment] = useState<{ secret: string; otpauthUrl: string } | null>(null);
  const [backupCodes, setBackupCodes] = useState<string[] | null>(null);
  const [code, setCode] = useState('');
  const [disableOpen, setDisableOpen] = useState(false);
  const [busy, setBusy] = useState(false);

  const startEnroll = async () => {
    setBusy(true);
    try {
      setEnrollment(await authApi.totpEnroll());
      setCode('');
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const confirm = async () => {
    setBusy(true);
    try {
      const { backupCodes: codes } = await authApi.totpConfirm(code.trim());
      setBackupCodes(codes);
      setEnrollment(null);
      setCode('');
      await refresh();
    } catch {
      message.error(t('settings.totp.invalidCode'));
    } finally {
      setBusy(false);
    }
  };

  const disable = async () => {
    setBusy(true);
    try {
      await authApi.totpDisable(code.trim());
      message.success(t('settings.totp.disabled'));
      setDisableOpen(false);
      setCode('');
      await refresh();
    } catch {
      message.error(t('settings.totp.invalidCode'));
    } finally {
      setBusy(false);
    }
  };

  const copyBackupCodes = async () => {
    await navigator.clipboard.writeText((backupCodes ?? []).join('\n'));
    message.success(t('settings.totp.copied'));
  };

  return (
    <Card size="small" title={t('settings.totp.title')} style={{ marginBottom: 16 }}>
      {backupCodes ? (
        <>
          <Typography.Paragraph type="warning" style={{ fontSize: TYPE.secondary }}>
            {t('settings.totp.backupHint')}
          </Typography.Paragraph>
          <pre style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary, lineHeight: 1.8 }}>
            {backupCodes.join('\n')}
          </pre>
          <Button onClick={() => void copyBackupCodes()} style={{ marginRight: 8 }}>
            {t('settings.totp.copyCodes')}
          </Button>
          <Button type="primary" onClick={() => setBackupCodes(null)}>
            {t('settings.totp.done')}
          </Button>
        </>
      ) : enrollment ? (
        <>
          <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start', flexWrap: 'wrap' }}>
            <QRCodeSVG value={enrollment.otpauthUrl} size={160} />
            <div style={{ flex: 1, minWidth: 220 }}>
              <Typography.Paragraph style={{ fontSize: TYPE.secondary }}>
                {t('settings.totp.scanHint')}
              </Typography.Paragraph>
              <Typography.Paragraph copyable style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary }}>
                {enrollment.secret}
              </Typography.Paragraph>
              <Input
                placeholder={t('settings.totp.codePlaceholder')}
                value={code}
                onChange={(e) => setCode(e.target.value)}
                style={{ marginBottom: 8 }}
                autoComplete="one-time-code"
              />
              <Button type="primary" loading={busy} onClick={() => void confirm()} style={{ marginRight: 8 }}>
                {t('settings.totp.confirm')}
              </Button>
              <Button onClick={() => setEnrollment(null)}>{t('settings.totp.cancel')}</Button>
            </div>
          </div>
        </>
      ) : user?.totpEnabled ? (
        <>
          <Tag color={STATUS_COLORS.ready}>{t('settings.totp.enabled')}</Tag>
          <Button danger size="small" onClick={() => setDisableOpen(true)} style={{ marginLeft: 8 }}>
            {t('settings.totp.disable')}
          </Button>
        </>
      ) : (
        <Button onClick={() => void startEnroll()} loading={busy}>
          {t('settings.totp.enable')}
        </Button>
      )}

      <Modal
        open={disableOpen}
        title={t('settings.totp.disable')}
        okText={t('settings.totp.disable')}
        okButtonProps={{ danger: true, loading: busy }}
        onOk={() => void disable()}
        onCancel={() => setDisableOpen(false)}
        destroyOnHidden
      >
        <Typography.Paragraph style={{ fontSize: TYPE.secondary }}>
          {t('settings.totp.disableHint')}
        </Typography.Paragraph>
        <Input
          placeholder={t('settings.totp.codePlaceholder')}
          value={code}
          onChange={(e) => setCode(e.target.value)}
          autoComplete="one-time-code"
        />
      </Modal>
    </Card>
  );
}

function SecurityTab() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const sessionsQuery = useQuery({ queryKey: ['bff', 'my-sessions'], queryFn: sessionsApi.listMine });

  const revoke = async (id: string) => {
    try {
      await sessionsApi.revokeMine(id);
      message.success(t('settings.sessions.revoked'));
      await sessionsQuery.refetch();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <>
      <TotpSection />
      <Card size="small" title={t('settings.sessions.myTitle')}>
        <SessionsTable
          sessions={sessionsQuery.data?.sessions ?? []}
          loading={sessionsQuery.isLoading}
          onRevoke={(id) => void revoke(id)}
        />
      </Card>
    </>
  );
}

const AUDIT_ACTIONS = [
  'login_success', 'login_failure', 'login_throttled', 'account_locked', 'logout',
  'session_revoked', 'password_changed', 'user_created', 'user_updated', 'user_deleted',
  'unlock', 'http_mutation',
];

function AuditTab() {
  const { t } = useTranslation();
  const [action, setAction] = useState<string>('');
  const auditQuery = useQuery({
    queryKey: ['bff', 'audit', action],
    queryFn: () => auditApi.list({ limit: 200, action: action || undefined }),
  });

  return (
    <>
      <Select
        allowClear
        placeholder={t('settings.audit.filterAction')}
        style={{ width: 240, marginBottom: 12 }}
        value={action || undefined}
        onChange={(v) => setAction(v ?? '')}
        options={AUDIT_ACTIONS.map((a) => ({ value: a, label: a }))}
      />
      <Table<AuditEntry>
        size="small"
        rowKey="id"
        loading={auditQuery.isLoading}
        dataSource={auditQuery.data?.entries ?? []}
        columns={[
          { title: t('settings.audit.ts'), dataIndex: 'ts', width: 230, render: (v: string) => <span style={dataTextStyle}>{v}</span> },
          { title: t('settings.audit.actor'), dataIndex: 'actor', width: 110, render: (v: string | null) => v ?? '-' },
          { title: t('settings.audit.action'), dataIndex: 'action', width: 160, render: (v: string) => <Tag>{v}</Tag> },
          { title: t('settings.audit.target'), dataIndex: 'target', width: 110, render: (v: string | null) => v ?? '-' },
          { title: t('settings.audit.ip'), dataIndex: 'ip', width: 120, render: (v: string | null) => <span style={dataTextStyle}>{v ?? '-'}</span> },
          {
            title: t('settings.audit.detail'),
            dataIndex: 'detail',
            ellipsis: true,
            render: (v: Record<string, unknown> | null) => (v ? JSON.stringify(v) : '-'),
          },
        ]}
      />
    </>
  );
}

function InvitesTab() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [form] = Form.useForm<{ role: string; maxUses: number; expiresInHours: number }>();
  const [busy, setBusy] = useState(false);

  const invitesQuery = useQuery({ queryKey: ['bff', 'invites'], queryFn: invitesApi.list });

  const create = async (values: { role: string; maxUses: number; expiresInHours: number }) => {
    setBusy(true);
    try {
      await invitesApi.create({
        role: values.role as InviteInfo['role'],
        maxUses: values.maxUses,
        expiresInHours: values.expiresInHours > 0 ? values.expiresInHours : null,
      });
      message.success(t('settings.invites.created'));
      await invitesQuery.refetch();
      setDrawerOpen(false);
      form.resetFields();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const copy = async (code: string) => {
    await navigator.clipboard.writeText(code);
    message.success(t('settings.invites.copied'));
  };

  const revoke = async (code: string) => {
    try {
      await invitesApi.revoke(code);
      message.success(t('settings.invites.revoked'));
      await invitesQuery.refetch();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 12 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setDrawerOpen(true)}>
          {t('settings.invites.add')}
        </Button>
      </div>
      <Table<InviteInfo>
        size="small"
        rowKey="code"
        loading={invitesQuery.isLoading}
        dataSource={invitesQuery.data?.invites ?? []}
        pagination={false}
        columns={[
          {
            title: t('settings.invites.code'),
            dataIndex: 'code',
            render: (v: string) => <span style={dataTextStyle}>{v}</span>,
          },
          { title: t('settings.invites.role'), dataIndex: 'role', width: 100, render: (r: string) => <Tag>{r}</Tag> },
          {
            title: t('settings.invites.uses'),
            width: 90,
            render: (_: unknown, i: InviteInfo) => `${i.useCount}/${i.maxUses}`,
          },
          {
            title: t('settings.invites.expiresAt'),
            dataIndex: 'expiresAt',
            render: (v: string | null) => <span style={dataTextStyle}>{v ?? t('settings.invites.never')}</span>,
          },
          {
            title: t('settings.invites.status'),
            width: 110,
            render: (_: unknown, i: InviteInfo) =>
              i.revokedAt ? <Tag>{t('settings.invites.revokedTag')}</Tag> : null,
          },
          {
            title: '',
            width: 180,
            render: (_: unknown, i: InviteInfo) => (
              <span>
                <Button type="text" size="small" onClick={() => void copy(i.code)}>
                  {t('settings.invites.copy')}
                </Button>
                {!i.revokedAt && (
                  <Popconfirm title={t('settings.invites.revokeConfirm')} onConfirm={() => void revoke(i.code)}>
                    <Button type="text" size="small" danger>
                      {t('settings.invites.revoke')}
                    </Button>
                  </Popconfirm>
                )}
              </span>
            ),
          },
        ]}
      />

      <Drawer
        title={t('settings.invites.addTitle')}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={360}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={create}
          initialValues={{ role: 'viewer', maxUses: 1, expiresInHours: 72 }}
          requiredMark={false}
        >
          <Form.Item name="role" label={t('settings.invites.role')} rules={[{ required: true }]}>
            <Select
              options={[
                { value: 'viewer', label: 'viewer' },
                { value: 'operator', label: 'operator' },
                { value: 'admin', label: 'admin' },
              ]}
            />
          </Form.Item>
          <Form.Item name="maxUses" label={t('settings.invites.maxUses')} rules={[{ required: true }]}>
            <Input type="number" min={1} />
          </Form.Item>
          <Form.Item
            name="expiresInHours"
            label={t('settings.invites.expiresInHours')}
            extra={t('settings.invites.expiresHint')}
            rules={[{ required: true }]}
          >
            <Input type="number" min={0} />
          </Form.Item>
          <Button type="primary" htmlType="submit" block loading={busy}>
            {t('settings.invites.create')}
          </Button>
        </Form>
      </Drawer>
    </>
  );
}

export function SettingsPage() {
  const { t } = useTranslation();
  const { can } = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const tabs = [
    { key: 'instances', label: t('settings.tabs.instances'), children: <InstancesTab /> },
    { key: 'keys', label: t('settings.tabs.keys'), children: <AdminKeysTab /> },
    { key: 'security', label: t('settings.tabs.security'), children: <SecurityTab /> },
  ];
  if (can('admin')) {
    tabs.push({ key: 'users', label: t('settings.tabs.users'), children: <UsersTab /> });
    tabs.push({ key: 'invites', label: t('settings.tabs.invites'), children: <InvitesTab /> });
    tabs.push({ key: 'audit', label: t('settings.tabs.audit'), children: <AuditTab /> });
  }
  const requested = searchParams.get('tab');
  const activeTab = tabs.some((tab) => tab.key === requested) ? requested! : 'instances';
  return (
    <>
      <PageHeader title={t('nav.settings')} />
      <Tabs
        activeKey={activeTab}
        onChange={(key) => setSearchParams({ tab: key }, { replace: true })}
        items={tabs}
      />
    </>
  );
}
