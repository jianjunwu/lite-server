import { useState } from 'react';
import { App, Button, Card, Checkbox, Drawer, Form, Input, Popconfirm, Table, Tabs, Tag, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { bffFetch, setAdminKey } from '../api/client';
import { useInstances } from '../api/hooks';
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
        <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
          {t('settings.instances.add')}
        </Button>
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
              ) : (
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
              ),
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

export function SettingsPage() {
  const { t } = useTranslation();
  return (
    <>
      <PageHeader title={t('nav.settings')} />
      <Tabs
        items={[
          { key: 'instances', label: t('settings.tabs.instances'), children: <InstancesTab /> },
          { key: 'keys', label: t('settings.tabs.keys'), children: <AdminKeysTab /> },
        ]}
      />
    </>
  );
}
