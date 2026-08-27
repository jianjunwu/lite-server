import { useEffect, useState } from 'react';
import { App, Button, Checkbox, Drawer, Form, Input } from 'antd';
import { useTranslation } from 'react-i18next';
import { bffFetch } from '../api/client';
import type { InstanceInfo } from '../api/types';
import { MONO_FONT } from '../theme';

export interface InstanceFormValues {
  id: string;
  name: string;
  base_url: string;
  admin_key?: string;
  probe?: boolean;
}

interface InstanceFormProps {
  open: boolean;
  /** Non-null = edit that instance (id field disabled); null = create. */
  editing: InstanceInfo | null;
  onClose: () => void;
  /** Called after a successful create/update — the caller invalidates its list. */
  onSaved: () => void;
}

/** Shared instance create/edit form. Used by /instances and the settings
 * instances tab so the drawer logic lives once (plan §3.3). */
export function InstanceForm({ open, editing, onClose, onSaved }: InstanceFormProps) {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const [form] = Form.useForm<InstanceFormValues>();
  const [busy, setBusy] = useState(false);

  // Sync the form whenever the drawer opens: create resets to defaults,
  // edit pre-fills the instance (admin key always starts blank).
  useEffect(() => {
    if (!open) return;
    if (editing) {
      form.setFieldsValue({
        id: editing.id,
        name: editing.name,
        base_url: editing.base_url,
        admin_key: '',
        probe: false,
      });
    } else {
      form.resetFields();
    }
  }, [open, editing, form]);

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
      onSaved();
      onClose();
      form.resetFields();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Drawer
      title={editing ? t('settings.instances.editTitle', { id: editing.id }) : t('settings.instances.addTitle')}
      open={open}
      onClose={onClose}
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
  );
}
