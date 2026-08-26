import { useState } from 'react';
import { App, Button, Dropdown, Form, Input, Modal, Tag } from 'antd';
import { UserOutlined, KeyOutlined, LogoutOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';
import { useAuth } from '../context/AuthContext';
import { authApi } from '../api/auth';

// Role tags are identity markers, not alerts — keep them off the status
// hues (red/amber mean "something is wrong" everywhere else).
const ROLE_COLORS: Record<string, string> = {
  admin: '#6E6E73',
  operator: '#0071E3',
  viewer: '#8E8E93',
};

export function UserMenu() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { user, logout, refresh } = useAuth();
  const [changeOpen, setChangeOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<{ currentPassword: string; newPassword: string; confirm: string }>();

  if (!user) return null;

  const submitChange = async (values: { currentPassword: string; newPassword: string }) => {
    setBusy(true);
    try {
      await authApi.changePassword(values.currentPassword, values.newPassword);
      message.success(t('auth.passwordChanged'));
      setChangeOpen(false);
      form.resetFields();
      await refresh();
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
          items: [
            { key: 'change', icon: <KeyOutlined />, label: t('auth.changePassword'), onClick: () => setChangeOpen(true) },
            { type: 'divider' },
            { key: 'logout', icon: <LogoutOutlined />, label: t('auth.logout'), onClick: () => void logout() },
          ],
        }}
      >
        <Button type="text" icon={<UserOutlined />}>
          {user.username}
          <Tag color={ROLE_COLORS[user.role]} style={{ border: 'none', color: '#fff', marginLeft: 6 }}>
            {user.role}
          </Tag>
        </Button>
      </Dropdown>

      <Modal
        open={changeOpen}
        title={t('auth.changePassword')}
        okText={t('auth.changePassword')}
        onOk={form.submit}
        onCancel={() => setChangeOpen(false)}
        confirmLoading={busy}
        destroyOnHidden
      >
        <Form form={form} layout="vertical" onFinish={submitChange} preserve={false} requiredMark={false}>
          <Form.Item name="currentPassword" label={t('auth.currentPassword')} rules={[{ required: true }]}>
            <Input.Password autoComplete="current-password" />
          </Form.Item>
          <Form.Item name="newPassword" label={t('auth.newPassword')} rules={[{ required: true, min: 12 }]}>
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          <Form.Item
            name="confirm"
            label={t('auth.confirmPassword')}
            dependencies={['newPassword']}
            rules={[
              { required: true },
              ({ getFieldValue }) => ({
                validator: (_, value) =>
                  value === getFieldValue('newPassword' as never)
                    ? Promise.resolve()
                    : Promise.reject(new Error(t('auth.passwordMismatch'))),
              }),
            ]}
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}
