import { useState, type ReactNode } from 'react';
import { App, Button, Card, Form, Input, Spin, Typography } from 'antd';
import { ThunderboltFilled } from '@ant-design/icons';
import { Navigate, useLocation } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useAuth } from '../context/AuthContext';
import { TYPE } from '../theme';
import { useNeutrals } from '../context/ThemeModeContext';

/** Forced password change: rendered instead of the app while
 * me.mustChangePassword is set (bootstrap / admin reset). */
function ForceChangePassword() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { refresh } = useAuth();
  const neutrals = useNeutrals();
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<{ currentPassword: string; newPassword: string; confirm: string }>();

  const submit = async (values: { currentPassword: string; newPassword: string }) => {
    setBusy(true);
    try {
      const { authApi } = await import('../api/auth');
      await authApi.changePassword(values.currentPassword, values.newPassword);
      message.success(t('auth.passwordChanged'));
      await refresh();
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: neutrals.bgPage }}>
      <Card style={{ width: 400 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16 }}>
          <ThunderboltFilled style={{ color: '#4F46E5', fontSize: 22 }} />
          <Typography.Text strong style={{ fontSize: TYPE.pageTitle }}>lite-ui</Typography.Text>
        </div>
        <Typography.Paragraph type="secondary" style={{ fontSize: TYPE.body }}>
          {t('auth.mustChangeHint')}
        </Typography.Paragraph>
        <Form form={form} layout="vertical" onFinish={submit} requiredMark={false}>
          <Form.Item name="currentPassword" label={t('auth.currentPassword')} rules={[{ required: true }]}>
            <Input.Password autoComplete="current-password" />
          </Form.Item>
          <Form.Item name="newPassword" label={t('auth.newPassword')} rules={[{ required: true, min: 8 }]}>
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
          <Button type="primary" htmlType="submit" block loading={busy}>
            {t('auth.changePassword')}
          </Button>
        </Form>
      </Card>
    </div>
  );
}

/** Route guard: /me probe → login redirect → forced password change → app. */
export function RequireAuth({ children }: { children: ReactNode }) {
  const { user, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return (
      <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Spin size="large" />
      </div>
    );
  }
  if (!user) {
    return <Navigate to="/login" state={{ from: location.pathname + location.search }} replace />;
  }
  if (user.mustChangePassword) {
    return <ForceChangePassword />;
  }
  return <>{children}</>;
}
