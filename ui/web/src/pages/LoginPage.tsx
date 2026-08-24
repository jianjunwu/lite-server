import { useState } from 'react';
import { Alert, Button, Card, Form, Input, Typography } from 'antd';
import { ThunderboltFilled } from '@ant-design/icons';
import { useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useAuth } from '../context/AuthContext';
import { TYPE } from '../theme';
import { useNeutrals } from '../context/ThemeModeContext';

export function LoginPage() {
  const { t } = useTranslation();
  const { login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const neutrals = useNeutrals();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (values: { username: string; password: string }) => {
    setBusy(true);
    setError(null);
    try {
      await login(values.username, values.password);
      const from = (location.state as { from?: string } | null)?.from ?? '/overview';
      navigate(from, { replace: true });
    } catch {
      setError(t('auth.invalidCredentials'));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: neutrals.bgPage }}>
      <Card style={{ width: 360 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 24 }}>
          <ThunderboltFilled style={{ color: '#4F46E5', fontSize: 22 }} />
          <Typography.Text strong style={{ fontSize: TYPE.pageTitle }}>lite-ui</Typography.Text>
        </div>
        {error && <Alert type="error" message={error} showIcon style={{ marginBottom: 16 }} />}
        <Form layout="vertical" onFinish={submit} requiredMark={false}>
          <Form.Item name="username" label={t('auth.username')} rules={[{ required: true }]}>
            <Input autoFocus autoComplete="username" />
          </Form.Item>
          <Form.Item name="password" label={t('auth.password')} rules={[{ required: true }]}>
            <Input.Password autoComplete="current-password" />
          </Form.Item>
          <Button type="primary" htmlType="submit" block loading={busy}>
            {t('auth.login')}
          </Button>
        </Form>
      </Card>
    </div>
  );
}
