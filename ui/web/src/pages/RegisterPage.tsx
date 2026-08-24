import { useState } from 'react';
import { Alert, Button, Card, Form, Input, Typography } from 'antd';
import { ThunderboltFilled } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { Link, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { ApiError } from '../api/client';
import { authApi } from '../api/auth';
import { useAuth } from '../context/AuthContext';
import { TYPE } from '../theme';
import { useNeutrals } from '../context/ThemeModeContext';

interface RegisterFormValues {
  username: string;
  password: string;
  confirm: string;
  inviteCode?: string;
}

export function RegisterPage() {
  const { t } = useTranslation();
  const { refresh } = useAuth();
  const navigate = useNavigate();
  const neutrals = useNeutrals();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const statusQuery = useQuery({
    queryKey: ['bff', 'registration'],
    queryFn: authApi.registrationStatus,
    retry: false,
  });
  const inviteRequired = statusQuery.data?.inviteRequired ?? false;

  const submit = async (values: RegisterFormValues) => {
    setBusy(true);
    setError(null);
    try {
      await authApi.register(
        values.username.trim(),
        values.password,
        inviteRequired ? values.inviteCode?.trim() : undefined,
      );
      await refresh();
      navigate('/overview', { replace: true });
    } catch (err) {
      const code =
        err instanceof ApiError && err.body && typeof err.body === 'object'
          ? (err.body as { error?: unknown }).error
          : null;
      if (code === 'invite_required') setError(t('auth.inviteRequired'));
      else if (code === 'invalid_invite') setError(t('auth.invalidInvite'));
      else setError(err instanceof Error ? err.message : String(err));
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
          <Form.Item
            name="username"
            label={t('auth.username')}
            rules={[{ required: true, pattern: /^[a-zA-Z0-9_.-]{2,32}$/ }]}
          >
            <Input autoFocus autoComplete="username" />
          </Form.Item>
          <Form.Item
            name="password"
            label={t('auth.password')}
            extra={t('auth.passwordPolicy')}
            rules={[{ required: true, min: 12 }]}
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          <Form.Item
            name="confirm"
            label={t('auth.confirmPassword')}
            dependencies={['password']}
            rules={[
              { required: true },
              ({ getFieldValue }) => ({
                validator: (_, value) =>
                  value === getFieldValue('password' as never)
                    ? Promise.resolve()
                    : Promise.reject(new Error(t('auth.passwordMismatch'))),
              }),
            ]}
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          {inviteRequired && (
            <Form.Item name="inviteCode" label={t('auth.inviteCode')} rules={[{ required: true }]}>
              <Input autoComplete="off" />
            </Form.Item>
          )}
          <Button type="primary" htmlType="submit" block loading={busy}>
            {t('auth.register')}
          </Button>
          <div style={{ marginTop: 12, textAlign: 'center' }}>
            <Link to="/login">{t('auth.backToLogin')}</Link>
          </div>
        </Form>
      </Card>
    </div>
  );
}
