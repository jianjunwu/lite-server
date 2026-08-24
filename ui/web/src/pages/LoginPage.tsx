import { useState } from 'react';
import { Alert, Button, Card, Form, Input, Typography } from 'antd';
import { ThunderboltFilled } from '@ant-design/icons';
import { Link, Navigate, useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { ApiError } from '../api/client';
import { useAuth } from '../context/AuthContext';
import { TYPE } from '../theme';
import { useNeutrals } from '../context/ThemeModeContext';

export function LoginPage() {
  const { t } = useTranslation();
  const { login, verifyTotp, user, loading } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const neutrals = useNeutrals();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [totpChallenge, setTotpChallenge] = useState<string | null>(null);

  if (!loading && user) {
    return <Navigate to="/overview" replace />;
  }

  const destination = () => (location.state as { from?: string } | null)?.from ?? '/overview';

  const submit = async (values: { username: string; password: string }) => {
    setBusy(true);
    setError(null);
    try {
      const result = await login(values.username, values.password);
      if ('totpRequired' in result) {
        setTotpChallenge(result.challenge);
        return;
      }
      navigate(destination(), { replace: true });
    } catch (err) {
      if (err instanceof ApiError && err.status === 423) {
        const secs =
          err.body && typeof err.body === 'object'
            ? (err.body as { retryAfterSec?: number }).retryAfterSec
            : undefined;
        setError(t('auth.accountLocked', { seconds: secs ?? 900 }));
      } else if (err instanceof ApiError && err.status === 429) {
        setError(t('auth.tooManyAttempts'));
      } else {
        setError(t('auth.invalidCredentials'));
      }
    } finally {
      setBusy(false);
    }
  };

  const submitTotp = async (values: { code: string }) => {
    if (!totpChallenge) return;
    setBusy(true);
    setError(null);
    try {
      await verifyTotp(totpChallenge, values.code.trim());
      navigate(destination(), { replace: true });
    } catch {
      setError(t('auth.invalidTotpCode'));
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
        {totpChallenge ? (
          <Form layout="vertical" onFinish={submitTotp} requiredMark={false}>
            <Form.Item
              name="code"
              label={t('auth.totpCode')}
              extra={t('auth.totpCodeHint')}
              rules={[{ required: true }]}
            >
              <Input autoFocus autoComplete="one-time-code" />
            </Form.Item>
            <Button type="primary" htmlType="submit" block loading={busy}>
              {t('auth.verify')}
            </Button>
            <div style={{ marginTop: 12, textAlign: 'center' }}>
              <Link to="/login" onClick={() => setTotpChallenge(null)}>
                {t('auth.backToLogin')}
              </Link>
            </div>
          </Form>
        ) : (
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
            <div style={{ marginTop: 12, textAlign: 'center' }}>
              <Link to="/register">{t('auth.registerLink')}</Link>
            </div>
          </Form>
        )}
      </Card>
    </div>
  );
}
