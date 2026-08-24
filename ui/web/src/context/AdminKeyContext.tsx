import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';
import { Form, Input, Modal, Typography } from 'antd';
import { useTranslation } from 'react-i18next';
import { setKeyRequester } from '../api/mutations';
import { MONO_FONT } from '../theme';

interface PendingRequest {
  instanceId: string;
  resolve: (key: string | null) => void;
}

const noop = async (): Promise<string | null> => null;
const RequestKeyContext = createContext<(instanceId: string) => Promise<string | null>>(noop);

/**
 * Global admin-key prompt: mutations that hit a 401 ask here for the
 * instance key; the user types it once per browser session (sessionStorage,
 * per instance) and the failed mutation retries automatically.
 */
export function AdminKeyProvider({ children }: { children: ReactNode }) {
  const { t } = useTranslation();
  const [pending, setPending] = useState<PendingRequest | null>(null);
  const [form] = Form.useForm<{ key: string }>();

  const requestKey = useCallback((instanceId: string) => {
    return new Promise<string | null>((resolve) => {
      setPending({ instanceId, resolve });
    });
  }, []);

  useEffect(() => {
    setKeyRequester(requestKey);
    return () => setKeyRequester(null);
  }, [requestKey]);

  const close = (key: string | null) => {
    pending?.resolve(key);
    setPending(null);
    form.resetFields();
  };

  return (
    <RequestKeyContext.Provider value={requestKey}>
      {children}
      <Modal
        open={pending !== null}
        title={t('adminKey.title')}
        okText={t('adminKey.submit')}
        cancelText={t('adminKey.cancel')}
        onOk={form.submit}
        onCancel={() => close(null)}
        destroyOnHidden
      >
        <Typography.Paragraph type="secondary" style={{ fontSize: 13 }}>
          {t('adminKey.hint')}{' '}
          <Typography.Text code style={{ fontFamily: MONO_FONT }}>
            {pending?.instanceId}
          </Typography.Text>
        </Typography.Paragraph>
        <Form form={form} onFinish={(values) => close(values.key)} preserve={false}>
          <Form.Item name="key" rules={[{ required: true, message: t('adminKey.required') }]}>
            <Input.Password autoFocus placeholder="x-admin-key" data-testid="admin-key-input" />
          </Form.Item>
        </Form>
      </Modal>
    </RequestKeyContext.Provider>
  );
}

export function useRequestAdminKey() {
  return useContext(RequestKeyContext);
}
