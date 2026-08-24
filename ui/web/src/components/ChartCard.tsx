import { Card, Empty, Skeleton, Alert, Button } from 'antd';
import { useTranslation } from 'react-i18next';
import type { ReactNode } from 'react';

interface ChartCardProps {
  title: ReactNode;
  loading: boolean;
  error: Error | null;
  isEmpty: boolean;
  onRetry?: () => void;
  extra?: ReactNode;
  children: ReactNode;
  height?: number;
}

/** Unified three-state wrapper: loading skeleton / empty / error-with-retry. */
export function ChartCard({ title, loading, error, isEmpty, onRetry, extra, children, height = 240 }: ChartCardProps) {
  const { t } = useTranslation();
  let body: ReactNode;
  if (loading) {
    body = <Skeleton active paragraph={{ rows: 4 }} />;
  } else if (error) {
    body = (
      <Alert
        type="error"
        showIcon
        message={error.message}
        action={onRetry ? <Button size="small" onClick={onRetry}>{t('common.retry')}</Button> : undefined}
      />
    );
  } else if (isEmpty) {
    body = <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={t('common.empty')} />;
  } else {
    body = children;
  }
  return (
    <Card title={title} extra={extra} size="small" styles={{ body: { minHeight: height } }}>
      {body}
    </Card>
  );
}
