import { Card, Empty } from 'antd';
import { useTranslation } from 'react-i18next';

export function PlaygroundPage() {
  const { t } = useTranslation();
  return (
    <Card>
      <Empty description={`${t('nav.playground')} — ${t('common.comingSoon')} (M4)`} />
    </Card>
  );
}
