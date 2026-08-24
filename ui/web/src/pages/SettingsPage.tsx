import { Card, Empty } from 'antd';
import { useTranslation } from 'react-i18next';

export function SettingsPage() {
  const { t } = useTranslation();
  return (
    <Card>
      <Empty description={`${t('nav.settings')} — ${t('common.comingSoon')} (M2)`} />
    </Card>
  );
}
