import { Card, Table, Tag } from 'antd';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useAlerts } from '../api/hooks';
import type { AlertItem } from '../api/types';
import { STATUS_COLORS } from '../theme';
import { formatTime } from '../components/format';

export function AlertsPage() {
  const { t } = useTranslation();
  const { instanceId } = useInstance();
  const alertsQuery = useAlerts(instanceId);
  const alerts = alertsQuery.data?.alerts ?? [];

  return (
    <Card size="small">
      <Table<AlertItem>
        rowKey={(a) => `${a.model}/${a.version}/${a.rule}/${a.severity}`}
        loading={alertsQuery.isLoading}
        dataSource={alerts}
        pagination={{ pageSize: 50, hideOnSinglePage: true }}
        locale={{ emptyText: t('alerts.noAlerts') }}
        columns={[
          {
            title: t('alerts.severity'),
            dataIndex: 'severity',
            width: 110,
            render: (s: AlertItem['severity']) => (
              <Tag color={s === 'critical' ? STATUS_COLORS.error : STATUS_COLORS.warning} style={{ color: '#fff', border: 'none' }}>
                {s}
              </Tag>
            ),
          },
          {
            title: t('models.name'),
            dataIndex: 'model',
            render: (m: string, a) => (
              <Link to={`/models/${encodeURIComponent(m)}/versions/${encodeURIComponent(a.version)}`}>
                {m}/{a.version}
              </Link>
            ),
          },
          { title: t('alerts.rule'), dataIndex: 'rule', width: 140 },
          { title: t('alerts.message'), dataIndex: 'message' },
          { title: t('alerts.value'), dataIndex: 'value', width: 100 },
          { title: t('alerts.threshold'), dataIndex: 'threshold', width: 110 },
          {
            title: t('alerts.time'),
            dataIndex: 'timestamp',
            width: 110,
            render: (ts: number) => formatTime(ts),
          },
        ]}
      />
    </Card>
  );
}
