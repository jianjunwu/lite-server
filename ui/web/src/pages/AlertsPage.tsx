import { Card, Table } from 'antd';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useAlerts } from '../api/hooks';
import type { AlertItem } from '../api/types';
import { STATUS_COLORS, dataTextStyle } from '../theme';
import { formatTime } from '../components/format';
import { PageHeader } from '../components/PageHeader';

function SeverityText({ severity }: { severity: AlertItem['severity'] }) {
  const color = severity === 'critical' ? STATUS_COLORS.error : STATUS_COLORS.warning;
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
      <span aria-hidden style={{ width: 8, height: 8, borderRadius: '50%', background: color }} />
      <span style={{ color }}>{severity}</span>
    </span>
  );
}

export function AlertsPage() {
  const { t } = useTranslation();
  const { instanceId } = useInstance();
  const alertsQuery = useAlerts(instanceId);
  const alerts = alertsQuery.data?.alerts ?? [];

  return (
    <>
      <PageHeader title={t('alerts.title')} subtitle={instanceId} />
      <Card size="small">
        <Table<AlertItem>
          rowKey={(a) => `${a.model}/${a.version}/${a.rule}/${a.severity}`}
          loading={alertsQuery.isLoading}
          dataSource={alerts}
          pagination={{ pageSize: 50, hideOnSinglePage: true }}
          locale={{ emptyText: t('alerts.noAlerts') }}
          columns={[
            { title: t('alerts.severity'), dataIndex: 'severity', width: 120, render: (s: AlertItem['severity']) => <SeverityText severity={s} /> },
            {
              title: t('models.name'),
              dataIndex: 'model',
              render: (m: string, a) => (
                <Link to={`/models/${encodeURIComponent(m)}/versions/${encodeURIComponent(a.version)}`} style={dataTextStyle}>
                  {m}/{a.version}
                </Link>
              ),
            },
            { title: t('alerts.rule'), dataIndex: 'rule', width: 140, render: (r: string) => <span style={dataTextStyle}>{r}</span> },
            { title: t('alerts.message'), dataIndex: 'message' },
            { title: t('alerts.value'), dataIndex: 'value', width: 100, render: (v: number) => <span style={dataTextStyle}>{v}</span> },
            { title: t('alerts.threshold'), dataIndex: 'threshold', width: 110, render: (v: number) => <span style={dataTextStyle}>{v}</span> },
            {
              title: t('alerts.time'),
              dataIndex: 'timestamp',
              width: 110,
              render: (ts: number) => <span style={dataTextStyle}>{formatTime(ts)}</span>,
            },
          ]}
        />
      </Card>
    </>
  );
}
