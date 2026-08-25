import { useState } from 'react';
import { Button, Card, Table } from 'antd';
import { BellOutlined, BellFilled } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useAlerts } from '../api/hooks';
import {
  ensureNotificationPermission, loadNotifyPref, notificationsSupported, saveNotifyPref,
} from '../api/notifications';
import type { AlertItem } from '../api/types';
import { STATUS_COLORS, dataTextStyle } from '../theme';
import { formatTime } from '../components/format';
import { PageHero, Reveal } from '../components/PageHero';
import { App } from 'antd';

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
  const { message } = App.useApp();
  const { instanceId } = useInstance();
  const alertsQuery = useAlerts(instanceId);
  const alerts = alertsQuery.data?.alerts ?? [];
  const [notifyOn, setNotifyOn] = useState(loadNotifyPref());

  const toggleNotify = async () => {
    if (notifyOn) {
      saveNotifyPref(false);
      setNotifyOn(false);
      return;
    }
    if (!notificationsSupported()) {
      message.warning(t('alerts.notifyUnsupported'));
      return;
    }
    const granted = await ensureNotificationPermission();
    if (!granted) {
      message.warning(t('alerts.notifyDenied'));
      return;
    }
    saveNotifyPref(true);
    setNotifyOn(true);
    message.success(t('alerts.notifyEnabled'));
  };

  return (
    <>
      <PageHero
        eyebrow={t('alerts.title')}
        live
        statement={
          alertsQuery.isLoading
            ? t('alerts.title')
            : alerts.length === 0
              ? t('alerts.stmtNone')
              : t('alerts.stmtFiring', { count: alerts.length })
        }
        tone={alerts.length === 0 ? 'ink' : 'error'}
        subline={instanceId}
        extra={
          <Button
            size="small"
            icon={notifyOn ? <BellFilled /> : <BellOutlined />}
            type={notifyOn ? 'primary' : 'default'}
            onClick={toggleNotify}
          >
            {notifyOn ? t('alerts.notifyOn') : t('alerts.notifyOff')}
          </Button>
        }
      />
      <Reveal order={1}>
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
      </Reveal>
    </>
  );
}
