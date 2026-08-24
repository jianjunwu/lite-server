import { useEffect } from 'react';
import { Alert, Button, Layout, Menu, Select, Space, Typography } from 'antd';
import {
  DashboardOutlined,
  AppstoreOutlined,
  LineChartOutlined,
  AlertOutlined,
  CodeOutlined,
  SettingOutlined,
  ThunderboltFilled,
} from '@ant-design/icons';
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstances, useHealthSummary } from '../api/hooks';
import { useInstance } from '../context/InstanceContext';
import { setLanguage } from '../i18n';
import i18n from '../i18n';
import { StatusDot, statusKind } from '../components/StatusBadge';
import { TaskBell } from '../components/TaskBell';
import { GlobalSearch } from '../components/GlobalSearch';
import { useAlertNotifier } from '../api/useAlertNotifier';
import { useThemeMode, useNeutrals } from '../context/ThemeModeContext';
import { MoonOutlined, SunOutlined } from '@ant-design/icons';
import { UserMenu } from '../components/UserMenu';


const { Sider, Header, Content } = Layout;

export function AppLayout() {
  const { t } = useTranslation();
  const location = useLocation();
  const navigate = useNavigate();
  const { instanceId, setInstanceId } = useInstance();
  const { dark, toggle } = useThemeMode();
  const neutrals = useNeutrals();
  const instancesQuery = useInstances();
  const instances = instancesQuery.data?.instances ?? [];
  const currentHealth = useHealthSummary(instanceId);
  useAlertNotifier(instanceId);

  // Default to the first configured instance once the list arrives.
  useEffect(() => {
    if (!instanceId && instances.length > 0) {
      setInstanceId(instances[0].id);
    }
  }, [instanceId, instances]);

  const selectedKey = '/' + (location.pathname.split('/')[1] || 'overview');

  const menuItems = [
    {
      type: 'group' as const,
      label: t('nav.groupMonitor'),
      children: [
        { key: '/overview', icon: <DashboardOutlined />, label: <Link to="/overview">{t('nav.overview')}</Link> },
        { key: '/models', icon: <AppstoreOutlined />, label: <Link to="/models">{t('nav.models')}</Link> },
        { key: '/metrics', icon: <LineChartOutlined />, label: <Link to="/metrics">{t('nav.metrics')}</Link> },
        { key: '/alerts', icon: <AlertOutlined />, label: <Link to="/alerts">{t('nav.alerts')}</Link> },
      ],
    },
    {
      type: 'group' as const,
      label: t('nav.groupTools'),
      children: [
        { key: '/playground', icon: <CodeOutlined />, label: <Link to="/playground">{t('nav.playground')}</Link> },
        { key: '/settings', icon: <SettingOutlined />, label: <Link to="/settings">{t('nav.settings')}</Link> },
      ],
    },
  ];

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider width={208} collapsible breakpoint="lg" style={{ borderRight: `1px solid ${neutrals.border}` }}>
        <div style={{ height: 48, display: 'flex', alignItems: 'center', paddingLeft: 20, gap: 8 }}>
          <ThunderboltFilled style={{ color: '#4F46E5', fontSize: 18 }} />
          <Typography.Text strong style={{ fontSize: 15 }}>lite-ui</Typography.Text>
        </div>
        <Menu mode="inline" selectedKeys={[selectedKey]} items={menuItems} style={{ border: 'none' }} />
      </Sider>
      <Layout>
        <Header
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '0 16px',
            borderBottom: `1px solid ${neutrals.border}`,
            position: 'sticky',
            top: 0,
            zIndex: 10,
          }}
        >
          <Space size="middle">
            <span style={{ fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.06em', color: neutrals.textSecondary }}>{t('common.instance')}</span>
            {instanceId && (
              <StatusDot
                kind={currentHealth.isError ? 'offline' : statusKind(currentHealth.data?.status ?? 'loading')}
              />
            )}
            <Select
              style={{ minWidth: 220 }}
              value={instanceId ?? undefined}
              loading={instancesQuery.isLoading}
              onChange={(id) => setInstanceId(id)}
              options={instances.map((i) => ({ value: i.id, label: `${i.name} (${i.base_url})` }))}
              placeholder={t('common.instance')}
            />
          </Space>
          <Space size="middle">
            <GlobalSearch />
            <Button type="text" icon={dark ? <SunOutlined /> : <MoonOutlined />} onClick={toggle} aria-label="theme" />
            <TaskBell />
            <Select
              size="small"
              value={i18n.language}
              style={{ width: 110 }}
              onChange={setLanguage}
              options={[
                { value: 'en', label: 'English' },
                { value: 'zh', label: '中文' },
              ]}
            />
            <UserMenu />
          </Space>
        </Header>
        {instanceId && currentHealth.isError && (
          <Alert type="warning" showIcon banner message={`${t('common.unreachable')}: ${instanceId}`} />
        )}
        <Content style={{ padding: 24 }}>
          <Outlet context={{ navigate }} />
        </Content>
      </Layout>
    </Layout>
  );
}
