import { useEffect } from 'react';
import { Alert, Button, Divider, Empty, Layout, Menu, Select, Space, Tag, Typography } from 'antd';
import {
  DashboardOutlined,
  ClusterOutlined,
  AlertOutlined,
  CodeOutlined,
  SettingOutlined,
  ThunderboltFilled,
} from '@ant-design/icons';
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstances, useHealthSummary } from '../api/hooks';
import { useInstance } from '../context/InstanceContext';
import { useInstanceLink } from '../context/useInstanceLink';
import { setLanguage } from '../i18n';
import i18n from '../i18n';
import { StatusDot, statusKind } from '../components/StatusBadge';
import { TaskBell } from '../components/TaskBell';
import { GlobalSearch } from '../components/GlobalSearch';
import { useAlertNotifier } from '../api/useAlertNotifier';
import { useThemeMode, useNeutrals } from '../context/ThemeModeContext';
import { MoonOutlined, SunOutlined } from '@ant-design/icons';
import { UserMenu } from '../components/UserMenu';
import { SPACE } from '../tokens';
import { TYPE } from '../theme';


const { Sider, Header, Content } = Layout;

export function AppLayout() {
  const { t } = useTranslation();
  const location = useLocation();
  const navigate = useNavigate();
  const { instanceId, setInstanceId } = useInstance();
  const ilink = useInstanceLink();
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

  // Hierarchy browse: /models has no sider entry — it is reached from the
  // instance detail ("view all models"), the overview and global search.
  const menuItems = [
    {
      type: 'group' as const,
      label: t('nav.groupMonitor'),
      children: [
        { key: '/overview', icon: <DashboardOutlined />, label: <Link to={ilink('/overview')}>{t('nav.overview')}</Link> },
        { key: '/instances', icon: <ClusterOutlined />, label: <Link to={ilink('/instances')}>{t('nav.instances')}</Link> },
        { key: '/alerts', icon: <AlertOutlined />, label: <Link to={ilink('/alerts')}>{t('nav.alerts')}</Link> },
      ],
    },
    {
      type: 'group' as const,
      label: t('nav.groupTools'),
      children: [
        { key: '/playground', icon: <CodeOutlined />, label: <Link to={ilink('/playground')}>{t('nav.playground')}</Link> },
        { key: '/settings', icon: <SettingOutlined />, label: <Link to={ilink('/settings')}>{t('nav.settings')}</Link> },
      ],
    },
  ];

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider width={232} collapsible breakpoint="lg" style={{ borderRight: `1px solid ${neutrals.border}` }}>
        <div style={{ height: 56, display: 'flex', alignItems: 'center', paddingLeft: SPACE[5], gap: SPACE[2] }}>
          <ThunderboltFilled style={{ color: '#0071E3', fontSize: 18 }} />
          <Typography.Text strong style={{ fontSize: TYPE.cardTitle }}>{t('common.appName')}</Typography.Text>
        </div>
        <Menu mode="inline" selectedKeys={[selectedKey]} items={menuItems} style={{ border: 'none', padding: `0 ${SPACE[2]}px` }} />
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
            <span style={{ fontSize: TYPE.eyebrow, textTransform: 'uppercase', letterSpacing: '0.08em', color: neutrals.textSecondary }}>{t('common.instance')}</span>
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
              options={instances.map((i) => ({
                value: i.id,
                label: (
                  <span style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
                    <span>
                      {i.name} ({i.base_url})
                    </span>
                    {i.effective_role && <Tag style={{ marginInlineEnd: 0 }}>{i.effective_role}</Tag>}
                  </span>
                ),
              }))}
              placeholder={t('common.instance')}
              notFoundContent={
                <div style={{ padding: '4px 8px', color: neutrals.textSecondary }}>{t('common.empty')}</div>
              }
              popupRender={(menu) => (
                <>
                  {menu}
                  <Divider style={{ margin: '4px 0' }} />
                  <Button
                    type="text"
                    size="small"
                    icon={<SettingOutlined />}
                    style={{ width: '100%', textAlign: 'left' }}
                    onClick={() => navigate(ilink('/instances'))}
                  >
                    {t('settings.instances.manage')}
                  </Button>
                </>
              )}
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
        <Content style={{ padding: SPACE[6] }}>
          <div style={{ maxWidth: 1440, margin: '0 auto' }}>
            {instancesQuery.isSuccess && instanceId && !instances.some((i) => i.id === instanceId) ? (
              // ?i= points at an instance the BFF filtered out (grant "none")
              // or that no longer exists — say so instead of rendering a dead shell.
              <Empty
                style={{ marginTop: 96 }}
                description={
                  <>
                    <div>{t('common.noInstanceAccess')}</div>
                    <div style={{ fontSize: TYPE.secondary, color: neutrals.textSecondary }}>
                      {t('common.noInstanceAccessBody')}
                    </div>
                  </>
                }
              />
            ) : (
              <Outlet context={{ navigate }} />
            )}
          </div>
        </Content>
      </Layout>
    </Layout>
  );
}
