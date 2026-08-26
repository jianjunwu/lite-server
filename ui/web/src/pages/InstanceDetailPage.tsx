import { useMemo, useState } from 'react';
import { Alert, Button, Card, Collapse, Empty, Input, Table, Tabs, Tag, Typography } from 'antd';
import { ReloadOutlined, SearchOutlined, SettingOutlined, TableOutlined } from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useNavigate, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { ApiError, apiFetch } from '../api/client';
import { useInstances, useModels, useServerInfo, useTimelineAll } from '../api/hooks';
import { useServerConfig } from '../api/config';
import type { ModelListItem } from '../api/types';
import { PageHeader } from '../components/PageHeader';
import { StatNum } from '../components/StatNum';
import { StatusBadge } from '../components/StatusBadge';
import { groupServerConfig, sourceTagColor } from '../components/config/serverConfigSchema';
import { useNeutrals } from '../context/ThemeModeContext';
import { MONO_FONT, TYPE, dataTextStyle } from '../theme';
import { SPACE } from '../tokens';

type ProbeName = 'livez' | 'readyz' | 'startupz';

/** Probe result: 'ok' (2xx), 'fail' (HTTP error, e.g. 503 draining), or
 * 'unreachable' (network/BFF-level failure). */
function useProbe(instanceId: string | null, name: ProbeName) {
  return useQuery({
    queryKey: [instanceId, 'probe', name],
    queryFn: async (): Promise<'ok' | 'fail'> => {
      try {
        await apiFetch(instanceId!, `/${name}`);
        return 'ok';
      } catch (err) {
        if (err instanceof ApiError && err.status > 0 && err.status !== 502) return 'fail';
        throw err;
      }
    },
    enabled: instanceId !== null,
    refetchInterval: 10_000,
    retry: 0,
  });
}

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return '-';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

/** M5 read-only config tab: search + per-section grouped rows, each
 * `path | value | source badge` (plan §4.2). Secrets arrive redacted. */
function ConfigTab({ instanceId }: { instanceId: string }) {
  const { t } = useTranslation();
  const neutrals = useNeutrals();
  const query = useServerConfig(instanceId);
  const [filter, setFilter] = useState('');

  const groups = useMemo(() => {
    if (!query.data) return [];
    const all = groupServerConfig(query.data.config, query.data.sources, query.data.redacted);
    const q = filter.trim().toLowerCase();
    if (!q) return all;
    return all
      .map((g) => ({ ...g, rows: g.rows.filter((r) => r.path.toLowerCase().includes(q)) }))
      .filter((g) => g.rows.length > 0);
  }, [query.data, filter]);

  if (query.isError) {
    // 404 on instances predating M5; anything else (unreachable, 401 without
    // an admin key) gets the same degraded copy — the rest of the page works.
    return <Empty description={t('instance.config.unsupported')} />;
  }

  return (
    <Card size="small" loading={query.isLoading}>
      <Input
        allowClear
        prefix={<SearchOutlined style={{ color: neutrals.textSecondary }} />}
        placeholder={t('instance.config.searchPlaceholder')}
        style={{ maxWidth: 320, marginBottom: SPACE[3], fontFamily: MONO_FONT }}
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
      />
      {query.data && groups.length === 0 && (
        <Empty
          description={
            filter.trim() ? t('instance.config.noMatch', { query: filter.trim() }) : t('instance.config.empty')
          }
        />
      )}
      <Collapse
        ghost
        defaultActiveKey={groups.map((g) => g.key)}
        items={groups.map((g) => ({
          key: g.key,
          label: t(`instance.config.groups.${g.key}`, { defaultValue: g.key }),
          children: (
            <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[2] }}>
              {g.rows.map((row) => (
                <div
                  key={row.path}
                  style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: SPACE[4] }}
                >
                  <span style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary }}>{row.path}</span>
                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: SPACE[2], minWidth: 0 }}>
                    <span
                      style={{
                        ...dataTextStyle,
                        fontSize: TYPE.secondary,
                        textAlign: 'right',
                        wordBreak: 'break-all',
                      }}
                    >
                      {row.redacted ? '••••••' : formatValue(row.value)}
                    </span>
                    <Tag color={sourceTagColor(row.source)} style={{ marginInlineEnd: 0 }}>
                      {t(`instance.config.source.${row.source}`)}
                    </Tag>
                  </span>
                </div>
              ))}
            </div>
          ),
        }))}
      />
    </Card>
  );
}

function tabLabel(icon: React.ReactNode, text: string) {
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
      {icon}
      {text}
    </span>
  );
}

/** M5: instance detail page (plan §4.2). The instance comes from the :id
 * route param and is fixed for the whole page — links out pin ?i={id}. */
export function InstanceDetailPage() {
  const { t } = useTranslation();
  const { id = '' } = useParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [tab, setTab] = useState('overview');

  const instancesQuery = useInstances();
  const instance = (instancesQuery.data?.instances ?? []).find((i) => i.id === id);
  const infoQuery = useServerInfo(id);
  const modelsQuery = useModels(id);
  const timelineQuery = useTimelineAll(id);
  const livez = useProbe(id, 'livez');
  const readyz = useProbe(id, 'readyz');
  const startupz = useProbe(id, 'startupz');

  const probes = [
    { name: 'livez' as const, query: livez },
    { name: 'readyz' as const, query: readyz },
    { name: 'startupz' as const, query: startupz },
  ];

  // Last timeline point per snapshot: RSS/CPU are process-level (any defined
  // value), active streams aggregate across models.
  const lastPoints = useMemo(
    () =>
      (timelineQuery.data?.snapshots ?? [])
        .map((s) => s.entries[s.entries.length - 1])
        .filter((p) => p !== undefined),
    [timelineQuery.data],
  );
  const rssMb = lastPoints.map((p) => p.rss_mb).find((v) => v != null);
  const cpuPercent = lastPoints.map((p) => p.cpu_percent).find((v) => v != null);
  const activeStreams =
    lastPoints.length > 0
      ? lastPoints.reduce((sum, p) => sum + (p.active_streams ?? 0), 0)
      : undefined;

  if (!instancesQuery.isLoading && !instance) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[5] }}>
        <PageHeader
          title={id}
          breadcrumb={[{ title: t('nav.settings'), href: '/settings' }, { title: id }]}
        />
        <Card size="small">
          <Empty description={t('instance.detail.notFound')} />
        </Card>
      </div>
    );
  }

  const online = !infoQuery.isError;
  const loadedCount = infoQuery.data?.loaded_models.length;
  const statement = online
    ? [
        t('instance.detail.online'),
        infoQuery.data ? `v${infoQuery.data.version}` : null,
        loadedCount !== undefined ? t('instance.detail.modelsLoaded', { count: loadedCount }) : null,
      ]
        .filter(Boolean)
        .join(' · ')
    : `${t('instance.detail.offline')} · ${instance?.base_url ?? id}`;

  const models: ModelListItem[] = modelsQuery.data?.models ?? [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[5] }}>
      <PageHeader
        breadcrumb={[
          { title: t('nav.settings'), href: '/settings' },
          { title: t('settings.tabs.instances'), href: '/settings?tab=instances' },
          { title: instance?.name ?? id },
        ]}
        title={instance?.name ?? id}
        subtitle={statement}
        extra={
          <Button
            size="small"
            icon={<ReloadOutlined />}
            onClick={() => queryClient.invalidateQueries({ queryKey: [id] })}
          >
            {t('common.refresh')}
          </Button>
        }
      />

      {!online && (
        <Alert type="warning" showIcon message={t('instance.detail.unreachable')} />
      )}

      <Tabs
        activeKey={tab}
        onChange={setTab}
        items={[
          {
            key: 'overview',
            label: tabLabel(<TableOutlined aria-hidden />, t('instance.detail.tabs.overview')),
            children: (
              <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[5] }}>
                <Card size="small">
                  <div style={{ display: 'flex', gap: SPACE[7], flexWrap: 'wrap' }}>
                    <StatNum
                      label={t('instance.detail.stats.version')}
                      value={infoQuery.data ? `v${infoQuery.data.version}` : '-'}
                    />
                    <StatNum
                      label={t('instance.detail.stats.modelsLoaded')}
                      value={loadedCount ?? '-'}
                    />
                    <StatNum
                      label={t('instance.detail.stats.rss')}
                      value={rssMb != null ? Math.round(rssMb) : '-'}
                      unit={rssMb != null ? 'MB' : undefined}
                    />
                    <StatNum
                      label={t('instance.detail.stats.cpu')}
                      value={cpuPercent != null ? cpuPercent.toFixed(1) : '-'}
                      unit={cpuPercent != null ? '%' : undefined}
                    />
                    <StatNum
                      label={t('instance.detail.stats.activeStreams')}
                      value={activeStreams ?? '-'}
                    />
                  </div>
                </Card>

                <Card size="small" title={t('instance.detail.probes.title')}>
                  <div style={{ display: 'flex', gap: SPACE[7], flexWrap: 'wrap' }}>
                    {probes.map(({ name, query }) => (
                      <span key={name} style={{ display: 'inline-flex', alignItems: 'center', gap: SPACE[2] }}>
                        <span style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary }}>{name}</span>
                        {query.isError ? (
                          <StatusBadge status="offline" text="-" />
                        ) : query.data === 'ok' ? (
                          <StatusBadge status="ready" text="ok" />
                        ) : query.data === 'fail' ? (
                          <StatusBadge status="failed" text="fail" />
                        ) : (
                          <Typography.Text type="secondary" style={{ fontSize: TYPE.secondary }}>…</Typography.Text>
                        )}
                      </span>
                    ))}
                  </div>
                </Card>

                <Card size="small" title={t('instance.detail.models.title')}>
                  <Table<ModelListItem>
                    size="small"
                    rowKey={(r) => `${r.name}/${r.version}`}
                    loading={modelsQuery.isLoading}
                    dataSource={models}
                    pagination={false}
                    locale={{ emptyText: t('instance.detail.models.empty') }}
                    onRow={(r) => ({
                      style: { cursor: 'pointer' },
                      onClick: () =>
                        navigate(`/models/${encodeURIComponent(r.name)}?i=${encodeURIComponent(id)}`),
                    })}
                    columns={[
                      {
                        title: t('models.name'),
                        dataIndex: 'name',
                        render: (v: string) => <span style={dataTextStyle}>{v}</span>,
                      },
                      {
                        title: t('common.version'),
                        dataIndex: 'version',
                        width: 120,
                        render: (v: string) => <span style={dataTextStyle}>{v}</span>,
                      },
                      {
                        title: t('common.status'),
                        dataIndex: 'status',
                        width: 140,
                        render: (v: string) => <StatusBadge status={v} />,
                      },
                      {
                        title: t('common.workers'),
                        dataIndex: 'workers',
                        width: 100,
                        align: 'right',
                        render: (v: number) => <span style={dataTextStyle}>{v}</span>,
                      },
                    ]}
                  />
                </Card>
              </div>
            ),
          },
          {
            key: 'config',
            label: tabLabel(<SettingOutlined aria-hidden />, t('instance.detail.tabs.config')),
            children: <ConfigTab instanceId={id} />,
          },
        ]}
      />
    </div>
  );
}
