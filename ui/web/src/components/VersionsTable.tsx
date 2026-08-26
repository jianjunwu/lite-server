import { Table, type TableProps } from 'antd';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import type { VersionInfo } from '../api/types';
import { StatusBadge } from './StatusBadge';
import { VersionActions } from './VersionActions';
import { formatAge } from './format';
import { useNeutrals } from '../context/ThemeModeContext';
import { useInstanceLink } from '../context/useInstanceLink';
import { dataTextStyle, TYPE } from '../theme';

interface VersionsTableProps {
  model: string;
  versions: VersionInfo[];
  loading?: boolean;
  /** Append the operations column (M2 management actions). */
  ops?: boolean;
}

/** Shared versions table — used by the Models page expand row and the model
 * detail Versions tab. The traffic river lives at page level (card body /
 * detail hero), once per view, not inside this table. */
export function VersionsTable({ model, versions, loading, ops }: VersionsTableProps) {
  const { t } = useTranslation();
  const neutrals = useNeutrals();
  const ilink = useInstanceLink();

  const columns: TableProps<VersionInfo>['columns'] = [
    {
      title: t('common.version'),
      dataIndex: 'version',
      width: 110,
      render: (v: string) => (
        <Link to={ilink(`/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(v)}`)} style={dataTextStyle}>
          {v}
        </Link>
      ),
    },
    {
      title: t('common.status'),
      dataIndex: 'status',
      width: 130,
      render: (s: string) => (
        <StatusBadge status={s} text={s === 'unloaded' ? t('models.unloaded') : undefined} />
      ),
    },
    {
      title: t('common.active'),
      dataIndex: 'active',
      width: 90,
      render: (a: boolean) =>
        a ? (
          <span style={{ fontSize: TYPE.eyebrow, textTransform: 'uppercase', letterSpacing: '0.06em', color: neutrals.textPrimary }}>
            ● {t('common.active')}
          </span>
        ) : null,
    },
    {
      title: t('common.weight'),
      dataIndex: 'weight',
      width: 90,
      render: (w: number) => <span style={dataTextStyle}>{w}%</span>,
    },
    {
      title: t('common.workers'),
      width: 90,
      render: (_: unknown, v: VersionInfo) => (
        <span style={dataTextStyle}>{v.workers.ready}/{v.workers.total}</span>
      ),
    },
    {
      title: t('common.loadedAt'),
      dataIndex: 'loaded_at',
      width: 100,
      render: (ts: number | null) => <span style={dataTextStyle}>{ts ? t('common.ageAgo', { age: formatAge(ts) }) : '-'}</span>,
    },
  ];

  if (ops) {
    columns.push({
      title: t('ops.actions'),
      key: 'ops',
      render: (_: unknown, v: VersionInfo) => <VersionActions model={model} version={v} />,
    });
  }

  return (
    <Table<VersionInfo>
      size="small"
      rowKey="version"
      loading={loading}
      dataSource={versions}
      pagination={false}
      columns={columns}
    />
  );
}
