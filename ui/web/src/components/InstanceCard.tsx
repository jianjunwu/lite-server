import { Button, Card, Popconfirm, Skeleton, Tag, Tooltip, Typography } from 'antd';
import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import type { HealthSummary, InstanceInfo, ServerInfo } from '../api/types';
import { StatusBadge } from './StatusBadge';
import { useNeutrals } from '../context/ThemeModeContext';
import { TYPE, dataTextStyle } from '../theme';
import { SPACE } from '../tokens';

interface InstanceCardProps {
  inst: InstanceInfo;
  /** Health payload; undefined while loading or when unreachable. */
  health: HealthSummary | undefined;
  healthLoading: boolean;
  unreachable: boolean;
  /** /info payload (server version), may lag behind health. */
  info: ServerInfo | undefined;
  /** Instances-list mode: hover edit/delete. Omitted on the overview. */
  onEdit?: () => void;
  onDelete?: () => void;
}

/**
 * L0 instance card (plan §3.2): name + version · base_url · role tag ·
 * status badge · count row (models / versions / workers). No model rows,
 * no RSS/CPU, no traffic — the instance detail page owns those. With hover
 * actions it is a management card; without, it is a link into the detail.
 */
export function InstanceCard({ inst, health, healthLoading, unreachable, info, onEdit, onDelete }: InstanceCardProps) {
  const { t } = useTranslation();
  const neutrals = useNeutrals();
  const to = `/instances/${encodeURIComponent(inst.id)}?i=${encodeURIComponent(inst.id)}`;

  // /health workers is the registry total (incl. stopped slots) — verified
  // in src/registry/mod.rs server_status(): workers: mv.workers.len().
  const models = health?.models ?? [];
  const versionCount = models.reduce((sum, m) => sum + m.versions.length, 0);
  const workerCount = models.reduce((sum, m) => sum + m.versions.reduce((w, v) => w + v.workers, 0), 0);

  const title = (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: SPACE[2] }}>
      {onEdit || onDelete ? (
        <Link to={to}>{inst.name}</Link>
      ) : (
        <span>{inst.name}</span>
      )}
      {info?.version && (
        <Typography.Text type="secondary" style={{ fontSize: TYPE.eyebrow, fontWeight: 400 }}>
          {info.version}
        </Typography.Text>
      )}
    </span>
  );

  const body = (
    <Card
      className="lift instance-card"
      title={title}
      extra={
        unreachable ? (
          <StatusBadge status="offline" text={t('common.unreachable')} />
        ) : (
          <StatusBadge status={health?.status} />
        )
      }
      loading={healthLoading && !unreachable}
    >
      {unreachable ? (
        <Typography.Text type="secondary" style={dataTextStyle}>{inst.base_url}</Typography.Text>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[2] }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: SPACE[3] }}>
            <span style={{ ...dataTextStyle, fontSize: TYPE.secondary, color: neutrals.textSecondary }}>
              {inst.base_url}
            </span>
            {inst.effective_role && <Tag style={{ marginInlineEnd: 0 }}>{inst.effective_role}</Tag>}
          </div>
          <span style={{ fontSize: TYPE.secondary, color: neutrals.textSecondary }}>
            {t('instances.counts', {
              models: models.length,
              versions: versionCount,
              workers: workerCount,
            })}
          </span>
          {onEdit && onDelete && (
            <div className="hover-only" style={{ display: 'flex', justifyContent: 'flex-end', gap: SPACE[1] }}>
              <Tooltip title={t('settings.instances.edit')}>
                <Button type="text" size="small" icon={<EditOutlined aria-hidden />} aria-label={t('settings.instances.edit')} onClick={onEdit} />
              </Tooltip>
              <Popconfirm title={t('settings.instances.deleteConfirm', { id: inst.id })} onConfirm={onDelete}>
                <Tooltip title={t('settings.instances.delete')}>
                  <Button type="text" size="small" danger icon={<DeleteOutlined aria-hidden />} aria-label={t('settings.instances.delete')} />
                </Tooltip>
              </Popconfirm>
            </div>
          )}
        </div>
      )}
    </Card>
  );

  return onEdit || onDelete ? body : <Link to={to}>{body}</Link>;
}
