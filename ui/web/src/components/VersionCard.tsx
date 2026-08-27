import { Card } from 'antd';
import { Link, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import type { TimelineEntry, VersionInfo } from '../api/types';
import { StatusBadge } from './StatusBadge';
import { VersionActions } from './VersionActions';
import { versionColor } from './TrafficRiver';
import { formatMs, formatNumber } from './format';
import { useInstanceLink } from '../context/useInstanceLink';
import { useNeutrals } from '../context/ThemeModeContext';
import { dataTextStyle, TYPE } from '../theme';
import { SPACE } from '../tokens';

interface VersionCardProps {
  model: string;
  version: VersionInfo;
  /** Index among loaded versions — matches the TrafficRiver palette. */
  colorIndex: number;
  /** Latest timeline point for this version (drives QPS/p99/RSS/streams). */
  latest?: TimelineEntry;
  /** Append the operations row (VersionActions). */
  ops?: boolean;
}

/** L3 version card (plan §3.2), shared by the model detail page and the
 * models list expanded region: version + active dot · status · weight bar ·
 * data row (worker r/t · QPS · p99 · RSS · active streams) · actions.
 * Charts live on the version detail page, never in this card. */
export function VersionCard({ model, version, colorIndex, latest, ops }: VersionCardProps) {
  const { t } = useTranslation();
  const neutrals = useNeutrals();
  const ilink = useInstanceLink();
  const navigate = useNavigate();
  const barColor = colorIndex >= 0 ? versionColor(colorIndex) : neutrals.textSecondary;
  const detailUrl = ilink(`/models/${encodeURIComponent(model)}/versions/${encodeURIComponent(version.version)}`);

  // One line of current values from the latest point; older instances omit
  // rss_mb entirely (fieldState "unsupported → -" pattern).
  const dataRow = [
    `Worker ${version.workers.ready}/${version.workers.total}`,
    latest ? `${formatNumber(latest.qps)} QPS` : '-',
    latest && latest.p99_ms > 0 ? `${formatMs(latest.p99_ms)} p99` : '-',
    latest && latest.rss_mb != null ? `${Math.round(latest.rss_mb)} MB` : '-',
    latest ? `${latest.active_streams} streams` : '-',
  ];

  return (
    <Card
      size="small"
      hoverable
      onClick={() => navigate(detailUrl)}
      styles={{ body: { display: 'flex', flexDirection: 'column', gap: SPACE[2] } }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: SPACE[3] }}>
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: SPACE[2] }}>
          <Link to={detailUrl} style={{ ...dataTextStyle, fontWeight: 600 }} onClick={(e) => e.stopPropagation()}>
            {version.version}
          </Link>
          {version.active && (
            <span style={{ color: barColor, fontSize: TYPE.secondary }}>●</span>
          )}
        </span>
        <StatusBadge status={version.status} text={version.status === 'unloaded' ? t('models.unloaded') : undefined} />
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: SPACE[2] }}>
        <div style={{ flex: 1, height: 6, borderRadius: 3, background: neutrals.textSecondary + '33', overflow: 'hidden' }}>
          <div style={{ width: `${Math.max(0, Math.min(100, version.weight))}%`, height: '100%', background: barColor }} />
        </div>
        <span style={{ ...dataTextStyle, fontSize: TYPE.secondary }}>{version.weight}%</span>
      </div>
      <span style={{ ...dataTextStyle, fontSize: TYPE.secondary, color: neutrals.textSecondary }}>
        {dataRow.join(' · ')}
      </span>
      {ops && (
        <div onClick={(e) => e.stopPropagation()}>
          <VersionActions model={model} version={version} />
        </div>
      )}
    </Card>
  );
}
