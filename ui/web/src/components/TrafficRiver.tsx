import { Tooltip } from 'antd';
import { SERIES_COLORS, dataTextStyle, TYPE } from '../theme';
import type { VersionInfo } from '../api/types';

interface TrafficRiverProps {
  versions: VersionInfo[];
  height?: number;
  onSelect?: (version: string) => void;
}

export function versionColor(index: number): string {
  return SERIES_COLORS[index % SERIES_COLORS.length];
}

/**
 * Traffic river: per-model version weights as a proportional segmented bar.
 * The active (serving) version renders at full opacity with a solid dot;
 * inactive versions are muted. This is the product's signature element —
 * canary traffic split made visible.
 */
export function TrafficRiver({ versions, height = 12, onSelect }: TrafficRiverProps) {
  const total = versions.reduce((sum, v) => sum + v.weight, 0);
  if (versions.length === 0 || total <= 0) return <span style={dataTextStyle}>-</span>;

  return (
    <div style={{ minWidth: 140 }}>
      <div
        role="img"
        aria-label={versions.map((v) => `${v.version} ${v.weight}%`).join(', ')}
        style={{ display: 'flex', height, borderRadius: 3, overflow: 'hidden', width: '100%' }}
      >
        {versions.map((v, idx) => (
          <Tooltip key={v.version} title={`${v.version} — ${v.weight}%${v.active ? ' (active)' : ''}`}>
            <div
              onClick={onSelect ? () => onSelect(v.version) : undefined}
              style={{
                width: `${(v.weight / total) * 100}%`,
                background: versionColor(idx),
                opacity: v.active ? 1 : 0.4,
                cursor: onSelect ? 'pointer' : 'default',
                borderRight: idx < versions.length - 1 ? '2px solid #fff' : 'none',
              }}
            />
          </Tooltip>
        ))}
      </div>
      <div style={{ display: 'flex', gap: 12, marginTop: 4, flexWrap: 'wrap' }}>
        {versions.map((v, idx) => (
          <span
            key={v.version}
            style={{ ...dataTextStyle, fontSize: TYPE.eyebrow, color: '#4B5563', whiteSpace: 'nowrap' }}
          >
            <span style={{ color: versionColor(idx) }}>{v.active ? '●' : '○'}</span>
            {v.version} {v.weight}%
          </span>
        ))}
      </div>
    </div>
  );
}
