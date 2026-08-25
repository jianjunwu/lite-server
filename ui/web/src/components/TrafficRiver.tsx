import { useRef, useState } from 'react';
import { Button, Space, Tag, Tooltip } from 'antd';
import { useTranslation } from 'react-i18next';
import { SERIES_COLORS, dataTextStyle, TYPE } from '../theme';
import type { VersionInfo } from '../api/types';
import { useNeutrals } from '../context/ThemeModeContext';
import { boundaryDrag } from './trafficDrag';
import { useApplyRouting } from './useApplyRouting';

interface TrafficRiverProps {
  versions: VersionInfo[];
  height?: number;
  onSelect?: (version: string) => void;
  /** Model name — required for the editable apply flow. */
  model?: string;
  /** Show divider handles: drag or arrow-key to shift weight between
   * adjacent versions, then Apply. Requires `model`. */
  editable?: boolean;
}

export function versionColor(index: number): string {
  return SERIES_COLORS[index % SERIES_COLORS.length];
}

/** Pixels of pointer travel before a press on a handle counts as a drag. */
const DRAG_THRESHOLD_PX = 4;

/**
 * Traffic river: per-model version weights as a proportional segmented bar.
 * The active (serving) version renders at full opacity with a solid dot;
 * inactive versions are muted. This is the product's signature element —
 * canary traffic split made visible.
 *
 * In editable mode the segment dividers become handles: dragging one moves
 * weight between the two adjacent versions only, so the total stays at 100
 * by construction. Edits are a local draft until Apply (confirm modal +
 * setRouting) or Reset.
 */
export function TrafficRiver({ versions, height = 12, onSelect, model, editable }: TrafficRiverProps) {
  const { t } = useTranslation();
  const neutrals = useNeutrals();
  const { apply, busy } = useApplyRouting(model ?? '');
  const barRef = useRef<HTMLDivElement>(null);
  const dragRef = useRef<{ index: number; startX: number; base: number[]; moved: boolean } | null>(null);
  const [draft, setDraft] = useState<Record<string, number> | null>(null);

  const serverWeights = versions.map((v) => v.weight);
  // Drafts are keyed by version name and go stale the moment the version set
  // changes (versions refetch every 10s) — otherwise weights built against
  // old positions would be applied to the wrong versions.
  const draftStale =
    draft !== null &&
    (Object.keys(draft).length !== versions.length || versions.some((v) => !(v.version in draft)));
  const weights = draft && !draftStale ? versions.map((v) => draft[v.version]) : serverWeights;
  const total = serverWeights.reduce((sum, w) => sum + w, 0);
  const canEdit = Boolean(editable && model && versions.length > 1 && total > 0);

  if (versions.length === 0 || total <= 0) return <span style={dataTextStyle}>-</span>;

  const setDraftFromWeights = (next: number[]) =>
    setDraft(Object.fromEntries(versions.map((v, i) => [v.version, next[i]])));

  const dragBy = (index: number, deltaPct: number) =>
    setDraftFromWeights(boundaryDrag(weights, index, deltaPct));

  const onHandlePointerDown = (e: React.PointerEvent<HTMLDivElement>, index: number) => {
    e.preventDefault();
    e.stopPropagation();
    e.currentTarget.setPointerCapture(e.pointerId);
    dragRef.current = { index, startX: e.clientX, base: weights, moved: false };
  };

  const onHandlePointerMove = (e: React.PointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current;
    if (!drag || !barRef.current) return;
    const dx = e.clientX - drag.startX;
    if (!drag.moved && Math.abs(dx) < DRAG_THRESHOLD_PX) return;
    const barWidth = barRef.current.getBoundingClientRect().width;
    if (barWidth <= 0) return;
    drag.moved = true;
    setDraftFromWeights(boundaryDrag(drag.base, drag.index, (dx / barWidth) * total));
  };

  const onHandlePointerUp = () => {
    dragRef.current = null;
  };

  const onHandleKeyDown = (e: React.KeyboardEvent<HTMLDivElement>, index: number) => {
    const step = e.shiftKey ? 10 : 1;
    if (e.key === 'ArrowRight') {
      e.preventDefault();
      dragBy(index, step);
    } else if (e.key === 'ArrowLeft') {
      e.preventDefault();
      dragBy(index, -step);
    }
  };

  const applyDraft = () => {
    if (!draft || draftStale) return;
    apply(
      Object.fromEntries(versions.map((v) => [v.version, draft[v.version]])),
      Object.fromEntries(versions.map((v, i) => [v.version, serverWeights[i]])),
      () => setDraft(null),
    );
  };

  // Cumulative left offsets (%) of the dividers between segments.
  const boundaries: number[] = [];
  weights.slice(0, -1).reduce((acc, w, i) => {
    const next = acc + (w / total) * 100;
    boundaries[i] = next;
    return next;
  }, 0);

  return (
    <div style={{ minWidth: 140 }}>
      <div style={{ position: 'relative' }} ref={barRef}>
        <div
          role="img"
          aria-label={versions.map((v, i) => `${v.version} ${weights[i]}%`).join(', ')}
          style={{ display: 'flex', height, borderRadius: 3, overflow: 'hidden', width: '100%' }}
        >
          {versions.map((v, idx) => (
            <Tooltip key={v.version} title={`${v.version} — ${weights[idx]}%${v.active ? ' (active)' : ''}`}>
              <div
                className="river-seg"
                onClick={onSelect ? () => onSelect(v.version) : undefined}
                style={{
                  width: `${(weights[idx] / total) * 100}%`,
                  background: versionColor(idx),
                  opacity: v.active ? 1 : 0.4,
                  cursor: onSelect ? 'pointer' : 'default',
                  borderRight: idx < versions.length - 1 ? '2px solid #fff' : 'none',
                }}
              />
            </Tooltip>
          ))}
        </div>
        {canEdit &&
          boundaries.map((left, i) => (
            <div
              key={`${versions[i].version}|${versions[i + 1].version}`}
              role="slider"
              tabIndex={0}
              aria-label={t('routing.dragHandle', { a: versions[i].version, b: versions[i + 1].version })}
              aria-valuemin={0}
              aria-valuemax={weights[i] + weights[i + 1]}
              aria-valuenow={weights[i]}
              onPointerDown={(e) => onHandlePointerDown(e, i)}
              onPointerMove={onHandlePointerMove}
              onPointerUp={onHandlePointerUp}
              onKeyDown={(e) => onHandleKeyDown(e, i)}
              style={{
                position: 'absolute',
                left: `${left}%`,
                top: -2,
                height: height + 4,
                width: 10,
                transform: 'translateX(-50%)',
                cursor: 'col-resize',
                zIndex: 2,
              }}
            >
              <div
                style={{
                  margin: '0 auto',
                  width: 2,
                  height: '100%',
                  background: '#fff',
                  boxShadow: '0 0 0 1px rgba(0, 0, 0, 0.3)',
                  borderRadius: 1,
                }}
              />
            </div>
          ))}
      </div>
      <div style={{ display: 'flex', gap: 12, marginTop: 4, flexWrap: 'wrap', alignItems: 'center' }}>
        {versions.map((v, idx) => (
          <span
            key={v.version}
            style={{ ...dataTextStyle, fontSize: TYPE.eyebrow, color: neutrals.textSecondary, whiteSpace: 'nowrap' }}
          >
            <span style={{ color: versionColor(idx) }}>{v.active ? '●' : '○'}</span>
            {v.version} {weights[idx]}%
          </span>
        ))}
        {canEdit && draft && !draftStale && (
          <Space size="small">
            <Tag color="#B45309" style={{ border: 'none', color: '#fff', marginInlineEnd: 0 }}>
              {t('routing.modified')}
            </Tag>
            <Button type="primary" size="small" disabled={busy} onClick={applyDraft}>
              {t('routing.apply')}
            </Button>
            <Button size="small" disabled={busy} onClick={() => setDraft(null)}>
              {t('routing.reset')}
            </Button>
          </Space>
        )}
      </div>
    </div>
  );
}
