import { LoadingOutlined } from '@ant-design/icons';
import { STATUS_COLORS, TYPE } from '../theme';

type StatusKind = 'ready' | 'loading' | 'warning' | 'error' | 'offline';

/** Map free-form server status strings onto the five status kinds. */
export function statusKind(status: string | null | undefined): StatusKind {
  const s = (status ?? '').toLowerCase();
  if (['ready', 'loaded', 'ok', 'healthy', 'running', 'active'].includes(s)) return 'ready';
  if (['loading', 'starting', 'initializing', 'pending'].includes(s)) return 'loading';
  if (['degraded', 'warning', 'unloading', 'draining'].includes(s)) return 'warning';
  if (['failed', 'error', 'unhealthy'].includes(s)) return 'error';
  return 'offline';
}

export function StatusDot({ kind, size = 8 }: { kind: StatusKind; size?: number }) {
  const color = STATUS_COLORS[kind];
  if (kind === 'loading') {
    return <LoadingOutlined style={{ color, fontSize: size + 2 }} />;
  }
  return (
    <span
      aria-hidden
      style={{
        display: 'inline-block',
        width: size,
        height: size,
        borderRadius: '50%',
        background: color,
        flexShrink: 0,
      }}
    />
  );
}

/**
 * Quiet status badge: colored dot + colored text. Anomalies are what should
 * catch the eye — a screen full of solid tags hides them.
 */
export function StatusBadge({ status, text }: { status: string | null | undefined; text?: string }) {
  const kind = statusKind(status);
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: TYPE.secondary }}>
      <StatusDot kind={kind} />
      <span style={{ color: STATUS_COLORS[kind] }}>{text ?? status ?? 'unknown'}</span>
    </span>
  );
}
