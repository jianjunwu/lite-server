import { Tag } from 'antd';
import {
  CheckCircleFilled,
  LoadingOutlined,
  WarningFilled,
  CloseCircleFilled,
  MinusCircleFilled,
} from '@ant-design/icons';
import { STATUS_COLORS } from '../theme';

type StatusKind = 'ready' | 'loading' | 'warning' | 'error' | 'offline';

const ICONS: Record<StatusKind, React.ReactNode> = {
  ready: <CheckCircleFilled />,
  loading: <LoadingOutlined />,
  warning: <WarningFilled />,
  error: <CloseCircleFilled />,
  offline: <MinusCircleFilled />,
};

/** Map free-form server status strings onto the five status kinds. */
export function statusKind(status: string | null | undefined): StatusKind {
  const s = (status ?? '').toLowerCase();
  if (['ready', 'loaded', 'ok', 'healthy', 'running', 'active'].includes(s)) return 'ready';
  if (['loading', 'starting', 'initializing', 'pending'].includes(s)) return 'loading';
  if (['degraded', 'warning', 'unloading', 'draining'].includes(s)) return 'warning';
  if (['failed', 'error', 'unhealthy'].includes(s)) return 'error';
  return 'offline';
}

export function StatusBadge({ status, text }: { status: string | null | undefined; text?: string }) {
  const kind = statusKind(status);
  return (
    <Tag icon={ICONS[kind]} color={STATUS_COLORS[kind]} style={{ color: '#fff', border: 'none' }}>
      {text ?? status ?? 'unknown'}
    </Tag>
  );
}
