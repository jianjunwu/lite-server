import { Alert, Button, Card, Col, Empty, Progress, Row, Skeleton, Typography } from 'antd';
import { useTranslation } from 'react-i18next';
import { useAcceleratorMetrics } from '../api/hooks';
import type { AcceleratorReading } from '../api/types';
import { formatAge, formatBytes, formatNumber } from './format';
import { memoryPercent } from './accelerator';
import { SPACE } from '../tokens';
import { TYPE, dataTextStyle } from '../theme';

const { Text } = Typography;

function DeviceCard({ reading }: { reading: AcceleratorReading }) {
  const { t } = useTranslation();
  const memPct = memoryPercent(reading);
  return (
    <Card size="small" title={`${reading.device} · ${reading.accel}`}>
      <div style={{ display: 'flex', gap: SPACE[6], alignItems: 'center', flexWrap: 'wrap' }}>
        <div style={{ textAlign: 'center' }}>
          <Progress
            type="dashboard"
            size={96}
            percent={reading.utilization_percent ?? 0}
            format={(p) => (reading.utilization_percent == null ? '-' : `${formatNumber(p ?? 0)}%`)}
          />
          <div style={{ fontSize: TYPE.eyebrow, marginTop: SPACE[2] }}>{t('accelerator.utilization')}</div>
        </div>
        <div style={{ flex: 1, minWidth: 180 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <Text type="secondary" style={{ fontSize: TYPE.eyebrow }}>{t('accelerator.memory')}</Text>
            <span style={{ ...dataTextStyle, fontSize: 12 }}>
              {reading.memory_used_bytes != null ? formatBytes(reading.memory_used_bytes) : '-'}
              {' / '}
              {reading.memory_total_bytes != null ? formatBytes(reading.memory_total_bytes) : '-'}
            </span>
          </div>
          <Progress percent={memPct ?? 0} showInfo={false} status={memPct == null ? 'exception' : 'normal'} />
          <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: SPACE[3] }}>
            <Text type="secondary" style={{ fontSize: TYPE.eyebrow }}>{t('accelerator.temperature')}</Text>
            <span style={{ ...dataTextStyle, fontSize: 12 }}>
              {reading.temperature_celsius != null ? `${formatNumber(reading.temperature_celsius)} °C` : '-'}
            </span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: SPACE[3] }}>
            <Text type="secondary" style={{ fontSize: TYPE.eyebrow }}>{t('accelerator.updated')}</Text>
            <span style={{ ...dataTextStyle, fontSize: 12 }}>
              {t('common.ageAgo', { age: formatAge(reading.updated_at) })}
            </span>
          </div>
        </div>
      </div>
    </Card>
  );
}

interface AcceleratorPanelProps {
  instanceId: string | null;
  /** Poll interval; false pauses (MetricsPage pause control). */
  pollMs?: number | false;
}

/** M4: per-device accelerator cards fed by GET /metrics/accelerator. Stands
 * alone (fetches its own data) so the instance detail page can mount it
 * directly; GPU/MLU/NPU share one render path. */
export function AcceleratorPanel({ instanceId, pollMs = 10_000 }: AcceleratorPanelProps) {
  const { t } = useTranslation();
  const query = useAcceleratorMetrics(instanceId, pollMs);

  if (query.isLoading) {
    return <Skeleton active paragraph={{ rows: 4 }} />;
  }
  if (query.error) {
    return (
      <Alert
        type="error"
        showIcon
        message={query.error.message}
        action={<Button size="small" onClick={() => query.refetch()}>{t('common.retry')}</Button>}
      />
    );
  }
  // 404 → null: old instance, or features.accelerator_metrics disabled.
  if (query.data == null) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={t('accelerator.unsupported')} />;
  }
  if (query.data.length === 0) {
    return (
      <Empty
        image={Empty.PRESENTED_IMAGE_SIMPLE}
        description={
          <>
            <div>{t('accelerator.empty')}</div>
            <Text type="secondary" style={{ fontSize: TYPE.secondary }}>{t('accelerator.emptyHint')}</Text>
          </>
        }
      />
    );
  }
  return (
    <Row gutter={[SPACE[5], SPACE[5]]}>
      {query.data.map((r) => (
        <Col xs={24} lg={12} xxl={8} key={`${r.accel}:${r.device}`}>
          <DeviceCard reading={r} />
        </Col>
      ))}
    </Row>
  );
}
