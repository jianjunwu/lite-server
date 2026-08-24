import { Tooltip } from 'antd';
import { STATUS_COLORS } from '../theme';
import type { WorkerHealth } from '../api/types';

interface WorkerMatrixProps {
  workers: WorkerHealth[];
}

/** One colored square per worker: green = healthy, red = ejected. */
export function WorkerMatrix({ workers }: WorkerMatrixProps) {
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
      {workers.map((w) => {
        const color = w.healthy ? STATUS_COLORS.ready : STATUS_COLORS.error;
        const label = `worker ${w.worker_id}: ${w.healthy ? 'healthy' : w.ejected ? 'ejected' : 'unhealthy'}`;
        return (
          <Tooltip key={w.worker_id} title={label}>
            <div
              aria-label={label}
              style={{
                width: 18,
                height: 18,
                borderRadius: 4,
                background: color,
                border: `1px solid ${color}`,
              }}
            />
          </Tooltip>
        );
      })}
    </div>
  );
}
