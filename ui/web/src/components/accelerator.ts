import type { AcceleratorReading } from '../api/types';

/** Memory usage as a 0-100 Progress percent; null when either side is
 * unreported or the total is not positive. */
export function memoryPercent(r: AcceleratorReading): number | null {
  if (r.memory_used_bytes == null || r.memory_total_bytes == null || r.memory_total_bytes <= 0) {
    return null;
  }
  return Math.min(100, (r.memory_used_bytes / r.memory_total_bytes) * 100);
}

export interface AcceleratorSummary {
  devices: number;
  /** Mean utilization across devices that report it; null when none do. */
  avgUtilization: number | null;
  /** Sums across reporting devices; null when no device reports the field. */
  memoryUsedBytes: number | null;
  memoryTotalBytes: number | null;
}

/** Fleet-level rollup for the MetricsPage hero stats. */
export function summarizeReadings(readings: AcceleratorReading[]): AcceleratorSummary {
  let utilSum = 0;
  let utilCount = 0;
  let usedSum = 0;
  let usedSeen = false;
  let totalSum = 0;
  let totalSeen = false;
  for (const r of readings) {
    if (r.utilization_percent != null) {
      utilSum += r.utilization_percent;
      utilCount += 1;
    }
    if (r.memory_used_bytes != null) {
      usedSum += r.memory_used_bytes;
      usedSeen = true;
    }
    if (r.memory_total_bytes != null) {
      totalSum += r.memory_total_bytes;
      totalSeen = true;
    }
  }
  return {
    devices: readings.length,
    avgUtilization: utilCount > 0 ? utilSum / utilCount : null,
    memoryUsedBytes: usedSeen ? usedSum : null,
    memoryTotalBytes: totalSeen ? totalSum : null,
  };
}
