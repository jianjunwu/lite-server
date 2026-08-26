import { describe, expect, it } from 'vitest';
import { memoryPercent, summarizeReadings } from '../components/accelerator';
import type { AcceleratorReading } from '../api/types';

function reading(extra: Partial<AcceleratorReading> = {}): AcceleratorReading {
  return {
    device: '0',
    accel: 'cuda',
    utilization_percent: null,
    memory_used_bytes: null,
    memory_total_bytes: null,
    temperature_celsius: null,
    updated_at: 0,
    ...extra,
  };
}

describe('memoryPercent', () => {
  it('should_return_ratio_percent_when_both_sides_reported', () => {
    expect(memoryPercent(reading({ memory_used_bytes: 4, memory_total_bytes: 8 }))).toBe(50);
  });

  it('should_return_null_when_either_side_missing', () => {
    expect(memoryPercent(reading({ memory_used_bytes: 4 }))).toBeNull();
    expect(memoryPercent(reading({ memory_total_bytes: 8 }))).toBeNull();
    expect(memoryPercent(reading())).toBeNull();
  });

  it('should_return_null_for_non_positive_total', () => {
    expect(memoryPercent(reading({ memory_used_bytes: 4, memory_total_bytes: 0 }))).toBeNull();
  });

  it('should_clamp_over_100_when_used_exceeds_total', () => {
    expect(memoryPercent(reading({ memory_used_bytes: 12, memory_total_bytes: 8 }))).toBe(100);
  });
});

describe('summarizeReadings', () => {
  it('should_return_nulls_for_an_empty_fleet', () => {
    expect(summarizeReadings([])).toEqual({
      devices: 0,
      avgUtilization: null,
      memoryUsedBytes: null,
      memoryTotalBytes: null,
    });
  });

  it('should_average_utilization_over_reporting_devices_only', () => {
    const s = summarizeReadings([
      reading({ device: '0', utilization_percent: 60 }),
      reading({ device: '1', utilization_percent: 30 }),
      reading({ device: '2' }), // not reporting utilization
    ]);
    expect(s.devices).toBe(3);
    expect(s.avgUtilization).toBe(45);
  });

  it('should_sum_memory_across_reporting_devices', () => {
    const s = summarizeReadings([
      reading({ device: '0', memory_used_bytes: 100, memory_total_bytes: 200 }),
      reading({ device: '1', memory_used_bytes: 50 }), // no total
    ]);
    expect(s.memoryUsedBytes).toBe(150);
    expect(s.memoryTotalBytes).toBe(200);
  });
});
