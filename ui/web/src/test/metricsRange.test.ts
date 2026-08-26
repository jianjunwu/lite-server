import { describe, expect, it } from 'vitest';
import { fieldState, stepForRange, trimToRange } from '../components/timelineChart';
import type { TimelineEntry, TimelineSnapshot } from '../api/types';

function entry(ts: number, extra: Partial<TimelineEntry> = {}): TimelineEntry {
  return {
    timestamp: ts,
    qps: 0,
    p99_ms: 0,
    queue_depth: 0,
    active_workers: 0,
    active_streams: 0,
    ...extra,
  };
}

function snap(entries: TimelineEntry[], version = '1'): TimelineSnapshot {
  return { model: 'm', version, entries };
}

describe('stepForRange', () => {
  it('should_return_1_when_range_fits_target_points', () => {
    expect(stepForRange(300, 10)).toBe(1);
  });

  it('should_downsample_to_stay_near_target_points', () => {
    expect(stepForRange(3600, 10)).toBe(2); // 360 raw points at 10s spacing
    expect(stepForRange(86400, 60)).toBe(5); // 1440 raw points at 60s spacing
  });

  it('should_round_up_partial_steps', () => {
    expect(stepForRange(3010, 10)).toBe(2);
  });

  it('should_never_return_less_than_1', () => {
    expect(stepForRange(0, 10)).toBe(1);
    expect(stepForRange(300, 0)).toBe(1); // zero/unknown interval stays safe
  });
});

describe('trimToRange', () => {
  const entries = [entry(100), entry(200), entry(300), entry(400)];

  it('should_keep_entries_within_range_of_the_newest_timestamp', () => {
    const out = trimToRange([snap(entries)], 150);
    expect(out[0].entries.map((e) => e.timestamp)).toEqual([300, 400]);
  });

  it('should_anchor_at_the_newest_entry_across_snapshots_not_wall_clock', () => {
    const out = trimToRange([snap([entry(50)], 'old'), snap(entries, 'new')], 150);
    expect(out[0].entries).toEqual([]);
    expect(out[1].entries.map((e) => e.timestamp)).toEqual([300, 400]);
  });

  it('should_keep_the_boundary_timestamp_inclusive', () => {
    const out = trimToRange([snap(entries)], 100);
    expect(out[0].entries.map((e) => e.timestamp)).toEqual([300, 400]);
  });

  it('should_return_everything_when_range_is_null', () => {
    expect(trimToRange([snap(entries)], null)[0].entries).toHaveLength(4);
  });

  it('should_pass_through_when_there_are_no_entries', () => {
    expect(trimToRange([snap([])], 300)[0].entries).toEqual([]);
  });
});

describe('fieldState', () => {
  it('should_be_unsupported_when_every_entry_lacks_the_field', () => {
    expect(fieldState([snap([entry(1), entry(2)])], 'ttft_p99_ms')).toBe('unsupported');
  });

  it('should_be_not_reported_when_field_present_but_all_null', () => {
    const entries = [entry(1, { tokens_per_s: null }), entry(2, { tokens_per_s: null })];
    expect(fieldState([snap(entries)], 'tokens_per_s')).toBe('not-reported');
  });

  it('should_be_ok_when_any_entry_has_a_value', () => {
    const entries = [entry(1, { tokens_per_s: null }), entry(2, { tokens_per_s: 3 })];
    expect(fieldState([snap(entries)], 'tokens_per_s')).toBe('ok');
  });

  it('should_be_ok_for_zero_valued_numeric_fields', () => {
    expect(fieldState([snap([entry(1, { retries_per_s: 0 })])], 'retries_per_s')).toBe('ok');
  });

  it('should_be_unsupported_when_only_some_snapshots_lack_the_field', () => {
    // Mixed fleet: a snapshot from an old instance (field absent) alongside a
    // new one (field present) — the field IS supported where present.
    const mixed = [snap([entry(1)], 'old'), snap([entry(2, { cpu_percent: 12 })], 'new')];
    expect(fieldState(mixed, 'cpu_percent')).toBe('ok');
  });
});
