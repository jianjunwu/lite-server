import { describe, expect, it } from 'vitest';
import { formatAge, formatBytes, formatMs, formatNumber } from '../components/format';

describe('formatNumber', () => {
  it('should_format_thousands_with_k_suffix', () => {
    expect(formatNumber(1234)).toBe('1.2k');
  });
  it('should_format_millions_with_M_suffix', () => {
    expect(formatNumber(2_500_000)).toBe('2.5M');
  });
  it('should_keep_small_integers_as_is', () => {
    expect(formatNumber(42)).toBe('42');
  });
  it('should_return_dash_for_non_finite', () => {
    expect(formatNumber(NaN)).toBe('-');
  });
});

describe('formatBytes', () => {
  it('should_format_megabytes', () => {
    expect(formatBytes(48 * 1024 * 1024)).toBe('48.0 MB');
  });
  it('should_format_bytes_below_1KB', () => {
    expect(formatBytes(512)).toBe('512.0 B');
  });
});

describe('formatMs', () => {
  it('should_format_sub_millisecond_as_microseconds', () => {
    expect(formatMs(0.5)).toBe('500µs');
  });
  it('should_format_seconds', () => {
    expect(formatMs(1500)).toBe('1.50s');
  });
});

describe('formatAge', () => {
  it('should_round_to_the_largest_unit_only', () => {
    // "36m8s" reads like a countdown; a quiet "36m" is enough for a table.
    expect(formatAge(1000, 1000 + 182)).toBe('3m');
    expect(formatAge(1000, 1000 + 45)).toBe('45s');
    expect(formatAge(1000, 1000 + 7300)).toBe('2h');
    expect(formatAge(1000, 1000 + 3 * 86400 + 7200)).toBe('3d');
  });
  it('should_return_dash_for_null', () => {
    expect(formatAge(null)).toBe('-');
  });
});
