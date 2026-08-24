import { describe, expect, it } from 'vitest';
import { boundaryDrag } from '../components/trafficDrag';

describe('boundaryDrag', () => {
  it('should_move_weight_from_right_to_left_on_positive_delta', () => {
    expect(boundaryDrag([60, 40], 0, 10)).toEqual([70, 30]);
  });

  it('should_move_weight_from_left_to_right_on_negative_delta', () => {
    expect(boundaryDrag([60, 40], 0, -15)).toEqual([45, 55]);
  });

  it('should_clamp_left_segment_at_zero', () => {
    expect(boundaryDrag([60, 40], 0, -100)).toEqual([0, 100]);
  });

  it('should_clamp_right_segment_at_zero', () => {
    expect(boundaryDrag([60, 40], 0, 100)).toEqual([100, 0]);
  });

  it('should_only_touch_the_adjacent_pair', () => {
    expect(boundaryDrag([50, 30, 20], 1, 10)).toEqual([50, 40, 10]);
    expect(boundaryDrag([50, 30, 20], 0, -20)).toEqual([30, 50, 20]);
  });

  it('should_keep_the_total_invariant', () => {
    const result = boundaryDrag([33, 33, 34], 0, 7.6);
    expect(result.reduce((a, b) => a + b, 0)).toBe(100);
  });

  it('should_snap_to_integer_weights', () => {
    const result = boundaryDrag([60, 40], 0, 3.4);
    expect(result.every((w) => Number.isInteger(w))).toBe(true);
  });

  it('should_not_mutate_the_input', () => {
    const input = [60, 40];
    boundaryDrag(input, 0, 10);
    expect(input).toEqual([60, 40]);
  });
});
