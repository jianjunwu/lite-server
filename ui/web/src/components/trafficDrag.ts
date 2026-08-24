/**
 * Move `deltaPct` percentage points across the boundary between segments
 * `index` and `index + 1`. Only the adjacent pair changes — the pair sum
 * (and therefore the total) is invariant, so a full bar always stays at
 * 100. The result is clamped so neither segment goes below 0, and snapped
 * to integers to satisfy routing-weight validation. Returns a new array.
 */
export function boundaryDrag(weights: number[], index: number, deltaPct: number): number[] {
  const pairSum = weights[index] + weights[index + 1];
  const left = Math.round(Math.min(Math.max(weights[index] + deltaPct, 0), pairSum));
  const next = [...weights];
  next[index] = left;
  next[index + 1] = pairSum - left;
  return next;
}
