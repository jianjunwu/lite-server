/**
 * TS mirror of tokens.css — the single source of px values for inline
 * styles. Keep in sync with tokens.css (.claude/apple-ui-redesign-plan.md §2).
 */
export const SPACE = {
  1: 4,
  2: 8,
  3: 12,
  4: 16,
  5: 24,
  6: 32,
  7: 48,
  8: 80,
} as const;

export const RADIUS = {
  lg: 24,
  sm: 12,
  pill: 980,
} as const;

/** Section gap in the hero layer (>=80 per plan); data layer uses 5/6. */
export const SECTION_GAP = SPACE[8];

/** Stagger delay for route-enter reveals (40ms per block). */
export function revealStyle(order: number): React.CSSProperties {
  return { animationDelay: `${order * 40}ms` };
}
