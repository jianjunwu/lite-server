import type { ReactNode } from 'react';
import { STATUS_COLORS, TYPE, dataTextStyle } from '../theme';
import { SPACE } from '../tokens';
import { useNeutrals } from '../context/ThemeModeContext';

type Tone = 'ink' | 'ready' | 'warning' | 'error';

interface StatNumProps {
  /** Short uppercase caption under the numeral (e.g. "FLEET QPS"). */
  label: ReactNode;
  value: ReactNode;
  /** Small unit or qualifier after the value (e.g. "ms"). */
  unit?: ReactNode;
  tone?: Tone;
  /** Optional content under the label (e.g. a sparkline). */
  children?: ReactNode;
}

/**
 * Data-zone big numeral: 64px tabular figure + short caption (plan §2.2).
 * No card chrome — whitespace does the grouping.
 */
export function StatNum({ label, value, unit, tone = 'ink', children }: StatNumProps) {
  const neutrals = useNeutrals();
  const color = tone === 'ink' ? neutrals.textPrimary : STATUS_COLORS[tone];
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[2], minWidth: 0 }}>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: SPACE[2] }}>
        <span
          style={{
            ...dataTextStyle,
            fontSize: TYPE.dataNum,
            fontWeight: 600,
            letterSpacing: '-0.02em',
            lineHeight: 1,
            color,
          }}
        >
          {value}
        </span>
        {unit && <span style={{ fontSize: TYPE.cardTitle, color: neutrals.textSecondary }}>{unit}</span>}
      </div>
      <span
        style={{
          fontSize: TYPE.eyebrow,
          fontWeight: 600,
          textTransform: 'uppercase',
          letterSpacing: '0.08em',
          color: neutrals.textSecondary,
        }}
      >
        {label}
      </span>
      {children}
    </div>
  );
}
