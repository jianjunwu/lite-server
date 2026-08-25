import type { ReactNode } from 'react';
import { STATUS_COLORS, TYPE } from '../theme';
import { SPACE, revealStyle } from '../tokens';
import { useNeutrals } from '../context/ThemeModeContext';

type Tone = 'ink' | 'ready' | 'warning' | 'error';

interface PageHeroProps {
  /** Small uppercase page label above the statement (e.g. "Overview"). */
  eyebrow: ReactNode;
  /** The live status statement — the page's single thesis, 56px. */
  statement: ReactNode;
  tone?: Tone;
  /** Supporting line under the statement. */
  subline?: ReactNode;
  /** Actions / selectors, right-aligned at the baseline. */
  extra?: ReactNode;
  /** Show a live indicator next to the eyebrow. */
  live?: boolean;
}

/**
 * Hero layer of a page: one live statement, generous whitespace.
 * The statement is generated from real-time data — quiet good news when
 * all is well, colored when something needs attention (plan §3).
 */
export function PageHero({ eyebrow, statement, tone = 'ink', subline, extra, live }: PageHeroProps) {
  const neutrals = useNeutrals();
  const color = tone === 'ink' ? neutrals.textPrimary : STATUS_COLORS[tone];
  return (
    <div className="reveal" style={{ marginBottom: SPACE[8] }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: SPACE[2], marginBottom: SPACE[3] }}>
        <span
          style={{
            fontSize: TYPE.eyebrow,
            fontWeight: 600,
            textTransform: 'uppercase',
            letterSpacing: '0.08em',
            color: neutrals.textSecondary,
          }}
        >
          {eyebrow}
        </span>
        {live && (
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: SPACE[1] }}>
            <span
              aria-hidden
              style={{ width: 6, height: 6, borderRadius: '50%', background: STATUS_COLORS.ready }}
            />
            <span style={{ fontSize: TYPE.eyebrow, color: neutrals.textMuted }}>live</span>
          </span>
        )}
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', gap: SPACE[5] }}>
        <div style={{ minWidth: 0 }}>
          <h1
            style={{
              fontSize: TYPE.hero,
              fontWeight: 600,
              letterSpacing: '-0.02em',
              lineHeight: 1.1,
              margin: 0,
              color,
            }}
          >
            {statement}
          </h1>
          {subline && (
            <div style={{ fontSize: TYPE.lead, color: neutrals.textSecondary, marginTop: SPACE[3] }}>{subline}</div>
          )}
        </div>
        {extra && <div style={{ flexShrink: 0 }}>{extra}</div>}
      </div>
    </div>
  );
}

/** Stagger wrapper for blocks following the hero. */
export function Reveal({ order, children }: { order: number; children: ReactNode }) {
  return (
    <div className="reveal" style={revealStyle(order)}>
      {children}
    </div>
  );
}
