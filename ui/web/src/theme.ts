import { theme, type ThemeConfig } from 'antd';

// Design tokens per .claude/frontend-ui-plan.md §7.0:
// restrained engineering dashboard — single brand accent, border-separated
// cards (no shadows), compact density.
export const themeConfig: ThemeConfig = {
  algorithm: [theme.defaultAlgorithm, theme.compactAlgorithm],
  token: {
    colorPrimary: '#4F46E5',
    colorBorderSecondary: '#E5E7EB',
    borderRadius: 6,
    fontFamily:
      "Inter, system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif",
  },
  components: {
    Layout: {
      headerHeight: 48,
      headerBg: '#ffffff',
      siderBg: '#ffffff',
    },
    Card: {
      boxShadowTertiary: 'none',
    },
  },
};

// Status semantics: same hues everywhere (badges, worker matrix, donut,
// threshold lines). Always paired with icons/text (colorblind-safe).
export const STATUS_COLORS = {
  ready: '#16A34A',
  loading: '#2563EB',
  warning: '#D97706',
  error: '#DC2626',
  offline: '#9CA3AF',
} as const;

// Categorical palette for version series overlay — deliberately disjoint
// from the status hues.
export const SERIES_COLORS = [
  '#4F46E5',
  '#0EA5E9',
  '#8B5CF6',
  '#EC4899',
  '#F59E0B',
  '#10B981',
  '#6366F1',
  '#14B8A6',
];

export const MONO_FONT = "'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace";

// Typography: Inter for prose (what humans read), mono for data (what the
// machine reports — metrics, version ids, timestamps, request ids).
export const DATA_FONT = MONO_FONT;

// Type scale — the only sizes allowed in the UI.
export const TYPE = {
  eyebrow: 11, // uppercase labels, card eyebrows
  secondary: 12, // secondary text, table meta
  body: 13, // default prose
  cardTitle: 15, // card / section titles
  pageTitle: 20, // page-level title
  hero: 28, // hero numbers
} as const;

export const dataTextStyle: React.CSSProperties = {
  fontFamily: DATA_FONT,
  fontVariantNumeric: 'tabular-nums',
};

export const eyebrowStyle: React.CSSProperties = {
  fontSize: TYPE.eyebrow,
  textTransform: 'uppercase',
  letterSpacing: '0.06em',
  color: '#6B7280',
};
