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
