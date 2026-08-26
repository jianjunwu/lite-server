import { theme, type ThemeConfig } from 'antd';

// Design tokens per .claude/apple-ui-redesign-plan.md §2:
// Apple-style visual language — single accent, white cards with diffuse
// shadows, 24px card radius, pill buttons. Values mirror tokens.css.
export function getThemeConfig(dark: boolean): ThemeConfig {
  return {
    algorithm: dark ? [theme.darkAlgorithm, theme.compactAlgorithm] : [theme.defaultAlgorithm, theme.compactAlgorithm],
    token: {
      colorPrimary: dark ? '#0A84FF' : '#0071E3',
      colorInfo: dark ? '#0A84FF' : '#0071E3',
      colorText: dark ? '#F5F5F7' : '#1D1D1F',
      colorTextSecondary: dark ? '#98989D' : '#6E6E73',
      colorBgLayout: dark ? '#000000' : '#FFFFFF',
      colorBgContainer: dark ? '#1C1C1E' : '#FFFFFF',
      colorBgElevated: dark ? '#2C2C2E' : '#FFFFFF',
      colorFillSecondary: dark ? '#2C2C2E' : '#F5F5F7',
      colorBorderSecondary: dark ? '#38383A' : '#E8E8ED',
      borderRadius: 12,
      borderRadiusLG: 24,
      fontSize: 14,
      fontFamily:
        "-apple-system, 'SF Pro Text', Inter, 'PingFang SC', 'Noto Sans SC', 'Helvetica Neue', Arial, sans-serif",
    },
    components: {
      Layout: {
        headerHeight: 56,
        headerBg: dark ? '#000000' : '#FFFFFF',
        siderBg: dark ? '#000000' : '#FFFFFF',
        bodyBg: dark ? '#000000' : '#FFFFFF',
        // The stock trigger bar is antd navy; blend it into the sider.
        triggerBg: dark ? '#000000' : '#FFFFFF',
        triggerColor: dark ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.45)',
      },
      Card: {
        borderRadiusLG: 24,
        boxShadowTertiary: dark ? '0 4px 24px rgba(0,0,0,0.4)' : '0 4px 24px rgba(0,0,0,0.06)',
      },
      Button: {
        borderRadius: 980,
        borderRadiusSM: 980,
        borderRadiusLG: 980,
      },
      Menu: {
        itemBorderRadius: 12,
        itemSelectedBg: dark ? '#2C2C2E' : '#F5F5F7',
        itemSelectedColor: dark ? '#0A84FF' : '#0071E3',
      },
      Table: {
        headerBg: dark ? '#1C1C1E' : '#FFFFFF',
      },
    },
  };
}

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
  '#0071E3',
  '#0EA5E9',
  '#8B5CF6',
  '#EC4899',
  '#F59E0B',
  '#10B981',
  '#6366F1',
  '#14B8A6',
];

export const MONO_FONT = "'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace";

// Typography: sans for prose (what humans read), mono for data (what the
// machine reports — metrics, version ids, timestamps, request ids).
export const DATA_FONT = MONO_FONT;

// Type scale — the only sizes allowed in the UI.
export const TYPE = {
  eyebrow: 12, // uppercase labels, card eyebrows
  secondary: 13, // secondary text, table meta
  body: 14, // default prose
  lead: 15, // hero-layer prose (subtitles, statements)
  cardTitle: 17, // card / section titles
  pageTitle: 32, // page-level title
  hero: 56, // hero status statement
  dataNum: 64, // big data-zone numerals
} as const;

export const dataTextStyle: React.CSSProperties = {
  fontFamily: DATA_FONT,
  fontVariantNumeric: 'tabular-nums',
};
