import { createContext, useContext, useMemo, useState, type ReactNode } from 'react';

const STORAGE_KEY = 'lite-ui-theme-mode';

export interface Neutrals {
  border: string;
  textPrimary: string;
  textSecondary: string;
  textMuted: string;
  bgPage: string;
  /** Subtle tinted backgrounds for diff add/remove lines. */
  diffAddBg: string;
  diffRemoveBg: string;
}

const LIGHT: Neutrals = {
  border: '#E5E7EB',
  textPrimary: '#111827',
  textSecondary: '#6B7280',
  textMuted: '#9CA3AF',
  bgPage: '#F9FAFB',
  diffAddBg: '#ECFDF5',
  diffRemoveBg: '#FEF2F2',
};

const DARK: Neutrals = {
  border: '#374151',
  textPrimary: '#F3F4F6',
  textSecondary: '#9CA3AF',
  textMuted: '#6B7280',
  bgPage: '#0B0F19',
  diffAddBg: 'rgba(22, 163, 74, 0.16)',
  diffRemoveBg: 'rgba(220, 38, 38, 0.16)',
};

export interface ChartColors {
  axis: string;
  split: string;
  text: string;
}

const CHART_LIGHT: ChartColors = { axis: '#D1D5DB', split: '#E5E7EB', text: '#6B7280' };
const CHART_DARK: ChartColors = { axis: '#4B5563', split: '#374151', text: '#9CA3AF' };

interface ThemeModeValue {
  dark: boolean;
  toggle: () => void;
  neutrals: Neutrals;
  chartColors: ChartColors;
}

const ThemeModeContext = createContext<ThemeModeValue>({
  dark: false,
  toggle: () => {},
  neutrals: LIGHT,
  chartColors: CHART_LIGHT,
});

export function ThemeModeProvider({ children }: { children: ReactNode }) {
  const [dark, setDark] = useState(() => localStorage.getItem(STORAGE_KEY) === 'dark');

  const value = useMemo<ThemeModeValue>(
    () => ({
      dark,
      toggle: () =>
        setDark((cur) => {
          localStorage.setItem(STORAGE_KEY, cur ? 'light' : 'dark');
          return !cur;
        }),
      neutrals: dark ? DARK : LIGHT,
      chartColors: dark ? CHART_DARK : CHART_LIGHT,
    }),
    [dark],
  );

  return <ThemeModeContext.Provider value={value}>{children}</ThemeModeContext.Provider>;
}

export function useThemeMode(): ThemeModeValue {
  return useContext(ThemeModeContext);
}

export function useNeutrals(): Neutrals {
  return useContext(ThemeModeContext).neutrals;
}

export function useChartColors(): ChartColors {
  return useContext(ThemeModeContext).chartColors;
}
