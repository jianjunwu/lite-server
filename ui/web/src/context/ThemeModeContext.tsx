import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';

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

// Apple palette — mirrors tokens.css (:root / [data-theme='dark']).
const LIGHT: Neutrals = {
  border: '#E8E8ED',
  textPrimary: '#1D1D1F',
  textSecondary: '#6E6E73',
  textMuted: '#86868B',
  bgPage: '#FFFFFF',
  diffAddBg: '#ECFDF5',
  diffRemoveBg: '#FEF2F2',
};

const DARK: Neutrals = {
  border: '#38383A',
  textPrimary: '#F5F5F7',
  textSecondary: '#98989D',
  textMuted: '#6E6E73',
  bgPage: '#000000',
  diffAddBg: 'rgba(22, 163, 74, 0.16)',
  diffRemoveBg: 'rgba(220, 38, 38, 0.16)',
};

export interface ChartColors {
  axis: string;
  split: string;
  text: string;
}

const CHART_LIGHT: ChartColors = { axis: '#D2D2D7', split: '#E8E8ED', text: '#6E6E73' };
const CHART_DARK: ChartColors = { axis: '#38383A', split: '#2C2C2E', text: '#98989D' };

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

  // Drive the tokens.css variable set from the same source of truth.
  useEffect(() => {
    document.documentElement.dataset.theme = dark ? 'dark' : 'light';
  }, [dark]);

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
