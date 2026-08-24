import type { ReactNode } from 'react';
import { TYPE } from '../theme';
import { useNeutrals } from '../context/ThemeModeContext';

interface PageHeaderProps {
  title: ReactNode;
  subtitle?: ReactNode;
  extra?: ReactNode;
}

/** Single page-title hierarchy: 20px semibold title, optional 12px subtitle. */
export function PageHeader({ title, subtitle, extra }: PageHeaderProps) {
  const neutrals = useNeutrals();
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 16 }}>
      <div>
        <h1 style={{ fontSize: TYPE.pageTitle, fontWeight: 600, margin: 0, lineHeight: 1.3 }}>{title}</h1>
        {subtitle && <div style={{ fontSize: TYPE.secondary, color: neutrals.textSecondary, marginTop: 2 }}>{subtitle}</div>}
      </div>
      {extra}
    </div>
  );
}
