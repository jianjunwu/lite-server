import type { ReactNode } from 'react';
import { Breadcrumb, Button } from 'antd';
import { ArrowLeftOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { TYPE } from '../theme';
import { useNeutrals } from '../context/ThemeModeContext';

interface BreadcrumbItem {
  title: ReactNode;
  href?: string;
}

interface PageHeaderProps {
  title: ReactNode;
  subtitle?: ReactNode;
  extra?: ReactNode;
  /** Hierarchy trail above the title, e.g. Models / {name} / {version}. */
  breadcrumb?: BreadcrumbItem[];
  /** Back arrow left of the title; pages pass a deterministic destination. */
  onBack?: () => void;
}

/** Single page-title hierarchy: 32px semibold title, optional 13px subtitle. */
export function PageHeader({ title, subtitle, extra, breadcrumb, onBack }: PageHeaderProps) {
  const { t } = useTranslation();
  const neutrals = useNeutrals();
  return (
    <div style={{ marginBottom: 16 }}>
      {breadcrumb && (
        <Breadcrumb
          style={{ fontSize: TYPE.secondary, marginBottom: 4 }}
          items={breadcrumb.map((item) => ({
            title: item.href ? <Link to={item.href}>{item.title}</Link> : item.title,
          }))}
        />
      )}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {onBack && (
            <Button
              type="text"
              size="small"
              icon={<ArrowLeftOutlined />}
              aria-label={t('common.back')}
              onClick={onBack}
            />
          )}
          <div>
            <h1 style={{ fontSize: TYPE.pageTitle, fontWeight: 600, letterSpacing: '-0.02em', margin: 0, lineHeight: 1.2 }}>{title}</h1>
            {subtitle && (
              <div style={{ fontSize: TYPE.secondary, color: neutrals.textSecondary, marginTop: 2 }}>{subtitle}</div>
            )}
          </div>
        </div>
        {extra}
      </div>
    </div>
  );
}
