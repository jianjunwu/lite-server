import { Card, Statistic } from 'antd';
import type { ReactNode } from 'react';
import { dataTextStyle } from '../theme';

interface StatCardProps {
  title: ReactNode;
  value: string | number;
  suffix?: ReactNode;
  /** Optional small content under the value (e.g. a sparkline). */
  children?: ReactNode;
}

export function StatCard({ title, value, suffix, children }: StatCardProps) {
  return (
    <Card size="small">
      <Statistic
        title={title}
        value={value}
        suffix={suffix}
        valueStyle={{ ...dataTextStyle, fontWeight: 600 }}
      />
      {children}
    </Card>
  );
}
