import { useState } from 'react';
import { Button, InputNumber, Space, Table, Tag } from 'antd';
import { useTranslation } from 'react-i18next';
import { validateWeights } from '../api/mutations';
import { useApplyRouting } from './useApplyRouting';
import type { VersionInfo } from '../api/types';
import { dataTextStyle } from '../theme';

interface RoutingEditorProps {
  model: string;
  versions: VersionInfo[];
  onClose: () => void;
}

/** Inline routing edit: per-version weight inputs, live sum validation
 * (must equal 100), before/after diff in the confirm modal. */
export function RoutingEditor({ model, versions, onClose }: RoutingEditorProps) {
  const { t } = useTranslation();
  const { apply, busy } = useApplyRouting(model);
  const [weights, setWeights] = useState<Record<string, number>>(
    Object.fromEntries(versions.map((v) => [v.version, v.weight])),
  );

  const validation = validateWeights(weights);
  const before = Object.fromEntries(versions.map((v) => [v.version, v.weight]));

  const confirm = () => apply(weights, before, onClose);

  return (
    <div style={{ marginTop: 16 }}>
      <Table<VersionInfo>
        size="small"
        rowKey="version"
        dataSource={versions}
        pagination={false}
        columns={[
          { title: t('common.version'), dataIndex: 'version', width: 110, render: (v: string) => <span style={dataTextStyle}>{v}</span> },
          {
            title: t('routing.current'),
            dataIndex: 'weight',
            width: 110,
            render: (w: number) => <span style={dataTextStyle}>{w}%</span>,
          },
          {
            title: t('routing.new'),
            key: 'new',
            width: 160,
            render: (_: unknown, v: VersionInfo) => (
              <InputNumber
                min={0}
                max={100}
                value={weights[v.version]}
                onChange={(val) => setWeights((prev) => ({ ...prev, [v.version]: val ?? 0 }))}
                addonAfter="%"
                size="small"
              />
            ),
          },
        ]}
      />
      <Space style={{ marginTop: 12 }} size="middle">
        <Tag color={validation.ok ? '#16A34A' : '#DC2626'} style={{ border: 'none', color: '#fff' }}>
          {t('routing.sum')}: {validation.sum}/100
        </Tag>
        <Button type="primary" size="small" disabled={!validation.ok || busy} onClick={confirm}>
          {t('routing.apply')}
        </Button>
        <Button size="small" onClick={onClose} disabled={busy}>
          {t('routing.cancel')}
        </Button>
      </Space>
    </div>
  );
}
