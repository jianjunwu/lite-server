import { Collapse, Empty } from 'antd';
import { useTranslation } from 'react-i18next';
import { groupModelConfig } from './modelConfigSchema';
import { MONO_FONT, TYPE, dataTextStyle } from '../../theme';
import { SPACE } from '../../tokens';

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return '-';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

function FieldRows({ entries }: { entries: [string, unknown][] }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[2] }}>
      {entries.map(([k, v]) => (
        <div key={k} style={{ display: 'flex', justifyContent: 'space-between', gap: SPACE[4] }}>
          <span style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary }}>{k}</span>
          <span
            style={{
              ...dataTextStyle,
              fontSize: TYPE.secondary,
              textAlign: 'right',
              wordBreak: 'break-all',
            }}
          >
            {formatValue(v)}
          </span>
        </div>
      ))}
    </div>
  );
}

/** Read-only grouped model config (M1; edit mode arrives in M2). Secrets
 * arrive already redacted ("***") from the server — no reveal affordance. */
export function ConfigForm({
  config,
  hasFile,
}: {
  config: Record<string, unknown>;
  hasFile: boolean;
}) {
  const { t } = useTranslation();
  if (!hasFile) return <Empty description={t('modelConfig.noFile')} />;
  if (Object.keys(config).length === 0) return <Empty description={t('modelConfig.empty')} />;

  const { groups, advanced } = groupModelConfig(config);
  const items = [
    ...groups.map(({ meta, entries }) => ({
      key: meta.key,
      label: t(`modelConfig.groups.${meta.key}`),
      children: <FieldRows entries={entries} />,
    })),
    ...(advanced.length > 0
      ? [
          {
            key: 'advanced',
            label: t('modelConfig.groups.advanced'),
            children: <FieldRows entries={advanced} />,
          },
        ]
      : []),
  ];
  return <Collapse ghost items={items} defaultActiveKey={items.map((i) => i.key)} />;
}
