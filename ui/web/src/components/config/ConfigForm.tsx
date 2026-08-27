import { useEffect, useRef, useState } from 'react';
import { Collapse, Empty, Input, InputNumber, Switch, Typography } from 'antd';
import { WarningOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';
import { groupModelConfig, MODEL_CONFIG_GROUPS, type ConfigFieldMeta } from './modelConfigSchema';
import { deepEqual } from './configDraft';
import { MONO_FONT, STATUS_COLORS, TYPE, dataTextStyle } from '../../theme';
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

type ControlKind = 'number' | 'boolean' | 'string' | 'json';

/** Schema type picks the control, but a value that doesn't match the schema
 * type (custom YAML) falls back to the raw JSON editor. */
function controlKind(meta: ConfigFieldMeta, value: unknown): ControlKind {
  if (value !== undefined && value !== null) {
    if (meta.type === 'number' && typeof value !== 'number') return 'json';
    if (meta.type === 'boolean' && typeof value !== 'boolean') return 'json';
    if (meta.type === 'string' && typeof value !== 'string') return 'json';
  }
  return meta.type === 'list' || meta.type === 'object' ? 'json' : meta.type;
}

/** JSON TextArea for list/object values (and the whole advanced block):
 * parses on every keystroke; invalid text is kept locally and reported via
 * onValidity so the save button can lock. */
function JsonField({
  value,
  onChange,
  onValidity,
  danger,
}: {
  value: unknown;
  onChange: (v: unknown) => void;
  onValidity: (valid: boolean) => void;
  danger?: boolean;
}) {
  const { t } = useTranslation();
  const [text, setText] = useState(() =>
    value === undefined ? '' : JSON.stringify(value, null, 2),
  );
  const [error, setError] = useState<string | null>(null);
  return (
    <div style={{ flex: 1, maxWidth: 420 }}>
      <Input.TextArea
        value={text}
        autoSize={{ minRows: 1, maxRows: 10 }}
        style={{
          fontFamily: MONO_FONT,
          fontSize: TYPE.secondary,
          borderColor: danger ? STATUS_COLORS.error : undefined,
        }}
        status={error ? 'error' : undefined}
        onChange={(e) => {
          const next = e.target.value;
          setText(next);
          try {
            const parsed = next.trim() === '' ? undefined : JSON.parse(next);
            setError(null);
            onValidity(true);
            onChange(parsed);
          } catch {
            setError(t('modelConfig.jsonInvalid'));
            onValidity(false);
          }
        }}
      />
      {error && (
        <Typography.Text type="danger" style={{ fontSize: TYPE.secondary }}>
          {error}
        </Typography.Text>
      )}
    </div>
  );
}

function EditRow({
  meta,
  draftValue,
  originalValue,
  onChange,
  onValidity,
}: {
  meta: ConfigFieldMeta;
  draftValue: unknown;
  originalValue: unknown;
  onChange: (v: unknown) => void;
  onValidity: (valid: boolean) => void;
}) {
  const { t } = useTranslation();
  const kind = controlKind(meta, draftValue);
  const changed = !deepEqual(draftValue, originalValue);
  const danger = meta.danger === true && changed;
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[1] }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: SPACE[4] }}>
        <span style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary }}>
          {meta.key}
          {meta.unit && (
            <Typography.Text type="secondary" style={{ fontSize: TYPE.secondary }}>
              {' '}
              ({meta.unit})
            </Typography.Text>
          )}
        </span>
        {kind === 'number' && (
          <InputNumber
            value={typeof draftValue === 'number' ? draftValue : undefined}
            style={{ width: 160, borderColor: danger ? STATUS_COLORS.error : undefined }}
            onChange={(v) => onChange(v ?? undefined)}
          />
        )}
        {kind === 'boolean' && (
          <Switch checked={draftValue === true} onChange={(v) => onChange(v)} />
        )}
        {kind === 'string' && (
          <Input
            value={typeof draftValue === 'string' ? draftValue : ''}
            style={{ width: 260, borderColor: danger ? STATUS_COLORS.error : undefined }}
            onChange={(e) => onChange(e.target.value === '' ? undefined : e.target.value)}
          />
        )}
        {kind === 'json' && (
          <JsonField value={draftValue} onChange={onChange} onValidity={onValidity} danger={danger} />
        )}
      </div>
      {danger && (
        <Typography.Text type="danger" style={{ fontSize: TYPE.secondary }}>
          <WarningOutlined /> {t('modelConfig.dangerWarning')}
        </Typography.Text>
      )}
    </div>
  );
}

export interface ConfigFormProps {
  config: Record<string, unknown>;
  hasFile: boolean;
  /** Edit mode (M2): controls replace values, draft edits flow through
   * onChange (undefined removes the key from the draft). */
  editing?: boolean;
  draft?: Record<string, unknown>;
  onChange?: (key: string, value: unknown) => void;
  /** Aggregate JSON-textarea validity; the save button locks while false. */
  onValidityChange?: (valid: boolean) => void;
}

/** Schema-driven grouped model config — read-only (M1) and edit (M2) modes.
 * Secrets arrive already redacted ("***") from the server — no reveal
 * affordance; redacted subtrees are excluded from the patch on save. */
export function ConfigForm({
  config,
  hasFile,
  editing = false,
  draft,
  onChange,
  onValidityChange,
}: ConfigFormProps) {
  const { t } = useTranslation();
  // Per-field JSON validity (keyed by field key); aggregate reported upward.
  const validity = useRef(new Map<string, boolean>());
  // An edit session's reports must not leak into the next one — a stale
  // `false` would lock the Save button even when every field is valid.
  useEffect(() => {
    if (!editing) validity.current.clear();
  }, [editing]);
  if (!hasFile) return <Empty description={t('modelConfig.noFile')} />;
  if (!editing && Object.keys(config).length === 0) {
    return <Empty description={t('modelConfig.empty')} />;
  }

  const reportValidity = (key: string, valid: boolean) => {
    validity.current.set(key, valid);
    onValidityChange?.([...validity.current.values()].every(Boolean));
  };

  const current = editing ? (draft ?? {}) : config;
  const { groups, advanced } = groupModelConfig(current);

  const editField = (meta: ConfigFieldMeta) => (
    <EditRow
      key={meta.key}
      meta={meta}
      draftValue={current[meta.key]}
      originalValue={config[meta.key]}
      onChange={(v) => onChange?.(meta.key, v)}
      onValidity={(valid) => reportValidity(meta.key, valid)}
    />
  );

  const items = editing
    ? [
        // Edit mode shows every schema field (absent ones get empty controls
        // so new keys can be added), then one JSON block for advanced keys.
        ...MODEL_CONFIG_GROUPS.map((meta) => ({
          key: meta.key,
          label: t(`modelConfig.groups.${meta.key}`),
          children: (
            <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[3] }}>
              {meta.fields.map(editField)}
            </div>
          ),
        })),
        {
          key: 'advanced',
          label: t('modelConfig.groups.advanced'),
          children: (
            <JsonField
              value={Object.fromEntries(advanced)}
              danger={false}
              onValidity={(valid) => reportValidity('__advanced__', valid)}
              onChange={(v) => {
                const next = (v ?? {}) as Record<string, unknown>;
                // Replace the advanced block: drop keys that vanished, set the rest.
                for (const [k] of advanced) {
                  if (!(k in next)) onChange?.(k, undefined);
                }
                for (const [k, val] of Object.entries(next)) onChange?.(k, val);
              }}
            />
          ),
        },
      ]
    : [
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
