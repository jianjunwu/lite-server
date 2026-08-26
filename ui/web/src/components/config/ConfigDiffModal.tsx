import { useState } from 'react';
import { Modal, Radio, Typography } from 'antd';
import { diffLines } from 'diff';
import { useTranslation } from 'react-i18next';
import type { ConfigPatchMode } from '../../api/config';
import { applyMergePatch, stableStringify } from './configDraft';
import { MONO_FONT, STATUS_COLORS, TYPE } from '../../theme';
import { SPACE } from '../../tokens';
import { useNeutrals } from '../../context/ThemeModeContext';

/**
 * M2 save confirmation (plan §4.3): line diff of the on-disk tree vs the
 * merged result, plus the apply-mode choice. Dumb component — submission
 * and result branches live in ConfigEditor.
 */
export function ConfigDiffModal({
  open,
  submitting,
  original,
  patch,
  onCancel,
  onOk,
}: {
  open: boolean;
  submitting: boolean;
  original: Record<string, unknown>;
  patch: Record<string, unknown>;
  onCancel: () => void;
  onOk: (mode: ConfigPatchMode) => void;
}) {
  const { t } = useTranslation();
  const neutrals = useNeutrals();
  const [mode, setMode] = useState<ConfigPatchMode>('apply_reload');

  const merged = applyMergePatch(original, patch);
  const parts = diffLines(stableStringify(original), stableStringify(merged));
  const changeCount = parts.filter((p) => p.added || p.removed).length;

  return (
    <Modal
      open={open}
      title={t('modelConfig.diffTitle')}
      okText={t('modelConfig.save')}
      cancelText={t('modelConfig.cancel')}
      confirmLoading={submitting}
      onOk={() => onOk(mode)}
      onCancel={onCancel}
      width={640}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: SPACE[3] }}>
        <Typography.Text type="secondary" style={{ fontSize: TYPE.secondary }}>
          {t('modelConfig.diffChanges', { count: changeCount })}
        </Typography.Text>
        <pre
          style={{
            fontFamily: MONO_FONT,
            fontSize: TYPE.secondary,
            margin: 0,
            maxHeight: 320,
            overflow: 'auto',
          }}
        >
          {parts.map((p, i) => (
            <span
              key={i}
              style={{
                display: 'block',
                background: p.added ? neutrals.diffAddBg : p.removed ? neutrals.diffRemoveBg : 'transparent',
                color: p.added ? STATUS_COLORS.ready : p.removed ? STATUS_COLORS.error : undefined,
              }}
            >
              {p.value}
            </span>
          ))}
        </pre>
        <Radio.Group
          value={mode}
          onChange={(e) => setMode(e.target.value as ConfigPatchMode)}
          style={{ display: 'flex', flexDirection: 'column', gap: SPACE[2] }}
          options={[
            { value: 'apply_reload', label: t('modelConfig.mode.applyReload') },
            { value: 'write_only', label: t('modelConfig.mode.writeOnly') },
            { value: 'dry_run', label: t('modelConfig.mode.dryRun') },
          ]}
        />
      </div>
    </Modal>
  );
}
