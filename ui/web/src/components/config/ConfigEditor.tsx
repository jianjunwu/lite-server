import { useEffect, useRef, useState } from 'react';
import { Alert, App, Button, Tag } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import {
  patchModelConfig,
  type ConfigPatchMode,
  type ModelConfigPatchResponse,
  type ModelConfigResponse,
} from '../../api/config';
import { ApiError } from '../../api/client';
import { withAdminKeyRetry } from '../../api/mutations';
import { useCanInstance } from '../../context/useEffectiveRole';
import { useNeutrals } from '../../context/ThemeModeContext';
import { TYPE } from '../../theme';
import { SPACE } from '../../tokens';
import { ConfigForm } from './ConfigForm';
import { ConfigDiffModal } from './ConfigDiffModal';
import { buildPatch } from './configDraft';

/**
 * M2 edit state machine (plan §4.3): read → edit (polling paused upstream)
 * → diff confirm → PATCH → result branch. Viewers get the read view with a
 * read-only badge instead of the edit entry.
 */
export function ConfigEditor({
  instanceId,
  model,
  version,
  data,
  onEditingChange,
}: {
  instanceId: string;
  model: string;
  version: string;
  data: ModelConfigResponse;
  onEditingChange: (editing: boolean) => void;
}) {
  const { t } = useTranslation();
  const { message, modal } = App.useApp();
  const queryClient = useQueryClient();
  const can = useCanInstance();
  const neutrals = useNeutrals();

  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<Record<string, unknown>>({});
  const [jsonValid, setJsonValid] = useState(true);
  const [diffOpen, setDiffOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [validationError, setValidationError] = useState<string | null>(null);

  useEffect(() => () => onEditingChange(false), [onEditingChange]);

  // Identity change without remount (the instance switcher only swaps a
  // search param): drop the edit session. A draft built from another
  // config's tree must never become submittable against this config's etag.
  const identity = `${instanceId} ${model} ${version}`;
  const prevIdentity = useRef(identity);
  useEffect(() => {
    if (prevIdentity.current !== identity) {
      prevIdentity.current = identity;
      setEditing(false);
      setDraft({});
      setValidationError(null);
      setDiffOpen(false);
      onEditingChange(false);
    }
  }, [identity, onEditingChange]);

  const built = buildPatch(data.config, draft);
  const dirty = editing && Object.keys(built.patch).length > 0;

  const invalidate = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: [instanceId, 'model-config', model, version] }),
      queryClient.invalidateQueries({ queryKey: [instanceId, 'versions', model] }),
      queryClient.invalidateQueries({ queryKey: [instanceId, 'model-health', model, version] }),
    ]);
  };

  const exitEdit = () => {
    setEditing(false);
    setDraft({});
    setValidationError(null);
    setDiffOpen(false);
    onEditingChange(false);
  };

  const enterEdit = () => {
    setDraft(structuredClone(data.config));
    setJsonValid(true);
    setValidationError(null);
    setEditing(true);
    onEditingChange(true);
  };

  const cancelEdit = () => {
    if (!dirty) {
      exitEdit();
      return;
    }
    modal.confirm({
      title: t('modelConfig.discardTitle'),
      content: t('modelConfig.discardBody'),
      okButtonProps: { danger: true },
      onOk: exitEdit,
    });
  };

  const handleChange = (key: string, value: unknown) => {
    setValidationError(null);
    setDraft((prev) => {
      const next = { ...prev };
      if (value === undefined) {
        delete next[key];
      } else {
        next[key] = value;
      }
      return next;
    });
  };

  const submit = async (mode: ConfigPatchMode, force: boolean) => {
    setSubmitting(true);
    try {
      const resp = await withAdminKeyRetry(instanceId, () =>
        patchModelConfig(instanceId, model, version, {
          patch: built.patch,
          if_match: data.etag,
          force,
          mode,
        }),
      );
      setDiffOpen(false);
      if (mode === 'dry_run') {
        showDryRunResult(resp);
        return;
      }
      message.success(
        resp.reloaded ? t('modelConfig.saved') : t('modelConfig.savedWriteOnly'),
      );
      await invalidate();
      exitEdit();
    } catch (err) {
      handleSubmitError(err, mode);
    } finally {
      setSubmitting(false);
    }
  };

  const showDryRunResult = (resp: ModelConfigPatchResponse) => {
    const warnings = resp.warnings.join('\n');
    if (resp.valid) {
      modal.info({
        title: t('modelConfig.dryRunValid'),
        content: warnings || undefined,
      });
    } else {
      modal.warning({
        title: t('modelConfig.dryRunInvalid'),
        content: warnings || undefined,
      });
    }
  };

  const handleSubmitError = (err: unknown, mode: ConfigPatchMode) => {
    if (!(err instanceof ApiError)) {
      message.error(String(err));
      return;
    }
    const body = (err.body ?? {}) as { rolled_back?: unknown; current_etag?: unknown };
    if (err.status === 409) {
      setDiffOpen(false);
      modal.confirm({
        title: t('modelConfig.conflictTitle'),
        content: t('modelConfig.conflictBody'),
        okText: t('modelConfig.forceOverwrite'),
        cancelText: t('modelConfig.conflictViewLatest'),
        onOk: () => submit(mode, true),
        onCancel: () => {
          void invalidate();
          exitEdit();
        },
      });
      return;
    }
    if (body.rolled_back === true) {
      setDiffOpen(false);
      modal.error({
        title: t('modelConfig.reloadFailedTitle'),
        content: `${err.message}\n${t('modelConfig.rolledBack')}`,
      });
      return;
    }
    if (err.status === 400 || err.status === 422) {
      // Field-level detail from the server, echoed above the form.
      setValidationError(err.message);
      setDiffOpen(false);
      return;
    }
    message.error(err.message);
  };

  if (!can('operator')) {
    return (
      <div>
        <Tag style={{ marginBottom: 8 }}>{t('modelConfig.readOnly')}</Tag>
        <ConfigForm config={data.config} hasFile={data.has_file} />
      </div>
    );
  }

  return (
    <div>
      {!editing && (
        <div style={{ marginBottom: 12 }}>
          <Button onClick={enterEdit}>{t('modelConfig.edit')}</Button>
        </div>
      )}
      {editing && built.skipped.length > 0 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message={t('modelConfig.redactedSkipped', { keys: built.skipped.join(', ') })}
        />
      )}
      {editing && validationError && (
        <Alert type="error" showIcon style={{ marginBottom: 12 }} message={validationError} />
      )}
      <ConfigForm
        config={data.config}
        hasFile={data.has_file || editing}
        editing={editing}
        draft={draft}
        onChange={handleChange}
        onValidityChange={setJsonValid}
      />
      {editing && (
        <div
          style={{
            position: 'sticky',
            bottom: 0,
            zIndex: 10,
            display: 'flex',
            alignItems: 'center',
            gap: SPACE[3],
            padding: `${SPACE[2]}px ${SPACE[3]}px`,
            marginTop: 12,
            background: neutrals.bgPage,
            borderTop: `1px solid ${neutrals.border}`,
          }}
        >
          <span style={{ flex: 1, fontSize: TYPE.secondary, color: neutrals.textSecondary }}>
            {t('modelConfig.unsavedCount', { count: Object.keys(built.patch).length })}
          </span>
          <Button onClick={cancelEdit}>{t('modelConfig.discard')}</Button>
          <Button
            type="primary"
            disabled={!dirty || !jsonValid}
            onClick={() => setDiffOpen(true)}
          >
            {t('modelConfig.save')}
          </Button>
        </div>
      )}
      <ConfigDiffModal
        open={diffOpen}
        submitting={submitting}
        original={data.config}
        patch={built.patch}
        onCancel={() => setDiffOpen(false)}
        onOk={(mode) => void submit(mode, false)}
      />
    </div>
  );
}
