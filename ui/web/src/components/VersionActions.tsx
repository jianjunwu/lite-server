import { useState } from 'react';
import { App, Button, Checkbox, Input, Modal, Popconfirm, Space } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { modelOps, withAdminKeyRetry } from '../api/mutations';
import type { VersionInfo } from '../api/types';
import { MONO_FONT } from '../theme';

interface VersionActionsProps {
  model: string;
  version: VersionInfo;
}

/**
 * Per-version operations, confirmation graded by blast radius:
 * reload/unload → Popconfirm; activate / unload-active → Modal;
 * delete → Modal with type-to-confirm (+ force for the active version).
 */
export function VersionActions({ model, version }: VersionActionsProps) {
  const { t } = useTranslation();
  const { message, modal } = App.useApp();
  const { instanceId } = useInstance();
  const queryClient = useQueryClient();
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleteConfirmText, setDeleteConfirmText] = useState('');
  const [forceDelete, setForceDelete] = useState(false);
  const [busy, setBusy] = useState(false);

  if (!instanceId) return null;

  const run = async (op: () => Promise<unknown>, successKey: string) => {
    setBusy(true);
    try {
      await withAdminKeyRetry(instanceId, op);
      message.success(t(successKey));
      await queryClient.invalidateQueries({ queryKey: [instanceId] });
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const confirmActivate = () => {
    modal.confirm({
      title: t('ops.activateTitle'),
      content: t('ops.activateBody', { model, version: version.version }),
      okText: t('ops.activate'),
      onOk: () => run(() => modelOps.activateVersion(instanceId, model, version.version), 'ops.activated'),
    });
  };

  const confirmUnloadActive = () => {
    modal.confirm({
      title: t('ops.unloadActiveTitle'),
      content: t('ops.unloadActiveBody', { model, version: version.version }),
      okText: t('ops.unload'),
      okButtonProps: { danger: true },
      onOk: () => run(() => modelOps.unloadVersion(instanceId, model, version.version), 'ops.unloaded'),
    });
  };

  const submitDelete = async () => {
    await run(
      () => modelOps.deleteVersion(instanceId, model, version.version, forceDelete),
      'ops.deleted',
    );
    setDeleteOpen(false);
    setDeleteConfirmText('');
    setForceDelete(false);
  };

  return (
    <Space size={4} wrap>
      <Popconfirm
        title={t('ops.reloadConfirm', { version: version.version })}
        onConfirm={() => run(() => modelOps.reloadVersion(instanceId, model, version.version), 'ops.reloaded')}
      >
        <Button type="text" size="small" disabled={busy}>{t('ops.reload')}</Button>
      </Popconfirm>

      {!version.active && (
        <Button type="text" size="small" disabled={busy} onClick={confirmActivate}>
          {t('ops.activate')}
        </Button>
      )}

      {version.active ? (
        <Button type="text" size="small" disabled={busy} onClick={confirmUnloadActive}>
          {t('ops.unload')}
        </Button>
      ) : (
        <Popconfirm
          title={t('ops.unloadConfirm', { version: version.version })}
          onConfirm={() => run(() => modelOps.unloadVersion(instanceId, model, version.version), 'ops.unloaded')}
        >
          <Button type="text" size="small" disabled={busy}>{t('ops.unload')}</Button>
        </Popconfirm>
      )}

      <Button type="text" size="small" danger disabled={busy} onClick={() => setDeleteOpen(true)}>
        {t('ops.delete')}
      </Button>

      <Modal
        open={deleteOpen}
        title={t('ops.deleteTitle', { version: version.version })}
        okText={t('ops.delete')}
        okButtonProps={{ danger: true, disabled: deleteConfirmText !== version.version || busy }}
        onOk={submitDelete}
        onCancel={() => {
          setDeleteOpen(false);
          setDeleteConfirmText('');
          setForceDelete(false);
        }}
      >
        <p style={{ fontSize: 13 }}>{t('ops.deleteBody', { model, version: version.version })}</p>
        <Input
          value={deleteConfirmText}
          onChange={(e) => setDeleteConfirmText(e.target.value)}
          placeholder={version.version}
          style={{ fontFamily: MONO_FONT }}
        />
        {version.active && (
          <Checkbox
            checked={forceDelete}
            onChange={(e) => setForceDelete(e.target.checked)}
            style={{ marginTop: 12 }}
          >
            {t('ops.forceDeleteActive')}
          </Checkbox>
        )}
      </Modal>
    </Space>
  );
}
