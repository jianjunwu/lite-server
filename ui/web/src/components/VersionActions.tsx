import { useState } from 'react';
import { App, Button, Checkbox, Input, Modal, Popconfirm, Space } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { modelOps, withAdminKeyRetry } from '../api/mutations';
import { downloadModelPackage } from '../api/download';
import { useTasks } from '../context/TaskContext';
import { lifecycleKey, useLifecycleOp, type LifecycleAction } from './useLifecycleOp';
import { formatBytes } from './format';
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
 * Lifecycle ops (load/reload/unload/activate) are tracked as tasks until
 * the registry reaches the target state; delete resolves synchronously.
 */
export function VersionActions({ model, version }: VersionActionsProps) {
  const { t } = useTranslation();
  const { message, modal } = App.useApp();
  const { instanceId } = useInstance();
  const queryClient = useQueryClient();
  const { addTask, updateTask } = useTasks();
  const { runLifecycle, pending } = useLifecycleOp();
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleteConfirmText, setDeleteConfirmText] = useState('');
  const [forceDelete, setForceDelete] = useState(false);
  const [busy, setBusy] = useState(false);

  if (!instanceId) return null;

  const lifecycleLoading = (action: LifecycleAction) =>
    pending === lifecycleKey(action, model, version.version);

  const run = async (op: () => Promise<unknown>, successKey: string, successValues?: Record<string, string>) => {
    setBusy(true);
    try {
      await withAdminKeyRetry(instanceId, op);
      message.success(t(successKey, successValues));
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
      onOk: () => runLifecycle('activate', model, version.version),
    });
  };

  const confirmUnloadActive = () => {
    modal.confirm({
      title: t('ops.unloadActiveTitle'),
      content: t('ops.unloadActiveBody', { model, version: version.version }),
      okText: t('ops.unload'),
      okButtonProps: { danger: true },
      onOk: () => runLifecycle('unload', model, version.version),
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

  const startDownload = () => {
    // Resumable when the browser has the File System Access API, plain
    // anchor navigation otherwise; progress rides the task bell either way.
    const taskId = addTask({
      title: t('ops.downloadTaskTitle', { model, version: version.version }),
      kind: 'download',
      progress: 0,
    });
    const handle = downloadModelPackage(instanceId, model, version.version, {
      onProgress: (percent, loaded, total) =>
        updateTask(taskId, { progress: percent, detail: `${formatBytes(loaded)} / ${formatBytes(total)}` }),
    });
    updateTask(taskId, { abort: () => handle.abort() });
    handle.promise.then(
      ({ fileName }) => {
        updateTask(taskId, { status: 'success', progress: 100, abort: undefined });
        message.success(t('ops.downloadDone', { fileName }));
      },
      (err) => {
        const text = err instanceof Error ? err.message : String(err);
        updateTask(taskId, { status: 'error', detail: text, abort: undefined });
        message.error(text);
      },
    );
  };

  // Repository versions that are not loaded get lifecycle ops only:
  // Load / Download / Delete. Runtime ops would 404 against the registry.
  if (version.loaded === false) {
    return (
      <Space size={4} wrap>
        <Button type="text" size="small" disabled={busy} onClick={startDownload}>
          {t('ops.download')}
        </Button>
        <Popconfirm
          title={t('ops.loadConfirm', { version: version.version })}
          onConfirm={() => runLifecycle('load', model, version.version)}
        >
          <Button type="text" size="small" loading={lifecycleLoading('load')} disabled={busy}>
            {t('ops.load')}
          </Button>
        </Popconfirm>
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
          }}
        >
          <p style={{ fontSize: 13 }}>{t('ops.deleteBody', { model, version: version.version })}</p>
          <Input
            value={deleteConfirmText}
            onChange={(e) => setDeleteConfirmText(e.target.value)}
            placeholder={version.version}
            style={{ fontFamily: MONO_FONT }}
          />
        </Modal>
      </Space>
    );
  }

  return (
    <Space size={4} wrap>
      <Button type="text" size="small" disabled={busy} onClick={startDownload}>
        {t('ops.download')}
      </Button>

      <Popconfirm
        title={t('ops.reloadConfirm', { version: version.version })}
        onConfirm={() => runLifecycle('reload', model, version.version)}
      >
        <Button type="text" size="small" loading={lifecycleLoading('reload')} disabled={busy}>
          {t('ops.reload')}
        </Button>
      </Popconfirm>

      {!version.active && (
        <Button type="text" size="small" loading={lifecycleLoading('activate')} disabled={busy} onClick={confirmActivate}>
          {t('ops.activate')}
        </Button>
      )}

      {version.active ? (
        <Button type="text" size="small" loading={lifecycleLoading('unload')} disabled={busy} onClick={confirmUnloadActive}>
          {t('ops.unload')}
        </Button>
      ) : (
        <Popconfirm
          title={t('ops.unloadConfirm', { version: version.version })}
          onConfirm={() => runLifecycle('unload', model, version.version)}
        >
          <Button type="text" size="small" loading={lifecycleLoading('unload')} disabled={busy}>
            {t('ops.unload')}
          </Button>
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
