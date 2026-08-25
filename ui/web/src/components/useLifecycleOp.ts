import { App } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ApiError, apiFetch } from '../api/client';
import { modelOps, withAdminKeyRetry } from '../api/mutations';
import type { VersionsResponse } from '../api/types';
import { useInstance } from '../context/InstanceContext';
import { useTasks } from '../context/TaskContext';
import { statusKind } from './StatusBadge';

export type LifecycleAction = 'load' | 'unload' | 'reload' | 'activate';

export const LIFECYCLE_POLL_MS = 3_000;
export const LIFECYCLE_TIMEOUT_MS = 10 * 60_000;

/** Terminal condition per action, judged against the registry's version list.
 * `null` response = the versions endpoint 404'd (nothing loaded). */
export function isSettled(action: LifecycleAction, resp: VersionsResponse | null, version: string): boolean {
  const v = resp?.versions.find((x) => x.version === version);
  switch (action) {
    case 'load':
    case 'reload':
      return v !== undefined && statusKind(v.status) === 'ready';
    case 'activate':
      return resp?.active_version === version;
    case 'unload':
      return v === undefined;
  }
}

const MUTATE: Record<LifecycleAction, (inst: string, model: string, version: string) => Promise<unknown>> = {
  load: modelOps.loadVersion,
  unload: modelOps.unloadVersion,
  reload: modelOps.reloadVersion,
  activate: modelOps.activateVersion,
};

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Task-store key phase before the server accepts: drives button spinners. */
export function lifecycleKey(action: LifecycleAction, model: string, version: string): string {
  return `${action}:${model}:${version}`;
}

/**
 * Lifecycle ops are accepted asynchronously: the mutation returns long before
 * the registry reaches the target state, which used to leave the user staring
 * at a silent screen. This hook fires the mutation, then tracks the
 * transition as a task in the bell — polling the versions endpoint until the
 * state settles or a generous timeout trips. Like downloads, the watch keeps
 * running after the user navigates away.
 */
export function useLifecycleOp() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { instanceId } = useInstance();
  const { addTask, updateTask } = useTasks();
  const queryClient = useQueryClient();
  const [pending, setPending] = useState<string | null>(null);

  const runLifecycle = useCallback(
    async (action: LifecycleAction, model: string, version: string) => {
      if (!instanceId) return;
      setPending(lifecycleKey(action, model, version));
      try {
        await withAdminKeyRetry(instanceId, () => MUTATE[action](instanceId, model, version));
      } catch (err) {
        message.error(err instanceof Error ? err.message : String(err));
        return;
      } finally {
        setPending(null);
      }
      await queryClient.invalidateQueries({ queryKey: [instanceId] });
      const taskId = addTask({
        title: t(`ops.task.${action}`, { model, version }),
        kind: 'load',
        detail: t('ops.taskWaiting'),
      });

      // Background watch — the caller returns here; the task settles later.
      const deadline = Date.now() + LIFECYCLE_TIMEOUT_MS;
      const watch = async () => {
        for (;;) {
          await sleep(LIFECYCLE_POLL_MS);
          let resp: VersionsResponse | null = null;
          let fetchFailed = false;
          try {
            resp = await apiFetch<VersionsResponse>(
              instanceId,
              `/v2/models/${encodeURIComponent(model)}/versions`,
            );
          } catch (err) {
            // 404 = nothing loaded (a valid verdict for unload); anything else
            // is transient and simply skips this round — the deadline decides.
            if (!(err instanceof ApiError && err.status === 404)) fetchFailed = true;
          }
          if (!fetchFailed && isSettled(action, resp, version)) {
            const done = t(`ops.taskDone.${action}`, { model, version });
            updateTask(taskId, { status: 'success', progress: 100, detail: done });
            message.success(done);
            await queryClient.invalidateQueries({ queryKey: [instanceId] });
            return;
          }
          if (Date.now() >= deadline) {
            updateTask(taskId, {
              status: 'error',
              detail: t('ops.taskTimeout', { minutes: LIFECYCLE_TIMEOUT_MS / 60_000 }),
            });
            return;
          }
          // Transient fetch errors skip the state update; the deadline above
          // still terminates the watch.
          if (fetchFailed) continue;
          const v = resp?.versions.find((x) => x.version === version);
          updateTask(taskId, {
            detail: v
              ? t('ops.taskState', { status: v.status, ready: v.workers.ready, total: v.workers.total })
              : t('ops.taskWaiting'),
          });
        }
      };
      watch().catch((err: unknown) =>
        updateTask(taskId, { status: 'error', detail: err instanceof Error ? err.message : String(err) }),
      );
    },
    [instanceId, addTask, updateTask, message, queryClient, t],
  );

  return { runLifecycle, pending };
}
