import { useCallback, useState } from 'react';
import { App } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { modelOps, withAdminKeyRetry } from '../api/mutations';
import { TYPE } from '../theme';

/**
 * Shared "apply routing weights" flow used by both the numeric RoutingEditor
 * and the TrafficRiver drag editor: confirm modal with a before/after diff,
 * admin-key retry, cache invalidation, success toast.
 */
export function useApplyRouting(model: string) {
  const { t } = useTranslation();
  const { message, modal } = App.useApp();
  const { instanceId } = useInstance();
  const queryClient = useQueryClient();
  const [busy, setBusy] = useState(false);

  const apply = useCallback(
    (weights: Record<string, number>, before: Record<string, number>, onApplied?: () => void) => {
      if (!instanceId) return;
      const diff = Object.keys(weights)
        .map((v) => `${v}: ${before[v]}% → ${weights[v]}%`)
        .join('\n');
      modal.confirm({
        title: t('routing.applyTitle'),
        content: (
          <pre style={{ fontSize: TYPE.secondary, margin: 0, whiteSpace: 'pre-wrap' }}>
            {t('routing.applyBody')}
            {'\n'}
            {diff}
          </pre>
        ),
        okText: t('routing.apply'),
        onOk: async () => {
          setBusy(true);
          try {
            await withAdminKeyRetry(instanceId, () => modelOps.setRouting(instanceId, model, weights));
            message.success(t('routing.applied'));
            await queryClient.invalidateQueries({ queryKey: [instanceId] });
            onApplied?.();
          } catch (err) {
            message.error(err instanceof Error ? err.message : String(err));
          } finally {
            setBusy(false);
          }
        },
      });
    },
    [instanceId, model, modal, message, queryClient, t],
  );

  return { apply, busy };
}
