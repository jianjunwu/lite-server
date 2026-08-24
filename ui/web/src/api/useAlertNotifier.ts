import { useEffect, useRef } from 'react';
import { useAlerts } from '../api/hooks';
import { fireAlertNotification, loadNotifyPref } from '../api/notifications';
import type { AlertItem } from '../api/types';

const alertKey = (a: AlertItem) => `${a.model}/${a.version}/${a.rule}/${a.severity}`;

/**
 * Watch the current instance's alerts and fire browser notifications for
 * newly firing alerts. Only when: pref enabled + permission granted + the
 * tab is hidden (visible-tab noise is what the alerts page is for).
 */
export function useAlertNotifier(instanceId: string | null) {
  const alertsQuery = useAlerts(instanceId, 15_000);
  const seen = useRef<Set<string> | null>(null);

  // Switching instances: the other instance's firing alerts are not "newly
  // firing" for this user — reset so the first batch is a silent baseline.
  useEffect(() => {
    seen.current = null;
  }, [instanceId]);

  useEffect(() => {
    const alerts = alertsQuery.data?.alerts;
    if (!alerts) return;
    if (seen.current === null) {
      // First snapshot: don't spam for already-firing alerts.
      seen.current = new Set(alerts.map(alertKey));
      return;
    }
    if (!loadNotifyPref()) {
      seen.current = new Set(alerts.map(alertKey));
      return;
    }
    for (const alert of alerts) {
      const key = alertKey(alert);
      if (seen.current.has(key)) continue;
      if (document.visibilityState === 'hidden') {
        fireAlertNotification(alert);
      }
    }
    // Snapshot the currently-firing set: resolved alerts are dropped so a
    // re-fire notifies again.
    seen.current = new Set(alerts.map(alertKey));
  }, [alertsQuery.data]);
}
