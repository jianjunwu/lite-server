import type { AlertItem } from './types';

const PREF_KEY = 'lite-ui-alert-notify';

export function notificationsSupported(): boolean {
  return typeof Notification !== 'undefined';
}

export function loadNotifyPref(): boolean {
  return localStorage.getItem(PREF_KEY) === '1';
}

export function saveNotifyPref(enabled: boolean) {
  localStorage.setItem(PREF_KEY, enabled ? '1' : '0');
}

export async function ensureNotificationPermission(): Promise<boolean> {
  if (!notificationsSupported()) return false;
  if (Notification.permission === 'granted') return true;
  if (Notification.permission === 'denied') return false;
  return (await Notification.requestPermission()) === 'granted';
}

export function fireAlertNotification(alert: AlertItem) {
  if (!notificationsSupported() || Notification.permission !== 'granted') return;
  const title = `[${alert.severity}] ${alert.rule} — ${alert.model}/${alert.version}`;
  new Notification(title, { body: alert.message, tag: `${alert.model}/${alert.version}/${alert.rule}` });
}
