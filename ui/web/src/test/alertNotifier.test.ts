import { renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { useAlerts } from '../api/hooks';
import { fireAlertNotification } from '../api/notifications';
import { useAlertNotifier } from '../api/useAlertNotifier';
import type { AlertItem } from '../api/types';

vi.mock('../api/hooks', () => ({ useAlerts: vi.fn() }));
vi.mock('../api/notifications', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../api/notifications')>();
  return { ...mod, fireAlertNotification: vi.fn() };
});

const mockUseAlerts = vi.mocked(useAlerts);
const mockFire = vi.mocked(fireAlertNotification);

function alert(model: string): AlertItem {
  return {
    model,
    version: 'v1',
    rule: 'high_latency',
    message: 'boom',
    severity: 'critical',
    timestamp: 1,
    value: 2,
    threshold: 1,
  };
}

afterEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

describe('useAlertNotifier', () => {
  it('should_not_renotify_the_first_batch_after_an_instance_switch', () => {
    localStorage.setItem('lite-ui-alert-notify', '1');
    Object.defineProperty(document, 'visibilityState', { value: 'hidden', configurable: true });

    mockUseAlerts.mockReturnValue({ data: { alerts: [alert('m-on-a')] } } as never);
    const { rerender } = renderHook(({ id }) => useAlertNotifier(id), {
      initialProps: { id: 'inst-a' },
    });
    // First snapshot on a new instance is a baseline, never notified.
    expect(mockFire).not.toHaveBeenCalled();

    mockUseAlerts.mockReturnValue({ data: { alerts: [alert('m-on-b')] } } as never);
    rerender({ id: 'inst-b' });
    // The new instance's firing alerts are not "newly firing" for this user —
    // without a reset they would all look new vs instance A's snapshot.
    expect(mockFire).not.toHaveBeenCalled();
  });

  it('should_notify_for_genuinely_new_alerts_on_the_same_instance', () => {
    localStorage.setItem('lite-ui-alert-notify', '1');
    Object.defineProperty(document, 'visibilityState', { value: 'hidden', configurable: true });

    mockUseAlerts.mockReturnValue({ data: { alerts: [] } } as never);
    const { rerender } = renderHook(({ id }) => useAlertNotifier(id), {
      initialProps: { id: 'inst-a' },
    });
    mockUseAlerts.mockReturnValue({ data: { alerts: [alert('m-new')] } } as never);
    rerender({ id: 'inst-a' });
    expect(mockFire).toHaveBeenCalledTimes(1);
  });
});
