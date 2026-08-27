import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { MetricsPage } from '../pages/MetricsPage';

let currentInstance = 'inst-a';

interface MockTimeline {
  snapshots: never[];
  coverageSeconds?: number;
  intervalSeconds?: number;
}

const timelineByInstance: Record<string, MockTimeline> = {
  // M3 instance: reports a 5-minute retention window via X-Timeline-* headers.
  'inst-a': { snapshots: [], coverageSeconds: 300, intervalSeconds: 10 },
  // Pre-M3 instance: no headers, so no coverage metadata at all.
  'inst-b': { snapshots: [] },
};

vi.mock('../api/hooks', () => ({
  useModels: () => ({ data: { models: [] }, isLoading: false }),
  useVersions: () => ({ data: { versions: [] } }),
  useAlerts: () => ({ data: { alerts: [] } }),
  useTimelineAll: () => ({
    data: timelineByInstance[currentInstance],
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  }),
  useAcceleratorMetrics: () => ({ data: null }),
  useInstanceName: () => 'Prod',
}));

vi.mock('../context/InstanceContext', () => ({
  useInstance: () => ({ instanceId: currentInstance, setInstanceId: vi.fn() }),
}));

vi.mock('../context/ThemeModeContext', () => ({
  useThemeMode: () => ({ dark: false, toggle: vi.fn() }),
  useNeutrals: () => new Proxy({}, { get: () => '#888' }),
  useChartColors: () => ['#4F46E5'],
}));

vi.mock('../components/EChart', () => ({ EChart: () => null }));

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

function tree() {
  return (
    <MemoryRouter initialEntries={['/metrics']}>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <MetricsPage />
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>
  );
}

afterEach(() => {
  currentInstance = 'inst-a';
});

describe('MetricsPage coverage state', () => {
  it('should_reset_range_availability_when_switching_to_an_instance_without_timeline_headers', async () => {
    const view = render(tree());
    // coverage=300s: the 15 min and 1 hour ranges exceed the window and are disabled.
    await waitFor(() => {
      expect(view.container.querySelectorAll('.ant-segmented-item-disabled').length).toBe(2);
    });

    // Switching instances only changes the ?i= search param — the page stays mounted.
    currentInstance = 'inst-b';
    view.rerender(tree());

    // The pre-M3 instance reports no coverage: every range must be enabled again.
    await waitFor(() => {
      expect(view.container.querySelectorAll('.ant-segmented-item-disabled')).toHaveLength(0);
    });
  });
});
