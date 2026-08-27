import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { InstanceDetailPage } from '../pages/InstanceDetailPage';

const timelineAllSpy = vi.fn((..._args: unknown[]) => ({
  data: { snapshots: [] },
  isLoading: false,
  error: null,
  refetch: vi.fn(),
}));
const acceleratorSpy = vi.fn((..._args: unknown[]) => ({
  data: null,
  isLoading: false,
  error: null,
  refetch: vi.fn(),
}));

vi.mock('../api/hooks', () => ({
  useInstances: () => ({
    data: {
      instances: [
        { id: 'inst-1', name: 'Inst 1', base_url: 'http://backend:8000', has_admin_key: false, readonly: false },
      ],
    },
    isLoading: false,
  }),
  useServerInfo: () => ({ data: { version: '0.9.0', loaded_models: [] }, isError: false }),
  useModels: () => ({ data: { models: [] }, isLoading: false }),
  useTimelineAll: (...args: unknown[]) => timelineAllSpy(...args),
  useAcceleratorMetrics: (...args: unknown[]) => acceleratorSpy(...args),
  useInstanceName: () => 'Prod',
}));

vi.mock('../api/config', () => ({
  useServerConfig: () => ({ data: undefined, isLoading: false, isError: true }),
}));

vi.mock('../context/ThemeModeContext', () => ({
  useNeutrals: () => new Proxy({}, { get: () => '#888' }),
}));

function renderPage() {
  // Health probes go through the real api client; any response settles them.
  vi.stubGlobal(
    'fetch',
    vi.fn(() =>
      Promise.resolve(
        new Response(JSON.stringify({}), { status: 200, headers: { 'content-type': 'application/json' } }),
      ),
    ),
  );
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <MemoryRouter initialEntries={['/instances/inst-1']}>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <Routes>
            <Route path="/instances/:id" element={<InstanceDetailPage />} />
          </Routes>
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  timelineAllSpy.mockClear();
  acceleratorSpy.mockClear();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

/** Third argument (`active`) of the most recent useAcceleratorMetrics call. */
function lastAcceleratorActive(): unknown {
  const calls = acceleratorSpy.mock.calls;
  return calls[calls.length - 1]?.[2];
}

describe('InstanceDetailPage accelerator tab polling', () => {
  it('should_poll_accelerator_metrics_only_while_the_accelerator_tab_is_active', () => {
    renderPage();
    // antd keeps an activated pane mounted, so without an explicit active
    // gate the panel keeps polling after switching back to Overview.
    fireEvent.click(screen.getByRole('tab', { name: 'Accelerator' }));
    expect(acceleratorSpy).toHaveBeenCalled();
    expect(lastAcceleratorActive()).not.toBe(false);

    fireEvent.click(screen.getByRole('tab', { name: 'Overview' }));
    expect(lastAcceleratorActive()).toBe(false);
  });
});

describe('InstanceDetailPage overview timeline', () => {
  it('should_request_a_downsampled_timeline_since_only_the_latest_point_is_used', () => {
    renderPage();
    // The overview stats read only the last entry per snapshot; a step beyond
    // the retention window makes the server answer with just that point.
    expect(timelineAllSpy).toHaveBeenCalledWith('inst-1', expect.anything(), expect.any(Number));
  });
});
