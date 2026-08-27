import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { VersionDetailPage } from '../pages/VersionDetailPage';

let mockTimeline: {
  data: { model: string; version: string; entries: Record<string, unknown>[] } | undefined;
  isLoading: boolean;
  error: Error | null;
} = { data: undefined, isLoading: false, error: null };

vi.mock('../api/hooks', () => ({
  useInstanceName: () => 'Prod',
  useMergedVersions: () => ({
    versions: [
      {
        version: 'v1',
        status: 'ready',
        active: true,
        weight: 100,
        workers: { ready: 1, total: 2 },
        loaded_at: null,
        loaded: true,
      },
    ],
    activeVersion: 'v1',
    isLoading: false,
    inRepo: true,
    hasLoaded: true,
  }),
  useModelReady: () => ({ data: { ready: true } }),
  useModelHealth: () => ({ data: { healthy_workers: 1, total_workers: 2, workers: [] }, isLoading: false }),
  useTimeline: () => mockTimeline,
}));

vi.mock('../api/config', () => ({
  useModelConfig: () => ({ data: { config: {}, sources: {}, redacted: [] }, isLoading: false, isError: false }),
}));

vi.mock('../context/InstanceContext', () => ({
  useInstance: () => ({ instanceId: 'prod', setInstanceId: vi.fn() }),
}));

vi.mock('../context/useEffectiveRole', () => ({
  useCanInstance: () => () => true,
  useEffectiveRole: () => 'operator',
}));

vi.mock('../context/ThemeModeContext', () => ({
  useThemeMode: () => ({ dark: false, toggle: vi.fn() }),
  useNeutrals: () => new Proxy({}, { get: () => '#888' }),
  useChartColors: () => ['#4F46E5'],
}));

vi.mock('../components/EChart', () => ({ EChart: () => null }));
vi.mock('../components/WorkerMatrix', () => ({ WorkerMatrix: () => null }));

function renderPage() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <MemoryRouter initialEntries={['/models/echo/versions/v1?i=prod']}>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <Routes>
            <Route path="/models/:name/versions/:version" element={<VersionDetailPage />} />
          </Routes>
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => {
  mockTimeline = { data: undefined, isLoading: false, error: null };
});

describe('VersionDetailPage stat row', () => {
  it('should_show_qps_p99_worker_rss_and_streams_from_the_latest_point', () => {
    mockTimeline = {
      data: {
        model: 'echo',
        version: 'v1',
        entries: [{ qps: 42, p99_ms: 12, queue_depth: 3, rss_mb: 800, active_streams: 5 }],
      },
      isLoading: false,
      error: null,
    };
    renderPage();
    expect(screen.getByText('42')).toBeTruthy();
    expect(screen.getByText('12.0ms')).toBeTruthy();
    // Worker ready/total from the registry, not the timeline.
    expect(screen.getByText('1/2')).toBeTruthy();
    expect(screen.getByText('800')).toBeTruthy();
    expect(screen.getByText('MB')).toBeTruthy();
    expect(screen.getByText('5')).toBeTruthy();
    expect(screen.getByText('Active streams')).toBeTruthy();
  });

  it('should_render_dashes_when_no_timeline_point_is_available', () => {
    renderPage();
    expect(screen.getAllByText('-').length).toBeGreaterThanOrEqual(3);
  });
});
