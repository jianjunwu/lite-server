import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { InstanceDetailPage } from '../pages/InstanceDetailPage';

const timelineAllSpy = vi.fn((..._args: unknown[]) => ({
  data: { snapshots: [] as unknown[] },
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
let mockModels: { models: { name: string; version: string; status: string; model_type: string; workers: number }[] } = {
  models: [],
};
let mockHealth: { status: string; models: { name: string; active_version: string | null; versions: { version: string; status: string; workers: number }[] }[] } = {
  status: 'ready',
  models: [],
};

vi.mock('../api/hooks', () => ({
  useInstances: () => ({
    data: {
      instances: [
        { id: 'inst-1', name: 'Inst 1', base_url: 'http://backend:8000', has_admin_key: false, readonly: false, effective_role: 'admin' },
      ],
    },
    isLoading: false,
  }),
  useServerInfo: () => ({ data: { version: '0.9.0', loaded_models: [] }, isError: false }),
  useModels: () => ({ data: mockModels, isLoading: false }),
  useHealthSummary: () => ({ data: mockHealth, isLoading: false, isError: false }),
  useTimelineAll: (...args: unknown[]) => timelineAllSpy(...args),
  useAcceleratorMetrics: (...args: unknown[]) => acceleratorSpy(...args),
  useInstanceName: () => 'Prod',
}));

const uploadProps = vi.hoisted(() => vi.fn());
vi.mock('../components/UploadDrawer', () => ({
  UploadDrawer: (props: { open: boolean; existingModels: string[]; model?: string }) => {
    uploadProps(props);
    return props.open ? <div>upload-drawer-mock</div> : null;
  },
}));

vi.mock('../context/AuthContext', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../context/AuthContext')>();
  return {
    ...mod,
    useAuth: () => ({
      user: { username: 'admin', role: 'admin', createdAt: '', mustChangePassword: false },
    }),
  };
});

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
  uploadProps.mockClear();
  mockModels = { models: [] };
  mockHealth = { status: 'ready', models: [] };
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

  it('should_sum_worker_rss_across_versions_instead_of_taking_any_value', () => {
    // rss_mb is per model/version (summed over the version's live workers),
    // so the instance-level number is the cross-version sum of latest points.
    timelineAllSpy.mockReturnValue({
      data: {
        snapshots: [
          { model: 'echo', version: 'v1', entries: [{ timestamp: 1, qps: 1, p99_ms: 1, queue_depth: 0, active_workers: 1, active_streams: 1, rss_mb: 100, cpu_percent: 12.3 }] },
          { model: 'echo', version: 'v2', entries: [{ timestamp: 1, qps: 1, p99_ms: 1, queue_depth: 0, active_workers: 1, active_streams: 1, rss_mb: 50, cpu_percent: 12.3 }] },
        ],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    renderPage();
    expect(screen.getByText('150')).toBeTruthy();
    expect(screen.getByText('Worker RSS')).toBeTruthy();
    // CPU is process-level — one value, not summed.
    expect(screen.getByText('12.3')).toBeTruthy();
  });

  it('should_render_one_l1_card_per_model_when_v2_models_has_multiple_version_rows', () => {
    // /v2/models returns one row per (model, version); the L1 grid is one
    // card per model (plan §3.2).
    mockModels = {
      models: [
        { name: 'echo', version: 'v1', status: 'ready', model_type: 'Echo', workers: 2 },
        { name: 'echo', version: 'v2', status: 'ready', model_type: 'Echo', workers: 1 },
      ],
    };
    mockHealth = {
      status: 'ready',
      models: [
        {
          name: 'echo',
          active_version: 'v2',
          versions: [
            { version: 'v1', status: 'ready', workers: 2 },
            { version: 'v2', status: 'ready', workers: 1 },
          ],
        },
      ],
    };
    renderPage();
    // Exactly one L1 card, enriched from health (3 workers across versions).
    expect(screen.getAllByText('echo').length).toBe(1);
    expect(screen.getByText(/2 versions · 3 workers/)).toBeTruthy();
  });

  it('should_render_l1_model_entry_cards_with_health_derived_counts', () => {
    mockModels = {
      models: [{ name: 'echo', version: 'v1', status: 'ready', model_type: 'Echo', workers: 2 }],
    };
    mockHealth = {
      status: 'ready',
      models: [
        {
          name: 'echo',
          active_version: 'v1',
          versions: [
            { version: 'v1', status: 'ready', workers: 2 },
            { version: 'v2', status: 'ready', workers: 1 },
          ],
        },
      ],
    };
    renderPage();
    const card = screen.getByText('echo').closest('.ant-card') as HTMLElement;
    expect(card).toBeTruthy();
    expect(screen.getByText(/2 versions · 3 workers/)).toBeTruthy();
    expect(card.closest('a')).toHaveAttribute('href', '/models/echo?i=inst-1');
  });

  it('should_open_the_upload_drawer_from_the_upload_model_button', async () => {
    mockModels = { models: [{ name: 'echo', version: 'v1', status: 'ready', model_type: 'Echo', workers: 2 }] };
    renderPage();
    fireEvent.click(screen.getByRole('button', { name: /Upload model/ }));
    expect(await screen.findByText('upload-drawer-mock')).toBeTruthy();
    const lastCall = uploadProps.mock.calls.at(-1)?.[0];
    // Instance-level upload targets a new model — no model preset.
    expect(lastCall?.open).toBe(true);
    expect(lastCall?.existingModels).toEqual(['echo']);
    expect(lastCall?.model).toBeUndefined();
  });
});
