import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { ModelDetailPage } from '../pages/ModelDetailPage';
import type { MergedVersionsResult } from '../api/hooks';

const mockMerged: MergedVersionsResult = {
  versions: [],
  activeVersion: null,
  isLoading: false,
  inRepo: true,
  hasLoaded: false,
};

const mockModelsList = {
  data: [
    {
      name: 'echo',
      status: 'ready' as const,
      versionCount: 1,
      workers: 1,
      modelType: 'litapi',
      repoVersions: ['1'],
      drifted: false,
    },
  ],
  isLoading: false,
  repoUnavailable: false,
};
const mockHealth: { data: { total_workers: number; workers: never[] } | undefined; isLoading: boolean } = {
  data: undefined,
  isLoading: false,
};
const mockTimelineAll: {
  data: {
    snapshots: { model: string; version: string; entries: Record<string, unknown>[] }[];
    coverageSeconds?: number;
    intervalSeconds?: number;
  } | undefined;
  isLoading: boolean;
  error: Error | null;
} = { data: undefined, isLoading: false, error: null };
let mockAlerts: { alerts: { model: string; rule: string; severity: string; threshold: number }[] } = { alerts: [] };

vi.mock('../api/hooks', () => ({
  useMergedModels: () => mockModelsList,
  useMergedVersions: () => mockMerged,
  useModelHealth: () => mockHealth,
  useTimelineAll: () => mockTimelineAll,
  useAlerts: () => ({ data: mockAlerts }),
  useInstanceName: () => 'Prod',
}));

vi.mock('../api/download', () => ({ downloadModelPackage: vi.fn() }));
const echartOptions = vi.hoisted(() => vi.fn());
vi.mock('../components/EChart', () => ({
  EChart: (props: { option: unknown }) => {
    echartOptions(props.option);
    return null;
  },
}));
const uploadProps = vi.hoisted(() => vi.fn());
vi.mock('../components/UploadDrawer', () => ({
  UploadDrawer: (props: { open: boolean; model?: string }) => {
    uploadProps(props);
    return props.open ? <div>upload-drawer-mock</div> : null;
  },
}));

vi.mock('../context/InstanceContext', () => ({
  useInstance: () => ({ instanceId: 'prod', setInstanceId: vi.fn() }),
}));

vi.mock('../context/AuthContext', () => ({
  useAuth: () => ({ user: { username: 'op', role: 'operator' }, can: (r: string) => r !== 'admin' }),
}));

vi.mock('../context/useEffectiveRole', () => ({
  useCanInstance: () => () => true,
  useEffectiveRole: () => 'operator',
}));

vi.mock('../context/TaskContext', () => ({
  useTasks: () => ({ addTask: () => 'task-1', updateTask: vi.fn() }),
}));

vi.mock('../context/ThemeModeContext', () => ({
  useThemeMode: () => ({ dark: false, toggle: vi.fn() }),
  useNeutrals: () => new Proxy({}, { get: () => '#888' }),
  useChartColors: () => ['#4F46E5'],
}));

function renderPage(model = 'ghost', fetchImpl?: (url: string) => Response) {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) =>
      Promise.resolve(
        fetchImpl?.(String(input)) ??
          new Response(JSON.stringify({ error: 'not found' }), {
            status: 404,
            headers: { 'content-type': 'application/json' },
          }),
      ),
    ),
  );
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <MemoryRouter initialEntries={[`/models/${model}?i=prod`]}>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <Routes>
            <Route path="/models/:name" element={<ModelDetailPage />} />
            <Route path="/models/:name/versions/:version" element={<div>version detail probe</div>} />
          </Routes>
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  localStorage.clear();
  mockMerged.versions = [];
  mockMerged.activeVersion = null;
  mockMerged.isLoading = false;
  mockMerged.inRepo = true;
  mockMerged.hasLoaded = false;
  mockHealth.data = undefined;
  mockTimelineAll.data = undefined;
  mockTimelineAll.isLoading = false;
  mockTimelineAll.error = null;
  mockAlerts = { alerts: [] };
  echartOptions.mockClear();
  uploadProps.mockClear();
});

const loadedVersionRow = (version: string, weight: number, active: boolean) => ({
  version,
  status: 'ready',
  active,
  weight,
  workers: { ready: 1, total: 1 },
  loaded_at: null,
  loaded: true,
});

describe('ModelDetailPage statement', () => {
  const loadedVersion = loadedVersionRow;

  it('should_count_workers_across_all_loaded_versions_not_just_one', () => {
    // The model-level health endpoint describes a single version; the
    // statement must sum per-version workers instead of inheriting that
    // partial count (multi_version showed "1 workers" while running 2).
    mockMerged.versions = [loadedVersion('v1', 70, false), loadedVersion('v2', 30, true)];
    mockMerged.activeVersion = 'v2';
    mockMerged.hasLoaded = true;
    mockHealth.data = { total_workers: 1, workers: [] };
    renderPage('echo');
    expect(screen.getByText(/2 of 2 versions ready · v2 active · 2 workers/)).toBeInTheDocument();
  });

  it('should_use_singular_forms_for_one_version_and_one_worker', () => {
    mockMerged.versions = [loadedVersion('1', 100, true)];
    mockMerged.activeVersion = '1';
    mockMerged.hasLoaded = true;
    mockHealth.data = { total_workers: 1, workers: [] };
    renderPage('echo');
    expect(screen.getByText(/1 of 1 version ready · 1 active · 1 worker(?!s)/)).toBeInTheDocument();
  });
});

describe('ModelDetailPage traffic river', () => {
  it('should_render_the_river_once_not_duplicated_in_hero_and_table', () => {
    mockMerged.versions = [loadedVersionRow('v1', 70, false), loadedVersionRow('v2', 30, true)];
    mockMerged.activeVersion = 'v2';
    mockMerged.hasLoaded = true;
    renderPage('echo');
    expect(screen.getAllByRole('img', { name: /%/ })).toHaveLength(1);
  });
});

describe('ModelDetailPage unloaded model', () => {
  it('should_show_unloaded_state_with_per_version_load_actions_instead_of_a_dead_page', () => {
    mockMerged.versions = [
      {
        version: '1',
        status: 'unloaded',
        active: false,
        weight: 0,
        workers: { ready: 0, total: 0 },
        loaded_at: null,
        loaded: false,
      },
    ];
    renderPage();
    // Header marks the model unloaded, no error UI.
    expect(screen.getAllByText('Unloaded').length).toBeGreaterThan(0);
    // The default metrics tab degrades to a quiet "load first" hint.
    expect(screen.getAllByText(/available after loading/i).length).toBeGreaterThan(0);
    // Per-version load action lives on the version card (panes render lazily).
    fireEvent.click(screen.getByRole('tab', { name: 'Versions' }));
    expect(screen.getByRole('button', { name: 'Load' })).toBeTruthy();
  });

  it('should_render_breadcrumb_back_to_models_with_instance_param', () => {
    mockMerged.versions = [
      {
        version: '1',
        status: 'ready',
        active: true,
        weight: 100,
        workers: { ready: 1, total: 1 },
        loaded_at: null,
        loaded: true,
      },
    ];
    mockMerged.activeVersion = '1';
    mockMerged.hasLoaded = true;
    renderPage('echo');
    const link = screen.getByRole('link', { name: 'Models' });
    expect(link.getAttribute('href')).toBe('/models?i=prod');
  });

  it('should_show_not_found_state_for_a_model_missing_from_repo_and_registry', () => {
    mockMerged.inRepo = false;
    renderPage('nope');
    expect(screen.getByText(/not found/i)).toBeTruthy();
    expect(screen.getByRole('link', { name: /Models/ })).toBeTruthy();
  });

  it('should_list_model_grants_in_the_access_tab_for_instance_admins', async () => {
    mockMerged.versions = [
      {
        version: '1',
        status: 'ready',
        active: true,
        weight: 100,
        workers: { ready: 1, total: 1 },
        loaded_at: null,
        loaded: true,
      },
    ];
    mockMerged.activeVersion = '1';
    mockMerged.hasLoaded = true;
    renderPage('echo');
    // The access panel queries the BFF once its tab is opened (lazy panes).
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes('/api/model-grants')) {
          return Promise.resolve(
            new Response(JSON.stringify({ grants: [{ username: 'u1', role: 'viewer' }] }), {
              status: 200,
              headers: { 'content-type': 'application/json' },
            }),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify({ error: 'not found' }), {
            status: 404,
            headers: { 'content-type': 'application/json' },
          }),
        );
      }),
    );
    fireEvent.click(screen.getByRole('tab', { name: 'Access' }));
    expect(await screen.findByText('u1')).toBeTruthy();
  });

  it('should_render_a_glyph_labelled_with_the_model_type_in_the_header', () => {
    mockMerged.versions = [
      {
        version: '1',
        status: 'ready',
        active: true,
        weight: 100,
        workers: { ready: 1, total: 1 },
        loaded_at: null,
        loaded: true,
      },
    ];
    mockMerged.activeVersion = '1';
    mockMerged.hasLoaded = true;
    renderPage('echo');
    expect(screen.getByRole('img', { name: /model type: litapi/i })).toBeTruthy();
  });

  it('should_state_ready_active_worker_counts_once_in_the_subtitle_without_stat_chips', () => {
    mockMerged.versions = [
      {
        version: '1',
        status: 'ready',
        active: true,
        weight: 100,
        workers: { ready: 4, total: 4 },
        loaded_at: null,
        loaded: true,
      },
    ];
    mockMerged.activeVersion = '1';
    mockMerged.hasLoaded = true;
    renderPage('echo');

    // The subtitle statement carries the counts; a separate chip row must
    // not repeat them.
    expect(screen.getByText('1 of 1 version ready · 1 active · 4 workers')).toBeTruthy();
    expect(screen.queryByRole('button', { name: /4 workers/i })).toBeNull();
    expect(screen.queryByRole('button', { name: /1\/1 ready/i })).toBeNull();
    expect(screen.queryByRole('link', { name: /1 · 100%/ })).toBeNull();
  });
});

/** One loaded echo v1 at 100% traffic — the baseline for the live-data suites. */
function loadEcho() {
  mockMerged.versions = [loadedVersionRow('1', 100, true)];
  mockMerged.activeVersion = '1';
  mockMerged.hasLoaded = true;
}

function timelineWith(entry: Record<string, unknown>) {
  mockTimelineAll.data = { snapshots: [{ model: 'echo', version: '1', entries: [entry] }] };
}

const chartTitles = () =>
  [...document.querySelectorAll('.ant-card-head-title')].map((el) => el.textContent);

describe('ModelDetailPage KPI strip', () => {
  it('should_show_current_qps_p99_queue_and_workers_from_the_latest_timeline_point', () => {
    loadEcho();
    timelineWith({ qps: 1.2, p99_ms: 1.8, queue_depth: 3 });
    renderPage('echo');
    expect(screen.getByText('Current QPS')).toBeTruthy();
    expect(screen.getByText('1.2')).toBeTruthy();
    expect(screen.getByText('Current p99')).toBeTruthy();
    expect(screen.getByText('1.8ms')).toBeTruthy();
    expect(screen.getByText('3')).toBeTruthy();
    expect(screen.getAllByText('1/1').length).toBeGreaterThanOrEqual(1);
  });

  it('should_render_dashes_when_no_timeline_data_is_available', () => {
    loadEcho();
    renderPage('echo');
    expect(screen.getByText('Current QPS')).toBeTruthy();
    expect(screen.getAllByText('-').length).toBeGreaterThanOrEqual(3);
  });
});

describe('ModelDetailPage metrics tab', () => {
  it('should_render_the_throughput_group_by_default', () => {
    loadEcho();
    timelineWith({ qps: 1.2, p99_ms: 1.8, queue_depth: 0, in_flight: 0, worker_saturation: 0.1, ttft_p99_ms: 0.5 });
    renderPage('echo');
    fireEvent.click(screen.getByRole('tab', { name: 'Metrics' }));
    expect(chartTitles()).toEqual(['QPS', 'Tokens/s', 'Stream bytes/s']);
  });

  it('should_expose_group_tabs_range_and_refresh_controls', () => {
    loadEcho();
    renderPage('echo');
    fireEvent.click(screen.getByRole('tab', { name: 'Metrics' }));
    for (const g of ['Throughput', 'Latency', 'Queue & concurrency', 'Resources', 'Health']) {
      expect(screen.getByRole('tab', { name: g })).toBeTruthy();
    }
    expect(screen.getByText('5 min')).toBeTruthy();
    // Range segmented + version overlay + refresh selects.
    expect(screen.getAllByRole('combobox').length).toBeGreaterThanOrEqual(2);
  });

  it('should_mark_a_chart_unsupported_when_the_instance_omits_that_field', () => {
    loadEcho();
    // Pre-M3 schema: no ttft/tbt at all → the card explains instead of
    // faking zeros; p99 is present and renders.
    timelineWith({ qps: 1.2, p99_ms: 1.8, queue_depth: 0 });
    renderPage('echo');
    fireEvent.click(screen.getByRole('tab', { name: 'Metrics' }));
    fireEvent.click(screen.getByRole('tab', { name: 'Latency' }));
    expect(screen.getAllByText('This instance version does not report this metric')).toHaveLength(2);
  });

  it('should_not_offer_cpu_in_the_resources_group', () => {
    loadEcho();
    timelineWith({ qps: 1.2 });
    renderPage('echo');
    fireEvent.click(screen.getByRole('tab', { name: 'Metrics' }));
    fireEvent.click(screen.getByRole('tab', { name: 'Resources' }));
    expect(chartTitles()).toEqual(['Active workers', 'Worker saturation', 'Worker RSS']);
    expect(screen.queryByText('CPU')).toBeNull();
  });

  it('should_draw_alert_thresholds_on_the_p99_chart', () => {
    loadEcho();
    timelineWith({ qps: 1.2, p99_ms: 1.8 });
    mockAlerts = {
      alerts: [{ model: 'echo', rule: 'p99_ms', severity: 'critical', threshold: 5 }],
    };
    renderPage('echo');
    fireEvent.click(screen.getByRole('tab', { name: 'Metrics' }));
    fireEvent.click(screen.getByRole('tab', { name: 'Latency' }));
    const serialized = JSON.stringify(echartOptions.mock.calls.map((c) => c[0]));
    expect(serialized).toContain('critical threshold');
    expect(serialized).toContain('5');
  });

  it('should_state_which_version_is_faster_in_the_comparison_line', () => {
    mockMerged.versions = [loadedVersionRow('v1', 50, true), loadedVersionRow('v2', 50, false)];
    mockMerged.activeVersion = 'v1';
    mockMerged.hasLoaded = true;
    mockTimelineAll.data = {
      snapshots: [
        { model: 'echo', version: 'v1', entries: [{ timestamp: 1, qps: 10, p99_ms: 100, queue_depth: 0 }] },
        { model: 'echo', version: 'v2', entries: [{ timestamp: 1, qps: 10, p99_ms: 200, queue_depth: 0 }] },
      ],
    };
    renderPage('echo');
    expect(screen.getByText(/v1 is 100.0ms faster at p99 than v2/)).toBeTruthy();
  });

  it('should_disable_ranges_beyond_the_instances_retention_window', async () => {
    loadEcho();
    // M3 instance: 5-minute retention via X-Timeline-* headers.
    mockTimelineAll.data = {
      snapshots: [],
      coverageSeconds: 300,
      intervalSeconds: 10,
    };
    renderPage('echo');
    fireEvent.click(screen.getByRole('tab', { name: 'Metrics' }));
    await waitFor(() => {
      expect(document.querySelectorAll('.ant-segmented-item-disabled').length).toBe(2);
    });
  });

  it('should_open_the_upload_drawer_with_the_model_preset', async () => {
    loadEcho();
    renderPage('echo');
    fireEvent.click(screen.getByRole('button', { name: /Upload new version/ }));
    expect(await screen.findByText('upload-drawer-mock')).toBeTruthy();
    const lastCall = uploadProps.mock.calls.at(-1)?.[0];
    expect(lastCall?.open).toBe(true);
    expect(lastCall?.model).toBe('echo');
  });
});

describe('ModelDetailPage traffic strip', () => {
  it('should_explain_the_drag_interaction_when_editable', () => {
    mockMerged.versions = [loadedVersionRow('v1', 70, false), loadedVersionRow('v2', 30, true)];
    mockMerged.activeVersion = 'v2';
    mockMerged.hasLoaded = true;
    renderPage('echo');
    expect(screen.getByText(/drag a divider to shift weight/i)).toBeTruthy();
  });
});

describe('ModelDetailPage information architecture', () => {
  it('should_default_to_the_metrics_tab_and_drop_the_workers_and_compare_tabs', () => {
    loadEcho();
    renderPage('echo');
    const active = document.querySelector('.ant-tabs-tab-active');
    expect(active?.textContent).toBe('Metrics');
    expect(screen.getByRole('tab', { name: 'Versions' })).toBeTruthy();
    expect(screen.getByRole('tab', { name: 'Access' })).toBeTruthy();
    expect(screen.queryByRole('tab', { name: 'Workers' })).toBeNull();
    expect(screen.queryByRole('tab', { name: 'Compare' })).toBeNull();
  });
});

describe('ModelDetailPage version cards', () => {
  function twoVersionsWithMetrics() {
    mockMerged.versions = [loadedVersionRow('v1', 70, false), loadedVersionRow('v2', 30, true)];
    mockMerged.activeVersion = 'v2';
    mockMerged.hasLoaded = true;
    // Per-version streams drive the version cards; the KPI strip now derives
    // from the same data (sum across versions).
    mockTimelineAll.data = {
      snapshots: [
        { model: 'echo', version: 'v1', entries: [{ qps: 0.3, p99_ms: 2.1 }] },
        { model: 'echo', version: 'v2', entries: [{ qps: 1.2, p99_ms: 1.8 }] },
      ],
    };
  }

  it('should_render_a_card_per_version_with_weight_and_current_metrics', () => {
    twoVersionsWithMetrics();
    renderPage('echo');
    fireEvent.click(screen.getByRole('tab', { name: 'Versions' }));
    expect(screen.getByText('70%')).toBeTruthy();
    expect(screen.getByText('30%')).toBeTruthy();
    expect(screen.getByText(/0\.3 QPS/)).toBeTruthy();
    expect(screen.getByText(/2\.1ms p99/)).toBeTruthy();
    expect(screen.getByText(/1\.2 QPS/)).toBeTruthy();
    expect(screen.getByText(/1\.8ms p99/)).toBeTruthy();
  });

  it('should_link_each_card_to_its_version_detail_page', () => {
    twoVersionsWithMetrics();
    renderPage('echo');
    fireEvent.click(screen.getByRole('tab', { name: 'Versions' }));
    const link = screen.getByRole('link', { name: 'v2' });
    expect(link.getAttribute('href')).toBe('/models/echo/versions/v2?i=prod');
  });
});
