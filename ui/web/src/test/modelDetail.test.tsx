import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen } from '@testing-library/react';
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

vi.mock('../api/hooks', () => ({
  useMergedModels: () => mockModelsList,
  useMergedVersions: () => mockMerged,
  useModelHealth: () => mockHealth,
  useTimeline: () => ({ data: undefined, isLoading: false, error: null, refetch: vi.fn() }),
}));

vi.mock('../api/download', () => ({ downloadModelPackage: vi.fn() }));
vi.mock('../components/EChart', () => ({ EChart: () => null }));

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

function renderPage(model = 'ghost') {
  vi.stubGlobal(
    'fetch',
    vi.fn(() =>
      Promise.resolve(
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
          </Routes>
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  mockMerged.versions = [];
  mockMerged.activeVersion = null;
  mockMerged.isLoading = false;
  mockMerged.inRepo = true;
  mockMerged.hasLoaded = false;
  mockHealth.data = undefined;
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
    expect(screen.getByRole('button', { name: 'Load' })).toBeTruthy();
    // Runtime tabs degrade to a quiet "load first" hint (panes render lazily).
    fireEvent.click(screen.getByRole('tab', { name: 'Workers' }));
    expect(screen.getAllByText(/available after loading/i).length).toBeGreaterThan(0);
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

  it('should_jump_to_tabs_and_version_detail_from_the_stat_chips', () => {
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
    mockHealth.data = { total_workers: 4, workers: [] };
    renderPage('echo');

    fireEvent.click(screen.getByRole('button', { name: /4 workers/i }));
    expect(screen.getByRole('tab', { name: 'Workers' }).getAttribute('aria-selected')).toBe('true');

    fireEvent.click(screen.getByRole('button', { name: /1\/1 ready/i }));
    expect(screen.getByRole('tab', { name: 'Versions' }).getAttribute('aria-selected')).toBe('true');

    const activeChip = screen.getByRole('link', { name: /1 · 100%/ });
    expect(activeChip.getAttribute('href')).toContain('/models/echo/versions/1');
  });
});
