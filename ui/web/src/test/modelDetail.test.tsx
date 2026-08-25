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

vi.mock('../api/hooks', () => ({
  useMergedVersions: () => mockMerged,
  useModelHealth: () => ({ data: undefined, isLoading: false }),
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
});
