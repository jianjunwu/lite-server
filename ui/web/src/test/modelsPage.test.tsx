import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { ModelsPage } from '../pages/ModelsPage';
import type { MergedModel } from '../api/merge';
import type { MergedVersionsResult } from '../api/hooks';

const mockList: { data: MergedModel[]; isLoading: boolean; repoUnavailable: boolean } = {
  data: [],
  isLoading: false,
  repoUnavailable: false,
};
const mockVersions: Record<string, MergedVersionsResult> = {};

vi.mock('../api/hooks', () => ({
  useMergedModels: () => mockList,
  useMergedVersions: (_inst: string, model: string) =>
    mockVersions[model] ?? {
      versions: [],
      activeVersion: null,
      isLoading: false,
      inRepo: true,
      hasLoaded: false,
    },
  useInstanceName: () => 'Prod',
}));

vi.mock('../components/UploadDrawer', () => ({ UploadDrawer: () => null }));
vi.mock('../api/download', () => ({ downloadModelPackage: vi.fn() }));

vi.mock('../context/InstanceContext', () => ({
  useInstance: () => ({ instanceId: 'prod', setInstanceId: vi.fn() }),
}));

vi.mock('../context/AuthContext', () => ({
  useAuth: () => ({ user: { username: 'op', role: 'operator' }, can: (r: string) => r !== 'admin' }),
}));

let mockCanInstance = true;
vi.mock('../context/useEffectiveRole', () => ({
  useCanInstance: () => () => mockCanInstance,
  useEffectiveRole: () => (mockCanInstance ? 'operator' : 'viewer'),
}));

const runLifecycle = vi.fn();
vi.mock('../components/useLifecycleOp', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  useLifecycleOp: () => ({ runLifecycle, pending: null }),
}));

vi.mock('../context/TaskContext', () => ({
  useTasks: () => ({ addTask: () => 'task-1', updateTask: vi.fn() }),
}));

vi.mock('../context/ThemeModeContext', () => ({
  useThemeMode: () => ({ dark: false, toggle: vi.fn() }),
  useNeutrals: () => new Proxy({}, { get: () => '#888' }),
  useChartColors: () => ['#4F46E5'],
}));

const model = (over: Partial<MergedModel>): MergedModel => ({
  name: 'm',
  status: 'ready',
  versionCount: 1,
  workers: 1,
  modelType: 'litapi',
  repoVersions: ['1'],
  drifted: false,
  ...over,
});

function renderPage() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <MemoryRouter initialEntries={['/models?i=prod']}>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <ModelsPage />
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  mockList.data = [];
  mockCanInstance = true;
  runLifecycle.mockClear();
  for (const k of Object.keys(mockVersions)) delete mockVersions[k];
});

describe('ModelsPage repository view', () => {
  it('should_keep_unloaded_repo_models_visible_with_a_load_action', () => {
    mockList.data = [
      model({ name: 'echo', status: 'ready' }),
      model({ name: 'ghost', status: 'unloaded', workers: 0 }),
    ];
    renderPage();
    expect(screen.getByText('ghost')).toBeTruthy();
    expect(screen.getByText('Unloaded')).toBeTruthy();
    // Single repo version → direct Load action on the row.
    expect(screen.getByRole('button', { name: 'Load' })).toBeTruthy();
  });

  it('should_link_model_names_without_dropping_the_instance_param', () => {
    mockList.data = [model({ name: 'echo' })];
    renderPage();
    const link = screen.getByRole('link', { name: 'echo' });
    expect(link.getAttribute('href')).toContain('i=prod');
  });

  it('should_filter_rows_by_status_segment', async () => {
    mockList.data = [
      model({ name: 'echo', status: 'ready' }),
      model({ name: 'ghost', status: 'unloaded', workers: 0 }),
    ];
    renderPage();
    // Segment labels carry counts ("Unloaded 1").
    fireEvent.click(await screen.findByText(/Unloaded/, { selector: '.ant-segmented-item *' }));
    expect(screen.queryByText('echo')).toBeNull();
    expect(screen.getByText('ghost')).toBeTruthy();
  });

  it('should_hide_load_and_upload_actions_for_an_effective_viewer', () => {
    mockCanInstance = false;
    mockList.data = [model({ name: 'ghost', status: 'unloaded', workers: 0 })];
    renderPage();
    expect(screen.queryByRole('button', { name: 'Load' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Upload model' })).toBeNull();
  });

  it('should_show_drift_badge_for_loaded_models_missing_from_disk', () => {
    mockList.data = [
      model({ name: 'echo', status: 'ready', drifted: true }),
      model({ name: 'ghost', status: 'unloaded', workers: 0 }),
    ];
    renderPage();
    expect(screen.getByLabelText('drift warning')).toBeTruthy();
  });

  it('should_show_a_quiet_line_when_everything_is_unloaded', () => {
    mockList.data = [
      model({ name: 'a', status: 'unloaded', workers: 0 }),
      model({ name: 'b', status: 'unloaded', workers: 0 }),
    ];
    renderPage();
    expect(screen.getByText(/2 models in repository, none loaded/)).toBeTruthy();
  });

  it('should_show_upload_guidance_in_the_empty_state', () => {
    mockList.data = [];
    renderPage();
    expect(screen.getByText(/Upload a model or run an example/)).toBeTruthy();
  });

  it('should_delete_a_model_after_typing_its_name', async () => {
    const fetchMock = vi.fn<(input: RequestInfo | URL, init?: RequestInit) => Promise<Response>>(() =>
      Promise.resolve(
        new Response(JSON.stringify({ success: true }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      ),
    );
    vi.stubGlobal('fetch', fetchMock);
    mockList.data = [model({ name: 'ghost', status: 'unloaded', workers: 0 })];
    renderPage();
    fireEvent.click(screen.getByRole('button', { name: 'Actions' }));
    fireEvent.click(await screen.findByText('Delete model'));
    const input = await screen.findByPlaceholderText('ghost');
    fireEvent.change(input, { target: { value: 'ghost' } });
    fireEvent.click(screen.getByRole('checkbox'));
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0];
    expect(String(url)).toContain('/api/i/prod/v2/models/ghost?force=true');
    expect((init as RequestInit).method).toBe('DELETE');
  });

  it('should_collapse_the_versions_table_until_the_disclosure_is_opened', async () => {
    mockList.data = [model({ name: 'ghost', status: 'unloaded', workers: 0 })];
    mockVersions['ghost'] = {
      versions: [
        {
          version: '1',
          status: 'unloaded',
          active: false,
          weight: 0,
          workers: { ready: 0, total: 0 },
          loaded_at: null,
          loaded: false,
        },
      ],
      activeVersion: null,
      isLoading: false,
      inRepo: true,
      hasLoaded: false,
    };
    renderPage();
    // Collapsed by default: the card stays a summary until asked.
    expect(screen.queryByRole('link', { name: '1' })).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: /show versions/i }));
    // Expanded in place: repo versions with per-version Load actions.
    expect(await screen.findByRole('link', { name: '1' })).toBeTruthy();
    expect((await screen.findAllByRole('button', { name: 'Load' })).length).toBeGreaterThan(0);
  });

  it('should_render_a_glyph_labelled_with_the_model_type', () => {
    mockList.data = [model({ name: 'echo', modelType: 'litapi' })];
    renderPage();
    expect(screen.getByRole('img', { name: /model type: litapi/i })).toBeTruthy();
  });

  it('should_copy_the_model_name_from_the_card_action', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    mockList.data = [model({ name: 'echo' })];
    renderPage();
    fireEvent.click(screen.getByRole('button', { name: /copy model name/i }));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith('echo'));
  });

  it('should_navigate_to_the_detail_page_when_clicking_the_card_body', () => {
    mockList.data = [model({ name: 'ghost', status: 'unloaded', workers: 0 })];
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <MemoryRouter initialEntries={['/models?i=prod']}>
        <QueryClientProvider client={queryClient}>
          <AntdApp>
            <Routes>
              <Route path="/models" element={<ModelsPage />} />
              <Route path="/models/:name" element={<div>detail probe</div>} />
            </Routes>
          </AntdApp>
        </QueryClientProvider>
      </MemoryRouter>,
    );
    // The statement is plain card body — clicking it opens the detail page.
    fireEvent.click(screen.getByText('No versions loaded'));
    expect(screen.getByText('detail probe')).toBeTruthy();
  });

  it('should_pluralize_the_stat_rail_labels', () => {
    mockList.data = [model({ name: 'echo' }), model({ name: 'ghost', status: 'unloaded', workers: 0 })];
    mockVersions['echo'] = {
      versions: [
        {
          version: '1',
          status: 'ready',
          active: true,
          weight: 100,
          workers: { ready: 1, total: 1 },
          loaded_at: null,
          loaded: true,
        },
      ],
      activeVersion: '1',
      isLoading: false,
      inRepo: true,
      hasLoaded: true,
    };
    renderPage();
    // Singular: one version, one worker (label is the rail's own text node).
    expect(screen.getByText('version')).toBeTruthy();
    expect(screen.getByText('worker')).toBeTruthy();
    // Zero: still plural.
    expect(screen.getByText('versions')).toBeTruthy();
    expect(screen.getByText('workers')).toBeTruthy();
  });

  it('should_track_the_card_load_action_through_the_lifecycle_watcher', async () => {
    mockList.data = [model({ name: 'ghost', status: 'unloaded', workers: 0 })];
    renderPage();
    fireEvent.click(screen.getByRole('button', { name: 'Load' }));
    fireEvent.click(await screen.findByRole('button', { name: 'OK' }));
    await waitFor(() => expect(runLifecycle).toHaveBeenCalledWith('load', 'ghost', '1'));
  });
});
