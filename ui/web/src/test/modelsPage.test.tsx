import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
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
  mockList.data = [];
  mockCanInstance = true;
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

  it('should_show_repo_versions_with_load_action_in_the_expand_row', async () => {
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
    // Expand the row via the antd expand icon.
    const expandIcon = document.querySelector('.ant-table-row-expand-icon');
    expect(expandIcon).not.toBeNull();
    fireEvent.click(expandIcon as Element);
    expect(await screen.findByRole('link', { name: '1' })).toBeTruthy();
    expect((await screen.findAllByText('Unloaded')).length).toBeGreaterThan(0);
    // Per-version Load action from VersionActions.
    expect((await screen.findAllByRole('button', { name: 'Load' })).length).toBeGreaterThan(0);
  });
});
