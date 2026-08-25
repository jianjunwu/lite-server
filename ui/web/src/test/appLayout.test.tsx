import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { AppLayout } from '../layout/AppLayout';

const INSTANCES = [
  { id: 'echo-local', name: 'Echo Local', base_url: 'http://127.0.0.1:8000', effective_role: 'admin' as const },
];

let mockInstances: typeof INSTANCES = INSTANCES;
let mockInstanceId: string | null = 'echo-local';

vi.mock('../api/hooks', () => ({
  useInstances: () => ({ data: { instances: mockInstances }, isLoading: false, isSuccess: true }),
  useHealthSummary: () => ({ isError: false, data: { status: 'healthy' } }),
}));

vi.mock('../api/useAlertNotifier', () => ({ useAlertNotifier: () => {} }));

// TaskBell needs TaskProvider; irrelevant to the instance picker.
vi.mock('../components/TaskBell', () => ({ TaskBell: () => null }));

vi.mock('../context/InstanceContext', () => ({
  useInstance: () => ({ instanceId: mockInstanceId, setInstanceId: vi.fn() }),
}));

vi.mock('../context/AuthContext', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../context/AuthContext')>();
  return {
    ...mod,
    useAuth: () => ({
      user: { username: 'admin', role: 'admin', createdAt: '', mustChangePassword: false },
      can: () => true,
      logout: vi.fn(),
      refresh: vi.fn(),
    }),
  };
});

vi.mock('../context/ThemeModeContext', () => ({
  useThemeMode: () => ({ dark: false, toggle: vi.fn() }),
  useNeutrals: () => ({ border: '#ddd', textSecondary: '#888', bgPage: '#fff' }),
}));

afterEach(() => {
  vi.unstubAllGlobals();
  mockInstances = INSTANCES;
  mockInstanceId = 'echo-local';
});

function LocationProbe() {
  const loc = useLocation();
  return <div data-testid="loc">{loc.pathname + loc.search}</div>;
}

function renderLayout() {
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
    <MemoryRouter initialEntries={['/overview']}>
      <QueryClientProvider client={queryClient}>
        <Routes>
          <Route element={<AppLayout />}>
            <Route path="*" element={<LocationProbe />} />
          </Route>
        </Routes>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

async function openInstanceSelect() {
  const [combobox] = await screen.findAllByRole('combobox');
  fireEvent.mouseDown(combobox);
}

describe('AppLayout instance picker', () => {
  it('should_navigate_to_instances_tab_from_manage_entry', async () => {
    renderLayout();
    await openInstanceSelect();
    fireEvent.click(await screen.findByRole('button', { name: /Manage instances/ }));
    expect(screen.getByTestId('loc').textContent).toBe('/settings?tab=instances&i=echo-local');
  });

  it('should_show_manage_entry_when_no_instances', async () => {
    mockInstances = [];
    renderLayout();
    await openInstanceSelect();
    expect(await screen.findByRole('button', { name: /Manage instances/ })).toBeTruthy();
  });

  it('should_keep_the_instance_param_when_navigating_from_the_sider', async () => {
    renderLayout();
    fireEvent.click(await screen.findByRole('link', { name: 'Models' }));
    expect(screen.getByTestId('loc').textContent).toBe('/models?i=echo-local');
  });

  it('should_show_the_effective_role_tag_in_the_instance_picker', async () => {
    renderLayout();
    await openInstanceSelect();
    expect((await screen.findAllByText('admin')).length).toBeGreaterThan(0);
  });

  it('should_show_a_no_access_page_for_an_instance_outside_the_visible_list', async () => {
    // A ?i= deep link to an instance the BFF filtered out (grant "none") —
    // or a typo — gets a page-level empty state instead of a dead shell.
    mockInstanceId = 'ghost';
    renderLayout();
    expect(await screen.findByText(/no access/i)).toBeTruthy();
    expect(screen.queryByTestId('loc')).toBeNull();
  });
});
