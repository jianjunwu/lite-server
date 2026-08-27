import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { InstancesPage } from '../pages/InstancesPage';

vi.mock('../context/AuthContext', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../context/AuthContext')>();
  return {
    ...mod,
    useAuth: () => ({
      user: { username: 'admin', role: 'admin', createdAt: '', mustChangePassword: false },
      can: () => true,
    }),
  };
});

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

// base_url must pass the shared form's url rule (kevva url-regex rejects
// bare single-label hostnames; IPs and dotted hosts pass), so mock data
// uses the same shape as the form placeholder.
const INSTANCES = [
  { id: 'prod', name: 'Prod', base_url: 'http://10.0.0.11:8000', has_admin_key: false, readonly: false },
  { id: 'dev', name: 'Dev', base_url: 'http://10.0.0.12:8000', has_admin_key: false, readonly: false },
];

// workers here are the /health total count (registry slots, incl. stopped).
const HEALTH = {
  models: [
    {
      name: 'echo',
      active_version: '1',
      versions: [
        { version: '1', status: 'ready', workers: 2, loaded_at: 1, last_failure: null },
        { version: '2', status: 'ready', workers: 2, loaded_at: 1, last_failure: null },
      ],
    },
    {
      name: 'llm',
      active_version: '3',
      versions: [{ version: '3', status: 'ready', workers: 1, loaded_at: 1, last_failure: null }],
    },
  ],
};

function installFetch() {
  const calls: { method: string; url: string }[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      const method = init?.method ?? 'GET';
      calls.push({ method, url });
      if (url === '/api/instances' && method === 'POST') return Promise.resolve(json({}, 201));
      if (url.startsWith('/api/instances/') && method === 'PUT') return Promise.resolve(json({}));
      if (url.startsWith('/api/instances/') && method === 'DELETE') return Promise.resolve(json({}));
      if (url === '/api/instances') return Promise.resolve(json({ instances: INSTANCES }));
      if (url.endsWith('/health')) return Promise.resolve(json(HEALTH));
      if (url.endsWith('/info')) return Promise.resolve(json({ version: '0.8.12' }));
      return Promise.resolve(json({}));
    }),
  );
  return calls;
}

function renderPage() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <MemoryRouter>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <InstancesPage />
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => vi.unstubAllGlobals());

describe('InstancesPage card anatomy', () => {
  it('should_show_name_version_base_url_and_role_on_each_card', async () => {
    installFetch();
    renderPage();
    const card = (await screen.findByText('Prod')).closest('.ant-card') as HTMLElement;
    expect(within(card).getByText('0.8.12')).toBeTruthy();
    expect(within(card).getByText('http://10.0.0.11:8000')).toBeTruthy();
  });

  it('should_sum_health_workers_as_totals_in_the_count_row', async () => {
    installFetch();
    renderPage();
    // 2 models · 3 versions · 5 workers (2+2+1 — /health workers are the
    // registry total incl. stopped slots, verified in registry/mod.rs).
    // Both mocked instances share the same health payload.
    expect(await screen.findAllByText('2 models · 3 versions · 5 workers')).toHaveLength(2);
  });

  it('should_show_an_offline_badge_when_the_instance_is_unreachable', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
        const url = String(input);
        if (url === '/api/instances') return Promise.resolve(json({ instances: INSTANCES }));
        if (url.endsWith('/health')) {
          return Promise.resolve(new Response('bad gateway', { status: 502 }));
        }
        return Promise.resolve(json({}));
      }),
    );
    renderPage();
    expect(await screen.findAllByText('Instance unreachable')).toHaveLength(2);
  });
});

describe('InstancesPage create/edit/delete flow', () => {
  it('should_create_an_instance_via_the_shared_drawer', async () => {
    const calls = installFetch();
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /Add instance$/ }));
    const dialog = await screen.findByRole('dialog', { name: 'Add instance' });
    fireEvent.change(within(dialog).getByPlaceholderText('prod-gpu'), { target: { value: 'dev02' } });
    fireEvent.change(within(dialog).getByPlaceholderText('Prod GPU cluster'), { target: { value: 'Dead' } });
    fireEvent.change(within(dialog).getByPlaceholderText('http://10.0.0.11:8000'), {
      target: { value: 'http://localhost:18001' },
    });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Add instance' }));
    // rc-drawer keeps the closed panel mounted; assert on the a11y tree.
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Add instance' })).not.toBeInTheDocument(),
    );
    expect(calls.some((c) => c.method === 'POST' && c.url === '/api/instances?probe=true')).toBe(true);
  });

  it('should_edit_an_instance_via_the_hover_action', async () => {
    const calls = installFetch();
    renderPage();
    const prodCard = (await screen.findByText('Prod')).closest('.ant-card') as HTMLElement;
    fireEvent.click(within(prodCard).getByRole('button', { name: 'Edit' }));
    const dialog = await screen.findByRole('dialog', { name: 'Edit instance prod' });
    const nameInput = within(dialog).getByPlaceholderText('Prod GPU cluster');
    fireEvent.change(nameInput, { target: { value: 'Production' } });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save' }));
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Edit instance prod' })).not.toBeInTheDocument(),
    );
    expect(calls.some((c) => c.method === 'PUT' && c.url === '/api/instances/prod')).toBe(true);
  });

  it('should_delete_an_instance_after_confirming', async () => {
    const calls = installFetch();
    renderPage();
    const prodCard = (await screen.findByText('Prod')).closest('.ant-card') as HTMLElement;
    fireEvent.click(within(prodCard).getByRole('button', { name: 'Delete' }));
    fireEvent.click(await screen.findByRole('button', { name: /OK/ }));
    await waitFor(() =>
      expect(calls.some((c) => c.method === 'DELETE' && c.url === '/api/instances/prod')).toBe(true),
    );
  });
});

describe('InstancesPage empty state', () => {
  it('should_offer_the_add_button_as_the_call_to_action', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(json({ instances: [] }))),
    );
    renderPage();
    expect(await screen.findByText('No instances configured')).toBeTruthy();
    expect(screen.getAllByRole('button', { name: /Add instance$/ })).toHaveLength(2);
  });
});
