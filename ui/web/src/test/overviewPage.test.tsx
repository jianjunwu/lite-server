import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { OverviewPage } from '../pages/OverviewPage';

vi.mock('../api/hooks', () => ({
  useInstances: () => ({
    data: {
      instances: [
        { id: 'prod', name: 'Prod', base_url: 'http://backend:8000', has_admin_key: false, readonly: false, effective_role: 'admin' },
      ],
    },
    isLoading: false,
  }),
}));

vi.mock('../components/EChart', () => ({ EChart: () => null }));

vi.mock('../context/ThemeModeContext', () => ({
  useNeutrals: () => new Proxy({}, { get: () => '#888' }),
}));

function stubInstanceApis() {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      let body: unknown = {};
      if (url.endsWith('/health')) {
        body = {
          models: [
            {
              name: 'echo',
              active_version: '1',
              versions: [{ version: '1', status: 'ready', workers: 1, loaded_at: 1, last_failure: null }],
            },
          ],
        };
      } else if (url.endsWith('/info')) {
        body = { version: '0.8.12' };
      } else if (url.includes('/metrics/timeline')) {
        body = { snapshots: [] };
      } else if (url.includes('/metrics/alerts')) {
        body = { alerts: [] };
      }
      return Promise.resolve(
        new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } }),
      );
    }),
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('OverviewPage instance model rows', () => {
  it('should_pin_status_dots_to_a_fixed_name_column_so_rows_align_at_any_card_width', async () => {
    stubInstanceApis();
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <MemoryRouter initialEntries={['/overview?i=prod']}>
        <QueryClientProvider client={queryClient}>
          <OverviewPage />
        </QueryClientProvider>
      </MemoryRouter>,
    );
    const name = await screen.findByRole('link', { name: 'echo' });
    // A percentage basis lets the dot cluster drift with card width; a fixed
    // basis keeps dots right after names on every row.
    expect(name.style.flex).toBe('0 0 180px');
  });
});

describe('OverviewPage hero when every instance is unreachable', () => {
  function stubAllDown() {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response('bad gateway', { status: 502, headers: { 'content-type': 'text/plain' } }),
        ),
      ),
    );
  }

  function renderPage() {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <MemoryRouter initialEntries={['/overview?i=prod']}>
        <QueryClientProvider client={queryClient}>
          <OverviewPage />
        </QueryClientProvider>
      </MemoryRouter>,
    );
  }

  it('should_state_unreachable_instead_of_claiming_zero_versions_ready', async () => {
    stubAllDown();
    renderPage();
    expect(await screen.findByText('All instances unreachable')).toBeTruthy();
    expect(screen.queryByText(/versions ready/)).toBeNull();
  });

  it('should_show_an_offline_indicator_instead_of_a_green_live_dot', async () => {
    stubAllDown();
    renderPage();
    expect(await screen.findByText('offline')).toBeTruthy();
    expect(screen.queryByText('live')).toBeNull();
  });
});
