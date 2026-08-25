import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { SettingsPage } from '../pages/SettingsPage';

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

afterEach(() => {
  vi.unstubAllGlobals();
});

function stubFetch() {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL): Promise<Response> => {
      const url = String(input);
      const body = url.includes('/api/instances') ? { instances: [] } : {};
      return Promise.resolve(
        new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } }),
      );
    }),
  );
}

function renderAt(url: string) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <MemoryRouter initialEntries={[url]}>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <SettingsPage />
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

describe('SettingsPage tab deep-linking', () => {
  it('should_default_to_instances_tab', async () => {
    stubFetch();
    renderAt('/settings');
    const tab = await screen.findByRole('tab', { name: 'Instances' });
    expect(tab).toHaveAttribute('aria-selected', 'true');
  });

  it('should_activate_tab_from_query_param', async () => {
    stubFetch();
    renderAt('/settings?tab=keys');
    const tab = await screen.findByRole('tab', { name: 'Admin keys' });
    expect(tab).toHaveAttribute('aria-selected', 'true');
  });

  it('should_fall_back_to_instances_for_unknown_tab', async () => {
    stubFetch();
    renderAt('/settings?tab=bogus');
    const tab = await screen.findByRole('tab', { name: 'Instances' });
    expect(tab).toHaveAttribute('aria-selected', 'true');
  });
});
