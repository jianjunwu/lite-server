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

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

function installFetch() {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => Promise.resolve(json({ instances: [] }))),
  );
}

function renderSettings() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <MemoryRouter>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <SettingsPage />
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => vi.unstubAllGlobals());

// Instance create/edit/delete flows live on /instances (shared InstanceForm)
// and are covered by instancesPage.test.tsx; this tab is a compat entry card.
describe('SettingsPage instances tab', () => {
  it('should_offer_an_entry_card_linking_to_the_instances_page', async () => {
    installFetch();
    renderSettings();
    const link = await screen.findByRole('link', { name: /Manage instances/ });
    expect(link).toHaveAttribute('href', '/instances');
  });

  it('should_explain_where_instance_management_moved', async () => {
    installFetch();
    renderSettings();
    expect(
      await screen.findByText(/Instance management lives on the Instances page/),
    ).toBeTruthy();
  });
});
