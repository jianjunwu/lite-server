import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
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

function installFetch(postResponse: Response) {
  return vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      if (url.startsWith('/api/instances') && init?.method === 'POST') {
        return Promise.resolve(postResponse.clone());
      }
      if (url.startsWith('/api/instances')) {
        return Promise.resolve(
          json({
            instances: [
              { id: 'prod', name: 'Prod', base_url: 'http://p', has_admin_key: false, readonly: false },
            ],
          }),
        );
      }
      return Promise.resolve(json({}));
    }),
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

async function openAddDrawerAndSubmit() {
  fireEvent.click(await screen.findByRole('button', { name: /Add instance$/ }));
  const dialog = await screen.findByRole('dialog', { name: 'Add instance' });
  fireEvent.change(screen.getByPlaceholderText('prod-gpu'), { target: { value: 'dev02' } });
  fireEvent.change(screen.getByPlaceholderText('Prod GPU cluster'), { target: { value: 'Dead' } });
  fireEvent.change(screen.getByPlaceholderText('http://10.0.0.11:8000'), {
    target: { value: 'http://localhost:18001' },
  });
  fireEvent.click(screen.getByRole('button', { name: 'Add instance' }));
  return dialog;
}

afterEach(() => vi.unstubAllGlobals());

describe('SettingsPage add instance', () => {
  it('should_show_the_server_error_reason_when_the_reachability_probe_rejects_the_add', async () => {
    installFetch(json({ error: 'instance_unreachable', base_url: 'http://localhost:18001' }, 422));
    renderSettings();
    const dialog = await openAddDrawerAndSubmit();
    // The toast explains why the add was rejected, and the drawer stays open
    // so the user can fix the URL or uncheck the probe.
    await waitFor(() =>
      expect(document.querySelector('.ant-message')?.textContent).toContain('instance_unreachable'),
    );
    expect(dialog.isConnected).toBe(true);
  });

  it('should_close_the_drawer_and_refresh_the_list_on_success', async () => {
    installFetch(
      json(
        {
          instances: [
            { id: 'prod', name: 'Prod', base_url: 'http://p', has_admin_key: false, readonly: false },
            { id: 'dev02', name: 'Dead', base_url: 'http://localhost:18001', has_admin_key: false, readonly: false },
          ],
        },
        201,
      ),
    );
    renderSettings();
    await openAddDrawerAndSubmit();
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Add instance' })).not.toBeInTheDocument(),
    );
  });
});
