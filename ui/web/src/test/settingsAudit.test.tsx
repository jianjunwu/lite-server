import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { SettingsPage } from '../pages/SettingsPage';

let role = 'admin';
vi.mock('../context/AuthContext', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../context/AuthContext')>();
  return {
    ...mod,
    useAuth: () => ({
      user: { username: 'someone', role, createdAt: '', mustChangePassword: false },
      can: (required: string) => role === 'admin' || required !== 'admin',
    }),
  };
});

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } });

const ENTRIES = [
  { id: 1, ts: '2026-01-01T00:00:00Z', actor: 'admin', action: 'login_success', target: null, ip: '127.0.0.1', detail: null },
];

let calls: Array<{ url: string; method: string }>;

function installFetch() {
  calls = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      calls.push({ url, method: init?.method ?? 'GET' });
      if (url.includes('/api/audit')) return Promise.resolve(json({ entries: ENTRIES }));
      if (url.includes('/api/auth/sessions')) return Promise.resolve(json({ sessions: [] }));
      if (url.includes('/api/users')) return Promise.resolve(json({ users: [] }));
      if (url.includes('/api/instances')) return Promise.resolve(json({ instances: [] }));
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

afterEach(() => {
  vi.unstubAllGlobals();
  role = 'admin';
});

describe('SettingsPage audit tab', () => {
  it('should_list_audit_entries_for_admin', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Audit' }));
    expect(await screen.findByText('login_success')).toBeTruthy();
  });

  it('should_refetch_with_the_action_filter', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Audit' }));
    await screen.findByText('login_success');
    const select = document.querySelector('.ant-select-selector') as HTMLElement;
    fireEvent.mouseDown(select);
    fireEvent.click(await screen.findByText('login_failure', { selector: '.ant-select-item-option-content' }));
    await waitFor(() =>
      expect(calls.some((c) => c.url.includes('/api/audit') && c.url.includes('action=login_failure'))).toBe(true),
    );
  });

  it('should_hide_the_audit_tab_from_non_admins', async () => {
    role = 'viewer';
    installFetch();
    renderSettings();
    await screen.findByRole('tab', { name: 'Instances' });
    expect(screen.queryByRole('tab', { name: 'Audit' })).toBeNull();
  });
});
