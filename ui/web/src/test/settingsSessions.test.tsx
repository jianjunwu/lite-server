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

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } });

const MY_SESSIONS = [
  { id: 'aaa', createdAt: '2026-01-01T00:00:00Z', lastSeenAt: '2026-01-01T01:00:00Z', ip: '127.0.0.1', userAgent: 'vitest', current: true },
  { id: 'bbb', createdAt: '2026-01-02T00:00:00Z', lastSeenAt: '2026-01-02T01:00:00Z', ip: '10.0.0.2', userAgent: 'other', current: false },
];

let calls: Array<{ url: string; method: string }>;

function installFetch() {
  calls = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      calls.push({ url, method: init?.method ?? 'GET' });
      if (url.includes('/api/users/') && url.includes('/sessions')) {
        return Promise.resolve(json({ sessions: MY_SESSIONS }));
      }
      if (url.includes('/api/users')) {
        return Promise.resolve(
          json({ users: [{ username: 'u1', role: 'viewer', createdAt: '2026-01-01', mustChangePassword: false }] }),
        );
      }
      if (url.includes('/api/auth/sessions')) return Promise.resolve(json({ sessions: MY_SESSIONS }));
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

afterEach(() => vi.unstubAllGlobals());

describe('SettingsPage sessions', () => {
  it('should_list_my_sessions_in_the_security_tab_and_revoke_one', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Security' }));
    expect(await screen.findByText('current')).toBeTruthy();
    const revokes = await screen.findAllByRole('button', { name: 'Revoke' });
    fireEvent.click(revokes[1]);
    await waitFor(() =>
      expect(calls.some((c) => c.method === 'DELETE' && c.url.includes('/api/auth/sessions/bbb'))).toBe(true),
    );
  });

  it('should_let_admin_view_and_kick_a_user_session', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Users' }));
    fireEvent.click(await screen.findByRole('button', { name: 'Sessions' }));
    expect(await screen.findByText('Sessions of u1')).toBeTruthy();
    fireEvent.click((await screen.findAllByRole('button', { name: 'Revoke' }))[0]);
    await waitFor(() =>
      expect(calls.some((c) => c.method === 'DELETE' && c.url.includes('/api/users/u1/sessions/aaa'))).toBe(true),
    );
  });
});
