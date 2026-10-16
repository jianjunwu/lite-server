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

const INVITES = [
  { code: 'code-aaa', role: 'viewer', maxUses: 1, useCount: 0, expiresAt: null, createdBy: 'admin', createdAt: '2026-01-01', revokedAt: null },
];

let calls: Array<{ url: string; method: string; body: string | null }>;

function installFetch() {
  calls = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      calls.push({ url, method: init?.method ?? 'GET', body: (init?.body as string) ?? null });
      if (url.includes('/api/invites')) {
        if ((init?.method ?? 'GET') === 'POST') return Promise.resolve(json({ invite: INVITES[0] }));
        return Promise.resolve(json({ invites: INVITES }));
      }
      if (url.includes('/api/auth/sessions')) return Promise.resolve(json({ sessions: [] }));
      if (url.includes('/api/users')) return Promise.resolve(json({ users: [] }));
      if (url.includes('/api/audit')) return Promise.resolve(json({ entries: [] }));
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

describe('SettingsPage invites tab', () => {
  it('should_list_invites_and_create_one_with_operator_role', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Invites' }));
    expect(await screen.findByText('code-aaa')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: /New invite/ }));
    const roleSelect = (await screen.findByText('viewer', { selector: '.ant-select-selection-item' }))
      .closest('.ant-select-selector') as HTMLElement;
    fireEvent.mouseDown(roleSelect);
    fireEvent.click(await screen.findByText('operator', { selector: '.ant-select-item-option-content' }));
    fireEvent.click(screen.getByRole('button', { name: 'Create invite' }));
    await waitFor(() =>
      expect(calls.some((c) => c.method === 'POST' && c.url.includes('/api/invites') && c.body?.includes('"role":"operator"'))).toBe(true),
    );
  });

  it('should_send_max_uses_and_expires_in_hours_as_numbers', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Invites' }));
    await screen.findByText('code-aaa');
    fireEvent.click(screen.getByRole('button', { name: /New invite/ }));
    const [maxUsesInput, expiresInput] = (await screen.findAllByRole('spinbutton')) as HTMLInputElement[];
    fireEvent.change(maxUsesInput, { target: { value: '50' } });
    fireEvent.change(expiresInput, { target: { value: '24' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create invite' }));
    await waitFor(() =>
      expect(calls.some((c) => c.method === 'POST' && c.url.includes('/api/invites'))).toBe(true),
    );
    const posted = calls.find((c) => c.method === 'POST' && c.url.includes('/api/invites'))!;
    const body = JSON.parse(posted.body ?? '{}');
    // The BFF parses raw JSON and calls int()/timedelta() on these, so a
    // numeric input that arrives as a string is an internal error there.
    expect(typeof body.maxUses, `maxUses must be a number, got ${posted.body}`).toBe('number');
    expect(typeof body.expiresInHours, `expiresInHours must be a number, got ${posted.body}`).toBe('number');
  });

  it('should_not_forward_a_non_integer_numeric_entry_as_a_string', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Invites' }));
    await screen.findByText('code-aaa');
    fireEvent.click(screen.getByRole('button', { name: /New invite/ }));
    const [maxUsesInput] = (await screen.findAllByRole('spinbutton')) as HTMLInputElement[];
    fireEvent.change(maxUsesInput, { target: { value: '1e3' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create invite' }));
    // Wait for the form to either submit or reject the entry client-side.
    await waitFor(() => {
      const posted = calls.some((c) => c.method === 'POST' && c.url.includes('/api/invites'));
      const rejected = document.querySelectorAll('.ant-form-item-explain-error').length > 0;
      expect(posted || rejected).toBe(true);
    });
    const posted = calls.find((c) => c.method === 'POST' && c.url.includes('/api/invites'));
    if (posted) {
      const body = JSON.parse(posted.body ?? '{}');
      expect(typeof body.maxUses, `1e3 must not reach the BFF as a string: ${posted.body}`).toBe('number');
    }
  });

  it('should_revoke_an_invite_after_confirmation', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Invites' }));
    await screen.findByText('code-aaa');
    fireEvent.click(screen.getByRole('button', { name: 'Revoke' }));
    fireEvent.click(await screen.findByRole('button', { name: 'OK' }));
    await waitFor(() =>
      expect(calls.some((c) => c.method === 'DELETE' && c.url.includes('/api/invites/code-aaa'))).toBe(true),
    );
  });
});
