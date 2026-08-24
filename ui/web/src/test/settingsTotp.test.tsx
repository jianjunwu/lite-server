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
      user: { username: 'admin', role: 'admin', createdAt: '', mustChangePassword: false, totpEnabled: false },
      can: () => true,
      refresh: vi.fn().mockResolvedValue(undefined),
    }),
  };
});

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } });

let calls: Array<{ url: string; method: string; body: string | null }>;

function installFetch() {
  calls = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      calls.push({ url, method: init?.method ?? 'GET', body: (init?.body as string) ?? null });
      if (url.includes('/api/auth/totp/enroll')) {
        return Promise.resolve(json({ secret: 'JBSWY3DPEHPK3PXP', otpauthUrl: 'otpauth://totp/lite-ui:admin?secret=JBSWY3DPEHPK3PXP' }));
      }
      if (url.includes('/api/auth/totp/confirm')) {
        return Promise.resolve(json({ backupCodes: ['aaaa1111', 'bbbb2222'] }));
      }
      if (url.includes('/api/auth/sessions')) return Promise.resolve(json({ sessions: [] }));
      if (url.includes('/api/users')) return Promise.resolve(json({ users: [] }));
      if (url.includes('/api/invites')) return Promise.resolve(json({ invites: [] }));
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

describe('SettingsPage two-factor enrollment', () => {
  it('should_enroll_and_confirm_totp_then_show_backup_codes_once', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Security' }));
    fireEvent.click(await screen.findByRole('button', { name: 'Enable 2FA' }));
    expect(await screen.findByText('JBSWY3DPEHPK3PXP')).toBeTruthy();
    fireEvent.change(screen.getByPlaceholderText('6-digit code'), { target: { value: '123456' } });
    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }));
    expect(await screen.findByText(/aaaa1111/)).toBeTruthy();
    const confirmCall = calls.find((c) => c.url.includes('/api/auth/totp/confirm'));
    expect(confirmCall?.body).toContain('"code":"123456"');
    // Backup codes are only shown once: dismissing reveals the sessions card.
    fireEvent.click(screen.getByRole('button', { name: 'Done' }));
    await waitFor(() => expect(screen.queryByText(/aaaa1111/)).toBeNull());
  });
});
