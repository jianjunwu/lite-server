import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { RegisterPage } from '../pages/RegisterPage';

const refresh = vi.fn().mockResolvedValue(undefined);
vi.mock('../context/AuthContext', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../context/AuthContext')>();
  return {
    ...mod,
    useAuth: () => ({ user: null, loading: false, refresh }),
  };
});

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

let inviteRequired = false;
let calls: Array<{ url: string; method: string; body: string | null }>;

function installFetch(registerResponse?: () => Response) {
  calls = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      calls.push({ url, method: init?.method ?? 'GET', body: (init?.body as string) ?? null });
      if (url.includes('/api/auth/registration')) {
        return Promise.resolve(json({ open: !inviteRequired, inviteRequired }));
      }
      if (url.includes('/api/auth/register')) {
        return Promise.resolve(
          registerResponse?.() ?? json({ user: { username: 'founder', role: 'admin', mustChangePassword: false } }, 201),
        );
      }
      return Promise.resolve(json({}));
    }),
  );
}

function renderRegister() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <MemoryRouter>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <RegisterPage />
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

function fillForm(username: string, password: string, inviteCode?: string) {
  fireEvent.change(screen.getByLabelText('Username'), { target: { value: username } });
  fireEvent.change(screen.getByLabelText('Password'), { target: { value: password } });
  fireEvent.change(screen.getByLabelText('Confirm password'), { target: { value: password } });
  if (inviteCode !== undefined) {
    fireEvent.change(screen.getByLabelText('Invite code'), { target: { value: inviteCode } });
  }
}

afterEach(() => {
  vi.unstubAllGlobals();
  inviteRequired = false;
  refresh.mockClear();
});

describe('RegisterPage', () => {
  it('should_hide_the_invite_code_field_when_registration_is_open', async () => {
    installFetch();
    renderRegister();
    await screen.findByLabelText('Username');
    expect(screen.queryByLabelText('Invite code')).toBeNull();
  });

  it('should_require_and_send_the_invite_code_when_registration_is_closed', async () => {
    inviteRequired = true;
    installFetch();
    renderRegister();
    await screen.findByLabelText('Invite code');
    fillForm('newbie', 'Newbie-pass-12', 'invite-xyz');
    fireEvent.click(screen.getByRole('button', { name: 'Register' }));
    await waitFor(() => expect(refresh).toHaveBeenCalled());
    const registerCall = calls.find((c) => c.url.includes('/api/auth/register'));
    expect(registerCall?.body).toContain('"inviteCode":"invite-xyz"');
  });

  it('should_show_a_specific_error_for_an_invalid_invite', async () => {
    inviteRequired = true;
    installFetch(() => json({ error: 'invalid_invite' }, 400));
    renderRegister();
    await screen.findByLabelText('Invite code');
    fillForm('newbie', 'Newbie-pass-12', 'bad-code');
    fireEvent.click(screen.getByRole('button', { name: 'Register' }));
    expect(await screen.findByText('Invalid, expired, or exhausted invite code')).toBeTruthy();
  });
});
