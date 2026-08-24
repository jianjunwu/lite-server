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

let calls: Array<{ url: string; method: string; body: string | null }>;

function installFetch() {
  calls = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      calls.push({ url, method: init?.method ?? 'GET', body: (init?.body as string) ?? null });
      if (url.includes('/api/users')) {
        return Promise.resolve(
          json({ users: [{ username: 'u1', role: 'viewer', createdAt: '2026-01-01', mustChangePassword: false }] }),
        );
      }
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

async function openEditDrawer() {
  fireEvent.click(await screen.findByRole('tab', { name: 'Users' }));
  fireEvent.click(await screen.findByRole('button', { name: 'Edit' }));
  return screen.findByLabelText('Password');
}

const putCalls = () => calls.filter((c) => c.method === 'PUT');

afterEach(() => vi.unstubAllGlobals());

describe('SettingsPage user edit', () => {
  it('should_block_a_short_password_with_a_validation_error_instead_of_silently_ignoring_it', async () => {
    installFetch();
    renderSettings();
    const password = await openEditDrawer();
    fireEvent.change(password, { target: { value: 'short' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    // No PUT goes out, and the form explains why.
    await waitFor(() =>
      expect(document.querySelector('.ant-form-item-explain-error')).not.toBeNull(),
    );
    expect(putCalls()).toHaveLength(0);
  });

  it('should_send_the_password_in_the_patch_when_valid', async () => {
    installFetch();
    renderSettings();
    const password = await openEditDrawer();
    fireEvent.change(password, { target: { value: 'long-enough-1' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(putCalls()).toHaveLength(1));
    expect(putCalls()[0].body).toContain('"password":"long-enough-1"');
  });
});
