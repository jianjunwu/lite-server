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
      if (url.includes('/api/users/u1/model-grants')) {
        if (init?.method === 'PUT') {
          return Promise.resolve(
            json({ grants: [{ instance_id: 'prod', model: 'alpha', role: 'viewer' }] }),
          );
        }
        return Promise.resolve(json({ grants: [] }));
      }
      if (url.includes('/api/users/u1/grants')) return Promise.resolve(json({ grants: [] }));
      if (url.includes('/api/users')) {
        return Promise.resolve(
          json({ users: [{ username: 'u1', role: 'viewer', createdAt: '2026-01-01', mustChangePassword: false }] }),
        );
      }
      if (url.includes('/api/instances')) {
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

describe('SettingsPage instance grants', () => {
  it('should_save_an_instance_grant_from_the_access_drawer', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Users' }));
    fireEvent.click(await screen.findByRole('button', { name: 'Access' }));
    // The drawer lists each visible instance with a role select (first
    // combobox; the model-access section adds more below).
    const [combobox] = await screen.findAllByRole('combobox');
    fireEvent.mouseDown(combobox);
    fireEvent.click(await screen.findByText('operator', { selector: '.ant-select-item-option *' }));
    await waitFor(() =>
      expect(putCalls().some((c) => c.url.includes('/api/users/u1/grants/prod'))).toBe(true),
    );
    const grantPut = putCalls().find((c) => c.url.includes('/api/users/u1/grants/prod'));
    expect(grantPut?.body).toBe('{"role":"operator"}');
  });

  it('should_add_a_model_grant_from_the_access_drawer', async () => {
    installFetch();
    renderSettings();
    fireEvent.click(await screen.findByRole('tab', { name: 'Users' }));
    fireEvent.click(await screen.findByRole('button', { name: 'Access' }));
    const input = await screen.findByPlaceholderText('model name');
    fireEvent.change(input, { target: { value: 'alpha' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add grant' }));
    await waitFor(() =>
      expect(putCalls().some((c) => c.url.includes('/api/users/u1/model-grants/prod/alpha'))).toBe(true),
    );
    const grantPut = putCalls().find((c) => c.url.includes('/api/users/u1/model-grants/prod/alpha'));
    expect(grantPut?.body).toBe('{"role":"viewer"}');
  });
});
