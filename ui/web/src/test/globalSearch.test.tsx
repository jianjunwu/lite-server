import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { GlobalSearch } from '../components/GlobalSearch';

vi.mock('../api/hooks', () => ({
  useInstances: () => ({
    data: { instances: [{ id: 'prod', name: 'Prod', base_url: 'http://p' }] },
    isLoading: false,
  }),
  useInstanceName: () => 'Prod',
}));

vi.mock('../context/ThemeModeContext', () => ({
  useNeutrals: () => new Proxy({}, { get: () => '#888' }),
}));

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } });

function renderSearch() {
  const fetchMock = vi.fn((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes('/v2/repository/index')) {
      return Promise.resolve(
        json({
          models: [
            { name: 'echo', version: '1', path: '/r/echo/1', has_config: true, type: 'litapi' },
            { name: 'ghost', version: '1', path: '/r/ghost/1', has_config: true, type: 'litapi' },
          ],
        }),
      );
    }
    if (url.includes('/v2/models')) {
      return Promise.resolve(
        json({ models: [{ name: 'echo', version: '1', status: 'ready', model_type: 'LitAPI', workers: 1 }] }),
      );
    }
    return Promise.resolve(new Response('{}', { status: 404 }));
  });
  vi.stubGlobal('fetch', fetchMock);
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <MemoryRouter>
      <QueryClientProvider client={queryClient}>
        <GlobalSearch />
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => vi.unstubAllGlobals());

describe('GlobalSearch repository models', () => {
  it('should_find_unloaded_models_from_the_repo_index', async () => {
    renderSearch();
    const input = screen.getByRole('combobox');
    fireEvent.mouseDown(input);
    fireEvent.change(input, { target: { value: 'ghost' } });
    // ghost exists only in the repository scan (never loaded).
    await waitFor(() => expect(screen.getByText('ghost')).toBeTruthy());
    // The loaded model still shows up from /v2/models.
    fireEvent.change(input, { target: { value: 'echo' } });
    await waitFor(() => expect(screen.getByText('echo')).toBeTruthy());
  });
});
