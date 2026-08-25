import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { useMergedModels, useRepoIndex, useVersions } from '../api/hooks';

function wrapper({ children }: { children: ReactNode }) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } });

const notFound = () =>
  new Response(JSON.stringify({ error: 'model not found' }), {
    status: 404,
    headers: { 'content-type': 'application/json' },
  });

afterEach(() => vi.unstubAllGlobals());

describe('useVersions', () => {
  it('should_not_fetch_when_model_is_empty', async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    renderHook(() => useVersions('prod', ''), { wrapper });
    // An empty model would poll /v2/models//versions (404) every 10s.
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('should_fetch_when_model_is_set', async () => {
    const fetchMock = vi.fn().mockResolvedValue(json({ versions: [], active_version: 'v1' }));
    vi.stubGlobal('fetch', fetchMock);
    renderHook(() => useVersions('prod', 'm'), { wrapper });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(String(fetchMock.mock.calls[0][0])).toContain('/v2/models/m/versions');
  });

  it('should_resolve_null_on_404_because_unloaded_is_a_state_not_an_error', async () => {
    const fetchMock = vi.fn().mockResolvedValue(notFound());
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useVersions('prod', 'm'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toBeNull();
  });
});

describe('useRepoIndex', () => {
  it('should_post_to_repository_index', async () => {
    const fetchMock = vi.fn().mockResolvedValue(json({ models: [] }));
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useRepoIndex('prod'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    const [url, init] = fetchMock.mock.calls[0];
    expect(String(url)).toContain('/api/i/prod/v2/repository/index');
    expect((init as RequestInit).method).toBe('POST');
  });
});

describe('useMergedModels', () => {
  it('should_union_repo_and_loaded_models', async () => {
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
          json({ models: [{ name: 'echo', version: '1', status: 'ready', model_type: 'LitAPI', workers: 2 }] }),
        );
      }
      return Promise.resolve(notFound());
    });
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useMergedModels('prod'), { wrapper });
    await waitFor(() => expect(result.current.data.length).toBe(2));
    const byName = Object.fromEntries(result.current.data.map((m) => [m.name, m]));
    expect(byName.echo.status).toBe('ready');
    expect(byName.ghost.status).toBe('unloaded');
  });

  it('should_degrade_to_loaded_only_when_repo_index_fails', async () => {
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes('/v2/repository/index')) return Promise.resolve(notFound());
      if (url.includes('/v2/models')) {
        return Promise.resolve(
          json({ models: [{ name: 'echo', version: '1', status: 'ready', model_type: 'LitAPI', workers: 1 }] }),
        );
      }
      return Promise.resolve(notFound());
    });
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useMergedModels('prod'), { wrapper });
    // useRepoIndex retries once (~1s backoff) — wait for the failure to settle.
    await waitFor(() => expect(result.current.repoUnavailable).toBe(true), { timeout: 3000 });
    expect(result.current.data.length).toBe(1);
    expect(result.current.data[0]).toMatchObject({ name: 'echo', status: 'ready' });
  });
});
