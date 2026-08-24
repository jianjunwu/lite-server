import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { useVersions } from '../api/hooks';

function wrapper({ children }: { children: ReactNode }) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } });

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
});
