import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { isSettled, LIFECYCLE_POLL_MS, LIFECYCLE_TIMEOUT_MS, useLifecycleOp } from '../components/useLifecycleOp';
import type { VersionsResponse } from '../api/types';

const addTask = vi.fn(() => 'task-1');
const updateTask = vi.fn();

vi.mock('../context/TaskContext', () => ({
  useTasks: () => ({ addTask, updateTask }),
}));

vi.mock('../context/InstanceContext', () => ({
  useInstance: () => ({ instanceId: 'prod', setInstanceId: vi.fn() }),
}));

const version = (over: Partial<VersionsResponse['versions'][number]>) => ({
  version: '2',
  status: 'loading',
  active: false,
  weight: 0,
  workers: { ready: 0, total: 1 },
  loaded_at: null,
  ...over,
});

function wrapper({ children }: { children: ReactNode }) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return (
    <QueryClientProvider client={queryClient}>
      <AntdApp>{children}</AntdApp>
    </QueryClientProvider>
  );
}

/** fetch stub: mutations (non-GET) succeed; GET versions pops from the queue
 * (last entry repeats once the queue drains). */
function stubVersionsQueue(queue: Array<{ status: number; body: unknown }>) {
  let last = queue[queue.length - 1];
  return vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const method = init?.method ?? 'GET';
    if (method !== 'GET') {
      return Promise.resolve(
        new Response(JSON.stringify({ success: true }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      );
    }
    const next = queue.length > 0 ? (queue.shift() as typeof last) : last;
    last = next;
    return Promise.resolve(
      new Response(JSON.stringify(next.body), {
        status: next.status,
        headers: { 'content-type': 'application/json' },
      }),
    );
  });
}

beforeEach(() => {
  addTask.mockClear();
  updateTask.mockClear();
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

const resp = (versions: VersionsResponse['versions'], active_version: string | null): VersionsResponse => ({
  name: 'm',
  versions,
  active_version,
});

describe('isSettled', () => {
  it('should_settle_load_and_reload_only_when_the_version_is_ready', () => {
    const loading = resp([version({ status: 'loading' })], null);
    const ready = resp([version({ status: 'ready' })], '2');
    expect(isSettled('load', loading, '2')).toBe(false);
    expect(isSettled('reload', loading, '2')).toBe(false);
    expect(isSettled('load', ready, '2')).toBe(true);
    expect(isSettled('reload', ready, '2')).toBe(true);
  });

  it('should_settle_activate_only_when_the_version_becomes_active', () => {
    const inactive = resp([version({})], null);
    const active = resp([version({ active: true })], '2');
    expect(isSettled('activate', inactive, '2')).toBe(false);
    expect(isSettled('activate', active, '2')).toBe(true);
  });

  it('should_settle_unload_once_the_version_leaves_the_registry', () => {
    const present = resp([version({})], null);
    const gone = resp([], null);
    expect(isSettled('unload', present, '2')).toBe(false);
    expect(isSettled('unload', gone, '2')).toBe(true);
    // 404 (nothing loaded at all) also means gone.
    expect(isSettled('unload', null, '2')).toBe(true);
  });
});

describe('useLifecycleOp', () => {
  it('should_open_a_task_on_accept_and_settle_it_when_the_version_turns_ready', async () => {
    vi.stubGlobal(
      'fetch',
      stubVersionsQueue([
        { status: 200, body: { versions: [version({ status: 'loading' })], active_version: null } },
        { status: 200, body: { versions: [version({ status: 'ready' })], active_version: '2' } },
      ]),
    );
    const { result } = renderHook(() => useLifecycleOp(), { wrapper });

    await act(async () => {
      await result.current.runLifecycle('load', 'm', '2');
    });
    expect(addTask).toHaveBeenCalledWith(expect.objectContaining({ kind: 'load' }));

    // First poll: still loading → task stays running with a state detail.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(LIFECYCLE_POLL_MS);
    });
    expect(updateTask).toHaveBeenCalledWith('task-1', expect.objectContaining({ detail: expect.stringMatching(/loading/i) }));
    expect(updateTask).not.toHaveBeenCalledWith('task-1', expect.objectContaining({ status: 'success' }));

    // Second poll: ready → success.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(LIFECYCLE_POLL_MS);
    });
    expect(updateTask).toHaveBeenCalledWith('task-1', expect.objectContaining({ status: 'success', progress: 100 }));
  });

  it('should_settle_unload_when_the_versions_endpoint_starts_404ing', async () => {
    vi.stubGlobal('fetch', stubVersionsQueue([{ status: 404, body: { error: 'not found' } }]));
    const { result } = renderHook(() => useLifecycleOp(), { wrapper });

    await act(async () => {
      await result.current.runLifecycle('unload', 'm', '2');
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(LIFECYCLE_POLL_MS);
    });
    expect(updateTask).toHaveBeenCalledWith('task-1', expect.objectContaining({ status: 'success' }));
  });

  it('should_fail_the_task_after_the_timeout_when_never_settled', async () => {
    vi.stubGlobal(
      'fetch',
      stubVersionsQueue([{ status: 200, body: { versions: [version({ status: 'loading' })], active_version: null } }]),
    );
    const { result } = renderHook(() => useLifecycleOp(), { wrapper });

    await act(async () => {
      await result.current.runLifecycle('load', 'm', '2');
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(LIFECYCLE_TIMEOUT_MS + LIFECYCLE_POLL_MS);
    });
    expect(updateTask).toHaveBeenCalledWith('task-1', expect.objectContaining({ status: 'error' }));
  });

  it('should_not_open_a_task_when_the_mutation_is_rejected', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response(JSON.stringify({ error: 'boom' }), {
            status: 500,
            headers: { 'content-type': 'application/json' },
          }),
        ),
      ),
    );
    const { result } = renderHook(() => useLifecycleOp(), { wrapper });
    await act(async () => {
      await result.current.runLifecycle('load', 'm', '2');
    });
    expect(addTask).not.toHaveBeenCalled();
  });
});
