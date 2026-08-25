import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { ApiError } from '../api/client';
import { downloadModelPackage } from '../api/download';
import { VersionActions } from '../components/VersionActions';
import type { VersionInfo } from '../api/types';

vi.mock('../api/download', () => ({ downloadModelPackage: vi.fn() }));

vi.mock('../context/InstanceContext', () => ({
  useInstance: () => ({ instanceId: 'prod' }),
}));

const updateTask = vi.fn();
vi.mock('../context/TaskContext', () => ({
  useTasks: () => ({ addTask: () => 'task-1', updateTask }),
}));

const version: VersionInfo = {
  version: '1',
  status: 'ready',
  active: false,
  weight: 1,
  workers: { ready: 1, total: 1 },
  loaded_at: null,
};

function renderActions(v: VersionInfo = version) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={queryClient}>
      <AntdApp>
        <VersionActions model="m" version={v} />
      </AntdApp>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.mocked(downloadModelPackage).mockReset();
  updateTask.mockClear();
});

describe('VersionActions download task', () => {
  it('should_register_abort_on_the_task_and_surface_cancel_as_an_error', async () => {
    const abortHandle = vi.fn();
    let rejectPromise: (err: unknown) => void = () => {};
    vi.mocked(downloadModelPackage).mockReturnValue({
      promise: new Promise((_resolve, reject) => {
        rejectPromise = reject;
      }),
      abort: abortHandle,
    });
    renderActions();

    fireEvent.click(screen.getByRole('button', { name: 'Download' }));

    // The running task carries an abort callback wired to the handle.
    await waitFor(() =>
      expect(updateTask).toHaveBeenCalledWith(
        'task-1',
        expect.objectContaining({ abort: expect.any(Function) }),
      ),
    );
    const registration = updateTask.mock.calls.find(
      ([, patch]) => typeof (patch as { abort?: unknown }).abort === 'function',
    );
    (registration![1] as { abort: () => void }).abort();
    expect(abortHandle).toHaveBeenCalledTimes(1);

    // The resulting rejection surfaces as the standard cancelled state.
    rejectPromise(new ApiError(0, null, null, 'download cancelled'));
    await waitFor(() =>
      expect(updateTask).toHaveBeenCalledWith(
        'task-1',
        expect.objectContaining({ status: 'error', detail: 'download cancelled' }),
      ),
    );
  });
});

describe('VersionActions unloaded version', () => {
  const unloaded: VersionInfo = {
    version: '2',
    status: 'unloaded',
    active: false,
    weight: 0,
    workers: { ready: 0, total: 0 },
    loaded_at: null,
    loaded: false,
  };

  it('should_offer_load_and_delete_but_no_runtime_ops', () => {
    renderActions(unloaded);
    expect(screen.getByRole('button', { name: 'Load' })).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Delete' })).toBeTruthy();
    expect(screen.queryByRole('button', { name: 'Unload' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Activate' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Reload' })).toBeNull();
  });

  it('should_keep_runtime_ops_for_loaded_versions', () => {
    renderActions(version);
    expect(screen.getByRole('button', { name: 'Unload' })).toBeTruthy();
    expect(screen.queryByRole('button', { name: 'Load' })).toBeNull();
  });
});
