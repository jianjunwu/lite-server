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

function renderActions() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={queryClient}>
      <AntdApp>
        <VersionActions model="m" version={version} />
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
