import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { ApiError } from '../api/client';
import { UploadDrawer } from '../components/UploadDrawer';
import { uploadModelFilesResumable } from '../api/upload';

vi.mock('../api/upload', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../api/upload')>();
  return { ...mod, uploadModelFilesResumable: vi.fn() };
});

vi.mock('../context/InstanceContext', () => ({
  useInstance: () => ({ instanceId: 'prod' }),
}));

const updateTask = vi.fn();
vi.mock('../context/TaskContext', () => ({
  useTasks: () => ({ addTask: () => 'task-1', updateTask }),
}));

function renderDrawer(onClose = vi.fn()) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <MemoryRouter>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <UploadDrawer open onClose={onClose} existingModels={[]} />
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
  return onClose;
}

/** Fill the form and attach one file, then submit. */
async function fillAndSubmit() {
  fireEvent.change(screen.getByLabelText('Model'), { target: { value: 'm' } });
  fireEvent.change(screen.getByLabelText('Version'), { target: { value: '1' } });
  const file = new File(['x'.repeat(64)], 'w.bin');
  const input = document.querySelector('input[type="file"]') as HTMLInputElement;
  fireEvent.change(input, { target: { files: [file] } });
  await waitFor(() => expect(screen.getByText(/files selected/)).toBeTruthy());
  fireEvent.click(screen.getByRole('button', { name: 'Upload' }));
}

function handleOf(result: unknown) {
  return { promise: result instanceof Promise ? result : Promise.resolve(result), abort: vi.fn() };
}

beforeEach(() => {
  vi.mocked(uploadModelFilesResumable).mockReset();
  updateTask.mockClear();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('UploadDrawer overwrite flow', () => {
  it('should_offer_force_retry_after_a_version_exists_409', async () => {
    const conflict = new ApiError(
      409,
      null,
      { code: 'conflict', message: 'version 1 of model m already exists; pass ?force=true to overwrite' },
      'conflict',
    );
    vi.mocked(uploadModelFilesResumable)
      .mockImplementationOnce(() => handleOf(Promise.reject(conflict)))
      .mockImplementationOnce(() =>
        handleOf({ success: true, model: 'm', version: '1', files: ['w.bin'], loaded: false }),
      );
    renderDrawer();

    await fillAndSubmit();
    // The overwrite confirm appears instead of a plain error.
    await screen.findAllByText('Overwrite existing version?');
    fireEvent.click(screen.getByRole('button', { name: 'Overwrite' }));

    await waitFor(() => expect(uploadModelFilesResumable).toHaveBeenCalledTimes(2));
    const retryOpts = vi.mocked(uploadModelFilesResumable).mock.calls[1][4] as { force?: boolean };
    expect(retryOpts.force).toBe(true);
  });

  it('should_not_retry_when_the_overwrite_confirm_is_cancelled', async () => {
    const conflict = new ApiError(
      409,
      null,
      { code: 'conflict', message: 'version 1 of model m already exists; pass ?force=true to overwrite' },
      'conflict',
    );
    vi.mocked(uploadModelFilesResumable).mockImplementationOnce(() =>
      handleOf(Promise.reject(conflict)),
    );
    renderDrawer();

    await fillAndSubmit();
    await screen.findAllByText('Overwrite existing version?');
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));

    await waitFor(() =>
      expect(updateTask).toHaveBeenCalledWith('task-1', expect.objectContaining({ status: 'error' })),
    );
    expect(uploadModelFilesResumable).toHaveBeenCalledTimes(1);
  });

  it('should_show_the_owner_on_an_ownership_403_without_a_retry_offer', async () => {
    const denied = new ApiError(
      403,
      null,
      { error: 'forbidden', reason: 'not_version_owner', owner: 'op1' },
      'forbidden',
    );
    vi.mocked(uploadModelFilesResumable).mockImplementationOnce(() =>
      handleOf(Promise.reject(denied)),
    );
    renderDrawer();

    await fillAndSubmit();
    await screen.findByText(/op1/);
    expect(screen.queryAllByText('Overwrite existing version?')).toHaveLength(0);
    expect(uploadModelFilesResumable).toHaveBeenCalledTimes(1);
  });
});
