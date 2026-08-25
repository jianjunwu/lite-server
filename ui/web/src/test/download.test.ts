import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { downloadModelPackage, __test__ } from '../api/download';

interface WriteRec {
  position: number;
  data: Uint8Array;
}

function installPicker() {
  const writes: WriteRec[] = [];
  const state = { closed: false, aborted: false };
  const writable = {
    write: vi.fn(async (chunk: { type: string; position?: number; data: Uint8Array }) => {
      writes.push({ position: chunk.position ?? -1, data: chunk.data });
    }),
    close: vi.fn(async () => {
      state.closed = true;
    }),
    abort: vi.fn(async () => {
      state.aborted = true;
    }),
  };
  const picker = vi.fn(async (_opts?: { suggestedName?: string }) => ({
    createWritable: async () => writable,
  }));
  (window as unknown as { showSaveFilePicker: unknown }).showSaveFilePicker = picker;
  return { writes, state, picker };
}

function streamOf(parts: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const p of parts) controller.enqueue(new TextEncoder().encode(p));
      controller.close();
    },
  });
}

function okResponse(parts: string[], init?: { status?: number; headers?: Record<string, string> }) {
  return new Response(streamOf(parts), {
    status: init?.status ?? 200,
    headers: init?.headers,
  });
}

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
  delete (window as unknown as { showSaveFilePicker?: unknown }).showSaveFilePicker;
});

describe('downloadModelPackage', () => {
  it('should_stream_fresh_download_to_disk_and_clear_state', async () => {
    const { writes, state } = installPicker();
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      okResponse(['01234', '56789'], { headers: { etag: '"e1"', 'content-length': '10' } }),
    );
    vi.stubGlobal('fetch', fetchMock);

    const progress: Array<[number, number, number]> = [];
    const handle = downloadModelPackage('prod', 'm', '1', {
      onProgress: (p, loaded, total) => progress.push([p, loaded, total]),
    });
    const result = await handle.promise;

    expect(result.fileName).toBe('m_v1.lma');
    expect(writes.map((w) => w.position)).toEqual([0, 5]);
    expect(writes.map((w) => new TextDecoder().decode(w.data)).join('')).toBe('0123456789');
    expect(state.closed).toBe(true);
    expect(localStorage.getItem(__test__.storageKey('prod', 'm', '1'))).toBeNull();
    expect(progress[progress.length - 1]).toEqual([100, 10, 10]);
    // No Range header on a fresh download.
    const reqHeaders = (fetchMock.mock.calls[0][1] as RequestInit).headers as Record<string, string>;
    expect('range' in reqHeaders).toBe(false);
  });

  it('should_resume_from_stored_offset_with_if_range', async () => {
    const { writes } = installPicker();
    localStorage.setItem(
      __test__.storageKey('prod', 'm', '1'),
      JSON.stringify({ etag: '"e1"', offset: 5, fileName: 'm_v1.lma' }),
    );
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      okResponse(['56789'], {
        status: 206,
        headers: {
          etag: '"e1"',
          'content-range': 'bytes 5-9/10',
          'content-length': '5',
        },
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    const handle = downloadModelPackage('prod', 'm', '1');
    await handle.promise;

    const reqHeaders = (fetchMock.mock.calls[0][1] as RequestInit).headers as Record<string, string>;
    expect(reqHeaders['range']).toBe('bytes=5-');
    expect(reqHeaders['if-range']).toBe('"e1"');
    expect(writes.map((w) => w.position)).toEqual([5]);
  });

  it('should_restart_from_zero_when_server_returns_200_to_a_ranged_request', async () => {
    const { writes } = installPicker();
    localStorage.setItem(
      __test__.storageKey('prod', 'm', '1'),
      JSON.stringify({ etag: '"stale"', offset: 5, fileName: 'm_v1.lma' }),
    );
    // ETag changed server-side: the ranged request gets a full 200 body.
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      okResponse(['0123456789'], { headers: { etag: '"e2"', 'content-length': '10' } }),
    );
    vi.stubGlobal('fetch', fetchMock);

    const handle = downloadModelPackage('prod', 'm', '1');
    await handle.promise;

    expect(writes[0].position).toBe(0);
    expect(writes.map((w) => new TextDecoder().decode(w.data)).join('')).toBe('0123456789');
  });

  it('should_keep_state_for_resume_after_a_network_failure', async () => {
    installPicker();
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      // One chunk, then the connection dies mid-stream.
      let sent = false;
      return new Response(
        new ReadableStream({
          pull(controller) {
            if (!sent) {
              sent = true;
              controller.enqueue(new TextEncoder().encode('01234'));
            } else {
              controller.error(new Error('connection reset'));
            }
          },
        }),
        { headers: { etag: '"e1"', 'content-length': '10' } },
      );
    });
    vi.stubGlobal('fetch', fetchMock);

    const handle = downloadModelPackage('prod', 'm', '1');
    await expect(handle.promise).rejects.toThrow();

    const stored = JSON.parse(localStorage.getItem(__test__.storageKey('prod', 'm', '1'))!);
    expect(stored.offset).toBe(5);
    expect(stored.etag).toBe('"e1"');
  });

  it('should_release_the_response_stream_when_the_picker_is_cancelled', async () => {
    (window as unknown as { showSaveFilePicker: unknown }).showSaveFilePicker = vi.fn(
      async () => {
        throw new DOMException('The user aborted a request', 'AbortError');
      },
    );
    const cancelSpy = vi.fn();
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode('01234'));
          },
          cancel: cancelSpy,
        }),
        { headers: { etag: '"e1"', 'content-length': '10' } },
      ),
    );
    vi.stubGlobal('fetch', fetchMock);

    const handle = downloadModelPackage('prod', 'm', '1');
    await expect(handle.promise).rejects.toThrow('download cancelled');

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect(init.signal?.aborted).toBe(true);
    expect(cancelSpy).toHaveBeenCalled();
  });

  it('should_rethrow_and_release_the_stream_when_createWritable_fails', async () => {
    (window as unknown as { showSaveFilePicker: unknown }).showSaveFilePicker = vi.fn(
      async () => ({
        createWritable: async () => {
          throw new Error('disk full');
        },
      }),
    );
    const cancelSpy = vi.fn();
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode('01234'));
          },
          cancel: cancelSpy,
        }),
        { headers: { etag: '"e1"', 'content-length': '10' } },
      ),
    );
    vi.stubGlobal('fetch', fetchMock);

    const handle = downloadModelPackage('prod', 'm', '1');
    await expect(handle.promise).rejects.toThrow('disk full');
    expect(cancelSpy).toHaveBeenCalled();
  });

  it('should_fall_back_to_anchor_navigation_without_fs_access_api', async () => {
    // No showSaveFilePicker installed.
    const click = vi
      .spyOn(HTMLAnchorElement.prototype, 'click')
      .mockImplementation(() => {});
    const handle = downloadModelPackage('prod', 'm', '1');
    await handle.promise;
    expect(click).toHaveBeenCalledTimes(1);
    click.mockRestore();
  });
});
