import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// Mock the legacy multipart path so the small-file fallback is observable.
vi.mock('../api/mutations', () => ({
  uploadModelFiles: vi.fn(() => ({
    promise: Promise.resolve({ success: true, model: 'm', version: '1', files: [], loaded: true }),
    abort: vi.fn(),
  })),
}));

import { uploadModelFiles } from '../api/mutations';
import {
  uploadModelChunked,
  uploadModelFilesResumable,
  __test__,
} from '../api/upload';

interface RecordedReq {
  url: string;
  method: string;
  body?: Blob;
}

/** A stateful fake of the chunked-upload API, driven through global fetch. */
function makeServer(opts: {
  chunkSize?: number;
  failFirstChunkOnce?: boolean;
  sessionExists?: boolean;
} = {}) {
  const chunkSize = opts.chunkSize ?? 8;
  const requests: RecordedReq[] = [];
  const chunks = new Map<string, Blob>();
  let sessionCounter = 0;
  let failedOnce = false;

  const json = (data: unknown, status = 200) =>
    new Response(JSON.stringify(data), {
      status,
      headers: { 'content-type': 'application/json' },
    });

  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method ?? 'GET';
    requests.push({ url, method, body: init?.body instanceof Blob ? init.body : undefined });

    const base = '/api/i/prod/v2/repository/models/m/versions/1/upload-sessions';
    if (url === base && method === 'POST') {
      const body = JSON.parse(String(init?.body)) as { files: { name: string; size: number }[] };
      const sid = `sess-${++sessionCounter}`;
      for (const f of body.files) {
        for (let ci = 0; ci < Math.ceil(f.size / chunkSize); ci++) {
          if (opts.sessionExists && ci === 0) chunks.set(`0/${ci}`, new Blob(['x'.repeat(chunkSize)]));
        }
      }
      return json({ session_id: sid, chunk_size: chunkSize }, 201);
    }
    const getMatch = url.match(/upload-sessions\/([^/?]+)$/);
    if (getMatch && method === 'GET') {
      if (!opts.sessionExists) return json({ error: 'upload session not found' }, 404);
      // Chunk 0 already landed in a previous browser session.
      return json({
        session_id: getMatch[1],
        chunk_size: chunkSize,
        state: 'uploading',
        files: [{ name: 'w.bin', size: 24, received_chunks: [0], complete: false }],
      });
    }
    const putMatch = url.match(/upload-sessions\/([^/]+)\/files\/(\d+)\/chunks\/(\d+)$/);
    if (putMatch && method === 'PUT') {
      if (opts.failFirstChunkOnce && !failedOnce) {
        failedOnce = true;
        return json({ error: 'boom' }, 500);
      }
      chunks.set(`${putMatch[2]}/${putMatch[3]}`, init?.body as Blob);
      return json({ received: Number(putMatch[3]) });
    }
    const completeMatch = url.match(/upload-sessions\/([^/]+)\/complete/);
    if (completeMatch && method === 'POST') {
      return json({ success: true, model: 'm', version: '1', files: ['w.bin'], loaded: true });
    }
    throw new Error(`unexpected request: ${method} ${url}`);
  });

  return { fetchMock, requests, chunks };
}

function fileOf(chunks: string[]): File {
  return new File(chunks, 'w.bin', { lastModified: 1234 });
}

async function blobText(b: Blob | undefined): Promise<string> {
  // jsdom's Blob lacks .text() — read via FileReader.
  if (!b) return '';
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(reader.error);
    reader.readAsText(b);
  });
}

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  vi.mocked(uploadModelFiles).mockClear();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('uploadModelChunked', () => {
  it('should_init_put_all_chunks_and_complete', async () => {
    const server = makeServer({ chunkSize: 8 });
    vi.stubGlobal('fetch', server.fetchMock);

    const file = fileOf(['abcdefgh', 'ijklmnop', 'qrst']); // 20 bytes → 3 chunks
    const handle = uploadModelChunked('prod', 'm', '1', [file], { retryDelayMs: 0 });
    const result = await handle.promise;

    expect(result.success).toBe(true);
    const methods = server.requests.map((r) => `${r.method} ${r.url}`);
    expect(methods[0]).toContain('POST');
    expect(methods[0]).toContain('upload-sessions');
    const puts = server.requests.filter((r) => r.method === 'PUT');
    expect(puts).toHaveLength(3);
    const assembled = (await Promise.all(puts.map((p) => blobText(p.body)))).join('');
    expect(assembled).toBe('abcdefghijklmnopqrst');
    expect(methods[methods.length - 1]).toContain('complete');
    // Session record cleaned after success.
    expect(localStorage.getItem(__test__.storageKey('prod', 'm', '1'))).toBeNull();
  });

  it('should_resume_from_server_bitmap_and_skip_received_chunks', async () => {
    const server = makeServer({ chunkSize: 8, sessionExists: true });
    vi.stubGlobal('fetch', server.fetchMock);

    // A stored session for the same file fingerprint.
    const file = fileOf(['abcdefgh', 'ijklmnop', 'qrstuvwx']); // 24 bytes
    localStorage.setItem(
      __test__.storageKey('prod', 'm', '1'),
      JSON.stringify({
        sessionId: 'sess-existing',
        fingerprints: [`w.bin:${file.size}:1234`],
      }),
    );

    const handle = uploadModelChunked('prod', 'm', '1', [file], { retryDelayMs: 0 });
    await handle.promise;

    const puts = server.requests.filter((r) => r.method === 'PUT');
    // Chunk 0 was already on the server — only 1 and 2 are sent.
    expect(puts).toHaveLength(2);
    expect(puts.every((p) => !p.url.endsWith('/chunks/0'))).toBe(true);
    // No init POST — the stored session was reused.
    expect(
      server.requests.some(
        (r) => r.method === 'POST' && r.url.endsWith('upload-sessions'),
      ),
    ).toBe(false);
  });

  it('should_reinit_when_the_stored_session_is_gone', async () => {
    const server = makeServer({ chunkSize: 8, sessionExists: false });
    vi.stubGlobal('fetch', server.fetchMock);

    const file = fileOf(['abcdefgh']);
    localStorage.setItem(
      __test__.storageKey('prod', 'm', '1'),
      JSON.stringify({ sessionId: 'sess-gone', fingerprints: [`w.bin:${file.size}:1234`] }),
    );

    const handle = uploadModelChunked('prod', 'm', '1', [file], { retryDelayMs: 0 });
    await handle.promise;

    expect(
      server.requests.some((r) => r.method === 'POST' && r.url.endsWith('upload-sessions')),
    ).toBe(true);
    expect(server.requests.filter((r) => r.method === 'PUT')).toHaveLength(1);
  });

  it('should_retry_a_failed_chunk_with_backoff', async () => {
    const server = makeServer({ chunkSize: 8, failFirstChunkOnce: true });
    vi.stubGlobal('fetch', server.fetchMock);

    const file = fileOf(['abcdefgh']);
    const handle = uploadModelChunked('prod', 'm', '1', [file], { retryDelayMs: 0 });
    const result = await handle.promise;

    expect(result.success).toBe(true);
    // One failed PUT + one successful retry.
    expect(server.requests.filter((r) => r.method === 'PUT')).toHaveLength(2);
  });

  it('should_report_progress_as_chunks_land', async () => {
    const server = makeServer({ chunkSize: 8 });
    vi.stubGlobal('fetch', server.fetchMock);

    const progress: Array<[number, number, number]> = [];
    const file = fileOf(['abcdefgh', 'ijklmnop']);
    const handle = uploadModelChunked('prod', 'm', '1', [file], {
      retryDelayMs: 0,
      onProgress: (percent, loaded, total) => progress.push([percent, loaded, total]),
    });
    await handle.promise;

    expect(progress.length).toBeGreaterThanOrEqual(2);
    const last = progress[progress.length - 1];
    expect(last).toEqual([100, 16, 16]);
  });

  it('should_reject_on_abort_without_completing', async () => {
    const server = makeServer({ chunkSize: 8 });
    // Never-resolving PUT to keep the upload in flight.
    server.fetchMock.mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith('upload-sessions')) {
        return new Response(JSON.stringify({ session_id: 'sess-1', chunk_size: 8 }), {
          status: 201,
        });
      }
      if (init?.method === 'PUT') {
        return new Promise<Response>((_resolve, reject) => {
          init.signal?.addEventListener('abort', () =>
            reject(new DOMException('aborted', 'AbortError')),
          );
        });
      }
      throw new Error(`unexpected: ${url}`);
    });
    vi.stubGlobal('fetch', server.fetchMock);

    const file = fileOf(['abcdefgh']);
    const handle = uploadModelChunked('prod', 'm', '1', [file], { retryDelayMs: 0 });
    handle.abort();
    await expect(handle.promise).rejects.toThrow(/cancelled|aborted/i);
    expect(server.requests.some((r) => r.url.includes('complete'))).toBe(false);
  });
});

describe('uploadModelFilesResumable', () => {
  it('should_use_legacy_multipart_for_small_files', async () => {
    const server = makeServer();
    vi.stubGlobal('fetch', server.fetchMock);

    const file = fileOf(['tiny']);
    const handle = uploadModelFilesResumable('prod', 'm', '1', [file]);
    await handle.promise;

    expect(uploadModelFiles).toHaveBeenCalledTimes(1);
    expect(server.requests).toHaveLength(0);
  });
});
