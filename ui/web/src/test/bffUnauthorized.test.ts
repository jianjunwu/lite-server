import { afterEach, describe, expect, it, vi } from 'vitest';
import { setOnBffUnauthorized } from '../api/client';
import { uploadModelFiles } from '../api/mutations';
import { inferUnary, streamEvents } from '../api/playground';

const BFF_401 = new Response(JSON.stringify({ error: 'bff_unauthenticated' }), {
  status: 401,
  headers: { 'content-type': 'application/json' },
});
const INSTANCE_401 = new Response(JSON.stringify({ error: 'admin key required' }), {
  status: 401,
  headers: { 'content-type': 'application/json' },
});

let notified: number;
let handler: () => void;

function stubFetch(res: Response) {
  vi.stubGlobal('fetch', vi.fn(() => Promise.resolve(res.clone())));
}

afterEach(() => {
  vi.unstubAllGlobals();
  setOnBffUnauthorized(null);
});

function installHandler() {
  notified = 0;
  handler = () => {
    notified += 1;
  };
  setOnBffUnauthorized(handler);
}

describe('BFF 401 notification outside client.ts', () => {
  it('should_notify_on_a_bff_401_from_inferUnary_but_not_on_an_instance_401', async () => {
    installHandler();
    stubFetch(BFF_401);
    await expect(inferUnary('inst', 'm', null, '{}')).rejects.toThrow();
    expect(notified).toBe(1);

    stubFetch(INSTANCE_401);
    await expect(inferUnary('inst', 'm', null, '{}')).rejects.toThrow();
    expect(notified).toBe(1);
  });

  it('should_notify_on_a_bff_401_from_streamEvents', async () => {
    installHandler();
    stubFetch(BFF_401);
    const error = await new Promise<Error>((resolve) => {
      streamEvents('inst', 'm', null, '{}', {
        onEvent: () => {},
        onDone: () => {},
        onError: resolve,
      });
    });
    expect(error).toBeTruthy();
    expect(notified).toBe(1);
  });

  class FakeXHR {
    static last: FakeXHR | null = null;
    status = 0;
    responseText = '';
    upload = { addEventListener: () => {} };
    private listeners: Record<string, Array<() => void>> = {};
    addEventListener(type: string, fn: () => void) {
      (this.listeners[type] ??= []).push(fn);
    }
    open() {}
    setRequestHeader() {}
    getResponseHeader() {
      return null;
    }
    send() {
      FakeXHR.last = this;
    }
    respond(status: number, body: string) {
      this.status = status;
      this.responseText = body;
      for (const fn of this.listeners.load ?? []) fn();
    }
  }

  it('should_notify_on_a_bff_401_from_uploads_but_not_on_an_instance_401', async () => {
    installHandler();
    vi.stubGlobal('XMLHttpRequest', FakeXHR);
    const file = new File(['x'], 'model.bin');

    const first = uploadModelFiles('inst', 'm', '1', [file]).promise;
    FakeXHR.last!.respond(401, JSON.stringify({ error: 'bff_unauthenticated' }));
    await expect(first).rejects.toThrow();
    expect(notified).toBe(1);

    const second = uploadModelFiles('inst', 'm', '1', [file]).promise;
    FakeXHR.last!.respond(401, JSON.stringify({ error: 'admin key required' }));
    await expect(second).rejects.toThrow();
    expect(notified).toBe(1);
  });
});
