import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { PlaygroundPage } from '../pages/PlaygroundPage';
import { InstanceProvider } from '../context/InstanceContext';

const MODELS = {
  models: [{ name: 'm', version: 'v1', status: 'READY', model_type: 'text', workers: 1 }],
};
const VERSIONS = { versions: [], active_version: 'v1' };

const json = (body: unknown) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

let inferSignal: AbortSignal | null | undefined;
let eventsSignal: AbortSignal | null | undefined;

function installFetch() {
  inferSignal = undefined;
  eventsSignal = undefined;
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      if (url.endsWith('/infer')) {
        inferSignal = init?.signal;
        return new Promise(() => {}); // hangs; only abort can end it
      }
      if (url.endsWith('/events')) {
        eventsSignal = init?.signal;
        return Promise.resolve(
          new Response(new ReadableStream({ start() {} }), { status: 200 }),
        );
      }
      if (url.includes('/versions')) return Promise.resolve(json(VERSIONS));
      if (url.includes('/v2/models')) return Promise.resolve(json(MODELS));
      return Promise.resolve(json({}));
    }),
  );
}

function renderPage() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <MemoryRouter initialEntries={['/?i=prod']}>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <InstanceProvider>
            <PlaygroundPage />
          </InstanceProvider>
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

async function sendEnabled() {
  // antd icons contribute their aria-label to the accessible name
  // ("send Send"), so match loosely.
  const send = await screen.findByRole('button', { name: /send$/i });
  await waitFor(() => expect(send).not.toBeDisabled());
  return send;
}

afterEach(() => vi.unstubAllGlobals());

describe('PlaygroundPage abort handling', () => {
  it('should_abort_unary_request_when_stop_is_clicked', async () => {
    installFetch();
    renderPage();
    fireEvent.click(await sendEnabled());
    await waitFor(() => expect(inferSignal).toBeTruthy());
    fireEvent.click(screen.getByRole('button', { name: /stop$/i }));
    expect(inferSignal!.aborted).toBe(true);
  });

  it('should_abort_streaming_request_when_page_unmounts', async () => {
    installFetch();
    const { unmount } = renderPage();
    const send = await sendEnabled();
    fireEvent.click(screen.getByText('SSE'));
    fireEvent.click(send);
    await waitFor(() => expect(eventsSignal).toBeTruthy());
    unmount();
    expect(eventsSignal!.aborted).toBe(true);
  });
});
