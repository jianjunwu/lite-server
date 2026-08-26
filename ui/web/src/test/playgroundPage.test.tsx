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
let inferHeaders: Record<string, string> | undefined;

function installFetch() {
  inferSignal = undefined;
  eventsSignal = undefined;
  inferHeaders = undefined;
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = String(input);
      if (url.endsWith('/infer')) {
        inferSignal = init?.signal;
        inferHeaders = init?.headers as Record<string, string>;
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

describe('PlaygroundPage version selector', () => {
  it('should_label_the_unversioned_option_as_weighted_routing_not_active', async () => {
    installFetch();
    renderPage();
    // Model select is first; version-A select is second.
    const comboboxes = await screen.findAllByRole('combobox');
    fireEvent.mouseDown(comboboxes[1]);
    // The bare (unversioned) choice goes through weighted routing — "active"
    // is only the fallback, so the label must not claim it pins active.
    expect(await screen.findByText('Auto (weighted routing)')).toBeTruthy();
    expect(screen.queryByText(/^active/)).toBeNull();
  });
});

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

describe('PlaygroundPage custom headers', () => {
  afterEach(() => localStorage.clear());

  it('should_send_added_header_rows_with_unary_request', async () => {
    installFetch();
    renderPage();
    fireEvent.click(await screen.findByText('Headers'));
    fireEvent.click(await screen.findByRole('button', { name: /add header/i }));
    fireEvent.change(await screen.findByPlaceholderText('Header name'), { target: { value: 'X-Trace-Id' } });
    fireEvent.change(await screen.findByPlaceholderText('Value'), { target: { value: 't-42' } });
    fireEvent.click(await sendEnabled());
    await waitFor(() => expect(inferHeaders).toBeTruthy());
    expect(inferHeaders!['x-trace-id']).toBe('t-42');
    expect(inferHeaders!['x-requested-with']).toBe('lite-ui');
  });

  it('should_persist_header_rows_across_remounts', async () => {
    installFetch();
    const { unmount } = renderPage();
    fireEvent.click(await screen.findByText('Headers'));
    fireEvent.click(await screen.findByRole('button', { name: /add header/i }));
    fireEvent.change(await screen.findByPlaceholderText('Header name'), { target: { value: 'X-A' } });
    fireEvent.change(await screen.findByPlaceholderText('Value'), { target: { value: '1' } });
    unmount();

    renderPage();
    fireEvent.click(await screen.findByText(/Headers/));
    expect(await screen.findByDisplayValue('X-A')).toBeTruthy();
    expect(await screen.findByDisplayValue('1')).toBeTruthy();
  });
});

describe('PlaygroundPage response headers', () => {
  function installResolvingFetch() {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL): Promise<Response> => {
        const url = String(input);
        if (url.endsWith('/infer')) {
          return Promise.resolve(
            new Response('{"output": 10}', {
              status: 200,
              headers: { 'content-type': 'application/json', 'x-backend-hdr': 'b-1' },
            }),
          );
        }
        if (url.includes('/versions')) return Promise.resolve(json(VERSIONS));
        if (url.includes('/v2/models')) return Promise.resolve(json(MODELS));
        return Promise.resolve(json({}));
      }),
    );
  }

  it('should_show_response_headers_after_unary_completes', async () => {
    installResolvingFetch();
    renderPage();
    fireEvent.click(await sendEnabled());
    fireEvent.click(await screen.findByText('Response headers'));
    expect(await screen.findByText(/x-backend-hdr: b-1/)).toBeTruthy();
  });

  it('should_replace_old_response_with_error_when_body_is_invalid_json', async () => {
    installResolvingFetch();
    renderPage();
    fireEvent.click(await sendEnabled());
    expect(await screen.findByText(/"output": 10/)).toBeTruthy();

    fireEvent.change(screen.getByRole('textbox'), { target: { value: '{invalid json' } });
    fireEvent.click(await sendEnabled());

    // The stale success must not survive a failed send: the panel switches
    // to the error state instead of showing both.
    await waitFor(() => expect(screen.queryByText(/"output": 10/)).toBeNull());
    expect(await screen.findByText('Request body is not valid JSON')).toBeTruthy();
  });
});
