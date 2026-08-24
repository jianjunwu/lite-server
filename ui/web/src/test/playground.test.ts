import { afterEach, describe, expect, it, vi } from 'vitest';
import { SseParser, deleteTemplate, listTemplates, saveTemplate, streamEvents } from '../api/playground';

describe('SseParser', () => {
  it('should_parse_complete_frames', () => {
    const p = new SseParser();
    expect(p.push('data: chunk-0\n\ndata: chunk-1\n\n')).toEqual(['chunk-0', 'chunk-1']);
  });

  it('should_buffer_partial_frames_across_pushes', () => {
    const p = new SseParser();
    expect(p.push('data: chu')).toEqual([]);
    expect(p.push('nk-0\n\nda')).toEqual(['chunk-0']);
    expect(p.push('ta: x\n\n')).toEqual(['x']);
  });

  it('should_strip_single_leading_space_after_data_colon', () => {
    const p = new SseParser();
    expect(p.push('data: {"a":1}\n\n')).toEqual(['{"a":1}']);
  });

  it('should_join_multiline_data_fields', () => {
    const p = new SseParser();
    expect(p.push('data: line1\ndata: line2\n\n')).toEqual(['line1\nline2']);
  });

  it('should_ignore_non_data_lines', () => {
    const p = new SseParser();
    expect(p.push('event: message\ndata: payload\n\n')).toEqual(['payload']);
  });

  it('should_emit_done_marker_as_regular_payload', () => {
    const p = new SseParser();
    expect(p.push('data: [DONE]\n\n')).toEqual(['[DONE]']);
  });

  it('should_parse_crlf_framed_events', () => {
    // The SSE spec allows \r\n line endings; an upstream behind certain
    // proxies may frame with \r\n\r\n.
    const p = new SseParser();
    expect(p.push('data: a\r\n\r\ndata: b\r\n\r\n')).toEqual(['a', 'b']);
  });
});

describe('streamEvents', () => {
  afterEach(() => vi.unstubAllGlobals());

  // Mirrors real fetch: aborting the request signal errors the body stream.
  const abortableStream = (_input: RequestInfo | URL, init?: RequestInit) =>
    Promise.resolve(
      new Response(
        new ReadableStream({
          start(c) {
            init?.signal?.addEventListener('abort', () =>
              c.error(new DOMException('Aborted', 'AbortError')),
            );
          },
        }),
        { status: 200 },
      ),
    );

  it('should_call_onDone_when_aborted_mid_stream', async () => {
    vi.stubGlobal('fetch', vi.fn().mockImplementation(abortableStream));
    const cb = { onEvent: vi.fn(), onDone: vi.fn(), onError: vi.fn() };
    const abort = streamEvents('prod', 'm', null, '{}', cb);
    await new Promise((r) => setTimeout(r, 10));
    abort();
    // The caller's send() promise settles via onDone — aborting must not
    // leave it hanging.
    await vi.waitFor(() => expect(cb.onDone).toHaveBeenCalledTimes(1));
    expect(cb.onError).not.toHaveBeenCalled();
  });

  it('should_cancel_the_reader_when_done_marker_arrives', async () => {
    const cancel = vi.fn();
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new TextEncoder().encode('data: [DONE]\n\n'));
      },
      cancel,
    });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(stream, { status: 200 })));
    const cb = { onEvent: vi.fn(), onDone: vi.fn(), onError: vi.fn() };
    streamEvents('prod', 'm', null, '{}', cb);
    await vi.waitFor(() => expect(cb.onDone).toHaveBeenCalled());
    // Without cancel the connection lingers until the upstream closes it.
    await vi.waitFor(() => expect(cancel).toHaveBeenCalled());
  });
});

describe('request templates', () => {
  afterEach(() => localStorage.clear());

  it('should_save_and_list_templates_per_model', () => {
    saveTemplate('echo', 'basic', '{"input": 1}');
    saveTemplate('echo', 'big', '{"input": 99}');
    saveTemplate('other', 'x', '{}');
    expect(listTemplates('echo').map((t) => t.name)).toEqual(['basic', 'big']);
    expect(listTemplates('other')).toHaveLength(1);
  });

  it('should_overwrite_template_with_same_name', () => {
    saveTemplate('echo', 'basic', '{"input": 1}');
    saveTemplate('echo', 'basic', '{"input": 2}');
    const list = listTemplates('echo');
    expect(list).toHaveLength(1);
    expect(list[0].body).toBe('{"input": 2}');
  });

  it('should_delete_template', () => {
    saveTemplate('echo', 'basic', '{}');
    deleteTemplate('echo', 'basic');
    expect(listTemplates('echo')).toHaveLength(0);
  });

  it('should_return_empty_for_corrupt_storage', () => {
    localStorage.setItem('lite-ui-tpl:echo', 'not-json');
    expect(listTemplates('echo')).toEqual([]);
  });
});
