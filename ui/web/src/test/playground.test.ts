import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  SseParser, deleteTemplate, listTemplates, loadHeaders, mergeHeaders, saveHeaders, saveTemplate, streamEvents,
} from '../api/playground';
import { setAdminKey } from '../api/client';

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

describe('mergeHeaders', () => {
  afterEach(() => sessionStorage.clear());

  it('should_keep_defaults_when_no_extra_rows', () => {
    expect(mergeHeaders('prod', [])).toEqual({
      'content-type': 'application/json',
      'x-requested-with': 'lite-ui',
    });
  });

  it('should_add_user_rows_case_insensitively', () => {
    const merged = mergeHeaders('prod', [{ name: 'X-Trace-Id', value: 't-1' }]);
    expect(merged['x-trace-id']).toBe('t-1');
  });

  it('should_let_user_rows_override_content_type_and_admin_key', () => {
    setAdminKey('prod', 'stored-key');
    const merged = mergeHeaders('prod', [
      { name: 'content-type', value: 'text/plain' },
      { name: 'X-Admin-Key', value: 'explicit-key' },
    ]);
    expect(merged['content-type']).toBe('text/plain');
    expect(merged['x-admin-key']).toBe('explicit-key');
  });

  it('should_never_let_user_rows_override_csrf_header', () => {
    const merged = mergeHeaders('prod', [{ name: 'X-Requested-With', value: 'forged' }]);
    expect(merged['x-requested-with']).toBe('lite-ui');
  });

  it('should_skip_blank_names_and_empty_rows', () => {
    const merged = mergeHeaders('prod', [
      { name: '  ', value: 'x' },
      { name: '', value: 'y' },
    ]);
    expect(Object.keys(merged)).toEqual(['content-type', 'x-requested-with']);
  });
});

describe('playground header storage', () => {
  afterEach(() => localStorage.clear());

  it('should_roundtrip_headers_per_instance_and_model', () => {
    saveHeaders('prod', 'echo', [{ name: 'x-a', value: '1' }]);
    saveHeaders('prod', 'other', [{ name: 'x-b', value: '2' }]);
    saveHeaders('dev', 'echo', [{ name: 'x-c', value: '3' }]);
    expect(loadHeaders('prod', 'echo')).toEqual([{ name: 'x-a', value: '1' }]);
    expect(loadHeaders('prod', 'other')).toEqual([{ name: 'x-b', value: '2' }]);
    expect(loadHeaders('dev', 'echo')).toEqual([{ name: 'x-c', value: '3' }]);
  });

  it('should_return_empty_for_corrupt_storage', () => {
    localStorage.setItem('lite-ui-headers:prod:echo', 'not-json');
    expect(loadHeaders('prod', 'echo')).toEqual([]);
  });
});

describe('inferUnary response headers', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('should_return_response_headers_with_result', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response('{"output": 10}', {
            status: 200,
            headers: { 'content-type': 'application/json', 'x-request-id': 'r-1', 'x-backend-hdr': 'b-1' },
          }),
        ),
      ),
    );
    const { inferUnary } = await import('../api/playground');
    const res = await inferUnary('prod', 'm', null, '{}');
    expect(res.headers['x-backend-hdr']).toBe('b-1');
    expect(res.headers['content-type']).toBe('application/json');
  });
});
