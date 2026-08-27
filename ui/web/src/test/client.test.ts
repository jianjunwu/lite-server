import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, apiFetch, bffFetch, getAdminKey, setAdminKey } from '../api/client';

afterEach(() => {
  vi.unstubAllGlobals();
  sessionStorage.clear();
});

function mockFetch(res: Partial<Response>) {
  const fn = vi.fn().mockResolvedValue({
    ok: true,
    status: 200,
    headers: new Headers(),
    json: () => Promise.resolve({}),
    text: () => Promise.resolve(''),
    ...res,
  });
  vi.stubGlobal('fetch', fn);
  return fn;
}

describe('apiFetch', () => {
  it('should_prefix_path_with_instance_proxy_route', async () => {
    const fn = mockFetch({ json: () => Promise.resolve({ ok: true }) });
    await apiFetch('prod', '/v2/models');
    expect(fn).toHaveBeenCalledWith('/api/i/prod/v2/models', expect.anything());
  });

  it('should_attach_session_admin_key_header', async () => {
    setAdminKey('prod', 'k-1');
    const fn = mockFetch({});
    await apiFetch('prod', '/v2/models');
    const headers = fn.mock.calls[0][1].headers as Headers;
    expect(headers.get('x-admin-key')).toBe('k-1');
  });

  it('should_throw_ApiError_with_request_id_on_failure', async () => {
    mockFetch({
      ok: false,
      status: 401,
      headers: new Headers({ 'x-request-id': 'req-9' }),
      text: () => Promise.resolve(JSON.stringify({ error: 'unauthorized' })),
    });
    const err = (await apiFetch('prod', '/x').catch((e) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(401);
    expect(err.requestId).toBe('req-9');
    expect(err.message).toBe('unauthorized');
  });

  it('should_extract_message_from_object_error_envelope', async () => {
    mockFetch({
      ok: false,
      status: 404,
      text: () =>
        Promise.resolve(
          JSON.stringify({
            error: { code: 'route_not_found', message: 'route not found', param: null, type: 'not_found_error' },
          }),
        ),
    });
    const err = (await apiFetch('prod', '/x').catch((e) => e)) as ApiError;
    expect(err.message).toBe('route not found');
  });

  it('should_fall_back_to_http_status_when_error_has_no_message', async () => {
    mockFetch({
      ok: false,
      status: 500,
      text: () => Promise.resolve(JSON.stringify({ error: { code: 'internal' } })),
    });
    const err = (await apiFetch('prod', '/x').catch((e) => e)) as ApiError;
    expect(err.message).toBe('HTTP 500');
  });

  it('should_wrap_network_failure_as_status_0', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('conn refused')));
    const err = (await apiFetch('prod', '/x').catch((e) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(0);
  });
});

describe('admin key storage', () => {
  it('should_isolate_keys_per_instance', () => {
    setAdminKey('a', 'key-a');
    setAdminKey('b', 'key-b');
    expect(getAdminKey('a')).toBe('key-a');
    expect(getAdminKey('b')).toBe('key-b');
  });
  it('should_remove_key_when_set_to_null', () => {
    setAdminKey('a', 'key-a');
    setAdminKey('a', null);
    expect(getAdminKey('a')).toBeNull();
  });
});

describe('apiFetch error messages', () => {
  it('should_explain_model_denied_with_the_model_name', async () => {
    mockFetch({
      ok: false,
      status: 403,
      text: () =>
        Promise.resolve(JSON.stringify({ error: 'forbidden', reason: 'model_denied', model: 'beta' })),
    });
    const err = (await apiFetch('prod', '/v2/models/beta/reload', { method: 'POST' }).catch((e) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.message).toBe('No access to model "beta"');
  });

  it('should_fall_back_to_the_plain_error_string_for_other_forbidden_bodies', async () => {
    mockFetch({
      ok: false,
      status: 403,
      text: () =>
        Promise.resolve(JSON.stringify({ error: 'forbidden', reason: 'instance_denied', instance: 'prod' })),
    });
    const err = (await apiFetch('prod', '/v2/models', { method: 'GET' }).catch((e) => e)) as ApiError;
    expect(err.message).toBe('forbidden');
  });
});

describe('bffFetch', () => {
  it('should_surface_the_server_error_reason_instead_of_a_bare_http_status', async () => {
    mockFetch({
      ok: false,
      status: 422,
      text: () =>
        Promise.resolve(JSON.stringify({ error: 'instance_unreachable', base_url: 'http://localhost:18001' })),
    });
    const err = (await bffFetch('/api/instances?probe=true', { method: 'POST' }).catch((e) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(422);
    expect(err.message).toBe('instance_unreachable');
  });

  it('should_fall_back_to_http_status_when_the_body_has_no_error', async () => {
    mockFetch({
      ok: false,
      status: 500,
      text: () => Promise.resolve('boom'),
    });
    const err = (await bffFetch('/api/x').catch((e) => e)) as ApiError;
    expect(err.message).toBe('HTTP 500');
  });
});
