import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import { AddressInfo } from 'node:net';
import { buildApp } from '../src/app.js';
import type { InstanceRegistry } from '../src/config.js';

let upstream: Server;
let upstreamUrl: string;
let lastRequest: { headers: IncomingMessage['headers']; body: string; url: string };

function handler(req: IncomingMessage, res: ServerResponse) {
  const chunks: Buffer[] = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => {
    lastRequest = { headers: req.headers, body: Buffer.concat(chunks).toString(), url: req.url ?? '' };
    if (req.url === '/sse') {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write('data: one\n\n');
      res.write('data: two\n\n');
      res.end();
      return;
    }
    if (req.url === '/fail500') {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'boom' }));
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json', 'x-request-id': 'req-123' });
    res.end(JSON.stringify({ ok: true, url: req.url }));
  });
}

function makeRegistry(): InstanceRegistry {
  const map = new Map([
    ['plain', { id: 'plain', name: 'Plain', baseUrl: upstreamUrl, readonly: false }],
    ['keyed', { id: 'keyed', name: 'Keyed', baseUrl: upstreamUrl, adminKey: 'server-key', readonly: false }],
    ['dead', { id: 'dead', name: 'Dead', baseUrl: 'http://127.0.0.1:1', readonly: false }],
  ]);
  return { list: () => [...map.values()], get: (id) => map.get(id) };
}

beforeAll(async () => {
  upstream = createServer(handler);
  await new Promise<void>((resolve) => upstream.listen(0, '127.0.0.1', resolve));
  upstreamUrl = `http://127.0.0.1:${(upstream.address() as AddressInfo).port}`;
});

afterAll(async () => {
  await new Promise((resolve) => upstream.close(resolve));
});

describe('GET /api/instances', () => {
  it('should_list_instances_without_leaking_admin_key', async () => {
    const app = buildApp(makeRegistry());
    const res = await app.inject({ method: 'GET', url: '/api/instances' });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.instances).toHaveLength(3);
    const keyed = body.instances.find((i: { id: string }) => i.id === 'keyed');
    expect(keyed.has_admin_key).toBe(true);
    expect(JSON.stringify(body)).not.toContain('server-key');
    await app.close();
  });
});

describe('proxy /api/i/:id/*', () => {
  it('should_forward_get_with_query_and_return_upstream_response', async () => {
    const app = buildApp(makeRegistry());
    const res = await app.inject({ method: 'GET', url: '/api/i/plain/v2/models?x=1' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ ok: true, url: '/v2/models?x=1' });
    expect(res.headers['x-request-id']).toBe('req-123');
    await app.close();
  });

  it('should_return_404_for_unknown_instance', async () => {
    const app = buildApp(makeRegistry());
    const res = await app.inject({ method: 'GET', url: '/api/i/nope/v2/models' });
    expect(res.statusCode).toBe(404);
    expect(res.json().error).toBe('unknown_instance');
    await app.close();
  });

  it('should_inject_instance_admin_key_when_browser_sends_none', async () => {
    const app = buildApp(makeRegistry());
    await app.inject({ method: 'GET', url: '/api/i/keyed/v2/models' });
    expect(lastRequest.headers['x-admin-key']).toBe('server-key');
    await app.close();
  });

  it('should_prefer_browser_admin_key_over_instance_key', async () => {
    const app = buildApp(makeRegistry());
    await app.inject({
      method: 'GET',
      url: '/api/i/keyed/v2/models',
      headers: { 'x-admin-key': 'browser-key' },
    });
    expect(lastRequest.headers['x-admin-key']).toBe('browser-key');
    await app.close();
  });

  it('should_not_send_admin_key_header_for_keyless_instance', async () => {
    const app = buildApp(makeRegistry());
    await app.inject({ method: 'GET', url: '/api/i/plain/v2/models' });
    expect(lastRequest.headers['x-admin-key']).toBeUndefined();
    await app.close();
  });

  it('should_forward_post_body', async () => {
    const app = buildApp(makeRegistry());
    const res = await app.inject({
      method: 'POST',
      url: '/api/i/plain/v2/models/m/infer',
      headers: { 'content-type': 'application/json' },
      payload: JSON.stringify({ input: 21 }),
    });
    expect(res.statusCode).toBe(200);
    expect(lastRequest.body).toBe(JSON.stringify({ input: 21 }));
    await app.close();
  });

  it('should_stream_sse_response_end_to_end', async () => {
    const app = buildApp(makeRegistry());
    const res = await app.inject({ method: 'GET', url: '/api/i/plain/sse' });
    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('text/event-stream');
    expect(res.body).toBe('data: one\n\ndata: two\n\n');
    await app.close();
  });

  it('should_pass_through_upstream_error_status_and_body', async () => {
    const app = buildApp(makeRegistry());
    const res = await app.inject({ method: 'GET', url: '/api/i/plain/fail500' });
    expect(res.statusCode).toBe(500);
    expect(res.json()).toEqual({ error: 'boom' });
    await app.close();
  });

  it('should_return_502_when_instance_unreachable', async () => {
    const app = buildApp(makeRegistry());
    const res = await app.inject({ method: 'GET', url: '/api/i/dead/v2/models' });
    expect(res.statusCode).toBe(502);
    const body = res.json();
    expect(body.error).toBe('instance_unreachable');
    expect(body.instance).toBe('dead');
    await app.close();
  });
});
