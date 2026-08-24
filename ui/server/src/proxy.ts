import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { request as undiciRequest } from 'undici';
import type { InstanceRegistry, InstanceConfig } from './config.js';

// Hop-by-hop headers that must not cross the proxy in either direction.
const HOP_BY_HOP = new Set([
  'connection',
  'keep-alive',
  'transfer-encoding',
  'te',
  'trailer',
  'upgrade',
  'host',
  'content-length',
]);

function buildUpstreamHeaders(req: FastifyRequest, inst: InstanceConfig): Record<string, string> {
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(req.headers)) {
    if (HOP_BY_HOP.has(name) || value === undefined) continue;
    headers[name] = Array.isArray(value) ? value.join(', ') : value;
  }
  // Admin key priority: explicit browser header > instance-level key.
  if (typeof req.headers['x-admin-key'] === 'string') {
    headers['x-admin-key'] = req.headers['x-admin-key'];
  } else if (inst.adminKey) {
    headers['x-admin-key'] = inst.adminKey;
  } else {
    delete headers['x-admin-key'];
  }
  return headers;
}

async function proxyHandler(req: FastifyRequest, reply: FastifyReply, registry: InstanceRegistry) {
  const { id } = req.params as { id: string };
  const inst = registry.get(id);
  if (!inst) {
    return reply.code(404).send({ error: 'unknown_instance', instance: id });
  }

  const tail = (req.params as { '*': string })['*'] ?? '';
  const queryIndex = req.raw.url?.indexOf('?') ?? -1;
  const query = queryIndex >= 0 ? req.raw.url!.slice(queryIndex) : '';
  const url = `${inst.baseUrl}/${tail}${query}`;

  const method = req.method.toUpperCase();
  const hasBody = method !== 'GET' && method !== 'HEAD';

  let upstream;
  try {
    upstream = await undiciRequest(url, {
      method: method as 'GET',
      headers: buildUpstreamHeaders(req, inst),
      // req.body is the verbatim Buffer (buffering parser in app.ts).
      body: hasBody ? (req.body as Buffer) : undefined,
    });
  } catch (err) {
    req.log.warn({ err, instance: id, url }, 'upstream request failed');
    return reply.code(502).send({ error: 'instance_unreachable', instance: id });
  }

  reply.code(upstream.statusCode);
  for (const [name, value] of Object.entries(upstream.headers)) {
    if (HOP_BY_HOP.has(name) || value === undefined) continue;
    reply.header(name, value as string | string[]);
  }
  // Send the body as a stream: SSE / chunked responses pass through unbuffered.
  return reply.send(upstream.body);
}

export function registerProxyRoutes(app: FastifyInstance, registry: InstanceRegistry) {
  app.get('/api/instances', async () => ({
    instances: registry.list().map((i) => ({
      id: i.id,
      name: i.name,
      base_url: i.baseUrl,
      has_admin_key: Boolean(i.adminKey),
      readonly: i.readonly,
    })),
  }));

  app.all('/api/i/:id/*', (req, reply) => proxyHandler(req, reply, registry));
  // Trailing-slash-less form: /api/i/:id proxies to the instance root.
  app.all('/api/i/:id', (req, reply) => proxyHandler(req, reply, registry));
}
