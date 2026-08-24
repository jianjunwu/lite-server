import type { FastifyInstance } from 'fastify';
import { request as undiciRequest } from 'undici';
import { StoreError, type InstanceConfig, type InstanceRegistry } from './config.js';

interface InstanceStoreLike extends InstanceRegistry {
  create(input: unknown): InstanceConfig;
  update(id: string, patch: unknown): InstanceConfig;
  remove(id: string): void;
}

function isStore(registry: InstanceRegistry): registry is InstanceStoreLike {
  return (
    typeof (registry as InstanceStoreLike).create === 'function' &&
    typeof (registry as InstanceStoreLike).update === 'function' &&
    typeof (registry as InstanceStoreLike).remove === 'function'
  );
}

function publicView(instances: InstanceConfig[]) {
  return {
    instances: instances.map((i) => ({
      id: i.id,
      name: i.name,
      base_url: i.baseUrl,
      has_admin_key: Boolean(i.adminKey),
      readonly: i.readonly,
    })),
  };
}

function statusFor(err: unknown): number {
  if (err instanceof StoreError) {
    switch (err.code) {
      case 'invalid':
        return 400;
      case 'duplicate':
        return 409;
      case 'not_found':
        return 404;
      case 'readonly':
        return 403;
    }
  }
  return 500;
}

/** Reachability probe: GET {base_url}/info with a short timeout. */
async function probe(baseUrl: string): Promise<boolean> {
  try {
    const res = await undiciRequest(`${baseUrl}/info`, {
      method: 'GET',
      signal: AbortSignal.timeout(2000),
    });
    await res.body.dump();
    return res.statusCode >= 200 && res.statusCode < 300;
  } catch {
    return false;
  }
}

export function registerInstanceRoutes(app: FastifyInstance, registry: InstanceRegistry) {
  app.get('/api/instances', async () => publicView(registry.list()));

  if (!isStore(registry)) return;

  const store = registry;

  app.post('/api/instances', async (req, reply) => {
    const probeWanted = (req.query as { probe?: string }).probe === 'true';
    const body = req.body as { base_url?: string };
    if (probeWanted && typeof body?.base_url === 'string') {
      const ok = await probe(body.base_url.replace(/\/$/, ''));
      if (!ok) {
        return reply.code(422).send({ error: 'instance_unreachable', base_url: body.base_url });
      }
    }
    try {
      store.create(req.body);
      return reply.code(201).send(publicView(store.list()));
    } catch (err) {
      return reply.code(statusFor(err)).send({ error: err instanceof Error ? err.message : 'error' });
    }
  });

  app.put('/api/instances/:id', async (req, reply) => {
    try {
      store.update((req.params as { id: string }).id, req.body);
      return reply.send(publicView(store.list()));
    } catch (err) {
      return reply.code(statusFor(err)).send({ error: err instanceof Error ? err.message : 'error' });
    }
  });

  app.delete('/api/instances/:id', async (req, reply) => {
    try {
      store.remove((req.params as { id: string }).id);
      return reply.send(publicView(store.list()));
    } catch (err) {
      return reply.code(statusFor(err)).send({ error: err instanceof Error ? err.message : 'error' });
    }
  });
}
