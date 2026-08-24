import Fastify, { type FastifyInstance } from 'fastify';
import fastifyStatic from '@fastify/static';
import { existsSync } from 'node:fs';
import type { InstanceRegistry } from './config.js';
import { registerProxyRoutes } from './proxy.js';
import { registerInstanceRoutes } from './routes-instances.js';

export interface AppOptions {
  /** Directory of the built SPA; served at / when it exists. */
  webDist?: string;
  logger?: boolean;
  /** Max proxied request body in bytes (uploads). Default 1 GiB. */
  bodyLimit?: number;
}

export function buildApp(registry: InstanceRegistry, opts: AppOptions = {}): FastifyInstance {
  const app = Fastify({
    logger: opts.logger ?? false,
    bodyLimit: opts.bodyLimit ?? 1 << 30,
  });

  // BFF's own APIs (default JSON parsing).
  registerInstanceRoutes(app, registry);

  // Instance proxy lives in its own encapsulation scope with a verbatim
  // buffering parser (see proxy.ts).
  app.register(async (scope) => {
    registerProxyRoutes(scope, registry);
  });

  if (opts.webDist && existsSync(opts.webDist)) {
    app.register(fastifyStatic, { root: opts.webDist, wildcard: true });
    // SPA fallback: unknown non-API GET paths serve index.html.
    app.setNotFoundHandler((req, reply) => {
      if (req.method === 'GET' && !req.url.startsWith('/api/')) {
        return reply.sendFile('index.html');
      }
      return reply.code(404).send({ error: 'not_found' });
    });
  }

  return app;
}
