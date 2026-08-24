import Fastify, { type FastifyInstance } from 'fastify';
import fastifyStatic from '@fastify/static';
import { existsSync } from 'node:fs';
import type { InstanceRegistry } from './config.js';
import { registerProxyRoutes } from './proxy.js';

export interface AppOptions {
  /** Directory of the built SPA; served at / when it exists. */
  webDist?: string;
  logger?: boolean;
}

export function buildApp(registry: InstanceRegistry, opts: AppOptions = {}): FastifyInstance {
  const app = Fastify({ logger: opts.logger ?? false });

  // Buffer request bodies verbatim so the proxy can forward bytes as-is
  // (no JSON parse/re-serialize round-trip). Control-plane bodies are small;
  // streaming uploads (M2) will bypass this with a dedicated raw route.
  // 'application/json' must be overridden explicitly — the specific parser
  // wins over '*' in fastify's matching.
  const bufferParser = (
    _req: unknown,
    payload: NodeJS.ReadableStream,
    done: (err: Error | null, body?: Buffer) => void,
  ) => {
    const chunks: Buffer[] = [];
    payload.on('data', (c: Buffer) => chunks.push(c));
    payload.on('end', () => done(null, Buffer.concat(chunks)));
    payload.on('error', done);
  };
  app.addContentTypeParser('application/json', bufferParser);
  app.addContentTypeParser('*', bufferParser);

  registerProxyRoutes(app, registry);

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
