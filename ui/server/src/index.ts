import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { InstanceStore } from './config.js';
import { buildApp } from './app.js';

const here = dirname(fileURLToPath(import.meta.url));

const port = Number(process.env.LITE_UI_PORT ?? 8600);
const host = process.env.LITE_UI_HOST ?? '0.0.0.0';
const configPath = process.env.LITE_UI_INSTANCES_FILE ?? join(here, '..', 'instances.yaml');
const webDist = process.env.LITE_UI_WEB_DIST ?? join(here, '..', '..', 'web', 'dist');

const registry = new InstanceStore({ configPath, env: process.env });
const app = buildApp(registry, { webDist, logger: true });

app
  .listen({ port, host })
  .then(() => {
    app.log.info(`lite-ui listening on http://${host}:${port}, ${registry.list().length} instance(s) configured`);
  })
  .catch((err) => {
    app.log.error(err);
    process.exit(1);
  });
