import { dirname, join } from 'node:path';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { InstanceStore } from './config.js';
import { UserStore } from './auth/users.js';
import { buildApp } from './app.js';

const here = dirname(fileURLToPath(import.meta.url));

const port = Number(process.env.LITE_UI_PORT ?? 8600);
const host = process.env.LITE_UI_HOST ?? '0.0.0.0';
const configPath = process.env.LITE_UI_INSTANCES_FILE ?? join(here, '..', 'instances.yaml');
const authPath = process.env.LITE_UI_AUTH_FILE ?? join(here, '..', 'auth.yaml');
const authEnabled = (process.env.LITE_UI_AUTH ?? 'true') !== 'false';

// SPA location differs between the source tree (ui/web/dist) and the release
// tarball (sibling web-dist/); pick the first that exists.
const webDist =
  process.env.LITE_UI_WEB_DIST ??
  [join(here, '..', 'web-dist'), join(here, '..', '..', 'web', 'dist')].find((p) => existsSync(p));

const registry = new InstanceStore({ configPath, env: process.env });
const userStore = new UserStore({ authPath, secretPath: `${authPath}.secret`, env: process.env });
await userStore.ready;

const app = buildApp(registry, {
  webDist,
  logger: true,
  auth: { enabled: authEnabled, userStore },
});

app
  .listen({ port, host })
  .then(() => {
    app.log.info(
      `lite-ui listening on http://${host}:${port}, ${registry.list().length} instance(s), auth ${authEnabled ? 'on' : 'OFF'}`,
    );
  })
  .catch((err) => {
    app.log.error(err);
    process.exit(1);
  });
