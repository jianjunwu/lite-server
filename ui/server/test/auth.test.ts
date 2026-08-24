import { describe, expect, it } from 'vitest';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer, type Server } from 'node:http';
import { AddressInfo } from 'node:net';
import { UserStore } from '../src/auth/users.js';
import { buildApp } from '../src/app.js';
import { InstanceStore } from '../src/config.js';

function tempAuthDir(): { authPath: string; secretPath: string } {
  const dir = mkdtempSync(join(tmpdir(), 'lite-ui-auth-'));
  return { authPath: join(dir, 'auth.yaml'), secretPath: join(dir, 'auth.secret') };
}

async function makeRegistry(): Promise<InstanceStore> {
  const upstream: Server = createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, method: req.method }));
  });
  await new Promise<void>((r) => upstream.listen(0, '127.0.0.1', r));
  const baseUrl = `http://127.0.0.1:${(upstream.address() as AddressInfo).port}`;
  const dir = mkdtempSync(join(tmpdir(), 'lite-ui-inst-'));
  const configPath = join(dir, 'instances.yaml');
  writeFileSync(configPath, `instances:\n  - { id: plain, name: P, base_url: "${baseUrl}" }\n`);
  return new InstanceStore({ configPath, env: {} });
}

async function setup(opts: { env?: NodeJS.ProcessEnv; authEnabled?: boolean } = {}) {
  const { authPath, secretPath } = tempAuthDir();
  const env = opts.env ?? {};
  const store = new UserStore({ authPath, secretPath, env });
  await store.ready;
  const registry = await makeRegistry();
  const app = buildApp(registry, {
    auth: { enabled: opts.authEnabled ?? true, userStore: store },
  });
  return { app, store, authPath };
}

async function login(app: ReturnType<typeof buildApp>, username: string, password: string) {
  const res = await app.inject({
    method: 'POST',
    url: '/api/auth/login',
    payload: { username, password },
  });
  const cookie = res.cookies.find((c) => c.name === 'lite_ui_token');
  return { res, cookie: cookie ? `${cookie.name}=${cookie.value}` : null };
}

describe('bootstrap & login', () => {
  it('should_bootstrap_admin_from_env_password_and_persist', async () => {
    const { store, authPath } = await setup({ env: { LITE_UI_ADMIN_PASSWORD: 'boot-pass-1' } });
    const admin = await store.verify('admin', 'boot-pass-1');
    expect(admin?.role).toBe('admin');
    expect(admin?.mustChangePassword).toBe(true);
    expect(readFileSync(authPath, 'utf8')).toContain('admin');
  });

  it('should_login_and_set_httponly_cookie', async () => {
    const { app } = await setup({ env: { LITE_UI_ADMIN_PASSWORD: 'boot-pass-1' } });
    const { res, cookie } = await login(app, 'admin', 'boot-pass-1');
    expect(res.statusCode).toBe(200);
    expect(cookie).toBeTruthy();
    expect(res.cookies[0].httpOnly).toBe(true);
    expect(res.json().user.username).toBe('admin');
    await app.close();
  });

  it('should_reject_wrong_password_with_401', async () => {
    const { app } = await setup({ env: { LITE_UI_ADMIN_PASSWORD: 'boot-pass-1' } });
    const { res, cookie } = await login(app, 'admin', 'nope');
    expect(res.statusCode).toBe(401);
    expect(cookie).toBeNull();
    await app.close();
  });

  it('should_return_401_for_api_without_cookie', async () => {
    const { app } = await setup({ env: { LITE_UI_ADMIN_PASSWORD: 'boot-pass-1' } });
    const res = await app.inject({ method: 'GET', url: '/api/instances' });
    expect(res.statusCode).toBe(401);
    expect(res.json().error).toBe('unauthenticated');
    await app.close();
  });
});

describe('RBAC', () => {
  async function setupWithUsers() {
    const ctx = await setup({ env: { LITE_UI_ADMIN_PASSWORD: 'boot-pass-1' } });
    await ctx.store.setPassword('admin', 'admin-pass-1'); // clear mustChangePassword
    await ctx.store.create({ username: 'viewer1', password: 'viewer-pass-1', role: 'viewer' });
    await ctx.store.create({ username: 'op1', password: 'op-pass-123', role: 'operator' });
    // create() sets mustChangePassword; clear it so these users can act.
    await ctx.store.setPassword('viewer1', 'viewer-pass-1');
    await ctx.store.setPassword('op1', 'op-pass-123');
    return ctx;
  }

  it('should_allow_viewer_get_but_forbid_proxy_mutation', async () => {
    const { app } = await setupWithUsers();
    const { cookie } = await login(app, 'viewer1', 'viewer-pass-1');
    const get = await app.inject({ method: 'GET', url: '/api/i/plain/v2/models', headers: { cookie: cookie! } });
    expect(get.statusCode).toBe(200);
    const post = await app.inject({
      method: 'POST',
      url: '/api/i/plain/v2/models/m/reload',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
    });
    expect(post.statusCode).toBe(403);
    expect(post.json().error).toBe('forbidden');
    await app.close();
  });

  it('should_allow_viewer_inference_post_but_not_admin_post', async () => {
    const { app } = await setupWithUsers();
    const { cookie } = await login(app, 'viewer1', 'viewer-pass-1');
    const infer = await app.inject({
      method: 'POST',
      url: '/api/i/plain/v2/models/m/infer',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
      payload: { input: 1 },
    });
    expect(infer.statusCode).toBe(200);
    const events = await app.inject({
      method: 'POST',
      url: '/api/i/plain/v2/models/m/events',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
      payload: { input: 1 },
    });
    expect(events.statusCode).toBe(200);
    const reload = await app.inject({
      method: 'POST',
      url: '/api/i/plain/v2/models/m/reload',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
    });
    expect(reload.statusCode).toBe(403);
    await app.close();
  });

  it('should_allow_operator_proxy_mutation_but_forbid_instance_write', async () => {
    const { app } = await setupWithUsers();
    const { cookie } = await login(app, 'op1', 'op-pass-123');
    const post = await app.inject({
      method: 'POST',
      url: '/api/i/plain/v2/models/m/reload',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
    });
    expect(post.statusCode).toBe(200);
    const put = await app.inject({
      method: 'PUT',
      url: '/api/instances/plain',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
      payload: { name: 'x' },
    });
    expect(put.statusCode).toBe(403);
    await app.close();
  });

  it('should_allow_admin_everything', async () => {
    const { app } = await setupWithUsers();
    const { cookie } = await login(app, 'admin', 'admin-pass-1');
    const res = await app.inject({
      method: 'POST',
      url: '/api/users',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
      payload: { username: 'u2', password: 'u2-pass-123', role: 'viewer' },
    });
    expect(res.statusCode).toBe(201);
    await app.close();
  });

  it('should_reject_mutation_without_csrf_header', async () => {
    const { app } = await setupWithUsers();
    const { cookie } = await login(app, 'op1', 'op-pass-123');
    const res = await app.inject({
      method: 'POST',
      url: '/api/i/plain/v2/models/m/reload',
      headers: { cookie: cookie! },
    });
    expect(res.statusCode).toBe(403);
    expect(res.json().error).toBe('csrf_header_missing');
    await app.close();
  });
});

describe('users CRUD & password change', () => {
  it('should_change_password_and_clear_must_change_flag', async () => {
    const { app } = await setup({ env: { LITE_UI_ADMIN_PASSWORD: 'boot-pass-1' } });
    const { cookie } = await login(app, 'admin', 'boot-pass-1');
    const res = await app.inject({
      method: 'POST',
      url: '/api/auth/change-password',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
      payload: { currentPassword: 'boot-pass-1', newPassword: 'new-pass-123' },
    });
    expect(res.statusCode).toBe(200);
    const me = await app.inject({ method: 'GET', url: '/api/auth/me', headers: { cookie: cookie! } });
    expect(me.json().user.mustChangePassword).toBe(false);
    await app.close();
  });

  it('should_block_api_until_password_changed_when_flag_set', async () => {
    const { app } = await setup({ env: { LITE_UI_ADMIN_PASSWORD: 'boot-pass-1' } });
    const { cookie } = await login(app, 'admin', 'boot-pass-1');
    const res = await app.inject({ method: 'GET', url: '/api/instances', headers: { cookie: cookie! } });
    expect(res.statusCode).toBe(403);
    expect(res.json().error).toBe('password_change_required');
    await app.close();
  });

  it('should_forbid_deleting_self_and_last_admin', async () => {
    const { app, store } = await setup({ env: { LITE_UI_ADMIN_PASSWORD: 'boot-pass-1' } });
    await store.setPassword('admin', 'admin-pass-1');
    const { cookie } = await login(app, 'admin', 'admin-pass-1');
    const res = await app.inject({
      method: 'DELETE',
      url: '/api/users/admin',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
    });
    expect(res.statusCode).toBe(403); // cannot delete yourself / last admin
    await app.close();
  });

  it('should_reject_duplicate_username_with_409', async () => {
    const { app, store } = await setup({ env: { LITE_UI_ADMIN_PASSWORD: 'boot-pass-1' } });
    await store.setPassword('admin', 'admin-pass-1');
    const { cookie } = await login(app, 'admin', 'admin-pass-1');
    const res = await app.inject({
      method: 'POST',
      url: '/api/users',
      headers: { cookie: cookie!, 'x-requested-with': 'lite-ui' },
      payload: { username: 'admin', password: 'whatever-123', role: 'viewer' },
    });
    expect(res.statusCode).toBe(409);
    await app.close();
  });
});

describe('auth disabled', () => {
  it('should_pass_everything_through_with_synthetic_admin', async () => {
    const { app } = await setup({ env: {}, authEnabled: false });
    const res = await app.inject({ method: 'GET', url: '/api/instances' });
    expect(res.statusCode).toBe(200);
    await app.close();
  });
});
