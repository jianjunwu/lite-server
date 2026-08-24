import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import fastifyCookie from '@fastify/cookie';
import fastifyJwt from '@fastify/jwt';
import { AuthError, publicUser, roleRank, type Role, type UserStore } from './users.js';

declare module '@fastify/jwt' {
  interface FastifyJWT {
    payload: { username: string; role: Role };
    user: { username: string; role: Role };
  }
}

const COOKIE_NAME = 'lite_ui_token';
const CSRF_HEADER = 'x-requested-with';
const CSRF_VALUE = 'lite-ui';

export interface AuthOptions {
  enabled: boolean;
  userStore: UserStore;
}

function authStatusFor(err: unknown): number {
  if (err instanceof AuthError) {
    switch (err.code) {
      case 'invalid':
        return 400;
      case 'duplicate':
        return 409;
      case 'not_found':
        return 404;
      case 'forbidden':
        return 403;
    }
  }
  return 500;
}

/**
 * Local-account auth: JWT in an httpOnly cookie, three-role RBAC enforced at
 * the route layer (never trusted to the frontend). Must be registered on the
 * root instance BEFORE any routes so the guard hook covers everything.
 */
export async function registerAuth(app: FastifyInstance, opts: AuthOptions) {
  const { userStore: store, enabled } = opts;

  await app.register(fastifyCookie);
  await app.register(fastifyJwt, {
    secret: store.secret,
    cookie: { cookieName: COOKIE_NAME, signed: false },
  });

  // ---- guard --------------------------------------------------------------
  app.addHook('onRequest', async (req: FastifyRequest, reply: FastifyReply) => {
    if (!enabled) {
      req.user = { username: 'local', role: 'admin' };
      return;
    }
    const url = req.raw.url ?? '';
    if (!url.startsWith('/api/')) return; // static assets are public
    if (url === '/api/auth/login' && req.method === 'POST') return;

    try {
      await req.jwtVerify();
    } catch {
      return reply.code(401).send({ error: 'unauthenticated' });
    }

    const current = store.get(req.user.username);
    const mustChange = current?.mustChangePassword === true;
    const isPasswordFlow =
      url === '/api/auth/me' || url === '/api/auth/change-password' || url === '/api/auth/logout';
    if (mustChange && !isPasswordFlow) {
      return reply.code(403).send({ error: 'password_change_required' });
    }

    if (req.method !== 'GET') {
      // CSRF: cookie auth + custom header a cross-site form cannot send.
      if (req.headers[CSRF_HEADER] !== CSRF_VALUE) {
        return reply.code(403).send({ error: 'csrf_header_missing' });
      }
      if (url.startsWith('/api/auth/')) return; // any authenticated user
      if (url.startsWith('/api/i/')) {
        if (roleRank(req.user.role) >= roleRank('operator')) return;
        return reply.code(403).send({ error: 'forbidden', required: 'operator' });
      }
      // Everything else (/api/instances, /api/users, ...) is admin territory.
      if (roleRank(req.user.role) >= roleRank('admin')) return;
      return reply.code(403).send({ error: 'forbidden', required: 'admin' });
    }

    // GETs: any authenticated user — except the user directory itself.
    if (url.startsWith('/api/users') && roleRank(req.user.role) < roleRank('admin')) {
      return reply.code(403).send({ error: 'forbidden', required: 'admin' });
    }
  });

  // ---- audit --------------------------------------------------------------
  app.addHook('onResponse', async (req, reply) => {
    const url = req.raw.url ?? '';
    if (req.method !== 'GET' && url.startsWith('/api/')) {
      req.log.info(
        { user: req.user?.username, method: req.method, url, status: reply.statusCode },
        'ui mutation',
      );
    }
  });

  // ---- auth routes --------------------------------------------------------
  app.post('/api/auth/login', async (req, reply) => {
    const { username, password } = (req.body ?? {}) as { username?: string; password?: string };
    const user = typeof username === 'string' ? await store.verify(username, password ?? '') : null;
    if (!user) {
      return reply.code(401).send({ error: 'invalid_credentials' });
    }
    const token = app.jwt.sign(
      { username: user.username, role: user.role },
      { expiresIn: '12h' },
    );
    reply.setCookie(COOKIE_NAME, token, {
      path: '/',
      httpOnly: true,
      sameSite: 'lax',
      secure: 'auto',
    });
    return { user: publicUser(user) };
  });

  app.post('/api/auth/logout', async (_req, reply) => {
    reply.clearCookie(COOKIE_NAME, { path: '/' });
    return { ok: true };
  });

  app.get('/api/auth/me', async (req) => {
    const current = store.get(req.user.username);
    return { user: current ? publicUser(current) : { ...req.user, mustChangePassword: false } };
  });

  app.post('/api/auth/change-password', async (req, reply) => {
    const { currentPassword, newPassword } = (req.body ?? {}) as {
      currentPassword?: string;
      newPassword?: string;
    };
    const ok = await store.verify(req.user.username, currentPassword ?? '');
    if (!ok) return reply.code(401).send({ error: 'invalid_credentials' });
    try {
      await store.setPassword(req.user.username, newPassword ?? '');
    } catch (err) {
      return reply.code(authStatusFor(err)).send({ error: err instanceof Error ? err.message : 'error' });
    }
    const user = store.get(req.user.username)!;
    const token = app.jwt.sign({ username: user.username, role: user.role }, { expiresIn: '12h' });
    reply.setCookie(COOKIE_NAME, token, { path: '/', httpOnly: true, sameSite: 'lax', secure: 'auto' });
    return { user: publicUser(user) };
  });

  // ---- user management (admin; enforced by the guard) ---------------------
  app.get('/api/users', async () => ({ users: store.list() }));

  app.post('/api/users', async (req, reply) => {
    try {
      const user = await store.create(req.body as { username: unknown; password: unknown; role: unknown });
      return reply.code(201).send({ user });
    } catch (err) {
      return reply.code(authStatusFor(err)).send({ error: err instanceof Error ? err.message : 'error' });
    }
  });

  app.put('/api/users/:name', async (req, reply) => {
    try {
      const user = await store.update(
        (req.params as { name: string }).name,
        req.body as { role?: unknown; password?: unknown },
        req.user.username,
      );
      return { user };
    } catch (err) {
      return reply.code(authStatusFor(err)).send({ error: err instanceof Error ? err.message : 'error' });
    }
  });

  app.delete('/api/users/:name', async (req, reply) => {
    try {
      store.remove((req.params as { name: string }).name, req.user.username);
      return { ok: true };
    } catch (err) {
      return reply.code(authStatusFor(err)).send({ error: err instanceof Error ? err.message : 'error' });
    }
  });
}
