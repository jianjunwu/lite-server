import { readFileSync, writeFileSync, renameSync, existsSync, chmodSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import { parse, stringify } from 'yaml';
import bcrypt from 'bcryptjs';

export type Role = 'viewer' | 'operator' | 'admin';
export const ROLES: Role[] = ['viewer', 'operator', 'admin'];

export function roleRank(role: Role): number {
  return ROLES.indexOf(role);
}

export interface UserRecord {
  username: string;
  passwordHash: string;
  role: Role;
  createdAt: string;
  mustChangePassword: boolean;
}

export interface PublicUser {
  username: string;
  role: Role;
  createdAt: string;
  mustChangePassword: boolean;
}

export function publicUser(u: UserRecord): PublicUser {
  return {
    username: u.username,
    role: u.role,
    createdAt: u.createdAt,
    mustChangePassword: u.mustChangePassword,
  };
}

export class AuthError extends Error {
  constructor(
    public code: 'invalid' | 'duplicate' | 'not_found' | 'forbidden',
    message: string,
  ) {
    super(message);
    this.name = 'AuthError';
  }
}

const USERNAME_PATTERN = /^[a-zA-Z0-9_.-]{2,32}$/;

interface RawUser {
  username?: unknown;
  password_hash?: unknown;
  role?: unknown;
  created_at?: unknown;
  must_change_password?: unknown;
}

export interface UserStoreOptions {
  authPath: string;
  secretPath: string;
  env: NodeJS.ProcessEnv;
}

/**
 * Local account store: users in auth.yaml (bcrypt hashes), JWT secret in a
 * sibling 0600 file. Bootstrap: first run with no users creates `admin`
 * from LITE_UI_ADMIN_PASSWORD or a random password printed once.
 */
export class UserStore {
  private users = new Map<string, UserRecord>();
  private jwtSecret = '';
  readonly ready: Promise<void>;

  constructor(private opts: UserStoreOptions) {
    this.ready = this.init();
  }

  get secret(): string {
    return this.jwtSecret;
  }

  private async init() {
    if (existsSync(this.opts.authPath)) {
      const doc = parse(readFileSync(this.opts.authPath, 'utf8')) as { users?: RawUser[] } | null;
      for (const raw of doc?.users ?? []) {
        if (typeof raw.username !== 'string' || typeof raw.password_hash !== 'string') continue;
        this.users.set(raw.username, {
          username: raw.username,
          passwordHash: raw.password_hash,
          role: ROLES.includes(raw.role as Role) ? (raw.role as Role) : 'viewer',
          createdAt: typeof raw.created_at === 'string' ? raw.created_at : new Date().toISOString(),
          mustChangePassword: raw.must_change_password === true,
        });
      }
    }

    if (this.users.size === 0) {
      const fromEnv = this.opts.env.LITE_UI_ADMIN_PASSWORD;
      const password = fromEnv ?? randomBytes(9).toString('base64url');
      const hash = await bcrypt.hash(password, 10);
      this.users.set('admin', {
        username: 'admin',
        passwordHash: hash,
        role: 'admin',
        createdAt: new Date().toISOString(),
        mustChangePassword: true,
      });
      this.persist();
      if (!fromEnv) {
        // Printed once; the file only stores the hash.
        console.log(`[lite-ui] bootstrap admin password: ${password} (you must change it on first login)`);
      }
    }

    if (existsSync(this.opts.secretPath)) {
      this.jwtSecret = readFileSync(this.opts.secretPath, 'utf8').trim();
    } else {
      this.jwtSecret = randomBytes(48).toString('hex');
      writeFileSync(this.opts.secretPath, this.jwtSecret, { mode: 0o600 });
      chmodSync(this.opts.secretPath, 0o600);
    }
  }

  list(): PublicUser[] {
    return [...this.users.values()].map(publicUser);
  }

  get(username: string): UserRecord | undefined {
    return this.users.get(username);
  }

  async verify(username: string, password: string): Promise<UserRecord | null> {
    const user = this.users.get(username);
    if (!user) return null;
    return (await bcrypt.compare(password, user.passwordHash)) ? user : null;
  }

  private validateUsername(username: unknown): string {
    if (typeof username !== 'string' || !USERNAME_PATTERN.test(username)) {
      throw new AuthError('invalid', `invalid username: ${JSON.stringify(username)}`);
    }
    return username;
  }

  private validatePassword(password: unknown): string {
    if (typeof password !== 'string' || password.length < 8) {
      throw new AuthError('invalid', 'password must be at least 8 characters');
    }
    return password;
  }

  private validateRole(role: unknown): Role {
    if (!ROLES.includes(role as Role)) {
      throw new AuthError('invalid', `invalid role: ${JSON.stringify(role)}`);
    }
    return role as Role;
  }

  async create(input: { username: unknown; password: unknown; role: unknown }): Promise<PublicUser> {
    const username = this.validateUsername(input.username);
    const password = this.validatePassword(input.password);
    const role = this.validateRole(input.role);
    if (this.users.has(username)) {
      throw new AuthError('duplicate', `user "${username}" already exists`);
    }
    const record: UserRecord = {
      username,
      passwordHash: await bcrypt.hash(password, 10),
      role,
      createdAt: new Date().toISOString(),
      mustChangePassword: true,
    };
    this.users.set(username, record);
    this.persist();
    return publicUser(record);
  }

  async update(
    username: string,
    patch: { role?: unknown; password?: unknown },
    actor: string,
  ): Promise<PublicUser> {
    const existing = this.users.get(username);
    if (!existing) throw new AuthError('not_found', `unknown user "${username}"`);

    if (patch.role !== undefined) {
      const role = this.validateRole(patch.role);
      if (existing.role === 'admin' && role !== 'admin' && this.adminCount() <= 1) {
        throw new AuthError('forbidden', 'cannot demote the last admin');
      }
      existing.role = role;
    }
    if (patch.password !== undefined) {
      existing.passwordHash = await bcrypt.hash(this.validatePassword(patch.password), 10);
      existing.mustChangePassword = true;
    }
    this.persist();
    return publicUser(existing);
  }

  /** Self-service password change; clears the must-change flag. */
  async setPassword(username: string, password: string): Promise<void> {
    const existing = this.users.get(username);
    if (!existing) throw new AuthError('not_found', `unknown user "${username}"`);
    existing.passwordHash = await bcrypt.hash(this.validatePassword(password), 10);
    existing.mustChangePassword = false;
    this.persist();
  }

  remove(username: string, actor: string): void {
    const existing = this.users.get(username);
    if (!existing) throw new AuthError('not_found', `unknown user "${username}"`);
    if (username === actor) throw new AuthError('forbidden', 'cannot delete yourself');
    if (existing.role === 'admin' && this.adminCount() <= 1) {
      throw new AuthError('forbidden', 'cannot delete the last admin');
    }
    this.users.delete(username);
    this.persist();
  }

  private adminCount(): number {
    return [...this.users.values()].filter((u) => u.role === 'admin').length;
  }

  private persist() {
    const doc = {
      users: [...this.users.values()].map((u) => ({
        username: u.username,
        password_hash: u.passwordHash,
        role: u.role,
        created_at: u.createdAt,
        must_change_password: u.mustChangePassword,
      })),
    };
    const tmp = `${this.opts.authPath}.tmp-${process.pid}`;
    writeFileSync(tmp, stringify(doc), { mode: 0o600 });
    renameSync(tmp, this.opts.authPath);
  }
}
