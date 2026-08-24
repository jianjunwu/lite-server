import { readFileSync, writeFileSync, renameSync } from 'node:fs';
import { parse, stringify } from 'yaml';

export interface InstanceConfig {
  id: string;
  name: string;
  baseUrl: string;
  adminKey?: string;
  readonly: boolean;
}

export interface InstanceRegistry {
  list(): InstanceConfig[];
  get(id: string): InstanceConfig | undefined;
}

interface RawInstance {
  id?: unknown;
  name?: unknown;
  base_url?: unknown;
  admin_key?: unknown;
  admin_key_env?: unknown;
}

const ID_PATTERN = /^[a-z0-9][a-z0-9-]*$/;

function normalize(raw: RawInstance, env: NodeJS.ProcessEnv, source: string, readonly: boolean): InstanceConfig {
  if (typeof raw.id !== 'string' || !ID_PATTERN.test(raw.id)) {
    throw new Error(`invalid instance id in ${source}: ${JSON.stringify(raw.id)} (must match ${ID_PATTERN})`);
  }
  if (typeof raw.base_url !== 'string') {
    throw new Error(`invalid base_url for instance "${raw.id}" in ${source}: not a string`);
  }
  let url: URL;
  try {
    url = new URL(raw.base_url);
  } catch {
    throw new Error(`invalid base_url for instance "${raw.id}" in ${source}: ${raw.base_url}`);
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`invalid base_url for instance "${raw.id}" in ${source}: protocol must be http(s)`);
  }
  let adminKey: string | undefined;
  if (typeof raw.admin_key === 'string' && raw.admin_key.length > 0) {
    adminKey = raw.admin_key;
  } else if (typeof raw.admin_key_env === 'string' && raw.admin_key_env.length > 0) {
    adminKey = env[raw.admin_key_env];
  }
  return {
    id: raw.id,
    name: typeof raw.name === 'string' && raw.name.length > 0 ? raw.name : raw.id,
    baseUrl: url.origin + (url.pathname === '/' ? '' : url.pathname.replace(/\/$/, '')),
    adminKey,
    readonly,
  };
}

export function loadInstances(opts: { configPath: string; env: NodeJS.ProcessEnv }): InstanceRegistry {
  const instances = new Map<string, InstanceConfig>();

  let fileContent: string | null = null;
  try {
    fileContent = readFileSync(opts.configPath, 'utf8');
  } catch {
    // Missing file is fine: env-only or empty registry.
  }
  if (fileContent !== null) {
    const doc = parse(fileContent) as { instances?: RawInstance[] } | null;
    const rawList = doc?.instances ?? [];
    if (!Array.isArray(rawList)) {
      throw new Error(`invalid ${opts.configPath}: "instances" must be a list`);
    }
    for (const raw of rawList) {
      const inst = normalize(raw, opts.env, opts.configPath, false);
      if (instances.has(inst.id)) {
        throw new Error(`duplicate instance id "${inst.id}" in ${opts.configPath}`);
      }
      instances.set(inst.id, inst);
    }
  }

  const envJson = opts.env.LITE_UI_INSTANCES;
  if (envJson) {
    const rawList = JSON.parse(envJson) as RawInstance[];
    if (!Array.isArray(rawList)) {
      throw new Error('invalid LITE_UI_INSTANCES: must be a JSON array');
    }
    for (const raw of rawList) {
      const inst = normalize(raw, opts.env, 'LITE_UI_INSTANCES', true);
      if (instances.has(inst.id)) {
        throw new Error(`duplicate instance id "${inst.id}" (env)`);
      }
      instances.set(inst.id, inst);
    }
  }

  return {
    list: () => [...instances.values()],
    get: (id) => instances.get(id),
  };
}

export type StoreErrorCode = 'invalid' | 'duplicate' | 'not_found' | 'readonly';

export class StoreError extends Error {
  constructor(
    public code: StoreErrorCode,
    message: string,
  ) {
    super(message);
    this.name = 'StoreError';
  }
}

export interface InstanceInput {
  id?: unknown;
  name?: unknown;
  base_url?: unknown;
  admin_key?: unknown;
}

/**
 * Mutable instance registry with atomic yaml write-back. Env-injected
 * instances stay readonly and are never persisted.
 */
export class InstanceStore implements InstanceRegistry {
  private instances = new Map<string, InstanceConfig>();

  constructor(private opts: { configPath: string; env: NodeJS.ProcessEnv }) {
    const loaded = loadInstances(opts);
    for (const inst of loaded.list()) {
      this.instances.set(inst.id, inst);
    }
  }

  list(): InstanceConfig[] {
    return [...this.instances.values()];
  }

  get(id: string): InstanceConfig | undefined {
    return this.instances.get(id);
  }

  create(input: InstanceInput): InstanceConfig {
    const inst = this.validate(input);
    if (this.instances.has(inst.id)) {
      throw new StoreError('duplicate', `instance id "${inst.id}" already exists`);
    }
    this.instances.set(inst.id, inst);
    this.persist();
    return inst;
  }

  update(id: string, patch: Omit<InstanceInput, 'id'>): InstanceConfig {
    const existing = this.instances.get(id);
    if (!existing) throw new StoreError('not_found', `unknown instance "${id}"`);
    if (existing.readonly) throw new StoreError('readonly', `instance "${id}" is env-managed (readonly)`);
    const merged = this.validate({
      id,
      name: patch.name ?? existing.name,
      base_url: patch.base_url ?? existing.baseUrl,
      admin_key: patch.admin_key !== undefined ? patch.admin_key : existing.adminKey,
    });
    this.instances.set(id, merged);
    this.persist();
    return merged;
  }

  remove(id: string): void {
    const existing = this.instances.get(id);
    if (!existing) throw new StoreError('not_found', `unknown instance "${id}"`);
    if (existing.readonly) throw new StoreError('readonly', `instance "${id}" is env-managed (readonly)`);
    this.instances.delete(id);
    this.persist();
  }

  private validate(input: InstanceInput): InstanceConfig {
    try {
      return normalize(input, this.opts.env, 'api', false);
    } catch (err) {
      throw new StoreError('invalid', err instanceof Error ? err.message : String(err));
    }
  }

  /** Atomic write: temp file + rename. Only file-managed instances persist. */
  private persist() {
    const doc = {
      instances: this.list()
        .filter((i) => !i.readonly)
        .map((i) => ({
          id: i.id,
          name: i.name,
          base_url: i.baseUrl,
          ...(i.adminKey ? { admin_key: i.adminKey } : {}),
        })),
    };
    const tmp = `${this.opts.configPath}.tmp-${process.pid}`;
    writeFileSync(tmp, stringify(doc));
    renameSync(tmp, this.opts.configPath);
  }
}
