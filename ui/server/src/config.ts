import { readFileSync } from 'node:fs';
import { parse } from 'yaml';

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
