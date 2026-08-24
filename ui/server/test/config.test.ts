import { describe, expect, it } from 'vitest';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadInstances } from '../src/config.js';

function writeYaml(content: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'lite-ui-test-'));
  const path = join(dir, 'instances.yaml');
  writeFileSync(path, content);
  return path;
}

describe('loadInstances', () => {
  it('should_load_instances_from_yaml_file', () => {
    const path = writeYaml(`
instances:
  - id: local
    name: Local dev
    base_url: http://localhost:8000
  - id: prod
    name: Prod
    base_url: http://10.0.0.11:8000
    admin_key: secret
`);
    const registry = loadInstances({ configPath: path, env: {} });
    expect(registry.list()).toHaveLength(2);
    expect(registry.get('prod')?.adminKey).toBe('secret');
  });

  it('should_resolve_admin_key_from_env_when_admin_key_env_set', () => {
    const path = writeYaml(`
instances:
  - id: prod
    name: Prod
    base_url: http://10.0.0.11:8000
    admin_key_env: PROD_KEY
`);
    const registry = loadInstances({ configPath: path, env: { PROD_KEY: 'from-env' } });
    expect(registry.get('prod')?.adminKey).toBe('from-env');
  });

  it('should_mark_env_injected_instances_readonly', () => {
    const registry = loadInstances({
      configPath: '/nonexistent/instances.yaml',
      env: { LITE_UI_INSTANCES: JSON.stringify([{ id: 'env1', name: 'Env', base_url: 'http://h:1' }]) },
    });
    const inst = registry.get('env1');
    expect(inst?.readonly).toBe(true);
    expect(inst?.baseUrl).toBe('http://h:1');
  });

  it('should_throw_on_invalid_id', () => {
    const path = writeYaml(`
instances:
  - id: "Bad ID!"
    name: X
    base_url: http://localhost:8000
`);
    expect(() => loadInstances({ configPath: path, env: {} })).toThrow(/id/i);
  });

  it('should_throw_on_invalid_base_url', () => {
    const path = writeYaml(`
instances:
  - id: ok
    name: X
    base_url: ftp://nope
`);
    expect(() => loadInstances({ configPath: path, env: {} })).toThrow(/base_url/i);
  });

  it('should_throw_on_duplicate_id', () => {
    const path = writeYaml(`
instances:
  - { id: a, name: A, base_url: "http://h:1" }
  - { id: a, name: B, base_url: "http://h:2" }
`);
    expect(() => loadInstances({ configPath: path, env: {} })).toThrow(/duplicate/i);
  });

  it('should_return_empty_registry_when_file_missing', () => {
    const registry = loadInstances({ configPath: '/nonexistent/x.yaml', env: {} });
    expect(registry.list()).toHaveLength(0);
  });

  it('should_strip_trailing_slash_from_base_url', () => {
    const path = writeYaml(`
instances:
  - { id: a, name: A, base_url: "http://h:8000/" }
`);
    const registry = loadInstances({ configPath: path, env: {} });
    expect(registry.get('a')?.baseUrl).toBe('http://h:8000');
  });
});
