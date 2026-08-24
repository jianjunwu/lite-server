import { describe, expect, it } from 'vitest';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { InstanceStore } from '../src/config.js';
import { buildApp } from '../src/app.js';

function tempDir(): string {
  return mkdtempSync(join(tmpdir(), 'lite-ui-crud-'));
}

function seed(path: string, content: string) {
  writeFileSync(path, content);
}

const SEED = `
instances:
  - id: local
    name: Local
    base_url: http://localhost:8000
`;

describe('instance CRUD', () => {
  it('should_create_instance_and_persist_to_yaml', async () => {
    const dir = tempDir();
    const file = join(dir, 'instances.yaml');
    seed(file, SEED);
    const store = new InstanceStore({ configPath: file, env: {} });
    const app = buildApp(store);

    const res = await app.inject({
      method: 'POST',
      url: '/api/instances',
      payload: { id: 'gpu-1', name: 'GPU 1', base_url: 'http://10.0.0.2:8000' },
    });
    expect(res.statusCode).toBe(201);
    expect(res.json().instances.map((i: { id: string }) => i.id)).toEqual(['local', 'gpu-1']);

    const persisted = readFileSync(file, 'utf8');
    expect(persisted).toContain('gpu-1');
    expect(persisted).toContain('http://10.0.0.2:8000');
    await app.close();
  });

  it('should_reject_duplicate_id_with_409', async () => {
    const file = join(tempDir(), 'instances.yaml');
    seed(file, SEED);
    const app = buildApp(new InstanceStore({ configPath: file, env: {} }));
    const res = await app.inject({
      method: 'POST',
      url: '/api/instances',
      payload: { id: 'local', name: 'Dup', base_url: 'http://h:1' },
    });
    expect(res.statusCode).toBe(409);
    await app.close();
  });

  it('should_reject_invalid_payload_with_400', async () => {
    const file = join(tempDir(), 'instances.yaml');
    seed(file, SEED);
    const app = buildApp(new InstanceStore({ configPath: file, env: {} }));
    for (const payload of [
      { id: 'Bad ID', name: 'x', base_url: 'http://h:1' },
      { id: 'ok', name: 'x', base_url: 'ftp://h' },
      { id: 'ok', name: 'x' },
    ]) {
      const res = await app.inject({ method: 'POST', url: '/api/instances', payload });
      expect(res.statusCode).toBe(400);
    }
    await app.close();
  });

  it('should_update_instance_and_persist', async () => {
    const file = join(tempDir(), 'instances.yaml');
    seed(file, SEED);
    const app = buildApp(new InstanceStore({ configPath: file, env: {} }));
    const res = await app.inject({
      method: 'PUT',
      url: '/api/instances/local',
      payload: { name: 'Renamed', base_url: 'http://localhost:9000', admin_key: 'k' },
    });
    expect(res.statusCode).toBe(200);
    const persisted = readFileSync(file, 'utf8');
    expect(persisted).toContain('Renamed');
    expect(persisted).toContain('localhost:9000');
    expect(persisted).toContain('admin_key');
    // GET never leaks the key.
    const list = await app.inject({ method: 'GET', url: '/api/instances' });
    expect(JSON.stringify(list.json())).not.toContain('"k"');
    expect(list.json().instances[0].has_admin_key).toBe(true);
    await app.close();
  });

  it('should_delete_instance_and_persist', async () => {
    const file = join(tempDir(), 'instances.yaml');
    seed(file, SEED);
    const app = buildApp(new InstanceStore({ configPath: file, env: {} }));
    const res = await app.inject({ method: 'DELETE', url: '/api/instances/local' });
    expect(res.statusCode).toBe(200);
    expect(res.json().instances).toHaveLength(0);
    expect(readFileSync(file, 'utf8')).not.toContain('local');
    await app.close();
  });

  it('should_return_404_for_unknown_instance_on_update_and_delete', async () => {
    const file = join(tempDir(), 'instances.yaml');
    seed(file, SEED);
    const app = buildApp(new InstanceStore({ configPath: file, env: {} }));
    const put = await app.inject({ method: 'PUT', url: '/api/instances/nope', payload: { name: 'x' } });
    const del = await app.inject({ method: 'DELETE', url: '/api/instances/nope' });
    expect(put.statusCode).toBe(404);
    expect(del.statusCode).toBe(404);
    await app.close();
  });

  it('should_reject_mutation_of_readonly_env_instance_with_403', async () => {
    const file = join(tempDir(), 'instances.yaml');
    seed(file, SEED);
    const store = new InstanceStore({
      configPath: file,
      env: { LITE_UI_INSTANCES: JSON.stringify([{ id: 'env1', name: 'E', base_url: 'http://h:1' }]) },
    });
    const app = buildApp(store);
    const put = await app.inject({ method: 'PUT', url: '/api/instances/env1', payload: { name: 'x' } });
    const del = await app.inject({ method: 'DELETE', url: '/api/instances/env1' });
    expect(put.statusCode).toBe(403);
    expect(del.statusCode).toBe(403);
    await app.close();
  });

  it('should_probe_reachability_when_requested_and_reject_unreachable_with_422', async () => {
    const file = join(tempDir(), 'instances.yaml');
    seed(file, SEED);
    const app = buildApp(new InstanceStore({ configPath: file, env: {} }));
    const res = await app.inject({
      method: 'POST',
      url: '/api/instances?probe=true',
      payload: { id: 'dead', name: 'Dead', base_url: 'http://127.0.0.1:1' },
    });
    expect(res.statusCode).toBe(422);
    expect(res.json().error).toBe('instance_unreachable');
    // Not saved.
    expect(readFileSync(file, 'utf8')).not.toContain('dead');
    await app.close();
  });
});
