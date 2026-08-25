import { describe, expect, it } from 'vitest';
import { mergeModelList, mergeVersionList } from '../api/merge';
import type { ModelListItem, RepoIndexEntry } from '../api/types';

const repoEntry = (name: string, version: string | null, type = 'litapi'): RepoIndexEntry => ({
  name,
  version,
  path: `/repo/${name}/${version ?? 'pkg.lma'}`,
  has_config: true,
  type,
});

const loadedRow = (name: string, version: string, status = 'ready', workers = 2): ModelListItem => ({
  name,
  version,
  status,
  model_type: 'LitAPI',
  workers,
});

describe('mergeModelList', () => {
  it('should_mark_repo_only_model_as_unloaded', () => {
    const merged = mergeModelList([repoEntry('ghost', '1')], []);
    expect(merged).toHaveLength(1);
    expect(merged[0]).toMatchObject({ name: 'ghost', status: 'unloaded', versionCount: 1, workers: 0 });
  });

  it('should_mark_loaded_model_ready_and_count_version_union', () => {
    const merged = mergeModelList(
      [repoEntry('echo', '1'), repoEntry('echo', '2')],
      [loadedRow('echo', '1')],
    );
    expect(merged).toHaveLength(1);
    expect(merged[0]).toMatchObject({ name: 'echo', status: 'ready', versionCount: 2, workers: 2 });
  });

  it('should_keep_loaded_models_when_repo_index_is_unavailable', () => {
    // Older servers without /v2/repository/index: degrade to the loaded-only view.
    const merged = mergeModelList(undefined, [loadedRow('echo', '1')]);
    expect(merged).toMatchObject([{ name: 'echo', status: 'ready' }]);
  });

  it('should_flag_degraded_when_any_loaded_version_has_failed', () => {
    const merged = mergeModelList(undefined, [loadedRow('m', '1', 'failed')]);
    expect(merged[0].status).toBe('degraded');
  });

  it('should_flag_loading_when_a_version_is_starting', () => {
    const merged = mergeModelList(undefined, [loadedRow('m', '1', 'loading')]);
    expect(merged[0].status).toBe('loading');
  });

  it('should_list_registry_only_model_alongside_repo_models', () => {
    // Drift: loaded but deleted from disk — still listed.
    const merged = mergeModelList([repoEntry('ghost', '1')], [loadedRow('echo', '1')]);
    expect(merged.map((m) => m.name).sort()).toEqual(['echo', 'ghost']);
  });

  it('should_prefer_registry_model_type_and_fall_back_to_repo_type', () => {
    const withRegistry = mergeModelList([repoEntry('a', '1', 'ensemble')], [loadedRow('a', '1')]);
    expect(withRegistry[0].modelType).toBe('LitAPI');
    const repoOnly = mergeModelList([repoEntry('b', '1', 'ensemble')], []);
    expect(repoOnly[0].modelType).toBe('ensemble');
  });
});

describe('mergeVersionList', () => {
  const resp = {
    name: 'echo',
    active_version: '1',
    versions: [
      { version: '1', status: 'ready', active: true, weight: 100, workers: { ready: 2, total: 2 }, loaded_at: 1000 },
    ],
  };

  it('should_mark_all_versions_unloaded_when_registry_404s', () => {
    const merged = mergeVersionList([repoEntry('echo', '1'), repoEntry('echo', '2')], null);
    expect(merged.activeVersion).toBeNull();
    expect(merged.versions).toHaveLength(2);
    expect(merged.versions.every((v) => v.loaded === false && v.status === 'unloaded')).toBe(true);
  });

  it('should_overlay_registry_state_and_append_repo_only_versions', () => {
    const merged = mergeVersionList([repoEntry('echo', '1'), repoEntry('echo', '2')], resp);
    expect(merged.activeVersion).toBe('1');
    const v1 = merged.versions.find((v) => v.version === '1');
    const v2 = merged.versions.find((v) => v.version === '2');
    expect(v1).toMatchObject({ loaded: true, status: 'ready', active: true, weight: 100 });
    expect(v2).toMatchObject({ loaded: false, status: 'unloaded', active: false });
  });

  it('should_skip_null_version_artifacts_in_the_version_list', () => {
    const merged = mergeVersionList([repoEntry('pkg', null, 'artifact')], null);
    expect(merged.versions).toHaveLength(0);
  });
});
