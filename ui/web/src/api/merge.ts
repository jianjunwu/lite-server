// Repository ∪ registry merge: the repository scan is the source of truth for
// which models exist; the registry adds runtime state for the loaded ones.
// This is what keeps unloaded models visible instead of vanishing.

import type { ModelListItem, RepoIndexEntry, VersionInfo, VersionsResponse } from './types';
import { statusKind } from '../components/StatusBadge';

export type MergedModelStatus = 'ready' | 'loading' | 'degraded' | 'unloaded';

export interface MergedModel {
  name: string;
  status: MergedModelStatus;
  /** |repo versions ∪ loaded versions| — what the Versions column shows. */
  versionCount: number;
  workers: number;
  modelType: string;
  /** Repository versions available for loading (null-version artifacts excluded). */
  repoVersions: string[];
  /** Loaded in the registry but absent from the repository scan — the disk
   * copy vanished underneath a running model. Only computed when the scan
   * succeeded (undefined repo = old server = no verdict). */
  drifted: boolean;
}

export function mergeModelList(
  repo: RepoIndexEntry[] | undefined,
  loaded: ModelListItem[] | undefined,
): MergedModel[] {
  const byName = new Map<string, MergedModel>();
  const ensure = (name: string): MergedModel => {
    let m = byName.get(name);
    if (!m) {
      m = { name, status: 'unloaded', versionCount: 0, workers: 0, modelType: 'unknown', repoVersions: [], drifted: false };
      byName.set(name, m);
    }
    return m;
  };

  const versionSets = new Map<string, Set<string>>();
  const seen = (name: string, version: string) => {
    let set = versionSets.get(name);
    if (!set) versionSets.set(name, (set = new Set()));
    set.add(version);
  };
  const repoNames = new Set<string>();

  for (const entry of repo ?? []) {
    const m = ensure(entry.name);
    repoNames.add(entry.name);
    if (m.modelType === 'unknown') m.modelType = entry.type;
    if (entry.version !== null) {
      m.repoVersions.push(entry.version);
      seen(entry.name, entry.version);
    }
  }
  for (const row of loaded ?? []) {
    const m = ensure(row.name);
    m.modelType = row.model_type;
    m.workers += row.workers;
    seen(row.name, row.version);
  }

  for (const m of byName.values()) {
    m.versionCount = versionSets.get(m.name)?.size ?? 0;
    m.repoVersions.sort();
    const rows = (loaded ?? []).filter((r) => r.name === m.name);
    if (rows.length > 0) {
      const kinds = rows.map((r) => statusKind(r.status));
      m.status = kinds.includes('error')
        ? 'degraded'
        : kinds.includes('loading')
          ? 'loading'
          : 'ready';
      if (repo !== undefined && !repoNames.has(m.name)) m.drifted = true;
    }
  }

  return [...byName.values()].sort((a, b) => a.name.localeCompare(b.name));
}

export interface MergedVersion extends VersionInfo {
  loaded: boolean;
}

export interface MergedVersions {
  versions: MergedVersion[];
  activeVersion: string | null;
}

/** Version list for one model: registry state overlaid on repo contents.
 * `resp` is null when the versions endpoint 404s (nothing loaded). */
export function mergeVersionList(
  repoEntries: RepoIndexEntry[],
  resp: VersionsResponse | null,
): MergedVersions {
  const versions: MergedVersion[] = (resp?.versions ?? []).map((v) => ({ ...v, loaded: true }));
  const loadedNames = new Set(versions.map((v) => v.version));
  for (const entry of repoEntries) {
    if (entry.version === null || loadedNames.has(entry.version)) continue;
    versions.push({
      version: entry.version,
      status: 'unloaded',
      active: false,
      weight: 0,
      workers: { ready: 0, total: 0 },
      loaded_at: null,
      loaded: false,
    });
  }
  const result = { versions, activeVersion: resp?.active_version ?? null };
  // Weight 0 on every loaded version means "no routing configured": the
  // router then sends all traffic to the active version. Surface that
  // effective 100% instead of a misleading "serving 0% of traffic".
  if (result.activeVersion !== null) {
    const loaded = result.versions.filter((v) => v.loaded);
    if (loaded.length > 0 && loaded.every((v) => v.weight === 0)) {
      for (const v of loaded) if (v.active) v.weight = 100;
    }
  }
  return result;
}
