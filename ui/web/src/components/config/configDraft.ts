/** M2 client-side draft logic (admin-enhancement plan §4.3): deep merge-patch
 * construction (RFC 7386), merged-result preview, and stable stringify for
 * the diff view. Pure functions — the vitest target. */

/** True when any string leaf in the subtree is the server's redaction
 * placeholder. Such subtrees must never be sent back: writing "***" would
 * clobber the real secret on disk. */
export function containsRedacted(value: unknown): boolean {
  if (value === '***') return true;
  if (Array.isArray(value)) return value.some(containsRedacted);
  if (value !== null && typeof value === 'object') {
    return Object.values(value as Record<string, unknown>).some(containsRedacted);
  }
  return false;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

/** Key-order-insensitive deep equality for JSON trees. */
export function deepEqual(a: unknown, b: unknown): boolean {
  return stableStringify(a) === stableStringify(b);
}

/** JSON.stringify with recursively sorted object keys — stable output for
 * comparisons and the diff view. */
export function stableStringify(value: unknown): string {
  return JSON.stringify(sortKeys(value), null, 2);
}

function sortKeys(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(sortKeys);
  if (isPlainObject(v)) {
    return Object.fromEntries(
      Object.entries(v)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, val]) => [k, sortKeys(val)]),
    );
  }
  return v;
}

export interface PatchBuild {
  /** RFC 7386 merge-patch; empty object when nothing changed. */
  patch: Record<string, unknown>;
  /** Human-readable notes, e.g. skipped redacted subtrees. */
  skipped: string[];
}

/**
 * Deep-diff original vs draft into an RFC 7386 merge-patch: changed/added
 * keys map to their new value, removed keys map to null, objects recurse,
 * arrays compare-and-replace wholesale. Subtrees holding redacted secrets
 * are never patched (skipped with a note instead).
 */
export function buildPatch(
  original: Record<string, unknown>,
  draft: Record<string, unknown>,
): PatchBuild {
  const skipped: string[] = [];
  const patch = deepDiff(original, draft, '', skipped);
  return { patch: isPlainObject(patch) ? patch : {}, skipped };
}

/** Returns the patch fragment for next-vs-orig, or undefined when unchanged. */
function deepDiff(orig: unknown, next: unknown, path: string, skipped: string[]): unknown {
  if (isPlainObject(orig) && isPlainObject(next)) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(next)) {
      const childPath = path ? `${path}.${k}` : k;
      if (!(k in orig)) {
        out[k] = v;
        continue;
      }
      const d = deepDiff(orig[k], v, childPath, skipped);
      if (d !== undefined) out[k] = d;
    }
    for (const k of Object.keys(orig)) {
      if (!(k in next)) {
        const childPath = path ? `${path}.${k}` : k;
        if (containsRedacted(orig[k])) {
          skipped.push(childPath);
          continue;
        }
        out[k] = null;
      }
    }
    return Object.keys(out).length > 0 ? out : undefined;
  }
  if (deepEqual(orig, next)) return undefined;
  if (containsRedacted(orig)) {
    skipped.push(path);
    return undefined;
  }
  return next;
}

/** RFC 7386 merge application — the client-side preview of what the server
 * will produce by merging `patch` into the on-disk tree. */
export function applyMergePatch(target: unknown, patch: unknown): unknown {
  if (!isPlainObject(patch)) return patch;
  const base: Record<string, unknown> = isPlainObject(target) ? { ...target } : {};
  for (const [k, v] of Object.entries(patch)) {
    if (v === null) {
      delete base[k];
    } else {
      base[k] = applyMergePatch(base[k], v);
    }
  }
  return base;
}
