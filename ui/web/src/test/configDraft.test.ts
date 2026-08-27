import { describe, expect, it } from 'vitest';
import { applyMergePatch, buildPatch, containsRedacted, deepEqual, stableStringify } from '../components/config/configDraft';

describe('buildPatch', () => {
  it('emits only changed top-level keys', () => {
    const { patch, skipped } = buildPatch(
      { max_batch_size: 16, accelerator: 'cuda' },
      { max_batch_size: 32, accelerator: 'cuda' },
    );
    expect(patch).toEqual({ max_batch_size: 32 });
    expect(skipped).toEqual([]);
  });

  it('maps removed keys to null', () => {
    const { patch } = buildPatch({ a: 1, b: 2 }, { a: 1 });
    expect(patch).toEqual({ b: null });
  });

  it('recurses into nested objects instead of replacing them', () => {
    const { patch } = buildPatch(
      { policies: { rate_limit: { rps: 10 }, auth: { keys: ['a'] } } },
      { policies: { rate_limit: { rps: 20 }, auth: { keys: ['a'] } } },
    );
    expect(patch).toEqual({ policies: { rate_limit: { rps: 20 } } });
  });

  it('maps nested removals to null', () => {
    const { patch } = buildPatch({ a: { b: 1, c: 2 } }, { a: { b: 1 } });
    expect(patch).toEqual({ a: { c: null } });
  });

  it('replaces arrays wholesale', () => {
    const { patch } = buildPatch({ devices: [0, 1] }, { devices: [2] });
    expect(patch).toEqual({ devices: [2] });
  });

  it('returns an empty patch when nothing changed', () => {
    const { patch } = buildPatch({ a: 1, b: { c: [1, 2] } }, { a: 1, b: { c: [1, 2] } });
    expect(patch).toEqual({});
  });

  it('never patches subtrees holding redacted secrets', () => {
    const { patch, skipped } = buildPatch(
      { policies: { auth: { keys: ['***', '***'] }, rate_limit: { rps: 10 } } },
      { policies: { auth: { keys: ['newkey'] }, rate_limit: { rps: 20 } } },
    );
    expect(patch).toEqual({ policies: { rate_limit: { rps: 20 } } });
    expect(skipped).toEqual(['policies.auth.keys']);
  });

  it('never deletes subtrees holding redacted secrets', () => {
    const { patch, skipped } = buildPatch(
      { api_key: '***', max_batch_size: 16 },
      { max_batch_size: 16 },
    );
    expect(patch).toEqual({});
    expect(skipped).toEqual(['api_key']);
  });
});

describe('applyMergePatch', () => {
  it('round-trips buildPatch output back to the draft', () => {
    const original = { a: 1, b: { c: 2, d: 3 }, e: [1] };
    const draft = { a: 10, b: { c: 2 }, f: 'new' };
    const { patch } = buildPatch(original, draft);
    expect(applyMergePatch(original, patch)).toEqual(draft);
  });

  it('merges into an empty object when the target is not an object', () => {
    expect(applyMergePatch(null, { a: 1, b: null })).toEqual({ a: 1 });
  });
});

describe('containsRedacted', () => {
  it('finds the placeholder at any depth', () => {
    expect(containsRedacted('***')).toBe(true);
    expect(containsRedacted({ a: [{ b: '***' }] })).toBe(true);
    expect(containsRedacted({ a: 'plain' })).toBe(false);
    expect(containsRedacted(42)).toBe(false);
  });
});

describe('stableStringify / deepEqual', () => {
  it('compares objects key-order-insensitively', () => {
    expect(deepEqual({ a: 1, b: 2 }, { b: 2, a: 1 })).toBe(true);
    expect(stableStringify({ b: 2, a: 1 })).toBe(stableStringify({ a: 1, b: 2 }));
  });
});
