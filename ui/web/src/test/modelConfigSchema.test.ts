import { describe, expect, it } from 'vitest';
import { groupModelConfig } from '../components/config/modelConfigSchema';

describe('groupModelConfig', () => {
  it('should group schema-known fields under their declared groups', () => {
    const { groups, advanced } = groupModelConfig({
      max_batch_size: 16,
      accelerator: 'cuda',
      request_timeout: 30,
    });
    expect(groups.map((g) => g.meta.key)).toEqual(['batching', 'resources', 'queue']);
    expect(groups[0].entries).toEqual([['max_batch_size', 16]]);
    expect(advanced).toEqual([]);
  });

  it('should preserve MODEL_CONFIG_GROUPS ordering regardless of input order', () => {
    const { groups } = groupModelConfig({
      request_timeout: 30,
      max_batch_size: 16,
    });
    expect(groups.map((g) => g.meta.key)).toEqual(['batching', 'queue']);
  });

  it('should bucket unknown keys into advanced untouched', () => {
    const { groups, advanced } = groupModelConfig({
      max_batch_size: 16,
      ensemble: { steps: ['a', 'b'] },
      custom_flag: true,
    });
    expect(groups.map((g) => g.meta.key)).toEqual(['batching']);
    expect(advanced).toEqual([
      ['ensemble', { steps: ['a', 'b'] }],
      ['custom_flag', true],
    ]);
  });

  it('should return no groups for an empty config', () => {
    const { groups, advanced } = groupModelConfig({});
    expect(groups).toEqual([]);
    expect(advanced).toEqual([]);
  });
});
