import { renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { useCanInstance, useEffectiveRole } from '../context/useEffectiveRole';
import type { InstanceInfo } from '../api/types';

let mockInstances: Partial<InstanceInfo>[] = [];
let mockInstanceId: string | null = 'prod';
let mockGlobalRole = 'operator';

vi.mock('../context/InstanceContext', () => ({
  useInstance: () => ({ instanceId: mockInstanceId, setInstanceId: vi.fn() }),
}));

vi.mock('../context/AuthContext', () => ({
  useAuth: () => ({ user: { username: 'u', role: mockGlobalRole } }),
}));

vi.mock('../api/hooks', () => ({
  useInstances: () => ({ data: { instances: mockInstances } }),
}));

describe('useEffectiveRole', () => {
  it('should_use_the_instance_grant_role_when_annotated', () => {
    mockInstances = [{ id: 'prod', effective_role: 'viewer' }];
    const { result } = renderHook(() => useEffectiveRole());
    expect(result.current).toBe('viewer');
  });

  it('should_fall_back_to_the_global_role_without_annotation', () => {
    mockInstances = [{ id: 'prod' }];
    const { result } = renderHook(() => useEffectiveRole());
    expect(result.current).toBe('operator');
  });

  it('should_fall_back_to_the_global_role_for_an_unknown_instance', () => {
    mockInstances = [{ id: 'prod', effective_role: 'viewer' }];
    mockInstanceId = 'ghost';
    const { result } = renderHook(() => useEffectiveRole());
    expect(result.current).toBe('operator');
    mockInstanceId = 'prod';
  });
});

describe('useCanInstance', () => {
  it('should_gate_on_the_effective_role', () => {
    mockInstances = [{ id: 'prod', effective_role: 'viewer' }];
    const { result } = renderHook(() => useCanInstance());
    expect(result.current('viewer')).toBe(true);
    expect(result.current('operator')).toBe(false);
    expect(result.current('admin')).toBe(false);
  });
});
