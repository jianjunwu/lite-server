import { roleAtLeast, type Role } from '../api/auth';
import { useInstances } from '../api/hooks';
import { useAuth } from './AuthContext';
import { useInstance } from './InstanceContext';

/** Caller's role on the currently selected instance: the BFF annotates each
 * instance with the per-instance grant result; absent annotation (older BFF,
 * unknown instance) falls back to the global role. */
export function useEffectiveRole(): Role {
  const { user } = useAuth();
  const { instanceId } = useInstance();
  const instancesQuery = useInstances();
  const annotated = instancesQuery.data?.instances.find((i) => i.id === instanceId)?.effective_role;
  return annotated ?? user?.role ?? 'viewer';
}

/** Role gate for instance-scoped operations (load/unload/upload/...).
 * Settings management surfaces keep using the global role from useAuth. */
export function useCanInstance() {
  const effective = useEffectiveRole();
  return (required: Role) => roleAtLeast(effective, required);
}
