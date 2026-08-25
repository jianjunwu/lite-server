import { useInstance } from './InstanceContext';

/** Link builder that keeps the current ?i={instance} selection across
 * navigation. Dropping it silently resets the view to the first instance. */
export function useInstanceLink() {
  const { instanceId } = useInstance();
  return (path: string): string =>
    instanceId
      ? `${path}${path.includes('?') ? '&' : '?'}i=${encodeURIComponent(instanceId)}`
      : path;
}
