import { createContext, useContext, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';

interface InstanceContextValue {
  instanceId: string | null;
  setInstanceId: (id: string) => void;
}

const InstanceContext = createContext<InstanceContextValue>({
  instanceId: null,
  setInstanceId: () => {},
});

/** Current instance, stored in the ?i= query param so links are shareable. */
export function InstanceProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams();
  const instanceId = searchParams.get('i');

  const setInstanceId = (id: string) => {
    const next = new URLSearchParams(searchParams);
    next.set('i', id);
    setSearchParams(next, { replace: true });
  };

  return (
    <InstanceContext.Provider value={{ instanceId, setInstanceId }}>
      {children}
    </InstanceContext.Provider>
  );
}

export function useInstance() {
  return useContext(InstanceContext);
}
